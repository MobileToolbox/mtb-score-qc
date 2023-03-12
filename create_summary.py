import requests
import json
import pandas as pd
import numpy as np
from pandas import json_normalize
import math
import synapseclient 
from pyarrow import fs
import pyarrow.parquet as pq
import bridgeclient
from synapseHelpers import thisCodeInSynapse

PARENT_BRIDGE_PROJECT = 'syn26253351'
PARENT_BRIDGE_FILE_VIEW = 'syn27651981'
PARENT_BRIDGE_PARTICIPANTS = 'syn26890553'
QUERY = 'select * from %s'
FIRST_COLUMNS = ['recordId', 'wasCompleted', 'inSynapseParent', 'inStudyProject', 'inParquet', 'inScores', 'assessmentId',
                 'startedOn', 'externalId', 'healthCode', 'studyId', 'studyMemberships', 'instanceGuid']


syn = synapseclient.login()
bridge = bridgeclient.bridgeConnector(email=None, password=None, study='mobile-toolbox')
      
usedEntities = list()

def getStudies(syn, trackingTable='syn50615998'):
    return syn.tableQuery(QUERY %trackingTable).asDataFrame()

def studyEntityIds(syn, studyId):
    """Returns synapse entity ids of key components of the study.
    This is inferred from both the templetized structure of projects and the names of entities.

    :param syn:          A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param studyId:      The synapse Id of the project belonging to a study

    :returns:           A dictionary with keys: projectId, bridgeFileViewId, participantVersionsId, parquetFolderId, scoreFileId
    """
    entities = list(syn.getChildren(studyId))
    entDict =dict()
    entDict['projectId'] = studyId
    try:
        entDict['bridgeFileViewId'] = [ent['id'] for ent in entities if ent['name']=='Bridge Raw Data View' and
                                       ent['type']=='org.sagebionetworks.repo.model.table.EntityView'][0]
    except IndexError:
        entDict['bridgeFileViewId'] = None

    try:
        entDict['participantVersionsId'] = [ent['id'] for ent in entities if ent['name']=='Participant Versions' and
                                            ent['type']=='org.sagebionetworks.repo.model.table.TableEntity'][0]
    except IndexError:
        entDict['participantVersionsId'] = None

    try:
        entDict['parquetFolderId'] = [ent['id'] for ent in entities if ent['name']=='parquet' and
                                      ent['type']=='org.sagebionetworks.repo.model.Folder'][0]
    except IndexError:
        entDict['parquetFolderId'] = None

    #Score File is in subfolder. Get the folder and get add file in folder
    scoreFolderId = [ent['id'] for ent in entities if ent['name']=='score' and
                     ent['type']=='org.sagebionetworks.repo.model.Folder'][0]
    try:
        entDict['scoreFileId'] = list(syn.getChildren(scoreFolderId))[0]['id']
    except IndexError:
        entDict['scoreFileId'] = None
        
    return entDict


def getBridgeAdherenceData(bridge, studyNames):
    """Find all adherence records in Bridge in the studyNames.

    :param bridge:       A bridgeclient: bridge = bridgeclient.login()- Must be logged into Bridge

    :param studyNames:   Container with study names (e.g. ['cxhnxd'])

    :returns: data frame of adherence records
    """
    #Get all participants in Bridge
    #Todo could perhaps go through all studyIds
    participants = bridge.getParticipants() #startDate='2022-04-16T12:36:10.994Z', endDate='2022-04-19T12:36:10.994Z')
    # Filter out participants in studies being observed
    idx = [(a[0] if a else None) in studyNames for a in participants['studyIds']]
    participants = participants.loc[idx]
    print(len(participants), 'Participants in studies')

    #Get adherence records for each individual then merge together with relevant participant fields
    dfs = [bridge.getAdherence(userId=row['id'], studyId=row['studyIds'][0]) for i, row in participants.iterrows()]
    adherence = (pd.concat(dfs)
                 .drop('type', axis=1)
                 .merge(participants[['id', 'studyIds', 'externalId']], left_on='userId', right_on='id')
                 .drop('id', axis=1)
                 )
    adherence['studyIds'] = adherence.studyIds.map(lambda x: x[0]) #Assumes that participants are in only 1 study
    adherence.insert(0, "wasCompleted", (~adherence['finishedOn'].isnull() & ~adherence['declined']).astype('int'))
    # TODO add another column that includes upload information when implemented. Jack is addingn uploadedOn
    # to the adherence Record

    return adherence

def getSynapseBridgeExports(syn, studyNames):
    """Finds all data in the parent Bridge Export project that has been uploaded to Synaose.
    Throws out duplicate uplpads and filters by studyNames.

    :param syn:          A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param studyNames:   Container with study names (e.g. 'cxhnxd')

    :returns: data frame of data in Synapse
    """

    allDataEnt = syn.tableQuery(QUERY %PARENT_BRIDGE_FILE_VIEW)
    allParticipantsEnt = syn.tableQuery(QUERY %PARENT_BRIDGE_PARTICIPANTS)
    usedEntities.extend([allDataEnt.tableId, allParticipantsEnt.tableId]) #Add for provenance

    #Fetch data and filter duplicates
    data = (allDataEnt
            .asDataFrame()
            .sort_values('uploadedOn') #Sort before removing duplicates
            .drop_duplicates(['recordId'], keep='last') #Remove multiple uploads
            )
 
    participants = allParticipantsEnt.asDataFrame()

    # Merge the participant information with the data
    data = (data
            .merge(participants, on=['healthCode', 'participantVersion'], how='inner')
            .assign(studyId = lambda x: x.studyMemberships.str.strip('|').str.split('=').str[0])
            .assign(externalId = lambda x: x.studyMemberships.str.strip('|').str.split('=').str[1])
            .drop_duplicates(['recordId'], keep='last') #Remove multiple uploads
            )
    data.insert(0, "inSynapseParent", 1)
    return data.loc[data['studyId'].isin(studyNames)] # filter out tests and non-studies

    
def getJSONStudyData(syn, studyViews):
    """Given a list of entity views  get all recordiIds of Data Uploaded by Bridge in Study

    :param syn:          A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param studyViews:   Collection of (e.g. pandas series) of synapse entity ids pointing to Entity Views

    :returns: dataFrame with recordId column and "inStudyProject Column
    """
    tables = [syn.tableQuery('select recordId from %s' %table).asDataFrame() for table in studyViews]
    usedEntities.extend(list(studyViews))
    return (pd.concat(tables)
            .assign(inStudyProject=1)
            .drop_duplicates()
            )

def getStudyParquetData(syn, studyParquetFolders):
    """Given a list of parquet folders get all recordiIds that has data in Parquet format.

    :param syn:          A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param studyParquetFolders:   Collection of (e.g. pandas series) of synapse FolderEntity with Parquet data.

    :returns: dataFrame with recordId column and "inParquet" Column
    """
    dfs = []
    for folder_id in studyParquetFolders:
        print(folder_id)
        #Fetch access token for folder
        token = syn.get_sts_storage_token(folder_id, permission="read_only") # , output_format='bash'
        s3 = fs.S3FileSystem(access_key=token['accessKeyId'], secret_key = token['secretAccessKey'], session_token = token['sessionToken'])

        #Fetch the recordIds fromt the taskData
        try:
            dataset = pq.ParquetDataset(token['bucket']+'/'+token['baseKey']+'/'+'dataset_sharedschema_v1/', filesystem=s3) 
        except FileNotFoundError: #This indicates that it is an older project with different schema (e.g. construct validation)
            dataset = pq.ParquetDataset(token['bucket']+'/'+token['baseKey']+'/'+'dataset_taskresult/', filesystem=s3)
        df = dataset.read().to_pandas()
        dfs.append(df[['recordid']])
        usedEntities.append(folder_id)

    return (pd.concat(dfs)
            .drop_duplicates()
            .assign(inParquet=1)
            .rename(columns={'recordid':'recordId'})
            )


def getStudyScoreData(syn, studyScoreFiles):
    """Given a list of parquet folders get all recordiIds that has data in Parquet format.

    :param syn:          A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param studyScoreFiles:   Collection of (e.g. pandas series) of synapse fileEntities with Scores

    :returns: dataFrame with recordId column and "inParquet" Column
    """
    dfs = []
    for file_id in studyScoreFiles:
        print(file_id)
        if file_id is None:
            continue
        ent = syn.get(file_id)
        usedEntities.append(ent)
        df = pd.read_csv(ent.path)
        dfs.append(df[['recordid']])
        
    return (pd.concat(dfs)
            .drop_duplicates()
            .assign(inScores=1)
            .rename(columns={'recordid':'recordId'})
            )

def visualizeAdherence(allData, studies):
    import seaborn as sns
    import upsetplot
    import pylab as plt

    #Remove fnamea as it isn't scored
    allData = allData.query("assessmentId!='fnamea'")
    #Generate heatmap
    df = allData[['wasCompleted', 'inSynapseParent', 'inStudyProject', 'inParquet', 'inScores']]
    sns.heatmap(df, cbar=False)

    #Generate Upset Plot
    df_up = df.astype('bool').groupby(list(df.columns)).size()
    upsetplot.plot(df_up, orientation='horizontal')

    
    #Per study heatmaps
    for i, df in allData.groupby('studyId'):
        df1 = df[['wasCompleted', 'inSynapseParent', 'inStudyProject', 'inParquet', 'inScores']]
        df1 = df1.fillna(0)
        plt.figure()
        sns.heatmap(df1.astype('int'), cbar=False)
        label = (studies.query("studyId=='%s'"%i)['name']+'('+
                 studies.query("studyId=='%s'"%i)['id'] + ')').iloc[0]
        plt.title(label)
        print(label)


    #generate Sankey plot
    


if __name__ == "__main__":
    #Get information about studies in Synapse
    studies = getStudies(syn)
    studies = studies.merge(pd.DataFrame([studyEntityIds(syn, id) for id in studies.id]), left_on='id', right_on='projectId')

    #TODO remove filter to run on all data
    studies = studies.iloc[[3,4],:]
    #TODO ignore construct validation
 
    
    #Get adherence Records from Bridge
    adherence = getBridgeAdherenceData(bridge, studyNames = set(studies['studyId']))
    print('Number of adherenceRecords', len(adherence))

    #Get data from parent Bridge project
    bridgeSynapseUpload = getSynapseBridgeExports(syn,  set(studies.studyId))
    allData = adherence.merge(bridgeSynapseUpload, on=['instanceGuid', 'externalId'], how='outer')
    allData.insert(0, 'inSynapseParent', allData.pop('inSynapseParent'))
    print('Number of records in parent export', len(bridgeSynapseUpload))
    print(allData.shape)
    
    #Merge in information from study specific BridgeExports
    studySynapseData = getJSONStudyData(syn, studies['bridgeFileViewId'])
    allData = studySynapseData.merge(allData, on='recordId', how='outer')
    print('Number of records in studyProjects', len(studySynapseData))


    #Merge in information from ParquetData
    parquetData = getStudyParquetData(syn, studies['parquetFolderId'])
    allData = parquetData.merge(allData, on='recordId', how='outer')
    print('Number of records in parquetData', len(parquetData))

    
    #Merge in information from Score Files
    scoreData = getStudyScoreData(syn, studies['scoreFileId'])
    allData = scoreData.merge(allData, on='recordId', how='outer')
    print('Number of records in scoreData', len(scoreData))

    #Clean up Data: sort by Date, fillna for bool columns, extract os
    allData = (allData
               .assign(assessmentGuid=np.where(allData.assessmentGuid_y.isnull(), allData.assessmentGuid_x, allData.assessmentGuid_y))
               .drop(['assessmentGuid_x', 'assessmentGuid_y'], axis=1)
               .assign(clientTimeZone=np.where(allData.clientTimeZone_y.isnull(), allData.clientTimeZone_x, allData.clientTimeZone_y))
               .drop(['clientTimeZone_x', 'clientTimeZone_y'], axis=1)
               .assign(studyId=np.where(allData.studyIds.isnull(), allData.studyId, allData.studyIds))
               .drop('studyIds', axis=1)
               .sort_values(['studyId', 'startedOn'], ascending=True)
               .reset_index(drop=True)
          )
    #Reorder Columns
    allData = allData[FIRST_COLUMNS + sorted(set(allData.columns)-set(FIRST_COLUMNS))]

    #Fill in 0 for all places we don't have data
    boolCols = ['inScores', 'inParquet', 'inStudyProject', 'inSynapseParent', 'wasCompleted']
    allData[boolCols] = allData[boolCols].fillna(value=0)
    #Change timestamps to time dates
    timestampCols = ['modifiedOn', 'createdOn', 'exportedOn', 'eventTimestamp_y']
    allData[timestampCols] = allData[timestampCols].astype('datetime64[ms]')
    #Change time strings to timedates
    timeStringCols =  ['uploadedOn', 'finishedOn', 'startedOn', 'eventTimestamp_x']
    allData[timeStringCols] = allData[timeStringCols].apply(pd.to_datetime)

    
    

    
#    #Store data to Synapse
#    allData.to_csv('completion_records.csv')
#    syn.store(synapseclient.File('completion_records.csv', parent='syn26253351'),
#              used=usedEntities, executed = thisCodeInSynapse('syn1774100'))
#
    visualizeAdherence(allData.query("assessmentId!='fnamea'"), studies)
