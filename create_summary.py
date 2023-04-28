import json
import pandas as pd
import numpy as np
from pandas import json_normalize
import synapseclient 
from pyarrow import fs
import pyarrow.parquet as pq
import bridgeclient
from synapseHelpers import thisCodeInSynapse
from tqdm import tqdm

PARENT_BRIDGE_PROJECT = 'syn26253351'
PARENT_BRIDGE_FILE_VIEW = 'syn27651981'
PARENT_BRIDGE_PARTICIPANTS = 'syn26890553'
QUERY = 'select * from %s'
FIRST_COLUMNS = ['recordId', 'wasCompleted', 'inSynapseParent', 'inStudyProject', 'inParquet', 'inScores', 'assessmentId',
                 'startedOn', 'externalId', 'healthCode', 'studyId', 'studyMemberships', 'instanceGuid']

NON_SHARING_USERS_IDS = ['-q-or4T2YwHzSRCn5vYThB9p', '0Yh0AIZMDr9YfZ2Pn_YG_Jo9', '1lYj8QEd26aOe4bbmJyspc-5', '1TxW4vsFCug37so9E8c217WQ', '2-Ht4PbtHtYVyiTyX7N1_bIq', '39iYuL3pEBamI49yuubwgyjW', '4cPy-ypC93OA_aDTFpqBmABx', '5y6kzVUAVvM7jiRuv-58Sv-E', '7B3Mz5LdK3hz0OHtgb_-P7HE', '8KLadsK3gXyIG7ZZ8ZmHFH2D', 'ADz4u-ypRetc7X-zcocAYzzY', 'AVxq2Z8df9xLepePhW4SVkLK', 'B23vuLk4-KZ3EaK6fno9450n', 'BgvtLFxsjKRUphhylQHn2uTr', 'C47o8CXOfa74Xn8W5N7nKyQF', 'c6BAonK0iKhZvtqp9qUG1oh0', 'cGIwz1KztqTVjZ6QWh3UvviG', 'dHkAtQZK4y3U1wHbDlIgJhvN', 'dJJ6MxpqvTKF3UOrk8XKVyfq', 'DsYVJFAvtDKJ3WQSZWqo24YK', 'e4gk_Ow1CpctYq_LuWfkdSpe', 'etHDqtrXmGYCCABvSHrExVsx', 'fdgYSze7Lc5O6hrh6Uy15LrU', 'G0HyAJ15bGh4RCSrdCOqPRkl', 'GFPbssrvkLwbOaRc7gIS8Raz', 'gmHi393OgJnr-hFIPaw5TJ8i', 'GvUa1hx8ytCrUhk9vMER6Ebf', 'H59exPp9UeP37zOW9IwDITZU', 'Hdw8YFfH5shzK02IbMfDpZK3', 'HhAmaucZUi4p5rUk4H8bIgE1', 'Hkza6GwTwzThGa1NgH1StUP_', 'iLcZc_MXhRrpMiQcU23hJhcd', 'im7cVmap78Ln6uxdWvceozUI', 'jfG-mkffVUDUM-Wgus1foQpM', 'jfyk6kN1gePe7-It99ZfYP2m', 'jhjDnaSw0dFIbmyzYgDe3WFI', 'kQklmnpU17rWl3N00-4gTZob', 'l4dN9EU1dP84d2DSUpFsQfH8', 'llqNTUC-epiDWTVD9McCZ-OA', 'mLr_YfOj0VASVcjXhByUfbrh', 'mn_SenJdAOS5BtGwU6cS3xc0', 'mOv2xsCtrdx3yWkcvTcbULE-', 'N_2xdt59GkNopkPxVq83CYDM', 'N8P4YILDLoxGxYvH4YfGcQUU', 'njrDDA1uz0YS8WUan-f50Qlj', 'NLPrUw0ZJs5-8Uj9T-ox1i3u', 'nLwLkDma9f0bg1yPokdnkEOp', 'NmSkRApWsempUhvDLvW9p8wQ', 'qO9nWQIDKWn7xKgs4eBjavEX', 'qthcQKvlZZ6WvAlK2-zIOwXF', 'qZ0Zqg1hg0eTdn8I8olNrf4d', 'SnmHtI1HM9uWV5m884Bfkelr', 'SZFZWukTmr3Iy7WA2cIuD0a0', 'T_7dq2Y45ndiJ2gIW60RpRV4', 't1N-PXrtzutnPCY7e-Ho4luL', 'TaAEig2YWmdTYz18eqZEzBr9', 'tdvMyRPgSTgp8C2VlLRj7HbR', 'UhzANzIB5MB3X-pIM3jrQIKa', 'UZypiU9AG_o9fIrZSKt0H0Sa', 'v9rvtx53k70GpEGT6r6NaKxa', 'w_7f4mxd1dEo9JrFg5bW_xm_', 'Wad6wpx_1CSitQWJFPz8q80d', 'WPIJQeRvLFB87Zh-h1w_Veae', 'wrzL5aXoiouavpIAtjy0_xO9', 'wswVHIenzD5ql_3fF-lFpShr', 'YjTO073poMWKsM_uX-OIvVof', 'YW35kYSYnxBxUK-REWxpgGyF', 'zkqlhGLudu07lfGAFsqUBOzJ']
NON_SHARING_LAST_CREATEDON = '2023-04-27T13:41:31.586Z'


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
    print(len(participants), 'participants in studies')
    
    # Fetch participant details for new users to determine sharing scope
    newParticipants = participants[pd.to_datetime(participants.createdOn)>pd.to_datetime(NON_SHARING_LAST_CREATEDON)]
    if len(newParticipants)==0:
        ids = set(NON_SHARING_USERS_IDS)
    else:
        meta = [bridge.getParticipantMetaData(row['id'])
            for i, row in tqdm(newParticipants.iterrows(), desc='Sharing Scope', total=len(newParticipants))]
        new_ids = [row['id'] for row in meta if row['sharingScope']=='no_sharing']
        ids = set(NON_SHARING_USERS_IDS).union(set(new_ids))
        print('New non sharing users found:')
        print(new_ids)
        print('Last created User', participants.createdOn.max())
    
    #Filter out participants who have no_sharing set for sharing scope.
    participants = participants[~participants['id'].isin(ids)]
    print(len(participants), 'participants with sharing')
    #Get adherence records for each individual then merge together with relevant participant fields
    dfs = [bridge.getAdherence(userId=row['id'], studyId=row['studyIds'][0]) for i, row in tqdm(participants.iterrows(), desc='Adherence', total=len(participants))]
    adherence = (pd.concat(dfs)
                 .drop('type', axis=1)
                 .merge(participants[['id', 'studyIds', 'externalId']], left_on='userId', right_on='id')
                 .drop('id', axis=1)
                 .drop('uploadedOn', axis=1)   #Creates conflict with Synapse records - should update.
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

def extractClientInfo(labels):
    """Extracts information in clientInfo column by fixing different formats of the data and returning a dataframe

    values are either strings representing json, NaN or a string of format
    'ClientInfo [appName=Mobile Toolbox, appVersion=14, ... sdkVersion=1]'
    """
    def loadjson(item):
        if pd.isnull(item): return item
        try:
            return json.loads(item)
        except json.JSONDecodeError: #If data is not json but starts with "ClientInfo [..."
            return json.loads(item.replace('ClientInfo [', '{"').replace(']', '"}').replace('=', '":"').replace(', ', '", "'))

    return pd.json_normalize(labels.apply(loadjson)).drop('type', axis=1) 

def visualizeAdherence(allData, studies):
    import seaborn as sns
    import upsetplot
    import pylab as plt

    #Remove fnamea as it isn't scored
    allData = allData.query("assessmentId!='fnamea'")
    # If adherence record is missing set startedOn to uploadedOn
    allData = allData.assign(startedOn = np.where(allData.startedOn.isnull(), allData.uploadedOn, allData.startedOn))
    allData = allData.set_index('startedOn')
    #Generate heatmap
    df = allData[['wasCompleted', 'inSynapseParent', 'inStudyProject', 'inParquet', 'inScores']]
    ax = sns.heatmap(df, cbar=False)
    ticklabels = ['NaT' if pd.isnull(df.index[int(tick)]) else df.index[int(tick)].strftime('%Y-%m-%d') for tick in ax.get_yticks()]
    ax.set_yticklabels(ticklabels);

    #Generate Upset Plot
    df_up = df.astype('bool').groupby(list(df.columns)).size()
    upsetplot.plot(df_up, orientation='horizontal')

    #Per study heatmaps
    for i, df in allData.groupby('studyId'):
        df1 = df[['wasCompleted', 'inSynapseParent', 'inStudyProject', 'inParquet', 'inScores']]
        plt.figure()
        ax = sns.heatmap(df1, cbar=False)
        ticklabels = ['NaT' if pd.isnull(df1.index[int(tick)]) else df1.index[int(tick)].strftime('%Y-%m-%d') for tick in ax.get_yticks()]
        ax.set_yticklabels(ticklabels);
        label = (studies.query("studyId=='%s'"%i)['name']+'('+
                 studies.query("studyId=='%s'"%i)['id'] + ')').iloc[0]
        plt.title(label)
        print(label)


    #Plot percent completed across different parameters
    df = allData
    ax = plt.subplot(2,2,1)
    (df.query('wasCompleted==1').groupby(pd.Grouper(freq='W'))[['inSynapseParent', 'inStudyProject',
                                                                'inParquet', 'inScores']].mean()*100).plot(ax=ax)
    plt.ylabel('completed [%]')
    for i, label in enumerate(['assessmentId','studyId','osName']):
        ax = plt.subplot(2,2,i+2)
        (df.query('wasCompleted==1').groupby(label)[['inSynapseParent', 'inStudyProject',
                                                     'inParquet', 'inScores']].mean()*100).plot(ax=ax, legend=False)
        plt.ylabel('Available [%]')
        plt.ylim(0,100)

    #Plot time changes
    allData['completion time [s]'] = (allData.finishedOn-allData.index).map(lambda x:x.seconds)
    allData['export delay [s]'] = (allData.exportedOn-allData.finishedOn).map(lambda x:x.seconds)
    allData['upload delay [s]'] = (allData.uploadedOn-allData.finishedOn).map(lambda x:x.seconds)
    allData['in Synapse delay [s]'] = (allData.createdOn-allData.finishedOn).map(lambda x:x.seconds)

    plt.figure()
    ax = plt.subplot(2,1,1)
    for dts in ['completion time [s]', 'export delay [s]', 'upload delay [s]', 'in Synapse delay [s]']:
        sns.distplot(allData[dts], label=dts + ' median=%0.0fs'% allData[dts].median(), hist=False, ax=ax)
    plt.legend()
    ax = plt.subplot(2,1,2)
    df.groupby(pd.Grouper(freq='W'))[ ['completion time [s]', 'export delay [s]', 'upload delay [s]', 'in Synapse delay [s]']].median().plot(logy=True, ax=ax)
    plt.ylabel('Delay [s]')

    
if __name__ == "__main__":
    #Get information about studies in Synapse
    studies = getStudies(syn)
    studies = studies.merge(pd.DataFrame([studyEntityIds(syn, id) for id in studies.id]), left_on='id', right_on='projectId')
    #Remove construct validation 
    #studies = studies.query("studyId=='hktrrx'")
    
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
               .merge(extractClientInfo(allData.clientInfo), left_index=True, right_index=True) #Fix clientInfo
               .drop('clientInfo', axis=1)
               .sort_values(['studyId', 'startedOn'], ascending=True)
               .reset_index(drop=True)
          )
    #Reorder Columns
    allData = allData[FIRST_COLUMNS + sorted(set(allData.columns)-set(FIRST_COLUMNS))]

    #Fill in 0 for all places we don't have data
    #We don't do this for wasCompleted as we aren't sure if it was completed if adherence is missing
    boolCols = ['inScores', 'inParquet', 'inStudyProject', 'inSynapseParent']
    allData[boolCols] = allData[boolCols].fillna(value=0)
    #Change timestamps to time dates
    timestampCols = ['modifiedOn', 'createdOn', 'exportedOn', 'eventTimestamp_y']
    allData[timestampCols] = (1000000*allData[timestampCols]).astype('datetime64[ns, UTC]')
    #Change time strings to timedates
    timeStringCols =  ['uploadedOn', 'finishedOn', 'startedOn', 'eventTimestamp_x']
    allData[timeStringCols] = allData[timeStringCols].apply(pd.to_datetime)
    
    #Store data to Synapse
    allData.to_csv('completion_records.csv', index=False)
    syn.store(synapseclient.File('completion_records.csv', parent='syn26253351'),
              used=usedEntities, executed = thisCodeInSynapse('syn1774100'))



