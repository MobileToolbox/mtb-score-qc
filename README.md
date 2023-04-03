# mtb-score-qc

Update Summary Data

```python create_summary.py```

Update Dashboard
```
jupyter nbconvert Completion_dashboard.ipynb --to html --execute --no-input --TagRemovePreprocessor.remove_single_output_tags='{"remove_cell"}'

synapse add Completion_dashboard.html --parentid syn26253351 --used completion_records.csv --executed https://github.com/MobileToolbox/mtb-score-qc/blob/main/Completion_dashboard.ipynb
```
