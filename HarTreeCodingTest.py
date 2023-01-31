import sys
import math
import pandas as pd

#import datasets from csv
dataset1 = pd.read_csv('.\dataset1.csv')
dataset2 = pd.read_csv('.\dataset2.csv')

#function to reorder and drop columns
def reorder_columns(columns, first_cols=[], last_cols=[], drop_cols=[]):
    columns = list(set(columns) - set(first_cols))
    columns = list(set(columns) - set(drop_cols))
    columns = list(set(columns) - set(last_cols))
    new_order = first_cols + columns + last_cols
    return new_order

#JOIN DATASET AND GET TIER IN LAST COLUMN
result = pd.merge(dataset1, dataset2, how="inner", on="counter_party")

#save tier merge data to csv
result.to_csv('joined_sets_with_tier.csv', index=False)

#returns as a series the columsn with calculations
def agg_results(x):
    names = {
        'Max (rating by CounterParty)': x['rating'].max(),
        'Sum (value where status=ARAP)': x[x['status']=='ARAP']['value'].sum(), #checks on status column and based on result adds values column
        'Sum (value where status=ACCR)': x[x['status']=='ACCR']['value'].sum()
    }
    return pd.Series(names)

#will be used to capture smaller groupby dataframes to later combine into final result
df_agg = list()

#create df with calculations for legal_entity
legal_entity_results = result.groupby('legal_entity').apply(agg_results)
legal_entity_results = legal_entity_results.reset_index() #resets index, needed to concat
df_agg.append(legal_entity_results)

#create df with calculations for counter_party
counterparty_results = result.groupby('counter_party').apply(agg_results)
counterparty_results = counterparty_results.reset_index() #resets index, needed to concat
df_agg.append(counterparty_results)

#create df with calculations for legal_entity and counter_party
legal_entity_counterparty_results = result.groupby(['legal_entity', 'counter_party']).apply(agg_results)
legal_entity_counterparty_results = legal_entity_counterparty_results.reset_index() #resets index, needed to concat
df_agg.append(legal_entity_counterparty_results)

#create df with calculations for tier
tier_results = result.groupby(['tier']).apply(agg_results)
tier_results = tier_results.reset_index()
df_agg.append(tier_results)

#concat all dfs by rows and ignore index
final_results = pd.concat(df_agg, axis='rows', ignore_index=True)

#reorder columns
final_results_cols = reorder_columns(final_results, ['legal_entity', 'counter_party'], ['tier','Max (rating by CounterParty)', 'Sum (value where status=ARAP)', 'Sum (value where status=ACCR)'], [])

#fill NA values
final_results = final_results[final_results_cols].fillna('Total')

#save final result to csv
final_results.to_csv('final_results.csv', index=False)

