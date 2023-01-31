import apache_beam as beam
from apache_beam.dataframe.io import read_csv
import itertools
import logging
from apache_beam.dataframe.convert import to_dataframe

#PIPELINE SET UP TO RUN WITHOUT OPTIONS CONFIGURATION
#options can be added later if needed

with beam.Pipeline() as pipeline:
    datasets = '.\dataset*.csv'
    dataset1 = '.\dataset1.csv'
    dataset2 = '.\dataset2.csv' 

    #tried to use dataframes but still could not merge the sets

    # testset1 = pipeline | 'read set 1' >> read_csv(dataset1)
    # testset2 = pipeline | 'read set 2' >> read_csv(dataset2)

    # agg = testset1.innerjoin(testset2)
    # agg.to_csv('out.csv')

    # set2 = pipeline | 'read set 2' >> read_csv(dataset2) | beam.Map(print)

    #import set 1 without headers produces a list of lists or pcollection in apache beam
    #skipping headers for now for both sets
    set1 = ( pipeline
    | "Read set 1" >> beam.io.ReadFromText(dataset1, skip_header_lines=0)
    | "Split set 1" >> beam.Map(lambda x:x.split(","))
    | "Filter 1" >> beam.Map(lambda x: (x[2], x))
    | "Groupby key" >> beam.GroupByKey()
    | "Print set 1" >> beam.Map(print)
    )

    #import set 2 without headers produces a list of lists or pcollection in apache beam
    set2 = ( pipeline
    | "Read set 2" >> beam.io.ReadFromText(dataset2, skip_header_lines=0)
    | "Split set 2" >> beam.Map(lambda x:x.split(","))
    | "Filter 2" >> beam.Map(lambda x: (x[0], x))
    | "Groupby key 2" >> beam.GroupByKey()
    | "Print set 2" >> beam.Map(print)
    )

    #flattened should create a single 
    flattened = (
        (set1, set2) 
        | "Flatten both sets" >> beam.GroupBy()
        | "Print flattened sets" >> beam.Map(print))



