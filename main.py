######## imports

### open lib
from pathlib import Path
import pandas as pd
import gzip
import graphviz
import re
import os

# import pythonplantuml

### self defined lib
from ingestion_lib.ingest import ingest_src
from ingestion_lib.analyse import parse_dataset, infer_column_type, add_key_column, dupe_check, find_relation

######## Class, method Fn definitions


######## Source specific configs

# dir_path = os.getcwd()
# root_path = os.path.abspath(os.path.join(dir_path, '..'))
src_location = Path(__file__).parent / 'source_files\\'
extension = '.csv'
separator = ';'
# file_name = 'report_2025-04-14_135506'
# compression = 'gzip'
# classification  = ['raw']

tbl_key_dict = {
    'categories':['type','category'],
    'records':['date','account','type','category']
}

######## MAIN

##### INGESTION

# Specify configs
ingested_data = ingest_src(
                        src_location = src_location,\
                        extension = extension,\
                        separator = separator,\
                    )

# Begin extraction
dataframe_dict = ingested_data.ingest()

# Create keys

fk_col = set(tbl_key_dict['records']).intersection(set(tbl_key_dict['categories']))
fk_col = list(fk_col)

categories_key_column = tbl_key_dict['categories']
categories_df = dataframe_dict['categories']
categories_df['category'] = categories_df.item.combine_first(categories_df.subcategory).combine_first(categories_df.payment_category)
categories_df = add_key_column(categories_df, categories_key_column, 'category_key')

records_key_column = tbl_key_dict['records']
records_df = dataframe_dict['records']
records_df = add_key_column(records_df, fk_col, 'category_key')
records_df = add_key_column(records_df, records_key_column, 'record_key')

##### Validation

# Analyse

to_parse = {'categories': categories_df, 'records':records_df}
tbl_parsed_dict = parse_dataset(to_parse).tbl_parsed_dict
categories = tbl_parsed_dict['categories']
records = tbl_parsed_dict['records']

# Unit test

for tbl, data in tbl_parsed_dict.items():
    df = data['df']

    # Check 1: Pk uniqueness
    pk_count = df[data['pk']].drop_duplicates().shape[0]
    row_count = df.shape[0]
    is_unique = pk_count == row_count

    tbl_parsed_dict[tbl]["check_pk"] = "PASS" if is_unique else "FAIL"

    # Check 2: Not nulls
    nulls = df.isnull().sum().to_dict()
    null_columns = [col for col, count in nulls.items() if count > 0]
    tbl_parsed_dict[tbl]["check_null_columns"] = ", ".join(null_columns) if null_columns else "None"


# Relationship analysis

tbl_parsed_dict['fk_checks'] = {}

# type-category pair not accounted for

ref_values = categories_df['category_key'].drop_duplicates()
missing = records_df['category_key'].drop_duplicates()

missing_join_ref = set(missing.to_list()).difference(set(ref_values.to_list()))
counts_to_remove = records_df[records_df['category_key'].isin(missing_join_ref)]['category_key'].count()
tbl_parsed_dict['fk_checks']['missing_fk'] = missing_join_ref
tbl_parsed_dict['fk_checks']['missing_fk_record_count'] = counts_to_remove.astype(int)


##### Metadataprint

for key, val in tbl_parsed_dict.items():
    print(key)
    for key2, val2 in val.items():
        if key2 != 'df':
            print('\t',key2,': ',val2)

##### Cleanup

records_df = records_df[~records_df['category_key'].isin(missing_join_ref)].dropna(axis=1, how='all')  #inplace=True
# categories_df = categories_df.drop_duplicates()

# debug
# x = dupe_check(records_df,'record_key')
# x = fk_col
# print(x)