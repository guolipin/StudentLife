import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import numpy as np
import pandas as pd


def pre_post_reader(data_path, select_cols=None, mode='remove', mapping=None, to_int=False):
    # read csv file
    df = pd.read_csv(data_path,usecols=select_cols)

    col_names = df.columns
    if mapping != None:
        if isinstance(mapping,dict):
            df = category_to_numeric(df, col_names[2:], mapping)
        elif isinstance(mapping, list) and len(mapping)==len(col_names[2:]): 
            for i, col in enumerate(col_names[2:]):
                df[col] = df[col].map(mapping[i])

    if mode == 'remove':
            df.dropna(subset = col_names, inplace=True)
    elif mode == 'replace':
        df.replace(np.nan,0, inplace=True)

    # Find duplicate values in the 'uid' column
    duplicate_uids = df[df.duplicated(subset='uid', keep=False)]['uid'].unique()
    # Filter the DataFrame to keep only rows with duplicate 'uid' values
    new_df = df[df['uid'].isin(duplicate_uids)]

    if to_int == True:
        # convert float64 to int64
        new_df[col_names[2:]] = new_df.loc[:, col_names[2:]].astype('int64')
        
    # sperate pre_df and post_df
    pre_df = new_df.loc[new_df['type']=='pre']
    post_df = new_df.loc[new_df['type']=='post']

    return new_df, pre_df, post_df

def get_pre_post_difference(pre_df, post_df, cols, name):
    sum1 = pre_df.loc[:, cols].copy().sum(axis=1).tolist()
    sum2 = post_df.loc[: ,cols].copy().sum(axis=1).tolist()
    print('Check uid matching in the pre and post df:', pre_df['uid'].tolist() == post_df['uid'].tolist())
    diff_df = pd.DataFrame({f'{name}_pre':sum1, f'{name}_post':sum2}, index=pre_df['uid'])
    diff_df[f'{name}_delta'] = diff_df[f'{name}_post'] - diff_df[f'{name}_pre']
    diff_df.to_csv(f'../results/{name}.csv')
    # diff_df.sort_values(by=[f'{name}_delta'],ascending=False, inplace=True)
    return diff_df

def get_pos_neg_difference(pre_df, post_df, pos, neg,name):
    pos_sum1 = pre_df.loc[:, pos].copy().sum(axis=1).tolist()
    neg_sum1 = pre_df.loc[:, neg].copy().sum(axis=1).apply(lambda x:-x).tolist()
    pos_sum2 = post_df.loc[: ,pos].copy().sum(axis=1).tolist()
    neg_sum2 = post_df.loc[: ,neg].copy().sum(axis=1).apply(lambda x:-x).tolist()
    print('Check uid matching in the pre and post df:', pre_df['uid'].tolist() == post_df['uid'].tolist())
    diff_df = pd.DataFrame({f'{name}_pos_pre':pos_sum1, f'{name}_neg_pre':neg_sum1, f'{name}_pos_post':pos_sum2,f'{name}_neg_post':neg_sum2}, index=pre_df['uid'])
    diff_df[f'{name}_pos_delta'] = diff_df[f'{name}_pos_post'] - diff_df[f'{name}_pos_pre']
    diff_df[f'{name}_neg_delta'] = diff_df[f'{name}_neg_post'] - diff_df[f'{name}_neg_pre']
    diff_df[f'{name}_delta'] = diff_df[f'{name}_pos_delta'] + diff_df[f'{name}_neg_delta']
    diff_df.to_csv(f'../results/{name}.csv')
    return diff_df

def category_to_numeric(df, columns_to_map, mapping):
    """
    Map categorical values to numeric values in specified columns of a DataFrame.
    
    Args:
    df (pandas.DataFrame): The DataFrame containing the data.
    columns_to_map (list): List of column names to apply the mapping.
    mapping (dict): A dictionary specifying the mapping of categorical values to numeric values.
    
    Returns:
    pandas.DataFrame: The DataFrame with mapped values.
    """
    def map_values(value):
        return mapping.get(value, value)  # Use get() to handle unknown values

    # Apply the mapping function to selected columns
    df[columns_to_map] = df[columns_to_map].applymap(map_values)

    return df

def get_status(diff_df, mapping_fc, name):
    # Apply the mapping function to the 'Score' column
    diff_df[f'{name}_pre_severity'] = diff_df[f'{name}_pre'].apply(mapping_fc)
    diff_df[f'{name}_post_severity'] = diff_df[f'{name}_post'].apply(mapping_fc)
    diff_df.to_csv(f'../results/{name}.csv')
    return diff_df