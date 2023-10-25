import os
import numpy as np
import pandas as pd

def data_reader(dir_path, col_names=None):
    """
    Reading data with different formats: csv, txt, json.
    """
    uid_names = []
    uid_dfs = []
    for file in os.listdir(dir_path):
        # get the file name
        name = os.path.splitext(file)[0]
        # get the file path
        file_path = os.path.join(dir_path, file)

        # identify different formats
        if file.endswith(".csv"):
            df = pd.read_csv(file_path, usecols=col_names)
        elif file.endswith(".txt"):
            df = pd.read_csv(file_path, delimiter=',',header=None, names=col_names)
        elif file.endswith('.json'):
            with open(file_path, 'r') as tmp:
                if tmp.read(2) == '[]':
                    continue
                else:
                    df = pd.read_json(file_path)

        # handle column names case-insenstive
        df.columns = df.columns.str.lower()

        # standardize different names of timestamp columns
        df = standardize_names(df)

        # deal with different formats of timestamp
        columns = df.columns.tolist()
        df = handle_timestamp(df, columns)

        # deal with NaN value
        # df.fillna('', inplace=True)
        df.replace(np.nan, '',inplace=True)
        
        uid_names.append(name)
        uid_dfs.append(df)

    # sort data
    sorted_uid_names, sorted_uid_dfs = sort_data(uid_names, uid_dfs)
    return sorted_uid_names, sorted_uid_dfs
    
def custom_sort_key(filename):
    # Define a custom sorting key function
    parts = filename.split(".")
    if len(parts) == 2:
        name, ext = parts
        if name.startswith("u"):
            numeric_part = name[1:]
            try:
                return int(numeric_part)
            except ValueError:
                pass
    return filename

def sort_data(uid_names, uid_dfs):
    """sort dataframe according to the order of uid"""   
    # Sort the filenames and the corresponding DataFrames using the custom sorting key
    sorted_uid_names, sorted_uid_dfs = zip(*sorted(zip(uid_names, uid_dfs), key=lambda x: custom_sort_key(x[0])))
    return sorted_uid_names,sorted_uid_dfs

def get_info(df):
    # check the index datatype:
    if pd.api.types.is_datetime64_any_dtype(df.index):
        print(f"Time period: {df.index.max()-df.index.min()}")
    # else:
    #     print(f"Time period: {df['start'].min()} to {df['start'].max()}, total is {df['start'].max()-df['start'].min()}")
    print("-"*40)
    df.info()

def standardize_names(df):
    # standardize the names of different timestamp columns to match.
    if 'time' in df.columns:
        df.rename(columns={"time": "timestamp"}, inplace=True)
    if "start_timestamp" and " end_timestamp" in df.columns:
        df.rename(columns={"start_timestamp": "start", " end_timestamp": "end"}, inplace=True)
    if 'resp_time' in df.columns:
        df.rename(columns={"resp_time": "timestamp"}, inplace=True)
    return df

def convert_timestamp(df, col_name):
    try:
        df[col_name] = pd.to_datetime(df[col_name], unit='s')
    except:    
        # convert timestamp to %y-%m-%d %H:%M:%S format
        df[col_name] = pd.to_datetime(df[col_name], format='%Y-%m-%d %H:%M:%S')
    return df[col_name]

def handle_timestamp(df,columns):
    # check timestamp column
    if 'timestamp' in columns:
        df['timestamp'] = convert_timestamp(df, 'timestamp')
        # sort timestamp
        df.sort_values(by='timestamp', inplace=True)
        # get day of week column
        df['day_of_week'] = df['timestamp'].dt.day_name()
        # set timestamp as index
        df = df.set_index('timestamp')
    # handle start - end timestamp
    if "start" and "end" in columns:
        df['start'] = convert_timestamp(df, 'start')
        df['end'] = convert_timestamp(df, 'end')
        # sort timestamp
        df.sort_values(by='start', inplace=True)
        # add new column
        df['period'] = df['end']-df['start']
        # set timestamp as index
        df.set_index('start',inplace=True)
    return df

# def map_categorical_column_to_numbers(df, column_name):
#     # Get unique values from the specified column
#     unique_values = df[column_name].unique()
    
#     # Create a mapping from unique values to numbers
#     value_to_number = {value: number for number, value in enumerate(unique_values)}
    
#     # Create a new column by mapping the specified column to numbers
#     new_column_name = f'{column_name}_num'
#     df[new_column_name] = df[column_name].map(value_to_number)
    
#     return df