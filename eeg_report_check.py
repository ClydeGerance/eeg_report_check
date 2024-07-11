import pandas as pd
import numpy as np
import os
import re
import sys

# Function to extract day number from filename
def get_day_number(filename):
    if 'Day 1' in filename:
        return 1
    elif 'Day 2' in filename:
        return 2
    elif 'Day 3' in filename:
        return 3
    else:
        raise ValueError("Day number not found in filename.")

# Function to process person data
def process_person_data(root_dir_person, day_pattern, metric_name):
    all_data_person = []
    for filename_person in os.listdir(root_dir_person):
        if day_pattern in filename_person:
            columns_to_keep = ["Timestamp", f"PM.{metric_name}.Min", f"PM.{metric_name}.Max"]
            data_person = pd.read_csv(os.path.join(root_dir_person, filename_person), header=1)
            data_person = data_person[columns_to_keep]
            data_person = data_person.dropna()
            data_person["Timestamp"] = data_person["Timestamp"].astype(float)
            data_person["Timestamp"] = data_person["Timestamp"].apply(lambda x: f"{x:.0f}")
            all_data_person.append(data_person)
    combined_data_person = pd.concat(all_data_person, ignore_index=True)
    return combined_data_person

# Function to create array from DataFrame
def create_array_from_df(df, metric_name):
    array = []
    for index, row in df.iterrows():
        array.append([row['Timestamp'], row[f"PM.{metric_name}.Min"]])
        array.append([row['Timestamp'], row[f"PM.{metric_name}.Max"]])
    array = np.array(array)
    df_array = pd.DataFrame(array, columns=['Timestamp', 'Value'])
    df_array = df_array.drop_duplicates()
    df_array = df_array.sort_values(by='Value')
    df_array = df_array.sort_values(by='Timestamp')
    return df_array.values

# Function to process metric data
def process_metric_data(root_dir_metric, filename_metric, column_prefixes):
    data_metric = pd.read_csv(f"{root_dir_metric}/{filename_metric}")
    transposed_data_metric = data_metric.transpose()
    new_header = transposed_data_metric.iloc[1]
    transposed_data_metric = transposed_data_metric[2:]
    transposed_data_metric.columns = new_header
    transposed_data_metric.reset_index(drop=True, inplace=True)
    transposed_data_metric = transposed_data_metric.drop(transposed_data_metric.index[:19])
    transposed_data_metric.columns = ["Online/Live", "Min/Max", "Timestamp"] + list(transposed_data_metric.columns[3:])
    
    if "Day 1" in filename_metric:
        transposed_data_metric["Timestamp"] = pd.to_datetime("June 5, 2024 ") + pd.to_timedelta(transposed_data_metric["Timestamp"])
    elif "Day 2" in filename_metric:
        transposed_data_metric["Timestamp"] = pd.to_datetime("June 6, 2024 ") + pd.to_timedelta(transposed_data_metric["Timestamp"])
    elif "Day 3" in filename_metric:
        transposed_data_metric["Timestamp"] = pd.to_datetime("June 7, 2024") + pd.to_timedelta(transposed_data_metric["Timestamp"])

    transposed_data_metric["Timestamp"] = (pd.to_datetime(transposed_data_metric["Timestamp"]).astype('int64') // 10**9) - 28800
    transposed_data_metric["Timestamp"] = transposed_data_metric["Timestamp"].astype(float)
    transposed_data_metric["Timestamp"] = transposed_data_metric["Timestamp"].apply(lambda x: f"{x:.0f}")
    
    array_metric = []
    for index, row in transposed_data_metric.iterrows():
        for prefix in column_prefixes:
            array_metric.append([row['Timestamp'], row[prefix]])
    array_metric = np.array(array_metric)
    df_array_metric = pd.DataFrame(array_metric, columns=['Timestamp', 'Value'])
    df_array_metric['Value'] = pd.to_numeric(df_array_metric['Value'], errors='coerce')
    df_array_metric = df_array_metric.dropna(subset=['Value'])
    df_array_metric = df_array_metric.sort_values(by='Value')
    df_array_metric = df_array_metric.sort_values(by='Timestamp')
    return df_array_metric.values

# Function to find non-matching indices
def find_non_matching_indices(array_person, array_metric):
    person_matched = True
    metric_matched = True

    for person_item in array_person:
        person_string = person_item[0]
        person_value = round(float(person_item[1]), 1)
        found_match = False
        
        for metric_item in array_metric:
            metric_string = metric_item[0]
            metric_value = round(metric_item[1], 1)
            
            if person_string == metric_string and person_value == metric_value:
                found_match = True
                break
        
        if not found_match:
            person_matched = False
            print(f"No matching value found for person item '{person_item}'.")

    for metric_item in array_metric:
        metric_string = metric_item[0]
        metric_value = round(metric_item[1], 1)
        found_match = False
        
        for person_item in array_person:
            person_string = person_item[0]
            person_value = round(float(person_item[1]), 1)
            
            if metric_string == person_string and metric_value == person_value:
                found_match = True
                break
        
        if not found_match:
            metric_matched = False
            print(f"No matching value found for metric item '{metric_item}'.")

    if person_matched and metric_matched:
        print("Everything is matched.")

def main():
    # Get input for filename_metric
    filename_metric = input("Enter the filename_metric: ")

    # Define root directories
    root_dir_person = "data_person"
    root_dir_metric = "data_metric"
    root_dir_save = "data_text"

    # Create output directory if it doesn't exist
    if not os.path.exists(root_dir_save):
        os.makedirs(root_dir_save)

    # Redirect stdout to a file
    output_file = os.path.join(root_dir_save, f"{filename_metric}.txt")
    sys.stdout = open(output_file, 'w')

    try:
        # Extract day number from filename_metric
        day_number = get_day_number(filename_metric)

        # Define the regex pattern to match the day number in filename_person
        day_pattern = f'D{day_number}'

        # Extract metric name from filename_metric
        metric_name_match = re.search(r'Day \d+ - (\w+)', filename_metric)
        if metric_name_match:
            metric_name = metric_name_match.group(1)
        else:
            raise ValueError("Metric name not found in the filename")

        # Process person data
        combined_data_person = process_person_data(root_dir_person, day_pattern, metric_name)

        # Create array_person from combined_data_person
        array_person = create_array_from_df(combined_data_person, metric_name)

        # Define column prefixes based on day number
        column_prefixes = [f'{i}-AMPerson{i}D{day_number}' for i in range(1, 11)] + \
                          [f'{i}-PMPerson{i}D{day_number}' for i in range(1, 11)]

        # Process metric data
        array_metric = process_metric_data(root_dir_metric, filename_metric, column_prefixes)

        # Find non-matching indices
        find_non_matching_indices(array_person, array_metric)

    finally:
        # Close the file and reset stdout
        sys.stdout.close()
        sys.stdout = sys.__stdout__

if __name__ == "__main__":
    main()
