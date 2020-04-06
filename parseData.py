import pandas as pd
import os
import re


# i can make count map more efficient since it should be the same and is a repeated calculation
def process(df, category_map):
    new_df = pd.DataFrame(columns=['time', 'major', 'category', 'num_asleep', 'year', 'count'])
    counter = 0
    i = 0
    delay_count = 0
    columns = list(df.columns.values)
    print(columns)
    for index, row in df.iterrows():
        # the goal is to have a new column with Time, Major, value
        # major map contains major, count for a specific date
        major_map = {}
        # for granularity
        # this map is going to be behind by 1
        count_map = {}
        # columns map contains raw data of tuples of major, yes or no
        print(row)
        date = row[0]

        print(len(row))
        for j in range(1, len(row)):
            raw_data = columns[j].split(' (')
            major = raw_data[0]
            major = major.split('.')[0]

            year = raw_data[1].split(')')[0] if len(raw_data) > 1 else 'N/A'
            # gets count for each major
            value = df.iloc[i, j]
            key = (major, year)
            if value == 'YES':
                if key in major_map:
                    major_map[key] = major_map[key] + 1
                else:
                    major_map[key] = 1
                if key in count_map:
                    count_map[key] = count_map[key] + 1
                else:
                    count_map[key] = 1
            else:
                if key in count_map:
                    count_map[key] = count_map[key] + 1
                else:
                    count_map[key] = 1

        # outputs the new rows into our new data set     
        for key in major_map:
            # print(key)
            value = major_map[key]
            name = key[0] if len(key) > 1 is not None else 'N/A'
            year = key[1] if len(key) > 1 else 'N/A'
            count = count_map[key]
            category = category_map[name] if name in category_map else 'NULL'
            new_list = [date, name, category, value, year, count]
            new_df.loc[counter] = new_list
            counter = counter + 1
        i = i + 1
    return new_df


def processGranular(df, category_map):
    new_df = pd.DataFrame(columns=['time', 'major', 'category', 'num_asleep', 'year', 'count'])
    counter = 0
    i = 0
    delay_count = 0
    major_map_delayed = {}
    date_delayed = ''
    count_map = {}
    columns = list(df.columns.values)
    for j in range(1, len(columns)):
        raw_data = columns[j].split(' (')
        major = raw_data[0]
        year = raw_data[1].split(')')[0] if len(raw_data) > 1 else 'N/A'
        # gets count for each major
        key = (major, year)
        if key in count_map:
            count_map[key] = count_map[key] + 1
        else:
            count_map[key] = 1
    for index, row in df.iterrows():
        # the goal is to have a new column with Time, Major, value
        # major map contains major, count for a specific date
        major_map = {}
        # for granularity
        # this map is going to be behind by 1
        # columns map contains raw data of tuples of major, yes or no
        print(row)
        date = row[0]

        print(len(row))
        for j in range(1, len(row)):
            raw_data = columns[j].split(' (')
            major = raw_data[0]
            year = raw_data[1].split(')')[0] if len(raw_data) > 1 else 'N/A'
            # gets count for each major
            value = df.iloc[i, j]
            key = (major, year)
            if value == 'YES':
                if key in major_map:
                    major_map[key] = major_map[key] + 1
                else:
                    major_map[key] = 1
        # if the first index skip, else evaluate
        if delay_count != 0:
            for j in range(0, 3):
                for key in major_map_delayed:
                    # dates = []
                    # date_prefix = date_delayed.split(":")[0]
                    # dates.append(date_prefix + ":15")
                    # dates.append(date_prefix + ":30")
                    # dates.append(date_prefix + ":45")
                    date_prefix = date_delayed.split(":")[0]
                    new_date = date_prefix + ":" + str(15 * (j + 1))

                    diff = 0
                    try:
                        diff = - major_map_delayed[key] + major_map[key]
                    except:
                        print("error with" + str(key))

                    original_value = major_map_delayed[key]
                    # values = []
                    # values.append(original_value + diff/float(4))
                    # values.append(original_value + diff/float(2))
                    # values.append(original_value + 3 * diff/float(4))
                    new_value = original_value + (j + 1) * diff / float(4)

                    # value = values[j]
                    name = key[0] if len(key) > 1 is not None else 'N/A'
                    year = key[1] if len(key) > 1 else 'N/A'
                    count = count_map[key]
                    # calculated_date = dates[j]
                    calculated_date = new_date
                    category = category_map[name] if name in category_map else 'NULL'
                    new_list = [calculated_date, name, category, new_value, year, count]
                    new_df.loc[counter] = new_list
                    counter = counter + 1

        # outputs the new rows into our new data set
        for key in major_map:
            # print(key)
            value = major_map[key]
            name = key[0] if len(key) > 1 is not None else 'N/A'
            year = key[1] if len(key) > 1 else 'N/A'
            count = count_map[key]
            category = category_map[name] if name in category_map else 'NULL'
            new_list = [date, name, category, value, year, count]
            new_df.loc[counter] = new_list
            counter = counter + 1
        major_map_delayed = major_map
        delay_count = delay_count + 1
        date_delayed = date
        i = i + 1
    return new_df


def preprocess(df):
    new_df = pd.DataFrame()
    # list_dates = df[0].values.tolist()
    # new_df.insert(0, 'Date', list_dates, True)
    for col in df:
        raw_data = col.split(' (')
        raw_majors = raw_data[0].strip()
        raw_majors = raw_majors.split('.')[0]
        raw_years = raw_data[1].split(')')[0] if len(raw_data) > 1 else 'N/A'
        raw_years = raw_years.split('.')[0]
        # right now we're not using years
        majors = raw_majors.split(',')
        # print(majors)
        # for every major in this column lets make a new one
        for i in range(0, len(majors)):
            new_col = majors[i].strip()
            if new_col is not '':
                list_val = df[col].values.tolist()
                new_df.insert(0, new_col + " (" + raw_years + ")", list_val, True)
    list_dates = df.iloc[:, 0].tolist()
    new_df.insert(0, 'date', list_dates, True)
    return new_df


rel_path = 'raw_data/raw_data_v2.csv'
script_dir = os.path.dirname(__file__)
abs_file_path = os.path.join(script_dir, rel_path)
df = pd.read_csv(abs_file_path)
category_map = {
    'Computer Science': 'ENG',
    'Political Science': 'CAS',
    'Nursing': 'NURS',
    'BBB': 'CAS',
    'Urban Studies': 'CAS',
    'Finance': 'Wharton',
    'Communication': 'CAS',
    'DMD': 'ENG',
    'Chemistry': 'CAS',
    'Statistics': 'Wharton',
    'OIDD': 'Wharton',
    'Misc': 'N/A',
    'Music': 'CAS',
    'Biology': 'CAS',
    'PPE': 'CAS',
    'GSWS': 'CAS',
    'Mathematical Economics': 'CAS',
    'Economics': 'CAS',
    'Business Analytics': 'Wharton',
    'Marketing': 'Wharton',
    'Bioengineering': 'ENG',
    'CBE': 'ENG',
    'Philosophy': 'CAS',
    'Nutrition': 'CAS',
    'English': 'CAS',
    'Communications': 'CAS',
    'History': 'CAS'

}
# print(df)
df = preprocess(df)
print(df)
df = processGranular(df, category_map)
output_path = os.path.join(script_dir, 'clean_data/clean_sleep_granular_faster_v1.csv')
df.to_csv(output_path, index=False)
print("done")
