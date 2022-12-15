import pandas as pd
import numpy as np
import os.path

CURRENT_YEAR = 2022
NUM_PAST_YEARS = 1
INPUT_CSV = './input.csv'
TARRANT_COUNTY_FOLDER = './tarrant_county/'
OUTPUT_FOLDER = './output/'


# strips every column that contains strings in the dataframe
# df: Pandas DataFrame to strip
# returns: stripped dataframe
def trim_dataset(df):
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)


# load deliminated file from the county's website
def load_county_year(path_to_csv, separator='|', endoding='cp1252'):
    df = pd.read_csv(path_to_csv, sep=separator, encoding=endoding, low_memory=False)
    df = trim_dataset(df)
    return df


# returns array of size past_years, containing dataframes of those yeas
def init_county(current_year, past_years, attributes, path, file_extension='.txt', saved_file_extention='.csv'):
    output = []
    for year in range(past_years):
        print("Loading year " + str(current_year - year))
        current_path = path + str(current_year - year) + file_extension
        saved_path = path + 'saved/' + str(current_year - year) + saved_file_extention
        current_county_year = []

        # if there is a saved file, load the saved file
        if os.path.exists(saved_path):
            current_county_year = load_county_year(saved_path)

            # if the saved file does not have the attributes needed, then load the raw file
            if current_county_year.columns.to_list()[1:] == attributes:
                output.append(current_county_year)

        else:
            # otherwise load the raw file
            current_county_year = load_county_year(current_path)[attributes]
            current_county_year.to_csv(saved_path, sep='|')  # save file for later use
            output.append(current_county_year)

    return output


# create and clean working sheet
# returns: working sheet, sites, attributes
def init_working_sheet(path_to):
    working_sheet = pd.read_csv(path_to)
    working_sheet = working_sheet.apply(lambda s: s.astype(str).str.upper())  # make everything uppercase
    attributes = working_sheet.columns.to_list()
    sites = np.array(working_sheet[attributes[0]])
    working_sheet = working_sheet.set_index(sites)  # makes the index for the sheep searchable by attribute specified
    return working_sheet, sites, attributes


# Searches dataframe county_year, for sites with attributes,
# returns edited working_sheet
def county_search(sites, attributes, county_year):
    if attributes[0] == 'Account_Num':
        sites = [int(s) for s in sites]

    county_year = county_year.set_index(attributes[0])
    return county_year.loc[sites]


if __name__ == '__main__':
    working_sheet, sites, attributes = init_working_sheet(INPUT_CSV)
    tarrant_county = init_county(CURRENT_YEAR, NUM_PAST_YEARS, attributes, TARRANT_COUNTY_FOLDER)

    # searches every year for the attributes.
    # then output years into attributes
    for year in range(NUM_PAST_YEARS):
        print("Searching year " + str(CURRENT_YEAR - year))
        output = county_search(sites, attributes, tarrant_county[year])  # search current year in county appraisal database for sites
        output_path = OUTPUT_FOLDER + str(CURRENT_YEAR - year) + '.csv'  # output the sites
        output.to_csv(output_path, columns=attributes[1:])
