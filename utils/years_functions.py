"""
This module contains functions to work years datasets 
"""

import os
import pandas as pd
import logging as log

log.basicConfig(level=log.INFO)


def load_datasets(path : str) -> dict:
    """
    Load all datasets from the data folder

    Parameters:
    -----------
    None

    Returns:
    --------
    datasets (dict): A dictionary with the year as key and the dataset as value
    """

    datasets = {}
    
    for filename in os.listdir(path):
        if filename.endswith('.csv'):  

            year = int(filename.split('.')[0][-4:])
            
            full_path = os.path.join(path, filename)
            df = pd.read_csv(full_path)
            datasets[year] = df

    log.info(f"Loaded {len(datasets)} datasets")
    return datasets


def length_dataset(df : pd.DataFrame) -> None:
    """
    Print the length and the head of each dataset

    Parameters:
    -----------
    None

    Returns:
    --------
    None
    """

    print(f"Number of Row : {df.shape[0]}\nNumber of Columns : {df.shape[1]}")
    print('Structure of the DataFrame'.center(75, '-'))


def summary_dataset(df : pd.DataFrame) -> pd.DataFrame:
    """
    Print the summary of each dataset

    Parameters:
    -----------
    df (pd.DataFrame): The dataset to be summarized

    Returns:
    --------
    summary (pd.DataFrame): A DataFrame with the summary of the dataset
    """

    summary = pd.DataFrame({
        'dtypes': df.dtypes,
        'null count': df.isnull().sum(),
        'unique values': df.nunique(),
        'duplicate count': df.duplicated().sum(),
    })

    return summary


def compare_rows_columns(df_dict) -> None:
    """
    Compare the number of rows and columns of DataFrames in the given dictionary.

    Parameters:
    -----------
    df_dict (dict): A dictionary with the year as key and the DataFrame as value.

    Returns:
    --------
    None (prints the comparison)
    """
    for year, df in df_dict.items():
        print(f"Dataset for {year}:")
        print(f"Number of rows: {df.shape[0]}")
        print(f"Number of columns: {df.shape[1]}")
        print()


def compare_column_names(df_dict) -> None:
    """
    Compare the column names of DataFrames in the given dictionary.

    Parameters:
    -----------
    df_dict (dict): A dictionary with the year as key and the DataFrame as value.

    Returns:
    --------
    None (prints the comparison)
    """
    all_columns = set()
    for df in df_dict.values():
        all_columns.update(df.columns)

    max_column_length = max(len(col) for col in all_columns)
    
    print(f"{'Column Names':<{max_column_length}}", end="\t")
    for year in df_dict.keys():
        print(year, end="\t")
    print()

    for col in sorted(all_columns):
        print(f"{col:<{max_column_length}}", end="\t")
        for year in df_dict.keys():
            if col in df_dict[year].columns:
                print("X", end="\t")
            else:
                print("", end="\t")
        print()


def normalize_column_names(df_dict) -> dict:
    """
    Normalize column names in DataFrames.

    Parameters:
    -----------
    df_dict (dict): A dictionary with the year as key and the DataFrame as value.

    Returns:
    --------
    dict: A dictionary with the year as key and the DataFrame with normalized column names as value.
    """
    column_mapping = {
        'Country': 'Country',
        'Country or region': 'Country',
        'Dystopia Residual': 'Dystopia_Residual',
        'Dystopia.Residual': 'Dystopia_Residual',
        'Economy (GDP per Capita)': 'Economy',
        'Economy..GDP.per.Capita.': 'Economy',
        'Family': 'Social_Support',
        'Freedom': 'Freedom',
        'Freedom to make life choices': 'Freedom',
        'GDP per capita': 'Economy',
        'Generosity': 'Generosity',
        'Happiness Rank': 'Happiness_Rank',
        'Happiness Score': 'Happiness_Score',
        'Happiness.Rank': 'Happiness_Rank',
        'Happiness.Score': 'Happiness_Score',
        'Health (Life Expectancy)': 'Health',
        'Health..Life.Expectancy.': 'Health',
        'Healthy life expectancy': 'Health',
        'Lower Confidence Interval': 'Lower_Confidence_Interval',
        'Overall rank': 'Happiness_Rank',
        'Perceptions of corruption': 'Trust',
        'Trust (Government Corruption)': 'Trust',
        'Trust..Government.Corruption.': 'Trust',
        'Region': 'Region',
        'Score': 'Happiness_Score',
        'Social support': 'Social_Support',
        'Standard Error': 'Standard_Error',
        'Upper Confidence Interval': 'Upper_Confidence_Interval',
        'Whisker.high': 'Whisker_High',
        'Whisker.low': 'Whisker_Low'
    }

    normalized_datasets = {}
    for year, df in df_dict.items():
        df = df.rename(columns=column_mapping)
        normalized_datasets[year] = df

    log.info("Column names normalized")
    return normalized_datasets


def add_year_column(df_dict) -> dict:
    """
    Add a 'Year' column to each DataFrame in the dictionary.

    Parameters:
    -----------
    df_dict (dict): A dictionary with the year as key and the DataFrame as value.

    Returns:
    --------
    dict: A dictionary with the 'Year' column added to each DataFrame.
    """
    for year, df in df_dict.items():
        df['Year'] = year

    return df_dict


def concatenate_common_columns(df_dict) -> pd.DataFrame:
    """
    Concatenate DataFrames keeping only common columns.

    Parameters:
    -----------
    df_dict (dict): A dictionary with the year as key and the DataFrame as value.

    Returns:
    --------
    pandas.DataFrame: Concatenated DataFrame with only common columns.
    """
    common_columns = list(set.intersection(*[set(df.columns) for df in df_dict.values()]))

    dfs_common_columns = {year: df[common_columns] for year, df in df_dict.items()}

    concatenated_df = pd.concat(dfs_common_columns.values(), ignore_index=True)

    return concatenated_df


def map_country_to_continent(df : pd.DataFrame) -> pd.DataFrame:
    """
    Map countries to continents.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame to map countries to continents.

    Returns:
    --------
    pd.DataFrame
        The DataFrame with the 'Continent' column added.
    """

    country_to_continent = {
        'Switzerland': 'Europe',
        'Iceland': 'Europe',
        'Denmark': 'Europe',
        'Norway': 'Europe',
        'Canada': 'North America',
        'Finland': 'Europe',
        'Netherlands': 'Europe',
        'Sweden': 'Europe',
        'New Zealand': 'Oceania',
        'Australia': 'Oceania',
        'Israel': 'Asia',
        'Costa Rica': 'North America',
        'Austria': 'Europe',
        'Mexico': 'North America',
        'United States': 'North America',
        'Brazil': 'South America',
        'Luxembourg': 'Europe',
        'Ireland': 'Europe',
        'Belgium': 'Europe',
        'United Arab Emirates': 'Asia',
        'United Kingdom': 'Europe',
        'Oman': 'Asia',
        'Venezuela': 'South America',
        'Singapore': 'Asia',
        'Panama': 'North America',
        'Germany': 'Europe',
        'Chile': 'South America',
        'Qatar': 'Asia',
        'France': 'Europe',
        'Argentina': 'South America',
        'Czech Republic': 'Europe',
        'Uruguay': 'South America',
        'Colombia': 'South America',
        'Thailand': 'Asia',
        'Saudi Arabia': 'Asia',
        'Spain': 'Europe',
        'Malta': 'Europe',
        'Taiwan': 'Asia',
        'Kuwait': 'Asia',
        'Suriname': 'South America',
        'Trinidad and Tobago': 'North America',
        'El Salvador': 'North America',
        'Guatemala': 'North America',
        'Uzbekistan': 'Asia',
        'Slovakia': 'Europe',
        'Japan': 'Asia',
        'South Korea': 'Asia',
        'Ecuador': 'South America',
        'Bahrain': 'Asia',
        'Italy': 'Europe',
        'Bolivia': 'South America',
        'Moldova': 'Europe',
        'Paraguay': 'South America',
        'Kazakhstan': 'Asia',
        'Slovenia': 'Europe',
        'Lithuania': 'Europe',
        'Nicaragua': 'North America',
        'Peru': 'South America',
        'Belarus': 'Europe',
        'Poland': 'Europe',
        'Malaysia': 'Asia',
        'Croatia': 'Europe',
        'Libya': 'Africa',
        'Russia': 'Europe',
        'Jamaica': 'North America',
        'North Cyprus': 'Europe',
        'Cyprus': 'Europe',
        'Algeria': 'Africa',
        'Kosovo': 'Europe',
        'Turkmenistan': 'Asia',
        'Mauritius': 'Africa',
        'Hong Kong': 'Asia',
        'Estonia': 'Europe',
        'Indonesia': 'Asia',
        'Vietnam': 'Asia',
        'Turkey': 'Asia',
        'Kyrgyzstan': 'Asia',
        'Nigeria': 'Africa',
        'Bhutan': 'Asia',
        'Azerbaijan': 'Asia',
        'Pakistan': 'Asia',
        'Jordan': 'Asia',
        'Montenegro': 'Europe',
        'China': 'Asia',
        'Zambia': 'Africa',
        'Romania': 'Europe',
        'Serbia': 'Europe',
        'Portugal': 'Europe',
        'Latvia': 'Europe',
        'Philippines': 'Asia',
        'Somaliland region': 'Africa',
        'Morocco': 'Africa',
        'Macedonia': 'Europe',
        'Mozambique': 'Africa',
        'Albania': 'Europe',
        'Bosnia and Herzegovina': 'Europe',
        'Lesotho': 'Africa',
        'Dominican Republic': 'North America',
        'Laos': 'Asia',
        'Mongolia': 'Asia',
        'Swaziland': 'Africa',
        'Greece': 'Europe',
        'Lebanon': 'Asia',
        'Hungary': 'Europe',
        'Honduras': 'North America',
        'Tajikistan': 'Asia',
        'Tunisia': 'Africa',
        'Palestinian Territories': 'Asia',
        'Bangladesh': 'Asia',
        'Iran': 'Asia',
        'Ukraine': 'Europe',
        'Iraq': 'Asia',
        'South Africa': 'Africa',
        'Ghana': 'Africa',
        'Zimbabwe': 'Africa',
        'Liberia': 'Africa',
        'India': 'Asia',
        'Sudan': 'Africa',
        'Haiti': 'North America',
        'Congo (Kinshasa)': 'Africa',
        'Nepal': 'Asia',
        'Ethiopia': 'Africa',
        'Sierra Leone': 'Africa',
        'Mauritania': 'Africa',
        'Kenya': 'Africa',
        'Djibouti': 'Africa',
        'Armenia': 'Asia',
        'Botswana': 'Africa',
        'Myanmar': 'Asia',
        'Georgia': 'Asia',
        'Malawi': 'Africa',
        'Sri Lanka': 'Asia',
        'Cameroon': 'Africa',
        'Bulgaria': 'Europe',
        'Egypt': 'Africa',
        'Yemen': 'Asia',
        'Angola': 'Africa',
        'Mali': 'Africa',
        'Congo (Brazzaville)': 'Africa',
        'Comoros': 'Africa',
        'Uganda': 'Africa',
        'Senegal': 'Africa',
        'Gabon': 'Africa',
        'Niger': 'Africa',
        'Cambodia': 'Asia',
        'Tanzania': 'Africa',
        'Madagascar': 'Africa',
        'Central African Republic': 'Africa',
        'Chad': 'Africa',
        'Guinea': 'Africa',
        'Ivory Coast': 'Africa',
        'Burkina Faso': 'Africa',
        'Afghanistan': 'Asia',
        'Rwanda': 'Africa',
        'Benin': 'Africa',
        'Syria': 'Asia',
        'Burundi': 'Africa',
        'Togo': 'Africa',
        'Puerto Rico': 'North America',
        'Belize': 'North America',
        'Somalia': 'Africa',
        'Somaliland Region': 'Africa',
        'Namibia': 'Africa',
        'South Sudan': 'Africa',
        'Taiwan Province of China': 'Asia',
        'Hong Kong S.A.R., China': 'Asia',
        'Trinidad & Tobago': 'North America',
        'Northern Cyprus': 'Europe',
        'North Macedonia': 'Europe',
        'Gambia': 'Africa'
    }
    
    df['Continent'] = df['Country'].map(country_to_continent)
    return df





