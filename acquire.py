import pandas as pd
from env import get_connection
import os

def acquire_zillow():
    """
    Acquire Zillow data from a SQL database.

    This function connects to a SQL database named 'zillow', executes a query to retrieve specific property data
    for the year 2017, and returns the data as a DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing Zillow property data for the year 2017.

    Note:
        - This function relies on a helper function 'get_db_connection' to establish a database connection.
    """
    # Create a helper function to get the necessary database connection URL.
    def get_db_connection(database):
        return get_connection(database)

    # Connect to the SQL 'zillow' database.
    url = "zillow"

    # Use this query to get the data.
    sql_query = '''
                    SELECT pred.parcelid, pred.logerror, pred.transactiondate, ac.airconditioningdesc, arc_sty.architecturalstyledesc, 
                        bc.buildingclassdesc, hs.heatingorsystemdesc, plu.propertylandusedesc, st.storydesc, ct.typeconstructiondesc,
                        prop.basementsqft, prop.bathroomcnt, prop.bedroomcnt, prop.calculatedbathnbr, prop.finishedfloor1squarefeet, 
                        prop.calculatedfinishedsquarefeet, prop.finishedsquarefeet12, prop.finishedsquarefeet13, prop.finishedsquarefeet15, 
                        prop.finishedsquarefeet50, prop.finishedsquarefeet6, prop.fips, prop.fireplacecnt, prop.fullbathcnt, prop.garagecarcnt, 
                        prop.garagetotalsqft, prop.hashottuborspa, prop.latitude, prop.longitude, prop.lotsizesquarefeet, prop.poolcnt, prop.poolsizesum, 
                        prop.propertycountylandusecode, prop.propertyzoningdesc, prop.rawcensustractandblock, prop.roomcnt, prop.threequarterbathnbr, 
                        prop.unitcnt, prop.yardbuildingsqft17, prop.yardbuildingsqft26, prop.yearbuilt, prop.numberofstories, prop.fireplaceflag, 
                        prop.structuretaxvaluedollarcnt, prop.taxvaluedollarcnt, prop.assessmentyear, prop.landtaxvaluedollarcnt, prop.taxamount, 
                        prop.taxdelinquencyflag, prop.taxdelinquencyyear, prop.censustractandblock, prop.regionidzip, prop.buildingqualitytypeid, 
                        prop.decktypeid, prop.pooltypeid10, prop.pooltypeid2, prop.pooltypeid7, prop.regionidcity, prop.regionidcounty, prop.regionidneighborhood
                    FROM properties_2017 AS prop 
                    RIGHT JOIN (
                        SELECT MAX(transactiondate) AS max_transactiondate, parcelid
                        FROM predictions_2017
                        WHERE transactiondate LIKE '2017%%'
                        GROUP BY parcelid
                    ) AS max_dates ON prop.parcelid = max_dates.parcelid
                    LEFT JOIN predictions_2017 AS pred ON max_dates.parcelid = pred.parcelid AND max_dates.max_transactiondate = pred.transactiondate
                    LEFT JOIN airconditioningtype AS ac ON prop.airconditioningtypeid = ac.airconditioningtypeid
                    LEFT JOIN architecturalstyletype AS arc_sty ON prop.architecturalstyletypeid = arc_sty.architecturalstyletypeid
                    LEFT JOIN buildingclasstype AS bc ON prop.buildingclasstypeid = bc.buildingclasstypeid
                    LEFT JOIN heatingorsystemtype AS hs ON prop.heatingorsystemtypeid = hs.heatingorsystemtypeid
                    LEFT JOIN propertylandusetype AS plu ON prop.propertylandusetypeid = plu.propertylandusetypeid
                    LEFT JOIN storytype AS st ON prop.storytypeid = st.storytypeid
                    LEFT JOIN typeconstructiontype AS ct ON prop.typeconstructiontypeid = ct.typeconstructiontypeid
                    WHERE prop.propertylandusetypeid = 261;
                '''

    # Assign the data to a DataFrame.
    df = pd.read_sql(sql_query, get_connection(url))
    # Cache the data by saving it to a CSV file.
    df.to_csv('zillow_v1.csv')

    return df

# -----------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------
def acquire_all_zillow():

    # Create a helper function to get the necessary database connection URL.
    def get_db_connection(database):
        return get_connection(database)

    # Connect to the SQL 'zillow' database.
    url = "zillow"

    # Use this query to get the data.
    sql_query = '''
                    SELECT pred.parcelid, pred.logerror, pred.transactiondate, ac.airconditioningdesc, arc_sty.architecturalstyledesc, 
                            bc.buildingclassdesc, hs.heatingorsystemdesc, plu.propertylandusedesc, st.storydesc, ct.typeconstructiondesc,
                            prop.*
                    FROM properties_2017 AS prop 
                    RIGHT JOIN (
                        SELECT MAX(transactiondate) AS max_transactiondate, parcelid
                        FROM predictions_2017
                        WHERE transactiondate LIKE '2017%%'
                        GROUP BY parcelid
                    ) AS max_dates ON prop.parcelid = max_dates.parcelid
                    LEFT JOIN predictions_2017 AS pred ON max_dates.parcelid = pred.parcelid AND max_dates.max_transactiondate = pred.transactiondate
                    LEFT JOIN airconditioningtype AS ac ON prop.airconditioningtypeid = ac.airconditioningtypeid
                    LEFT JOIN architecturalstyletype AS arc_sty ON prop.architecturalstyletypeid = arc_sty.architecturalstyletypeid
                    LEFT JOIN buildingclasstype AS bc ON prop.buildingclasstypeid = bc.buildingclasstypeid
                    LEFT JOIN heatingorsystemtype AS hs ON prop.heatingorsystemtypeid = hs.heatingorsystemtypeid
                    LEFT JOIN propertylandusetype AS plu ON prop.propertylandusetypeid = plu.propertylandusetypeid
                    LEFT JOIN storytype AS st ON prop.storytypeid = st.storytypeid
                    LEFT JOIN typeconstructiontype AS ct ON prop.typeconstructiontypeid = ct.typeconstructiontypeid
                    WHERE prop.propertylandusetypeid = 261;
                '''

    # Assign the data to a DataFrame.
    df = pd.read_sql(sql_query, get_connection(url))
    # Cache the data by saving it to a CSV file.
    df.to_csv('zillow_v2.csv')

    return df

# -----------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------

def get_zillow_data():
    """
    Get Zillow data either from a CSV file or the SQL database.

    This function first checks if a CSV file named 'zillow.csv' exists. If it does, it reads the data from the CSV
    file into a DataFrame. If the CSV file doesn't exist, it calls the 'acquire_zillow' function to fetch the data
    from the SQL database, caches the data into a CSV file, and returns the DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing Zillow property data.

    Note:
        - The data is cached in 'zillow.csv' to avoid repeated database queries.
    """
    if os.path.isfile('zillow_v1.csv'):
        # If the CSV file exists, read in data from the CSV file.
        df = pd.read_csv('zillow_v1.csv', index_col=0)
    else:
        # Read fresh data from the database into a DataFrame.
        df = acquire_zillow()

    return df

# -----------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------

def get_all_zillow_data():
    """
    Get Zillow data either from a CSV file or the SQL database.

    This function first checks if a CSV file named 'zillow.csv' exists. If it does, it reads the data from the CSV
    file into a DataFrame. If the CSV file doesn't exist, it calls the 'acquire_zillow' function to fetch the data
    from the SQL database, caches the data into a CSV file, and returns the DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing Zillow property data.

    Note:
        - The data is cached in 'zillow.csv' to avoid repeated database queries.
    """
    if os.path.isfile('zillow_v2.csv'):
        # If the CSV file exists, read in data from the CSV file.
        df = pd.read_csv('zillow_v2.csv', index_col=0)
    else:
        # Read fresh data from the database into a DataFrame.
        df = acquire_all_zillow()

    return df

# -----------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------
def acquire_zillow_full():
    """
    Acquire Zillow data from a SQL database.

    This function connects to a SQL database named 'zillow', executes a query to retrieve specific property data
    for the year 2017, and returns the data as a DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing Zillow property data for the year 2017.

    Note:
        - This function relies on a helper function 'get_db_connection' to establish a database connection.
    """
    # Create a helper function to get the necessary database connection URL.
    def get_db_connection(database):
        return get_connection(database)

    # Connect to the SQL 'zillow' database.
    url = "zillow"

    # Use this query to get the data.
    sql_query = '''
                    SELECT pred.parcelid, pred.logerror, pred.transactiondate, ac.airconditioningdesc, arc_sty.architecturalstyledesc, 
                            bc.buildingclassdesc, hs.heatingorsystemdesc, plu.propertylandusedesc, st.storydesc, ct.typeconstructiondesc,
                            prop.basementsqft, prop.bathroomcnt, prop.bedroomcnt, prop.calculatedbathnbr, prop.finishedfloor1squarefeet, 
                            prop.calculatedfinishedsquarefeet, prop.finishedsquarefeet12, prop.finishedsquarefeet13, prop.finishedsquarefeet15, 
                            prop.finishedsquarefeet50, prop.finishedsquarefeet6, prop.fips, prop.fireplacecnt, prop.fullbathcnt, prop.garagecarcnt, 
                            prop.garagetotalsqft, prop.hashottuborspa, prop.latitude, prop.longitude, prop.lotsizesquarefeet, prop.poolcnt, prop.poolsizesum, 
                            prop.propertycountylandusecode, prop.propertyzoningdesc, prop.rawcensustractandblock, prop.roomcnt, prop.threequarterbathnbr, 
                            prop.unitcnt, prop.yardbuildingsqft17, prop.yardbuildingsqft26, prop.yearbuilt, prop.numberofstories, prop.fireplaceflag, 
                            prop.structuretaxvaluedollarcnt, prop.taxvaluedollarcnt, prop.assessmentyear, prop.landtaxvaluedollarcnt, prop.taxamount, 
                            prop.taxdelinquencyflag, prop.taxdelinquencyyear, prop.censustractandblock, prop.regionidzip,prop.buildingqualitytypeid, 
                            prop.decktypeid, prop.pooltypeid10, prop.pooltypeid2, prop.pooltypeid7, prop.regionidcity, prop.regionidcounty, prop.regionidneighborhood
                    FROM predictions_2017 AS pred
                    LEFT JOIN properties_2017 AS prop ON pred.parcelid = prop.parcelid
                    LEFT JOIN airconditioningtype AS ac ON prop.airconditioningtypeid = ac.airconditioningdesc
                    LEFT JOIN architecturalstyletype AS arc_sty ON prop.architecturalstyletypeid = arc_sty.architecturalstyletypeid
                    LEFT JOIN buildingclasstype AS bc ON prop.buildingclasstypeid = bc.buildingclasstypeid
                    LEFT JOIN heatingorsystemtype AS hs ON prop.heatingorsystemtypeid = hs.heatingorsystemtypeid
                    LEFT JOIN propertylandusetype AS plu ON prop.propertylandusetypeid = plu.propertylandusetypeid
                    LEFT JOIN storytype AS st ON prop.storytypeid = st.storytypeid
                    LEFT JOIN typeconstructiontype AS ct ON prop.typeconstructiontypeid = ct.typeconstructiontypeid
                    WHERE pred.transactiondate LIKE '2017%%'
                    AND prop.propertylandusetypeid = 261;
                '''

    # Assign the data to a DataFrame.
    df = pd.read_sql(sql_query, get_connection(url))

    return df

# -----------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------

def get_zillow_data_full():
    """
    Get Zillow data either from a CSV file or the SQL database.

    This function first checks if a CSV file named 'zillow.csv' exists. If it does, it reads the data from the CSV
    file into a DataFrame. If the CSV file doesn't exist, it calls the 'acquire_zillow' function to fetch the data
    from the SQL database, caches the data into a CSV file, and returns the DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing Zillow property data.

    Note:
        - The data is cached in 'zillow.csv' to avoid repeated database queries.
    """
    if os.path.isfile('zillow_full.csv'):
        # If the CSV file exists, read in data from the CSV file.
        df = pd.read_csv('zillow_full.csv', index_col=0)
    else:
        # Read fresh data from the database into a DataFrame.
        df = acquire_zillow_full()
        # Cache the data by saving it to a CSV file.
        df.to_csv('zillow_full.csv')
    return df