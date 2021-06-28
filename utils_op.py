import io
import logging
import pandas as pd
import gc
import os
import pandas.io.sql as sqlio
import boto3
from uuid import uuid4

from os import remove as remove_file
from time import time
from os.path import expanduser
from socket import gethostname
from numpy import nan, array_split
from random import randint


def get_conn_cur(driver='13', server=os.environ.get("MB_SERVER"), database=os.environ.get("MB_DATABASE"),
                 username=os.environ.get("MB_USERNAME"), password=os.environ.get("MB_PASSWORD")):
    """Conseguir conexion y cursor para conectarse a la base de datos"""
    pyodbc.connect('DRIVER={ODBC Driver '+driver+' for SQL Server};DATABASE=' + database +
                   ';Data Source=tcp:0.0.0.0,1234;SERVER=' + server + ';Info=True;UID=CloudManager;PWD='+password)
    connection = pyodbc.connect('DRIVER={ODBC Driver '+driver+' for SQL Server};SERVER=' +
                                server+';DATABASE='+database+';UID='+username+';PWD=' + password)
    cursor = connection.cursor()
    return connection, cursor


def s3_upload(file_path, bucket_name, s3_file_name):
    """Upload a file without checking if the file exists or not.
    NOTE: Try to avoid using this function and use instead:
        -s3_upload_first_time
        -s3_update_file

    Parameters
    ----------
    file_path: str
        Local path to the file
    bucket_name: str
        Name of the bucket where the file will be uploaded
    s3_file_name: str
        Name of the destination file in s3.
        You can use folder-like structure 'folder/file_name.ext'
    """
    s3 = boto3.client('s3')
    print("WARNING! Try to avoid using this function and use 's3_upload_first_time' or 's3_update_file' instead")
    s3.upload_file(file_path, bucket_name, s3_file_name)


def get_cases(conn, data, table, id_column='id'):
    assert(len(data.drop_duplicates(subset=id_column)) == len(data))
    sql = "select * from {};".format(table)
    data_server = sqlio.read_sql_query(sql, conn)
    data[id_column] = data[id_column].astype(str)
    data_server[id_column] = data_server[id_column].astype(str)
    data_complete = data_server[[id_column]].merge(data, on=id_column, how='outer', indicator=True)
    data_new = data_complete[data_complete['_merge'] == 'right_only']
    data_new = data_new.drop(columns='_merge')
    data_update = data_complete[data_complete['_merge'] == 'both']
    data_update = data_update.drop(columns='_merge')
    data_new["id"] = [uuid4() for x in range(len(data_new))]
    return data_new, data_update


def pd_read_pickle_s3(s3_client, bucket, key, *args, **kwargs):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_pickle(io.BytesIO(obj['Body'].read()), *args, **kwargs)


def pd_read_csv_s3(s3_client, bucket, key, *args, **kwargs):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), *args, **kwargs)


def pd_to_csv_s3(df, s3_client, bucket, key, *args, **kwargs):
    # csv_buffer = io.StringIO() # For python 3
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, *args, **kwargs)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

class IMDBException(Exception):
    """This is a Exception raise when something does terrible wrong while inserting data."""
    pass


def _get_useful_columns(dataframe, colnames, insert_geometry, latitud, longitud):
    """Compare colnames with the columns in the dataframe and return only the columns list that are gonna be used."""
    erase_geomcols = False
    if not colnames:
        colnames = dataframe.columns.values.tolist()
    elif (latitud not in colnames) and (
                longitud not in colnames) and insert_geometry:  # TODO: what if I only include one geocol
        colnames.extend([latitud, longitud])
        erase_geomcols = True
    return colnames, erase_geomcols


def _get_missing_columns(table, colnames, cursor, geocols=None):
    """ Compare the columns from the dataframe with the columns of the table and returns the missing columns"""
    cursor.execute(
        """SELECT column_name
           FROM information_schema.columns
           WHERE table_name = '{table_name}';""".format(table_name=table.lower())
    )
    result = cursor.fetchall()
    # Obtaining the columns of the table
    columns_in_db = [element[0] for element in result]
    if not geocols:
        geocols = []
    all_columns = geocols  # It is ok if latitud or longitud are not in the destiny table
    all_columns.extend(columns_in_db)
    all_columns = set(all_columns)
    colnames_set = set(colnames)
    missing_columns = colnames_set - all_columns
    return missing_columns


def _create_temp_file(dataframe, temporary_table, saving_path, separator):
    """Create and save a csv to be copied into the temporal table"""
    temporary_csv = r'{0}/supercalifragilisticoespialidoso_archivo_actualizacion_sql_{1}.csv'.format(
        saving_path, temporary_table)
    # Creating a .csv from the dataframe so it can be uploaded later by PostgreSQL itself.
    # Comentar esta linea para poder insertar jsons
    dataframe.to_csv(temporary_csv, header=None, encoding='utf-8', index=False,
                     sep=separator, na_rep=nan,  quotechar='\'')
    # This part of the code specifically replaces ".0|" (or any other separator) to just "|" in the .txt file generated.
    # This is useful when inserting integers, because if any null value is given, the whole column transforms to float,
    # which may cause problems later on
    # Opens the file in an only read mode, transforms it and then uses a write mode to replace it.
    f = open(temporary_csv, 'r')
    filedata = f.read()
    f.close()
    newdata = filedata.replace('.0' + separator, separator)
    newdata = newdata.replace('.0\n', '\n')
    f = open(temporary_csv, 'w')
    f.write(newdata)
    f.close()
    return temporary_csv


def _create_temp_table(cursor, temp_table, destiny_table, columns_string, erase_geomcols=False,
                       latitud=None, longitud=None):
    """Creates a temporal table in the database"""
    cursor.execute("""DROP TABLE IF EXISTS {temp_table};""".format(temp_table=temp_table))
    # Creating temporary_table
    cursor.execute(
        """CREATE TABLE {temp_table} AS
                            SELECT {colnames_str}
                            FROM {destiny_table}
                            LIMIT 0;""".format(temp_table=temp_table,
                                               colnames_str=columns_string, destiny_table=destiny_table)
    )
    # If erase_geomcols is True, it means the table originally does not have latlong columns, so we add them
    if erase_geomcols:
        cursor.execute(
            """ALTER TABLE {temp_table}
            ADD COLUMN {lat} numeric(30,15) NOT NULL,
            ADD COLUMN {lon} numeric(30,15) NOT NULL;""".format(temp_table=temp_table,
                                                                lat=latitud, lon=longitud)
        )


def _duplicate_temp_table(cursor, temp_table, destiny_table, columns_string):
    """Creates a temporal table in the database"""
    # Creating temporary_table
    cursor.execute(
        """CREATE TABLE {temp_table} AS
                            SELECT {colnames_str}
                            FROM {destiny_table}
                            LIMIT 0;""".format(temp_table=temp_table,
                                               colnames_str=columns_string, destiny_table=destiny_table)
    )
    return True


def insert_dataframe_to_postgresql(connection, table, dataframe, colnames=None, latitud='latitud', longitud='longitud',
                                   insert_geometry=False, geo_to_fill=None, separator="|", chunks=1, srid=4326):
    """
    Take a dataframe and insert all its values into a table in a given database.

    Due to the nature of a dataframe only its existing columns will be inserted, whereas the empty columns will be
    filled out with NULLs.
    Also, since the connection is provided by the user, it's imperative to close it manually after its usage.

    Parameters
    ----------
    connection : psycopg2 connection
        Variable connection to a database.
    table : str
        The table name where the data is going to be inserted.
    dataframe : pandas dataframe
        The dataframe whose values will be updated into the posgresql table.
    colnames : Optional[list]
        Column list of names to be added to the dataframe. If no colnames is specified,
        the whole dataframe will be used.
    latitud: str, optional
        The name of the column latitud in the DataFrame
    longitud: str, optional
        The name of the column longitud in the DataFrame
    insert_geometry : Optional[bool]
        If True, it will create a point geometry from lat-long data.
    geo_to_fill : Optional[str]
        Column name in the posgresql table of the created point geometry.
    separator : Optional[str]
        This function uses a .csv as a translator between the dataframe and the postgresql table,
        with this character we specify exactly where the raw data will be cutted, we decided a pipe (|) to cut the
        data normally but if the raw data already has pipes we shall choose another character as separator.
    chunks : Optional[int]
        Number of parts that the dataframe will be splited and then inserted one by one.
    srid : Optional[int]
        Spatial reference system key.
    """
    if dataframe.empty:
        raise IMDBException("Dataframe is empty.")

    # We need to cut the dataframe to the data columns the user defined in colnames. if not, all will be used.
    colnames, erase_geomcols = _get_useful_columns(dataframe, colnames, insert_geometry, latitud, longitud)
    dataframe = dataframe[colnames]
    # We have to figure out if all the columns of the dataframe are in the destiny table.
    my_cursor = connection.cursor()
    # Checking if at least one of the columns of the dataframe doesn't exist in the table
    missing_columns = _get_missing_columns(table, colnames, my_cursor, [latitud, longitud])

    if missing_columns:
        logging.error('Missing columns: ' + ', '.join(list(missing_columns)))
        # An assert is triggered when there are problems in the dataframe.
        raise IMDBException("At least one column of the dataframe doesn't exist in the destiny table.")

    # Dividing data
    dataframe_list = array_split(dataframe, chunks)
    lista_aux = [dataframe]
    del dataframe
    del lista_aux
    logging.info("Starting insert procedure...")
    home_directory = expanduser("~")
    hostname = gethostname().lower()
    for df in dataframe_list:
        start_time = time()
        # If a column in the destiny table doesn't exist in the dataframe, it is created in the same dataframe with
        # NULL values
        # copy_dataframe = df.copy()
        # These variables are meant to gather names and routes of the temporary table and the .csv file respectively.
        temporary_table = "{tablename}_temp_{hostname}_".format(tablename=table, hostname=hostname).replace('-', '_').replace(".", "")
        temporary_table += str(randint(1, 10000000)).zfill(8)
        temporary_csv = _create_temp_file(df, temporary_table, home_directory, separator)
        lista = [df]
        del df
        del lista
        gc.collect()
        # if latitud and longitud were not originally on colnames, it means the insert statement should not have these
        # columns inserted
        if erase_geomcols:
            colnames.remove(latitud)
            colnames.remove(longitud)
        # Constructing the insert statement
        columns_string = ", ".join(colnames)
        try:
            # For security reasons, first we must delete (if exists) the temporary table. And then create it
            _create_temp_table(my_cursor, temporary_table, table, columns_string, erase_geomcols, latitud, longitud)
            # Loading .csv to temporary table recently created.
            file_handler = open(temporary_csv, 'r')
            my_cursor.copy_from(file_handler, temporary_table, sep=separator, null='nan')
            file_handler.close()
            # Executing query for inserting values.
            logging.info("Inserting values...")
            # if insert_geometry is True, then a new column is added to the insert statement and the select gets a
            # function to create the geometry from the latlong columns
            destiny_columns = columns_string
            destiny_values = columns_string
            if insert_geometry:
                destiny_columns += ", " + geo_to_fill
                destiny_values += ", ST_SetSRID(ST_MakePoint({lon}," \
                                  " {lat}),{srid}) AS {tofill}".format(lon=longitud, lat=latitud, tofill=geo_to_fill,
                                                                       srid=str(srid))
            # The insertion is executed.
            my_cursor.execute(
                """INSERT INTO {destiny_table} ({columns})
                        SELECT {values} FROM {temp_table};""".format(destiny_table=table, columns=destiny_columns,
                                                                     values=destiny_values, temp_table=temporary_table)
            )
            signal = "Operation successful."
        # If an exception occurs, a message is triggered.
        except Exception as e:
            logging.error("An error occurred during method execution: ")
            logging.error(e)
            raise e
        # Dropping the temporary table.
        my_cursor.execute("""DROP TABLE IF EXISTS {0};""".format(temporary_table))
        connection.commit()
        # Removing the auxiliar .csv file.
        remove_file(temporary_csv)
        end_time = time()
        time_elapsed = end_time - start_time
        logging.info(signal)
        logging.info("Time elapsed inserting to postgresql (seconds): " + str(time_elapsed))


def update_dataframe_to_postgresql(connection, table, dataframe, key, separator="|"):
    """
    Update a postgresql table with the data in a dataframe.

    Due to the nature of a dataframe only its existing columns will be updated, even when the destiny table
    has more columns. It's highly recommended that the column related to the key parameter consists of unique values
    if not the rows might be overwritten.
    Also, since the connection is provided by the user, it's imperative to close it manually after its usage.

    Parameters
    ----------
    connection : psycopg2 connection
        Variable connection to a database.
    table : str
        The table in the database that will be updated.
    dataframe : pandas dataframe
        The dataframe whose values will be updated into the postgresql table.
    key : str
        The key to make possible the comparison to find out which rows will be updated.
    separator : Optional[str]
        This function uses a .csv as a translator between the dataframe and the postgresql table,
        with this character we specify exactly where the raw data will be cutted, we decided a pipe (|) to cut the
        data normally but if the raw data already has pipes we shall choose another character as separator.
    """
    if dataframe.empty:
        raise IMDBException("Dataframe is empty.")

    dataframe_columns = dataframe.columns.values.tolist()

    # Checking if at least one of the columns of the dataframe doesn't exist in the table
    my_cursor = connection.cursor()
    missing_columns = _get_missing_columns(table, dataframe_columns, my_cursor)

    if missing_columns:
        logging.error('Missing columns: ' + ', '.join(list(missing_columns)))
        # An assert is triggered when there are problems in the dataframe.
        raise IMDBException("At least one column of the dataframe doesn't exist in the destiny table.")

    logging.info("Starting procedure...")
    start = time()
    copy_dataframe = dataframe.copy()
    home_directory = expanduser("~")

    # Creating a temperary table
    temporary_table = "{dest_table}_temp_{host_name}".format(dest_table=table, host_name=gethostname().lower()).replace(
        '-', '_').replace(".", "")
    temporary_table += str(randint(1, 10000000)).zfill(8)
    columns_string = ','.join(dataframe_columns)
    _create_temp_table(my_cursor, temporary_table, table, columns_string)

    # Creating a .csv from the dataframe so it can be uploaded later by PostgreSQL itself.
    temporary_csv = _create_temp_file(copy_dataframe, temporary_table, home_directory, separator)

    # Loading .csv to temporary table recently created.
    file_handler = open(temporary_csv, 'r')
    my_cursor.copy_from(file_handler, temporary_table, sep=separator, null='nan')
    file_handler.close()

    # Building query for updating
    equals_strings = [column + " = " + temporary_table + "." + column for column in dataframe_columns if column != key]
    set_instruction = "SET " + ", ".join(equals_strings)

    print("Updating values of dataframe in table...")
    my_cursor.execute(
        """UPDATE {dest_table}
                     {values}
                     FROM {temp_table}
                     WHERE {dest_table}.{key} = {temp_table}.{key};""".format(dest_table=table, values=set_instruction,
                                                                              temp_table=temporary_table, key=key)
    )

    my_cursor.execute("""DROP TABLE IF EXISTS {temp_table};""".format(temp_table=temporary_table))
    connection.commit()
    remove_file(temporary_csv)
    end = time()
    time_elapsed = end - start
    print("Operation successful.")
    print("Time elapsed (seconds): ", time_elapsed)
