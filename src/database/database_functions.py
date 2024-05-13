"""
Database Functions Utility

This module provides a utility function to establish a connection to a PostgreSQL database using SQLAlchemy
"""
# Operating System
# ======================================================================
import sys
import os

file_path = os.getenv('WORK_DIR')

sys.path.append(os.path.abspath(file_path))

# Database and SQL
# ======================================================================
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, inspect, Table, MetaData, insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker


# Data Manipulation
# ======================================================================
import pandas as pd

# Logging and Event Handling
# ======================================================================
import logging as log

# JSON
# ======================================================================
import json

log.basicConfig(level=log.INFO)

with open(f'{file_path}/db_settings.json', 'r') as file:
    credentials = json.load(file)

def get_engine(): 
    """
    Establish a connection to a PostgreSQL database using SQLAlchemy

    Parameters
    ----------
    credentials : dict
        A dictionary containing the database connection credentials

    Returns
    -------
    engine : sqlalchemy.engine.base.Engine
        A SQLAlchemy Engine object representing the established connection.

    Raises
    ------
    SQLAlchemyError: If there is an error establishing the database connection.

    """
    dialect = credentials.get('PGDIALECT')
    user = credentials.get('PGUSER')
    passwd = credentials.get('PGPASSWD')
    host = credentials.get('PGHOST')
    port = credentials.get('PGPORT')
    db = credentials.get('PGDB')

    url = f"{dialect}://{user}:{passwd}@{host}:{port}/{db}"


    try:
        if not database_exists(url):
            create_database(url)
            log.info(f'Database  created successfully!')
        
        engine = create_engine(url)
        log.info(f'Conected successfully to database!')
        return engine
    except SQLAlchemyError as e:
        log.error(f'Error: {e}')

def create_table(connection , db_model, table_name) -> None:
    """
    Create a table in a PostgreSQL database using SQLAlchemy

    Parameters
    ----------
    connection : sqlalchemy.engine.base.Engine
        A SQLAlchemy Engine object representing the established connection.
    
    db_model : sqlalchemy.orm.decl_api.DeclarativeMeta
        A SQLAlchemy DeclarativeMeta object representing the database model.
    
    table_name : str
        A string representing the name of the table to be created.
    
    Returns
    -------
    None
    """
    try:
        if inspect(connection).has_table(table_name):
            db_model.__table__.drop(connection)
            log.info("Table dropped successfully.")
        
        db_model.__table__.create(connection)
        log.info("Table created successfully.")
    except SQLAlchemyError as e:
        log.error(f'Error: {e}')


def insert_data(df : pd.DataFrame, table_name : str, connection):
    """
    Insert data into a table in a PostgreSQL database using to_sql method

    Parameters
    ----------
    df : pandas.DataFrame
        A pandas DataFrame object representing the data to be inserted.
    
    table_name : str
        A string representing the name of the table to insert the data into.
    
    connection : sqlalchemy.engine.base.Engine
        A SQLAlchemy Engine object representing the established connection.
    
    Returns
    -------
    None
    """
    try:
        df.to_sql(table_name, connection, if_exists='replace', index=False)
        log.info("Data uploaded")
    except Exception as e:
        log.error(f"Error: {e}")

def create_session(engine):
    """
    Create a session to interact with a PostgreSQL database using SQLAlchemy

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
        A SQLAlchemy Engine object representing the established connection.
    
    Returns
    -------
    session : sqlalchemy.orm.session.Session
        A SQLAlchemy Session object representing the established connection.
    """

    Session = sessionmaker(bind=engine)
    session = Session()

    log.info("Session created successfully.")
    return session

def query_table(db_model, connection, session):
    """
    Query a table in a PostgreSQL database using SQLAlchemy

    Parameters
    ----------
    db_model : sqlalchemy.orm.decl_api.DeclarativeMeta
        A SQLAlchemy DeclarativeMeta object representing the database model.

    connection : sqlalchemy.engine.base.Engine
        A SQLAlchemy Engine object representing the established connection.
    
    session : sqlalchemy.orm.session.Session
        A SQLAlchemy Session object representing the established connection.
    
    Returns
    -------
    df : pandas.DataFrame
        A pandas DataFrame object representing the queried data.
    """

    query = session.query(db_model).statement
    df = pd.read_sql(query, connection)

    log.info("Data queried successfully.")
    return df

"""
 Make sure to replace the placeholder credentials with your actual database credentials.
"""