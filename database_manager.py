from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from config import TARGET_DB_CONFIG, DB_CONFIG
import pandas as pd
import urllib
import logging
import json
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from tabulate import tabulate
from typing import Dict
from datetime import datetime,timedelta

# Set pandas display options - 데이터 프레임 전체보기
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', None)

# SQLAlchemy Base setup
Base = declarative_base()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager: # steller_football new DatabaseManager
  def __init__(self, source_config: Dict[str,str], target_config: Dict[str,str]): #setting database_loader
    self.source_config = source_config
    self.target_config = target_config
    self.source_engine = None
    self.target_engine = None
    self.source_session = None
    self.target_session = None
    self.connect_database(self.source_config, 'source')
    self.connect_database(self.target_config, 'target')

  def create_connection_engine(self,config: Dict[str,str])-> str: #setting database url (mssql, postgresql)
    db_type = config['db_type']
    if db_type == 'mssql':
      password = urllib.parse.quote_plus(config['password'])
      return f"mssql+pyodbc://{config['user']}:{password}@{config['host']}:{config['port']}/{config['db_name']}?driver=ODBC+Driver+17+for+SQL+Server"
    elif db_type == 'postgresql':
      password = urllib.parse.quote_plus(config['password'])
      return  f"postgresql://{config['user']}:{password}@{config['host']}:{config['port']}/{config['db_name']}"
    else:
      raise ValueError(f"Unsupported database type: {db_type}")

  def connect_database(self, config: Dict[str,str], db_role:str):
    db_type = config['db_type']
    try:
      connection_string = self.create_connection_engine(config)
      if db_type == 'mssql' and db_role == 'source':
        self.source_engine = create_engine(connection_string, echo=False)
        self.source_session = sessionmaker(autocommit=False, autoflush=False, bind=self.source_engine)
        with self.source_engine.connect().execution_options(autocommit=True).begin() as conn:
          raw_conn = conn.connection
          raw_conn.fast_executemany = True
        logger.info(f"{db_type.capitalize()} database connection established.")
      elif db_type == 'postgresql' and db_role =='target':
        self.target_engine = create_engine(connection_string, echo=False)
        self.target_session = sessionmaker(autocommit=False, autoflush=False, bind=self.target_engine)
        logger.info(f"{db_type.capitalize()} database connection established")
    except Exception as e:
      logger.error(f"Error connecting to {db_type} deatabse: {e}")
  def close(self):
    try:
      if self.source_engine is not None:
        self.source_engine.dispose()
      if self.target_engine is not None:
        self.target_engine.dispose()
      logger.info(f"Database connection closed")
    except Exception as e:
      logger.error(f"Error closing database connection: {e}")

  def get_target_games(self): #steller target game loader
    try:
      query = text("EXEC LS_QUARRY.dbo.SP_TB_CHATGPT_FUN_FACT_TARGET_GAME")
      with self.source_engine.connect() as conn:
        df = pd.read_sql(query,conn)
        # df = df[['SEASON_ID','GAME_ID','LS_GAME_ID']]
        return df
    except Exception as e:
      print(f"Failed to fetch users: {e}")
      return pd.DataFrame()
  # def get_ls_game(self): #get ls_game
  #   try:
  #     query = text("""
  #     SELECT GAME_ID, SEASON_ID, COMPE, MATCH_DATE, MATCH_TIME, HOME_TEAM_ID,
  #            HOME_TEAM_NAME, AWAY_TEAM_ID, AWAY_TEAM_NAME, LEAGUE_ID, STATE
  #     FROM LIVESCORE_NEW.dbo.TB_LS_GAME
  #     WHERE TRY_CONVERT(DATETIME, CAST(MATCH_DATE AS CHAR(8)) + ' ' + RIGHT('0000' + CAST(MATCH_TIME AS VARCHAR(4)), 4)) IS NOT NULL
  #     AND TRY_CONVERT(DATETIME, CAST(MATCH_DATE AS CHAR(8)) + ' ' + RIGHT('0000' + CAST(MATCH_TIME AS VARCHAR(4)), 4))
  #     BETWEEN DATEADD(HOUR, 3, GETDATE()) AND DATEADD(HOUR, 72, GETDATE())
  #     AND STATE = 'B'
  #     """)
  #     with self.source_engine.connect() as conn:
  #       df = pd.read_sql(query, conn)
  #       df = df.rename(columns={'GAME_ID':'LS_GAME_ID'})
  #       return df
  #   except Exception as e:
  #     print(f"Failed to fetch users: {e}")
  #     return pd.DataFrame()
  # def merge_game_info(self): #source of steller football info
  #   target_games_df = self.get_target_games()
  #   ls_game_df = self.get_ls_game()
  #   #merged target_game & ls_game
  #   df = pd.merge(target_games_df,ls_game_df, on='LS_GAME_ID',how='inner')
  #   df = df.drop('SEASON_ID_y',axis=1)
  #   df = df.rename(columns={'SEASON_ID_x':'SEASON_ID'})
  #
  #   df = df[df['COMPE']=='soccer'] # compe only soccer

    return df
  def get_fun_fact(self,season_id:str,sp_id:str,le_id:str,game_id:str,language_code:str):
    try:
      query = text("""
                EXEC LS_QUARRY.dbo.SP_TB_BETRADAR_GAME_FUN_FACTS_SELECT 
                :season_id, :sp_id, :le_id, :game_id, :language_code
            """)
      with self.source_engine.connect() as conn:
        df = pd.read_sql(query,conn, params={"season_id": season_id,"sp_id": sp_id,"le_id": le_id,"game_id": game_id,"language_code":language_code})
      return df
    except Exception as e:
      print(f"Failed to fetch from table TB_BETRADAR_GAME_FUN_FACTS: {e}")
      return pd.DataFrame()

  def get_odds(self, ls_game_id:str): #load bet odds talbe and custom odds dataset
    try:
      query = text("EXEC LIVESCORE_NEW.dbo.SP_TB_LS_GAME_BETTING_RATE_BETSAPI_HISTORY_LIST_CHATGPT :ls_game_id, '', 'N'")
      with self.source_engine.connect() as conn:
        df = pd.read_sql(query,conn, params={"ls_game_id": ls_game_id})
        if df.empty:
          logger.info(f"No data found for ls_game_id{ls_game_id} in query")
          return pd.DataFrame()
        df = df[df['BET_FLAG'] != '1']
        rename_dict = {'GAME_ID':'game_id', 'SEQ_NO': 'seq_no', 'H_BET_RT': 'home_odds',
                       'D_BET_RT': 'draw_odds', 'A_BET_RT': 'away_odds', 'REG_DATE': 'time'}
        df.rename(columns=rename_dict, inplace=True)
        df['time'] = pd.to_datetime(df['time'].dt.strftime('%Y-%m-%d %H:%M'))
        columns = ['game_id', 'time', 'home_odds', 'draw_odds', 'away_odds']
        df = df[columns].sort_values(by='time',ascending=True)

        if df.empty:
          return {},{}
        initial_values = df.iloc[0].to_dict()
        final_values = df.iloc[-1].to_dict()

        if len(df) == 1:
          final_values = initial_values

        initial_odds = {'game_id': initial_values.get('game_id',None),
                        'time': initial_values.get('time', None),
                        'initial_home_odds': initial_values.get('home_odds',None),
                        'initial_draw_odds' : initial_values.get('draw_odds',None),
                        'initial_away_odds': initial_values.get('away_odds',None)}

        final_odds = {'game_id': initial_values.get('game_id',None),
                      'time': final_values.get('time', None),
                      'final_home_odds': final_values.get('home_odds', None),
                      'final_draw_odds': final_values.get('draw_odds', None),
                      'final_away_odds': final_values.get('away_odds', None)}

      return initial_odds, final_odds
    except Exception as e:
      print(f"Failed to fetch from table BETTING_RATE_BETSAPI_HISTORY_LIST_CHATGPT: {e}")

  def save_preview(self, season_id, sp_id, le_id, game_id, language_code, fact_desc):
    try:
      query = text("""
              EXEC LS_QUARRY.dbo.SP_TB_CHATGPT_GAME_FUN_FACTS_UPDATE_INSERT
              :season_id, :sp_id, :le_id, :game_id, :language_code, :fact_desc
          """)
      with self.source_engine.connect() as conn:
        conn.execute(query, {
          'season_id': int(season_id),
          'sp_id': int(sp_id),
          'le_id': int(le_id),
          'game_id': game_id,
          'language_code': language_code,
          'fact_desc': fact_desc
        })
        conn.commit()
      logger.info("Preview data saved successfully.")
    except SQLAlchemyError as e:
      logger.error(f"Failed to save preview data: {e}")


if __name__ == "__main__":
  db = DatabaseManager(DB_CONFIG, TARGET_DB_CONFIG)

  # Fetch target games
  df = db.get_target_games()
  print(df)

  # # Merge game info
  # merge_info = db.merge_game_info()
  # print(merge_info)

  if not df.empty:
    first_game_id = df.iloc[0]['GAME_ID']
    print(f"Testing with GAME_ID: {first_game_id}")

    # Get fun fact data with additional parameters
    season_id = int(df.iloc[0]['SEASON_ID'])
    sp_id = int(df.iloc[0]['SP_ID'])  # Assuming these columns exist and converting to int
    le_id = int(df.iloc[0]['LE_ID'])  # Assuming these columns exist and converting to int
    language_code = 'EN'  # Assuming a default language code

    fun_fact_data = db.get_fun_fact(season_id, sp_id, le_id, first_game_id, language_code)
    print("Fun Fact Data:")
    print(fun_fact_data)
  else:
    print("No target games found.")