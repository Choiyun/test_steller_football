import pandas as pd
import logging
from config import TARGET_DB_CONFIG, DB_CONFIG
from database_manager import DatabaseManager
from langchain_community.document_loaders import DataFrameLoader

# Set pandas display options - 데이터 프레임 전체보기
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', None)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
  def __init__(self, ls_game_id):
        self.db = DatabaseManager(DB_CONFIG, TARGET_DB_CONFIG)
        self.ls_game_id = ls_game_id

  def analyze_odds(self):
        initial_odds, final_odds = self.db.get_odds(self.ls_game_id)
        if not initial_odds or not final_odds:
            logger.error(f"No valid odds data available for game ID {self.ls_game_id}")
            return pd.DataFrame()

        initial_values = pd.Series(initial_odds)
        final_values = pd.Series(final_odds)
        num_rows = len(pd.DataFrame([initial_values, final_values]))
        prob = self.cal_prob(initial_values, final_values, num_rows)
        odds_df = self.process_odds(initial_values, final_values, prob)

        # 중복된 행 제거
        odds_df = odds_df.drop_duplicates(subset=['time'], keep='first')
        odds_df['time'] = pd.to_datetime(odds_df['time']).dt.strftime('%Y-%m-%d %H:%M')
        odds_loader = DataFrameLoader(odds_df, page_content_column='time')
        odds_doc = odds_loader.load()

        return {'odds_doc': odds_doc, 'odds_df': odds_df}

  @staticmethod
  def nomalized_prob(home_odds: float, draw_odds: float, away_odds: float) -> tuple[float, float, float]:
        try:
            # Calculate win probabilities
            home_prob = round(1 / float(home_odds), 3)
            draw_prob = round(1 / float(draw_odds), 3)
            away_prob = round(1 / float(away_odds), 3)

            # Normalize probabilities
            total_prob = home_prob + draw_prob + away_prob
            normalized_home_prob = round(home_prob / total_prob, 3)
            normalized_draw_prob = round(draw_prob / total_prob, 3)
            normalized_away_prob = round(away_prob / total_prob, 3)

            return normalized_home_prob, normalized_draw_prob, normalized_away_prob
        except Exception as e:
            logger.error(f"Error calculating normalized probabilities: {e}")
            return 0.0, None, 0.0

  def cal_prob(self, initial_values: pd.Series, final_values: pd.Series, num_rows: int) -> dict[str, any]:
        try:
            normalized_initial_prob = self.nomalized_prob(
                initial_values['initial_home_odds'],
                initial_values['initial_draw_odds'],
                initial_values['initial_away_odds']
            )
            normalized_final_prob = self.nomalized_prob(
                final_values['final_home_odds'],
                final_values['final_draw_odds'],
                final_values['final_away_odds']
            )
            prob_changes = {
                'home': round(normalized_final_prob[0] * 100 - normalized_initial_prob[0] * 100, 1) if num_rows > 1 else 0.0,
                'draw': round(normalized_final_prob[1] * 100 - normalized_initial_prob[1] * 100, 1) if num_rows > 1 else 0.0,
                'away': round(normalized_final_prob[2] * 100 - normalized_initial_prob[2] * 100, 1) if num_rows > 1 else 0.0
            }

            return {
                'initial_home': round(normalized_initial_prob[0] * 100, 1),
                'initial_away': round(normalized_initial_prob[2] * 100, 1),
                'initial_draw': round(normalized_initial_prob[1] * 100, 1),
                'final_home': round(normalized_final_prob[0] * 100, 1),
                'final_away': round(normalized_final_prob[2] * 100, 1),
                'final_draw': round(normalized_final_prob[1] * 100, 1),
                'home_change': prob_changes['home'],
                'draw_change': prob_changes['draw'],
                'away_change': prob_changes['away']
            }
        except Exception as e:
            logger.error(f"Error calculating probabilities: {e}", exc_info=True)
            return {
                'initial_home': None,
                'initial_away': None,
                'initial_draw': None,
                'final_home': None,
                'final_away': None,
                'final_draw': None,
                'home_change': None,
                'draw_change': None,
                'away_change': None}
  def process_odds(self, initial_values: pd.Series, final_values: pd.Series, prob: dict[str, float]) -> pd.DataFrame:
        try:
            data = {'game_id': [initial_values['game_id'], final_values['game_id']],
                    'time': [initial_values['time'], final_values['time']],
                    'home_odds': [initial_values['initial_home_odds'], final_values['final_home_odds']],
                    'draw_odds': [initial_values['initial_draw_odds'], final_values['final_draw_odds']],
                    'away_odds': [initial_values['initial_away_odds'], final_values['final_away_odds']],
                    'home_probability': [prob['initial_home'], prob['final_home']],
                    'draw_probability': [prob['initial_draw'], prob['final_draw']],
                    'away_probability': [prob['initial_away'], prob['final_away']],
                    'home_probability_change': [None, prob['home_change']],
                    'draw_probability_change': [None, prob['draw_change']],
                    'away_probability_change': [None, prob['away_change']]
                    }

            return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Error creating odds dataframe: {e}", exc_info=True)
            return pd.DataFrame()

  def process_fun_facts(self, season_id, sp_id, le_id, game_id, language_code):
    data = self.db.get_fun_fact(season_id, sp_id, le_id, game_id, language_code)

    if data.empty:
      return []
    # columns = ['0', '1', '2']
    # renamed_columns = {'0': 'id', '1': 'n', '2': 'facts'}

    df = pd.DataFrame.from_records(data)
    # df.rename(columns=renamed_columns, inplace=True)

    loader = DataFrameLoader(df, page_content_column="G_ID")
    docs = loader.load()

    return docs


if __name__ == "__main__":
    # 예시 ls_game_id 값 설정
    example_ls_game_id = 'P20240804122712477'  # 실제 값으로 변경하세요

    season_id = 2024  # 실제 값으로 변경하세요
    sp_id = 0  # 실제 값으로 변경하세요
    le_id = 41001  # 실제 값으로 변경하세요
    game_id = '51749581'  # 실제 값으로 변경하세요
    language_code = 'EN'  # 실제 값으로 변경하세요

    # DataProcessor 인스턴스 생성 시 ls_game_id를 제공
    dp = DataProcessor(example_ls_game_id)

    # Fun facts 처리
    docs = dp.process_fun_facts(season_id, sp_id, le_id, game_id, language_code)

    # 결과 출력
    for doc in docs:
      print(doc)
