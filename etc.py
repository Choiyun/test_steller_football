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


def cal_prob(self, initial_values: pd.Series, final_values: pd.Series, num_rows: int) -> Dict[str, any]:
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
      'away_change': None
    }


def analyze_odds(self, game_id: str) -> pd.DataFrame:
  try:
    initial_odds, final_odds = self.get_odds(ls_game_id=game_id)

    if not initial_odds or not final_odds:
      logger.error(f"No vaild odds data available for game ID {game_id}")
      return pd.DataFrame()

    initial_values = pd.Series(initial_odds)
    final_values = pd.Series(final_odds)
    num_rows = len(pd.DataFrame([initial_values, final_values]))
    prob = self.cal_prob(initial_values, final_values, num_rows)
    odds_df = self.create_odds_dataframe(initial_values, final_values, prob)

    # 중복된 행 제거
    odds_df = odds_df.drop_duplicates(subset=['time'], keep='first')
    return odds_df

  except Exception as e:
    logger.error(f"Error analyzing odds for game ID {game_id}: {e}", exc_info=True)
    return pd.DataFrame()
  def create_odds_dataframe(self, initial_values:pd.Series, final_values:pd.Series, prob:Dict[str,float]) -> pd.DataFrame:
    try:
      data = {'game_id':[initial_values['game_id'], final_values['game_id']],
              'time': [initial_values['time'], final_values['time']],
              'home_odds': [initial_values['initial_home_odds'], final_values['final_home_odds']],
              'draw_odds': [initial_values['initial_draw_odds'], final_values['final_draw_odds']],
              'away_odds': [initial_values['initial_away_odds'], final_values['final_away_odds']],
              'home_prob': [prob['initial_home'], prob['final_home']],
              'draw_prob': [prob['initial_draw'], prob['final_draw']],
              'away_prob': [prob['initial_away'], prob['final_away']],
              'home_prob_changes':[None, prob['home_change']],
              'draw_prob_changes':[None, prob['draw_change']],
              'away_prob_changes':[None, prob['away_change']]
              }
      return pd.DataFrame(data)

    except Exception as e:
      logger.error(f"Error creating odds dataframe: {e}", exc_info=True)
      return pd.DataFrame()

  def save_preview(self, season_id, sp_id, le_id, game_id, language_code, fact_desc):
        self.cursor.execute("EXEC LS_QUARRY.dbo.SP_TB_CHATGPT_GAME_FUN_FACTS_UPDATE_INSERT ?, ?, ?, ?, ?, ?",
                            (season_id, sp_id, le_id, game_id, language_code, fact_desc))
        self.conn.commit()

#
if __name__ == "__main__":
  db = DatabaseManager(DB_CONFIG, TARGET_DB_CONFIG)
  df = db.get_target_games()
  print(df)
  merge = db.merge_game_info()
  print(merge)

  merge_info = db.merge_game_info()

  if not merge_info.empty:
    first_game_id = merge_info.iloc[0]['GAME_ID']
    print(f"Testing with GAME_ID: {first_game_id}")

    fun_fact_data = db.get_fun_fact(first_game_id)
    print("Table A Data:")
    print(fun_fact_data)
  else:
    print("No target games found.")
  #
  # 특정 LS_GAME_ID로 get_table_b 테스트
  test_ls_game_id = 'B20242751619375'
  print(f"Testing with LS_GAME_ID: {test_ls_game_id}")

  table_b_data = db.get_odds(test_ls_game_id)
  print("Table B Data:")
  print(table_b_data)

  initial_odds, final_odds = db.get_odds(test_ls_game_id)
  df = db.analyze_odds(test_ls_game_id)
  print(df)