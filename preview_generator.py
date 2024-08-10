import json
import logging
from typing import List, Dict
from pydantic import BaseModel, Field
from database_manager import DatabaseManager
from data_processor import DataProcessor
from langchain_openai import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.output_parsers import JsonOutputParser, PydanticOutputParser
from langchain_community.callbacks import get_openai_callback
from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, HumanMessagePromptTemplate
from config import OPENAI_API_KEY, DB_CONFIG, TARGET_DB_CONFIG
from prompt import default_prompt, tournament_prompt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AnalysisSection(BaseModel):
    title: str
    content: str

class Conclusion(BaseModel):
    conclusion: str
