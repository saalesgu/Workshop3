# Database and SQL
# ======================================================================

from sqlalchemy import Column, Integer, String, DateTime, Boolean, Date, Float
from sqlalchemy.orm import declarative_base

BASE = declarative_base()

class Model(BASE):
    __tablename__ = 'ml_model'

    id = Column(Integer, primary_key=True)
    Social_Support = Column(Float, nullable=False)
    Year = Column(Integer, nullable=False)
    Trust = Column(Float, nullable=False)
    Generosity = Column(Float, nullable=False)
    Health = Column(Float, nullable=False)
    Economy = Column(Float, nullable=False)
    Freedom = Column(Float, nullable=False)
    Continent_Africa = Column(Integer, nullable=False)
    Continent_Asia = Column(Integer, nullable=False)
    Continent_Europe = Column(Integer, nullable=False)
    Continent_North_America = Column(Integer, nullable=False)
    Continent_Oceania = Column(Integer, nullable=False)
    Continent_South_America = Column(Integer, nullable=False)
    Economy_Health = Column(Float, nullable=False)
    Trust_Freedom = Column(Float, nullable=False)
    Economy_Trust = Column(Float, nullable=False)
    Trust_Health = Column(Float, nullable=False)
    Happiness_Score = Column(Float, nullable=False)
    Predicted_Happiness_Score = Column(Float, nullable=False)