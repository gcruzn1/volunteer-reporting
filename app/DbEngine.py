"""
WIP - Idea is to abstract reads/writes to store data,
be it Google sheets, forms, a database, AWS s3, or any other storage
"""
import pymysql
import logging
from CloudEngine import CloudEngine

class DatabaseEngine:

    def __init__(self, **kwargs) -> None:
        self.engine = self.createEngine(**kwargs)
    
    def createEngine(self, **kwargs):
        match kwargs.get("type"):
            case "GoogleServiceClient":
                return CloudEngine().get_engine()
            case "mysql":
                return pymysql.connect(**kwargs)
            case _:
                pass
        