import pandas as pd
import logging

from sqlalchemy import create_engine

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::DataConnectionModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)
    
class DataConn:
    def __init__(self, config: dict,schema: str, table_name:str):
        self.config = config
        self.schema = schema
        self.table_name = table_name
        self.db_engine = None

    def get_conn(self):
        username = self.config.get('REDSHIFT_USERNAME')
        password = self.config.get('REDSHIFT_PASSWORD')
        host = self.config.get('REDSHIFT_HOST')
        port = self.config.get('REDSHIFT_PORT', '5439')
        dbname = self.config.get('REDSHIFT_DBNAME')

        # Construct the connection URL
        connection_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
        self.db_engine = create_engine(connection_url)
        print("conexion", username)
        print(type(self.db_engine.connect()))


        try:
            with self.db_engine.connect() as connection:
                result = connection.execute('SELECT 1;')
            if result:
                logging.info("Connection created")
                return
        except Exception as e:
            logging.error(f"Failed to create connection: {e}")
            raise
    
    def check_table_exists(self, table_name: str) -> bool:
        with self.db_engine.connect() as connection:
            query_checker = f"""
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = '{self.schema}'
                AND table_name = '{table_name}';
            """
            result = connection.execute(query_checker)
                        
            if not result.fetchone():
                logging.error(f"No {table_name} has been created")
                raise ValueError(f"No {table_name} has been created")
            
            logging.info(f"{table_name} exists")

    def create_table(self, schema, table_name: str):
        with self.db_engine.connect() as connection:
            create_table_query = f"""
                DROP TABLE IF EXISTS "{schema}"."{table_name}";

                CREATE TABLE "{schema}"."{table_name}" (
                    date TIMESTAMP,
                    open_price FLOAT,
                    high_price FLOAT,
                    low_price FLOAT,
                    close_price FLOAT,
                    volume BIGINT,
                    symbol VARCHAR(10),
                    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            connection.execute(create_table_query)

    def upload_data(self, data: pd.DataFrame, table: str):
        if self.db_engine is None:
            logging.warn("Execute it before")
            self.get_conn()

        try:
            data.to_sql(
                table,
                con=self.db_engine,
                schema=self.schema,
                if_exists='append',
                index=False
            )

            logging.info(f"Data from the DataFrame has been uploaded to the {self.schema}.{table} table in Redshift.")
            
        except Exception as e:
            logging.error(f"Failed to upload data to {self.schema}.{table}:\n{e}")
            raise

    def close_conn(self):
        if self.db_engine:
            self.db_engine.dispose()
            logging.info("Connection to Redshift closed.")
        else:
            logging.warning("No active connection to close.")


