from clickhouse_sqlalchemy import make_session
from decouple import config
from loguru import logger
from sqlalchemy import create_engine, text


class ClickhouseHandler:
    def __init__(self):
        url = f"clickhouse://{config('CLICKHOUSE_USER')}:{config('CLICKHOUSE_PASSWORD')}@{config('CLICKHOUSE_HOST')}/" \
              f"{config('CLICKHOUSE_DB')}"

        self.engine = create_engine(url)
        self.session = None

    def __enter__(self):
        logger.debug(f"Open db session to: {config('CLICKHOUSE_HOST')}/{config('CLICKHOUSE_DB')}")
        self.session = make_session(self.engine)

        logger.debug("Session was opened")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug(f"Close db session to: {config('CLICKHOUSE_HOST')}/{config('CLICKHOUSE_DB')}")
        self.session.close()
        logger.debug("Session was closed")

    def ping(self):
        logger.debug(str(self.session.execute(text("select 'pong';")).all()))

    def execute_sql(self, sql: text):
        logger.debug(f"Execute sql: {str(sql)}")
        return self.session.execute(sql).all()
