import time
from db.db import ClickhouseHandler

from sqlalchemy import text


def read_script(filepath: str) -> text:
    """get sql from file"""
    try:
        f = open(file=filepath, mode="r")

        return text(f.read())
    except Exception as e:
        return text(f"select {str(e)}")


def wait_until_db_available():
    tries = 10
    actual_tries = 0

    while actual_tries < tries:
        time.sleep(10)

        try:
            db = ClickhouseHandler()

            with db as session:
                session.ping()

                break
        except Exception as e:
            actual_tries += 1
            print(e)


if __name__ == "__main__":
    wait_until_db_available()

    db = ClickhouseHandler()

    with db as session:
        session.ping()
