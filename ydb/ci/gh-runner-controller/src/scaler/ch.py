import json
import logging
import time
from typing import List

import requests


class CHException(Exception):
    pass


class InsertException(CHException):
    pass


class Clickhouse:
    def __init__(self, fqdns, db="default", username=None, password=None):
        self.logger = logging.getLogger(__name__)
        self.db = db
        fqdn = fqdns.split(",")[0]
        # FIXME: hardcoded ports
        self.url = f"http://{fqdn}:8123"
        self.auth = {}

        if username:
            self.auth["X-ClickHouse-User"] = username

        if password:
            self.auth["X-ClickHouse-Key"] = password

    def _select(self, query: str):
        params = {
            "database": self.db,
            "query": query,
            "default_format": "JSONEachRow",
        }

        for i in range(5):
            response = None

            try:
                response = requests.get(self.url, params=params, headers=self.auth, timeout=(5, 10))
                response.raise_for_status()
                return response.text
            except Exception as ex:
                self.logger.warning("Cannot fetch data with exception %s", str(ex))
                if response:
                    self.logger.warning("Response text %s", response.text)
                time.sleep(0.1 * i)

        raise CHException("Cannot fetch data from clickhouse")

    def select(self, query: str) -> List[dict]:
        text = self._select(query)
        result = []
        for line in text.split("\n"):
            if line:
                result.append(json.loads(line))
        return result
