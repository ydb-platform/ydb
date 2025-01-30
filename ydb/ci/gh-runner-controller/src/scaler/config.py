import logging

import yaml

from .yc import discover_folder_id


# FIXME: better config using models like pydantic ?
class Config:
    def __init__(self, config=None):
        self.logger = logging.getLogger(__name__)
        self._config = {} if config is None else config

    @classmethod
    def load(cls, path):
        with open(path, "r") as fp:
            instance = cls(yaml.load(fp, Loader=yaml.SafeLoader)["config"])
        return instance

    def __getattr__(self, item):
        return self._config.get(item)

    def late_discover_from_yc(self, yc):
        if self.yc_folder_id_use_metadata:
            folder_id = discover_folder_id()
            self.logger.info("autodiscover: folder_id=%s", folder_id)
            self._config["yc_folder_id"] = folder_id

        if self.gh_token_secret_id:
            self.logger.info("try to get Github webhook secret %s", self.gh_token_secret_id)
            secret = yc.get_lockbox_secret_entries(self.gh_token_secret_id)
            self._config["gh_token"] = secret[self.gh_token_secret_key]

        if self.clickhouse_secret_id:
            self.logger.info("try to get Clickhouse secret from %s", self.clickhouse_secret_id)
            secret = yc.get_lockbox_secret_entries(self.clickhouse_secret_id)

            self._config.update(
                {
                    "clickhouse_fqdns": secret["ch_fqdns"],
                    "clickhouse_database": secret["ch_database"],
                    "clickhouse_username": secret["ch_username"],
                    "clickhouse_password": secret["ch_password"],
                }
            )
