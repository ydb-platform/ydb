# small wrapper for operations with dynconfig

import logging
import os

from ydb import Driver as YdbDriver, DriverConfig, AnonymousCredentials, credentials_from_env_variables
from ydb.draft import DynamicConfigClient


logger = logging.getLogger(__name__)


DYNCONFIG_NAME = 'dynconfig.yaml'


class Client():
    def __init__(self, node_list, domain, credentials=credentials_from_env_variables(), allow_fallback_to_anonymous_credentials=True):
        self.config = DriverConfig(
            endpoint=f'grpc://{node_list[0]}:2135',
            database=domain,
            credentials=credentials,
            endpoints=[f'grpc://{x}:2135' for x in node_list],
        )
        self.allow_fallback_to_anonymous_credentials = allow_fallback_to_anonymous_credentials
        self.driver = YdbDriver(self.config)

    def __enter__(self):
        try:
            self.driver.wait(timeout=5)
        except TimeoutError:
            if self.allow_fallback_to_anonymous_credentials:
                logger.warning('Trying to fallback to anonymous credentials. '
                               'To get rid of this message either set YDB_ANONYMOUS_CREDENTIALS=1 '
                               'Or set correct credentials in env as it described in ydb-python-sdk.')
                config = self.config
                config.credentials = AnonymousCredentials()
                self.driver = YdbDriver(config)
                try:
                    self.driver.wait(timeout=5)
                except TimeoutError:
                    logger.error('Unable to connect to static nodes for dynconfig management.')
                    raise
            else:
                logger.error('Unable to connect to static nodes for dynconfig management.')
                raise

        return DynamicConfigClient(self.driver)

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass


def get_config_path(project_path):
    return os.path.join(project_path, DYNCONFIG_NAME)


def get_config_abspath(project_path):
    return os.path.abspath(get_config_path(project_path))


def get_local_config(project_path):
    dynconfig_file = get_config_abspath(project_path)
    if not os.path.exists(dynconfig_file):
        return None
    with open(dynconfig_file, 'r') as config:
        return config.read()


def write_local_config(project_path, new_config):
    dynconfig_file = get_config_abspath(project_path)
    with open(dynconfig_file, 'w') as config:
        config.write(new_config)


def get_remote_config(client):
    return client.get_config().config


def apply_config(client, project_path, force=False, allow_unknown_fields=True):
    config = get_local_config(project_path)
    if force:
        client.set_config(config, False, allow_unknown_fields)
    else:
        client.replace_config(config, False, allow_unknown_fields)
    # TODO: there is small race here, we can pull back config applied after our's, but for dev slices it's fine
    # as far as even in real production with long approve process we faced conflict only once in a three months
    return get_remote_config(client)
