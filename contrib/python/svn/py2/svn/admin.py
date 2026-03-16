"""svnadmin calls for managing a repository."""

import logging

import svn.exception
import svn.common_base

_LOGGER = logging.getLogger(__name__)


class Admin(svn.common_base.CommonBase):
    def __init__(self, svnadmin_filepath='svnadmin', env={}, *args, **kwargs):
        super(Admin, self).__init__(*args, **kwargs)

        self.__env = env
        self.__svnadmin_filepath = svnadmin_filepath

    def create(self, path, svnadmin_filepath='svnadmin'):
        return self.__run_command('create', [path], do_combine=True)

    def __run_command(self, subcommand, args, **kwargs):
        cmd = [self.__svnadmin_filepath]

        cmd += [subcommand] + args
        return self.external_command(cmd, environment=self.__env, **kwargs)
