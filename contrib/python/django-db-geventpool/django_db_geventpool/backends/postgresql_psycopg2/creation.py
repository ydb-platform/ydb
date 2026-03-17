# coding=utf-8

from django.db.backends.postgresql.creation import DatabaseCreation as OriginalDatabaseCreation


class DatabaseCreationMixin(object):
    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        self.connection.closeall()
        return super(DatabaseCreationMixin, self)._create_test_db(verbosity, autoclobber, keepdb)

    def _destroy_test_db(self, test_database_name, verbosity):
        self.connection.closeall()
        return super(DatabaseCreationMixin, self)._destroy_test_db(test_database_name, verbosity)


class DatabaseCreation(DatabaseCreationMixin, OriginalDatabaseCreation):
    pass
