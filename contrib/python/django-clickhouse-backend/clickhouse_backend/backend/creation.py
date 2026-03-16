import sys

from clickhouse_driver.errors import ErrorCodes
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.utils import strip_quotes
from django.conf import settings
from django.utils.module_loading import import_string


class DatabaseCreation(BaseDatabaseCreation):
    def _quote_name(self, name):
        return self.connection.ops.quote_name(name)

    def sql_table_creation_suffix(self):
        test_settings = self.connection.settings_dict["TEST"]
        cluster = test_settings.get("cluster")
        engine = test_settings.get("engine")

        parts = []
        if cluster:
            parts.append(f"ON CLUSTER {self.connection.ops.quote_name(cluster)}")
        if engine:
            parts.append(f"ENGINE = {engine}")
        return " ".join(parts)

    def _get_on_cluster(self):
        test_settings = self.connection.settings_dict["TEST"]
        cluster = test_settings.get("cluster")
        if cluster:
            return f"ON CLUSTER {self.connection.ops.quote_name(cluster)}"
        return ""

    def _database_exists(self, cursor, database_name):
        cursor.execute(
            "SELECT 1 FROM system.databases WHERE name = %s",
            [strip_quotes(database_name)],
        )
        return cursor.fetchone() is not None

    def create_test_db(
        self, verbosity=1, autoclobber=False, serialize=True, keepdb=False
    ):
        super().create_test_db(verbosity, autoclobber, serialize, keepdb)
        test_settings = self.connection.settings_dict["TEST"]
        if "fake_transaction" in test_settings:
            self.connection.fake_transaction = test_settings["fake_transaction"]
        self.mark_expected_failures_and_skips()

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """
        Internal implementation - create the test db tables.
        """
        if not self.connection.settings_dict["TEST"].get("managed", True):
            return
        test_database_name = self._get_test_db_name()
        test_db_params = {
            "dbname": self.connection.ops.quote_name(test_database_name),
            "suffix": self.sql_table_creation_suffix(),
        }
        # Create the test database and connect to it.
        with self._nodb_cursor() as cursor:
            try:
                self._execute_create_test_db(cursor, test_db_params, keepdb)
            except Exception as e:
                # if we want to keep the db, then no need to do any of the below,
                # just return and skip it all.
                if keepdb:
                    return test_database_name

                self.log("Got an error creating the test database: %s" % e)
                if not autoclobber:
                    confirm = input(
                        "Type 'yes' if you would like to try deleting the test "
                        "database '%s', or 'no' to cancel: " % test_database_name
                    )
                if autoclobber or confirm == "yes":
                    try:
                        if verbosity >= 1:
                            self.log(
                                "Destroying old test database for alias %s..."
                                % (
                                    self._get_database_display_str(
                                        verbosity, test_database_name
                                    ),
                                )
                            )
                        sql = "DROP DATABASE %(dbname)s" % test_db_params
                        on_cluster = self._get_on_cluster()
                        if on_cluster:
                            sql = f"{sql} {on_cluster} SYNC"
                        cursor.execute(sql)
                        self._execute_create_test_db(cursor, test_db_params, keepdb)
                    except Exception as e:
                        self.log("Got an error recreating the test database: %s" % e)
                        sys.exit(2)
                else:
                    self.log("Tests cancelled.")
                    sys.exit(1)

        return test_database_name

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        try:
            if keepdb and self._database_exists(cursor, parameters["dbname"]):
                # If the database should be kept and it already exists, don't
                # try to create a new one.
                return
            super()._execute_create_test_db(cursor, parameters, keepdb)
        except Exception as e:
            if (
                not e.args
                or getattr(e.args[0], "code", "") != ErrorCodes.DATABASE_ALREADY_EXISTS
            ):
                # All errors except "database already exists" cancel tests.
                self.log("Got an error creating the test database: %s" % e)
                sys.exit(2)
            elif not keepdb:
                # If the database should be kept, ignore "database already
                # exists".
                raise

    def _destroy_test_db(self, test_database_name, verbosity):
        test_settings = self.connection.settings_dict["TEST"]
        if not test_settings.get("managed", True):
            return
        sql = "DROP DATABASE %s" % self.connection.ops.quote_name(test_database_name)
        on_cluster = self._get_on_cluster()
        if on_cluster:
            sql = f"{sql} {on_cluster} SYNC"
        with self._nodb_cursor() as cursor:
            cursor.execute(sql)

    def mark_expected_failures_and_skips(self):
        """
        Mark tests in Django's test suite which are expected failures on this
        database and test which should be skipped on this database.
        """
        # Only load unittest if we're actually testing.
        from unittest import expectedFailure, skip

        for test_name in self.connection.features.django_test_expected_failures:
            test_case_name, _, test_method_name = test_name.rpartition(".")
            test_app = test_name.split(".")[0]
            # Importing a test app that isn't installed raises RuntimeError.
            if test_app in settings.INSTALLED_APPS:
                test_case = import_string(test_case_name)
                test_method = getattr(test_case, test_method_name)
                setattr(test_case, test_method_name, expectedFailure(test_method))
        for reason, tests in self.connection.features.django_test_skips.items():
            for test_name in tests:
                test_case_name, _, test_method_name = test_name.rpartition(".")
                test_app = test_name.split(".")[0]
                # Importing a test app that isn't installed raises RuntimeError.
                if test_app in settings.INSTALLED_APPS:
                    test_case = import_string(test_case_name)
                    test_method = getattr(test_case, test_method_name)
                    setattr(test_case, test_method_name, skip(reason)(test_method))
