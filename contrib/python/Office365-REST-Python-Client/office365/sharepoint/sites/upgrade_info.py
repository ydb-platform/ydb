import datetime

from office365.runtime.client_value import ClientValue


class UpgradeInfo(ClientValue):
    """A class containing site collection upgrade information."""

    def __init__(
        self,
        error_file=None,
        errors=None,
        last_updated=datetime.datetime.min,
        log_file=None,
        request_date=datetime.datetime.min,
        retry_count=None,
        start_time=datetime.datetime.min,
        status=None,
        upgrade_type=None,
        warnings=None,
    ):
        """
        :param str error_file: Specifies the location of the file that contains upgrade errors.
        :param int errors: Specifies the number of errors encountered during the site collection upgrade.
        :param datetime.datetime last_updated: Specifies the DateTime of the latest upgrade progress update.
        :param str log_file: Specifies the location of the file that contains upgrade log.
        :param datetime.datetime request_date: Specifies the DateTime when the site collection upgrade was requested.
        :param int retry_count: Specifies how many times the site collection upgrade was attempted.
        :param datetime.datetime start_time: Specifies the DateTime when the site collection upgrade was started.
        :param int status: Specifies the current site collection upgrade status.
        :param int upgrade_type: Specifies the type of the site collection upgrade type. The type can be either a
             build-to-build upgrade, or a version-to-version upgrade.
        :param int warnings: Specifies the number of warnings encountered during the site collection upgrade.
        """
        self.ErrorFile = error_file
        self.Errors = errors
        self.LastUpdated = last_updated
        self.LogFile = log_file
        self.RequestDate = request_date
        self.RetryCount = retry_count
        self.StartTime = start_time
        self.Status = status
        self.UpgradeType = upgrade_type
        self.Warnings = warnings
