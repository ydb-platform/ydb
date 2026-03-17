from office365.outlook.calendar.working_hours import WorkingHours
from office365.outlook.locale_info import LocaleInfo
from office365.outlook.mail.automatic_replies_setting import AutomaticRepliesSetting
from office365.runtime.client_value import ClientValue


class MailboxSettings(ClientValue):
    """Settings for the primary mailbox of a user."""

    def __init__(
        self,
        time_format=None,
        time_zone=None,
        automatic_replies_setting=AutomaticRepliesSetting(),
        archive_folder=None,
        date_format=None,
        language=LocaleInfo(),
        working_hours=WorkingHours(),
    ):
        """
        :param str time_format: The time format for the user's mailbox.
        :param str time_zone: The default time zone for the user's mailbox.
        :param AutomaticRepliesSetting automatic_replies_setting: 	Configuration settings to automatically notify
            the sender of an incoming email with a message from the signed-in user.
        :param str archive_folder: Folder ID of an archive folder for the user.
        :param str date_format: The date format for the user's mailbox.
        :param LocaleInfo language: The locale information for the user, including the preferred language
            and country/region.
        :param WorkingHours working_hours: The days of the week and hours in a specific time zone that the user works.
        """
        super(MailboxSettings, self).__init__()
        self.timeFormat = time_format
        self.timeZone = time_zone
        self.automaticRepliesSetting = automatic_replies_setting
        self.archiveFolder = archive_folder
        self.dateFormat = date_format
        self.language = language
        self.workingHours = working_hours
