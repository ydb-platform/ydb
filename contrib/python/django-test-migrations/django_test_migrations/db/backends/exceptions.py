from typing import ClassVar


class BaseDatabaseConfigurationException(Exception):  # noqa: N818
    """Base exception for errors related to database configuration."""


class DatabaseConfigurationNotFound(BaseDatabaseConfigurationException):
    """``BaseDatabaseConfiguration`` subclass when given db vendor not found."""

    message_template: ClassVar[str] = (
        '``BaseDatabaseConfiguration`` subclass for "{0}" vendor not found'
    )

    def __init__(self, vendor: str) -> None:
        """Format and set message from args and ``message_template``."""
        super().__init__(self.message_template.format(vendor))


class DatabaseConfigurationSettingNotFound(BaseDatabaseConfigurationException):
    """Database configurations setting not found."""

    message_template: ClassVar[str] = (
        'Database vendor "{0}" does not support setting "{1}"'
    )

    def __init__(self, vendor: str, setting_name: str) -> None:
        """Format and set message from args and ``message_template``."""
        super().__init__(self.message_template.format(vendor, setting_name))
