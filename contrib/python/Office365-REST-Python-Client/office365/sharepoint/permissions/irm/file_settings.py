from typing import Optional

from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class InformationRightsManagementFileSettings(Entity):
    """Represents the Information Rights Management (IRM) settings of a file."""

    def reset(self):
        """Resets all properties to the default value."""
        qry = ServiceOperationQuery(self, "Reset")
        self.context.add_query(qry)
        return self

    @property
    def allow_print(self):
        # type: () -> Optional[bool]
        """
        Gets a value indicating whether or not the user can print the downloaded document.
        True if print is allowed; otherwise, it is false. The default value is false.
        """
        return self.properties.get("AllowPrint", None)

    @allow_print.setter
    def allow_print(self, value):
        # type: (bool) -> None
        """
        Sets a value indicating whether or not the user can print the downloaded document.
        """
        self.set_property("AllowPrint", value)

    @property
    def allow_script(self):
        # type: () -> Optional[bool]
        """
        Gets a value indicating whether or not the user can run a script on the downloaded document.
        True if the script is allowed to run; otherwise, it is false. The default value is false.
        """
        return self.properties.get("AllowScript", None)

    @allow_script.setter
    def allow_script(self, value):
        # type: (bool) -> None
        """
        Sets a value indicating whether or not the user can run a script on the downloaded document.
        """
        self.set_property("AllowPrint", value)

    @property
    def allow_write_copy(self):
        # type: () -> Optional[bool]
        """
        Getsa value indicating whether or not the user can write on a copy of the downloaded document.
        True if write on a copy is allowed; otherwise, it is false. The default value is false.
        """
        return self.properties.get("AllowWriteCopy", None)
