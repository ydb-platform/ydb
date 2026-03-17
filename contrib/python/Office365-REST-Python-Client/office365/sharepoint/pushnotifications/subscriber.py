import datetime
from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class PushNotificationSubscriber(Entity):
    """Represents a push notification subscriber over a site."""

    @property
    def custom_args(self):
        # type: () -> Optional[str]
        """Gets the custom arguments specified by the app."""
        return self.properties.get("CustomArgs", None)

    @property
    def service_token(self):
        # type: () -> Optional[str]
        """Specifies the delivery channel URI for push notifications. It must not be null. It must not be empty."""
        return self.properties.get("ServiceToken", None)

    @service_token.setter
    def service_token(self, value):
        # type: (str) -> None
        """Specifies the delivery channel URI for push notifications. It must not be null. It must not be empty."""
        self.set_property("ServiceToken", value)

    @property
    def device_app_instance_id(self):
        # type: () -> Optional[str]
        """Specifies a device app instance identifier."""
        return self.properties.get("DeviceAppInstanceId", None)

    @property
    def last_modified_time_stamp(self):
        # type: () -> Optional[datetime.datetime]
        """Specifies the time and date when the subscriber was last updated."""
        return self.properties.get("LastModifiedTimeStamp", datetime.datetime.min)

    @property
    def registration_time_stamp(self):
        # type: () -> Optional[datetime.datetime]
        """Specifies the time and date when the subscriber registered for push notifications."""
        return self.properties.get("RegistrationTimeStamp", datetime.datetime.min)

    @property
    def user(self):
        """Gets the SharePoint user who created this subscriber."""
        from office365.sharepoint.principal.users.user import User

        return self.properties.get(
            "User", User(self.context, ResourcePath("user", self.resource_path))
        )
