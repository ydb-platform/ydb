from office365.outlook.calendar.sharing.message_action import (
    CalendarSharingMessageAction,
)
from office365.outlook.mail.messages.message import Message


class CalendarSharingMessage(Message):
    """"""

    @property
    def sharing_message_action(self):
        """"""
        return self.properties.setdefault(
            "sharingMessageAction", CalendarSharingMessageAction()
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_type_mapping = {
                "sharingMessageAction": self.sharing_message_action,
            }
            default_value = property_type_mapping.get(name, None)

        return super(CalendarSharingMessage, self).get_property(name, default_value)
