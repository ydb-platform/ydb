from typing import TYPE_CHECKING

from defusedxml.ElementTree import fromstring

from .property_decorators import property_is_boolean
from .target import Target
from tableauserverclient.models import ScheduleItem

if TYPE_CHECKING:
    from .target import Target


class SubscriptionItem:
    def __init__(self, subject: str, schedule_id: str, user_id: str, target: "Target") -> None:
        self._id = None
        self.attach_image = True
        self.attach_pdf = False
        self.message = None
        self.page_orientation = None
        self.page_size_option = None
        self.schedule_id = schedule_id
        self.send_if_view_empty = True
        self.subject = subject
        self.suspended = False
        self.target = target
        self.user_id = user_id
        self.schedule = None

    def __repr__(self) -> str:
        if self.id is not None:
            return "<Subscription#{_id} subject({subject}) schedule_id({schedule_id}) user_id({user_id}) \
                target({target})".format(
                **self.__dict__
            )
        else:
            return "<Subscription subject({subject}) schedule_id({schedule_id}) user_id({user_id}) \
                target({target})".format(
                **self.__dict__
            )

    @property
    def id(self):
        return self._id

    @property
    def attach_image(self) -> bool:
        return self._attach_image

    @attach_image.setter
    @property_is_boolean
    def attach_image(self, value: bool):
        self._attach_image = value

    @property
    def attach_pdf(self) -> bool:
        return self._attach_pdf

    @attach_pdf.setter
    @property_is_boolean
    def attach_pdf(self, value: bool) -> None:
        self._attach_pdf = value

    @property
    def send_if_view_empty(self) -> bool:
        return self._send_if_view_empty

    @send_if_view_empty.setter
    @property_is_boolean
    def send_if_view_empty(self, value: bool) -> None:
        self._send_if_view_empty = value

    @property
    def suspended(self) -> bool:
        return self._suspended

    @suspended.setter
    @property_is_boolean
    def suspended(self, value: bool) -> None:
        self._suspended = value

    @classmethod
    def from_response(cls: type, xml: bytes, ns) -> list["SubscriptionItem"]:
        parsed_response = fromstring(xml)
        all_subscriptions_xml = parsed_response.findall(".//t:subscription", namespaces=ns)

        all_subscriptions = [SubscriptionItem._parse_element(x, ns) for x in all_subscriptions_xml]
        return all_subscriptions

    @classmethod
    def _parse_element(cls, element, ns):
        schedule_element = element.find(".//t:schedule", namespaces=ns)
        content_element = element.find(".//t:content", namespaces=ns)
        user_element = element.find(".//t:user", namespaces=ns)

        # Schedule element
        schedule_id = None
        schedule = None
        if schedule_element is not None:
            schedule_id = schedule_element.get("id", None)

            # If schedule id is not provided, then TOL with full schedule provided
            if schedule_id is None:
                schedule = ScheduleItem.from_element(element, ns)

        # Content element
        target = None
        send_if_view_empty = None
        if content_element is not None:
            target = Target(content_element.get("id", None), content_element.get("type"))
            send_if_view_empty = string_to_bool(content_element.get("sendIfViewEmpty", ""))

        # User element
        user_id = None
        if user_element is not None:
            user_id = user_element.get("id", None)

        # Main attributes
        id_ = element.get("id", None)
        subject = element.get("subject", None)
        attach_image = string_to_bool(element.get("attachImage", ""))
        attach_pdf = string_to_bool(element.get("attachPdf", ""))
        message = element.get("message", None)
        page_orientation = element.get("pageOrientation", None)
        page_size_option = element.get("pageSizeOption", None)
        suspended = string_to_bool(element.get("suspended", ""))

        # Create SubscriptionItem and set fields
        sub = cls(subject, schedule_id, user_id, target)
        sub._id = id_
        sub.attach_image = attach_image
        sub.attach_pdf = attach_pdf
        sub.message = message
        sub.page_orientation = page_orientation
        sub.page_size_option = page_size_option
        sub.send_if_view_empty = send_if_view_empty
        sub.suspended = suspended
        sub.schedule = schedule

        return sub


# Used to convert string represented boolean to a boolean type
def string_to_bool(s: str) -> bool:
    return s.lower() == "true"
