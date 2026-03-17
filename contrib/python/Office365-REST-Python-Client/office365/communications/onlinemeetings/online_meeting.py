from datetime import datetime
from typing import AnyStr, Optional

from office365.communications.onlinemeetings.participants import MeetingParticipants
from office365.communications.onlinemeetings.recordings.call import CallRecording
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.outlook.mail.item_body import ItemBody
from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.types.collections import StringCollection


class OnlineMeeting(Entity):
    """
    Contains information about a meeting, including the URL used to join a meeting,
    the attendees list, and the description.
    """

    def get_virtual_appointment_join_web_url(self):
        """Get a join web URL for a Microsoft Virtual Appointment. This web URL includes enhanced
        business-to-customer experiences such as mobile browser join and virtual lobby rooms.
        With Teams Premium, you can configure a custom lobby room experience for attendees by adding your company
        logo and access the Virtual Appointments usage report for organizational analytics.
        """
        return_type = ClientResult(self.context, str())
        qry = FunctionQuery(self, "getVirtualAppointmentJoinWebUrl", None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def allow_attendee_to_enable_camera(self):
        # type: () -> Optional[bool]
        """Indicates whether attendees can turn on their camera."""
        return self.properties.get("allowAttendeeToEnableCamera", None)

    @property
    def allow_attendee_to_enable_mic(self):
        # type: () -> Optional[bool]
        """Indicates whether attendees can turn on their microphone."""
        return self.properties.get("allowAttendeeToEnableMic", None)

    @property
    def allowed_presenters(self):
        """Specifies who can be a presenter in a meeting. Possible values are listed in the following table."""
        return self.properties.get("allowedPresenters", StringCollection())

    @property
    def allow_meeting_chat(self):
        # type: () -> Optional[bool]
        """Specifies the mode of meeting chat."""
        return self.properties.get("allowMeetingChat", None)

    @property
    def allow_participants_to_change_name(self):
        # type: () -> Optional[bool]
        """Specifies if participants are allowed to rename themselves in an instance of the meeting."""
        return self.properties.get("allowParticipantsToChangeName", None)

    @property
    def attendee_report(self):
        # type: () -> Optional[AnyStr]
        """The content stream of the attendee report of a Microsoft Teams live event."""
        return self.properties.get("attendeeReport", None)

    @property
    def participants(self):
        """
        The participants associated with the online meeting. This includes the organizer and the attendees.
        """
        return self.properties.get("participants", MeetingParticipants())

    @property
    def subject(self):
        # type: () -> Optional[str]
        """The subject of the online meeting."""
        return self.properties.get("subject", None)

    @subject.setter
    def subject(self, value):
        # type: (str) -> None
        self.set_property("subject", value)

    @property
    def start_datetime(self):
        """Gets the meeting start time in UTC."""
        return self.properties.get("startDateTime", datetime.min)

    @start_datetime.setter
    def start_datetime(self, value):
        # type: (datetime) -> None
        """Sets the meeting start time in UTC."""
        self.set_property("startDateTime", value.isoformat())

    @property
    def end_datetime(self):
        """Gets the meeting end time in UTC."""
        return self.properties.get("endDateTime", datetime.min)

    @end_datetime.setter
    def end_datetime(self, value):
        # type: (datetime) -> None
        """Sets the meeting end time in UTC."""
        self.set_property("endDateTime", value.isoformat())

    @property
    def join_information(self):
        """The join URL of the online meeting. Read-only."""
        return self.properties.get("joinInformation", ItemBody())

    @property
    def join_web_url(self):
        # type: () -> Optional[str]
        """The join URL of the online meeting. Read-only."""
        return self.properties.get("joinWebUrl", None)

    @property
    def video_teleconference_id(self):
        # type: () -> Optional[str]
        """The video teleconferencing ID."""
        return self.properties.get("videoTeleconferenceId", None)

    @property
    def recordings(self):
        # type: () -> EntityCollection[CallRecording]
        """The recordings of an online meeting"""
        return self.properties.get(
            "recordings",
            EntityCollection(
                self.context,
                CallRecording,
                ResourcePath("recordings", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "endDateTime": self.end_datetime,
                "joinInformation": self.join_information,
                "startDateTime": self.start_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(OnlineMeeting, self).get_property(name, default_value)
