from datetime import datetime
from typing import List, Optional

from office365.booking.appointment import BookingAppointment
from office365.booking.custom_question import BookingCustomQuestion
from office365.booking.customers.base import BookingCustomerBase
from office365.booking.service import BookingService
from office365.booking.staff.availability_item import StaffAvailabilityItem
from office365.booking.staff.member_base import BookingStaffMemberBase
from office365.booking.work_hours import BookingWorkHours
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.outlook.mail.physical_address import PhysicalAddress
from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery


class BookingBusiness(Entity):
    """Represents a business in Microsoft Bookings. This is the top level object in the Microsoft Bookings API.
    It contains business information and related business objects such as appointments, customers, services,
    and staff members."""

    def get_staff_availability(
        self, staff_ids=None, start_datetime=None, end_datetime=None
    ):
        # type: (List[str], datetime, datetime) -> ClientResult[ClientValueCollection[StaffAvailabilityItem]]
        """
        Get the availability information of staff members of a Microsoft Bookings calendar.
        :param list[str] staff_ids: The list of staff IDs
        :param datetime.datetime start_datetime:
        :param datetime.datetime end_datetime:
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(StaffAvailabilityItem)
        )
        payload = {
            "staffIds": staff_ids,
            "startDateTime": start_datetime,
            "endDateTime": end_datetime,
        }
        qry = ServiceOperationQuery(
            self, "getStaffAvailability", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def publish(self):
        """
        Make the scheduling page of a business available to external customers.

        Set the isPublished property to true, and the publicUrl property to the URL of the scheduling page.
        """
        qry = ServiceOperationQuery(
            self,
            "publish",
        )
        self.context.add_query(qry)
        return self

    @property
    def address(self):
        """
        The street address of the business. The address property, together with phone and webSiteUrl, appear in the
        footer of a business scheduling page. The attribute type of physicalAddress is not supported in v1.0.
        Internally we map the addresses to the type others.
        """
        return self.properties.get("address", PhysicalAddress())

    @property
    def business_hours(self):
        # type: () -> ClientValueCollection[BookingWorkHours]
        """The hours of operation for the business."""
        return self.properties.get(
            "businessHours", ClientValueCollection(BookingWorkHours)
        )

    def display_name(self):
        # type: () -> Optional[str]
        """
        The name of the business, which interfaces with customers. This name appears at the top of the business
        scheduling page.
        """
        return self.properties.get("displayName", None)

    @property
    def appointments(self):
        # type: () -> EntityCollection[BookingAppointment]
        """All the appointments of this business. Read-only. Nullable."""
        return self.properties.get(
            "appointments",
            EntityCollection(
                self.context,
                BookingAppointment,
                ResourcePath("appointments", self.resource_path),
            ),
        )

    @property
    def calendar_view(self):
        # type: () -> EntityCollection[BookingAppointment]
        """The set of appointments of this business in a specified date range. Read-only. Nullable."""
        return self.properties.get(
            "calendarView",
            EntityCollection(
                self.context,
                BookingAppointment,
                ResourcePath("calendarView", self.resource_path),
            ),
        )

    @property
    def customers(self):
        # type: () -> EntityCollection[BookingCustomerBase]
        """All the customers of this business. Read-only. Nullable."""
        return self.properties.get(
            "customers",
            EntityCollection(
                self.context,
                BookingCustomerBase,
                ResourcePath("customers", self.resource_path),
            ),
        )

    @property
    def custom_questions(self):
        """All the services offered by this business. Read-only. Nullable."""
        return self.properties.get(
            "customQuestions",
            EntityCollection(
                self.context,
                BookingCustomQuestion,
                ResourcePath("customQuestions", self.resource_path),
            ),
        )

    @property
    def services(self):
        """All the services offered by this business. Read-only. Nullable."""
        return self.properties.get(
            "services",
            EntityCollection(
                self.context,
                BookingService,
                ResourcePath("services", self.resource_path),
            ),
        )

    @property
    def staff_members(self):
        """The collection of open extensions defined for the message. Nullable."""
        return self.properties.get(
            "staffMembers",
            EntityCollection(
                self.context,
                BookingStaffMemberBase,
                ResourcePath("staffMembers", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "businessHours": self.business_hours,
                "calendarView": self.calendar_view,
                "customQuestions": self.custom_questions,
                "staffMembers": self.staff_members,
            }
            default_value = property_mapping.get(name, None)
        return super(BookingBusiness, self).get_property(name, default_value)
