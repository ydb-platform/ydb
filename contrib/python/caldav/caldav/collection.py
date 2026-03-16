"""
I'm trying to be consistent with the terminology in the RFCs:

CalendarSet is a collection of Calendars
Calendar is a collection of CalendarObjectResources
Principal is not a collection, but holds a CalendarSet.

There are also some Mailbox classes to deal with RFC6638.

A SynchronizableCalendarObjectCollection contains a local copy of objects from a calendar on the server.
"""
import logging
import sys
import uuid
import warnings
from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union
from urllib.parse import ParseResult
from urllib.parse import quote
from urllib.parse import SplitResult
from urllib.parse import unquote

import icalendar
from icalendar.caselessdict import CaselessDict

try:
    from typing import ClassVar, Optional, Union, Type

    TimeStamp = Optional[Union[date, datetime]]
except:
    pass

if TYPE_CHECKING:
    from icalendar import vCalAddress

    from .davclient import DAVClient

if sys.version_info < (3, 9):
    from typing import Callable, Container, Iterable, Iterator, Sequence

    from typing_extensions import DefaultDict, Literal
else:
    from collections import defaultdict as DefaultDict
    from collections.abc import Callable, Container, Iterable, Iterator, Sequence
    from typing import Literal

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self

from .calendarobjectresource import CalendarObjectResource
from .calendarobjectresource import Event
from .calendarobjectresource import FreeBusy
from .calendarobjectresource import Journal
from .calendarobjectresource import Todo
from .davobject import DAVObject
from .elements.cdav import CalendarData
from .elements import cdav
from .elements import dav
from .lib import error
from .lib import vcal
from .lib.python_utilities import to_wire
from .lib.url import URL

_CC = TypeVar("_CC", bound="CalendarObjectResource")
log = logging.getLogger("caldav")


class CalendarSet(DAVObject):
    """
    A CalendarSet is a set of calendars.
    """

    def calendars(self) -> List["Calendar"]:
        """
        List all calendar collections in this set.

        Returns:
         * [Calendar(), ...]
        """
        cals = []

        data = self.children(cdav.Calendar.tag)
        for c_url, c_type, c_name in data:
            try:
                cal_id = c_url.split("/")[-2]
                if not cal_id:
                    continue
            except:
                log.error(f"Calendar {c_name} has unexpected url {c_url}")
                cal_id = None
            cals.append(
                Calendar(self.client, id=cal_id, url=c_url, parent=self, name=c_name)
            )

        return cals

    def make_calendar(
        self,
        name: Optional[str] = None,
        cal_id: Optional[str] = None,
        supported_calendar_component_set: Optional[Any] = None,
        method=None,
    ) -> "Calendar":
        """
        Utility method for creating a new calendar.

        Args:
          name: the display name of the new calendar
          cal_id: the uuid of the new calendar
          supported_calendar_component_set: what kind of objects
           (EVENT, VTODO, VFREEBUSY, VJOURNAL) the calendar should handle.
           Should be set to ['VTODO'] when creating a task list in Zimbra -
           in most other cases the default will be OK.

        Returns:
          Calendar(...)-object
        """
        return Calendar(
            self.client,
            name=name,
            parent=self,
            id=cal_id,
            supported_calendar_component_set=supported_calendar_component_set,
        ).save(method=method)

    def calendar(
        self, name: Optional[str] = None, cal_id: Optional[str] = None
    ) -> "Calendar":
        """
        The calendar method will return a calendar object.  If it gets a cal_id
        but no name, it will not initiate any communication with the server

        Args:
          name: return the calendar with this display name
          cal_id: return the calendar with this calendar id or URL

        Returns:
          Calendar(...)-object
        """
        if name and not cal_id:
            for calendar in self.calendars():
                display_name = calendar.get_display_name()
                if display_name == name:
                    return calendar
        if name and not cal_id:
            raise error.NotFoundError(
                "No calendar with name %s found under %s" % (name, self.url)
            )
        if not cal_id and not name:
            cals = self.calendars()
            if not cals:
                raise error.NotFoundError("no calendars found")
            return cals[0]

        if self.client is None:
            raise ValueError("Unexpected value None for self.client")

        if cal_id is None:
            raise ValueError("Unexpected value None for cal_id")

        if str(URL.objectify(cal_id).canonical()).startswith(
            str(self.client.url.canonical())
        ):
            url = self.client.url.join(cal_id)
        elif isinstance(cal_id, URL) or (
            isinstance(cal_id, str)
            and (cal_id.startswith("https://") or cal_id.startswith("http://"))
        ):
            if self.url is None:
                raise ValueError("Unexpected value None for self.url")

            url = self.url.join(cal_id)
        else:
            if self.url is None:
                raise ValueError("Unexpected value None for self.url")

            if cal_id is None:
                raise ValueError("Unexpected value None for cal_id")

            url = self.url.join(quote(cal_id) + "/")

        return Calendar(self.client, name=name, parent=self, url=url, id=cal_id)


class Principal(DAVObject):
    """
    This class represents a DAV Principal. It doesn't do much, except
    keep track of the URLs for the calendar-home-set, etc.

    A principal MUST have a non-empty DAV:displayname property
    (defined in Section 13.2 of [RFC2518]),
    and a DAV:resourcetype property (defined in Section 13.9 of [RFC2518]).
    Additionally, a principal MUST report the DAV:principal XML element
    in the value of the DAV:resourcetype property.

    (TODO: the resourcetype is actually never checked, and the DisplayName
    is not stored anywhere)
    """

    def __init__(
        self,
        client: Optional["DAVClient"] = None,
        url: Union[str, ParseResult, SplitResult, URL, None] = None,
        calendar_home_set: URL = None,
        **kwargs,  ## to be passed to super.__init__
    ) -> None:
        """
        Returns a Principal.

        End-users usually shouldn't need to construct Principal-objects directly.  Use davclient.principal() to get the principal object of the logged-in user  and davclient.principals() to get other principals.

        Args:
          client: a DAVClient() object
          url: The URL, if known.
          calendar_home_set: the calendar home set, if known

        If url is not given, deduct principal path as well as calendar home set
        path from doing propfinds.
        """
        self._calendar_home_set = calendar_home_set

        super(Principal, self).__init__(client=client, url=url, **kwargs)
        if url is None:
            if self.client is None:
                raise ValueError("Unexpected value None for self.client")

            self.url = self.client.url
            cup = self.get_property(dav.CurrentUserPrincipal())

            if cup is None:
                log.warning("calendar server lacking a feature:")
                log.warning("current-user-principal property not found")
                log.warning("assuming %s is the principal URL" % self.client.url)

            self.url = self.client.url.join(URL.objectify(cup))

    def make_calendar(
        self,
        name: Optional[str] = None,
        cal_id: Optional[str] = None,
        supported_calendar_component_set: Optional[Any] = None,
        method=None,
    ) -> "Calendar":
        """
        Convenience method, bypasses the self.calendar_home_set object.
        See CalendarSet.make_calendar for details.
        """
        return self.calendar_home_set.make_calendar(
            name,
            cal_id,
            supported_calendar_component_set=supported_calendar_component_set,
            method=method,
        )

    def calendar(
        self,
        name: Optional[str] = None,
        cal_id: Optional[str] = None,
        cal_url: Optional[str] = None,
    ) -> "Calendar":
        """
        The calendar method will return a calendar object.
        It will not initiate any communication with the server.
        """
        if not cal_url:
            return self.calendar_home_set.calendar(name, cal_id)
        else:
            if self.client is None:
                raise ValueError("Unexpected value None for self.client")

            return Calendar(self.client, url=self.client.url.join(cal_url))

    def get_vcal_address(self) -> "vCalAddress":
        """
        Returns the principal, as an icalendar.vCalAddress object.
        """
        from icalendar import vCalAddress, vText

        cn = self.get_display_name()
        ids = self.calendar_user_address_set()
        cutype = self.get_property(cdav.CalendarUserType())
        ret = vCalAddress(ids[0])
        ret.params["cn"] = vText(cn)
        ret.params["cutype"] = vText(cutype)
        return ret

    @property
    def calendar_home_set(self):
        if not self._calendar_home_set:
            calendar_home_set_url = self.get_property(cdav.CalendarHomeSet())
            ## owncloud returns /remote.php/dav/calendars/tobixen@e.email/
            ## in that case the @ should be quoted.  Perhaps other
            ## implementations return already quoted URLs.  Hacky workaround:
            if (
                calendar_home_set_url is not None
                and "@" in calendar_home_set_url
                and "://" not in calendar_home_set_url
            ):
                calendar_home_set_url = quote(calendar_home_set_url)
            self.calendar_home_set = calendar_home_set_url
        return self._calendar_home_set

    @calendar_home_set.setter
    def calendar_home_set(self, url) -> None:
        if isinstance(url, CalendarSet):
            self._calendar_home_set = url
            return
        sanitized_url = URL.objectify(url)
        ## TODO: sanitized_url should never be None, this needs more
        ## research.  added here as it solves real-world issues, ref
        ## https://github.com/python-caldav/caldav/pull/56
        if sanitized_url is not None:
            if (
                sanitized_url.hostname
                and sanitized_url.hostname != self.client.url.hostname
            ):
                # icloud (and others?) having a load balanced system,
                # where each principal resides on one named host
                ## TODO:
                ## Here be dragons.  sanitized_url will be the root
                ## of all future objects derived from client.  Changing
                ## the client.url root by doing a principal.calendars()
                ## is an unacceptable side effect and may be a cause of
                ## incompatibilities with icloud.  Do more research!
                self.client.url = sanitized_url
        self._calendar_home_set = CalendarSet(
            self.client, self.client.url.join(sanitized_url)
        )

    def calendars(self) -> List["Calendar"]:
        """
        Return the principal's calendars.
        """
        return self.calendar_home_set.calendars()

    def freebusy_request(self, dtstart, dtend, attendees):
        """Sends a freebusy-request for some attendee to the server
        as per RFC6638.
        """

        freebusy_ical = icalendar.Calendar()
        freebusy_ical.add("prodid", "-//tobixen/python-caldav//EN")
        freebusy_ical.add("version", "2.0")
        freebusy_ical.add("method", "REQUEST")
        uid = uuid.uuid1()
        freebusy_comp = icalendar.FreeBusy()
        freebusy_comp.add("uid", uid)
        freebusy_comp.add("dtstamp", datetime.now())
        freebusy_comp.add("dtstart", dtstart)
        freebusy_comp.add("dtend", dtend)
        freebusy_ical.add_component(freebusy_comp)
        outbox = self.schedule_outbox()
        caldavobj = FreeBusy(data=freebusy_ical, parent=outbox)
        caldavobj.add_organizer()
        for attendee in attendees:
            caldavobj.add_attendee(attendee, no_default_parameters=True)

        response = self.client.post(
            outbox.url,
            caldavobj.data,
            headers={"Content-Type": "text/calendar; charset=utf-8"},
        )
        return response.find_objects_and_props()

    def calendar_user_address_set(self) -> List[Optional[str]]:
        """
        defined in RFC6638
        """
        _addresses: Optional[_Element] = self.get_property(
            cdav.CalendarUserAddressSet(), parse_props=False
        )

        if _addresses is None:
            raise error.NotFoundError("No calendar user addresses given from server")

        assert not [x for x in _addresses if x.tag != dav.Href().tag]
        addresses = list(_addresses)
        ## possibly the preferred attribute is iCloud-specific.
        ## TODO: do more research on that
        addresses.sort(key=lambda x: -int(x.get("preferred", 0)))
        return [x.text for x in addresses]

    def schedule_inbox(self) -> "ScheduleInbox":
        """
        Returns the schedule inbox, as defined in RFC6638
        """
        return ScheduleInbox(principal=self)

    def schedule_outbox(self) -> "ScheduleOutbox":
        """
        Returns the schedule outbox, as defined in RFC6638
        """
        return ScheduleOutbox(principal=self)


class Calendar(DAVObject):
    """
    The `Calendar` object is used to represent a calendar collection.
    Refer to the RFC for details:
    https://tools.ietf.org/html/rfc4791#section-5.3.1
    """

    def _create(
        self, name=None, id=None, supported_calendar_component_set=None, method=None
    ) -> None:
        """
        Create a new calendar with display name `name` in `parent`.
        """
        if id is None:
            id = str(uuid.uuid1())
        self.id = id

        if method is None:
            if self.client:
                supported = self.client.features.is_supported(
                    "create-calendar", return_type=dict
                )
                if supported["support"] not in ("full", "fragile", "quirk"):
                    raise error.MkcalendarError(
                        "Creation of calendars (allegedly) not supported on this server"
                    )
                if (
                    supported["support"] == "quirk"
                    and supported["behaviour"] == "mkcol-required"
                ):
                    method = "mkcol"
                else:
                    method = "mkcalendar"
            else:
                method = "mkcalendar"

        path = self.parent.url.join(id + "/")
        self.url = path

        # TODO: mkcalendar seems to ignore the body on most servers?
        # at least the name doesn't get set this way.
        # zimbra gives 500 (!) if body is omitted ...

        prop = dav.Prop()
        if name:
            display_name = dav.DisplayName(name)
            prop += [
                display_name,
            ]
        if supported_calendar_component_set:
            sccs = cdav.SupportedCalendarComponentSet()
            for scc in supported_calendar_component_set:
                sccs += cdav.Comp(scc)
            prop += sccs
        if method == "mkcol":
            from caldav.lib.debug import printxml

            prop += dav.ResourceType() + [dav.Collection(), cdav.Calendar()]

        set = dav.Set() + prop

        mkcol = (dav.Mkcol() if method == "mkcol" else cdav.Mkcalendar()) + set

        r = self._query(
            root=mkcol, query_method=method, url=path, expected_return_value=201
        )

        # COMPATIBILITY ISSUE
        # name should already be set, but we've seen caldav servers failing
        # on setting the DisplayName on calendar creation
        # (DAViCal, Zimbra, ...).  Doing an attempt on explicitly setting the
        # display name using PROPPATCH.
        if name:
            try:
                self.set_properties([display_name])
            except Exception as e:
                ## TODO: investigate.  Those asserts break.
                try:
                    current_display_name = self.get_display_name()
                    error.assert_(current_display_name == name)
                except:
                    log.warning(
                        "calendar server does not support display name on calendar?  Ignoring",
                        exc_info=True,
                    )

    def delete(self):
        ## TODO: remove quirk handling from the functional tests
        ## TODO: this needs test code
        quirk_info = self.client.features.is_supported("delete-calendar", dict)
        wipe = quirk_info["support"] in ("unsupported", "fragile")
        if quirk_info["support"] == "fragile":
            ## Do some retries on deleting the calendar
            for x in range(0, 20):
                try:
                    super().delete()
                except error.DeleteError:
                    pass
                try:
                    x = self.events()
                    sleep(0.3)
                except error.NotFoundError:
                    wipe = False
                    break

        if wipe:
            for x in self.search():
                x.delete()
        else:
            super().delete()

    def get_supported_components(self) -> List[Any]:
        """
        returns a list of component types supported by the calendar, in
        string format (typically ['VJOURNAL', 'VTODO', 'VEVENT'])
        """
        if self.url is None:
            raise ValueError("Unexpected value None for self.url")

        props = [cdav.SupportedCalendarComponentSet()]
        response = self.get_properties(props, parse_response_xml=False)
        response_list = response.find_objects_and_props()
        prop = response_list[unquote(self.url.path)][
            cdav.SupportedCalendarComponentSet().tag
        ]
        return [supported.get("name") for supported in prop]

    def save_with_invites(self, ical: str, attendees, **attendeeoptions) -> None:
        """
        sends a schedule request to the server.  Equivalent with save_event, save_todo, etc,
        but the attendees will be added to the ical object before sending it to the server.
        """
        ## TODO: consolidate together with save_*
        obj = self._calendar_comp_class_by_data(ical)(data=ical, client=self.client)
        obj.parent = self
        obj.add_organizer()
        for attendee in attendees:
            obj.add_attendee(attendee, **attendeeoptions)
        obj.id = obj.icalendar_instance.walk("vevent")[0]["uid"]
        obj.save()
        return obj

    def _use_or_create_ics(self, ical, objtype, **ical_data):
        if ical_data or (
            (isinstance(ical, str) or isinstance(ical, bytes))
            and b"BEGIN:VCALENDAR" not in to_wire(ical)
        ):
            ## TODO: the ical_fragment code is not much tested
            if ical and "ical_fragment" not in ical_data:
                ical_data["ical_fragment"] = ical
            return vcal.create_ical(objtype=objtype, **ical_data)
        return ical

    def save_object(
        self,
        ## TODO: this should be made optional.  The class may be given in the ical object.
        ## TODO: also, accept a string.
        objclass: Type[DAVObject],
        ## TODO: ical may also be a vobject or icalendar instance
        ical: Optional[str] = None,
        no_overwrite: bool = False,
        no_create: bool = False,
        **ical_data,
    ) -> "CalendarResourceObject":
        """Add a new event to the calendar, with the given ical.

        Args:
          objclass: Event, Journal or Todo
          ical: ical object (text, icalendar or vobject instance)
          no_overwrite: existing calendar objects should not be overwritten
          no_create: don't create a new object, existing calendar objects should be updated
          dt_start: properties to be inserted into the icalendar object
        , dt_end: properties to be inserted into the icalendar object
          summary: properties to be inserted into the icalendar object
          alarm_trigger: when given, one alarm will be added
          alarm_action: when given, one alarm will be added
          alarm_attach: when given, one alarm will be added

        Note that the list of parameters going into the icalendar
        object and alamrs is not complete.  Refer to the RFC or the
        icalendar library for a full list of properties.
        """
        o = objclass(
            self.client,
            data=self._use_or_create_ics(
                ical, objtype=f"V{objclass.__name__.upper()}", **ical_data
            ),
            parent=self,
        )
        o = o.save(no_overwrite=no_overwrite, no_create=no_create)
        ## TODO: Saving nothing is currently giving an object with None as URL.
        ## This should probably be changed in some future version to raise an error
        ## See also CalendarObjectResource.save()
        if o.url is not None:
            o._handle_reverse_relations(fix=True)
        return o

    ## TODO: maybe we should deprecate those three
    def save_event(self, *largs, **kwargs) -> "Event":
        """
        Returns ``self.save_object(Event, ...)`` - see :class:`save_object`
        """
        return self.save_object(Event, *largs, **kwargs)

    def save_todo(self, *largs, **kwargs) -> "Todo":
        """
        Returns ``self.save_object(Todo, ...)`` - so see :class:`save_object`
        """
        return self.save_object(Todo, *largs, **kwargs)

    def save_journal(self, *largs, **kwargs) -> "Journal":
        """
        Returns ``self.save_object(Journal, ...)`` - so see :class:`save_object`
        """
        return self.save_object(Journal, *largs, **kwargs)

    ## legacy aliases
    ## TODO: should be deprecated

    ## TODO: think more through this - is `save_foo` better than `add_foo`?
    ## `save_foo` should not be used for updating existing content on the
    ## calendar!
    add_object = save_object
    add_event = save_event
    add_todo = save_todo
    add_journal = save_journal

    def save(self, method=None):
        """
        The save method for a calendar is only used to create it, for now.
        We know we have to create it when we don't have a url.

        Returns:
         * self
        """
        if self.url is None:
            self._create(
                id=self.id, name=self.name, method=method, **self.extra_init_options
            )
        return self

    # def data2object_class

    def _multiget(
        self, event_urls: Iterable[URL], raise_notfound: bool = False
    ) -> Iterable[str]:
        """
        get multiple events' data.
        TODO: Does it overlap the _request_report_build_resultlist method
        """
        if self.url is None:
            raise ValueError("Unexpected value None for self.url")

        rv = []
        prop = dav.Prop() + cdav.CalendarData()
        root = (
            cdav.CalendarMultiGet()
            + prop
            + [dav.Href(value=u.path) for u in event_urls]
        )
        response = self._query(root, 1, "report")
        results = response.expand_simple_props([cdav.CalendarData()])
        if raise_notfound:
            for href in response.statuses:
                status = response.statuses[href]
                if status and "404" in status:
                    raise error.NotFoundError(f"Status {status} in {href}")
        for r in results:
            yield (r, results[r][cdav.CalendarData.tag])

    ## Replace the last lines with
    def multiget(
        self, event_urls: Iterable[URL], raise_notfound: bool = False
    ) -> Iterable[_CC]:
        """
        get multiple events' data
        TODO: Does it overlap the _request_report_build_resultlist method?
        @author mtorange@gmail.com (refactored by Tobias)
        """
        results = self._multiget(event_urls, raise_notfound=raise_notfound)
        for url, data in results:
            yield self._calendar_comp_class_by_data(data)(
                self.client,
                url=self.url.join(url),
                data=data,
                parent=self,
            )

    def calendar_multiget(self, *largs, **kwargs):
        """
        get multiple events' data
        @author mtorange@gmail.com
        (refactored by Tobias)
        This is for backward compatibility.  It may be removed in 3.0 or later release.
        """
        return list(self.multiget(*largs, **kwargs))

    def date_search(
        self,
        start: datetime,
        end: Optional[datetime] = None,
        compfilter: None = "VEVENT",
        expand: Union[bool, Literal["maybe"]] = "maybe",
        verify_expand: bool = False,
    ) -> Sequence["CalendarObjectResource"]:
        # type (TimeStamp, TimeStamp, str, str) -> CalendarObjectResource
        """Deprecated.  Use self.search() instead.

        Search events by date in the calendar.

        Args
         start : defaults to datetime.today().
         end : same as above.
         compfilter : defaults to events only.  Set to None to fetch all calendar components.
         expand : should recurrent events be expanded?  (to preserve backward-compatibility the default "maybe" will be changed into True unless the date_search is open-ended)
         verify_expand : not in use anymore, but kept for backward compatibility

        Returns:
         * [CalendarObjectResource(), ...]

        Recurring events are expanded if they are occurring during the
        specified time frame and if an end timestamp is given.

        Note that this is a deprecated method.  The `search` method is
        nearly equivalent.  Differences: default for ``compfilter`` is
        to search for all objects, default for ``expand`` is
        ``False``, and it has a different default
        ``split_expanded=True``.
        """
        ## date_search will probably disappear in 3.0
        warnings.warn(
            "use `calendar.search rather than `calendar.date_search`",
            DeprecationWarning,
            stacklevel=2,
        )

        if verify_expand:
            logging.warning(
                "verify_expand in date_search does not work anymore, as we're doing client side expansion instead"
            )

        ## for backward compatibility - expand should be false
        ## in an open-ended date search, otherwise true
        if expand == "maybe":
            expand = end

        if compfilter == "VEVENT":
            comp_class = Event
        elif compfilter == "VTODO":
            comp_class = Todo
        else:
            comp_class = None

        objects = self.search(
            start=start,
            end=end,
            comp_class=comp_class,
            expand=expand,
            split_expanded=False,
        )

        return objects

    ## TODO: this logic has been partly duplicated in calendar_multiget, but
    ## the code there is much more readable and condensed than this.
    ## Can code below be refactored?
    def _request_report_build_resultlist(
        self, xml, comp_class=None, props=None, no_calendardata=False
    ):
        """
        Takes some input XML, does a report query on a calendar object
        and returns the resource objects found.
        """
        matches = []
        if props is None:
            props_ = [cdav.CalendarData()]
        else:
            props_ = [cdav.CalendarData()] + props
        response = self._query(xml, 1, "report")
        results = response.expand_simple_props(props_)
        for r in results:
            pdata = results[r]
            if cdav.CalendarData.tag in pdata:
                cdata = pdata.pop(cdav.CalendarData.tag)
                comp_class_ = (
                    self._calendar_comp_class_by_data(cdata)
                    if comp_class is None
                    else comp_class
                )
            else:
                cdata = None
            if comp_class_ is None:
                ## no CalendarData fetched - which is normal i.e. when doing a sync-token report and only asking for the URLs
                comp_class_ = CalendarObjectResource
            url = URL(r)
            if url.hostname is None:
                # Quote when result is not a full URL
                url = quote(r)
            ## icloud hack - icloud returns the calendar URL as well as the calendar item URLs
            if self.url.join(url) == self.url:
                continue
            matches.append(
                comp_class_(
                    self.client,
                    url=self.url.join(url),
                    data=cdata,
                    parent=self,
                    props=pdata,
                )
            )
        return (response, matches)

    def search(
        self,
        xml: str = None,
        server_expand: bool = False,
        split_expanded: bool = True,
        sort_reverse: bool = False,
        props: Optional[List[cdav.CalendarData]] = None,
        filters=None,
        post_filter=None,
        _hacks=None,
        **searchargs,
    ) -> List[_CC]:
        """Sends a search request towards the server, processes the
        results if needed and returns the objects found.

        Refactoring 2025-11: a new class
        class:`caldav.search.CalDAVSearcher` has been made, and
        this method is sort of a wrapper for
        CalDAVSearcher.search, ensuring backward
        compatibility.  The documentation may be slightly overlapping.

        I believe that for simple tasks, this method will be easier to
        use than the new interface, hence there are no plans for the
        foreseeable future to deprecate it.  This search method will
        continue working as it has been doing before for all
        foreseeable future.  I believe that for simple tasks, this
        method will be easier to use than to construct a
        CalDAVSearcher object and do searches from there.  The
        refactoring was made necessary because the parameter list to
        `search` was becoming unmanagable.  Advanced searches should
        be done via the new interface.

        Caveat: The searching is done on the server side, the RFC is
        not very crystal clear on many of the corner cases, and
        servers often behave differently when presented with a search
        request.  There is planned work to work around server
        incompatibilities on the client side, but as for now
        complicated searches will give different results on different
        servers.

        ``todo`` - searches explicitly for todo.  Unless
        ``include_completed`` is specified, there is some special
        logic ensuring only pending tasks is returned.

        There is corresponding ``event`` and ``journal`` bools to
        specify that the search should be only for events or journals.
        When neither are set, one should expect to get all objects
        returned - but quite some calendar servers will return
        nothing.  This will be solved client-side in the future, as
        for 2.0 it's recommended to search separately for tasks,
        events and journals to ensure consistent behaviour across
        different calendar servers and providers.

        ``sort_keys`` refers to (case-insensitive) properties in the
        icalendar object, ``sort_reverse`` can also be given.  The
        sorting will be done client-side.

        Use ``start`` and ``end`` for time-range searches.  Open-ended
        searches are supported (i.e. "everything in the future"), but
        it's recommended to use closed ranges (i.e. have an "event
        horizon" of a year and ask for "everything from now and one
        year ahead") and get the data expanded.

        With the boolean ``expand`` set, you don't have to think too
        much about recurrences - they will be expanded, and with the
        (default) ``split_expanded`` set, each recurrence will be
        returned as a separate list object (otherwise all recurrences
        will be put into one ``VCALENDAR`` and returned as one
        ``Event``).  This makes it safe to use the ``event.component``
        property.  The non-expanded resultset may include events where
        the timespan doesn't match the date interval you searched for,
        as well as items with multiple components ("special"
        recurrences), meaning you may need logic on the client side to
        handle the recurrences.  *Only time range searches over closed
        time intervals may be expanded*.

        As for 2.0, the expand-logic is by default done on the
        client-side, for consistent results across various server
        incompabilities.  However, you may force server-side expansion
        by setting ``server_expand=True``

        Text attribute search parameters can be given to query the
        "properties" in the calendar data: category, uid, summary,
        comment, description, location, status.  According to the RFC,
        a substring search should be done.

        You may use no_category, no_summary, etc to search for objects
        that are missing those attributes.

        Negated text matches are not supported yet.

        For power-users, those parameters are also supported:

         * ``xml`` - use this search query, and ignore other filter parameters
         * ``comp_class`` - alternative to the ``event``, ``todo`` or ``journal`` booleans described above.
         * ``filters`` - other kind of filters (in lxml tree format)

        """
        ## Late import to avoid cyclic imports
        from .search import CalDAVSearcher

        ## This is basically a wrapper for CalDAVSearcher.search
        ## The logic below will massage the parameters in ``searchargs``
        ## and put them into the CalDAVSearcher object.

        if searchargs.get("expand", True) not in (True, False):
            warnings.warn(
                "in cal.search(), expand should be a bool",
                DeprecationWarning,
                stacklevel=2,
            )
            if searchargs["expand"] == "client":
                searchargs["expand"] = True
            if searchargs["expand"] == "server":
                server_expand = True
                searchargs["expand"] = False

        ## Transfer all the arguments to CalDAVSearcher
        my_searcher = CalDAVSearcher()
        for key in searchargs:
            assert key[0] != "_"  ## not allowed
            alias = key
            if key == "class_":  ## because class is a reserved word
                alias = "class"
            if key == "no_category":
                alias = "no_categories"
            if key == "no_class_":
                alias = "no_class"
            if key == "sort_keys":
                if isinstance(searchargs["sort_keys"], str):
                    searchargs["sort_keys"] = [searchargs["sort_keys"]]
                for sortkey in searchargs["sort_keys"]:
                    my_searcher.add_sort_key(sortkey, sort_reverse)
                    continue
            elif key == "comp_class" or key in my_searcher.__dataclass_fields__:
                setattr(my_searcher, key, searchargs[key])
                continue
            elif alias.startswith("no_"):
                my_searcher.add_property_filter(
                    alias[3:], searchargs[key], operator="undef"
                )
            else:
                my_searcher.add_property_filter(alias, searchargs[key])

        if not xml and filters:
            xml = filters

        return my_searcher.search(
            self, server_expand, split_expanded, props, xml, post_filter, _hacks
        )

    def freebusy_request(self, start: datetime, end: datetime) -> "FreeBusy":
        """
        Search the calendar, but return only the free/busy information.

        Args:
          start : defaults to datetime.today().
          end : same as above.

        Returns:
          [FreeBusy(), ...]
        """

        root = cdav.FreeBusyQuery() + [cdav.TimeRange(start, end)]
        response = self._query(root, 1, "report")
        return FreeBusy(self, response.raw)

    def todos(
        self,
        sort_keys: Sequence[str] = ("due", "priority"),
        include_completed: bool = False,
        sort_key: Optional[str] = None,
    ) -> List["Todo"]:
        """
        Fetches a list of todo events (this is a wrapper around search).

        Args:
          sort_keys: use this field in the VTODO for sorting (iterable of lower case string, i.e. ('priority','due')).
          include_completed: boolean - by default, only pending tasks are listed
          sort_key: DEPRECATED, for backwards compatibility with version 0.4.
        """
        if sort_key:
            sort_keys = (sort_key,)

        return self.search(
            todo=True, include_completed=include_completed, sort_keys=sort_keys
        )

    def _calendar_comp_class_by_data(self, data):
        """
        takes some data, either as icalendar text or icalender object (TODO:
        consider vobject) and returns the appropriate
        CalendarResourceObject child class.
        """
        if data is None:
            ## no data received - we'd need to load it before we can know what
            ## class it really is.  Assign the base class as for now.
            return CalendarObjectResource
        if hasattr(data, "split"):
            for line in data.split("\n"):
                line = line.strip()
                if line == "BEGIN:VEVENT":
                    return Event
                if line == "BEGIN:VTODO":
                    return Todo
                if line == "BEGIN:VJOURNAL":
                    return Journal
                if line == "BEGIN:VFREEBUSY":
                    return FreeBusy
        elif hasattr(data, "subcomponents"):
            if not len(data.subcomponents):
                return CalendarObjectResource

            ical2caldav = {
                icalendar.Event: Event,
                icalendar.Todo: Todo,
                icalendar.Journal: Journal,
                icalendar.FreeBusy: FreeBusy,
            }
            for sc in data.subcomponents:
                if sc.__class__ in ical2caldav:
                    return ical2caldav[sc.__class__]
        return CalendarObjectResource

    def event_by_url(self, href, data: Optional[Any] = None) -> "Event":
        """
        Returns the event with the given URL.
        """
        return Event(url=href, data=data, parent=self).load()

    def object_by_uid(
        self,
        uid: str,
        comp_filter: Optional[cdav.CompFilter] = None,
        comp_class: Optional["CalendarObjectResource"] = None,
    ) -> "Event":
        """
        Get one event from the calendar.

        Args:
         uid: the event uid
         comp_class: filter by component type (Event, Todo, Journal)
         comp_filter: for backward compatibility.  Don't use!

        Returns:
         Event() or None
        """
        ## late import to avoid cyclic dependencies
        from .search import CalDAVSearcher

        ## 2025-11: some logic validating the comp_filter and
        ## comp_class has been removed, and replaced with the
        ## recommendation not to use comp_filter.  We're still using
        ## comp_filter internally, but it's OK, it doesn't need to be
        ## validated.

        ## Lots of old logic has been removed, the new search logic
        ## can do the things for us:
        searcher = CalDAVSearcher(comp_class=comp_class)
        ## Default is substring
        searcher.add_property_filter("uid", uid, "==")
        items_found = searcher.search(
            self, xml=comp_filter, _hacks="insist", post_filter=True
        )

        if not items_found:
            raise error.NotFoundError("%s not found on server" % uid)
        error.assert_(len(items_found) == 1)
        return items_found[0]

    def todo_by_uid(self, uid: str) -> "CalendarObjectResource":
        """
        Returns the task with the given uid (wraps around :class:`object_by_uid`)
        """
        return self.object_by_uid(uid, comp_filter=cdav.CompFilter("VTODO"))

    def event_by_uid(self, uid: str) -> "CalendarObjectResource":
        """
        Returns the event with the given uid (wraps around :class:`object_by_uid`)
        """
        return self.object_by_uid(uid, comp_filter=cdav.CompFilter("VEVENT"))

    def journal_by_uid(self, uid: str) -> "CalendarObjectResource":
        """
        Returns the journal with the given uid (wraps around :class:`object_by_uid`)
        """
        return self.object_by_uid(uid, comp_filter=cdav.CompFilter("VJOURNAL"))

    # alias for backward compatibility
    event = event_by_uid

    def events(self) -> List["Event"]:
        """
        List all events from the calendar.

        Returns:
         * [Event(), ...]
        """
        return self.search(comp_class=Event)

    def _generate_fake_sync_token(self, objects: List["CalendarObjectResource"]) -> str:
        """
        Generate a fake sync token for servers without sync support.
        Uses a hash of all ETags to detect changes.

        Args:
            objects: List of calendar objects to generate token from

        Returns:
            A fake sync token string
        """
        import hashlib

        etags = []
        for obj in objects:
            if hasattr(obj, "props") and dav.GetEtag.tag in obj.props:
                etags.append(str(obj.props[dav.GetEtag.tag]))
            elif hasattr(obj, "url"):
                ## If no etag, use URL as fallback identifier
                etags.append(str(obj.url.canonical()))
        etags.sort()  ## Consistent ordering
        combined = "|".join(etags)
        hash_value = hashlib.md5(combined.encode()).hexdigest()
        return f"fake-{hash_value}"

    def objects_by_sync_token(
        self,
        sync_token: Optional[Any] = None,
        load_objects: bool = False,
        disable_fallback: bool = False,
    ) -> "SynchronizableCalendarObjectCollection":
        """objects_by_sync_token aka objects

        Do a sync-collection report, ref RFC 6578 and
        https://github.com/python-caldav/caldav/issues/87

        This method will return all objects in the calendar if no
        sync_token is passed (the method should then be referred to as
        "objects"), or if the sync_token is unknown to the server.  If
        a sync-token known by the server is passed, it will return
        objects that are added, deleted or modified since last time
        the sync-token was set.

        If load_objects is set to True, the objects will be loaded -
        otherwise empty CalendarObjectResource objects will be returned.

        This method will return a SynchronizableCalendarObjectCollection object, which is
        an iterable.

        This method transparently falls back to retrieving all objects if the server
        doesn't support sync tokens. The fallback behavior is identical from the user's
        perspective, but less efficient as it transfers the entire calendar on each sync.

        If disable_fallback is set to True, the method will raise an exception instead
        of falling back to retrieving all objects. This is useful for testing whether
        the server truly supports sync tokens.
        """
        ## Check if we should attempt to use sync tokens
        ## (either server supports them, or we haven't checked yet, or this is a fake token)
        use_sync_token = True
        sync_support = self.client.features.is_supported("sync-token", return_type=dict)
        if sync_support.get("support") == "unsupported":
            if disable_fallback:
                raise error.ReportError("Sync tokens are not supported by the server")
            use_sync_token = False
        ## If sync_token looks like a fake token, don't try real sync-collection
        if (
            sync_token
            and isinstance(sync_token, str)
            and sync_token.startswith("fake-")
        ):
            use_sync_token = False

        if use_sync_token:
            try:
                cmd = dav.SyncCollection()
                token = dav.SyncToken(value=sync_token)
                level = dav.SyncLevel(value="1")
                props = dav.Prop() + dav.GetEtag()
                root = cmd + [level, token, props]
                (response, objects) = self._request_report_build_resultlist(
                    root, props=[dav.GetEtag()], no_calendardata=True
                )
                ## TODO: look more into this, I think sync_token should be directly available through response object
                try:
                    sync_token = response.sync_token
                except:
                    sync_token = response.tree.findall(".//" + dav.SyncToken.tag)[
                        0
                    ].text

                ## this is not quite right - the etag we've fetched can already be outdated
                if load_objects:
                    for obj in objects:
                        try:
                            obj.load()
                        except error.NotFoundError:
                            ## The object was deleted
                            pass
                return SynchronizableCalendarObjectCollection(
                    calendar=self, objects=objects, sync_token=sync_token
                )
            except (error.ReportError, error.DAVError) as e:
                ## Server doesn't support sync tokens or the sync-collection REPORT failed
                if disable_fallback:
                    raise
                log.info(
                    f"Sync-collection REPORT failed ({e}), falling back to full retrieval"
                )
                ## Fall through to fallback implementation

        ## FALLBACK: Server doesn't support sync tokens
        ## Retrieve all objects and emulate sync token behavior
        log.debug("Using fallback sync mechanism (retrieving all objects)")

        ## Use search() to get all objects. search() will include CalendarData by default.
        ## We can't avoid this in the fallback mechanism without significant refactoring.
        all_objects = list(self.search())

        ## Load objects if requested (objects may already have data from search)
        if load_objects:
            for obj in all_objects:
                ## Only load if not already loaded
                if not hasattr(obj, "_data") or obj._data is None:
                    try:
                        obj.load()
                    except error.NotFoundError:
                        pass

        ## Fetch ETags for all objects if not already present
        ## ETags are crucial for detecting changes in the fallback mechanism
        if all_objects and (
            not hasattr(all_objects[0], "props")
            or dav.GetEtag.tag not in all_objects[0].props
        ):
            ## Use PROPFIND to fetch ETags for all objects
            try:
                ## Do a depth-1 PROPFIND on the calendar to get all ETags
                response = self._query_properties([dav.GetEtag()], depth=1)
                etag_props = response.expand_simple_props([dav.GetEtag()])

                ## Map ETags to objects by URL (using string keys for reliable comparison)
                url_to_obj = {str(obj.url.canonical()): obj for obj in all_objects}
                log.debug(f"Fallback: Fetching ETags for {len(url_to_obj)} objects")
                for url_str, props in etag_props.items():
                    canonical_url_str = str(self.url.join(url_str).canonical())
                    if canonical_url_str in url_to_obj:
                        if not hasattr(url_to_obj[canonical_url_str], "props"):
                            url_to_obj[canonical_url_str].props = {}
                        url_to_obj[canonical_url_str].props.update(props)
                        log.debug(f"Fallback: Added ETag to {canonical_url_str}")
            except Exception as e:
                ## If fetching ETags fails, we'll fall back to URL-based tokens
                ## which can't detect content changes, only additions/deletions
                log.debug(f"Failed to fetch ETags for fallback sync: {e}")
                pass

        ## Generate a fake sync token based on current state
        fake_sync_token = self._generate_fake_sync_token(all_objects)

        ## If a sync_token was provided, check if anything has changed
        if (
            sync_token
            and isinstance(sync_token, str)
            and sync_token.startswith("fake-")
        ):
            ## Compare the provided token with the new token
            if sync_token == fake_sync_token:
                ## Nothing has changed, return empty collection
                return SynchronizableCalendarObjectCollection(
                    calendar=self, objects=[], sync_token=fake_sync_token
                )
            ## If tokens differ, return all objects (emulating a full sync)
            ## In a real implementation, we'd return only changed objects,
            ## but that requires storing previous state which we don't have

        return SynchronizableCalendarObjectCollection(
            calendar=self, objects=all_objects, sync_token=fake_sync_token
        )

    objects = objects_by_sync_token

    def journals(self) -> List["Journal"]:
        """
        List all journals from the calendar.

        Returns:
         * [Journal(), ...]
        """
        return self.search(comp_class=Journal)


class ScheduleMailbox(Calendar):
    """
    RFC6638 defines an inbox and an outbox for handling event scheduling.

    TODO: As ScheduleMailboxes works a bit like calendars, I've chosen
    to inheritate the Calendar class, but this is a bit incorrect, a
    ScheduleMailbox is a collection, but not really a calendar.  We
    should create a common base class for ScheduleMailbox and Calendar
    eventually.
    """

    def __init__(
        self,
        client: Optional["DAVClient"] = None,
        principal: Optional[Principal] = None,
        url: Union[str, ParseResult, SplitResult, URL, None] = None,
    ) -> None:
        """
        Will locate the mbox if no url is given
        """
        super(ScheduleMailbox, self).__init__(client=client, url=url)
        self._items = None
        if not client and principal:
            self.client = principal.client
        if not principal and client:
            if self.client is None:
                raise ValueError("Unexpected value None for self.client")

            principal = self.client.principal
        if url is not None:
            if client is None:
                raise ValueError("Unexpected value None for client")

            self.url = client.url.join(URL.objectify(url))
        else:
            if principal is None:
                raise ValueError("Unexpected value None for principal")

            if self.client is None:
                raise ValueError("Unexpected value None for self.client")

            self.url = principal.url
            try:
                # we ignore the type here as this is defined in sub-classes only; require more changes to
                # properly fix in a future revision
                self.url = self.client.url.join(URL(self.get_property(self.findprop())))  # type: ignore
            except:
                logging.error("something bad happened", exc_info=True)
                error.assert_(self.client.check_scheduling_support())
                self.url = None
                # we ignore the type here as this is defined in sub-classes only; require more changes to
                # properly fix in a future revision
                raise error.NotFoundError(
                    "principal has no %s.  %s"
                    % (str(self.findprop()), error.ERR_FRAGMENT)  # type: ignore
                )

    def get_items(self):
        """
        TODO: work in progress
        TODO: perhaps this belongs to the super class?
        """
        if not self._items:
            try:
                self._items = self.objects(load_objects=True)
            except:
                logging.debug(
                    "caldav server does not seem to support a sync-token REPORT query on a scheduling mailbox"
                )
                error.assert_("google" in str(self.url))
                self._items = [
                    CalendarObjectResource(url=x[0], client=self.client)
                    for x in self.children()
                ]
                for x in self._items:
                    x.load()
        else:
            try:
                self._items.sync()
            except:
                self._items = [
                    CalendarObjectResource(url=x[0], client=self.client)
                    for x in self.children()
                ]
                for x in self._items:
                    x.load()
        return self._items

    ## TODO: work in progress


#    def get_invites():
#        for item in self.get_items():
#            if item.vobject_instance.vevent.


class ScheduleInbox(ScheduleMailbox):
    findprop = cdav.ScheduleInboxURL


class ScheduleOutbox(ScheduleMailbox):
    findprop = cdav.ScheduleOutboxURL


class SynchronizableCalendarObjectCollection:
    """
    This class may hold a cached snapshot of a calendar, and changes
    in the calendar can easily be copied over through the sync method.

    To create a SynchronizableCalendarObjectCollection object, use
    calendar.objects(load_objects=True)
    """

    def __init__(self, calendar, objects, sync_token) -> None:
        self.calendar = calendar
        self.sync_token = sync_token
        self.objects = objects
        self._objects_by_url = None

    def __iter__(self) -> Iterator[Any]:
        return self.objects.__iter__()

    def __len__(self) -> int:
        return len(self.objects)

    def objects_by_url(self):
        """
        returns a dict of the contents of the SynchronizableCalendarObjectCollection, URLs -> objects.
        """
        if self._objects_by_url is None:
            self._objects_by_url = {}
            for obj in self:
                self._objects_by_url[obj.url.canonical()] = obj
        return self._objects_by_url

    def sync(self) -> Tuple[Any, Any]:
        """
        This method will contact the caldav server,
        request all changes from it, and sync up the collection.

        This method transparently falls back to comparing full calendar state
        if the server doesn't support sync tokens.
        """
        updated_objs = []
        deleted_objs = []

        ## Check if we're using fake sync tokens (fallback mode)
        is_fake_token = isinstance(self.sync_token, str) and self.sync_token.startswith(
            "fake-"
        )

        if not is_fake_token:
            ## Try to use real sync tokens
            try:
                updates = self.calendar.objects_by_sync_token(
                    self.sync_token, load_objects=False
                )

                ## If we got a fake token back, we've fallen back
                if isinstance(
                    updates.sync_token, str
                ) and updates.sync_token.startswith("fake-"):
                    is_fake_token = True
                else:
                    ## Real sync token path
                    obu = self.objects_by_url()
                    for obj in updates:
                        obj.url = obj.url.canonical()
                        if (
                            obj.url in obu
                            and dav.GetEtag.tag in obu[obj.url].props
                            and dav.GetEtag.tag in obj.props
                        ):
                            if (
                                obu[obj.url].props[dav.GetEtag.tag]
                                == obj.props[dav.GetEtag.tag]
                            ):
                                continue
                        obu[obj.url] = obj
                        try:
                            obj.load()
                            updated_objs.append(obj)
                        except error.NotFoundError:
                            deleted_objs.append(obj)
                            obu.pop(obj.url)

                    self.objects = list(obu.values())
                    self._objects_by_url = None  ## Invalidate cache
                    self.sync_token = updates.sync_token
                    return (updated_objs, deleted_objs)
            except (error.ReportError, error.DAVError):
                ## Sync failed, fall back
                is_fake_token = True

        if is_fake_token:
            ## FALLBACK: Compare full calendar state
            log.debug("Using fallback sync mechanism (comparing all objects)")

            ## Retrieve all current objects from server
            current_objects = list(self.calendar.search())

            ## Load them
            for obj in current_objects:
                try:
                    obj.load()
                except error.NotFoundError:
                    pass

            ## Build URL-indexed dicts for comparison
            current_by_url = {obj.url.canonical(): obj for obj in current_objects}
            old_by_url = self.objects_by_url()

            ## Find updated and new objects
            for url, obj in current_by_url.items():
                if url in old_by_url:
                    ## Object exists in both - check if modified
                    ## Compare data if available, otherwise consider it unchanged
                    old_data = (
                        old_by_url[url].data
                        if hasattr(old_by_url[url], "data")
                        else None
                    )
                    new_data = obj.data if hasattr(obj, "data") else None
                    if old_data != new_data and new_data is not None:
                        updated_objs.append(obj)
                else:
                    ## New object
                    updated_objs.append(obj)

            ## Find deleted objects
            for url in old_by_url:
                if url not in current_by_url:
                    deleted_objs.append(old_by_url[url])

            ## Update internal state
            self.objects = list(current_by_url.values())
            self._objects_by_url = None  ## Invalidate cache
            self.sync_token = self.calendar._generate_fake_sync_token(self.objects)

        return (updated_objs, deleted_objs)
