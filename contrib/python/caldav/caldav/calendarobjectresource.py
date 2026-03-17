"""
Calendar Objects Resources, as defined in the RFC 4791.

There are three subclasses Todo, Journal and Event.  Those mirrors objects stored on the server.  The word ``CalendarObjectResource`` is long, complicated and may be hard to understand.  When you read the word "event" in any documentation, issue discussions, etc, then most likely it should be read as "a CalendarObjectResource, like an event, a task or a journal".  Do not make the mistake of going directly to the Event-class if you want to contribute code for handling "events" - consider that the same code probably will be appicable to Joural and Todo events, if so, CalendarObjectResource is the right class!  Clear as mud?

FreeBusy is also defined as a Calendar Object Resource in the RFC, and it is a bit different .  Perhaps there should be another class layer between CalendarObjectResource and Todo/Event/Journal to indicate that the three latter are closely related, while FreeBusy is something different.

Alarms and Time zone objects does not have any class as for now.  Those are typically subcomponents of an event/task/journal component.

Users of the library should not need to construct any of those objects.  To add new content to the calendar, use ``calendar.save_event``, ``calendar.save_todo`` or ``calendar.save_journal``.  Those methods will return a CalendarObjectResource.
"""
import logging
import re
import sys
import uuid
import warnings
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union
from urllib.parse import ParseResult
from urllib.parse import quote
from urllib.parse import SplitResult
from urllib.parse import unquote

import icalendar
from dateutil.rrule import rrulestr
from icalendar import vCalAddress
from icalendar import vText

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

from .davobject import DAVObject
from .elements.cdav import CalendarData
from .elements import cdav
from .elements import dav
from .lib import error
from .lib import vcal
from .lib.error import errmsg
from .lib.python_utilities import to_normal_str
from .lib.python_utilities import to_unicode
from .lib.python_utilities import to_wire
from .lib.url import URL

log = logging.getLogger("caldav")


class CalendarObjectResource(DAVObject):
    """Ref RFC 4791, section 4.1, a "Calendar Object Resource" can be an
    event, a todo-item, a journal entry, or a free/busy entry

    As per the RFC, a CalendarObjectResource can at most contain one
    calendar component, with the exception of recurrence components.
    Meaning that event.data typically contains one VCALENDAR with one
    VEVENT and possibly one VTIMEZONE.

    In the case of expanded calendar date searches, each recurrence
    will (by default) wrapped in a distinct CalendarObjectResource
    object.  This is a deviation from the definition given in the RFC.
    """

    ## There is also STARTTOFINISH, STARTTOSTART and FINISHTOFINISH in RFC9253,
    ## those do not seem to have any reverse
    ## (FINISHTOSTART and STARTTOFINISH may seem like reverse relations, but
    ## as I read the RFC, FINISHTOSTART seems like the reverse of DEPENDS-ON)
    ## (STARTTOSTART and FINISHTOFINISH may also seem like symmetric relations,
    ## meaning they are their own reverse, but as I read the RFC they are
    ## asymmetric)
    RELTYPE_REVERSE_MAP: ClassVar = {
        "PARENT": "CHILD",
        "CHILD": "PARENT",
        "SIBLING": "SIBLING",
        ## this is how Tobias Brox inteprets RFC9253:
        "DEPENDS-ON": "FINISHTOSTART",
        "FINISHTOSTART": "DEPENDENT",
        ## next/first is a special case, linked list
        ## it needs special handling when length of list<>2
        # "NEXT": "FIRST",
        # "FIRST": "NEXT",
    }

    _ENDPARAM = None

    _vobject_instance = None
    _icalendar_instance = None
    _data = None

    def __init__(
        self,
        client: Optional["DAVClient"] = None,
        url: Union[str, ParseResult, SplitResult, URL, None] = None,
        data: Optional[Any] = None,
        parent: Optional[Any] = None,
        id: Optional[Any] = None,
        props: Optional[Any] = None,
    ) -> None:
        """
        CalendarObjectResource has an additional parameter for its constructor:
         * data = "...", vCal data for the event
        """
        super(CalendarObjectResource, self).__init__(
            client=client, url=url, parent=parent, id=id, props=props
        )
        if data is not None:
            self.data = data
            if id:
                old_id = self.icalendar_component.pop("UID", None)
                self.icalendar_component.add("UID", id)

    def set_end(self, end, move_dtstart=False):
        """The RFC specifies that a VEVENT/VTODO cannot have both
        dtend/due and duration, so when setting dtend/due, the duration
        field must be evicted

        WARNING: this method is likely to be deprecated and parts of
        it moved to the icalendar library.  If you decide to use it,
        please put caldav<3.0 in the requirements.
        """
        i = self.icalendar_component
        ## TODO: are those lines useful for anything?
        if hasattr(end, "tzinfo") and not end.tzinfo:
            end = end.astimezone(timezone.utc)
        duration = self.get_duration()
        i.pop("DURATION", None)
        i.pop(self._ENDPARAM, None)

        if move_dtstart and duration and "DTSTART" in i:
            i.pop("DTSTART")
            i.add("DTSTART", end - duration)

        i.add(self._ENDPARAM, end)

    def add_organizer(self) -> None:
        """
        goes via self.client, finds the principal, figures out the right attendee-format and adds an
        organizer line to the event
        """
        if self.client is None:
            raise ValueError("Unexpected value None for self.client")

        principal = self.client.principal()
        ## TODO: remove Organizer-field, if exists
        ## TODO: what if walk returns more than one vevent?
        self.icalendar_component.add("organizer", principal.get_vcal_address())

    def split_expanded(self) -> List[Self]:
        """This was used internally for processing search results.
        Library users probably don't need to care about this one.

        The logic is now handled directly in the search method.

        This method is probably used by nobody and nothing, but
        it can't be removed easily as it's exposed as part of the
        public API
        """

        warnings.warn(
            "obj.split_expanded is likely to be removed in a future version of caldav.  Feel free to protest if you need it",
            DeprecationWarning,
            stacklevel=2,
        )

        i = self.icalendar_instance.subcomponents
        tz_ = [x for x in i if isinstance(x, icalendar.Timezone)]
        ntz = [x for x in i if not isinstance(x, icalendar.Timezone)]
        if len(ntz) == 1:
            return [self]
        if tz_:
            error.assert_(len(tz_) == 1)
        ret = []
        for ical_obj in ntz:
            obj = self.copy(keep_uid=True)
            obj.icalendar_instance.subcomponents = []
            if tz_:
                obj.icalendar_instance.subcomponents.append(tz_[0])
            obj.icalendar_instance.subcomponents.append(ical_obj)
            ret.append(obj)
        return ret

    def expand_rrule(
        self, start: datetime, end: datetime, include_completed: bool = True
    ) -> None:
        """This method will transform the calendar content of the
        event and expand the calendar data from a "master copy" with
        RRULE set and into a "recurrence set" with RECURRENCE-ID set
        and no RRULE set.  The main usage is for client-side expansion
        in case the calendar server does not support server-side
        expansion.  If doing a `self.load`, the calendar
        content will be replaced with the "master copy".

        :param event: Event
        :param start: datetime
        :param end: datetime

        """
        ## TODO: this has been *copied* over to the icalendar-searcher package.
        ## This code was previously used internally by the search.
        ## By now it's probably dead code, used by nothing and nobody.
        ## Since it's exposed as part of the API, I cannot delete it, but I can
        ## deprecate it.
        warnings.warn(
            "obj.expand_rrule is likely to be removed in a future version of caldav.  Feel free to protest if you need it",
            DeprecationWarning,
            stacklevel=2,
        )

        import recurring_ical_events

        recurrings = recurring_ical_events.of(
            self.icalendar_instance, components=["VJOURNAL", "VTODO", "VEVENT"]
        ).between(start, end)

        recurrence_properties = {"exdate", "exrule", "rdate", "rrule"}

        error.assert_(
            not any(
                x
                for x in recurrings
                if not recurrence_properties.isdisjoint(set(x.keys()))
            )
        )

        calendar = self.icalendar_instance
        calendar.subcomponents = []
        for occurrence in recurrings:
            ## Ignore completed task recurrences
            if (
                not include_completed
                and occurrence.name == "VTODO"
                and occurrence.get("STATUS") in ("COMPLETED", "CANCELLED")
            ):
                continue
            ## TODO: If there are no reports of missing RECURRENCE-ID until 2027,
            ## the if-statement below may be deleted
            error.assert_("RECURRENCE-ID" in occurrence)
            if "RECURRENCE-ID" not in occurrence:
                occurrence.add("RECURRENCE-ID", occurrence.get("DTSTART").dt)
            calendar.add_component(occurrence)

    def set_relation(
        self, other, reltype=None, set_reverse=True
    ) -> None:  ## TODO: logic to find and set siblings?
        """
        Sets a relation between this object and another object (given by uid or object).
        """
        ##TODO: test coverage
        reltype = reltype.upper()
        if isinstance(other, CalendarObjectResource):
            if other.id:
                uid = other.id
            else:
                uid = other.icalendar_component["uid"]
        else:
            uid = other
            if set_reverse:
                other = self.parent.object_by_uid(uid)
        if set_reverse:
            ## TODO: special handling of NEXT/FIRST.
            ## STARTTOFINISH does not have any equivalent "reverse".
            reltype_reverse = self.RELTYPE_REVERSE_MAP[reltype]
            other.set_relation(other=self, reltype=reltype_reverse, set_reverse=False)

        existing_relation = self.icalendar_component.get("related-to", None)
        existing_relations = (
            existing_relation
            if isinstance(existing_relation, list)
            else [existing_relation]
        )
        for rel in existing_relations:
            if rel == uid:
                return

        # without str(…), icalendar ignores properties
        #  because if type(uid) == vText
        #  then Component._encode does miss adding properties
        #  see https://github.com/collective/icalendar/issues/557
        #  workaround should be safe to remove if issue gets fixed
        uid = str(uid)
        self.icalendar_component.add(
            "related-to", uid, parameters={"RELTYPE": reltype}, encode=True
        )

        self.save()

    ## TODO: this method is undertested in the caldav library.
    ## However, as this consolidated and eliminated quite some duplicated code in the
    ## plann project, it is extensively tested in plann.
    def get_relatives(
        self,
        reltypes: Optional[Container[str]] = None,
        relfilter: Optional[Callable[[Any], bool]] = None,
        fetch_objects: bool = True,
        ignore_missing: bool = True,
    ) -> DefaultDict[str, Set[str]]:
        """
        By default, loads all objects pointed to by the RELATED-TO
        property and loads the related objects.

        It's possible to filter, either by passing a set or a list of
        acceptable relation types in reltypes, or by passing a lambda
        function in relfilter.

        TODO: Make it possible to  also check up reverse relationships

        TODO: this is partially overlapped by plann.lib._relships_by_type
        in the plann tool.  Should consolidate the code.

        TODO: should probably return some kind of object instead of a weird dict structure.
        (but due to backward compatibility requirement, such an object should behave like
        the current dict)
        """
        from .collection import Calendar  ## late import to avoid cycling imports

        ret = defaultdict(set)
        relations = self.icalendar_component.get("RELATED-TO", [])
        if not isinstance(relations, list):
            relations = [relations]
        for rel in relations:
            if relfilter and not relfilter(rel):
                continue
            reltype = rel.params.get("RELTYPE", "PARENT")
            if reltypes and reltype not in reltypes:
                continue
            ret[reltype].add(str(rel))

        if fetch_objects:
            for reltype in ret:
                uids = ret[reltype]
                reltype_set = set()

                if self.parent is None:
                    raise ValueError("Unexpected value None for self.parent")

                if not isinstance(self.parent, Calendar):
                    raise ValueError(
                        "self.parent expected to be of type Calendar but it is not"
                    )

                for obj in uids:
                    try:
                        reltype_set.add(self.parent.object_by_uid(obj))
                    except error.NotFoundError:
                        if not ignore_missing:
                            raise

                ret[reltype] = reltype_set

        return ret

    def _set_reverse_relation(self, other, reltype):
        ## TODO: handle RFC9253 better!  Particularly next/first-lists
        reverse_reltype = self.RELTYPE_REVERSE_MAP.get(reltype)
        if not reverse_reltype:
            logging.error(
                "Reltype %s not supported in object uid %s" % (reltype, self.id)
            )
            return
        other.set_relation(self, reverse_reltype, other)

    def _verify_reverse_relation(self, other, reltype) -> tuple:
        revreltype = self.RELTYPE_REVERSE_MAP[reltype]
        ## TODO: special case FIRST/NEXT needs special handling
        other_relations = other.get_relatives(
            fetch_objects=False, reltypes={revreltype}
        )
        if not str(self.icalendar_component["uid"]) in other_relations[revreltype]:
            ## I don't remember why we need to return a tuple
            ## but it's propagated through the "public" methods, so we'll
            ## have to leave it like this.
            return (other, revreltype)
        return False

    def _handle_reverse_relations(
        self, verify: bool = False, fix: bool = False, pdb: bool = False
    ) -> list:
        """
        Goes through all relations and verifies that the return relation is set
        if verify is set:
            Returns a list of objects missing a reverse.
            Use public method check_reverse_relations instead
        if verify and fix is set:
            Fixup all objects missing a reverse.
            Use public method fix_reverse_relations instead.
        If fix but not verify is set:
            Assume all reverse relations are missing.
            Used internally when creating new objects.
        """
        ret = []
        assert verify or fix
        relations = self.get_relatives()
        for reltype in relations:
            for other in relations[reltype]:
                if verify:
                    foobar = self._verify_reverse_relation(other, reltype)
                    if foobar:
                        ret.append(foobar)
                        if pdb:
                            breakpoint()
                        if fix:
                            self._set_reverse_relation(other, reltype)
                elif fix:
                    self._set_reverse_relation(other, reltype)
        return ret

    def check_reverse_relations(self, pdb: bool = False) -> List[tuple]:
        """
        Will verify that for all the objects we point at though
        the RELATED-TO property, the other object points back to us as
        well.

        Returns a list of tuples.  Each tuple contains an object that
        do not point back as expected, and the expected reltype
        """
        return self._handle_reverse_relations(verify=True, fix=False, pdb=pdb)

    def fix_reverse_relations(self, pdb: bool = False) -> list:
        """
        Will ensure that for all the objects we point at though
        the RELATED-TO property, the other object points back to us as
        well.

        Returns a list of tuples.  Each tuple contains an object that
        did not point back as expected, and the expected reltype
        """
        return self._handle_reverse_relations(verify=True, fix=True, pdb=pdb)

    def _get_icalendar_component(self, assert_one=False):
        """Returns the icalendar subcomponent - which should be an
        Event, Journal, Todo or FreeBusy from the icalendar class

        See also https://github.com/python-caldav/caldav/issues/232
        """
        self.load(only_if_unloaded=True)
        if not self.icalendar_instance:
            return None
        ## PERFORMANCE TODO: no point creating a big list here
        ret = [
            x
            for x in self.icalendar_instance.subcomponents
            if not isinstance(x, icalendar.Timezone)
        ]
        error.assert_(len(ret) == 1 or not assert_one)
        for x in ret:
            for cl in (
                icalendar.Event,
                icalendar.Journal,
                icalendar.Todo,
                icalendar.FreeBusy,
            ):
                if isinstance(x, cl):
                    return x
        error.assert_(False)

    def _set_icalendar_component(self, value) -> None:
        s = self.icalendar_instance.subcomponents
        i = [i for i in range(0, len(s)) if not isinstance(s[i], icalendar.Timezone)]
        if len(i) == 1:
            self.icalendar_instance.subcomponents[i[0]] = value
        else:
            my_instance = icalendar.Calendar()
            my_instance.add("prodid", self.icalendar_instance["prodid"])
            my_instance.add("version", self.icalendar_instance["version"])
            my_instance.add_component(value)
            self.icalendar_instance = my_instance

    icalendar_component = property(
        _get_icalendar_component,
        _set_icalendar_component,
        doc="icalendar component - this is the simplest way to access the event/task - it will give you the first component that isn't a timezone component.  For recurrence sets, the master component will be returned.  For any non-recurring event/task/journal, there should be only one calendar component in the object.  For results from an expanded search, there should be only one calendar component in the object",
    )

    component = icalendar_component

    def get_due(self):
        """
        A VTODO may have due or duration set.  Return or calculate due.

        WARNING: this method is likely to be deprecated and moved to
        the icalendar library.  If you decide to use it, please put
        caldav<3.0 in the requirements.
        """
        i = self.icalendar_component
        if "DUE" in i:
            return i["DUE"].dt
        elif "DTEND" in i:
            return i["DTEND"].dt
        elif "DURATION" in i and "DTSTART" in i:
            return i["DTSTART"].dt + i["DURATION"].dt
        else:
            return None

    get_dtend = get_due

    def add_attendee(
        self, attendee, no_default_parameters: bool = False, **parameters
    ) -> None:
        """
        For the current (event/todo/journal), add an attendee.

        The attendee can be any of the following:
        * A principal
        * An email address prepended with "mailto:"
        * An email address without the "mailto:"-prefix
        * A two-item tuple containing a common name and an email address
        * (not supported, but planned: an ical text line starting with the word "ATTENDEE")

        Any number of attendee parameters can be given, those will be used
        as defaults unless no_default_parameters is set to True:

        partstat=NEEDS-ACTION
        cutype=UNKNOWN (unless a principal object is given)
        rsvp=TRUE
        role=REQ-PARTICIPANT
        schedule-agent is not set
        """
        from .collection import Principal  ## late import to avoid cycling imports

        if isinstance(attendee, Principal):
            attendee_obj = attendee.get_vcal_address()
        elif isinstance(attendee, vCalAddress):
            attendee_obj = attendee
        elif isinstance(attendee, tuple):
            if attendee[1].startswith("mailto:"):
                attendee_obj = vCalAddress(attendee[1])
            else:
                attendee_obj = vCalAddress("mailto:" + attendee[1])
            attendee_obj.params["cn"] = vText(attendee[0])
        elif isinstance(attendee, str):
            if attendee.startswith("ATTENDEE"):
                raise NotImplementedError(
                    "do we need to support this anyway?  Should be trivial, but can't figure out how to do it with the icalendar.Event/vCalAddress objects right now"
                )
            elif attendee.startswith("mailto:"):
                attendee_obj = vCalAddress(attendee)
            elif "@" in attendee and ":" not in attendee and ";" not in attendee:
                attendee_obj = vCalAddress("mailto:" + attendee)
        else:
            error.assert_(False)
            attendee_obj = vCalAddress()

        ## TODO: if possible, check that the attendee exists
        ## TODO: check that the attendee will not be duplicated in the event.
        if not no_default_parameters:
            ## Sensible defaults:
            attendee_obj.params["partstat"] = "NEEDS-ACTION"
            if "cutype" not in attendee_obj.params:
                attendee_obj.params["cutype"] = "UNKNOWN"
            attendee_obj.params["rsvp"] = "TRUE"
            attendee_obj.params["role"] = "REQ-PARTICIPANT"
        params = {}
        for key in parameters:
            new_key = key.replace("_", "-")
            if parameters[key] is True:
                params[new_key] = "TRUE"
            else:
                params[new_key] = parameters[key]
        attendee_obj.params.update(params)
        ievent = self.icalendar_component
        ievent.add("attendee", attendee_obj)

    def is_invite_request(self) -> bool:
        """
        Returns True if this object is a request, see
        https://www.rfc-editor.org/rfc/rfc2446.html#section-3.2.2
        """
        self.load(only_if_unloaded=True)
        return self.icalendar_instance.get("method", None) == "REQUEST"

    def is_invite_reply(self) -> bool:
        """
        Returns True if the object is a reply, see
        https://www.rfc-editor.org/rfc/rfc2446.html#section-3.2.3
        """
        self.load(only_if_unloaded=True)
        return self.icalendar_instance.get("method", None) == "REPLY"

    def accept_invite(self, calendar: Optional["Calendar"] = None) -> None:
        """
        Accepts an invite - to be used on an invite object.
        """
        self._reply_to_invite_request("ACCEPTED", calendar)

    def decline_invite(self, calendar: Optional["Calendar"] = None) -> None:
        """
        Declines an invite - to be used on an invite object.
        """
        self._reply_to_invite_request("DECLINED", calendar)

    def tentatively_accept_invite(self, calendar: Optional[Any] = None) -> None:
        """
        Tentatively accept an invite - to be used on an invite object.
        """
        self._reply_to_invite_request("TENTATIVE", calendar)

    ## TODO: DELEGATED is also a valid option, and for vtodos the
    ## partstat can also be set to COMPLETED and IN-PROGRESS.

    def _reply_to_invite_request(self, partstat, calendar) -> None:
        error.assert_(self.is_invite_request())
        if not calendar:
            calendar = self.client.principal().calendars()[0]
        ## we need to modify the icalendar code, update our own participant status
        self.icalendar_instance.pop("METHOD")
        self.change_attendee_status(partstat=partstat)
        self.get_property(cdav.ScheduleTag(), use_cached=True)
        try:
            calendar.save_event(self.data)
        except Exception:
            ## TODO - TODO - TODO
            ## RFC6638 does not seem to be very clear (or
            ## perhaps I should read it more thoroughly) neither on
            ## how to handle conflicts, nor if the reply should be
            ## posted to the "outbox", saved back to the same url or
            ## sent to a calendar.
            self.load()
            self.get_property(cdav.ScheduleTag(), use_cached=False)
            outbox = self.client.principal().schedule_outbox()
            if calendar.url != outbox.url:
                self._reply_to_invite_request(partstat, calendar=outbox)
            else:
                self.save()

    def copy(self, keep_uid: bool = False, new_parent: Optional[Any] = None) -> Self:
        """
        Events, todos etc can be copied within the same calendar, to another
        calendar or even to another caldav server
        """
        obj = self.__class__(
            parent=new_parent or self.parent,
            data=self.data,
            id=self.id if keep_uid else str(uuid.uuid1()),
        )
        if new_parent or not keep_uid:
            obj.url = obj._generate_url()
        else:
            obj.url = self.url
        return obj

    ## TODO: move get-logics to a load_by_get method.
    ## The load method should deal with "server quirks".
    def load(self, only_if_unloaded: bool = False) -> Self:
        """
        (Re)load the object from the caldav server.
        """
        if only_if_unloaded and self.is_loaded():
            return self

        if self.url is None:
            raise ValueError("Unexpected value None for self.url")

        if self.client is None:
            raise ValueError("Unexpected value None for self.client")

        try:
            r = self.client.request(str(self.url))
            if r.status and r.status == 404:
                raise error.NotFoundError(errmsg(r))
            self.data = r.raw
        except error.NotFoundError:
            raise
        except:
            return self.load_by_multiget()
        if "Etag" in r.headers:
            self.props[dav.GetEtag.tag] = r.headers["Etag"]
        if "Schedule-Tag" in r.headers:
            self.props[cdav.ScheduleTag.tag] = r.headers["Schedule-Tag"]
        return self

    def load_by_multiget(self) -> Self:
        """
        Some servers do not accept a GET, but we can still do a REPORT
        with a multiget query
        """
        error.assert_(self.url)
        mydata = self.parent._multiget(event_urls=[self.url], raise_notfound=True)
        try:
            url, self.data = next(mydata)
        except StopIteration:
            ## We shouldn't come here.  Something is wrong.
            ## TODO: research it
            ## As of 2025-05-20, this code section is used by
            ## TestForServerECloud::testCreateOverwriteDeleteEvent
            raise error.NotFoundError(self.url)
        error.assert_(self.data)
        error.assert_(next(mydata, None) is None)
        return self

    ## TODO: self.id should either always be available or never
    ## TODO: run this logic on load, to ensure `self.id` is set after loading
    def _find_id_path(self, id=None, path=None) -> None:
        """
        With CalDAV, every object has a URL.  With icalendar, every object
        should have a UID.  This UID may or may not be copied into self.id.

        This method will:

        0) if ID is given, assume that as the UID, and set it in the object
        1) if UID is given in the object, assume that as the ID
        2) if ID is not given, but the path is given, generate the ID from the
           path
        3) If neither ID nor path is given, use the uuid method to generate an
           ID (TODO: recommendation in the RFC is to concat some timestamp, serial or
           random number and a domain)
        4) if no path is given, generate the URL from the ID
        """
        i = self._get_icalendar_component(assert_one=False)
        if not id and getattr(self, "id", None):
            id = self.id
        if not id:
            id = i.pop("UID", None)
            if id:
                id = str(id)
        if not path and getattr(self, "path", None):
            path = self.path
        if id is None and path is not None and str(path).endswith(".ics"):
            ## TODO: do we ever get here?  Perhaps this if is completely moot?
            id = re.search("(/|^)([^/]*).ics", str(path)).group(2)
        if id is None:
            id = str(uuid.uuid1())

        i.pop("UID", None)
        i.add("UID", id)

        self.id = id

        for x in self.icalendar_instance.subcomponents:
            if not isinstance(x, icalendar.Timezone):
                error.assert_(x.get("UID", None) == self.id)

        if path is None:
            path = self._generate_url()
        else:
            path = self.parent.url.join(path)

        self.url = URL.objectify(path)

    def _put(self, retry_on_failure=True):
        ## SECURITY TODO: we should probably have a check here to verify that no such object exists already
        r = self.client.put(
            self.url, self.data, {"Content-Type": 'text/calendar; charset="utf-8"'}
        )
        if r.status == 302:
            path = [x[1] for x in r.headers if x[0] == "location"][0]
        elif r.status not in (204, 201):
            if retry_on_failure:
                try:
                    import vobject
                except ImportError:
                    retry_on_failure = False
            if retry_on_failure:
                ## This looks like a noop, but the object may be "cleaned".
                ## See https://github.com/python-caldav/caldav/issues/43
                self.vobject_instance
                return self._put(False)
            else:
                raise error.PutError(errmsg(r))

    def _create(self, id=None, path=None, retry_on_failure=True) -> None:
        ## TODO: Find a better method name
        self._find_id_path(id=id, path=path)
        self._put()

    def _generate_url(self):
        ## See https://github.com/python-caldav/caldav/issues/143 for the rationale behind double-quoting slashes
        ## TODO: should try to wrap my head around issues that arises when id contains weird characters.  maybe it's
        ## better to generate a new uuid here, particularly if id is in some unexpected format.
        if not self.id:
            self.id = self._get_icalendar_component(assert_one=False)["UID"]
        return self.parent.url.join(quote(self.id.replace("/", "%2F")) + ".ics")

    def change_attendee_status(self, attendee: Optional[Any] = None, **kwargs) -> None:
        """
        Updates the attendee-line according to the arguments received
        """
        from .collection import Principal  ## late import to avoid cycling imports

        if not attendee:
            if self.client is None:
                raise ValueError("Unexpected value None for self.client")

            attendee = self.client.principal()

        cnt = 0

        if isinstance(attendee, Principal):
            attendee_emails = attendee.calendar_user_address_set()
            for addr in attendee_emails:
                try:
                    self.change_attendee_status(addr, **kwargs)
                    ## TODO: can probably just return now
                    cnt += 1
                except error.NotFoundError:
                    pass
            if not cnt:
                raise error.NotFoundError(
                    "Principal %s is not invited to event" % str(attendee)
                )
            error.assert_(cnt == 1)
            return

        ical_obj = self.icalendar_component
        attendee_lines = ical_obj["attendee"]
        if isinstance(attendee_lines, str):
            attendee_lines = [attendee_lines]
        strip_mailto = lambda x: str(x).lower().replace("mailto:", "")
        for attendee_line in attendee_lines:
            if strip_mailto(attendee_line) == strip_mailto(attendee):
                attendee_line.params.update(kwargs)
                cnt += 1
        if not cnt:
            raise error.NotFoundError("Participant %s not found in attendee list")
        error.assert_(cnt == 1)

    def save(
        self,
        no_overwrite: bool = False,
        no_create: bool = False,
        obj_type: Optional[str] = None,
        increase_seqno: bool = True,
        if_schedule_tag_match: bool = False,
        only_this_recurrence: bool = True,
        all_recurrences: bool = False,
    ) -> Self:
        """Save the object, can be used for creation and update.

        no_overwrite and no_create will check if the object exists.
        Those two are mutually exclusive.  Some servers don't support
        searching for an object uid without explicitly specifying what
        kind of object it should be, hence obj_type can be passed.
        obj_type is only used in conjunction with no_overwrite and
        no_create.

        is_schedule_tag_match is currently ignored. (TODO - fix or remove)

        The SEQUENCE should be increased when saving a new version of
        the object.  If this behaviour is unwanted, then
        increase_seqno should be set to False.  Also, if SEQUENCE is
        not set, then this will be ignored.

        The behaviour when saving a single recurrence object to the
        server is as far as I can understand not defined in the RFCs,
        but all servers I've tested against will overwrite the full
        event with the recurrence instance (effectively deleting the
        recurrence rule).  That's almost for sure not what the caller
        intended.  only_this_recurrence and all_recurrences only
        applies when trying to save a recurrence object.  They are by
        nature mutually exclusive, but since only_this_recurrence is
        True by default, it will be ignored if all_recurrences is set.

        If you want to sent the recurrence as it is to the server,
        you should set both all_recurrences and only_this_recurrence
        to False.

        Returns:
         * self

        """
        ## Rather than passing the icalendar data verbatimely, we're
        ## efficiently running the icalendar code through the icalendar
        ## library.  This may cause data modifications and may "unfix"
        ## https://github.com/python-caldav/caldav/issues/43
        ## TODO: think more about this
        if not obj_type:
            obj_type = self.__class__.__name__.lower()
        if (
            self._vobject_instance is None
            and self._data is None
            and self._icalendar_instance is None
        ):
            ## TODO: This makes no sense.  We should probably raise an error.
            ## But the behaviour should be officially deprecated first.
            return self

        path = self.url.path if self.url else None

        def get_self():
            self.id = self.id or self.icalendar_component.get("uid")
            if self.id:
                try:
                    if obj_type:
                        return getattr(self.parent, "%s_by_uid" % obj_type)(self.id)
                    else:
                        return self.parent.object_by_uid(self.id)
                except error.NotFoundError:
                    return None
            return None

        if no_overwrite or no_create:
            ## SECURITY TODO: path names on the server does not
            ## necessarily map cleanly to UUIDs.  We need to do quite
            ## some refactoring here to ensure all corner cases are
            ## covered.  Doing a GET first to check if the resource is
            ## found and then a PUT also gives a potential race
            ## condition.  (Possibly the API gives no safe way to ensure
            ## a unique new calendar item is created to the server without
            ## overwriting old stuff or vice versa - it seems silly to me
            ## to do a PUT instead of POST when creating new data).
            ## TODO: the "find id"-logic is duplicated in _create,
            ## should be refactored
            existing = get_self()
            if not self.id and no_create:
                raise error.ConsistencyError("no_create flag was set, but no ID given")
            if no_overwrite and existing:
                raise error.ConsistencyError(
                    "no_overwrite flag was set, but object already exists"
                )

            if no_create and not existing:
                raise error.ConsistencyError(
                    "no_create flag was set, but object does not exists"
                )

        ## Save a single recurrence-id and all calendars servers seems
        ## to overwrite the full object, effectively deleting the
        ## RRULE.  I can't find this behaviour specified in the RFC.
        ## That's probably not what the caller intended intended.
        if (
            only_this_recurrence or all_recurrences
        ) and "RECURRENCE-ID" in self.icalendar_component:
            obj = get_self()  ## get the full object, not only the recurrence
            ici = obj.icalendar_instance  # ical instance
            if all_recurrences:
                occ = obj.icalendar_component  ## original calendar component
                ncc = self.icalendar_component.copy()  ## new calendar component
                for prop in ["exdate", "exrule", "rdate", "rrule"]:
                    if prop in occ:
                        ncc[prop] = occ[prop]

                ## dtstart_diff = how much we've moved the time
                ## TODO: we may easily have timezone problems here and events shifting some hours ...
                dtstart_diff = (
                    ncc.start.astimezone() - ncc["recurrence-id"].dt.astimezone()
                )
                new_duration = ncc.duration
                ncc.pop("dtstart")
                ncc.add("dtstart", occ.start + dtstart_diff)
                for ep in ("duration", "dtend"):
                    if ep in ncc:
                        ncc.pop(ep)
                ncc.add("dtend", ncc.start + new_duration)
                ncc.pop("recurrence-id")
                s = ici.subcomponents

                ## Replace the "root" subcomponent
                comp_idxes = (
                    i
                    for i in range(0, len(s))
                    if not isinstance(s[i], icalendar.Timezone)
                )
                comp_idx = next(comp_idxes)
                s[comp_idx] = ncc

                ## The recurrence-ids of all objects has to be
                ## recalculated (this is probably not quite right.  If
                ## we move the time of a daily meeting from 8 to 10,
                ## then we need to do this.  If we move the date of
                ## the first instance, then probably we shouldn't
                ## ... oh well ... so many complications)
                if dtstart_diff:
                    for i in comp_idxes:
                        rid = s[i].pop("recurrence-id")
                        s[i].add("recurrence-id", rid.dt + dtstart_diff)

                return obj.save(increase_seqno=increase_seqno)
            if only_this_recurrence:
                existing_idx = [
                    i
                    for i in range(0, len(ici.subcomponents))
                    if ici.subcomponents[i].get("recurrence-id")
                    == self.icalendar_component["recurrence-id"]
                ]
                error.assert_(len(existing_idx) <= 1)
                if existing_idx:
                    ici.subcomponents[existing_idx[0]] = self.icalendar_component
                else:
                    ici.add_component(self.icalendar_component)
                return obj.save(increase_seqno=increase_seqno)

        if "SEQUENCE" in self.icalendar_component:
            seqno = self.icalendar_component.pop("SEQUENCE", None)
            if seqno is not None:
                self.icalendar_component.add("SEQUENCE", seqno + 1)

        self._create(id=self.id, path=path)
        return self

    def is_loaded(self):
        """Returns True if there exists data in the object.  An
        object is considered not to be loaded if it contains no data
        but just the URL.

        TOOD: bad side effect, converts the data to a string,
        potentially breaking couplings
        """
        return (
            (self._data and self._data.count("BEGIN:") > 1)
            or self._vobject_instance
            or self._icalendar_instance
        )

    def has_component(self):
        """
        Returns True if there exists a VEVENT, VTODO or VJOURNAL in the data.
        Returns False if it's only a VFREEBUSY, VTIMEZONE or unknown components.

        TODO: Bad side-effect: converts to data - any icalendar instances coupled to the object
        will be decoupled.

        Used internally after search to remove empty search results (sometimes Google return such)
        """
        return (
            self._data
            or self._vobject_instance
            or (self._icalendar_instance and self.icalendar_component)
        ) and self.data.count("BEGIN:VEVENT") + self.data.count(
            "BEGIN:VTODO"
        ) + self.data.count(
            "BEGIN:VJOURNAL"
        ) > 0

    def __str__(self) -> str:
        return "%s: %s" % (self.__class__.__name__, self.url)

    ## implementation of the properties self.data,
    ## self.vobject_instance and self.icalendar_instance follows.  The
    ## rule is that only one of them can be set at any time, this
    ## since vobject_instance and icalendar_instance are mutable,
    ## and any modification to those instances should apply
    def _set_data(self, data):
        ## The __init__ takes a data attribute, and it should be allowable to
        ## set it to a vobject object or an icalendar object, hence we should
        ## do type checking on the data (TODO: but should probably use
        ## isinstance rather than this kind of logic
        if type(data).__module__.startswith("vobject"):
            self._set_vobject_instance(data)
            return self

        if type(data).__module__.startswith("icalendar"):
            self._set_icalendar_instance(data)
            return self

        self._data = vcal.fix(data)
        self._vobject_instance = None
        self._icalendar_instance = None
        return self

    def _get_data(self):
        if self._data:
            return to_normal_str(self._data)
        elif self._vobject_instance:
            return to_normal_str(self._vobject_instance.serialize())
        elif self._icalendar_instance:
            return to_normal_str(self._icalendar_instance.to_ical())
        return None

    def _get_wire_data(self):
        if self._data:
            return to_wire(self._data)
        elif self._vobject_instance:
            return to_wire(self._vobject_instance.serialize())
        elif self._icalendar_instance:
            return to_wire(self._icalendar_instance.to_ical())
        return None

    data: Any = property(
        _get_data, _set_data, doc="vCal representation of the object as normal string"
    )
    wire_data = property(
        _get_wire_data,
        _set_data,
        doc="vCal representation of the object in wire format (UTF-8, CRLN)",
    )

    def _set_vobject_instance(self, inst: "vobject.base.Component"):
        self._vobject_instance = inst
        self._data = None
        self._icalendar_instance = None
        return self

    def _get_vobject_instance(self) -> Optional["vobject.base.Component"]:
        try:
            import vobject
        except ImportError:
            logging.critical(
                "A vobject instance has been requested, but the vobject library is not installed (vobject is no longer an official dependency in 2.0)"
            )
            return None
        if not self._vobject_instance:
            if self._get_data() is None:
                return None
            try:
                self._set_vobject_instance(
                    vobject.readOne(to_unicode(self._get_data()))  # type: ignore
                )
            except:
                log.critical(
                    "Something went wrong while loading icalendar data into the vobject class.  ical url: "
                    + str(self.url)
                )
                raise
        return self._vobject_instance

    ## event.instance has always yielded a vobject, but will probably yield an icalendar_instance
    ## in version 3.0!
    def _get_deprecated_vobject_instance(self) -> Optional["vobject.base.Component"]:
        warnings.warn(
            "use event.vobject_instance or event.icalendar_instance",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._get_vobject_instance()

    def _set_deprecated_vobject_instance(self, inst: "vobject.base.Component"):
        warnings.warn(
            "use event.vobject_instance or event.icalendar_instance",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._get_vobject_instance(inst)

    vobject_instance: "vobject.base.VBase" = property(
        _get_vobject_instance,
        _set_vobject_instance,
        doc="vobject instance of the object",
    )

    instance: "vobject.base.VBase" = property(
        _get_deprecated_vobject_instance,
        _set_deprecated_vobject_instance,
        doc="vobject instance of the object (DEPRECATED!  This will yield an icalendar instance in caldav 3.0)",
    )

    def _set_icalendar_instance(self, inst):
        if not isinstance(inst, icalendar.Calendar):
            ## assume inst is an Event, Journal or Todo.
            ## TODO: perhaps a bit better sanity checking here?
            try:  ## DEPRECATION TODO: remove this try/except the future
                ## icalendar 7.x behaviour (not released yet as of 2025-09
                cal = icalendar.Calendar.new()
            except:
                cal = icalendar.Calendar()
                cal.add("prodid", "-//python-caldav//caldav//en_DK")
                cal.add("version", "2.0")
            cal.add_component(inst)
            inst = cal
        self._icalendar_instance = inst
        self._data = None
        self._vobject_instance = None
        return self

    def _get_icalendar_instance(self):
        if not self._icalendar_instance:
            if not self.data:
                return None
            self.icalendar_instance = icalendar.Calendar.from_ical(
                to_unicode(self.data)
            )
        return self._icalendar_instance

    icalendar_instance: Any = property(
        _get_icalendar_instance,
        _set_icalendar_instance,
        doc="icalendar instance of the object",
    )

    def get_duration(self) -> timedelta:
        """According to the RFC, either DURATION or DUE should be set
        for a task, but never both - implicitly meaning that DURATION
        is the difference between DTSTART and DUE (personally I
        believe that's stupid.  If a task takes five minutes to
        complete - say, fill in some simple form that should be
        delivered before midnight at new years eve, then it feels
        natural for me to define "duration" as five minutes, DTSTART
        to "some days before new years eve" and DUE to 20xx-01-01
        00:00:00 - but I digress.

        This method will return DURATION if set, otherwise the
        difference between DUE and DTSTART (if both of them are set).

        TODO: should be fixed for Event class as well (only difference
        is that DTEND is used rather than DUE) and possibly also for
        Journal (defaults to one day, probably?)

        WARNING: this method is likely to be deprecated and moved to
        the icalendar library.  If you decide to use it, please put
        caldav<3.0 in the requirements.
        """
        i = self.icalendar_component
        return self._get_duration(i)

    def _get_duration(self, i):
        if "DURATION" in i:
            return i["DURATION"].dt
        elif "DTSTART" in i and self._ENDPARAM in i:
            end = i[self._ENDPARAM].dt
            start = i["DTSTART"].dt
            ## We do have a problem here if one is a date and the other is a
            ## datetime.  This is NOT explicitly defined as a technical
            ## breach in the RFC, so we need to work around it.
            if isinstance(end, datetime) != isinstance(start, datetime):
                start = datetime(start.year, start.month, start.day)
                end = datetime(end.year, end.month, end.day)
            return end - start
        elif "DTSTART" in i and not isinstance(i["DTSTART"], datetime):
            return timedelta(days=1)
        else:
            return timedelta(0)


class Event(CalendarObjectResource):
    """
    The `Event` object is used to represent an event (VEVENT).

    As of 2020-12 it adds very little to the inheritated class.  (I have
    frequently asked myself if we need those subclasses ... perhaps
    not)
    """

    set_dtend = CalendarObjectResource.set_end
    _ENDPARAM = "DTEND"


class Journal(CalendarObjectResource):
    """
    The `Journal` object is used to represent a journal entry (VJOURNAL).

    As of 2020-12 it adds nothing to the inheritated class.  (I have
    frequently asked myself if we need those subclasses ... perhaps
    not)
    """

    pass


class FreeBusy(CalendarObjectResource):
    """
    The `FreeBusy` object is used to represent a freebusy response from
    the server.  __init__ is overridden, as a FreeBusy response has no
    URL or ID.  The inheritated methods .save and .load is moot and
    will probably throw errors (perhaps the class hierarchy should be
    rethought, to prevent the FreeBusy from inheritating moot methods)

    Update: With RFC6638 a freebusy object can have a URL and an ID.
    """

    def __init__(
        self,
        parent,
        data,
        url: Union[str, ParseResult, SplitResult, URL, None] = None,
        id: Optional[Any] = None,
    ) -> None:
        CalendarObjectResource.__init__(
            self, client=parent.client, url=url, data=data, parent=parent, id=id
        )


class Todo(CalendarObjectResource):
    """The `Todo` object is used to represent a todo item (VTODO).  A
    Todo-object can be completed.

    There is some extra logic here - arguably none of it belongs to
    the caldav library, and should be moved either to the icalendar
    library or to the plann library (plann is a cli-tool, should
    probably be split up into one library for advanced calendaring
    operations and the cli-tool as separate packages)
    """

    _ENDPARAM = "DUE"

    def _next(self, ts=None, i=None, dtstart=None, rrule=None, by=None, no_count=True):
        """Special logic to fint the next DTSTART of a recurring
        just-completed task.

        If any BY*-parameters are present, assume the task should have
        fixed deadlines and preserve information from the previous
        dtstart.  If no BY*-parameters are present, assume the
        frequency is meant to be the interval between the tasks.

        Examples:

        1) Garbage collection happens every week on a Tuesday, but
        never earlier than 09 in the morning.  Hence, it may be
        important to take out the thrash Monday evenings or Tuesday
        morning.  DTSTART of the original task is set to Tuesday
        2022-11-01T08:50, DUE to 09:00.

        1A) Task is completed 07:50 on the 1st of November.  Next
        DTSTART should be Tuesday the 7th of November at 08:50.

        1B) Task is completed 09:15 on the 1st of November (which is
        probably OK, since they usually don't come before 09:30).
        Next DTSTART should be Tuesday the 7th of November at 08:50.

        1C) Task is completed at the 5th of November.  We've lost the
        DUE, but the calendar has no idea weather the DUE was a very
        hard due or not - and anyway, probably we'd like to do it
        again on Tuesday, so next DTSTART should be Tuesday the 7th of
        November at 08:50.

        1D) Task is completed at the 7th of November at 07:50.  Next
        DTSTART should be one hour later.  Now, this is very silly,
        but an algorithm cannot do guesswork on weather it's silly or
        not.  If DTSTART would be set to the earliest possible time
        one could start thinking on this task (like, Monday evening),
        then we would get Tue the 14th of November, which does make
        sense.  Unfortunately the icalendar standard does not specify
        what should be used for DTSTART and DURATION/DUE.

        1E) Task is completed on the 7th of November at 08:55.  This
        efficiently means we've lost the 1st of November recurrence
        but have done the 7th of November recurrence instead, so next
        timestamp will be the 14th of November.

        2) Floors at home should be cleaned like once a week, but
        there is no fixed deadline for it.  For some people it may
        make sense to have a routine doing it i.e. every Tuesday, but
        this is not a strict requirement.  If it wasn't done one
        Tuesday, it's probably even more important to do it Wednesday.
        If the floor was cleaned on a Saturday, it probably doesn't
        make sense cleaning it again on Tuesday, but it probably
        shouldn't wait until next Tuesday.  Rrule is set to
        FREQ=WEEKLY, but without any BYDAY.  The original VTODO is set
        up with DTSTART 16:00 on Tuesday the 1st of November and DUE
        17:00.  After 17:00 there will be dinner, so best to get it
        done before that.

        2A) Floor cleaning was finished 14:30.  The next recurrence
        has DTSTART set to 13:30 (and DUE set to 14:30).  The idea
        here is that since the floor starts accumulating dirt right
        after 14:30, obviously it is overdue at 16:00 Tuesday the 7th.

        2B) Floor cleaning was procrastinated with one day and
        finished Wednesday at 14:30.  Next instance will be Wednesday
        in a week, at 14:30.

        2C) Floor cleaning was procrastinated with two weeks and
        finished Tuesday the 14th at 14:30. Next instance will be
        Tuesday the 21st at 14:30.

        While scenario 2 is the most trivial to implement, it may not
        be the correct understanding of the RFC, and it may be tricky
        to get the RECURRENCE-ID set correctly.

        """
        if not i:
            i = self.icalendar_component
        if not rrule:
            rrule = i["RRULE"]
        if not dtstart:
            if by is True or (
                by is None and any((x for x in rrule if x.startswith("BY")))
            ):
                if "DTSTART" in i:
                    dtstart = i["DTSTART"].dt
                else:
                    dtstart = ts or datetime.now()
            else:
                dtstart = ts or datetime.now() - self._get_duration(i)
        ## dtstart should be compared to the completion timestamp, which
        ## is set in UTC in the complete() method.  However, dtstart
        ## may be a naïve or a floating timestamp
        ## (TODO: what if it's a date?)
        ## (TODO: we need test code for those corner cases!)
        if hasattr(dtstart, "astimezone"):
            dtstart = dtstart.astimezone(timezone.utc)
        if not ts:
            ts = dtstart
        ## Counting is taken care of other places
        if no_count and "COUNT" in rrule:
            rrule = rrule.copy()
            rrule.pop("COUNT")
        rrule = rrulestr(rrule.to_ical().decode("utf-8"), dtstart=dtstart)
        return rrule.after(ts)

    def _reduce_count(self, i=None) -> bool:
        if not i:
            i = self.icalendar_component
        if "COUNT" in i["RRULE"]:
            if i["RRULE"]["COUNT"][0] == 1:
                return False
            i["RRULE"]["COUNT"][0] -= 1
        return True

    def _complete_recurring_safe(self, completion_timestamp):
        """This mode will create a new independent task which is
        marked as completed, and modify the existing recurring task.
        It is probably the most safe way to handle the completion of a
        recurrence of a recurring task, though the link between the
        completed task and the original task is lost.
        """
        ## If count is one, then it is not really recurring
        if not self._reduce_count():
            return self.complete(handle_rrule=False)
        next_dtstart = self._next(completion_timestamp)
        if not next_dtstart:
            return self.complete(handle_rrule=False)

        completed = self.copy()
        completed.url = self.parent.url.join(completed.id + ".ics")
        completed.icalendar_component.pop("RRULE")
        completed.save()
        completed.complete()

        duration = self.get_duration()
        i = self.icalendar_component
        i.pop("DTSTART", None)
        i.add("DTSTART", next_dtstart)
        self.set_duration(duration, movable_attr="DUE")

        self.save()

    def _complete_recurring_thisandfuture(self, completion_timestamp) -> None:
        """The RFC is not much helpful, a lot of guesswork is needed
        to consider what the "right thing" to do wrg of a completion of
        recurring tasks is ... but this is my shot at it.

        1) The original, with rrule, will be kept as it is.  The rrule
        string is fetched from the first subcomponent of the
        icalendar.

        2) If there are multiple recurrence instances in subcomponents
        and the last one is marked with RANGE=THISANDFUTURE, then
        select this one.  If it has the rrule property set, use this
        rrule rather than the original one.  Drop the RANGE parameter.
        Calculate the next RECURRENCE-ID from the DTSTART of this
        object.  Mark task as completed.  Increase SEQUENCE.

        3) Create a new recurrence instance with RANGE=THISANDFUTURE,
        without RRULE set (Ref
        https://github.com/Kozea/Radicale/issues/1264).  Set the
        RECURRENCE-ID to the one calculated in #2.  Calculate the
        DTSTART based on rrule and completion timestamp/date.
        """
        recurrences = self.icalendar_instance.subcomponents
        orig = recurrences[0]
        if "STATUS" not in orig:
            orig["STATUS"] = "NEEDS-ACTION"

        if len(recurrences) == 1:
            ## We copy the original one
            just_completed = orig.copy()
            just_completed.pop("RRULE")
            just_completed.add(
                "RECURRENCE-ID", orig.get("DTSTART", completion_timestamp)
            )
            seqno = just_completed.pop("SEQUENCE", 0)
            just_completed.add("SEQUENCE", seqno + 1)
            recurrences.append(just_completed)

        prev = recurrences[-1]
        rrule = prev.get("RRULE", orig["RRULE"])
        thisandfuture = prev.copy()
        seqno = thisandfuture.pop("SEQUENCE", 0)
        thisandfuture.add("SEQUENCE", seqno + 1)

        ## If we have multiple recurrences, assume the last one is a THISANDFUTURE.
        ## (Otherwise, the data is coming from another client ...)
        ## The RANGE parameter needs to be removed
        if len(recurrences) > 2:
            if prev["RECURRENCE-ID"].params.get("RANGE", None) == "THISANDFUTURE":
                prev["RECURRENCE-ID"].params.pop("RANGE")
            else:
                raise NotImplementedError(
                    "multiple instances found, but last one is not of type THISANDFUTURE, possibly this has been created by some incompatible client, but we should deal with it"
                )
        self._complete_ical(prev, completion_timestamp)

        thisandfuture.pop("RECURRENCE-ID", None)
        thisandfuture.add("RECURRENCE-ID", self._next(i=prev, rrule=rrule))
        thisandfuture["RECURRENCE-ID"].params["RANGE"] = "THISANDFUTURE"
        rrule2 = thisandfuture.pop("RRULE", None)

        ## Counting logic
        if rrule2 is not None:
            count = rrule2.get("COUNT", None)
            if count is not None and count[0] in (0, 1):
                for i in recurrences:
                    self._complete_ical(i, completion_timestamp=completion_timestamp)
            thisandfuture.add("RRULE", rrule2)
        else:
            count = rrule.get("COUNT", None)
            if count is not None and count[0] <= len(
                [x for x in recurrences if not self.is_pending(x)]
            ):
                self._complete_ical(
                    recurrences[0], completion_timestamp=completion_timestamp
                )
                self.save(increase_seqno=False)
                return

        rrule = rrule2 or rrule

        duration = self._get_duration(i=prev)
        thisandfuture.pop("DTSTART", None)
        thisandfuture.pop("DUE", None)
        next_dtstart = self._next(i=prev, rrule=rrule, ts=completion_timestamp)
        thisandfuture.add("DTSTART", next_dtstart)
        self._set_duration(i=thisandfuture, duration=duration, movable_attr="DUE")
        self.icalendar_instance.subcomponents.append(thisandfuture)
        self.save(increase_seqno=False)

    def complete(
        self,
        completion_timestamp: Optional[datetime] = None,
        handle_rrule: bool = False,
        rrule_mode: Literal["safe", "this_and_future"] = "safe",
    ) -> None:
        """Marks the task as completed.

        Parameters
        ----------
        completion_timestamp : datetime
            Defaults to ``datetime.now()``.
        handle_rrule : Bool
            If set to True, the library will try to be smart if
            the task is recurring.  The default is False, for backward
            compatibility.  I may consider making this one mandatory.
        rrule_mode : str
            The RFC leaves a lot of room for interpretation on how
            to handle recurring tasks, and what works on one server may break at
            another.  The following modes are accepted:
            * this_and_future - see doc for _complete_recurring_thisandfuture for details
            * safe - see doc for _complete_recurring_safe for details
        """
        if not completion_timestamp:
            completion_timestamp = datetime.now(timezone.utc)

        if "RRULE" in self.icalendar_component and handle_rrule:
            return getattr(self, "_complete_recurring_%s" % rrule_mode)(
                completion_timestamp
            )
        self._complete_ical(completion_timestamp=completion_timestamp)
        self.save()

    def _complete_ical(self, i=None, completion_timestamp=None) -> None:
        if i is None:
            i = self.icalendar_component
        assert self.is_pending(i)
        status = i.pop("STATUS", None)
        i.add("STATUS", "COMPLETED")
        i.add("COMPLETED", completion_timestamp)

    def is_pending(self, i=None) -> Optional[bool]:
        if i is None:
            i = self.icalendar_component
        if i.get("COMPLETED", None) is not None:
            return False
        if i.get("STATUS", "NEEDS-ACTION") in ("NEEDS-ACTION", "IN-PROCESS"):
            return True
        if i.get("STATUS", "NEEDS-ACTION") in ("CANCELLED", "COMPLETED"):
            return False
        ## input data does not conform to the RFC
        assert False

    def uncomplete(self) -> None:
        """Undo completion - marks a completed task as not completed"""
        ### TODO: needs test code for code coverage!
        ## (it has been tested through the calendar-cli test code)
        if "status" in self.icalendar_component:
            self.icalendar_component.pop("status")
        self.icalendar_component.add("status", "NEEDS-ACTION")
        if "completed" in self.icalendar_component:
            self.icalendar_component.pop("completed")
        self.save()

    ## TODO: should be moved up to the base class
    def set_duration(self, duration, movable_attr="DTSTART"):
        """
        If DTSTART and DUE/DTEND is already set, one of them should be moved.  Which one?  I believe that for EVENTS, the DTSTART should remain constant and DTEND should be moved, but for a task, I think the due date may be a hard deadline, hence by default we'll move DTSTART.

        TODO: can this be written in a better/shorter way?

        WARNING: this method may be deprecated and moved to
        the icalendar library at some point in the future.
        """
        i = self.icalendar_component
        return self._set_duration(i, duration, movable_attr)

    def _set_duration(self, i, duration, movable_attr="DTSTART") -> None:
        if ("DUE" in i or "DURATION" in i) and "DTSTART" in i:
            i.pop(movable_attr, None)
            if movable_attr == "DUE":
                i.pop("DURATION", None)
            if movable_attr == "DTSTART":
                i.add("DTSTART", i["DUE"].dt - duration)
            elif movable_attr == "DUE":
                i.add("DUE", i["DTSTART"].dt + duration)
        elif "DUE" in i:
            i.add("DTSTART", i["DUE"].dt - duration)
        elif "DTSTART" in i:
            i.add("DUE", i["DTSTART"].dt + duration)
        else:
            if "DURATION" in i:
                i.pop("DURATION")
            i.add("DURATION", duration)

    def set_due(self, due, move_dtstart=False, check_dependent=False):
        """The RFC specifies that a VTODO cannot have both due and
        duration, so when setting due, the duration field must be
        evicted

        check_dependent=True will raise some error if there exists a
        parent calendar component (through RELATED-TO), and the parents
        due or dtend is before the new dtend).

        WARNING: this method may become deprecated and parts of
        it moved to the icalendar library at some point in the future.

        WARNING: the check_dependent-logic may be rewritten to support
        RFC9253 in 3.x
        """
        i = self.icalendar_component
        if hasattr(due, "tzinfo") and not due.tzinfo:
            due = due.astimezone(timezone.utc)
        if check_dependent:
            parents = self.get_relatives({"PARENT"})
            for parent in parents["PARENT"]:
                pend = parent.get_dtend()
                ## Make sure both timestamps aren't "naive":
                if hasattr(pend, "tzinfo") and not pend.tzinfo:
                    pend = pend.astimezone(timezone.utc)
                ## pend and due may be date and datetime, then they cannot be compared directly
                if pend and pend.strftime("%s") < due.strftime("%s"):
                    if check_dependent == "return":
                        return parent
                    raise error.ConsistencyError(
                        "parent object has due/end %s, cannot procrastinate child object without first procrastinating parent object"
                    )
        CalendarObjectResource.set_end(self, due, move_dtstart)

    set_end = set_due
