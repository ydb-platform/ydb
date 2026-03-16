"""The base for :rfc:`5545` components."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Literal, overload

from icalendar.attr import (
    CONCEPTS_TYPE_SETTER,
    LINKS_TYPE_SETTER,
    RELATED_TO_TYPE_SETTER,
    comments_property,
    concepts_property,
    links_property,
    refids_property,
    related_to_property,
    single_utc_property,
    uid_property,
)
from icalendar.cal.component_factory import ComponentFactory
from icalendar.caselessdict import CaselessDict
from icalendar.error import InvalidCalendar, JCalParsingError
from icalendar.parser import (
    Contentline,
    Contentlines,
    Parameters,
    q_join,
    q_split,
)
from icalendar.parser_tools import DEFAULT_ENCODING
from icalendar.prop import VPROPERTY, TypesFactory, vDDDLists, vText
from icalendar.timezone import tzp
from icalendar.tools import is_date

if TYPE_CHECKING:
    from icalendar.compatibility import Self

_marker = []


class Component(CaselessDict):
    """Base class for calendar components.

    Component is the base object for calendar, Event and the other
    components defined in :rfc:`5545`. Normally you will not use this class
    directly, but rather one of the subclasses.
    """

    name: ClassVar[str | None] = None
    """The name of the component.

    This should be defined in each component class.

    Example: ``VCALENDAR``.
    """

    required: ClassVar[tuple[()]] = ()
    """These properties are required."""

    singletons: ClassVar[tuple[()]] = ()
    """These properties must appear only once."""

    multiple: ClassVar[tuple[()]] = ()
    """These properties may occur more than once."""

    exclusive: ClassVar[tuple[()]] = ()
    """These properties are mutually exclusive."""

    inclusive: ClassVar[(tuple[str] | tuple[tuple[str, str]])] = ()
    """These properties are inclusive.

    In other words, if the first property in the tuple occurs, then the
    second one must also occur.

    Example:

        .. code-block:: python

            ('duration', 'repeat')
    """

    ignore_exceptions: ClassVar[bool] = False
    """Whether or not to ignore exceptions when parsing.

    If ``True``, and this component can't be parsed, then it will silently
    ignore it, rather than let the exception propagate upwards.
    """

    types_factory: ClassVar[TypesFactory] = TypesFactory.instance()
    _components_factory: ClassVar[ComponentFactory | None] = None

    subcomponents: list[Component]
    """All subcomponents of this component."""

    @classmethod
    def get_component_class(cls, name: str) -> type[Component]:
        """Return a component with this name.

        Parameters:
            name: Name of the component, i.e. ``VCALENDAR``
        """
        if cls._components_factory is None:
            cls._components_factory = ComponentFactory()
        return cls._components_factory.get_component_class(name)

    @classmethod
    def register(cls, component_class: type[Component]) -> None:
        """Register a custom component class.

        Parameters:
            component_class: Component subclass to register.
                Must have a ``name`` attribute.

        Raises:
            ValueError: If ``component_class`` has no ``name`` attribute.
            ValueError: If a component with this name is already registered.

        Examples:
            Create a custom icalendar component with the name ``X-EXAMPLE``:

            .. code-block:: pycon

                >>> from icalendar import Component
                >>> class XExample(Component):
                ...     name = "X-EXAMPLE"
                ...     def custom_method(self):
                ...         return "custom"
                >>> Component.register(XExample)
        """
        if not hasattr(component_class, "name") or component_class.name is None:
            raise ValueError(f"{component_class} must have a 'name' attribute")

        if cls._components_factory is None:
            cls._components_factory = ComponentFactory()

        # Check if already registered
        existing = cls._components_factory.get(component_class.name)
        if existing is not None and existing is not component_class:
            raise ValueError(
                f"Component '{component_class.name}' is already registered"
                f" as {existing}"
            )

        cls._components_factory.add_component_class(component_class)

    @staticmethod
    def _infer_value_type(
        value: date | datetime | timedelta | time | tuple | list,
    ) -> str | None:
        """Infer the ``VALUE`` parameter from a Python type.

        Parameters:
            value: Python native type, one of :py:class:`date`, :py:mod:`datetime`,
                :py:class:`timedelta`, :py:mod:`time`, :py:class:`tuple`,
                or :py:class:`list`.

        Returns:
            str or None: The ``VALUE`` parameter string, for example, "DATE",
                "TIME", or other string, or ``None``
                if no specific ``VALUE`` is needed.
        """
        if isinstance(value, list):
            if not value:
                return None
            # Check if ALL items are date (but not datetime)
            if all(is_date(item) for item in value):
                return "DATE"
            # Check if ALL items are time
            if all(isinstance(item, time) for item in value):
                return "TIME"
            # Mixed types or other types - don't infer
            return None
        if is_date(value):
            return "DATE"
        if isinstance(value, time):
            return "TIME"
        # Don't infer PERIOD - it's too risky and vPeriod already handles it
        return None

    def __init__(self, *args, **kwargs):
        """Set keys to upper for initial dict."""
        super().__init__(*args, **kwargs)
        # set parameters here for properties that use non-default values
        self.subcomponents: list[Component] = []  # Components can be nested.
        self.errors = []  # If we ignored exception(s) while
        # parsing a property, contains error strings

    def __bool__(self):
        """Returns True, CaselessDict would return False if it had no items."""
        return True

    def __getitem__(self, key):
        """Get property value from the component dictionary."""
        return super().__getitem__(key)

    def get(self, key, default=None):
        """Get property value with default."""
        try:
            return self[key]
        except KeyError:
            return default

    def is_empty(self):
        """Returns True if Component has no items or subcomponents, else False."""
        return bool(not list(self.values()) + self.subcomponents)

    #############################
    # handling of property values

    @classmethod
    def _encode(cls, name, value, parameters=None, encode=1):
        """Encode values to icalendar property values.

        :param name: Name of the property.
        :type name: string

        :param value: Value of the property. Either of a basic Python type of
                      any of the icalendar's own property types.
        :type value: Python native type or icalendar property type.

        :param parameters: Property parameter dictionary for the value. Only
                           available, if encode is set to True.
        :type parameters: Dictionary

        :param encode: True, if the value should be encoded to one of
                       icalendar's own property types (Fallback is "vText")
                       or False, if not.
        :type encode: Boolean

        :returns: icalendar property value
        """
        if not encode:
            return value
        if isinstance(value, cls.types_factory.all_types):
            # Don't encode already encoded values.
            obj = value
        else:
            # Extract VALUE parameter if present, or infer it from the Python type
            value_param = None
            if parameters and "VALUE" in parameters:
                value_param = parameters["VALUE"]
            elif not isinstance(value, cls.types_factory.all_types):
                inferred = cls._infer_value_type(value)
                if inferred:
                    value_param = inferred
                    # Auto-set the VALUE parameter
                    if parameters is None:
                        parameters = {}
                    if "VALUE" not in parameters:
                        parameters["VALUE"] = inferred

            klass = cls.types_factory.for_property(name, value_param)
            obj = klass(value)
        if parameters:
            if not hasattr(obj, "params"):
                obj.params = Parameters()
            for key, item in parameters.items():
                if item is None:
                    if key in obj.params:
                        del obj.params[key]
                else:
                    obj.params[key] = item
        return obj

    def add(
        self,
        name: str,
        value,
        parameters: dict[str, str] | Parameters = None,
        encode: bool = True,
    ):
        """Add a property.

        :param name: Name of the property.
        :type name: string

        :param value: Value of the property. Either of a basic Python type of
                      any of the icalendar's own property types.
        :type value: Python native type or icalendar property type.

        :param parameters: Property parameter dictionary for the value. Only
                           available, if encode is set to True.
        :type parameters: Dictionary

        :param encode: True, if the value should be encoded to one of
                       icalendar's own property types (Fallback is "vText")
                       or False, if not.
        :type encode: Boolean

        :returns: None
        """
        if isinstance(value, datetime) and name.lower() in (
            "dtstamp",
            "created",
            "last-modified",
        ):
            # RFC expects UTC for those... force value conversion.
            value = tzp.localize_utc(value)

        # encode value
        if (
            encode
            and isinstance(value, list)
            and name.lower() not in ["rdate", "exdate", "categories"]
        ):
            # Individually convert each value to an ical type except rdate and
            # exdate, where lists of dates might be passed to vDDDLists.
            value = [self._encode(name, v, parameters, encode) for v in value]
        else:
            value = self._encode(name, value, parameters, encode)

        # set value
        if name in self:
            # If property already exists, append it.
            oldval = self[name]
            if isinstance(oldval, list):
                if isinstance(value, list):
                    value = oldval + value
                else:
                    oldval.append(value)
                    value = oldval
            else:
                value = [oldval, value]
        self[name] = value

    def _decode(self, name: str, value: VPROPERTY):
        """Internal for decoding property values."""

        # TODO: Currently the decoded method calls the icalendar.prop instances
        # from_ical. We probably want to decode properties into Python native
        # types here. But when parsing from an ical string with from_ical, we
        # want to encode the string into a real icalendar.prop property.
        if hasattr(value, "ical_value"):
            return value.ical_value
        if isinstance(value, vDDDLists):
            # TODO: Workaround unfinished decoding
            return value
        decoded = self.types_factory.from_ical(name, value)
        # TODO: remove when proper decoded is implemented in every prop.* class
        # Workaround to decode vText properly
        if isinstance(decoded, vText):
            decoded = decoded.encode(DEFAULT_ENCODING)
        return decoded

    def decoded(self, name: str, default: Any = _marker) -> Any:
        """Returns decoded value of property.

        A component maps keys to icalendar property value types.
        This function returns values compatible to native Python types.
        """
        if name in self:
            value = self[name]
            if isinstance(value, list):
                return [self._decode(name, v) for v in value]
            return self._decode(name, value)
        if default is _marker:
            raise KeyError(name)
        return default

    ########################################################################
    # Inline values. A few properties have multiple values inlined in in one
    # property line. These methods are used for splitting and joining these.

    def get_inline(self, name, decode=1):
        """Returns a list of values (split on comma)."""
        vals = [v.strip('" ') for v in q_split(self[name])]
        if decode:
            return [self._decode(name, val) for val in vals]
        return vals

    def set_inline(self, name, values, encode=1):
        """Converts a list of values into comma separated string and sets value
        to that.
        """
        if encode:
            values = [self._encode(name, value, encode=1) for value in values]
        self[name] = self.types_factory["inline"](q_join(values))

    #########################
    # Handling of components

    def add_component(self, component: Component):
        """Add a subcomponent to this component."""
        self.subcomponents.append(component)

    def _walk(self, name, select):
        """Walk to given component."""
        result = []
        if (name is None or self.name == name) and select(self):
            result.append(self)
        for subcomponent in self.subcomponents:
            result += subcomponent._walk(name, select)
        return result

    def walk(self, name=None, select=lambda _: True) -> list[Component]:
        """Recursively traverses component and subcomponents. Returns sequence
        of same. If name is passed, only components with name will be returned.

        :param name: The name of the component or None such as ``VEVENT``.
        :param select: A function that takes the component as first argument
          and returns True/False.
        :returns: A list of components that match.
        :rtype: list[Component]
        """
        if name is not None:
            name = name.upper()
        return self._walk(name, select)

    def with_uid(self, uid: str) -> list[Component]:
        """Return a list of components with the given UID.

        Parameters:
            uid: The UID of the component.

        Returns:
            list[Component]: List of components with the given UID.
        """
        return [c for c in self.walk() if c.get("uid") == uid]

    #####################
    # Generation

    def property_items(
        self,
        recursive=True,
        sorted: bool = True,
    ) -> list[tuple[str, object]]:
        """Returns properties in this component and subcomponents as:
        [(name, value), ...]
        """
        v_text = self.types_factory["text"]
        properties = [("BEGIN", v_text(self.name).to_ical())]
        property_names = self.sorted_keys() if sorted else self.keys()

        for name in property_names:
            values = self[name]
            if isinstance(values, list):
                # normally one property is one line
                for value in values:
                    properties.append((name, value))
            else:
                properties.append((name, values))
        if recursive:
            # recursion is fun!
            for subcomponent in self.subcomponents:
                properties += subcomponent.property_items(sorted=sorted)
        properties.append(("END", v_text(self.name).to_ical()))
        return properties

    @overload
    @classmethod
    def from_ical(
        cls, st: str | bytes, multiple: Literal[False] = False
    ) -> Component: ...

    @overload
    @classmethod
    def from_ical(cls, st: str | bytes, multiple: Literal[True]) -> list[Component]: ...

    @classmethod
    def from_ical(
        cls, st: str | bytes, multiple: bool = False
    ) -> Component | list[Component]:
        """Parse iCalendar data into component instances.

        Handles standard and custom components (``X-*``, IANA-registered).

        Parameters:
            st: iCalendar data as bytes or string
            multiple: If ``True``, returns list. If ``False``, returns single component.

        Returns:
            Component or list of components

        See Also:
            :doc:`/how-to/custom-components` for examples of parsing custom components
        """
        from icalendar.prop import vBroken

        stack = []  # a stack of components
        comps = []
        for line in Contentlines.from_ical(st):  # raw parsing
            if not line:
                continue

            try:
                name, params, vals = line.parts()
            except ValueError as e:
                # if unable to parse a line within a component
                # that ignores exceptions, mark the component
                # as broken and skip the line. otherwise raise.
                component = stack[-1] if stack else None
                if not component or not component.ignore_exceptions:
                    raise
                component.errors.append((None, str(e)))
                continue

            uname = name.upper()
            # check for start of component
            if uname == "BEGIN":
                # try and create one of the components defined in the spec,
                # otherwise get a general Components for robustness.
                c_name = vals.upper()
                c_class = cls.get_component_class(c_name)
                # If component factory cannot resolve ``c_name``, the generic
                # ``Component`` class is used which does not have the name set.
                # That's opposed to the usage of ``cls``, which represents a
                # more concrete subclass with a name set (e.g. VCALENDAR).
                component = c_class()
                if not getattr(component, "name", ""):  # undefined components
                    component.name = c_name
                stack.append(component)
            # check for end of event
            elif uname == "END":
                # we are done adding properties to this component
                # so pop it from the stack and add it to the new top.
                if not stack:
                    # The stack is currently empty, the input must be invalid
                    raise ValueError("END encountered without an accompanying BEGIN!")

                component = stack.pop()
                if not stack:  # we are at the end
                    comps.append(component)
                else:
                    stack[-1].add_component(component)
                if vals == "VTIMEZONE" and "TZID" in component:
                    tzp.cache_timezone_component(component)
            # we are adding properties to the current top of the stack
            else:
                # Extract VALUE parameter if present
                value_param = params.get("VALUE") if params else None
                factory = cls.types_factory.for_property(name, value_param)
                component = stack[-1] if stack else None
                if not component:
                    # only accept X-COMMENT at the end of the .ics file
                    # ignore these components in parsing
                    if uname == "X-COMMENT":
                        break
                    raise ValueError(
                        f'Property "{name}" does not have a parent component.'
                    )
                datetime_names = (
                    "DTSTART",
                    "DTEND",
                    "RECURRENCE-ID",
                    "DUE",
                    "RDATE",
                    "EXDATE",
                )

                # Determine TZID for datetime properties
                tzid = params.get("TZID") if params and name in datetime_names else None

                # Handle special cases for value list preparation
                if name.upper() == "CATEGORIES":
                    # Special handling for CATEGORIES - need raw value
                    # before unescaping to properly split on unescaped commas
                    from icalendar.parser import (
                        split_on_unescaped_comma,
                    )

                    line_str = str(line)
                    # Use rfind to get the last colon (value separator)
                    # to handle parameters with colons like ALTREP="http://..."
                    colon_idx = line_str.rfind(":")
                    if colon_idx > 0:
                        raw_value = line_str[colon_idx + 1 :]
                        # Parse categories immediately (not lazily) for both
                        # strict and tolerant components.
                        # CATEGORIES needs special comma handling
                        try:
                            category_list = split_on_unescaped_comma(raw_value)
                            vals_inst = factory(category_list)
                            vals_inst.params = params
                            component.add(name, vals_inst, encode=0)
                        except ValueError as e:
                            if not component.ignore_exceptions:
                                raise
                            component.errors.append((uname, str(e)))
                        continue
                    # Fallback to normal processing if we can't find colon
                    vals_list = [vals]
                elif name == "FREEBUSY":
                    # Handle FREEBUSY comma-separated values
                    vals_list = vals.split(",")
                # Workaround broken ICS files with empty RDATE
                # (not EXDATE - let it parse and fail)
                elif name == "RDATE" and vals == "":
                    vals_list = []
                else:
                    vals_list = [vals]

                # Parse all properties eagerly
                for val in vals_list:
                    try:
                        if tzid:
                            parsed_val = factory.from_ical(val, tzid)
                        else:
                            parsed_val = factory.from_ical(val)
                        vals_inst = factory(parsed_val)
                        vals_inst.params = params
                        component.add(name, vals_inst, encode=0)
                    except Exception as e:
                        if not component.ignore_exceptions:
                            raise
                        # Error-tolerant mode: create vBroken
                        expected_type = getattr(factory, "__name__", "unknown")
                        broken_prop = vBroken.from_parse_error(
                            raw_value=val,
                            params=params,
                            property_name=name,
                            expected_type=expected_type,
                            error=e,
                        )
                        component.errors.append((name, str(e)))
                        component.add(name, broken_prop, encode=0)

        if multiple:
            return comps
        if len(comps) > 1:
            raise ValueError(
                cls._format_error(
                    "Found multiple components where only one is allowed", st
                )
            )
        if len(comps) < 1:
            raise ValueError(
                cls._format_error(
                    "Found no components where exactly one is required", st
                )
            )
        return comps[0]

    @staticmethod
    def _format_error(error_description, bad_input, elipsis="[...]"):
        # there's three character more in the error, ie. ' ' x2 and a ':'
        max_error_length = 100 - 3
        if len(error_description) + len(bad_input) + len(elipsis) > max_error_length:
            truncate_to = max_error_length - len(error_description) - len(elipsis)
            return f"{error_description}: {bad_input[:truncate_to]} {elipsis}"
        return f"{error_description}: {bad_input}"

    def content_line(self, name, value, sorted: bool = True):
        """Returns property as content line."""
        params = getattr(value, "params", Parameters())
        return Contentline.from_parts(name, params, value, sorted=sorted)

    def content_lines(self, sorted: bool = True):
        """Converts the Component and subcomponents into content lines."""
        contentlines = Contentlines()
        for name, value in self.property_items(sorted=sorted):
            cl = self.content_line(name, value, sorted=sorted)
            contentlines.append(cl)
        contentlines.append("")  # remember the empty string in the end
        return contentlines

    def to_ical(self, sorted: bool = True):
        """
        :param sorted: Whether parameters and properties should be
                       lexicographically sorted.
        """

        content_lines = self.content_lines(sorted=sorted)
        return content_lines.to_ical()

    def __repr__(self):
        """String representation of class with all of it's subcomponents."""
        subs = ", ".join(str(it) for it in self.subcomponents)
        return (
            f"{self.name or type(self).__name__}"
            f"({dict(self)}{', ' + subs if subs else ''})"
        )

    def __eq__(self, other):
        if len(self.subcomponents) != len(other.subcomponents):
            return False

        properties_equal = super().__eq__(other)
        if not properties_equal:
            return False

        # The subcomponents might not be in the same order,
        # neither there's a natural key we can sort the subcomponents by nor
        # are the subcomponent types hashable, so  we cant put them in a set to
        # check for set equivalence. We have to iterate over the subcomponents
        # and look for each of them in the list.
        for subcomponent in self.subcomponents:
            if subcomponent not in other.subcomponents:
                return False

        return True

    DTSTAMP = stamp = single_utc_property(
        "DTSTAMP",
        """RFC 5545:

        Conformance:  This property MUST be included in the "VEVENT",
        "VTODO", "VJOURNAL", or "VFREEBUSY" calendar components.

        Description: In the case of an iCalendar object that specifies a
        "METHOD" property, this property specifies the date and time that
        the instance of the iCalendar object was created.  In the case of
        an iCalendar object that doesn't specify a "METHOD" property, this
        property specifies the date and time that the information
        associated with the calendar component was last revised in the
        calendar store.

        The value MUST be specified in the UTC time format.

        In the case of an iCalendar object that doesn't specify a "METHOD"
        property, this property is equivalent to the "LAST-MODIFIED"
        property.
    """,
    )
    LAST_MODIFIED = single_utc_property(
        "LAST-MODIFIED",
        """RFC 5545:

        Purpose:  This property specifies the date and time that the
        information associated with the calendar component was last
        revised in the calendar store.

        Note: This is analogous to the modification date and time for a
        file in the file system.

        Conformance:  This property can be specified in the "VEVENT",
        "VTODO", "VJOURNAL", or "VTIMEZONE" calendar components.
    """,
    )

    @property
    def last_modified(self) -> datetime:
        """Datetime when the information associated with the component was last revised.

        Since :attr:`LAST_MODIFIED` is an optional property,
        this returns :attr:`DTSTAMP` if :attr:`LAST_MODIFIED` is not set.
        """
        return self.LAST_MODIFIED or self.DTSTAMP

    @last_modified.setter
    def last_modified(self, value):
        self.LAST_MODIFIED = value

    @last_modified.deleter
    def last_modified(self):
        del self.LAST_MODIFIED

    @property
    def created(self) -> datetime:
        """Datetime when the information associated with the component was created.

        Since :attr:`CREATED` is an optional property,
        this returns :attr:`DTSTAMP` if :attr:`CREATED` is not set.
        """
        return self.CREATED or self.DTSTAMP

    @created.setter
    def created(self, value):
        self.CREATED = value

    @created.deleter
    def created(self):
        del self.CREATED

    def is_thunderbird(self) -> bool:
        """Whether this component has attributes that indicate that Mozilla Thunderbird created it."""
        return any(attr.startswith("X-MOZ-") for attr in self.keys())

    @staticmethod
    def _utc_now():
        """Return now as UTC value."""
        return datetime.now(timezone.utc)

    uid = uid_property
    comments = comments_property
    links = links_property
    related_to = related_to_property
    concepts = concepts_property
    refids = refids_property

    CREATED = single_utc_property(
        "CREATED",
        """
        CREATED specifies the date and time that the calendar
        information was created by the calendar user agent in the calendar
        store.

        Conformance:
            The property can be specified once in "VEVENT",
            "VTODO", or "VJOURNAL" calendar components.  The value MUST be
            specified as a date with UTC time.

        """,
    )

    _validate_new = True

    @staticmethod
    def _validate_start_and_end(start, end):
        """This validates start and end.

        Raises:
            ~error.InvalidCalendar: If the information is not valid
        """
        if start is None or end is None:
            return
        if start > end:
            raise InvalidCalendar("end must be after start")

    @classmethod
    def new(
        cls,
        created: date | None = None,
        comments: list[str] | str | None = None,
        concepts: CONCEPTS_TYPE_SETTER = None,
        last_modified: date | None = None,
        links: LINKS_TYPE_SETTER = None,
        refids: list[str] | str | None = None,
        related_to: RELATED_TO_TYPE_SETTER = None,
        stamp: date | None = None,
    ) -> Component:
        """Create a new component.

        Parameters:
            comments: The :attr:`comments` of the component.
            concepts: The :attr:`concepts` of the component.
            created: The :attr:`created` of the component.
            last_modified: The :attr:`last_modified` of the component.
            links: The :attr:`links` of the component.
            related_to: The :attr:`related_to` of the component.
            stamp: The :attr:`DTSTAMP` of the component.

        Raises:
            ~error.InvalidCalendar: If the content is not valid
                according to :rfc:`5545`.

        .. warning:: As time progresses, we will be stricter with the
            validation.
        """
        component = cls()
        component.DTSTAMP = stamp
        component.created = created
        component.last_modified = last_modified
        component.comments = comments
        component.links = links
        component.related_to = related_to
        component.concepts = concepts
        component.refids = refids
        return component

    def to_jcal(self) -> list:
        """Convert this component to a jCal object.

        Returns:
            jCal object

        See also :attr:`to_json`.

        In this example, we create a simple VEVENT component and convert it to jCal:

        .. code-block:: pycon

            >>> from icalendar import Event
            >>> from datetime import date
            >>> from pprint import pprint
            >>> event = Event.new(summary="My Event", start=date(2025, 11, 22))
            >>> pprint(event.to_jcal())
            ['vevent',
             [['dtstamp', {}, 'date-time', '2025-05-17T08:06:12Z'],
              ['summary', {}, 'text', 'My Event'],
              ['uid', {}, 'text', 'd755cef5-2311-46ed-a0e1-6733c9e15c63'],
              ['dtstart', {}, 'date', '2025-11-22']],
             []]
        """
        properties = []
        for key, value in self.items():
            for item in value if isinstance(value, list) else [value]:
                properties.append(item.to_jcal(key.lower()))
        return [
            self.name.lower(),
            properties,
            [subcomponent.to_jcal() for subcomponent in self.subcomponents],
        ]

    def to_json(self) -> str:
        """Return this component as a jCal JSON string.

        Returns:
            JSON string

        See also :attr:`to_jcal`.
        """
        return json.dumps(self.to_jcal())

    @classmethod
    def from_jcal(cls, jcal: str | list) -> Component:
        """Create a component from a jCal list.

        Parameters:
            jcal: jCal list or JSON string according to :rfc:`7265`.

        Raises:
            ~error.JCalParsingError: If the jCal provided is invalid.
            ~json.JSONDecodeError: If the provided string is not valid JSON.

        This reverses :func:`to_json` and :func:`to_jcal`.

        The following code parses an example from :rfc:`7265`:

        .. code-block:: pycon

            >>> from icalendar import Component
            >>> jcal = ["vcalendar",
            ...   [
            ...     ["calscale", {}, "text", "GREGORIAN"],
            ...     ["prodid", {}, "text", "-//Example Inc.//Example Calendar//EN"],
            ...     ["version", {}, "text", "2.0"]
            ...   ],
            ...   [
            ...     ["vevent",
            ...       [
            ...         ["dtstamp", {}, "date-time", "2008-02-05T19:12:24Z"],
            ...         ["dtstart", {}, "date", "2008-10-06"],
            ...         ["summary", {}, "text", "Planning meeting"],
            ...         ["uid", {}, "text", "4088E990AD89CB3DBB484909"]
            ...       ],
            ...       []
            ...     ]
            ...   ]
            ... ]
            >>> calendar = Component.from_jcal(jcal)
            >>> print(calendar.name)
            VCALENDAR
            >>> print(calendar.prodid)
            -//Example Inc.//Example Calendar//EN
            >>> event = calendar.events[0]
            >>> print(event.summary)
            Planning meeting

        """
        if isinstance(jcal, str):
            jcal = json.loads(jcal)
        if not isinstance(jcal, list) or len(jcal) != 3:
            raise JCalParsingError(
                "A component must be a list with 3 items.", cls, value=jcal
            )
        name, properties, subcomponents = jcal
        if not isinstance(name, str):
            raise JCalParsingError(
                "The name must be a string.", cls, path=[0], value=name
            )
        if name.upper() != cls.name:
            # delegate to correct component class
            component_cls = cls.get_component_class(name.upper())
            return component_cls.from_jcal(jcal)
        component = cls()
        if not isinstance(properties, list):
            raise JCalParsingError(
                "The properties must be a list.", cls, path=1, value=properties
            )
        for i, prop in enumerate(properties):
            JCalParsingError.validate_property(prop, cls, path=[1, i])
            prop_name = prop[0]
            prop_value = prop[2]
            prop_cls: type[VPROPERTY] = cls.types_factory.for_property(
                prop_name, prop_value
            )
            with JCalParsingError.reraise_with_path_added(1, i):
                v_prop = prop_cls.from_jcal(prop)
            # if we use the default value for that property, we can delete the
            # VALUE parameter
            if prop_cls == cls.types_factory.for_property(prop_name):
                del v_prop.VALUE
            component.add(prop_name, v_prop)
        if not isinstance(subcomponents, list):
            raise JCalParsingError(
                "The subcomponents must be a list.", cls, 2, value=subcomponents
            )
        for i, subcomponent in enumerate(subcomponents):
            with JCalParsingError.reraise_with_path_added(2, i):
                component.subcomponents.append(cls.from_jcal(subcomponent))
        return component

    def copy(self, recursive: bool = False) -> Self:
        """Copy the component.

        Parameters:
            recursive:
                If ``True``, this creates copies of the component, its subcomponents,
                and all its properties.
                If ``False``, this only creates a shallow copy of the component.

        Returns:
            A copy of the component.

        Examples:

            Create a shallow copy of a component:

            .. code-block:: pycon

                >>> from icalendar import Event
                >>> event = Event.new(description="Event to be copied")
                >>> event_copy = event.copy()
                >>> str(event_copy.description)
                'Event to be copied'

            Shallow copies lose their subcomponents:

            .. code-block:: pycon

                >>> from icalendar import Calendar
                >>> calendar = Calendar.example()
                >>> len(calendar.subcomponents)
                3
                >>> calendar_copy = calendar.copy()
                >>> len(calendar_copy.subcomponents)
                0

            A recursive copy also copies all the subcomponents:

            .. code-block:: pycon

                >>> full_calendar_copy = calendar.copy(recursive=True)
                >>> len(full_calendar_copy.subcomponents)
                3
                >>> full_calendar_copy.events[0] == calendar.events[0]
                True
                >>> full_calendar_copy.events[0] is calendar.events[0]
                False

        """
        if recursive:
            return deepcopy(self)
        return super().copy()


__all__ = ["Component"]
