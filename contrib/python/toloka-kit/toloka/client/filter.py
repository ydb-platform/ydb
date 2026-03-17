__all__ = [
    'FilterCondition',
    'FilterOr',
    'FilterAnd',
    'Condition',
    'Profile',
    'Computed',
    'Skill',
    'Gender',
    'Country',
    'Citizenship',
    'Education',
    'AdultAllowed',
    'DateOfBirth',
    'City',
    'Languages',
    'RegionByPhone',
    'RegionByIp',
    'DeviceCategory',
    'ClientType',
    'OSFamily',
    'OSVersion',
    'OSVersionMajor',
    'OSVersionMinor',
    'OSVersionBugfix',
    'UserAgentType',
    'UserAgentFamily',
    'UserAgentVersion',
    'UserAgentVersionMajor',
    'UserAgentVersionMinor',
    'UserAgentVersionBugfix',
]
import copy
import inspect
from enum import unique
from typing import Any, List, Optional, Union, ClassVar, Dict

from .primitives.base import BaseTolokaObject
from .primitives.operators import (
    CompareOperator,
    StatefulComparableConditionMixin,
    IdentityConditionMixin,
    ComparableConditionMixin,
    IdentityOperator,
    InclusionConditionMixin,
    InclusionOperator,
)
from ..util._codegen import attribute
from ..util._docstrings import inherit_docstrings
from ..util._extendable_enum import ExtendableStrEnum


class FilterCondition(BaseTolokaObject):
    """Filters for selecting Tolokers who can access tasks.

    For example, you can select Tolokers who have some skill, speak certain languages, or use a smartphone.

    Filters can be combined using the `|` and  `&` operators. Some filters support the `~` operator.

    Example:
        Filtering by language and device category.

        >>> filter = (
        >>>    (toloka.client.filter.Languages.in_('EN')) &
        >>>    (toloka.client.filter.DeviceCategory == toloka.client.filter.DeviceCategory.SMARTPHONE)
        >>> )
        ...
    """

    def __or__(self, other: 'FilterCondition'):
        if isinstance(other, FilterOr):
            return other | self
        return FilterOr(or_=[self, other])

    def __and__(self, other: 'FilterCondition'):
        if isinstance(other, FilterAnd):
            return other & self
        return FilterAnd(and_=[self, other])

    def __invert__(self) -> 'FilterCondition':
        raise NotImplementedError('It is abstract method')

    @classmethod
    def structure(cls, data: dict):
        if 'or' in data:
            return FilterOr.structure(data)
        if 'and' in data:
            return FilterAnd.structure(data)
        else:
            return Condition.structure(data)


class FilterOr(FilterCondition, kw_only=False):
    """Supports combining filters using the `|` operator.

    Attributes:
        or_: A list of filters.
    """

    or_: List[FilterCondition] = attribute(origin='or', required=True)

    def __or__(self, other: FilterCondition):
        self.or_.append(other)
        return self

    def __invert__(self) -> 'FilterAnd':
        return FilterAnd(and_=[~condition for condition in self.or_])

    def __iter__(self):
        return iter(self.or_)

    def __getitem__(self, item):
        return self.or_.__getitem__(item)

    @classmethod
    def structure(cls, data):
        return super(FilterCondition, cls).structure(data)


class FilterAnd(FilterCondition, kw_only=False):
    """Supports combining filters using the `&` operator.

    Attributes:
        and_: A list of filters.
    """

    and_: List[FilterCondition] = attribute(origin='and', required=True)

    def __and__(self, other):
        self.and_.append(other)
        return self

    def __invert__(self) -> FilterOr:
        return FilterOr(or_=[~condition for condition in self.and_])

    def __iter__(self):
        return iter(self.and_)

    def __getitem__(self, item):
        return self.and_.__getitem__(item)

    @classmethod
    def structure(cls, data):
        return super(FilterCondition, cls).structure(data)


class Condition(FilterCondition, spec_field='category', spec_enum='Category'):
    """A base class that supports filter conditions.

    Any condition belongs to some category and has a condition operator and a value. These attributes are mapped to API parameters.

    Attributes:
        operator: An operator used in a condition.
            Allowed set of operators depends on the filter.
        value: A value to compare with.
            For example, the minimum value of some skill, or a language specified in a Toloker's profile.
    """

    @unique
    class Category(ExtendableStrEnum):
        PROFILE = 'profile'
        COMPUTED = 'computed'
        SKILL = 'skill'

    operator: Any = attribute(required=True)
    value: Any = attribute(required=True)

    def __invert__(self) -> 'Condition':
        condition_copy = copy.deepcopy(self)
        condition_copy.operator = ~self.operator
        return condition_copy

    @classmethod
    def structure(cls, data):
        return super(FilterCondition, cls).structure(data)


@inherit_docstrings
class Profile(Condition, spec_value=Condition.Category.PROFILE, spec_field='key', spec_enum='Key'):
    """A base class for a category of filters that use Toloker's profile.
    """

    @unique
    class Key(ExtendableStrEnum):
        """Filter names in the `profile` category.
        """

        GENDER = 'gender'
        COUNTRY = 'country'
        CITIZENSHIP = 'citizenship'
        EDUCATION = 'education'
        ADULT_ALLOWED = 'adult_allowed'
        DATE_OF_BIRTH = 'date_of_birth'
        CITY = 'city'
        LANGUAGES = 'languages'
        VERIFIED = 'verified'


@inherit_docstrings
class Computed(Condition, spec_value=Condition.Category.COMPUTED, spec_field='key', spec_enum='Key'):
    """A base class for a category of filters that use connection and client information.
    """

    @unique
    class Key(ExtendableStrEnum):
        """Filter names in the `computed` category.
        """

        CLIENT_TYPE = 'client_type'

        REGION_BY_PHONE = 'region_by_phone'
        REGION_BY_IP = 'region_by_ip'
        DEVICE_CATEGORY = 'device_category'
        OS_FAMILY = 'os_family'
        OS_VERSION = 'os_version'
        USER_AGENT_TYPE = 'user_agent_type'
        USER_AGENT_FAMILY = 'user_agent_family'
        USER_AGENT_VERSION = 'user_agent_version'

        OS_VERSION_MAJOR = 'os_version_major'
        OS_VERSION_MINOR = 'os_version_minor'
        OS_VERSION_BUGFIX = 'os_version_bugfix'
        USER_AGENT_VERSION_MAJOR = 'user_agent_version_major'
        USER_AGENT_VERSION_MINOR = 'user_agent_version_minor'
        USER_AGENT_VERSION_BUGFIX = 'user_agent_version_bugfix'


class Skill(StatefulComparableConditionMixin, Condition, order=False, eq=False, kw_only=False, spec_value=Condition.Category.SKILL):
    """Filtering Tolokers by skills.

    Pass the ID of a skill to the filter constructor.
    To select Tolokers without a skill, compare created filter with `None`.

    Example:
        Selecting Tolokers with a skill with ID '224' greater than 70.
        >>> filter = toloka.client.filter.Skill('224') > 70
        ...

    Attributes:
        key: The ID of a skill.
        operator: An operator in the condition.
        value: A value to compare the skill with.
    """

    key: str = attribute(required=True)
    operator: CompareOperator = attribute(default=CompareOperator.EQ, required=True)
    value: Optional[float] = attribute(default=None, required=True)


@inherit_docstrings
class Gender(Profile, IdentityConditionMixin, spec_value=Profile.Key.GENDER):
    """Filtering Tolokers by gender.

    Attributes:
        value: Toloker's gender specified in the profile.
    """

    @unique
    class Gender(ExtendableStrEnum):
        """Toloker's gender.
        """

        MALE = 'MALE'
        FEMALE = 'FEMALE'

    MALE = Gender.MALE
    FEMALE = Gender.FEMALE

    value: Gender = attribute(required=True, autocast=True)


@inherit_docstrings
class Country(Profile, IdentityConditionMixin, spec_value=Profile.Key.COUNTRY):
    """Filtering Tolokers by a country of residence specified in their profiles.

    Attributes:
        value: A two-letter code of the country taken from the [ISO 3166-1](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) standard.
    """

    value: str = attribute(required=True)  # ISO 3166-1 alpha-2


@inherit_docstrings
class Citizenship(Profile, IdentityConditionMixin, spec_value=Profile.Key.CITIZENSHIP):
    """Filtering Tolokers by a country of citizenship specified in their profiles.

    Attributes:
        value: A two-letter code of the country taken from the [ISO 3166-1](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) standard.
    """

    value: str = attribute(required=True)  # ISO 3166-1 alpha-2


@inherit_docstrings
class Education(Profile, IdentityConditionMixin, spec_value=Profile.Key.EDUCATION):
    """Filtering Tolokers by a level of education specified in their profiles.

    Attributes:
        value: Toloker's level of education.
    """

    @unique
    class Education(ExtendableStrEnum):
        """Toloker's education level.
        """

        BASIC = 'BASIC'
        MIDDLE = 'MIDDLE'
        HIGH = 'HIGH'

    BASIC = Education.BASIC
    MIDDLE = Education.MIDDLE
    HIGH = Education.HIGH

    value: Education = attribute(required=True, autocast=True)


@inherit_docstrings
class AdultAllowed(Profile, IdentityConditionMixin, spec_value=Profile.Key.ADULT_ALLOWED):
    """Filtering Tolokers who agreed to work with adult content.

    Attributes:
        value:
            * `True` — Toloker agrees to work with adult content.
            * `False` — Toloker does not agree to work with adult content.

    Example:

        >>> adult_allowed_filter = toloka.client.filter.AdultAllowed == True
        >>> adult_not_allowed_filter = toloka.client.filter.AdultAllowed == False
        ...
    """

    value: bool = attribute(required=True)

    def __invert__(self) -> 'Condition':
        """Enforce to use `==` operator"""
        condition_copy = copy.deepcopy(self)
        if condition_copy.operator == IdentityOperator.NE:
            condition_copy.operator = IdentityOperator.EQ
        else:
            condition_copy.value = not condition_copy.value
        return condition_copy


@inherit_docstrings
class DateOfBirth(Profile, ComparableConditionMixin, spec_value=Profile.Key.DATE_OF_BIRTH):
    """Filtering Tolokers by a date of birth.

    Attributes:
        value: The date of birth in seconds since January 1, 1970 (UNIX time).
    """

    value: int = attribute(required=True)


@inherit_docstrings
class City(Profile, InclusionConditionMixin, spec_value=Profile.Key.CITY):
    """Filtering Tolokers by a city specified in their profiles.

    Attributes:
        value: The [ID](https://toloka.ai/docs/api/regions) of the city.
    """

    value: int = attribute(required=True)


@inherit_docstrings
class Languages(Profile, InclusionConditionMixin, spec_value=Profile.Key.LANGUAGES):
    """Filtering Tolokers by languages specified in their profiles.

    Attributes:
        value: Languages specified in the profile. A two-letter [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) code in upper case is used.
        verified: If set to `True`, only Tolokers who have passed a language test are selected.
            Tests are available for languages: `AR`, `DE`, `EN`, `ES`, `FR`,
            `HE`, `ID`, `JA`, `PT`, `RU`, `SV`, `ZH-HANS`.

    Example:

        >>> from toloka.client.primitives.operators import InclusionOperator
        >>>
        >>> updated_pool = toloka_client.get_pool('38955320')
        >>> updated_pool.filter=(
        >>>     (toloka.filter.Languages(operator=InclusionOperator.IN, value=['EN', 'DE'], verified=True)) &
        >>>     (toloka.filter.Languages(operator=InclusionOperator.IN, value=['FR'], verified=True))
        >>> )
        >>> toloka_client.update_pool(pool_id=updated_pool.id, pool=updated_pool)
        ...
    """

    VERIFIED_LANGUAGES_TO_SKILLS: ClassVar[Dict[str, str]] = {
        'AR': '30724',
        'DE': '26377',
        'EN': '26366',
        'ES': '32346',
        'FR': '26711',
        'HE': '44954',
        'HU': '54378',
        'ID': '39821',
        'JA': '26513',
        'PT': '26714',
        'RU': '26296',
        'SV': '29789',
        'UK': '48836',
        'ZH-HANS': '44742',
    }

    VERIFIED_LANGUAGE_SKILL_VALUE: ClassVar[int] = 100

    value: Union[str, List[str]] = attribute(required=True)

    def __new__(cls, *args, **kwargs):
        """Handling `verified` parameter of the `Languages` filter class.

        If the class is instantiated with `verified=True` then combined conditions is returned:
        `FilterOr([
            FilterAnd([language_1, verified_skill_for_language_1]),
            ...
            FilterAnd([language_n, verified_skill_for_language_n])
        ])`
        """
        bound_args = inspect.signature(cls.__init__).bind(None, *args, **kwargs).arguments
        languages = bound_args['value']
        operator = bound_args['operator']
        verified = bound_args.pop('verified', False)

        if verified and operator == InclusionOperator.NOT_IN:
            raise ValueError('"Language not in" filter does not support verified=True argument')
        if verified and operator == InclusionOperator.IN:
            skills_mapping = cls.VERIFIED_LANGUAGES_TO_SKILLS
            try:
                if not isinstance(languages, list):
                    languages = [languages]
                result_conditions = FilterOr([])
                for language in languages:
                    verified_language_condition = (
                        Languages(operator=operator, value=language) &
                        Skill(skills_mapping[language]).eq(cls.VERIFIED_LANGUAGE_SKILL_VALUE)
                    )
                    result_conditions |= verified_language_condition

                return result_conditions
            except KeyError:
                if not isinstance(languages, str):
                    unsupported_languages = set(languages) - skills_mapping.keys()
                else:
                    unsupported_languages = [languages]
                raise ValueError(
                    'Following languages are not supported as verified languages:\n' + '\n'.join(unsupported_languages)
                )
        if isinstance(languages, list):
            if operator == InclusionOperator.IN:
                return FilterOr([Languages(operator=operator, value=language) for language in languages])
            else:
                return FilterAnd([Languages(operator=operator, value=language) for language in languages])
        return super().__new__(cls, *args, **kwargs)

    def __getnewargs__(self):
        """Due to redefined __new__ method class can't be deepcopied or pickled without __getnewargs__ definition"""
        return self.operator, self.value


# add fake parameter "verified: bool = False" to Languages.__init__ signature. This parameter will be consumed in
# Languages.__new__ while the actual __init__ is managed by attrs.
languages_init_signature = inspect.signature(Languages.__init__)
languages_init_signature_parameters = dict(languages_init_signature.parameters)
languages_init_signature_parameters['verified'] = inspect.Parameter(
    name='verified', kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, default=False, annotation=bool,
)
Languages.__init__.__annotations__['verified'] = bool
Languages.__init__.__signature__ = languages_init_signature.replace(parameters=languages_init_signature_parameters.values())


@inherit_docstrings
class Verified(Profile, IdentityConditionMixin, spec_value=Profile.Key.VERIFIED):
    """Filtering Tolokers that passed verification.

    Attributes:
        value: A flag showing whether a Toloker passed verification.

    Example:

        >>> verified_filter = toloka.client.filter.Verified == True
        >>> unverified_filter = toloka.client.filter.Verified == False
        ...
    """
    value: bool = attribute(required=True)

    def __invert__(self) -> 'Condition':
        """Enforce to use `==` operator"""
        condition_copy = copy.deepcopy(self)
        if condition_copy.operator == IdentityOperator.NE:
            condition_copy.operator = IdentityOperator.EQ
        else:
            condition_copy.value = not condition_copy.value
        return condition_copy


@inherit_docstrings
class RegionByPhone(Computed, InclusionConditionMixin, spec_value=Computed.Key.REGION_BY_PHONE):
    """Filtering Tolokers by a region which is determined by their mobile phone number.

    Attributes:
        value: The ID from the [list of regions](https://toloka.ai/docs/api/regions).
    """

    value: int = attribute(required=True)


@inherit_docstrings
class RegionByIp(Computed, InclusionConditionMixin, spec_value=Computed.Key.REGION_BY_IP):
    """Filtering Tolokers by a region which is determined by their IP address.

    Attributes:
        value: The ID from the [list of regions](https://toloka.ai/docs/api/regions).
    """

    value: int = attribute(required=True)


@inherit_docstrings
class DeviceCategory(Computed, IdentityConditionMixin, spec_value=Computed.Key.DEVICE_CATEGORY):
    """Filtering Tolokers by their device category.

    Attributes:
        value: The Toloker's device category.
    """

    @unique
    class DeviceCategory(ExtendableStrEnum):
        """Device categories.
        """

        PERSONAL_COMPUTER = 'PERSONAL_COMPUTER'
        SMARTPHONE = 'SMARTPHONE'
        TABLET = 'TABLET'
        WEARABLE_COMPUTER = 'WEARABLE_COMPUTER'

    PERSONAL_COMPUTER = DeviceCategory.PERSONAL_COMPUTER
    SMARTPHONE = DeviceCategory.SMARTPHONE
    TABLET = DeviceCategory.TABLET
    WEARABLE_COMPUTER = DeviceCategory.WEARABLE_COMPUTER

    value: DeviceCategory = attribute(required=True, autocast=True)


@inherit_docstrings
class ClientType(Computed, IdentityConditionMixin, spec_value=Computed.Key.CLIENT_TYPE):
    """Filtering Tolokers by a client application type.

    Attributes:
        value: The client application type.
    """

    @unique
    class ClientType(ExtendableStrEnum):
        """Client application types.
        """

        BROWSER = 'BROWSER'
        TOLOKA_APP = 'TOLOKA_APP'

    value: ClientType = attribute(required=True, autocast=True)


@inherit_docstrings
class OSFamily(Computed, IdentityConditionMixin, spec_value=Computed.Key.OS_FAMILY):
    """Filtering Tolokers by their OS family.

    Attributes:
        value: The OS family.
    """

    @unique
    class OSFamily(ExtendableStrEnum):
        """OS families.

        Attributes:
            WINDOWS: Microsoft Windows operating system developed and marketed by Microsoft for personal computers.
            OS_X: macOS operating system developed by Apple Inc. since 2001 for Mac computers.
            MAC_OS: Classic Mac OS operating system developed by Apple Inc. before 2001 for the Macintosh family of personal computers.
            LINUX: A family of open-source Unix-like operating systems based on the Linux kernel.
            BSD: An operating system based on Research Unix, developed and distributed by the CSRG, and its open-source descendants like FreeBSD, OpenBSD, NetBSD, and DragonFly BSD.
            ANDROID: Android mobile operating system based on a modified version of the Linux kernel, designed primarily for touchscreen mobile devices.
            IOS:  iOS mobile operating system developed by Apple Inc. exclusively for its mobile devices.
            BLACKBERRY: BlackBerry OS mobile operating system developed by BlackBerry Limited for its BlackBerry smartphone devices.
        """

        WINDOWS = 'WINDOWS'
        OS_X = 'OS_X'
        MAC_OS = 'MAC_OS'
        LINUX = 'LINUX'
        BSD = 'BSD'
        ANDROID = 'ANDROID'
        IOS = 'IOS'
        BLACKBERRY = 'BLACKBERRY'

    WINDOWS = OSFamily.WINDOWS
    OS_X = OSFamily.OS_X
    MAC_OS = OSFamily.MAC_OS
    LINUX = OSFamily.LINUX
    BSD = OSFamily.BSD
    ANDROID = OSFamily.ANDROID
    IOS = OSFamily.IOS
    BLACKBERRY = OSFamily.BLACKBERRY

    value: OSFamily = attribute(required=True, autocast=True)


@inherit_docstrings
class OSVersion(Computed, ComparableConditionMixin, spec_value=Computed.Key.OS_VERSION):
    """Filtering Tolokers by an OS version.

    The version consists of major and minor version numbers, for example, `14.4`.
    The version is represented as a single floating point number in conditions.

    Attributes:
        value: The version of the OS.
    """

    value: float = attribute(required=True)


@inherit_docstrings
class OSVersionMajor(Computed, ComparableConditionMixin, spec_value=Computed.Key.OS_VERSION_MAJOR):
    """Filtering Tolokers by an OS major version.

    Attributes:
        value: The major version of the OS.
    """

    value: int = attribute(required=True)


@inherit_docstrings
class OSVersionMinor(Computed, ComparableConditionMixin, spec_value=Computed.Key.OS_VERSION_MINOR):
    """Filtering Tolokers by an OS minor version.

    Attributes:
        value: The minor version of the OS.
    """

    value: int = attribute(required=True)


@inherit_docstrings
class OSVersionBugfix(Computed, ComparableConditionMixin, spec_value=Computed.Key.OS_VERSION_BUGFIX):
    """Filtering Tolokers by a build number or a bugfix version of their OS.

    Attributes:
        value: The build number or the bugfix version of the OS.
    """

    value: int = attribute(required=True)


@inherit_docstrings
class UserAgentType(Computed, IdentityConditionMixin, spec_value=Computed.Key.USER_AGENT_TYPE):
    """Filtering Tolokers by a user agent type.

    Attributes:
        value: The user agent type.
    """

    @unique
    class UserAgentType(ExtendableStrEnum):
        """User agent types.
        """

        BROWSER = 'BROWSER'
        MOBILE_BROWSER = 'MOBILE_BROWSER'
        OTHER = 'OTHER'

    BROWSER = UserAgentType.BROWSER
    MOBILE_BROWSER = UserAgentType.MOBILE_BROWSER
    OTHER = UserAgentType.OTHER

    value: UserAgentType = attribute(required=True, autocast=True)


@inherit_docstrings
class UserAgentFamily(Computed, IdentityConditionMixin, spec_value=Computed.Key.USER_AGENT_FAMILY):
    """Filtering Tolokers by a user agent family.

    Attributes:
        value: The user agent family.
    """

    @unique
    class UserAgentFamily(ExtendableStrEnum):
        """User agent families.
        """

        IE = 'IE'
        CHROMIUM = 'CHROMIUM'
        CHROME = 'CHROME'
        FIREFOX = 'FIREFOX'
        SAFARI = 'SAFARI'
        YANDEX_BROWSER = 'YANDEX_BROWSER'

        IE_MOBILE = 'IE_MOBILE'
        CHROME_MOBILE = 'CHROME_MOBILE'
        MOBILE_FIREFOX = 'MOBILE_FIREFOX'
        MOBILE_SAFARI = 'MOBILE_SAFARI'

    IE = UserAgentFamily.IE
    CHROMIUM = UserAgentFamily.CHROMIUM
    CHROME = UserAgentFamily.CHROME
    FIREFOX = UserAgentFamily.FIREFOX
    SAFARI = UserAgentFamily.SAFARI
    YANDEX_BROWSER = UserAgentFamily.YANDEX_BROWSER

    IE_MOBILE = UserAgentFamily.IE_MOBILE
    CHROME_MOBILE = UserAgentFamily.CHROME_MOBILE
    MOBILE_FIREFOX = UserAgentFamily.MOBILE_FIREFOX
    MOBILE_SAFARI = UserAgentFamily.MOBILE_SAFARI

    value: UserAgentFamily = attribute(required=True, autocast=True)


@inherit_docstrings
class UserAgentVersion(Computed, ComparableConditionMixin, spec_value=Computed.Key.USER_AGENT_VERSION):
    """Filtering Tolokers by a browser version.

    The version consists of major and minor version numbers.
    The version is represented as a single floating point number in conditions.

    Attributes:
        value: The version of the browser.
    """

    value: float


@inherit_docstrings
class UserAgentVersionMajor(Computed, ComparableConditionMixin, spec_value=Computed.Key.USER_AGENT_VERSION_MAJOR):
    """Filtering Tolokers by a major browser version.

    Attributes:
        value: The major browser version.
    """

    value: int


@inherit_docstrings
class UserAgentVersionMinor(Computed, ComparableConditionMixin, spec_value=Computed.Key.USER_AGENT_VERSION_MINOR):
    """Filtering Tolokers by a minor browser version.

    Attributes:
        value: The minor browser version.
    """

    value: int


@inherit_docstrings
class UserAgentVersionBugfix(Computed, ComparableConditionMixin, spec_value=Computed.Key.USER_AGENT_VERSION_BUGFIX):
    """Filtering Tolokers by a build number or a bugfix version of their browser.

    Attributes:
        value: The build number or the bugfix version of the browser.
    """

    value: int
