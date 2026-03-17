from ..base import ParametrizedValue


class SubjectBuiltin:

    name = None

    def __init__(self, regexp):
        self.regexp = regexp


class SubjectCustom(ParametrizedValue):
    """Represents a routing subject that supports various check."""

    args_joiner = ';'

    def __init__(self, subject, *, negate=False):
        """
        :param Var|str subject: Handwritten subject or a Var heir representing it.

        :param bool negate: Use to negate subject for rule.
            .. note:: You can also use tilde (~) instead of this argument for nagation.

        """
        self._subject = subject
        self.negate = negate
        super().__init__()

    def __invert__(self):
        self.negate = True
        return self

    def _setup(self, name, arg=None):
        self.name = f'{name}'
        self.args = [self._subject, arg]
        return self

    def exists(self):
        """Check if the subject exists in the filesystem."""
        return self._setup('exists')

    def isfile(self):
        """Check if the subject is a file."""
        return self._setup('isfile')

    def isdir(self):
        """Check if the subject is a directory."""
        return self._setup('isdir')

    def islink(self):
        """Check if the subject is a link."""
        return self._setup('islink')

    def isexec(self):
        """Check if the subject is an executable file."""
        return self._setup('isexec')

    def islord(self):
        """Check if the subject is a Legion Lord."""
        return self._setup('lord')

    def contains_ipv4(self):
        """Check if the subject is ip v4."""
        return self._setup('ipv4in')

    def contains_ipv6(self):
        """Check if the subject is ip v6."""
        return self._setup('ipv6in')

    def eq(self, val):
        """Check if the subject is equal to the specified pattern."""
        return self._setup('eq', val)

    def ge(self, val):
        """Check if the subject is greater than or equal to the specified pattern."""
        return self._setup('ishigherequal', val)

    def le(self, val):
        """Check if the subject is less than or equal to the specified pattern."""
        return self._setup('islowerequal', val)

    def gt(self, val):
        """Check if the subject is greater than the specified pattern."""
        return self._setup('ishigher', val)

    def lt(self, val):
        """Check if the subject is less than the specified pattern."""
        return self._setup('islower', val)

    def startswith(self, val):
        """Check if the subject starts with the specified pattern."""
        return self._setup('startswith', val)

    def endswith(self, val):
        """Check if the subject ends with the specified pattern."""
        return self._setup('endswith', val)

    def matches(self, regexp):
        """Check if the subject matches the specified regexp."""
        return self._setup('regexp', regexp)

    def isempty(self):
        """Check if the subject is empty."""
        return self._setup('empty')

    def contains(self, val):
        """Check if the subject contains the specified pattern."""
        return self._setup('contains', val)


class SubjectPathInfo(SubjectBuiltin):
    """Default subject, maps to PATH_INFO."""

    name = ''


class SubjectRequestUri(SubjectBuiltin):
    """Checks REQUEST_URI for a value."""

    name = 'uri'


class SubjectQueryString(SubjectBuiltin):
    """Checks QUERY_STRING for a value."""

    name = 'qs'


class SubjectRemoteAddr(SubjectBuiltin):
    """Checks REMOTE_ADDR for a value."""

    name = 'remote-addr'


class SubjectRemoteUser(SubjectBuiltin):
    """Checks REMOTE_USER for a value."""

    name = 'remote-user'


class SubjectHttpHost(SubjectBuiltin):
    """Checks HTTP_HOST for a value."""

    name = 'host'


class SubjectHttpReferer(SubjectBuiltin):
    """Checks HTTP_REFERER for a value."""

    name = 'referer'


class SubjectHttpUserAgent(SubjectBuiltin):
    """Checks HTTP_USER_AGENT for a value."""

    name = 'user-agent'


class SubjectStatus(SubjectBuiltin):
    """Checks HTTP response status code.

    .. warning:: Not available in the request chain.

    """
    name = 'status'
