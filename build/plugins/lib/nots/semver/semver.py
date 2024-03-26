import re


class Version:
    """
    This class is intended to provide utility methods to work with semver ranges.
    Right now it is limited to the simplest case: a ">=" operator followed by an exact version with no prerelease or build specification.
    Example: ">= 1.2.3"
    """

    @classmethod
    def from_str(cls, input):
        """
        :param str input: save exact formatted version e.g. 1.2.3
        :rtype: Version
        :raises: ValueError
        """
        parts = input.strip().split(".", 2)
        major = int(parts[0])
        minor = int(parts[1])
        patch = int(parts[2])

        return cls(major, minor, patch)

    STABLE_VERSION_RE = re.compile(r'^\d+\.\d+\.\d+$')

    @classmethod
    def is_stable(cls, v):
        """
        Verifies that the version is in a supported format.

        :param v:string with the version
        :return: bool
        """
        return cls.STABLE_VERSION_RE.match(v) is not None

    @classmethod
    def cmp(cls, a, b):
        """
        Compare two versions. Should be used with "cmp_to_key" wrapper in sorted(), min(), max()...

        For example:
                sorted(["1.2.3", "2.4.2", "1.2.7"], key=cmp_to_key(Version.cmp))

        :param a:string with version or Version instance
        :param b:string with version or Version instance
        :return: int
        :raises: ValueError
        """
        a_version = a if isinstance(a, cls) else cls.from_str(a)
        b_version = b if isinstance(b, cls) else cls.from_str(b)

        if a_version > b_version:
            return 1
        elif a_version < b_version:
            return -1
        else:
            return 0

    __slots__ = "_values"

    def __init__(self, major, minor, patch):
        """
        :param int major
        :param int minor
        :param int patch
        :raises ValueError
        """
        version_parts = {
            "major": major,
            "minor": minor,
            "patch": patch,
        }

        for name, value in version_parts.items():
            value = int(value)
            version_parts[name] = value
            if value < 0:
                raise ValueError("{!r} is negative. A version can only be positive.".format(name))

        self._values = (version_parts["major"], version_parts["minor"], version_parts["patch"])

    def __str__(self):
        return "{}.{}.{}".format(self._values[0], self._values[1], self._values[2])

    def __repr__(self):
        return '<Version({})>'.format(self)

    def __eq__(self, other):
        """
        :param Version|str other
        :rtype: bool
        """
        if isinstance(other, str):
            if self.is_stable(other):
                other = self.from_str(other)
            else:
                return False

        return self.as_tuple() == other.as_tuple()

    def __ne__(self, other):
        return not self == other

    def __gt__(self, other):
        """
        :param Version other
        :rtype: bool
        """
        return self.as_tuple() > other.as_tuple()

    def __ge__(self, other):
        """
        :param Version other
        :rtype: bool
        """
        return self.as_tuple() >= other.as_tuple()

    def __lt__(self, other):
        """
        :param Version other
        :rtype: bool
        """
        return self.as_tuple() < other.as_tuple()

    def __le__(self, other):
        """
        :param Version other
        :rtype: bool
        """
        return self.as_tuple() <= other.as_tuple()

    @property
    def major(self):
        """The major part of the version (read-only)."""
        return self._values[0]

    @major.setter
    def major(self, value):
        raise AttributeError("Attribute 'major' is readonly")

    @property
    def minor(self):
        """The minor part of the version (read-only)."""
        return self._values[1]

    @minor.setter
    def minor(self, value):
        raise AttributeError("Attribute 'minor' is readonly")

    @property
    def patch(self):
        """The patch part of the version (read-only)."""
        return self._values[2]

    @patch.setter
    def patch(self, value):
        raise AttributeError("Attribute 'patch' is readonly")

    def as_tuple(self):
        """
        :rtype: tuple
        """
        return self._values


class Operator:
    EQ = "="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="


class VersionRange:
    @classmethod
    def operator_is_ok(self, operator):
        return [Operator.GE, Operator.EQ, None].count(operator)

    @classmethod
    def from_str(cls, input):
        """
        :param str input
        :rtype: VersionRange
        :raises: ValueError
        """
        m = re.match(r"^\s*([<>=]+)?\s*(\d+\.\d+\.\d+)\s*$", input)
        res = m.groups() if m else None
        if not res or not cls.operator_is_ok(res[0]):
            raise ValueError(
                "Unsupported version range: '{}'. Currently we only support ranges with stable versions and GE / EQ: '>= 1.2.3' / '= 1.2.3' / '1.2.3'".format(
                    input
                )
            )

        version = Version.from_str(res[1])

        return cls(res[0], version)

    __slots__ = ("_operator", "_version")

    def __init__(self, operator, version):
        """
        :param str operator
        :raises: ValueError
        """
        if not self.operator_is_ok(operator):
            raise ValueError("Unsupported range operator '{}'".format(operator))

        # None defaults to Operator.EQ
        self._operator = operator or Operator.EQ
        self._version = version

    @property
    def operator(self):
        """The comparison operator to be used (read-only)."""
        return self._operator

    @operator.setter
    def operator(self, value):
        raise AttributeError("Attribute 'operator' is readonly")

    @property
    def version(self):
        """Version to be used with the operator (read-only)."""
        return self._version

    @version.setter
    def version(self, value):
        raise AttributeError("Attribute 'version' is readonly")

    def is_satisfied_by(self, version):
        """
        :param Version version
        :rtype: bool
        :raises: ValueError
        """
        if self._operator == Operator.GE:
            return version >= self._version

        if self._operator == Operator.EQ:
            return version == self._version

        raise ValueError("Unsupported operator '{}'".format(self._operator))
