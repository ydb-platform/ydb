from .compat import str_compat, repr_compat

try:
    from collections.abc import Set  # PY3
except ImportError:
    from collections import Set  # PY2


@repr_compat
@str_compat
class Role(Set):

    """
    A ``Role`` object can be used to filter specific fields against a sequence.

    The ``Role`` contains two things: a set of names and a function.
    The function describes how to filter, taking a field name as input and then
    returning ``True`` or ``False`` to indicate that field should or should not
    be skipped.

    A ``Role`` can be operated on as a ``Set`` object representing the fields
    it has an opinion on.  When Roles are combined with other roles, only the
    filtering behavior of the first role is used.
    """

    def __init__(self, function, fields):
        self.function = function
        self.fields = set(fields)

    def _from_iterable(self, iterable):
        return Role(self.function, iterable)

    def __contains__(self, value):
        return value in self.fields

    def __iter__(self):
        return iter(self.fields)

    def __len__(self):
        return len(self.fields)

    def __eq__(self, other):
        return (self.function.__name__ == other.function.__name__ and
                self.fields == other.fields)

    def __str__(self):
        return '%s(%s)' % (self.function.__name__,
                           ', '.join("'%s'" % f for f in self.fields))

    def __repr__(self):
        return '<Role %s>' % str(self)

    # edit role fields
    def __add__(self, other):
        fields = self.fields.union(other)
        return self._from_iterable(fields)

    def __sub__(self, other):
        fields = self.fields.difference(other)
        return self._from_iterable(fields)

    # apply role to field
    def __call__(self, name, value):
        return self.function(name, value, self.fields)

    # static filter functions
    @staticmethod
    def wholelist(name, value, seq):
        """
        Accepts a field name, value, and a field list.  This function
        implements acceptance of all fields by never requesting a field be
        skipped, thus returns False for all input.

        :param name:
            The field name to inspect.
        :param value:
            The field's value.
        :param seq:
            The list of fields associated with the ``Role``.
        """
        return False

    @staticmethod
    def whitelist(name, value, seq):
        """
        Implements the behavior of a whitelist by requesting a field be skipped
        whenever its name is not in the list of fields.

        :param name:
            The field name to inspect.
        :param value:
            The field's value.
        :param seq:
            The list of fields associated with the ``Role``.
        """

        if seq is not None and len(seq) > 0:
            return name not in seq
        return True

    @staticmethod
    def blacklist(name, value, seq):
        """
        Implements the behavior of a blacklist by requesting a field be skipped
        whenever its name is found in the list of fields.

        :param name:
            The field name to inspect.
        :param value:
            The field's value.
        :param seq:
            The list of fields associated with the ``Role``.
        """
        if seq is not None and len(seq) > 0:
            return name in seq
        return False
