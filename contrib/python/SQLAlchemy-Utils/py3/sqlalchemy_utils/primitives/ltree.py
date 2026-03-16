import re

from ..utils import str_coercible

path_matcher = re.compile(r'^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*$')


@str_coercible
class Ltree:
    """
    Ltree class wraps a valid string label path. It provides various
    convenience properties and methods.

    ::

        from sqlalchemy_utils import Ltree

        Ltree('1.2.3').path  # '1.2.3'


    Ltree always validates the given path.

    ::

        Ltree(None)  # raises TypeError

        Ltree('..')  # raises ValueError


    Validator is also available as class method.

    ::

        Ltree.validate('1.2.3')
        Ltree.validate(None)  # raises TypeError


    Ltree supports equality operators.

    ::

        Ltree('Countries.Finland') == Ltree('Countries.Finland')
        Ltree('Countries.Germany') != Ltree('Countries.Finland')


    Ltree objects are hashable.


    ::

        assert hash(Ltree('Finland')) == hash('Finland')


    Ltree objects have length.

    ::

        assert len(Ltree('1.2')) == 2
        assert len(Ltree('some.one.some.where'))  # 4


    You can easily find subpath indexes.

    ::

        assert Ltree('1.2.3').index('2.3') == 1
        assert Ltree('1.2.3.4.5').index('3.4') == 2


    Ltree objects can be sliced.


    ::

        assert Ltree('1.2.3')[0:2] == Ltree('1.2')
        assert Ltree('1.2.3')[1:] == Ltree('2.3')


    Finding longest common ancestor.


    ::

        assert Ltree('1.2.3.4.5').lca('1.2.3', '1.2.3.4', '1.2.3') == '1.2'
        assert Ltree('1.2.3.4.5').lca('1.2', '1.2.3') == '1'


    Ltree objects can be concatenated.

    ::

        assert Ltree('1.2') + Ltree('1.2') == Ltree('1.2.1.2')
    """

    def __init__(self, path_or_ltree):
        if isinstance(path_or_ltree, Ltree):
            self.path = path_or_ltree.path
        elif isinstance(path_or_ltree, str):
            self.validate(path_or_ltree)
            self.path = path_or_ltree
        else:
            raise TypeError(
                "Ltree() argument must be a string or an Ltree, not '{}'"
                .format(
                    type(path_or_ltree).__name__
                )
            )

    @classmethod
    def validate(cls, path):
        if path_matcher.match(path) is None:
            raise ValueError(
                f"'{path}' is not a valid ltree path."
            )

    def __len__(self):
        return len(self.path.split('.'))

    def index(self, other):
        subpath = Ltree(other).path.split('.')
        parts = self.path.split('.')
        for index, _ in enumerate(parts):
            if parts[index:len(subpath) + index] == subpath:
                return index
        raise ValueError('subpath not found')

    def descendant_of(self, other):
        """
        is left argument a descendant of right (or equal)?

        ::

            assert Ltree('1.2.3.4.5').descendant_of('1.2.3')
        """
        subpath = self[:len(Ltree(other))]
        return subpath == other

    def ancestor_of(self, other):
        """
        is left argument an ancestor of right (or equal)?

        ::

            assert Ltree('1.2.3').ancestor_of('1.2.3.4.5')
        """
        subpath = Ltree(other)[:len(self)]
        return subpath == self

    def __getitem__(self, key):
        if isinstance(key, int):
            return Ltree(self.path.split('.')[key])
        elif isinstance(key, slice):
            return Ltree('.'.join(self.path.split('.')[key]))
        raise TypeError(
            'Ltree indices must be integers, not {}'.format(
                key.__class__.__name__
            )
        )

    def lca(self, *others):
        """
        Lowest common ancestor, i.e., longest common prefix of paths

        ::

            assert Ltree('1.2.3.4.5').lca('1.2.3', '1.2.3.4', '1.2.3') == '1.2'
        """
        other_parts = [Ltree(other).path.split('.') for other in others]
        parts = self.path.split('.')
        for index, element in enumerate(parts):
            if any(
                other[index] != element or
                len(other) <= index + 1 or
                len(parts) == index + 1
                for other in other_parts
            ):
                if index == 0:
                    return None
                return Ltree('.'.join(parts[0:index]))

    def __add__(self, other):
        return Ltree(self.path + '.' + Ltree(other).path)

    def __radd__(self, other):
        return Ltree(other) + self

    def __eq__(self, other):
        if isinstance(other, Ltree):
            return self.path == other.path
        elif isinstance(other, str):
            return self.path == other
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.path)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.path!r})'

    def __unicode__(self):
        return self.path

    def __contains__(self, label):
        return label in self.path.split('.')

    def __gt__(self, other):
        return self.path > other.path

    def __lt__(self, other):
        return self.path < other.path

    def __ge__(self, other):
        return self.path >= other.path

    def __le__(self, other):
        return self.path <= other.path
