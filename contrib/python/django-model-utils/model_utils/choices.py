import copy


class Choices:
    """
    A class to encapsulate handy functionality for lists of choices
    for a Django model field.

    Each argument to ``Choices`` is a choice, represented as either a
    string, a two-tuple, or a three-tuple.

    If a single string is provided, that string is used as the
    database representation of the choice as well as the
    human-readable presentation.

    If a two-tuple is provided, the first item is used as the database
    representation and the second the human-readable presentation.

    If a triple is provided, the first item is the database
    representation, the second a valid Python identifier that can be
    used as a readable label in code, and the third the human-readable
    presentation. This is most useful when the database representation
    must sacrifice readability for some reason: to achieve a specific
    ordering, to use an integer rather than a character field, etc.

    Regardless of what representation of each choice is originally
    given, when iterated over or indexed into, a ``Choices`` object
    behaves as the standard Django choices list of two-tuples.

    If the triple form is used, the Python identifier names can be
    accessed as attributes on the ``Choices`` object, returning the
    database representation. (If the single or two-tuple forms are
    used and the database representation happens to be a valid Python
    identifier, the database representation itself is available as an
    attribute on the ``Choices`` object, returning itself.)

    Option groups can also be used with ``Choices``; in that case each
    argument is a tuple consisting of the option group name and a list
    of options, where each option in the list is either a string, a
    two-tuple, or a triple as outlined above.

    """

    def __init__(self, *choices):
        # list of choices expanded to triples - can include optgroups
        self._triples = []
        # list of choices as (db, human-readable) - can include optgroups
        self._doubles = []
        # dictionary mapping db representation to human-readable
        self._display_map = {}
        # dictionary mapping Python identifier to db representation
        self._identifier_map = {}
        # set of db representations
        self._db_values = set()

        self._process(choices)

    def _store(self, triple, triple_collector, double_collector):
        self._identifier_map[triple[1]] = triple[0]
        self._display_map[triple[0]] = triple[2]
        self._db_values.add(triple[0])
        triple_collector.append(triple)
        double_collector.append((triple[0], triple[2]))

    def _process(self, choices, triple_collector=None, double_collector=None):
        if triple_collector is None:
            triple_collector = self._triples
        if double_collector is None:
            double_collector = self._doubles

        store = lambda c: self._store(c, triple_collector, double_collector)

        for choice in choices:
            if isinstance(choice, (list, tuple)):
                if len(choice) == 3:
                    store(choice)
                elif len(choice) == 2:
                    if isinstance(choice[1], (list, tuple)):
                        # option group
                        group_name = choice[0]
                        subchoices = choice[1]
                        tc = []
                        triple_collector.append((group_name, tc))
                        dc = []
                        double_collector.append((group_name, dc))
                        self._process(subchoices, tc, dc)
                    else:
                        store((choice[0], choice[0], choice[1]))
                else:
                    raise ValueError(
                        "Choices can't take a list of length %s, only 2 or 3"
                        % len(choice)
                    )
            else:
                store((choice, choice, choice))

    def __len__(self):
        return len(self._doubles)

    def __iter__(self):
        return iter(self._doubles)

    def __reversed__(self):
        return reversed(self._doubles)

    def __getattr__(self, attname):
        try:
            return self._identifier_map[attname]
        except KeyError:
            raise AttributeError(attname)

    def __getitem__(self, key):
        return self._display_map[key]

    def __add__(self, other):
        if isinstance(other, self.__class__):
            other = other._triples
        else:
            other = list(other)
        return Choices(*(self._triples + other))

    def __radd__(self, other):
        # radd is never called for matching types, so we don't check here
        other = list(other)
        return Choices(*(other + self._triples))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._triples == other._triples
        return False

    def __repr__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join("%s" % repr(i) for i in self._triples)
        )

    def __contains__(self, item):
        return item in self._db_values

    def __deepcopy__(self, memo):
        return self.__class__(*copy.deepcopy(self._triples, memo))

    def subset(self, *new_identifiers):
        identifiers = set(self._identifier_map.keys())

        if not identifiers.issuperset(new_identifiers):
            raise ValueError(
                'The following identifiers are not present: %s' %
                identifiers.symmetric_difference(new_identifiers),
            )

        return self.__class__(*[
            choice for choice in self._triples
            if choice[1] in new_identifiers
        ])
