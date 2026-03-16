# -*- coding: utf-8 -*-

from deepdiff.helper import items, RemapDict, strings, short_repr, Verbose, notpresent
from ast import literal_eval
from copy import copy

FORCE_DEFAULT = 'fake'
UP_DOWN = {'up': 'down', 'down': 'up'}

REPORT_KEYS = {
    "type_changes",
    "dictionary_item_added",
    "dictionary_item_removed",
    "values_changed",
    "unprocessed",
    "iterable_item_added",
    "iterable_item_removed",
    "attribute_added",
    "attribute_removed",
    "set_item_removed",
    "set_item_added",
    "repetition_change",
}


class DoesNotExist(Exception):
    pass


class ResultDict(RemapDict):
    def cleanup(self):
        """
        Remove empty keys from this object. Should always be called after the result is final.
        :return:
        """
        empty_keys = [k for k, v in getattr(self, items)() if not v]

        for k in empty_keys:
            del self[k]


class TreeResult(ResultDict):
    def __init__(self):
        for key in REPORT_KEYS:
            self[key] = set()


class TextResult(ResultDict):
    def __init__(self, tree_results=None):

        # TODO: centralize keys
        self.update({
            "type_changes": {},
            "dictionary_item_added": self.__set_or_dict(),
            "dictionary_item_removed": self.__set_or_dict(),
            "values_changed": {},
            "unprocessed": [],
            "iterable_item_added": {},
            "iterable_item_removed": {},
            "attribute_added": self.__set_or_dict(),
            "attribute_removed": self.__set_or_dict(),
            "set_item_removed": set(),
            "set_item_added": set(),
            "repetition_change": {}
        })

        if tree_results:
            self._from_tree_results(tree_results)

    def __set_or_dict(self):
        return {} if Verbose.level >= 2 else set()

    def _from_tree_results(self, tree):
        """
        Populate this object by parsing an existing reference-style result dictionary.
        :param tree: A TreeResult
        :return:
        """
        self._from_tree_type_changes(tree)
        self._from_tree_default(tree, 'dictionary_item_added')
        self._from_tree_default(tree, 'dictionary_item_removed')
        self._from_tree_value_changed(tree)
        self._from_tree_unprocessed(tree)
        self._from_tree_default(tree, 'iterable_item_added')
        self._from_tree_default(tree, 'iterable_item_removed')
        self._from_tree_default(tree, 'attribute_added')
        self._from_tree_default(tree, 'attribute_removed')
        self._from_tree_set_item_removed(tree)
        self._from_tree_set_item_added(tree)
        self._from_tree_repetition_change(tree)

    def _from_tree_default(self, tree, report_type):
        if report_type in tree:
            for change in tree[report_type]:  # report each change
                # determine change direction (added or removed)
                # Report t2 (the new one) whenever possible.
                # In cases where t2 doesn't exist (i.e. stuff removed), report t1.
                if change.t2 is not notpresent:
                    item = change.t2
                else:
                    item = change.t1

                # do the reporting
                report = self[report_type]
                if isinstance(report, set):
                    report.add(change.path(force=FORCE_DEFAULT))
                elif isinstance(report, dict):
                    report[change.path(force=FORCE_DEFAULT)] = item
                elif isinstance(report, list):  # pragma: no cover
                    # we don't actually have any of those right now, but just in case
                    report.append(change.path(force=FORCE_DEFAULT))
                else:  # pragma: no cover
                    # should never happen
                    raise TypeError("Cannot handle {} report container type.".
                                    format(report))

    def _from_tree_type_changes(self, tree):
        if 'type_changes' in tree:
            for change in tree['type_changes']:
                remap_dict = RemapDict({
                    'old_type': type(change.t1),
                    'new_type': type(change.t2)
                })
                self['type_changes'][change.path(
                    force=FORCE_DEFAULT)] = remap_dict
                if Verbose.level:
                    remap_dict.update(old_value=change.t1, new_value=change.t2)

    def _from_tree_value_changed(self, tree):
        if 'values_changed' in tree:
            for change in tree['values_changed']:
                the_changed = {'new_value': change.t2, 'old_value': change.t1}
                self['values_changed'][change.path(
                    force=FORCE_DEFAULT)] = the_changed
                if 'diff' in change.additional:
                    the_changed.update({'diff': change.additional['diff']})

    def _from_tree_unprocessed(self, tree):
        if 'unprocessed' in tree:
            for change in tree['unprocessed']:
                self['unprocessed'].append("%s: %s and %s" % (change.path(
                    force=FORCE_DEFAULT), change.t1, change.t2))

    def _from_tree_set_item_removed(self, tree):
        if 'set_item_removed' in tree:
            for change in tree['set_item_removed']:
                path = change.up.path(
                )  # we want't the set's path, the removed item is not directly accessible
                item = change.t1
                if isinstance(item, strings):
                    item = "'%s'" % item
                self['set_item_removed'].add("%s[%s]" % (path, str(item)))
                # this syntax is rather peculiar, but it's DeepDiff 2.x compatible

    def _from_tree_set_item_added(self, tree):
        if 'set_item_added' in tree:
            for change in tree['set_item_added']:
                path = change.up.path(
                )  # we want't the set's path, the added item is not directly accessible
                item = change.t2
                if isinstance(item, strings):
                    item = "'%s'" % item
                self['set_item_added'].add("%s[%s]" % (path, str(item)))
                # this syntax is rather peculiar, but it's DeepDiff 2.x compatible)

    def _from_tree_repetition_change(self, tree):
        if 'repetition_change' in tree:
            for change in tree['repetition_change']:
                path = change.path(force=FORCE_DEFAULT)
                self['repetition_change'][path] = RemapDict(change.additional[
                    'repetition'])
                self['repetition_change'][path]['value'] = change.t1


class DiffLevel(object):
    """
    An object of this class represents a single object-tree-level in a reported change.
    A double-linked list of these object describes a single change on all of its levels.
    Looking at the tree of all changes, a list of those objects represents a single path through the tree
    (which is just fancy for "a change").
    This is the result object class for object reference style reports.

    Example:

    >>> t1 = {2: 2, 4: 44}
    >>> t2 = {2: "b", 5: 55}
    >>> ddiff = DeepDiff(t1, t2, view='tree')
    >>> ddiff
    {'dictionary_item_added': {<DiffLevel id:4560126096, t1:None, t2:55>},
     'dictionary_item_removed': {<DiffLevel id:4560126416, t1:44, t2:None>},
     'type_changes': {<DiffLevel id:4560126608, t1:2, t2:b>}}

    Graph:

    <DiffLevel id:123, original t1,t2>          <DiffLevel id:200, original t1,t2>
                    ↑up                                         ↑up
                    |                                           |
                    | ChildRelationship                         | ChildRelationship
                    |                                           |
                    ↓down                                       ↓down
    <DiffLevel id:13, t1:None, t2:55>            <DiffLevel id:421, t1:44, t2:None>
    .path() = 'root[5]'                         .path() = 'root[4]'

    Note that the 2 top level DiffLevel objects are 2 different objects even though
    they are essentially talking about the same diff operation.


    A ChildRelationship object describing the relationship between t1 and it's child object,
    where t1's child object equals down.t1.

    Think about it like a graph:

    +---------------------------------------------------------------+
    |                                                               |
    |    parent                 difflevel                 parent    |
    |      +                          ^                     +       |
    +------|--------------------------|---------------------|-------+
           |                      |   | up                  |
           | Child                |   |                     | ChildRelationship
           | Relationship         |   |                     |
           |                 down |   |                     |
    +------|----------------------|-------------------------|-------+
    |      v                      v                         v       |
    |    child                  difflevel                 child     |
    |                                                               |
    +---------------------------------------------------------------+


    The child_rel example:

    # dictionary_item_removed is a set so in order to get an item from it:
    >>> (difflevel,) = ddiff['dictionary_item_removed'])
    >>> difflevel.up.t1_child_rel
    <DictRelationship id:456, parent:{2: 2, 4: 44}, child:44, param:4>

    >>> (difflevel,) = ddiff['dictionary_item_added'])
    >>> difflevel
    <DiffLevel id:4560126096, t1:None, t2:55>

    >>> difflevel.up
    >>> <DiffLevel id:4560154512, t1:{2: 2, 4: 44}, t2:{2: 'b', 5: 55}>

    >>> difflevel.up
    <DiffLevel id:4560154512, t1:{2: 2, 4: 44}, t2:{2: 'b', 5: 55}>

    # t1 didn't exist
    >>> difflevel.up.t1_child_rel

    # t2 is added
    >>> difflevel.up.t2_child_rel
    <DictRelationship id:4560154384, parent:{2: 'b', 5: 55}, child:55, param:5>

    """

    def __init__(self,
                 t1,
                 t2,
                 down=None,
                 up=None,
                 report_type=None,
                 child_rel1=None,
                 child_rel2=None,
                 additional=None,
                 verbose_level=1):
        """
        :param child_rel1: Either:
                            - An existing ChildRelationship object describing the "down" relationship for t1; or
                            - A ChildRelationship subclass. In this case, we will create the ChildRelationship objects
                              for both t1 and t2.
                            Alternatives for child_rel1 and child_rel2 must be used consistently.
        :param child_rel2: Either:
                            - An existing ChildRelationship object describing the "down" relationship for t2; or
                            - The param argument for a ChildRelationship class we shall create.
                           Alternatives for child_rel1 and child_rel2 must be used consistently.
        """

        # The current-level object in the left hand tree
        self.t1 = t1

        # The current-level object in the right hand tree
        self.t2 = t2

        # Another DiffLevel object describing this change one level deeper down the object tree
        self.down = down

        # Another DiffLevel object describing this change one level further up the object tree
        self.up = up

        self.report_type = report_type

        # If this object is this change's deepest level, this contains a string describing the type of change.
        # Examples: "set_item_added", "values_changed"

        # Note: don't use {} as additional's default value - this would turn out to be always the same dict object
        self.additional = {} if additional is None else additional

        # For some types of changes we store some additional information.
        # This is a dict containing this information.
        # Currently, this is used for:
        # - values_changed: In case the changes data is a multi-line string,
        #                   we include a textual diff as additional['diff'].
        # - repetition_change: additional['repetition']:
        #                      e.g. {'old_repeat': 2, 'new_repeat': 1, 'old_indexes': [0, 2], 'new_indexes': [2]}
        # the user supplied ChildRelationship objects for t1 and t2

        # A ChildRelationship object describing the relationship between t1 and it's child object,
        # where t1's child object equals down.t1.
        # If this relationship is representable as a string, str(self.t1_child_rel) returns a formatted param parsable python string,
        # e.g. "[2]", ".my_attribute"
        self.t1_child_rel = child_rel1

        # Another ChildRelationship object describing the relationship between t2 and it's child object.
        self.t2_child_rel = child_rel2

        # Will cache result of .path() per 'force' as key for performance
        self._path = {}

    def __repr__(self):
        if Verbose.level:
            if self.additional:
                additional_repr = short_repr(self.additional, max_length=35)
                result = "<{} {}>".format(self.path(), additional_repr)
            else:
                t1_repr = short_repr(self.t1)
                t2_repr = short_repr(self.t2)
                result = "<{} t1:{}, t2:{}>".format(self.path(), t1_repr, t2_repr)
        else:
            result = "<{}>".format(self.path())
        return result

    def __setattr__(self, key, value):
        # Setting up or down, will set the opposite link in this linked list.
        if key in UP_DOWN and value is not None:
            self.__dict__[key] = value
            opposite_key = UP_DOWN[key]
            value.__dict__[opposite_key] = self
        else:
            self.__dict__[key] = value

    @property
    def repetition(self):
        return self.additional['repetition']

    def auto_generate_child_rel(self, klass, param):
        """
        Auto-populate self.child_rel1 and self.child_rel2.
        This requires self.down to be another valid DiffLevel object.
        :param klass: A ChildRelationship subclass describing the kind of parent-child relationship,
                      e.g. DictRelationship.
        :param param: A ChildRelationship subclass-dependent parameter describing how to get from parent to child,
                      e.g. the key in a dict
        """
        if self.down.t1 is not notpresent:
            self.t1_child_rel = ChildRelationship.create(
                klass=klass, parent=self.t1, child=self.down.t1, param=param)
        if self.down.t2 is not notpresent:
            self.t2_child_rel = ChildRelationship.create(
                klass=klass, parent=self.t2, child=self.down.t2, param=param)

    @property
    def all_up(self):
        """
        Get the root object of this comparison.
        (This is a convenient wrapper for following the up attribute as often as you can.)
        :rtype: DiffLevel
        """
        level = self
        while level.up:
            level = level.up
        return level

    @property
    def all_down(self):
        """
        Get the leaf object of this comparison.
        (This is a convenient wrapper for following the down attribute as often as you can.)
        :rtype: DiffLevel
        """
        level = self
        while level.down:
            level = level.down
        return level

    def path(self, root="root", force=None):
        """
        A python syntax string describing how to descend to this level, assuming the top level object is called root.
        Returns None if the path is not representable as a string.
        This might be the case for example if there are sets involved (because then there's not path at all) or because
        custom objects used as dictionary keys (then there is a path but it's not representable).
        Example: root['ingredients'][0]
        Note: We will follow the left side of the comparison branch, i.e. using the t1's to build the path.
        Using t1 or t2 should make no difference at all, except for the last step of a child-added/removed relationship.
        If it does in any other case, your comparison path is corrupt.
        :param root: The result string shall start with this var name
        :param force: Bends the meaning of "no string representation".
                      If None:
                        Will strictly return Python-parsable expressions. The result those yield will compare
                        equal to the objects in question.
                      If 'yes':
                        Will return a path including '(unrepresentable)' in place of non string-representable parts.
                      If 'fake':
                        Will try to produce an output optimized for readability.
                        This will pretend all iterables are subscriptable, for example.
        """
        # TODO: We could optimize this by building on top of self.up's path if it is cached there
        if force in self._path:
            result = self._path[force]
            return None if result is None else "{}{}".format(root, result)

        result = ""
        level = self.all_up  # start at the root

        # traverse all levels of this relationship
        while level and level is not self:
            # get this level's relationship object
            next_rel = level.t1_child_rel or level.t2_child_rel  # next relationship object to get a formatted param from

            # t1 and t2 both are empty
            if next_rel is None:
                break

            # Build path for this level
            item = next_rel.get_param_repr(force)
            if item:
                result += item
            else:
                # it seems this path is not representable as a string
                result = None
                break

            # Prepare processing next level
            level = level.down

        self._path[force] = result
        result = None if result is None else "{}{}".format(root, result)
        return result

    def create_deeper(self,
                      new_t1,
                      new_t2,
                      child_relationship_class,
                      child_relationship_param=None,
                      report_type=None):
        """
        Start a new comparison level and correctly link it to this one.
        :rtype: DiffLevel
        :return: New level
        """
        level = self.all_down
        result = DiffLevel(
            new_t1, new_t2, down=None, up=level, report_type=report_type)
        level.down = result
        level.auto_generate_child_rel(
            klass=child_relationship_class, param=child_relationship_param)
        return result

    def branch_deeper(self,
                      new_t1,
                      new_t2,
                      child_relationship_class,
                      child_relationship_param=None,
                      report_type=None):
        """
        Branch this comparison: Do not touch this comparison line, but create a new one with exactly the same content,
        just one level deeper.
        :rtype: DiffLevel
        :return: New level in new comparison line
        """
        branch = self.copy()
        return branch.create_deeper(new_t1, new_t2, child_relationship_class,
                                    child_relationship_param, report_type)

    def copy(self):
        """
        Get a deep copy of this comparision line.
        :return: The leaf ("downmost") object of the copy.
        """
        orig = self.all_up
        result = copy(orig)  # copy top level

        while orig is not None:
            result.additional = copy(orig.additional)

            if orig.down is not None:  # copy and create references to the following level
                # copy following level
                result.down = copy(orig.down)

                if orig.t1_child_rel is not None:
                    result.t1_child_rel = ChildRelationship.create(
                        klass=orig.t1_child_rel.__class__,
                        parent=result.t1,
                        child=result.down.t1,
                        param=orig.t1_child_rel.param)
                if orig.t2_child_rel is not None:
                    result.t2_child_rel = ChildRelationship.create(
                        klass=orig.t2_child_rel.__class__,
                        parent=result.t2,
                        child=result.down.t2,
                        param=orig.t2_child_rel.param)

            # descend to next level
            orig = orig.down
            if result.down is not None:
                result = result.down
        return result


class ChildRelationship(object):
    """
    Describes the relationship between a container object (the "parent") and the contained
    "child" object.
    """

    # Format to a be used for representing param.
    # E.g. for a dict, this turns a formatted param param "42" into "[42]".
    param_repr_format = None

    # This is a hook allowing subclasses to manipulate param strings.
    # :param string: Input string
    # :return: Manipulated string, as appropriate in this context.
    quote_str = None

    @staticmethod
    def create(klass, parent, child, param=None):
        if not issubclass(klass, ChildRelationship):
            raise TypeError
        return klass(parent, child, param)

    def __init__(self, parent, child, param=None):
        # The parent object of this relationship, e.g. a dict
        self.parent = parent

        # The child object of this relationship, e.g. a value in a dict
        self.child = child

        # A subclass-dependent parameter describing how to get from parent to child, e.g. the key in a dict
        self.param = param

    def __repr__(self):
        name = "<{} parent:{}, child:{}, param:{}>"
        parent = short_repr(self.parent)
        child = short_repr(self.child)
        param = short_repr(self.param)
        return name.format(self.__class__.__name__, parent, child, param)

    def get_param_repr(self, force=None):
        """
        Returns a formatted param python parsable string describing this relationship,
        or None if the relationship is not representable as a string.
        This string can be appended to the parent Name.
        Subclasses representing a relationship that cannot be expressed as a string override this method to return None.
        Examples: "[2]", ".attribute", "['mykey']"
        :param force: Bends the meaning of "no string representation".
              If None:
                Will strictly return partials of Python-parsable expressions. The result those yield will compare
                equal to the objects in question.
              If 'yes':
                Will return a formatted param including '(unrepresentable)' instead of the non string-representable part.

        """
        return self.stringify_param(force)

    def get_param_from_obj(self, obj):  # pragma: no cover
        """
        Get item from external object.

        This is used to get the item with the same path from another object.
        This way you can apply the path tree to any object.
        """
        pass

    def stringify_param(self, force=None):
        """
        Convert param to a string. Return None if there is no string representation.
        This is called by get_param_repr()
        :param force: Bends the meaning of "no string representation".
                      If None:
                        Will strictly return Python-parsable expressions. The result those yield will compare
                        equal to the objects in question.
                      If 'yes':
                        Will return '(unrepresentable)' instead of None if there is no string representation
        """
        param = self.param
        if isinstance(param, strings):
            result = param if self.quote_str is None else self.quote_str.format(param)
        else:
            candidate = str(param)
            try:
                resurrected = literal_eval(candidate)
                # Note: This will miss string-representable custom objects.
                # However, the only alternative I can currently think of is using eval() which is inherently dangerous.
            except (SyntaxError, ValueError):
                result = None
            else:
                result = candidate if resurrected == param else None

        if result:
            result = ':' if self.param_repr_format is None else self.param_repr_format.format(result)

        return result


class DictRelationship(ChildRelationship):
    param_repr_format = "[{}]"
    quote_str = "'{}'"

    def get_param_from_obj(self, obj):
        return obj.get(self.param)


class SubscriptableIterableRelationship(DictRelationship):
    # for our purposes, we can see lists etc. as special cases of dicts

    def get_param_from_obj(self, obj):
        return obj[self.param]


class InaccessibleRelationship(ChildRelationship):
    pass


# there is no random access to set elements
class SetRelationship(InaccessibleRelationship):
    pass


class NonSubscriptableIterableRelationship(InaccessibleRelationship):

    param_repr_format = "[{}]"

    def get_param_repr(self, force=None):
        if force == 'yes':
            result = "(unrepresentable)"
        elif force == 'fake' and self.param:
            result = self.stringify_param()
        else:
            result = None

        return result


class AttributeRelationship(ChildRelationship):
    param_repr_format = ".{}"

    def get_param_from_obj(self, obj):
        return getattr(obj, self.param)
