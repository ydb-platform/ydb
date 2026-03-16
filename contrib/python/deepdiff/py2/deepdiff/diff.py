#!/usr/bin/env python
# -*- coding: utf-8 -*-

# In order to run the docstrings:
# python3 -m deepdiff.diff
# You might need to run it many times since dictionaries come in different orders
# every time you run the docstrings.
# However the docstring expects it in a specific order in order to pass!

from __future__ import absolute_import
from __future__ import print_function

import difflib
import logging
import jsonpickle

from decimal import Decimal

from collections import Mapping
from collections import Iterable

from deepdiff.helper import py3, strings, bytes_type, numbers, ListItemRemovedOrAdded, notpresent, IndexedHash, Verbose
from deepdiff.model import RemapDict, ResultDict, TextResult, TreeResult, DiffLevel
from deepdiff.model import DictRelationship, AttributeRelationship  # , REPORT_KEYS
from deepdiff.model import SubscriptableIterableRelationship, NonSubscriptableIterableRelationship, SetRelationship
from deepdiff.contenthash import DeepHash

if py3:  # pragma: no cover
    from itertools import zip_longest
else:  # pragma: no cover
    from itertools import izip_longest as zip_longest

logger = logging.getLogger(__name__)


class DeepDiff(ResultDict):
    r"""
    **DeepDiff**

    Deep Difference of dictionaries, iterables, strings and almost any other object.
    It will recursively look for all the changes.

    DeepDiff 3.0 added the concept of views.
    There is a default "text" view and a "tree" view.

    **Parameters**

    t1 : A dictionary, list, string or any python object that has __dict__ or __slots__
        This is the first item to be compared to the second item

    t2 : dictionary, list, string or almost any python object that has __dict__ or __slots__
        The second item is to be compared to the first one

    ignore_order : Boolean, defalt=False ignores orders for iterables.
        Note that if you have iterables contatining any unhashable, ignoring order can be expensive.
        Normally ignore_order does not report duplicates and repetition changes.
        In order to report repetitions, set report_repetition=True in addition to ignore_order=True

    report_repetition : Boolean, default=False reports repetitions when set True
        ONLY when ignore_order is set True too. This works for iterables.
        This feature currently is experimental and is not production ready.

    significant_digits : int >= 0, default=None.
        If it is a non negative integer, it compares only that many digits AFTER
        the decimal point.

        This only affects floats, decimal.Decimal and complex.

        Internally it uses "{:.Xf}".format(Your Number) to compare numbers where X=significant_digits

        Note that "{:.3f}".format(1.1135) = 1.113, but "{:.3f}".format(1.11351) = 1.114

        For Decimals, Python's format rounds 2.5 to 2 and 3.5 to 4 (to the closest even number)

    verbose_level : int >= 0, default = 1.
        Higher verbose level shows you more details.
        For example verbose level 1 shows what dictionary item are added or removed.
        And verbose level 2 shows the value of the items that are added or removed too.

    exclude_paths: list, default = None.
        List of paths to exclude from the report.

    exclude_types: list, default = None.
        List of object types to exclude from the report.

    view: string, default = text
        Starting the version 3 you can choosethe view into the deepdiff results.
        The default is the text view which has been the only view up until now.
        The new view is called the tree view which allows you to traverse through
        the tree of changed items.

    **Returns**

        A DeepDiff object that has already calculated the difference of the 2 items.

    **Supported data types**

    int, string, unicode, dictionary, list, tuple, set, frozenset, OrderedDict, NamedTuple and custom objects!

    **Text View**

    Text view is the original and currently the default view of DeepDiff.

    It is called text view because the results contain texts that represent the path to the data:

    Example of using the text view.
        >>> from deepdiff import DeepDiff
        >>> t1 = {1:1, 3:3, 4:4}
        >>> t2 = {1:1, 3:3, 5:5, 6:6}
        >>> ddiff = DeepDiff(t1, t2)
        >>> print(ddiff)
        {'dictionary_item_added': {'root[5]', 'root[6]'}, 'dictionary_item_removed': {'root[4]'}}

    So for example ddiff['dictionary_item_removed'] is a set if strings thus this is called the text view.

    .. seealso::
        The following examples are using the *default text view.*
        The Tree View is introduced in DeepDiff v3 and provides
        traversing capabilitie through your diffed data and more!
        Read more about the Tree View at the bottom of this page.

    Importing
        >>> from deepdiff import DeepDiff
        >>> from pprint import pprint

    Same object returns empty
        >>> t1 = {1:1, 2:2, 3:3}
        >>> t2 = t1
        >>> print(DeepDiff(t1, t2))
        {}

    Type of an item has changed
        >>> t1 = {1:1, 2:2, 3:3}
        >>> t2 = {1:1, 2:"2", 3:3}
        >>> pprint(DeepDiff(t1, t2), indent=2)
        { 'type_changes': { 'root[2]': { 'new_type': <class 'str'>,
                                         'new_value': '2',
                                         'old_type': <class 'int'>,
                                         'old_value': 2}}}

    Value of an item has changed
        >>> t1 = {1:1, 2:2, 3:3}
        >>> t2 = {1:1, 2:4, 3:3}
        >>> pprint(DeepDiff(t1, t2, verbose_level=0), indent=2)
        {'values_changed': {'root[2]': {'new_value': 4, 'old_value': 2}}}

    Item added and/or removed
        >>> t1 = {1:1, 3:3, 4:4}
        >>> t2 = {1:1, 3:3, 5:5, 6:6}
        >>> ddiff = DeepDiff(t1, t2)
        >>> pprint (ddiff)
        {'dictionary_item_added': {'root[5]', 'root[6]'},
         'dictionary_item_removed': {'root[4]'}}

    Set verbose level to 2 in order to see the added or removed items with their values
        >>> t1 = {1:1, 3:3, 4:4}
        >>> t2 = {1:1, 3:3, 5:5, 6:6}
        >>> ddiff = DeepDiff(t1, t2, verbose_level=2)
        >>> pprint(ddiff, indent=2)
        { 'dictionary_item_added': {'root[5]': 5, 'root[6]': 6},
          'dictionary_item_removed': {'root[4]': 4}}

    String difference
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":"world"}}
        >>> t2 = {1:1, 2:4, 3:3, 4:{"a":"hello", "b":"world!"}}
        >>> ddiff = DeepDiff(t1, t2)
        >>> pprint (ddiff, indent = 2)
        { 'values_changed': { 'root[2]': {'new_value': 4, 'old_value': 2},
                              "root[4]['b']": { 'new_value': 'world!',
                                                'old_value': 'world'}}}


    String difference 2
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":"world!\nGoodbye!\n1\n2\nEnd"}}
        >>> t2 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":"world\n1\n2\nEnd"}}
        >>> ddiff = DeepDiff(t1, t2)
        >>> pprint (ddiff, indent = 2)
        { 'values_changed': { "root[4]['b']": { 'diff': '--- \n'
                                                        '+++ \n'
                                                        '@@ -1,5 +1,4 @@\n'
                                                        '-world!\n'
                                                        '-Goodbye!\n'
                                                        '+world\n'
                                                        ' 1\n'
                                                        ' 2\n'
                                                        ' End',
                                                'new_value': 'world\n1\n2\nEnd',
                                                'old_value': 'world!\n'
                                                             'Goodbye!\n'
                                                             '1\n'
                                                             '2\n'
                                                             'End'}}}

        >>>
        >>> print (ddiff['values_changed']["root[4]['b']"]["diff"])
        --- 
        +++ 
        @@ -1,5 +1,4 @@
        -world!
        -Goodbye!
        +world
         1
         2
         End


    Type change
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, 3]}}
        >>> t2 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":"world\n\n\nEnd"}}
        >>> ddiff = DeepDiff(t1, t2)
        >>> pprint (ddiff, indent = 2)
        { 'type_changes': { "root[4]['b']": { 'new_type': <class 'str'>,
                                              'new_value': 'world\n\n\nEnd',
                                              'old_type': <class 'list'>,
                                              'old_value': [1, 2, 3]}}}

    And if you don't care about the value of items that have changed type, please set verbose level to 0
        >>> t1 = {1:1, 2:2, 3:3}
        >>> t2 = {1:1, 2:"2", 3:3}
        >>> pprint(DeepDiff(t1, t2, verbose_level=0), indent=2)
        { 'type_changes': { 'root[2]': { 'new_type': <class 'str'>,
                                         'old_type': <class 'int'>}}}

    List difference
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, 3, 4]}}
        >>> t2 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2]}}
        >>> ddiff = DeepDiff(t1, t2)
        >>> pprint (ddiff, indent = 2)
        {'iterable_item_removed': {"root[4]['b'][2]": 3, "root[4]['b'][3]": 4}}

    List difference 2:
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, 3]}}
        >>> t2 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 3, 2, 3]}}
        >>> ddiff = DeepDiff(t1, t2)
        >>> pprint (ddiff, indent = 2)
        { 'iterable_item_added': {"root[4]['b'][3]": 3},
          'values_changed': { "root[4]['b'][1]": {'new_value': 3, 'old_value': 2},
                              "root[4]['b'][2]": {'new_value': 2, 'old_value': 3}}}

    List difference ignoring order or duplicates: (with the same dictionaries as above)
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, 3]}}
        >>> t2 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 3, 2, 3]}}
        >>> ddiff = DeepDiff(t1, t2, ignore_order=True)
        >>> print (ddiff)
        {}

    List difference ignoring order but reporting repetitions:
        >>> from deepdiff import DeepDiff
        >>> from pprint import pprint
        >>> t1 = [1, 3, 1, 4]
        >>> t2 = [4, 4, 1]
        >>> ddiff = DeepDiff(t1, t2, ignore_order=True, report_repetition=True)
        >>> pprint(ddiff, indent=2)
        { 'iterable_item_removed': {'root[1]': 3},
          'repetition_change': { 'root[0]': { 'new_indexes': [2],
                                              'new_repeat': 1,
                                              'old_indexes': [0, 2],
                                              'old_repeat': 2,
                                              'value': 1},
                                 'root[3]': { 'new_indexes': [0, 1],
                                              'new_repeat': 2,
                                              'old_indexes': [3],
                                              'old_repeat': 1,
                                              'value': 4}}}

    List that contains dictionary:
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, {1:1, 2:2}]}}
        >>> t2 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, {1:3}]}}
        >>> ddiff = DeepDiff(t1, t2)
        >>> pprint (ddiff, indent = 2)
        { 'dictionary_item_removed': {"root[4]['b'][2][2]"},
          'values_changed': {"root[4]['b'][2][1]": {'new_value': 3, 'old_value': 1}}}

    Sets:
        >>> t1 = {1, 2, 8}
        >>> t2 = {1, 2, 3, 5}
        >>> ddiff = DeepDiff(t1, t2)
        >>> pprint(ddiff)
        {'set_item_added': {'root[5]', 'root[3]'}, 'set_item_removed': {'root[8]'}}

    Named Tuples:
        >>> from collections import namedtuple
        >>> Point = namedtuple('Point', ['x', 'y'])
        >>> t1 = Point(x=11, y=22)
        >>> t2 = Point(x=11, y=23)
        >>> pprint (DeepDiff(t1, t2))
        {'values_changed': {'root.y': {'new_value': 23, 'old_value': 22}}}

    Custom objects:
        >>> class ClassA(object):
        ...     a = 1
        ...     def __init__(self, b):
        ...         self.b = b
        ...
        >>> t1 = ClassA(1)
        >>> t2 = ClassA(2)
        >>>
        >>> pprint(DeepDiff(t1, t2))
        {'values_changed': {'root.b': {'new_value': 2, 'old_value': 1}}}

    Object attribute added:
        >>> t2.c = "new attribute"
        >>> pprint(DeepDiff(t1, t2))
        {'attribute_added': {'root.c'},
         'values_changed': {'root.b': {'new_value': 2, 'old_value': 1}}}

    Approximate decimals comparison (Significant digits after the point):
        >>> t1 = Decimal('1.52')
        >>> t2 = Decimal('1.57')
        >>> DeepDiff(t1, t2, significant_digits=0)
        {}
        >>> DeepDiff(t1, t2, significant_digits=1)
        {'values_changed': {'root': {'old_value': Decimal('1.52'), 'new_value': Decimal('1.57')}}}

    Approximate float comparison (Significant digits after the point):
        >>> t1 = [ 1.1129, 1.3359 ]
        >>> t2 = [ 1.113, 1.3362 ]
        >>> pprint(DeepDiff(t1, t2, significant_digits=3))
        {}
        >>> pprint(DeepDiff(t1, t2))
        {'values_changed': {'root[0]': {'new_value': 1.113, 'old_value': 1.1129},
                            'root[1]': {'new_value': 1.3362, 'old_value': 1.3359}}}
        >>> pprint(DeepDiff(1.23*10**20, 1.24*10**20, significant_digits=1))
        {'values_changed': {'root': {'new_value': 1.24e+20, 'old_value': 1.23e+20}}}


    .. note::
        All the examples for the text view work for the tree view too. You just need to set view='tree' to get it in tree form.


    **Tree View**

    Starting the version 3 You can chooe the view into the deepdiff results.
    The tree view provides you with tree objects that you can traverse through to find
    the parents of the objects that are diffed and the actual objects that are being diffed.
    This view is very useful when dealing with nested objects.
    Note that tree view always returns results in the form of Python sets.

    You can traverse through the tree elements!

    .. note::
        The Tree view is just a different representation of the diffed data.
        Behind the scene, DeepDiff creates the tree view first and then converts it to textual representation for the text view.

    .. code:: text

        +---------------------------------------------------------------+
        |                                                               |
        |    parent(t1)              parent node            parent(t2)  |
        |      +                          ^                     +       |
        +------|--------------------------|---------------------|-------+
               |                      |   | up                  |
               | Child                |   |                     | ChildRelationship
               | Relationship         |   |                     |
               |                 down |   |                     |
        +------|----------------------|-------------------------|-------+
        |      v                      v                         v       |
        |    child(t1)              child node               child(t2)  |
        |                                                               |
        +---------------------------------------------------------------+


    :up: Move up to the parent node
    :down: Move down to the child node
    :path(): Get the path to the current node
    :t1: The first item in the current node that is being diffed
    :t2: The second item in the current node that is being diffed
    :additional: Additional information about the node i.e. repetition
    :repetition: Shortcut to get the repetition report


    The tree view allows you to have more than mere textual representaion of the diffed objects.
    It gives you the actual objects (t1, t2) throughout the tree of parents and children.

    **Examples Tree View**

    .. note::
        The Tree View is introduced in DeepDiff 3.
        Set view='tree' in order to use this view.

    Value of an item has changed (Tree View)
        >>> from deepdiff import DeepDiff
        >>> from pprint import pprint
        >>> t1 = {1:1, 2:2, 3:3}
        >>> t2 = {1:1, 2:4, 3:3}
        >>> ddiff_verbose0 = DeepDiff(t1, t2, verbose_level=0, view='tree')
        >>> ddiff_verbose0
        {'values_changed': {<root[2]>}}
        >>>
        >>> ddiff_verbose1 = DeepDiff(t1, t2, verbose_level=1, view='tree')
        >>> ddiff_verbose1
        {'values_changed': {<root[2] t1:2, t2:4>}}
        >>> set_of_values_changed = ddiff_verbose1['values_changed']
        >>> # since set_of_values_changed includes only one item in a set
        >>> # in order to get that one item we can:
        >>> (changed,) = set_of_values_changed
        >>> changed  # Another way to get this is to do: changed=list(set_of_values_changed)[0]
        <root[2] t1:2, t2:4>
        >>> changed.t1
        2
        >>> changed.t2
        4
        >>> # You can traverse through the tree, get to the parents!
        >>> changed.up
        <root t1:{1: 1, 2: 2,...}, t2:{1: 1, 2: 4,...}>

    List difference (Tree View)
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, 3, 4]}}
        >>> t2 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2]}}
        >>> ddiff = DeepDiff(t1, t2, view='tree')
        >>> ddiff
        {'iterable_item_removed': {<root[4]['b'][3] t1:4, t2:Not Present>, <root[4]['b'][2] t1:3, t2:Not Present>}}
        >>> # Note that the iterable_item_removed is a set. In this case it has 2 items in it.
        >>> # One way to get one item from the set is to convert it to a list
        >>> # And then get the first item of the list:
        >>> removed = list(ddiff['iterable_item_removed'])[0]
        >>> removed
        <root[4]['b'][2] t1:3, t2:Not Present>
        >>>
        >>> parent = removed.up
        >>> parent
        <root[4]['b'] t1:[1, 2, 3, 4], t2:[1, 2]>
        >>> parent.path()
        "root[4]['b']"
        >>> parent.t1
        [1, 2, 3, 4]
        >>> parent.t2
        [1, 2]
        >>> parent.up
        <root[4] t1:{'a': 'hello...}, t2:{'a': 'hello...}>
        >>> parent.up.up
        <root t1:{1: 1, 2: 2,...}, t2:{1: 1, 2: 2,...}>
        >>> parent.up.up.t1
        {1: 1, 2: 2, 3: 3, 4: {'a': 'hello', 'b': [1, 2, 3, 4]}}
        >>> parent.up.up.t1 == t1  # It is holding the original t1 that we passed to DeepDiff
        True

    List difference 2  (Tree View)
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, 3]}}
        >>> t2 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 3, 2, 3]}}
        >>> ddiff = DeepDiff(t1, t2, view='tree')
        >>> pprint(ddiff, indent = 2)
        { 'iterable_item_added': {<root[4]['b'][3] t1:Not Present, t2:3>},
          'values_changed': { <root[4]['b'][1] t1:2, t2:3>,
                              <root[4]['b'][2] t1:3, t2:2>}}
        >>>
        >>> # Note that iterable_item_added is a set with one item.
        >>> # So in order to get that one item from it, we can do:
        >>>
        >>> (added,) = ddiff['iterable_item_added']
        >>> added
        <root[4]['b'][3] t1:Not Present, t2:3>
        >>> added.up.up
        <root[4] t1:{'a': 'hello...}, t2:{'a': 'hello...}>
        >>> added.up.up.path()
        'root[4]'
        >>> added.up.up.down
        <root[4]['b'] t1:[1, 2, 3], t2:[1, 3, 2, 3]>
        >>>
        >>> # going up twice and then down twice gives you the same node in the tree:
        >>> added.up.up.down.down == added
        True

    List difference ignoring order but reporting repetitions (Tree View)
        >>> t1 = [1, 3, 1, 4]
        >>> t2 = [4, 4, 1]
        >>> ddiff = DeepDiff(t1, t2, ignore_order=True, report_repetition=True, view='tree')
        >>> pprint(ddiff, indent=2)
        { 'iterable_item_removed': {<root[1] t1:3, t2:Not Present>},
          'repetition_change': { <root[3] {'repetition': {'old_repeat': 1,...}>,
                                 <root[0] {'repetition': {'old_repeat': 2,...}>}}
        >>>
        >>> # repetition_change is a set with 2 items.
        >>> # in order to get those 2 items, we can do the following.
        >>> # or we can convert the set to list and get the list items.
        >>> # or we can iterate through the set items
        >>>
        >>> (repeat1, repeat2) = ddiff['repetition_change']
        >>> repeat1  # the default verbosity is set to 1.
        <root[0] {'repetition': {'old_repeat': 2,...}>
        >>> # The actual data regarding the repetitions can be found in the repetition attribute:
        >>> repeat1.repetition
        {'old_repeat': 1, 'new_repeat': 2, 'old_indexes': [3], 'new_indexes': [0, 1]}
        >>>
        >>> # If you change the verbosity, you will see less:
        >>> ddiff = DeepDiff(t1, t2, ignore_order=True, report_repetition=True, view='tree', verbose_level=0)
        >>> ddiff
        {'repetition_change': {<root[3]>, <root[0]>}, 'iterable_item_removed': {<root[1]>}}
        >>> (repeat1, repeat2) = ddiff['repetition_change']
        >>> repeat1
        <root[0]>
        >>>
        >>> # But the verbosity level does not change the actual report object.
        >>> # It only changes the textual representaion of the object. We get the actual object here:
        >>> repeat1.repetition
        {'old_repeat': 1, 'new_repeat': 2, 'old_indexes': [3], 'new_indexes': [0, 1]}
        >>> repeat1.t1
        4
        >>> repeat1.t2
        4
        >>> repeat1.up
        <root>

    List that contains dictionary (Tree View)
        >>> t1 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, {1:1, 2:2}]}}
        >>> t2 = {1:1, 2:2, 3:3, 4:{"a":"hello", "b":[1, 2, {1:3}]}}
        >>> ddiff = DeepDiff(t1, t2, view='tree')
        >>> pprint (ddiff, indent = 2)
        { 'dictionary_item_removed': {<root[4]['b'][2][2] t1:2, t2:Not Present>},
          'values_changed': {<root[4]['b'][2][1] t1:1, t2:3>}}

    Sets (Tree View):
        >>> t1 = {1, 2, 8}
        >>> t2 = {1, 2, 3, 5}
        >>> ddiff = DeepDiff(t1, t2, view='tree')
        >>> print(ddiff)
        {'set_item_removed': {<root: t1:8, t2:Not Present>}, 'set_item_added': {<root: t1:Not Present, t2:5>, <root: t1:Not Present, t2:3>}}
        >>> # grabbing one item from set_item_removed set which has one item only
        >>> (item,) = ddiff['set_item_removed']
        >>> item.up
        <root t1:{8, 1, 2}, t2:{1, 2, 3, 5}>
        >>> item.up.t1 == t1
        True

    Named Tuples (Tree View):
        >>> from collections import namedtuple
        >>> Point = namedtuple('Point', ['x', 'y'])
        >>> t1 = Point(x=11, y=22)
        >>> t2 = Point(x=11, y=23)
        >>> print(DeepDiff(t1, t2, view='tree'))
        {'values_changed': {<root.y t1:22, t2:23>}}

    Custom objects (Tree View):
        >>> class ClassA(object):
        ...     a = 1
        ...     def __init__(self, b):
        ...         self.b = b
        ...
        >>> t1 = ClassA(1)
        >>> t2 = ClassA(2)
        >>>
        >>> print(DeepDiff(t1, t2, view='tree'))
        {'values_changed': {<root.b t1:1, t2:2>}}

    Object attribute added (Tree View):
        >>> t2.c = "new attribute"
        >>> pprint(DeepDiff(t1, t2, view='tree'))
        {'attribute_added': {<root.c t1:Not Present, t2:'new attribute'>},
         'values_changed': {<root.b t1:1, t2:2>}}

    Approximate decimals comparison (Significant digits after the point) (Tree View):
        >>> t1 = Decimal('1.52')
        >>> t2 = Decimal('1.57')
        >>> DeepDiff(t1, t2, significant_digits=0, view='tree')
        {}
        >>> ddiff = DeepDiff(t1, t2, significant_digits=1, view='tree')
        >>> ddiff
        {'values_changed': {<root t1:Decimal('1.52'), t2:Decimal('1.57')>}}
        >>> (change1,) = ddiff['values_changed']
        >>> change1
        <root t1:Decimal('1.52'), t2:Decimal('1.57')>
        >>> change1.t1
        Decimal('1.52')
        >>> change1.t2
        Decimal('1.57')
        >>> change1.path()
        'root'

    Approximate float comparison (Significant digits after the point) (Tree View):
        >>> t1 = [ 1.1129, 1.3359 ]
        >>> t2 = [ 1.113, 1.3362 ]
        >>> ddiff = DeepDiff(t1, t2, significant_digits=3, view='tree')
        >>> ddiff
        {}
        >>> ddiff = DeepDiff(t1, t2, view='tree')
        >>> pprint(ddiff, indent=2)
        { 'values_changed': { <root[0] t1:1.1129, t2:1.113>,
                              <root[1] t1:1.3359, t2:1.3362>}}
        >>> ddiff = DeepDiff(1.23*10**20, 1.24*10**20, significant_digits=1, view='tree')
        >>> ddiff
        {'values_changed': {<root t1:1.23e+20, t2:1.24e+20>}}


    .. note::
        All the examples for the text view work for the tree view too. You just need to set view='tree' to get it in tree form.

    **Serialization**

    DeepDiff uses jsonpickle in order to serialize and deserialize its results into json.

    Serialize and then deserialize back to deepdiff
        >>> t1 = {1: 1, 2: 2, 3: 3}
        >>> t2 = {1: 1, 2: "2", 3: 3}
        >>> ddiff = DeepDiff(t1, t2)
        >>> jsoned = ddiff.json
        >>> jsoned
        '{"type_changes": {"root[2]": {"py/object": "deepdiff.helper.RemapDict", "new_type": {"py/type": "__builtin__.str"}, "new_value": "2", "old_type": {"py/type": "__builtin__.int"}, "old_value": 2}}}'
        >>> ddiff_new = DeepDiff.from_json(jsoned)
        >>> ddiff == ddiff_new
        True

    **Pycon 2016 Talk**
    I gave a talk about how DeepDiff does what it does at Pycon 2016.
    `Diff it to Dig it Pycon 2016 video <https://www.youtube.com/watch?v=J5r99eJIxF4>`_

    And here is more info: http://zepworks.com/blog/diff-it-to-digg-it/


    """

    def __init__(self,
                 t1,
                 t2,
                 ignore_order=False,
                 report_repetition=False,
                 significant_digits=None,
                 exclude_paths=set(),
                 exclude_types=set(),
                 verbose_level=1,
                 view='text',
                 **kwargs):
        if kwargs:
            raise ValueError((
                "The following parameter(s) are not valid: %s\n"
                "The valid parameters are ignore_order, report_repetition, significant_digits,"
                "exclude_paths, exclude_types, verbose_level and view.") % ', '.join(kwargs.keys()))

        self.ignore_order = ignore_order
        self.report_repetition = report_repetition
        self.exclude_paths = set(exclude_paths)
        self.exclude_types = set(exclude_types)
        self.exclude_types_tuple = tuple(
            exclude_types)  # we need tuple for checking isinstance
        self.hashes = {}

        if significant_digits is not None and significant_digits < 0:
            raise ValueError(
                "significant_digits must be None or a non-negative integer")
        self.significant_digits = significant_digits

        self.tree = TreeResult()

        Verbose.level = verbose_level

        root = DiffLevel(t1, t2)
        self.__diff(root, parents_ids=frozenset({id(t1)}))

        self.tree.cleanup()

        if view == 'tree':
            self.update(self.tree)
            del self.tree
        else:
            result_text = TextResult(tree_results=self.tree)
            result_text.cleanup()  # clean up text-style result dictionary
            self.update(
                result_text
            )  # be compatible to DeepDiff 2.x if user didn't specify otherwise

    # TODO: adding adding functionality
    # def __add__(self, other):
    #     if isinstance(other, DeepDiff):
    #         result = deepcopy(self)
    #         result.update(other)
    #     else:
    #         result = deepcopy(other)
    #         for key in REPORT_KEYS:
    #             if key in self:
    #                 getattr(self, "_do_{}".format(key))(result)

    #     return result

    # __radd__ = __add__

    # def _do_iterable_item_added(self, result):
    #     for item in self['iterable_item_added']:
    #         pass

    def __report_result(self, report_type, level):
        """
        Add a detected change to the reference-style result dictionary.
        report_type will be added to level.
        (We'll create the text-style report from there later.)
        :param report_type: A well defined string key describing the type of change.
                            Examples: "set_item_added", "values_changed"
        :param parent: A DiffLevel object describing the objects in question in their
                       before-change and after-change object structure.

        :rtype: None
        """
        if not self.__skip_this(level):
            level.report_type = report_type
            self.tree[report_type].add(level)

    @staticmethod
    def __add_to_frozen_set(parents_ids, item_id):
        parents_ids = set(parents_ids)
        parents_ids.add(item_id)
        return frozenset(parents_ids)

    @staticmethod
    def __dict_from_slots(object):
        def unmangle(attribute):
            if attribute.startswith('__'):
                return '_{type}{attribute}'.format(
                    type=type(object).__name__,
                    attribute=attribute
                )
            return attribute

        slots = object.__slots__
        if isinstance(slots, strings):
            return {slots: getattr(object, unmangle(slots))}
        return {i: getattr(object, unmangle(i)) for i in slots}

    def __diff_obj(self, level, parents_ids=frozenset({}),
                   is_namedtuple=False):
        """Difference of 2 objects"""
        try:
            if is_namedtuple:
                t1 = level.t1._asdict()
                t2 = level.t2._asdict()
            else:
                t1 = level.t1.__dict__
                t2 = level.t2.__dict__
        except AttributeError:
            try:
                t1 = self.__dict_from_slots(level.t1)
                t2 = self.__dict_from_slots(level.t2)
            except AttributeError:
                self.__report_result('unprocessed', level)
                return

        self.__diff_dict(
            level,
            parents_ids,
            print_as_attribute=True,
            override=True,
            override_t1=t1,
            override_t2=t2)

    def __skip_this(self, level):
        """
        Check whether this comparison should be skipped because one of the objects to compare meets exclusion criteria.
        :rtype: bool
        """
        skip = False
        if self.exclude_paths and level.path() in self.exclude_paths:
            skip = True
        else:
            if isinstance(level.t1, self.exclude_types_tuple) or isinstance(
                    level.t2, self.exclude_types_tuple):
                skip = True

        return skip

    def __diff_dict(self,
                    level,
                    parents_ids=frozenset({}),
                    print_as_attribute=False,
                    override=False,
                    override_t1=None,
                    override_t2=None):
        """Difference of 2 dictionaries"""
        if override:
            # for special stuff like custom objects and named tuples we receive preprocessed t1 and t2
            # but must not spoil the chain (=level) with it
            t1 = override_t1
            t2 = override_t2
        else:
            t1 = level.t1
            t2 = level.t2

        if print_as_attribute:
            item_added_key = "attribute_added"
            item_removed_key = "attribute_removed"
            rel_class = AttributeRelationship
        else:
            item_added_key = "dictionary_item_added"
            item_removed_key = "dictionary_item_removed"
            rel_class = DictRelationship

        t1_keys = set(t1.keys())
        t2_keys = set(t2.keys())

        t_keys_intersect = t2_keys.intersection(t1_keys)

        t_keys_added = t2_keys - t_keys_intersect
        t_keys_removed = t1_keys - t_keys_intersect

        for key in t_keys_added:
            change_level = level.branch_deeper(
                notpresent,
                t2[key],
                child_relationship_class=rel_class,
                child_relationship_param=key)
            self.__report_result(item_added_key, change_level)

        for key in t_keys_removed:
            change_level = level.branch_deeper(
                t1[key],
                notpresent,
                child_relationship_class=rel_class,
                child_relationship_param=key)
            self.__report_result(item_removed_key, change_level)

        for key in t_keys_intersect:  # key present in both dicts - need to compare values
            item_id = id(t1[key])
            if parents_ids and item_id in parents_ids:
                continue
            parents_ids_added = self.__add_to_frozen_set(parents_ids, item_id)

            # Go one level deeper
            next_level = level.branch_deeper(
                t1[key],
                t2[key],
                child_relationship_class=rel_class,
                child_relationship_param=key)
            self.__diff(next_level, parents_ids_added)

    def __diff_set(self, level):
        """Difference of sets"""
        t1_hashtable = self.__create_hashtable(level.t1, level)
        t2_hashtable = self.__create_hashtable(level.t2, level)

        t1_hashes = set(t1_hashtable.keys())
        t2_hashes = set(t2_hashtable.keys())

        hashes_added = t2_hashes - t1_hashes
        hashes_removed = t1_hashes - t2_hashes

        items_added = [t2_hashtable[i].item for i in hashes_added]
        items_removed = [t1_hashtable[i].item for i in hashes_removed]

        for item in items_added:
            change_level = level.branch_deeper(
                notpresent, item, child_relationship_class=SetRelationship)
            self.__report_result('set_item_added', change_level)

        for item in items_removed:
            change_level = level.branch_deeper(
                item, notpresent, child_relationship_class=SetRelationship)
            self.__report_result('set_item_removed', change_level)

    @staticmethod
    def __iterables_subscriptable(t1, t2):
        try:
            if getattr(t1, '__getitem__') and getattr(t2, '__getitem__'):
                return True
            else:  # pragma: no cover
                return False  # should never happen
        except AttributeError:
            return False

    def __diff_iterable(self, level, parents_ids=frozenset({})):
        """Difference of iterables"""
        # We're handling both subscriptable and non-subscriptable iterables. Which one is it?
        subscriptable = self.__iterables_subscriptable(level.t1, level.t2)
        if subscriptable:
            child_relationship_class = SubscriptableIterableRelationship
        else:
            child_relationship_class = NonSubscriptableIterableRelationship

        for i, (x, y) in enumerate(
                zip_longest(
                    level.t1, level.t2, fillvalue=ListItemRemovedOrAdded)):
            if y is ListItemRemovedOrAdded:  # item removed completely
                change_level = level.branch_deeper(
                    x,
                    notpresent,
                    child_relationship_class=child_relationship_class,
                    child_relationship_param=i)
                self.__report_result('iterable_item_removed', change_level)

            elif x is ListItemRemovedOrAdded:  # new item added
                change_level = level.branch_deeper(
                    notpresent,
                    y,
                    child_relationship_class=child_relationship_class,
                    child_relationship_param=i)
                self.__report_result('iterable_item_added', change_level)

            else:  # check if item value has changed
                item_id = id(x)
                if parents_ids and item_id in parents_ids:
                    continue
                parents_ids_added = self.__add_to_frozen_set(parents_ids,
                                                             item_id)

                # Go one level deeper
                next_level = level.branch_deeper(
                    x,
                    y,
                    child_relationship_class=child_relationship_class,
                    child_relationship_param=i)
                self.__diff(next_level, parents_ids_added)

    def __diff_str(self, level):
        """Compare strings"""
        if level.t1 == level.t2:
            return

        # do we add a diff for convenience?
        do_diff = True
        if isinstance(level.t1, bytes_type):
            try:
                t1_str = level.t1.decode('ascii')
                t2_str = level.t2.decode('ascii')
            except UnicodeDecodeError:
                do_diff = False
        else:
            t1_str = level.t1
            t2_str = level.t2
        if do_diff:
            if u'\n' in t1_str or u'\n' in t2_str:
                diff = difflib.unified_diff(
                    t1_str.splitlines(), t2_str.splitlines(), lineterm='')
                diff = list(diff)
                if diff:
                    level.additional['diff'] = u'\n'.join(diff)

        self.__report_result('values_changed', level)

    def __diff_tuple(self, level, parents_ids):
        # Checking to see if it has _fields. Which probably means it is a named
        # tuple.
        try:
            level.t1._asdict
        # It must be a normal tuple
        except AttributeError:
            self.__diff_iterable(level, parents_ids)
        # We assume it is a namedtuple then
        else:
            self.__diff_obj(level, parents_ids, is_namedtuple=True)

    def __create_hashtable(self, t, level):
        """Create hashtable of {item_hash: item}"""

        def add_hash(hashes, item_hash, item, i):
            if item_hash in hashes:
                hashes[item_hash].indexes.append(i)
            else:
                hashes[item_hash] = IndexedHash([i], item)

        hashes = {}
        for (i, item) in enumerate(t):
            try:
                hashes_all = DeepHash(item,
                                      hashes=self.hashes,
                                      significant_digits=self.significant_digits)
                item_hash = hashes_all.get(id(item), item)
            except Exception as e:  # pragma: no cover
                logger.warning("Can not produce a hash for %s."
                               "Not counting this object.\n %s" %
                               (level.path(), e))
            else:
                if item_hash is hashes_all.unprocessed:  # pragma: no cover
                    logger.warning("Item %s was not processed while hashing "
                                   "thus not counting this object." %
                                   level.path())
                else:
                    add_hash(hashes, item_hash, item, i)
        return hashes

    def __diff_iterable_with_contenthash(self, level):
        """Diff of unhashable iterables. Only used when ignoring the order."""
        t1_hashtable = self.__create_hashtable(level.t1, level)
        t2_hashtable = self.__create_hashtable(level.t2, level)

        t1_hashes = set(t1_hashtable.keys())
        t2_hashes = set(t2_hashtable.keys())

        hashes_added = t2_hashes - t1_hashes
        hashes_removed = t1_hashes - t2_hashes

        if self.report_repetition:
            for hash_value in hashes_added:
                for i in t2_hashtable[hash_value].indexes:
                    change_level = level.branch_deeper(
                        notpresent,
                        t2_hashtable[hash_value].item,
                        child_relationship_class=SubscriptableIterableRelationship,  # TODO: that might be a lie!
                        child_relationship_param=i
                    )  # TODO: what is this value exactly?
                    self.__report_result('iterable_item_added', change_level)

            for hash_value in hashes_removed:
                for i in t1_hashtable[hash_value].indexes:
                    change_level = level.branch_deeper(
                        t1_hashtable[hash_value].item,
                        notpresent,
                        child_relationship_class=SubscriptableIterableRelationship,  # TODO: that might be a lie!
                        child_relationship_param=i)
                    self.__report_result('iterable_item_removed', change_level)

            items_intersect = t2_hashes.intersection(t1_hashes)

            for hash_value in items_intersect:
                t1_indexes = t1_hashtable[hash_value].indexes
                t2_indexes = t2_hashtable[hash_value].indexes
                t1_indexes_len = len(t1_indexes)
                t2_indexes_len = len(t2_indexes)
                if t1_indexes_len != t2_indexes_len:  # this is a repetition change!
                    # create "change" entry, keep current level untouched to handle further changes
                    repetition_change_level = level.branch_deeper(
                        t1_hashtable[hash_value].item,
                        t2_hashtable[hash_value].item,  # nb: those are equal!
                        child_relationship_class=SubscriptableIterableRelationship,  # TODO: that might be a lie!
                        child_relationship_param=t1_hashtable[hash_value]
                        .indexes[0])
                    repetition_change_level.additional['repetition'] = RemapDict(
                        old_repeat=t1_indexes_len,
                        new_repeat=t2_indexes_len,
                        old_indexes=t1_indexes,
                        new_indexes=t2_indexes)
                    self.__report_result('repetition_change',
                                         repetition_change_level)

        else:
            for hash_value in hashes_added:
                change_level = level.branch_deeper(
                    notpresent,
                    t2_hashtable[hash_value].item,
                    child_relationship_class=SubscriptableIterableRelationship,  # TODO: that might be a lie!
                    child_relationship_param=t2_hashtable[hash_value].indexes[
                        0])  # TODO: what is this value exactly?
                self.__report_result('iterable_item_added', change_level)

            for hash_value in hashes_removed:
                change_level = level.branch_deeper(
                    t1_hashtable[hash_value].item,
                    notpresent,
                    child_relationship_class=SubscriptableIterableRelationship,  # TODO: that might be a lie!
                    child_relationship_param=t1_hashtable[hash_value].indexes[
                        0])
                self.__report_result('iterable_item_removed', change_level)

    def __diff_numbers(self, level):
        """Diff Numbers"""

        if self.significant_digits is not None and isinstance(level.t1, (
                float, complex, Decimal)):
            # Bernhard10: I use string formatting for comparison, to be consistent with usecases where
            # data is read from files that were previousely written from python and
            # to be consistent with on-screen representation of numbers.
            # Other options would be abs(t1-t2)<10**-self.significant_digits
            # or math.is_close (python3.5+)
            # Note that abs(3.25-3.251) = 0.0009999999999998899 < 0.001
            # Note also that "{:.3f}".format(1.1135) = 1.113, but "{:.3f}".format(1.11351) = 1.114
            # For Decimals, format seems to round 2.5 to 2 and 3.5 to 4 (to closest even number)
            t1_s = ("{:.%sf}" % self.significant_digits).format(level.t1)
            t2_s = ("{:.%sf}" % self.significant_digits).format(level.t2)

            # Special case for 0: "-0.00" should compare equal to "0.00"
            if set(t1_s) <= set("-0.") and set(t2_s) <= set("-0."):
                return
            elif t1_s != t2_s:
                self.__report_result('values_changed', level)
        else:
            if level.t1 != level.t2:
                self.__report_result('values_changed', level)

    def __diff_types(self, level):
        """Diff types"""
        level.report_type = 'type_changes'
        self.__report_result('type_changes', level)

    def __diff(self, level, parents_ids=frozenset({})):
        """The main diff method"""
        if level.t1 is level.t2:
            return

        if self.__skip_this(level):
            return

        if type(level.t1) != type(level.t2):
            self.__diff_types(level)

        elif isinstance(level.t1, strings):
            self.__diff_str(level)

        elif isinstance(level.t1, numbers):
            self.__diff_numbers(level)

        elif isinstance(level.t1, Mapping):
            self.__diff_dict(level, parents_ids)

        elif isinstance(level.t1, tuple):
            self.__diff_tuple(level, parents_ids)

        elif isinstance(level.t1, (set, frozenset)):
            self.__diff_set(level)

        elif isinstance(level.t1, Iterable):
            if self.ignore_order:
                self.__diff_iterable_with_contenthash(level)
            else:
                self.__diff_iterable(level, parents_ids)

        else:
            self.__diff_obj(level, parents_ids)

        return

    @property
    def json(self):
        if not hasattr(self, '_json'):
            # copy of self removes all the extra attributes since it assumes
            # we have only a simple dictionary.
            copied = self.copy()
            self._json = jsonpickle.encode(copied)
        return self._json

    @json.deleter
    def json(self):
        del self._json

    @classmethod
    def from_json(self, value):
        return jsonpickle.decode(value)


if __name__ == "__main__":  # pragma: no cover
    if not py3:
        import sys
        sys.exit(
            "Please run with Python 3 to verify the doc strings: python3 -m deepdiff.diff"
        )
    import doctest
    doctest.testmod()
