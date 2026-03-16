"""Category-related objects.

The |category.Categories| object is returned by ``Plot.categories`` and contains zero or
more |category.Category| objects, each representing one of the category labels
associated with the plot. Categories can be hierarchical, so there are members allowing
discovery of the depth of that hierarchy and providing means to navigate it.
"""

from __future__ import annotations

from collections.abc import Sequence


class Categories(Sequence):
    """
    A sequence of |category.Category| objects, each representing a category
    label on the chart. Provides properties for dealing with hierarchical
    categories.
    """

    def __init__(self, xChart):
        super(Categories, self).__init__()
        self._xChart = xChart

    def __getitem__(self, idx):
        pt = self._xChart.cat_pts[idx]
        return Category(pt, idx)

    def __iter__(self):
        cat_pts = self._xChart.cat_pts
        for idx, pt in enumerate(cat_pts):
            yield Category(pt, idx)

    def __len__(self):
        # a category can be "null", meaning the Excel cell for it is empty.
        # In this case, there is no c:pt element for it. The "empty" category
        # will, however, be accounted for in c:cat//c:ptCount/@val, which
        # reflects the true length of the categories collection.
        return self._xChart.cat_pt_count

    @property
    def depth(self):
        """
        Return an integer representing the number of hierarchical levels in
        this category collection. Returns 1 for non-hierarchical categories
        and 0 if no categories are present (generally meaning no series are
        present).
        """
        cat = self._xChart.cat
        if cat is None:
            return 0
        if cat.multiLvlStrRef is None:
            return 1
        return len(cat.lvls)

    @property
    def flattened_labels(self):
        """
        Return a sequence of tuples, each containing the flattened hierarchy
        of category labels for a leaf category. Each tuple is in parent ->
        child order, e.g. ``('US', 'CA', 'San Francisco')``, with the leaf
        category appearing last. If this categories collection is
        non-hierarchical, each tuple will contain only a leaf category label.
        If the plot has no series (and therefore no categories), an empty
        tuple is returned.
        """
        cat = self._xChart.cat
        if cat is None:
            return ()

        if cat.multiLvlStrRef is None:
            return tuple([(category.label,) for category in self])

        return tuple(
            [
                tuple([category.label for category in reversed(flat_cat)])
                for flat_cat in self._iter_flattened_categories()
            ]
        )

    @property
    def levels(self):
        """
        Return a sequence of |CategoryLevel| objects representing the
        hierarchy of this category collection. The sequence is empty when the
        category collection is not hierarchical, that is, contains only
        leaf-level categories. The levels are ordered from the leaf level to
        the root level; so the first level will contain the same categories
        as this category collection.
        """
        cat = self._xChart.cat
        if cat is None:
            return []
        return [CategoryLevel(lvl) for lvl in cat.lvls]

    def _iter_flattened_categories(self):
        """
        Generate a ``tuple`` object for each leaf category in this
        collection, containing the leaf category followed by its "parent"
        categories, e.g. ``('San Francisco', 'CA', 'USA'). Each tuple will be
        the same length as the number of levels (excepting certain edge
        cases which I believe always indicate a chart construction error).
        """
        levels = self.levels
        if not levels:
            return
        leaf_level, remaining_levels = levels[0], levels[1:]
        for category in leaf_level:
            yield self._parentage((category,), remaining_levels)

    def _parentage(self, categories, levels):
        """
        Return a tuple formed by recursively concatenating *categories* with
        its next ancestor from *levels*. The idx value of the first category
        in *categories* determines parentage in all levels. The returned
        sequence is in child -> parent order. A parent category is the
        Category object in a next level having the maximum idx value not
        exceeding that of the leaf category.
        """
        # exhausting levels is the expected recursion termination condition
        if not levels:
            return tuple(categories)

        # guard against edge case where next level is present but empty. That
        # situation is not prohibited for some reason.
        if not levels[0]:
            return tuple(categories)

        parent_level, remaining_levels = levels[0], levels[1:]
        leaf_node = categories[0]

        # Make the first parent the default. A possible edge case is where no
        # parent is defined for one or more leading values, e.g. idx > 0 for
        # the first parent.
        parent = parent_level[0]
        for category in parent_level:
            if category.idx > leaf_node.idx:
                break
            parent = category

        extended_categories = tuple(categories) + (parent,)
        return self._parentage(extended_categories, remaining_levels)


class Category(str):
    """
    An extension of `str` that provides the category label as its string
    value, and additional attributes representing other aspects of the
    category.
    """

    def __new__(cls, pt, *args):
        category_label = "" if pt is None else pt.v.text
        return str.__new__(cls, category_label)

    def __init__(self, pt, idx=None):
        """
        *idx* is a required attribute of a c:pt element, but must be
        specified when pt is None, as when a "placeholder" category is
        created to represent a missing c:pt element.
        """
        self._element = self._pt = pt
        self._idx = idx

    @property
    def idx(self):
        """
        Return an integer representing the index reference of this category.
        For a leaf node, the index identifies the category. For a parent (or
        other ancestor) category, the index specifies the first leaf category
        that ancestor encloses.
        """
        if self._pt is None:
            return self._idx
        return self._pt.idx

    @property
    def label(self):
        """
        Return the label of this category as a string.
        """
        return str(self)


class CategoryLevel(Sequence):
    """
    A sequence of |category.Category| objects representing a single level in
    a hierarchical category collection. This object is only used when the
    categories are hierarchical, meaning they have more than one level and
    higher level categories group those at lower levels.
    """

    def __init__(self, lvl):
        self._element = self._lvl = lvl

    def __getitem__(self, offset):
        return Category(self._lvl.pt_lst[offset])

    def __len__(self):
        return len(self._lvl.pt_lst)
