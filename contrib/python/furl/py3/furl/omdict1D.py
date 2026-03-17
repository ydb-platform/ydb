# -*- coding: utf-8 -*-

#
# furl - URL manipulation made simple.
#
# Ansgar Grunseid
# grunseid.com
# grunseid@gmail.com
#
# License: Build Amazing Things (Unlicense)
#

from orderedmultidict import omdict

from .common import is_iterable_but_not_string, absent as _absent


class omdict1D(omdict):

    """
    One dimensional ordered multivalue dictionary. Whenever a list of
    values is passed to set(), __setitem__(), add(), update(), or
    updateall(), it's treated as multiple values and the appropriate
    'list' method is called on that list, like setlist() or
    addlist(). For example:

      omd = omdict1D()

      omd[1] = [1,2,3]
      omd[1] != [1,2,3] # True.
      omd[1] == 1 # True.
      omd.getlist(1) == [1,2,3] # True.

      omd.add(2, [2,3,4])
      omd[2] != [2,3,4] # True.
      omd[2] == 2 # True.
      omd.getlist(2) == [2,3,4] # True.

      omd.update([(3, [3,4,5])])
      omd[3] != [3,4,5] # True.
      omd[3] == 3 # True.
      omd.getlist(3) == [3,4,5] # True.

      omd = omdict([(1,None),(2,None)])
      omd.updateall([(1,[1,11]), (2,[2,22])])
      omd.allitems == [(1,1), (1,11), (2,2), (2,22)]
    """

    def add(self, key, value):
        if not is_iterable_but_not_string(value):
            value = [value]

        if value:
            self._map.setdefault(key, list())

        for val in value:
            node = self._items.append(key, val)
            self._map[key].append(node)

        return self

    def set(self, key, value):
        return self._set(key, value)

    def __setitem__(self, key, value):
        return self._set(key, value)

    def _bin_update_items(self, items, replace_at_most_one,
                          replacements, leftovers):
        """
        Subclassed from omdict._bin_update_items() to make update() and
        updateall() process lists of values as multiple values.

        <replacements> and <leftovers> are modified directly, ala pass by
        reference.
        """
        for key, values in items:
            # <values> is not a list or an empty list.
            like_list_not_str = is_iterable_but_not_string(values)
            if not like_list_not_str or (like_list_not_str and not values):
                values = [values]

            for value in values:
                # If the value is [], remove any existing leftovers with
                # key <key> and set the list of values itself to [],
                # which in turn will later delete <key> when [] is
                # passed to omdict.setlist() in
                # omdict._update_updateall().
                if value == []:
                    replacements[key] = []
                    leftovers[:] = [lst for lst in leftovers if key != lst[0]]
                # If there are existing items with key <key> that have
                # yet to be marked for replacement, mark that item's
                # value to be replaced by <value> by appending it to
                # <replacements>.
                elif (key in self and
                      replacements.get(key, _absent) in [[], _absent]):
                    replacements[key] = [value]
                elif (key in self and not replace_at_most_one and
                      len(replacements[key]) < len(self.values(key))):
                    replacements[key].append(value)
                elif replace_at_most_one:
                    replacements[key] = [value]
                else:
                    leftovers.append((key, value))

    def _set(self, key, value):
        if not is_iterable_but_not_string(value):
            value = [value]
        self.setlist(key, value)

        return self
