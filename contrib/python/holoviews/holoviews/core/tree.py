
from . import util
from .pprint import PrettyPrinter


class AttrTree:
    """An AttrTree offers convenient, multi-level attribute access for
    collections of objects. AttrTree objects may also be combined
    together using the update method or merge classmethod. Here is an
    example of adding a ViewableElement to an AttrTree and accessing it:

    >>> t = AttrTree()
    >>> t.Example.Path = 1
    >>> t.Example.Path                             #doctest: +ELLIPSIS
    1

    """

    _disabled_prefixes = [] # Underscore attributes that should be
    _sanitizer = util.sanitize_identifier

    @classmethod
    def merge(cls, trees):
        """Merge a collection of AttrTree objects.

        """
        first = trees[0]
        for tree in trees:
            first.update(tree)
        return first


    def __dir__(self):
        """The _dir_mode may be set to 'default' or 'user' in which case
        only the child nodes added by the user are listed.

        """
        dict_keys = self.__dict__.keys()
        if self.__dict__['_dir_mode'] == 'user':
            return self.__dict__['children']
        else:
            return dir(type(self)) + list(dict_keys)

    def __init__(self, items=None, identifier=None, parent=None, dir_mode='default'):
        """
        Parameters
        ----------
        items
            Items as (path, value) pairs to construct
            (sub)tree down to given leaf values.
        identifier
            A string identifier for the current node (if any)
        parent
            The parent node (if any)

        Note that the root node does not have a parent and does not
        require an identifier.

        """
        self.__dict__['parent'] = parent
        self.__dict__['identifier'] = type(self)._sanitizer(identifier, escape=False)
        self.__dict__['children'] = []
        self.__dict__['_fixed'] = False
        self.__dict__['_dir_mode'] = dir_mode  # Either 'default' or 'user'

        fixed_error = 'No attribute %r in this AttrTree, and none can be added because fixed=True'
        self.__dict__['_fixed_error'] = fixed_error
        self.__dict__['data'] = {}
        items = items.items() if isinstance(items, dict) else items
        # Python 3
        items = list(items) if items else items
        items = [] if not items else items
        for path, item in items:
            self.set_path(path, item)

    @property
    def root(self):
        root = self
        while root.parent is not None:
            root = root.parent
        return root

    @property
    def path(self):
        """Returns the path up to the root for the current node.

        """
        if self.parent:
            return '.'.join([self.parent.path, str(self.identifier)])
        else:
            return self.identifier if self.identifier else self.__class__.__name__


    @property
    def fixed(self):
        """If fixed, no new paths can be created via attribute access

        """
        return self.__dict__['_fixed']

    @fixed.setter
    def fixed(self, val):
        self.__dict__['_fixed'] = val


    def update(self, other):
        """Updated the contents of the current AttrTree with the
        contents of a second AttrTree.

        """
        if not isinstance(other, AttrTree):
            raise Exception('Can only update with another AttrTree type.')
        fixed_status = (self.fixed, other.fixed)
        (self.fixed, other.fixed) = (False, False)
        for identifier, element in other.items():
            if identifier not in self.data:
                self[identifier] = element
            else:
                self[identifier].update(element)
        (self.fixed, other.fixed) = fixed_status


    def set_path(self, path, val):
        """Set the given value at the supplied path where path is either
        a tuple of strings or a string in A.B.C format.

        """
        path = tuple(path.split('.')) if isinstance(path , str) else tuple(path)

        disallowed = [p for p in path if not type(self)._sanitizer.allowable(p)]
        if any(disallowed):
            raise Exception("Attribute strings in path elements cannot be "
                            "correctly escaped : {}".format(','.join(repr(el) for el in disallowed)))
        if len(path) > 1:
            attrtree = self.__getattr__(path[0])
            attrtree.set_path(path[1:], val)
        else:
            self.__setattr__(path[0], val)


    def filter(self, path_filters):
        """Filters the loaded AttrTree using the supplied path_filters.

        """
        if not path_filters: return self

        # Convert string path filters
        path_filters = [tuple(pf.split('.')) if not isinstance(pf, tuple)
                        else pf for pf in path_filters]

        # Search for substring matches between paths and path filters
        new_attrtree = self.__class__()
        for path, item in self.data.items():
            if any([all([subpath in path for subpath in pf]) for pf in path_filters]):
                new_attrtree.set_path(path, item)

        return new_attrtree


    def _propagate(self, path, val):
        """Propagate the value up to the root node.

        """
        if val == '_DELETE':
            if path in self.data:
                del self.data[path]
            else:
                items = [(key, v) for key, v in self.data.items()
                         if not all(k==p for k, p in zip(key, path, strict=None))]
                self.data = dict(items)
        else:
            self.data[path] = val
        if self.parent is not None:
            self.parent._propagate((self.identifier, *path), val)


    def __setitem__(self, identifier, val):
        """Set a value at a child node with given identifier. If at a root
        node, multi-level path specifications is allowed (i.e. 'A.B.C'
        format or tuple format) in which case the behaviour matches
        that of set_path.

        """
        if isinstance(identifier, str) and '.' not in identifier:
            self.__setattr__(identifier, val)
        elif isinstance(identifier, str) and self.parent is None:
            self.set_path(tuple(identifier.split('.')), val)
        elif isinstance(identifier, tuple) and self.parent is None:
            self.set_path(identifier, val)
        else:
            raise Exception("Multi-level item setting only allowed from root node.")


    def __getitem__(self, identifier):
        """For a given non-root node, access a child element by identifier.

        If the node is a root node, you may also access elements using
        either tuple format or the 'A.B.C' string format.

        """
        split_label = (tuple(identifier.split('.'))
                       if isinstance(identifier, str) else tuple(identifier))
        if len(split_label) == 1:
            identifier = split_label[0]
            if identifier in self.children:
                return self.__dict__[identifier]
            else:
                raise KeyError(identifier)
        path_item = self
        for identifier in split_label:
            path_item = path_item[identifier]
        return path_item


    def __delitem__(self, identifier):
        split_label = (tuple(identifier.split('.'))
                       if isinstance(identifier, str) else tuple(identifier))
        if len(split_label) == 1:
            identifier = split_label[0]
            if identifier in self.children:
                del self.__dict__[identifier]
                self.children.pop(self.children.index(identifier))
            else:
                raise KeyError(identifier)
            self._propagate(split_label, '_DELETE')
        else:
            path_item = self
            for identifier in split_label[:-1]:
                path_item = path_item[identifier]
            del path_item[split_label[-1]]


    def __setattr__(self, identifier, val):
        # Getattr is skipped for root and first set of children
        shallow = (self.parent is None or self.parent.parent is None)

        if util.tree_attribute(identifier) and self.fixed and shallow:
            raise AttributeError(self._fixed_error % identifier)

        super().__setattr__(identifier, val)

        if util.tree_attribute(identifier):
            if identifier not in self.children:
                self.children.append(identifier)
            self._propagate((identifier,), val)


    def __getattr__(self, identifier):
        """Access a identifier from the AttrTree or generate a new AttrTree
        with the chosen attribute path.

        """
        try:
            return super().__getattr__(identifier)
        except AttributeError:
            pass

        # Attributes starting with __ get name mangled
        if identifier.startswith(('_' + type(self).__name__, '__')):
            raise AttributeError(f'Attribute {identifier} not found.')
        elif self.fixed==True:
            raise AttributeError(self._fixed_error % identifier)


        if not any(identifier.startswith(prefix)
                   for prefix in type(self)._disabled_prefixes):
            sanitized = type(self)._sanitizer(identifier, escape=False)
        else:
            sanitized = identifier

        if sanitized in self.children:
            return self.__dict__[sanitized]


        if not sanitized.startswith('_') and util.tree_attribute(identifier):
            self.children.append(sanitized)
            dir_mode = self.__dict__['_dir_mode']
            child_tree = self.__class__(identifier=sanitized,
                                        parent=self, dir_mode=dir_mode)
            self.__dict__[sanitized] = child_tree
            return child_tree
        else:
            raise AttributeError(f'{type(self).__name__!r} object has no attribute {identifier}.')


    def __iter__(self):
        return iter(self.data.values())


    def __contains__(self, name):
        return name in self.children or name in self.data


    def __len__(self):
        return len(self.data)


    def get(self, identifier, default=None):
        """Get a node of the AttrTree using its path string.

        Parameters
        ----------
        identifier
            Path string of the node to return
        default
            Value to return if no node is found

        Returns
        -------
        The indexed node of the AttrTree
        """
        split_label = (tuple(identifier.split('.'))
                       if isinstance(identifier, str) else tuple(identifier))
        if len(split_label) == 1:
            identifier = split_label[0]
            return self.__dict__.get(identifier, default)
        path_item = self
        for identifier in split_label:
            if path_item == default or path_item is None:
                return default
            path_item = path_item.get(identifier, default)
        return path_item

    def keys(self):
        """Keys of nodes in the AttrTree

        """
        return list(self.data.keys())


    def items(self):
        """Keys and nodes of the AttrTree

        """
        return list(self.data.items())


    def values(self):
        """Nodes of the AttrTree

        """
        return list(self.data.values())


    def pop(self, identifier, default=None):
        """Pop a node of the AttrTree using its path string.

        Parameters
        ----------
        identifier
            Path string of the node to return
        default
            Value to return if no node is found

        Returns
        -------
        The node that was removed from the AttrTree
        """
        if identifier in self.children:
            item = self[identifier]
            self.__delitem__(identifier)
            return item
        else:
            return default


    def __repr__(self):
        return PrettyPrinter.pprint(self)


__all__ = ['AttrTree']
