import itertools

from pytool.lang import UNSET

import humbledb
from humbledb import _version
from humbledb.document import Document
from humbledb.errors import NoConnection


class Page(Document):
    """Document class used by :class:`Array`."""

    size = "s"  # Number of entries in this page
    """ Number of entries currently in this page. """
    entries = "e"  # Array of entries
    """ Array of entries. """
    _opts = {"safe": True} if _version._lt("3.0.0") else {}


class ArrayMeta(type):
    """
    Metaclass for Arrays. This ensures that we have all the needed
    configuration options, as well as creating the :class:`Page` subclass that
    is specific to each Array subclass.

    """

    def __new__(mcs, name, bases, cls_dict):
        # Skip the Array base class
        if (
            name == "Array"
            and not len(bases)
            and mcs is ArrayMeta
            and cls_dict["__qualname__"] == "Array"
        ):
            return type.__new__(mcs, name, bases, cls_dict)
        # The dictionary for subclassing the Page document
        page_dict = {}
        # Check for required class members
        for member in "config_database", "config_collection":
            if member not in cls_dict:
                raise TypeError("{!r} missing required {!r}".format(name, member))
            # Move the config to the page
            page_dict[member] = cls_dict.pop(member)
        # Create our page subclass and assign to cls._page
        cls_dict["_page"] = type(name + "Page", (Page,), page_dict)
        # Return our new Array
        return type.__new__(mcs, name, bases, cls_dict)

    # Shortcut methods
    @property
    def size(cls):
        return cls._page.size

    @property
    def entries(cls):
        return cls._page.entries

    @property
    def find(cls):
        return cls._page.find

    @property
    def update(cls):
        return cls._page.update

    @property
    def remove(cls):  # This needs a try/except for tests
        try:
            return cls._page.remove
        except NoConnection:
            pass  # Collection not available yet


class Array(metaclass=ArrayMeta):
    """
    HumbleDB Array object. This helps manage paginated array documents in
    MongoDB. This class is designed to be inherited from, and not instantiated
    directly.

    If you know the `page_count` for this array ahead of time, passing it in
    to the constructor will save an extra query on the first append for a given
    instance.

    :param str _id: Sets the array's shared id
    :param int page_count: Total number of pages that already exist (optional)

    """

    config_max_size = 100
    """ Soft limit on the maximum number of entries per page. """

    config_page_marker = "#"
    """ Combined with the array_id and page number to create the page _id. """

    config_padding = 0
    """ Number of bytes to pad new page creation with. """

    def __init__(self, _id, page_count=UNSET):
        self._array_id = _id
        self.page_count = page_count

    def page_id(self, page_number=None):
        """
        Return the document ID for `page_number`. If page number is not
        specified the :attr:`Array.page_count` is used.

        :param int page_number: A page number (optional)

        """
        page_number = page_number or self.page_count or 0
        return "{}{:05d}".format(self._id, page_number)

    @property
    def _id(self):
        return "{}{}".format(self._array_id, self.config_page_marker)

    @property
    def _id_regex(self):
        _id = self._id.replace(".", "\.")
        return {"$regex": "^" + _id}

    def new_page(self, page_number):
        """
        Creates a new page document.

        :param int page_number: The page number to create

        """
        # Shortcut the page class
        Page = self._page
        # Create a new page instance
        page = Page()
        page._id = self.page_id(page_number)
        page.size = 0
        page.entries = []
        page["padding"] = "0" * self.config_padding
        # Insert the new page
        try:
            # We need to do this as safe, because otherwise it may not be
            # available to a subsequent call to append
            Page.insert(page, **Page._opts)
        except humbledb.errors.DuplicateKeyError:
            # A race condition already created this page, so we are done
            return
        # Remove the padding
        Page.update({"_id": page._id}, {"$unset": {"padding": 1}}, **Page._opts)

    def append(self, entry):
        """
        Append an entry to this array and return the page count.

        :param dict entry: New entry
        :returns: Total number of pages

        """
        # If we haven't set a page count, we query for it. This is generally a
        # very fast query.
        if self.page_count is UNSET:
            self.page_count = self.pages()
        # See if we have to create our initial page
        if self.page_count < 1:
            self.page_count = 1
            self.new_page(self.page_count)
        # Shortcut page class
        Page = self._page
        query = {"_id": self.page_id()}
        modify = {"$inc": {Page.size: 1}, "$push": {Page.entries: entry}}
        fields = {Page.size: 1}
        # Append our entry to our page and get the page's size
        page = Page.find_and_modify(query, modify, new=True, fields=fields)
        if not page:
            raise RuntimeError("Append failed: page does not exist.")
        # If we need to, we create the next page
        if page.size >= self.config_max_size:
            self.page_count += 1
            self.new_page(self.page_count)
        # Return the page count
        return self.page_count

    def remove(self, spec):
        """
        Remove first element matching `spec` from each page in this array.

        Due to how this is handled, all ``null`` values will be removed from
        the array.

        :param dict spec: Dictionary matching items to be removed
        :returns: ``True`` if an element was removed

        """
        Page = self._page
        # Since we can't reliably use dot-notation when the query is against an
        # embedded document, we need to use the $elemMatch operator instead
        if isinstance(spec, dict):
            query_spec = {"$elemMatch": spec}
        else:
            query_spec = spec
        # Update to set first instance matching ``spec`` on each page to
        # ``null`` (via $unset)
        query = {"_id": self._id_regex, Page.entries: query_spec}
        modify = {"$unset": {Page.entries + ".$": spec}, "$inc": {Page.size: -1}}
        result = Page.update(query, modify, multi=True)
        if not result or not result.get("updatedExisting", None):
            return
        # Update to remove all ``null`` entries from this array
        query = {"_id": self._id_regex, Page.entries: None}
        result = Page.update(query, {"$pull": {Page.entries: None}}, multi=True)
        # Check the result and return True if anything was modified
        if result and result.get("updatedExisting", None):
            return True

    def _all(self):
        """Return a cursor for iterating over all the pages."""
        Page = self._page
        return Page.find({"_id": self._id_regex}).sort("_id")

    def all(self):
        """Return all entries in this array."""
        cursor = self._all()
        return list(itertools.chain.from_iterable(p.entries for p in cursor))

    def clear(self):
        """Remove all documents in this array."""
        self._page.remove({self._page._id: self._id_regex})
        self.page_count = 0

    def length(self):
        """Return the total number of items in this array."""
        # This is implemented rather than __len__ because it incurs a query,
        # and we don't want to query transparently
        Page = self._page
        if _version._lt("3.0.0"):
            cursor = Page.find({"_id": self._id_regex}, fields={Page.size: 1, "_id": 0})
        else:
            cursor = Page.find({"_id": self._id_regex}, {Page.size: 1, "_id": 0})
        return sum(p.size for p in cursor)

    def pages(self):
        """Return the total number of pages in this array."""
        Page = self._page
        return Page.find({"_id": self._id_regex}).count()

    def __getitem__(self, index):
        """
        Return a page or pages for the given index or slice respectively.

        :param index: Integer index or ``slice()`` object

        """
        if not isinstance(index, (int, slice)):
            raise TypeError("Array indices must be integers, not %s" % type(index))
        Page = self._page  # Shorthand the Page class
        # If we have an integer index, it's a simple query for the page number
        if isinstance(index, int):
            if index < 0:
                raise IndexError("Array indices must be positive")
            # Page numbers are not zero indexed
            index += 1
            page = Page.find_one({"_id": self.page_id(index)})
            if not page:
                raise IndexError("Array index out of range")
            return page.entries
        # If we have a slice, we attempt to get the pages for [start, stop)
        if isinstance(index, slice):
            if index.step:
                raise TypeError("Arrays do not allow extended slices")
            if index.start and index.start < 0:
                raise IndexError("Array indices must be positive")
            if index.stop and index.stop < 0:
                raise IndexError("Array indices must be positive")
            # Page numbers are not zero indexed
            start = (index.start or 0) + 1
            stop = (index.stop or 2**32) + 1
            start = "{}{:05d}".format(self._id, start)
            stop = "{}{:05d}".format(self._id, stop)
            cursor = Page.find({"_id": {"$gte": start, "$lt": stop}})
            return list(itertools.chain.from_iterable(p.entries for p in cursor))
        # This comment will never be reached
