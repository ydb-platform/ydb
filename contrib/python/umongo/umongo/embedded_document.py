"""umongo EmbeddedDocument"""
import marshmallow as ma

from .template import Implementation, Template
from .data_objects import BaseDataObject
from .expose_missing import EXPOSE_MISSING
from .exceptions import AbstractDocumentError


__all__ = (
    'EmbeddedDocumentTemplate',
    'EmbeddedDocument',
    'EmbeddedDocumentOpts',
    'EmbeddedDocumentImplementation'
)


class EmbeddedDocumentTemplate(Template):
    """
    Base class to define a umongo embedded document.

    .. note::
        Once defined, this class must be registered inside a
        :class:`umongo.instance.BaseInstance` to obtain it corresponding
        :class:`umongo.embedded_document.EmbeddedDocumentImplementation`.
    """


EmbeddedDocument = EmbeddedDocumentTemplate
"Shortcut to EmbeddedDocumentTemplate"


class EmbeddedDocumentOpts:
    """
    Configuration for an :class:`umongo.embedded_document.EmbeddedDocument`.

    Should be passed as a Meta class to the :class:`EmbeddedDocument`

    .. code-block:: python

        @instance.register
        class MyEmbeddedDoc(EmbeddedDocument):
            class Meta:
                abstract = True

        assert MyEmbeddedDoc.opts.abstract == True


    ==================== ====================== ===========
    attribute            configurable in Meta   description
    ==================== ====================== ===========
    template             no                     Origin template of the embedded document
    instance             no                     Implementation's instance
    abstract             yes                    Embedded document can only be inherited
    is_child             no                     Embedded document inherit of a non-abstract
                                                embedded document
    strict               yes                    Don't accept unknown fields from mongo
                                                (default: True)
    offspring            no                     List of embedded documents inheriting this one
    ==================== ====================== ===========
    """
    def __repr__(self):
        return ('<{ClassName}('
                'instance={self.instance}, '
                'template={self.template}, '
                'abstract={self.abstract}, '
                'is_child={self.is_child}, '
                'strict={self.strict}, '
                'offspring={self.offspring})>'
                .format(ClassName=self.__class__.__name__, self=self))

    def __init__(self, instance, template, abstract=False,
                 is_child=False, strict=True, offspring=None):
        self.instance = instance
        self.template = template
        self.abstract = abstract
        self.is_child = is_child
        self.strict = strict
        self.offspring = set(offspring) if offspring else set()


class EmbeddedDocumentImplementation(Implementation, BaseDataObject):
    """
    Represent an embedded document once it has been implemented inside a
    :class:`umongo.instance.BaseInstance`.
    """

    __slots__ = ('_data', )
    opts = EmbeddedDocumentOpts(None, EmbeddedDocumentTemplate, abstract=True)

    def __init__(self, **kwargs):
        super().__init__()
        if self.opts.abstract:
            raise AbstractDocumentError("Cannot instantiate an abstract EmbeddedDocument")
        self._data = self.DataProxy(kwargs)

    def __repr__(self):
        return '<object EmbeddedDocument %s.%s(%s)>' % (
            self.__module__, self.__class__.__name__, dict(self._data.items()))

    def __eq__(self, other):
        if isinstance(other, dict):
            return self._data == other
        if hasattr(other, '_data'):
            return self._data == other._data
        return NotImplemented

    def is_modified(self):
        return self._data.is_modified()

    def clear_modified(self):
        """
        Reset the list of document's modified items.
        """
        self._data.clear_modified()

    def required_validate(self):
        self._data.required_validate()

    @classmethod
    def build_from_mongo(cls, data, use_cls=True):
        """
        Create an embedded document instance from MongoDB data

        :param data: data as retrieved from MongoDB
        :param use_cls: if the data contains a ``_cls`` field,
            use it determine the EmbeddedDocument class to instanciate
        """
        # If a _cls is specified, we have to use this document class
        if use_cls and '_cls' in data:
            cls = cls.opts.instance.retrieve_embedded_document(data['_cls'])
        doc = cls()
        doc.from_mongo(data)
        return doc

    def from_mongo(self, data):
        self._data.from_mongo(data)

    def to_mongo(self, update=False):
        return self._data.to_mongo(update=update)

    def update(self, data):
        """
        Update the embedded document with the given data.
        """
        return self._data.update(data)

    def dump(self):
        """
        Dump the embedded document.
        """
        return self._data.dump()

    def items(self):
        return self._data.items()

    # Data-proxy accessor shortcuts

    def __getitem__(self, name):
        value = self._data.get(name)
        return None if value is ma.missing and not EXPOSE_MISSING.get() else value

    def __delitem__(self, name):
        self._data.delete(name)

    def __setitem__(self, name, value):
        self._data.set(name, value)

    def __setattr__(self, name, value):
        if name in self._fields:
            self._data.set(name, value)
        else:
            super().__setattr__(name, value)

    def __getattr__(self, name):
        if name in self._fields:
            value = self._data.get(name)
            return None if value is ma.missing and not EXPOSE_MISSING.get() else value
        raise AttributeError(name)

    def __delattr__(self, name):
        if name in self._fields:
            self._data.delete(name)
        else:
            super().__delattr__(name)

    def __dir__(self):
        return dir(type(self)) + list(self._fields)
