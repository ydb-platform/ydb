import abc

from .exceptions import (
    NotRegisteredDocumentError, AlreadyRegisteredDocumentError, NoDBDefinedError)
from .document import DocumentTemplate
from .embedded_document import EmbeddedDocumentTemplate
from .template import get_template


class Instance(abc.ABC):
    """
    Abstract instance class

    Instances aims at collecting and implementing :class:`umongo.template.Template`::

        # Doc is a template, cannot use it for the moment
        class Doc(DocumentTemplate):
            pass

        instance = MyFrameworkInstance()
        # doc_cls is the instance's implementation of Doc
        doc_cls = instance.register(Doc)
        # Implementations are registered as attribute into the instance
        instance.Doc is doc_cls
        # Now we can work with the implementations
        doc_cls.find()

    .. note::
        Instance registration is divided between :class:`umongo.Document` and
        :class:`umongo.EmbeddedDocument`.
    """
    BUILDER_CLS = None

    def __init__(self, db=None):
        self.builder = self.BUILDER_CLS(self)
        self._doc_lookup = {}
        self._embedded_lookup = {}
        self._mixin_lookup = {}
        self._db = db
        if db is not None:
            self.set_db(db)

    @classmethod
    def from_db(cls, db):
        from .frameworks import find_instance_from_db
        instance_cls = find_instance_from_db(db)
        instance = instance_cls()
        instance.set_db(db)
        return instance

    def retrieve_document(self, name_or_template):
        """
        Retrieve a :class:`umongo.document.DocumentImplementation` registered into this
        instance from it name or it template class (i.e. :class:`umongo.Document`).
        """
        if not isinstance(name_or_template, str):
            name_or_template = name_or_template.__name__
        if name_or_template not in self._doc_lookup:
            raise NotRegisteredDocumentError(
                'Unknown document class "%s"' % name_or_template)
        return self._doc_lookup[name_or_template]

    def retrieve_embedded_document(self, name_or_template):
        """
        Retrieve a :class:`umongo.embedded_document.EmbeddedDocumentImplementation`
        registered into this instance from it name or it template class
        (i.e. :class:`umongo.EmbeddedDocument`).
        """
        if not isinstance(name_or_template, str):
            name_or_template = name_or_template.__name__
        if name_or_template not in self._embedded_lookup:
            raise NotRegisteredDocumentError(
                'Unknown embedded document class "%s"' % name_or_template)
        return self._embedded_lookup[name_or_template]

    def register(self, template):
        """
        Generate an :class:`umongo.template.Implementation` from the given
        :class:`umongo.template.Template` for this instance.

        :param template: :class:`umongo.template.Template` to implement
        :return: The :class:`umongo.template.Implementation` generated

        .. note::
            This method can be used as a decorator. This is useful when you
            only have a single instance to work with to directly use the
            class you defined::

                @instance.register
                class MyEmbedded(EmbeddedDocument):
                    pass

                @instance.register
                class MyDoc(Document):
                    emb = fields.EmbeddedField(MyEmbedded)

                MyDoc.find()

        """
        # Retrieve the template if another implementation has been provided instead
        template = get_template(template)
        if issubclass(template, DocumentTemplate):
            implementation = self._register_doc(template)
        elif issubclass(template, EmbeddedDocumentTemplate):
            implementation = self._register_embedded_doc(template)
        else:  # MixinDocument
            implementation = self._register_mixin_doc(template)
        return implementation

    def _register_doc(self, template):
        implementation = self.builder.build_from_template(template)
        if implementation.__name__ in self._doc_lookup:
            raise AlreadyRegisteredDocumentError(
                'Document `%s` already registered' % implementation.__name__)
        self._doc_lookup[implementation.__name__] = implementation
        return implementation

    def _register_embedded_doc(self, template):
        implementation = self.builder.build_from_template(template)
        if implementation.__name__ in self._embedded_lookup:
            raise AlreadyRegisteredDocumentError(
                'EmbeddedDocument `%s` already registered' % implementation.__name__)
        self._embedded_lookup[implementation.__name__] = implementation
        return implementation

    def _register_mixin_doc(self, template):
        implementation = self.builder.build_from_template(template)
        if implementation.__name__ in self._mixin_lookup:
            raise AlreadyRegisteredDocumentError(
                'MixinDocument `%s` already registered' % implementation.__name__)
        self._mixin_lookup[implementation.__name__] = implementation
        return implementation

    @property
    def db(self):
        if self._db is None:
            raise NoDBDefinedError('db not set, please call set_db')
        return self._db

    @abc.abstractmethod
    def is_compatible_with(self, db):
        return NotImplemented

    def set_db(self, db):
        """
        Set the database to use whithin this instance.

        .. note::
            The documents registered in the instance cannot be used
            before this function is called.
        """
        assert self.is_compatible_with(db)
        self._db = db
