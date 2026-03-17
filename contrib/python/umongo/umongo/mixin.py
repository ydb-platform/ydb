"""umongo MixinDocument"""
from .template import Implementation, Template

__all__ = (
    'MixinDocumentTemplate',
    'MixinDocument',
    'MixinDocumentImplementation'
)


class MixinDocumentTemplate(Template):
    """
    Base class to define a umongo mixin document.

    .. note::
        Once defined, this class must be registered inside a
        :class:`umongo.instance.BaseInstance` to obtain it corresponding
        :class:`umongo.mixin.MixinDocumentImplementation`.
    """


MixinDocument = MixinDocumentTemplate
"Shortcut to MixinDocumentTemplate"


class MixinDocumentOpts:
    """
    Configuration for an :class:`umongo.mixin.MixinDocument`.

    ==================== ====================== ===========
    attribute            configurable in Meta   description
    ==================== ====================== ===========
    template             no                     Origin template of the embedded document
    instance             no                     Implementation's instance
    ==================== ====================== ===========
    """
    def __repr__(self):
        return ('<{ClassName}('
                'instance={self.instance}, '
                'template={self.template}, '
                .format(ClassName=self.__class__.__name__, self=self))

    def __init__(self, instance, template):
        self.instance = instance
        self.template = template


class MixinDocumentImplementation(Implementation):
    """
    Represent a mixin document once it has been implemented inside a
    :class:`umongo.instance.BaseInstance`.
    """
    opts = MixinDocumentOpts(None, MixinDocumentTemplate)

    def __repr__(self):
        return '<object MixinDocument %s.%s>' % (self.__module__, self.__class__.__name__)
