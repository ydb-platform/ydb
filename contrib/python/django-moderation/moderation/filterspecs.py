from django.contrib.admin.filters import FieldListFilter
from django.contrib.contenttypes.models import ContentType
from django.utils.encoding import smart_text
from django.utils.translation import ugettext as _

from . import moderation


def _registered_content_types():
    "Return sorted content types for all registered models."
    content_types = []
    registered = list(moderation._registered_models.keys())
    registered.sort(key=lambda obj: obj.__name__)
    for model in registered:
        content_types.append(ContentType.objects.get_for_model(model))
    return content_types


class RegisteredContentTypeListFilter(FieldListFilter):

    def __init__(self, field, request, params,
                 model, model_admin, field_path):
        self.lookup_kwarg = '%s' % field_path
        self.lookup_val = request.GET.get(self.lookup_kwarg)
        self.content_types = _registered_content_types()
        super().__init__(
            field, request, params, model, model_admin, field_path)

    def expected_parameters(self):
        return [self.lookup_kwarg]

    def choices(self, cl):
        yield {
            'selected': self.lookup_val is None,
            'query_string': cl.get_query_string({}, [self.lookup_kwarg]),
            'display': _('All')}
        for ct_type in self.content_types:
            yield {
                'selected': smart_text(ct_type.id) == self.lookup_val,
                'query_string': cl.get_query_string({
                    self.lookup_kwarg: ct_type.id}),
                'display': str(ct_type),
            }
