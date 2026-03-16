from django.forms import BaseFormSet, BaseModelFormSet

from .utils import bind_subform


class SiteformFormSetMixin(BaseFormSet):
    """Custom formset to allow fields rendering subform to have multiple forms."""

    def render(self, *args, **kwargs):
        return f'{self.management_form}' + ('\n'.join(form.render() for form in self))

    def _construct_form(self, i, **kwargs):
        form = super()._construct_form(i, **kwargs)

        # Need to update subform linking for fields since
        # a formset doesn't do it.
        for field in form.fields.values():
            bind_subform(subform=form, field=field)

        return form


class ModelFormSet(SiteformFormSetMixin, BaseModelFormSet):
    """Formset for model forms."""
