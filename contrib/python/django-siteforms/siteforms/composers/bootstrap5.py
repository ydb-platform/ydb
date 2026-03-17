from typing import Tuple, Union

from django.forms import CheckboxInput, BoundField, Select, SelectMultiple

from .base import FormComposer, TypeAttrs, ALL_FIELDS, FORM, ALL_ROWS, SUBMIT, FIELDS_READONLY
from ..fields import SubformField
from ..utils import UNSET


class Bootstrap5(FormComposer):
    """Bootstrap 5 theming composer."""

    SIZE_SMALL = 'sm'
    SIZE_NORMAL = ''
    SIZE_LARGE = 'lg'

    opt_form_inline: bool = False
    """Make form inline."""

    opt_columns: Union[bool, Tuple[str, str]] = False
    """Enabled two-columns mode. 

    Expects a columns tuple: (label_columns_count, control_columns_count).

    If `True` default tuple ('col-2', 'col-10') is used.

    """

    opt_size: str = SIZE_NORMAL
    """Apply size to form elements."""

    opt_disabled_plaintext: bool = False
    """Render disabled fields as plain text."""

    opt_checkbox_switch: bool = False
    """Use switches for checkboxes."""

    opt_feedback_tooltips: bool = False
    """Whether to render feedback in tooltips."""

    opt_feedback_valid: bool = True
    """Whether to render feedback for valid fields."""

    opt_labels_floating : bool = False
    """Whether input labels should be floating."""

    opt_tag_help: str = 'div'

    _size_mod: Tuple[str, ...] = ('col-form-label', 'form-control', 'form-select', 'btn')

    @classmethod
    def _hook_init_subclass(cls):
        super()._hook_init_subclass()

        columns = cls.opt_columns

        if columns:

            if columns is True:
                columns = ('col-2', 'col-10')
                cls.opt_columns = columns

            cls.attrs_labels.update({
                ALL_FIELDS: {'class': 'col-form-label'},
            })

            cls.layout.update({
                CheckboxInput: '{label}{field}{feedback}{help}',
            })

            cls.wrappers.update({
                ALL_ROWS: '{fields}',
                ALL_FIELDS: '{field}',
                CheckboxInput: '{field}',
            })

        if cls.opt_form_inline:
            cls.attrs.update({
                FORM: {'class': 'row row-cols-auto align-items-end'},
            })

    def _apply_layout(self, *, fld: BoundField, field: str, label: str, hint: str, feedback: str) -> str:

        if isinstance(fld.field.widget, CheckboxInput):
            field = self._get_wrapper_checkbox(fld).replace('{field}', f'{field}{label}{feedback}{hint}')
            label = ''
            hint = ''
            feedback = ''

        opt_columns = self.opt_columns

        if opt_columns:
            col_label, col_control = opt_columns
            label = f'<div class="{col_label}">{label}</div>'
            field = f'<div class="{col_control}">{field}{feedback}{hint}</div>'
            hint = ''

        return super()._apply_layout(fld=fld, field=field, label=label, hint=hint, feedback=feedback)

    def _apply_wrapper(self, *, fld: BoundField, content) -> str:
        wrapped = super()._apply_wrapper(fld=fld, content=content)

        if self.opt_columns:
            wrapped = f'<div class="row mb-2">{wrapped}</div>'

        return wrapped

    def _get_attr_feedback(self, field: BoundField):
        return f"invalid-{'tooltip' if self.opt_feedback_tooltips else 'feedback'}"

    def _get_wrapper_fields(self, field: BoundField):

        css = ''
        if self.opt_labels_floating and not isinstance(field.field, SubformField):
            css = ' form-floating'

        return '<div class="mb-3%s">{field}</div>' % css

    def _get_wrapper_checkbox(self, field: BoundField):

        css = 'form-check'
        if self.opt_checkbox_switch:
            css = f'{css} form-switch'

        return '<div class="mb-3 %s">{field}</div>' % css

    def _get_layout_fields(self, field: BoundField):

        if self.opt_labels_floating and not isinstance(field.field, SubformField):
            return '{field}{label}{feedback}{help}'

        return '{label}{field}{feedback}{help}'

    def _render_field(self, field: BoundField, attrs: TypeAttrs = None) -> str:
        attrs = attrs or self._attrs_get_basic(self.attrs, field)

        css = attrs.get('class', '')

        if self.form.is_submitted:
            css = f"{css} {'is-invalid' if field.errors else ('is-valid' if self.opt_feedback_valid else '')}"

        if self.opt_disabled_plaintext:
            is_disabled = field.name in self.form.disabled_fields
            if is_disabled:
                css = ' form-control-plaintext'

        attrs['class'] = css

        return super()._render_field(field, attrs)

    def render(self, *, render_form_tag: bool = UNSET) -> str:
        out = super().render(render_form_tag=render_form_tag)

        size = self.opt_size

        if size:
            # Apply sizing.
            mod = f'-{size}'

            # Workaround form-control- prefix clashes.
            clashing = {}
            for idx, term in enumerate(('form-control-plaintext', 'btn-')):
                tmp_key = f'tmp_{idx}'
                clashing[tmp_key] = term
                out = out.replace(term, tmp_key)

            for val in self._size_mod:
                out = out.replace(val, f'{val} {val}{mod}')

            # Roll off the workaround.
            for tmp_key, term in clashing.items():
                out = out.replace(tmp_key, term)

        return out

    attrs: TypeAttrs = {
        SUBMIT: {'class': 'btn btn-primary mt-3'},
        ALL_FIELDS: {'class': 'form-control'},
        ALL_ROWS: {'class': 'row mx-1'},
        # ALL_GROUPS: {'class': ''},  # todo maybe
        FIELDS_READONLY: {'class': 'bg-light form-control border border-light'},
        CheckboxInput: {'class': 'form-check-input'},
        Select: {'class': 'form-select'},
        SelectMultiple: {'class': 'form-select'},
    }

    attrs_help: TypeAttrs = {
        ALL_FIELDS: {'class': 'form-text'},
    }

    attrs_feedback: TypeAttrs = {
        FORM: {'class': 'alert alert-danger mb-4', 'role': 'alert'},
        ALL_FIELDS: {'class': _get_attr_feedback},
    }

    attrs_labels: TypeAttrs = {
        ALL_FIELDS: {'class': 'form-label'},
        CheckboxInput: {'class': 'form-check-label'},
    }

    wrappers: TypeAttrs = {
        ALL_FIELDS: _get_wrapper_fields,
        SUBMIT: '<div class="mb-3">{submit}</div>',
        # FIELDS_STACKED: '<div class="col">{field}</div>',  # todo maybe
    }

    layout: TypeAttrs = {
        ALL_FIELDS: _get_layout_fields,
    }
