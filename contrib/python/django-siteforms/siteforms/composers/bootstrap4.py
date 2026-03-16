from typing import Optional, Tuple, Union

from django.forms import FileInput, ClearableFileInput, CheckboxInput, BoundField, Select, SelectMultiple

from .base import (
    FormComposer, TypeAttrs, ALL_FIELDS, FORM, ALL_GROUPS, ALL_ROWS, SUBMIT, FIELDS_STACKED, FIELDS_READONLY,
) # noqa
from ..utils import UNSET


class Bootstrap4(FormComposer):
    """Bootstrap 4 theming composer."""

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

    opt_custom_controls: bool = False
    """Use custom controls from Bootstrap 4."""

    opt_checkbox_switch: bool = False
    """Use switches for checkboxes (if custom controls)."""

    opt_size: str = SIZE_NORMAL
    """Apply size to form elements."""

    opt_disabled_plaintext: bool = False
    """Render disabled fields as plain text."""

    opt_feedback_tooltips: bool = False
    """Whether to render feedback in tooltips."""

    opt_feedback_valid: bool = True
    """Whether to render feedback for valid fields."""

    _size_mod: Tuple[str, ...] = ('col-form-label', 'form-control', 'input-group')
    _file_cls = {'class': 'form-control-file'}

    @classmethod
    def _hook_init_subclass(cls):
        super()._hook_init_subclass()

        if cls.opt_custom_controls:

            cls._size_mod = tuple(list(cls._size_mod) + [
                'custom-select',
            ])

            cls._file_cls = {'class': 'custom-file-input'}

            _file_label = {'class': 'custom-file-label'}
            _file_wrapper = '<div class="custom-file mx-1">{field}</div>'
            _select_cls = {'class': 'custom-select'}

            cls.attrs.update({
                FileInput: cls._file_cls,
                ClearableFileInput: cls._file_cls,
                Select: _select_cls,
                SelectMultiple: _select_cls,
                CheckboxInput: {'class': 'custom-control-input'},
            })

            cls.attrs_labels.update({
                FileInput: _file_label,
                ClearableFileInput: _file_label,
                CheckboxInput: {'class': 'custom-control-label'},
            })

            cls.wrappers.update({
                FileInput: _file_wrapper,
                ClearableFileInput: _file_wrapper,
            })

        columns = cls.opt_columns

        if columns:

            if columns is True:
                columns = ('col-2', 'col-10')
                cls.opt_columns = columns

            cls.attrs_labels.update({
                ALL_FIELDS: {'class': 'col-form-label'},
            })

            cls.layout.pop(CheckboxInput, None)

            cls.wrappers.update({
                ALL_ROWS: '{fields}',
                ALL_FIELDS: '{field}',
            })

    def _get_attr_form(self) -> Optional[str]:
        # todo maybe needs-validation
        if self.opt_form_inline:
            return 'form-inline'
        return None

    def _get_attr_check_input(self, field: BoundField):
        css = 'form-check-input'
        if not self.opt_render_labels:
            css += ' position-static'
        return css

    def _apply_layout(self, *, fld: BoundField, field: str, label: str, hint: str, feedback: str) -> str:

        opt_columns = self.opt_columns
        opt_custom = self.opt_custom_controls
        widget = fld.field.widget

        is_cb = isinstance(widget, CheckboxInput)
        is_file = isinstance(widget, (FileInput, ClearableFileInput))

        if is_cb:

            if opt_custom:
                variant = 'custom-switch' if self.opt_checkbox_switch else 'custom-checkbox'
                css = f'custom-control mx-1 {variant}'  # todo +custom-control-inline

            else:
                css = 'form-check'  # todo +form-check-inline

            field = f'<div class="{css}">{field}{label}{feedback}{hint}</div>'
            label = ''
            hint = ''
            feedback = ''

        if opt_columns and not (opt_custom and is_file):
            col_label, col_control = opt_columns
            label = f'<div class="{col_label}">{label}</div>'
            field = f'<div class="{col_control}">{field}{feedback}{hint}</div>'
            hint = ''

        return super()._apply_layout(fld=fld, field=field, label=label, hint=hint, feedback=feedback)

    def _apply_wrapper(self, *, fld: BoundField, content) -> str:
        wrapped = super()._apply_wrapper(fld=fld, content=content)

        if self.opt_columns:
            wrapped = f'<div class="form-group row">{wrapped}</div>'

        return wrapped

    def _get_attr_feedback(self, field: BoundField):
        return f"invalid-{'tooltip' if self.opt_feedback_tooltips else 'feedback'}"

    def _render_feedback(self, field: BoundField) -> str:
        out = super()._render_feedback(field)

        if field.errors:
            # prepend hidden input to workaround feedback not shown for subforms
            out = f'<input type="hidden" class="form-control is-invalid">{out}'

        return out

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
            for idx, term in enumerate(('form-control-file', 'form-control-plaintext')):
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
        FORM: {'class': _get_attr_form},
        SUBMIT: {'class': 'btn btn-primary mt-3'},  # todo control-group?
        ALL_FIELDS: {'class': 'form-control'},
        ALL_ROWS: {'class': 'form-row mx-0'},
        ALL_GROUPS: {'class': 'form-group'},
        FIELDS_READONLY: {'class': 'bg-light form-control border border-light'},
        FileInput: _file_cls,
        ClearableFileInput: _file_cls,
        CheckboxInput: {'class': _get_attr_check_input},
    }

    attrs_help: TypeAttrs = {
        ALL_FIELDS: {'class': 'form-text text-muted'},
    }

    attrs_feedback: TypeAttrs = {
        FORM: {'class': 'alert alert-danger mb-4', 'role': 'alert'},
        ALL_FIELDS: {'class': _get_attr_feedback},
    }

    attrs_labels: TypeAttrs = {
        CheckboxInput: {'class': 'form-check-label'},
    }

    wrappers: TypeAttrs = {
        ALL_FIELDS: '<div class="form-group col">{field}</div>',
        FIELDS_STACKED: '<div class="form-group col">{field}</div>',
    }
