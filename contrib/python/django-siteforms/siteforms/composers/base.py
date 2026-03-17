from collections import defaultdict
from functools import partial
from typing import Dict, Any, Optional, Union, List, Type, TypeVar

from django.forms import BoundField, CheckboxInput, Form
from django.forms.utils import flatatt
from django.forms.widgets import Input
from django.middleware.csrf import get_token
from django.utils.translation import gettext_lazy as _

from ..utils import merge_dict, UNSET
from ..widgets import ReadOnlyWidget

if False:  # pragma: nocover
    from ..base import SiteformsMixin  # noqa

TypeAttrs = Dict[Union[str, Type[Input]], Any]


ALL_FIELDS = '__fields__'
"""Denotes every field."""

ALL_GROUPS = '__groups__'
"""Denotes every group."""

ALL_ROWS = '__rows__'
"""Denotes every row."""

FIELDS_STACKED = '__stacked__'
"""Denotes stacked fields when layout is applied.
E.g. in 'group': {'a', ['b', 'c']} b and c are stacked.

"""

FIELDS_READONLY = '__readonly__'
"""Denotes fields considered read only (using ReadOnlyWidget)."""

FORM = '__form__'
"""Denotes a form."""

SUBMIT = '__submit__'
"""Submit button."""

_VALUE = '__value__'

TypeComposer = TypeVar('TypeComposer', bound='FormComposer')


class FormatDict(dict):

    def __missing__(self, key: str) -> str:  # pragma: nocover
        return ''


class FormComposer:
    """Base form composer."""
    
    opt_render_form_tag: bool = True
    """Render form tag."""

    opt_label_colon: bool = True
    """Whether to render colons after label's texts."""

    opt_render_labels: bool = True
    """Render label elements."""

    opt_render_help: bool = True
    """Render hints (help texts)."""

    opt_title_label: bool = False
    """Render label as title for form field."""

    opt_title_help: bool = False
    """Render help as title for form field."""

    opt_placeholder_label: bool = False
    """Put title (verbose name) into field's placeholders."""

    opt_placeholder_help: bool = False
    """Put hint (help text) into field's placeholders."""

    opt_tag_help: str = 'small'
    """Tag to be used for hints."""

    opt_tag_feedback: str = 'div'
    """Tag to be used for feedback."""

    opt_tag_feedback_line: str = 'div'
    """Tag to be used for feedback."""

    opt_submit: str = _('Submit')
    """Submit button text."""

    opt_submit_name: str = '__submit'
    """Submit button name."""

    ########################################################

    attrs_labels: TypeAttrs = None
    """Attributes to apply to labels."""

    attrs_help: TypeAttrs = None
    """Attributes to apply to hints."""

    attrs_feedback: TypeAttrs = None
    """Attributes to apply to feedback (validation notes).
    
    FORM macros here denotes global (non-field) form feedback attrs.
    
    """

    groups: Dict[str, str] = None
    """Map alias to group titles."""

    wrappers: TypeAttrs = {
        ALL_FIELDS: '<span>{field}</span>',
        ALL_ROWS: '<div {attrs}>{fields}</div>',
        ALL_GROUPS: '<fieldset {attrs}><legend>{title}</legend>{rows}</fieldset>',
        SUBMIT: '{submit}',
        FIELDS_STACKED: '<div>{field}</div>',
    }
    """Wrappers for fields, groups, rows, submit button."""

    layout: TypeAttrs = {
        FORM: ALL_FIELDS,
        ALL_FIELDS: '{label}{field}{feedback}{help}',
        CheckboxInput: '{field}{label}{feedback}{help}',
    }
    """Layout instructions for fields and form."""

    ########################################################

    def __init__(self, form: Union['SiteformsMixin', Form]):
        self.form = form
        self.groups = self.groups or {}
        self.attrs_feedback = self.attrs_feedback or {}

    def __init_subclass__(cls) -> None:
        # Implements attributes enrichment - inherits attrs values from parents.
        super().__init_subclass__()

        def enrich_attr(attr: str):
            attrs_dict = {}

            for base in cls.__bases__:
                if issubclass(base, FormComposer):
                    attrs_dict = merge_dict(getattr(base, attr), attrs_dict)

            setattr(cls, attr, merge_dict(getattr(cls, attr), attrs_dict))

        enrich_attr('attrs')
        enrich_attr('attrs_labels')
        enrich_attr('attrs_help')
        enrich_attr('wrappers')
        enrich_attr('layout')
        cls._hook_init_subclass()

    @classmethod
    def _hook_init_subclass(cls):
        """"""

    def _get_attr_aria_describedby(self, field: BoundField) -> Optional[str]:
        if self.opt_render_help:
            return f'{field.id_for_label}_help'
        return None

    def _get_attr_aria_label(self, field: BoundField) -> Optional[str]:
        if not self.opt_render_labels:
            return field.label
        return None

    def _get_attr_form_enctype(self):

        if self.form.is_multipart():
            return 'multipart/form-data'

        return None

    def _get_attr_form_method(self):
        return self.form.src or 'POST'

    attrs: TypeAttrs = {
        FORM: {
            'method': _get_attr_form_method,
            'enctype': _get_attr_form_enctype,
        },
        ALL_FIELDS: {
            'aria-describedby': _get_attr_aria_describedby,
            'aria-label': _get_attr_aria_label,
        },
    }
    """Attributes to apply to basic elements (form, fields, widget types, groups)."""

    def _attrs_get(
            self,
            container: Optional[Dict[str, Any]],
            key: str = None,
            *,
            obj: Any = None,
            accumulated: Dict[str, str] = None,
    ):
        accumulate = accumulated is not None
        accumulated = accumulated or {}
        container = container or {}

        if key is None:
            attrs = container
        else:
            attrs = container.get(key, {})

        if attrs:
            if not isinstance(attrs, dict):
                attrs = {_VALUE: attrs}

            attrs_ = {}
            for key, val in attrs.items():

                if callable(val):
                    if obj is None:
                        val = val(self)
                    else:
                        val = val(self, obj)

                if val is not None:

                    if accumulate:
                        val_str = f'{val}'
                        if val_str[0] == '+':
                            # append to a value, e.g. for 'class' attribute
                            val = f"{accumulated.get(key, '')} {val_str[1:]}"

                    attrs_[key] = val

            attrs = attrs_

            if accumulate:
                accumulated.update(**attrs)

        return attrs

    def _attrs_get_basic(self, container: Dict[str, Any], field: BoundField):
        attrs = {}
        get_attrs = partial(self._attrs_get, container, obj=field, accumulated=attrs)

        for item in (ALL_FIELDS, field.field.widget.__class__, field.name):
            attrs.update(**get_attrs(item))

        if isinstance(field.field.widget, ReadOnlyWidget):
            attrs.update({**get_attrs(FIELDS_READONLY)})

        return attrs

    def _render_field(self, field: BoundField, attrs: TypeAttrs = None) -> str:

        attrs = attrs or self._attrs_get_basic(self.attrs, field)

        placeholder = attrs.get('placeholder')

        if placeholder is None:

            if self.opt_placeholder_label:
                attrs['placeholder'] = field.label

            elif self.opt_placeholder_help:
                attrs['placeholder'] = field.help_text

        title = attrs.get('title')

        if title is None:

            title_label = self.opt_title_label
            title_help = self.opt_title_help

            if title_label or title_help:

                if title_label:
                    attrs['title'] = field.label

                elif title_help:
                    attrs['title'] = field.help_text

        out = field.as_widget(attrs=attrs)

        if field.field.show_hidden_initial:
            out += field.as_hidden(only_initial=True)

        return f'{out}'

    def _render_label(self, field: BoundField) -> str:
        label = field.label_tag(
            attrs=self._attrs_get_basic(self.attrs_labels, field),
            label_suffix=(
                # Get rid of colons entirely.
                '' if not self.opt_label_colon else (
                    # Or deduce...
                    '' if isinstance(field.field.widget, CheckboxInput) else None
                )
            )
        )
        return f'{label}'

    def _format_feedback_lines(self, errors: List) -> str:
        tag = self.opt_tag_feedback_line
        return '\n'.join([f'<{tag}>{error}</{tag}>' for error in errors])

    def _render_feedback(self, field: BoundField) -> str:

        form = self.form

        if not form.is_submitted:
            return ''

        errors = field.errors
        if not errors:
            return ''

        if field.is_hidden:
            # Gather hidden field errors into non-field group.
            for error in errors:
                form.add_error(
                    None,
                    _('Hidden field "%(name)s": %(error)s') %
                    {'name': field.name, 'error': str(error)})
            return ''

        attrs = self._attrs_get_basic(self.attrs_feedback, field)
        tag = self.opt_tag_feedback

        return f'<{tag} {flatatt(attrs)}>{self._format_feedback_lines(errors)}</{tag}>'

    def _render_feedback_nonfield(self) -> str:
        errors = self.form.non_field_errors()
        if not errors:
            return ''

        attrs = self.attrs_feedback.get(FORM, {})
        tag = self.opt_tag_feedback
        return f'<{tag} {flatatt(attrs)}>{self._format_feedback_lines(errors)}</{tag}>'

    def _render_help(self, field: BoundField) -> str:
        help_text = field.help_text

        if not help_text:
            return ''

        attrs = self._attrs_get_basic(self.attrs_help, field)
        attrs['id'] = f'{field.id_for_label}_help'
        tag = self.opt_tag_help

        return f'<{tag} {flatatt(attrs)}>{help_text}</{tag}>'

    def _format_value(self, src: dict, **kwargs) -> str:
        return src[_VALUE].format_map(FormatDict(**kwargs))

    def _apply_layout(self, *, fld: BoundField, field: str, label: str, hint: str, feedback: str) -> str:
        return self._format_value(
            self._attrs_get_basic(self.layout, fld),
            label=label,
            field=field,
            help=hint,
            feedback=feedback,
        )

    def _apply_wrapper(self, *, fld: BoundField, content) -> str:
        return self._format_value(
            self._attrs_get_basic(self.wrappers, fld),
            field=content,
        )

    def _render_field_box(self, field: BoundField) -> str:

        if field.is_hidden:
            self._render_feedback(field)
            return str(field)

        label = ''
        hint = ''

        if self.opt_render_labels:
            label = self._render_label(field)

        if self.opt_render_help:
            hint = self._render_help(field)

        out = self._apply_layout(
            fld=field,
            field=self._render_field(field),
            label=label,
            hint=hint,
            feedback=self._render_feedback(field),
        )

        return self._apply_wrapper(fld=field, content=out)

    def _render_group(self, alias: str, *, rows: List[Union[BoundField, List[BoundField]]]) -> str:

        get_attrs = self._attrs_get
        attrs = self.attrs
        wrappers = self.wrappers
        format_value = self._format_value
        render_field_box = self._render_field_box

        def get_group_params(container: dict) -> dict:
            get_params = partial(get_attrs, container)
            return {**get_params(ALL_GROUPS), **get_params(alias)}

        def render(field: Union[BoundField, List[Union[BoundField, str]]], wrap: bool = False) -> str:

            if isinstance(field, list):
                out = []

                for subfield in field:

                    if isinstance(subfield, list):
                        rendered = render(subfield, wrap=len(subfield) > 1)

                    elif isinstance(subfield, str):
                        rendered = subfield

                    else:
                        rendered = render_field_box(subfield)

                    out.append(rendered)

                out = '\n'.join(out)
                if wrap:
                    out = format_value(wrapper_stacked, field=out)

                return out

            return render_field_box(field)

        wrapper_stacked = get_attrs(wrappers, FIELDS_STACKED)
        wrapper_rows = get_attrs(wrappers, ALL_ROWS)

        html = format_value(
            get_group_params(wrappers),
            attrs=flatatt(get_group_params(attrs)),
            title=self.groups.get(alias, ''),
            rows='\n'.join(
                format_value(
                    wrapper_rows,
                    attrs=flatatt(get_attrs(attrs, ALL_ROWS, obj=fields)),
                    fields=render(fields),
                )
                for fields in rows
            )
        )

        return html

    def _render_layout(self) -> str:
        form = self.form

        fields = {name: form[name] for name in form.fields}

        form_layout = self.layout[FORM]

        out = []

        if isinstance(form_layout, str):
            # Simple layout defined by macros.

            if form_layout == ALL_FIELDS:
                # all fields, no grouping
                render_field_box = self._render_field_box
                out.extend(render_field_box(field) for field in fields.values())

            else:  # pragma: nocover
                raise ValueError(f'Unsupported form layout macros: {form_layout}')

        elif isinstance(form_layout, dict):
            # Advanced layout with groups.
            render_group = self._render_group
            grouped = defaultdict(list)

            def add_fields_left():
                group.extend([[field] for field in fields.values()])
                fields.clear()

            def add_fields_left_hidden():
                # This will allow rendering of hidden fields.
                # Useful in case of subforms as formsets with custom
                # layout when ALL_FIELDS is not used but we need to preserve
                # hidden fields with IDs to save form properly.
                hidden = [field for field in fields.values() if field.is_hidden]
                if hidden:
                    grouped['_hidden'] = hidden
                fields.clear()

            for group_alias, rows in form_layout.items():

                group = grouped[group_alias]

                if isinstance(rows, str):
                    # Macros.

                    if rows == ALL_FIELDS:
                        # All the fields left as separate rows.
                        add_fields_left()

                    else:  # pragma: nocover
                        raise ValueError(f'Unsupported group layout macros: {rows}')

                else:

                    for row in rows:
                        if isinstance(row, str):

                            if row == ALL_FIELDS:
                                # All the fields left as separate rows.
                                add_fields_left()

                            else:
                                # One field in row.
                                group.append([fields.pop(row)])

                        else:
                            # Several fields in a row.
                            row_items = []
                            for row_item in row:
                                if not isinstance(row_item, list):
                                    row_item = [row_item]
                                row_items.append([fields.pop(row_subitem, '') for row_subitem in row_item])
                            group.append(row_items)

            add_fields_left_hidden()

            for group_alias, rows in grouped.items():
                out.append(render_group(group_alias, rows=rows))

        out.insert(0, self._render_feedback_nonfield())

        return '\n'.join(out)

    def _render_submit(self) -> str:
        get_attr = self._attrs_get
        return self._format_value(
            get_attr(self.wrappers, SUBMIT),
            submit=(
                f'<button type="submit" name="{self.opt_submit_name}" value="{self.form.submit_marker}"'
                f'{flatatt(get_attr(self.attrs, SUBMIT))}>{self.opt_submit}</button>'
            )
        )

    def render(self, *, render_form_tag: bool = UNSET) -> str:
        """Renders form to string.

        :param render_form_tag: Can be used to override `opt_render_form_tag` class setting.

        """
        html = self._render_layout()

        render_form_tag = self.opt_render_form_tag if render_form_tag is UNSET else render_form_tag

        if render_form_tag:
            get_attr = partial(self._attrs_get, self.attrs)
            form = self.form

            form_id = form.id or ''
            if form_id:
                form_id = f' id="{form.id}"'

            request = form.request

            csrf = ''
            if request and form.src == 'POST':  # do not leak csrf token for GET
                csrf = f'<input type="hidden" name="csrfmiddlewaretoken" value="{get_token(request)}">'

            action = ''
            target_url = form.target_url
            if target_url:
                action = f' action="{target_url}"'

            html = (
                f'<form {flatatt(get_attr(FORM))}{form_id}{action}>'
                f'{csrf}'
                f'{html}'
                f'{self._render_submit()}'
                '</form>'
            )

        return html
