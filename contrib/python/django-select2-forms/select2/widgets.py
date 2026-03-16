from itertools import chain
import json

import django
from django.urls import reverse
from django.forms import widgets
from django.forms.utils import flatatt
from django.utils.datastructures import MultiValueDict
from django.utils.html import escape, conditional_escape
from django.utils.encoding import force_str
from django.utils.safestring import mark_safe

from .utils import combine_css_classes


__all__ = ('Select', 'SelectMultiple',)


class Select(widgets.Input):

    allow_multiple_selected = False

    class Media:
        js = (
            "admin/js/jquery.init.js",
            "select2/js/select2.jquery_ready.js",
            "select2/js/select2.jquery_ui_sortable.js",
            "select2/js/select2.js",
        )
        css = {
            "all": (
                "select2/css/select2.css",
            )}

    js_options_map = {
        'maximum_selection_size': 'maximumSelectionSize',
        'allow_clear': 'allowClear',
        'minimum_input_length': 'minimumInputLength',
        'minimum_results_for_search': 'minimumResultsForSearch',
        'close_on_select': 'closeOnSelect',
        'open_on_enter': 'openOnEnter',
        'token_separators': 'tokenSeparators',
        'ajax_quiet_millis': 'quietMillis',
        'quiet_millis': 'quietMillis',
        'data_type': 'dataType',
    }

    js_options = None
    sortable = False
    default_class = ('django-select2',)
    ajax = False

    def __init__(self, attrs=None, choices=(), js_options=None, *args, **kwargs):
        self.ajax = kwargs.pop('ajax', self.ajax)
        self.js_options = {}
        if js_options is not None:
            for k, v in js_options.items():
                if k in self.js_options_map:
                    k = self.js_options_map[k]
                self.js_options[k] = v

        attrs = attrs.copy() if attrs is not None else {}

        if 'sortable' in kwargs:
            self.sortable = kwargs.pop('sortable')

        self.attrs = getattr(self, 'attrs', {}) or {}

        self.attrs.update({
            'data-placeholder': kwargs.pop('overlay', None),
            'class': combine_css_classes(attrs.get('class', ''), self.default_class),
            'data-sortable': json.dumps(self.sortable),
        })

        self.attrs.update(attrs)
        self.choices = iter(choices)

    def reverse(self, lookup_view):
        opts = getattr(self, 'model', self.field.model)._meta
        return reverse(lookup_view, kwargs={
            'app_label': opts.app_label,
            'model_name': opts.object_name.lower(),
            'field_name': self.field.name,
        })

    def option_to_data(self, option_value, option_label):
        if not option_value:
            return
        if isinstance(option_label, (list, tuple)):
            return {
                "text": force_str(option_value),
                "children": [_f for _f in [self.option_to_data(v, l) for v, l in option_label] if _f],
            }
        return {
            "id": force_str(option_value),
            "text": force_str(option_label),
        }

    def render(self, name, value, attrs=None, choices=(), js_options=None, **kwargs):
        options = {}
        attrs = dict(self.attrs, **(attrs or {}))
        js_options = js_options or {}

        for k, v in dict(self.js_options, **js_options).items():
            if k in self.js_options_map:
                k = self.js_options_map[k]
            options[k] = v

        if self.ajax:
            ajax_url = options.pop('ajax_url', None)
            quiet_millis = options.pop('quietMillis', 100)
            is_jsonp = options.pop('jsonp', False)

            ajax_opts = options.get('ajax', {})

            default_ajax_opts = {
                'url': ajax_url or self.reverse('select2_fetch_items'),
                'dataType': 'jsonp' if is_jsonp else 'json',
                'quietMillis': quiet_millis,
            }
            for k, v in ajax_opts.items():
                if k in self.js_options_map:
                    k = self.js_options_map[k]
                default_ajax_opts[k] = v
            options['ajax'] = default_ajax_opts

        if not self.is_required:
            options.update({'allowClear': options.get('allowClear', True)})

        if self.sortable and not self.ajax:
            data = []
            for option_value, option_label in chain(self.choices, choices):
                data.append(self.option_to_data(option_value, option_label))
            options['data'] = list([_f for _f in data if _f])

        attrs.update({
            'data-select2-options': json.dumps(options),
        })

        if self.ajax:
            attrs.update({
                'data-init-selection-url': self.reverse('select2_init_selection'),
            })
        if self.ajax or self.sortable:
            self.input_type = 'hidden'
            return super(Select, self).render(name, value, attrs=attrs)
        else:
            return self.render_select(name, value, attrs=attrs, choices=choices)

    def render_select(self, name, value, attrs=None, choices=()):
        if value is None:
            value = ''
        attrs = attrs or {}
        attrs['name'] = name
        final_attrs = self.build_attrs(attrs)
        output = [u'<select%s>' % flatatt(final_attrs)]
        if not isinstance(value, (list, tuple)):
            value = [value]
        options = self.render_options(choices, value)
        if options:
            output.append(options)
        output.append(u'</select>')
        return mark_safe(u'\n'.join(output))

    def render_option(self, selected_choices, option_value, option_label):
        option_value = force_str(option_value)
        if option_value in selected_choices:
            selected_html = u' selected="selected"'
            if not self.allow_multiple_selected:
                # Only allow for a single selection.
                selected_choices.remove(option_value)
        else:
            selected_html = ''
        return u'<option value="%s"%s>%s</option>' % (
            escape(option_value), selected_html,
            conditional_escape(str(option_label)))

    def render_options(self, choices, selected_choices):
        # Normalize to strings.
        selected_choices = set(force_str(v) for v in selected_choices)
        output = []
        for option_value, option_label in chain(self.choices, choices):
            if isinstance(option_label, (list, tuple)):
                output.append(u'<optgroup label="%s">' % escape(force_str(option_value)))
                for option in option_label:
                    output.append(self.render_option(selected_choices, *option))
                output.append(u'</optgroup>')
            else:
                output.append(self.render_option(selected_choices, option_value, option_label))
        return u'\n'.join(output)


class SelectMultiple(Select):

    allow_multiple_selected = True

    def __init__(self, attrs=None, choices=(), js_options=None, *args, **kwargs):
        options = {}
        default_attrs = {}
        ajax = kwargs.get('ajax', self.ajax)
        sortable = kwargs.get('sortable', self.sortable)
        if ajax or sortable:
            options.update({'multiple': True})
        else:
            default_attrs.update({
                'multiple': 'multiple',
            })
        attrs = dict(default_attrs, **attrs) if attrs else default_attrs
        if js_options is not None:
            options.update(js_options)

        super(SelectMultiple, self).__init__(attrs=attrs, choices=choices,
                js_options=options, *args, **kwargs)

    def format_value(self, value):
        if isinstance(value, list):
            value = u','.join([force_str(v) for v in value])
        return value

    if django.VERSION < (1, 10):
        _format_value = format_value

    def value_from_datadict(self, data, files, name):
        # Since ajax widgets use hidden or text input fields, when using ajax the value needs to be a string.
        if not self.ajax and not self.sortable and isinstance(data, MultiValueDict):
            value = data.getlist(name)
        else:
            value = data.get(name, None)
        if isinstance(value, str):
            return [v for v in value.split(',') if v]
        return value
