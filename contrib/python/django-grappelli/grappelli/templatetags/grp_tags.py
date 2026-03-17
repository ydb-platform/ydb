# coding: utf-8

import json
from functools import wraps

from django import template
from django.contrib.contenttypes.models import ContentType
from django.template.loader import get_template
from django.utils.formats import get_format
from django.utils.safestring import mark_safe
from django.utils.translation import get_language
from django.utils.translation import gettext as _

from grappelli.settings import (ADMIN_TITLE, ADMIN_URL, CLEAN_INPUT_TYPES,
                                SWITCH_USER, SWITCH_USER_ORIGINAL,
                                SWITCH_USER_TARGET)

try:
    from django.contrib.auth import get_user_model
    User = get_user_model()
except ImportError:
    from django.contrib.auth.models import User

register = template.Library()


# GENERIC OBJECTS
class do_get_generic_objects(template.Node):
    def __init__(self):
        pass

    def render(self, context):
        objects = {}
        for c in ContentType.objects.all().order_by('id'):
            objects[c.id] = {'pk': c.id, 'app': c.app_label, 'model': c.model}
        return json.dumps(objects)


@register.tag
def get_content_types(parser, token):
    """
    Returns a list of installed applications and models.
    Needed for lookup of generic relationships.
    """
    return do_get_generic_objects()


# ADMIN_TITLE
@register.simple_tag
def get_admin_title():
    """
    Returns the Title for the Admin-Interface.
    """
    return ADMIN_TITLE


# SITE_TITLE
@register.simple_tag
def get_site_title():
    """
    Returns the Title for the Admin-Interface.
    """
    return ADMIN_TITLE or _("Django site admin")


# RETURNS CURRENT LANGUAGE
@register.simple_tag
def get_lang():
    return get_language()


# ADMIN_URL
@register.simple_tag
def get_admin_url():
    """
    Returns the URL for the Admin-Interface.
    """
    return ADMIN_URL


@register.simple_tag
def get_date_format():
    return get_format('DATE_INPUT_FORMATS')[0]


@register.simple_tag
def get_time_format():
    return get_format('TIME_INPUT_FORMATS')[0]


@register.simple_tag
def get_datetime_format():
    return get_format('DATETIME_INPUT_FORMATS')[0]


@register.simple_tag
def grappelli_admin_title():
    return ADMIN_TITLE


@register.simple_tag
def grappelli_clean_input_types():
    return CLEAN_INPUT_TYPES


@register.filter
def classname(obj, arg=None):
    classname = obj.__class__.__name__.lower()
    if arg:
        if arg.lower() == classname:
            return True
        return False
    return classname


@register.filter
def classpath(obj):
    module = obj.__module__
    classname = obj.__class__.__name__
    return "%s,%s" % (module, classname)


# FORMSETSORT FOR SORTABLE INLINES

@register.filter
def formsetsort(formset, arg):
    """
    Takes a list of formset dicts, returns that list sorted by the sortable field.
    """
    if arg:
        sorted_list = []
        unsorted_list = []
        for item in formset:
            position = item.form[arg].value()
            if isinstance(position, int) and item.original:  # normal view
                sorted_list.append((position, item))
            elif position and hasattr(item.form, 'cleaned_data'):  # error validation
                sorted_list.append((int(position), item))
            else:
                unsorted_list.append(item)
        sorted_list.sort(key=lambda i: i[0])
        sorted_list = [item[1] for item in sorted_list] + unsorted_list
    else:
        sorted_list = formset
    return sorted_list


# RELATED LOOKUPS

def safe_json_else_list_tag(f):
    """
    Decorator. Registers function as a simple_tag.
    Try: Return value of the decorated function marked safe and json encoded.
    Except: Return []
    """
    @wraps(f)
    def inner(model_admin):
        try:
            return mark_safe(json.dumps(f(model_admin)))
        except:
            return []
    return register.simple_tag(inner)


@safe_json_else_list_tag
def get_related_lookup_fields_fk(model_admin):
    return model_admin.related_lookup_fields.get("fk", [])


@safe_json_else_list_tag
def get_related_lookup_fields_m2m(model_admin):
    return model_admin.related_lookup_fields.get("m2m", [])


@safe_json_else_list_tag
def get_related_lookup_fields_generic(model_admin):
    return model_admin.related_lookup_fields.get("generic", [])


# AUTOCOMPLETES

@safe_json_else_list_tag
def get_autocomplete_lookup_fields_fk(model_admin):
    return model_admin.autocomplete_lookup_fields.get("fk", [])


@safe_json_else_list_tag
def get_autocomplete_lookup_fields_m2m(model_admin):
    return model_admin.autocomplete_lookup_fields.get("m2m", [])


@safe_json_else_list_tag
def get_autocomplete_lookup_fields_generic(model_admin):
    return model_admin.autocomplete_lookup_fields.get("generic", [])


# SORTABLE EXCLUDES
@safe_json_else_list_tag
def get_sortable_excludes(model_admin):
    return model_admin.sortable_excludes


@register.filter
def prettylabel(value):
    return mark_safe(value.replace(":</label>", "</label>"))


# CUSTOM ADMIN LIST FILTER
# WITH TEMPLATE DEFINITION
@register.simple_tag
def admin_list_filter(cl, spec):
    field_name = getattr(spec, "field", None)
    parameter_name = getattr(spec, "parameter_name", None)
    if field_name is not None:
        field_name = spec.field.name
    elif parameter_name is not None:
        field_name = spec.parameter_name
    try:
        tpl = get_template(cl.model_admin.change_list_filter_template)
    except:  # noqa
        tpl = get_template(spec.template)
    return tpl.render({
        'title': spec.title,
        'choices': list(spec.choices(cl)),
        'field_name': field_name,
        'spec': spec,
    })


@register.simple_tag(takes_context=True)
def switch_user_dropdown(context):
    if SWITCH_USER:
        tpl = get_template("admin/includes_grappelli/switch_user_dropdown.html")
        request = context["request"]
        session_user = request.session.get("original_user", {"id": request.user.id, "username": request.user.get_username()})
        try:
            original_user = User.objects.get(pk=session_user["id"], is_staff=True)
        except User.DoesNotExist:
            return ""
        if SWITCH_USER_ORIGINAL(original_user):
            object_list = [user for user in User.objects.filter(is_staff=True).exclude(pk=original_user.pk) if SWITCH_USER_TARGET(original_user, user)]
            return tpl.render({
                'request': request,
                'object_list': object_list
            })
    return ""
