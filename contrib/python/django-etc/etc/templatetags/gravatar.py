import hashlib

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

from django import template
from django.template.defaultfilters import safe
from django.contrib.auth import get_user_model

USER_MODEL = get_user_model()

register = template.Library()


def get_gravatar_url(obj, size=65, default='identicon'):
    """

    See http://ru.gravatar.com/site/implement/images/ for details on options.

    :param UserModel, str obj:
    :param int size:
    :param str default: 404, mm (mystery-man), identicon, monsterid, wavatar, retro, blank
    :return:
    """
    if isinstance(obj, USER_MODEL):
        email = obj.email or obj.username
    else:
        email = obj

    if email:
        return ('http://www.gravatar.com/avatar/%s/?%s' %
                (hashlib.md5(email.encode()).hexdigest(), urlencode({'size': size, 'd': default})))
    return ''


@register.simple_tag
def gravatar_get_url(obj, size=65, default='identicon'):
    """Returns Gravatar image URL for a given string or UserModel.

    Example:

        {% load gravatar %}
        {% gravatar_get_url user_model %}

    :param UserModel, str obj:
    :param int size:
    :param str default:
    :return:
    """
    return get_gravatar_url(obj, size=size, default=default)


@register.simple_tag
def gravatar_get_img(obj, size=65, default='identicon'):
    """Returns Gravatar image HTML tag for a given string or UserModel.

    Example:

        {% load gravatar %}
        {% gravatar_get_img user_model %}

    :param UserModel, str obj:
    :param int size:
    :param str default:
    :return:
    """
    url = get_gravatar_url(obj, size=size, default=default)
    if url:
        return safe('<img src="%s" class="gravatar">' % url)
    return ''
