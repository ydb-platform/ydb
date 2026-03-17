from django import template

register = template.Library()


def getattribute(value, arg):
    """Gets an attribute of an object dynamically from a string name"""

    return getattr(value, arg, None)


register.filter("getattribute", getattribute)
