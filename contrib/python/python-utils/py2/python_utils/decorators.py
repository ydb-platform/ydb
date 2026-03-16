

def set_attributes(**kwargs):
    '''Decorator to set attributes on functions and classes

    A common usage for this pattern is the Django Admin where
    functions can get an optional short_description. To illustrate:

    Example from the Django admin using this decorator:
    https://docs.djangoproject.com/en/3.0/ref/contrib/admin/#django.contrib.admin.ModelAdmin.list_display

    Our simplified version:

    >>> @set_attributes(short_description='Name')
    ... def upper_case_name(self, obj):
    ...     return ("%s %s" % (obj.first_name, obj.last_name)).upper()

    The standard Django version:

    >>> def upper_case_name(obj):
    ...     return ("%s %s" % (obj.first_name, obj.last_name)).upper()

    >>> upper_case_name.short_description = 'Name'

    '''
    def _set_attributes(function):
        for key, value in kwargs.items():
            setattr(function, key, value)
        return function
    return _set_attributes
