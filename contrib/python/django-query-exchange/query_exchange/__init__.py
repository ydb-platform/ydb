from django.utils.http import urlencode
from django.core.urlresolvers import reverse
from django.utils.itercompat import is_iterable


def reverse_with_query(viewname, urlconf=None, args=None, kwargs=None, prefix=None, current_app=None,
                       params=None, keep=None, exclude=None, add=None, remove=None):
    url = reverse(viewname, urlconf, args, kwargs, prefix, current_app)

    if params is not None:
        url += '?' + process_query(params, keep, exclude, add, remove)

    return url


def process_query(params, keep=None, exclude=None, add=None, remove=None):
    data = dict(_extract_items(params))

    keep = keep or []
    exclude = exclude or []

    if keep:
        data = dict((k, data[k]) for k in keep if k in data)
    elif exclude:
        for k in exclude:
            data.pop(k, None)

    if add:
        add = dict(_extract_items(add))

        for k, v in add.iteritems():
            if k in data and keep and k in keep:
                data[k].extend(v)
            else:
                data[k] = v

    if remove:
        remove = dict(_extract_items(remove))
        for k, v in remove.iteritems():
            if k in data:
                for value in v:
                    if value in data[k]:
                        data[k].remove(value)


    return urlencode([(k, v) for k, l in sorted(data.iteritems()) for v in l])


def _is_iterable(iterable):
    return not isinstance(iterable, basestring) and is_iterable(iterable)


def _extract_items(iterable):
    if hasattr(iterable, 'iterlists'):
        return ((k, v[:]) for k, v in iterable.iterlists())

    return ((k, _is_iterable(v) and list(v) or [v])
                for k, v in (iterable.iteritems()
                             if hasattr(iterable, 'iteritems')
                             else iterable))
