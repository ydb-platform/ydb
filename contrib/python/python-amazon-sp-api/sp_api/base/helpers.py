from io import BytesIO
import hashlib
import base64
import warnings
import functools
from urllib import parse


def fill_query_params(query, *args):
    return query.format(*[parse.quote(arg, safe='') for arg in args])


def sp_endpoint(path, method='GET'):
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            kwargs.update({
                'path': path,
                'method': method
            })
            return function(*args, **kwargs)

        return wrapper

    return decorator


def create_md5(file):
    hash_md5 = hashlib.md5()
    if isinstance(file, BytesIO):
        for chunk in iter(lambda: file.read(4096), b''):
            hash_md5.update(chunk)
        file.seek(0)
        return base64.b64encode(hash_md5.digest()).decode()
    if isinstance(file, str):
        with open(file, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        return base64.b64encode(hash_md5.digest()).decode()
    for chunk in iter(lambda: file.read(4096), b''):
        hash_md5.update(chunk)
    return base64.b64encode(hash_md5.digest()).decode()


def nest_dict(flat: dict()):
    """
    Convert flat dictionary to nested dictionary.

    Input
    {
        "AmazonOrderId":1,
        "ShipFromAddress.Name" : "Seller",
        "ShipFromAddress.AddressLine1": "Street",
    }

    Output
    {
        "AmazonOrderId":1,
        "ShipFromAddress.: {
            "Name" : "Seller",
            "AddressLine1": "Street",
        }
    }


    Args:
        flat:dict():

    Returns:
        nested:dict():
    """

    result = {}
    for k, v in flat.items():
        _nest_dict_rec(k, v, result)
    return result


def _nest_dict_rec(k, v, out):
    k, *rest = k.split('.', 1)
    if rest:
        _nest_dict_rec(rest[0], v, out.setdefault(k, {}))
    else:
        out[k] = v


def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func
