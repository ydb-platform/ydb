import trafaret as t
from . import codes


class KeysSubset(t.Key):
    """
    From checkers and converters dict must be returned. Some for errors.

    >>> from . import extract_error, Mapping, String
    >>> cmp_pwds = lambda x: {'pwd': x['pwd'] if x.get('pwd') == x.get('pwd1') else DataError('Not equal')}
    >>> d = Dict({KeysSubset('pwd', 'pwd1'): cmp_pwds, 'key1': String})
    >>> sorted(d.check({'pwd': 'a', 'pwd1': 'a', 'key1': 'b'}).keys())
    ['key1', 'pwd']
    >>> extract_error(d.check, {'pwd': 'a', 'pwd1': 'c', 'key1': 'b'})
    {'pwd': 'Not equal'}
    >>> extract_error(d.check, {'pwd': 'a', 'pwd1': None, 'key1': 'b'})
    {'pwd': 'Not equal'}
    >>> get_values = (lambda d, keys: [d[k] for k in keys if k in d])
    >>> join = (lambda d: {'name': ' '.join(get_values(d, ['name', 'last']))})
    >>> Dict({KeysSubset('name', 'last'): join}).check({'name': 'Adam', 'last': 'Smith'})
    {'name': 'Adam Smith'}
    """

    def __init__(self, *keys):
        self.keys = keys
        self.name = '[%s]' % ', '.join(self.keys)
        self.trafaret = t.Any()

    def __call__(self, data):
        subdict = dict((k, data.get(k)) for k in self.keys if k in data)
        keys_names = self.keys
        res = t.catch_error(self.trafaret, subdict)
        if isinstance(res, t.DataError):
            for k, e in res.error.items():
                if not isinstance(e, t.DataError):
                    raise RuntimeError('Please use DataError instance')
                yield k, e, keys_names
        else:
            for k, v in res.items():
                yield k, v, keys_names


def subdict(name, *keys, **kw):
    """
    Subdict key.

    Takes a `name`, any number of keys as args and keyword argument `trafaret`.
    Use it like:
    ::

        def check_passwords_equal(data):
            if data['password'] != data['password_confirm']:
                return t.DataError('Passwords are not equal')
            return data['password']

        passwords_key = subdict(
            'password',
            t.Key('password', trafaret=check_password),
            t.Key('password_confirm', trafaret=check_password),
            trafaret=check_passwords_equal,
        )

        signup_trafaret = t.Dict(
            t.Key('email', trafaret=t.Email),
            passwords_key,
        )

    """
    trafaret = kw.pop('trafaret')  # coz py2k

    def inner(data, context=None):
        errors = False
        preserve_output = []
        touched = set()
        collect = {}
        for key in keys:
            for k, v, names in key(data, context=context):
                touched.update(names)
                preserve_output.append((k, v, names))
                if isinstance(v, t.DataError):
                    errors = True
                else:
                    collect[k] = v
        if errors:
            for out in preserve_output:
                yield out
        elif collect:
            yield name, t.catch(trafaret, collect), touched

    return inner


def xor_key(first, second, trafaret):
    """
    xor_key - takes `first` and `second` key names and `trafaret`.

    Checks if we have only `first` or only `second` in data, not both,
    and at least one.

    Then checks key value against trafaret.
    """
    trafaret = t.ensure_trafaret(trafaret)

    def check_(value):
        if (first in value) ^ (second in value):
            key = first if first in value else second
            yield first, t.catch_error(trafaret, value[key]), (key,)
        elif first in value and second in value:
            yield (
                first,
                t.DataError(
                    error='correct only if {} is not defined'.format(second),
                    code=codes.ONLY_ONE_MUST_BE_DEFINED
                ),
                (first,)
            )
            yield (
                second,
                t.DataError(
                    error='correct only if {} is not defined'.format(first),
                    code=codes.ONLY_ONE_MUST_BE_DEFINED
                ),
                (second,)
            )
        else:
            yield (
                first,
                t.DataError(
                    error='is required if {} is not defined'.format(second),
                    code=codes.ONE_IS_REQUIRED
                ),
                (first,)
            )
            yield (
                second,
                t.DataError(
                    error='is required if {} is not defined'.format(first),
                    code=codes.ONE_IS_REQUIRED
                ),
                (second,)
            )

    return check_


def confirm_key(name, confirm_name, trafaret):
    """
    confirm_key - takes `name`, `confirm_name` and `trafaret`.

    Checks if data['name'] equals data['confirm_name'] and both
    are valid against `trafaret`.
    """
    def check_(value):
        first, second = None, None
        if name in value:
            first = value[name]
            yield name, t.catch_error(trafaret, first), (name,)
        else:
            yield name, t.DataError('is required', code=codes.REQUIRED), (name,)
        if confirm_name in value:
            second = value[confirm_name]
            yield confirm_name, t.catch_error(trafaret, second), (confirm_name,)
        else:
            yield confirm_name, t.DataError('is required', code=codes.REQUIRED), (confirm_name,)
        if not (first and second):
            return
        if first != second:
            yield (
                confirm_name,
                t.DataError(
                    'must be equal to {}'.format(name),
                    code=codes.MUST_BE_EQUAL
                ),
                (confirm_name,)
            )
    return check_
