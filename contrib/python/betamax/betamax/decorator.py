import functools
import unittest

import requests

from . import recorder


def use_cassette(cassette_name, cassette_library_dir=None,
                 default_cassette_options={}, **use_cassette_kwargs):
    r"""Provide a Betamax-wrapped Session for convenience.

    .. versionadded:: 0.5.0

    This decorator can be used to get a plain Session that has been wrapped in
    Betamax. For example,

    .. code-block:: python

        from betamax.decorator import use_cassette

        @use_cassette('example-decorator', cassette_library_dir='.')
        def test_get(session):
            # do things with session

    :param str cassette_name:
        Name of the cassette file in which interactions will be stored.
    :param str cassette_library_dir:
        Directory in which cassette files will be stored.
    :param dict default_cassette_options:
        Dictionary of default cassette options to set for the cassette used
        when recording these interactions.
    :param \*\*use_cassette_kwargs:
        Keyword arguments passed to :meth:`~betamax.Betamax.use_cassette`
    """
    def actual_decorator(func):
        @functools.wraps(func)
        def test_wrapper(*args, **kwargs):
            session = requests.Session()
            recr = recorder.Betamax(
                session=session,
                cassette_library_dir=cassette_library_dir,
                default_cassette_options=default_cassette_options
            )

            if args:
                fst, args = args[0], args[1:]
                if isinstance(fst, unittest.TestCase):
                    args = (fst, session) + args
                else:
                    args = (session, fst) + args
            else:
                args = (session,)

            with recr.use_cassette(cassette_name, **use_cassette_kwargs):
                func(*args, **kwargs)

        return test_wrapper
    return actual_decorator
