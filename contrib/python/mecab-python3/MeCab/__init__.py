# Second-level Python wrappers for the SWIG-generated MeCab interface.
# This file bypasses and replaces the SWIG-generated MeCab.py.
# Since we use SWIG's `-builtin` mode, it does almost nothing that we
# need, and it gets in the way of the shimming that we do need.
# Theoretically we could accomplish everything below with
# modifications to MeCab.i, but then we would have to diverge that file
# further from upstream.

from __future__ import absolute_import, print_function
from . import _MeCab

import os
import sys
import shlex


#
# Most of the public symbols come directly from the internal _MeCab
# module.
#
def _reexport_filtered(thismod, submod):
    skips = {
        # implementation details
        "Model_version", "Tagger_version", "SWIG_PyInstanceMethod_New",

        # classes exported via a wrapper
        "Tagger", "Model",
    }
    selected = []
    for sym in dir(submod):
        if sym.startswith("MECAB_") or (sym[0] != '_' and sym not in skips):
            selected.append(sym)
            setattr(thismod, sym, getattr(submod, sym))
    return selected


__all__ = _reexport_filtered(__import__(__name__), _MeCab)
del _reexport_filtered

#
# Version information
#
# Note that this is the version of MeCab, not of mecab-python3. Since MeCab
# hasn't been updated since roughly 2013 this has been 0.996 for years and is
# unlikely to ever change.
#
# This is preserved to avoid breaking API compatibility, but should be removed
# in v2, should that ever happen.
#

VERSION = _MeCab.Tagger_version()
__all__.append("VERSION")


def try_import_unidic():
    """Import unidic or unidic-lite if available. Return dicdir.

    This is specifically for dictionaries installed via pip.
    """
    try:
        import unidic
        return unidic.DICDIR
    except ImportError:
        try:
            import unidic_lite
            return unidic_lite.DICDIR
        except ImportError:
            # This is OK, just give up.
            return


FAILMESSAGE = """
----------------------------------------------------------

Failed initializing MeCab. Please see the README for possible solutions:

    https://github.com/SamuraiT/mecab-python3#common-issues

If you are still having trouble, please file an issue here, and include the
ERROR DETAILS below:

    https://github.com/SamuraiT/mecab-python3/issues

issueを英語で書く必要はありません。

------------------- ERROR DETAILS ------------------------"""


def get_error_details(args):
    """Instantiate a Model to get output from MeCab.

    Due to an upstream bug, errors in Tagger intialization don't give useful
    error output."""
    try:
        Model(args, error_check=True)
    except RuntimeError as err:
        # get the MeCab error string
        errstr = str(err)[len('RuntimeError: '):]
        return errstr

    return "No error, your args appear to work."


def error_info(args):
    """Render guide to solving initialization errors."""
    msg = FAILMESSAGE + "\n"
    msg += "arguments: " + str(args) + "\n"
    # Check if a dictionary is found

    dict_path = try_import_unidic()
    msg += f"default dictionary path: {dict_path}\n"

    message = get_error_details(args)
    msg += message + "\n"
    msg += '----------------------------------------------------------\n'
    return msg

class Tagger(_MeCab.Tagger):
    def __init__(self, rawargs=""):
        # First check for Unidic.
        unidicdir = try_import_unidic()
        args = rawargs
        if unidicdir:
            mecabrc = os.path.join(unidicdir, 'mecabrc')
            args = '-r "{}" -d "{}" '.format(mecabrc, unidicdir) + args

        # The first argument here isn't used. In the MeCab binary the argc and
        # argv from the shell are re-used, so the first element will be the
        # binary name.
        args = ['', '-C'] + shlex.split(args)

        # need to encode the strings to bytes, see here:
        # https://stackoverflow.com/questions/48391926/python-swig-in-typemap-does-not-work
        args = [x.encode('utf-8') for x in args]

        try:
            super(Tagger, self).__init__(args)
        except RuntimeError as ee:
            raise RuntimeError(error_info(rawargs)) from ee


class Model(_MeCab.Model):
    def __init__(self, rawargs="", error_check=False):
        # Note this is the same as the code for the Tagger.
        unidicdir = try_import_unidic()
        args = rawargs
        if unidicdir:
            mecabrc = os.path.join(unidicdir, 'mecabrc')
            args = '-r "{}" -d "{}" '.format(mecabrc, unidicdir) + args
        args = ['', '-C'] + shlex.split(args)
        args = [x.encode('utf-8') for x in args]

        try:
            super(Model, self).__init__(args)
        except RuntimeError as ee:
            if not error_check:
                raise RuntimeError(error_info(rawargs)) from ee
            else:
                raise


__all__.append("Model")
__all__.append("Tagger")
