# (c) 2007 Chris AtLee <chris@atlee.ca>
# (c) 2010 Grzegorz Nosek <root@localdomain.pl>
# (c) 2015 Min RK <benjaminrk@gmail.com>
# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license.php
"""
PAM module for python

Provides an authenticate function that will allow the caller to authenticate
a user against the Pluggable Authentication Modules (PAM) on the system.

Implemented using ctypes, so no compilation is necessary.
"""

__version__ = '1.2.0'

__all__ = [
    'PAMError',
    'authenticate',
    'open_session',
    'close_session',
    'check_account',
    'change_password',
]

import getpass
import sys
from ctypes import (
    CDLL,
    CFUNCTYPE,
    POINTER,
    Structure,
    byref,
    c_char,
    c_char_p,
    c_int,
    c_uint,
    c_void_p,
    cast,
    pointer,
    sizeof,
)
from ctypes.util import find_library

# Python 3 bytes/unicode compat
unicode = str
raw_input = input


def _bytes_to_str(s, encoding='utf8'):
    return s.decode(encoding)


def _cast_bytes(s, encoding='utf8'):
    if isinstance(s, unicode):
        return s.encode(encoding)
    return s


LIBPAM = CDLL(find_library("pam"))
LIBC = CDLL(find_library("c"))

CALLOC = LIBC.calloc
CALLOC.restype = c_void_p
CALLOC.argtypes = [c_uint, c_uint]

STRDUP = LIBC.strdup
STRDUP.argstypes = [c_char_p]
STRDUP.restype = POINTER(c_char)  # NOT c_char_p !!!!

# Various constants
PAM_PROMPT_ECHO_OFF = 1
PAM_PROMPT_ECHO_ON = 2
PAM_ERROR_MSG = 3
PAM_TEXT_INFO = 4

# These constants are libpam-specific
PAM_ESTABLISH_CRED = 0x0002
PAM_DELETE_CRED = 0x0004
PAM_REINITIALIZE_CRED = 0x0008
PAM_REFRESH_CRED = 0x0010

# constants for PAM_ variables for pam_set_item()
PAM_SERVICE = 1
PAM_USER = 2
PAM_TTY = 3
PAM_RHOST = 4
PAM_RUSER = 8

# PAM error codes
PAM_SUCCESS = 0
PAM_BAD_ITEM = 29


class PamHandle(Structure):
    """wrapper class for pam_handle_t"""

    _fields_ = [("handle", c_void_p)]

    def __init__(self):
        Structure.__init__(self)
        self.handle = 0

    def get_item(self, item_type, encoding='utf-8'):
        voidPointer = c_void_p()
        retval = PAM_GET_ITEM(self, item_type, byref(voidPointer))
        if retval == PAM_BAD_ITEM:
            return None
        if retval != PAM_SUCCESS:
            raise PAMError(errno=retval)

        s = cast(voidPointer, c_char_p)
        if s.value is None:
            return None
        return _bytes_to_str(s.value, encoding)

    def set_item(self, item_type, item, encoding='utf-8'):
        retval = PAM_SET_ITEM(self, item_type, item.encode(encoding))
        if retval != PAM_SUCCESS:
            raise PAMError(errno=retval)

    def get_env(self, var, encoding='utf-8'):
        ret = PAM_GETENV(self, var.encode(encoding))
        if ret is None:
            raise PAMError()
        else:
            return ret.decode(encoding)

    def put_env(self, k, v, encoding='utf-8'):
        retval = PAM_PUTENV(self, (f'{k}={v}').encode(encoding))
        if retval != PAM_SUCCESS:
            raise PAMError(errno=retval)

    def del_env(self, k, encoding='utf-8'):
        retval = PAM_PUTENV(self, k.encode(encoding))
        if retval != PAM_SUCCESS:
            raise PAMError(errno=retval)

    def get_envlist(self, encoding='utf-8'):
        ret = PAM_GETENVLIST(self)
        if ret is None:
            raise PAMError()

        parsed = {}
        for i in PAM_GETENVLIST(self):
            if i:
                k, v = i.decode(encoding).split('=', 1)
                parsed[k] = v
            else:
                break
        return parsed

    def open_session(self):
        retval = PAM_OPEN_SESSION(self, 0)
        if retval != PAM_SUCCESS:
            raise PAMError(errno=retval)

    def close_session(self):
        retval = PAM_CLOSE_SESSION(self, 0)
        if retval != PAM_SUCCESS:
            raise PAMError(errno=retval)


PAM_STRERROR = LIBPAM.pam_strerror
PAM_STRERROR.restype = c_char_p
PAM_STRERROR.argtypes = [PamHandle, c_int]


def pam_strerror(handle, errno):
    """Wrap bytes-only PAM_STRERROR in native str"""
    return _bytes_to_str(PAM_STRERROR(handle, errno))


class PAMError(Exception):
    errno = None
    message = ''

    def __init__(self, message='', errno=None):
        self.errno = errno
        if message:
            self.message = message
        else:
            if errno is None:
                self.message = "Unknown"
            else:
                self.message = pam_strerror(PamHandle(), errno)

    def __repr__(self):
        en = '' if self.errno is None else ' %i' % self.errno
        return f"<PAM Error{en}: '{self.message}'>"

    def __str__(self):
        en = '' if self.errno is None else ' %i' % self.errno
        return f'[PAM Error{en}] {self.message}'


class PamMessage(Structure):
    """wrapper class for pam_message structure"""

    _fields_ = [
        ("msg_style", c_int),
        ("msg", POINTER(c_char)),
    ]

    def __repr__(self):
        return "<PamMessage %i '%s'>" % (self.msg_style, _bytes_to_str(self.msg))


class PamResponse(Structure):
    """wrapper class for pam_response structure"""

    _fields_ = [
        ("resp", POINTER(c_char)),
        ("resp_retcode", c_int),
    ]

    def __repr__(self):
        return "<PamResponse %i '%s'>" % (self.resp_retcode, _bytes_to_str(self.resp))


CONV_FUNC = CFUNCTYPE(
    c_int, c_int, POINTER(POINTER(PamMessage)), POINTER(POINTER(PamResponse)), c_void_p
)


class PamConv(Structure):
    """wrapper class for pam_conv structure"""

    _fields_ = [("conv", CONV_FUNC), ("appdata_ptr", c_void_p)]


PAM_START = LIBPAM.pam_start
PAM_START.restype = c_int
PAM_START.argtypes = [c_char_p, c_char_p, POINTER(PamConv), POINTER(PamHandle)]

PAM_END = LIBPAM.pam_end
PAM_END.restype = c_int
PAM_END.argtypes = [PamHandle, c_int]

PAM_AUTHENTICATE = LIBPAM.pam_authenticate
PAM_AUTHENTICATE.restype = c_int
PAM_AUTHENTICATE.argtypes = [PamHandle, c_int]

PAM_OPEN_SESSION = LIBPAM.pam_open_session
PAM_OPEN_SESSION.restype = c_int
PAM_OPEN_SESSION.argtypes = [PamHandle, c_int]

PAM_CLOSE_SESSION = LIBPAM.pam_close_session
PAM_CLOSE_SESSION.restype = c_int
PAM_CLOSE_SESSION.argtypes = [PamHandle, c_int]

PAM_ACCT_MGMT = LIBPAM.pam_acct_mgmt
PAM_ACCT_MGMT.restype = c_int
PAM_ACCT_MGMT.argtypes = [PamHandle, c_int]

PAM_CHAUTHTOK = LIBPAM.pam_chauthtok
PAM_CHAUTHTOK.restype = c_int
PAM_CHAUTHTOK.argtypes = [PamHandle, c_int]

PAM_SETCRED = LIBPAM.pam_setcred
PAM_SETCRED.restype = c_int
PAM_SETCRED.argtypes = [PamHandle, c_int]

PAM_GETENV = LIBPAM.pam_getenv
PAM_GETENV.restype = c_char_p
PAM_GETENV.argtypes = [PamHandle, c_char_p]

PAM_GETENVLIST = LIBPAM.pam_getenvlist
PAM_GETENVLIST.restype = POINTER(c_char_p)
PAM_GETENVLIST.argtypes = [PamHandle]

PAM_PUTENV = LIBPAM.pam_putenv
PAM_PUTENV.restype = c_int
PAM_PUTENV.argtypes = [PamHandle, c_char_p]

PAM_SET_ITEM = LIBPAM.pam_set_item
PAM_SET_ITEM.restype = c_int
PAM_SET_ITEM.argtypes = [PamHandle, c_int, c_char_p]

PAM_GET_ITEM = LIBPAM.pam_get_item
PAM_GET_ITEM.restype = c_int
PAM_GET_ITEM.argtypes = [PamHandle, c_int, POINTER(c_void_p)]


@CONV_FUNC
def default_conv(n_messages, messages, p_response, app_data):
    addr = CALLOC(n_messages, sizeof(PamResponse))
    p_response[0] = cast(addr, POINTER(PamResponse))
    if not sys.stdin.isatty():
        return 0
    for i in range(n_messages):
        msg = messages[i].contents
        style = msg.msg_style
        msg_string = _bytes_to_str(cast(msg.msg, c_char_p).value)
        if style == PAM_TEXT_INFO:
            # back from POINTER(c_char) to c_char_p
            print(msg_string)
        elif style == PAM_ERROR_MSG:
            print(msg_string, file=sys.stderr)
        elif style in (PAM_PROMPT_ECHO_ON, PAM_PROMPT_ECHO_OFF):
            if style == PAM_PROMPT_ECHO_ON:
                read_pw = raw_input
            else:
                read_pw = getpass.getpass

            pw_copy = STRDUP(_cast_bytes(read_pw(msg_string)))
            p_response.contents[i].resp = pw_copy
            p_response.contents[i].resp_retcode = 0
        else:
            print(repr(messages[i].contents))
    return 0


def new_simple_password_conv(passwords, encoding):
    passwords = [_cast_bytes(password, encoding) for password in passwords]
    passwords.reverse()

    @CONV_FUNC
    def conv_func(n_messages, messages, p_response, app_data):
        """Simple conversation function that responds to any
        prompt where the echo is off with the supplied password"""
        # Create an array of n_messages response objects
        addr = CALLOC(n_messages, sizeof(PamResponse))
        p_response[0] = cast(addr, POINTER(PamResponse))
        for i in range(n_messages):
            if messages[i].contents.msg_style == PAM_PROMPT_ECHO_OFF:
                if not passwords:
                    return 1
                pw_copy = STRDUP(passwords.pop())
                p_response.contents[i].resp = pw_copy
                p_response.contents[i].resp_retcode = 0
        return 0

    return conv_func


def pam_start(service, username, conv_func=default_conv, encoding='utf8'):
    service = _cast_bytes(service, encoding)
    username = _cast_bytes(username, encoding)

    handle = PamHandle()
    conv = pointer(PamConv(conv_func, 0))
    retval = PAM_START(service, username, conv, pointer(handle))

    if retval != 0:
        PAM_END(handle, retval)
        raise PAMError(errno=retval)

    return handle


def pam_end(handle, retval=0):
    e = PAM_END(handle, retval)
    if retval == 0 and e == 0:
        return
    if retval == 0:
        retval = e
    raise PAMError(errno=retval)


def authenticate(
    username,
    password=None,
    service='login',
    encoding='utf-8',
    resetcred=PAM_REINITIALIZE_CRED,
    close=True,
    check=True,
):
    """Returns None if the given username and password authenticate for the
    given service.  Raises PAMError otherwise

    ``username``: the username to authenticate

    ``password``: the password in plain text. It can also be an iterable of
                  passwords when using multifactor authentication.
                  Defaults to None to use PAM's conversation interface

    ``service``: the PAM service to authenticate against.
                 Defaults to 'login'

    The above parameters can be strings or bytes.  If they are strings,
    they will be encoded using the encoding given by:

    ``encoding``: the encoding to use for the above parameters if they
                  are given as strings.  Defaults to 'utf-8'

    ``resetcred``: Use the pam_setcred() function to
                   reinitialize the credentials.
                   Defaults to 'PAM_REINITIALIZE_CRED'.

    ``check``: If True (default) Check that account is valid with PAM_ACCT_MGMT.
               added in 1.2.

    ``close``: If True (default) the transaction will be closed after
                   authentication; if False the (open) PamHandle instance
                   will be returned.
    """

    if password is None:
        conv_func = default_conv
    else:
        if isinstance(password, str):
            password = (password,)
        conv_func = new_simple_password_conv(password, encoding)

    handle = pam_start(service, username, conv_func=conv_func, encoding=encoding)

    retval = PAM_AUTHENTICATE(handle, 0)
    if retval == 0 and check:
        retval = PAM_ACCT_MGMT(handle, 0)

    # Re-initialize credentials (for Kerberos users, etc)
    # Don't check return code of pam_setcred(), it shouldn't matter
    # if this fails
    if retval == 0 and resetcred:
        PAM_SETCRED(handle, resetcred)

    if close:
        return pam_end(handle, retval)
    elif retval != 0:
        raise PAMError(errno=retval)
    else:
        return handle


def open_session(username, service='login', encoding='utf-8'):
    handle = pam_start(service, username, encoding=encoding)
    return pam_end(handle, PAM_OPEN_SESSION(handle, 0))


def close_session(username, service='login', encoding='utf-8'):
    handle = pam_start(service, username, encoding=encoding)
    return pam_end(handle, PAM_CLOSE_SESSION(handle, 0))


def check_account(username, service='login', encoding='utf-8'):
    handle = pam_start(service, username, encoding=encoding)
    return pam_end(handle, PAM_ACCT_MGMT(handle, 0))


def change_password(username, password=None, service='login', encoding='utf-8'):
    if password is None:
        conv_func = default_conv
    else:
        # Password x2 to answer the "Retype new UNIX password:" prompt
        # TODO: If we're not running as root the first prompt will be
        # 'current password' which we will not answer, so this will not work
        # in that case.
        conv_func = new_simple_password_conv((password, password), encoding)
    handle = pam_start(service, username, conv_func=conv_func, encoding=encoding)
    return pam_end(handle, PAM_CHAUTHTOK(handle, 0))


if __name__ == "__main__":
    import optparse

    usage = "usage: %prog [options] [username]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option(
        '-a',
        '--authenticate',
        dest='authenticate',
        action='store_true',
        help='authenticate user',
    )
    parser.add_option(
        '-o',
        '--open-session',
        dest='open_session',
        action='store_true',
        help='open session',
    )
    parser.add_option(
        '-c',
        '--close-session',
        dest='close_session',
        action='store_true',
        help='close session',
    )
    parser.add_option(
        '-v',
        '--validate-account',
        dest='validate_account',
        action='store_true',
        help='check account validity',
    )
    parser.add_option(
        '-p',
        '--change-password',
        dest='change_password',
        action='store_true',
        help='change password',
    )
    parser.add_option(
        '-s',
        '--service',
        dest='service',
        action='store',
        default='login',
        help='PAM service to use [default: %default]',
    )
    parser.add_option(
        '-P',
        '--ask-password',
        dest='ask_password',
        action='store_true',
        help="own password prompt instead of PAM's",
    )

    (o, a) = parser.parse_args()

    if not (
        o.authenticate
        or o.open_session
        or o.close_session
        or o.validate_account
        or o.change_password
    ):
        parser.error("One of -a, -o, -c, -v or -p is mandatory")

    try:
        user = a[0]
    except IndexError:
        user = getpass.getuser()

    if o.authenticate:
        if o.ask_password:
            password = getpass.getpass()
        else:
            password = None

        try:
            authenticate(user, password, o.service)
        except PAMError as e:
            sys.exit(e)

    if o.open_session:
        try:
            open_session(user, o.service)
        except PAMError as e:
            sys.exit(e)

    if o.close_session:
        try:
            close_session(user, o.service)
        except PAMError as e:
            sys.exit(e)

    if o.validate_account:
        try:
            check_account(user, o.service)
        except PAMError as e:
            sys.exit(e)

    if o.change_password:
        if o.ask_password:
            password = getpass.getpass()
        else:
            password = None

        try:
            change_password(user, password, o.service)
        except PAMError as e:
            sys.exit(e)
