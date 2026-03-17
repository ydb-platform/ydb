# Copyright 2010-2025 The pygit2 contributors
#
# This file is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2,
# as published by the Free Software Foundation.
#
# In addition to the permissions in the GNU General Public License,
# the authors give you unlimited permission to link the compiled
# version of this file into combinations with other programs,
# and to distribute those combinations without any restriction
# coming from the use of this file.  (The General Public License
# restrictions do apply in other respects; for example, they cover
# modification of the file, and distribution when not linked into
# a combined executable.)
#
# This file is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING.  If not, write to
# the Free Software Foundation, 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301, USA.

"""
In this module we keep everything concerning callback. This is how it works,
with an example:

1. The pygit2 API calls libgit2, it passes a payload object
   e.g. Remote.fetch calls git_remote_fetch

2. libgit2 calls Python callbacks
   e.g. git_remote_fetch calls _transfer_progress_cb

3. Optionally, the Python callback may proxy to a user defined function
   e.g. _transfer_progress_cb calls RemoteCallbacks.transfer_progress

4. The user defined function may return something on success, or raise an
   exception on error, or raise the special Passthrough exception.

5. The callback may return in 3 different ways to libgit2:

   - Returns GIT_OK on success.
   - Returns GIT_PASSTHROUGH if the user defined function raised Passthrough,
     this tells libgit2 to act as if this callback didn't exist in the first
     place.
   - Returns GIT_EUSER if another exception was raised, and keeps the exception
     in the payload to be re-raised later.

6. libgit2 returns to the pygit2 API, with an error code
   e.g. git_remote_fetch returns to Remote.fetch

7. The pygit2 API will:

   - Return something on success.
   - Raise the original exception if libgit2 returns GIT_EUSER
   - Raise another exception if libgit2 returns another error code

The payload object is passed all the way, so pygit2 API can send information to
the inner user defined function, and this can send back results to the pygit2
API.
"""

# Standard Library
from collections.abc import Callable, Generator
from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING, Optional, ParamSpec, TypeVar

# pygit2
from ._pygit2 import DiffFile, Oid
from .credentials import Keypair, Username, UserPass
from .enums import CheckoutNotify, CheckoutStrategy, CredentialType, StashApplyProgress
from .errors import Passthrough, check_error
from .ffi import C, ffi
from .utils import StrArray, maybe_string, ptr_to_bytes, to_bytes

_Credentials = Username | UserPass | Keypair

if TYPE_CHECKING:
    from pygit2._libgit2.ffi import GitProxyOptionsC

    from ._pygit2 import CloneOptions, PushOptions
    from .remotes import PushUpdate, TransferProgress
#
# The payload is the way to pass information from the pygit2 API, through
# libgit2, to the Python callbacks. And back.
#


class Payload:
    repository: Callable | None
    remote: Callable | None
    clone_options: 'CloneOptions'

    def __init__(self, **kw: object) -> None:
        for key, value in kw.items():
            setattr(self, key, value)
        self._stored_exception = None

    def check_error(self, error_code: int) -> None:
        if error_code == C.GIT_EUSER:
            assert self._stored_exception is not None
            raise self._stored_exception
        elif self._stored_exception is not None:
            # A callback mapped to a C function returning void
            # might still have raised an exception.
            raise self._stored_exception

        check_error(error_code)


class RemoteCallbacks(Payload):
    """Base class for pygit2 remote callbacks.

    Inherit from this class and override the callbacks which you want to use
    in your class, which you can then pass to the network operations.

    For the credentials, you can either subclass and override the 'credentials'
    method, or if it's a constant value, pass the value to the constructor,
    e.g. RemoteCallbacks(credentials=credentials).

    You can as well pass the certificate the same way, for example:
    RemoteCallbacks(certificate=certificate).
    """

    push_options: 'PushOptions'

    def __init__(
        self,
        credentials: _Credentials | None = None,
        certificate_check: Callable[[None, bool, bytes], bool] | None = None,
    ) -> None:
        super().__init__()
        if credentials is not None:
            self.credentials = credentials  # type: ignore[method-assign, assignment]
        if certificate_check is not None:
            self.certificate_check = certificate_check  # type: ignore[method-assign, assignment]

    def sideband_progress(self, string: str) -> None:
        """
        Progress output callback.  Override this function with your own
        progress reporting function

        Parameters:

        string : str
            Progress output from the remote.
        """

    def credentials(
        self,
        url: str,
        username_from_url: str | None,
        allowed_types: CredentialType,
    ) -> _Credentials:
        """
        Credentials callback.  If the remote server requires authentication,
        this function will be called and its return value used for
        authentication. Override it if you want to be able to perform
        authentication.

        Returns: credential

        Parameters:

        url : str
            The url of the remote.

        username_from_url : str or None
            Username extracted from the url, if any.

        allowed_types : CredentialType
            A combination of CredentialType bitflags representing the
            credential types supported by the remote.
        """
        raise Passthrough

    def certificate_check(self, certificate: None, valid: bool, host: bytes) -> bool:
        """
        Certificate callback. Override with your own function to determine
        whether to accept the server's certificate.

        Returns: True to connect, False to abort.

        Parameters:

        certificate : None
            The certificate. It is currently always None while we figure out
            how to represent it cross-platform.

        valid : bool
            Whether the TLS/SSH library thinks the certificate is valid.

        host : str
            The hostname we want to connect to.
        """

        raise Passthrough

    def push_negotiation(self, updates: list['PushUpdate']) -> None:
        """
        During a push, called once between the negotiation step and the upload.
        Provides information about what updates will be performed.

        Override with your own function to check the pending updates
        and possibly reject them (by raising an exception).
        """

    def transfer_progress(self, stats: 'TransferProgress') -> None:
        """
        During the download of new data, this will be regularly called with
        the indexer's progress.

        Override with your own function to report transfer progress.

        Parameters:

        stats : TransferProgress
            The progress up to now.
        """

    def push_transfer_progress(
        self, objects_pushed: int, total_objects: int, bytes_pushed: int
    ) -> None:
        """
        During the upload portion of a push, this will be regularly called
        with progress information.

        Be aware that this is called inline with pack building operations,
        so performance may be affected.

        Override with your own function to report push transfer progress.
        """

    def update_tips(self, refname: str, old: Oid, new: Oid) -> None:
        """
        Update tips callback. Override with your own function to report
        reference updates.

        Parameters:

        refname : str
            The name of the reference that's being updated.

        old : Oid
            The reference's old value.

        new : Oid
            The reference's new value.
        """

    def push_update_reference(self, refname: str, message: str) -> None:
        """
        Push update reference callback. Override with your own function to
        report the remote's acceptance or rejection of reference updates.

        refname : str
            The name of the reference (on the remote).

        message : str
            Rejection message from the remote. If None, the update was accepted.
        """


class CheckoutCallbacks(Payload):
    """Base class for pygit2 checkout callbacks.

    Inherit from this class and override the callbacks that you want to use
    in your class, which you can then pass to checkout operations.
    """

    def __init__(self) -> None:
        super().__init__()

    def checkout_notify_flags(self) -> CheckoutNotify:
        """
        Returns a bit mask of the notifications to receive from a checkout
        (a combination of enums.CheckoutNotify constants).

        By default, if you override `checkout_notify`, all notifications will
        be enabled. You can fine tune the notification types to enable by
        overriding `checkout_notify_flags`.

        Please note that the flags are only sampled once when checkout begins.
        You cannot change the flags while a checkout is in progress.
        """
        if type(self).checkout_notify == CheckoutCallbacks.checkout_notify:
            # If the user hasn't overridden the notify function,
            # filter out all notifications.
            return CheckoutNotify.NONE
        else:
            # If the user provides their own notify function,
            # enable all notifications by default.
            return CheckoutNotify.ALL

    def checkout_notify(
        self,
        why: CheckoutNotify,
        path: str,
        baseline: Optional[DiffFile],
        target: Optional[DiffFile],
        workdir: Optional[DiffFile],
    ) -> None:
        """
        Checkout will invoke an optional notification callback for
        certain cases - you pick which ones via `checkout_notify_flags`.

        Raising an exception from this callback will cancel the checkout.
        The exception will be propagated back and raised by the
        Repository.checkout_... call.

        Notification callbacks are made prior to modifying any files on disk,
        so canceling on any notification will still happen prior to any files
        being modified.
        """
        pass

    def checkout_progress(
        self, path: str, completed_steps: int, total_steps: int
    ) -> None:
        """
        Optional callback to notify the consumer of checkout progress.
        """
        pass


class StashApplyCallbacks(CheckoutCallbacks):
    """Base class for pygit2 stash apply callbacks.

    Inherit from this class and override the callbacks that you want to use
    in your class, which you can then pass to stash apply or pop operations.
    """

    def stash_apply_progress(self, progress: StashApplyProgress) -> None:
        """
        Stash application progress notification function.

        `progress` is a StashApplyProgress constant.

        Raising an exception from this callback will abort the stash
        application.
        """
        pass


#
# The context managers below wrap the calls to libgit2 functions, which them in
# turn call to callbacks defined later in this module. These context managers
# are used in the pygit2 API, see for instance remote.py
#


@contextmanager
def git_clone_options(payload, opts=None):
    if opts is None:
        opts = ffi.new('git_clone_options *')
        C.git_clone_options_init(opts, C.GIT_CLONE_OPTIONS_VERSION)

    handle = ffi.new_handle(payload)

    # Plug callbacks
    if payload.repository:
        opts.repository_cb = C._repository_create_cb
        opts.repository_cb_payload = handle
    if payload.remote:
        opts.remote_cb = C._remote_create_cb
        opts.remote_cb_payload = handle

    # Give back control
    payload._stored_exception = None
    payload.clone_options = opts
    yield payload


@contextmanager
def git_fetch_options(payload, opts=None):
    if payload is None:
        payload = RemoteCallbacks()

    if opts is None:
        opts = ffi.new('git_fetch_options *')
        C.git_fetch_options_init(opts, C.GIT_FETCH_OPTIONS_VERSION)

    # Plug callbacks
    opts.callbacks.sideband_progress = C._sideband_progress_cb
    opts.callbacks.transfer_progress = C._transfer_progress_cb
    opts.callbacks.update_tips = C._update_tips_cb
    opts.callbacks.credentials = C._credentials_cb
    opts.callbacks.certificate_check = C._certificate_check_cb
    # Payload
    handle = ffi.new_handle(payload)
    opts.callbacks.payload = handle

    # Give back control
    payload.fetch_options = opts
    payload._stored_exception = None
    yield payload


@contextmanager
def git_proxy_options(
    payload: object,
    opts: Optional['GitProxyOptionsC'] = None,
    proxy: None | bool | str = None,
) -> Generator['GitProxyOptionsC', None, None]:
    if opts is None:
        opts = ffi.new('git_proxy_options *')
        C.git_proxy_options_init(opts, C.GIT_PROXY_OPTIONS_VERSION)
    if proxy is None:
        opts.type = C.GIT_PROXY_NONE
    elif proxy is True:
        opts.type = C.GIT_PROXY_AUTO
    elif type(proxy) is str:
        opts.type = C.GIT_PROXY_SPECIFIED
        # Keep url in memory, otherwise memory is freed and bad things happen
        payload.__proxy_url = ffi.new('char[]', to_bytes(proxy))  # type: ignore[attr-defined]
        opts.url = payload.__proxy_url  # type: ignore[attr-defined]
    else:
        raise TypeError('Proxy must be None, True, or a string')
    yield opts


@contextmanager
def git_push_options(payload, opts=None):
    if payload is None:
        payload = RemoteCallbacks()

    opts = ffi.new('git_push_options *')
    C.git_push_options_init(opts, C.GIT_PUSH_OPTIONS_VERSION)

    # Plug callbacks
    opts.callbacks.sideband_progress = C._sideband_progress_cb
    opts.callbacks.transfer_progress = C._transfer_progress_cb
    opts.callbacks.update_tips = C._update_tips_cb
    opts.callbacks.credentials = C._credentials_cb
    opts.callbacks.certificate_check = C._certificate_check_cb
    opts.callbacks.push_update_reference = C._push_update_reference_cb
    opts.callbacks.push_negotiation = C._push_negotiation_cb
    # Per libgit2 sources, push_transfer_progress may incur a performance hit.
    # So, set it only if the user has overridden the no-op stub.
    if (
        type(payload).push_transfer_progress
        is not RemoteCallbacks.push_transfer_progress
    ):
        opts.callbacks.push_transfer_progress = C._push_transfer_progress_cb
    # Payload
    handle = ffi.new_handle(payload)
    opts.callbacks.payload = handle

    # Give back control
    payload.push_options = opts
    payload._stored_exception = None
    yield payload


@contextmanager
def git_remote_callbacks(payload):
    if payload is None:
        payload = RemoteCallbacks()

    cdata = ffi.new('git_remote_callbacks *')
    C.git_remote_init_callbacks(cdata, C.GIT_REMOTE_CALLBACKS_VERSION)

    # Plug callbacks
    cdata.credentials = C._credentials_cb
    cdata.update_tips = C._update_tips_cb
    cdata.certificate_check = C._certificate_check_cb
    # Payload
    handle = ffi.new_handle(payload)
    cdata.payload = handle

    # Give back control
    payload._stored_exception = None
    payload.remote_callbacks = cdata
    yield payload


#
# C callbacks
#
# These functions are called by libgit2. They cannot raise exceptions, since
# they return to libgit2, they can only send back error codes.
#
# They cannot be overridden, but sometimes the only thing these functions do is
# to proxy the call to a user defined function. If user defined functions
# raises an exception, the callback must store it somewhere and return
# GIT_EUSER to libgit2, then the outer Python code will be able to reraise the
# exception.
#

P = ParamSpec('P')
T = TypeVar('T')


def libgit2_callback(f: Callable[P, T]) -> Callable[P, T]:
    @wraps(f)
    def wrapper(*args):
        data = ffi.from_handle(args[-1])
        args = args[:-1] + (data,)
        try:
            return f(*args)
        except Passthrough:
            # A user defined callback can raise Passthrough to decline to act;
            # then libgit2 will behave as if there was no callback set in the
            # first place.
            return C.GIT_PASSTHROUGH
        except BaseException as e:
            # Keep the exception to be re-raised later, and inform libgit2 that
            # the user defined callback has failed.
            data._stored_exception = e
            return C.GIT_EUSER

    return ffi.def_extern()(wrapper)  # type: ignore[attr-defined]


def libgit2_callback_void(f: Callable[P, T]) -> Callable[P, T]:
    @wraps(f)
    def wrapper(*args):
        data = ffi.from_handle(args[-1])
        args = args[:-1] + (data,)
        try:
            f(*args)
        except Passthrough:
            # A user defined callback can raise Passthrough to decline to act;
            # then libgit2 will behave as if there was no callback set in the
            # first place.
            pass  # Function returns void
        except BaseException as e:
            # Keep the exception to be re-raised later
            data._stored_exception = e
            pass  # Function returns void, so we can't do much here.

    return ffi.def_extern()(wrapper)  # type: ignore[attr-defined]


@libgit2_callback
def _certificate_check_cb(cert_i, valid, host, data):
    # We want to simulate what should happen if libgit2 supported pass-through
    # for this callback. For SSH, 'valid' is always False, because it doesn't
    # look at known_hosts, but we do want to let it through in order to do what
    # libgit2 would if the callback were not set.
    try:
        is_ssh = cert_i.cert_type == C.GIT_CERT_HOSTKEY_LIBSSH2

        # python's parsing is deep in the libraries and assumes an OpenSSL-owned cert
        val = data.certificate_check(None, bool(valid), ffi.string(host))
        if not val:
            return C.GIT_ECERTIFICATE
    except Passthrough:
        if is_ssh:
            return 0
        elif valid:
            return 0
        else:
            return C.GIT_ECERTIFICATE

    return 0


@libgit2_callback
def _credentials_cb(cred_out, url, username, allowed, data):
    credentials = getattr(data, 'credentials', None)
    if not credentials:
        return 0

    # convert int flags to enum before forwarding to user code
    allowed = CredentialType(allowed)

    ccred = get_credentials(credentials, url, username, allowed)
    cred_out[0] = ccred[0]
    return 0


@libgit2_callback
def _push_negotiation_cb(updates, num_updates, data):
    from .remotes import PushUpdate

    push_negotiation = getattr(data, 'push_negotiation', None)
    if not push_negotiation:
        return 0

    py_updates = [PushUpdate(updates[i]) for i in range(num_updates)]
    push_negotiation(py_updates)
    return 0


@libgit2_callback
def _push_update_reference_cb(ref, msg, data):
    push_update_reference = getattr(data, 'push_update_reference', None)
    if not push_update_reference:
        return 0

    refname = maybe_string(ref)
    message = maybe_string(msg)
    push_update_reference(refname, message)
    return 0


@libgit2_callback
def _remote_create_cb(remote_out, repo, name, url, data):
    from .repository import Repository

    remote = data.remote(
        Repository._from_c(repo, False), ffi.string(name), ffi.string(url)
    )
    remote_out[0] = remote._remote
    # we no longer own the C object
    remote._remote = ffi.NULL

    return 0


@libgit2_callback
def _repository_create_cb(repo_out, path, bare, data):
    repository = data.repository(ffi.string(path), bare != 0)
    # we no longer own the C object
    repository._disown()
    repo_out[0] = repository._repo

    return 0


@libgit2_callback
def _sideband_progress_cb(string, length, data):
    sideband_progress = getattr(data, 'sideband_progress', None)
    if not sideband_progress:
        return 0

    s = ffi.string(string, length).decode('utf-8')
    sideband_progress(s)
    return 0


@libgit2_callback
def _transfer_progress_cb(stats_ptr, data):
    from .remotes import TransferProgress

    transfer_progress = getattr(data, 'transfer_progress', None)
    if not transfer_progress:
        return 0

    transfer_progress(TransferProgress(stats_ptr))
    return 0


@libgit2_callback
def _push_transfer_progress_cb(current, total, bytes_pushed, payload):
    push_transfer_progress = getattr(payload, 'push_transfer_progress', None)
    if not push_transfer_progress:
        return 0

    push_transfer_progress(current, total, bytes_pushed)
    return 0


@libgit2_callback
def _update_tips_cb(refname, a, b, data):
    update_tips = getattr(data, 'update_tips', None)
    if not update_tips:
        return 0

    s = maybe_string(refname)
    a = Oid(raw=bytes(ffi.buffer(a)[:]))
    b = Oid(raw=bytes(ffi.buffer(b)[:]))
    update_tips(s, a, b)
    return 0


#
# Other functions, used above.
#


def get_credentials(fn, url, username, allowed):
    """Call fn and return the credentials object."""
    url_str = maybe_string(url)
    username_str = maybe_string(username)

    creds = fn(url_str, username_str, allowed)

    credential_type = getattr(creds, 'credential_type', None)
    credential_tuple = getattr(creds, 'credential_tuple', None)
    if not credential_type or not credential_tuple:
        raise TypeError('credential does not implement interface')

    cred_type = credential_type

    if not (allowed & cred_type):
        raise TypeError('invalid credential type')

    ccred = ffi.new('git_credential **')
    if cred_type == CredentialType.USERPASS_PLAINTEXT:
        name, passwd = credential_tuple
        err = C.git_credential_userpass_plaintext_new(
            ccred, to_bytes(name), to_bytes(passwd)
        )

    elif cred_type == CredentialType.SSH_KEY:
        name, pubkey, privkey, passphrase = credential_tuple
        name = to_bytes(name)
        if pubkey is None and privkey is None:
            err = C.git_credential_ssh_key_from_agent(ccred, name)
        else:
            err = C.git_credential_ssh_key_new(
                ccred, name, to_bytes(pubkey), to_bytes(privkey), to_bytes(passphrase)
            )

    elif cred_type == CredentialType.USERNAME:
        (name,) = credential_tuple
        err = C.git_credential_username_new(ccred, to_bytes(name))

    elif cred_type == CredentialType.SSH_MEMORY:
        name, pubkey, privkey, passphrase = credential_tuple
        if pubkey is None and privkey is None:
            raise TypeError('SSH keys from memory are empty')
        err = C.git_credential_ssh_key_memory_new(
            ccred,
            to_bytes(name),
            to_bytes(pubkey),
            to_bytes(privkey),
            to_bytes(passphrase),
        )
    else:
        raise TypeError('unsupported credential type')

    check_error(err)

    return ccred


#
# Checkout callbacks
#


@libgit2_callback
def _checkout_notify_cb(
    why, path_cstr, baseline, target, workdir, data: CheckoutCallbacks
):
    pypath = maybe_string(path_cstr)
    pybaseline = DiffFile.from_c(ptr_to_bytes(baseline))
    pytarget = DiffFile.from_c(ptr_to_bytes(target))
    pyworkdir = DiffFile.from_c(ptr_to_bytes(workdir))

    try:
        data.checkout_notify(why, pypath, pybaseline, pytarget, pyworkdir)  # type: ignore[arg-type]
    except Passthrough:
        # Unlike most other operations with optional callbacks, checkout
        # doesn't support the GIT_PASSTHROUGH return code, so we must bypass
        # libgit2_callback's error handling and return 0 explicitly here.
        pass

    # If the user's callback has raised any other exception type,
    # it's caught by the libgit2_callback decorator by now.
    # So, return success code to libgit2.
    return 0


@libgit2_callback_void
def _checkout_progress_cb(path, completed_steps, total_steps, data: CheckoutCallbacks):
    data.checkout_progress(maybe_string(path), completed_steps, total_steps)  # type: ignore[arg-type]


def _git_checkout_options(
    callbacks=None,
    strategy=None,
    directory=None,
    paths=None,
    c_checkout_options_ptr=None,
):
    if callbacks is None:
        payload = CheckoutCallbacks()
    else:
        payload = callbacks

    # Get handle to payload
    handle = ffi.new_handle(payload)

    # Create the options struct to pass
    if not c_checkout_options_ptr:
        opts = ffi.new('git_checkout_options *')
    else:
        opts = c_checkout_options_ptr
    check_error(C.git_checkout_options_init(opts, 1))

    # References we need to keep to strings and so forth
    refs = [handle]

    # pygit2's default is SAFE | RECREATE_MISSING
    if strategy is None:
        strategy = CheckoutStrategy.SAFE | CheckoutStrategy.RECREATE_MISSING
    opts.checkout_strategy = int(strategy)

    if directory:
        target_dir = ffi.new('char[]', to_bytes(directory))
        refs.append(target_dir)
        opts.target_directory = target_dir

    if paths:
        strarray = StrArray(paths)
        refs.append(strarray)
        opts.paths = strarray.ptr[0]

    # If we want to receive any notifications, set up notify_cb in the options
    notify_flags = payload.checkout_notify_flags()
    if notify_flags != CheckoutNotify.NONE:
        opts.notify_cb = C._checkout_notify_cb
        opts.notify_flags = int(notify_flags)
        opts.notify_payload = handle

    # Set up progress callback if the user has provided their own
    if type(payload).checkout_progress != CheckoutCallbacks.checkout_progress:
        opts.progress_cb = C._checkout_progress_cb
        opts.progress_payload = handle

    # Give back control
    payload.checkout_options = opts
    payload._ffi_handle = handle
    payload._refs = refs
    payload._stored_exception = None
    return payload


@contextmanager
def git_checkout_options(callbacks=None, strategy=None, directory=None, paths=None):
    yield _git_checkout_options(
        callbacks=callbacks, strategy=strategy, directory=directory, paths=paths
    )


#
# Stash callbacks
#


@libgit2_callback
def _stash_apply_progress_cb(progress: StashApplyProgress, data: StashApplyCallbacks):
    try:
        data.stash_apply_progress(progress)
    except Passthrough:
        # Unlike most other operations with optional callbacks, stash apply
        # doesn't support the GIT_PASSTHROUGH return code, so we must bypass
        # libgit2_callback's error handling and return 0 explicitly here.
        pass

    # If the user's callback has raised any other exception type,
    # it's caught by the libgit2_callback decorator by now.
    # So, return success code to libgit2.
    return 0


@contextmanager
def git_stash_apply_options(
    callbacks=None, reinstate_index=False, strategy=None, directory=None, paths=None
):
    if callbacks is None:
        callbacks = StashApplyCallbacks()

    # Set up stash options
    stash_apply_options = ffi.new('git_stash_apply_options *')
    check_error(C.git_stash_apply_options_init(stash_apply_options, 1))

    flags = reinstate_index * C.GIT_STASH_APPLY_REINSTATE_INDEX
    stash_apply_options.flags = flags

    # Now set up checkout options
    c_checkout_options_ptr = ffi.addressof(stash_apply_options.checkout_options)
    payload = _git_checkout_options(
        callbacks=callbacks,
        strategy=strategy,
        directory=directory,
        paths=paths,
        c_checkout_options_ptr=c_checkout_options_ptr,
    )
    assert payload == callbacks
    assert payload.checkout_options == c_checkout_options_ptr

    # Set up stash progress callback if the user has provided their own
    if type(callbacks).stash_apply_progress != StashApplyCallbacks.stash_apply_progress:
        stash_apply_options.progress_cb = C._stash_apply_progress_cb
        stash_apply_options.progress_payload = payload._ffi_handle

    # Give back control
    payload.stash_apply_options = stash_apply_options
    yield payload
