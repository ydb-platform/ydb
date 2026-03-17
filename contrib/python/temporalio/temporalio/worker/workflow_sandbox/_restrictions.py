"""Restrictions for workflow sandbox.

.. warning::
    This API for this module is considered unstable and may change in future.
"""

from __future__ import annotations

import dataclasses
import datetime
import functools
import inspect
import logging
import math
import operator
import random
import types
import warnings
from copy import copy, deepcopy
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    cast,
)

try:
    import pydantic
    import pydantic_core

    HAVE_PYDANTIC = True
except ImportError:
    HAVE_PYDANTIC = False

import temporalio.workflow

logger = logging.getLogger(__name__)

LOG_TRACE = False


def _trace(message: object, *args: object) -> None:
    if LOG_TRACE:
        logger.debug(message, *args)


class RestrictedWorkflowAccessError(temporalio.workflow.NondeterminismError):
    """Error that occurs when a workflow accesses something non-deterministic.

    Attributes:
        qualified_name: Fully qualified name of what was accessed.
    """

    def __init__(
        self, qualified_name: str, *, override_message: Optional[str] = None
    ) -> None:
        """Create restricted workflow access error."""
        super().__init__(
            override_message
            or RestrictedWorkflowAccessError.default_message(qualified_name)
        )
        self.qualified_name = qualified_name

    @staticmethod
    def default_message(qualified_name: str) -> str:
        """Get default message for restricted access."""
        return (
            f"Cannot access {qualified_name} from inside a workflow. "
            "If this is code from a module not used in a workflow or known to "
            "only be used deterministically from a workflow, mark the import "
            "as pass through."
        )


@dataclass(frozen=True)
class SandboxRestrictions:
    """Set of restrictions that can be applied to a sandbox."""

    passthrough_modules: Set[str]
    """
    Modules which pass through because we know they are side-effect free (or the
    side-effecting pieces are restricted). These modules will not be reloaded,
    but instead will just be forwarded from outside of the sandbox. Any module
    listed will apply to all children.
    """

    invalid_modules: SandboxMatcher
    """
    Modules which cannot even be imported. If possible, use
    :py:attr:`invalid_module_members` instead so modules that are unused by
    running code can still be imported for other non-running code. The check
    whether a module matches here is an access match using the fully qualified
    module name.
    """

    invalid_module_members: SandboxMatcher
    """
    Module members which cannot be accessed. This includes variables, functions,
    class methods (including __init__, etc). The check compares the against the
    fully qualified path to the item.
    """

    passthrough_all_modules: bool = False
    """
    Pass through all modules, do not sandbox any modules. This is the equivalent
    of setting :py:attr:`passthrough_modules` to a list of all modules imported
    by the workflow. This is unsafe. This means modules are never reloaded per
    workflow run which means workflow authors have to be careful that they don't
    import modules that do non-deterministic things. Note, just because a module
    is passed through from outside the sandbox doesn't mean runtime restrictions
    on invalid calls are not still applied.
    """

    disable_lazy_sys_module_passthrough: bool = False
    """
    By default when __contains__ or __getitem__ is called on sys.modules in the
    sandbox, if it is not present in the sandbox but is outside the sandbox and
    it is marked passthrough, it is lazily added to the sandboxed sys.modules
    and returned. This option disables that feature and forces all passthroughs
    to have been explicitly imported.
    """

    passthrough_modules_minimum: ClassVar[Set[str]]
    """Set of modules that must be passed through at the minimum."""

    passthrough_modules_with_temporal: ClassVar[Set[str]]
    """Minimum modules that must be passed through and the Temporal modules."""

    passthrough_modules_maximum: ClassVar[Set[str]]
    """
    All modules that can be passed through. This includes all standard library
    modules.
    """

    passthrough_modules_default: ClassVar[Set[str]]
    """Same as :py:attr:`passthrough_modules_maximum`."""

    invalid_module_members_default: ClassVar[SandboxMatcher]
    """
    Default set of module members Temporal suggests be restricted for
    non-determinism reasons.
    """

    default: ClassVar[SandboxRestrictions]
    """
    Combination of :py:attr:`passthrough_modules_default`,
    :py:attr:`invalid_module_members_default`, and no invalid modules."""

    def with_passthrough_modules(self, *modules: str) -> SandboxRestrictions:
        """Create a new restriction set with the given modules added to the
        :py:attr:`passthrough_modules` set.
        """
        return dataclasses.replace(
            self, passthrough_modules=self.passthrough_modules | set(modules)
        )

    def with_passthrough_all_modules(self) -> SandboxRestrictions:
        """Create a new restriction set with :py:attr:`passthrough_all_modules`
        as true.
        """
        return dataclasses.replace(self, passthrough_all_modules=True)


# We intentionally use specific fields instead of generic "matcher" callbacks
# for optimization reasons.
@dataclass(frozen=True)
class SandboxMatcher:
    """Matcher that is used to match modules and members during restriction and
    pass through checks.

    This class is intentionally immutable. Changes need to be made as new
    instances.
    """

    @staticmethod
    def nested_child(path: Sequence[str], child: SandboxMatcher) -> SandboxMatcher:
        """Create a matcher where the given child is put at the given path.

        Args:
            path: Path to the child.
            child: Child matcher to set

        Returns:
            Created matcher.
        """
        ret = child
        for key in reversed(path):
            ret = SandboxMatcher(children={key: ret})
        return ret

    access: Set[str] = frozenset()  # type: ignore
    """Immutable set of names to match access.
    
    This is often only used for pass through checks and not member restrictions.
    If this is used for member restrictions, even importing/accessing the value
    will fail as opposed to :py:attr:`use` which is for when it is used.

    An string containing a single asterisk can be used to match all.
    """

    use: Set[str] = frozenset()  # type: ignore
    """Immutable set of names to match use.
    
    This is best used for member restrictions on functions/classes because the
    restriction will not apply to referencing/importing the item, just when it
    is used.

    An string containing a single asterisk can be used to match all.
    """

    children: Mapping[str, SandboxMatcher] = dataclasses.field(default_factory=dict)
    """Immutable mapping of child matchers."""

    match_self: bool = False
    """If ``True``, this matcher matches."""

    only_runtime: bool = False
    """
    If ``True``, any matches on this matcher only apply at runtime, not import
    time.
    """

    leaf_message: Optional[str] = None
    """
    Override message to use in error/warning. Defaults to a common message.
    This is only applicable to leafs, so this must only be set when
    ``match_self`` is ``True`` and this matcher is on ``children`` of a parent.
    """

    leaf_warning: Optional[Type[Warning]] = None
    """
    If set, issues a warning instead of raising an error. This is only
    applicable to leafs, so this must only be set when ``match_self`` is
    ``True`` and this matcher is on ``children`` of a parent.
    """

    exclude: Set[str] = frozenset()  # type: ignore
    """Immutable set of names to exclude.
    
    These override anything that may have been matched elsewhere.
    """

    all: ClassVar[SandboxMatcher]
    """Shortcut for an always-matched matcher."""

    all_runtime: ClassVar[SandboxMatcher]
    """Shortcut for an always-runtime-matched matcher."""

    none: ClassVar[SandboxMatcher]
    """Shortcut for a never-matched matcher."""

    all_uses: ClassVar[SandboxMatcher]
    """Shortcut for a matcher that matches any :py:attr:`use`."""

    all_uses_runtime: ClassVar[SandboxMatcher]
    """Shortcut for a matcher that matches any :py:attr:`use` at runtime."""

    def __post_init__(self):
        """Post initialization validations."""
        if self.leaf_message and not self.match_self:
            raise ValueError("Cannot set leaf_message without match_self")
        if self.leaf_warning and not self.match_self:
            raise ValueError("Cannot set leaf_warning without match_self")

    def access_matcher(
        self, context: RestrictionContext, *child_path: str, include_use: bool = False
    ) -> Optional[SandboxMatcher]:
        """Perform a match check and return matcher.

        Args:
            context: Current restriction context.
            child_path: Full path to the child being accessed.
            include_use: Whether to include the :py:attr:`use` set in the check.

        Returns:
            The matcher if matched.
        """
        # We prefer to avoid recursion
        matcher = self
        for v in child_path:
            if v in matcher.exclude:
                return None
            # Does not match if this is runtime only and we're not runtime
            if not context.is_runtime and matcher.only_runtime:
                return None

            # Considered matched if self matches or access matches. Note, "use"
            # does not match by default because we allow it to be accessed but
            # not used.
            if matcher.match_self or v in matcher.access or "*" in matcher.access:
                return matcher
            if include_use and (v in matcher.use or "*" in matcher.use):
                return matcher
            child_matcher = matcher.children.get(v) or matcher.children.get("*")
            if not child_matcher:
                return None
            matcher = child_matcher
        if not context.is_runtime and matcher.only_runtime:
            return None
        if not matcher.match_self:
            return None
        return matcher

    def match_access(
        self, context: RestrictionContext, *child_path: str, include_use: bool = False
    ) -> bool:
        """Perform a match check.

        Args:
            context: Current restriction context.
            child_path: Full path to the child being accessed.
            include_use: Whether to include the :py:attr:`use` set in the check.

        Returns:
            ``True`` if matched.
        """
        return (
            self.access_matcher(context, *child_path, include_use=include_use)
            is not None
        )

    def child_matcher(self, *child_path: str) -> Optional[SandboxMatcher]:
        """Return a child matcher for the given path.

        Unlike :py:meth:`match_access`, this will match if in py:attr:`use` in
        addition to :py:attr:`access`. If in neither, it uses the child mapping.

        Args:
            child_path: Full path to the child to get matcher for.

        Returns:
            Matcher that can be used to check children.
        """
        # We prefer to avoid recursion
        matcher: Optional[SandboxMatcher] = self
        only_runtime = self.only_runtime
        for v in child_path:
            # Use all if it matches self, access, _or_ use. Use doesn't match
            # self but matches all children.
            assert matcher  # MyPy help
            if v in matcher.exclude:
                return None
            if (
                matcher.match_self
                or v in matcher.access
                or v in matcher.use
                or "*" in matcher.access
                or "*" in matcher.use
            ):
                if only_runtime:
                    return SandboxMatcher.all_runtime
                return SandboxMatcher.all
            matcher = matcher.children.get(v) or matcher.children.get("*")
            if not matcher:
                return None
            # If this matcher is runtime only, then we ensure all children are
            if matcher.only_runtime:
                only_runtime = True
        return matcher

    def __or__(self, other: SandboxMatcher) -> SandboxMatcher:
        """Combine this matcher with another."""
        if self.only_runtime != other.only_runtime:
            raise ValueError("Cannot combine only-runtime and non-only-runtime")
        if self.leaf_message != other.leaf_message:
            raise ValueError("Cannot combine different messages")
        if self.leaf_warning != other.leaf_warning:
            raise ValueError("Cannot combine different warning values")
        if self.match_self or other.match_self:
            return SandboxMatcher.all
        new_children = dict(self.children) if self.children else {}
        if other.children:
            for other_k, other_v in other.children.items():
                if other_k in new_children:
                    new_children[other_k] = new_children[other_k] | other_v
                else:
                    new_children[other_k] = other_v
        return SandboxMatcher(
            access=self.access | other.access,
            use=self.use | other.use,
            children=new_children,
            only_runtime=self.only_runtime,
            leaf_message=self.leaf_message,
            leaf_warning=self.leaf_warning,
        )

    def with_child_unrestricted(self, *child_path: str) -> SandboxMatcher:
        """Create new matcher with the given child path removed.

        Traverses the child mapping, if present, until on leaf of path where it
        is removed from :py:attr:`access`, :py:attr:`use`, and
        :py:attr:`children`.

        Args:
            child_path: Full path to the child to remove restrictions for.

        Returns:
            Copied matcher from this matcher without this child restricted.
        """
        assert child_path
        # If there's only one item in path, make sure not in access, use, or
        # children. Otherwise, just remove from child.
        to_replace: Dict[str, Any] = {}
        if len(child_path) == 1:
            if child_path[0] in self.access:
                to_replace["access"] = set(self.access)
                to_replace["access"].remove(child_path[0])
            if child_path[0] in self.use:
                to_replace["use"] = set(self.use)
                to_replace["use"].remove(child_path[0])
            if child_path[0] in self.children:
                to_replace["children"] = dict(self.children)
                del to_replace["children"][child_path[0]]
        elif child_path[0] in self.children:
            to_replace["children"] = dict(self.children)
            to_replace["children"][child_path[0]] = self.children[
                child_path[0]
            ].with_child_unrestricted(*child_path[1:])
        # Replace if needed
        if not to_replace:
            return self
        return dataclasses.replace(self, **to_replace)


SandboxMatcher.all = SandboxMatcher(match_self=True)
SandboxMatcher.all_runtime = SandboxMatcher(match_self=True, only_runtime=True)
SandboxMatcher.none = SandboxMatcher()
SandboxMatcher.all_uses = SandboxMatcher(use={"*"})
SandboxMatcher.all_uses_runtime = SandboxMatcher(use={"*"}, only_runtime=True)

SandboxRestrictions.passthrough_modules_minimum = {
    "grpc",
    # Due to some side-effecting calls made on import, these need to be
    # allowed
    "pathlib",
    "importlib",
    # Python 3.7 libs often import this to support things in older Python.
    # This does not work natively due to an issue with extending
    # zipfile.ZipFile. TODO(cretz): Fix when subclassing restricted classes
    # is fixed.
    "importlib_metadata",
    # Platform is needed because its initialization makes restricted calls and
    # we don't want to disallow them
    "platform",
    # Due to a metaclass conflict in sandbox, we need zipfile module to pass
    # through always
    "zipfile",
    # Very general modules needed by many things including pytest's
    # assertion rewriter
    "typing",
    # Required for Pydantic TypedDict fields.
    "typing_extensions",
    # Required due to https://github.com/protocolbuffers/protobuf/issues/10143
    # for older versions. This unfortunately means that on those versions,
    # everyone using Python protos has to pass their module through.
    "google.protobuf",
    "temporalio.api",
    "temporalio.bridge.proto",
    # Must pass through the entire bridge in even the most minimum causes
    # because PyO3 does not allow re-init since 0.17. See
    # https://github.com/PyO3/pyo3/pull/2523.
    "temporalio.bridge.temporal_sdk_bridge",
}

SandboxRestrictions.passthrough_modules_with_temporal = (
    SandboxRestrictions.passthrough_modules_minimum
    | {
        # is_subclass is broken in sandbox due to Python bug on ABC C extension.
        # So we have to include all things that might extend an ABC class and
        # do a subclass check. See https://bugs.python.org/issue44847 and
        # https://wrapt.readthedocs.io/en/latest/issues.html#using-issubclass-on-abstract-classes
        "asyncio",
        "abc",
        "nexusrpc",
        "temporalio",
        # Due to pkg_resources use of base classes caused by the ABC issue
        # above, and Otel's use of pkg_resources, we pass it through
        "pkg_resources",
        # Due to how Pydantic is importing lazily inside of some classes, we choose
        # to always pass it through
        "pydantic",
        # OpenAI and OpenAI agent modules in workflows we always want to pass
        # through and reference the out-of-sandbox forms
        "openai",
        "agents",
    }
)

# sys.stdlib_module_names is only available on 3.10+, so we hardcode here. A
# test will fail if this list doesn't match the latest Python version it was
# generated against, spitting out the expected list. This is a string instead
# of a list of strings due to black wanting to format this to one item each
# line in a list.
_stdlib_module_names = (
    "__future__,_abc,_aix_support,_ast,_asyncio,_bisect,_blake2,_bootsubprocess,_bz2,_codecs,"
    "_codecs_cn,_codecs_hk,_codecs_iso2022,_codecs_jp,_codecs_kr,_codecs_tw,_collections,"
    "_collections_abc,_compat_pickle,_compression,_contextvars,_crypt,_csv,_ctypes,_curses,"
    "_curses_panel,_datetime,_dbm,_decimal,_elementtree,_frozen_importlib,_frozen_importlib_external,"
    "_functools,_gdbm,_hashlib,_heapq,_imp,_io,_json,_locale,_lsprof,_lzma,_markupbase,"
    "_md5,_msi,_multibytecodec,_multiprocessing,_opcode,_operator,_osx_support,_overlapped,"
    "_pickle,_posixshmem,_posixsubprocess,_py_abc,_pydecimal,_pyio,_queue,_random,_scproxy,"
    "_sha1,_sha256,_sha3,_sha512,_signal,_sitebuiltins,_socket,_sqlite3,_sre,_ssl,_stat,"
    "_statistics,_string,_strptime,_struct,_symtable,_thread,_threading_local,_tkinter,"
    "_tokenize,_tracemalloc,_typing,_uuid,_warnings,_weakref,_weakrefset,_winapi,_zoneinfo,"
    "abc,aifc,antigravity,argparse,array,ast,asynchat,asyncio,asyncore,atexit,audioop,"
    "base64,bdb,binascii,bisect,builtins,bz2,cProfile,calendar,cgi,cgitb,chunk,cmath,cmd,"
    "code,codecs,codeop,collections,colorsys,compileall,concurrent,configparser,contextlib,"
    "contextvars,copy,copyreg,crypt,csv,ctypes,curses,dataclasses,datetime,dbm,decimal,"
    "difflib,dis,distutils,doctest,email,encodings,ensurepip,enum,errno,faulthandler,fcntl,"
    "filecmp,fileinput,fnmatch,fractions,ftplib,functools,gc,genericpath,getopt,getpass,"
    "gettext,glob,graphlib,grp,gzip,hashlib,heapq,hmac,html,http,idlelib,imaplib,imghdr,"
    "imp,importlib,inspect,io,ipaddress,itertools,json,keyword,lib2to3,linecache,locale,"
    "logging,lzma,mailbox,mailcap,marshal,math,mimetypes,mmap,modulefinder,msilib,msvcrt,"
    "multiprocessing,netrc,nis,nntplib,nt,ntpath,nturl2path,numbers,opcode,operator,optparse,"
    "os,ossaudiodev,pathlib,pdb,pickle,pickletools,pipes,pkgutil,platform,plistlib,poplib,"
    "posix,posixpath,pprint,profile,pstats,pty,pwd,py_compile,pyclbr,pydoc,pydoc_data,"
    "pyexpat,queue,quopri,random,re,readline,reprlib,resource,rlcompleter,runpy,sched,"
    "secrets,select,selectors,shelve,shlex,shutil,signal,site,smtpd,smtplib,sndhdr,socket,"
    "socketserver,spwd,sqlite3,sre_compile,sre_constants,sre_parse,ssl,stat,statistics,"
    "string,stringprep,struct,subprocess,sunau,symtable,sys,sysconfig,syslog,tabnanny,"
    "tarfile,telnetlib,tempfile,termios,textwrap,this,threading,time,timeit,tkinter,token,"
    "tokenize,tomllib,trace,traceback,tracemalloc,tty,turtle,turtledemo,types,typing,unicodedata,"
    "unittest,urllib,uu,uuid,venv,warnings,wave,weakref,webbrowser,winreg,winsound,wsgiref,"
    "xdrlib,xml,xmlrpc,zipapp,zipfile,zipimport,zlib,zoneinfo"
)

SandboxRestrictions.passthrough_modules_maximum = (
    SandboxRestrictions.passthrough_modules_with_temporal
    | {
        # All stdlib modules except "sys" and their children. Children are not
        # listed in stdlib names but we need them because in some cases (e.g. os
        # manually setting sys.modules["os.path"]) they have certain child
        # expectations.
        v
        for v in _stdlib_module_names.split(",")
        if v != "sys"
    }
)

SandboxRestrictions.passthrough_modules_default = (
    SandboxRestrictions.passthrough_modules_maximum
)


def _public_callables(parent: Any, *, exclude: Set[str] = set()) -> Set[str]:
    ret: Set[str] = set()
    for name, member in inspect.getmembers(parent):
        # Name must be public and callable and not in exclude and not a class
        if (
            not name.startswith("_")
            and name not in exclude
            and callable(member)
            and not inspect.isclass(member)
        ):
            ret.add(name)
    return ret


SandboxRestrictions.invalid_module_members_default = SandboxMatcher(
    children={
        "__builtins__": SandboxMatcher(
            use={
                "breakpoint",
                "input",
                "open",
            },
            # Too many things use open() at import time, e.g. pytest's assertion
            # rewriter
            only_runtime=True,
        ),
        "asyncio": SandboxMatcher(
            children={
                "as_completed": SandboxMatcher(
                    children={
                        "__call__": SandboxMatcher(
                            match_self=True,
                            leaf_warning=UserWarning,
                            leaf_message="asyncio.as_completed() is non-deterministic, use workflow.as_completed() instead",
                        )
                    },
                ),
                "wait": SandboxMatcher(
                    children={
                        "__call__": SandboxMatcher(
                            match_self=True,
                            leaf_warning=UserWarning,
                            leaf_message="asyncio.wait() is non-deterministic, use workflow.wait() instead",
                        )
                    },
                ),
            }
        ),
        # TODO(cretz): Fix issues with class extensions on restricted proxy
        # "argparse": SandboxMatcher.all_uses_runtime,
        "bz2": SandboxMatcher(use={"open"}),
        "concurrent": SandboxMatcher(
            children={"futures": SandboxMatcher.all_uses_runtime}
        ),
        # Python's own re lib registers itself. This is mostly ok to not
        # restrict since it's just global picklers that people may want to
        # register globally.
        # "copyreg": SandboxMatcher.all_uses,
        "curses": SandboxMatcher.all_uses,
        "datetime": SandboxMatcher(
            children={
                "date": SandboxMatcher(use={"today"}),
                "datetime": SandboxMatcher(use={"now", "today", "utcnow"}),
            }
        ),
        "dbm": SandboxMatcher.all_uses,
        "filecmp": SandboxMatcher.all_uses,
        "fileinput": SandboxMatcher.all_uses,
        "ftplib": SandboxMatcher.all_uses,
        "getopt": SandboxMatcher.all_uses,
        "getpass": SandboxMatcher.all_uses,
        "gettext": SandboxMatcher.all_uses,
        "glob": SandboxMatcher.all_uses,
        "gzip": SandboxMatcher(use={"open"}),
        "http": SandboxMatcher(
            children={
                "client": SandboxMatcher.all_uses,
                "server": SandboxMatcher.all_uses,
            },
        ),
        "imaplib": SandboxMatcher.all_uses,
        # TODO(cretz): When restricting io, getting:
        #   AttributeError: module 'io' has no attribute 'BufferedIOBase"
        # From minimal imports in shutil/compression libs
        # "io": SandboxMatcher(use={"open", "open_code"}, only_runtime=True),
        "locale": SandboxMatcher.all_uses,
        "lzma": SandboxMatcher(use={"open"}),
        "marshal": SandboxMatcher(use={"dump", "load"}),
        "mmap": SandboxMatcher.all_uses_runtime,
        "multiprocessing": SandboxMatcher.all_uses_runtime,
        # We cannot restrict linecache because some packages like attrs' attr
        # use it during dynamic code generation
        # "linecache": SandboxMatcher.all_uses,
        # Restrict almost everything in OS at runtime
        "os": SandboxMatcher(
            access={"name"},
            use={"*"},
            # As of https://github.com/python/cpython/pull/112097, os.stat
            # calls are now made when displaying errors
            exclude={"stat"},
            # Only restricted at runtime
            only_runtime=True,
        ),
        "pathlib": SandboxMatcher(
            children={
                # We allow instantiation and all PurePath calls on Path, so we
                # have to list what we don't like explicitly here
                "Path": SandboxMatcher(
                    use={
                        "chmod",
                        "cwd",
                        "exists",
                        "expanduser",
                        "glob",
                        "group",
                        "hardlink_to",
                        "home",
                        "is_block_device",
                        "is_char_device",
                        "is_dir",
                        "is_fifo",
                        "is_file",
                        "is_mount",
                        "is_socket",
                        "is_symlink",
                        "iterdir",
                        "lchmod",
                        "link_to",
                        "lstat",
                        "mkdir",
                        "open",
                        "owner",
                        "read_bytes",
                        "read_link",
                        "read_text",
                        "rename",
                        "replace",
                        "resolve",
                        "rglob",
                        "rmdir",
                        "samefile",
                        "stat",
                        "symlink_to",
                        "touch",
                        "unlink",
                        "write_bytes",
                        "write_text",
                    }
                )
            }
        ),
        "platform": SandboxMatcher.all_uses_runtime,
        "poplib": SandboxMatcher.all_uses,
        # Everything but instantiating Random and SecureRandom
        "random": SandboxMatcher(
            use=_public_callables(random, exclude={"Random", "SecureRandom"}),
        ),
        "readline": SandboxMatcher.all_uses,
        "sched": SandboxMatcher.all_uses,
        # Only time-safe comparison remains after these restrictions
        "secrets": SandboxMatcher(
            use={
                "choice",
                "randbelow",
                "randbits",
                "SystemRandom",
                "token_bytes",
                "token_hex",
                "token_urlsafe",
            }
        ),
        "select": SandboxMatcher.all_uses_runtime,
        "selectors": SandboxMatcher.all_uses_runtime,
        "shelve": SandboxMatcher.all_uses,
        "shutil": SandboxMatcher.all_uses,
        "signal": SandboxMatcher.all_uses_runtime,
        "smtplib": SandboxMatcher.all_uses,
        "socket": SandboxMatcher.all_uses_runtime,
        "socketserver": SandboxMatcher.all_uses,
        "subprocess": SandboxMatcher.all_uses,
        # TODO(cretz): Can't currently restrict anything on sys because of
        # type(sys) being used as types.ModuleType among other things
        # "sys": SandboxMatcher(access={"argv"}, only_runtime=True),
        "time": SandboxMatcher(
            # TODO(cretz): Some functions in this package are only
            # non-deterministic when they lack a param (e.g. ctime)
            use={
                "pthread_getcpuclockid",
                "get_clock_info",
                "localtime",
                "monotonic",
                "monotonic_ns",
                "perf_counter",
                "perf_counter_ns" "process_time",
                "process_time_ns",
                "sleep",
                "time",
                "time_ns",
                "thread_time",
                "thread_time_ns",
                "tzset",
            },
            # We allow time calls at import time
            only_runtime=True,
        ),
        # There's a good use case for sqlite in memory, so we're not restricting
        # it in any way. Technically we could restrict some of the global
        # settings, but they are often import side effects usable in and out of
        # the sandbox.
        # "sqlite3": SandboxMatcher.all_uses,
        "tarfile": SandboxMatcher(
            use={"open"},
            children={"TarFile": SandboxMatcher(use={"extract", "extractall"})},
        ),
        "tempfile": SandboxMatcher.all_uses,
        "threading": SandboxMatcher.all_uses_runtime,
        "urllib": SandboxMatcher(
            children={"request": SandboxMatcher.all_uses},
        ),
        "uuid": SandboxMatcher(use={"uuid1", "uuid4"}, only_runtime=True),
        "webbrowser": SandboxMatcher.all_uses,
        "xmlrpc": SandboxMatcher.all_uses,
        "zipfile": SandboxMatcher(
            children={"ZipFile": SandboxMatcher(use={"extract", "extractall"})}
        ),
        "zoneinfo": SandboxMatcher(
            children={
                "ZoneInfo": SandboxMatcher(
                    use={"clear_cache", "from_file", "reset_tzpath"}
                )
            }
        ),
    }
)

SandboxRestrictions.default = SandboxRestrictions(
    passthrough_modules=SandboxRestrictions.passthrough_modules_default,
    invalid_modules=SandboxMatcher.none,
    invalid_module_members=SandboxRestrictions.invalid_module_members_default,
)


class RestrictionContext:
    """Context passed around restrictions.

    Attributes:
        is_runtime: ``True`` if we're currently running the workflow, ``False``
            if we're just importing it.
    """

    @staticmethod
    def unwrap_if_proxied(v: Any) -> Any:
        """Unwrap a proxy object if proxied."""
        if type(v) is _RestrictedProxy:
            v = _RestrictionState.from_proxy(v).obj
        return v

    def __init__(self) -> None:
        """Create a restriction context."""
        self.is_runtime = False


@dataclass
class _RestrictionState:
    @staticmethod
    def from_proxy(v: _RestrictedProxy) -> _RestrictionState:
        # To prevent recursion, must use __getattribute__ on object to get the
        # restriction state
        try:
            return object.__getattribute__(v, "__temporal_state")
        except AttributeError:
            # This mostly occurs when accessing a field of an extended class on
            # that has been proxied
            raise RuntimeError(
                "Restriction state not present. Using subclasses of proxied objects is unsupported."
            ) from None

    name: str
    obj: object
    context: RestrictionContext
    matcher: SandboxMatcher

    def assert_child_not_restricted(self, name: str) -> None:
        if temporalio.workflow.unsafe.is_sandbox_unrestricted():
            return
        matcher = self.matcher.access_matcher(self.context, name)
        if not matcher:
            return

        logger.warning("%s on %s restricted", name, self.name)
        # Issue warning instead of error if configured to do so
        if matcher.leaf_warning:
            warnings.warn(
                matcher.leaf_message
                or RestrictedWorkflowAccessError.default_message(f"{self.name}.{name}"),
                matcher.leaf_warning,
            )
        else:
            raise RestrictedWorkflowAccessError(
                f"{self.name}.{name}", override_message=matcher.leaf_message
            )

    def set_on_proxy(self, v: _RestrictedProxy) -> None:
        # To prevent recursion, must use __setattr__ on object to set the
        # restriction state
        object.__setattr__(v, "__temporal_state", self)


class _RestrictedProxyLookup:
    def __init__(
        self,
        access_func: Optional[Callable] = None,
        *,
        fallback_func: Optional[Callable] = None,
        class_value: Optional[Any] = None,
        is_attr: bool = False,
    ) -> None:
        bind_func: Optional[Callable[[_RestrictedProxy, Any], Callable]]
        if hasattr(access_func, "__get__"):
            # A Python function, can be turned into a bound method.

            def bind_func(instance: _RestrictedProxy, obj: Any) -> Callable:
                return access_func.__get__(obj, type(obj))  # type: ignore

        elif access_func is not None:
            # A C function, use partial to bind the first argument.

            def bind_func(instance: _RestrictedProxy, obj: Any) -> Callable:
                return functools.partial(access_func, obj)  # type: ignore

        else:
            # Use getattr, which will produce a bound method.
            bind_func = None

        self.bind_func = bind_func
        self.fallback_func = fallback_func
        self.class_value = class_value
        self.is_attr = is_attr

    def __set_name__(self, owner: _RestrictedProxy, name: str) -> None:
        self.name = name

    def __get__(self, instance: _RestrictedProxy, owner: Optional[Type] = None) -> Any:
        if instance is None:
            if self.class_value is not None:
                return self.class_value

            return self

        try:
            state: _RestrictionState = object.__getattribute__(
                instance, "__temporal_state"
            )
        except RuntimeError:
            if self.fallback_func is None:
                raise

            fallback = self.fallback_func.__get__(instance, owner)

            if self.is_attr:
                # __class__ and __doc__ are attributes, not methods.
                # Call the fallback to get the value.
                return fallback()

            return fallback

        if self.bind_func is not None:
            return self.bind_func(instance, state.obj)

        return getattr(state.obj, self.name)

    def __repr__(self) -> str:
        return f"proxy {self.name}"

    def __call__(self, instance: _RestrictedProxy, *args: Any, **kwargs: Any) -> Any:
        """Support calling unbound methods from the class. For example,
        this happens with ``copy.copy``, which does
        ``type(x).__copy__(x)``. ``type(x)`` can't be proxied, so it
        returns the proxy type and descriptor.
        """
        return self.__get__(instance, type(instance))(*args, **kwargs)


class _RestrictedProxyIOp(_RestrictedProxyLookup):
    __slots__ = ()

    def __init__(
        self,
        access_func: Optional[Callable] = None,
        *,
        fallback_func: Optional[Callable] = None,
    ) -> None:
        super().__init__(access_func, fallback_func=fallback_func)

        def bind_f(instance: _RestrictedProxy, obj: Any) -> Callable:
            def i_op(self: Any, other: Any) -> _RestrictedProxy:
                f(self, other)  # type: ignore
                return instance

            return i_op.__get__(obj, type(obj))  # type: ignore

        self.bind_f = bind_f


_OpF = TypeVar("_OpF", bound=Callable[..., Any])


def _l_to_r_op(op: _OpF) -> _OpF:
    """Swap the argument order to turn an l-op into an r-op."""

    def r_op(obj: Any, other: Any) -> Any:
        return op(other, obj)

    return cast(_OpF, r_op)


_do_not_restrict: Tuple[Type, ...] = (bool, int, float, complex, str, bytes, bytearray)
if HAVE_PYDANTIC:
    # The datetime validator in pydantic_core
    # https://github.com/pydantic/pydantic-core/blob/741961c05847d9e9ee517cd783e24c2b58e5596b/src/input/input_python.rs#L548-L582
    # does some runtime type inspection that a RestrictedProxy instance
    # fails. For this reason we do not restrict date/datetime instances when
    # Pydantic is being used. Other restricted types such as pathlib.Path
    # and uuid.UUID which are likely to be used in Pydantic model fields
    # currently pass Pydantic's validation when wrapped by RestrictedProxy.
    _do_not_restrict += (datetime.date,)  # e.g. datetime.datetime


def _is_restrictable(v: Any) -> bool:
    return v is not None and not isinstance(v, _do_not_restrict)


class _RestrictedProxy:
    def __init__(self, *args, **kwargs) -> None:
        # When we instantiate this class, we have the signature of:
        #   __init__(
        #       self,
        #       name: str,
        #       obj: Any,
        #       context: RestrictionContext,
        #       matcher: SandboxMatcher
        #   )
        # However when Python subclasses a class, it calls metaclass() on the
        # class object which doesn't match these args. For now, we'll just
        # ignore inits on these metadata classes.
        # TODO(cretz): Properly support subclassing restricted classes in
        # sandbox
        if isinstance(args[2], RestrictionContext):
            _trace("__init__ on %s", args[0])
            _RestrictionState(
                name=args[0], obj=args[1], context=args[2], matcher=args[3]
            ).set_on_proxy(self)
        else:
            _trace("__init__ unrecognized with args %s", args)

    def __getattribute__(self, __name: str) -> Any:
        if HAVE_PYDANTIC and __name == "__get_pydantic_core_schema__":
            return object.__getattribute__(self, "__get_pydantic_core_schema__")
        state = _RestrictionState.from_proxy(self)
        _trace("__getattribute__ %s on %s", __name, state.name)
        # We do not restrict __spec__ or __name__
        if __name != "__spec__" and __name != "__name__":
            state.assert_child_not_restricted(__name)
        ret = object.__getattribute__(self, "__getattr__")(__name)

        # Since Python 3.11, the importer references __spec__ on module, so we
        # allow that through.
        if __name != "__spec__":
            child_matcher = state.matcher.child_matcher(__name)
            if child_matcher and _is_restrictable(ret):
                ret = _RestrictedProxy(
                    f"{state.name}.{__name}", ret, state.context, child_matcher
                )
        return ret

    def __setattr__(self, __name: str, __value: Any) -> None:
        state = _RestrictionState.from_proxy(self)
        _trace("__setattr__ %s on %s", __name, state.name)
        state.assert_child_not_restricted(__name)
        setattr(state.obj, __name, __value)

    def __call__(self, *args, **kwargs) -> _RestrictedProxy:
        state = _RestrictionState.from_proxy(self)
        _trace("__call__ on %s", state.name)
        state.assert_child_not_restricted("__call__")
        ret = state.obj(*args, **kwargs)  # type: ignore
        # Always wrap the result of a call to self with the same restrictions
        # (this is often instantiating a class)
        if _is_restrictable(ret):
            ret = _RestrictedProxy(state.name, ret, state.context, state.matcher)
        return ret

    def __getitem__(self, key: Any) -> Any:
        state = _RestrictionState.from_proxy(self)
        if isinstance(key, str):
            state.assert_child_not_restricted(key)
        _trace("__getitem__ %s on %s", key, state.name)
        ret = operator.getitem(state.obj, key)  # type: ignore
        # If there is a child matcher, restrict if we can
        if isinstance(key, str):
            child_matcher = state.matcher.child_matcher(key)
            if child_matcher and _is_restrictable(ret):
                ret = _RestrictedProxy(
                    f"{state.name}.{key}", ret, state.context, child_matcher
                )
        return ret

    if HAVE_PYDANTIC:
        # Instruct pydantic to use the proxied type when determining the schema
        # https://docs.pydantic.dev/latest/concepts/types/#customizing-validation-with-__get_pydantic_core_schema__
        @classmethod
        def __get_pydantic_core_schema__(
            cls,
            source_type: Any,
            handler: pydantic.GetCoreSchemaHandler,
        ) -> pydantic_core.CoreSchema:
            return handler(RestrictionContext.unwrap_if_proxied(source_type))

    __doc__ = _RestrictedProxyLookup(  # type: ignore
        class_value=__doc__, fallback_func=lambda self: type(self).__doc__, is_attr=True
    )
    __wrapped__ = _RestrictedProxyLookup(
        fallback_func=lambda self: _RestrictionState.from_proxy(self).obj, is_attr=True
    )
    # __del__ should only delete the proxy
    __repr__ = _RestrictedProxyLookup(  # type: ignore
        repr, fallback_func=lambda self: f"<{type(self).__name__} unbound>"
    )
    __str__ = _RestrictedProxyLookup(str)  # type: ignore
    __bytes__ = _RestrictedProxyLookup(bytes)
    __format__ = _RestrictedProxyLookup(format)  # type: ignore
    __lt__ = _RestrictedProxyLookup(operator.lt)
    __le__ = _RestrictedProxyLookup(operator.le)
    __eq__ = _RestrictedProxyLookup(operator.eq)  # type: ignore
    __ne__ = _RestrictedProxyLookup(operator.ne)  # type: ignore
    __gt__ = _RestrictedProxyLookup(operator.gt)
    __ge__ = _RestrictedProxyLookup(operator.ge)
    __hash__ = _RestrictedProxyLookup(hash)  # type: ignore
    __bool__ = _RestrictedProxyLookup(bool, fallback_func=lambda self: False)
    __getattr__ = _RestrictedProxyLookup(getattr)
    # __setattr__ = _RestrictedProxyLookup(setattr)  # type: ignore
    __delattr__ = _RestrictedProxyLookup(delattr)  # type: ignore
    __dir__ = _RestrictedProxyLookup(dir, fallback_func=lambda self: [])  # type: ignore
    # __get__ (proxying descriptor not supported)
    # __set__ (descriptor)
    # __delete__ (descriptor)
    # __set_name__ (descriptor)
    # __objclass__ (descriptor)
    # __slots__ used by proxy itself
    # __dict__ (__getattr__)
    # __weakref__ (__getattr__)
    # __prepare__ (metaclass)
    __class__ = _RestrictedProxyLookup(  # type: ignore
        fallback_func=lambda self: type(self), is_attr=True
    )  # type: ignore
    __instancecheck__ = _RestrictedProxyLookup(
        lambda self, other: isinstance(other, self)
    )
    __subclasscheck__ = _RestrictedProxyLookup(
        lambda self, other: issubclass(other, self)
    )
    # __class_getitem__ triggered through __getitem__
    __len__ = _RestrictedProxyLookup(len)
    __length_hint__ = _RestrictedProxyLookup(operator.length_hint)
    __setitem__ = _RestrictedProxyLookup(operator.setitem)
    __delitem__ = _RestrictedProxyLookup(operator.delitem)
    # __missing__ triggered through __getitem__
    __iter__ = _RestrictedProxyLookup(iter)
    __next__ = _RestrictedProxyLookup(next)
    __reversed__ = _RestrictedProxyLookup(reversed)
    __contains__ = _RestrictedProxyLookup(operator.contains)
    __add__ = _RestrictedProxyLookup(operator.add)
    __sub__ = _RestrictedProxyLookup(operator.sub)
    __mul__ = _RestrictedProxyLookup(operator.mul)
    __matmul__ = _RestrictedProxyLookup(operator.matmul)
    __truediv__ = _RestrictedProxyLookup(operator.truediv)
    __floordiv__ = _RestrictedProxyLookup(operator.floordiv)
    __mod__ = _RestrictedProxyLookup(operator.mod)
    __divmod__ = _RestrictedProxyLookup(divmod)
    __pow__ = _RestrictedProxyLookup(pow)
    __lshift__ = _RestrictedProxyLookup(operator.lshift)
    __rshift__ = _RestrictedProxyLookup(operator.rshift)
    __and__ = _RestrictedProxyLookup(operator.and_)
    __xor__ = _RestrictedProxyLookup(operator.xor)
    __or__ = _RestrictedProxyLookup(operator.or_)
    __radd__ = _RestrictedProxyLookup(_l_to_r_op(operator.add))
    __rsub__ = _RestrictedProxyLookup(_l_to_r_op(operator.sub))
    __rmul__ = _RestrictedProxyLookup(_l_to_r_op(operator.mul))
    __rmatmul__ = _RestrictedProxyLookup(_l_to_r_op(operator.matmul))
    __rtruediv__ = _RestrictedProxyLookup(_l_to_r_op(operator.truediv))
    __rfloordiv__ = _RestrictedProxyLookup(_l_to_r_op(operator.floordiv))
    __rmod__ = _RestrictedProxyLookup(_l_to_r_op(operator.mod))
    __rdivmod__ = _RestrictedProxyLookup(_l_to_r_op(divmod))
    __rpow__ = _RestrictedProxyLookup(_l_to_r_op(pow))
    __rlshift__ = _RestrictedProxyLookup(_l_to_r_op(operator.lshift))
    __rrshift__ = _RestrictedProxyLookup(_l_to_r_op(operator.rshift))
    __rand__ = _RestrictedProxyLookup(_l_to_r_op(operator.and_))
    __rxor__ = _RestrictedProxyLookup(_l_to_r_op(operator.xor))
    __ror__ = _RestrictedProxyLookup(_l_to_r_op(operator.or_))
    __iadd__ = _RestrictedProxyIOp(operator.iadd)
    __isub__ = _RestrictedProxyIOp(operator.isub)
    __imul__ = _RestrictedProxyIOp(operator.imul)
    __imatmul__ = _RestrictedProxyIOp(operator.imatmul)
    __itruediv__ = _RestrictedProxyIOp(operator.itruediv)
    __ifloordiv__ = _RestrictedProxyIOp(operator.ifloordiv)
    __imod__ = _RestrictedProxyIOp(operator.imod)
    __ipow__ = _RestrictedProxyIOp(operator.ipow)
    __ilshift__ = _RestrictedProxyIOp(operator.ilshift)
    __irshift__ = _RestrictedProxyIOp(operator.irshift)
    __iand__ = _RestrictedProxyIOp(operator.iand)
    __ixor__ = _RestrictedProxyIOp(operator.ixor)
    __ior__ = _RestrictedProxyIOp(operator.ior)
    __neg__ = _RestrictedProxyLookup(operator.neg)
    __pos__ = _RestrictedProxyLookup(operator.pos)
    __abs__ = _RestrictedProxyLookup(abs)
    __invert__ = _RestrictedProxyLookup(operator.invert)
    __complex__ = _RestrictedProxyLookup(complex)
    __int__ = _RestrictedProxyLookup(int)
    __float__ = _RestrictedProxyLookup(float)
    __index__ = _RestrictedProxyLookup(operator.index)
    __round__ = _RestrictedProxyLookup(round)
    __trunc__ = _RestrictedProxyLookup(math.trunc)
    __floor__ = _RestrictedProxyLookup(math.floor)
    __ceil__ = _RestrictedProxyLookup(math.ceil)
    __enter__ = _RestrictedProxyLookup()
    __exit__ = _RestrictedProxyLookup()
    __await__ = _RestrictedProxyLookup()
    __aiter__ = _RestrictedProxyLookup()
    __anext__ = _RestrictedProxyLookup()
    __aenter__ = _RestrictedProxyLookup()
    __aexit__ = _RestrictedProxyLookup()
    __copy__ = _RestrictedProxyLookup(copy)
    __deepcopy__ = _RestrictedProxyLookup(deepcopy)


class RestrictedModule(_RestrictedProxy, types.ModuleType):  # type: ignore
    """Module that is restricted."""

    def __init__(
        self,
        mod: types.ModuleType,
        context: RestrictionContext,
        matcher: SandboxMatcher,
    ) -> None:
        """Create a restricted module."""
        _RestrictedProxy.__init__(self, mod.__name__, mod, context, matcher)
        types.ModuleType.__init__(self, mod.__name__, mod.__doc__)
