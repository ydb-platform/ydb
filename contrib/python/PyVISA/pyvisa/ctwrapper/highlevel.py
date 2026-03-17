# -*- coding: utf-8 -*-
"""Highlevel wrapper of the VISA Library.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import ctypes
import logging
from collections import OrderedDict
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    SupportsBytes,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pyvisa import constants, errors, highlevel, logger, typing

from ..util import (
    DebugInfo,
    LibraryPath,
    add_user_dll_extra_paths,
    read_user_library_path,
)
from . import functions, types
from .cthelper import Library, find_library

logger = logging.LoggerAdapter(logger, {"backend": "ivi"})  # type: ignore


T = TypeVar("T", bound=highlevel.VisaLibraryBase)


def add_visa_methods(aclass: Type[T]) -> Type[T]:
    """Add the methods to the library."""
    for method in functions.visa_functions:
        setattr(aclass, method, getattr(functions, method))
    return aclass


def _args_to_str(args: Tuple[Any, ...]) -> Tuple[str, ...]:
    """Convert function arguments to str."""
    out = []
    for arg in args:
        try:
            out.append(str(arg._obj))
        except AttributeError:
            out.append(arg)
    return tuple(out)


def unique(seq):
    """Keep unique while preserving order."""
    seen = set()
    return [x for x in seq if not (x in seen or seen.add(x))]


@add_visa_methods
class IVIVisaLibrary(highlevel.VisaLibraryBase):
    """High level IVI-VISA Library wrapper using ctypes.

    The easiest way to instantiate the library is to let `pyvisa` find the
    right one for you. This looks first in your configuration file (~/.pyvisarc).
    If it fails, it uses `ctypes.util.find_library` to try to locate a library
    in a way similar to what the compiler does:

       >>> visa_library = IVIVisaLibrary()

    But you can also specify the path:

        >>> visa_library = IVIVisaLibrary('/my/path/visa.so')

    """

    @staticmethod
    def get_library_paths() -> Tuple[LibraryPath, ...]:
        """Return a tuple of possible library paths."""
        # Add extra .dll dependency search paths before attempting to load libraries
        add_user_dll_extra_paths()

        # Try to find IVI libraries using known names.
        tmp: List[str | None]
        tmp = [
            find_library(library_path)
            for library_path in ("visa", "visa32", "visa32.dll", "visa64", "visa64.dll")
        ]

        logger.debug("Automatically found library files: %s" % tmp)

        # Prepend the path provided by the user in configuration files (if any).
        user_lib = read_user_library_path()
        if user_lib:
            tmp.insert(0, user_lib)

        # Deduplicate and convert string paths to LibraryPath objects
        return tuple(
            LibraryPath(library_path)
            for library_path in unique(tmp)
            if library_path is not None
        )

    @classmethod
    def get_debug_info(cls) -> DebugInfo:
        """Return a list of lines with backend info."""
        from pyvisa import __version__

        d: Dict[str, Any] = OrderedDict()
        d["Version"] = "%s (bundled with PyVISA)" % __version__

        paths = cls.get_library_paths()

        for ndx, visalib in enumerate(paths, 1):
            nfo: Dict[str, Union[str, List[str]]] = OrderedDict()
            nfo["found by"] = visalib.found_by
            nfo["architecture"] = [str(a.value) for a in visalib.arch]
            try:
                lib = cls(visalib)
                sess, _ = lib.open_default_resource_manager()
                nfo["Vendor"] = str(
                    lib.get_attribute(
                        sess, constants.ResourceAttribute.resource_manufacturer_name
                    )[0]
                )
                nfo["Impl. Version"] = str(
                    lib.get_attribute(
                        sess, constants.ResourceAttribute.resource_impl_version
                    )[0]
                )
                nfo["Spec. Version"] = str(
                    lib.get_attribute(
                        sess, constants.ResourceAttribute.resource_spec_version
                    )[0]
                )
                lib.close(sess)
            except Exception as e:
                err_string = str(e)
                if "No matching architecture" in err_string:
                    nfo["Could not get more info"] = (
                        "Interpreter and library have different bitness."
                    )
                else:
                    nfo["Could not get more info"] = err_string.split("\n")

            d["#%d: %s" % (ndx, visalib)] = nfo

        if not paths:
            d["Binary library"] = "Not found"

        return d

    def _init(self) -> None:
        try:
            lib = Library(self.library_path)
        except OSError as exc:
            raise errors.LibraryError.from_exception(exc, self.library_path)

        self.lib = lib
        self._async_read_jobs: List[Tuple[types.ViJobId, SupportsBytes]] = []

        # Set the argtypes, restype and errcheck for each function
        # of the visa library. Additionally store in `_functions` the
        # name of the functions.
        functions.set_signatures(self.lib, errcheck=self._return_handler)

        logger.debug(
            "Library signatures: %d ok, %d failed",
            len(getattr(self.lib, "_functions", [])),
            len(getattr(self.lib, "_functions_failed", [])),
        )

        # Set the library functions as attributes of the object.
        for method_name in getattr(self.lib, "_functions", []):
            setattr(self, method_name, getattr(self.lib, method_name))

    def _return_handler(self, ret_value: int, func: Callable, arguments: tuple) -> Any:
        """Check return values for errors and warnings."""
        logger.debug(
            "%s%s -> %r",
            func.__name__,
            _args_to_str(arguments),
            ret_value,
            extra=self._logging_extra,
        )

        # The first argument of almost all registered visa functions is a session.
        # We store the error code per session
        session = None
        if func.__name__ not in ("viFindNext",):
            try:
                session = arguments[0]
            except KeyError:
                raise Exception(
                    "Function %r does not seem to be a valid "
                    "visa function (len args %d)" % (func, len(arguments))
                )

            # Functions that use the first parameter to get a session value.
            if func.__name__ in ("viOpenDefaultRM",):
                session = session._obj.value

            if not isinstance(session, int):
                # Functions that might or might not have a session in the first argument.
                if func.__name__ not in (
                    "viClose",
                    "viGetAttribute",
                    "viSetAttribute",
                    "viStatusDesc",
                ):
                    raise Exception(
                        "Function %r does not seem to be a valid "
                        "visa function (type args[0] %r)" % (func, type(session))
                    )

                # Set session back to a safe value
                session = None

        return self.handle_return_value(session, ret_value)  # type: ignore

    def list_resources(
        self, session: typing.VISARMSession, query: str = "?*::INSTR"
    ) -> Tuple[str, ...]:
        r"""Returns a tuple of all connected devices matching query.

        Parameters
        ----------
        session : typing.VISARMSession
            Unused, present for consistency
        query : str
            Query using the VISA Resource Regular Expression syntax - which is
            not the same as the Python regular expression syntax. (see below)

        Notes
        -----

        The VISA Resource Regular Expression syntax is defined in the VISA
        Library specification:
        http://www.ivifoundation.org/docs/vpp43.pdf

        Symbol      Meaning
        ----------  ----------

        ?           Matches any one character.

        \           Makes the character that follows it an ordinary character
                    instead of special character. For example, when a question
                    mark follows a backslash (\?), it matches the ? character
                    instead of any one character.

        [list]      Matches any one character from the enclosed list. You can
                    use a hyphen to match a range of characters.

        [^list]     Matches any character not in the enclosed list. You can use
                    a hyphen to match a range of characters.

        *           Matches 0 or more occurrences of the preceding character or
                    expression.

        +           Matches 1 or more occurrences of the preceding character or
                    expression.

        Exp|exp     Matches either the preceding or following expression. The or
                    operator | matches the entire expression that precedes or
                    follows it and not just the character that precedes or follows
                    it. For example, VXI|GPIB means (VXI)|(GPIB), not VX(I|G)PIB.

        (exp)       Grouping characters or expressions.

        Thus the default query, '?*::INSTR', matches any sequences of characters
        ending ending with '::INSTR'.

        """
        resources: List[str] = []

        # Ignore some type checks because method are dynamically set
        try:
            (
                find_list,
                return_counter,
                instrument_description,
                _err,
            ) = self._find_resources(  # type: ignore
                session, query
            )
        except errors.VisaIOError as e:
            if e.error_code == constants.StatusCode.error_resource_not_found:
                return ()
            raise e

        try:
            resources.append(instrument_description)
            for i in range(return_counter - 1):
                resources.append(self._find_next(find_list)[0])  # type: ignore
        finally:
            # This is the only occurrence of a find list so ignore the typing error
            # since it would make things harder to follow to document ViFindList as
            # supported.
            self.close(find_list)  # type: ignore

        return tuple(resource for resource in resources)

    def read_asynchronously(
        self, session: typing.VISASession, count: int
    ) -> Tuple[SupportsBytes, types.ViJobId, constants.StatusCode]:
        """Read data from device or interface asynchronously.

        Corresponds to viReadAsync function of the VISA library. Since the
        asynchronous operation may complete before the function call return
        implementation should make sure that get_buffer_from_id will be able
        to return the proper buffer before this method returns.

        Parameters
        ----------
        session : typing.VISASession
            Unique logical identifier to a session.
        count : int
            Number of bytes to be read.

        Returns
        -------
        SupportsBytes
            Buffer in which the data will be written.
        types.ViJobId
            Id of the job.
        constants.StatusCode
            Return value of the library call.

        """
        # The buffer actually supports bytes but typing fails
        buffer = ctypes.create_string_buffer(count)
        job_id = types.ViJobId()
        self._async_read_jobs.append((job_id, buffer))  # type: ignore
        ret = self.lib.viReadAsync(session, buffer, count, ctypes.byref(job_id))
        return buffer, job_id.value, ret  # type: ignore

    def get_buffer_from_id(self, job_id: typing.VISAJobID) -> Optional[SupportsBytes]:
        """Retrieve the buffer associated with a job id created in read_asynchronously.

        Parameters
        ----------
        job_id : VISAJobID
            Id of the job for which to retrieve the buffer.

        Returns
        -------
        SupportsBytes
            Buffer in which the data are stored.

        """
        for jid, buffer in self._async_read_jobs:
            if job_id == jid.value:
                return buffer

        return None
