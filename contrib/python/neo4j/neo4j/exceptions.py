# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ruff: noqa: N818
# Not going to rename all Error classes that don't end on Error,
# which would break pretty much all users just to please the linter.


"""
Module containing the core driver exceptions.

Driver API Errors
=================
+ Neo4jError
  + ClientError
    + CypherSyntaxError
    + CypherTypeError
    + ConstraintError
    + AuthError
      + TokenExpired
    + Forbidden
  + DatabaseError
  + TransientError
    + DatabaseUnavailable
    + NotALeader
    + ForbiddenOnReadOnlyDatabase

+ DriverError
  + SessionError
  + TransactionError
    + TransactionNestingError
  + ResultError
    + ResultFailedError
    + ResultConsumedError
    + ResultNotSingleError
  + BrokenRecordError
  + SessionExpired
  + ServiceUnavailable
    + RoutingServiceUnavailable
    + WriteServiceUnavailable
    + ReadServiceUnavailable
    + IncompleteCommit
  + ConfigurationError
    + AuthConfigurationError
    + CertificateConfigurationError
    + UnsupportedServerProduct
  + ConnectionPoolError
    + ConnectionAcquisitionTimeoutError
"""

from __future__ import annotations as _

from copy import deepcopy as _deepcopy
from enum import Enum as _Enum

from . import _typing as _t


if _t.TYPE_CHECKING:
    from ._async.work import (
        AsyncManagedTransaction as _AsyncManagedTransaction,
        AsyncResult as _AsyncResult,
        AsyncSession as _AsyncSession,
        AsyncTransaction as _AsyncTransaction,
    )
    from ._sync.work import (
        ManagedTransaction as _ManagedTransaction,
        Result as _Result,
        Session as _Session,
        Transaction as _Transaction,
    )

    _TTransaction: _t.TypeAlias = (
        _AsyncManagedTransaction
        | _AsyncTransaction
        | _ManagedTransaction
        | _Transaction
    )
    _TResult: _t.TypeAlias = _AsyncResult | _Result
    _TSession: _t.TypeAlias = _AsyncSession | _Session
    _T = _t.TypeVar("_T")


__all__ = [
    "AuthConfigurationError",
    "AuthError",
    "BrokenRecordError",
    "CertificateConfigurationError",
    "ClientError",
    "ConfigurationError",
    "ConnectionAcquisitionTimeoutError",
    "ConnectionPoolError",
    "ConstraintError",
    "CypherSyntaxError",
    "CypherTypeError",
    "DatabaseError",
    "DatabaseUnavailable",
    "DriverError",
    "Forbidden",
    "ForbiddenOnReadOnlyDatabase",
    "GqlError",
    "GqlErrorClassification",
    "IncompleteCommit",
    "Neo4jError",
    "NotALeader",
    "ReadServiceUnavailable",
    "ResultConsumedError",
    "ResultError",
    "ResultFailedError",
    "ResultNotSingleError",
    "RoutingServiceUnavailable",
    "ServiceUnavailable",
    "SessionError",
    "SessionExpired",
    "TokenExpired",
    "TransactionError",
    "TransactionNestingError",
    "TransientError",
    "UnsupportedServerProduct",
    "WriteServiceUnavailable",
]


_CLASSIFICATION_CLIENT: _t.Final[str] = "ClientError"
_CLASSIFICATION_TRANSIENT: _t.Final[str] = "TransientError"
_CLASSIFICATION_DATABASE: _t.Final[str] = "DatabaseError"


_ERROR_REWRITE_MAP: dict[str, tuple[str, str | None]] = {
    # This error can be retried ed. The driver just needs to re-authenticate
    # with the same credentials.
    "Neo.ClientError.Security.AuthorizationExpired": (
        _CLASSIFICATION_TRANSIENT,
        None,
    ),
    # In 5.0, this error has been re-classified as ClientError.
    # For backwards compatibility with Neo4j 4.4 and earlier, we re-map it in
    # the driver, too.
    "Neo.TransientError.Transaction.Terminated": (
        _CLASSIFICATION_CLIENT,
        "Neo.ClientError.Transaction.Terminated",
    ),
    # In 5.0, this error has been re-classified as ClientError.
    # For backwards compatibility with Neo4j 4.4 and earlier, we re-map it in
    # the driver, too.
    "Neo.TransientError.Transaction.LockClientStopped": (
        _CLASSIFICATION_CLIENT,
        "Neo.ClientError.Transaction.LockClientStopped",
    ),
}


_UNKNOWN_NEO4J_CODE: _t.Final[str] = "Neo.DatabaseError.General.UnknownError"
# TODO: 7.0 - Make _UNKNOWN_GQL_MESSAGE the default message
_UNKNOWN_MESSAGE: _t.Final[str] = "An unknown error occurred"
_UNKNOWN_GQL_STATUS: _t.Final[str] = "50N42"
_UNKNOWN_GQL_DESCRIPTION: _t.Final[str] = (
    "error: general processing exception - unexpected error"
)
_UNKNOWN_GQL_MESSAGE: _t.Final[str] = (
    f"{_UNKNOWN_GQL_STATUS}: "
    "Unexpected error has occurred. See debug log for details."
)
_UNKNOWN_GQL_DIAGNOSTIC_RECORD: _t.Final[tuple[tuple[str, _t.Any], ...]] = (
    ("OPERATION", ""),
    ("OPERATION_CODE", "0"),
    ("CURRENT_SCHEMA", "/"),
)


class GqlErrorClassification(str, _Enum):
    """
    Server-side GQL error category.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Hence, can also be compared to its string value::

        >>> GqlErrorClassification.CLIENT_ERROR == "CLIENT_ERROR"
        True
        >>> GqlErrorClassification.DATABASE_ERROR == "DATABASE_ERROR"
        True
        >>> GqlErrorClassification.TRANSIENT_ERROR == "TRANSIENT_ERROR"
        True

    .. seealso:: :attr:`.GqlError.gql_classification`

    .. versionadded:: 5.26

    .. versionchanged:: 6.0 Stabilized from preview.
    """

    CLIENT_ERROR = "CLIENT_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    TRANSIENT_ERROR = "TRANSIENT_ERROR"
    #: Used when the server provides a Classification which the driver is
    #: unaware of.
    #: This can happen when connecting to a server newer than the driver or
    #: before GQL errors were introduced.
    UNKNOWN = "UNKNOWN"


class GqlError(Exception):
    """
    The GQL compliant data of an error.

    This error isn't raised by the driver as is.
    Instead, only subclasses are raised.
    Further, it is used as the :attr:`__cause__` of GqlError subclasses.

    Sometimes it is helpful or necessary to traverse the cause chain of
    GqlErrors to fully understand or appropriately handle the error. In such
    cases, users can either traverse the :attr:`__cause__` attribute of the
    error(s) or use the helper method :meth:`.find_by_gql_status`. Note that
    :attr:`__cause__` is a standard attribute of all Python
    :class:`BaseException` s: the cause chain may contain other exception types
    besides GqlError.

    .. versionadded: 5.26

    .. versionchanged:: 6.0 Stabilized from preview.
    """

    _gql_status: str
    _message: str
    _gql_status_description: str
    _gql_raw_classification: str | None
    _gql_classification: GqlErrorClassification
    _status_diagnostic_record: dict[str, _t.Any]  # original, internal only
    _diagnostic_record: dict[str, _t.Any]  # copy to be used externally
    _gql_cause: GqlError | None

    __cause__: BaseException | None
    """
    The GqlError's cause, if any.

    Sometimes it is helpful or necessary to traverse the cause chain of
    GqlErrors to fully understand or appropriately handle the error.

    .. seealso:: :meth:`.find_by_gql_status`
    """

    @staticmethod
    def _hydrate_cause(**metadata: _t.Any) -> GqlError:
        meta_extractor = _MetaExtractor(metadata)
        gql_status = meta_extractor.str_value("gql_status")
        description = meta_extractor.str_value("description")
        message = meta_extractor.str_value("message")
        if gql_status is None or description is None or message is None:
            gql_status = _UNKNOWN_GQL_STATUS
            message = _UNKNOWN_GQL_MESSAGE
            description = _UNKNOWN_GQL_DESCRIPTION
        diagnostic_record = meta_extractor.map_value("diagnostic_record")
        cause_map = meta_extractor.map_value("cause")
        if cause_map is not None:
            cause = GqlError._hydrate_cause(**cause_map)
        else:
            cause = None
        inst = GqlError()
        inst._init_gql(
            gql_status=gql_status,
            message=message,
            description=description,
            diagnostic_record=diagnostic_record,
            cause=cause,
        )
        return inst

    def _init_gql(
        self,
        *,
        gql_status: str,
        message: str,
        description: str,
        diagnostic_record: dict[str, _t.Any] | None = None,
        cause: GqlError | None = None,
    ) -> None:
        self._gql_status = gql_status
        self._message = message
        self._gql_status_description = description
        if diagnostic_record is not None:
            self._status_diagnostic_record = diagnostic_record
        self._gql_cause = cause

    def _set_gql_unknown(self) -> None:
        self._gql_status = _UNKNOWN_GQL_STATUS
        self._message = _UNKNOWN_GQL_MESSAGE
        self._gql_status_description = _UNKNOWN_GQL_DESCRIPTION

    def __getattribute__(self, item):
        if item != "__cause__":
            return super().__getattribute__(item)
        gql_cause = self._get_attr_or_none("_gql_cause")
        if gql_cause is None:
            # No GQL cause, no magic needed
            return super().__getattribute__(item)
        local_cause = self._get_attr_or_none("__cause__")
        if local_cause is None:
            # We have a GQL cause but no local cause
            # => set the GQL cause as the local cause
            self.__cause__ = gql_cause
            self.__suppress_context__ = True
            self._gql_cause = None
            return super().__getattribute__(item)
        # We have both a GQL cause and a local cause
        # => traverse the cause chain and append the local cause.
        root = gql_cause
        seen_errors = {id(self), id(root)}
        while True:
            cause = getattr(root, "__cause__", None)
            if cause is None:
                root.__cause__ = local_cause
                root.__suppress_context__ = True
                self.__cause__ = gql_cause
                self.__suppress_context__ = True
                self._gql_cause = None
                return gql_cause
            root = cause
            if id(root) in seen_errors:
                # Circular cause chain -> we have no choice but to either
                # overwrite the cause or ignore the new one.
                return local_cause
            seen_errors.add(id(root))

    def _get_attr_or_none(self, item):
        try:
            return super().__getattribute__(item)
        except AttributeError:
            return None

    @property
    def gql_status(self) -> str:
        """
        The GQLSTATUS returned from the server.

        The status code ``50N42`` (unknown error) is a special code that the
        driver will use for polyfilling (when connected to an old,
        non-GQL-aware server).
        Further, it may be used by servers during the transition-phase to
        GQLSTATUS-awareness.

        .. note::
            This means that the code ``50N42`` is not guaranteed to be stable
            and may change in future versions of the driver or the server.
        """
        if hasattr(self, "_gql_status"):
            return self._gql_status
        self._set_gql_unknown()
        return self._gql_status

    @property
    def message(self) -> str:
        """
        The error message returned by the server.

        It is a string representation of the error that occurred.

        This message is meant for human consumption and debugging purposes.
        Don't rely on it in a programmatic way.

        This value is never :data:`None` unless the subclass in question
        states otherwise.
        """
        if hasattr(self, "_message"):
            return self._message
        self._set_gql_unknown()
        return self._message

    @property
    def gql_status_description(self) -> str:
        """
        A description of the GQLSTATUS returned from the server.

        It describes the error that occurred in detail.

        This description is meant for human consumption and debugging purposes.
        Don't rely on it in a programmatic way.
        """
        if hasattr(self, "_gql_status_description"):
            return self._gql_status_description
        self._set_gql_unknown()
        return self._gql_status_description

    @property
    def gql_raw_classification(self) -> str | None:
        """
        Vendor specific classification of the error.

        This is a convenience accessor for ``_classification`` in the
        diagnostic record. :data:`None` is returned if the classification is
        not available or not a string.
        """
        if hasattr(self, "_gql_raw_classification"):
            return self._gql_raw_classification

        diag_record = self._get_status_diagnostic_record()
        classification = diag_record.get("_classification")
        if not isinstance(classification, str):
            self._gql_raw_classification = None
        else:
            self._gql_raw_classification = classification
        return self._gql_raw_classification

    @property
    def gql_classification(self) -> GqlErrorClassification:
        """The stable GqlErrorClassification for this error."""
        if hasattr(self, "_gql_classification"):
            return self._gql_classification
        classification = self.gql_raw_classification
        if not (
            isinstance(classification, str)
            and classification
            in _t.cast(_t.Iterable[str], iter(GqlErrorClassification))
        ):
            self._gql_classification = GqlErrorClassification.UNKNOWN
        else:
            self._gql_classification = GqlErrorClassification(classification)
        return self._gql_classification

    @property
    def diagnostic_record(self) -> _t.Mapping[str, _t.Any]:
        """The diagnostic record for this error."""
        if hasattr(self, "_diagnostic_record"):
            return self._diagnostic_record
        self._diagnostic_record = _deepcopy(
            self._get_status_diagnostic_record()
        )
        return self._diagnostic_record

    def _get_status_diagnostic_record(self) -> dict[str, _t.Any]:
        if hasattr(self, "_status_diagnostic_record"):
            return self._status_diagnostic_record

        self._status_diagnostic_record = dict(_UNKNOWN_GQL_DIAGNOSTIC_RECORD)
        return self._status_diagnostic_record

    def find_by_gql_status(self, status: str) -> GqlError | None:
        """
        Return the first GqlError in the cause chain with the given GQL status.

        This method traverses this GQLErorrs's :attr:`__cause__` chain,
        starting with this error itself, and returns the first error that has
        the given GQL status. If no error matches, :data:`None` is returned.

        Example::

            def invalid_syntax(err: GqlError) -> bool:
                return err.find_by_gql_status("42001") is not None

        :param status: The GQL status to search for.

        :returns: The first matching error or :data:`None`.

        .. versionadded:: 6.0
        """
        if self.gql_status == status:
            return self

        cause = self.__cause__
        while cause is not None:
            if isinstance(cause, GqlError) and cause.gql_status == status:
                return cause
            cause = getattr(cause, "__cause__", None)

        return None

    def __str__(self):
        return (
            f"{{gql_status: {self.gql_status}}} "
            f"{{gql_status_description: {self.gql_status_description}}} "
            f"{{message: {self.message}}} "
            f"{{diagnostic_record: {self.diagnostic_record}}} "
            f"{{raw_classification: {self.gql_raw_classification}}}"
        )


# Neo4jError
class Neo4jError(GqlError):
    """Raised when the Cypher engine returns an error to the client."""

    _neo4j_code: str
    _classification: str
    _category: str
    _title: str
    #: (dict) Any additional information returned by the server.
    _metadata: dict[str, _t.Any]
    _from_server: bool

    _retryable = False

    def __init__(self, *args: object) -> None:
        Exception.__init__(self, *args)
        self._from_server = False
        self._neo4j_code = _UNKNOWN_NEO4J_CODE
        self._message = _UNKNOWN_MESSAGE
        _, self._classification, self._category, self._title = (
            self._neo4j_code.split(".")
        )

    @staticmethod
    def _hydrate_neo4j(**metadata: _t.Any) -> Neo4jError:
        meta_extractor = _MetaExtractor(metadata)
        code = meta_extractor.str_value("code") or _UNKNOWN_NEO4J_CODE
        message = meta_extractor.str_value("message") or _UNKNOWN_MESSAGE
        inst = Neo4jError._basic_hydrate(
            neo4j_code=code,
            message=message,
        )
        inst._init_gql(
            gql_status=_UNKNOWN_GQL_STATUS,
            message=message,
            description=f"{_UNKNOWN_GQL_DESCRIPTION}. {message}",
        )
        inst._metadata = meta_extractor.rest()
        return inst

    @staticmethod
    def _hydrate_gql(**metadata: _t.Any) -> Neo4jError:
        meta_extractor = _MetaExtractor(metadata)
        gql_status = meta_extractor.str_value("gql_status")
        status_description = meta_extractor.str_value("description")
        message = meta_extractor.str_value("message")
        if gql_status is None or status_description is None or message is None:
            gql_status = _UNKNOWN_GQL_STATUS
            # TODO: 7.0 - Make this fall back to _UNKNOWN_GQL_MESSAGE
            message = _UNKNOWN_MESSAGE
            status_description = _UNKNOWN_GQL_DESCRIPTION
        neo4j_code = meta_extractor.str_value(
            "neo4j_code",
            _UNKNOWN_NEO4J_CODE,
        )
        diagnostic_record = meta_extractor.map_value("diagnostic_record")
        cause_map = meta_extractor.map_value("cause")
        if cause_map is not None:
            cause = Neo4jError._hydrate_cause(**cause_map)
        else:
            cause = None

        inst = Neo4jError._basic_hydrate(
            neo4j_code=neo4j_code,
            message=message,
        )
        inst._init_gql(
            gql_status=gql_status,
            message=message,
            description=status_description,
            diagnostic_record=diagnostic_record,
            cause=cause,
        )
        inst._metadata = meta_extractor.rest()

        return inst

    @staticmethod
    def _basic_hydrate(*, neo4j_code: str, message: str) -> Neo4jError:
        try:
            _, classification, category, title = neo4j_code.split(".")
        except ValueError:
            classification = _CLASSIFICATION_DATABASE
            category = "General"
            title = "UnknownError"
        else:
            classification_override, code_override = _ERROR_REWRITE_MAP.get(
                neo4j_code, (None, None)
            )
            if classification_override is not None:
                classification = classification_override
            if code_override is not None:
                neo4j_code = code_override

        error_class: type[Neo4jError] = Neo4jError._extract_error_class(
            classification, neo4j_code
        )

        assert issubclass(error_class, Exception)
        inst = Exception.__new__(error_class)
        inst._from_server = True
        inst._neo4j_code = neo4j_code
        inst._classification = classification
        inst._category = category
        inst._title = title
        inst._message = message

        return inst

    @staticmethod
    def _extract_error_class(classification, code) -> type[Neo4jError]:
        if classification == _CLASSIFICATION_CLIENT:
            try:
                return _client_errors[code]
            except KeyError:
                return ClientError

        elif classification == _CLASSIFICATION_TRANSIENT:
            try:
                return _transient_errors[code]
            except KeyError:
                return TransientError

        elif classification == _CLASSIFICATION_DATABASE:
            return DatabaseError

        else:
            return Neo4jError

    @property
    def message(self) -> str:
        """The error message returned by the server."""
        return self._message

    @property
    def code(self) -> str:
        """
        The neo4j error code returned by the server.

        For example, "Neo.ClientError.Security.AuthorizationExpired".
        """
        return self._neo4j_code

    @property
    def classification(self) -> str:
        # Undocumented, will likely be removed with support for neo4j codes
        return self._classification

    @property
    def category(self) -> str:
        # Undocumented, will likely be removed with support for neo4j codes
        return self._category

    @property
    def title(self) -> str:
        # Undocumented, will likely be removed with support for neo4j codes
        return self._title

    @property
    def metadata(self) -> dict[str, _t.Any]:
        # Undocumented, might be useful for debugging
        return self._metadata

    def is_retryable(self) -> bool:
        """
        Whether the error is retryable.

        Indicates whether a transaction that yielded this error makes sense to
        retry. This method makes mostly sense when implementing a custom
        retry policy in conjunction with :ref:`explicit-transactions-ref`.

        .. warning::

            Auto-commit transactions
            (:meth:`.Session.run`/:meth:`.AsyncSession.run`) are not retryable
            regardless of this value.

        :returns: :data:`True` if the error is retryable,
            :data:`False` otherwise.

        .. versionadded:: 5.0
        """
        return self._retryable

    def _unauthenticates_all_connections(self) -> bool:
        return (
            self._neo4j_code == "Neo.ClientError.Security.AuthorizationExpired"
        )

    def _is_fatal_during_discovery(self) -> bool:
        # checks if the code is an error that is caused by the client. In this
        # case the driver should fail fast during discovery.
        code = self._neo4j_code
        if not isinstance(code, str):
            return False
        if code in {
            "Neo.ClientError.Database.DatabaseNotFound",
            "Neo.ClientError.Transaction.InvalidBookmark",
            "Neo.ClientError.Transaction.InvalidBookmarkMixture",
            "Neo.ClientError.Statement.TypeError",
            "Neo.ClientError.Statement.ArgumentError",
            "Neo.ClientError.Request.Invalid",
        }:
            return True
        return (
            code.startswith("Neo.ClientError.Security.")
            and code != "Neo.ClientError.Security.AuthorizationExpired"
        )

    def _has_security_code(self) -> bool:
        if self._neo4j_code is None:
            return False
        return self._neo4j_code.startswith("Neo.ClientError.Security.")

    def __str__(self):
        if not getattr(self, "_from_server", False):
            return Exception.__str__(self)
        code = self._neo4j_code
        message = self._message
        # TODO: 7.0 - Check if including neo4j_code is still useful
        gql_status = self._gql_status
        gql_description = self._gql_status_description
        return (
            f"{{neo4j_code: {code}}} "
            f"{{message: {message}}} "
            f"{{gql_status: {gql_status}}} "
            f"{{gql_status_description: {gql_description}}}"
        )


class _MetaExtractor:
    def __init__(self, metadata: dict[str, _t.Any]) -> None:
        self._metadata = metadata

    def rest(self) -> dict[str, _t.Any]:
        return self._metadata

    @_t.overload
    def str_value(self, key: str) -> str | None: ...

    @_t.overload
    def str_value(self, key: str, default: _T) -> str | _T: ...

    def str_value(
        self, key: str, default: _T | None = None
    ) -> str | _T | None:
        res = self._metadata.pop(key, default)
        if not isinstance(res, str):
            res = default
        return res

    @_t.overload
    def map_value(self, key: str) -> dict[str, _t.Any] | None: ...

    @_t.overload
    def map_value(self, key: str, default: _T) -> dict[str, _t.Any] | _T: ...

    def map_value(
        self, key: str, default: _T | None = None
    ) -> dict[str, _t.Any] | _T | None:
        res = self._metadata.pop(key, default)
        if not (
            isinstance(res, dict) and all(isinstance(k, str) for k in res)
        ):
            res = default
        return res


# Neo4jError > ClientError
class ClientError(Neo4jError):
    """
    Bad client request.

    The Client sent a bad request - changing the request might yield a
    successful outcome.
    """


# Neo4jError > ClientError > CypherSyntaxError
class CypherSyntaxError(ClientError):
    pass


# Neo4jError > ClientError > CypherTypeError
class CypherTypeError(ClientError):
    pass


# Neo4jError > ClientError > ConstraintError
class ConstraintError(ClientError):
    pass


# Neo4jError > ClientError > AuthError
class AuthError(ClientError):
    """Raised when authentication failure occurs."""


# Neo4jError > ClientError > AuthError > TokenExpired
class TokenExpired(AuthError):
    """Raised when the authentication token has expired."""


# Neo4jError > ClientError > Forbidden
class Forbidden(ClientError):
    pass


# Neo4jError > DatabaseError
class DatabaseError(Neo4jError):
    """The database failed to service the request."""


# Neo4jError > TransientError
class TransientError(Neo4jError):
    """
    Transient Error.

    The database cannot service the request right now, retrying later might
    yield a successful outcome.
    """

    _retryable = True


# Neo4jError > TransientError > DatabaseUnavailable
class DatabaseUnavailable(TransientError):
    pass


# Neo4jError > TransientError > NotALeader
class NotALeader(TransientError):
    pass


# Neo4jError > TransientError > ForbiddenOnReadOnlyDatabase
class ForbiddenOnReadOnlyDatabase(TransientError):
    pass


_client_errors: dict[str, type[Neo4jError]] = {
    # ConstraintError
    "Neo.ClientError.Schema.ConstraintValidationFailed": ConstraintError,
    "Neo.ClientError.Schema.ConstraintViolation": ConstraintError,
    "Neo.ClientError.Statement.ConstraintVerificationFailed": ConstraintError,
    "Neo.ClientError.Statement.ConstraintViolation": ConstraintError,
    # CypherSyntaxError
    "Neo.ClientError.Statement.InvalidSyntax": CypherSyntaxError,
    "Neo.ClientError.Statement.SyntaxError": CypherSyntaxError,
    # CypherTypeError
    "Neo.ClientError.Procedure.TypeError": CypherTypeError,
    "Neo.ClientError.Statement.InvalidType": CypherTypeError,
    "Neo.ClientError.Statement.TypeError": CypherTypeError,
    # Forbidden
    "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase": ForbiddenOnReadOnlyDatabase,  # noqa: E501
    "Neo.ClientError.General.ReadOnly": Forbidden,
    "Neo.ClientError.Schema.ForbiddenOnConstraintIndex": Forbidden,
    "Neo.ClientError.Schema.IndexBelongsToConstraint": Forbidden,
    "Neo.ClientError.Security.Forbidden": Forbidden,
    "Neo.ClientError.Transaction.ForbiddenDueToTransactionType": Forbidden,
    # AuthError
    "Neo.ClientError.Security.AuthorizationFailed": AuthError,
    "Neo.ClientError.Security.Unauthorized": AuthError,
    # TokenExpired
    "Neo.ClientError.Security.TokenExpired": TokenExpired,
    # NotALeader
    "Neo.ClientError.Cluster.NotALeader": NotALeader,
}

_transient_errors: dict[str, type[Neo4jError]] = {
    # DatabaseUnavailableError
    "Neo.TransientError.General.DatabaseUnavailable": DatabaseUnavailable
}


# DriverError
class DriverError(GqlError):
    """Raised when the Driver raises an error."""

    def is_retryable(self) -> bool:
        """
        Whether the error is retryable.

        Indicates whether a transaction that yielded this error makes sense to
        retry. This method makes mostly sense when implementing a custom
        retry policy in conjunction with :ref:`explicit-transactions-ref`.

        .. warning::

            Auto-commit transactions
            (:meth:`.Session.run`/:meth:`.AsyncSession.run`) are not retryable
            regardless of this value.

        :returns: :data:`True` if the error is retryable,
            :data:`False` otherwise.

        .. versionadded:: 5.0
        """
        return False

    def __str__(self):
        return Exception.__str__(self)


# DriverError > SessionError
class SessionError(DriverError):
    """Raised when an error occurs while using a session."""

    session: _TSession

    def __init__(self, session_, *args: object) -> None:
        super().__init__(*args)
        self.session = session_


# DriverError > TransactionError
class TransactionError(DriverError):
    """Raised when an error occurs while using a transaction."""

    transaction: _TTransaction

    def __init__(self, transaction_, *args: object) -> None:
        super().__init__(*args)
        self.transaction = transaction_


# DriverError > TransactionError > TransactionNestingError
class TransactionNestingError(TransactionError):
    """Raised when transactions are nested incorrectly."""


# DriverError > ResultError
class ResultError(DriverError):
    """Raised when an error occurs while using a result object."""

    result: _TResult

    def __init__(self, result_, *args: object) -> None:
        super().__init__(*args)
        self.result = result_


# DriverError > ResultError > ResultFailedError
class ResultFailedError(ResultError):
    """
    Raised when trying to access records of a failed result.

    A :class:`.Result` will be considered failed if
     * itself encountered an error while fetching records
     * another result within the same transaction encountered an error while
       fetching records

    .. versionadded: 5.14
    """


# DriverError > ResultError > ResultConsumedError
class ResultConsumedError(ResultError):
    """Raised when trying to access records of a consumed result."""


# DriverError > ResultError > ResultNotSingleError
class ResultNotSingleError(ResultError):
    """Raised when a result should have exactly one record but does not."""


# DriverError > BrokenRecordError
class BrokenRecordError(DriverError):
    """
    Raised when accessing a Record's field that couldn't be decoded.

    This can for instance happen when the server sends a zoned datetime with a
    zone id unknown to the client.
    """


# DriverError > SessionExpired
class SessionExpired(DriverError):
    """
    The session has expired.

    Raised when a session is no longer able to fulfil the purpose described by
    its original parameters.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        message = ""
        description = "error: connection exception"
        if len(args) > 0 and isinstance(args[0], str):
            message = args[0]
            description = f"{description}. {message}"
        self._init_gql(
            gql_status="08000",
            message=message,
            description=description,
        )

    def is_retryable(self) -> bool:
        return True


# DriverError > ServiceUnavailable
class ServiceUnavailable(DriverError):
    """
    Raised when no database service is available.

    This may be due to incorrect configuration or could indicate a runtime
    failure of a database service that the driver is unable to route around.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        message = ""
        description = "error: connection exception"
        if len(args) > 0 and isinstance(args[0], str):
            message = args[0]
            description = f"{description}. {message}"
        self._init_gql(
            gql_status="08000",
            message=message,
            description=description,
        )

    def is_retryable(self) -> bool:
        return True


# DriverError > ServiceUnavailable > RoutingServiceUnavailable
class RoutingServiceUnavailable(ServiceUnavailable):
    """Raised when no routing service is available."""


# DriverError > ServiceUnavailable > WriteServiceUnavailable
class WriteServiceUnavailable(ServiceUnavailable):
    """Raised when no write service is available."""


# DriverError > ServiceUnavailable > ReadServiceUnavailable
class ReadServiceUnavailable(ServiceUnavailable):
    """Raised when no read service is available."""


# DriverError > ServiceUnavailable > IncompleteCommit
class IncompleteCommit(ServiceUnavailable):
    """
    Raised when the client looses connection while committing a transaction.

    Raised when a disconnection occurs while still waiting for a commit
    response. For non-idempotent write transactions, this leaves the data
    in an unknown state with regard to whether the transaction completed
    successfully or not.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        message = ""
        if len(args) > 0 and isinstance(args[0], str):
            message = args[0]
        self._init_gql(
            gql_status="08007",
            message=message,
            description=(
                "error: connection exception - transaction resolution unknown"
            ),
        )

    def is_retryable(self) -> bool:
        return False


# DriverError > ConfigurationError
class ConfigurationError(DriverError):
    """Raised when there is an error concerning a configuration."""


# DriverError > ConfigurationError > AuthConfigurationError
class AuthConfigurationError(ConfigurationError):
    """Raised when there is an error with the authentication configuration."""


# DriverError > ConfigurationError > CertificateConfigurationError
class CertificateConfigurationError(ConfigurationError):
    """Raised when there is an error with the certificate configuration."""


# DriverError > ConfigurationError > UnsupportedServerProduct
class UnsupportedServerProduct(ConfigurationError):
    """
    Raised when an unsupported server product is detected.

    .. versionchanged:: 6.0
        This exception is now a subclass of :class:`ConfigurationError`.
        Before it was a subclass of :class:`Exception`.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


# DriverError > ConnectionPoolError
class ConnectionPoolError(DriverError):
    """Raised when the connection pool encounters an error."""


# DriverError > ConnectionPoolError > ConnectionAcquisitionTimeoutError
class ConnectionAcquisitionTimeoutError(ConnectionPoolError):
    """
    Raised when no connection became available in time.

    The amount of time is determined by the connection acquisition timeout
    configuration option.
    """
