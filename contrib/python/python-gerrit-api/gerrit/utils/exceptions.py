"""Module for custom exceptions.

Where possible we try to throw exceptions with non-generic,
meaningful names.
"""


class GerritAPIException(Exception):
    """
    Base class for all errors
    """


class ClientError(GerritAPIException):
    """
    Client Error
    """


class ServerError(GerritAPIException):
    """
    Server Error
    """


class UnauthorizedError(GerritAPIException):
    """
    401 Unauthorized
    """


class AuthError(GerritAPIException):
    """
    403 Forbidden is returned if the operation is not allowed because the calling user does not have
    sufficient permissions.
    """


class ValidationError(GerritAPIException):
    """
    400 Bad Request is returned if the request is not understood by the server due to malformed
    syntax.
    E.g. 400 Bad Request is returned if JSON input is expected but the 'Content-Type' of the request
     is not 'application/json' or the request body doesn't contain valid JSON.
    400 Bad Request is also returned if required input fields are not set or if options are set
    which cannot be used together.
    """


class NotAllowedError(GerritAPIException):
    """
    405 Method Not Allowed is returned if the resource exists but doesn't support the operation.
    """


class ConflictError(GerritAPIException):
    """
    409 Conflict is returned if the request cannot be completed because the current state of the
    resource doesn't allow the operation.
    """


class NotFoundError(GerritAPIException):
    """
    Resource cannot be found
    """


class UnknownBranch(KeyError, NotFoundError):
    """
    Gerrit does not recognize the branch requested.
    """


class UnknownTag(KeyError, NotFoundError):
    """
    Gerrit does not recognize the tag requested.
    """


class UnknownFile(KeyError, NotFoundError):
    """
    Gerrit does not recognize the revision file requested.
    """


class FileContentNotFoundError(GerritAPIException):
    """
    File content not found
    """


class ProjectNotFoundError(GerritAPIException):
    """
    Project cannot be found
    """


class ProjectAlreadyExistsError(GerritAPIException):
    """
    Project already exists
    """


class BranchNotFoundError(GerritAPIException):
    """
    Branch cannot be found
    """


class BranchAlreadyExistsError(GerritAPIException):
    """
    Branch already exists
    """


class TagNotFoundError(GerritAPIException):
    """
    Tag cannot be found
    """


class TagAlreadyExistsError(GerritAPIException):
    """
    Tag already exists
    """


class CommitNotFoundError(GerritAPIException):
    """
    Commit cannot be found
    """


class GroupNotFoundError(GerritAPIException):
    """
    Group cannot be found
    """


class GroupAlreadyExistsError(GerritAPIException):
    """
    Group already exists
    """


class GroupMemberNotFoundError(GerritAPIException):
    """
    Group member cannot be found
    """


class GroupMemberAlreadyExistsError(GerritAPIException):
    """
    Group member already exists
    """


class ChangeNotFoundError(GerritAPIException):
    """
    Change cannot be found
    """


class ReviewerNotFoundError(GerritAPIException):
    """
    Reviewer cannot be found
    """


class ReviewerAlreadyExistsError(GerritAPIException):
    """
    Reviewer already exists
    """


class AccountNotFoundError(GerritAPIException):
    """
    Account cannot be found
    """


class AccountAlreadyExistsError(GerritAPIException):
    """
    Account already exists
    """


class AccountEmailNotFoundError(GerritAPIException):
    """
    Account Email cannot be found
    """


class AccountEmailAlreadyExistsError(GerritAPIException):
    """
    Account Email already exists
    """


class SSHKeyNotFoundError(GerritAPIException):
    """
    SSH key cannot be found
    """


class GPGKeyNotFoundError(GerritAPIException):
    """
    GPG key cannot be found
    """


class ChangeEditNotFoundError(GerritAPIException):
    """
    Change edit cannot be found
    """
