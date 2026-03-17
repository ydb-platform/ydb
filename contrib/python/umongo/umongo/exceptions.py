"""umongo exceptions"""


class UMongoError(Exception):
    """Base umongo error"""


class NoCompatibleInstanceError(UMongoError):
    """Can't find instance compatible with database"""


class AbstractDocumentError(UMongoError):
    """Raised when instantiating an abstract document"""


class DocumentDefinitionError(UMongoError):
    """Error in document definition"""


class NoDBDefinedError(UMongoError):
    """No database defined"""


class NotRegisteredDocumentError(UMongoError):
    """Document not registered"""


class AlreadyRegisteredDocumentError(UMongoError):
    """Document already registerd"""


class UpdateError(UMongoError):
    """Error while updating document"""


class DeleteError(UMongoError):
    """Error while deleting document"""


class AlreadyCreatedError(UMongoError):
    """Modifying id of an already created document"""


class NotCreatedError(UMongoError):
    """Document does not exist in database"""


class NoneReferenceError(UMongoError):
    """Retrieving a None reference"""


class UnknownFieldInDBError(UMongoError):
    """Data from database contains unknown field"""
