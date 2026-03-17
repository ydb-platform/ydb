class WrongDocumentUpdateStrategy(Exception):
    pass


class DocumentNotFound(Exception):
    pass


class DocumentAlreadyCreated(Exception):
    pass


class DocumentWasNotSaved(Exception):
    pass


class CollectionWasNotInitialized(Exception):
    pass


class MigrationException(Exception):
    pass


class ReplaceError(Exception):
    pass


class StateManagementIsTurnedOff(Exception):
    pass


class StateNotSaved(Exception):
    pass


class RevisionIdWasChanged(Exception):
    pass


class NotSupported(Exception):
    pass


class MongoDBVersionError(Exception):
    pass


class ViewWasNotInitialized(Exception):
    pass


class ViewHasNoSettings(Exception):
    pass


class UnionHasNoRegisteredDocs(Exception):
    pass


class UnionDocNotInited(Exception):
    pass


class DocWasNotRegisteredInUnionClass(Exception):
    pass


class Deprecation(Exception):
    pass


class ApplyChangesException(Exception):
    pass
