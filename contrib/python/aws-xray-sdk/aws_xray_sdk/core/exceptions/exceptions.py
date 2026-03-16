class InvalidSamplingManifestError(Exception):
    pass


class SegmentNotFoundException(Exception):
    pass


class InvalidDaemonAddressException(Exception):
    pass


class SegmentNameMissingException(Exception):
    pass


class SubsegmentNameMissingException(Exception):
    pass


class FacadeSegmentMutationException(Exception):
    pass


class MissingPluginNames(Exception):
    pass


class AlreadyEndedException(Exception):
    pass
