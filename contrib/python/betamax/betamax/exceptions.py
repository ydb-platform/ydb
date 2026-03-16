class BetamaxError(Exception):
    def __init__(self, message):
        super(BetamaxError, self).__init__(message)


class MissingDirectoryError(BetamaxError):
    pass


class ValidationError(BetamaxError):
    pass


class InvalidOption(ValidationError):
    pass


class BodyBytesValidationError(ValidationError):
    pass


class MatchersValidationError(ValidationError):
    pass


class RecordValidationError(ValidationError):
    pass


class RecordIntervalValidationError(ValidationError):
    pass


class PlaceholdersValidationError(ValidationError):
    pass


class PlaybackRepeatsValidationError(ValidationError):
    pass


class SerializerValidationError(ValidationError):
    pass


validation_error_map = {
    'allow_playback_repeats': PlaybackRepeatsValidationError,
    'match_requests_on': MatchersValidationError,
    'record': RecordValidationError,
    'placeholders': PlaceholdersValidationError,
    'preserve_exact_body_bytes': BodyBytesValidationError,
    're_record_interval': RecordIntervalValidationError,
    'serialize': SerializerValidationError,  # TODO: Remove this
    'serialize_with': SerializerValidationError
}
