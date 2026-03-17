from pyhanko_certvalidator.errors import ValidationError

__all__ = ['PastValidatePrecheckFailure', 'TimeSlideFailure']


class PastValidatePrecheckFailure(ValidationError):
    pass


class TimeSlideFailure(ValidationError):
    pass
