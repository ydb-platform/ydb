from dataclasses import dataclass

from exception import ValidationError


US_IN_MSEC = 1000


@dataclass
class Parameters:
    start_us: int
    end_us: int
    interval_us: int
    mean: float
    sigma: float

    @classmethod
    def from_strings(cls, start: str, end: str, interval: str, mean: str, sigma: str):
        try:
            parameters = cls(
                start_us=int(start) * US_IN_MSEC,
                end_us=int(end) * US_IN_MSEC,
                interval_us=int(interval) * US_IN_MSEC,
                mean=float(mean),
                sigma=float(sigma),
            )
        except ValueError:
            raise ValidationError

        parameters.validate()

        return parameters

    def validate(self):
        if self.start_us >= self.end_us:
            raise ValidationError

        if self.interval_us == 0:
            raise ValidationError
