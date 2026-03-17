"""OpenAPI core validation datatypes module"""
import attr


@attr.s
class BaseValidationResult(object):
    errors = attr.ib(factory=list)

    def raise_for_errors(self):
        for error in self.errors:
            raise error
