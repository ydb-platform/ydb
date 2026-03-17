import attr


@attr.s
class TemplateResult(object):
    pattern = attr.ib(default=None)
    variables = attr.ib(default=None)

    @property
    def resolved(self):
        if not self.variables:
            return self.pattern
        return self.pattern.format(**self.variables)
