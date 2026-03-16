# -*- coding: utf-8 -*-


class NgxParserBaseException(Exception):
    def __init__(self, strerror, filename, lineno):
        self.args = (strerror, filename, lineno)
        self.filename = filename
        self.lineno = lineno
        self.strerror = strerror

    def __str__(self):
        if self.lineno is not None:
            return '%s in %s:%s' % self.args
        else:
            return '%s in %s' % self.args


class NgxParserSyntaxError(NgxParserBaseException):
    pass


class NgxParserDirectiveError(NgxParserBaseException):
    pass


class NgxParserDirectiveArgumentsError(NgxParserDirectiveError):
    pass


class NgxParserDirectiveContextError(NgxParserDirectiveError):
    pass


class NgxParserDirectiveUnknownError(NgxParserDirectiveError):
    pass
