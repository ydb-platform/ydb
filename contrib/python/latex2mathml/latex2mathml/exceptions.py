class NumeratorNotFoundError(Exception):
    pass


class DenominatorNotFoundError(Exception):
    pass


class ExtraLeftOrMissingRightError(Exception):
    pass


class MissingSuperScriptOrSubscriptError(Exception):
    pass


class DoubleSubscriptsError(Exception):
    pass


class DoubleSuperscriptsError(Exception):
    pass


class NoAvailableTokensError(Exception):
    pass


class InvalidStyleForGenfracError(Exception):
    pass


class MissingEndError(Exception):
    pass


class InvalidAlignmentError(Exception):
    pass


class InvalidWidthError(Exception):
    pass


class LimitsMustFollowMathOperatorError(Exception):
    pass
