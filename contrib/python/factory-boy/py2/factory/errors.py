# -*- coding: utf-8 -*-
# Copyright: See the LICENSE file.


class FactoryError(Exception):
    """Any exception raised by factory_boy."""


class AssociatedClassError(FactoryError):
    """Exception for Factory subclasses lacking Meta.model."""


class UnknownStrategy(FactoryError):
    """Raised when a factory uses an unknown strategy."""


class UnsupportedStrategy(FactoryError):
    """Raised when trying to use a strategy on an incompatible Factory."""


class CyclicDefinitionError(FactoryError):
    """Raised when a cyclical declaration occurs."""


class InvalidDeclarationError(FactoryError):
    """Raised when a sub-declaration has no related declaration.

    This means that the user declared 'foo__bar' without adding a declaration
    at 'foo'.
    """
