from flake8_commas import _base


class CommaChecker(_base.CommaChecker):
    name = __name__


__all__ = ['CommaChecker']
