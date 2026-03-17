"""Tablib - Command-line Interface table export support.

   Generates a representation for CLI from the dataset.
   Wrapper for tabulate library.
"""
from tabulate import tabulate as Tabulate


class CLIFormat:
    """ Class responsible to export to CLI Format """
    title = 'cli'
    DEFAULT_FMT = 'plain'

    @classmethod
    def export_set(cls, dataset, **kwargs):
        """Returns CLI representation of a Dataset."""
        if dataset.headers:
            kwargs.setdefault('headers', dataset.headers)
        kwargs.setdefault('tablefmt', cls.DEFAULT_FMT)
        return Tabulate(dataset, **kwargs)
