""" Tablib - TSV (Tab Separated Values) Support.
"""

from ._csv import CSVFormat


class TSVFormat(CSVFormat):
    title = 'tsv'
    extensions = ('tsv',)

    DEFAULT_DELIMITER = '\t'
