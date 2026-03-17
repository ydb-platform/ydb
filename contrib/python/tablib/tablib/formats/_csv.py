""" Tablib - *SV Support.
"""

import csv
from io import StringIO


class CSVFormat:
    title = 'csv'
    extensions = ('csv',)

    DEFAULT_DELIMITER = ','

    @classmethod
    def export_stream_set(cls, dataset, **kwargs):
        """Returns CSV representation of Dataset as file-like."""
        stream = StringIO()

        kwargs.setdefault('delimiter', cls.DEFAULT_DELIMITER)

        _csv = csv.writer(stream, **kwargs)

        for row in dataset._package(dicts=False):
            _csv.writerow(row)

        stream.seek(0)
        return stream

    @classmethod
    def export_set(cls, dataset, **kwargs):
        """Returns CSV representation of Dataset."""
        stream = cls.export_stream_set(dataset, **kwargs)
        return stream.getvalue()

    @classmethod
    def import_set(cls, dset, in_stream, headers=True, skip_lines=0, **kwargs):
        """Returns dataset from CSV stream."""

        dset.wipe()

        kwargs.setdefault('delimiter', cls.DEFAULT_DELIMITER)

        rows = csv.reader(in_stream, **kwargs)
        for i, row in enumerate(rows):
            if i < skip_lines:
                continue
            if i == skip_lines and headers:
                dset.headers = row
            elif row:
                if i > 0 and len(row) < dset.width:
                    row += [''] * (dset.width - len(row))
                dset.append(row)

    @classmethod
    def detect(cls, stream, delimiter=None):
        """Returns True if given stream is valid CSV."""
        try:
            csv.Sniffer().sniff(stream.read(2048), delimiters=delimiter or cls.DEFAULT_DELIMITER)
            return True
        except Exception:
            return False
