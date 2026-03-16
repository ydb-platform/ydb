""" Tablib - YAML Support.
"""

import yaml

import tablib


class YAMLFormat:
    title = 'yaml'
    extensions = ('yaml', 'yml')

    @classmethod
    def export_set(cls, dataset):
        """Returns YAML representation of Dataset."""
        return yaml.safe_dump(
            dataset._package(), default_flow_style=None, allow_unicode=True
        )

    @classmethod
    def export_book(cls, databook):
        """Returns YAML representation of Databook."""
        return yaml.safe_dump(
            databook._package(), default_flow_style=None, allow_unicode=True
        )

    @classmethod
    def import_set(cls, dset, in_stream):
        """Returns dataset from YAML stream."""

        dset.wipe()
        dset.dict = yaml.safe_load(in_stream)

    @classmethod
    def import_book(cls, dbook, in_stream):
        """Returns databook from YAML stream."""

        dbook.wipe()

        for sheet in yaml.safe_load(in_stream):
            data = tablib.Dataset()
            data.title = sheet['title']
            data.dict = sheet['data']
            dbook.add_sheet(data)

    @classmethod
    def detect(cls, stream):
        """Returns True if given stream is valid YAML."""
        try:
            _yaml = yaml.safe_load(stream)
            if isinstance(_yaml, (list, tuple, dict)):
                return True
            else:
                return False
        except (yaml.parser.ParserError, yaml.reader.ReaderError,
                yaml.scanner.ScannerError):
            return False
