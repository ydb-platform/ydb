""" Tablib - HTML export support.
"""
from html.parser import HTMLParser
from xml.etree import ElementTree as ET


class HTMLFormat:
    BOOK_ENDINGS = 'h3'

    title = 'html'
    extensions = ('html', )

    @classmethod
    def export_set(cls, dataset):
        """HTML representation of a Dataset."""

        table = ET.Element('table')
        if dataset.headers is not None:
            head = ET.Element('thead')
            tr = ET.Element('tr')
            for header in dataset.headers:
                th = ET.Element('th')
                th.text = str(header) if header is not None else ''
                tr.append(th)
            head.append(tr)
            table.append(head)

        body = ET.Element('tbody')
        for row in dataset:
            tr = ET.Element('tr')
            for item in row:
                td = ET.Element('td')
                td.text = str(item) if item is not None else ''
                tr.append(td)
            body.append(tr)
        table.append(body)

        return ET.tostring(table, method='html', encoding='unicode')

    @classmethod
    def export_book(cls, databook):
        """HTML representation of a Databook."""

        result = ''
        for i, dset in enumerate(databook._datasets):
            title = dset.title if dset.title else f'Set {i}'
            result += f'<{cls.BOOK_ENDINGS}>{title}</{cls.BOOK_ENDINGS}>\n'
            result += dset.html
            result += '\n'

        return result

    @classmethod
    def import_set(cls, dset, in_stream, table_id=None):
        """Returns dataset from HTML content."""

        dset.wipe()
        parser = TablibHTMLParser(dset, table_id=table_id)
        parser.feed(in_stream.read())
        if not parser.table_found:
            if table_id:
                raise ValueError(f'No <table> found with id="{table_id}" in input HTML')
            else:
                raise ValueError('No <table> found in input HTML')


class TablibHTMLParser(HTMLParser):
    def __init__(self, dataset, *args, table_id=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.dset = dataset
        self.table_id = table_id
        self.table_found = False
        self.table_open = False
        self.thead_open = False
        self.cell_open = False
        self.headers = []
        self.current_row = []
        self.current_data = ''

    def handle_starttag(self, tag, attrs):
        if (
            tag == 'table' and not self.table_found and
            (not self.table_id or dict(attrs).get('id') == self.table_id)
        ):
            self.table_open = True
            self.table_found = True
        elif self.table_open:
            if tag == 'thead':
                self.thead_open = True
            elif tag in ['td', 'th']:
                self.cell_open = True

    def handle_endtag(self, tag):
        if not self.table_open:
            return
        if tag == 'table':
            self.table_open = False
        elif tag == 'thead':
            self.thead_open = False
            self.dset.headers = self.headers
        elif tag == 'tr' and self.current_row:
            self.dset.append(self.current_row)
            self.current_row = []
        elif tag in ['td', 'th']:
            if self.thead_open:
                self.headers.append(self.current_data)
            else:
                self.current_row.append(self.current_data)
            self.cell_open = False
            self.current_data = ''

    def handle_data(self, data):
        if self.cell_open:
            self.current_data += data
