from __future__ import unicode_literals
import codecs
import csv
from django.conf import settings
from rest_framework.renderers import *
from io import StringIO
from rest_framework_csv.orderedrows import OrderedRows
from rest_framework_csv.misc import Echo
from types import GeneratorType

from logging import getLogger
log = getLogger(__name__)


class CSVRenderer(BaseRenderer):
    """
    Renderer which serializes to CSV
    """

    media_type = 'text/csv'
    format = 'csv'
    level_sep = '.'
    header = None
    labels = None  # {'<field>':'<label>'}
    writer_opts = None

    def render(self, data, media_type=None, renderer_context={}, writer_opts=None):
        """
        Renders serialized *data* into CSV. For a dictionary:
        """
        if data is None:
            return ''

        if not isinstance(data, list):
            data = [data]

        if writer_opts is not None:
            log.warning('The writer_opts argument is deprecated. Set the '
                        'writer_opts on the renderer class, instance, or pass '
                        'writer_opts into the renderer_context instead.')

        writer_opts = renderer_context.get('writer_opts', writer_opts or self.writer_opts or {})
        header = renderer_context.get('header', self.header)
        labels = renderer_context.get('labels', self.labels)
        encoding = renderer_context.get('encoding', settings.DEFAULT_CHARSET)

        table = self.tablize(data, header=header, labels=labels)
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer, **writer_opts)
        for row in table:
            csv_writer.writerow(row)

        return csv_buffer.getvalue().encode(encoding)

    def tablize(self, data, header=None, labels=None):
        """
        Convert a list of data into a table.

        If there is a header provided to tablize it will efficiently yield each
        row as needed. If no header is provided, tablize will need to process
        each row in the data in order to construct a complete header. Thus, if
        you have a lot of data and want to stream it, you should probably
        provide a header to the renderer (using the `header` attribute, or via
        the `renderer_context`).
        """
        # Try to pull the header off of the data, if it's not passed in as an
        # argument.
        if not header and hasattr(data, 'header'):
            header = data.header

        if data:
            # First, flatten the data (i.e., convert it to a list of
            # dictionaries that are each exactly one level deep).  The key for
            # each item designates the name of the column that the item will
            # fall into.
            data = self.flatten_data(data)

            # Get the set of all unique headers, and sort them (unless already provided).
            if not header:
                # We don't have to materialize the data generator unless we
                # have to build a header.
                data = tuple(data)
                header_fields = set()
                for item in data:
                    header_fields.update(list(item.keys()))
                header = sorted(header_fields)

            # Return your "table", with the headers as the first row.
            if labels:
                yield [labels.get(x, x) for x in header]
            else:
                yield header

            # Create a row for each dictionary, filling in columns for which the
            # item has no data with None values.
            for item in data:
                row = [item.get(key, None) for key in header]
                yield row

        elif header:
            # If there's no data but a header was supplied, yield the header.
            if labels:
                yield [labels.get(x, x) for x in header]
            else:
                yield header

        else:
            # Generator will yield nothing if there's no data and no header
            pass

    def flatten_data(self, data):
        """
        Convert the given data collection to a list of dictionaries that are
        each exactly one level deep. The key for each value in the dictionaries
        designates the name of the column that the value will fall into.
        """
        for item in data:
            flat_item = self.flatten_item(item)
            yield flat_item

    def flatten_item(self, item):
        if isinstance(item, list):
            flat_item = self.flatten_list(item)
        elif isinstance(item, dict):
            flat_item = self.flatten_dict(item)
        else:
            flat_item = {'': item}

        return flat_item

    def nest_flat_item(self, flat_item, prefix):
        """
        Given a "flat item" (a dictionary exactly one level deep), nest all of
        the column headers in a namespace designated by prefix.  For example:

         header... | with prefix... | becomes...
        -----------|----------------|----------------
         'lat'     | 'location'     | 'location.lat'
         ''        | '0'            | '0'
         'votes.1' | 'user'         | 'user.votes.1'

        """
        nested_item = {}
        for header, val in flat_item.items():
            nested_header = self.level_sep.join([prefix, header]) if header else prefix
            nested_item[nested_header] = val
        return nested_item

    def flatten_list(self, l):
        flat_list = {}
        for index, item in enumerate(l):
            index = str(index)
            flat_item = self.flatten_item(item)
            nested_item = self.nest_flat_item(flat_item, index)
            flat_list.update(nested_item)
        return flat_list

    def flatten_dict(self, d):
        flat_dict = {}
        for key, item in d.items():
            key = str(key)
            flat_item = self.flatten_item(item)
            nested_item = self.nest_flat_item(flat_item, key)
            flat_dict.update(nested_item)
        return flat_dict

    def headers():
        doc = ("The headers property. Kept around for backward compatibility."
               "Use the header attribute instead.")
        def fget(self):
            log.warning('The CSVRenderer.headers property is deprecated. '
                        'Use CSVRenderer.header instead.')
            return self.header
        def fset(self, value):
            log.warning('The CSVRenderer.headers property is deprecated. '
                        'Use CSVRenderer.header instead.')
            self.header = value
        def fdel(self):
            log.warning('The CSVRenderer.headers property is deprecated. '
                        'Use CSVRenderer.header instead.')
            del self.header
        return locals()
    headers = property(**headers())


class CSVRendererWithUnderscores(CSVRenderer):
    level_sep = '_'


class CSVStreamingRenderer(CSVRenderer):
    def render(self, data, media_type=None, renderer_context={}):
        """
        Renders serialized *data* into CSV to be used with Django
        StreamingHttpResponse. We need to return a generator here, so Django
        can iterate over it, rendering and returning each line.

        >>> renderer = CSVStreamingRenderer()
        >>> renderer.header = ['a', 'b']
        >>> data = [{'a': 1, 'b': 2}]
        >>> from django.http import StreamingHttpResponse
        >>> response = StreamingHttpResponse(renderer.render(data),
                                             content_type='text/csv')
        >>> response['Content-Disposition'] = 'attachment; filename="f.csv"'
        >>> # return response

        """
        if data is None:
            yield ''

        self.labels = renderer_context.get('labels', self.labels)

        if not isinstance(data, GeneratorType) and not isinstance(data, list):
            data = [data]

        writer_opts = renderer_context.get('writer_opts', self.writer_opts or {})
        header = renderer_context.get('header', self.header)
        labels = renderer_context.get('labels', self.labels)
        encoding = renderer_context.get('encoding', settings.DEFAULT_CHARSET)
        bom = renderer_context.get('bom', False)

        if bom and encoding == settings.DEFAULT_CHARSET:
            yield codecs.BOM_UTF8

        table = self.tablize(data, header=header, labels=labels)
        csv_buffer = Echo()
        csv_writer = csv.writer(csv_buffer, **writer_opts)
        for row in table:
            yield csv_writer.writerow(row).encode(encoding)


class PaginatedCSVRenderer (CSVRenderer):
    """
    Paginated renderer (when pagination is turned on for DRF)
    """
    results_field = 'results'

    def render(self, data, *args, **kwargs):
        if not isinstance(data, list):
            data = data.get(self.results_field, [])
        return super(PaginatedCSVRenderer, self).render(data, *args, **kwargs)
