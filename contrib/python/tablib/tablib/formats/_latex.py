"""Tablib - LaTeX table export support.

   Generates a LaTeX booktabs-style table from the dataset.
"""
import re


class LATEXFormat:
    title = 'latex'
    extensions = ('tex',)

    TABLE_TEMPLATE = """\
%% Note: add \\usepackage{booktabs} to your preamble
%%
\\begin{table}[!htbp]
  \\centering
  %(CAPTION)s
  \\begin{tabular}{%(COLSPEC)s}
    \\toprule
%(HEADER)s
    %(MIDRULE)s
%(BODY)s
    \\bottomrule
  \\end{tabular}
\\end{table}
"""

    TEX_RESERVED_SYMBOLS_MAP = {
        '\\': '\\textbackslash{}',
        '{': '\\{',
        '}': '\\}',
        '$': '\\$',
        '&': '\\&',
        '#': '\\#',
        '^': '\\textasciicircum{}',
        '_': '\\_',
        '~': '\\textasciitilde{}',
        '%': '\\%',
    }

    TEX_RESERVED_SYMBOLS_RE = re.compile(
        '({})'.format('|'.join(map(re.escape, TEX_RESERVED_SYMBOLS_MAP.keys()))))

    @classmethod
    def export_set(cls, dataset):
        """Returns LaTeX representation of dataset

        :param dataset: dataset to serialize
        :type dataset: tablib.core.Dataset
        """

        caption = f'\\caption{{{dataset.title}}}' if dataset.title else '%'
        colspec = cls._colspec(dataset.width)
        header = cls._serialize_row(dataset.headers) if dataset.headers else ''
        midrule = cls._midrule(dataset.width)
        body = '\n'.join([cls._serialize_row(row) for row in dataset])
        return cls.TABLE_TEMPLATE % {'CAPTION': caption, 'COLSPEC': colspec,
                                     'HEADER': header, 'MIDRULE': midrule, 'BODY': body}

    @classmethod
    def _colspec(cls, dataset_width):
        """Generates the column specification for the LaTeX `tabular` environment
        based on the dataset width.

        The first column is justified to the left, all further columns are aligned
        to the right.

        .. note:: This is only a heuristic and most probably has to be fine-tuned
        post export. Column alignment should depend on the data type, e.g., textual
        content should usually be aligned to the left while numeric content almost
        always should be aligned to the right.

        :param dataset_width: width of the dataset
        """

        spec = 'l'
        for _ in range(1, dataset_width):
            spec += 'r'
        return spec

    @classmethod
    def _midrule(cls, dataset_width):
        """Generates the table `midrule`, which may be composed of several
        `cmidrules`.

        :param dataset_width: width of the dataset to serialize
        """

        if not dataset_width or dataset_width == 1:
            return '\\midrule'
        return ' '.join([cls._cmidrule(colindex, dataset_width) for colindex in
                         range(1, dataset_width + 1)])

    @classmethod
    def _cmidrule(cls, colindex, dataset_width):
        """Generates the `cmidrule` for a single column with appropriate trimming
        based on the column position.

        :param colindex: Column index
        :param dataset_width: width of the dataset
        """

        rule = '\\cmidrule(%s){%d-%d}'
        if colindex == 1:
            # Rule of first column is trimmed on the right
            return rule % ('r', colindex, colindex)
        if colindex == dataset_width:
            # Rule of last column is trimmed on the left
            return rule % ('l', colindex, colindex)
        # Inner columns are trimmed on the left and right
        return rule % ('lr', colindex, colindex)

    @classmethod
    def _serialize_row(cls, row):
        """Returns string representation of a single row.

        :param row: single dataset row
        """

        new_row = [cls._escape_tex_reserved_symbols(str(item)) if item else ''
                   for item in row]
        return 6 * ' ' + ' & '.join(new_row) + ' \\\\'

    @classmethod
    def _escape_tex_reserved_symbols(cls, string):
        """Escapes all TeX reserved symbols ('_', '~', etc.) in a string.

        :param string: String to escape
        """
        def replace(match):
            return cls.TEX_RESERVED_SYMBOLS_MAP[match.group()]
        return cls.TEX_RESERVED_SYMBOLS_RE.sub(replace, string)
