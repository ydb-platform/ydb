import codecs
import csv
import operator
import os.path
import re
import traceback
from functools import reduce

import prettytable
import six
import sqlalchemy
import sqlparse

from .column_guesser import ColumnGuesserMixin

try:
    from pgspecial.main import PGSpecial
except ImportError:
    PGSpecial = None


def unduplicate_field_names(field_names):
    """Append a number to duplicate field names to make them unique. """
    res = []
    for k in field_names:
        if k in res:
            i = 1
            while k + "_" + str(i) in res:
                i += 1
            k += "_" + str(i)
        res.append(k)
    return res


class UnicodeWriter(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = six.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        if six.PY2:
            _row = [s.encode("utf-8") if hasattr(s, "encode") else s for s in row]
        else:
            _row = row
        self.writer.writerow(_row)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        if six.PY2:
            data = data.decode("utf-8")
            # ... and re-encode it into the target encoding
            data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)
        self.queue.seek(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class CsvResultDescriptor(object):
    """Provides IPython Notebook-friendly output for the feedback after a ``.csv`` called."""

    def __init__(self, file_path):
        self.file_path = file_path

    def __repr__(self):
        return "CSV results at %s" % os.path.join(os.path.abspath("."), self.file_path)

    def _repr_html_(self):
        return '<a href="%s">CSV results</a>' % os.path.join(
            ".", "files", self.file_path
        )


def _nonbreaking_spaces(match_obj):
    """
    Make spaces visible in HTML by replacing all `` `` with ``&nbsp;``

    Call with a ``re`` match object.  Retain group 1, replace group 2
    with nonbreaking speaces.
    """
    spaces = "&nbsp;" * len(match_obj.group(2))
    return "%s%s" % (match_obj.group(1), spaces)


_cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")


class ResultSet(list, ColumnGuesserMixin):
    """
    Results of a SQL query.

    Can access rows listwise, or by string value of leftmost column.
    """

    def __init__(self, sqlaproxy, config):
        self.config = config
        if sqlaproxy.returns_rows:
            self.keys = sqlaproxy.keys()
            if config.autolimit:
                list.__init__(self, sqlaproxy.fetchmany(size=config.autolimit))
            else:
                list.__init__(self, sqlaproxy.fetchall())
            self.field_names = unduplicate_field_names(self.keys)
            self.pretty = PrettyTable(self.field_names, style=prettytable.__dict__[config.style.upper()])
        else:
            list.__init__(self, [])
            self.pretty = None

    def _repr_html_(self):
        _cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")
        if self.pretty:
            self.pretty.add_rows(self)
            result = self.pretty.get_html_string()
            result = _cell_with_spaces_pattern.sub(_nonbreaking_spaces, result)
            if self.config.displaylimit and len(self) > self.config.displaylimit:
                result = (
                    '%s\n<span style="font-style:italic;text-align:center;">%d rows, truncated to displaylimit of %d</span>'
                    % (result, len(self), self.config.displaylimit)
                )
            return result
        else:
            return None

    def __str__(self, *arg, **kwarg):
        self.pretty.add_rows(self)
        return str(self.pretty or "")

    def __getitem__(self, key):
        """
        Access by integer (row position within result set)
        or by string (value of leftmost column)
        """
        try:
            return list.__getitem__(self, key)
        except TypeError:
            result = [row for row in self if row[0] == key]
            if not result:
                raise KeyError(key)
            if len(result) > 1:
                raise KeyError('%d results for "%s"' % (len(result), key))
            return result[0]

    def dict(self):
        """Returns a single dict built from the result set

        Keys are column names; values are a tuple"""
        return dict(zip(self.keys, zip(*self)))

    def dicts(self):
        """Iterator yielding a dict for each row"""
        for row in self:
            yield dict(zip(self.keys, row))

    def DataFrame(self):
        """Returns a Pandas DataFrame instance built from the result set."""
        import pandas as pd

        frame = pd.DataFrame(self, columns=(self and self.keys) or [])
        return frame

    def pie(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab pie chart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        Values (pie slice sizes) are taken from the
        rightmost column (numerical values required).
        All other columns are used to label the pie slices.

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.pie``.
        """
        self.guess_pie_columns(xlabel_sep=key_word_sep)
        import matplotlib.pylab as plt

        pie = plt.pie(self.ys[0], labels=self.xlabels, **kwargs)
        plt.title(title or self.ys[0].name)
        return pie

    def plot(self, title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The first and last columns are taken as the X and Y
        values.  Any columns between are ignored.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """
        import matplotlib.pylab as plt

        self.guess_plot_columns()
        self.x = self.x or range(len(self.ys[0]))
        coords = reduce(operator.add, [(self.x, y) for y in self.ys])
        plot = plt.plot(*coords, **kwargs)
        if hasattr(self.x, "name"):
            plt.xlabel(self.x.name)
        ylabel = ", ".join(y.name for y in self.ys)
        plt.title(title or ylabel)
        plt.ylabel(ylabel)
        return plot

    def bar(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab bar plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The last quantitative column is taken as the Y values;
        all other columns are combined to label the X axis.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns
        key_word_sep: string used to separate column values
                      from each other in labels

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.bar``.
        """
        import matplotlib.pylab as plt

        self.guess_pie_columns(xlabel_sep=key_word_sep)
        plot = plt.bar(range(len(self.ys[0])), self.ys[0], **kwargs)
        if self.xlabels:
            plt.xticks(range(len(self.xlabels)), self.xlabels, rotation=45)
        plt.xlabel(self.xlabel)
        plt.ylabel(self.ys[0].name)
        return plot

    def csv(self, filename=None, **format_params):
        """Generate results in comma-separated form.  Write to ``filename`` if given.
           Any other parameters will be passed on to csv.writer."""
        if not self.pretty:
            return None  # no results
        self.pretty.add_rows(self)
        if filename:
            encoding = format_params.get("encoding", "utf-8")
            if six.PY2:
                outfile = open(filename, "wb")
            else:
                outfile = open(filename, "w", newline="", encoding=encoding)
        else:
            outfile = six.StringIO()
        writer = UnicodeWriter(outfile, **format_params)
        writer.writerow(self.field_names)
        for row in self:
            writer.writerow(row)
        if filename:
            outfile.close()
            return CsvResultDescriptor(filename)
        else:
            return outfile.getvalue()


def interpret_rowcount(rowcount):
    if rowcount < 0:
        result = "Done."
    else:
        result = "%d rows affected." % rowcount
    return result


class FakeResultProxy(object):
    """A fake class that pretends to behave like the ResultProxy from
    SqlAlchemy.
    """

    def __init__(self, cursor, headers):
        if cursor is None:
            cursor = []
            headers = []
        if isinstance(cursor, list):
            self.from_list(source_list=cursor)
        else:
            self.fetchall = cursor.fetchall
            self.fetchmany = cursor.fetchmany
            self.rowcount = cursor.rowcount
        self.keys = lambda: headers
        self.returns_rows = True

    def from_list(self, source_list):
        """Simulates SQLA ResultProxy from a list."""

        self.fetchall = lambda: source_list
        self.rowcount = len(source_list)

        def fetchmany(size):
            pos = 0
            while pos < len(source_list):
                yield source_list[pos: pos + size]
                pos += size

        self.fetchmany = fetchmany


# some dialects have autocommit
# specific dialects break when commit is used:

_COMMIT_BLACKLIST_DIALECTS = ("athena", "bigquery", "clickhouse", "ingres", "mssql", "teradata", "vertica")


def _commit(conn, config):
    """Issues a commit, if appropriate for current config and dialect"""

    _should_commit = config.autocommit and all(
        dialect not in str(conn.dialect) for dialect in _COMMIT_BLACKLIST_DIALECTS
    )

    if _should_commit:
        try:
            conn.internal_connection.commit()
        except sqlalchemy.exc.OperationalError:
            pass  # not all engines can commit
        except Exception as ex:
            conn.internal_connection.rollback()
            print(traceback.format_exc())
            raise ex


def run(conn, sql, config, user_namespace):
    if sql.strip():
        for statement in sqlparse.split(sql):
            first_word = sql.strip().split()[0].lower()
            if first_word == "begin":
                raise Exception("ipython_sql does not support transactions")
            if first_word.startswith("\\") and \
                ("postgres" in str(conn.dialect) or
                 "redshift" in str(conn.dialect)):
                if not PGSpecial:
                    raise ImportError("pgspecial not installed")
                pgspecial = PGSpecial()
                _, cur, headers, _ = pgspecial.execute(
                    conn.internal_connection.connection.cursor(), statement
                )[0]
                result = FakeResultProxy(cur, headers)
            else:
                txt = sqlalchemy.sql.text(statement)
                result = conn.internal_connection.execute(txt, user_namespace)
            _commit(conn=conn, config=config)
            if result and config.feedback:
                print(interpret_rowcount(result.rowcount))
        resultset = ResultSet(result, config)
        if config.autopandas:
            return resultset.DataFrame()
        else:
            return resultset
        # returning only last result, intentionally
    else:
        return "Connected: %s" % conn.name


class PrettyTable(prettytable.PrettyTable):
    def __init__(self, *args, **kwargs):
        self.row_count = 0
        self.displaylimit = None
        return super(PrettyTable, self).__init__(*args, **kwargs)

    def add_rows(self, data):
        if self.row_count and (data.config.displaylimit == self.displaylimit):
            return  # correct number of rows already present
        self.clear_rows()
        self.displaylimit = data.config.displaylimit
        if self.displaylimit == 0:
            self.displaylimit = None  # TODO: remove this to make 0 really 0
        if self.displaylimit in (None, 0):
            self.row_count = len(data)
        else:
            self.row_count = min(len(data), self.displaylimit)
        for row in data[: self.displaylimit]:
            self.add_row(row)
