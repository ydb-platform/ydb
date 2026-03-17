"""
Splits tabular data in the form of a list of rows into columns;
makes guesses about the role of each column for plotting purposes
(X values, Y values, and text labels).
"""


class Column(list):
    """Store a column of tabular data; record its name and whether it is numeric"""
    is_quantity = True
    name = ""

    def __init__(self, *arg, **kwarg):
        pass


def is_quantity(val):
    """Is ``val`` a quantity (int, float, datetime, etc) (not str, bool)?
    
    Relies on presence of __sub__.
    """
    return hasattr(val, "__sub__")


class ColumnGuesserMixin(object):
    """
    plot: [x, y, y...], y
    pie: ... y
    """

    def __init__(self):
        self.keys = None

    def _build_columns(self):
        self.columns = [Column() for col in self.keys]
        for row in self:
            for (col_idx, col_val) in enumerate(row):
                col = self.columns[col_idx]
                col.append(col_val)
                if (col_val is not None) and (not is_quantity(col_val)):
                    col.is_quantity = False

        for (idx, key_name) in enumerate(self.keys):
            self.columns[idx].name = key_name

        self.x = Column()
        self.ys = []

    def _get_y(self):
        for idx in range(len(self.columns) - 1, -1, -1):
            if self.columns[idx].is_quantity:
                self.ys.insert(0, self.columns.pop(idx))
                return True

    def _get_x(self):
        for idx in range(len(self.columns)):
            if self.columns[idx].is_quantity:
                self.x = self.columns.pop(idx)
                return True

    def _get_xlabel(self, xlabel_sep=" "):
        self.xlabels = []
        if self.columns:
            for row_idx in range(len(self.columns[0])):
                self.xlabels.append(
                    xlabel_sep.join(str(c[row_idx]) for c in self.columns)
                )
        self.xlabel = ", ".join(c.name for c in self.columns)

    def _guess_columns(self):
        self._build_columns()
        self._get_y()
        if not self.ys:
            raise AttributeError("No quantitative columns found for chart")

    def guess_pie_columns(self, xlabel_sep=" "):
        """
        Assigns x, y, and x labels from the data set for a pie chart.
        
        Pie charts simply use the last quantity column as 
        the pie slice size, and everything else as the
        pie slice labels.
        """
        self._guess_columns()
        self._get_xlabel(xlabel_sep)

    def guess_plot_columns(self):
        """
        Assigns ``x`` and ``y`` series from the data set for a plot.
        
        Plots use:
          the rightmost quantity column as a Y series
          optionally, the leftmost quantity column as the X series
          any other quantity columns as additional Y series
        """
        self._guess_columns()
        self._get_x()
        while self._get_y():
            pass
