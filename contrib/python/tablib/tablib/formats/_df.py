""" Tablib - DataFrame Support.
"""

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None


class DataFrameFormat:
    title = 'df'
    extensions = ('df',)

    @classmethod
    def detect(cls, stream):
        """Returns True if given stream is a DataFrame."""
        if DataFrame is None:
            return False
        elif isinstance(stream, DataFrame):
            return True
        try:
            DataFrame(stream.read())
            return True
        except ValueError:
            return False

    @classmethod
    def export_set(cls, dset):
        """Returns DataFrame representation of DataBook."""
        if DataFrame is None:
            raise NotImplementedError(
                'DataFrame Format requires `pandas` to be installed.'
                ' Try `pip install "tablib[pandas]"`.')
        dataframe = DataFrame(dset.dict, columns=dset.headers)
        return dataframe

    @classmethod
    def import_set(cls, dset, in_stream):
        """Returns dataset from DataFrame."""
        dset.wipe()
        dset.dict = in_stream.to_dict(orient='records')
