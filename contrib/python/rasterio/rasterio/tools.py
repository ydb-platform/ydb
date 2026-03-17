"""Rasterio tools module

See this RFC about Rasterio tools:
https://github.com/rasterio/rasterio/issues/1300.
"""

import json

import rasterio
from rasterio.features import dataset_features


class JSONSequenceTool:
    """Extracts data from a dataset file and saves a JSON sequence
    """

    def __init__(self, func):
        """Initialize tool

        Parameters
        ----------
        func : callable
            A function or other callable object that takes a dataset and
            yields JSON serializable objects.
        """
        self.func = func

    def __call__(self, src_path, dst_path, src_kwargs=None, dst_kwargs=None, func_args=None, func_kwargs=None, config=None):
        """Execute the tool's primary function and perform file I/O

        Parameters
        ----------
        src_path : str or PathLike object
            A dataset path or URL. Will be opened in "r" mode using
            src_opts.
        dst_path : str or Path-like object
            A path or or PathLike object. Will be opened in "w" mode.
        src_kwargs : dict
            Options that will be passed to rasterio.open when opening
            src.
        dst_kwargs : dict
            Options that will be passed to json.dumps when serializing
            output.
        func_args : sequence
            Extra positional arguments for the tool's primary function.
        func_kwargs : dict
            Extra keyword arguments for the tool's primary function.
        config : dict
            Rasterio Env options.

        Returns
        -------
        None

        Side effects
        ------------
        Writes sequences of JSON texts to the named output file.
        """

        src_kwargs = src_kwargs or {}
        dst_kwargs = dst_kwargs or {}
        func_args = func_args or []
        func_kwargs = func_kwargs or {}
        config = config or {}

        with rasterio.Env(**config):
            with open(dst_path, 'w') as fdst, rasterio.open(src_path, **src_kwargs) as dsrc:
                for obj in self.func(dsrc, *func_args, **func_kwargs):
                    fdst.write(json.dumps(obj, **dst_kwargs))


dataset_features_tool = JSONSequenceTool(dataset_features)
