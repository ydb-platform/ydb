# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

"""
These are the base classes in :mod:`cartopy.io` that new resources can leverage
to implement a new reader or tile client. Together they provide a collection of
sub-packages for loading, saving and retrieving various data formats.

"""

import collections
from pathlib import Path
import string
from urllib.request import urlopen
import warnings

from cartopy import config


def fh_getter(fh, mode='r', needs_filename=False):
    """
    Convenience function for opening files.

    Parameters
    ----------
    fh
        File handle, filename or (file handle, filename) tuple.
    mode: optional
        Open mode. Defaults to "r".
    needs_filename: optional
        Defaults to False

    Returns
    -------
    file handle, filename
        Opened in the given mode.

    """
    if mode != 'r':
        raise ValueError('Only mode "r" currently supported.')

    filename = None

    if isinstance(fh, str):
        filename = fh
        fh = open(fh, mode)
    elif isinstance(fh, tuple):
        fh, filename = fh

    if filename is None:
        try:
            filename = fh.name
        except AttributeError:  # does this occur?
            if needs_filename:
                raise ValueError('filename cannot be determined')
            else:
                filename = ''

    return fh, filename


class DownloadWarning(Warning):
    """Issued when a file is being downloaded by a :class:`Downloader`."""
    pass


class Downloader:
    """
    Represents a resource, that can be configured easily, which knows
    how to acquire itself (perhaps via HTTP).

    The key interface method is :meth:`path` - typically *all* external calls
    will be made to that method. To get hold of an appropriate
    :class:`Downloader` instance the :func:`Downloader.from_config` static
    method should be considered.

    Parameters
    ----------
    url_template
        The template of the full URL representing this
        resource.

    target_path_template
        The template of the full path to the file
        that this Downloader represents. Typically the path will be a
        subdirectory of ``config['data_dir']``, but this is not a strict
        requirement. If the file does not exist when calling
        :meth:`Downloader.path` it will be downloaded to this location.

    pre_downloaded_path_template: optional
        The template of a full path of a file which has been downloaded
        outside of this Downloader which should be used as the file that
        this resource represents. If the file does not exist when
        :meth:`Downloader.path` is called it will not be downloaded
        to this location (unlike the ``target_path_template`` argument).

    Note
    ----
        All ``*_template`` arguments should be formattable using the
        standard :meth:`string.format` rules. The formatting itself
        is not done until a call to a subsequent method (such as
        :meth:`Downloader.path`).

    """

    FORMAT_KEYS = ('config',)
    """
    The minimum keys which should be provided in the ``format_dict``
    argument for the ``path``, ``url``, ``target_path``,
    ``pre_downloaded_path`` and ``acquire_resource`` methods.

    """

    def __init__(self, url_template, target_path_template,
                 pre_downloaded_path_template=''):
        self.url_template = url_template
        self.target_path_template = target_path_template
        self.pre_downloaded_path_template = pre_downloaded_path_template

        # define a formatter which will process the templates. Subclasses
        # may override the standard ``''.format`` formatting by defining
        # their own formatter subclass here.
        self._formatter = string.Formatter()

    def url(self, format_dict):
        """
        The full URL that this resource represents.

        Parameters
        ----------
        format_dict
            The dictionary which is used to replace certain
            template variables. Subclasses should document which keys are
            expected as a minimum in their ``FORMAT_KEYS`` class attribute.

        """
        return self._formatter.format(self.url_template, **format_dict)

    def target_path(self, format_dict):
        """
        The path on disk of the file that this resource represents, must
        either exist, or be writable by the current user. This method
        does not check either of these conditions.

        Parameters
        ----------
        format_dict
            The dictionary which is used to replace certain
            template variables. Subclasses should document which keys are
            expected as a minimum in their ``FORMAT_KEYS`` class attribute.

        """
        return Path(self._formatter.format(self.target_path_template,
                                           **format_dict))

    def pre_downloaded_path(self, format_dict):
        """
        The path on disk of the file that this resource represents, if it does
        not exist, then no further action will be taken with this path, and all
        further processing will be done using :meth:`target_path` instead.

        Parameters
        ----------
        format_dict
            The dictionary which is used to replace certain
            template variables. Subclasses should document which keys are
            expected as a minimum in their ``FORMAT_KEYS`` class attribute.

        """
        p = self._formatter.format(self.pre_downloaded_path_template,
                                   **format_dict)
        return None if p == '' else Path(p)

    def path(self, format_dict):
        """
        Returns the path to a file on disk that this resource represents.

        If the file doesn't exist in :meth:`pre_downloaded_path` then it
        will check whether it exists in :meth:`target_path`, otherwise
        the resource will be downloaded via :meth:`acquire_resouce` from
        :meth:`url` to :meth:`target_path`.

        Typically, this is the method that most applications will call,
        allowing implementers of new Downloaders to specialise
        :meth:`acquire_resource`.

        Parameters
        ----------
        format_dict
            The dictionary which is used to replace certain
            template variables. Subclasses should document which keys are
            expected as a minimum in their ``FORMAT_KEYS`` class attribute.

        """
        pre_downloaded_path = self.pre_downloaded_path(format_dict)
        target_path = self.target_path(format_dict)
        if pre_downloaded_path is not None and pre_downloaded_path.exists():
            result_path = pre_downloaded_path
        elif target_path.exists():
            result_path = target_path
        else:
            # we need to download the file
            result_path = self.acquire_resource(target_path, format_dict)

        return result_path

    def acquire_resource(self, target_path, format_dict):
        """
        Download, via HTTP, the file that this resource represents.
        Subclasses will typically override this method.

        Parameters
        ----------
        format_dict
            The dictionary which is used to replace certain
            template variables. Subclasses should document which keys are
            expected as a minimum in their ``FORMAT_KEYS`` class attribute.

        """
        target_path = Path(target_path)
        target_dir = target_path.parent
        target_dir.mkdir(parents=True, exist_ok=True)

        url = self.url(format_dict)

        # try getting the resource (no exception handling, just let it raise)
        response = self._urlopen(url)

        target_path.write_bytes(response.read())

        return target_path

    def _urlopen(self, url):
        """
        Returns a file handle to the given HTTP resource URL.

        Caller should close the file handle when finished with it.

        """
        warnings.warn(f'Downloading: {url}', DownloadWarning)
        return urlopen(url)

    @staticmethod
    def from_config(specification, config_dict=None):
        """
        The ``from_config`` static method implements the logic for acquiring a
        Downloader (sub)class instance from the config dictionary.

        Parameters
        ----------
        specification
            Should be iterable, as it will be traversed in
            reverse order to find the most appropriate Downloader instance
            for this specification.  An example specification is
            ``('shapefiles', 'natural_earth')`` for the Natural Earth
            shapefiles.
        config_dict: optional
            typically this is left as None to use the
            default ``cartopy.config`` "downloaders" dictionary.

        Examples
        --------

        >>> from cartopy.io import Downloader
        >>>
        >>> dnldr = Downloader('https://example.com/{name}', './{name}.txt')
        >>> config = {('level_1', 'level_2'): dnldr}
        >>> d1 = Downloader.from_config(('level_1', 'level_2', 'level_3'),
        ...                             config_dict=config)
        >>> print(d1.url_template)
        https://example.com/{name}
        >>> print(d1.url({'name': 'item_name'}))
        https://example.com/item_name

        """
        spec_depth = len(specification)
        if config_dict is None:
            downloaders = config['downloaders']
        else:
            downloaders = config_dict

        result_downloader = None

        for i in range(spec_depth, 0, -1):
            lookup = specification[:i]
            downloadable_item = downloaders.get(lookup, None)
            if downloadable_item is not None:
                result_downloader = downloadable_item
                break

        if result_downloader is None:
            # should never really happen, but could if the user does
            # some strange things like not having any downloaders defined
            # in the config...
            raise ValueError('No generic downloadable item in the config '
                             f'dictionary for {specification}')

        return result_downloader


class LocatedImage(collections.namedtuple('LocatedImage', 'image, extent')):
    """
    Define an image and associated extent in the form:
       ``image, (min_x, max_x, min_y, max_y)``

    """


class RasterSource:
    """
    Define the cartopy raster fetching interface.

    A :class:`RasterSource` instance is able to supply images and
    associated extents (as a sequence of :class:`LocatedImage` instances)
    through its :meth:`~RasterSource.fetch_raster` method.

    As a result, further interfacing classes, such as
    :class:`cartopy.mpl.slippy_image_artist.SlippyImageArtist`, can then
    make use of the interface for functionality such as interactive image
    retrieval with pan and zoom functionality.

    .. _raster-source-interface:

    """

    def validate_projection(self, projection):
        """
        Raise an error if this raster source cannot provide images in the
        specified projection.

        Parameters
        ----------
        projection: :class:`cartopy.crs.Projection`
            The desired projection of the image.

        """
        raise NotImplementedError()

    def fetch_raster(self, projection, extent, target_resolution):
        """
        Return a sequence of images with extents given some constraining
        information.

        Parameters
        ----------
        projection: :class:`cartopy.crs.Projection`
            The desired projection of the image.
        extent: iterable of length 4
            The extent of the requested image in projected coordinates.
            The resulting image may not be defined exactly by these extents,
            and so the extent of the resulting image is also returned. The
            extents must be defined in the form
            ``(min_x, max_x, min_y, max_y)``.
        target_resolution: iterable of length 2
            The desired resolution of the image as ``(width, height)`` in
            pixels.

        Returns
        -------
        images
            A sequence of :class:`LocatedImage` instances.

        """
        raise NotImplementedError()


class RasterSourceContainer(RasterSource):
    """
    A container which simply calls the appropriate methods on the
    contained :class:`RasterSource`.

    """

    def __init__(self, contained_source):
        """
        Parameters
        ----------
        contained_source: :class:`RasterSource` instance.
            The source of the raster that this container is wrapping.

        """
        self._source = contained_source

    def fetch_raster(self, projection, extent, target_resolution):
        return self._source.fetch_raster(projection, extent,
                                         target_resolution)

    def validate_projection(self, projection):
        return self._source.validate_projection(projection)


class PostprocessedRasterSource(RasterSourceContainer):
    """
    A :class:`RasterSource` which wraps another, an then applies a
    post-processing step on the raster fetched from the contained source.

    """

    def __init__(self, contained_source, img_post_process):
        """
        Parameters
        ----------
        contained_source: :class:`RasterSource` instance.
            The source of the raster that this container is wrapping.
        img_post_process: callable
            Called after each `fetch_raster` call which yields a non-None
            image result. The callable must accept the :class:`LocatedImage`
            from the contained fetch_raster as its only argument, and must
            return a single LocatedImage.

        """
        super().__init__(contained_source)
        self._post_fetch_fn = img_post_process

    def fetch_raster(self, *args, **kwargs):
        fetch_raster = super().fetch_raster
        located_imgs = fetch_raster(*args, **kwargs)
        if located_imgs:
            located_imgs = [self._post_fetch_fn(img) for img in located_imgs]
        return located_imgs
