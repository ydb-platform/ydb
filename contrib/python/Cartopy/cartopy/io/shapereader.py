# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

"""
This module provides a basic interface for accessing shapefiles.
Combine the shapefile access of pyshp or fiona with the
geometry representation of shapely:

    >>> import cartopy.io.shapereader as shapereader
    >>> filename = shapereader.natural_earth(resolution='110m',
    ...                                      category='physical',
    ...                                      name='geography_regions_points')
    >>> reader = shapereader.Reader(filename)
    >>> len(reader)
    3
    >>> records = list(reader.records())
    >>> print(', '.join(str(r) for r in sorted(records[0].attributes.keys())))
    comment, ... name, name_alt, ... region, ...
    >>> print(records[0].attributes['name'])
    Niagara Falls
    >>> geoms = list(reader.geometries())
    >>> print(type(geoms[0]))
    <class 'shapely.geometry.point.Point'>
    >>> reader.close()

"""

import io
import itertools
from pathlib import Path
from urllib.error import HTTPError

from pyproj import CRS
import shapefile
import shapely.geometry as sgeom

from cartopy import config
import cartopy.crs as ccrs
from cartopy.io import Downloader


_HAS_FIONA = False
try:
    import fiona
    _HAS_FIONA = True
except ImportError:
    pass

__all__ = ["Reader", "Record"]


class Record:
    """
    A single logical entry from a shapefile, combining the attributes with
    their associated geometry.

    """

    def __init__(self, shape, attributes, fields):
        self._shape = shape

        self._bounds = None
        # if the record defines a bbox, then use that for the shape's bounds,
        # rather than using the full geometry in the bounds property
        if hasattr(shape, 'bbox'):
            self._bounds = tuple(shape.bbox)

        self._geometry = None
        """The cached geometry instance for this Record."""

        self.attributes = attributes
        """A dictionary mapping attribute names to attribute values."""

        self._fields = fields

    def __repr__(self):
        return f'<Record: {self.geometry!r}, {self.attributes!r}, <fields>>'

    def __str__(self):
        return f'Record({self.geometry}, {self.attributes}, <fields>)'

    @property
    def bounds(self):
        """
        The bounds of this Record's :meth:`~Record.geometry`.

        """
        if self._bounds is None and self.geometry is not None:
            self._bounds = self.geometry.bounds
        return self._bounds

    @property
    def geometry(self):
        """
        A shapely.geometry instance for this Record.

        The geometry may be ``None`` if a null shape is defined in the
        shapefile.

        """
        if not self._geometry and self._shape.shapeType != shapefile.NULL:
            self._geometry = sgeom.shape(self._shape)
        return self._geometry


class FionaRecord(Record):
    """
    A single logical entry from a shapefile, combining the attributes with
    their associated geometry. This extends the standard Record to work
    with the FionaReader.

    """

    def __init__(self, geometry, attributes):
        self._geometry = geometry
        self.attributes = attributes

        if geometry is not None:
            self._bounds = geometry.bounds
        else:
            self._bounds = None


class BasicReader:
    """
    Provide an interface for accessing the contents of a shapefile with the
    Python Shapefile Library (PyShp). See the PyShp
    `Readme <https://pypi.org/project/pyshp/>`_ for more information.

    The primary methods used on a BasicReader instance are
    :meth:`~cartopy.io.shapereader.BasicReader.records` and
    :meth:`~cartopy.io.shapereader.BasicReader.geometries`.

    """

    def __init__(self, filename, bbox=None, **kwargs):
        # Validate the filename/shapefile
        self._reader = reader = shapefile.Reader(filename, **kwargs)
        # Try to set the CRS from the .prj file if it exists
        self.crs = None
        try:
            with open(Path(filename).with_suffix('.prj'), 'rt') as fobj:
                self.crs = ccrs.Projection(CRS.from_wkt(fobj.read()))
        except FileNotFoundError:
            pass
        self._bbox = bbox
        if reader.shp is None or reader.shx is None or reader.dbf is None:
            raise ValueError("Incomplete shapefile definition "
                             "in '%s'." % filename)

        self._fields = self._reader.fields

    def close(self):
        return self._reader.close()

    def __len__(self):
        return len(self._reader)

    def geometries(self):
        """
        Return an iterator of shapely geometries from the shapefile.

        This interface is useful for accessing the geometries of the
        shapefile where knowledge of the associated metadata is not necessary.
        In the case where further metadata is needed use the
        :meth:`~cartopy.io.shapereader.BasicReader.records`
        interface instead, extracting the geometry from the record with the
        :meth:`~Record.geometry` method.

        """
        for shape in self._reader.iterShapes(bbox=self._bbox):
            # Skip the shape that can not be represented as geometry.
            if shape.shapeType != shapefile.NULL:
                yield sgeom.shape(shape)

    def records(self):
        """
        Return an iterator of :class:`~Record` instances.

        """

        # Ignore the "DeletionFlag" field which always comes first
        fields = self._reader.fields[1:]
        for shape_record in self._reader.iterShapeRecords(bbox=self._bbox):
            attributes = shape_record.record.as_dict()
            yield Record(shape_record.shape, attributes, fields)


class FionaReader:
    """
    Provides an interface for accessing the contents of a shapefile
    with the fiona library, which has a much faster reader than PyShp. See
    `fiona.open
    <https://fiona.readthedocs.io/en/latest/fiona.html#fiona.open>`_
    for additional information on supported kwargs.

    The primary methods used on a FionaReader instance are
    :meth:`~cartopy.io.shapereader.FionaReader.records` and
    :meth:`~cartopy.io.shapereader.FionaReader.geometries`.

    """

    def __init__(self, filename, bbox=None, **kwargs):
        self._data = []
        # Try to set the CRS from the .prj file if it exists
        self.crs = None
        try:
            with open(Path(filename).with_suffix('.prj'), 'rt') as fobj:
                self.crs = ccrs.Projection(CRS.from_wkt(fobj.read()))
        except FileNotFoundError:
            pass
        with fiona.open(filename, **kwargs) as f:
            if bbox is not None:
                assert len(bbox) == 4
                features = f.filter(bbox=bbox)
            else:
                features = f

            # Handle feature collections
            if hasattr(features, "__geo_interface__"):
                fs = features.__geo_interface__
            else:
                fs = features

            if isinstance(fs, dict) and fs.get('type') == 'FeatureCollection':
                features_lst = fs['features']
            else:
                features_lst = features

            for feature in features_lst:
                if hasattr(f, "__geo_interface__"):
                    feature = feature.__geo_interface__
                else:
                    feature = feature

                d = {'geometry': sgeom.shape(feature['geometry'])
                     if feature['geometry'] else None}
                d.update(feature['properties'])
                self._data.append(d)

    def close(self):
        # TODO: Keep the Fiona handle open until this is called.
        # This will enable us to pass down calls for bounding box queries,
        # rather than having to have it all in memory.
        pass

    def __len__(self):
        return len(self._data)

    def geometries(self):
        """
        Returns an iterator of shapely geometries from the shapefile.

        This interface is useful for accessing the geometries of the
        shapefile where knowledge of the associated metadata is desired.
        In the case where further metadata is needed use the
        :meth:`~cartopy.io.shapereader.FionaReader.records`
        interface instead, extracting the geometry from the record with the
        :meth:`~cartopy.io.shapereader.FionaRecord.geometry` method.

        """
        for item in self._data:
            yield item['geometry']

    def records(self):
        """
        Returns an iterator of :class:`~FionaRecord` instances.

        """
        for item in self._data:
            yield FionaRecord(item['geometry'],
                              {key: value for key, value in
                               item.items() if key != 'geometry'})


Reader = FionaReader if _HAS_FIONA else BasicReader
"""
Alias of the default available shapereader interface.

Will either be :class:`~cartopy.io.shapereader.FionaReader` (if fiona
installed) or :class:`~cartopy.io.shapereader.BasicReader`
(based on PyShp). Note that FionaReader has greater speed and additional
functionality, including attempting to auto-detect source encoding and
support for different format drivers. Both libraries support the 'encoding'
and 'bbox' keyword arguments. If specific functionality is needed,
BasicReader and FionaReader instances can also be created directly.

"""


def natural_earth(resolution='110m', category='physical', name='coastline'):
    """
    Return the path to the requested natural earth shapefile,
    downloading and unzipping if necessary.

    To identify valid components for this function, either browse
    NaturalEarthData.com, or if you know what you are looking for, go to
    https://github.com/nvkelso/natural-earth-vector/ to see the actual
    files which will be downloaded.

    Note
    ----
        Some of the Natural Earth shapefiles have special features which are
        described in the name. For example, the 110m resolution
        "admin_0_countries" data also has a sibling shapefile called
        "admin_0_countries_lakes" which excludes lakes in the country
        outlines. For details of what is available refer to the Natural Earth
        website, and look at the "download" link target to identify
        appropriate names.

    """
    # get hold of the Downloader (typically a NEShpDownloader instance)
    # which we can then simply call its path method to get the appropriate
    # shapefile (it will download if necessary)
    ne_downloader = Downloader.from_config(('shapefiles', 'natural_earth',
                                            resolution, category, name))
    format_dict = {'config': config, 'category': category,
                   'name': name, 'resolution': resolution}
    return ne_downloader.path(format_dict)


class NEShpDownloader(Downloader):
    """
    Specialise :class:`cartopy.io.Downloader` to download the zipped
    Natural Earth shapefiles and extract them to the defined location
    (typically user configurable).

    The keys which should be passed through when using the ``format_dict``
    are typically ``category``, ``resolution`` and ``name``.

    """
    FORMAT_KEYS = ('config', 'resolution', 'category', 'name')

    # Define the NaturalEarth URL template. Shapefiles are hosted on AWS since
    # 2021: https://github.com/nvkelso/natural-earth-vector/issues/445
    _NE_URL_TEMPLATE = ('https://naturalearth.s3.amazonaws.com/'
                        '{resolution}_{category}/ne_{resolution}_{name}.zip')

    def __init__(self,
                 url_template=_NE_URL_TEMPLATE,
                 target_path_template=None,
                 pre_downloaded_path_template='',
                 ):
        # adds some NE defaults to the __init__ of a Downloader
        Downloader.__init__(self, url_template,
                            target_path_template,
                            pre_downloaded_path_template)

    def zip_file_contents(self, format_dict):
        """
        Return a generator of the filenames to be found in the downloaded
        natural earth zip file.

        """
        for ext in ['.shp', '.dbf', '.shx', '.prj', '.cpg']:
            yield ('ne_{resolution}_{name}'
                   '{extension}'.format(extension=ext, **format_dict))

    def acquire_resource(self, target_path, format_dict):
        """
        Download the zip file and extracts the files listed in
        :meth:`zip_file_contents` to the target path.

        """
        from zipfile import ZipFile

        target_dir = Path(target_path).parent
        target_dir.mkdir(parents=True, exist_ok=True)

        url = self.url(format_dict)

        shapefile_online = self._urlopen(url)

        zfh = ZipFile(io.BytesIO(shapefile_online.read()), 'r')

        for member_path in self.zip_file_contents(format_dict):
            try:
                member = zfh.getinfo(member_path.replace("\\", "/"))
            except KeyError:
                # These extensions are required for shapefiles, so raise
                # if they are missing, continue on otherwise as the other
                # extensions are optional
                if member_path.endswith(('.shp', '.shx', '.dbf')):
                    raise
                continue
            target_path.with_suffix(Path(member_path).suffix).write_bytes(
                zfh.open(member).read()
            )

        shapefile_online.close()
        zfh.close()

        return target_path

    @staticmethod
    def default_downloader():
        """
        Return a generic, standard, NEShpDownloader instance.

        Typically, a user will not need to call this staticmethod.

        To find the path template of the NEShpDownloader:

            >>> ne_dnldr = NEShpDownloader.default_downloader()
            >>> print(ne_dnldr.target_path_template)
            {config[data_dir]}/shapefiles/natural_earth/{category}/\
ne_{resolution}_{name}.shp

        """
        default_spec = ('shapefiles', 'natural_earth', '{category}',
                        'ne_{resolution}_{name}.shp')
        ne_path_template = str(
            Path('{config[data_dir]}').joinpath(*default_spec))
        pre_path_template = str(
            Path('{config[pre_existing_data_dir]}').joinpath(*default_spec))
        return NEShpDownloader(target_path_template=ne_path_template,
                               pre_downloaded_path_template=pre_path_template)


# add a generic Natural Earth shapefile downloader to the config dictionary's
# 'downloaders' section.
_ne_key = ('shapefiles', 'natural_earth')
config['downloaders'].setdefault(_ne_key,
                                 NEShpDownloader.default_downloader())


def gshhs(scale='c', level=1):
    """
    Return the path to the requested GSHHS shapefile,
    downloading and unzipping if necessary.

    """
    # Get hold of the Downloader (typically a GSHHSShpDownloader instance)
    # and call its path method to get the appropriate shapefile (it will
    # download it if necessary).
    gshhs_downloader = Downloader.from_config(('shapefiles', 'gshhs',
                                               scale, level))
    format_dict = {'config': config, 'scale': scale, 'level': level}
    return gshhs_downloader.path(format_dict)


class GSHHSShpDownloader(Downloader):
    """
    Specialise :class:`cartopy.io.Downloader` to download the zipped
    GSHHS shapefiles and extract them to the defined location.

    The keys which should be passed through when using the ``format_dict``
    are ``scale`` (a single character indicating the resolution) and ``level``
    (a number indicating the type of feature).

    """
    FORMAT_KEYS = ('config', 'scale', 'level')

    gshhs_version = '2.3.7'

    _GSHHS_URL_TEMPLATE = (
        'https://www.ngdc.noaa.gov/mgg/shorelines/data/'
        f'gshhs/latest/gshhg-shp-{gshhs_version}.zip'
    )

    def __init__(self,
                 url_template=_GSHHS_URL_TEMPLATE,
                 target_path_template=None,
                 pre_downloaded_path_template=''):
        super().__init__(url_template, target_path_template,
                         pre_downloaded_path_template)

    def zip_file_contents(self, format_dict):
        """
        Return a generator of the filenames to be found in the downloaded
        GSHHS zip file for the specified resource.

        """
        for ext in ['.shp', '.dbf', '.shx']:
            p = Path('GSHHS_shp', '{scale}',
                     'GSHHS_{scale}_L{level}{extension}')
            yield str(p).format(extension=ext, **format_dict)

    def acquire_all_resources(self, format_dict):
        from zipfile import ZipFile

        # Download archive.
        url = self.url(format_dict)
        try:
            shapefile_online = self._urlopen(url)
        # error handling:
        except HTTPError:
            try:
                """
                case if GSHHS has had an update
                without changing the naming convention
                """
                url = (
                    f'https://www.ngdc.noaa.gov/mgg/shorelines/data/'
                    f'gshhs/oldversions/version{self.gshhs_version}/'
                    f'gshhg-shp-{self.gshhs_version}.zip'
                )
                shapefile_online = self._urlopen(url)
            except HTTPError:
                """
                case if GSHHS has had an update
                with changing the naming convention
                """
                url = (
                    'https://www.ngdc.noaa.gov/mgg/shorelines/data/'
                    'gshhs/oldversions/version2.3.6/'
                    'gshhg-shp-2.3.6.zip'
                )
                shapefile_online = self._urlopen(url)
        zfh = ZipFile(io.BytesIO(shapefile_online.read()), 'r')
        shapefile_online.close()

        # Iterate through all scales and levels and extract relevant files.
        modified_format_dict = dict(format_dict)
        scales = ('c', 'l', 'i', 'h', 'f')
        levels = (1, 2, 3, 4, 5, 6)
        for scale, level in itertools.product(scales, levels):
            # the combination c4 does not occur for some reason
            if scale == "c" and level == 4:
                continue
            modified_format_dict.update({'scale': scale, 'level': level})
            target_path = self.target_path(modified_format_dict)
            target_dir = target_path.parent
            target_dir.mkdir(parents=True, exist_ok=True)

            for member_path in self.zip_file_contents(modified_format_dict):
                member = zfh.getinfo(member_path.replace('\\', '/'))
                target_path.with_suffix(Path(member_path).suffix).write_bytes(
                    zfh.open(member).read()
                )
        zfh.close()

    def acquire_resource(self, target_path, format_dict):
        """
        Download the zip file and extracts the files listed in
        :meth:`zip_file_contents` to the target path.

        Note
        ----
            Because some of the GSHSS data is available with the cartopy
            repository, scales of "l" or "c" will not be downloaded if they
            exist in the ``cartopy.config['repo_data_dir']`` directory.

        """
        repo_fname_pattern = str(Path('shapefiles') / 'gshhs'
                                 / '{scale}' / 'GSHHS_{scale}_L?.shp')
        repo_fname_pattern = repo_fname_pattern.format(**format_dict)
        repo_fnames = list(config['repo_data_dir'].glob(repo_fname_pattern))
        if repo_fnames:
            assert len(repo_fnames) == 1, '>1 repo files found for GSHHS'
            return repo_fnames[0]
        self.acquire_all_resources(format_dict)
        if not target_path.exists():
            raise RuntimeError('Failed to download and extract GSHHS '
                               f'shapefile to {target_path!r}.')
        return target_path

    @staticmethod
    def default_downloader():
        """
        Return a GSHHSShpDownloader instance that expects (and if necessary
        downloads and installs) shapefiles in the data directory of the
        cartopy installation.

        Typically, a user will not need to call this staticmethod.

        To find the path template of the GSHHSShpDownloader:

            >>> gshhs_dnldr = GSHHSShpDownloader.default_downloader()
            >>> print(gshhs_dnldr.target_path_template)
            {config[data_dir]}/shapefiles/gshhs/{scale}/\
GSHHS_{scale}_L{level}.shp

        """
        default_spec = ('shapefiles', 'gshhs', '{scale}',
                        'GSHHS_{scale}_L{level}.shp')
        gshhs_path_template = str(
            Path('{config[data_dir]}').joinpath(*default_spec))
        pre_path_tmplt = str(
            Path('{config[pre_existing_data_dir]}').joinpath(*default_spec))
        return GSHHSShpDownloader(target_path_template=gshhs_path_template,
                                  pre_downloaded_path_template=pre_path_tmplt)


# Add a GSHHS shapefile downloader to the config dictionary's
# 'downloaders' section.
_gshhs_key = ('shapefiles', 'gshhs')
config['downloaders'].setdefault(_gshhs_key,
                                 GSHHSShpDownloader.default_downloader())
