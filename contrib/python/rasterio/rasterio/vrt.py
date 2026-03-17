"""rasterio.vrt: a module concerned with GDAL VRTs"""

import xml.etree.ElementTree as ET

from rasterio._warp import WarpedVRTReaderBase
from rasterio.dtypes import _gdal_typename
from rasterio.enums import MaskFlags, Resampling
from rasterio._path import _parse_path
from rasterio.transform import TransformMethodsMixin
from rasterio.windows import WindowMethodsMixin


class WarpedVRT(WarpedVRTReaderBase, WindowMethodsMixin, TransformMethodsMixin):
    """A virtual warped dataset.

    Abstracts the details of raster warping and allows access to data
    that is reprojected when read.

    This class is backed by an in-memory GDAL VRTWarpedDataset VRT file.

    Parameters
    ----------
    src_dataset : dataset object
        The warp source.
    src_crs : CRS or str, optional
        Overrides the coordinate reference system of `src_dataset`.
    src_transform : Affine, optional
        Overrides the transform of `src_dataset`.
    src_nodata : float, optional
        Overrides the nodata value of `src_dataset`, which is the
        default.
    crs : CRS or str, optional
        The coordinate reference system at the end of the warp
        operation.  Default: the crs of `src_dataset`. dst_crs was
        a deprecated alias for this parameter.
    transform : Affine, optional
        The transform for the virtual dataset. Default: will be
        computed from the attributes of `src_dataset`. dst_transform
        was a deprecated alias for this parameter.
    height, width: int, optional
        The dimensions of the virtual dataset. Defaults: will be
        computed from the attributes of `src_dataset`. dst_height
        and dst_width were deprecated alias for these parameters.
    nodata : float, optional
        Nodata value for the virtual dataset. Default: the nodata
        value of `src_dataset` or 0.0. dst_nodata was a deprecated
        alias for this parameter.
    resampling : Resampling, optional
        Warp resampling algorithm. Default: `Resampling.nearest`.
    tolerance : float, optional
        The maximum error tolerance in input pixels when
        approximating the warp transformation. Default: 0.125,
        or one-eigth of a pixel.
    src_alpha : int, optional
        Index of a source band to use as an alpha band for warping.
    dst_alpha : int, optional
        Index of a destination band to use as an alpha band for warping.
    add_alpha : bool, optional
        Whether to add an alpha masking band to the virtual dataset.
        Default: False. This option will cause deletion of the VRT
        nodata value.
    init_dest_nodata : bool, optional
        Whether or not to initialize output to `nodata`. Default:
        True.
    warp_mem_limit : int, optional
        The warp operation's memory limit in MB. The default (0)
        means 64 MB with GDAL 2.2.
    dtype : str, optional
        The working data type for warp operation and output.
    warp_extras : dict
        Additional arguments passed to both the image to image
        transformer :cpp:func:`GDALCreateGenImgProjTransformer2` (for example,
        MAX_GCP_ORDER=2) and the :cpp:struct:`GDALWarpOptions` (for example,
        INIT_DEST=NO_DATA).

    Attributes
    ----------
    src_dataset : dataset
        The dataset object to be virtually warped.
    resampling : int
        One of the values from rasterio.enums.Resampling. The default is
        `Resampling.nearest`.
    tolerance : float
        The maximum error tolerance in input pixels when approximating
        the warp transformation. The default is 0.125.
    src_nodata: int or float, optional
        The source nodata value.  Pixels with this value will not be
        used for interpolation. If not set, it will be default to the
        nodata value of the source image, if available.
    dst_nodata: int or float, optional
        The nodata value used to initialize the destination; it will
        remain in all areas not covered by the reprojected source.
        Defaults to the value of src_nodata, or 0 (gdal default).
    working_dtype : str, optional
        The working data type for warp operation and output.
    warp_extras : dict
        Additional arguments passed to both the image to image
        transformer :cpp:func:`GDALCreateGenImgProjTransformer2` (for example,
        MAX_GCP_ORDER=2) and the :cpp:struct:`GDALWarpOptions` (for example,
        INIT_DEST=NO_DATA).

    Examples
    --------

    >>> with rasterio.open('tests/data/RGB.byte.tif') as src:
    ...     with WarpedVRT(src, crs='EPSG:3857') as vrt:
    ...         data = vrt.read()

    """

    def __repr__(self):
        return "<{} WarpedVRT name='{}' mode='{}'>".format(
            self.closed and 'closed' or 'open', self.name, self.mode)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __del__(self):
        self.close()


def _boundless_vrt_doc(
    src_dataset,
    nodata=None,
    background=None,
    hidenodata=False,
    width=None,
    height=None,
    transform=None,
    masked=False,
    resampling=Resampling.nearest,
):
    """Make a VRT XML document.

    Parameters
    ----------
    src_dataset : Dataset
        The dataset to wrap.
    background : int or float, optional
        The background fill value for the boundless VRT.
    masked : bool
        If True, the src_dataset is replaced by its valid data mask.

    Returns
    -------
    str
        An XML text string.
    """
    nodata = nodata if nodata is not None else src_dataset.nodata
    width = width or src_dataset.width
    height = height or src_dataset.height
    transform = transform or src_dataset.transform

    vrtdataset = ET.Element('VRTDataset')
    vrtdataset.attrib['rasterYSize'] = str(height)
    vrtdataset.attrib['rasterXSize'] = str(width)
    srs = ET.SubElement(vrtdataset, 'SRS')
    srs.text = src_dataset.crs.wkt if src_dataset.crs else ""
    geotransform = ET.SubElement(vrtdataset, 'GeoTransform')
    geotransform.text = ','.join([str(v) for v in transform.to_gdal()])

    for bidx, ci, block_shape, dtype in zip(
        src_dataset.indexes,
        src_dataset.colorinterp,
        src_dataset.block_shapes,
        src_dataset.dtypes,
    ):
        vrtrasterband = ET.SubElement(vrtdataset, "VRTRasterBand")
        vrtrasterband.attrib["dataType"] = _gdal_typename(dtype)
        vrtrasterband.attrib["band"] = str(bidx)

        if background is not None or nodata is not None:
            nodatavalue = ET.SubElement(vrtrasterband, 'NoDataValue')
            nodatavalue.text = str(background or nodata)

            if hidenodata:
                hidenodatavalue = ET.SubElement(vrtrasterband, 'HideNoDataValue')
                hidenodatavalue.text = "1"

        colorinterp = ET.SubElement(vrtrasterband, 'ColorInterp')
        colorinterp.text = ci.name.capitalize()

        complexsource = ET.SubElement(vrtrasterband, 'ComplexSource')
        complexsource.attrib["resampling"] = resampling.name.replace("_", "")
        sourcefilename = ET.SubElement(complexsource, 'SourceFilename')
        sourcefilename.attrib['relativeToVRT'] = "0"
        sourcefilename.attrib["shared"] = "0"
        sourcefilename.text = _parse_path(src_dataset.name).as_vsi()
        sourceband = ET.SubElement(complexsource, 'SourceBand')
        sourceband.text = str(bidx)
        sourceproperties = ET.SubElement(complexsource, 'SourceProperties')
        sourceproperties.attrib['RasterXSize'] = str(width)
        sourceproperties.attrib['RasterYSize'] = str(height)
        sourceproperties.attrib['dataType'] = _gdal_typename(dtype)
        sourceproperties.attrib['BlockYSize'] = str(block_shape[0])
        sourceproperties.attrib['BlockXSize'] = str(block_shape[1])
        srcrect = ET.SubElement(complexsource, 'SrcRect')
        srcrect.attrib['xOff'] = '0'
        srcrect.attrib['yOff'] = '0'
        srcrect.attrib['xSize'] = str(src_dataset.width)
        srcrect.attrib['ySize'] = str(src_dataset.height)
        dstrect = ET.SubElement(complexsource, 'DstRect')
        dstrect.attrib['xOff'] = str((src_dataset.transform.xoff - transform.xoff) / transform.a)
        dstrect.attrib['yOff'] = str((src_dataset.transform.yoff - transform.yoff) / transform.e)
        dstrect.attrib['xSize'] = str(src_dataset.width * src_dataset.transform.a / transform.a)
        dstrect.attrib['ySize'] = str(src_dataset.height * src_dataset.transform.e / transform.e)

        if nodata is not None:
            nodata_elem = ET.SubElement(complexsource, 'NODATA')
            nodata_elem.text = str(nodata)

        if src_dataset.options is not None:
            openoptions = ET.SubElement(complexsource, 'OpenOptions')
            for ookey, oovalue in src_dataset.options.items():
                ooi = ET.SubElement(openoptions, 'OOI')
                ooi.attrib['key'] = str(ookey)
                ooi.text = str(oovalue)

        # Effectively replaces all values of the source dataset with
        # 255.  Due to GDAL optimizations, the source dataset will not
        # be read, so we get a performance improvement.
        if masked:
            scaleratio = ET.SubElement(complexsource, 'ScaleRatio')
            scaleratio.text = '0'
            scaleoffset = ET.SubElement(complexsource, 'ScaleOffset')
            scaleoffset.text = '255'

    if all(MaskFlags.per_dataset in flags for flags in src_dataset.mask_flag_enums):
        maskband = ET.SubElement(vrtdataset, 'MaskBand')
        vrtrasterband = ET.SubElement(maskband, 'VRTRasterBand')
        vrtrasterband.attrib['dataType'] = 'Byte'

        simplesource = ET.SubElement(vrtrasterband, 'SimpleSource')
        sourcefilename = ET.SubElement(simplesource, 'SourceFilename')
        sourcefilename.attrib['relativeToVRT'] = "0"
        sourcefilename.attrib["shared"] = "0"
        sourcefilename.text = _parse_path(src_dataset.name).as_vsi()

        sourceband = ET.SubElement(simplesource, 'SourceBand')
        sourceband.text = 'mask,1'
        sourceproperties = ET.SubElement(simplesource, 'SourceProperties')
        sourceproperties.attrib['RasterXSize'] = str(width)
        sourceproperties.attrib['RasterYSize'] = str(height)
        sourceproperties.attrib['dataType'] = 'Byte'
        sourceproperties.attrib['BlockYSize'] = str(block_shape[0])
        sourceproperties.attrib['BlockXSize'] = str(block_shape[1])
        srcrect = ET.SubElement(simplesource, 'SrcRect')
        srcrect.attrib['xOff'] = '0'
        srcrect.attrib['yOff'] = '0'
        srcrect.attrib['xSize'] = str(src_dataset.width)
        srcrect.attrib['ySize'] = str(src_dataset.height)
        dstrect = ET.SubElement(simplesource, 'DstRect')
        dstrect.attrib['xOff'] = str((src_dataset.transform.xoff - transform.xoff) / transform.a)
        dstrect.attrib['yOff'] = str((src_dataset.transform.yoff - transform.yoff) / transform.e)
        dstrect.attrib['xSize'] = str(src_dataset.width)
        dstrect.attrib['ySize'] = str(src_dataset.height)

    return ET.tostring(vrtdataset).decode('ascii')
