"""File translation command"""

import click
import numpy as np

import rasterio
from rasterio.enums import MaskFlags
from rasterio.rio import options
from rasterio.rio.helpers import resolve_inout
from rasterio.windows import subdivide, Window


@click.command(short_help="Copy and convert raster dataset.")
@options.files_inout_arg
@options.output_opt
@options.format_opt
@options.dtype_opt
@click.option('--scale-ratio', type=float, default=None,
              help="Source to destination scaling ratio.")
@click.option('--scale-offset', type=float, default=None,
              help="Source to destination scaling offset.")
@options.rgb_opt
@options.overwrite_opt
@options.creation_options
@click.pass_context
def convert(
        ctx, files, output, driver, dtype, scale_ratio, scale_offset,
        photometric, overwrite, creation_options):
    """Copy and convert raster datasets to other data types and formats.

    Data values may be linearly scaled when copying by using the
    --scale-ratio and --scale-offset options. Destination raster values
    are calculated as

      dst = scale_ratio * src + scale_offset

    For example, to scale uint16 data with an actual range of 0-4095 to
    0-255 as uint8:

      $ rio convert in16.tif out8.tif --dtype uint8 --scale-ratio 0.0625

    Format specific creation options may also be passed using --co. To
    tile a new GeoTIFF output file, do the following.

      --co tiled=true --co blockxsize=256 --co blockysize=256

    To compress it using the LZW method, add

      --co compress=LZW

    """
    with ctx.obj['env']:

        outputfile, files = resolve_inout(
            files=files, output=output, overwrite=overwrite, num_inputs=1
        )
        inputfile = files[0]

        with rasterio.open(inputfile) as src:

            # Use the input file's profile, updated by CLI
            # options, as the profile for the output file.
            profile = src.profile

            profile.pop('driver', None)
            if driver:
                profile['driver'] = driver
            if dtype:
                profile['dtype'] = dtype
            dst_dtype = profile['dtype']

            if photometric:
                creation_options['photometric'] = photometric

            profile.update(**creation_options)

            bxsize = bysize = 1024
            if profile.get('tiled', False):
                bxsize = int(profile.get('blockxsize', bxsize))
                bysize = int(profile.get('blockysize', bysize))
            srcds = Window(0, 0, src.width, src.height)

            with rasterio.open(outputfile, 'w', **profile) as dst:

                for chunk in subdivide(srcds, bxsize, bysize):
                    data = src.read(window=chunk, masked=True)

                    if scale_ratio:
                        # Cast to float64 before multiplying.
                        data = data.astype('float64', casting='unsafe', copy=False)
                        np.multiply(
                            data, scale_ratio, out=data, casting='unsafe')

                    if scale_offset:
                        # My understanding of copy=False is that this is a
                        # no-op if the array was cast for multiplication.
                        data = data.astype('float64', casting='unsafe', copy=False)
                        np.add(
                            data, scale_offset, out=data, casting='unsafe')

                    # Cast to the output dtype and write.
                    result = data.astype(dst_dtype, casting='unsafe', copy=False)
                    dst.write(result, window=chunk)

                    if MaskFlags.per_dataset in src.mask_flag_enums[0]:
                        dst.write_mask(src.read_masks(window=chunk)[0], window=chunk)

                    # TODO: copy other properties (GCPs etc). Several other
                    # programs need the same utility.
