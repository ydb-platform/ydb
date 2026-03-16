# coding: utf-8

import re
import os
import unicodedata
import math

import six
from django.utils.module_loading import import_string

from filebrowser.settings import STRICT_PIL, NORMALIZE_FILENAME, CONVERT_FILENAME
from filebrowser.settings import VERSION_PROCESSORS

if STRICT_PIL:
    from PIL import Image
else:
    try:
        from PIL import Image
    except ImportError:
        import Image


# Handle Pillow 10 antialias deprecation
ANTIALIAS = Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.ANTIALIAS


def convert_filename(value):
    """
    Convert Filename.
    """

    if NORMALIZE_FILENAME:
        chunks = value.split(os.extsep)
        normalized = []
        for v in chunks:
            v = unicodedata.normalize('NFKD', six.text_type(v)).encode('ascii', 'ignore').decode('ascii')
            v = re.sub(r'[^\w\s-]', '', v).strip()
            normalized.append(v)

        if len(normalized) > 1:
            value = '.'.join(normalized)
        else:
            value = normalized[0]

    if CONVERT_FILENAME:
        value = value.replace(" ", "_").lower()

    return value


def path_strip(path, root):
    if not path or not root:
        return path
    path = os.path.normcase(path)
    root = os.path.normcase(root)
    if path.startswith(root):
        return path[len(root):]
    return path


_default_processors = None


def process_image(source, processor_options, processors=None):
    """
    Process a source PIL image through a series of image processors, returning
    the (potentially) altered image.
    """
    global _default_processors
    if processors is None:
        if _default_processors is None:
            _default_processors = [import_string(name) for name in VERSION_PROCESSORS]
        processors = _default_processors
    image = source
    for processor in processors:
        image = processor(image, **processor_options)
    return image


def scale_and_crop(im, width=None, height=None, opts='', **kwargs):
    """
    Scale and Crop.
    """
    x, y = [float(v) for v in im.size]
    width = float(width or 0)
    height = float(height or 0)

    if (x, y) == (width, height):
        return im

    if 'upscale' not in opts:
        if (x < width or not width) and (y < height or not height):
            return im

    if width:
        xr = float(width)
    else:
        xr = float(x * height / y)
    if height:
        yr = float(height)
    else:
        yr = float(y * width / x)

    if 'crop' in opts:
        r = max(xr / x, yr / y)
    else:
        r = min(xr / x, yr / y)

    if r < 1.0 or (r > 1.0 and 'upscale' in opts):
        im = im.resize((int(math.ceil(x * r)), int(math.ceil(y * r))), resample=ANTIALIAS)

    if 'crop' in opts:
        x, y = [float(v) for v in im.size]
        ex, ey = (x - min(x, xr)) / 2, (y - min(y, yr)) / 2
        if ex or ey:
            im = im.crop((int(ex), int(ey), int(ex + xr), int(ey + yr)))
    return im

scale_and_crop.valid_options = ('crop', 'upscale')
