# coding: utf-8

import os
import tempfile

from django.contrib import messages
from django.core.files import File
from django.utils.translation import gettext_lazy as _

from filebrowser.settings import VERSION_QUALITY, STRICT_PIL

if STRICT_PIL:
    from PIL import Image
else:
    try:
        from PIL import Image
    except ImportError:
        import Image


def applies_to_all_images(fileobject):
    "Set image filetype"
    return fileobject.filetype == 'Image'


def transpose_image(request, fileobjects, operation):
    "Transpose image"
    for fileobject in fileobjects:
        root, ext = os.path.splitext(fileobject.filename)
        f = fileobject.site.storage.open(fileobject.path)
        im = Image.open(f)
        new_image = im.transpose(operation)
        tmpfile = File(tempfile.NamedTemporaryFile())

        try:
            new_image.save(tmpfile, format=Image.EXTENSION[ext], quality=VERSION_QUALITY, optimize=(os.path.splitext(fileobject.path)[1].lower() != '.gif'))
        except IOError:
            new_image.save(tmpfile, format=Image.EXTENSION[ext], quality=VERSION_QUALITY)

        try:
            saved_under = fileobject.site.storage.save(fileobject.path, tmpfile)
            if saved_under != fileobject.path:
                fileobject.site.storage.move(saved_under, fileobject.path, allow_overwrite=True)
            fileobject.delete_versions()
        finally:
            tmpfile.close()
            f.close()

        messages.add_message(request, messages.SUCCESS, _("Action applied successfully to '%s'" % (fileobject.filename)))


def flip_horizontal(request, fileobjects):
    "Flip image horizontally"
    transpose_image(request, fileobjects, 0)
flip_horizontal.short_description = _(u'Flip horizontal')
flip_horizontal.applies_to = applies_to_all_images


def flip_vertical(request, fileobjects):
    "Flip image vertically"
    transpose_image(request, fileobjects, 1)
flip_vertical.short_description = _(u'Flip vertical')
flip_vertical.applies_to = applies_to_all_images


def rotate_90_clockwise(request, fileobjects):
    "Rotate image 90 degrees clockwise"
    transpose_image(request, fileobjects, 4)
rotate_90_clockwise.short_description = _(u'Rotate 90° CW')
rotate_90_clockwise.applies_to = applies_to_all_images


def rotate_90_counterclockwise(request, fileobjects):
    "Rotate image 90 degrees counterclockwise"
    transpose_image(request, fileobjects, 2)
rotate_90_counterclockwise.short_description = _(u'Rotate 90° CCW')
rotate_90_counterclockwise.applies_to = applies_to_all_images


def rotate_180(request, fileobjects):
    "Rotate image 180 degrees"
    transpose_image(request, fileobjects, 3)
rotate_180.short_description = _(u'Rotate 180°')
rotate_180.applies_to = applies_to_all_images
