from email.mime.image import MIMEImage
import hashlib
import os

from django import template
from django.conf import settings
from django.contrib.staticfiles import finders
from django.core.files import File
from django.core.files.images import ImageFile

register = template.Library()


@register.simple_tag(takes_context=True)
def inline_image(context, file):
    assert hasattr(context.template, '_attached_images'), (
        "You must use template engine 'post_office' when rendering images using templatetag 'inline_image'."
    )
    if isinstance(file, ImageFile):
        fileobj = file
    elif os.path.isabs(file) and os.path.exists(file):
        fileobj = File(open(file, 'rb'), name=file)
    else:
        try:
            absfilename = finders.find(file)
            if absfilename is None:
                raise FileNotFoundError(f'No such file: {file}')
        except Exception:
            if settings.DEBUG:
                raise
            return ''
        fileobj = File(open(absfilename, 'rb'), name=file)
    raw_data = fileobj.read()
    image = MIMEImage(raw_data)
    md5sum = hashlib.md5(raw_data).hexdigest()
    image.add_header('Content-Disposition', 'inline', filename=md5sum)
    image.add_header('Content-ID', f'<{md5sum}>')
    context.template._attached_images.append(image)
    return f'cid:{md5sum}'
