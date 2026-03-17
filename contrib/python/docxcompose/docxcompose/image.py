import os.path


class ImageWrapper(object):
    """Image wrapper for image part creation out of an existing image part."""

    def __init__(self, img_part):
        self.sha1 = img_part.sha1
        self.filename = img_part.filename
        self.ext = os.path.splitext(self.filename)[1][1:]
        self.content_type = img_part.content_type
        self.blob = img_part.blob
