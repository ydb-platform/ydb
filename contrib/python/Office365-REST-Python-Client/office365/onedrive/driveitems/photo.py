from office365.runtime.client_value import ClientValue


class Photo(ClientValue):
    """The photo resource provides photo and camera properties, for example, EXIF metadata, on a driveItem."""

    def __init__(self, camera_make=None, camera_model=None):
        """
        :param str camera_make: Camera manufacturer. Read-only.
        :param str camera_make: Camera model. Read-only.
        """
        self.cameraMake = camera_make
        self.cameraModel = camera_model
