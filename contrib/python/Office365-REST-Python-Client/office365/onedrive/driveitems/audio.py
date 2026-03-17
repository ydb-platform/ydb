from office365.runtime.client_value import ClientValue


class Audio(ClientValue):
    """
    The Audio resource groups audio-related properties on an item into a single structure.

    If a DriveItem has a non-null audio facet, the item represents an audio file.
    The properties of the Audio resource are populated by extracting metadata from the file.
    """

    def __init__(self, album=None, album_artist=None, artist=None, bitrate=None):
        """
        :param str album: The title of the album for this audio file.
        :param str album_artist: The artist named on the album for the audio file.
        :param str artist: The performing artist for the audio file.
        :param long bitrate: Bitrate expressed in kbps.
        """
        self.album = album
        self.albumArtist = album_artist
        self.artist = artist
        self.bitrate = bitrate
