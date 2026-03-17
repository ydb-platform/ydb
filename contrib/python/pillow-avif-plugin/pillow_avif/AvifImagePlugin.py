from __future__ import division

from io import BytesIO
import sys

from PIL import ExifTags, Image, ImageFile

try:
    from pillow_avif import _avif

    SUPPORTED = True
except ImportError:
    SUPPORTED = False

# Decoder options as module globals, until there is a way to pass parameters
# to Image.open (see https://github.com/python-pillow/Pillow/issues/569)
DECODE_CODEC_CHOICE = "auto"
CHROMA_UPSAMPLING = "auto"
# Decoding is only affected by this for libavif **0.8.4** or greater.
DEFAULT_MAX_THREADS = 0

if sys.version_info[0] == 2:
    text_type = unicode  # noqa
else:
    text_type = str


def _accept(prefix):
    if prefix[4:8] != b"ftyp":
        return False
    major_brand = prefix[8:12]
    if major_brand in (
        # coding brands
        b"avif",
        b"avis",
        # We accept files with AVIF container brands; we can't yet know if
        # the ftyp box has the correct compatible brands, but if it doesn't
        # then the plugin will raise a SyntaxError which Pillow will catch
        # before moving on to the next plugin that accepts the file.
        #
        # Also, because this file might not actually be an AVIF file, we
        # don't raise an error if AVIF support isn't properly compiled.
        b"mif1",
        b"msf1",
    ):
        if not SUPPORTED:
            return (
                "image file could not be identified because AVIF support not installed"
            )
        return True
    return False


class AvifImageFile(ImageFile.ImageFile):
    format = "AVIF"
    format_description = "AVIF image"
    __frame = -1

    def _open(self):
        if not SUPPORTED:
            msg = "image file could not be opened because AVIF support not installed"
            raise SyntaxError(msg)

        if DECODE_CODEC_CHOICE != "auto" and not _avif.decoder_codec_available(
            DECODE_CODEC_CHOICE
        ):
            msg = "Invalid opening codec"
            raise ValueError(msg)
        self._decoder = _avif.AvifDecoder(
            self.fp.read(), DECODE_CODEC_CHOICE, CHROMA_UPSAMPLING, DEFAULT_MAX_THREADS
        )

        # Get info from decoder
        (
            width,
            height,
            self.n_frames,
            mode,
            icc,
            exif,
            exif_orientation,
            xmp,
        ) = self._decoder.get_info()
        self._size = (width, height)
        self.is_animated = self.n_frames > 1
        try:
            self.mode = self.rawmode = mode
        except AttributeError:
            self._mode = self.rawmode = mode

        if icc:
            self.info["icc_profile"] = icc
        if xmp:
            self.info["xmp"] = xmp

        if exif_orientation != 1 or exif:
            exif_data = Image.Exif()
            orientation_tag = next(
                k for k, v in ExifTags.TAGS.items() if v == "Orientation"
            )
            if exif:
                exif_data.load(exif)
                original_orientation = exif_data.get(orientation_tag, 1)
            else:
                original_orientation = 1
            if exif_orientation != original_orientation:
                exif_data[orientation_tag] = exif_orientation
                exif = exif_data.tobytes()
        if exif:
            self.info["exif"] = exif
        self.seek(0)

    def seek(self, frame):
        if not self._seek_check(frame):
            return

        # Set tile
        self.__frame = frame
        if hasattr(ImageFile, "_Tile"):
            self.tile = [ImageFile._Tile("raw", (0, 0) + self.size, 0, self.mode)]
        else:
            self.tile = [("raw", (0, 0) + self.size, 0, self.mode)]

    def load(self):
        if self.tile:
            # We need to load the image data for this frame
            (
                data,
                timescale,
                pts_in_timescales,
                duration_in_timescales,
            ) = self._decoder.get_frame(self.__frame)
            self.info["timestamp"] = round(1000 * (pts_in_timescales / timescale))
            self.info["duration"] = round(1000 * (duration_in_timescales / timescale))

            if self.fp and self._exclusive_fp:
                self.fp.close()
            self.fp = BytesIO(data)

        return super(AvifImageFile, self).load()

    def load_seek(self, pos):
        pass

    def tell(self):
        return self.__frame


def _save_all(im, fp, filename):
    _save(im, fp, filename, save_all=True)


def _save(im, fp, filename, save_all=False):
    info = im.encoderinfo.copy()
    if save_all:
        append_images = list(info.get("append_images", []))
    else:
        append_images = []

    total = 0
    for ims in [im] + append_images:
        total += getattr(ims, "n_frames", 1)

    qmin = info.get("qmin", -1)
    qmax = info.get("qmax", -1)
    quality = info.get("quality", 75)
    if not isinstance(quality, int) or quality < 0 or quality > 100:
        msg = "Invalid quality setting"
        raise ValueError(msg)

    duration = info.get("duration", 0)
    subsampling = info.get("subsampling", "4:2:0")
    speed = info.get("speed", 6)
    max_threads = info.get("max_threads", DEFAULT_MAX_THREADS)
    codec = info.get("codec", "auto")
    if codec != "auto" and not _avif.encoder_codec_available(codec):
        msg = "Invalid saving codec"
        raise ValueError(msg)
    range_ = info.get("range", "full")
    tile_rows_log2 = info.get("tile_rows", 0)
    tile_cols_log2 = info.get("tile_cols", 0)
    alpha_premultiplied = bool(info.get("alpha_premultiplied", False))
    autotiling = bool(info.get("autotiling", tile_rows_log2 == tile_cols_log2 == 0))

    icc_profile = info.get("icc_profile", im.info.get("icc_profile"))
    exif = info.get("exif", im.info.get("exif"))
    if isinstance(exif, Image.Exif):
        exif = exif.tobytes()

    exif_orientation = 0
    if exif:
        exif_data = Image.Exif()
        try:
            exif_data.load(exif)
        except SyntaxError:
            pass
        else:
            orientation_tag = next(
                k for k, v in ExifTags.TAGS.items() if v == "Orientation"
            )
            exif_orientation = exif_data.get(orientation_tag) or 0

    xmp = info.get("xmp", im.info.get("xmp") or im.info.get("XML:com.adobe.xmp"))

    if isinstance(xmp, text_type):
        xmp = xmp.encode("utf-8")

    advanced = info.get("advanced")
    if advanced is not None:
        if isinstance(advanced, dict):
            advanced = tuple(advanced.items())
        try:
            advanced = tuple(advanced)
        except TypeError:
            invalid = True
        else:
            invalid = any(not isinstance(v, tuple) or len(v) != 2 for v in advanced)
        if invalid:
            msg = (
                "advanced codec options must be a dict of key-value string "
                "pairs or a series of key-value two-tuples"
            )
            raise ValueError(msg)
        advanced = tuple(
            [(str(k).encode("utf-8"), str(v).encode("utf-8")) for k, v in advanced]
        )

    # Setup the AVIF encoder
    enc = _avif.AvifEncoder(
        im.size[0],
        im.size[1],
        subsampling,
        qmin,
        qmax,
        quality,
        speed,
        max_threads,
        codec,
        range_,
        tile_rows_log2,
        tile_cols_log2,
        alpha_premultiplied,
        autotiling,
        icc_profile or b"",
        exif or b"",
        exif_orientation,
        xmp or b"",
        advanced,
    )

    # Add each frame
    frame_idx = 0
    frame_duration = 0
    cur_idx = im.tell()
    is_single_frame = total == 1
    try:
        for ims in [im] + append_images:
            # Get # of frames in this image
            nfr = getattr(ims, "n_frames", 1)

            for idx in range(nfr):
                ims.seek(idx)
                ims.load()

                # Make sure image mode is supported
                frame = ims
                rawmode = ims.mode
                if ims.mode not in {"RGB", "RGBA"}:
                    alpha = (
                        "A" in ims.mode
                        or "a" in ims.mode
                        or (ims.mode == "P" and "A" in ims.im.getpalettemode())
                        or (
                            ims.mode == "P"
                            and ims.info.get("transparency", None) is not None
                        )
                    )
                    rawmode = "RGBA" if alpha else "RGB"
                    frame = ims.convert(rawmode)

                # Update frame duration
                if isinstance(duration, (list, tuple)):
                    frame_duration = duration[frame_idx]
                else:
                    frame_duration = duration

                # Append the frame to the animation encoder
                enc.add(
                    frame.tobytes("raw", rawmode),
                    int(frame_duration),
                    frame.size[0],
                    frame.size[1],
                    rawmode,
                    is_single_frame,
                )

                # Update frame index
                frame_idx += 1

                if not save_all:
                    break

    finally:
        im.seek(cur_idx)

    # Get the final output from the encoder
    data = enc.finish()
    if data is None:
        msg = "cannot write file as AVIF (encoder returned None)"
        raise OSError(msg)

    fp.write(data)


# Prevent Pillow's AVIF plugin from replacing this plugin
try:
    from PIL import AvifImagePlugin  # noqa: F401
except ImportError:
    pass

Image.register_open(AvifImageFile.format, AvifImageFile, _accept)
if SUPPORTED:
    Image.register_save(AvifImageFile.format, _save)
    Image.register_save_all(AvifImageFile.format, _save_all)
    Image.register_extensions(AvifImageFile.format, [".avif", ".avifs"])
    Image.register_mime(AvifImageFile.format, "image/avif")
