#
# These test cover the Image attributes that directly map to C-API functions.
#
import io
import numbers

from pytest import raises, mark

from wand.color import Color
from wand.font import Font
from wand.image import Image
from wand.version import MAGICK_VERSION_NUMBER


@mark.xfail
def test_alpha_channel_get():
    """Checks if image has alpha channel."""
    with Image(width=10, height=10, pseudo='gradient:red-transparent') as img:
        assert img.alpha_channel is True
    with Image(width=10, height=10, pseudo='gradient:red-black') as img:
        assert img.alpha_channel is False


@mark.xfail
def test_alpha_channel_set():
    """Sets alpha channel to off."""
    with Image(width=10, height=10, pseudo='gradient:red-transparent') as img:
        if MAGICK_VERSION_NUMBER < 0x700:
            enable_option = 'on'
            disable_option = False
        else:
            enable_option = 'associate'
            disable_option = 'disassociate'
        img.alpha_channel = enable_option
        assert img.alpha_channel is True
        img.alpha_channel = disable_option
        assert img.alpha_channel is False
        img.alpha_channel = 'opaque'
        assert img[0, 0].alpha == 1.0
        with raises(ValueError):
            img.alpha_channel = 'watermark'


def test_artifacts():
    with Image(filename='rose:') as img:
        img.artifacts['key'] = 'value'
        assert 'date:create' in img.artifacts
        assert img.artifacts['key'] == 'value'
        assert img.artifacts['not_a_value'] is None
        _ = len(img.artifacts)
        for _ in img.artifacts.items():
            pass
        del img.artifacts['key']


def test_background_color_get():
    """Gets the background color."""
    with Image(filename='rose:') as img:
        assert Color('white') == img.background_color


def test_background_color_set():
    """Sets the background color."""
    with Image(filename='rose:') as img:
        with Color('red') as color:
            img.background_color = color
            assert img.background_color == color
        img.background_color = 'green'
        assert img.background_color == Color('green')


def test_border_color():
    green = Color('green')
    with Image(filename='rose:') as img:
        img.border_color = 'green'
        assert img.border_color == green


def test_channel_depths():
    with Image(filename='rose:') as img:
        channels = dict(img.channel_depths)
        assert channels["red"] == channels["green"] == channels["blue"]


@mark.xfail
def test_channel_images():
    with Image(width=100, height=100, pseudo='plasma:') as img:
        assert img.channel_images['red'] != img.channel_images['blue']


def test_colors():
    with Image(filename='rose:') as img:
        assert img.colors == 3019


def test_colorspace_get():
    """Gets the image colorspace"""
    with Image(filename='rose:') as img:
        assert img.colorspace.endswith('rgb')


def test_colorspace_set():
    """Sets the image colorspace"""
    with Image(filename='rose:') as img:
        img.colorspace = 'cmyk'
        assert img.colorspace == 'cmyk'


def test_compose():
    with Image(filename='rose:') as img:
        assert img.compose == 'over'
        img.compose = 'blend'
        assert img.compose == 'blend'
        with raises(TypeError):
            img.compose = 0xDEADBEEF
        with raises(ValueError):
            img.compose = 'none'


def test_compression(fx_asset):
    with Image(filename=str(fx_asset.joinpath('mona-lisa.jpg'))) as img:
        # Legacy releases/library asserted ``'group4'`` compression type.
        # IM 7 will correctly report ``'jpeg'``, but ``'group4'`` should
        # still be apart of regression acceptance.
        assert img.compression in ('group4', 'jpeg')
        img.compression = 'zip'
        assert img.compression == 'zip'
        with raises(TypeError):
            img.compression = 0x60


def test_compression_quality_get(fx_asset):
    """Gets the image compression quality."""
    with Image(filename=str(fx_asset.joinpath('mona-lisa.jpg'))) as img:
        assert img.compression_quality == 80


def test_compression_quality_set(fx_asset):
    """Sets the image compression quality."""
    with Image(filename=str(fx_asset.joinpath('mona-lisa.jpg'))) as img:
        img.compression_quality = 50
        assert img.compression_quality == 50
        with raises(TypeError):
            img.compression_quality = 'high'


def test_delay_set_get():
    with Image(filename='rose:') as img:
        img.delay = 10
        assert img.delay == 10


def test_depth_get():
    """Gets the image depth"""
    with Image(filename='rose:') as img:
        assert img.depth == 8


def test_depth_set():
    """Sets the image depth"""
    with Image(filename='rose:') as img:
        img.depth = 16
        assert img.depth == 16


def test_dispose(fx_asset):
    with Image(filename=str(fx_asset.joinpath('nocomments.gif'))) as img:
        assert img.dispose == 'none'
        img.dispose = 'background'
        assert img.dispose == 'background'


def test_font_set(fx_asset):
    with Image(width=144, height=192, background=Color('#1e50a2')) as img:
        font = Font(
            path=str(fx_asset.joinpath('League_Gothic.otf')),
            color=Color('gold'),
            size=12,
            antialias=False
        )
        img.font = font
        assert img.font_path == font.path
        assert img.font_size == font.size
        assert img.font_color == font.color
        assert img.antialias == font.antialias
        assert img.font == font
        assert repr(img.font)
        fontStroke = Font(
            path=str(fx_asset.joinpath('League_Gothic.otf')),
            stroke_color=Color('ORANGE'),
            stroke_width=1.5
        )
        img.font = fontStroke
        assert img.stroke_color == fontStroke.stroke_color
        assert img.stroke_width == fontStroke.stroke_width
        img.font_color = 'gold'
        assert img.font_color == Color('gold')
        img.stroke_color = 'gold'
        assert img.stroke_color == Color('gold')
        fontColor = Font(
            path=str(fx_asset.joinpath('League_Gothic.otf')),
            color='YELLOW',
            stroke_color='PINK'
        )
        img.font = fontColor
        assert img.font_color == Color('YELLOW')
        assert img.stroke_color == Color('PINK')
        with raises(ValueError):
            img.font_size = -99


def test_format_get(fx_asset):
    """Gets the image format."""
    with Image(filename=str(fx_asset.joinpath('mona-lisa.jpg'))) as img:
        assert img.format == 'JPEG'
    with Image(filename=str(fx_asset.joinpath('croptest.png'))) as img:
        assert img.format == 'PNG'


def test_format_set():
    """Sets the image format."""
    with Image(filename='rose:') as img:
        img.format = 'png'
        assert img.format == 'PNG'
        strio = io.BytesIO()
        img.save(file=strio)
        strio.seek(0)
        with Image(file=strio) as png:
            assert png.format == 'PNG'
        with raises(ValueError):
            img.format = 'HONG'
        with raises(TypeError):
            img.format = 123


def test_fuzz():
    with Image(filename='rose:') as img:
        assert img.fuzz == 0.0
        img.fuzz = img.quantum_range
        assert img.fuzz == img.quantum_range


def test_gravity_set():
    with Image(width=144, height=192, background=Color('#1e50a2')) as img:
        img.gravity = 'center'
        assert img.gravity == 'center'


@mark.xfail
def test_histogram():
    with Image(width=1, height=2,
               pseudo='gradient:srgb(0,255,0)-srgb(0,0,255)') as a:
        h = a.histogram
        assert len(h) == 2
        assert frozenset(h) == frozenset([
            Color('srgb(0,255,0'),
            Color('srgb(0,0,255')
        ])
        assert dict(h) == {
            Color('srgb(0,255,0'): 1,
            Color('srgb(0,0,255'): 1,
        }
        assert Color('white') not in h
        assert Color('srgb(0,255,0)') in h
        assert Color('srgb(0,0,255)') in h
        assert h[Color('srgb(0,255,0)')] == 1
        assert h[Color('srgb(0,0,255)')] == 1


def test_interlace_scheme_get():
    with Image(filename='rose:') as img:
        expected = 'no'
        assert img.interlace_scheme == expected


def test_interlace_scheme_set():
    with Image(filename='rose:') as img:
        expected = 'plane'
        img.interlace_scheme = expected
        assert img.interlace_scheme == expected


def test_interpolate_method_get():
    with Image(filename='rose:') as img:
        expected = 'undefined'
        assert img.interpolate_method == expected


def test_interpolate_method_set():
    with Image(filename='rose:') as img:
        expected = 'spline'
        img.interpolate_method = expected
        assert img.interpolate_method == expected


def test_kurtosis():
    with Image(filename='rose:') as img:
        kurtosis = img.kurtosis
        assert isinstance(kurtosis, numbers.Real)
        assert kurtosis != 0.0


def test_length_of_bytes():
    with Image(filename='rose:') as img:
        assert img.length_of_bytes > 0
        img.resample(300, 300)
        assert img.length_of_bytes == 0


def test_loop():
    with Image(filename='rose:') as img:
        assert img.loop == 0
        img.loop = 1
        assert img.loop == 1


def test_matte_color():
    with Image(filename='rose:') as img:
        with Color('navy') as color:
            img.matte_color = color
            assert img.matte_color == color
            with raises(TypeError):
                img.matte_color = False
        img.matte_color = 'orange'
        assert img.matte_color == Color('orange')


def test_mean():
    with Image(filename='rose:') as img:
        mean = img.mean
        assert isinstance(mean, numbers.Real)
        assert mean != 0.0


def test_metadata(fx_asset):
    """Test metadata api"""
    with Image(filename=str(fx_asset.joinpath('beach.jpg'))) as img:
        assert len(img.metadata) > 0
        for key in img.metadata:
            assert isinstance(key, str)
        assert 'exif:ApertureValue' in img.metadata
        assert 'exif:UnknownValue' not in img.metadata
        assert img.metadata['exif:ApertureValue'] == '192/32'
        assert img.metadata.get('exif:UnknownValue', "IDK") == "IDK"


def test_mimetype():
    """Gets mimetypes of the image."""
    with Image(filename='rose:') as img:
        assert img.mimetype in (
            'image/x-portable-anymap',
            'image/x-portable-pixmap',
            'image/x-portable',
            'image/x-ppm',
        )


def test_minima_maxima():
    with Image(filename='rose:') as img:
        min_q = img.minima
        max_q = img.maxima
        assert min_q < max_q


def test_orientation_get(fx_asset):
    with Image(filename=str(fx_asset.joinpath('mona-lisa.jpg'))) as img:
        assert img.orientation == 'undefined'

    with Image(filename=str(fx_asset.joinpath('beach.jpg'))) as img:
        assert img.orientation == 'top_left'


def test_orientation_set(fx_asset):
    with Image(filename=str(fx_asset.joinpath('beach.jpg'))) as img:
        img.orientation = 'bottom_right'
        assert img.orientation == 'bottom_right'


def test_page_basic():
    with Image(filename='rose:') as img1:
        assert img1.page == (70, 46, 0, 0)
        assert img1.page_width == 70
        assert img1.page_height == 46
        assert img1.page_x == 0
        assert img1.page_y == 0
        with raises(TypeError):
            img1.page = 640


def test_page_setter():
    with Image(filename='rose:') as img:
        img.page = (70, 46, 10, 10)
        assert img.page == (70, 46, 10, 10)
        img.page = (70, 46, -10, -10)
        assert img.page == (70, 46, -10, -10)
        img.page_width = 60
        img.page_height = 40
        img.page_x = 0
        img.page_y = 0
        assert img.page == (60, 40, 0, 0)


def test_page_setter_papersize():
    with Image(filename='rose:') as img:
        img.page = 'a4'
        assert img.page == (595, 842, 0, 0)
        img.page = 'badvalue'
        assert img.page == (0, 0, 0, 0)


def test_primary_points():
    with Image(filename='rose:') as img:
        blue = [d/2 for d in img.blue_primary]
        img.blue_primary = blue
        assert blue == list(img.blue_primary)
        green = [d/2 for d in img.green_primary]
        img.green_primary = green
        assert green == list(img.green_primary)
        red = [d/2 for d in img.red_primary]
        img.red_primary = red
        assert red == list(img.red_primary)
        white = [d/2 for d in img.white_point]
        img.white_point = white
        assert white == list(img.white_point)
        with raises(TypeError):
            img.blue_primary = 0xDEADBEEF
        with raises(TypeError):
            img.green_primary = 0xDEADBEEF
        with raises(TypeError):
            img.red_primary = 0xDEADBEEF
        with raises(TypeError):
            img.white_point = 0xDEADBEEF


def test_profiles(fx_asset):
    with Image(filename=str(fx_asset.joinpath('beach.jpg'))) as img:
        assert len(img.profiles) == 1
        assert 'exif' in [d for d in img.profiles]
        exif_data = img.profiles['exif']
        assert exif_data is not None
        del img.profiles['exif']
        assert img.profiles['exif'] is None
        img.profiles['exif'] = exif_data
        assert img.profiles['exif'] == exif_data
        with raises(TypeError):
            img.profiles[0xDEADBEEF]
        with raises(TypeError):
            del img.profiles[0xDEADBEEF]
        with raises(TypeError):
            img.profiles[0xDEADBEEF] = 0xDEADBEEF
        with raises(TypeError):
            img.profiles['exif'] = 0xDEADBEEF


def test_rendering_intent(fx_asset):
    with Image(filename=str(fx_asset.joinpath('trimtest.png'))) as img:
        assert img.rendering_intent == 'perceptual'
        img.rendering_intent = 'relative'
        assert img.rendering_intent == 'relative'


def test_resolution_get(fx_asset):
    """Gets image resolution."""
    with Image(filename=str(fx_asset.joinpath('mona-lisa.jpg'))) as img:
        assert img.resolution == (72, 72)


def test_resolution_set_01(fx_asset):
    """Sets image resolution."""
    with Image(filename=str(fx_asset.joinpath('mona-lisa.jpg'))) as img:
        img.resolution = (100, 100)
        assert img.resolution == (100, 100)


def test_resolution_set_02(fx_asset):
    """Sets image resolution with integer as parameter."""
    with Image(filename=str(fx_asset.joinpath('mona-lisa.jpg'))) as img:
        img.resolution = 100
        assert img.resolution == (100, 100)


def test_resolution_set_03():
    """Sets image resolution on constructor"""
    with Image(filename='rose:', resolution=(100, 100)) as img:
        assert img.resolution == (100, 100)


def test_resolution_set_04():
    """Sets image resolution on constructor with integer as parameter."""
    with Image(filename='rose:', resolution=100) as img:
        assert img.resolution == (100, 100)


def test_sampling_factors():
    with Image(filename='rose:') as img:
        img.sampling_factors = "4:2:2"
        assert img.sampling_factors == (2, 1)
        with raises(TypeError):
            img.sampling_factors = {}


def test_scene():
    with Image(filename='rose:') as img:
        img.scene = 4
        assert img.scene == 4


def test_signature():
    """Gets the image signature."""
    with Image(filename='rose:') as img:
        assert img.signature


def test_size():
    """Gets the image size."""
    with Image(filename='rose:') as img:
        assert img.size == (70, 46)
        assert img.width == 70
        assert img.height == 46
        assert len(img) == 46


def test_skewness():
    with Image(filename='rose:') as img:
        skewness = img.skewness
        assert isinstance(skewness, numbers.Real)
        assert skewness != 0.0


def test_standard_deviation():
    with Image(filename='rose:') as img:
        standard_deviation = img.standard_deviation
        assert isinstance(standard_deviation, numbers.Real)
        assert standard_deviation != 0.0


def test_stroke_color_user_error():
    with Image(filename='rose:') as img:
        img.stroke_color = 'green'
        img.stroke_color = None
        assert img.stroke_color is None
        with raises(TypeError):
            img.stroke_color = 0xDEADBEEF


def test_type_get():
    """Gets the image type."""
    with Image(filename='rose:') as img:
        assert img.type == "truecolor"
        img.alpha_channel = True
        if MAGICK_VERSION_NUMBER < 0x700:
            expected = "truecolormatte"
        else:
            expected = "truecoloralpha"
        assert img.type == expected


def test_type_set():
    """Sets the image type."""
    with Image(filename='rose:') as img:
        img.type = "grayscale"
        assert img.type == "grayscale"


def test_ticks_per_second():
    with Image(filename='rose:') as img:
        assert img.ticks_per_second == 100
        img.ticks_per_second = 10
        assert img.ticks_per_second == 10


def test_units_get():
    """Gets the image resolution units."""
    with Image(filename='rose:', units='pixelsperinch') as img:
        assert img.units == "pixelsperinch"
    with Image(filename='rose:') as img:
        assert img.units == "undefined"


def test_units_set():
    """Sets the image resolution units."""
    with Image(filename='rose:') as img:
        img.units = "pixelspercentimeter"
        assert img.units == "pixelspercentimeter"


def test_virtual_pixel_get():
    """Gets image virtual pixel"""
    with Image(filename='rose:') as img:
        assert img.virtual_pixel == "undefined"


def test_virtual_pixel_set():
    """Sets image virtual pixel"""
    with Image(filename='rose:') as img:
        img.virtual_pixel = "tile"
        assert img.virtual_pixel == "tile"
        with raises(ValueError):
            img.virtual_pixel = "nothing"
