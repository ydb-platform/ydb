import ctypes
import time
import warnings

try:
    from memory_profiler import memory_usage
except ImportError:
    memory_usage = None
from pytest import mark, raises

from wand.color import Color
from wand.version import MAGICK_VERSION_INFO, QUANTUM_DEPTH  # noqa


def test_user_error():
    with raises(TypeError):
        Color()
    with raises(ValueError):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            Color('not_a_color')


def test_user_raw():
    with Color('black') as black:
        with Color(raw=black.raw) as c:
            assert c


def test_user_pickle():
    with Color('black') as black:
        # Can't trust string literal between IM versions, but the tuple
        # should be constant.
        assert len(black.__getinitargs__()) == 2


def test_equals():
    """Equality test."""
    assert Color('#fff') == Color('#ffffff') == Color('white')
    assert Color('#000') == Color('#000000') == Color('black')
    assert Color('rgba(0, 0, 0, 0)') == Color('rgba(0, 0, 0, 0)')
    assert Color('rgba(0, 0, 0, 0)') == Color('rgba(1, 1, 1, 0)')
    assert Color('green') != 'green'


def test_not_equals():
    """Equality test."""
    assert Color('#000') != Color('#fff')
    assert Color('rgba(0, 0, 0, 0)') != Color('rgba(0, 0, 0, 1)')
    assert Color('rgba(0, 0, 0, 1)') != Color('rgba(1, 1, 1, 1)')


def test_hash():
    """Hash test."""
    assert (hash(Color('#fff')) == hash(Color('#ffffff')) ==
            hash(Color('white')))
    assert (hash(Color('#000')) == hash(Color('#000000')) ==
            hash(Color('black')))
    assert hash(Color('rgba(0, 0, 0, 0))')) == hash(Color('rgba(0, 0, 0, 0))'))
    assert hash(Color('rgba(0, 0, 0, 0))')) == hash(Color('rgba(1, 1, 1, 0))'))


def test_red():
    assert Color('black').red == 0
    assert Color('red').red == 1
    assert Color('white').red == 1
    assert 0.5 <= Color('rgba(128, 0, 0, 1)').red < 0.51
    c = Color('none')
    c.red = 1
    assert c.red == 1


def test_green():
    assert Color('black').green == 0
    assert Color('#0f0').green == 1
    assert Color('white').green == 1
    assert 0.5 <= Color('rgba(0, 128, 0, 1)').green < 0.51
    c = Color('none')
    c.green = 1
    assert c.green == 1


def test_blue():
    assert Color('black').blue == 0
    assert Color('blue').blue == 1
    assert Color('white').blue == 1
    assert 0.5 <= Color('rgba(0, 0, 128, 1)').blue < 0.51
    c = Color('none')
    c.blue = 1
    assert c.blue == 1


def test_alpha():
    assert Color('rgba(0, 0, 0, 1)').alpha == 1
    assert Color('rgba(0, 0, 0, 0)').alpha == 0
    assert 0.49 <= Color('rgba(0, 0, 0, 0.5)').alpha <= 0.51
    c = Color('none')
    c.alpha = 1
    assert c.alpha == 1


def test_cyan():
    assert Color('cmyk(100%, 0, 0, 0)').cyan == 1
    assert Color('cmyk(0, 0, 0, 0)').cyan == 0
    c = Color('none')
    c.cyan = 1
    assert c.cyan == 1


def test_magenta():
    assert Color('cmyk(0, 100%, 0, 0)').magenta == 1
    assert Color('cmyk(0, 0, 0, 0)').magenta == 0
    c = Color('none')
    c.magenta = 1
    assert c.magenta == 1


def test_yellow():
    assert Color('cmyk(0, 0, 100%, 0)').yellow == 1
    assert Color('cmyk(0, 0, 0, 0)').yellow == 0
    c = Color('none')
    c.yellow = 1
    assert c.yellow == 1


def test_black():
    assert Color('cmyk(0, 0, 0, 100%)').black == 1
    assert Color('cmyk(0, 0, 0, 0)').black == 0
    c = Color('none')
    c.black = 1
    assert c.black == 1


def test_red_quantum():
    q = 2 ** QUANTUM_DEPTH - 1
    assert Color('black').red_quantum == 0
    assert Color('red').red_quantum == q
    assert Color('white').red_quantum == q
    assert (0.49 * q) < Color('rgba(128, 0, 0, 1)').red_quantum < (0.51 * q)
    c = Color('none')
    c.red_quantum = q
    assert c.red_quantum == q


def test_green_quantum():
    q = 2 ** QUANTUM_DEPTH - 1
    assert Color('black').green_quantum == 0
    assert Color('#0f0').green_quantum == q
    assert Color('white').green_quantum == q
    assert (0.49 * q) < Color('rgba(0, 128, 0, 1)').green_quantum < (0.51 * q)
    c = Color('none')
    c.green_quantum = q
    assert c.green_quantum == q


def test_blue_quantum():
    q = 2 ** QUANTUM_DEPTH - 1
    assert Color('black').blue_quantum == 0
    assert Color('blue').blue_quantum == q
    assert Color('white').blue_quantum == q
    assert (0.49 * q) < Color('rgba(0, 0, 128, 1)').blue_quantum < (0.51 * q)
    c = Color('none')
    c.blue_quantum = q
    assert c.blue_quantum == q


def test_alpha_quantum():
    q = 2 ** QUANTUM_DEPTH - 1
    assert Color('rgba(0, 0, 0, 1)').alpha_quantum == q
    assert Color('rgba(0, 0, 0, 0)').alpha_quantum == 0
    assert (0.49 * q) < Color('rgba(0, 0, 0, 0.5)').alpha_quantum < (0.51 * q)
    c = Color('none')
    c.alpha_quantum = q
    assert c.alpha_quantum == q


def test_cyan_quantum():
    q = 2 ** QUANTUM_DEPTH - 1
    assert int(Color('cmyk(100%, 0, 0, 0)').cyan_quantum) == q
    assert Color('cmyk(0, 0, 0, 0)').cyan_quantum == 0
    c = Color('none')
    c.cyan_quantum = q
    assert c.cyan_quantum == q


def test_magenta_quantum():
    q = 2 ** QUANTUM_DEPTH - 1
    assert int(Color('cmyk(0, 100%, 0, 0)').magenta_quantum) == q
    assert Color('cmyk(0, 0, 0, 0)').magenta_quantum == 0
    c = Color('none')
    c.magenta_quantum = q
    assert c.magenta_quantum == q


def test_yellow_quantum():
    q = 2 ** QUANTUM_DEPTH - 1
    assert int(Color('cmyk(0, 0, 100%, 0)').yellow_quantum) == q
    assert Color('cmyk(0, 0, 0, 0)').yellow_quantum == 0
    c = Color('none')
    c.yellow_quantum = q
    assert c.yellow_quantum == q


def test_black_quantum():
    q = 2 ** QUANTUM_DEPTH - 1
    assert int(Color('cmyk(0, 0, 0, 100%)').black_quantum) == q
    assert Color('cmyk(0, 0, 0, 0)').black_quantum == 0
    c = Color('none')
    c.black_quantum = q
    assert c.black_quantum == q


def test_red_int8():
    assert Color('black').red_int8 == 0
    assert Color('red').red_int8 == 255
    assert Color('white').red_int8 == 255
    assert Color('rgba(128, 0, 0, 1)').red_int8 == 128
    c = Color('none')
    c.red_int8 = 255
    assert c.red_int8 == 255


def test_green_int8():
    assert Color('black').green_int8 == 0
    assert Color('#0f0').green_int8 == 255
    assert Color('white').green_int8 == 255
    assert Color('rgba(0, 128, 0, 1)').green_int8 == 128
    c = Color('none')
    c.green_int8 = 255
    assert c.green_int8 == 255


def test_blue_int8():
    assert Color('black').blue_int8 == 0
    assert Color('blue').blue_int8 == 255
    assert Color('white').blue_int8 == 255
    assert Color('rgba(0, 0, 128, 1)').blue_int8 == 128
    c = Color('none')
    c.blue_int8 = 255
    assert c.blue_int8 == 255


def test_alpha_int8():
    assert Color('rgba(0, 0, 0, 1)').alpha_int8 == 255
    assert Color('rgba(0, 0, 0, 0)').alpha_int8 == 0
    if not (Color('rgb(127,0,0)').red_quantum <=
            Color('rgba(0,0,0,0.5').alpha_quantum <=
            Color('rgb(128,0,0)').red_quantum):
        # FIXME: I don't know why, but the value PixelGetAlphaQuantum() returns
        #        is inconsistent to other PixelGet{Red,Green,Blue}Quantum()
        #        functions in Travis CI.  We just skip the test in this case.
        return
    assert 127 <= Color('rgba(0, 0, 0, 0.5)').alpha_int8 <= 128
    c = Color('none')
    c.alpha_int8 = 255
    assert c.alpha_int8 == 255


def test_cyan_int8():
    assert Color('cmyk(100%, 0, 0, 0)').cyan_int8 == 255
    assert Color('cmyk(0, 0, 0, 0)').cyan_int8 == 0
    c = Color('none')
    c.cyan_int8 = 255
    assert c.cyan_int8 == 255


def test_magenta_int8():
    assert Color('cmyk(0, 100%, 0, 0)').magenta_int8 == 255
    assert Color('cmyk(0, 0, 0, 0)').magenta_int8 == 0
    c = Color('none')
    c.magenta_int8 = 255
    assert c.magenta_int8 == 255


def test_yellow_int8():
    assert Color('cmyk(0, 0, 100%, 0)').yellow_int8 == 255
    assert Color('cmyk(0, 0, 0, 0)').yellow_int8 == 0
    c = Color('none')
    c.yellow_int8 = 255
    assert c.yellow_int8 == 255


def test_black_int8():
    assert Color('cmyk(0, 0, 0, 100%)').black_int8 == 255
    assert Color('cmyk(0, 0, 0, 0)').black_int8 == 0
    c = Color('none')
    c.black_int8 = 255
    assert c.black_int8 == 255


def test_string():
    assert Color('black').string in ('rgb(0,0,0)', 'srgb(0,0,0)')
    assert str(Color('black')) in ('rgb(0,0,0)', 'srgb(0,0,0)')


def test_fuzz():
    c = Color('none')
    c.fuzz = 55.5
    assert c.fuzz == 55.5
    with raises(TypeError):
        c.fuzz = 'NaN'


def test_hsl():
    with Color.from_hsl(hue=0.0, saturation=1.0, lightness=0.5) as c:
        assert c == Color('#f00')
    with Color('#00f') as c:
        assert c.hsl() == (0.6666666666666666, 1.0, 0.5)


def color_memory_leak():
    for _ in range(5000):
        with Color('orange'):
            pass
    time.sleep(0.02)


@mark.skipif(memory_usage is None or MAGICK_VERSION_INFO <= (6, 6, 9, 7),
             reason='memory_usage is unavailable, or untestable')
def test_memory_leak():
    """https://github.com/emcconville/wand/pull/127"""
    minimum = 1.0
    with Color('NONE') as nil_color:
        minimum = ctypes.sizeof(nil_color.raw)
    consumes = memory_usage((color_memory_leak, (), {}))
    assert consumes[-1] - consumes[0] <= minimum


def test_color_assert_double_user_error():
    with Color('WHITE') as c:
        with raises(TypeError):
            c._assert_double('double')
        with raises(ValueError):
            c._assert_double(-1)
        with raises(ValueError):
            c._assert_double(1.1)


def test_color_assert_int8_user_error():
    with Color('WHITE') as c:
        with raises(TypeError):
            c._assert_int8(0.123)
        with raises(ValueError):
            c._assert_int8(-1)
        with raises(ValueError):
            c._assert_int8(0xFFFFF)


def test_color_assert_quantum_user_error():
    with Color('WHITE') as c:
        with raises(TypeError):
            c._assert_quantum('Q16')
        with raises(ValueError):
            c._assert_quantum(-1)


def test_color_repr_html():
    with Color('#AABBCC') as c:
        assert '#AABBCC' in c._repr_html_()
