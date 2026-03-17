import itertools

from pytest import mark, raises

from wand.api import library
from wand.color import Color
from wand.drawing import Drawing
from wand.image import Image
from wand.version import MAGICK_VERSION_NUMBER


def test_init_user_error():
    with raises(TypeError):
        with Drawing(0xDEADBEEF):
            pass


def test_is_drawing_wand():
    with Drawing() as ctx:
        assert library.IsDrawingWand(ctx.resource)


def test_set_get_border_color():
    with Drawing() as ctx:
        with Color("#0F0") as green:
            ctx.border_color = green
            assert green == ctx.border_color
        ctx.border_color = 'orange'
        assert ctx.border_color == Color('orange')
        # Assert user error
        with raises(TypeError):
            ctx.border_color = 0xDEADBEEF


def test_set_get_clip_path():
    with Drawing() as ctx:
        ctx.clip_path = 'path_id'
        assert ctx.clip_path == 'path_id'
        # Assert user error
        with raises(TypeError):
            ctx.clip_path = 0xDEADBEEF


def test_set_get_clip_rule():
    with Drawing() as ctx:
        ctx.clip_rule = 'evenodd'
        assert ctx.clip_rule == 'evenodd'
        with raises(TypeError):
            ctx.clip_rule = 0xDEADBEEF
        with raises(ValueError):
            ctx.clip_rule = 'not-a-rule'


def test_set_get_clip_units():
    with Drawing() as ctx:
        ctx.clip_units = 'object_bounding_box'
        assert ctx.clip_units == 'object_bounding_box'
        with raises(TypeError):
            ctx.clip_units = 0xDEADBEEF
        with raises(ValueError):
            ctx.clip_units = 'not-a-clip_unit'


def test_set_get_font():
    with Drawing() as ctx:
        """Setting this values doesn't actually check if the typeface file
        exists, but the get/set values should still agree."""
        ctx.font = 'GhostType.ttf'
        assert ctx.font == 'GhostType.ttf'
        with raises(TypeError):
            ctx.font = 0xDEADBEEF


def test_set_get_font_family():
    with Drawing() as ctx:
        assert ctx.font_family is None
        ctx.font_family = 'sans-serif'
        assert ctx.font_family == 'sans-serif'
        with raises(TypeError):
            ctx.font_family = 0xDEADBEEF


def test_set_get_font_resolution():
    with Drawing() as ctx:
        ctx.font_resolution = (78.0, 78.0)
        assert ctx.font_resolution == (78.0, 78.0)
        with raises(TypeError):
            ctx.font_resolution = 0xDEADBEEF
        with raises(ValueError):
            ctx.font_resolution = (78.0, 78.0, 78.0)


def test_set_get_font_size():
    with Drawing() as ctx:
        ctx.font_size = 22.2
        assert ctx.font_size == 22.2
        with raises(TypeError):
            ctx.font_size = '22.2%'
        with raises(ValueError):
            ctx.font_size = -22.2


def test_set_get_font_stretch():
    with Drawing() as ctx:
        ctx.font_stretch = 'condensed'
        assert ctx.font_stretch == 'condensed'
        with raises(TypeError):
            ctx.font_stretch = 0xDEADBEEF
        with raises(ValueError):
            ctx.font_stretch = 'not-a-stretch-type'


def test_set_get_font_style():
    with Drawing() as ctx:
        ctx.font_style = 'italic'
        assert ctx.font_style == 'italic'
        with raises(TypeError):
            ctx.font_style = 0xDEADBEEF
        with raises(ValueError):
            ctx.font_style = 'not-a-style-type'


def test_set_get_font_weight():
    with Drawing() as ctx:
        ctx.font_weight = 400  # Normal
        assert ctx.font_weight == 400
        with raises(TypeError):
            ctx.font_weight = '400'


def test_set_get_fill_color():
    with Drawing() as ctx:
        with Color('#333333') as black:
            ctx.fill_color = black
        assert ctx.fill_color == Color('#333333')
        ctx.fill_color = 'pink'
        ctx.fill_color == Color('PINK')


def test_set_get_stroke_color():
    with Drawing() as ctx:
        with Color('#333333') as black:
            ctx.stroke_color = black
        assert ctx.stroke_color == Color('#333333')
        ctx.stroke_color = 'skyblue'
        assert ctx.stroke_color == Color('SkyBlue')


def test_set_get_stroke_width():
    with Drawing() as ctx:
        ctx.stroke_width = 5
        assert ctx.stroke_width == 5


def test_set_get_text_alignment():
    with Drawing() as ctx:
        ctx.text_alignment = 'center'
        assert ctx.text_alignment == 'center'
        with raises(TypeError):
            ctx.text_alignment = 0xDEADBEEF
        with raises(ValueError):
            ctx.text_alignment = 'not-a-text-alignment-type'


def test_set_get_text_antialias():
    with Drawing() as ctx:
        ctx.text_antialias = True
        assert ctx.text_antialias is True


def test_set_get_text_decoration():
    with Drawing() as ctx:
        ctx.text_decoration = 'underline'
        assert ctx.text_decoration == 'underline'
        with raises(TypeError):
            ctx.text_decoration = 0xDEADBEEF
        with raises(ValueError):
            ctx.text_decoration = 'not-a-text-decoration-type'


@mark.skipif(MAGICK_VERSION_NUMBER < 0x689,
             reason='DrawGetTextDirection not supported.')
def test_set_get_text_direction():
    with Drawing() as ctx:
        ctx.text_direction = 'right_to_left'
        assert ctx.text_direction == 'right_to_left'


def test_set_get_text_encoding():
    with Drawing() as ctx:
        ctx.text_encoding = 'UTF-8'
        assert ctx.text_encoding == 'UTF-8'
        ctx.text_encoding = None


def test_set_get_text_interline_spacing():
    with Drawing() as ctx:
        ctx.text_interline_spacing = 10.11
        assert ctx.text_interline_spacing == 10.11
        with raises(TypeError):
            ctx.text_interline_spacing = '10.11'


def test_set_get_text_interword_spacing():
    with Drawing() as ctx:
        ctx.text_interword_spacing = 5.55
        assert ctx.text_interword_spacing == 5.55
        with raises(TypeError):
            ctx.text_interline_spacing = '5.55'


def test_set_get_text_kerning():
    with Drawing() as ctx:
        ctx.text_kerning = 10.22
        assert ctx.text_kerning == 10.22
        with raises(TypeError):
            ctx.text_kerning = '10.22'


def test_set_get_text_under_color():
    with Drawing() as ctx:
        with Color('#333333') as black:
            ctx.text_under_color = black
        assert ctx.text_under_color == Color('#333333')
        ctx.text_under_color = '#333'  # Smoke test
        with raises(TypeError):
            ctx.text_under_color = 0xDEADBEEF


def test_set_get_vector_graphics():
    with Drawing() as ctx:
        ctx.stroke_width = 7
        xml = ctx.vector_graphics
        assert xml.index("<stroke-width>7</stroke-width>") > 0
        ctx.vector_graphics = '<wand><stroke-width>8</stroke-width></wand>'
        xml = ctx.vector_graphics
        assert xml.index("<stroke-width>8</stroke-width>") > 0
        with raises(TypeError):
            ctx.vector_graphics = 0xDEADBEEF


def test_set_get_gravity():
    with Drawing() as ctx:
        ctx.gravity = 'center'
        assert ctx.gravity == 'center'
        with raises(TypeError):
            ctx.gravity = 0xDEADBEEF
        with raises(ValueError):
            ctx.gravity = 'not-a-gravity-type'


def test_clone_drawing_wand():
    with Drawing() as ctx:
        ctx.text_kerning = 10.22
        funcs = (lambda img: Drawing(drawing=ctx),
                 lambda img: ctx.clone())
        for func in funcs:
            with func(ctx) as cloned:
                assert ctx.resource is not cloned.resource
                assert ctx.text_kerning == cloned.text_kerning


def test_clear_drawing_wand():
    with Drawing() as ctx:
        ctx.text_kerning = 10.22
        assert ctx.text_kerning == 10.22
        ctx.clear()
        assert ctx.text_kerning == 0


def test_composite():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.fill_color = 'black'
            ctx.stroke_color = 'black'
            ctx.rectangle(25, 25, 49, 49)
            ctx.draw(img)
            ctx.composite('replace', 0, 0, 25, 25, img)
            ctx.draw(img)
        assert was != img.signature


def test_draw_arc():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.fill_color = 'red'
            ctx.stroke_color = 'black'
            ctx.arc((10, 10),   # Start
                    (40, 40),   # End
                    (-90, 90))  # Degree
            ctx.draw(img)
        assert was != img.signature


def test_draw_circle():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.fill_color = 'black'
            ctx.circle((25, 25),  # Origin
                       (40, 40))  # Perimeter
            ctx.draw(img)
        assert was != img.signature


def test_draw_color():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.fill_color = 'black'
            ctx.color(25, 25, 'floodfill')
            ctx.draw(img)
        assert was != img.signature


def test_draw_color_user_error():
    with Drawing() as draw:
        with raises(TypeError):
            draw.color('apples')
        with raises(TypeError):
            draw.color(1, 2, 4)
        with raises(ValueError):
            draw.color(1, 2, 'apples')


def test_draw_ellipse():
    with Image(width=50, height=50, background='#ccc') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.fill_color = 'red'
            ctx.ellipse((25, 25),  # origin
                        (20, 10))  # radius
            ctx.draw(img)
        assert was != img.signature


def test_draw_line():
    with Image(width=10, height=10, background='#ccc') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.fill_color = 'black'
            ctx.line((5, 5), (7, 5))
            ctx.draw(img)
        assert was != img.signature


@mark.skipif(MAGICK_VERSION_NUMBER >= 0x700,
             reason='wand.drawing.Drawing.matte removed with IM 7.')
def test_draw_matte():
    white = Color('rgba(0, 255, 255, 5%)')
    transparent = Color('transparent')
    with Image(width=50, height=50, background=white) as img:
        with Drawing() as draw:
            draw.fill_opacity = 0.0
            draw.matte(25, 25, 'floodfill')
            draw.draw(img)
            assert img[25, 25] == transparent


@mark.skipif(MAGICK_VERSION_NUMBER >= 0x700,
             reason='wand.drawing.Drawing.matte removed with IM 7.')
def test_draw_matte_user_error():
    with Drawing() as draw:
        with raises(TypeError):
            draw.matte('apples')
        with raises(TypeError):
            draw.matte(1, 2, 4)
        with raises(ValueError):
            draw.matte(1, 2, 'apples')


@mark.skipif(MAGICK_VERSION_NUMBER < 0x700,
             reason='wand.drawing.Drawing.alpha was added with IM 7.')
def test_draw_alpha():
    transparent = Color('transparent')
    with Image(width=50, height=50, pseudo='xc:white') as img:
        with Drawing() as draw:
            draw.fill_color = transparent
            draw.alpha(25, 25, 'floodfill')
            draw.draw(img)
        assert img[25, 25] == transparent


@mark.skipif(MAGICK_VERSION_NUMBER < 0x700,
             reason='wand.drawing.Drawing.alpha was added with IM 7.')
def test_draw_alpha_user_error():
    with Drawing() as draw:
        with raises(TypeError):
            draw.alpha()
        with raises(TypeError):
            draw.alpha(1, 2, 4)
        with raises(ValueError):
            draw.alpha(1, 2, 'apples')


def test_draw_point():
    white = Color('WHITE')
    black = Color('BLACK')
    with Image(width=5, height=5, background=white) as img:
        with Drawing() as draw:
            draw.stroke_color = black
            draw.point(2, 2)
            draw.draw(img)
            assert img[2, 2] == black


def test_draw_polygon():
    white = Color('WHITE')
    red = Color('RED')
    blue = Color('BLUE')
    with Image(width=50, height=50, background=white) as img:
        with Drawing() as draw:
            draw.fill_color = blue
            draw.stroke_color = red
            draw.polygon([(10, 10),
                          (40, 25),
                          (10, 40)])
            draw.draw(img)
            assert img[10, 25] == red
            assert img[25, 25] == blue
            assert img[35, 15] == img[35, 35] == white


def test_draw_polyline():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as draw:
            draw.fill_color = 'blue'
            draw.stroke_color = 'red'
            draw.polyline([(10, 10), (40, 25), (10, 40)])
            draw.draw(img)
        assert was != img.signature


def test_draw_push_pop():
    with Drawing() as draw:
        draw.stroke_width = 2
        draw.push()
        draw.stroke_width = 3
        assert 3 == draw.stroke_width
        draw.pop()
        assert 2 == draw.stroke_width


def test_draw_bezier():
    white = Color('WHITE')
    red = Color('RED')
    blue = Color('BLUE')
    with Image(width=50, height=50, background=white) as img:
        with Drawing() as draw:
            draw.fill_color = blue
            draw.stroke_color = red
            draw.bezier([(10, 10),
                         (10, 40),
                         (40, 10),
                         (40, 40)])
            draw.draw(img)
            assert img[10, 10] == img[25, 25] == img[40, 40] == red
            assert img[34, 32] == img[15, 18] == blue
            assert img[34, 38] == img[15, 12] == white


def test_path_curve():
    white = Color('WHITE')
    red = Color('RED')
    blue = Color('BLUE')
    with Image(width=50, height=50, background=white) as img:
        with Drawing() as draw:
            draw.fill_color = blue
            draw.stroke_color = red
            draw.path_start()
            draw.path_move(to=(0, 25), relative=True)
            draw.path_curve(to=(25, 25),
                            controls=((0, 0), (25, 0)))
            draw.path_curve(to=(25, 0),
                            controls=((0, 25), (25, 25)),
                            relative=True)
            draw.path_finish()
            draw.draw(img)
            assert img[25, 25] == red
            assert img[35, 35] == img[35, 35] == blue
            assert img[35, 15] == img[15, 35] == white


def test_path_curve_user_error():
    with Drawing() as draw:
        with raises(TypeError):
            draw.path_curve(to=(5, 7))
        with raises(TypeError):
            draw.path_curve(controls=(5, 7))


def test_path_curve_to_quadratic_bezier():
    white = Color('WHITE')
    red = Color('RED')
    blue = Color('BLUE')
    with Image(width=50, height=50, background=white) as img:
        with Drawing() as draw:
            draw.fill_color = blue
            draw.stroke_color = red
            draw.path_start()
            draw.path_move(to=(0, 25), relative=True)
            draw.path_curve_to_quadratic_bezier(to=(50, 25),
                                                control=(25, 50))
            draw.path_curve_to_quadratic_bezier(to=(-20, -20),
                                                control=(-25, 0),
                                                relative=True)
            draw.path_finish()
            draw.draw(img)
            assert img[30, 5] == red


def test_path_curve_to_quadratic_bezier_smooth():
    white = Color('WHITE')
    red = Color('RED')
    blue = Color('BLUE')
    with Image(width=50, height=50, background=white) as img:
        with Drawing() as draw:
            draw.fill_color = blue
            draw.stroke_color = red
            draw.path_start()
            draw.path_curve_to_quadratic_bezier(to=(25, 25),
                                                control=(25, 25))
            draw.path_curve_to_quadratic_bezier(to=(10, -10),
                                                smooth=True,
                                                relative=True)
            draw.path_curve_to_quadratic_bezier(to=(35, 35),
                                                smooth=True,
                                                relative=False)
            draw.path_curve_to_quadratic_bezier(to=(-10, -10),
                                                smooth=True,
                                                relative=True)
            draw.path_finish()
            draw.draw(img)
            assert img[25, 25] == red
            assert img[30, 30] == blue


def test_path_curve_quadratic_bezier_user_error():
    with Drawing() as draw:
        with raises(TypeError):
            draw.path_curve_to_quadratic_bezier()
        with raises(TypeError):
            draw.path_curve_to_quadratic_bezier(to=(5, 6))


def test_draw_path_elliptic_arc():
    white = Color('WHITE')
    red = Color('RED')
    blue = Color('BLUE')
    with Image(width=50, height=50, background=white) as img:
        with Drawing() as draw:
            draw.fill_color = blue
            draw.stroke_color = red
            draw.path_start()
            draw.path_move(to=(25, 0))
            draw.path_elliptic_arc(to=(25, 50), radius=(15, 25))
            draw.path_elliptic_arc(to=(0, -15), radius=(5, 5),
                                   clockwise=False, relative=True)
            draw.path_close()
            draw.path_finish()
            draw.draw(img)
            assert img[25, 35] == img[25, 20] == red
            assert img[15, 25] == img[30, 45] == blue


def test_draw_path_elliptic_arc_user_error():
    with Drawing() as draw:
        with raises(TypeError):
            draw.path_elliptic_arc(to=(5, 7))
        with raises(TypeError):
            draw.path_elliptic_arc(radius=(5, 7))


def test_draw_path_line():
    white = Color('#FFF')
    red = Color('#F00')
    blue = Color('#00F')
    with Image(width=50, height=50, background=white) as img:
        with Drawing() as draw:
            draw.fill_color = blue
            draw.stroke_color = red
            draw.stroke_width = 10
            draw.path_start()
            draw.path_move(to=(10, 10))
            draw.path_line(to=(40, 40))
            draw.path_line(to=(0, -10), relative=True)
            draw.path_horizontal_line(x=45)
            draw.path_vertical_line(y=25)
            draw.path_horizontal_line(x=-5, relative=True)
            draw.path_vertical_line(y=-5, relative=True)
            draw.path_close()
            draw.path_finish()
            draw.draw(img)
        assert img[40, 40] == img[40, 30] == red
        assert img[45, 25] == img[40, 20] == red


def test_draw_path_line_user_error():
    with Drawing() as draw:
        # Test missing value
        with raises(TypeError):
            draw.path_line()
        with raises(TypeError):
            draw.path_horizontal_line()
        with raises(TypeError):
            draw.path_vertical_line()


def test_draw_move_user_error():
    with Drawing() as draw:
        # Test missing value
        with raises(TypeError):
            draw.path_move()


@mark.parametrize('kwargs', itertools.product(
    [('right', 40), ('width', 30)],
    [('bottom', 40), ('height', 30)]
))
def test_draw_rectangle(kwargs):
    white = Color('WHITE')
    black = Color('BLACK')
    gray = Color('#ccc')
    with Image(width=50, height=50, background=white) as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.stroke_width = 2
            ctx.fill_color = black
            ctx.stroke_color = gray
            ctx.rectangle(left=10, top=10, **dict(kwargs))
            ctx.draw(img)
        assert was != img.signature


@mark.parametrize('kwargs', itertools.product(
    [('xradius', 10), ('yradius', 10)],
    [('xradius', 20)],
    [('yradius', 20)],
    [('radius', 10)]
))
def test_draw_rectangle_with_radius(kwargs):
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.stroke_width = 2
            ctx.fill_color = 'black'
            ctx.stroke_color = '#ccc'
            ctx.rectangle(left=10, top=10,
                          width=30, height=30, **dict(kwargs))
            ctx.draw(img)
        assert was != img.signature


def test_draw_rotate():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as draw:
            draw.stroke_color = 'black'
            draw.rotate(45)
            draw.line((3, 3), (35, 35))
            draw.draw(img)
        assert was != img.signature


def test_draw_scale():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.fill_color = 'black'
            ctx.scale(x=2.0, y=0.5)
            ctx.rectangle(top=5, left=5, width=20, height=20)
            ctx.draw(img)
        assert was != img.signature


def test_set_fill_pattern_url():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.push_pattern('green_circle', 0, 0, 10, 10)
            ctx.fill_color = 'green'
            ctx.stroke_color = 'black'
            ctx.circle(origin=(5, 5), perimeter=(5, 0))
            ctx.pop_pattern()
            ctx.set_fill_pattern_url('#green_circle')
            ctx.rectangle(top=5, left=5, width=40, height=40)
            ctx.draw(img)
        assert was != img.signature


def test_set_stroke_pattern_url():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.push_pattern('green_ring', 0, 0, 6, 6)
            ctx.fill_color = 'green'
            ctx.stroke_color = 'white'
            ctx.circle(origin=(3, 3), perimeter=(3, 0))
            ctx.pop_pattern()
            ctx.set_stroke_pattern_url('#green_ring')
            ctx.stroke_width = 6
            ctx.rectangle(top=5, left=5, width=40, height=40)
            ctx.draw(img)
        assert was != img.signature


def test_draw_skew():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.stroke_color = 'black'
            ctx.skew(x=11, y=-24)
            ctx.line((3, 3), (35, 35))
            ctx.draw(img)
        assert was != img.signature


def test_draw_translate():
    with Image(width=50, height=50, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.stroke_color = 'black'
            ctx.translate(x=5, y=5)
            ctx.line((3, 3), (35, 35))
            ctx.draw(img)
        assert was != img.signature


def test_draw_text(fx_asset):
    with Image(width=100, height=100, background='white') as img:
        was = img.signature
        with Drawing() as ctx:
            ctx.font = str(fx_asset.joinpath('League_Gothic.otf'))
            ctx.font_size = 25
            ctx.fill_color = 'black'
            ctx.gravity = 'west'
            ctx.text(0, 0, 'Hello Wand')
            ctx.draw(img)
        assert was != img.signature


def test_get_font_metrics(fx_asset):
    with Image(width=144, height=192, background=Color('#fff')) as img:
        with Drawing() as draw:
            draw.font = str(fx_asset.joinpath('League_Gothic.otf'))
            draw.font_size = 13
            nm1 = draw.get_font_metrics(img, 'asdf1234')
            nm2 = draw.get_font_metrics(img, 'asdf1234asdf1234')
            nm3 = draw.get_font_metrics(img, 'asdf1234\nasdf1234')
            assert nm1.character_width == draw.font_size
            assert nm1.text_width < nm2.text_width
            assert nm2.text_width <= nm3.text_width
            assert nm2.text_height == nm3.text_height
            m1 = draw.get_font_metrics(img, 'asdf1234', True)
            m2 = draw.get_font_metrics(img, 'asdf1234asdf1234', True)
            m3 = draw.get_font_metrics(img, 'asdf1234\nasdf1234', True)
            assert m1.character_width == draw.font_size
            assert m1.text_width < m2.text_width
            assert m2.text_width > m3.text_width
            assert m2.text_height < m3.text_height


def test_viewbox():
    with Drawing() as draw:
        with raises(TypeError):
            draw.viewbox(None, None, None, None)
        with raises(TypeError):
            draw.viewbox(10, None, None, None)
        with raises(TypeError):
            draw.viewbox(10, 10, None, None)
        with raises(TypeError):
            draw.viewbox(10, 10, 100, None)
        draw.viewbox(10, 10, 100, 100)


def test_regression_issue_163(tmp_path):
    """https://github.com/emcconville/wand/issues/163"""
    unicode_char = b'\xce\xa6'.decode('utf-8')
    with Drawing() as draw:
        with Image(width=500, height=500) as image:
            draw.font_size = 20
            draw.gravity = 'south_west'
            draw.text(0, 0, unicode_char)
            draw(image)
            image.save(filename=str(tmp_path / 'out.jpg'))


def test_set_get_fill_opacity():
    with Drawing() as ctx:
        ctx.fill_opacity = 1.0
        assert ctx.fill_opacity == 1.0


def test_set_get_fill_opacity_user_error():
    with Drawing() as ctx:
        with raises(TypeError):
            ctx.fill_opacity = "1.5"


def test_set_get_fill_rule():
    with Drawing() as ctx:
        valid = 'evenodd'
        notvalid = 'error'
        invalid = (1, 2)
        ctx.fill_rule = valid
        assert ctx.fill_rule == valid
        with raises(ValueError):
            ctx.fill_rule = notvalid
        with raises(TypeError):
            ctx.fill_rule = invalid
        ctx.fill_rule = 'undefined'  # reset


@mark.skipif(MAGICK_VERSION_NUMBER < 0x700,
             reason='DrawGetOpacity always returns 1.0')
def test_set_get_opacity():
    with Drawing() as ctx:
        assert ctx.opacity == 1.0
        ctx.push()
        ctx.opacity = 0.5
        ctx.push()
        ctx.opacity = 0.25
        assert 0.24 < ctx.opacity < 0.26  # Expect float precision issues
        ctx.pop()
        assert 0.49 < ctx.opacity < 0.51  # Expect float precision issues
        ctx.pop()


def test_set_get_stroke_antialias():
    with Drawing() as ctx:
        ctx.stroke_antialias = False
        assert not ctx.stroke_antialias


def test_set_get_stroke_dash_array():
    with Drawing() as ctx:
        dash_array = [2, 1, 4, 1]
        ctx.stroke_dash_array = dash_array
        assert ctx.stroke_dash_array == dash_array


def test_set_get_stroke_dash_offset():
    with Drawing() as ctx:
        ctx.stroke_dash_offset = 0.5
        assert ctx.stroke_dash_offset == 0.5


def test_set_get_stroke_line_cap():
    with Drawing() as ctx:
        ctx.stroke_line_cap = 'round'
        assert ctx.stroke_line_cap == 'round'


def test_set_get_stroke_line_cap_user_error():
    with Drawing() as ctx:
        with raises(TypeError):
            ctx.stroke_line_cap = 0x74321870
        with raises(ValueError):
            ctx.stroke_line_cap = 'apples'


def test_set_get_stroke_line_join():
    with Drawing() as ctx:
        ctx.stroke_line_join = 'miter'
        assert ctx.stroke_line_join == 'miter'


def test_set_get_stroke_line_join_user_error():
    with Drawing() as ctx:
        with raises(TypeError):
            ctx.stroke_line_join = 0x74321870
        with raises(ValueError):
            ctx.stroke_line_join = 'apples'


def test_set_get_stroke_miter_limit():
    with Drawing() as ctx:
        ctx.stroke_miter_limit = 5
        assert ctx.stroke_miter_limit == 5


def test_set_get_stroke_miter_limit_user_error():
    with Drawing() as ctx:
        with raises(TypeError):
            ctx.stroke_miter_limit = '5'


def test_set_get_stroke_opacity():
    with Drawing() as ctx:
        ctx.stroke_opacity = 1.0
        assert ctx.stroke_opacity == 1.0


def test_set_get_stroke_opacity_user_error():
    with Drawing() as ctx:
        with raises(TypeError):
            ctx.stroke_opacity = '1.0'


def test_set_get_stroke_width_user_error():
    with Drawing() as ctx:
        with raises(TypeError):
            ctx.stroke_width = '0.1234'
        with raises(ValueError):
            ctx.stroke_width = -1.5


def test_draw_affine():
    with Image(width=100, height=100, background='skyblue') as img:
        was = img.signature
        img.format = 'png'
        with Drawing() as ctx:
            ctx.affine([1.5, 0.5, 0, 1.5, 45, 25])
            ctx.rectangle(top=5, left=5, width=25, height=25)
            ctx.draw(img)
        assert was != img.signature
    with raises(ValueError):
        with Drawing() as ctx:
            ctx.affine([1.0])
    with raises(TypeError):
        with Drawing() as ctx:
            ctx.affine(['a', 'b', 'c', 'd', 'e', 'f'])


def test_draw_clip_path():
    skyblue = Color('skyblue')
    orange = Color('orange')
    with Image(width=100, height=100, background='skyblue') as img:
        with Drawing() as ctx:
            ctx.push_defs()
            ctx.push_clip_path("eyes_only")
            ctx.push()
            ctx.rectangle(top=0, left=0, width=50, height=50)
            ctx.pop()
            ctx.pop_clip_path()
            ctx.pop_defs()
            ctx.clip_path = "eyes_only"
            ctx.clip_rule = "nonzero"
            ctx.clip_path_units = "object_bounding_box"
            ctx.fill_color = orange
            ctx.rectangle(top=5, left=5, width=90, height=90)
            ctx.draw(img)
        assert img[75, 75] == skyblue
