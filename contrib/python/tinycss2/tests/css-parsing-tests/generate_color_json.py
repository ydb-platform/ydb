from argparse import ArgumentParser
from colorsys import hls_to_rgb
from json import dumps
from pathlib import Path


def _round(value):
    return round(value + 0.0000001, 6)


def _trim(value):
    return str(int(value) if value.is_integer() else value)


def _function(name, channels, alpha, level):
    channels = (
        'none' if channel is None else
        channel if isinstance(channel, str) else
        _trim(_round(channel))
        for channel in channels)
    if level == 3:
        if alpha is None:
            return f'{name}({", ".join(channels)})'
        else:
            return f'{name}a({", ".join(channels)}, {alpha})'
    elif level == 4:
        if alpha is None:
            return f'{name}({" ".join(channels)})'
        else:
            return f'{name}({" ".join(channels)} / {alpha})'


def _dump_tests(tests):
    lines = (f'  {dumps(key)}, {dumps(value)}' for key, value in tests.items())
    return f'[\n{",\n".join(lines)}\n]'


FUNCTIONS = {}


def levels(*levels, spaces=None):
    def wrapper(function, level, space):
        return (lambda: function(level, space)) if space else (lambda: function(level))

    def decorate(function):
        for level in levels:
            if spaces is None:
                names = (function.__name__,)
            else:
                names = spaces
            for name in names:
                space = name
                if level:
                    name += f'_{level}'
                FUNCTIONS[name] = wrapper(function, level, space if spaces else None)
    return decorate


@levels(3, 4)
def keywords(level):
    tests = {}
    keywords = KEYWORDS if level == 3 else {'rebeccapurple': (102, 51, 153, None)}
    for test, rgba in keywords.items():
        if isinstance(rgba, str):
            tests[test] = rgba
        elif rgba:
            (*rgb, alpha) = rgba
            tests[test] = _function('rgb', rgb, alpha, level=3)
        else:
            tests[test] = None
    return _dump_tests(tests)


@levels(3, 4)
def hexadecimal(level):
    tests = {}
    for test, rgba in HEXADECIMAL.items():
        if rgba:
            (*rgb, alpha) = rgba
            if level == 4 or alpha is None:
                alpha = None if alpha == 1 else alpha
                tests[test] = _function('rgb', rgb, alpha, level=3)
        else:
            tests[test] = None
    return _dump_tests(tests)


@levels(3, 4)
def hsl(level):
    tests = {}
    for hue, saturation, lightness, alpha in HSL_VALUES:
        if level == 3:
            if None in (hue, saturation, lightness):
                continue
            test = _function(
                'hsl', (hue, f'{saturation}%', f'{lightness}%'), alpha, level)
        else:
            test = _function('hsl', (hue, saturation, lightness), alpha, level)
        hue = 0 if hue is None else hue
        lightness = 0 if lightness is None else lightness
        saturation = 0 if saturation is None else saturation
        rgb = tuple(
            coordinate * 255 for coordinate in
            hls_to_rgb(hue / 360, lightness / 100, saturation / 100))
        reference = _function('rgb', rgb, None if alpha == 1 else alpha, level=3)
        tests[test] = reference
    return _dump_tests(tests)


@levels(4)
def hwb(level):
    tests = {}
    for hue, white, black, alpha in HWB_VALUES:
        test = _function('hwb', (hue, white, black), alpha, level)
        hue = 0 if hue is None else hue
        white = 0 if white is None else white
        black = 0 if black is None else black
        if white + black >= 100:
            rgb = (white / (white + black) * 255,) * 3
        else:
            rgb = (
                ((channel * (100 - white - black)) + white) / 100 * 255
                for channel in hls_to_rgb(hue / 360, 0.5, 1))
        reference = _function('rgb', rgb, None if alpha == 1 else alpha, level=3)
        tests[test] = reference
    return _dump_tests(tests)


@levels(4, spaces=('lab', 'oklab'))
def lab(level, space):
    tests = {}
    values = LAB_VALUES if space == 'lab' else OKLAB_VALUES
    for test, reference in values.items():
        (*lab, alpha) = test
        test = _function(space, lab, alpha, level)
        (*lab, alpha) = reference
        reference = _function(space, lab, alpha, level)
        tests[test] = reference
    return _dump_tests(tests)


@levels(4, spaces=('lch', 'oklch'))
def lch(level, space):
    tests = {}
    values = LCH_VALUES if space == 'lch' else OKLCH_VALUES
    for test, reference in values.items():
        (*lch, alpha) = test
        test = _function(space, lch, alpha, level)
        (*lch, alpha) = reference
        reference = _function(space, lch, alpha, level)
        tests[test] = reference
    return _dump_tests(tests)


@levels(4)
def function(level):
    tests = {}
    for color_space, color_space_result in COLOR_SPACES.items():
        if color_space_result:
            permutations = COLOR_VALUES.items()
        else:
            permutations = (next(iter(COLOR_VALUES.items())),)
        for test, reference in permutations:
            (*channels, alpha) = test
            values = (color_space, *channels)
            test = _function('color', values, alpha, level)
            if color_space_result and reference:
                (*channels, alpha) = reference
                if color_space == 'xyz':
                    color_space = 'xyz-d65'
                values = (color_space, *channels)
                reference = _function('color', values, alpha, level)
            else:
                reference = None
            tests[test] = reference
    return _dump_tests(tests)


HEXADECIMAL = {
    test: (red, green, blue, alpha)
    for alpha in (None, 1, round(136 / 255, 6), 0)
    for blue in (0, 68, 255)
    for green in (0, 102, 255)
    for red in (0, 187, 255)
    for test in (
        f'#{red:02x}{green:02x}{blue:02x}' +
        ('' if alpha is None else f'{int(round(alpha*255)):02x}'),
        f'#{int(red/17):x}{int(green/17):x}{int(blue/17):x}' +
        ('' if alpha is None else f'{int(round(alpha*255/17)):x}'),
    )
} | {
    test: (red, green, blue, alpha)
    for alpha in (None, 1, round(49 / 255, 6), 0)
    for blue in (17, 33, 198)
    for green in (4, 97, 200)
    for red in (28, 48, 188)
    for test in (
        f'#{red:02x}{green:02x}{blue:02x}' +
        ('' if alpha is None else f'{int(round(alpha*255)):02x}'),
    )
}

KEYWORDS = {
    'transparent': (0, 0, 0, 0),

    'aliceblue': (240, 248, 255, None),
    'antiquewhite': (250, 235, 215, None),
    'aqua': (0, 255, 255, None),
    'aquamarine': (127, 255, 212, None),
    'azure': (240, 255, 255, None),
    'beige': (245, 245, 220, None),
    'bisque': (255, 228, 196, None),
    'black': (0, 0, 0, None),
    'blanchedalmond': (255, 235, 205, None),
    'blue': (0, 0, 255, None),
    'blueviolet': (138, 43, 226, None),
    'brown': (165, 42, 42, None),
    'burlywood': (222, 184, 135, None),
    'cadetblue': (95, 158, 160, None),
    'chartreuse': (127, 255, 0, None),
    'chocolate': (210, 105, 30, None),
    'coral': (255, 127, 80, None),
    'cornflowerblue': (100, 149, 237, None),
    'cornsilk': (255, 248, 220, None),
    'crimson': (220, 20, 60, None),
    'cyan': (0, 255, 255, None),
    'darkblue': (0, 0, 139, None),
    'darkcyan': (0, 139, 139, None),
    'darkgoldenrod': (184, 134, 11, None),
    'darkgray': (169, 169, 169, None),
    'darkgreen': (0, 100, 0, None),
    'darkgrey': (169, 169, 169, None),
    'darkkhaki': (189, 183, 107, None),
    'darkmagenta': (139, 0, 139, None),
    'darkolivegreen': (85, 107, 47, None),
    'darkorange': (255, 140, 0, None),
    'darkorchid': (153, 50, 204, None),
    'darkred': (139, 0, 0, None),
    'darksalmon': (233, 150, 122, None),
    'darkseagreen': (143, 188, 143, None),
    'darkslateblue': (72, 61, 139, None),
    'darkslategray': (47, 79, 79, None),
    'darkslategrey': (47, 79, 79, None),
    'darkturquoise': (0, 206, 209, None),
    'darkviolet': (148, 0, 211, None),
    'deeppink': (255, 20, 147, None),
    'deepskyblue': (0, 191, 255, None),
    'dimgray': (105, 105, 105, None),
    'dimgrey': (105, 105, 105, None),
    'dodgerblue': (30, 144, 255, None),
    'firebrick': (178, 34, 34, None),
    'floralwhite': (255, 250, 240, None),
    'forestgreen': (34, 139, 34, None),
    'fuchsia': (255, 0, 255, None),
    'gainsboro': (220, 220, 220, None),
    'ghostwhite': (248, 248, 255, None),
    'gold': (255, 215, 0, None),
    'goldenrod': (218, 165, 32, None),
    'gray': (128, 128, 128, None),
    'green': (0, 128, 0, None),
    'greenyellow': (173, 255, 47, None),
    'grey': (128, 128, 128, None),
    'honeydew': (240, 255, 240, None),
    'hotpink': (255, 105, 180, None),
    'indianred': (205, 92, 92, None),
    'indigo': (75, 0, 130, None),
    'ivory': (255, 255, 240, None),
    'khaki': (240, 230, 140, None),
    'lavender': (230, 230, 250, None),
    'lavenderblush': (255, 240, 245, None),
    'lawngreen': (124, 252, 0, None),
    'lemonchiffon': (255, 250, 205, None),
    'lightblue': (173, 216, 230, None),
    'lightcoral': (240, 128, 128, None),
    'lightcyan': (224, 255, 255, None),
    'lightgoldenrodyellow': (250, 250, 210, None),
    'lightgray': (211, 211, 211, None),
    'lightgreen': (144, 238, 144, None),
    'lightgrey': (211, 211, 211, None),
    'lightpink': (255, 182, 193, None),
    'lightsalmon': (255, 160, 122, None),
    'lightseagreen': (32, 178, 170, None),
    'lightskyblue': (135, 206, 250, None),
    'lightslategray': (119, 136, 153, None),
    'lightslategrey': (119, 136, 153, None),
    'lightsteelblue': (176, 196, 222, None),
    'lightyellow': (255, 255, 224, None),
    'lime': (0, 255, 0, None),
    'limegreen': (50, 205, 50, None),
    'linen': (250, 240, 230, None),
    'magenta': (255, 0, 255, None),
    'maroon': (128, 0, 0, None),
    'mediumaquamarine': (102, 205, 170, None),
    'mediumblue': (0, 0, 205, None),
    'mediumorchid': (186, 85, 211, None),
    'mediumpurple': (147, 112, 219, None),
    'mediumseagreen': (60, 179, 113, None),
    'mediumslateblue': (123, 104, 238, None),
    'mediumspringgreen': (0, 250, 154, None),
    'mediumturquoise': (72, 209, 204, None),
    'mediumvioletred': (199, 21, 133, None),
    'midnightblue': (25, 25, 112, None),
    'mintcream': (245, 255, 250, None),
    'mistyrose': (255, 228, 225, None),
    'moccasin': (255, 228, 181, None),
    'navajowhite': (255, 222, 173, None),
    'navy': (0, 0, 128, None),
    'oldlace': (253, 245, 230, None),
    'olive': (128, 128, 0, None),
    'olivedrab': (107, 142, 35, None),
    'orange': (255, 165, 0, None),
    'orangered': (255, 69, 0, None),
    'orchid': (218, 112, 214, None),
    'palegoldenrod': (238, 232, 170, None),
    'palegreen': (152, 251, 152, None),
    'paleturquoise': (175, 238, 238, None),
    'palevioletred': (219, 112, 147, None),
    'papayawhip': (255, 239, 213, None),
    'peachpuff': (255, 218, 185, None),
    'peru': (205, 133, 63, None),
    'pink': (255, 192, 203, None),
    'plum': (221, 160, 221, None),
    'powderblue': (176, 224, 230, None),
    'purple': (128, 0, 128, None),
    'red': (255, 0, 0, None),
    'rosybrown': (188, 143, 143, None),
    'royalblue': (65, 105, 225, None),
    'saddlebrown': (139, 69, 19, None),
    'salmon': (250, 128, 114, None),
    'sandybrown': (244, 164, 96, None),
    'seagreen': (46, 139, 87, None),
    'seashell': (255, 245, 238, None),
    'sienna': (160, 82, 45, None),
    'silver': (192, 192, 192, None),
    'skyblue': (135, 206, 235, None),
    'slateblue': (106, 90, 205, None),
    'slategray': (112, 128, 144, None),
    'slategrey': (112, 128, 144, None),
    'snow': (255, 250, 250, None),
    'springgreen': (0, 255, 127, None),
    'steelblue': (70, 130, 180, None),
    'tan': (210, 180, 140, None),
    'teal': (0, 128, 128, None),
    'thistle': (216, 191, 216, None),
    'tomato': (255, 99, 71, None),
    'turquoise': (64, 224, 208, None),
    'violet': (238, 130, 238, None),
    'wheat': (245, 222, 179, None),
    'white': (255, 255, 255, None),
    'whitesmoke': (245, 245, 245, None),
    'yellow': (255, 255, 0, None),
    'yellowgreen': (154, 205, 50, None),

    'Black': (0, 0, 0, None),
    'BLACK': (0, 0, 0, None),
    'BLacK': (0, 0, 0, None),
    '\nblack\n': (0, 0, 0, None),

    'none': None,
    'bla/*bad*/ck': None,
    'black-black': None,
    'black-': None,
    '-black': None,
    '-bla-ck': None,
    '/* black */': None,
    'current/*yes*/Color': None,
}

HSL_VALUES = tuple(
    (hue, saturation, lightness, alpha)
    for alpha in (None, 1, 0.25, 0)
    for lightness in (0, 12.5, 44, 100, None)
    for saturation in (0, 33.33, 88, 100, None)
    for hue in (0, 17.5, 188, 360, None))

HWB_VALUES = tuple(
    (hue, white, black, alpha)
    for alpha in (None, 1, 0.55, 0)
    for white in (0, 55, 92.2, 100, None)
    for black in (0, 12, 86.2, 100, None)
    for hue in (0, 100, 190.7, 360, None))

COLOR_SPACES = {
    'srgb': 'srgb',
    'srgb-linear': 'srgb-linear',
    'display-p3': 'display-p3',
    'a98-rgb': 'a98-rgb',
    'prophoto-rgb': 'prophoto-rgb',
    'rec2020': 'rec2020',
    'xyz': 'xyz-d65',
    'xyz-d50': 'xyz-d50',
    'xyz-d65': 'xyz-d65',

    'xyz-d80': None,
    'rgb': None,
}

COLOR_VALUES = {
    ('0%', '0%', '0%', None): (0, 0, 0, None),
    ('10%', '10%', '10%', None): (0.1, 0.1, 0.1, None),
    (.2, .2, '25%', None): (0.2, 0.2, 0.25, None),
    (0, 0, 0, 1): (0, 0, 0, None),
    ('0%', 0, 0, 0.5): (0, 0, 0, 0.5),
    ('20%', 0, 10, 0.5): (0.2, 0, 10, 0.5),
    ('20%', 0, 10, '50%'): (0.2, 0, 10, 0.5),
    ('400%', 0, 10, '50%'): (4, 0, 10, 0.5),
    ('50%', -160, 160, None): (0.5, -160, 160, None),
    ('50%', -200, 200, None): (0.5, -200, 200, None),
    (0, 0, 0, '-10%'): (0, 0, 0, 0),
    (0, 0, 0, '110%'): (0, 0, 0, None),
    (0, 0, 0, '300%'): (0, 0, 0, None),
    (100, 200, 200, None): (100, 200, 200, None),
    (200, 200, 200, '200'): (200, 200, 200, None),
    (-200, -200, -200, None): (-200, -200, -200, None),
    (-200, -200, -200, -200): (-200, -200, -200, 0),
    ('100%', '200%', '200%', None): (1, 2, 2, None),
    ('200%', '200%', '100%', '200%'): (2, 2, 1, None),
    ('0%', '-100%', '-200.0%', '-200%'): (0, -1, -2, 0),
    (None, None, None, None): (None, None, None, None),
    (10, 'none', 'none', 0.5): (10, None, None, 0.5),

    ('10%90%.1%', None): (0.1, 0.9, 0.001, None),

    ('a', 'b', 'c', None): None,
    (1, 1, None): None,
    (1, 1, 1, 1, None): None,
    ('10% 90.% .1%', None): None,
}

def _lab_like_values(ab_ratio, lightness_ratio):
    return {
        test: (
            lightness * lightness_ratio,
            a * ab_ratio,
            b * ab_ratio,
            None if alpha in (None, 1) else alpha,
        )
        for alpha in (None, 1, 0.25, 0)
        for b in (0, 10, 100, 115, -10)
        for a in (0, 20, 100, 110, -10)
        for lightness in (0, 10, 100, 110, -10)
        for test in (
            (lightness * lightness_ratio, a * ab_ratio, b * ab_ratio, alpha),
            (f'{lightness}%', a * ab_ratio, b * ab_ratio,
             None if alpha is None else f'{alpha * 100}%'),
            (f'{lightness}%', f'{a}%', f'{b}%', alpha),
        )
    }

LAB_VALUES = _lab_like_values(1.25, 1)
OKLAB_VALUES = _lab_like_values(0.004, 0.01)

def _lch_like_values(chroma_ratio, lightness_ratio):
    return {
        test: (
            lightness * lightness_ratio,
            chroma * chroma_ratio,
            hue % 360,
            None if alpha in (None, 1) else alpha,
        )
        for alpha in (None, 1, 0.25, 0)
        for hue in (0, 10, 110, 700, -10)
        for chroma in (0, 30, 100, 150, -75)
        for lightness in (0, 10, 100, 110, -10)
        for test in (
            (lightness * lightness_ratio, chroma * chroma_ratio, hue, alpha),
            (f'{lightness}%', chroma * chroma_ratio, hue,
             None if alpha is None else f'{alpha * 100}%'),
            (f'{lightness}%', f'{chroma}%', f'{hue}deg', alpha),
        )
    }

LCH_VALUES = _lch_like_values(1.5, 1)
OKLCH_VALUES = _lch_like_values(0.004, 0.01)


parser = ArgumentParser(description='Generate JSON test files')
parser.add_argument('file', help='generation file', nargs='?', choices=FUNCTIONS)
args = parser.parse_args()
if args.file:
    print(FUNCTIONS[args.file]())
else:
    for name, function in FUNCTIONS.items():
        filename = f'color_{name}.json'
        print(f'Generating {filename}')
        Path(filename).write_text(function())
