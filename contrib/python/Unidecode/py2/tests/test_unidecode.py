# -*- coding: utf-8 -*-
# vim:ts=4 sw=4 expandtab softtabstop=4
import unittest
import sys
from unidecode import unidecode, unidecode_expect_ascii, unidecode_expect_nonascii, UnidecodeError
import warnings


class WarningLogger:
    def __init__(self):
        self.clear()

    def start(self, filter):
        self.showwarning_old = warnings.showwarning

        def showwarning_new(message, category, *args):
            if (filter in str(message)) and \
                    (category is RuntimeWarning):
                self.log.append((message, category))
            else:
                self.showwarning_old(message, category, *args)

        warnings.showwarning = showwarning_new
        warnings.filterwarnings("always")

    def stop(self):
        warnings.showwarning = self.showwarning_old

    def clear(self):
        self.log = []

if sys.version_info[0] >= 3:
    _chr = chr
else:
    _chr = unichr

class BaseTestUnidecode():
    @unittest.skipIf(sys.version_info[0] >= 3, "not python 2")
    def test_ascii_warning(self):
        wlog = WarningLogger()
        wlog.start("not an unicode object")

        for n in range(0,128):
            t = chr(n)

            r = self.unidecode(t)
            self.assertEqual(r, t)
            self.assertEqual(type(r), str)

        # Passing string objects to unidecode should raise a warning
        self.assertEqual(128, len(wlog.log))
        wlog.stop()

    def test_ascii(self):

        wlog = WarningLogger()
        wlog.start("not an unicode object")

        for n in range(0,128):
            t = _chr(n)

            r = self.unidecode(t)
            self.assertEqual(r, t)
            self.assertEqual(type(r), str)

        # unicode objects shouldn't raise warnings
        self.assertEqual(0, len(wlog.log))

        wlog.stop()

    def test_bmp(self):
        for n in range(0,0x10000):
            # skip over surrogate pairs, which throw a warning
            if 0xd800 <= n <= 0xdfff:
                continue

            # Just check that it doesn't throw an exception
            t = _chr(n)
            self.unidecode(t)

    def test_surrogates(self):
        wlog = WarningLogger()
        wlog.start("Surrogate character")

        for n in range(0xd800, 0xe000):
            t = _chr(n)
            s = self.unidecode(t)

            # Check that surrogate characters translate to nothing.
            self.assertEqual('', s)

        wlog.stop()
        self.assertEqual(0xe000-0xd800, len(wlog.log))

    def test_space(self):
        for n in range(0x80, 0x10000):
            t = _chr(n)
            if t.isspace():
                s = self.unidecode(t)
                self.assertTrue((s == '') or s.isspace(),
                        'unidecode(%r) should return an empty string or ASCII space, '
                        'since %r.isspace() is true. Instead it returns %r' % (t, t, s))

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_surrogate_pairs(self):
        # same character, written as a non-BMP character and a
        # surrogate pair
        s = u'\U0001d4e3'

        # Note: this needs to be constructed at run-time, otherwise
        # a "wide" Python seems to optimize it automatically into a
        # single character.
        s_sp_1 = u'\ud835'
        s_sp_2 = u'\udce3'
        s_sp = s_sp_1 + s_sp_2

        if sys.version_info < (3,4):
            self.assertEqual(s.encode('utf16'), s_sp.encode('utf16'))
        else:
            self.assertEqual(s.encode('utf16'), s_sp.encode('utf16', errors='surrogatepass'))

        wlog = WarningLogger()
        wlog.start("Surrogate character")

        a = self.unidecode(s)
        a_sp = self.unidecode(s_sp)

        self.assertEqual('T', a)

        # Two warnings should have been logged
        self.assertEqual(2, len(wlog.log))

        wlog.stop()

    def test_circled_latin(self):
        # 1 sequence of a-z
        for n in range(0, 26):
            a = chr(ord('a') + n)
            b = self.unidecode(_chr(0x24d0 + n))

            self.assertEqual(b, a)

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_mathematical_latin(self):
        # 13 consecutive sequences of A-Z, a-z with some codepoints
        # undefined. We just count the undefined ones and don't check
        # positions.
        empty = 0
        for n in range(0x1d400, 0x1d6a4):
            if n % 52 < 26:
                a = chr(ord('A') + n % 26)
            else:
                a = chr(ord('a') + n % 26)
            b = self.unidecode(_chr(n))

            if not b:
                empty += 1
            else:
                self.assertEqual(b, a)

        self.assertEqual(empty, 24)

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_mathematical_digits(self):
        # 5 consecutive sequences of 0-9
        for n in range(0x1d7ce, 0x1d800):
            a = chr(ord('0') + (n-0x1d7ce) % 10)
            b = self.unidecode(_chr(n))

            self.assertEqual(b, a)

    def test_specific(self):

        TESTS = [
                (u'Hello, World!',
                "Hello, World!"),

                (u'\'"\r\n',
                 "'\"\r\n"),

                (u'ČŽŠčžš',
                 "CZSczs"),

                (u'ア',
                 "a"),

                (u'α',
                "a"),

                (u'а',
                "a"),

                (u'ch\u00e2teau',
                "chateau"),

                (u'vi\u00f1edos',
                "vinedos"),

                (u'\u5317\u4EB0',
                "Bei Jing "),

                (u'Efﬁcient',
                "Efficient"),

                # https://github.com/iki/unidecode/commit/4a1d4e0a7b5a11796dc701099556876e7a520065
                (u'příliš žluťoučký kůň pěl ďábelské ódy',
                'prilis zlutoucky kun pel dabelske ody'),

                (u'PŘÍLIŠ ŽLUŤOUČKÝ KŮŇ PĚL ĎÁBELSKÉ ÓDY',
                'PRILIS ZLUTOUCKY KUN PEL DABELSKE ODY'),

                # Table that doesn't exist
                (u'\ua500',
                ''),

                # Table that has less than 256 entries
                (u'\u1eff',
                ''),
            ]

        for input, correct_output in TESTS:
            test_output = self.unidecode(input)
            self.assertEqual(test_output, correct_output)
            self.assertTrue(isinstance(test_output, str))

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_specific_wide(self):

        TESTS = [
                # Non-BMP character
                (u'\U0001d5a0',
                'A'),

                # Mathematical
                (u'\U0001d5c4\U0001d5c6/\U0001d5c1',
                'km/h'),

                (u'\u2124\U0001d552\U0001d55c\U0001d552\U0001d55b \U0001d526\U0001d52a\U0001d51e \U0001d4e4\U0001d4f7\U0001d4f2\U0001d4ec\U0001d4f8\U0001d4ed\U0001d4ee \U0001d4c8\U0001d4c5\u212f\U0001d4b8\U0001d4be\U0001d4bb\U0001d4be\U0001d4c0\U0001d4b6\U0001d4b8\U0001d4be\U0001d4bf\u212f \U0001d59f\U0001d586 \U0001d631\U0001d62a\U0001d634\U0001d622\U0001d637\U0001d626?!',
                'Zakaj ima Unicode specifikacije za pisave?!'),
            ]

        for input, correct_output in TESTS:
            test_output = self.unidecode(input)
            self.assertEqual(test_output, correct_output)
            self.assertTrue(isinstance(test_output, str))

    def test_wordpress_remove_accents(self):
        # This is the table from remove_accents() WordPress function.
        # https://core.trac.wordpress.org/browser/trunk/wp-includes/formatting.php

        def b(x):
            return (x,)

        wp_remove_accents = {
            # Decompositions for Latin-1 Supplement
            b(194)+b(170) : 'a', b(194)+b(186) : 'o',
            b(195)+b(128) : 'A', b(195)+b(129) : 'A',
            b(195)+b(130) : 'A', b(195)+b(131) : 'A',
            b(195)+b(133) : 'A',
            b(195)+b(134) : 'AE',b(195)+b(135) : 'C',
            b(195)+b(136) : 'E', b(195)+b(137) : 'E',
            b(195)+b(138) : 'E', b(195)+b(139) : 'E',
            b(195)+b(140) : 'I', b(195)+b(141) : 'I',
            b(195)+b(142) : 'I', b(195)+b(143) : 'I',
            b(195)+b(144) : 'D', b(195)+b(145) : 'N',
            b(195)+b(146) : 'O', b(195)+b(147) : 'O',
            b(195)+b(148) : 'O', b(195)+b(149) : 'O',
            b(195)+b(153) : 'U',
            b(195)+b(154) : 'U', b(195)+b(155) : 'U',
            b(195)+b(157) : 'Y',
            b(195)+b(160) : 'a', b(195)+b(161) : 'a',
            b(195)+b(162) : 'a', b(195)+b(163) : 'a',
            b(195)+b(165) : 'a',
            b(195)+b(166) : 'ae',b(195)+b(167) : 'c',
            b(195)+b(168) : 'e', b(195)+b(169) : 'e',
            b(195)+b(170) : 'e', b(195)+b(171) : 'e',
            b(195)+b(172) : 'i', b(195)+b(173) : 'i',
            b(195)+b(174) : 'i', b(195)+b(175) : 'i',
            b(195)+b(176) : 'd', b(195)+b(177) : 'n',
            b(195)+b(178) : 'o', b(195)+b(179) : 'o',
            b(195)+b(180) : 'o', b(195)+b(181) : 'o',
            b(195)+b(184) : 'o',
            b(195)+b(185) : 'u', b(195)+b(186) : 'u',
            b(195)+b(187) : 'u',
            b(195)+b(189) : 'y', b(195)+b(190) : 'th',
            b(195)+b(191) : 'y', b(195)+b(152) : 'O',
            # Decompositions for Latin Extended-A
            b(196)+b(128) : 'A', b(196)+b(129) : 'a',
            b(196)+b(130) : 'A', b(196)+b(131) : 'a',
            b(196)+b(132) : 'A', b(196)+b(133) : 'a',
            b(196)+b(134) : 'C', b(196)+b(135) : 'c',
            b(196)+b(136) : 'C', b(196)+b(137) : 'c',
            b(196)+b(138) : 'C', b(196)+b(139) : 'c',
            b(196)+b(140) : 'C', b(196)+b(141) : 'c',
            b(196)+b(142) : 'D', b(196)+b(143) : 'd',
            b(196)+b(144) : 'D', b(196)+b(145) : 'd',
            b(196)+b(146) : 'E', b(196)+b(147) : 'e',
            b(196)+b(148) : 'E', b(196)+b(149) : 'e',
            b(196)+b(150) : 'E', b(196)+b(151) : 'e',
            b(196)+b(152) : 'E', b(196)+b(153) : 'e',
            b(196)+b(154) : 'E', b(196)+b(155) : 'e',
            b(196)+b(156) : 'G', b(196)+b(157) : 'g',
            b(196)+b(158) : 'G', b(196)+b(159) : 'g',
            b(196)+b(160) : 'G', b(196)+b(161) : 'g',
            b(196)+b(162) : 'G', b(196)+b(163) : 'g',
            b(196)+b(164) : 'H', b(196)+b(165) : 'h',
            b(196)+b(166) : 'H', b(196)+b(167) : 'h',
            b(196)+b(168) : 'I', b(196)+b(169) : 'i',
            b(196)+b(170) : 'I', b(196)+b(171) : 'i',
            b(196)+b(172) : 'I', b(196)+b(173) : 'i',
            b(196)+b(174) : 'I', b(196)+b(175) : 'i',
            b(196)+b(176) : 'I', b(196)+b(177) : 'i',
            b(196)+b(178) : 'IJ',b(196)+b(179) : 'ij',
            b(196)+b(180) : 'J', b(196)+b(181) : 'j',
            b(196)+b(182) : 'K', b(196)+b(183) : 'k',
            b(196)+b(184) : 'k', b(196)+b(185) : 'L',
            b(196)+b(186) : 'l', b(196)+b(187) : 'L',
            b(196)+b(188) : 'l', b(196)+b(189) : 'L',
            b(196)+b(190) : 'l', b(196)+b(191) : 'L',
            b(197)+b(128) : 'l', b(197)+b(129) : 'L',
            b(197)+b(130) : 'l', b(197)+b(131) : 'N',
            b(197)+b(132) : 'n', b(197)+b(133) : 'N',
            b(197)+b(134) : 'n', b(197)+b(135) : 'N',
            b(197)+b(136) : 'n',
            b(197)+b(140) : 'O', b(197)+b(141) : 'o',
            b(197)+b(142) : 'O', b(197)+b(143) : 'o',
            b(197)+b(144) : 'O', b(197)+b(145) : 'o',
            b(197)+b(146) : 'OE',b(197)+b(147) : 'oe',
            b(197)+b(148) : 'R',b(197)+b(149) : 'r',
            b(197)+b(150) : 'R',b(197)+b(151) : 'r',
            b(197)+b(152) : 'R',b(197)+b(153) : 'r',
            b(197)+b(154) : 'S',b(197)+b(155) : 's',
            b(197)+b(156) : 'S',b(197)+b(157) : 's',
            b(197)+b(158) : 'S',b(197)+b(159) : 's',
            b(197)+b(160) : 'S', b(197)+b(161) : 's',
            b(197)+b(162) : 'T', b(197)+b(163) : 't',
            b(197)+b(164) : 'T', b(197)+b(165) : 't',
            b(197)+b(166) : 'T', b(197)+b(167) : 't',
            b(197)+b(168) : 'U', b(197)+b(169) : 'u',
            b(197)+b(170) : 'U', b(197)+b(171) : 'u',
            b(197)+b(172) : 'U', b(197)+b(173) : 'u',
            b(197)+b(174) : 'U', b(197)+b(175) : 'u',
            b(197)+b(176) : 'U', b(197)+b(177) : 'u',
            b(197)+b(178) : 'U', b(197)+b(179) : 'u',
            b(197)+b(180) : 'W', b(197)+b(181) : 'w',
            b(197)+b(182) : 'Y', b(197)+b(183) : 'y',
            b(197)+b(184) : 'Y', b(197)+b(185) : 'Z',
            b(197)+b(186) : 'z', b(197)+b(187) : 'Z',
            b(197)+b(188) : 'z', b(197)+b(189) : 'Z',
            b(197)+b(190) : 'z', b(197)+b(191) : 's',
            # Decompositions for Latin Extended-B
            b(200)+b(152) : 'S', b(200)+b(153) : 's',
            b(200)+b(154) : 'T', b(200)+b(155) : 't',

            # Vowels with diacritic (Vietnamese)
            # unmarked
            b(198)+b(160) : 'O', b(198)+b(161) : 'o',
            b(198)+b(175) : 'U', b(198)+b(176) : 'u',
            # grave accent
            b(225)+b(186)+b(166) : 'A', b(225)+b(186)+b(167) : 'a',
            b(225)+b(186)+b(176) : 'A', b(225)+b(186)+b(177) : 'a',
            b(225)+b(187)+b(128) : 'E', b(225)+b(187)+b(129) : 'e',
            b(225)+b(187)+b(146) : 'O', b(225)+b(187)+b(147) : 'o',
            b(225)+b(187)+b(156) : 'O', b(225)+b(187)+b(157) : 'o',
            b(225)+b(187)+b(170) : 'U', b(225)+b(187)+b(171) : 'u',
            b(225)+b(187)+b(178) : 'Y', b(225)+b(187)+b(179) : 'y',
            # hook
            b(225)+b(186)+b(162) : 'A', b(225)+b(186)+b(163) : 'a',
            b(225)+b(186)+b(168) : 'A', b(225)+b(186)+b(169) : 'a',
            b(225)+b(186)+b(178) : 'A', b(225)+b(186)+b(179) : 'a',
            b(225)+b(186)+b(186) : 'E', b(225)+b(186)+b(187) : 'e',
            b(225)+b(187)+b(130) : 'E', b(225)+b(187)+b(131) : 'e',
            b(225)+b(187)+b(136) : 'I', b(225)+b(187)+b(137) : 'i',
            b(225)+b(187)+b(142) : 'O', b(225)+b(187)+b(143) : 'o',
            b(225)+b(187)+b(148) : 'O', b(225)+b(187)+b(149) : 'o',
            b(225)+b(187)+b(158) : 'O', b(225)+b(187)+b(159) : 'o',
            b(225)+b(187)+b(166) : 'U', b(225)+b(187)+b(167) : 'u',
            b(225)+b(187)+b(172) : 'U', b(225)+b(187)+b(173) : 'u',
            b(225)+b(187)+b(182) : 'Y', b(225)+b(187)+b(183) : 'y',
            # tilde
            b(225)+b(186)+b(170) : 'A', b(225)+b(186)+b(171) : 'a',
            b(225)+b(186)+b(180) : 'A', b(225)+b(186)+b(181) : 'a',
            b(225)+b(186)+b(188) : 'E', b(225)+b(186)+b(189) : 'e',
            b(225)+b(187)+b(132) : 'E', b(225)+b(187)+b(133) : 'e',
            b(225)+b(187)+b(150) : 'O', b(225)+b(187)+b(151) : 'o',
            b(225)+b(187)+b(160) : 'O', b(225)+b(187)+b(161) : 'o',
            b(225)+b(187)+b(174) : 'U', b(225)+b(187)+b(175) : 'u',
            b(225)+b(187)+b(184) : 'Y', b(225)+b(187)+b(185) : 'y',
            # acute accent
            b(225)+b(186)+b(164) : 'A', b(225)+b(186)+b(165) : 'a',
            b(225)+b(186)+b(174) : 'A', b(225)+b(186)+b(175) : 'a',
            b(225)+b(186)+b(190) : 'E', b(225)+b(186)+b(191) : 'e',
            b(225)+b(187)+b(144) : 'O', b(225)+b(187)+b(145) : 'o',
            b(225)+b(187)+b(154) : 'O', b(225)+b(187)+b(155) : 'o',
            b(225)+b(187)+b(168) : 'U', b(225)+b(187)+b(169) : 'u',
            # dot below
            b(225)+b(186)+b(160) : 'A', b(225)+b(186)+b(161) : 'a',
            b(225)+b(186)+b(172) : 'A', b(225)+b(186)+b(173) : 'a',
            b(225)+b(186)+b(182) : 'A', b(225)+b(186)+b(183) : 'a',
            b(225)+b(186)+b(184) : 'E', b(225)+b(186)+b(185) : 'e',
            b(225)+b(187)+b(134) : 'E', b(225)+b(187)+b(135) : 'e',
            b(225)+b(187)+b(138) : 'I', b(225)+b(187)+b(139) : 'i',
            b(225)+b(187)+b(140) : 'O', b(225)+b(187)+b(141) : 'o',
            b(225)+b(187)+b(152) : 'O', b(225)+b(187)+b(153) : 'o',
            b(225)+b(187)+b(162) : 'O', b(225)+b(187)+b(163) : 'o',
            b(225)+b(187)+b(164) : 'U', b(225)+b(187)+b(165) : 'u',
            b(225)+b(187)+b(176) : 'U', b(225)+b(187)+b(177) : 'u',
            b(225)+b(187)+b(180) : 'Y', b(225)+b(187)+b(181) : 'y',
            # Vowels with diacritic (Chinese, Hanyu Pinyin)
            b(201)+b(145) : 'a',
            # macron
            b(199)+b(149) : 'U', b(199)+b(150) : 'u',
            # acute accent
            b(199)+b(151) : 'U', b(199)+b(152) : 'u',
            # caron
            b(199)+b(141) : 'A', b(199)+b(142) : 'a',
            b(199)+b(143) : 'I', b(199)+b(144) : 'i',
            b(199)+b(145) : 'O', b(199)+b(146) : 'o',
            b(199)+b(147) : 'U', b(199)+b(148) : 'u',
            b(199)+b(153) : 'U', b(199)+b(154) : 'u',
            # grave accent
            b(199)+b(155) : 'U', b(199)+b(156) : 'u',

            b(195)+b(132) : 'A',
            b(195)+b(150) : 'O',
            b(195)+b(156) : 'U',
            #b(195)+b(159) : 's',
            b(195)+b(164) : 'a',
            b(195)+b(182) : 'o',
            b(195)+b(188) : 'u',

            # Known differences:

            #b(195)+b(158) : 'TH',
            #b(197)+b(137) : 'N',
            #b(197)+b(138) : 'n',
            #b(197)+b(139) : 'N',

            # Euro Sign
            #b(226)+b(130)+b(172) : 'E',

            # GBP (Pound) Sign
            #b(194)+b(163) : '',
        }

        for utf8_input, correct_output in wp_remove_accents.items():
            if sys.version_info[0] >= 3:
                inp = bytes(utf8_input).decode('utf8')
            else:
                inp = ''.join(map(chr, utf8_input)).decode('utf8')

            output = self.unidecode(inp)

            self.assertEqual(correct_output, output)

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_unicode_text_converter(self):
        # Examples from http://www.panix.com/~eli/unicode/convert.cgi
        lower = [
            # Fullwidth
            u'\uff54\uff48\uff45 \uff51\uff55\uff49\uff43\uff4b \uff42\uff52\uff4f\uff57\uff4e \uff46\uff4f\uff58 \uff4a\uff55\uff4d\uff50\uff53 \uff4f\uff56\uff45\uff52 \uff54\uff48\uff45 \uff4c\uff41\uff5a\uff59 \uff44\uff4f\uff47 \uff11\uff12\uff13\uff14\uff15\uff16\uff17\uff18\uff19\uff10',
            # Double-struck
            u'\U0001d565\U0001d559\U0001d556 \U0001d562\U0001d566\U0001d55a\U0001d554\U0001d55c \U0001d553\U0001d563\U0001d560\U0001d568\U0001d55f \U0001d557\U0001d560\U0001d569 \U0001d55b\U0001d566\U0001d55e\U0001d561\U0001d564 \U0001d560\U0001d567\U0001d556\U0001d563 \U0001d565\U0001d559\U0001d556 \U0001d55d\U0001d552\U0001d56b\U0001d56a \U0001d555\U0001d560\U0001d558 \U0001d7d9\U0001d7da\U0001d7db\U0001d7dc\U0001d7dd\U0001d7de\U0001d7df\U0001d7e0\U0001d7e1\U0001d7d8',
            # Bold
            u'\U0001d42d\U0001d421\U0001d41e \U0001d42a\U0001d42e\U0001d422\U0001d41c\U0001d424 \U0001d41b\U0001d42b\U0001d428\U0001d430\U0001d427 \U0001d41f\U0001d428\U0001d431 \U0001d423\U0001d42e\U0001d426\U0001d429\U0001d42c \U0001d428\U0001d42f\U0001d41e\U0001d42b \U0001d42d\U0001d421\U0001d41e \U0001d425\U0001d41a\U0001d433\U0001d432 \U0001d41d\U0001d428\U0001d420 \U0001d7cf\U0001d7d0\U0001d7d1\U0001d7d2\U0001d7d3\U0001d7d4\U0001d7d5\U0001d7d6\U0001d7d7\U0001d7ce',
            # Bold italic
            u'\U0001d495\U0001d489\U0001d486 \U0001d492\U0001d496\U0001d48a\U0001d484\U0001d48c \U0001d483\U0001d493\U0001d490\U0001d498\U0001d48f \U0001d487\U0001d490\U0001d499 \U0001d48b\U0001d496\U0001d48e\U0001d491\U0001d494 \U0001d490\U0001d497\U0001d486\U0001d493 \U0001d495\U0001d489\U0001d486 \U0001d48d\U0001d482\U0001d49b\U0001d49a \U0001d485\U0001d490\U0001d488 1234567890',
            # Bold script
            u'\U0001d4fd\U0001d4f1\U0001d4ee \U0001d4fa\U0001d4fe\U0001d4f2\U0001d4ec\U0001d4f4 \U0001d4eb\U0001d4fb\U0001d4f8\U0001d500\U0001d4f7 \U0001d4ef\U0001d4f8\U0001d501 \U0001d4f3\U0001d4fe\U0001d4f6\U0001d4f9\U0001d4fc \U0001d4f8\U0001d4ff\U0001d4ee\U0001d4fb \U0001d4fd\U0001d4f1\U0001d4ee \U0001d4f5\U0001d4ea\U0001d503\U0001d502 \U0001d4ed\U0001d4f8\U0001d4f0 1234567890',
            # Fraktur
            u'\U0001d599\U0001d58d\U0001d58a \U0001d596\U0001d59a\U0001d58e\U0001d588\U0001d590 \U0001d587\U0001d597\U0001d594\U0001d59c\U0001d593 \U0001d58b\U0001d594\U0001d59d \U0001d58f\U0001d59a\U0001d592\U0001d595\U0001d598 \U0001d594\U0001d59b\U0001d58a\U0001d597 \U0001d599\U0001d58d\U0001d58a \U0001d591\U0001d586\U0001d59f\U0001d59e \U0001d589\U0001d594\U0001d58c 1234567890',
        ]

        for s in lower:
            o = self.unidecode(s)

            self.assertEqual('the quick brown fox jumps over the lazy dog 1234567890', o)

        upper = [
            # Fullwidth
            u'\uff34\uff28\uff25 \uff31\uff35\uff29\uff23\uff2b \uff22\uff32\uff2f\uff37\uff2e \uff26\uff2f\uff38 \uff2a\uff35\uff2d\uff30\uff33 \uff2f\uff36\uff25\uff32 \uff34\uff28\uff25 \uff2c\uff21\uff3a\uff39 \uff24\uff2f\uff27 \uff11\uff12\uff13\uff14\uff15\uff16\uff17\uff18\uff19\uff10',
            # Double-struck
            u'\U0001d54b\u210d\U0001d53c \u211a\U0001d54c\U0001d540\u2102\U0001d542 \U0001d539\u211d\U0001d546\U0001d54e\u2115 \U0001d53d\U0001d546\U0001d54f \U0001d541\U0001d54c\U0001d544\u2119\U0001d54a \U0001d546\U0001d54d\U0001d53c\u211d \U0001d54b\u210d\U0001d53c \U0001d543\U0001d538\u2124\U0001d550 \U0001d53b\U0001d546\U0001d53e \U0001d7d9\U0001d7da\U0001d7db\U0001d7dc\U0001d7dd\U0001d7de\U0001d7df\U0001d7e0\U0001d7e1\U0001d7d8',
            # Bold
            u'\U0001d413\U0001d407\U0001d404 \U0001d410\U0001d414\U0001d408\U0001d402\U0001d40a \U0001d401\U0001d411\U0001d40e\U0001d416\U0001d40d \U0001d405\U0001d40e\U0001d417 \U0001d409\U0001d414\U0001d40c\U0001d40f\U0001d412 \U0001d40e\U0001d415\U0001d404\U0001d411 \U0001d413\U0001d407\U0001d404 \U0001d40b\U0001d400\U0001d419\U0001d418 \U0001d403\U0001d40e\U0001d406 \U0001d7cf\U0001d7d0\U0001d7d1\U0001d7d2\U0001d7d3\U0001d7d4\U0001d7d5\U0001d7d6\U0001d7d7\U0001d7ce',
            # Bold italic
            u'\U0001d47b\U0001d46f\U0001d46c \U0001d478\U0001d47c\U0001d470\U0001d46a\U0001d472 \U0001d469\U0001d479\U0001d476\U0001d47e\U0001d475 \U0001d46d\U0001d476\U0001d47f \U0001d471\U0001d47c\U0001d474\U0001d477\U0001d47a \U0001d476\U0001d47d\U0001d46c\U0001d479 \U0001d47b\U0001d46f\U0001d46c \U0001d473\U0001d468\U0001d481\U0001d480 \U0001d46b\U0001d476\U0001d46e 1234567890',
            # Bold script
            u'\U0001d4e3\U0001d4d7\U0001d4d4 \U0001d4e0\U0001d4e4\U0001d4d8\U0001d4d2\U0001d4da \U0001d4d1\U0001d4e1\U0001d4de\U0001d4e6\U0001d4dd \U0001d4d5\U0001d4de\U0001d4e7 \U0001d4d9\U0001d4e4\U0001d4dc\U0001d4df\U0001d4e2 \U0001d4de\U0001d4e5\U0001d4d4\U0001d4e1 \U0001d4e3\U0001d4d7\U0001d4d4 \U0001d4db\U0001d4d0\U0001d4e9\U0001d4e8 \U0001d4d3\U0001d4de\U0001d4d6 1234567890',
            # Fraktur
            u'\U0001d57f\U0001d573\U0001d570 \U0001d57c\U0001d580\U0001d574\U0001d56e\U0001d576 \U0001d56d\U0001d57d\U0001d57a\U0001d582\U0001d579 \U0001d571\U0001d57a\U0001d583 \U0001d575\U0001d580\U0001d578\U0001d57b\U0001d57e \U0001d57a\U0001d581\U0001d570\U0001d57d \U0001d57f\U0001d573\U0001d570 \U0001d577\U0001d56c\U0001d585\U0001d584 \U0001d56f\U0001d57a\U0001d572 1234567890',
        ]

        for s in upper:
            o = self.unidecode(s)

            self.assertEqual('THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG 1234567890', o)

    def test_enclosed_alphanumerics(self):
        self.assertEqual(
            'aA20(20)20.20100',
            self.unidecode(u'ⓐⒶ⑳⒇⒛⓴⓾⓿'),
        )

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_errors_ignore(self):
        # unidecode doesn't have replacements for private use characters
        o = self.unidecode(u"test \U000f0000 test", errors='ignore')
        self.assertEqual('test  test', o)

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_errors_replace(self):
        o = self.unidecode(u"test \U000f0000 test", errors='replace')
        self.assertEqual('test ? test', o)

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_errors_replace_str(self):
        o = self.unidecode(u"test \U000f0000 test", errors='replace', replace_str='[?] ')
        self.assertEqual('test [?]  test', o)

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_errors_strict(self):
        with self.assertRaises(UnidecodeError) as e:
            o = self.unidecode(u"test \U000f0000 test", errors='strict')

        self.assertEqual(5, e.exception.index)

        # This checks that the exception is not chained (i.e. you don't get the
        # "During handling of the above exception, another exception occurred")
        if sys.version_info[0] >= 3:
            self.assertIsNone(e.exception.__context__)

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_errors_preserve(self):
        s = u"test \U000f0000 test"
        o = self.unidecode(s, errors='preserve')

        self.assertEqual(s, o)

    @unittest.skipIf(sys.maxunicode < 0x10000, "narrow build")
    def test_errors_invalid(self):
        with self.assertRaises(UnidecodeError) as e:
            self.unidecode(u"test \U000f0000 test", errors='invalid')

        # This checks that the exception is not chained (i.e. you don't get the
        # "During handling of the above exception, another exception occurred")
        if sys.version_info[0] >= 3:
            self.assertIsNone(e.exception.__context__)

class TestUnidecode(BaseTestUnidecode, unittest.TestCase):
    unidecode = staticmethod(unidecode)

class TestUnidecodeExpectASCII(BaseTestUnidecode, unittest.TestCase):
    unidecode = staticmethod(unidecode_expect_ascii)

class TestUnidecodeExpectNonASCII(BaseTestUnidecode, unittest.TestCase):
    unidecode = staticmethod(unidecode_expect_nonascii)

if __name__ == "__main__":
    unittest.main()
