#!/usr/bin/env python
import math
import unittest

import ephem

class StarTests(unittest.TestCase):
    def test_Arcturus(self):
        s = ephem.star('Arcturus')
        s.compute(ephem.date('2017/1/1 12:00'))
        self.assertEqual(str(s.ra), '14:16:25.12')
        self.assertEqual(str(s.dec), '19:05:39.8')

    def test_Fomalhaut(self):
        s = ephem.star('Fomalhaut')
        self.assertEqual(s.name, 'Fomalhaut')
        self.assertRaises(RuntimeError, getattr, s, 'ra')

    def test_Fomalhaut_compute(self):
        s = ephem.star('Fomalhaut')
        s.compute()
        self.assertEqual(s.name, 'Fomalhaut')
        self.assertEqual(str(s._ra), '22:57:39.05')

    def test_Fomalhaut_autocompute(self):
        s = ephem.star('Fomalhaut', '1971/1/1')
        self.assertEqual(s.name, 'Fomalhaut')
        self.assertEqual(str(s._ra), '22:57:39.05')

    def test_catalog_size(self):
        # Prevent catalog from changing size accidentally during editing.
        self.assertEqual(len(ephem.stars.stars), 116)

    def test_unknown_star(self):
        self.assertRaises(KeyError, ephem.star, 'Alpha Centauri')

class Test57NavigationStars(unittest.TestCase):
    """Tests all 57 common navigation stars against the Nautical Almanac
    for the date 2018-01-01 00:00:00.

    Reference data is from Her Majesty's Nautical Almanac Office Publication
    NP314-18 "The Nautical Almanac" (2017), ISBN 978-0-7077-41819.

    A secondary source, online, is:
    http://www.tecepe.com.br/scripts/AlmanacPagesISAPI.dll/pages?date=1%2F1%2F2018
    """
    NP314_DATE = '2018-01-01 00:00:00'
    # The resolution of NP314 is 0.1 minutes so we should only expect an error
    # of +/- 0.05 minutes.
    # In practice the errors exceed this so these tests are less
    # strict imposing an absolute error of +/- 0.3 minutes:
    # Tolerance        Failures
    # +/- 0.3 minutes     0
    # +/- 0.2 minutes     2
    # +/- 0.1 minutes     6
    # +/- 0.05 minutes   22
    MAX_ERROR_DEGREES = 0.3 * 1.0 / 60

    def _test_body(self, name, sha, dec, date=NP314_DATE):
        obj = ephem.star(name)
        obs = ephem.Observer()
        obs.date = date
        obj.compute(obs)
        exp_sha = ephem.degrees(sha.replace(' ', ':'))
        err_sha = math.degrees(2 * ephem.pi - obj.g_ra - exp_sha)
        self.assertTrue(
            abs(err_sha) < self.MAX_ERROR_DEGREES,
            'SHA fail: {0!s:} !< {1!s:}'
            .format(abs(err_sha), self.MAX_ERROR_DEGREES)
        )
        exp_dec = ephem.degrees(dec.replace(' ', ':'))
        err_dec = math.degrees(obj.g_dec - exp_dec)
        self.assertTrue(
            abs(err_dec) < self.MAX_ERROR_DEGREES,
            'Dec. fail: {0!s:} !< {1!s:}'
            .format(abs(err_dec), self.MAX_ERROR_DEGREES)
        )

    def test_Acamar(self):
        self._test_body('Acamar', '315 15.8', '-40 14.3')

    def test_Achernar(self):
        self._test_body('Achernar', '335 24.4', '-57 09.2')

    def test_Acrux(self):
        self._test_body('Acrux', '173 05.7', '-63 11.5')

    def test_Adhara(self):
        self._test_body('Adhara', '255 09.7', '-29 00.0')

    def test_Aldebaran(self):
        self._test_body('Aldebaran', '290 45.5', '16 32.5')

    def test_Alioth(self):
        self._test_body('Alioth', '166 18.1', '55 51.5')

    def test_Alkaid(self):
        self._test_body('Alkaid', '152 56.7', '49 13.3')

    def test_Alnair(self):
        self._test_body('Alnair', '27 40.3', '-46 52.6')

    def test_Alnilam(self):
        self._test_body('Alnilam', '275 42.9', '-1 11.7')

    def test_Alphard(self):
        self._test_body('Alphard', '217 52.8', '-8 44.3')

    def test_Alphecca(self):
        self._test_body('Alphecca', '126 8.7', '26 39.3')

    def test_Alpheratz(self):
        self._test_body('Alpheratz', '357 40.3', '29 11.4')

    def test_Altair(self):
        self._test_body('Altair', '62 5.6', '8 55.1')

    def test_Ankaa(self):
        self._test_body('Ankaa', '353 12.7', '-42 12.8')

    def test_Antares(self):
        self._test_body('Antares', '112 22.8', '-26 28.0')

    def test_Arcturus(self):
        self._test_body('Arcturus', '145 53.1', '19 5.4')

    def test_Atria(self):
        self._test_body('Atria', '107 22.3', '-69 3.2')

    def test_Avior(self):
        self._test_body('Avior', '234 16.1', '-59 34.0')

    def test_Bellatrix(self):
        self._test_body('Bellatrix', '278 28.4', '6 21.7')

    def test_Betelgeuse(self):
        self._test_body('Betelgeuse', '270 57.6', '7 24.4')

    def test_Canopus(self):
        self._test_body('Canopus', '263 54.2', '-52 42.5')

    def test_Capella(self):
        self._test_body('Capella', '280 29.3', '46 0.8')

    def test_Deneb(self):
        self._test_body('Deneb', '49 29.8', '45 20.9')

    def test_Denebola(self):
        self._test_body('Denebola', '182 30.5', '14 28.1')

    def test_Diphda(self):
        self._test_body('Diphda', '348 52.8', '-17 53.5')

    def test_Dubhe(self):
        self._test_body('Dubhe', '193 47.8', '61 39.0')

    def test_Elnath(self):
        self._test_body('Elnath', '278 8.4', '28 37.2')

    def test_Eltanin(self):
        self._test_body('Eltanin', '90 45.3', '51 29.3')

    def test_Enif(self):
        self._test_body('Enif', '33 44.3', '9 57.5')

    def test_Fomalhaut(self):
        self._test_body('Fomalhaut', '15 20.8', '-29 31.8')

    def test_Gacrux(self):
        self._test_body('Gacrux', '171 57.3', '-57 12.5')

    def test_Gienah(self):
        self._test_body('Gienah', '175 49.1', '-17 38.3')

    def test_Hadar(self):
        self._test_body('Hadar', '148 43.7', '-60 27.1')

    def test_Hamal(self):
        self._test_body('Hamal', '327 57.1', '23 32.8')

    def test_Kaus_Australis(self):
        self._test_body('Kaus Australis', '83 40.2', '-34 22.4')

    def test_Kochab(self):
        self._test_body('Kochab', '137 20.9', '74 4.8')

    def test_Markab(self):
        self._test_body('Markab', '13 35.4', '15 18.2')

    def test_Menkar(self):
        self._test_body('Menkar', '314 11.6', '4 9.4')

    def test_Menkent(self):
        self._test_body('Menkent', '148 4.1', '-36 27.2')

    def test_Miaplacidus(self):
        self._test_body('Miaplacidus', '221 38.1', '-69 47.3')

    def test_Mirfak(self):
        self._test_body('Mirfak', '308 35.5', '49 55.5')

    def test_Nunki(self):
        self._test_body('Nunki', '75 54.9', '-26 16.3')

    def test_Peacock(self):
        self._test_body('Peacock', '53 15.0', '-56 40.6')

    def test_Pollux(self):
        self._test_body('Pollux', '243 23.6', '27 58.7')

    def test_Procyon(self):
        self._test_body('Procyon', '244 56.2', '5 10.5')

    def test_Rasalhague(self):
        self._test_body('Rasalhague', '96 3.9', '12 33.0')

    def test_Regulus(self):
        self._test_body('Regulus', '207 40.0', '11 52.6')

    def test_Rigel(self):
        self._test_body('Rigel', '281 8.8', '-8 11.1')

    def test_Rigil_Kentaurus_2002(self):
        # The printed Nautical Almanac 2002, ISBN 0 11 887316 4
        # For 2002-01-01 00:00:00 has the following values.
        self._test_body('Rigil Kentaurus', '140 4.6', '-60 50.2', '2002-01-01 00:00:00')

    def test_Rigil_Kentaurus_2017(self):
        # Her Majesty's Nautical Almanac Office Publication NP314-18
        # "The Nautical Almanac" (2017). ISBN 978-0-7077-41819
        # For 2018-01-01 00:00:00 has the following values.
        self._test_body('Rigil Kentaurus', '139 47.8', '-60 54.1', '2018-01-01 00:00:00')

    def test_Sabik(self):
        self._test_body('Sabik', '102 9.3', '-15 44.6')

    def test_Schedar(self):
        self._test_body('Schedar', '349 36.9', '56 38.3')

    def test_Shaula(self):
        self._test_body('Shaula', '96 18.1', '-37 6.7')

    def test_Sirius(self):
        self._test_body('Sirius', '258 30.7', '-16 44.7')

    def test_Spica(self):
        self._test_body('Spica', '158 28.1', '-11 15.1')

    def test_Suhail(self):
        self._test_body('Suhail', '222 49.8', '-43 30.3')

    def test_Vega(self):
        self._test_body('Vega', '80 37.3', '38 48.2')

    def test_Zubenelgenubi(self):
        self._test_body('Zubenelgenubi', '137 2.2', '-16 6.7')

    def test_number_tables_len(self):
        self.assertTrue(len(ephem.stars.STAR_NAME_NUMBER) == 57)
        self.assertTrue(len(ephem.stars.STAR_NUMBER_NAME) == 57)

    def test_number_tables_range(self):
        self.assertEqual(sorted(ephem.stars.STAR_NAME_NUMBER.values()),
                         list(range(1, 58))
                         )
        self.assertEqual(sorted(ephem.stars.STAR_NUMBER_NAME.keys()),
                         list(range(1, 58))
                         )

    def test_number_tables_cross_reference(self):
        for name, number in ephem.stars.STAR_NAME_NUMBER.items():
            self.assertEqual(name, ephem.stars.STAR_NUMBER_NAME[number])
        for number, name in ephem.stars.STAR_NUMBER_NAME.items():
            self.assertEqual(number, ephem.stars.STAR_NAME_NUMBER[name])
