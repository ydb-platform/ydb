#!/usr/bin/python
"""
  Licensing:
    This code is distributed under the MIT license.

  Author:
    Original MATLAB version by John Burkardt.
    PYTHON version by Corrado Chisari

    Original code is available from http://people.sc.fsu.edu/~jburkardt/py_src/sobol/sobol.html
"""

import numpy as np
import unittest
from sobol_seq import i4_sobol, i4_sobol_generate, i4_uniform, i4_bit_hi1, i4_bit_lo0, prime_ge


class TestSobolSeq(unittest.TestCase):
    def test_sobol_01(self):
        """
        sobol_test01 tests bitxor.
        """
        print('\nSOBOL_TEST01'
            '------------'
            '  BITXOR is a MATLAB intrinsic function which returns'
            '  the bitwise exclusive OR of two integers.'
            '\n     I     J     BITXOR(I,J)')

        seed = 123456789

        target = np.array([
            [22, 96, 118],
            [83, 56, 107],
            [41,  6,  47],
            [26, 11,  17],
            [ 4, 64,  68],
            [ 6, 45,  43],
            [40, 76, 100],
            [80,  0,  80],
            [90, 35, 121],
            [ 9,  1,   8]])

        results = np.full((10, 3), np.nan)
        for test in range(10):

            [i, seed] = i4_uniform(0, 100, seed)
            [j, seed] = i4_uniform(0, 100, seed)
            k = np.bitwise_xor(i, j)
            results[test, :] = i, j, k

            print('%6d  %6d  %6d' % (i, j, k))

        assert np.all(target == results), "Array values not as expected"

        return

    def test_sobol_02(self):
        """
        sobol_test02 tests i4_bit_hi1.
        """
        print('\nSOBOL_TEST02'
            '  I4_BIT_HI1 returns the location of the high 1 bit.'
            '\n     I     I4_BIT_HI1(I)\n')

        seed = 123456789

        target = np.array([
            [22, 5],
            [96, 7],
            [83, 7],
            [56, 6],
            [41, 6],
            [ 6, 3],
            [26, 5],
            [11, 4],
            [ 4, 3],
            [64, 7]])

        results = np.full((10, 2), np.nan)
        for test in range(10):

            [i, seed] = i4_uniform(0, 100, seed)

            j = i4_bit_hi1(i)
            results[test, :] = i, j

            print('%6d %6d' % (i, j))

        assert np.all(target == results), "Array values not as expected"

        return

    def test_sobol_03(self):
        """
        sobol_test03 tests i4_bit_lo0.
        """
        print('\nSOBOL_TEST03'
            '  I4_BIT_LO0 returns the location of the low 0 bit.'
            '\n     I     I4_BIT_LO0(I)')

        seed = 123456789

        target = ([
            [22, 1],
            [96, 1],
            [83, 3],
            [56, 1],
            [41, 2],
            [ 6, 1],
            [26, 1],
            [11, 3],
            [ 4, 1],
            [64, 1]])

        results = np.full((10, 2), np.nan)
        for test in range(10):

            [i, seed] = i4_uniform(0, 100, seed)
            j = i4_bit_lo0(i)

            results[test, :] = i, j

            print('%6d %6d' % (i, j))

        assert np.all(target == results), "Array values not as expected"

        return

    def test_sobol_04(self):
        """
        sobol_test04 tests i4_sobol.
        """
        print('\nSOBOL_TEST04'
            '  I4_SOBOL returns the next element'
            '  of a Sobol sequence.'
            '\n  In this test, we call I4_SOBOL repeatedly.')

        dim_max = 4

        target = {
            2: np.array([
                [  0,   1, 0.000000,  0.000000],
                [  1,   2, 0.500000,  0.500000],
                [  2,   3, 0.750000,  0.250000],
                [  3,   4, 0.250000,  0.750000],
                [  4,   5, 0.375000,  0.375000],
                # ......................
                [106, 107, 0.9765625, 0.1953125],
                [107, 108, 0.4765625, 0.6953125],
                [108, 109, 0.3515625, 0.0703125],
                [109, 110, 0.8515625, 0.5703125],
                [110, 111, 0.6015625, 0.3203125]]),
            3: np.array([
                [  0,   1, 0.000000,  0.000000,  0.000000],
                [  1,   2, 0.500000,  0.500000,  0.500000],
                [  2,   3, 0.750000,  0.250000,  0.750000],
                [  3,   4, 0.250000,  0.750000,  0.250000],
                [  4,   5, 0.375000,  0.375000,  0.625000],
                # ......................
                [106, 107, 0.9765625, 0.1953125, 0.4921875],
                [107, 108, 0.4765625, 0.6953125, 0.9921875],
                [108, 109, 0.3515625, 0.0703125, 0.1171875],
                [109, 110, 0.8515625, 0.5703125, 0.6171875],
                [110, 111, 0.6015625, 0.3203125, 0.8671875]]),
            4: np.array([
                [  0,   1, 0.000000,  0.000000,  0.000000,  0.000000],
                [  1,   2, 0.500000,  0.500000,  0.500000,  0.500000],
                [  2,   3, 0.750000,  0.250000,  0.750000,  0.250000],
                [  3,   4, 0.250000,  0.750000,  0.250000,  0.750000],
                [  4,   5, 0.375000,  0.375000,  0.625000,  0.125000],
                # ......................
                [106, 107, 0.9765625, 0.1953125, 0.4921875, 0.6640625],
                [107, 108, 0.4765625, 0.6953125, 0.9921875, 0.1640625],
                [108, 109, 0.3515625, 0.0703125, 0.1171875, 0.7890625],
                [109, 110, 0.8515625, 0.5703125, 0.6171875, 0.2890625],
                [110, 111, 0.6015625, 0.3203125, 0.8671875, 0.5390625]])}


        for dim_num in range(2, dim_max + 1):

            seed = 0
            qs = prime_ge(dim_num)

            print('\n  Using dimension DIM_NUM =   %d' % dim_num)
            print('\n  Seed   Seed    I4_SOBOL'
                '  In     Out\n')

            results = np.full((111, 2 + dim_num), np.nan)
            for i in range(111):
                [r, seed_out] = i4_sobol(dim_num, seed)
                if (i < 5 or 105 < i):
                    out = '%6d %6d  ' % (seed, seed_out)
                    for j in range(dim_num):
                        out += '%10f  ' % r[j]
                    print(out)
                elif (i == 6):
                    print('  ......................')
                results[i, :] = [seed, seed_out] + list(r)
                seed = seed_out

            assert np.all(target[dim_num][0:5, :] == results[0:5, :]), "Start of array doesn't match"
            assert np.all(target[dim_num][5:10, :] == results[106:111, :]), "End of array doesn't match"

        return

    def test_sobol_05(self):
        """
        sobol_test05 tests i4_sobol.
        """
        print(''
            'SOBOL_TEST05'
            '  I4_SOBOL computes the next element of a Sobol sequence.'
            ''
            '  In this test, we demonstrate how the SEED can be'
            '  manipulated to skip ahead in the sequence, or'
            '  to come back to any part of the sequence.'
            '')

        target = np.array([
        [  0,   1, 0.000000,  0.000000,  0.000000],
        [  1,   2, 0.500000,  0.500000,  0.500000],
        [  2,   3, 0.750000,  0.250000,  0.750000],
        [  3,   4, 0.250000,  0.750000,  0.250000],
        [  4,   5, 0.375000,  0.375000,  0.625000],
        [100, 101, 0.4140625, 0.2578125, 0.3046875],
        [101, 102, 0.9140625, 0.7578125, 0.8046875],
        [102, 103, 0.6640625, 0.0078125, 0.5546875],
        [103, 104, 0.1640625, 0.5078125, 0.0546875],
        [104, 105, 0.2265625, 0.4453125, 0.7421875],
        [  3,   4, 0.250000,  0.750000,  0.250000],
        [  4,   5, 0.375000,  0.375000,  0.625000],
        [  5,   6, 0.875000,  0.875000,  0.125000],
        [  6,   7, 0.625000,  0.125000,  0.375000],
        [  7,   8, 0.125000,  0.625000,  0.875000],
        [ 98,  99, 0.7890625, 0.3828125, 0.1796875],
        [ 99, 100, 0.2890625, 0.8828125, 0.6796875],
        [100, 101, 0.4140625, 0.2578125, 0.3046875],
        [101, 102, 0.9140625, 0.7578125, 0.8046875],
        [102, 103, 0.6640625, 0.0078125, 0.5546875]])

        results = np.full_like(target, np.nan)

        dim_num = 3

        print(''
            '  Using dimension DIM_NUM =   %d\n' % dim_num)

        seed = 0

        print(''
            '  Seed  Seed   I4_SOBOL'
            '  In    Out'
            '')

        for i in range(5):
            [r, seed_out] = i4_sobol(dim_num, seed)
            out = '%6d %6d  ' % (seed, seed_out)
            for j in range(1, dim_num + 1):
                out += '%10f  ' % r[j - 1]
            print(out)
            results[i, :] = [seed, seed_out] + list(r)
            seed = seed_out

        print(''
            '  Jump ahead by increasing SEED:'
            '')

        seed = 100

        print(''
            '  Seed  Seed   I4_SOBOL'
            '  In    Out'
            '')

        for i in range(5):
            [r, seed_out] = i4_sobol(dim_num, seed)
            out = '%6d %6d  ' % (seed, seed_out)
            for j in range(1, dim_num + 1):
                out += '%10f  ' % r[j - 1]
            print(out)
            results[5 + i, :] = [seed, seed_out] + list(r)
            seed = seed_out
        print(''
            '  Jump back by decreasing SEED:'
            '')

        seed = 3

        print(''
            '  Seed  Seed   I4_SOBOL'
            '  In    Out'
            '')

        for i in range(5):
            [r, seed_out] = i4_sobol(dim_num, seed)
            out = '%6d %6d  ' % (seed, seed_out)
            for j in range(1, dim_num + 1):
                out += '%10f  ' % r[j - 1]
            print(out)
            results[10 + i, :] = [seed, seed_out] + list(r)
            seed = seed_out

        print(''
            '  Jump back by decreasing SEED:'
            '')

        seed = 98

        print(''
            '  Seed  Seed   I4_SOBOL'
            '  In    Out'
            '')

        for i in range(5):
            [r, seed_out] = i4_sobol(dim_num, seed)
            out = '%6d %6d  ' % (seed, seed_out)
            for j in range(1, dim_num + 1):
                out += '%10f  ' % r[j - 1]
            print(out)
            results[15 + i, :] = [seed, seed_out] + list(r)
            seed = seed_out

        assert np.all(target == results)

        return

    def test_sobol_generate(self):
        """
        sobol_test02 tests i4_sobol_generate.
        """
        print('\nSOBOL_TEST_GENERATE'
            '  I4_BIT_ returns the location of the high 1 bit.'
            '\n     I     I4_BIT_HI1(I)\n')

        target = np.array([
        [    0.5,     0.5,     0.5,     0.5,     0.5],
        [   0.75,    0.25,    0.75,    0.25,    0.75],
        [   0.25,    0.75,    0.25,    0.75,    0.25],
        [  0.375,   0.375,   0.625,   0.125,   0.875],
        [  0.875,   0.875,   0.125,   0.625,   0.375],
        [  0.625,   0.125,   0.375,   0.375,   0.125],
        [  0.125,   0.625,   0.875,   0.875,   0.625],
        [ 0.1875,  0.3125,  0.3125,  0.6875,  0.5625],
        [ 0.6875,  0.8125,  0.8125,  0.1875,  0.0625],
        [ 0.9375,  0.0625,  0.5625,  0.9375,  0.3125]])

        results = i4_sobol_generate(5, 10)

        assert np.all(target == results), "Array values not as expected"

        return

    def test_skip(self):
        no_skip = i4_sobol_generate(dim_num=4, n=5)
        assert no_skip.shape == (5, 4)

        skip_0 = i4_sobol_generate(dim_num=4, n=5, skip=0)
        np.testing.assert_array_equal(skip_0, no_skip)

        skip_2 = i4_sobol_generate(dim_num=4, n=5, skip=2)
        np.testing.assert_array_equal(skip_2[:-2], no_skip[2:,:])
        return


if __name__ == "__main__":
    unittest.main()
