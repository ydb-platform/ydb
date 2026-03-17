# Copyright (c) 2006-2012 Filip Wasilewski <http://en.ig.ma/>
# Copyright (c) 2012-2016 The PyWavelets Developers
#                         <https://github.com/PyWavelets/pywt>
# See COPYING for license details.

# Mapping of wavelet names to the C backend codes

cdef extern from "c/wavelets.h":
    ctypedef enum WAVELET_NAME:
        HAAR
        RBIO
        DB
        SYM
        COIF
        BIOR
        DMEY
        GAUS
        MEXH
        MORL
        CGAU
        SHAN
        FBSP
        CMOR

cdef __wname_to_code
__wname_to_code = {
    "haar": (HAAR, 0),

    "db1": (DB, 1),
    "db2": (DB, 2),
    "db3": (DB, 3),
    "db4": (DB, 4),
    "db5": (DB, 5),
    "db6": (DB, 6),
    "db7": (DB, 7),
    "db8": (DB, 8),
    "db9": (DB, 9),

    "db10": (DB, 10),
    "db11": (DB, 11),
    "db12": (DB, 12),
    "db13": (DB, 13),
    "db14": (DB, 14),
    "db15": (DB, 15),
    "db16": (DB, 16),
    "db17": (DB, 17),
    "db18": (DB, 18),
    "db19": (DB, 19),

    "db20": (DB, 20),
    "db21": (DB, 21),
    "db22": (DB, 22),
    "db23": (DB, 23),
    "db24": (DB, 24),
    "db25": (DB, 25),
    "db26": (DB, 26),
    "db27": (DB, 27),
    "db28": (DB, 28),
    "db29": (DB, 29),

    "db30": (DB, 30),
    "db31": (DB, 31),
    "db32": (DB, 32),
    "db33": (DB, 33),
    "db34": (DB, 34),
    "db35": (DB, 35),
    "db36": (DB, 36),
    "db37": (DB, 37),
    "db38": (DB, 38),

    "sym2": (SYM, 2),
    "sym3": (SYM, 3),
    "sym4": (SYM, 4),
    "sym5": (SYM, 5),
    "sym6": (SYM, 6),
    "sym7": (SYM, 7),
    "sym8": (SYM, 8),
    "sym9": (SYM, 9),

    "sym10": (SYM, 10),
    "sym11": (SYM, 11),
    "sym12": (SYM, 12),
    "sym13": (SYM, 13),
    "sym14": (SYM, 14),
    "sym15": (SYM, 15),
    "sym16": (SYM, 16),
    "sym17": (SYM, 17),
    "sym18": (SYM, 18),
    "sym19": (SYM, 19),
    "sym20": (SYM, 20),

    "coif1": (COIF, 1),
    "coif2": (COIF, 2),
    "coif3": (COIF, 3),
    "coif4": (COIF, 4),
    "coif5": (COIF, 5),
    "coif6": (COIF, 6),
    "coif7": (COIF, 7),
    "coif8": (COIF, 8),
    "coif9": (COIF, 9),

    "coif10": (COIF, 10),
    "coif11": (COIF, 11),
    "coif12": (COIF, 12),
    "coif13": (COIF, 13),
    "coif14": (COIF, 14),
    "coif15": (COIF, 15),
    "coif16": (COIF, 16),
    "coif17": (COIF, 17),


    "bior1.1": (BIOR, 11),
    "bior1.3": (BIOR, 13),
    "bior1.5": (BIOR, 15),
    "bior2.2": (BIOR, 22),
    "bior2.4": (BIOR, 24),
    "bior2.6": (BIOR, 26),
    "bior2.8": (BIOR, 28),
    "bior3.1": (BIOR, 31),
    "bior3.3": (BIOR, 33),
    "bior3.5": (BIOR, 35),
    "bior3.7": (BIOR, 37),
    "bior3.9": (BIOR, 39),
    "bior4.4": (BIOR, 44),
    "bior5.5": (BIOR, 55),
    "bior6.8": (BIOR, 68),

    "rbio1.1": (RBIO, 11),
    "rbio1.3": (RBIO, 13),
    "rbio1.5": (RBIO, 15),
    "rbio2.2": (RBIO, 22),
    "rbio2.4": (RBIO, 24),
    "rbio2.6": (RBIO, 26),
    "rbio2.8": (RBIO, 28),
    "rbio3.1": (RBIO, 31),
    "rbio3.3": (RBIO, 33),
    "rbio3.5": (RBIO, 35),
    "rbio3.7": (RBIO, 37),
    "rbio3.9": (RBIO, 39),
    "rbio4.4": (RBIO, 44),
    "rbio5.5": (RBIO, 55),
    "rbio6.8": (RBIO, 68),

    "dmey": (DMEY, 0),

    "gaus1": (GAUS, 1),
    "gaus2": (GAUS, 2),
    "gaus3": (GAUS, 3),
    "gaus4": (GAUS, 4),
    "gaus5": (GAUS, 5),
    "gaus6": (GAUS, 6),
    "gaus7": (GAUS, 7),
    "gaus8": (GAUS, 8),

    "mexh": (MEXH, 0),

    "morl": (MORL, 0),

    "cgau1": (CGAU, 1),
    "cgau2": (CGAU, 2),
    "cgau3": (CGAU, 3),
    "cgau4": (CGAU, 4),
    "cgau5": (CGAU, 5),
    "cgau6": (CGAU, 6),
    "cgau7": (CGAU, 7),
    "cgau8": (CGAU, 8),

    "shan": (SHAN, 0),

    "fbsp": (FBSP, 0),

    "cmor": (CMOR, 0),
}

## Lists of family names

cdef __wfamily_list_short, __wfamily_list_long
__wfamily_list_short = [
    "haar", "db", "sym", "coif", "bior", "rbio", "dmey", "gaus", "mexh",
    "morl", "cgau", "shan", "fbsp", "cmor"]
__wfamily_list_long = [
    "Haar", "Daubechies", "Symlets", "Coiflets", "Biorthogonal",
    "Reverse biorthogonal", "Discrete Meyer (FIR Approximation)", "Gaussian",
    "Mexican hat wavelet", "Morlet wavelet", "Complex Gaussian wavelets",
    "Shannon wavelets", "Frequency B-Spline wavelets",
    "Complex Morlet wavelets"]
