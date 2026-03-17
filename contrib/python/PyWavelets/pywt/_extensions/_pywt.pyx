# Copyright (c) 2006-2012 Filip Wasilewski <http://en.ig.ma/>
# Copyright (c) 2012-2018 The PyWavelets Developers
#                         <https://github.com/PyWavelets/pywt>
# See COPYING for license details.

__doc__ = """Cython wrapper for low-level C wavelet transform implementation."""
__all__ = ['Modes', 'DiscreteContinuousWavelet', 'Wavelet',
           'ContinuousWavelet', 'wavelist', 'families']


import warnings
import re

from . cimport c_wt
from . cimport common
from ._dwt cimport upcoef
from ._cwt cimport cwt_psi_single

from libc.math cimport pow, sqrt

import numpy as np


# Caution: order of _old_modes entries must match _Modes.modes below
_old_modes = ['zpd',
              'cpd',
              'sym',
              'ppd',
              'sp1',
              'per',
              ]

_attr_deprecation_msg = ('{old} has been renamed to {new} and will '
                         'be unavailable in a future version '
                         'of pywt.')

# Extract float/int parameters from a wavelet name. Examples:
#    re.findall(cwt_pattern, 'fbsp1-1.5-1') ->  ['1', 1.5', '1']
cwt_pattern = re.compile(r'\D+(\d+\.*\d*)+')


# raises exception if the wavelet name is undefined
cdef int is_discrete_wav(WAVELET_NAME name):
    cdef int is_discrete
    discrete = wavelet.is_discrete_wavelet(name)
    if discrete == -1:
        raise ValueError("unrecognized wavelet family name")
    return discrete


class _Modes(object):
    """
    Because the most common and practical way of representing digital signals
    in computer science is with finite arrays of values, some extrapolation of
    the input data has to be performed in order to extend the signal before
    computing the :ref:`Discrete Wavelet Transform <ref-dwt>` using the
    cascading filter banks algorithm.

    Depending on the extrapolation method, significant artifacts at the
    signal's borders can be introduced during that process, which in turn may
    lead to inaccurate computations of the :ref:`DWT <ref-dwt>` at the signal's
    ends.

    PyWavelets provides several methods of signal extrapolation that can be
    used to minimize this negative effect:

    zero - zero-padding                   0  0 | x1 x2 ... xn | 0  0
    constant - constant-padding          x1 x1 | x1 x2 ... xn | xn xn
    symmetric - symmetric-padding        x2 x1 | x1 x2 ... xn | xn xn-1
    reflect - reflect-padding            x3 x2 | x1 x2 ... xn | xn-1 xn-2
    periodic - periodic-padding        xn-1 xn | x1 x2 ... xn | x1 x2
    smooth - smooth-padding             (1st derivative interpolation)
    antisymmetric -                    -x2 -x1 | x1 x2 ... xn | -xn -xn-1
    antireflect -                      -x3 -x2 | x1 x2 ... xn | -xn-1 -xn-2

    DWT performed for these extension modes is slightly redundant, but ensure a
    perfect reconstruction for IDWT. To receive the smallest possible number of
    coefficients, computations can be performed with the periodization mode:

    periodization - like periodic-padding but gives the smallest possible
                    number of decomposition coefficients. IDWT must be
                    performed with the same mode.

    Examples
    --------
    >>> import pywt
    >>> pywt.Modes.modes
        ['zero', 'constant', 'symmetric', 'periodic', 'smooth', 'periodization', 'reflect', 'antisymmetric', 'antireflect']
    >>> # The different ways of passing wavelet and mode parameters
    >>> (a, d) = pywt.dwt([1,2,3,4,5,6], 'db2', 'smooth')
    >>> (a, d) = pywt.dwt([1,2,3,4,5,6], pywt.Wavelet('db2'), pywt.Modes.smooth)

    Notes
    -----
    Extending data in context of PyWavelets does not mean reallocation of the
    data in computer's physical memory and copying values, but rather computing
    the extra values only when they are needed. This feature saves extra
    memory and CPU resources and helps to avoid page swapping when handling
    relatively big data arrays on computers with low physical memory.

    """
    zero = common.MODE_ZEROPAD
    constant = common.MODE_CONSTANT_EDGE
    symmetric = common.MODE_SYMMETRIC
    reflect = common.MODE_REFLECT
    periodic = common.MODE_PERIODIC
    smooth = common.MODE_SMOOTH
    periodization = common.MODE_PERIODIZATION
    antisymmetric = common.MODE_ANTISYMMETRIC
    antireflect = common.MODE_ANTIREFLECT

    # Caution: order in modes list below must match _old_modes above
    modes = ["zero", "constant", "symmetric", "periodic", "smooth",
             "periodization", "reflect", "antisymmetric", "antireflect"]

    def from_object(self, mode):
        if isinstance(mode, int):
            if mode <= common.MODE_INVALID or mode >= common.MODE_MAX:
                raise ValueError("Invalid mode.")
            m = mode
        else:
            try:
                m = getattr(Modes, mode)
            except AttributeError:
                raise ValueError("Unknown mode name '%s'." % mode)

        return m

    def __getattr__(self, mode):
        # catch deprecated mode names
        if mode in _old_modes:
            new_mode = Modes.modes[_old_modes.index(mode)]
            warnings.warn(_attr_deprecation_msg.format(old=mode, new=new_mode),
                          DeprecationWarning)
            mode = new_mode
        return Modes.__getattribute__(mode)


Modes = _Modes()


class _DeprecatedMODES(_Modes):
    msg = ("MODES has been renamed to Modes and will be "
           "removed in a future version of pywt.")

    def __getattribute__(self, attr):
        """Override so that deprecation warning is shown
        every time MODES is used.

        N.B. have to use __getattribute__ as well as __getattr__
        to ensure warning on e.g. `MODES.symmetric`.
        """
        if not attr.startswith('_'):
            warnings.warn(_DeprecatedMODES.msg, DeprecationWarning)
        return _Modes.__getattribute__(self, attr)

    def __getattr__(self, attr):
        """Override so that deprecation warning is shown
        every time MODES is used.
        """
        warnings.warn(_DeprecatedMODES.msg, DeprecationWarning)
        return _Modes.__getattr__(self, attr)


MODES = _DeprecatedMODES()

###############################################################################
# Wavelet

include "wavelets_list.pxi"  # __wname_to_code

cdef object wname_to_code(name):
    cdef object code_number
    try:
        if len(name) > 4 and name[:4] in ['cmor', 'shan', 'fbsp']:
            name = name[:4]
        code_number = __wname_to_code[name]
        return code_number
    except KeyError:
        raise ValueError("Unknown wavelet name '%s', check wavelist() for the "
                         "list of available builtin wavelets." % name)


def wavelist(family=None, kind='all'):
    """
    wavelist(family=None, kind='all')

    Returns list of available wavelet names for the given family name.

    Parameters
    ----------
    family : str, optional
        Short family name. If the family name is None (default) then names
        of all the built-in wavelets are returned. Otherwise the function
        returns names of wavelets that belong to the given family.
        Valid names are::

            'haar', 'db', 'sym', 'coif', 'bior', 'rbio', 'dmey', 'gaus',
            'mexh', 'morl', 'cgau', 'shan', 'fbsp', 'cmor'

    kind : {'all', 'continuous', 'discrete'}, optional
        Whether to return only wavelet names of discrete or continuous
        wavelets, or all wavelets.  Default is ``'all'``.
        Ignored if ``family`` is specified.

    Returns
    -------
    wavelist : list of str
        List of available wavelet names.

    Examples
    --------
    >>> import pywt
    >>> pywt.wavelist('coif')
    ['coif1', 'coif2', 'coif3', 'coif4', 'coif5', 'coif6', 'coif7', ...
    >>> pywt.wavelist(kind='continuous')
    ['cgau1', 'cgau2', 'cgau3', 'cgau4', 'cgau5', 'cgau6', 'cgau7', ...

    """
    cdef object wavelets, sorting_list

    if kind not in ('all', 'continuous', 'discrete'):
        raise ValueError("Unrecognized value for `kind`: %s" % kind)

    def _check_kind(name, kind):
        if kind == 'all':
            return True

        family_code, family_number = wname_to_code(name)
        is_discrete = is_discrete_wav(family_code)
        if kind == 'discrete':
            return is_discrete
        else:
            return not is_discrete

    sorting_list = []  # for natural sorting order
    wavelets = []
    cdef object name
    if family is None:
        for name in __wname_to_code:
            if _check_kind(name, kind):
                sorting_list.append((name[:2], len(name), name))
    elif family in __wfamily_list_short:
        for name in __wname_to_code:
            if name.startswith(family):
                sorting_list.append((name[:2], len(name), name))
    else:
        raise ValueError("Invalid short family name '%s'." % family)

    sorting_list.sort()
    for x, x, name in sorting_list:
        wavelets.append(name)
    return wavelets


def families(int short=True):
    """
    families(short=True)

    Returns a list of available built-in wavelet families.

    Currently the built-in families are:

    * Haar (``haar``)
    * Daubechies (``db``)
    * Symlets (``sym``)
    * Coiflets (``coif``)
    * Biorthogonal (``bior``)
    * Reverse biorthogonal (``rbio``)
    * `"Discrete"` FIR approximation of Meyer wavelet (``dmey``)
    * Gaussian wavelets (``gaus``)
    * Mexican hat wavelet (``mexh``)
    * Morlet wavelet (``morl``)
    * Complex Gaussian wavelets (``cgau``)
    * Shannon wavelets (``shan``)
    * Frequency B-Spline wavelets (``fbsp``)
    * Complex Morlet wavelets (``cmor``)

    Parameters
    ----------
    short : bool, optional
        Use short names (default: True).

    Returns
    -------
    families : list
        List of available wavelet families.

    Examples
    --------
    >>> import pywt
    >>> pywt.families()
    ['haar', 'db', 'sym', 'coif', 'bior', 'rbio', 'dmey', 'gaus', 'mexh', 'morl', 'cgau', 'shan', 'fbsp', 'cmor']
    >>> pywt.families(short=False)
    ['Haar', 'Daubechies', 'Symlets', 'Coiflets', 'Biorthogonal', 'Reverse biorthogonal', 'Discrete Meyer (FIR Approximation)', 'Gaussian', 'Mexican hat wavelet', 'Morlet wavelet', 'Complex Gaussian wavelets', 'Shannon wavelets', 'Frequency B-Spline wavelets', 'Complex Morlet wavelets']

    """
    if short:
        return __wfamily_list_short[:]
    return __wfamily_list_long[:]


def DiscreteContinuousWavelet(name=u"", object filter_bank=None):
    """
    DiscreteContinuousWavelet(name, filter_bank=None) returns a
    Wavelet or a ContinuousWavelet object depending of the given name.

    In order to use a built-in wavelet the parameter name must be
    a valid name from the wavelist() list.
    To create a custom wavelet object, filter_bank parameter must
    be specified. It can be either a list of four filters or an object
    that a `filter_bank` attribute which returns a list of four
    filters - just like the Wavelet instance itself.

    For a ContinuousWavelet, filter_bank cannot be used and must remain unset.

    """
    if not name and filter_bank is None:
        raise TypeError("Wavelet name or filter bank must be specified.")
    if filter_bank is None:
        name = name.lower()
        family_code, family_number = wname_to_code(name)
        if is_discrete_wav(family_code):
            return Wavelet(name, filter_bank)
        else:
            return ContinuousWavelet(name)
    else:
        return Wavelet(name, filter_bank)


cdef public class Wavelet [type WaveletType, object WaveletObject]:
    """
    Wavelet(name, filter_bank=None) object describe properties of
    a wavelet identified by name.

    In order to use a built-in wavelet the parameter name must be
    a valid name from the wavelist() list.
    To create a custom wavelet object, filter_bank parameter must
    be specified. It can be either a list of four filters or an object
    that a `filter_bank` attribute which returns a list of four
    filters - just like the Wavelet instance itself.

    """
    #cdef readonly properties
    def __cinit__(self, name=u"", object filter_bank=None):
        cdef object family_code, family_number
        cdef object filters
        cdef pywt_index_t filter_length
        cdef object dec_lo, dec_hi, rec_lo, rec_hi

        if not name and filter_bank is None:
            raise TypeError("Wavelet name or filter bank must be specified.")

        if filter_bank is None:
            # builtin wavelet
            self.name = name.lower()
            family_code, family_number = wname_to_code(self.name)
            if is_discrete_wav(family_code):
                self.w = <wavelet.DiscreteWavelet*> wavelet.discrete_wavelet(family_code, family_number)
            if self.w is NULL:
                if self.name in wavelist(kind='continuous'):
                    raise ValueError("The `Wavelet` class is for discrete "
                          "wavelets, %s is a continuous wavelet.  Use "
                          "pywt.ContinuousWavelet instead" % self.name)
                else:
                    raise ValueError("Invalid wavelet name '%s'." % self.name)
            self.number = family_number
        else:
            if hasattr(filter_bank, "filter_bank"):
                filters = filter_bank.filter_bank
                if len(filters) != 4:
                    raise ValueError("Expected filter bank with 4 filters, "
                    "got filter bank with %d filters." % len(filters))
            elif hasattr(filter_bank, "get_filters_coeffs"):
                msg = ("Creating custom Wavelets using objects that define "
                       "`get_filters_coeffs` method is deprecated. "
                       "The `filter_bank` parameter should define a "
                       "`filter_bank` attribute instead of "
                       "`get_filters_coeffs` method.")
                warnings.warn(msg, DeprecationWarning)
                filters = filter_bank.get_filters_coeffs()
                if len(filters) != 4:
                    msg = ("Expected filter bank with 4 filters, got filter "
                           "bank with %d filters." % len(filters))
                    raise ValueError(msg)
            else:
                filters = filter_bank
                if len(filters) != 4:
                    msg = ("Expected list of 4 filters coefficients, "
                           "got %d filters." % len(filters))
                    raise ValueError(msg)
            try:
                dec_lo = np.asarray(filters[0], dtype=np.float64)
                dec_hi = np.asarray(filters[1], dtype=np.float64)
                rec_lo = np.asarray(filters[2], dtype=np.float64)
                rec_hi = np.asarray(filters[3], dtype=np.float64)
            except TypeError:
                raise ValueError("Filter bank with numeric values required.")

            if not (1 == dec_lo.ndim == dec_hi.ndim ==
                         rec_lo.ndim == rec_hi.ndim):
                raise ValueError("All filters in filter bank must be 1D.")

            filter_length = len(dec_lo)
            if not (0 < filter_length == len(dec_hi) == len(rec_lo) ==
                                         len(rec_hi)) > 0:
                raise ValueError("All filters in filter bank must have "
                                 "length greater than 0.")

            self.w = <wavelet.DiscreteWavelet*> wavelet.blank_discrete_wavelet(filter_length)
            if self.w is NULL:
                raise MemoryError("Could not allocate memory for given "
                                  "filter bank.")

            # copy values to struct
            copy_object_to_float32_array(dec_lo, self.w.dec_lo_float)
            copy_object_to_float32_array(dec_hi, self.w.dec_hi_float)
            copy_object_to_float32_array(rec_lo, self.w.rec_lo_float)
            copy_object_to_float32_array(rec_hi, self.w.rec_hi_float)

            copy_object_to_float64_array(dec_lo, self.w.dec_lo_double)
            copy_object_to_float64_array(dec_hi, self.w.dec_hi_double)
            copy_object_to_float64_array(rec_lo, self.w.rec_lo_double)
            copy_object_to_float64_array(rec_hi, self.w.rec_hi_double)

            self.name = name

    def __dealloc__(self):
        if self.w is not NULL:
            wavelet.free_discrete_wavelet(self.w)
            self.w = NULL

    def __reduce__(self):
        return (Wavelet, (self.name, self.filter_bank))

    def __len__(self):
        return self.w.dec_len

    property dec_lo:
        "Lowpass decomposition filter"
        def __get__(self):
            return float64_array_to_list(self.w.dec_lo_double, self.w.dec_len)

    property dec_hi:
        "Highpass decomposition filter"
        def __get__(self):
            return float64_array_to_list(self.w.dec_hi_double, self.w.dec_len)

    property rec_lo:
        "Lowpass reconstruction filter"
        def __get__(self):
            return float64_array_to_list(self.w.rec_lo_double, self.w.rec_len)

    property rec_hi:
        "Highpass reconstruction filter"
        def __get__(self):
            return float64_array_to_list(self.w.rec_hi_double, self.w.rec_len)

    property rec_len:
        "Reconstruction filters length"
        def __get__(self):
            return self.w.rec_len

    property dec_len:
        "Decomposition filters length"
        def __get__(self):
            return self.w.dec_len

    property family_number:
        "Wavelet family number"
        def __get__(self):
            return self.number

    property family_name:
        "Wavelet family name"
        def __get__(self):
            return self.w.base.family_name.decode('latin-1')

    property short_family_name:
        "Short wavelet family name"
        def __get__(self):
            return self.w.base.short_name.decode('latin-1')

    property orthogonal:
        "Is orthogonal"
        def __get__(self):
            return bool(self.w.base.orthogonal)
        def __set__(self, int value):
            self.w.base.orthogonal = (value != 0)

    property biorthogonal:
        "Is biorthogonal"
        def __get__(self):
            return bool(self.w.base.biorthogonal)
        def __set__(self, int value):
            self.w.base.biorthogonal = (value != 0)

    property symmetry:
        "Wavelet symmetry"
        def __get__(self):
            if self.w.base.symmetry == wavelet.ASYMMETRIC:
                return "asymmetric"
            elif self.w.base.symmetry == wavelet.NEAR_SYMMETRIC:
                return "near symmetric"
            elif self.w.base.symmetry == wavelet.SYMMETRIC:
                return "symmetric"
            elif self.w.base.symmetry == wavelet.ANTI_SYMMETRIC:
                return "anti-symmetric"
            else:
                return "unknown"

    property vanishing_moments_psi:
        "Number of vanishing moments for wavelet function"
        def __get__(self):
            if self.w.vanishing_moments_psi >= 0:
                return self.w.vanishing_moments_psi

    property vanishing_moments_phi:
        "Number of vanishing moments for scaling function"
        def __get__(self):
            if self.w.vanishing_moments_phi >= 0:
                return self.w.vanishing_moments_phi

    property filter_bank:
        """Returns tuple of wavelet filters coefficients
        (dec_lo, dec_hi, rec_lo, rec_hi)
        """
        def __get__(self):
            return (self.dec_lo, self.dec_hi, self.rec_lo, self.rec_hi)

    def get_filters_coeffs(self):
        warnings.warn("The `get_filters_coeffs` method is deprecated. "
                      "Use `filter_bank` attribute instead.", DeprecationWarning)
        return self.filter_bank

    property inverse_filter_bank:
        """Tuple of inverse wavelet filters coefficients
        (rec_lo[::-1], rec_hi[::-1], dec_lo[::-1], dec_hi[::-1])
        """
        def __get__(self):
            return (self.rec_lo[::-1], self.rec_hi[::-1], self.dec_lo[::-1],
                    self.dec_hi[::-1])

    def get_reverse_filters_coeffs(self):
        warnings.warn("The `get_reverse_filters_coeffs` method is deprecated. "
                      "Use `inverse_filter_bank` attribute instead.",
                      DeprecationWarning)
        return self.inverse_filter_bank

    def wavefun(self, int level=8):
        """
        wavefun(self, level=8)

        Calculates approximations of scaling function (`phi`) and wavelet
        function (`psi`) on xgrid (`x`) at a given level of refinement.

        Parameters
        ----------
        level : int, optional
            Level of refinement (default: 8).

        Returns
        -------
        [phi, psi, x] : array_like
            For orthogonal wavelets returns scaling function, wavelet function
            and xgrid - [phi, psi, x].

        [phi_d, psi_d, phi_r, psi_r, x] : array_like
            For biorthogonal wavelets returns scaling and wavelet function both
            for decomposition and reconstruction and xgrid

        Examples
        --------
        >>> import pywt
        >>> # Orthogonal
        >>> wavelet = pywt.Wavelet('db2')
        >>> phi, psi, x = wavelet.wavefun(level=5)
        >>> # Biorthogonal
        >>> wavelet = pywt.Wavelet('bior3.5')
        >>> phi_d, psi_d, phi_r, psi_r, x = wavelet.wavefun(level=5)

        """
        cdef pywt_index_t filter_length "filter_length"
        cdef pywt_index_t right_extent_length "right_extent_length"
        cdef pywt_index_t output_length "output_length"
        cdef pywt_index_t keep_length "keep_length"
        cdef np.float64_t n, n_mul
        cdef np.float64_t[::1] n_arr = <np.float64_t[:1]> &n,
        cdef np.float64_t[::1] n_mul_arr = <np.float64_t[:1]> &n_mul
        cdef double p "p"
        cdef double mul "mul"
        cdef Wavelet other "other"
        cdef phi_d, psi_d, phi_r, psi_r
        cdef psi_i
        cdef np.float64_t[::1] x, psi

        n = pow(sqrt(2.), <double>level)
        p = (pow(2., <double>level))

        if self.w.base.orthogonal:
            filter_length = self.w.dec_len
            output_length = <pywt_index_t> ((filter_length-1) * p + 1)
            keep_length = get_keep_length(output_length, level, filter_length)
            output_length = fix_output_length(output_length, keep_length)

            right_extent_length = get_right_extent_length(output_length,
                                                          keep_length)

            # phi, psi, x
            return [np.concatenate(([0.],
                                    keep(upcoef(True, n_arr, self, level, 0), keep_length),
                                    np.zeros(right_extent_length))),
                    np.concatenate(([0.],
                                    keep(upcoef(False, n_arr, self, level, 0), keep_length),
                                    np.zeros(right_extent_length))),
                    np.linspace(0.0, (output_length-1)/p, output_length)]
        else:
            if self.w.base.biorthogonal and (self.w.vanishing_moments_psi % 4) != 1:
                # FIXME: I don't think this branch is well tested
                n_mul = -n
            else:
                n_mul = n

            other = Wavelet(filter_bank=self.inverse_filter_bank)

            filter_length  = other.w.dec_len
            output_length = <pywt_index_t> ((filter_length-1) * p)
            keep_length = get_keep_length(output_length, level, filter_length)
            output_length = fix_output_length(output_length, keep_length)
            right_extent_length = get_right_extent_length(output_length, keep_length)

            phi_d  = np.concatenate(([0.],
                                     keep(upcoef(True, n_arr, other, level, 0), keep_length),
                                     np.zeros(right_extent_length)))
            psi_d  = np.concatenate(([0.],
                                     keep(upcoef(False, n_mul_arr, other, level, 0),
                                          keep_length),
                                     np.zeros(right_extent_length)))

            filter_length = self.w.dec_len
            output_length = <pywt_index_t> ((filter_length-1) * p)
            keep_length = get_keep_length(output_length, level, filter_length)
            output_length = fix_output_length(output_length, keep_length)
            right_extent_length = get_right_extent_length(output_length, keep_length)

            phi_r  = np.concatenate(([0.],
                                     keep(upcoef(True, n_arr, self, level, 0), keep_length),
                                     np.zeros(right_extent_length)))
            psi_r  = np.concatenate(([0.],
                                     keep(upcoef(False, n_mul_arr, self, level, 0),
                                          keep_length),
                                     np.zeros(right_extent_length)))

            return [phi_d, psi_d, phi_r, psi_r,
                    np.linspace(0.0, (output_length - 1) / p, output_length)]

    def __str__(self):
        s = []
        for x in [
            u"Wavelet %s"           % self.name,
            u"  Family name:    %s" % self.family_name,
            u"  Short name:     %s" % self.short_family_name,
            u"  Filters length: %d" % self.dec_len,
            u"  Orthogonal:     %s" % self.orthogonal,
            u"  Biorthogonal:   %s" % self.biorthogonal,
            u"  Symmetry:       %s" % self.symmetry,
            u"  DWT:            True",
            u"  CWT:            False"
            ]:
            s.append(x.rstrip())
        return u'\n'.join(s)

    def __repr__(self):
        repr = "{module}.{classname}(name='{name}', filter_bank={filter_bank})"
        return repr.format(module=type(self).__module__,
                           classname=type(self).__name__,
                           name=self.name,
                           filter_bank=self.filter_bank)


cdef public class ContinuousWavelet [type ContinuousWaveletType, object ContinuousWaveletObject]:
    """
    ContinuousWavelet(name, dtype) object describe properties of
    a continuous wavelet identified by name.

    In order to use a built-in wavelet the parameter name must be
    a valid name from the wavelist() list.

    """
    #cdef readonly properties
    def __cinit__(self, name=u"", dtype=np.float64):
        cdef object family_code, family_number

        # builtin wavelet
        self.name = name.lower()
        self.dt = dtype
        if np.dtype(self.dt) not in [np.float32, np.float64]:
            raise ValueError(
                "Only np.float32 and np.float64 dtype are supported for "
                "ContinuousWavelet objects.")
        if len(self.name) >= 4 and self.name[:4] in ['cmor', 'shan', 'fbsp']:
            base_name = self.name[:4]
            if base_name == self.name:
                if base_name == 'fbsp':
                    msg = (
                        "Wavelets of family {0}, without parameters "
                        "specified in the name are deprecated.  The name "
                        "should follow the format {0}M-B-C where M is the spline "
                        "order and B, C are floats representing the bandwidth "
                        "frequency and center frequency, respectively "
                        "(example, for backward compatibility: "
                        "{0} = {0}2-1.0-0.5).").format(base_name)
                elif base_name == 'shan':
                    msg = (
                        "Wavelets from the family {0}, without parameters "
                        "specified in the name are deprecated. The name "
                        "should follow the format {0}B-C, where B and C are floats "
                        "representing the bandwidth frequency and center "
                        "frequency, respectively (example, for backward "
                        "compatibility: {0} = {0}0.5-1.0)."
                        ).format(base_name)
                else:
                    msg = (
                        "Wavelets from the family {0}, without parameters "
                        "specified in the name are deprecated. The name "
                        "should follow the format {0}B-C, where B and C are floats "
                        "representing the bandwidth frequency and center "
                        "frequency, respectively (example, for backward "
                        "compatibility: {0} = {0}1.0-0.5)."
                        ).format(base_name)
                warnings.warn(msg, FutureWarning)
        else:
            base_name = self.name
        family_code, family_number = wname_to_code(base_name)
        self.w = <wavelet.ContinuousWavelet*> wavelet.continuous_wavelet(
            family_code, family_number)

        if self.w is NULL:
            raise ValueError("Invalid wavelet name '%s'." % self.name)
        self.number = family_number

        # set wavelet attributes based on frequencies extracted from the name
        if base_name != self.name:
            freqs = re.findall(cwt_pattern, self.name)
            if base_name in ['shan', 'cmor']:
                if len(freqs) != 2:
                    raise ValueError(
                        ("For wavelets of family {0}, the name should take "
                         "the form {0}B-C where B and C are floats "
                         "representing the bandwidth frequency and center "
                         "frequency, respectively. (example: {0}1.5-1.0)"
                        ).format(base_name))
                self.w.bandwidth_frequency = float(freqs[0])
                self.w.center_frequency = float(freqs[1])
            elif base_name in ['fbsp', ]:
                if len(freqs) != 3:
                    raise ValueError(
                        ("For wavelets of family {0}, the name should take "
                         "the form {0}M-B-C where M is the spline order and B"
                         ", C are floats representing the bandwidth frequency "
                         "and center frequency, respectively "
                         "(example: {0}1-1.5-1.0).").format(base_name))
                M = float(freqs[0])
                self.w.bandwidth_frequency = float(freqs[1])
                self.w.center_frequency = float(freqs[2])
                if M < 1 or M % 1 != 0:
                    raise ValueError(
                        "Wavelet spline order must be an integer >= 1.")
                self.w.fbsp_order = int(M)
            else:
                raise ValueError(
                    "Invalid continuous wavelet name '%s'." % self.name)


    def __dealloc__(self):
        if self.w is not NULL:
            wavelet.free_continuous_wavelet(self.w)
            self.w = NULL

    def __reduce__(self):
        return (ContinuousWavelet, (self.name, self.dt))

    property family_number:
        "Wavelet family number"
        def __get__(self):
            return self.number

    property family_name:
        "Wavelet family name"
        def __get__(self):
            return self.w.base.family_name.decode('latin-1')

    property short_family_name:
        "Short wavelet family name"
        def __get__(self):
            return self.w.base.short_name.decode('latin-1')

    property orthogonal:
        "Is orthogonal"
        def __get__(self):
            return bool(self.w.base.orthogonal)
        def __set__(self, int value):
            self.w.base.orthogonal = (value != 0)

    property biorthogonal:
        "Is biorthogonal"
        def __get__(self):
            return bool(self.w.base.biorthogonal)
        def __set__(self, int value):
            self.w.base.biorthogonal = (value != 0)

    property complex_cwt:
        "CWT is complex"
        def __get__(self):
            return bool(self.w.complex_cwt)
        def __set__(self, int value):
            self.w.complex_cwt = (value != 0)

    property lower_bound:
        "Lower Bound"
        def __get__(self):
            if self.w.lower_bound != self.w.upper_bound:
                return self.w.lower_bound
        def __set__(self, float value):
            self.w.lower_bound = value

    property upper_bound:
        "Upper Bound"
        def __get__(self):
            if self.w.upper_bound != self.w.lower_bound:
                return self.w.upper_bound
        def __set__(self, float value):
            self.w.upper_bound = value

    property center_frequency:
        "Center frequency (shan, fbsp, cmor)"
        def __get__(self):
            if self.w.center_frequency > 0:
                return self.w.center_frequency
        def __set__(self, float value):
            self.w.center_frequency = value

    property bandwidth_frequency:
        "Bandwidth frequency (shan, fbsp, cmor)"
        def __get__(self):
            if self.w.bandwidth_frequency > 0:
                return self.w.bandwidth_frequency
        def __set__(self, float value):
            self.w.bandwidth_frequency = value

    property fbsp_order:
        "order parameter for fbsp"
        def __get__(self):
            if self.w.fbsp_order != 0:
                return self.w.fbsp_order
        def __set__(self, unsigned int value):
            self.w.fbsp_order = value

    property symmetry:
        "Wavelet symmetry"
        def __get__(self):
            if self.w.base.symmetry == wavelet.ASYMMETRIC:
                return "asymmetric"
            elif self.w.base.symmetry == wavelet.NEAR_SYMMETRIC:
                return "near symmetric"
            elif self.w.base.symmetry == wavelet.SYMMETRIC:
                return "symmetric"
            elif self.w.base.symmetry == wavelet.ANTI_SYMMETRIC:
                return "anti-symmetric"
            else:
                return "unknown"

    def wavefun(self, int level=8, length=None):
        """
        wavefun(self, level=8, length=None)

        Calculates approximations of wavelet function (``psi``) on xgrid
        (``x``) at a given level of refinement or length itself.

        Parameters
        ----------
        level : int, optional
            Level of refinement (default: 8). Defines the length by
            ``2**level`` if length is not set.
        length : int, optional
            Number of samples. If set to None, the length is set to
            ``2**level`` instead.

        Returns
        -------
        psi : array_like
            Wavelet function computed for grid xval
        xval : array_like
            grid going from lower_bound to upper_bound

        Notes
        -----
        The effective support are set with ``lower_bound`` and ``upper_bound``.
        The wavelet function is complex for ``'cmor'``, ``'shan'``, ``'fbsp'``
        and ``'cgau'``.

        The complex frequency B-spline wavelet (``'fbsp'``) has
        ``bandwidth_frequency``, ``center_frequency`` and ``fbsp_order`` as
        additional parameters.

        The complex Shannon wavelet (``'shan'``) has ``bandwidth_frequency``
        and ``center_frequency`` as additional parameters.

        The complex Morlet wavelet (``'cmor'``) has ``bandwidth_frequency``
        and ``center_frequency`` as additional parameters.

        Examples
        --------
        >>> import pywt
        >>> import matplotlib.pyplot as plt
        >>> lb = -5
        >>> ub = 5
        >>> n = 1000
        >>> wavelet = pywt.ContinuousWavelet("gaus8")
        >>> wavelet.upper_bound = ub
        >>> wavelet.lower_bound = lb
        >>> [psi,xval] = wavelet.wavefun(length=n)
        >>> plt.plot(xval,psi)
        >>> plt.title("Gaussian Wavelet of order 8")
        >>> plt.show()

        >>> import numpy as np
        >>> import matplotlib.pyplot as plt
        >>> import pywt
        >>> lb = -5
        >>> ub = 5
        >>> n = 1000
        >>> wavelet = pywt.ContinuousWavelet("cgau4")
        >>> wavelet.upper_bound = ub
        >>> wavelet.lower_bound = lb
        >>> [psi,xval] = wavelet.wavefun(length=n)
        >>> fix, (ax1, ax2) = plt.subplots(2, 1)
        >>> ax1.plot(xval,np.real(psi))
        >>> ax1.set_title("Real part")
        >>> ax2.plot(xval,np.imag(psi))
        >>> ax2.set_title("Imaginary part")
        >>> plt.show()

        """
        cdef pywt_index_t output_length "output_length"
        cdef psi_i, psi_r, psi
        cdef np.float64_t[::1] x64, psi64
        cdef np.float32_t[::1] x32, psi32

        p = (pow(2., <double>level))

        if self.w is not NULL:
            if length is None:
                output_length = <pywt_index_t>p
            else:
                output_length = <pywt_index_t>length
            if (self.dt == np.float64):
                x64 = np.linspace(self.w.lower_bound, self.w.upper_bound, output_length, dtype=self.dt)
            else:
                x32 = np.linspace(self.w.lower_bound, self.w.upper_bound, output_length, dtype=self.dt)
            if self.w.complex_cwt:
                if (self.dt == np.float64):
                    psi_r, psi_i = cwt_psi_single(x64, self, output_length)
                    return [np.asarray(psi_r, dtype=self.dt) + 1j * np.asarray(psi_i, dtype=self.dt),
                        np.asarray(x64, dtype=self.dt)]
                else:
                    psi_r, psi_i = cwt_psi_single(x32, self, output_length)
                    return [np.asarray(psi_r, dtype=self.dt) + 1j * np.asarray(psi_i, dtype=self.dt),
                            np.asarray(x32, dtype=self.dt)]
            else:
                if (self.dt == np.float64):
                    psi = cwt_psi_single(x64, self, output_length)
                    return [np.asarray(psi, dtype=self.dt),
                            np.asarray(x64, dtype=self.dt)]

                else:
                    psi = cwt_psi_single(x32, self, output_length)
                    return [np.asarray(psi, dtype=self.dt),
                            np.asarray(x32, dtype=self.dt)]

    def __str__(self):
        s = []
        for x in [
            u"ContinuousWavelet %s" % self.name,
            u"  Family name:    %s" % self.family_name,
            u"  Short name:     %s" % self.short_family_name,
            u"  Symmetry:       %s" % self.symmetry,
            u"  DWT:            False",
            u"  CWT:            True",
            u"  Complex CWT:    %s" % self.complex_cwt
            ]:
            s.append(x.rstrip())
        return u'\n'.join(s)

    def __repr__(self):
        repr = "{module}.{classname}(name='{name}')"
        return repr.format(module=type(self).__module__,
                           classname=type(self).__name__,
                           name=self.name)


cdef pywt_index_t get_keep_length(pywt_index_t output_length,
                             int level, pywt_index_t filter_length):
    cdef pywt_index_t lplus "lplus"
    cdef pywt_index_t keep_length "keep_length"
    cdef int i "i"
    lplus = filter_length - 2
    keep_length = 1
    for i in range(level):
        keep_length = 2*keep_length+lplus
    return keep_length

cdef pywt_index_t fix_output_length(pywt_index_t output_length, pywt_index_t keep_length):
    if output_length-keep_length-2 < 0:
        output_length = keep_length+2
    return output_length

cdef pywt_index_t get_right_extent_length(pywt_index_t output_length, pywt_index_t keep_length):
    return output_length - keep_length - 1


def wavelet_from_object(wavelet):
    return c_wavelet_from_object(wavelet)


cdef c_wavelet_from_object(wavelet):
    if isinstance(wavelet, (Wavelet, ContinuousWavelet)):
        return wavelet
    else:
        return Wavelet(wavelet)


cpdef np.dtype _check_dtype(data):
    """Check for cA/cD input what (if any) the dtype is."""
    cdef np.dtype dt
    try:
        dt = data.dtype
        if dt not in (np.float64, np.float32, np.complex64, np.complex128):
            if dt == np.half:
                # half-precision input converted to single precision
                dt = np.dtype('float32')
            elif dt == np.complex256:
                # complex256 is not supported.  run at reduced precision
                dt = np.dtype('complex128')
            else:
                # integer input was always accepted; convert to float64
                dt = np.dtype('float64')
    except AttributeError:
        dt = np.dtype('float64')
    return dt


# TODO: Can this be replaced by the take parameter of upcoef? Or vice-versa?
def keep(arr, keep_length):
    length = len(arr)
    if keep_length < length:
        left_bound = (length - keep_length) // 2
        return arr[left_bound:left_bound + keep_length]
    return arr


# Some utility functions

cdef object float64_array_to_list(double* data, pywt_index_t n):
    cdef pywt_index_t i
    cdef object app
    cdef object ret
    ret = []
    app = ret.append
    for i in range(n):
        app(data[i])
    return ret


cdef void copy_object_to_float64_array(source, double* dest) except *:
    cdef pywt_index_t i
    cdef double x
    i = 0
    for x in source:
        dest[i] = x
        i = i + 1


cdef void copy_object_to_float32_array(source, float* dest) except *:
    cdef pywt_index_t i
    cdef float x
    i = 0
    for x in source:
        dest[i] = x
        i = i + 1
