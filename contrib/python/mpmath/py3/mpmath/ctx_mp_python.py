import inspect
import numbers
import sys

from . import function_docs
from .libmp import (MPQ, MPZ, ComplexResult, dps_to_prec, finf, fnan, fninf,
                    format_mpc, format_mpf, from_Decimal, from_float, from_int,
                    from_man_exp, from_npfloat, from_rational, from_str, fzero,
                    int_types, mpc_abs, mpc_add, mpc_add_mpf, mpc_conjugate,
                    mpc_div, mpc_div_mpf, mpc_hash, mpc_is_inf, mpc_is_nonzero,
                    mpc_mpf_div, mpc_mpf_sub, mpc_mul, mpc_mul_int,
                    mpc_mul_mpf, mpc_neg, mpc_pos, mpc_pow, mpc_pow_int,
                    mpc_pow_mpf, mpc_sub, mpc_sub_mpf, mpc_to_complex,
                    mpc_to_str, mpf_abs, mpf_add, mpf_div, mpf_eq, mpf_ge,
                    mpf_gt, mpf_hash, mpf_le, mpf_lt, mpf_mod, mpf_mul,
                    mpf_neg, mpf_pos, mpf_pow, mpf_sub, mpf_sum, prec_to_dps,
                    round_nearest, to_fixed, to_float, to_int, to_man_exp,
                    to_rational, to_str)


new = object.__new__

class mpnumeric:
    """Base class for mpf and mpc."""

# pickling support
def _make_mpf(x):
    from mpmath import mp
    return mp.mpf(x)

def _make_mpc(x, y):
    from mpmath import mp
    return mp.mpc(x, y)


class _mpf(mpnumeric):
    """
    An mpf instance holds a real-valued floating-point number. mpf:s
    work analogously to Python floats, but support arbitrary-precision
    arithmetic.
    """
    __slots__ = ['_mpf_', 'context']

    def __new__(cls, val=fzero, **kwargs):
        """A new mpf can be created from a Python float, an int, a
        or a decimal string representing a number in floating-point
        format."""
        ctx = cls.context
        prec, rounding = ctx._prec_rounding
        base = 0
        if kwargs:
            prec = kwargs.get('prec', prec)
            if 'dps' in kwargs:
                prec = dps_to_prec(kwargs['dps'])
            rounding = kwargs.get('rounding', rounding)
            base = kwargs.get('base', base)
        v = new(cls)
        if type(val) is cls:
            val = val._mpf_
        elif type(val) is tuple:
            if len(val) == 4:
                val = val[0], MPZ(val[1]), *val[2:]
            elif len(val) == 2:
                v._mpf_ = from_man_exp(val[0], val[1], prec, rounding)
                return v
            else:
                raise ValueError
        elif isinstance(val, str):
            val = from_str(val, prec, rounding, base)
        else:
            val = cls.mpf_convert_arg(val, prec, rounding)
        v._mpf_ = mpf_pos(val, prec, rounding)
        return v

    @classmethod
    def mpf_convert_arg(cls, x, prec, rounding):
        if isinstance(x, int_types): return from_int(x)
        if isinstance(x, float): return from_float(x)
        ctx = cls.context
        if isinstance(x, ctx.constant): return x.func(prec, rounding)
        if hasattr(x, '_mpf_'): return x._mpf_
        if hasattr(x, '_mpmath_'):
            t = ctx.convert(x._mpmath_(prec, rounding))
            if hasattr(t, '_mpf_'):
                return t._mpf_
        if hasattr(x, '_mpi_'):
            a, b = x._mpi_
            if a == b:
                return a
            raise ValueError("can only create mpf from zero-width interval")
        if isinstance(x, numbers.Rational): return from_rational(x.numerator,
                                                                 x.denominator,
                                                                 prec, rounding)
        if type(x).__module__ == 'decimal':
            return from_Decimal(x, prec, rounding)
        raise TypeError("cannot create mpf from " + repr(x))

    @classmethod
    def mpf_convert_rhs(cls, x):
        try:
            ctx = cls.context
            r = ctx.convert(x, strings=False)
            if hasattr(r, '_mpf_'):
                r = r._mpf_
            return r
        except (ValueError, TypeError):
            return NotImplemented

    @classmethod
    def mpf_convert_lhs(cls, x):
        x = cls.mpf_convert_rhs(x)
        if type(x) is tuple:
            ctx = cls.context
            return ctx.make_mpf(x)
        return x

    man_exp = property(lambda self: to_man_exp(self._mpf_, signed=False))
    man = property(lambda self: self.man_exp[0])
    exp = property(lambda self: self.man_exp[1])
    bc = property(lambda self: self.man.bit_length())

    real = property(lambda self: self)
    imag = property(lambda self: self.context.zero)

    conjugate = lambda self: self

    def as_integer_ratio(self):
        return to_rational(self._mpf_)

    def __reduce__(self): return _make_mpf, (self._mpf_,)

    def __repr__(self):
        ctx = self.context
        rounding = ctx._prec_rounding[1]
        if ctx.pretty:
            ndigits = (ctx._repr_digits
                       if ctx._pretty_repr_dps else ctx._str_digits)
            return to_str(self._mpf_, ndigits, rnd=rounding)
        return f"mpf({to_str(self._mpf_, ctx._repr_digits, rnd=rounding)!r})"

    def __str__(self):
        ctx = self.context
        rounding = ctx._prec_rounding[1]
        return to_str(self._mpf_, ctx._str_digits, rnd=rounding)

    def __hash__(self): return mpf_hash(self._mpf_)
    def __int__(self): return int(to_int(self._mpf_))

    def __float__(self):
        ctx = self.context
        rounding = ctx._prec_rounding[1]
        return to_float(self._mpf_, rnd=rounding)

    def __bool__(self): return self._mpf_ != fzero

    def __abs__(self):
        mpf, new, (prec, rounding) = self._ctxdata
        v = new(mpf)
        v._mpf_ = mpf_abs(self._mpf_, prec, rounding)
        return v

    def __pos__(self):
        mpf, new, (prec, rounding) = self._ctxdata
        v = new(mpf)
        v._mpf_ = mpf_pos(self._mpf_, prec, rounding)
        return v

    def __neg__(self):
        mpf, new, (prec, rounding) = self._ctxdata
        v = new(mpf)
        v._mpf_ = mpf_neg(self._mpf_, prec, rounding)
        return v

    def _cmp(self, other, func):
        if hasattr(other, '_mpf_'):
            other = other._mpf_
        else:
            other = self.mpf_convert_rhs(other)
            if other is NotImplemented:
                return other
        return func(self._mpf_, other)

    def __lt__(self, other): return self._cmp(other, mpf_lt)
    def __gt__(self, other): return self._cmp(other, mpf_gt)
    def __le__(self, other): return self._cmp(other, mpf_le)
    def __ge__(self, other): return self._cmp(other, mpf_ge)

    def __eq__(self, other):
        mpf, new, (prec, rounding) = self._ctxdata
        sval = self._mpf_
        if hasattr(other, '_mpf_'):
            oval = other._mpf_
            return mpf_eq(sval, oval)
        if hasattr(other, '_mpc_'):
            oval = other._mpc_
            return (oval[1] == fzero) and mpf_eq(oval[0], sval)
        try:
            ctx = mpf.context
            other = ctx.convert(other, strings=False)
        except TypeError:
            return NotImplemented
        return self.__eq__(other)

    def __add__(self, other):
        mpf, new, (prec, rounding) = self._ctxdata
        sval = self._mpf_
        if hasattr(other, '_mpf_'):
            oval = other._mpf_
            val = mpf_add(sval, oval, prec, rounding)
            obj = new(mpf)
            obj._mpf_ = val
            return obj
        if hasattr(other, '_mpc_'):
            oval = other._mpc_
            mpc = type(other)
            val = mpc_add_mpf(oval, sval, prec, rounding)
            obj = new(mpc)
            obj._mpc_ = val
            return obj
        try:
            ctx = mpf.context
            other = ctx.convert(other, strings=False)
        except TypeError:
            return NotImplemented
        return self.__add__(other)
    __radd__ = __add__

    def __sub__(self, other):
        mpf, new, (prec, rounding) = self._ctxdata
        sval = self._mpf_
        if hasattr(other, '_mpf_'):
            oval = other._mpf_
            val = mpf_sub(sval, oval, prec, rounding)
            obj = new(mpf)
            obj._mpf_ = val
            return obj
        if hasattr(other, '_mpc_'):
            oval = other._mpc_
            mpc = type(other)
            val = mpc_mpf_sub(sval, oval, prec, rounding)
            obj = new(mpc)
            obj._mpc_ = val
            return obj
        try:
            ctx = mpf.context
            other = ctx.convert(other, strings=False)
        except TypeError:
            return NotImplemented
        return self.__sub__(other)

    def __rsub__(self, other):
        other = self.mpf_convert_lhs(other)
        if other is NotImplemented:
            return other
        return other - self

    def __mul__(self, other):
        mpf, new, (prec, rounding) = self._ctxdata
        sval = self._mpf_
        if hasattr(other, '_mpf_'):
            oval = other._mpf_
            val = mpf_mul(sval, oval, prec, rounding)
            obj = new(mpf)
            obj._mpf_ = val
            return obj
        if hasattr(other, '_mpc_'):
            oval = other._mpc_
            mpc = type(other)
            val = mpc_mul_mpf(oval, sval, prec, rounding)
            obj = new(mpc)
            obj._mpc_ = val
            return obj
        try:
            ctx = mpf.context
            other = ctx.convert(other, strings=False)
        except TypeError:
            return NotImplemented
        return self.__mul__(other)
    __rmul__ = __mul__

    def __truediv__(self, other):
        mpf, new, (prec, rounding) = self._ctxdata
        sval = self._mpf_
        if hasattr(other, '_mpf_'):
            oval = other._mpf_
            val = mpf_div(sval, oval, prec, rounding)
            obj = new(mpf)
            obj._mpf_ = val
            return obj
        if hasattr(other, '_mpc_'):
            oval = other._mpc_
            mpc = type(other)
            val = mpc_mpf_div(sval, oval, prec, rounding)
            obj = new(mpc)
            obj._mpc_ = val
            return obj
        try:
            ctx = mpf.context
            other = ctx.convert(other, strings=False)
        except TypeError:
            return NotImplemented
        return self.__truediv__(other)

    def __rtruediv__(self, other):
        other = self.mpf_convert_lhs(other)
        if other is NotImplemented:
            return other
        return other / self

    def __mod__(self, other):
        mpf, new, (prec, rounding) = self._ctxdata
        sval = self._mpf_
        if hasattr(other, '_mpf_'):
            oval = other._mpf_
            val = mpf_mod(sval, oval, prec, rounding)
            obj = new(mpf)
            obj._mpf_ = val
            return obj
        if hasattr(other, '_mpc_'):
            return NotImplemented
        try:
            ctx = mpf.context
            other = ctx.convert(other, strings=False)
        except TypeError:
            return NotImplemented
        return self.__mod__(other)

    def __rmod__(self, other):
        other = self.mpf_convert_lhs(other)
        if other is NotImplemented:
            return other
        return other % self

    def __floordiv__(self, other):
        return (self - (self % other)) / other

    def __divmod__(self, other):
        mod = self % other
        return (self - mod) / other, mod

    def __pow__(self, other):
        mpf, new, (prec, rounding) = self._ctxdata
        ctx = mpf.context
        sval = self._mpf_
        if hasattr(other, '_mpf_'):
            oval = other._mpf_
            try:
                val = mpf_pow(sval, oval, prec, rounding)
                obj = new(mpf)
                obj._mpf_ = val
                return obj
            except ComplexResult:
                if ctx.trap_complex:
                    raise
                mpc = ctx.mpc
                val = mpc_pow_mpf((sval, fzero), oval, prec, rounding)
                obj = new(mpc)
                obj._mpc_ = val
                return obj
        if hasattr(other, '_mpc_'):
            oval = other._mpc_
            mpc = ctx.mpc
            val = mpc_pow((sval, fzero), oval, prec, rounding)
            obj = new(mpc)
            obj._mpc_ = val
            return obj
        try:
            other = ctx.convert(other, strings=False)
        except TypeError:
            return NotImplemented
        return self.__pow__(other)

    def __rpow__(self, other):
        other = self.mpf_convert_lhs(other)
        if other is NotImplemented:
            return other
        return other ** self

    def __format__(self, format_spec):
        """
        ``mpf`` objects allow for formatting similar to Python floats:

            >>> from mpmath import fp, mp, pi
            >>> mp.dps = 50
            >>> format(pi, '*^60.50f')
            '****3.14159265358979323846264338327950288419716939937511****'
            >>> f'{10*pi:.20e}'
            '3.14159265358979323846e+01'

        The format specification adopts the same general form as Python's
        :external:ref:`formatspec`.  All of Python's format types are
        supported, with the exception of ``'n'``.

        If precision is left as default, the resulting string is exactly the
        same as if printing a regular :external:class:`float`:

            >>> mp.dps = fp.dps
            >>> f"{mp.mpf('1.22'):.25f}"
            '1.2199999999999999733546474'
            >>> f'{1.22:.25f}'
            '1.2199999999999999733546474'
            >>> mp.dps = 50
            >>> f"{mp.mpf('1.22'):.25f}"
            '1.2200000000000000000000000'

        In addition to the normal Python features, four different kinds of
        rounding are supported:

            * ``'U'``: rounding towards plus infinity
            * ``'D'``: rounding towards minus infinity
            * ``'Y'``: rounding away from zero
            * ``'Z'``: rounding towards zero
            * ``'N'``: rounding to nearest (default)

        If it's not specified, the context's rounding mode is used.

        The rounding option must be set right before the presentation type:

            >>> x = mp.mpf('-1.2345678')
            >>> f'{x:.5Uf}'
            '-1.23456'
            >>> f'{x:.5Df}'
            '-1.23457'

        Format types ``'a'`` and ``'A'`` (use uppercase digits) allow to
        represent floating-point number as a C99-style hexadecimal string
        ``[±][0x]h[.hhh]p±d``, where there is one hexadecimal digit before the
        dot and the fractional part either is exact or the number of its
        hexadecimal digits is equal to the specified precision.  The exponent
        ``d`` is written in decimal, it always contains at least one digit,
        and it gives the power of 2 by which to multiply the coefficient.  If
        no digits follow the decimal point, the decimal point is also removed
        unless the ``'#'`` option is specified.

            >>> f'{x:a}'
            '-0x1.3c0ca2a5b1d5d0818d3359c99ff1a26f2b31063249p+0'
            >>> f'{x:.10a}'
            '-0x1.3c0ca2a5b2p+0'

        Format type ``'b'`` allows format number in binary:

            >>> f'{x:.15b}'
            '-1.001111000000110p+0'

        Alternate form (``'#'`` option) works like for ``'a'`` type.
        """

        _, _, (prec, rounding) = self._ctxdata
        ctx = self.context
        return format_mpf(self._mpf_, format_spec, prec, rounding,
                          ctx._pretty_repr_dps)

    def sqrt(self):
        ctx = self.context
        return ctx.sqrt(self)

    def ae(self, other, rel_eps=None, abs_eps=None):
        ctx = self.context
        return ctx.almosteq(self, other, rel_eps, abs_eps)

    def to_fixed(self, prec):
        return to_fixed(self._mpf_, prec)

    def __round__(self, ndigits=None):
        ctx = self.context
        if ctx.isfinite(self):
            frac = MPQ(*self.as_integer_ratio())
            res = round(frac, ndigits)
            res = ctx.convert(res)
        else:
            res = self
        if ndigits is None:
            res = int(res)
        return res


class _constant(_mpf):
    """Represents a mathematical constant with dynamic precision.
    When printed or used in an arithmetic operation, a constant
    is converted to a regular mpf at the working precision. A
    regular mpf can also be obtained using the operation +x."""

    def __new__(cls, func, name, docname='', _reprdps_getter=lambda: 15):
        a = object.__new__(cls)
        a.name = name
        a.func = func
        a._reprdps_getter = _reprdps_getter
        a.__doc__ = getattr(function_docs, docname, '')
        return a

    def __call__(self, prec=None, dps=None, rounding=None):
        prec2, rounding2 = self.context._prec_rounding
        if not prec: prec = prec2
        if not rounding: rounding = rounding2
        if dps: prec = dps_to_prec(dps)
        return self.context.make_mpf(self.func(prec, rounding))

    @property
    def _mpf_(self):
        prec, rounding = self.context._prec_rounding
        return self.func(prec, rounding)

    def __repr__(self):
        return "<%s: %s~>" % (self.name, self.context.nstr(self(dps=self._reprdps_getter())))


class _mpc(mpnumeric):
    """
    An mpc represents a complex number using a pair of mpf's (one
    for the real part and another for the imaginary part.) The mpc
    class behaves fairly similarly to Python's complex type.
    """

    __slots__ = ['_mpc_']

    def __new__(cls, real=0, imag=0):
        ctx = cls.context
        s = object.__new__(cls)
        if isinstance(real, str):
            real = ctx.convert(real)
        if isinstance(real, complex_types):
            r_real, r_imag = real.real, real.imag
        elif hasattr(real, '_mpc_'):
            r_real, r_imag = real._mpc_
        else:
            r_real, r_imag = real, 0
        if isinstance(imag, complex_types):
            i_real, i_imag = imag.real, imag.imag
        elif hasattr(imag, '_mpc_'):
            i_real, i_imag = imag._mpc_
        else:
            i_real, i_imag = imag, 0
        r_real, r_imag = map(ctx.mpf, [r_real, r_imag])
        i_real, i_imag = map(ctx.mpf, [i_real, i_imag])
        real = r_real - i_imag
        imag = r_imag + i_real
        s._mpc_ = (real._mpf_, imag._mpf_)
        return s

    real = property(lambda self: self.context.make_mpf(self._mpc_[0]))
    imag = property(lambda self: self.context.make_mpf(self._mpc_[1]))

    def __reduce__(self): return _make_mpc, self._mpc_

    def __repr__(self):
        ctx = self.context
        if ctx.pretty:
            ndigits = (ctx._repr_digits
                       if ctx._pretty_repr_dps else ctx._str_digits)
            return f"({mpc_to_str(self._mpc_, ndigits)})"
        r = repr(self.real)[4:-1]
        i = repr(self.imag)[4:-1]
        return f"{type(self).__name__}(real={r}, imag={i})"

    def __str__(self):
        ctx = self.context
        return f"({mpc_to_str(self._mpc_, ctx._str_digits)})"

    def __complex__(self):
        ctx = self.context
        return mpc_to_complex(self._mpc_, rnd=ctx._prec_rounding[1])

    def __pos__(self):
        mpc, new, (prec, rounding) = self._ctxdata
        v = new(mpc)
        v._mpc_ = mpc_pos(self._mpc_, prec, rounding)
        return v

    def __abs__(self):
        ctx = self.context
        mpf = ctx.mpf
        _, new, (prec, rounding) = self._ctxdata
        v = new(mpf)
        v._mpf_ = mpc_abs(self._mpc_, prec, rounding)
        return v

    def __neg__(self):
        mpc, new, (prec, rounding) = self._ctxdata
        v = new(mpc)
        v._mpc_ = mpc_neg(self._mpc_, prec, rounding)
        return v

    def conjugate(self):
        mpc, new, (prec, rounding) = self._ctxdata
        v = new(mpc)
        v._mpc_ = mpc_conjugate(self._mpc_, prec, rounding)
        return v

    def __bool__(self):
        return mpc_is_nonzero(self._mpc_)

    def __hash__(self):
        return mpc_hash(self._mpc_)

    @classmethod
    def mpc_convert_lhs(cls, x):
        ctx = cls.context
        try:
            return ctx.convert(x, strings=False)
        except (TypeError, ValueError):
            return NotImplemented

    def __eq__(self, other):
        if not hasattr(other, '_mpc_'):
            if isinstance(other, str):
                return False
            other = self.mpc_convert_lhs(other)
            if other is NotImplemented:
                return other
        return self.real == other.real and self.imag == other.imag

    def __add__(self, other):
        mpc, new, (prec, rounding) = self._ctxdata
        sval = self._mpc_
        if not hasattr(other, '_mpc_'):
            other = self.mpc_convert_lhs(other)
            if other is NotImplemented:
                return other
            if hasattr(other, '_mpf_'):
                oval = other._mpf_
                val = mpc_add_mpf(sval, oval, prec, rounding)
                obj = new(mpc)
                obj._mpc_ = val
                return obj
        oval = other._mpc_
        val = mpc_add(sval, oval, prec, rounding)
        obj = new(mpc)
        obj._mpc_ = val
        return obj
    __radd__ = __add__

    def __sub__(self, other):
        mpc, new, (prec, rounding) = self._ctxdata
        sval = self._mpc_
        if not hasattr(other, '_mpc_'):
            other = self.mpc_convert_lhs(other)
            if other is NotImplemented:
                return other
            if hasattr(other, '_mpf_'):
                oval = other._mpf_
                val = mpc_sub_mpf(sval, other._mpf_, prec, rounding)
                obj = new(mpc)
                obj._mpc_ = val
                return obj
        oval = other._mpc_
        val = mpc_sub(sval, oval, prec, rounding)
        obj = new(mpc)
        obj._mpc_ = val
        return obj

    def __rsub__(self, other):
        other = self.mpc_convert_lhs(other)
        if other is NotImplemented:
            return other
        return other - self

    def __mul__(self, other):
        mpc, new, (prec, rounding) = self._ctxdata
        sval = self._mpc_
        if not hasattr(other, '_mpc_'):
            if isinstance(other, int_types):
                val = mpc_mul_int(sval, other, prec, rounding)
                obj = new(mpc)
                obj._mpc_ = val
                return obj
            other = self.mpc_convert_lhs(other)
            if other is NotImplemented:
                return other
            if hasattr(other, '_mpf_'):
                oval = other._mpf_
                val = mpc_mul_mpf(sval, oval, prec, rounding)
                obj = new(mpc)
                obj._mpc_ = val
                return obj
        oval = other._mpc_
        val = mpc_mul(sval, oval, prec, rounding)
        obj = new(mpc)
        obj._mpc_ = val
        return obj

    def __rmul__(self, other):
        mpc, new, (prec, rounding) = self._ctxdata
        if isinstance(other, int_types):
            sval = self._mpc_
            val = mpc_mul_int(sval, other, prec, rounding)
            obj = new(mpc)
            obj._mpc_ = val
            return obj
        other = self.mpc_convert_lhs(other)
        if other is NotImplemented:
            return other
        return other * self

    def __truediv__(self, other):
        mpc, new, (prec, rounding) = self._ctxdata
        sval = self._mpc_
        if not hasattr(other, '_mpc_'):
            other = self.mpc_convert_lhs(other)
            if other is NotImplemented:
                return other
            if hasattr(other, '_mpf_'):
                oval = other._mpf_
                val = mpc_div_mpf(sval, oval, prec, rounding)
                obj = new(mpc)
                obj._mpc_ = val
                return obj
        oval = other._mpc_
        val = mpc_div(sval, oval, prec, rounding)
        obj = new(mpc)
        obj._mpc_ = val
        return obj

    def __rtruediv__(self, other):
        other = self.mpc_convert_lhs(other)
        if other is NotImplemented:
            return other
        return other / self

    def __pow__(self, other):
        mpc, new, (prec, rounding) = self._ctxdata
        sval = self._mpc_
        if isinstance(other, int_types):
            val = mpc_pow_int(sval, other, prec, rounding)
            obj = new(mpc)
            obj._mpc_ = val
            return obj
        other = self.mpc_convert_lhs(other)
        if other is NotImplemented:
            return other
        if hasattr(other, '_mpf_'):
            oval = other._mpf_
            val = mpc_pow_mpf(sval, oval, prec, rounding)
        else:
            oval = other._mpc_
            val = mpc_pow(sval, oval, prec, rounding)
        obj = new(mpc)
        obj._mpc_ = val
        return obj

    def __rpow__(self, other):
        other = self.mpc_convert_lhs(other)
        if other is NotImplemented:
            return other
        return other ** self

    def ae(self, other, rel_eps=None, abs_eps=None):
        ctx = self.context
        return ctx.almosteq(self, other, rel_eps, abs_eps)

    def __format__(self, format_spec):
        """
        ``mpc`` objects allow for formatting similar to Python
        :external:class:`complex`, specified in :external:ref:`formatspec`.

        All ``mpf``'s format types and options are supported, with
        the exception for ``'%'`` format type, ``'='`` alignment and
        zero padding.
        """
        ctx = self.context
        _, _, (prec, rounding) = self._ctxdata
        return format_mpc(self._mpc_, format_spec, prec, rounding,
                          ctx._pretty_repr_dps)


complex_types = (complex, _mpc)


class PythonMPContext:
    def __init__(ctx):
        ctx._prec_rounding = [sys.float_info.mant_dig, round_nearest]
        ctx._pretty_repr_dps = False
        ctx.mpf = type('mpf', (_mpf,), {})
        ctx.mpf._ctxdata = [ctx.mpf, new, ctx._prec_rounding]
        ctx.mpf.context = ctx
        ctx.mpc = type('mpc', (_mpc,), {})
        ctx.mpc._ctxdata = [ctx.mpc, new, ctx._prec_rounding]
        ctx.mpc.context = ctx
        ctx.constant = type('constant', (_constant,), {})
        ctx.constant._ctxdata = [ctx.mpf, new, ctx._prec_rounding]
        ctx.constant.context = ctx

    def make_mpf(ctx, v):
        a = new(ctx.mpf)
        a._mpf_ = v
        return a

    def make_mpc(ctx, v):
        a = new(ctx.mpc)
        a._mpc_ = v
        return a

    def default(ctx):
        ctx._prec = ctx._prec_rounding[0] = sys.float_info.mant_dig
        ctx._dps = sys.float_info.dig
        ctx.trap_complex = False

    def _set_prec(ctx, n):
        ctx._prec = ctx._prec_rounding[0] = max(1, int(n))
        ctx._dps = prec_to_dps(n)

    def _set_dps(ctx, n):
        ctx._prec = ctx._prec_rounding[0] = dps_to_prec(n)
        ctx._dps = max(1, int(n))

    def _set_rounding(ctx, r):
        try:
            ctx._prec_rounding[1] = ctx._parse_prec({'rounding': r})[1]
        except KeyError:
            raise ValueError('invalid rounding mode')

    prec = property(lambda ctx: ctx._prec, _set_prec)
    dps = property(lambda ctx: ctx._dps, _set_dps)
    rounding = property(lambda ctx: ctx._prec_rounding[1], _set_rounding)

    def _set_pretty_dps(ctx, v):
        ctx._pretty_repr_dps = True if v == 'repr' else False

    def _get_pretty_dps(ctx):
        return 'repr' if ctx._pretty_repr_dps else 'str'

    pretty_dps = property(_get_pretty_dps, _set_pretty_dps)

    def convert(ctx, x, strings=True):
        """
        Converts *x* to an ``mpf`` or ``mpc``. If *x* is of type ``mpf``,
        ``mpc``, ``int``, ``float``, ``complex``, the conversion
        will be performed losslessly.

        If *x* is a string, the result will be rounded to the present
        working precision. Strings representing fractions or complex
        numbers are permitted.

            >>> from mpmath import mpmathify
            >>> mpmathify(3.5)
            mpf('3.5')
            >>> mpmathify('2.1')
            mpf('2.1000000000000001')
            >>> mpmathify('3/4')
            mpf('0.75')
            >>> mpmathify('2+3j')
            mpc(real='2.0', imag='3.0')

        """
        if type(x) in ctx.types: return x
        if isinstance(x, int_types): return ctx.make_mpf(from_int(x))
        if isinstance(x, float): return ctx.make_mpf(from_float(x))
        if isinstance(x, complex):
            return ctx.make_mpc((from_float(x.real), from_float(x.imag)))
        if type(x).__module__ == 'numpy': return ctx.npconvert(x)
        prec, rounding = ctx._prec_rounding
        if hasattr(x, '_mpf_'): return ctx.make_mpf(x._mpf_)
        if hasattr(x, '_mpc_'): return ctx.make_mpc(x._mpc_)
        if hasattr(x, '_mpmath_'):
            return ctx.convert(x._mpmath_(prec, rounding))
        if isinstance(x, numbers.Rational):
            p, q = x.numerator, x.denominator
            return ctx.make_mpf(from_rational(p, q, prec, rounding))
        if strings and isinstance(x, str):
            try:
                _mpf_ = from_str(x, prec, rounding)
                return ctx.make_mpf(_mpf_)
            except ValueError:
                pass
        if type(x).__module__ == 'decimal':
            return ctx.make_mpf(from_Decimal(x, prec, rounding))
        return ctx._convert_fallback(x, strings)

    def npconvert(ctx, x):
        """
        Converts *x* to an ``mpf`` or ``mpc``. *x* should be a numpy
        scalar.
        """
        import numpy as np
        if isinstance(x, np.ndarray) and x.ndim == 0: x = x.item()
        if isinstance(x, (np.integer, int)): return ctx.make_mpf(from_int(int(x)))
        if isinstance(x, (np.floating, float)): return ctx.mpf(from_npfloat(x))
        if isinstance(x, (np.complexfloating, complex)):
            return ctx.make_mpc((from_npfloat(x.real), from_npfloat(x.imag)))
        raise TypeError("cannot create mpf from " + repr(x))

    def isinf(ctx, x):
        """
        Return *True* if the absolute value of *x* is infinite;
        otherwise return *False*::

            >>> from mpmath import isinf, inf, mpc
            >>> isinf(inf)
            True
            >>> isinf(-inf)
            True
            >>> isinf(3)
            False
            >>> isinf(3+4j)
            False
            >>> isinf(mpc(3,inf))
            True
            >>> isinf(mpc(inf,3))
            True

        """
        if hasattr(x, "_mpf_"):
            return x._mpf_ in (finf, fninf)
        if hasattr(x, "_mpc_"):
            return mpc_is_inf(x._mpc_)
        if isinstance(x, int_types) or isinstance(x, MPQ):
            return False
        x = ctx.convert(x)
        return ctx.isinf(x)

    def isnormal(ctx, x):
        """
        Determine whether *x* is "normal" in the sense of floating-point
        representation; that is, return *False* if *x* is zero, an
        infinity or NaN; otherwise return *True*. By extension, a
        complex number *x* is considered "normal" if its magnitude is
        normal::

            >>> from mpmath import isnormal, inf, nan, mpc
            >>> isnormal(3)
            True
            >>> isnormal(0)
            False
            >>> isnormal(inf); isnormal(-inf); isnormal(nan)
            False
            False
            False
            >>> isnormal(0+0j)
            False
            >>> isnormal(0+3j)
            True
            >>> isnormal(mpc(2,nan))
            False
        """
        if hasattr(x, "_mpf_"):
            if ctx.isfinite(x):
                return bool(to_man_exp(x._mpf_, signed=True)[0])
            return False
        if hasattr(x, "_mpc_"):
            re, im = x._mpc_
            re_normal = bool(re[1])
            im_normal = bool(im[1])
            if re == fzero: return im_normal
            if im == fzero: return re_normal
            return re_normal and im_normal
        if isinstance(x, int_types) or isinstance(x, MPQ):
            return bool(x)
        x = ctx.convert(x)
        return ctx.isnormal(x)

    def isint(ctx, x, gaussian=False):
        """
        Return *True* if *x* is integer-valued; otherwise return
        *False*::

            >>> from mpmath import isint, mpf, inf
            >>> isint(3)
            True
            >>> isint(mpf(3))
            True
            >>> isint(3.2)
            False
            >>> isint(inf)
            False

        Optionally, Gaussian integers can be checked for::

            >>> isint(3+0j)
            True
            >>> isint(3+2j)
            False
            >>> isint(3+2j, gaussian=True)
            True

        """
        if isinstance(x, int_types):
            return True
        if hasattr(x, "_mpf_"):
            if ctx.isfinite(x):
                man, exp = to_man_exp(x._mpf_, signed=True)
                return bool((man and exp >= 0) or x._mpf_ == fzero)
            return False
        if hasattr(x, "_mpc_"):
            re, im = x._mpc_
            if ctx.isfinite(x):
                man, exp = to_man_exp(re, signed=True)
                re_isint = bool((man and exp >= 0) or re == fzero)
                man, exp = to_man_exp(im, signed=True)
                im_isint = bool((man and exp >= 0) or im == fzero)
            else:
                return False
            if gaussian:
                return re_isint and im_isint
            return re_isint and im == fzero
        if isinstance(x, MPQ):
            p, q = x.numerator, x.denominator
            return p % q == 0
        x = ctx.convert(x)
        return ctx.isint(x, gaussian)

    def fsum(ctx, terms, absolute=False, squared=False):
        """
        Calculates a sum containing a finite number of terms (for infinite
        series, see :func:`~mpmath.nsum`). The terms will be converted to
        mpmath numbers. For len(terms) > 2, this function is generally
        faster and produces more accurate results than the builtin
        Python function :func:`sum`.

            >>> from mpmath import fsum
            >>> fsum([1, 2, 0.5, 7])
            mpf('10.5')

        With squared=True each term is squared, and with absolute=True
        the absolute value of each term is used.
        """
        prec, rnd = ctx._prec_rounding
        real = []
        imag = []
        for term in terms:
            reval = imval = 0
            if hasattr(term, "_mpf_"):
                reval = term._mpf_
            elif hasattr(term, "_mpc_"):
                reval, imval = term._mpc_
            else:
                term = ctx.convert(term)
                if hasattr(term, "_mpf_"):
                    reval = term._mpf_
                elif hasattr(term, "_mpc_"):
                    reval, imval = term._mpc_
                else:
                    raise NotImplementedError
            if imval:
                if squared:
                    if absolute:
                        real.append(mpf_mul(reval,reval))
                        real.append(mpf_mul(imval,imval))
                    else:
                        reval, imval = mpc_pow_int((reval,imval),2,prec+10)
                        real.append(reval)
                        imag.append(imval)
                elif absolute:
                    real.append(mpc_abs((reval,imval), prec))
                else:
                    real.append(reval)
                    imag.append(imval)
            else:
                if squared:
                    reval = mpf_mul(reval, reval)
                elif absolute:
                    reval = mpf_abs(reval)
                real.append(reval)
        s = mpf_sum(real, prec, rnd, absolute)
        if imag:
            s = ctx.make_mpc((s, mpf_sum(imag, prec, rnd)))
        else:
            s = ctx.make_mpf(s)
        return s

    def fdot(ctx, A, B=None, conjugate=False):
        r"""
        Computes the dot product of the iterables `A` and `B`,

        .. math ::

            \sum_{k=0} A_k B_k.

        Alternatively, :func:`~mpmath.fdot` accepts a single iterable of pairs.
        In other words, ``fdot(A,B)`` and ``fdot(zip(A,B))`` are equivalent.
        The elements are automatically converted to mpmath numbers.

        With ``conjugate=True``, the elements in the second vector
        will be conjugated:

        .. math ::

            \sum_{k=0} A_k \overline{B_k}

        **Examples**

            >>> from mpmath import fdot, j
            >>> A = [2, 1.5, 3]
            >>> B = [1, -1, 2]
            >>> fdot(A, B)
            mpf('6.5')
            >>> list(zip(A, B))
            [(2, 1), (1.5, -1), (3, 2)]
            >>> fdot(_)
            mpf('6.5')
            >>> A = [2, 1.5, 3j]
            >>> B = [1+j, 3, -1-j]
            >>> fdot(A, B)
            mpc(real='9.5', imag='-1.0')
            >>> fdot(A, B, conjugate=True)
            mpc(real='3.5', imag='-5.0')

        """
        if B is not None:
            A = zip(A, B)
        prec, rnd = ctx._prec_rounding
        real = []
        imag = []
        hasattr_ = hasattr
        types = (ctx.mpf, ctx.mpc)
        for a, b in A:
            if type(a) not in types: a = ctx.convert(a)
            if type(b) not in types: b = ctx.convert(b)
            a_real = hasattr_(a, "_mpf_")
            b_real = hasattr_(b, "_mpf_")
            if a_real and b_real:
                real.append(mpf_mul(a._mpf_, b._mpf_))
                continue
            a_complex = hasattr_(a, "_mpc_")
            b_complex = hasattr_(b, "_mpc_")
            if a_real and b_complex:
                aval = a._mpf_
                bre, bim = b._mpc_
                if conjugate:
                    bim = mpf_neg(bim)
                real.append(mpf_mul(aval, bre))
                imag.append(mpf_mul(aval, bim))
            elif b_real and a_complex:
                are, aim = a._mpc_
                bval = b._mpf_
                real.append(mpf_mul(are, bval))
                imag.append(mpf_mul(aim, bval))
            elif a_complex and b_complex:
                #re, im = mpc_mul(a._mpc_, b._mpc_, prec+20)
                are, aim = a._mpc_
                bre, bim = b._mpc_
                if conjugate:
                    bim = mpf_neg(bim)
                real.append(mpf_mul(are, bre))
                real.append(mpf_neg(mpf_mul(aim, bim)))
                imag.append(mpf_mul(are, bim))
                imag.append(mpf_mul(aim, bre))
            else:
                raise NotImplementedError
        s = mpf_sum(real, prec, rnd)
        if imag:
            s = ctx.make_mpc((s, mpf_sum(imag, prec, rnd)))
        else:
            s = ctx.make_mpf(s)
        return s

    def _wrap_libmp_function(ctx, mpf_f, mpc_f=None, mpi_f=None, doc="<no doc>"):
        """
        Given a low-level mpf_ function, and optionally similar functions
        for mpc_ and mpi_, defines the function as a context method.

        It is assumed that the return type is the same as that of
        the input; the exception is that propagation from mpf to mpc is possible
        by raising ComplexResult.

        """
        def f(x, **kwargs):
            if type(x) not in ctx.types:
                x = ctx.convert(x)
            prec, rounding = ctx._prec_rounding
            if kwargs:
                prec = kwargs.get('prec', prec)
                if 'dps' in kwargs:
                    prec = dps_to_prec(kwargs['dps'])
                rounding = kwargs.get('rounding', rounding)
            if hasattr(x, '_mpf_'):
                try:
                    return ctx.make_mpf(mpf_f(x._mpf_, prec, rounding))
                except ComplexResult:
                    # Handle propagation to complex
                    if ctx.trap_complex:
                        raise
                    return ctx.make_mpc(mpc_f((x._mpf_, fzero), prec, rounding))
            elif hasattr(x, '_mpc_'):
                return ctx.make_mpc(mpc_f(x._mpc_, prec, rounding))
            raise NotImplementedError("%s of a %s" % (name, type(x)))
        name = mpf_f.__name__[4:]
        f.__doc__ = function_docs.__dict__.get(name, "Computes the %s of x" % doc)
        f.__name__ = name
        return f

    # Called by SpecialFunctions.__init__()
    @classmethod
    def _wrap_specfun(cls, name, f, wrap):
        if wrap:
            def f_wrapped(ctx, *args, **kwargs):
                convert = ctx.convert
                args = [convert(a) for a in args]
                prec = ctx.prec
                try:
                    ctx.prec += 10
                    retval = f(ctx, *args, **kwargs)
                finally:
                    ctx.prec = prec
                return +retval
        else:
            f_wrapped = f
        f_wrapped.__doc__ = function_docs.__dict__.get(name, f.__doc__)
        f_wrapped.__signature__ = inspect.signature(f)
        f_wrapped.__name__ = f.__name__
        setattr(cls, name, f_wrapped)

    def _convert_param(ctx, x):
        if hasattr(x, "_mpc_"):
            v, im = x._mpc_
            if im != fzero:
                return x, 'C'
        elif hasattr(x, "_mpf_"):
            v = x._mpf_
        else:
            if type(x) in int_types:
                return int(x), 'Z'
            p = None
            if isinstance(x, tuple):
                p, q = x
            elif isinstance(x, str) and '/' in x:
                p, q = x.split('/')
                p = int(p)
                q = int(q)
            if p is not None:
                if not p % q:
                    return p // q, 'Z'
                return MPQ(p,q), 'Q'
            x = ctx.convert(x)
            if hasattr(x, "_mpc_"):
                v, im = x._mpc_
                if im != fzero:
                    return x, 'C'
            elif hasattr(x, "_mpf_"):
                v = x._mpf_
            else:
                raise NotImplementedError
        man, exp = to_man_exp(v, signed=True)
        if man:
            if exp >= -4:
                if exp >= 0:
                    return int(man) << exp, 'Z'
                p, q = int(man), (1<<(-exp))
                return MPQ(p,q), 'Q'
            x = ctx.make_mpf(v)
            return x, 'R'
        if not exp:
            return 0, 'Z'
        raise NotImplementedError

    def _mpf_mag(ctx, x):
        if x == fzero:
            return ctx.ninf
        if x in (finf, fninf, fnan):
            return ctx.make_mpf(mpf_abs(x))
        man, exp = to_man_exp(x, signed=True)
        return exp+man.bit_length()

    def mag(ctx, x):
        """
        Quick logarithmic magnitude estimate of a number. Returns an
        integer or infinity `m` such that `|x| <= 2^m`. It is not
        guaranteed that `m` is an optimal bound, but it will never
        be too large by more than 2 (and probably not more than 1).

        **Examples**

            >>> from mpmath import mp, mag, ceil, mpf, log, inf, nan
            >>> mp.pretty = True
            >>> mag(10), mag(10.0), mag(mpf(10)), int(ceil(log(10,2)))
            (4, 4, 4, 4)
            >>> mag(10j), mag(10+10j)
            (4, 5)
            >>> mag(0.01), int(ceil(log(0.01,2)))
            (-6, -6)
            >>> mag(0), mag(inf), mag(-inf), mag(nan)
            (-inf, inf, inf, nan)

        """
        if hasattr(x, "_mpf_"):
            return ctx._mpf_mag(x._mpf_)
        if hasattr(x, "_mpc_"):
            r, i = x._mpc_
            if r == fzero:
                return ctx._mpf_mag(i)
            if i == fzero:
                return ctx._mpf_mag(r)
            return 1+max(ctx._mpf_mag(r), ctx._mpf_mag(i))
        if isinstance(x, int_types):
            if x:
                return x.bit_length()
            return ctx.ninf
        if isinstance(x, MPQ):
            p, q = x.numerator, x.denominator
            if p:
                return 1 + p.bit_length() - q.bit_length()
            return ctx.ninf
        x = ctx.convert(x)
        return ctx.mag(x)


# Register with "numbers" ABC
#   We do not subclass, hence we do not use the @abstractmethod checks. While
#   this is less invasive it may turn out that we do not actually support
#   parts of the expected interfaces.  See
#   https://docs.python.org/3/library/numbers.html for list of abstract methods.
numbers.Complex.register(_mpc)
numbers.Real.register(_mpf)
