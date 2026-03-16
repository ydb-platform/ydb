import cmath
import functools
import inspect
import math
import warnings
import sys

from . import function_docs, libfp, libmp
from .ctx_base import StandardBaseContext
from .libmp import int_types, mpf_bernoulli, to_float


class FPContext(StandardBaseContext):
    """
    Context for fast low-precision arithmetic (usually, 53-bit precision,
    giving at most about 15 decimal digits), using Python's builtin float and
    complex types.
    """

    def __init__(ctx):
        super().__init__()
        ctx.pretty = False
        ctx._init_aliases()

    NoConvergence = libmp.NoConvergence

    @property
    def prec(ctx):
        return sys.float_info.mant_dig

    @prec.setter
    def prec(ctx, p):
        return

    @property
    def dps(ctx):
        return sys.float_info.dig

    @dps.setter
    def dps(ctx, p):
        return

    _fixed_precision = True

    zero = 0.0
    one = 1.0
    eps = sys.float_info.epsilon
    inf = libfp.INF
    ninf = -math.inf
    nan = math.nan
    j = 1j

    # Called by SpecialFunctions.__init__()
    @classmethod
    def _wrap_specfun(cls, name, f, wrap):
        if wrap:
            def f_wrapped(ctx, *args, **kwargs):
                convert = ctx.convert
                args = [convert(a) for a in args]
                return f(ctx, *args, **kwargs)
        else:
            f_wrapped = f
        f_wrapped.__doc__ = function_docs.__dict__.get(name, f.__doc__)
        try:
            f_wrapped.__signature__ = inspect.signature(f)
        except ValueError:  # pragma: no cover
            pass
        f_wrapped.__name__ = f.__name__
        setattr(cls, name, f_wrapped)

    @functools.lru_cache
    def bernoulli(ctx, n, plus=False):
        return to_float(mpf_bernoulli(n, ctx.prec, 'n', plus=plus), strict=True)

    pi = libfp.pi
    e = math.e
    euler = libfp.euler
    sqrt2 = 1.4142135623730950488
    sqrt5 = 2.2360679774997896964
    phi = 1.6180339887498948482
    ln2 = 0.69314718055994530942
    ln10 = 2.302585092994045684
    euler = libfp.euler
    catalan = 0.91596559417721901505
    khinchin = 2.6854520010653064453
    apery = 1.2020569031595942854
    glaisher = 1.2824271291006226369

    absmin = absmax = abs

    def is_special(ctx, x):
        warnings.warn("the is_special() method is deprecated",
                      DeprecationWarning)
        return not ctx.isnormal(x)

    def isnan(ctx, x):
        return x != x

    def isinf(ctx, x):
        return abs(x) == libfp.INF

    def isfinite(ctx, x):
        if type(x) is complex:
            return all(map(math.isfinite, [x.real, x.imag]))
        return math.isfinite(x)

    def isnormal(ctx, x):
        if type(x) is complex:
            return ctx.isnormal(abs(x))
        # XXX: can use math.isnormal() on Python 3.15+
        return bool(x) and math.isfinite(x) and abs(x) >= sys.float_info.min

    def isnpint(ctx, x):
        if type(x) is complex:
            if x.imag:
                return False
            x = x.real
        return math.isfinite(x) and x <= 0.0 and round(x) == x

    mpf = float
    mpc = complex

    def convert(ctx, x):
        try:
            return float(x)
        except:
            return complex(x)

    power = staticmethod(libfp.pow)
    sqrt = staticmethod(libfp.sqrt)
    exp = staticmethod(libfp.exp)
    ln = log = staticmethod(libfp.log)
    cos = staticmethod(libfp.cos)
    sin = staticmethod(libfp.sin)
    tan = staticmethod(libfp.tan)
    cos_sin = staticmethod(libfp.cos_sin)
    acos = staticmethod(libfp.acos)
    asin = staticmethod(libfp.asin)
    atan = staticmethod(libfp.atan)
    cosh = staticmethod(libfp.cosh)
    sinh = staticmethod(libfp.sinh)
    tanh = staticmethod(libfp.tanh)
    acosh = staticmethod(libfp.acosh)
    asinh = staticmethod(libfp.asinh)
    atanh = staticmethod(libfp.atanh)
    gamma = staticmethod(libfp.gamma)
    rgamma = staticmethod(libfp.rgamma)
    fac = factorial = staticmethod(libfp.factorial)
    floor = staticmethod(libfp.floor)
    ceil = staticmethod(libfp.ceil)
    cospi = staticmethod(libfp.cospi)
    sinpi = staticmethod(libfp.sinpi)
    cbrt = staticmethod(libfp.cbrt)
    _nthroot = staticmethod(libfp.nthroot)
    _ei = staticmethod(libfp.ei)
    _e1 = staticmethod(libfp.e1)
    _zeta = _zeta_int = staticmethod(libfp.zeta)
    arg = staticmethod(cmath.phase)
    loggamma = staticmethod(libfp.loggamma)

    def expj(ctx, x):
        return ctx.exp(ctx.j*x)

    def expjpi(ctx, x):
        return ctx.exp(ctx.j*ctx.pi*x)

    ldexp = math.ldexp
    frexp = math.frexp
    hypot = math.hypot

    def mag(ctx, z):
        if z:
            n, e = ctx.frexp(abs(z))
            if e:
                return e
            return ctx.convert(n)
        return ctx.ninf

    def isint(ctx, z):
        if z.imag:
            return False
        z = z.real
        try:
            return z == int(z)
        except:
            return False

    def nint_distance(ctx, z):
        n = round(z.real)
        if n == z:
            return n, ctx.ninf
        return n, ctx.mag(abs(z-n))

    def _convert_param(ctx, z):
        if type(z) is tuple:
            p, q = z
            return ctx.mpf(p) / q, 'R'
        intz = int(z.real)
        if z == intz:
            return intz, 'Z'
        return z, 'R'

    def _is_real_type(ctx, z):
        return isinstance(z, float) or isinstance(z, int_types)

    def _is_complex_type(ctx, z):
        return isinstance(z, complex)

    def hypsum(ctx, p, q, flags, coeffs, z, maxterms=6000, **kwargs):
        for i, c in enumerate(coeffs[p:], start=p):
            if flags[i] == 'Z':
                if c <= 0:
                    ok = False
                    for ii, cc in enumerate(coeffs[:p]):
                        # Note: c <= cc or c < cc, depending on convention
                        if flags[ii] == 'Z' and cc <= 0 and c <= cc:
                            ok = True
                    if not ok:
                        raise ZeroDivisionError("pole in hypergeometric series")
        num = range(p)
        den = range(p,p+q)
        if ctx.isinf(z):
            n = max(((n, c) for n, c in enumerate(coeffs[:p])
                     if flags[n] == 'Z' and c < 0), default=(-1, 0),
                    key=lambda x: x[1])[0]
            if n >= 0:
                n = -coeffs[n]
                t = z**n
                for k in range(n):
                    for i in num: t *= (coeffs[i]+k)
                    for i in den: t /= (coeffs[i]+k)
                    t /= (k+1)
                return t
        tol = ctx.eps
        s = t = 1.0
        k = 0
        while 1:
            for i in num: t *= (coeffs[i]+k)
            try:
                for i in den: t /= (coeffs[i]+k)
            except ZeroDivisionError:
                raise NotImplementedError
            k += 1; t /= k; t *= z; s += t
            if abs(t) < tol:
                return s
            if k > maxterms:
                raise ctx.NoConvergence

    atan2 = staticmethod(math.atan2)

    def psi(ctx, m, z):
        m = int(m)
        if m == 0:
            return ctx.digamma(z)
        return (-1)**(m+1) * ctx.fac(m) * ctx.zeta(m+1, z)

    digamma = staticmethod(libfp.digamma)

    def harmonic(ctx, x):
        x = ctx.convert(x)
        if x == 0 or x == 1:
            return x
        return ctx.digamma(x+1) + ctx.euler

    nstr = str

    def to_fixed(ctx, x, prec):
        return int(math.ldexp(x, prec))

    def rand(ctx):
        import random
        return random.random()

    _erf = staticmethod(math.erf)
    _erfc = staticmethod(math.erfc)

    def sum_accurately(ctx, terms, check_step=1):
        s = ctx.zero
        k = 0
        for term in terms():
            s += term
            if (not k % check_step) and term:
                if abs(term) <= 1e-18*abs(s):
                    break
            k += 1
        return s
