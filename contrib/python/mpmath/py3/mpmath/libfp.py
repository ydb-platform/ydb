"""
This module complements the math and cmath builtin modules by providing
fast machine precision versions of some additional functions (ei, e1, ...)
and wrapping math/cmath functions so that they can be called with either
real or complex arguments.
"""

import operator
import math
import cmath

# Irrational (?) constants
pi = math.pi
euler = 0.57721566490153286061

logpi = 1.1447298858494001741

def _mathfun_real(f_real, f_complex):
    def f(x, **kwargs):
        try:
            x = float(x)
            return f_real(x)
        except (TypeError, ValueError):
            x = complex(x)
            return f_complex(x)
    f.__name__ = f_real.__name__
    return f

def _mathfun(f_real, f_complex):
    def f(x, **kwargs):
        if type(x) is complex:
            return f_complex(x)
        try:
            return f_real(float(x))
        except (TypeError, ValueError):
            return f_complex(complex(x))
    f.__name__ = f_real.__name__
    return f

def _mathfun_n(f_real, f_complex):
    def f(*args, **kwargs):
        try:
            return f_real(*(float(x) for x in args))
        except (TypeError, ValueError):
            return f_complex(*(complex(x) for x in args))
    f.__name__ = f_real.__name__
    return f


def _tan_complex(z):
    if math.isinf(z.real):
        if z.imag == 0:
            return cmath.nan + 0j
        elif math.isfinite(z.imag):
            return cmath.nan + cmath.nanj
    return cmath.tan(z)

def _tanh_complex(z):
    if math.isinf(z.imag):
        if z.real == 0:
            return cmath.nanj
        elif math.isfinite(z.real):
            return cmath.nan + cmath.nanj
    return cmath.tanh(z)


pow = _mathfun_n(operator.pow, lambda x, y: complex(x)**y)
log = _mathfun_n(math.log, cmath.log)
sqrt = _mathfun(math.sqrt, cmath.sqrt)
exp = _mathfun_real(math.exp, cmath.exp)

cos = _mathfun_real(math.cos, cmath.cos)
sin = _mathfun_real(math.sin, cmath.sin)
tan = _mathfun_real(math.tan, _tan_complex)

acos = _mathfun(math.acos, cmath.acos)
asin = _mathfun(math.asin, cmath.asin)
atan = _mathfun_real(math.atan, cmath.atan)

cosh = _mathfun_real(math.cosh, cmath.cosh)
sinh = _mathfun_real(math.sinh, cmath.sinh)
tanh = _mathfun_real(math.tanh, _tanh_complex)

acosh = _mathfun(math.acosh, cmath.acosh)
asinh = _mathfun(math.asinh, cmath.asinh)
atanh = _mathfun_real(math.atanh, cmath.atanh)

floor = _mathfun_real(math.floor,
    lambda z: complex(math.floor(z.real), math.floor(z.imag)))
ceil = _mathfun_real(math.ceil,
    lambda z: complex(math.ceil(z.real), math.ceil(z.imag)))


cos_sin = _mathfun_real(lambda x: (math.cos(x), math.sin(x)),
                        lambda z: (cmath.cos(z), cmath.sin(z)))

cbrt = _mathfun(lambda x: x**(1./3), lambda z: z**(1./3))

def nthroot(x, n):
    r = 1./n
    try:
        return float(x) ** r
    except (ValueError, TypeError):
        return complex(x) ** r

def _sinpi_real(x):
    if x < 0:
        return -_sinpi_real(-x)
    n, r = divmod(x, 0.5)
    r *= pi
    n %= 4
    if n == 0: return math.sin(r)
    if n == 1: return math.cos(r)
    if n == 2: return -math.sin(r)
    if n == 3: return -math.cos(r)

def _cospi_real(x):
    if x < 0:
        x = -x
    n, r = divmod(x, 0.5)
    r *= pi
    n %= 4
    if n == 0: return math.cos(r)
    if n == 1: return -math.sin(r)
    if n == 2: return -math.cos(r)
    if n == 3: return math.sin(r)

def _sinpi_complex(z):
    if z.real < 0:
        return -_sinpi_complex(-z)
    n, r = divmod(z.real, 0.5)
    z = pi*complex(r, z.imag)
    n %= 4
    if n == 0: return cmath.sin(z)
    if n == 1: return cmath.cos(z)
    if n == 2: return -cmath.sin(z)
    if n == 3: return -cmath.cos(z)

def _cospi_complex(z):
    if z.real < 0:
        z = -z
    n, r = divmod(z.real, 0.5)
    z = pi*complex(r, z.imag)
    n %= 4
    if n == 0: return cmath.cos(z)
    if n == 1: return -cmath.sin(z)
    if n == 2: return -cmath.cos(z)
    if n == 3: return cmath.sin(z)

cospi = _mathfun_real(_cospi_real, _cospi_complex)
sinpi = _mathfun_real(_sinpi_real, _sinpi_complex)

def cotpi(x):
    try:
        return cospi(x) / sinpi(x)
    except OverflowError:
        if complex(x).imag > 10:
            return -1j
        if complex(x).imag < 10:
            return 1j
        raise

INF = math.inf

_exact_gamma = (INF, 1.0, 1.0, 2.0, 6.0, 24.0, 120.0, 720.0, 5040.0, 40320.0,
  362880.0, 3628800.0, 39916800.0, 479001600.0, 6227020800.0, 87178291200.0,
  1307674368000.0, 20922789888000.0, 355687428096000.0, 6402373705728000.0,
  121645100408832000.0, 2432902008176640000.0)

_max_exact_gamma = len(_exact_gamma)-1

# Lanczos coefficients used by the GNU Scientific Library
_lanczos_g = 7
_lanczos_p = (0.99999999999980993, 676.5203681218851, -1259.1392167224028,
     771.32342877765313, -176.61502916214059, 12.507343278686905,
     -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7)

def _gamma_complex(x):
    if not x.imag:
        if x.real == -INF:
            return math.nan
        return complex(math.gamma(x.real))
    if x.real < 0.5:
        # TODO: sinpi
        return pi / (_sinpi_complex(x)*_gamma_complex(1-x))
    else:
        x -= 1.0
        r = _lanczos_p[0]
        for i in range(1, _lanczos_g+2):
            r += _lanczos_p[i]/(x+i)
        t = x + _lanczos_g + 0.5
        return 2.506628274631000502417 * t**(x+0.5) * cmath.exp(-t) * r

gamma = _mathfun_real(math.gamma, _gamma_complex)

def rgamma(x):
    try:
        return 1./gamma(x)
    except ZeroDivisionError:
        return x*0.0

def factorial(x):
    return gamma(x+1.0)

# XXX: broken for negatives
def loggamma(x):
    if type(x) not in (float, complex):
        try:
            x = float(x)
        except (ValueError, TypeError):
            x = complex(x)
    # Reflection formula
    # http://functions.wolfram.com/GammaBetaErf/LogGamma/16/01/01/0003/
    if x.real < 0.0:
        if abs(x) < 0.5:
            v = log(gamma(x))
            if x.imag == 0:
                v = v.conjugate()
            return v
        z = 1-x
        re = z.real
        im = z.imag
        refloor = floor(re)
        if im == 0.0:
            imsign = 0
        elif im < 0.0:
            imsign = -1
        else:
            imsign = 1
        return (-pi*1j)*abs(refloor)*(1-abs(imsign)) + logpi - \
            log(sinpi(z-refloor)) - loggamma(z) + 1j*pi*refloor*imsign
    if x == 1.0 or x == 2.0:
        return x*0
    p = 0.
    while abs(x) < 11:
        p -= log(x)
        x += 1.0
    s = 0.918938533204672742 + (x-0.5)*log(x) - x
    r = 1./x
    r2 = r*r
    s += 0.083333333333333333333*r; r *= r2
    s += -0.0027777777777777777778*r; r *= r2
    s += 0.00079365079365079365079*r; r *= r2
    s += -0.0005952380952380952381*r; r *= r2
    s += 0.00084175084175084175084*r; r *= r2
    s += -0.0019175269175269175269*r; r *= r2
    s += 0.0064102564102564102564*r; r *= r2
    s += -0.02955065359477124183*r
    return s + p

_psi_coeff = [
0.083333333333333333333,
-0.0083333333333333333333,
0.003968253968253968254,
-0.0041666666666666666667,
0.0075757575757575757576,
-0.021092796092796092796,
0.083333333333333333333,
-0.44325980392156862745,
3.0539543302701197438,
-26.456212121212121212]

def _digamma_real(x):
    _intx = int(x)
    if _intx == x:
        if _intx <= 0:
            raise ZeroDivisionError("polygamma pole")
    if x < 0.5:
        x = 1.0-x
        s = pi*cotpi(x)
    else:
        s = 0.0
    while x < 10.0:
        s -= 1.0/x
        x += 1.0
    x2 = x**-2
    t = x2
    for c in _psi_coeff:
        s -= c*t
        if t < 1e-20:
            break
        t *= x2
    return s + math.log(x) - 0.5/x

def _digamma_complex(x):
    if not x.imag:
        return complex(_digamma_real(x.real))
    if x.real < 0.5:
        x = 1.0-x
        s = pi*cotpi(x)
    else:
        s = 0.0
    while abs(x) < 10.0:
        s -= 1.0/x
        x += 1.0
    x2 = x**-2
    t = x2
    for c in _psi_coeff:
        s -= c*t
        if abs(t) < 1e-20:
            break
        t *= x2
    return s + cmath.log(x) - 0.5/x

digamma = _mathfun_real(_digamma_real, _digamma_complex)

def _polyval(coeffs, x):
    p = coeffs[0]
    for c in coeffs[1:]:
        p = c + x*p
    return p

gauss42 = [
(0.99839961899006235, 0.0041059986046490839),
(-0.99839961899006235, 0.0041059986046490839),
(0.9915772883408609, 0.009536220301748501),
(-0.9915772883408609,0.009536220301748501),
(0.97934250806374812, 0.014922443697357493),
(-0.97934250806374812, 0.014922443697357493),
(0.96175936533820439,0.020227869569052644),
(-0.96175936533820439, 0.020227869569052644),
(0.93892355735498811, 0.025422959526113047),
(-0.93892355735498811,0.025422959526113047),
(0.91095972490412735, 0.030479240699603467),
(-0.91095972490412735, 0.030479240699603467),
(0.87802056981217269,0.03536907109759211),
(-0.87802056981217269, 0.03536907109759211),
(0.8402859832618168, 0.040065735180692258),
(-0.8402859832618168,0.040065735180692258),
(0.7979620532554873, 0.044543577771965874),
(-0.7979620532554873, 0.044543577771965874),
(0.75127993568948048,0.048778140792803244),
(-0.75127993568948048, 0.048778140792803244),
(0.70049459055617114, 0.052746295699174064),
(-0.70049459055617114,0.052746295699174064),
(0.64588338886924779, 0.056426369358018376),
(-0.64588338886924779, 0.056426369358018376),
(0.58774459748510932, 0.059798262227586649),
(-0.58774459748510932, 0.059798262227586649),
(0.5263957499311922, 0.062843558045002565),
(-0.5263957499311922, 0.062843558045002565),
(0.46217191207042191, 0.065545624364908975),
(-0.46217191207042191, 0.065545624364908975),
(0.39542385204297503, 0.067889703376521934),
(-0.39542385204297503, 0.067889703376521934),
(0.32651612446541151, 0.069862992492594159),
(-0.32651612446541151, 0.069862992492594159),
(0.25582507934287907, 0.071454714265170971),
(-0.25582507934287907, 0.071454714265170971),
(0.18373680656485453, 0.072656175243804091),
(-0.18373680656485453, 0.072656175243804091),
(0.11064502720851986, 0.073460813453467527),
(-0.11064502720851986, 0.073460813453467527),
(0.036948943165351772, 0.073864234232172879),
(-0.036948943165351772, 0.073864234232172879)]

EI_ASYMP_CONVERGENCE_RADIUS = 40.0

def ei_asymp(z, _e1=False):
    r = 1./z
    k = 1
    t = 1.0*k*r
    s = 1.0 + t
    while abs(t) >= 1e-16:
        k += 1
        t *= k*r
        s += t
    v = s*exp(z)/z
    if _e1:
        if type(z) is complex:
            zreal = z.real
            zimag = z.imag
        else:
            zreal = z
            zimag = 0.0
        if zimag == 0.0 and zreal > 0.0:
            v += pi*1j
    else:
        if type(z) is complex:
            if z.imag > 0:
                v += pi*1j
            if z.imag < 0:
                v -= pi*1j
    return v

def ei_taylor(z, _e1=False):
    s = t = z
    k = 2
    while 1:
        t = t*z/k
        term = t/k
        if abs(term) < 1e-17:
            break
        s += term
        k += 1
    s += euler
    if _e1:
        s += log(-z)
    else:
        if type(z) is float or z.imag == 0.0:
            s += math.log(abs(z))
        else:
            s += cmath.log(z)
    return s

def ei(z, _e1=False):
    try:
        z = float(z)
        typez = float
    except (TypeError, ValueError):
        z = complex(z)
        typez = complex
    if not z:
        return -INF
    absz = abs(z)
    if absz > EI_ASYMP_CONVERGENCE_RADIUS:
        return ei_asymp(z, _e1)
    elif absz <= 2.0 or (typez is float and z > 0.0):
        return ei_taylor(z, _e1)
    # Integrate, starting from whichever is smaller of a Taylor
    # series value or an asymptotic series value
    if typez is complex and z.real > 0.0:
        zref = z / absz
        ref = ei_taylor(zref, _e1)
    else:
        zref = EI_ASYMP_CONVERGENCE_RADIUS * z / absz
        ref = ei_asymp(zref, _e1)
    C = (zref-z)*0.5
    D = (zref+z)*0.5
    s = 0.0
    if type(z) is complex:
        _exp = cmath.exp
    else:
        _exp = math.exp
    for x,w in gauss42:
        t = C*x+D
        s += w*_exp(t)/t
    ref -= C*s
    return ref

def e1(z):
    try:
        z = float(z)
        typez = float
    except (TypeError, ValueError):
        z = complex(z)
        typez = complex
    # hack to get consistent signs if the imaginary part if 0
    # and signed
    if typez is complex and not z.imag:
        z = complex(z.real, 0.0)
    # end hack
    return -ei(-z, _e1=True)

_zeta_int = [
-0.5,
0.0,
1.6449340668482264365,1.2020569031595942854,1.0823232337111381915,
1.0369277551433699263,1.0173430619844491397,1.0083492773819228268,
1.0040773561979443394,1.0020083928260822144,1.0009945751278180853,
1.0004941886041194646,1.0002460865533080483,1.0001227133475784891,
1.0000612481350587048,1.0000305882363070205,1.0000152822594086519,
1.0000076371976378998,1.0000038172932649998,1.0000019082127165539,
1.0000009539620338728,1.0000004769329867878,1.0000002384505027277,
1.0000001192199259653,1.0000000596081890513,1.0000000298035035147,
1.0000000149015548284]

_zeta_P = [-3.50000000087575873, -0.701274355654678147,
-0.0672313458590012612, -0.00398731457954257841,
-0.000160948723019303141, -4.67633010038383371e-6,
-1.02078104417700585e-7, -1.68030037095896287e-9,
-1.85231868742346722e-11][::-1]

_zeta_Q = [1.00000000000000000, -0.936552848762465319,
-0.0588835413263763741, -0.00441498861482948666,
-0.000143416758067432622, -5.10691659585090782e-6,
-9.58813053268913799e-8, -1.72963791443181972e-9,
-1.83527919681474132e-11][::-1]

_zeta_1 = [3.03768838606128127e-10, -1.21924525236601262e-8,
2.01201845887608893e-7, -1.53917240683468381e-6,
-5.09890411005967954e-7, 0.000122464707271619326,
-0.000905721539353130232, -0.00239315326074843037,
0.084239750013159168, 0.418938517907442414, 0.500000001921884009]

_zeta_0 = [-3.46092485016748794e-10, -6.42610089468292485e-9,
1.76409071536679773e-7, -1.47141263991560698e-6, -6.38880222546167613e-7,
0.000122641099800668209, -0.000905894913516772796, -0.00239303348507992713,
0.0842396947501199816, 0.418938533204660256, 0.500000000000000052]

def zeta(s):
    """
    Riemann zeta function, real argument
    """
    if not isinstance(s, (float, int)):
        try:
            s = float(s)
        except (ValueError, TypeError):
            try:
                s = complex(s)
                if not s.imag:
                    return complex(zeta(s.real))
            except (ValueError, TypeError):
                pass
            raise NotImplementedError
    if s == 1:
        raise ValueError("zeta(1) pole")
    if s >= 27:
        return 1.0 + 2.0**(-s) + 3.0**(-s)
    n = int(s)
    if n == s:
        if n >= 0:
            return _zeta_int[n]
        if not (n % 2):
            return 0.0
    if s <= 0.0:
        return 2.**s*pi**(s-1)*_sinpi_real(0.5*s)*math.gamma(1-s)*zeta(1-s)
    if s <= 2.0:
        if s <= 1.0:
            return _polyval(_zeta_0,s)/(s-1)
        return _polyval(_zeta_1,s)/(s-1)
    z = _polyval(_zeta_P,s) / _polyval(_zeta_Q,s)
    return 1.0 + 2.0**(-s) + 3.0**(-s) + 4.0**(-s)*z
