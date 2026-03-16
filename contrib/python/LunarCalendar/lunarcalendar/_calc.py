# -*- coding: utf-8 -*-
# from __future__ import unicode_literals if import this, CTypes in ephem will error
# rewrite from https://github.com/kiyota-yoji/EastAsiaCalendars
import ephem
from math import pi, floor, fabs, sin, cos
import pytz


class Data:

    NUT_SCALE = 1e4
    NUT_SERIES = 106
    NUT_MAXMUL = 4
    SECPERCIRC = (3600. * 360.)

    '''Delaunay arguments, in arc seconds; they differ slightly from ELP82B'''
    delaunay = [
        [485866.733,  1717915922.633, 31.310,  0.064],  # M', moon mean anom
        [1287099.804, 129596581.224,  -0.577, -0.012],  # M, sun mean anom
        [335778.877,  1739527263.137, -13.257, 0.011],  # F, moon arg lat
        [1072261.307, 1602961601.328, -6.891,  0.019],  # D, elong moon sun
        [450160.280,  -6962890.539,   7.455,   0.008],  # Om, moon l asc node
    ]

    '''multipliers for Delaunay arguments'''
    # bounds:  -2..3, -2..2, -2/0/2/4, -4..4, 0..2
    multarg = [
        [0, 0, 0, 0, 1],
        [0, 0, 0, 0, 2],
        [-2, 0, 2, 0, 1],
        [2, 0, -2, 0, 0],
        [-2, 0, 2, 0, 2],
        [1, -1, 0, -1, 0],
        [0, -2, 2, -2, 1],
        [2, 0, -2, 0, 1],
        [0, 0, 2, -2, 2],
        [0, 1, 0, 0, 0],
        [0, 1, 2, -2, 2],
        [0, -1, 2, -2, 2],
        [0, 0, 2, -2, 1],
        [2, 0, 0, -2, 0],
        [0, 0, 2, -2, 0],
        [0, 2, 0, 0, 0],
        [0, 1, 0, 0, 1],
        [0, 2, 2, -2, 2],
        [0, -1, 0, 0, 1],
        [-2, 0, 0, 2, 1],
        [0, -1, 2, -2, 1],
        [2, 0, 0, -2, 1],
        [0, 1, 2, -2, 1],
        [1, 0, 0, -1, 0],
        [2, 1, 0, -2, 0],
        [0, 0, -2, 2, 1],
        [0, 1, -2, 2, 0],
        [0, 1, 0, 0, 2],
        [-1, 0, 0, 1, 1],
        [0, 1, 2, -2, 0],
        [0, 0, 2, 0, 2],
        [1, 0, 0, 0, 0],
        [0, 0, 2, 0, 1],
        [1, 0, 2, 0, 2],
        [1, 0, 0, -2, 0],
        [-1, 0, 2, 0, 2],
        [0, 0, 0, 2, 0],
        [1, 0, 0, 0, 1],
        [-1, 0, 0, 0, 1],
        [-1, 0, 2, 2, 2],
        [1, 0, 2, 0, 1],
        [0, 0, 2, 2, 2],
        [2, 0, 0, 0, 0],
        [1, 0, 2, -2, 2],
        [2, 0, 2, 0, 2],
        [0, 0, 2, 0, 0],
        [-1, 0, 2, 0, 1],
        [-1, 0, 0, 2, 1],
        [1, 0, 0, -2, 1],
        [-1, 0, 2, 2, 1],
        [1, 1, 0, -2, 0],
        [0, 1, 2, 0, 2],
        [0, -1, 2, 0, 2],
        [1, 0, 2, 2, 2],
        [1, 0, 0, 2, 0],
        [2, 0, 2, -2, 2],
        [0, 0, 0, 2, 1],
        [0, 0, 2, 2, 1],
        [1, 0, 2, -2, 1],
        [0, 0, 0, -2, 1],
        [1, -1, 0, 0, 0],
        [2, 0, 2, 0, 1],
        [0, 1, 0, -2, 0],
        [1, 0, -2, 0, 0],
        [0, 0, 0, 1, 0],
        [1, 1, 0, 0, 0],
        [1, 0, 2, 0, 0],
        [1, -1, 2, 0, 2],
        [-1, -1, 2, 2, 2],
        [-2, 0, 0, 0, 1],
        [3, 0, 2, 0, 2],
        [0, -1, 2, 2, 2],
        [1, 1, 2, 0, 2],
        [-1, 0, 2, -2, 1],
        [2, 0, 0, 0, 1],
        [1, 0, 0, 0, 2],
        [3, 0, 0, 0, 0],
        [0, 0, 2, 1, 2],
        [-1, 0, 0, 0, 2],
        [1, 0, 0, -4, 0],
        [-2, 0, 2, 2, 2],
        [-1, 0, 2, 4, 2],
        [2, 0, 0, -4, 0],
        [1, 1, 2, -2, 2],
        [1, 0, 2, 2, 1],
        [-2, 0, 2, 4, 2],
        [-1, 0, 4, 0, 2],
        [1, -1, 0, -2, 0],
        [2, 0, 2, -2, 1],
        [2, 0, 2, 2, 2],
        [1, 0, 0, 2, 1],
        [0, 0, 4, -2, 2],
        [3, 0, 2, -2, 2],
        [1, 0, 2, -2, 0],
        [0, 1, 2, 0, 1],
        [-1, -1, 0, 2, 1],
        [0, 0, -2, 0, 1],
        [0, 0, 2, -1, 2],
        [0, 1, 0, 2, 0],
        [1, 0, -2, -2, 0],
        [0, -1, 2, 0, 1],
        [1, 1, 0, -2, 1],
        [1, 0, -2, 2, 0],
        [2, 0, 0, 2, 0],
        [0, 0, 2, 4, 2],
        [0, 1, 0, 1, 0]
    ]

    '''amplitudes which  have secular terms; in 1/NUT_SCALE arc seconds
    {index, constant dPSI, T/10 in dPSI, constant in dEPS, T/10 in dEPS}'''
    ampsecul = [
        [0  ,-171996 ,-1742 ,92025 ,89],
        [1  ,2062    ,2     ,-895  ,5],
        [8  ,-13187  ,-16   ,5736  ,-31],
        [9  ,1426    ,-34   ,54    ,-1],
        [10 ,-517    ,12    ,224   ,-6],
        [11 ,217     ,-5    ,-95   ,3],
        [12 ,129     ,1     ,-70   ,0],
        [15 ,17      ,-1    ,0     ,0],
        [17 ,-16     ,1     ,7     ,0],
        [30 ,-2274   ,-2    ,977   ,-5],
        [31 ,712     ,1     ,-7    ,0],
        [32 ,-386    ,-4    ,200   ,0],
        [33 ,-301    ,0     ,129   ,-1],
        [37 ,63      ,1     ,-33   ,0],
        [38 ,-58     ,-1    ,32    ,0],
        # termination
        [-1, ]
    ]

    '''amplitudes which only have constant terms; same unit as above
    {dPSI, dEPS}
    indexes which are already in ampsecul[][] are zeroed'''
    ampconst = [
        [0,0],
        [0,0],
        [46,-24],
        [11,0],
        [-3,1],
        [-3,0],
        [-2,1],
        [1,0],
        [0,0],
        [0,0],
        [0,0],
        [0,0],
        [0,0],
        [48,1],
        [-22,0],
        [0,0],
        [-15,9],
        [0,0],
        [-12,6],
        [-6,3],
        [-5,3],
        [4,-2],
        [4,-2],
        [-4,0],
        [1,0],
        [1,0],
        [-1,0],
        [1,0],
        [1,0],
        [-1,0],
        [0,0],
        [0,0],
        [0,0],
        [0,0],
        [-158,-1],
        [123,-53],
        [63,-2],
        [0,0],
        [0,0],
        [-59,26],
        [-51,27],
        [-38,16],
        [29,-1],
        [29,-12],
        [-31,13],
        [26,-1],
        [21,-10],
        [16,-8],
        [-13,7],
        [-10,5],
        [-7,0],
        [7,-3],
        [-7,3],
        [-8,3],
        [6,0],
        [6,-3],
        [-6,3],
        [-7,3],
        [6,-3],
        [-5,3],
        [5,0],
        [-5,3],
        [-4,0],
        [4,0],
        [-4,0],
        [-3,0],
        [3,0],
        [-3,1],
        [-3,1],
        [-2,1],
        [-3,1],
        [-3,1],
        [2,-1],
        [-2,1],
        [2,-1],
        [-2,1],
        [2,0],
        [2,-1],
        [1,-1],
        [-1,0],
        [1,-1],
        [-2,1],
        [-1,0],
        [1,-1],
        [-1,1],
        [-1,1],
        [1,0],
        [1,0],
        [1,-1],
        [-1,0],
        [-1,0],
        [1,0],
        [1,0],
        [-1,0],
        [1,0],
        [1,0],
        [-1,0],
        [-1,0],
        [-1,0],
        [-1,0],
        [-1,0],
        [-1,0],
        [-1,0],
        [1,0],
        [-1,0],
        [1,0]
    ]


def degrad(x):
    return x * pi / 180.


def nutation(mj):
    '''
    static double lastmj = -10000, lastdeps, lastdpsi;
    double T, T2, T3, T10;			/* jul cent since J2000 */
    double prec;				/* series precis in arc sec */
    int i, isecul;				/* index in term table */
    '''
    delcache = []
    for i in range(5):
        delcache.append([None] * (2 * Data.NUT_MAXMUL + 1))

    # lastmj = None
    # if mj == lastmj:
    #     deps = lastdeps
    #     dpsi = lastdpsi
    #     return (deps, dpsi)

    prec = 0.0
    # augment for abundance of small terms
    prec *= Data.NUT_SCALE / 10

    T = (mj - ephem.J2000) / 36525.
    T2 = T * T
    T3 = T2 * T
    T10 = T/10.
    # calculate delaunay args and place in cache
    for i in range(5):
        x = (Data.delaunay[i][0] +
             Data.delaunay[i][1] * T +
             Data.delaunay[i][2] * T2 +
             Data.delaunay[i][3] * T3)
        # convert to radians
        x /= Data.SECPERCIRC
        x -= floor(x)
        x *= 2. * pi
        # fill cache table
        for j in range(2 * Data.NUT_MAXMUL + 1):
            delcache[i][j] = (j - Data.NUT_MAXMUL) * x
    # find dpsi and deps
    lastdpsi = lastdeps = 0.

    isecul = 0
    for i in range(Data.NUT_SERIES):
        # double arg = 0., ampsin, ampcos;
        arg = 0.
        if (Data.ampconst[i][0] or Data.ampconst[i][1]):
            # take non-secular terms from simple array
            ampsin = Data.ampconst[i][0]
            ampcos = Data.ampconst[i][1]
        else:
            # secular terms from different array
            ampsin = float(Data.ampsecul[isecul][1]) + float(Data.ampsecul[isecul][2]) * T10
            ampcos = float(Data.ampsecul[isecul][3]) + float(Data.ampsecul[isecul][4]) * T10
            isecul += 1

        for j in range(5):
            arg += delcache[j][Data.NUT_MAXMUL + Data.multarg[i][j]]

        if fabs(ampsin) >= prec:
            lastdpsi += ampsin * sin(arg)

        if fabs(ampcos) >= prec:
            lastdeps += ampcos * cos(arg)
    # convert to radians.
    lastdpsi = degrad(lastdpsi/3600./Data.NUT_SCALE)
    lastdeps = degrad(lastdeps/3600./Data.NUT_SCALE)

    # lastmj = mj
    return (lastdeps, lastdpsi)


##########################################


ABERR_CONST = (20.49552 / 3600. / 180. * pi)

twopi = pi * 2.
twelfth_pi = pi / 12.
twenty_fourth_pi = pi / 24.

degree = pi / 180.
arcminute = degree / 60.
arcsecond = arcminute / 60.
half_arcsecond = arcsecond / 2.
tiny = arcsecond / 360.

_sun = ephem.Sun()    # for computing equinoxes


def get_ap_hlon(mj, nutation_dpsi=None):
    _sun.compute(mj)
    if nutation_dpsi is None:
        nutation_dpsi = nutation(mj)[1]
    ap_hlon = _sun.hlon + nutation_dpsi - ABERR_CONST + pi
    if ap_hlon < 0.0:
        ap_hlon += twopi
    elif ap_hlon > twopi:
        ap_hlon -= twopi
    return ephem.degrees(ap_hlon)


def converge(d, deg):

    def get_diff(d, deg, nutation_dpsi=None):
        diff = float(deg) * degree - get_ap_hlon(d, nutation_dpsi)
        if diff > pi:
            diff -= twopi
        elif diff < -pi:
            diff += twopi
        return diff

    # converging iterations using the get_diff() function, not fixing nutation
    for i in range(100):
        diff = get_diff(d, deg)
        if abs(diff) < ephem.degrees('0:00:01'):
            break
        d = d + 365.25 * diff / twopi  # update d using the difference

    # apply the bisection method, fixing nutation
    nutation_dpsi = nutation(d)[1]
    d0, d1 = d-ephem.degrees('0:05:00'), d+ephem.degrees('0:05:00')
    f0, f1 = get_diff(d0, deg, nutation_dpsi), get_diff(d1, deg, nutation_dpsi)
    if f0 * f1 > 0.:
        raise AssertionError("warning: f0=%f, f1=%f" % (f0, f1))

    for i in range(20):  # limits the iteration number to 20.
        dn = (d0 + d1) / 2.
        fn = get_diff(dn, deg, nutation_dpsi)
        if fn * f0 > 0.:  # the midpoint has the same sign as the left side -> select the right side
            d0 = dn
            f0 = get_diff(d0, deg, nutation_dpsi)
        elif fn * f1 > 0.:  # the midpoint has the same sign as the right side -> select the left side
            d1 = dn
            f1 = get_diff(d1, deg, nutation_dpsi)
        elif fn == 0:
            return ephem.date(dn)
        else:
            raise AssertionError("warning: impossible")

    return ephem.Date((d0*abs(f1)+d1*abs(f0))/(abs(f0) + abs(f1)))


def solar_term_finder(mj, n, reverse=False):
    def solar_term_finder_deg(mj, deg, reverse=False):
        offset = deg * degree
        d0 = ephem.Date(mj)
        motion = -twopi if reverse else twopi

        angle_to_cover = motion - (get_ap_hlon(d0) - offset) % motion
        if abs(angle_to_cover) < tiny:
            angle_to_cover = motion
        d = d0 + 365.25 * angle_to_cover / twopi
        return converge(d, deg)

    deg = ((n + 21) % 24) * 15
    return solar_term_finder_deg(mj, deg, reverse)


def solar_term_finder_adjacent(mj, divisor=30.0, remainder=15.0, reverse=False):
    d0 = ephem.Date(mj)
    if reverse:
        deg = (floor((get_ap_hlon(d0) / degree - remainder) / divisor)) * divisor + remainder
    else:
        deg = (floor((get_ap_hlon(d0) / degree - remainder) / divisor) + 1) * divisor + remainder
    angle_to_cover = deg * degree - get_ap_hlon(d0)
    d = d0 + 365.25 * angle_to_cover / twopi
    return (int((deg % 360.0) / 15.0), deg % 360.0, converge(d, deg))


def annual_solar_terms(year):
    ref = ephem.previous_winter_solstice(str(year)) + 0.01
    result = []
    for j in range(24):
        i = (j - 2) % 24
        d = pytz.utc.localize(solar_term_finder(ref, i).datetime())
        result.append((i, d))

    return result


def specified_solar_term(year, solar_term_id):
    # the reference point is between "major cold" and "start of spring"
    ref = ephem.previous_winter_solstice(str(year)) + 37.5
    return pytz.utc.localize(solar_term_finder(ref, solar_term_id).datetime())
