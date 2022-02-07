// Boost.Geometry (aka GGL, Generic Geometry Library)
// This file is manually converted from PROJ4 (projects.h)

// Copyright (c) 2008-2012 Barend Gehrels, Amsterdam, the Netherlands.

// This file was modified by Oracle on 2017.
// Modifications copyright (c) 2017, Oracle and/or its affiliates.
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// This file is converted from PROJ4, http://trac.osgeo.org/proj
// PROJ4 is originally written by Gerald Evenden (then of the USGS)
// PROJ4 is maintained by Frank Warmerdam
// PROJ4 is converted to Geometry Library by Barend Gehrels (Geodan, Amsterdam)

// Original copyright notice:

// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

#ifndef BOOST_GEOMETRY_PROJECTIONS_IMPL_PROJECTS_HPP
#define BOOST_GEOMETRY_PROJECTIONS_IMPL_PROJECTS_HPP


#include <cstring>
#include <string>
#include <vector>

#include <boost/geometry/srs/projections/exception.hpp>
#include <boost/math/constants/constants.hpp>
#include <boost/mpl/if.hpp>
#include <boost/type_traits/is_pod.hpp>


namespace boost { namespace geometry { namespace projections
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail
{

/* some useful constants */
template <typename T>
inline T ONEPI() { return boost::math::constants::pi<T>(); }
template <typename T>
inline T HALFPI() { return boost::math::constants::half_pi<T>(); }
template <typename T>
inline T FORTPI() { return boost::math::constants::pi<T>() / T(4); }
template <typename T>
inline T TWOPI() { return boost::math::constants::two_pi<T>(); }
template <typename T>
inline T TWO_D_PI() { return boost::math::constants::two_div_pi<T>(); }
template <typename T>
inline T HALFPI_SQR() { return 2.4674011002723396547086227499689; }
template <typename T>
inline T PI_SQR() { return boost::math::constants::pi_sqr<T>(); }
template <typename T>
inline T THIRD() { return 0.3333333333333333333333333333333; }
template <typename T>
inline T TWOTHIRD() { return 0.6666666666666666666666666666666; }
template <typename T>
inline T PI_HALFPI() { return 4.7123889803846898576939650749193; }
template <typename T>
inline T TWOPI_HALFPI() { return 7.8539816339744830961566084581988; }
template <typename T>
inline T PI_DIV_3() { return 1.0471975511965977461542144610932; }

/* datum_type values */
static const int PJD_UNKNOWN = 0;
static const int PJD_3PARAM = 1;
static const int PJD_7PARAM = 2;
static const int PJD_GRIDSHIFT = 3;
static const int PJD_WGS84 = 4;   /* WGS84 (or anything considered equivelent) */

/* library errors */
static const int PJD_ERR_GEOCENTRIC = -45;
static const int PJD_ERR_AXIS = -47;
static const int PJD_ERR_GRID_AREA = -48;
static const int PJD_ERR_CATALOG = -49;

template <typename T>
struct pvalue
{
    std::string param;
    int used;

    int i;
    T f;
    std::string s;
};

template <typename T>
struct pj_const_pod
{
    int over;   /* over-range flag */
    int geoc;   /* geocentric latitude flag */
    int is_latlong; /* proj=latlong ... not really a projection at all */
    int is_geocent; /* proj=geocent ... not really a projection at all */
    T
        a,  /* major axis or radius if es==0 */
        a_orig, /* major axis before any +proj related adjustment */
        es, /* e ^ 2 */
        es_orig, /* es before any +proj related adjustment */
        e,  /* eccentricity */
        ra, /* 1/A */
        one_es, /* 1 - e^2 */
        rone_es, /* 1/one_es */
        lam0, phi0, /* central longitude, latitude */
        x0, y0, /* easting and northing */
        k0,    /* general scaling factor */
        to_meter, fr_meter, /* cartesian scaling */
        vto_meter, vfr_meter;      /* Vertical scaling. Internal unit [m] */

    int datum_type; /* PJD_UNKNOWN/3PARAM/7PARAM/GRIDSHIFT/WGS84 */
    T  datum_params[7];
    T  from_greenwich; /* prime meridian offset (in radians) */
    T  long_wrap_center; /* 0.0 for -180 to 180, actually in radians*/
    bool    is_long_wrap_set;

    // Initialize all variables to zero
    pj_const_pod()
    {
        std::memset(this, 0, sizeof(pj_const_pod));
    }
};

template <typename T>
struct pj_const_non_pod
{
    int over;   /* over-range flag */
    int geoc;   /* geocentric latitude flag */
    int is_latlong; /* proj=latlong ... not really a projection at all */
    int is_geocent; /* proj=geocent ... not really a projection at all */
    T
        a,  /* major axis or radius if es==0 */
        a_orig, /* major axis before any +proj related adjustment */
        es, /* e ^ 2 */
        es_orig, /* es before any +proj related adjustment */
        e,  /* eccentricity */
        ra, /* 1/A */
        one_es, /* 1 - e^2 */
        rone_es, /* 1/one_es */
        lam0, phi0, /* central longitude, latitude */
        x0, y0, /* easting and northing */
        k0,    /* general scaling factor */
        to_meter, fr_meter, /* cartesian scaling */
        vto_meter, vfr_meter;      /* Vertical scaling. Internal unit [m] */

    int datum_type; /* PJD_UNKNOWN/3PARAM/7PARAM/GRIDSHIFT/WGS84 */
    T  datum_params[7];
    T  from_greenwich; /* prime meridian offset (in radians) */
    T  long_wrap_center; /* 0.0 for -180 to 180, actually in radians*/
    bool    is_long_wrap_set;

    // Initialize all variables to zero
    pj_const_non_pod()
        : over(0), geoc(0), is_latlong(0), is_geocent(0)
        , a(0), a_orig(0), es(0), es_orig(0), e(0), ra(0)
        , one_es(0), rone_es(0), lam0(0), phi0(0), x0(0), y0(0), k0(0)
        , to_meter(0), fr_meter(0), vto_meter(0), vfr_meter(0)
        , datum_type(PJD_UNKNOWN)
        , from_greenwich(0), long_wrap_center(0), is_long_wrap_set(false)
    {
        datum_params[0] = 0;
        datum_params[1] = 0;
        datum_params[2] = 0;
        datum_params[3] = 0;
        datum_params[4] = 0;
        datum_params[5] = 0;
        datum_params[6] = 0;
    }
};

template <typename T>
struct pj_const
    : boost::mpl::if_c
        <
            boost::is_pod<T>::value,
            pj_const_pod<T>,
            pj_const_non_pod<T>
        >::type
{};

// PROJ4 complex. Might be replaced with std::complex
template <typename T>
struct COMPLEX { T r, i; };

struct PJ_ELLPS
{
    std::string id;    /* ellipse keyword name */
    std::string major;    /* a= value */
    std::string ell;    /* elliptical parameter */
    std::string name;    /* comments */
};

struct PJ_DATUMS
{
    std::string id;     /* datum keyword */
    std::string defn;   /* ie. "to_wgs84=..." */
    std::string ellipse_id; /* ie from ellipse table */
    std::string comments; /* EPSG code, etc */
};

struct PJ_PRIME_MERIDIANS
{
    std::string id;     /* prime meridian keyword */
    std::string defn;   /* offset from greenwich in DMS format. */
};

struct PJ_UNITS
{
    std::string id;    /* units keyword */
    std::string to_meter;    /* multiply by value to get meters */
    std::string name;    /* comments */
};

template <typename T>
struct DERIVS
{
    T x_l, x_p; /* derivatives of x for lambda-phi */
    T y_l, y_p; /* derivatives of y for lambda-phi */
};

template <typename T>
struct FACTORS
{
    DERIVS<T> der;
    T h, k;    /* meridinal, parallel scales */
    T omega, thetap;    /* angular distortion, theta prime */
    T conv;    /* convergence */
    T s;        /* areal scale factor */
    T a, b;    /* max-min scale error */
    int code;        /* info as to analytics, see following */
};

} // namespace detail
#endif // DOXYGEN_NO_DETAIL

/*!
    \brief parameters, projection parameters
    \details This structure initializes all projections
    \ingroup projection
*/
template <typename T>
struct parameters : public detail::pj_const<T>
{
    typedef T type;

    std::string name;
    std::vector<detail::pvalue<T> > params;
};

}}} // namespace boost::geometry::projections
#endif // BOOST_GEOMETRY_PROJECTIONS_IMPL_PROJECTS_HPP
