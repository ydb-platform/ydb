/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#ifndef eccodes_windef_H
#define eccodes_windef_H

/* Microsoft Windows Visual Studio support */
#if defined(_WIN32) && defined(_MSC_VER)
#define ECCODES_ON_WINDOWS
#ifndef YY_NO_UNISTD_H
#define YY_NO_UNISTD_H
#endif
#endif

#endif
