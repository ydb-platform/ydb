/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: ORIGINAL WORK
 *
 **********************************************************************/

#ifndef GEOS_IO_STRINGTOKENIZER_H
#define GEOS_IO_STRINGTOKENIZER_H

#include <geos/export.h>

#include <string>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace io {

class GEOS_DLL StringTokenizer {
public:
    enum {
        TT_EOF,
        TT_EOL,
        TT_NUMBER,
        TT_WORD
    };
    //StringTokenizer();
    explicit StringTokenizer(const std::string& txt);
    ~StringTokenizer() {}
    int nextToken();
    int peekNextToken();
    double getNVal();
    std::string getSVal();
private:
    const std::string& str;
    std::string stok;
    double ntok;
    std::string::const_iterator iter;

    // Declare type as noncopyable
    StringTokenizer(const StringTokenizer& other) = delete;
    StringTokenizer& operator=(const StringTokenizer& rhs) = delete;
};

} // namespace io
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // #ifndef GEOS_IO_STRINGTOKENIZER_H
