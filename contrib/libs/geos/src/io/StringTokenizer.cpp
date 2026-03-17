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

#include <geos/io/StringTokenizer.h>

#include <string>
#include <cstdlib>
#include <limits>

using namespace std;

namespace geos {
namespace io { // geos.io

/*public*/
StringTokenizer::StringTokenizer(const string& txt)
    :
    str(txt),
    stok(""),
    ntok(0.0)
{
    iter = str.begin();
}

double
strtod_with_vc_fix(const char* str, char** str_end)
{
    double dbl = strtod(str, str_end);
#if _MSC_VER && !__INTEL_COMPILER
    // Special handling of NAN and INF in MSVC, where strtod returns 0.0
    // for NAN and INF.
    // This fixes failing test GEOSisValidDetail::test<3>, maybe others
    // as well.
    // Note: this hack is not robust, Boost lexical_cast or
    // std::stod (C++11) would be better.
    if(*str_end[0] != '\0') {
        char sign = 0;
        const char* pos = str;
        if(*pos == '+' || *pos == '-') {
            sign = *pos++;
        }

        if(stricmp(pos, "inf") == 0) {
            if(!sign || sign == '+') {
                dbl = std::numeric_limits<double>::infinity();
            }
            else {
                dbl = -(std::numeric_limits<double>::infinity)();
            }
            *str_end[0] = '\0';
        }
        else if(stricmp(pos, "nan") == 0) {
            dbl = std::numeric_limits<double>::quiet_NaN();
            *str_end[0] = '\0';
        }
    }
#endif
    return dbl;
}

/*public*/
int
StringTokenizer::nextToken()
{
    string tok = "";
    if(iter == str.end()) {
        return StringTokenizer::TT_EOF;
    }
    switch(*iter) {
    case '(':
    case ')':
    case ',':
        return *iter++;
    case '\n':
    case '\r':
    case '\t':
    case ' ':
        string::size_type pos = str.find_first_not_of(" \n\r\t",
                                iter - str.begin());
        if(pos == string::npos) {
            return StringTokenizer::TT_EOF;
        }
        else {
            iter = str.begin() + pos;
            return nextToken();
        }
    }
    string::size_type pos = str.find_first_of("\n\r\t() ,",
                            iter - str.begin());
    if(pos == string::npos) {
        if(iter != str.end()) {
            tok.assign(iter, str.end());
            iter = str.end();
        }
        else {
            return StringTokenizer::TT_EOF;
        }
    }
    else {
        tok.assign(iter, str.begin() + pos);
        iter = str.begin() + pos;
    }
    char* stopstring;
    double dbl = strtod_with_vc_fix(tok.c_str(), &stopstring);
    if(*stopstring == '\0') {
        ntok = dbl;
        stok = "";
        return StringTokenizer::TT_NUMBER;
    }
    else {
        ntok = 0.0;
        stok = tok;
        return StringTokenizer::TT_WORD;
    }
}

/*public*/
int
StringTokenizer::peekNextToken()
{

    string::size_type pos;
    string tok = "";
    if(iter == str.end()) {
        return StringTokenizer::TT_EOF;
    }

    pos = str.find_first_not_of(" \r\n\t", iter - str.begin());

    if(pos == string::npos) {
        return StringTokenizer::TT_EOF;
    }
    switch(str[pos]) {
    case '(':
    case ')':
    case ',':
        return str[pos];
    }

    // It's either a Number or a Word, let's
    // see when it ends

    pos = str.find_first_of("\n\r\t() ,", iter - str.begin());

    if(pos == string::npos) {
        if(iter != str.end()) {
            tok.assign(iter, str.end());
        }
        else {
            return StringTokenizer::TT_EOF;
        }
    }
    else {
        tok.assign(iter, str.begin() + pos); //str.end());
    }

    char* stopstring;
    double dbl = strtod_with_vc_fix(tok.c_str(), &stopstring);
    if(*stopstring == '\0') {
        ntok = dbl;
        stok = "";
        return StringTokenizer::TT_NUMBER;
    }
    else {
        ntok = 0.0;
        stok = tok;
        return StringTokenizer::TT_WORD;
    }
}

/*public*/
double
StringTokenizer::getNVal()
{
    return ntok;
}

/*public*/
string
StringTokenizer::getSVal()
{
    return stok;
}

} // namespace geos.io
} // namespace geos
