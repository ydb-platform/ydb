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
 * Last port: io/ParseException.java rev. 1.13 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_IO_PARSEEXCEPTION_H
#define GEOS_IO_PARSEEXCEPTION_H

#include <geos/export.h>

#include <geos/util/GEOSException.h>

namespace geos {
namespace io {

/**
 * \class ParseException
 * \brief Notifies a parsing error
 */
class GEOS_DLL ParseException : public util::GEOSException {

public:

    ParseException();

    ParseException(const std::string& msg);

    ParseException(const std::string& msg, const std::string& var);

    ParseException(const std::string& msg, double num);

    ~ParseException() noexcept override {}

private:
    static std::string stringify(double num);
};

} // namespace io
} // namespace geos

#endif // #ifndef GEOS_IO_PARSEEXCEPTION_H
