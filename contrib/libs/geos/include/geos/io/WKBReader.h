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
 * Last port: io/WKBReader.java rev. 1.1 (JTS-1.7)
 *
 **********************************************************************/

#ifndef GEOS_IO_WKBREADER_H
#define GEOS_IO_WKBREADER_H

#include <geos/export.h>

#include <geos/io/ByteOrderDataInStream.h> // for composition

#include <iosfwd> // ostream, istream
#include <memory>
// #include <vector>
#include <array>

#define BAD_GEOM_TYPE_MSG "Bad geometry type encountered in"

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {

class GeometryFactory;
class Coordinate;
class Geometry;
class GeometryCollection;
class Point;
class LineString;
class LinearRing;
class Polygon;
class MultiPoint;
class MultiLineString;
class MultiPolygon;
class PrecisionModel;
class CoordinateSequence;

} // namespace geom
} // namespace geos


namespace geos {
namespace io {

/**
 * \class WKBReader
 *
 * \brief Reads a Geometry from Well-Known Binary format.
 *
 * This class is designed to support reuse of a single instance to read
 * multiple geometries. This class is not thread-safe; each thread should
 * create its own instance.
 *
 * The Well-known Binary format is defined in the <A
 * HREF="http://www.opengis.org/techno/specs.htm">OpenGIS Simple Features
 * Specification for SQL</A>.
 * This implementation supports the extended WKB standard which allows
 * representing 3-dimensional coordinates.
 *
 */
class GEOS_DLL WKBReader {

public:

    WKBReader(geom::GeometryFactory const& f);

    /// Inizialize parser with default GeometryFactory.
    WKBReader();

    /**
     * \brief Reads a Geometry from an istream.
     *
     * @param is the stream to read from
     * @return the Geometry read
     * @throws IOException
     * @throws ParseException
     */
    std::unique_ptr<geom::Geometry> read(std::istream& is);

    /**
     * \brief Reads a Geometry from an istream in hex format.
     *
     * @param is the stream to read from
     * @return the Geometry read
     * @throws IOException
     * @throws ParseException
     */
    std::unique_ptr<geom::Geometry> readHEX(std::istream& is);

    /**
     * \brief Print WKB in HEX form to out stream
     *
     * @param is is the stream to read from
     * @param os is the stream to write to
     */
    static std::ostream& printHEX(std::istream& is, std::ostream& os);

private:

    const geom::GeometryFactory& factory;

    // for now support the WKB standard only - may be generalized later
    unsigned int inputDimension;
    bool hasZ;
    bool hasM;

    ByteOrderDataInStream dis;

    std::array<double, 4> ordValues;

    std::unique_ptr<geom::Geometry> readGeometry();

    std::unique_ptr<geom::Point> readPoint();

    std::unique_ptr<geom::LineString> readLineString();

    std::unique_ptr<geom::LinearRing> readLinearRing();

    std::unique_ptr<geom::Polygon> readPolygon();

    std::unique_ptr<geom::MultiPoint> readMultiPoint();

    std::unique_ptr<geom::MultiLineString> readMultiLineString();

    std::unique_ptr<geom::MultiPolygon> readMultiPolygon();

    std::unique_ptr<geom::GeometryCollection> readGeometryCollection();

    std::unique_ptr<geom::CoordinateSequence> readCoordinateSequence(int); // throws IOException

    void readCoordinate(); // throws IOException

    // Declare type as noncopyable
    WKBReader(const WKBReader& other) = delete;
    WKBReader& operator=(const WKBReader& rhs) = delete;
};

} // namespace io
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // #ifndef GEOS_IO_WKBREADER_H
