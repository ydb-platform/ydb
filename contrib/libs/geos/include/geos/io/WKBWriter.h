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
 * Last port: io/WKBWriter.java rev. 1.1 (JTS-1.7)
 *
 **********************************************************************/

#ifndef GEOS_IO_WKBWRITER_H
#define GEOS_IO_WKBWRITER_H

#include <geos/export.h>

#include <geos/util/Machine.h> // for getMachineByteOrder
#include <iosfwd>
#include <cstdint>
#include <cstddef>

// Forward declarations
namespace geos {
namespace geom {

class CoordinateSequence;
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

} // namespace geom
} // namespace geos

namespace geos {
namespace io {

/**
 *
 * \class WKBWriter
 *
 * \brief Writes a Geometry into Well-Known Binary format.
 *
 * The WKB format is specified in the OGC Simple Features for SQL specification.
 * This implementation supports the extended WKB standard for representing
 * 3-dimensional coordinates.  The presence of 3D coordinates is signified
 * by setting the high bit of the wkbType word.
 *
 * Empty Points cannot be represented in WKB; an
 * IllegalArgumentException will be thrown if one is
 * written. The WKB specification does not support representing LinearRing
 * they will be written as LineString
 *
 * This class is designed to support reuse of a single instance to read multiple
 * geometries. This class is not thread-safe; each thread should create its own
 * instance.
 *
 * @see WKBReader
 */
class GEOS_DLL WKBWriter {

public:
    /*
     * \brief
     * Initializes writer with target coordinate dimension, endianness
     * flag and SRID value.
     *
     * @param dims Supported values are 2 or 3.  Note that 3 indicates
     * up to 3 dimensions will be written but 2D WKB is still produced for 2D geometries.
     * @param bo output byte order - default to native machine byte order.
     * Legal values include 0 (big endian/xdr) and 1 (little endian/ndr).
     * @param incudeSRID true if SRID should be included in WKB (an
     * extension).
     */
    WKBWriter(uint8_t dims = 2, int bo = getMachineByteOrder(), bool includeSRID = false);

    /*
     * \brief
     * Destructor.
     */
    virtual ~WKBWriter() = default;

    /*
     * \brief
     * Returns the output dimension used by the
     * <code>WKBWriter</code>.
     */
    virtual uint8_t
    getOutputDimension() const
    {
        return defaultOutputDimension;
    }

    /*
     * Sets the output dimension used by the <code>WKBWriter</code>.
     *
     * @param newOutputDimension Supported values are 2 or 3.
     * Note that 3 indicates up to 3 dimensions will be written but
     * 2D WKB is still produced for 2D geometries.
     */
    virtual void setOutputDimension(uint8_t newOutputDimension);

    /*
     * \brief
     * Returns the byte order used by the
     * <code>WKBWriter</code>.
     */
    virtual int
    getByteOrder() const
    {
        return byteOrder;
    }

    /*
     * Sets the byte order used by the
     * <code>WKBWriter</code>.
     */
    virtual void setByteOrder(int newByteOrder);

    /*
     * \brief
     * Returns whether SRID values are output by the
     * <code>WKBWriter</code>.
     */
    virtual bool
    getIncludeSRID() const
    {
        return includeSRID;
    }

    /*
     * Sets whether SRID values should be output by the
     * <code>WKBWriter</code>.
     */
    virtual void
    setIncludeSRID(bool newIncludeSRID)
    {
        includeSRID = newIncludeSRID;
    }

    /**
     * \brief Write a Geometry to an ostream.
     *
     * @param g the geometry to write
     * @param os the output stream
     * @throws IOException
     */
    void write(const geom::Geometry& g, std::ostream& os);
    // throws IOException, ParseException

    /**
     * \brief Write a Geometry to an ostream in binary hex format.
     *
     * @param g the geometry to write
     * @param os the output stream
     * @throws IOException
     */
    void writeHEX(const geom::Geometry& g, std::ostream& os);
    // throws IOException, ParseException

private:

    uint8_t defaultOutputDimension;
    uint8_t outputDimension;

    int byteOrder;

    bool includeSRID;

    std::ostream* outStream;

    unsigned char buf[8];

    void writePoint(const geom::Point& p);
    void writePointEmpty(const geom::Point& p);
    // throws IOException

    void writeLineString(const geom::LineString& ls);
    // throws IOException

    void writePolygon(const geom::Polygon& p);
    // throws IOException

    void writeGeometryCollection(const geom::GeometryCollection& c, int wkbtype);
    // throws IOException, ParseException

    void writeCoordinateSequence(const geom::CoordinateSequence& cs, bool sized);
    // throws IOException

    void writeCoordinate(const geom::CoordinateSequence& cs, size_t idx, bool is3d);
    // throws IOException

    void writeGeometryType(int geometryType, int SRID);
    // throws IOException

    void writeSRID(int SRID);
    // throws IOException

    void writeByteOrder();
    // throws IOException

    void writeInt(int intValue);
    // throws IOException

};

} // namespace io
} // namespace geos

#endif // #ifndef GEOS_IO_WKBWRITER_H
