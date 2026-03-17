/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: io/WKTWriter.java rev. 1.34 (JTS-1.7)
 *
 **********************************************************************/

#ifndef GEOS_IO_WKTWRITER_H
#define GEOS_IO_WKTWRITER_H

#include <geos/export.h>

#include <string>
#include <cctype>
#include <cstdint>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
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
}
namespace io {
class Writer;
}
}


namespace geos {
namespace io {

/**
 * \class WKTWriter
 *
 * \brief Outputs the textual representation of a Geometry.
 * See also WKTReader.
 *
 * The WKTWriter outputs coordinates rounded to the precision
 * model. No more than the maximum number of necessary decimal places will be
 * output.
 *
 * The Well-known Text format is defined in the <A
 * HREF="http://www.opengis.org/techno/specs.htm">OpenGIS Simple Features
 * Specification for SQL</A>.
 *
 * A non-standard "LINEARRING" tag is used for LinearRings. The WKT spec does
 * not define a special tag for LinearRings. The standard tag to use is
 * "LINESTRING".
 *
 * See WKTReader for parsing.
 *
 */
class GEOS_DLL WKTWriter {
public:
    WKTWriter();
    ~WKTWriter() = default;

    //string(count, ch) can be used for this
    //static string stringOfChar(char ch, int count);

    /// Returns WKT string for the given Geometry
    std::string write(const geom::Geometry* geometry);

    // Send Geometry's WKT to the given Writer
    void write(const geom::Geometry* geometry, Writer* writer);

    std::string writeFormatted(const geom::Geometry* geometry);

    void writeFormatted(const geom::Geometry* geometry, Writer* writer);

    /**
     * Generates the WKT for a N-point <code>LineString</code>.
     *
     * @param seq the sequence to outpout
     *
     * @return the WKT
     */
    static std::string toLineString(const geom::CoordinateSequence& seq);

    /**
     * Generates the WKT for a 2-point <code>LineString</code>.
     *
     * @param p0 the first coordinate
     * @param p1 the second coordinate
     *
     * @return the WKT
     */
    static std::string toLineString(const geom::Coordinate& p0, const geom::Coordinate& p1);

    /**
     * Generates the WKT for a <code>Point</code>.
     *
     * @param p0 the point coordinate
     *
     * @return the WKT
     */
    static std::string toPoint(const geom::Coordinate& p0);

    /**
     * Sets the rounding precision when writing the WKT
     * a precision of -1 disables it
     *
     * @param p0 the new precision to use
     *
     */
    void setRoundingPrecision(int p0);

    /**
     * Enables/disables trimming of unnecessary decimals
     *
     * @param p0 the trim boolean
     *
     */
    void setTrim(bool p0);

    /**
     * Enable old style 3D/4D WKT generation.
     *
     * By default the WKBWriter produces new style 3D/4D WKT
     * (ie. "POINT Z (10 20 30)") but if this method is used
     * to turn on old style WKT production then the WKT will
     * be formatted in the style "POINT (10 20 30)".
     *
     * @param useOld3D true or false
     */
    void
    setOld3D(bool useOld3D)
    {
        old3D = useOld3D;
    }

    /*
     * \brief
     * Returns the output dimension used by the
     * <code>WKBWriter</code>.
     */
    int
    getOutputDimension() const
    {
        return defaultOutputDimension;
    }

    /*
     * Sets the output dimension used by the <code>WKBWriter</code>.
     *
     * @param newOutputDimension Supported values are 2 or 3.
     *        Note that 3 indicates up to 3 dimensions will be
     *        written but 2D WKB is still produced for 2D geometries.
     */
    void setOutputDimension(uint8_t newOutputDimension);

protected:

    int decimalPlaces;

    void appendGeometryTaggedText(const geom::Geometry* geometry, int level, Writer* writer);

    void appendPointTaggedText(
        const geom::Coordinate* coordinate,
        int level, Writer* writer);

    void appendLineStringTaggedText(
        const geom::LineString* lineString,
        int level, Writer* writer);

    void appendLinearRingTaggedText(
        const geom::LinearRing* lineString,
        int level, Writer* writer);

    void appendPolygonTaggedText(
        const geom::Polygon* polygon,
        int level, Writer* writer);

    void appendMultiPointTaggedText(
        const geom::MultiPoint* multipoint,
        int level, Writer* writer);

    void appendMultiLineStringTaggedText(
        const geom::MultiLineString* multiLineString,
        int level, Writer* writer);

    void appendMultiPolygonTaggedText(
        const geom::MultiPolygon* multiPolygon,
        int level, Writer* writer);

    void appendGeometryCollectionTaggedText(
        const geom::GeometryCollection* geometryCollection,
        int level, Writer* writer);

    void appendPointText(const geom::Coordinate* coordinate, int level,
                         Writer* writer);

    void appendCoordinate(const geom::Coordinate* coordinate,
                          Writer* writer);

    std::string writeNumber(double d);

    void appendLineStringText(
        const geom::LineString* lineString,
        int level, bool doIndent, Writer* writer);

    void appendPolygonText(
        const geom::Polygon* polygon,
        int level, bool indentFirst, Writer* writer);

    void appendMultiPointText(
        const geom::MultiPoint* multiPoint,
        int level, Writer* writer);

    void appendMultiLineStringText(
        const geom::MultiLineString* multiLineString,
        int level, bool indentFirst, Writer* writer);

    void appendMultiPolygonText(
        const geom::MultiPolygon* multiPolygon,
        int level, Writer* writer);

    void appendGeometryCollectionText(
        const geom::GeometryCollection* geometryCollection,
        int level, Writer* writer);

private:

    enum {
        INDENT = 2
    };

//	static const int INDENT = 2;

    bool isFormatted;

    int roundingPrecision;

    bool trim;

    int level;

    uint8_t defaultOutputDimension;
    uint8_t outputDimension;
    bool old3D;

    void writeFormatted(
        const geom::Geometry* geometry,
        bool isFormatted, Writer* writer);

    void indent(int level, Writer* writer);
};

} // namespace geos::io
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // #ifndef GEOS_IO_WKTWRITER_H
