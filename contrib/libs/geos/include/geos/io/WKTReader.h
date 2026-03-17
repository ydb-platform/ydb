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
 * Last port: io/WKTReader.java rev. 1.1 (JTS-1.7)
 *
 **********************************************************************/

#ifndef GEOS_IO_WKTREADER_H
#define GEOS_IO_WKTREADER_H

#include <geos/export.h>

#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Geometry.h>
#include <string>

// Forward declarations
namespace geos {
namespace io {
class StringTokenizer;
}
namespace geom {

class Coordinate;
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
}


namespace geos {
namespace io {

/**
 * \class WKTReader
 * \brief WKT parser class; see also WKTWriter.
 */
class GEOS_DLL WKTReader {
public:
    //WKTReader();

    /**
     * \brief Inizialize parser with given GeometryFactory.
     *
     * Note that all Geometry objects created by the
     * parser will contain a pointer to the given factory
     * so be sure you'll keep the factory alive for the
     * whole WKTReader and created Geometry life.
     */
    WKTReader(const geom::GeometryFactory& gf);

    /** @deprecated in 3.4.0 */
    WKTReader(const geom::GeometryFactory* gf);

    /**
     * \brief Inizialize parser with default GeometryFactory.
     *
     */
    WKTReader();

    ~WKTReader();

    /// Parse a WKT string returning a Geometry
    std::unique_ptr<geom::Geometry> read(const std::string& wellKnownText);

//	Geometry* read(Reader& reader);	//Not implemented yet

protected:
    std::unique_ptr<geom::CoordinateSequence> getCoordinates(io::StringTokenizer* tokenizer);
    double getNextNumber(io::StringTokenizer* tokenizer);
    std::string getNextEmptyOrOpener(io::StringTokenizer* tokenizer, std::size_t& dim);
    std::string getNextCloserOrComma(io::StringTokenizer* tokenizer);
    std::string getNextCloser(io::StringTokenizer* tokenizer);
    std::string getNextWord(io::StringTokenizer* tokenizer);
    std::unique_ptr<geom::Geometry> readGeometryTaggedText(io::StringTokenizer* tokenizer);
    std::unique_ptr<geom::Point> readPointText(io::StringTokenizer* tokenizer);
    std::unique_ptr<geom::LineString> readLineStringText(io::StringTokenizer* tokenizer);
    std::unique_ptr<geom::LinearRing> readLinearRingText(io::StringTokenizer* tokenizer);
    std::unique_ptr<geom::MultiPoint> readMultiPointText(io::StringTokenizer* tokenizer);
    std::unique_ptr<geom::Polygon> readPolygonText(io::StringTokenizer* tokenizer);
    std::unique_ptr<geom::MultiLineString> readMultiLineStringText(io::StringTokenizer* tokenizer);
    std::unique_ptr<geom::MultiPolygon> readMultiPolygonText(io::StringTokenizer* tokenizer);
    std::unique_ptr<geom::GeometryCollection> readGeometryCollectionText(io::StringTokenizer* tokenizer);
private:
    const geom::GeometryFactory* geometryFactory;
    const geom::PrecisionModel* precisionModel;

    void getPreciseCoordinate(io::StringTokenizer* tokenizer, geom::Coordinate&, std::size_t& dim);

    bool isNumberNext(io::StringTokenizer* tokenizer);
};

} // namespace io
} // namespace geos

#ifdef GEOS_INLINE
# include <geos/io/WKTReader.inl>
#endif

#endif // #ifndef GEOS_IO_WKTREADER_H
