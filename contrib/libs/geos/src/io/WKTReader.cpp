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

#include <geos/io/WKTReader.h>
#include <geos/io/StringTokenizer.h>
#include <geos/io/ParseException.h>
#include <geos/io/CLocalizer.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Point.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/inline.h>
#include <geos/util.h>

#include <sstream>
#include <string>
#include <cassert>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

#ifndef GEOS_INLINE
#include <geos/io/WKTReader.inl>
#endif

using namespace std;
using namespace geos::geom;

namespace geos {
namespace io { // geos.io

std::unique_ptr<Geometry>
WKTReader::read(const string& wellKnownText)
{
    CLocalizer clocale;
    StringTokenizer tokenizer(wellKnownText);
    return readGeometryTaggedText(&tokenizer);
}

std::unique_ptr<CoordinateSequence>
WKTReader::getCoordinates(StringTokenizer* tokenizer)
{
    size_t dim = 2;
    string nextToken = getNextEmptyOrOpener(tokenizer, dim);
    if(nextToken == "EMPTY") {
        return geometryFactory->getCoordinateSequenceFactory()->create(std::size_t(0), dim);
    }

    Coordinate coord;
    getPreciseCoordinate(tokenizer, coord, dim);
    auto coordinates = detail::make_unique<CoordinateArraySequence>(0, dim);
    coordinates->add(coord);

    nextToken = getNextCloserOrComma(tokenizer);
    while(nextToken == ",") {
        getPreciseCoordinate(tokenizer, coord, dim);
        coordinates->add(coord);
        nextToken = getNextCloserOrComma(tokenizer);
    }

    return std::move(coordinates);
}


void
WKTReader::getPreciseCoordinate(StringTokenizer* tokenizer,
                                Coordinate& coord,
                                size_t& dim)
{
    coord.x = getNextNumber(tokenizer);
    coord.y = getNextNumber(tokenizer);
    if(isNumberNext(tokenizer)) {
        coord.z = getNextNumber(tokenizer);
        dim = 3;

        // If there is a fourth value (M) read and discard it.
        if(isNumberNext(tokenizer)) {
            getNextNumber(tokenizer);
        }

    }
    else {
        coord.z = DoubleNotANumber;
        dim = 2;
    }
    precisionModel->makePrecise(coord);
}

bool
WKTReader::isNumberNext(StringTokenizer* tokenizer)
{
    return tokenizer->peekNextToken() == StringTokenizer::TT_NUMBER;
}

double
WKTReader::getNextNumber(StringTokenizer* tokenizer)
{
    int type = tokenizer->nextToken();
    switch(type) {
    case StringTokenizer::TT_EOF:
        throw  ParseException("Expected number but encountered end of stream");
    case StringTokenizer::TT_EOL:
        throw  ParseException("Expected number but encountered end of line");
    case StringTokenizer::TT_NUMBER:
        return tokenizer->getNVal();
    case StringTokenizer::TT_WORD:
        throw  ParseException("Expected number but encountered word", tokenizer->getSVal());
    case '(':
        throw  ParseException("Expected number but encountered '('");
    case ')':
        throw  ParseException("Expected number but encountered ')'");
    case ',':
        throw  ParseException("Expected number but encountered ','");
    }
    assert(0); // Encountered unexpected StreamTokenizer type
    return 0;
}

string
WKTReader::getNextEmptyOrOpener(StringTokenizer* tokenizer, std::size_t& dim)
{
    string nextWord = getNextWord(tokenizer);

    // Skip the Z, M or ZM of an SF1.2 3/4 dim coordinate.
    if(nextWord == "Z" || nextWord == "ZM") {
        dim = 3;
    }

    // Skip the Z, M or ZM of an SF1.2 3/4 dim coordinate.
    if(nextWord == "Z" || nextWord == "M" || nextWord == "ZM") {
        nextWord = getNextWord(tokenizer);
    }

    if(nextWord == "EMPTY" || nextWord == "(") {
        return nextWord;
    }
    throw  ParseException("Expected 'Z', 'M', 'ZM', 'EMPTY' or '(' but encountered ", nextWord);
}

string
WKTReader::getNextCloserOrComma(StringTokenizer* tokenizer)
{
    string nextWord = getNextWord(tokenizer);
    if(nextWord == "," || nextWord == ")") {
        return nextWord;
    }
    throw  ParseException("Expected ')' or ',' but encountered", nextWord);
}

string
WKTReader::getNextCloser(StringTokenizer* tokenizer)
{
    string nextWord = getNextWord(tokenizer);
    if(nextWord == ")") {
        return nextWord;
    }
    throw  ParseException("Expected ')' but encountered", nextWord);
}

string
WKTReader::getNextWord(StringTokenizer* tokenizer)
{
    int type = tokenizer->nextToken();
    switch(type) {
    case StringTokenizer::TT_EOF:
        throw  ParseException("Expected word but encountered end of stream");
    case StringTokenizer::TT_EOL:
        throw  ParseException("Expected word but encountered end of line");
    case StringTokenizer::TT_NUMBER:
        throw  ParseException("Expected word but encountered number", tokenizer->getNVal());
    case StringTokenizer::TT_WORD: {
        string word = tokenizer->getSVal();
        int i = static_cast<int>(word.size());

        while(--i >= 0) {
            word[i] = static_cast<char>(toupper(word[i]));
        }
        return word;
    }
    case '(':
        return "(";
    case ')':
        return ")";
    case ',':
        return ",";
    }
    assert(0);
    //throw  ParseException("Encountered unexpected StreamTokenizer type");
    return "";
}

std::unique_ptr<Geometry>
WKTReader::readGeometryTaggedText(StringTokenizer* tokenizer)
{
    string type = getNextWord(tokenizer);
    if(type == "POINT") {
        return readPointText(tokenizer);
    }
    else if(type == "LINESTRING") {
        return readLineStringText(tokenizer);
    }
    else if(type == "LINEARRING") {
        return readLinearRingText(tokenizer);
    }
    else if(type == "POLYGON") {
        return readPolygonText(tokenizer);
    }
    else if(type == "MULTIPOINT") {
        return readMultiPointText(tokenizer);
    }
    else if(type == "MULTILINESTRING") {
        return readMultiLineStringText(tokenizer);
    }
    else if(type == "MULTIPOLYGON") {
        return readMultiPolygonText(tokenizer);
    }
    else if(type == "GEOMETRYCOLLECTION") {
        return readGeometryCollectionText(tokenizer);
    }
    throw ParseException("Unknown type", type);
}

std::unique_ptr<Point>
WKTReader::readPointText(StringTokenizer* tokenizer)
{
    size_t dim = 2;
    string nextToken = getNextEmptyOrOpener(tokenizer, dim);
    if(nextToken == "EMPTY") {
        return geometryFactory->createPoint(dim);
    }

    Coordinate coord;
    getPreciseCoordinate(tokenizer, coord, dim);
    getNextCloser(tokenizer);

    return std::unique_ptr<Point>(geometryFactory->createPoint(coord));
}

std::unique_ptr<LineString>
WKTReader::readLineStringText(StringTokenizer* tokenizer)
{
    auto&& coords = getCoordinates(tokenizer);
    return geometryFactory->createLineString(std::move(coords));
}

std::unique_ptr<LinearRing>
WKTReader::readLinearRingText(StringTokenizer* tokenizer)
{
    auto&& coords = getCoordinates(tokenizer);
    return geometryFactory->createLinearRing(std::move(coords));
}

std::unique_ptr<MultiPoint>
WKTReader::readMultiPointText(StringTokenizer* tokenizer)
{
    size_t dim = 2;
    string nextToken = getNextEmptyOrOpener(tokenizer, dim);
    if(nextToken == "EMPTY") {
        return geometryFactory->createMultiPoint();
    }

    int tok = tokenizer->peekNextToken();

    if(tok == StringTokenizer::TT_NUMBER) {

        // Try to parse deprecated form "MULTIPOINT(0 0, 1 1)"
        auto coords = detail::make_unique<CoordinateArraySequence>();

        do {
            Coordinate coord;
            getPreciseCoordinate(tokenizer, coord, dim);
            coords->add(coord);
            nextToken = getNextCloserOrComma(tokenizer);
        }
        while(nextToken == ",");

        return std::unique_ptr<MultiPoint>(geometryFactory->createMultiPoint(*coords));
    }

    else if(tok == '(') {
        // Try to parse correct form "MULTIPOINT((0 0), (1 1))"
        std::vector<std::unique_ptr<Point>> points;

        do {
            points.push_back(readPointText(tokenizer));
            nextToken = getNextCloserOrComma(tokenizer);
        } while(nextToken == ",");

        return geometryFactory->createMultiPoint(std::move(points));
    }

    else {
        stringstream err;
        err << "Unexpected token: ";
        switch(tok) {
        case StringTokenizer::TT_WORD:
            err << "WORD " << tokenizer->getSVal();
            break;
        case StringTokenizer::TT_NUMBER:
            err << "NUMBER " << tokenizer->getNVal();
            break;
        case StringTokenizer::TT_EOF:
        case StringTokenizer::TT_EOL:
            err << "EOF or EOL";
            break;
        case '(':
            err << "(";
            break;
        case ')':
            err << ")";
            break;
        case ',':
            err << ",";
            break;
        default:
            err << "??";
            break;
        }
        err << endl;
        throw ParseException(err.str());
    }
}

std::unique_ptr<Polygon>
WKTReader::readPolygonText(StringTokenizer* tokenizer)
{
    size_t dim = 2;
    string nextToken = getNextEmptyOrOpener(tokenizer, dim);
    if(nextToken == "EMPTY") {
        return geometryFactory->createPolygon(dim);
    }

    std::vector<std::unique_ptr<LinearRing>> holes;
    auto shell = readLinearRingText(tokenizer);
    nextToken = getNextCloserOrComma(tokenizer);
    while(nextToken == ",") {
        holes.push_back(readLinearRingText(tokenizer));
        nextToken = getNextCloserOrComma(tokenizer);
    }

    return geometryFactory->createPolygon(std::move(shell), std::move(holes));
}

std::unique_ptr<MultiLineString>
WKTReader::readMultiLineStringText(StringTokenizer* tokenizer)
{
    size_t dim = 2;
    string nextToken = getNextEmptyOrOpener(tokenizer, dim);
    if(nextToken == "EMPTY") {
        return geometryFactory->createMultiLineString();
    }

    std::vector<std::unique_ptr<LineString>> lineStrings;
    do {
        lineStrings.push_back(readLineStringText(tokenizer));
        nextToken = getNextCloserOrComma(tokenizer);
    } while (nextToken == ",");

    return geometryFactory->createMultiLineString(std::move(lineStrings));
}

std::unique_ptr<MultiPolygon>
WKTReader::readMultiPolygonText(StringTokenizer* tokenizer)
{
    size_t dim = 2;
    string nextToken = getNextEmptyOrOpener(tokenizer, dim);
    if(nextToken == "EMPTY") {
        return geometryFactory->createMultiPolygon();
    }

    std::vector<std::unique_ptr<Polygon>> polygons;
    do {
        polygons.push_back(readPolygonText(tokenizer));
        nextToken = getNextCloserOrComma(tokenizer);
    } while(nextToken == ",");

    return geometryFactory->createMultiPolygon(std::move(polygons));
}

std::unique_ptr<GeometryCollection>
WKTReader::readGeometryCollectionText(StringTokenizer* tokenizer)
{
    size_t dim = 2;
    string nextToken = getNextEmptyOrOpener(tokenizer, dim);
    if(nextToken == "EMPTY") {
        return geometryFactory->createGeometryCollection();
    }

    std::vector<std::unique_ptr<Geometry>> geoms;
    do {
        geoms.push_back(readGeometryTaggedText(tokenizer));
        nextToken = getNextCloserOrComma(tokenizer);
    } while(nextToken == ",");

    return geometryFactory->createGeometryCollection(std::move(geoms));
}

} // namespace geos.io
} // namespace geos
