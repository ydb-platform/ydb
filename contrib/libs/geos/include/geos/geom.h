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
 **********************************************************************/

#ifndef GEOS_GEOM_H
#define GEOS_GEOM_H

/** \mainpage GEOS - Geometry Engine Open Source
 *
 * \section intro_sec Introduction
 *
 * Geometry Engine Open Source is a C++ port of the Java Topology Suite
 * released under the LGPL license.
 * It has interfaces for C++ and C.
 *
 * \section getstart_sec Getting Started
 *
 * The recommended low-level interface to the GEOS library
 * is the simplified \ref c_iface. This will ensure stability of the
 * API and the ABI of the library during performance improvements
 * that will likely change classes definitions.
 *
 * If you don't care about adapting/rebuilding your client code
 * you can still use the \ref cpp_iface.
 */

/** \page c_iface C wrapper interface
 *
 * \section overview_c Overview
 *
 * This is the preferred access method for GEOS.
 *
 * It is designed to keep binary compatibility across releases.
 *
 * \section Usage
 *
 * In order to use the C-API of geos you must link your code against
 * libgeos_c.so and include the geos_c.h header file, which also contain
 * function-level documentation.
 *
 */

/** \page cpp_iface C++ interface
 *
 * \section overview_cpp Overview
 *
 * Main class is geos::geom::Geometry, from which all geometry types
 * derive.
 *
 * Construction and destruction of Geometries is done
 * using geos::geom::GeometryFactory.
 *
 * You'll feed it geos::geom::CoordinateSequence
 * for base geometries or vectors of geometries for collections.
 *
 * If you need to construct geometric shaped geometries, you
 * can use geos::geom::GeometricShapeFactory.
 *
 * GEOS version info (as a string) can be obtained using
 * geos::geom::geosversion(). The JTS version this release has been
 * ported from is available throu geos::geom::jtsport().
 *
 * \section io_sect Input / Output
 *
 * For WKT input/output you can use geos::io::WKTReader and geos::io::WKTWriter
 *
 * For WKB input/output you can use geos::io::WKBReader and geos::io::WKBWriter
 *
 * \section exc_sect Exceptions
 *
 * Internal exceptions are thrown as instances geos::util::GEOSException or
 * derived classes. GEOSException derives from std::exception.
 *
 * Note that prior to version 3.0.0, GEOSException were thrown by
 * pointer, and did not derive from std::exception.
 *
 */


namespace geos {

/// Contains the <CODE>Geometry</CODE> interface hierarchy and supporting classes.
///
/// The Java Topology Suite (JTS) is a Java API that implements a core
/// set of spatial data operations using an explicit precision model
/// and robust geometric algorithms. JTS is int ended to be used in the
/// development of applications that support the validation, cleaning,
/// integration and querying of spatial datasets.
///
/// JTS attempts to implement the OpenGIS Simple Features Specification
/// (SFS) as accurately as possible.  In some cases the SFS is unclear
/// or omits a specification; in this case J TS attempts to choose
/// a reasonable and consistent alternative.  Differences from and
/// elaborations of the SFS are documented in this specification.
///
/// <h2>Package Specification</h2>
///
/// - Java Topology Suite Technical Specifications
/// - <A HREF="http://www.opengis.org/techno/specs.htm">
///   OpenGIS Simple Features Specification for SQL</A>
///
namespace geom { // geos::geom

} // namespace geos::geom
} // namespace geos

#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/geom/CoordinateFilter.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/Dimension.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/GeometryComponentFilter.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/GeometryFilter.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/IntersectionMatrix.h>
#include <geos/geom/Location.h>
//#include <geos/geom/Triangle.h>

#ifdef __GNUC__
#warning *** DEPRECATED: You are using deprecated header geom.h. Please, update your sources according to new layout of GEOS headers and namespaces
#endif

using namespace geos::geom;



#endif // ndef GEOS_GEOM_H

