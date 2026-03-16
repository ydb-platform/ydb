/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2019      Nicklas Larsson
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_NAMESPACES_H
#define GEOS_NAMESPACES_H

namespace geos {

/// \brief
/// Contains classes and interfaces implementing fundamental computational
/// geometry algorithms.
///
/// ### Robustness
///
/// Geometrical algorithms involve a combination of combinatorial and numerical
/// computation. As with all numerical computation using finite-precision
/// numbers, the algorithms chosen are susceptible to problems of robustness.
/// A robustness problem occurs when a numerical calculation produces an
/// incorrect answer for some inputs due to round-off errors.  Robustness
/// problems are especially serious in geometric computation, since they can
/// result in errors during topology building.
///
/// There are many approaches to dealing with the problem of robustness in
/// geometrical computation. Not surprisingly, most robust algorithms are
/// substantially more complex and less performant than the non-robust
/// versions. Fortunately, JTS is sensitive to robustness problems in only a
/// few key functions (such as line intersection and the point-in-polygon
/// test). There are efficient robust algorithms available for these
/// functions, and these algorithms are implemented in JTS.
///
/// ### Computational Performance
///
/// Runtime performance is an important consideration for a production-quality
/// implementation of geometric algorithms. The most computationally intensive
/// algorithm used in JTS is intersection detection. JTS methods need to
/// determine both all intersection between the line segments in a single
/// Geometry (self-intersection) and all intersections between the line
/// segments of two different Geometries.
///
/// The obvious naive algorithm for intersection detection (comparing every
/// segment with every other) has unacceptably slow performance. There is a
/// large literature of faster algorithms for intersection detection.
/// Unfortunately, many of them involve substantial code complexity. JTS tries
/// to balance code simplicity with performance gains. It uses some simple
/// techniques to produce substantial performance gains for common types of
/// input data.
///
/// ### Package Specification
///
/// - Java Topology Suite Technical Specifications
/// - <A HREF="http://www.opengis.org/techno/specs.htm">
///       OpenGIS Simple Features Specification for SQL</A>
    namespace algorithm { // geos::algorithm

    /// Classes to compute distance metrics between geometries.
    namespace distance {}

    /// Classes which determine the Location of points in geometries.
    namespace locate {}
} // namespace geos::algorithm

namespace geom { // geos.geom

    /// \brief
    /// Contains classes and interfaces implementing algorithms that optimize
    /// the performance of repeated calls to specific geometric operations.
    namespace prep {}

    /// \brief
    /// Provides classes that parse and modify Geometry objects.
    namespace util {}
} // namespace geos.geom

/// \brief
/// Contains classes that implement topology graphs.
///
/// The Java Topology Suite (JTS) is a Java API that implements a core set of
/// spatial data operations using an explicit precision model and robust
/// geometric algorithms. JTS is int ended to be used in the development of
/// applications that support the validation, cleaning, integration and
/// querying of spatial datasets.
///
/// JTS attempts to implement the OpenGIS Simple Features Specification (SFS)
/// as accurately as possible.  In some cases the SFS is unclear or omits a
/// specification; in this case JTS attempts to choose a reasonable and
/// consistent alternative.  Differences from and elaborations of the SFS are
/// documented in this specification.
///
/// ### Package Specification
///
/// - Java Topology Suite Technical Specifications
/// - <A HREF="http://www.opengis.org/techno/specs.htm">
///   OpenGIS Simple Features Specification for SQL</A>
namespace geomgraph { // geos.geomgraph

    /// \brief
    /// Contains classes that implement indexes for performing noding on
    /// geometry graph edges.
    namespace index {}
} // namespace geos.geomgraph


/// Provides classes for various kinds of spatial indexes.
namespace index { // geos.index

    /// Contains classes that implement a Binary Interval Tree index
    namespace bintree {}

    /// Contains classes that implement Monotone Chains
    namespace chain {}

    /// \brief Contains classes that implement a static index on a set of
    /// 1-dimensional intervals, using an R-Tree packed based on the order of
    /// the interval midpoints.
    namespace intervalrtree {}

    /// Contains classes that implement a Quadtree spatial index
    namespace quadtree {}

    /// \brief Contains 2-D and 1-D versions of the Sort-Tile-Recursive (STR)
    /// tree, a query-only R-tree.
    namespace strtree {}

    /// \brief Contains classes which implement a sweepline algorithm for
    /// scanning geometric data structures.
    namespace sweepline {}
} // namespace geos.index

/// \brief
/// Contains the interfaces for converting JTS objects to and from other
/// formats.
///
/// The Java Topology Suite (JTS) is a Java API that implements a core set of
/// spatial data operations usin g an explicit precision model and robust
/// geometric algorithms. JTS is intended to be used in the devel opment of
/// applications that support the validation, cleaning, integration and
/// querying of spatial data sets.
///
/// JTS attempts to implement the OpenGIS Simple Features Specification (SFS)
/// as accurately as possible.  In some cases the SFS is unclear or omits a
/// specification; in this case JTS attempts to choose a reasonable and
/// consistent alternative.  Differences from and elaborations of the SFS are
/// documented in this specification.
///
/// ### Package Specification
///
/// - Java Topology Suite Technical Specifications
/// - <A HREF="http://www.opengis.org/techno/specs.htm">
///   OpenGIS Simple Features Specification for SQL</A>
///
namespace io {}

/// \brief Contains classes and interfaces implementing linear referencing on
/// linear geometries.
namespace linearref {}

/// \brief Classes to compute nodings for arrangements of line segments and
/// line segment sequences.
namespace noding { // geos.noding

    /// \brief Contains classes to implement the Snap Rounding algorithm for
    /// noding linestrings.
    namespace snapround {}
} // namespace geos.noding

/// Provides classes for implementing operations on geometries.
namespace operation { // geos.operation

    /// Provides classes for computing buffers of geometries
    namespace buffer {}

    /// Provides classes for computing the distance between geometries.
    namespace distance {}

    /// \brief Provides classes for computing the intersection of a Geometry
    /// and a clipping Rectangle.
    namespace intersection {}

    /// Line merging package.
    namespace linemerge {}

    /// \brief
    /// Contains classes that perform a topological overlay to compute boolean
    /// spatial functions.
    ///
    /// The Overlay Algorithm is used in spatial analysis methods for computing
    /// set-theoretic operations (boolean combinations) of input
    /// [Geometrys](\ref geom::Geometry).
    /// The algorithm for computing the overlay uses the intersection operations
    /// supported by topology graphs. To compute an overlay it is necessary to
    /// explicitly compute the resultant graph formed by the computed
    /// intersections.
    ///
    /// The algorithm to compute a set-theoretic spatial analysis method has the
    /// following steps:
    ///
    /// - Build topology graphs of the two input geometries. For each geometry
    ///   all self-intersection nodes are computed and added to the graph.
    /// - Compute nodes for all intersections between edges and nodes of the
    ///   graphs.
    /// - Compute the labeling for the computed nodes by merging the labels from
    ///   the input graphs.
    /// - Compute new edges between the compute intersection nodes.
    ///   Label the edges appropriately.
    /// - Build the resultant graph from the new nodes and edges.
    /// - Compute the labeling for isolated components of the graph. Add the
    ///   isolated components to the resultant graph.
    /// - Compute the result of the boolean combination by selecting the node
    ///   and edges with the appropriate labels. Polygonize areas and sew linear
    ///   geometries together.
    ///
    /// ### Package Specification
    ///
    /// - Java Topology Suite Technical Specifications
    /// - <A HREF="http://www.opengis.org/techno/specs.htm">
    ///   OpenGIS Simple Features Specification for SQL</A>
    namespace overlay {}

    /// An API for polygonizing sets of lines.
    namespace polygonize {}

    /// \brief Classes which implement topological predicates optimized for
    /// particular kinds of geometries.
    namespace predicate {}

    /// \brief Contains classes to implement the computation of the spatial
    /// relationships of `Geometry`s.
    ///
    /// The `relate` algorithm computes the `IntersectionMatrix` describing the
    /// relationship of two `Geometry`s. The algorithm for computing `relate`
    /// uses the intersection operations supported by topology graphs. Although
    /// the `relate` result depends on the resultant graph formed by the
    /// computed intersections, there is no need to explicitly compute the
    /// entire graph. It is sufficient to compute the local structure of the
    /// graph at each intersection node.
    ///
    /// The algorithm to compute `relate` has the following steps:
    ///
    /// - Build topology graphs of the two input geometries. For each geometry
    ///   all self-intersection nodes are computed and added to the graph.
    /// - Compute nodes for all intersections between edges and nodes of the
    ///   graphs.
    /// - Compute the labeling for the computed nodes by merging the labels
    ///   from the input graphs.
    /// - Compute the labeling for isolated components of the graph (see below)
    /// - Compute the `IntersectionMatrix` from the labels on the nodes and
    ///   edges.
    ///
    /// ### Labeling isolated components
    ///
    /// Isolated components are components (edges or nodes) of an input
    /// `Geometry` which do not contain any intersections with the other input
    /// `Geometry`. The topological relationship of these components to the
    /// other input `Geometry` must be computed in order to determine the
    /// complete labeling of the component. This can be done by testing whether
    /// the component lies in the interior or exterior of the other `Geometry`.
    /// If the other `Geometry` is 1-dimensional, the isolated component must
    /// lie in the exterior (since otherwise it would have an intersection with
    /// an edge of the `Geometry`). If the other `Geometry` is 2-dimensional,
    /// a Point-In-Polygon test can be used to determine whether the isolated
    /// component is in the interior or exterior.
    ///
    /// ### Package Specification
    ///
    /// - Java Topology Suite Technical Specifications
    /// - <A HREF="http://www.opengis.org/techno/specs.htm">
    ///    OpenGIS Simple Features Specification for SQL</A>
    namespace relate {}

    /// Find shared paths among two linear Geometry objects.
    namespace sharedpaths {}

    /// Classes to perform efficient unioning of collections of geometries.
    namespace geounion {}

    /// Provides classes for testing the validity of geometries.
    namespace valid {}
} // namespace geos.operation

/// Contains classes to implement a planar graph data structure.
namespace planargraph { // geos::planargraph

    /// Planargraph algorithms.
    namespace algorithm {}
} // namespace geos::planargraph

/// Provides classes for manipulating the precision model of Geometries.
namespace precision {}

/// Classes which implement algorithms for simplifying or generalizing geometries.
namespace simplify {}

/// Classes to compute Delaunay triangulations.
namespace triangulate { // geos.triangulate

    /// \brief Classes to implement a topological subdivision of quadeges, to
    /// support creating triangulations and Voronoi diagrams.
    namespace quadedge {}
} // namespace geos.triangulate

/// Utility classes for GEOS.
namespace util {}

} // namespace geos

#endif // GEOS_NAMESPACES_H
