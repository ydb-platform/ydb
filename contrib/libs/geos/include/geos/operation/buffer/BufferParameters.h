/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/BufferParameters.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_BUFFER_BUFFERPARAMETERS_H
#define GEOS_OP_BUFFER_BUFFERPARAMETERS_H

#include <geos/export.h>

//#include <vector>

//#include <geos/algorithm/LineIntersector.h> // for composition
//#include <geos/geom/Coordinate.h> // for composition
//#include <geos/geom/LineSegment.h> // for composition

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
class PrecisionModel;
}
namespace operation {
namespace buffer {
class OffsetCurveVertexList;
}
}
}

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/** \brief
 * Contains the parameters which
 * describe how a buffer should be constructed.
 *
 */
class GEOS_DLL BufferParameters {

public:

    /// End cap styles
    enum EndCapStyle {

        /// Specifies a round line buffer end cap style.
        CAP_ROUND = 1,

        /// Specifies a flat line buffer end cap style.
        CAP_FLAT = 2,

        /// Specifies a square line buffer end cap style.
        CAP_SQUARE = 3
    };

    /// Join styles
    enum JoinStyle {

        /// Specifies a round join style.
        JOIN_ROUND = 1,

        /// Specifies a mitre join style.
        JOIN_MITRE = 2,

        /// Specifies a bevel join style.
        JOIN_BEVEL = 3
    };

    /// \brief
    /// The default number of facets into which to divide a fillet
    /// of 90 degrees.
    ///
    /// A value of 8 gives less than 2% max error in the buffer distance.
    /// For a max error of < 1%, use QS = 12.
    /// For a max error of < 0.1%, use QS = 18.
    ///
    static constexpr int DEFAULT_QUADRANT_SEGMENTS = 8;

    /// The default mitre limit
    ///
    /// Allows fairly pointy mitres.
    ///
    static const double DEFAULT_MITRE_LIMIT; // 5.0 (in .cpp file)

    /// Creates a default set of parameters
    BufferParameters();

    /// Creates a set of parameters with the given quadrantSegments value.
    ///
    /// @param quadrantSegments the number of quadrant segments to use
    ///
    BufferParameters(int quadrantSegments);

    /// \brief
    /// Creates a set of parameters with the
    /// given quadrantSegments and endCapStyle values.
    ///
    /// @param quadrantSegments the number of quadrant segments to use
    /// @param endCapStyle the end cap style to use
    ///
    BufferParameters(int quadrantSegments, EndCapStyle endCapStyle);

    /// \brief
    /// Creates a set of parameters with the
    /// given parameter values.
    ///
    /// @param quadrantSegments the number of quadrant segments to use
    /// @param endCapStyle the end cap style to use
    /// @param joinStyle the join style to use
    /// @param mitreLimit the mitre limit to use
    ///
    BufferParameters(int quadrantSegments, EndCapStyle endCapStyle,
                     JoinStyle joinStyle, double mitreLimit);

    /// Gets the number of quadrant segments which will be used
    ///
    /// @return the number of quadrant segments
    ///
    int
    getQuadrantSegments() const
    {
        return quadrantSegments;
    }

    /// \brief
    /// Sets the number of line segments used to approximate
    /// an angle fillet.
    ///
    /// - If <tt>quadSegs</tt> >= 1, joins are round,
    ///   and <tt>quadSegs</tt> indicates the number of
    ///   segments to use to approximate a quarter-circle.
    /// - If <tt>quadSegs</tt> = 0, joins are bevelled (flat)
    /// - If <tt>quadSegs</tt> < 0, joins are mitred, and the value of qs
    ///   indicates the mitre ration limit as
    ///   <pre>
    ///    mitreLimit = |<tt>quadSegs</tt>|
    ///    </pre>
    ///
    /// For round joins, <tt>quadSegs</tt> determines the maximum
    /// error in the approximation to the true buffer curve.
    ///
    /// The default value of 8 gives less than 2% max error in the
    /// buffer distance.
    ///
    /// For a max error of < 1%, use QS = 12.
    /// For a max error of < 0.1%, use QS = 18.
    /// The error is always less than the buffer distance
    /// (in other words, the computed buffer curve is always inside
    ///  the true curve).
    ///
    /// @param quadSegs the number of segments in a fillet for a quadrant
    void setQuadrantSegments(int quadSegs);

    /// \brief
    /// Computes the maximum distance error due to a given level
    /// of approximation to a true arc.
    ///
    /// @param quadSegs the number of segments used to approximate
    ///                 a quarter-circle
    /// @return the error of approximation
    ///
    static double bufferDistanceError(int quadSegs);

    /// Gets the end cap style.
    ///
    /// @return the end cap style
    ///
    EndCapStyle
    getEndCapStyle() const
    {
        return endCapStyle;
    }

    /// Specifies the end cap style of the generated buffer.
    ///
    /// The styles supported are CAP_ROUND, CAP_BUTT,
    /// and CAP_SQUARE.
    ///
    /// The default is CAP_ROUND.
    ///
    /// @param style the end cap style to specify
    ///
    void
    setEndCapStyle(EndCapStyle style)
    {
        endCapStyle = style;
    }

    /// Gets the join style.
    ///
    /// @return the join style
    ///
    JoinStyle
    getJoinStyle() const
    {
        return joinStyle;
    }

    /// \brief
    /// Sets the join style for outside (reflex) corners between
    /// line segments.
    ///
    /// Allowable values are JOIN_ROUND (which is the default),
    /// JOIN_MITRE and JOIN_BEVEL.
    ///
    /// @param style the code for the join style
    ///
    void
    setJoinStyle(JoinStyle style)
    {
        joinStyle = style;
    }

    /// Gets the mitre ratio limit.
    ///
    /// @return the limit value
    ///
    double
    getMitreLimit() const
    {
        return mitreLimit;
    }

    /// Sets the limit on the mitre ratio used for very sharp corners.
    ///
    /// The mitre ratio is the ratio of the distance from the corner
    /// to the end of the mitred offset corner.
    /// When two line segments meet at a sharp angle,
    /// a miter join will extend far beyond the original geometry.
    /// (and in the extreme case will be infinitely far.)
    /// To prevent unreasonable geometry, the mitre limit
    /// allows controlling the maximum length of the join corner.
    /// Corners with a ratio which exceed the limit will be beveled.
    ///
    /// @param limit the mitre ratio limit
    ///
    void
    setMitreLimit(double limit)
    {
        mitreLimit = limit;
    }

    /**
     * Sets whether the computed buffer should be single-sided.
     * A single-sided buffer is constructed on only one side of each input line.
     *
     * The side used is determined by the sign of the buffer distance:
     * - a positive distance indicates the left-hand side
     * - a negative distance indicates the right-hand side
     *
     * The single-sided buffer of point geometries is
     * the same as the regular buffer.
     *
     * The End Cap Style for single-sided buffers is
     * always ignored,
     * and forced to the equivalent of <tt>CAP_FLAT</tt>.
     *
     * @param p_isSingleSided true if a single-sided buffer should be constructed
     */
    void
    setSingleSided(bool p_isSingleSided)
    {
        _isSingleSided = p_isSingleSided;
    }

    /**
     * Tests whether the buffer is to be generated on a single side only.
     *
     * @return true if the generated buffer is to be single-sided
     */
    bool
    isSingleSided() const
    {
        return _isSingleSided;
    }


private:

    /// Defaults to DEFAULT_QUADRANT_SEGMENTS;
    int quadrantSegments;

    /// Defaults to CAP_ROUND;
    EndCapStyle endCapStyle;

    /// Defaults to JOIN_ROUND;
    JoinStyle joinStyle;

    /// Defaults to DEFAULT_MITRE_LIMIT;
    double mitreLimit;

    bool _isSingleSided;
};

} // namespace geos::operation::buffer
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_BUFFER_BUFFERPARAMETERS_H

