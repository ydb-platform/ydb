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
 * Last port: operation/buffer/BufferParameters.java r278 (JTS-1.12)
 *
 **********************************************************************/

#include <cstdlib> // for std::abs()
#include <cmath> // for cos

#include <geos/constants.h>
#include <geos/operation/buffer/BufferParameters.h>


namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

// public static const
const double BufferParameters::DEFAULT_MITRE_LIMIT = 5.0;

// public
BufferParameters::BufferParameters()
    :
    quadrantSegments(DEFAULT_QUADRANT_SEGMENTS),
    endCapStyle(CAP_ROUND),
    joinStyle(JOIN_ROUND),
    mitreLimit(DEFAULT_MITRE_LIMIT),
    _isSingleSided(false)
{}

// public
BufferParameters::BufferParameters(int p_quadrantSegments)
    :
    quadrantSegments(DEFAULT_QUADRANT_SEGMENTS),
    endCapStyle(CAP_ROUND),
    joinStyle(JOIN_ROUND),
    mitreLimit(DEFAULT_MITRE_LIMIT),
    _isSingleSided(false)
{
    setQuadrantSegments(p_quadrantSegments);
}

// public
BufferParameters::BufferParameters(int p_quadrantSegments,
                                   EndCapStyle p_endCapStyle)
    :
    quadrantSegments(DEFAULT_QUADRANT_SEGMENTS),
    endCapStyle(CAP_ROUND),
    joinStyle(JOIN_ROUND),
    mitreLimit(DEFAULT_MITRE_LIMIT),
    _isSingleSided(false)
{
    setQuadrantSegments(p_quadrantSegments);
    setEndCapStyle(p_endCapStyle);
}

// public
BufferParameters::BufferParameters(int p_quadrantSegments,
                                   EndCapStyle p_endCapStyle,
                                   JoinStyle p_joinStyle,
                                   double p_mitreLimit)
    :
    quadrantSegments(DEFAULT_QUADRANT_SEGMENTS),
    endCapStyle(CAP_ROUND),
    joinStyle(JOIN_ROUND),
    mitreLimit(DEFAULT_MITRE_LIMIT),
    _isSingleSided(false)
{
    setQuadrantSegments(p_quadrantSegments);
    setEndCapStyle(p_endCapStyle);
    setJoinStyle(p_joinStyle);
    setMitreLimit(p_mitreLimit);
}

// public
void
BufferParameters::setQuadrantSegments(int quadSegs)
{
    quadrantSegments = quadSegs;

    /*
     * Indicates how to construct fillets.
     * If qs >= 1, fillet is round, and qs indicates number of
     * segments to use to approximate a quarter-circle.
     * If qs = 0, fillet is bevelled flat (i.e. no filleting is performed)
     * If qs < 0, fillet is mitred, and absolute value of qs
     * indicates maximum length of mitre according to
     *
     * mitreLimit = |qs|
     */
    if(quadrantSegments == 0) {
        joinStyle = JOIN_BEVEL;
    }
    if(quadrantSegments < 0) {
        joinStyle = JOIN_MITRE;
        mitreLimit = std::abs(quadrantSegments);
    }

    if(quadSegs <= 0) {
        quadrantSegments = 1;
    }

    /*
     * If join style was set by the quadSegs value,
     * use the default for the actual quadrantSegments value.
     */
    if(joinStyle != JOIN_ROUND) {
        quadrantSegments = DEFAULT_QUADRANT_SEGMENTS;
    }
}

// public static
double
BufferParameters::bufferDistanceError(int quadSegs)
{
    double alpha = MATH_PI / 2.0 / quadSegs;
    return 1 - cos(alpha / 2.0);
}

} // namespace geos.operation.buffer
} // namespace geos.operation
} // namespace geos

