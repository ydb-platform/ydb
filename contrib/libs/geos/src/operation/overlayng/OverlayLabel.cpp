/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/operation/overlayng/OverlayLabel.h>

namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

/*public*/
void
OverlayLabel::initBoundary(int index, Location locLeft, Location locRight, bool p_isHole)
{
    if (index == 0) {
        aDim = DIM_BOUNDARY;
        aIsHole = p_isHole;
        aLocLeft = locLeft;
        aLocRight = locRight;
        aLocLine = Location::INTERIOR;
    }
    else {
        bDim = DIM_BOUNDARY;
        bIsHole = p_isHole;
        bLocLeft = locLeft;
        bLocRight = locRight;
        bLocLine = Location::INTERIOR;
    }
}

/*public*/
void
OverlayLabel::initCollapse(int index, bool p_isHole)
{
    if (index == 0) {
        aDim = DIM_COLLAPSE;
        aIsHole = p_isHole;
    }
    else {
        bDim = DIM_COLLAPSE;
        bIsHole = p_isHole;
    }
}

/*public*/
void
OverlayLabel::initLine(int index)
{
    if (index == 0) {
        aDim = DIM_LINE;
        aLocLine = LOC_UNKNOWN;
    }
    else {
        bDim = DIM_LINE;
        bLocLine = LOC_UNKNOWN;
    }
}

/*public*/
void
OverlayLabel::initNotPart(int index)
{
    // this assumes locations are initialized to UNKNOWN
    if (index == 0) {
        aDim = DIM_NOT_PART;
    }
    else {
        bDim = DIM_NOT_PART;
    }
}

/*public*/
void
OverlayLabel::setLocationLine(int index, Location loc)
{
    if (index == 0) {
        aLocLine = loc;
    }
    else {
        bLocLine = loc;
    }
}

/*public*/
void
OverlayLabel::setLocationAll(int index, Location loc)
{
    if (index == 0) {
        aLocLine = loc;
        aLocLeft = loc;
        aLocRight = loc;
    }
    else {
        bLocLine = loc;
        bLocLeft = loc;
        bLocRight = loc;
    }
}

/*public*/
void
OverlayLabel::setLocationCollapse(int index)
{
    Location loc = isHole(index) ? Location::INTERIOR : Location::EXTERIOR;
    if (index == 0) {
        aLocLine = loc;
    }
    else {
        bLocLine = loc;
    }
}

/*public*/
bool
OverlayLabel::isLine() const
{
    return aDim == DIM_LINE || bDim == DIM_LINE;
}

/*public*/
bool
OverlayLabel::isLine(int index) const
{
    return index == 0 ? aDim == DIM_LINE : bDim == DIM_LINE;
}

/*public*/
bool
OverlayLabel::isLinear(int index) const
{
    if (index == 0) {
        return aDim == DIM_LINE || aDim == DIM_COLLAPSE;
    }
    return bDim == DIM_LINE || bDim == DIM_COLLAPSE;
}

/*public*/
bool
OverlayLabel::isKnown(int index) const
{
    if (index == 0) {
        return aDim != DIM_UNKNOWN;
    }
    return bDim != DIM_UNKNOWN;
}

/*public*/
bool
OverlayLabel::isNotPart(int index) const
{
    if (index == 0) {
        return aDim == DIM_NOT_PART;
    }
    return bDim == DIM_NOT_PART;
}

/*public*/
bool
OverlayLabel::isBoundaryEither() const
{
    return aDim == DIM_BOUNDARY || bDim == DIM_BOUNDARY;
}

/*public*/
bool
OverlayLabel::isBoundaryBoth() const
{
    return aDim == DIM_BOUNDARY && bDim == DIM_BOUNDARY;
}

/*public*/
bool
OverlayLabel::isBoundaryCollapse() const
{
    if (isLine()) return false;
    return ! isBoundaryBoth();
}

/*public*/
bool
OverlayLabel::isBoundaryTouch() const
{
    return isBoundaryBoth() &&
        getLocation(0, Position::RIGHT, true) != getLocation(1, Position::RIGHT, true);
}

/*public*/
bool
OverlayLabel::isBoundary(int index) const
{
    if (index == 0) {
        return aDim == DIM_BOUNDARY;
    }
    return bDim == DIM_BOUNDARY;
}

/*public*/
bool
OverlayLabel::isBoundarySingleton() const
{
    if (aDim == DIM_BOUNDARY && bDim == DIM_NOT_PART) {
        return true;
    }

    if (bDim == DIM_BOUNDARY && aDim == DIM_NOT_PART) {
        return true;
    }

    return false;
}

/*public*/
bool
OverlayLabel::isLineLocationUnknown(int index) const
{
    if (index == 0) {
        return aLocLine == LOC_UNKNOWN;
    }
    else {
        return bLocLine == LOC_UNKNOWN;
    }
}

/*public*/
bool
OverlayLabel::isLineInArea(int index) const
{
    if (index == 0) {
        return aLocLine == Location::INTERIOR;
    }
    return bLocLine == Location::INTERIOR;
}

/*public*/
bool
OverlayLabel::isHole(int index) const
{
    if (index == 0) {
        return aIsHole;
    }
    else {
        return bIsHole;
    }
}

/*public*/
bool
OverlayLabel::isCollapse(int index) const
{
    return dimension(index) == DIM_COLLAPSE;
}

/*public*/
bool
OverlayLabel::isInteriorCollapse() const
{
    if (aDim == DIM_COLLAPSE && aLocLine == Location::INTERIOR)
        return true;
    if (bDim == DIM_COLLAPSE && bLocLine == Location::INTERIOR)
        return true;

    return false;
}

/*public*/
bool
OverlayLabel::isCollapseAndNotPartInterior() const
{
    if (aDim == DIM_COLLAPSE &&
        bDim == DIM_NOT_PART &&
        bLocLine == Location::INTERIOR)
        return true;

    if (bDim == DIM_COLLAPSE &&
        aDim == DIM_NOT_PART &&
        aLocLine == Location::INTERIOR)
        return true;

    return false;
}

/*public*/
Location
OverlayLabel::getLineLocation(int index) const
{
    if (index == 0) {
        return aLocLine;
    }
    else {
        return bLocLine;
    }
}

/*public*/
bool
OverlayLabel::isLineInterior(int index) const
{
    if (index == 0) {
        return aLocLine == Location::INTERIOR;
    }
    return bLocLine == Location::INTERIOR;
}

/*public*/
Location
OverlayLabel::getLocation(int index, int position, bool isForward) const
{
    if (index == 0) {
        switch (position) {
        case Position::LEFT:
            return isForward ? aLocLeft : aLocRight;
        case Position::RIGHT:
            return isForward ? aLocRight : aLocLeft;
        case Position::ON:
            return aLocLine;
        }
    }

    switch (position) {
    case Position::LEFT:
        return isForward ? bLocLeft : bLocRight;
    case Position::RIGHT:
        return isForward ? bLocRight : bLocLeft;
    case Position::ON:
        return bLocLine;
    }

    return LOC_UNKNOWN;
}


/*public*/
Location
OverlayLabel::getLocationBoundaryOrLine(int index, int pos, bool isForward) const
{
    if (isBoundary(index)) {
        return getLocation(index, pos, isForward);
    }
    return getLineLocation(index);
}


/*public*/
Location
OverlayLabel::getLocation(int index) const {
    if (index == 0) {
        return aLocLine;
    }
    return bLocLine;
}

/*public*/
bool
OverlayLabel::hasSides(int index) const {
    if (index == 0) {
        return aLocLeft != LOC_UNKNOWN
            || aLocRight != LOC_UNKNOWN;
    }
    return bLocLeft != LOC_UNKNOWN
        || bLocRight != LOC_UNKNOWN;
}

/*public*/
OverlayLabel
OverlayLabel::copy() const
{
    OverlayLabel lbl = *this;
    return lbl;
}


/*private*/
std::string
OverlayLabel::dimensionSymbol(int dim) const
{
    switch (dim) {
    case DIM_LINE: return std::string("L");
    case DIM_COLLAPSE: return std::string("C");
    case DIM_BOUNDARY: return std::string("B");
    }
    return std::string("U");
}

/*public static*/
std::ostream&
operator<<(std::ostream& os, const OverlayLabel& ol)
{
    ol.toString(true, os);
    return os;
}

/*public*/
void
OverlayLabel::toString(bool isForward, std::ostream& os) const
{
    os << "A:";
    locationString(0, isForward, os);
    os << "/B:";
    locationString(1, isForward, os);
}

/*private*/
void
OverlayLabel::locationString(int index, bool isForward, std::ostream& os) const
{
    if (isBoundary(index)) {
        os << getLocation(index, Position::LEFT, isForward);
        os << getLocation(index, Position::RIGHT, isForward);
    }
    else {
        os << (index == 0 ? aLocLine : bLocLine);
    }
    if (isKnown(index))
    {
      os << dimensionSymbol(index == 0 ? aDim : bDim);
    }
    if (isCollapse(index)) {
        bool p_isHole = (index == 0 ? aIsHole : bIsHole);
        if (p_isHole)
            os << "h";
        else
            os << "s";
    }
}



} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
