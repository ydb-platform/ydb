/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 *
 **********************************************************************/

#ifndef GEOS_INDEX_INTERVALRTREE_INTERVALRTREENODE_H
#define GEOS_INDEX_INTERVALRTREE_INTERVALRTREENODE_H

#include <geos/constants.h>
#include <vector>
#include <limits>

// forward declarations
namespace geos {
namespace index {
class ItemVisitor;
}
}


namespace geos {
namespace index {
namespace intervalrtree {

class IntervalRTreeNode {
private:
protected:
    double min;
    double max;

    bool
    intersects(double queryMin, double queryMax) const
    {
        if(min > queryMax || max < queryMin) {
            return false;
        }

        return true;
    }

public:
    typedef std::vector<const IntervalRTreeNode*> ConstVect;

    IntervalRTreeNode()
        :	min(DoubleInfinity),
          max(DoubleNegInfinity)
    { }

    IntervalRTreeNode(double p_min, double p_max)
        :	min(p_min),
          max(p_max)
    { }

    virtual
    ~IntervalRTreeNode()
    { }

    double
    getMin() const
    {
        return min;
    }

    double
    getMax() const
    {
        return max;
    }

    virtual void query(double queryMin, double queryMax, ItemVisitor* visitor) const = 0;

    //std::string toString()
    //{
    //	return WKTWriter.toLineString(new Coordinate(min, 0), new Coordinate(max, 0));
    //}


    //class NodeComparator
    //{
    //public:
    static bool
    compare(const IntervalRTreeNode* n1, const IntervalRTreeNode* n2)
    {
        double mid1 = n1->getMin() + n1->getMax();
        double mid2 = n2->getMin() + n2->getMax();

        return mid1 > mid2;
    }
    //};

};

} // geos::index::intervalrtree
} // geos::index
} // geos

#endif // GEOS_INDEX_INTERVALRTREE_INTERVALRTREENODE_H

