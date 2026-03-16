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
 **********************************************************************/

#ifndef GEOS_INDEX_STRTREE_SIRTREE_H
#define GEOS_INDEX_STRTREE_SIRTREE_H

#include <geos/export.h>

#include <geos/index/strtree/AbstractSTRtree.h> // for inheritance
#include <geos/index/strtree/Interval.h> // for inline

#include <vector>
#include <memory>

namespace geos {
namespace index { // geos::index
namespace strtree { // geos::index::strtree

/** \brief
 * One-dimensional version of an STR-packed R-tree.
 *
 * SIR stands for "Sort-Interval-Recursive".
 *
 * STR-packed R-trees are described in:
 * P. Rigaux, Michel Scholl and Agnes Voisard. Spatial Databases With
 * Application To GIS. Morgan Kaufmann, San Francisco, 2002.
 *
 * @see STRtree
 */
class GEOS_DLL SIRtree: public AbstractSTRtree {
    using AbstractSTRtree::insert;
    using AbstractSTRtree::query;

public:

    /** \brief
     * Constructs an SIRtree with the default node capacity.
     */
    SIRtree();

    /** \brief
     * Constructs an SIRtree with the given maximum number of child nodes
     * that a node may have
     */
    SIRtree(std::size_t nodeCapacity);

    ~SIRtree() override;

    void insert(double x1, double x2, void* item);

    /**
     * Returns items whose bounds intersect the given bounds.
     * @param x1 possibly equal to x2
     * @param x2
     */
    std::vector<void*>*
    query(double x1, double x2)
    {
        std::vector<void*>* results = new std::vector<void*>();
        Interval interval(std::min(x1, x2), std::max(x1, x2));
        AbstractSTRtree::query(&interval, *results);
        return results;
    }

    /**
     * Returns items whose bounds intersect the given value.
     */
    std::vector<void*>*
    query(double x)
    {
        return query(x, x);
    }

    /**
     * Disable copy construction and assignment. Apparently needed to make this
     * class compile under MSVC. (See https://stackoverflow.com/q/29565299)
     */
    SIRtree(const SIRtree&) = delete;
    SIRtree& operator=(const SIRtree&) = delete;

protected:

    class SIRIntersectsOp: public AbstractSTRtree::IntersectsOp {
    public:
        bool intersects(const void* aBounds, const void* bBounds) override;
    };

    /** \brief
     * Sorts the childBoundables then divides them into groups of size M, where
     * M is the node capacity.
     */
    std::unique_ptr<BoundableList> createParentBoundables(
        BoundableList* childBoundables, int newLevel) override;

    AbstractNode* createNode(int level) override;

    IntersectsOp*
    getIntersectsOp() override
    {
        return intersectsOp;
    }

    std::unique_ptr<BoundableList> sortBoundables(const BoundableList* input);

private:
    IntersectsOp* intersectsOp;
    std::vector<std::unique_ptr<Interval>> intervals;
};


} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos

#endif // GEOS_INDEX_STRTREE_SIRTREE_H
