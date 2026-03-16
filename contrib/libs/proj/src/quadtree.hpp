/******************************************************************************
 * Project:  PROJ
 * Purpose:  Implementation of quadtree building and searching functions.
 *           Derived from shapelib, mapserver and GDAL implementations
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 1999-2008, Frank Warmerdam
 * Copyright (c) 2008-2020, Even Rouault <even dot rouault at spatialys.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

#ifndef QUADTREE_HPP
#define QUADTREE_HPP

#include "proj/util.hpp"

#include <functional>
#include <vector>

//! @cond Doxygen_Suppress

NS_PROJ_START

namespace QuadTree {

/* -------------------------------------------------------------------- */
/*      If the following is 0.5, psNodes will be split in half.  If it  */
/*      is 0.6 then each apSubNode will contain 60% of the parent       */
/*      psNode, with 20% representing overlap.  This can be help to     */
/*      prevent small objects on a boundary from shifting too high      */
/*      up the hQuadTree.                                               */
/* -------------------------------------------------------------------- */
constexpr double DEFAULT_SPLIT_RATIO = 0.55;

/** Describe a rectangle */
struct RectObj {
    double minx = 0; /**< Minimum x */
    double miny = 0; /**< Minimum y */
    double maxx = 0; /**< Maximum x */
    double maxy = 0; /**< Maximum y */

    /* Returns whether this rectangle is contained by other */
    inline bool isContainedBy(const RectObj &other) const {
        return minx >= other.minx && maxx <= other.maxx && miny >= other.miny &&
               maxy <= other.maxy;
    }

    /* Returns whether this rectangles overlaps other */
    inline bool overlaps(const RectObj &other) const {
        return minx <= other.maxx && maxx >= other.minx && miny <= other.maxy &&
               maxy >= other.miny;
    }

    /* Returns whether this rectangles contains the specified point */
    inline bool contains(double x, double y) const {
        return minx <= x && maxx >= x && miny <= y && maxy >= y;
    }

    /* Return whether this rectangles is different from other */
    inline bool operator!=(const RectObj &other) const {
        return minx != other.minx || miny != other.miny || maxx != other.maxx ||
               maxy != other.maxy;
    }
};

/** Quadtree */
template <class Feature> class QuadTree {

    struct Node {
        RectObj rect{}; /* area covered by this psNode */

        /* list of shapes stored at this node. */
        std::vector<std::pair<Feature, RectObj>> features{};

        std::vector<Node> subnodes{};

        explicit Node(const RectObj &rectIn) : rect(rectIn) {}
    };

    Node root{};
    unsigned nBucketCapacity = 8;
    double dfSplitRatio = DEFAULT_SPLIT_RATIO;

  public:
    /** Construct a new quadtree with the global bounds of all objects to be
     * inserted */
    explicit QuadTree(const RectObj &globalBounds) : root(globalBounds) {}

    /** Add a new feature, with its bounds specified in featureBounds */
    void insert(const Feature &feature, const RectObj &featureBounds) {
        insert(root, feature, featureBounds);
    }

#ifdef UNUSED
    /** Retrieve all features whose bounds intersects aoiRect */
    void
    search(const RectObj &aoiRect,
           std::vector<std::reference_wrapper<const Feature>> &features) const {
        search(root, aoiRect, features);
    }
#endif

    /** Retrieve all features whose bounds contains (x,y) */
    void search(double x, double y, std::vector<Feature> &features) const {
        search(root, x, y, features);
    }

  private:
    void splitBounds(const RectObj &in, RectObj &out1, RectObj &out2) {
        // The output bounds will be very similar to the input bounds,
        // so just copy over to start.
        out1 = in;
        out2 = in;

        // Split in X direction.
        if ((in.maxx - in.minx) > (in.maxy - in.miny)) {
            const double range = in.maxx - in.minx;

            out1.maxx = in.minx + range * dfSplitRatio;
            out2.minx = in.maxx - range * dfSplitRatio;
        }

        // Otherwise split in Y direction.
        else {
            const double range = in.maxy - in.miny;

            out1.maxy = in.miny + range * dfSplitRatio;
            out2.miny = in.maxy - range * dfSplitRatio;
        }
    }

    void insert(Node &node, const Feature &feature,
                const RectObj &featureBounds) {
        if (node.subnodes.empty()) {
            // If we have reached the max bucket capacity, try to insert
            // in a subnode if possible.
            if (node.features.size() >= nBucketCapacity) {
                RectObj half1;
                RectObj half2;
                RectObj quad1;
                RectObj quad2;
                RectObj quad3;
                RectObj quad4;

                splitBounds(node.rect, half1, half2);
                splitBounds(half1, quad1, quad2);
                splitBounds(half2, quad3, quad4);

                if (node.rect != quad1 && node.rect != quad2 &&
                    node.rect != quad3 && node.rect != quad4 &&
                    (featureBounds.isContainedBy(quad1) ||
                     featureBounds.isContainedBy(quad2) ||
                     featureBounds.isContainedBy(quad3) ||
                     featureBounds.isContainedBy(quad4))) {
                    node.subnodes.reserve(4);
                    node.subnodes.emplace_back(Node(quad1));
                    node.subnodes.emplace_back(Node(quad2));
                    node.subnodes.emplace_back(Node(quad3));
                    node.subnodes.emplace_back(Node(quad4));

                    auto features = std::move(node.features);
                    node.features.clear();
                    for (auto &pair : features) {
                        insert(node, pair.first, pair.second);
                    }

                    /* recurse back on this psNode now that it has apSubNodes */
                    insert(node, feature, featureBounds);
                    return;
                }
            }
        } else {
            // If we have sub nodes, then consider whether this object will
            // fit in them.
            for (auto &subnode : node.subnodes) {
                if (featureBounds.isContainedBy(subnode.rect)) {
                    insert(subnode, feature, featureBounds);
                    return;
                }
            }
        }

        // If none of that worked, just add it to this nodes list.
        node.features.push_back(
            std::pair<Feature, RectObj>(feature, featureBounds));
    }

#ifdef UNUSED
    void
    search(const Node &node, const RectObj &aoiRect,
           std::vector<std::reference_wrapper<const Feature>> &features) const {
        // Does this node overlap the area of interest at all?  If not,
        // return without adding to the list at all.
        if (!node.rect.overlaps(aoiRect))
            return;

        // Add the local features to the list.
        for (const auto &pair : node.features) {
            if (pair.second.overlaps(aoiRect)) {
                features.push_back(
                    std::reference_wrapper<const Feature>(pair.first));
            }
        }

        // Recurse to subnodes if they exist.
        for (const auto &subnode : node.subnodes) {
            search(subnode, aoiRect, features);
        }
    }
#endif

    static void search(const Node &node, double x, double y,
                       std::vector<Feature> &features) {
        // Does this node overlap the area of interest at all?  If not,
        // return without adding to the list at all.
        if (!node.rect.contains(x, y))
            return;

        // Add the local features to the list.
        for (const auto &pair : node.features) {
            if (pair.second.contains(x, y)) {
                features.push_back(pair.first);
            }
        }

        // Recurse to subnodes if they exist.
        for (const auto &subnode : node.subnodes) {
            search(subnode, x, y, features);
        }
    }
};

} // namespace QuadTree

NS_PROJ_END

//! @endcond

#endif //  QUADTREE_HPP
