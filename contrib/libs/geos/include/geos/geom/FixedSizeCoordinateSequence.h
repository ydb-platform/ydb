/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_FIXEDSIZECOORDINATESEQUENCE_H
#define GEOS_GEOM_FIXEDSIZECOORDINATESEQUENCE_H

#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateFilter.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/util.h>

#include <algorithm>
#include <array>
#include <memory>
#include <sstream>
#include <vector>

namespace geos {
namespace geom {

    template<size_t N>
    class FixedSizeCoordinateSequence : public CoordinateSequence {
    public:
        explicit FixedSizeCoordinateSequence(size_t dimension_in = 0) : dimension(dimension_in) {}

        std::unique_ptr<CoordinateSequence> clone() const final override {
            auto seq = detail::make_unique<FixedSizeCoordinateSequence<N>>(dimension);
            seq->m_data = m_data;
            return std::move(seq); // move needed for gcc 4.8
        }

        const Coordinate& getAt(size_t i) const final override {
            return m_data[i];
        }

        void getAt(size_t i, Coordinate& c) const final override {
            c = m_data[i];
        }

        size_t getSize() const final override {
            return N;
        }

        bool isEmpty() const final override {
            return N == 0;
        }

        void setAt(const Coordinate & c, size_t pos) final override {
            m_data[pos] = c;
        }

        void setOrdinate(size_t index, size_t ordinateIndex, double value) final override
        {
            switch(ordinateIndex) {
                case CoordinateSequence::X:
                    m_data[index].x = value;
                    break;
                case CoordinateSequence::Y:
                    m_data[index].y = value;
                    break;
                case CoordinateSequence::Z:
                    m_data[index].z = value;
                    break;
                default: {
                    std::stringstream ss;
                    ss << "Unknown ordinate index " << ordinateIndex;
                    throw geos::util::IllegalArgumentException(ss.str());
                    break;
                }
            }
        }

        size_t getDimension() const final override {
            if(dimension != 0) {
                return dimension;
            }

            if(isEmpty()) {
                return 3;
            }

            if(std::isnan(m_data[0].z)) {
                dimension = 2;
            }
            else {
                dimension = 3;
            }

            return dimension;
        }

        void toVector(std::vector<Coordinate> & out) const final override {
            out.insert(out.end(), m_data.begin(), m_data.end());
        }

        void setPoints(const std::vector<Coordinate> & v) final override {
            std::copy(v.begin(), v.end(), m_data.begin());
        }

        void apply_ro(CoordinateFilter* filter) const final override {
            std::for_each(m_data.begin(), m_data.end(),
                    [&filter](const Coordinate & c) { filter->filter_ro(&c); });
        }

        void apply_rw(const CoordinateFilter* filter) final override {
            std::for_each(m_data.begin(), m_data.end(),
                    [&filter](Coordinate &c) { filter->filter_rw(&c); });
            dimension = 0; // re-check (see http://trac.osgeo.org/geos/ticket/435)
        }

    private:
        std::array<Coordinate, N> m_data;
        mutable std::size_t dimension;
    };

}
}

#endif
