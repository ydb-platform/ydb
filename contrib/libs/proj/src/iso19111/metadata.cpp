/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  ISO19111:2019 implementation
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2018, Even Rouault <even dot rouault at spatialys dot com>
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
 ****************************************************************************/

#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

#include "proj/metadata.hpp"
#include "proj/common.hpp"
#include "proj/io.hpp"
#include "proj/util.hpp"

#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"

#include "proj_json_streaming_writer.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <vector>

using namespace NS_PROJ::internal;
using namespace NS_PROJ::io;
using namespace NS_PROJ::util;

#if 0
namespace dropbox{ namespace oxygen {
template<> nn<std::shared_ptr<NS_PROJ::metadata::Citation>>::~nn() = default;
template<> nn<NS_PROJ::metadata::ExtentPtr>::~nn() = default;
template<> nn<NS_PROJ::metadata::GeographicBoundingBoxPtr>::~nn() = default;
template<> nn<NS_PROJ::metadata::GeographicExtentPtr>::~nn() = default;
template<> nn<NS_PROJ::metadata::VerticalExtentPtr>::~nn() = default;
template<> nn<NS_PROJ::metadata::TemporalExtentPtr>::~nn() = default;
template<> nn<NS_PROJ::metadata::IdentifierPtr>::~nn() = default;
template<> nn<NS_PROJ::metadata::PositionalAccuracyPtr>::~nn() = default;
}}
#endif

NS_PROJ_START
namespace metadata {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct Citation::Private {
    optional<std::string> title{};
};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Citation::Citation() : d(std::make_unique<Private>()) {}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Constructs a citation by its title. */
Citation::Citation(const std::string &titleIn)
    : d(std::make_unique<Private>()) {
    d->title = titleIn;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Citation::Citation(const Citation &other)
    : d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

Citation::~Citation() = default;

// ---------------------------------------------------------------------------

Citation &Citation::operator=(const Citation &other) {
    if (this != &other) {
        *d = *other.d;
    }
    return *this;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns the name by which the cited resource is known. */
const optional<std::string> &Citation::title() PROJ_PURE_DEFN {
    return d->title;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct GeographicExtent::Private {};
//! @endcond

// ---------------------------------------------------------------------------

GeographicExtent::GeographicExtent() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
GeographicExtent::~GeographicExtent() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct GeographicBoundingBox::Private {
    double west_{};
    double south_{};
    double east_{};
    double north_{};

    Private(double west, double south, double east, double north)
        : west_(west), south_(south), east_(east), north_(north) {}

    bool intersects(const Private &other) const;

    std::unique_ptr<Private> intersection(const Private &other) const;
};
//! @endcond

// ---------------------------------------------------------------------------

GeographicBoundingBox::GeographicBoundingBox(double west, double south,
                                             double east, double north)
    : GeographicExtent(),
      d(std::make_unique<Private>(west, south, east, north)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
GeographicBoundingBox::~GeographicBoundingBox() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns the western-most coordinate of the limit of the dataset
 * extent.
 *
 * The unit is degrees.
 *
 * If eastBoundLongitude < westBoundLongitude(), then the bounding box crosses
 * the anti-meridian.
 */
double GeographicBoundingBox::westBoundLongitude() PROJ_PURE_DEFN {
    return d->west_;
}

// ---------------------------------------------------------------------------

/** \brief Returns the southern-most coordinate of the limit of the dataset
 * extent.
 *
 * The unit is degrees.
 */
double GeographicBoundingBox::southBoundLatitude() PROJ_PURE_DEFN {
    return d->south_;
}

// ---------------------------------------------------------------------------

/** \brief Returns the eastern-most coordinate of the limit of the dataset
 * extent.
 *
 * The unit is degrees.
 *
 * If eastBoundLongitude < westBoundLongitude(), then the bounding box crosses
 * the anti-meridian.
 */
double GeographicBoundingBox::eastBoundLongitude() PROJ_PURE_DEFN {
    return d->east_;
}

// ---------------------------------------------------------------------------

/** \brief Returns the northern-most coordinate of the limit of the dataset
 * extent.
 *
 * The unit is degrees.
 */
double GeographicBoundingBox::northBoundLatitude() PROJ_PURE_DEFN {
    return d->north_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GeographicBoundingBox.
 *
 * If east < west, then the bounding box crosses the anti-meridian.
 *
 * @param west Western-most coordinate of the limit of the dataset extent (in
 * degrees).
 * @param south Southern-most coordinate of the limit of the dataset extent (in
 * degrees).
 * @param east Eastern-most coordinate of the limit of the dataset extent (in
 * degrees).
 * @param north Northern-most coordinate of the limit of the dataset extent (in
 * degrees).
 * @return a new GeographicBoundingBox.
 */
GeographicBoundingBoxNNPtr GeographicBoundingBox::create(double west,
                                                         double south,
                                                         double east,
                                                         double north) {
    if (std::isnan(west) || std::isnan(south) || std::isnan(east) ||
        std::isnan(north)) {
        throw InvalidValueTypeException(
            "GeographicBoundingBox::create() does not accept NaN values");
    }
    if (south > north) {
        throw InvalidValueTypeException(
            "GeographicBoundingBox::create() does not accept south > north");
    }
    // Avoid creating a degenerate bounding box if reduced to a point or a line
    if (west == east) {
        if (west > -180)
            west =
                std::nextafter(west, -std::numeric_limits<double>::infinity());
        if (east < 180)
            east =
                std::nextafter(east, std::numeric_limits<double>::infinity());
    }
    if (south == north) {
        if (south > -90)
            south =
                std::nextafter(south, -std::numeric_limits<double>::infinity());
        if (north < 90)
            north =
                std::nextafter(north, std::numeric_limits<double>::infinity());
    }
    return GeographicBoundingBox::nn_make_shared<GeographicBoundingBox>(
        west, south, east, north);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool GeographicBoundingBox::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion,
    const io::DatabaseContextPtr &) const {
    auto otherExtent = dynamic_cast<const GeographicBoundingBox *>(other);
    if (!otherExtent)
        return false;
    return d->west_ == otherExtent->d->west_ &&
           d->south_ == otherExtent->d->south_ &&
           d->east_ == otherExtent->d->east_ &&
           d->north_ == otherExtent->d->north_;
}
//! @endcond

// ---------------------------------------------------------------------------

bool GeographicBoundingBox::contains(const GeographicExtentNNPtr &other) const {
    auto otherExtent = dynamic_cast<const GeographicBoundingBox *>(other.get());
    if (!otherExtent) {
        return false;
    }
    const double W = d->west_;
    const double E = d->east_;
    const double N = d->north_;
    const double S = d->south_;
    const double oW = otherExtent->d->west_;
    const double oE = otherExtent->d->east_;
    const double oN = otherExtent->d->north_;
    const double oS = otherExtent->d->south_;

    if (!(S <= oS && N >= oN)) {
        return false;
    }

    if (W == -180.0 && E == 180.0) {
        return oW != oE;
    }

    if (oW == -180.0 && oE == 180.0) {
        return false;
    }

    // Normal bounding box ?
    if (W < E) {
        if (oW < oE) {
            return W <= oW && E >= oE;
        } else {
            return false;
        }
        // No: crossing antimerian
    } else {
        if (oW < oE) {
            if (oW >= W) {
                return true;
            } else if (oE <= E) {
                return true;
            } else {
                return false;
            }
        } else {
            return W <= oW && E >= oE;
        }
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool GeographicBoundingBox::Private::intersects(const Private &other) const {
    const double W = west_;
    const double E = east_;
    const double N = north_;
    const double S = south_;
    const double oW = other.west_;
    const double oE = other.east_;
    const double oN = other.north_;
    const double oS = other.south_;

    // Check intersection along the latitude axis
    if (N < oS || S > oN) {
        return false;
    }

    // Check world coverage of this bbox, and other bbox overlapping
    // antimeridian (e.g. oW=175 and oE=-175)
    // Check oW > oE written for symmetry with the intersection() method.
    if (W == -180.0 && E == 180.0 && oW > oE) {
        return true;
    }

    // Check world coverage of other bbox, and this bbox overlapping
    // antimeridian (e.g. W=175 and E=-175)
    // Check W > E written for symmetry with the intersection() method.
    if (oW == -180.0 && oE == 180.0 && W > E) {
        return true;
    }

    // Normal bounding box ?
    if (W <= E) {
        if (oW <= oE) {
            if (std::max(W, oW) < std::min(E, oE)) {
                return true;
            }
            return false;
        }

        // Bail out on longitudes not in [-180,180]. We could probably make
        // some sense of them, but this check at least avoid potential infinite
        // recursion.
        if (oW > 180 || oE < -180) {
            return false;
        }

        return intersects(Private(oW, oS, 180.0, oN)) ||
               intersects(Private(-180.0, oS, oE, oN));

        // No: crossing antimeridian
    } else {
        if (oW <= oE) {
            return other.intersects(*this);
        }

        return true;
    }
}
//! @endcond

bool GeographicBoundingBox::intersects(
    const GeographicExtentNNPtr &other) const {
    auto otherExtent = dynamic_cast<const GeographicBoundingBox *>(other.get());
    if (!otherExtent) {
        return false;
    }
    return d->intersects(*(otherExtent->d));
}

// ---------------------------------------------------------------------------

GeographicExtentPtr
GeographicBoundingBox::intersection(const GeographicExtentNNPtr &other) const {
    auto otherExtent = dynamic_cast<const GeographicBoundingBox *>(other.get());
    if (!otherExtent) {
        return nullptr;
    }
    auto ret = d->intersection(*(otherExtent->d));
    if (ret) {
        auto bbox = GeographicBoundingBox::create(ret->west_, ret->south_,
                                                  ret->east_, ret->north_);
        return bbox.as_nullable();
    }
    return nullptr;
}

//! @cond Doxygen_Suppress
std::unique_ptr<GeographicBoundingBox::Private>
GeographicBoundingBox::Private::intersection(const Private &otherExtent) const {
    const double W = west_;
    const double E = east_;
    const double N = north_;
    const double S = south_;
    const double oW = otherExtent.west_;
    const double oE = otherExtent.east_;
    const double oN = otherExtent.north_;
    const double oS = otherExtent.south_;

    // Check intersection along the latitude axis
    if (N < oS || S > oN) {
        return nullptr;
    }

    // Check world coverage of this bbox, and other bbox overlapping
    // antimeridian (e.g. oW=175 and oE=-175)
    if (W == -180.0 && E == 180.0 && oW > oE) {
        return std::make_unique<Private>(oW, std::max(S, oS), oE,
                                         std::min(N, oN));
    }

    // Check world coverage of other bbox, and this bbox overlapping
    // antimeridian (e.g. W=175 and E=-175)
    if (oW == -180.0 && oE == 180.0 && W > E) {
        return std::make_unique<Private>(W, std::max(S, oS), E,
                                         std::min(N, oN));
    }

    // Normal bounding box ?
    if (W <= E) {
        if (oW <= oE) {
            const double resW = std::max(W, oW);
            const double resE = std::min(E, oE);
            if (resW < resE) {
                return std::make_unique<Private>(resW, std::max(S, oS), resE,
                                                 std::min(N, oN));
            }
            return nullptr;
        }

        // Bail out on longitudes not in [-180,180]. We could probably make
        // some sense of them, but this check at least avoid potential infinite
        // recursion.
        if (oW > 180 || oE < -180) {
            return nullptr;
        }

        // Return larger of two parts of the multipolygon
        auto inter1 = intersection(Private(oW, oS, 180.0, oN));
        auto inter2 = intersection(Private(-180.0, oS, oE, oN));
        if (!inter1) {
            return inter2;
        }
        if (!inter2) {
            return inter1;
        }
        if (inter1->east_ - inter1->west_ > inter2->east_ - inter2->west_) {
            return inter1;
        }
        return inter2;
        // No: crossing antimeridian
    } else {
        if (oW <= oE) {
            return otherExtent.intersection(*this);
        }

        return std::make_unique<Private>(std::max(W, oW), std::max(S, oS),
                                         std::min(E, oE), std::min(N, oN));
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct VerticalExtent::Private {
    double minimum_{};
    double maximum_{};
    common::UnitOfMeasureNNPtr unit_;

    Private(double minimum, double maximum,
            const common::UnitOfMeasureNNPtr &unit)
        : minimum_(minimum), maximum_(maximum), unit_(unit) {}
};
//! @endcond

// ---------------------------------------------------------------------------

VerticalExtent::VerticalExtent(double minimumIn, double maximumIn,
                               const common::UnitOfMeasureNNPtr &unitIn)
    : d(std::make_unique<Private>(minimumIn, maximumIn, unitIn)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
VerticalExtent::~VerticalExtent() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns the minimum of the vertical extent.
 */
double VerticalExtent::minimumValue() PROJ_PURE_DEFN { return d->minimum_; }

// ---------------------------------------------------------------------------

/** \brief Returns the maximum of the vertical extent.
 */
double VerticalExtent::maximumValue() PROJ_PURE_DEFN { return d->maximum_; }

// ---------------------------------------------------------------------------

/** \brief Returns the unit of the vertical extent.
 */
common::UnitOfMeasureNNPtr &VerticalExtent::unit() PROJ_PURE_DEFN {
    return d->unit_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a VerticalExtent.
 *
 * @param minimumIn minimum.
 * @param maximumIn maximum.
 * @param unitIn unit.
 * @return a new VerticalExtent.
 */
VerticalExtentNNPtr
VerticalExtent::create(double minimumIn, double maximumIn,
                       const common::UnitOfMeasureNNPtr &unitIn) {
    return VerticalExtent::nn_make_shared<VerticalExtent>(minimumIn, maximumIn,
                                                          unitIn);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool VerticalExtent::_isEquivalentTo(const util::IComparable *other,
                                     util::IComparable::Criterion,
                                     const io::DatabaseContextPtr &) const {
    auto otherExtent = dynamic_cast<const VerticalExtent *>(other);
    if (!otherExtent)
        return false;
    return d->minimum_ == otherExtent->d->minimum_ &&
           d->maximum_ == otherExtent->d->maximum_ &&
           d->unit_ == otherExtent->d->unit_;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns whether this extent contains the other one.
 */
bool VerticalExtent::contains(const VerticalExtentNNPtr &other) const {
    const double thisUnitToSI = d->unit_->conversionToSI();
    const double otherUnitToSI = other->d->unit_->conversionToSI();
    return d->minimum_ * thisUnitToSI <= other->d->minimum_ * otherUnitToSI &&
           d->maximum_ * thisUnitToSI >= other->d->maximum_ * otherUnitToSI;
}

// ---------------------------------------------------------------------------

/** \brief Returns whether this extent intersects the other one.
 */
bool VerticalExtent::intersects(const VerticalExtentNNPtr &other) const {
    const double thisUnitToSI = d->unit_->conversionToSI();
    const double otherUnitToSI = other->d->unit_->conversionToSI();
    return d->minimum_ * thisUnitToSI <= other->d->maximum_ * otherUnitToSI &&
           d->maximum_ * thisUnitToSI >= other->d->minimum_ * otherUnitToSI;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct TemporalExtent::Private {
    std::string start_{};
    std::string stop_{};

    Private(const std::string &start, const std::string &stop)
        : start_(start), stop_(stop) {}
};
//! @endcond

// ---------------------------------------------------------------------------

TemporalExtent::TemporalExtent(const std::string &startIn,
                               const std::string &stopIn)
    : d(std::make_unique<Private>(startIn, stopIn)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
TemporalExtent::~TemporalExtent() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns the start of the temporal extent.
 */
const std::string &TemporalExtent::start() PROJ_PURE_DEFN { return d->start_; }

// ---------------------------------------------------------------------------

/** \brief Returns the end of the temporal extent.
 */
const std::string &TemporalExtent::stop() PROJ_PURE_DEFN { return d->stop_; }

// ---------------------------------------------------------------------------

/** \brief Instantiate a TemporalExtent.
 *
 * @param start start.
 * @param stop stop.
 * @return a new TemporalExtent.
 */
TemporalExtentNNPtr TemporalExtent::create(const std::string &start,
                                           const std::string &stop) {
    return TemporalExtent::nn_make_shared<TemporalExtent>(start, stop);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool TemporalExtent::_isEquivalentTo(const util::IComparable *other,
                                     util::IComparable::Criterion,
                                     const io::DatabaseContextPtr &) const {
    auto otherExtent = dynamic_cast<const TemporalExtent *>(other);
    if (!otherExtent)
        return false;
    return start() == otherExtent->start() && stop() == otherExtent->stop();
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns whether this extent contains the other one.
 */
bool TemporalExtent::contains(const TemporalExtentNNPtr &other) const {
    return start() <= other->start() && stop() >= other->stop();
}

// ---------------------------------------------------------------------------

/** \brief Returns whether this extent intersects the other one.
 */
bool TemporalExtent::intersects(const TemporalExtentNNPtr &other) const {
    return start() <= other->stop() && stop() >= other->start();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct Extent::Private {
    optional<std::string> description_{};
    std::vector<GeographicExtentNNPtr> geographicElements_{};
    std::vector<VerticalExtentNNPtr> verticalElements_{};
    std::vector<TemporalExtentNNPtr> temporalElements_{};
};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Extent::Extent() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

Extent::Extent(const Extent &other) : d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

Extent::~Extent() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** Return a textual description of the extent.
 *
 * @return the description, or empty.
 */
const optional<std::string> &Extent::description() PROJ_PURE_DEFN {
    return d->description_;
}

// ---------------------------------------------------------------------------

/** Return the geographic element(s) of the extent
 *
 * @return the geographic element(s), or empty.
 */
const std::vector<GeographicExtentNNPtr> &
Extent::geographicElements() PROJ_PURE_DEFN {
    return d->geographicElements_;
}

// ---------------------------------------------------------------------------

/** Return the vertical element(s) of the extent
 *
 * @return the vertical element(s), or empty.
 */
const std::vector<VerticalExtentNNPtr> &
Extent::verticalElements() PROJ_PURE_DEFN {
    return d->verticalElements_;
}

// ---------------------------------------------------------------------------

/** Return the temporal element(s) of the extent
 *
 * @return the temporal element(s), or empty.
 */
const std::vector<TemporalExtentNNPtr> &
Extent::temporalElements() PROJ_PURE_DEFN {
    return d->temporalElements_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Extent.
 *
 * @param descriptionIn Textual description, or empty.
 * @param geographicElementsIn Geographic element(s), or empty.
 * @param verticalElementsIn Vertical element(s), or empty.
 * @param temporalElementsIn Temporal element(s), or empty.
 * @return a new Extent.
 */
ExtentNNPtr
Extent::create(const optional<std::string> &descriptionIn,
               const std::vector<GeographicExtentNNPtr> &geographicElementsIn,
               const std::vector<VerticalExtentNNPtr> &verticalElementsIn,
               const std::vector<TemporalExtentNNPtr> &temporalElementsIn) {
    auto extent = Extent::nn_make_shared<Extent>();
    extent->assignSelf(extent);
    extent->d->description_ = descriptionIn;
    extent->d->geographicElements_ = geographicElementsIn;
    extent->d->verticalElements_ = verticalElementsIn;
    extent->d->temporalElements_ = temporalElementsIn;
    return extent;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Extent from a bounding box
 *
 * @param west Western-most coordinate of the limit of the dataset extent (in
 * degrees).
 * @param south Southern-most coordinate of the limit of the dataset extent (in
 * degrees).
 * @param east Eastern-most coordinate of the limit of the dataset extent (in
 * degrees).
 * @param north Northern-most coordinate of the limit of the dataset extent (in
 * degrees).
 * @param descriptionIn Textual description, or empty.
 * @return a new Extent.
 */
ExtentNNPtr
Extent::createFromBBOX(double west, double south, double east, double north,
                       const util::optional<std::string> &descriptionIn) {
    return create(
        descriptionIn,
        std::vector<GeographicExtentNNPtr>{
            nn_static_pointer_cast<GeographicExtent>(
                GeographicBoundingBox::create(west, south, east, north))},
        std::vector<VerticalExtentNNPtr>(), std::vector<TemporalExtentNNPtr>());
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool Extent::_isEquivalentTo(const util::IComparable *other,
                             util::IComparable::Criterion criterion,
                             const io::DatabaseContextPtr &dbContext) const {
    auto otherExtent = dynamic_cast<const Extent *>(other);
    bool ret =
        (otherExtent &&
         description().has_value() == otherExtent->description().has_value() &&
         *description() == *otherExtent->description() &&
         d->geographicElements_.size() ==
             otherExtent->d->geographicElements_.size() &&
         d->verticalElements_.size() ==
             otherExtent->d->verticalElements_.size() &&
         d->temporalElements_.size() ==
             otherExtent->d->temporalElements_.size());
    if (ret) {
        for (size_t i = 0; ret && i < d->geographicElements_.size(); ++i) {
            ret = d->geographicElements_[i]->_isEquivalentTo(
                otherExtent->d->geographicElements_[i].get(), criterion,
                dbContext);
        }
        for (size_t i = 0; ret && i < d->verticalElements_.size(); ++i) {
            ret = d->verticalElements_[i]->_isEquivalentTo(
                otherExtent->d->verticalElements_[i].get(), criterion,
                dbContext);
        }
        for (size_t i = 0; ret && i < d->temporalElements_.size(); ++i) {
            ret = d->temporalElements_[i]->_isEquivalentTo(
                otherExtent->d->temporalElements_[i].get(), criterion,
                dbContext);
        }
    }
    return ret;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns whether this extent contains the other one.
 *
 * Behavior only well specified if each sub-extent category as at most
 * one element.
 */
bool Extent::contains(const ExtentNNPtr &other) const {
    bool res = true;
    if (d->geographicElements_.size() == 1 &&
        other->d->geographicElements_.size() == 1) {
        res = d->geographicElements_[0]->contains(
            other->d->geographicElements_[0]);
    }
    if (res && d->verticalElements_.size() == 1 &&
        other->d->verticalElements_.size() == 1) {
        res = d->verticalElements_[0]->contains(other->d->verticalElements_[0]);
    }
    if (res && d->temporalElements_.size() == 1 &&
        other->d->temporalElements_.size() == 1) {
        res = d->temporalElements_[0]->contains(other->d->temporalElements_[0]);
    }
    return res;
}

// ---------------------------------------------------------------------------

/** \brief Returns whether this extent intersects the other one.
 *
 * Behavior only well specified if each sub-extent category as at most
 * one element.
 */
bool Extent::intersects(const ExtentNNPtr &other) const {
    bool res = true;
    if (d->geographicElements_.size() == 1 &&
        other->d->geographicElements_.size() == 1) {
        res = d->geographicElements_[0]->intersects(
            other->d->geographicElements_[0]);
    }
    if (res && d->verticalElements_.size() == 1 &&
        other->d->verticalElements_.size() == 1) {
        res =
            d->verticalElements_[0]->intersects(other->d->verticalElements_[0]);
    }
    if (res && d->temporalElements_.size() == 1 &&
        other->d->temporalElements_.size() == 1) {
        res =
            d->temporalElements_[0]->intersects(other->d->temporalElements_[0]);
    }
    return res;
}

// ---------------------------------------------------------------------------

/** \brief Returns the intersection of this extent with another one.
 *
 * Behavior only well specified if there is one single GeographicExtent
 * in each object.
 * Returns nullptr otherwise.
 */
ExtentPtr Extent::intersection(const ExtentNNPtr &other) const {
    if (d->geographicElements_.size() == 1 &&
        other->d->geographicElements_.size() == 1) {
        if (contains(other)) {
            return other.as_nullable();
        }
        auto self = util::nn_static_pointer_cast<Extent>(shared_from_this());
        if (other->contains(self)) {
            return self.as_nullable();
        }
        auto geogIntersection = d->geographicElements_[0]->intersection(
            other->d->geographicElements_[0]);
        if (geogIntersection) {
            return create(util::optional<std::string>(),
                          std::vector<GeographicExtentNNPtr>{
                              NN_NO_CHECK(geogIntersection)},
                          std::vector<VerticalExtentNNPtr>{},
                          std::vector<TemporalExtentNNPtr>{});
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct Identifier::Private {
    optional<Citation> authority_{};
    std::string code_{};
    optional<std::string> codeSpace_{};
    optional<std::string> version_{};
    optional<std::string> description_{};
    optional<std::string> uri_{};

    Private() = default;

    Private(const std::string &codeIn, const PropertyMap &properties)
        : code_(codeIn) {
        setProperties(properties);
    }

  private:
    // cppcheck-suppress functionStatic
    void setProperties(const PropertyMap &properties);
};

// ---------------------------------------------------------------------------

void Identifier::Private::setProperties(
    const PropertyMap &properties) // throw(InvalidValueTypeException)
{
    {
        const auto pVal = properties.get(AUTHORITY_KEY);
        if (pVal) {
            if (auto genVal = dynamic_cast<const BoxedValue *>(pVal->get())) {
                if (genVal->type() == BoxedValue::Type::STRING) {
                    authority_ = Citation(genVal->stringValue());
                } else {
                    throw InvalidValueTypeException("Invalid value type for " +
                                                    AUTHORITY_KEY);
                }
            } else {
                auto citation = dynamic_cast<const Citation *>(pVal->get());
                if (citation) {
                    authority_ = *citation;
                } else {
                    throw InvalidValueTypeException("Invalid value type for " +
                                                    AUTHORITY_KEY);
                }
            }
        }
    }

    {
        const auto pVal = properties.get(CODE_KEY);
        if (pVal) {
            if (auto genVal = dynamic_cast<const BoxedValue *>(pVal->get())) {
                if (genVal->type() == BoxedValue::Type::INTEGER) {
                    code_ = toString(genVal->integerValue());
                } else if (genVal->type() == BoxedValue::Type::STRING) {
                    code_ = genVal->stringValue();
                } else {
                    throw InvalidValueTypeException("Invalid value type for " +
                                                    CODE_KEY);
                }
            } else {
                throw InvalidValueTypeException("Invalid value type for " +
                                                CODE_KEY);
            }
        }
    }

    properties.getStringValue(CODESPACE_KEY, codeSpace_);
    properties.getStringValue(VERSION_KEY, version_);
    properties.getStringValue(DESCRIPTION_KEY, description_);
    properties.getStringValue(URI_KEY, uri_);
}

//! @endcond

// ---------------------------------------------------------------------------

Identifier::Identifier(const std::string &codeIn,
                       const util::PropertyMap &properties)
    : d(std::make_unique<Private>(codeIn, properties)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

Identifier::Identifier() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

Identifier::Identifier(const Identifier &other)
    : d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

Identifier::~Identifier() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a Identifier.
 *
 * @param codeIn Alphanumeric value identifying an instance in the codespace
 * @param properties See \ref general_properties.
 * Generally, the Identifier::CODESPACE_KEY should be set.
 * @return a new Identifier.
 */
IdentifierNNPtr Identifier::create(const std::string &codeIn,
                                   const PropertyMap &properties) {
    return Identifier::nn_make_shared<Identifier>(codeIn, properties);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
IdentifierNNPtr
Identifier::createFromDescription(const std::string &descriptionIn) {
    auto id = Identifier::nn_make_shared<Identifier>();
    id->d->description_ = descriptionIn;
    return id;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return a citation for the organization responsible for definition and
 * maintenance of the code.
 *
 * @return the citation for the authority, or empty.
 */
const optional<Citation> &Identifier::authority() PROJ_PURE_DEFN {
    return d->authority_;
}

// ---------------------------------------------------------------------------

/** \brief Return the alphanumeric value identifying an instance in the
 * codespace.
 *
 * e.g. "4326" (for EPSG:4326 WGS 84 GeographicCRS)
 *
 * @return the code.
 */
const std::string &Identifier::code() PROJ_PURE_DEFN { return d->code_; }

// ---------------------------------------------------------------------------

/** \brief Return the organization responsible for definition and maintenance of
 * the code.
 *
 * e.g "EPSG"
 *
 * @return the authority codespace, or empty.
 */
const optional<std::string> &Identifier::codeSpace() PROJ_PURE_DEFN {
    return d->codeSpace_;
}

// ---------------------------------------------------------------------------

/** \brief Return the version identifier for the namespace.
 *
 * When appropriate, the edition is identified by the effective date, coded
 * using ISO 8601 date format.
 *
 * @return the version or empty.
 */
const optional<std::string> &Identifier::version() PROJ_PURE_DEFN {
    return d->version_;
}

// ---------------------------------------------------------------------------

/** \brief Return the natural language description of the meaning of the code
 * value.
 *
 * @return the description or empty.
 */
const optional<std::string> &Identifier::description() PROJ_PURE_DEFN {
    return d->description_;
}

// ---------------------------------------------------------------------------

/** \brief Return the URI of the identifier.
 *
 * @return the URI or empty.
 */
const optional<std::string> &Identifier::uri() PROJ_PURE_DEFN {
    return d->uri_;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void Identifier::_exportToWKT(WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == WKTFormatter::Version::WKT2;
    const std::string &l_code = code();
    std::string l_codeSpace = *codeSpace();
    std::string l_version = *version();
    const auto &dbContext = formatter->databaseContext();
    if (dbContext) {
        dbContext->getAuthorityAndVersion(*codeSpace(), l_codeSpace, l_version);
    }
    if (!l_codeSpace.empty() && !l_code.empty()) {
        if (isWKT2) {
            formatter->startNode(WKTConstants::ID, false);
            formatter->addQuotedString(l_codeSpace);
            try {
                (void)std::stoi(l_code);
                formatter->add(l_code);
            } catch (const std::exception &) {
                formatter->addQuotedString(l_code);
            }
            if (!l_version.empty()) {
                bool isDouble = false;
                (void)c_locale_stod(l_version, isDouble);
                if (isDouble) {
                    formatter->add(l_version);
                } else {
                    formatter->addQuotedString(l_version);
                }
            }
            if (authority().has_value() &&
                *(authority()->title()) != *codeSpace()) {
                formatter->startNode(WKTConstants::CITATION, false);
                formatter->addQuotedString(*(authority()->title()));
                formatter->endNode();
            }
            if (uri().has_value()) {
                formatter->startNode(WKTConstants::URI, false);
                formatter->addQuotedString(*(uri()));
                formatter->endNode();
            }
            formatter->endNode();
        } else {
            formatter->startNode(WKTConstants::AUTHORITY, false);
            formatter->addQuotedString(l_codeSpace);
            formatter->addQuotedString(l_code);
            formatter->endNode();
        }
    }
}

// ---------------------------------------------------------------------------

void Identifier::_exportToJSON(JSONFormatter *formatter) const {
    const std::string &l_code = code();
    std::string l_codeSpace = *codeSpace();
    std::string l_version = *version();
    const auto &dbContext = formatter->databaseContext();
    if (dbContext) {
        dbContext->getAuthorityAndVersion(*codeSpace(), l_codeSpace, l_version);
    }
    if (!l_codeSpace.empty() && !l_code.empty()) {
        auto writer = formatter->writer();
        auto objContext(formatter->MakeObjectContext(nullptr, false));
        writer->AddObjKey("authority");
        writer->Add(l_codeSpace);
        writer->AddObjKey("code");
        try {
            writer->Add(std::stoi(l_code));
        } catch (const std::exception &) {
            writer->Add(l_code);
        }

        if (!l_version.empty()) {
            writer->AddObjKey("version");
            bool isDouble = false;
            (void)c_locale_stod(l_version, isDouble);
            if (isDouble) {
                writer->AddUnquoted(l_version.c_str());
            } else {
                writer->Add(l_version);
            }
        }
        if (authority().has_value() &&
            *(authority()->title()) != *codeSpace()) {
            writer->AddObjKey("authority_citation");
            writer->Add(*(authority()->title()));
        }
        if (uri().has_value()) {
            writer->AddObjKey("uri");
            writer->Add(*(uri()));
        }
    }
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static bool isIgnoredChar(char ch) {
    return ch == ' ' || ch == '_' || ch == '-' || ch == '/' || ch == '(' ||
           ch == ')' || ch == '.' || ch == '&' || ch == ',';
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static char lower(char ch) {
    return ch >= 'A' && ch <= 'Z' ? ch - 'A' + 'a' : ch;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static const struct utf8_to_lower {
    const char *utf8;
    char ascii;
} map_utf8_to_lower[] = {
    {"\xc3\xa1", 'a'}, // a acute
    {"\xc3\xa4", 'a'}, // a tremma

    {"\xc4\x9b", 'e'}, // e reverse circumflex
    {"\xc3\xa8", 'e'}, // e grave
    {"\xc3\xa9", 'e'}, // e acute
    {"\xc3\xab", 'e'}, // e tremma

    {"\xc3\xad", 'i'}, // i grave

    {"\xc3\xb4", 'o'}, // o circumflex
    {"\xc3\xb6", 'o'}, // o tremma

    {"\xc3\xa7", 'c'}, // c cedilla
};

static const struct utf8_to_lower *get_ascii_replacement(const char *c_str) {
    for (const auto &pair : map_utf8_to_lower) {
        if (*c_str == pair.utf8[0] &&
            strncmp(c_str, pair.utf8, strlen(pair.utf8)) == 0) {
            return &pair;
        }
    }
    return nullptr;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

/** Checks if needle is a substring of c_str.
 *
 * e.g matchesLowerCase("JavaScript", "java") returns true
 */
static bool matchesLowerCase(const char *c_str, const char *needle) {
    size_t i = 0;
    for (; c_str[i] && needle[i]; ++i) {
        if (lower(c_str[i]) != lower(needle[i])) {
            return false;
        }
    }
    return needle[i] == 0;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static inline bool isdigit(char ch) { return ch >= '0' && ch <= '9'; }
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::string Identifier::canonicalizeName(const std::string &str,
                                         bool biggerDifferencesAllowed) {
    std::string res;
    const char *c_str = str.c_str();
    for (size_t i = 0; c_str[i] != 0; ++i) {
        const auto ch = lower(c_str[i]);
        if (ch == ' ' && c_str[i + 1] == '+' && c_str[i + 2] == ' ') {
            i += 2;
            continue;
        }

        // Canonicalize "19dd" (where d is a digit) as "dd"
        if (ch == '1' && !res.empty() && !isdigit(res.back()) &&
            c_str[i + 1] == '9' && isdigit(c_str[i + 2]) &&
            isdigit(c_str[i + 3])) {
            ++i;
            continue;
        }

        if (biggerDifferencesAllowed) {

            const auto skipSubstring = [](char l_ch, const char *l_str,
                                          size_t &idx, const char *substr) {
                if (l_ch == substr[0] && idx > 0 &&
                    isIgnoredChar(l_str[idx - 1]) &&
                    matchesLowerCase(l_str + idx, substr)) {
                    idx += strlen(substr) - 1;
                    return true;
                }
                return false;
            };

            // Skip "zone" or "height" if preceding character is a space
            if (skipSubstring(ch, c_str, i, "zone") ||
                skipSubstring(ch, c_str, i, "height")) {
                continue;
            }

            // Replace a substring by its first character if preceding character
            // is a space or a digit
            const auto replaceByFirstChar = [](char l_ch, const char *l_str,
                                               size_t &idx, const char *substr,
                                               std::string &l_res) {
                if (l_ch == substr[0] && idx > 0 &&
                    (isIgnoredChar(l_str[idx - 1]) ||
                     isdigit(l_str[idx - 1])) &&
                    matchesLowerCase(l_str + idx, substr)) {
                    l_res.push_back(l_ch);
                    idx += strlen(substr) - 1;
                    return true;
                }
                return false;
            };

            // Replace "north" or "south" by its first character if preceding
            // character is a space or a digit
            if (replaceByFirstChar(ch, c_str, i, "north", res) ||
                replaceByFirstChar(ch, c_str, i, "south", res)) {
                continue;
            }
        }

        if (static_cast<unsigned char>(ch) > 127) {
            const auto *replacement = get_ascii_replacement(c_str + i);
            if (replacement) {
                res.push_back(replacement->ascii);
                i += strlen(replacement->utf8) - 1;
                continue;
            }
        }

        if (matchesLowerCase(c_str + i, "_IntlFeet") &&
            c_str[i + strlen("_IntlFeet")] == 0) {
            res += "feet";
            break;
        }

        if (!isIgnoredChar(ch)) {
            res.push_back(ch);
        }
    }
    return res;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns whether two names are considered equivalent.
 *
 * Two names are equivalent by removing any space, underscore, dash, slash,
 * { or } character from them, and comparing in a case insensitive way.
 *
 * @param a first string
 * @param b second string
 * @param biggerDifferencesAllowed if true, "height" and "zone" words are
 * ignored, and "north" is shortened as "n" and "south" as "n".
 * @since 9.6
 */
bool Identifier::isEquivalentName(const char *a, const char *b,
                                  bool biggerDifferencesAllowed) noexcept {
    size_t i = 0;
    size_t j = 0;
    char lastValidA = 0;
    char lastValidB = 0;
    while (a[i] != 0 || b[j] != 0) {
        char aCh = lower(a[i]);
        char bCh = lower(b[j]);
        if (aCh == ' ' && a[i + 1] == '+' && a[i + 2] == ' ' && a[i + 3] != 0) {
            i += 3;
            continue;
        }
        if (bCh == ' ' && b[j + 1] == '+' && b[j + 2] == ' ' && b[j + 3] != 0) {
            j += 3;
            continue;
        }

        if (matchesLowerCase(a + i, "_IntlFeet") &&
            a[i + strlen("_IntlFeet")] == 0 &&
            matchesLowerCase(b + j, "_Feet") && b[j + strlen("_Feet")] == 0) {
            return true;
        } else if (matchesLowerCase(a + i, "_Feet") &&
                   a[i + strlen("_Feet")] == 0 &&
                   matchesLowerCase(b + j, "_IntlFeet") &&
                   b[j + strlen("_IntlFeet")] == 0) {
            return true;
        }

        if (isIgnoredChar(aCh)) {
            ++i;
            continue;
        }
        if (isIgnoredChar(bCh)) {
            ++j;
            continue;
        }

        // Canonicalize "19dd" (where d is a digit) as "dd"
        if (aCh == '1' && !isdigit(lastValidA) && a[i + 1] == '9' &&
            isdigit(a[i + 2]) && isdigit(a[i + 3])) {
            i += 2;
            lastValidA = '9';
            continue;
        }
        if (bCh == '1' && !isdigit(lastValidB) && b[j + 1] == '9' &&
            isdigit(b[j + 2]) && isdigit(b[j + 3])) {
            j += 2;
            lastValidB = '9';
            continue;
        }

        if (biggerDifferencesAllowed) {
            // Skip a substring if preceding character is a space
            const auto skipSubString = [](char ch, const char *str, size_t &idx,
                                          const char *substr) {
                if (ch == substr[0] && idx > 0 && isIgnoredChar(str[idx - 1]) &&
                    matchesLowerCase(str + idx, substr)) {
                    idx += strlen(substr);
                    return true;
                }
                return false;
            };

            bool skip = false;
            if (skipSubString(aCh, a, i, "zone"))
                skip = true;
            if (skipSubString(bCh, b, j, "zone"))
                skip = true;
            if (skip)
                continue;

            if (skipSubString(aCh, a, i, "height"))
                skip = true;
            if (skipSubString(bCh, b, j, "height"))
                skip = true;
            if (skip)
                continue;

            // Replace a substring by its first character if preceding character
            // is a space or a digit
            const auto replaceByFirstChar = [](char ch, const char *str,
                                               size_t &idx,
                                               const char *substr) {
                if (ch == substr[0] && idx > 0 &&
                    (isIgnoredChar(str[idx - 1]) || isdigit(str[idx - 1])) &&
                    matchesLowerCase(str + idx, substr)) {
                    idx += strlen(substr) - 1;
                    return true;
                }
                return false;
            };

            if (!replaceByFirstChar(aCh, a, i, "north"))
                replaceByFirstChar(aCh, a, i, "south");

            if (!replaceByFirstChar(bCh, b, j, "north"))
                replaceByFirstChar(bCh, b, j, "south");
        }

        if (static_cast<unsigned char>(aCh) > 127) {
            const auto *replacement = get_ascii_replacement(a + i);
            if (replacement) {
                aCh = replacement->ascii;
                i += strlen(replacement->utf8) - 1;
            }
        }
        if (static_cast<unsigned char>(bCh) > 127) {
            const auto *replacement = get_ascii_replacement(b + j);
            if (replacement) {
                bCh = replacement->ascii;
                j += strlen(replacement->utf8) - 1;
            }
        }

        if (aCh != bCh) {
            return false;
        }
        lastValidA = aCh;
        lastValidB = bCh;
        if (aCh != 0)
            ++i;
        if (bCh != 0)
            ++j;
    }
    return true;
}

// ---------------------------------------------------------------------------

/** \brief Returns whether two names are considered equivalent.
 *
 * Two names are equivalent by removing any space, underscore, dash, slash,
 * { or } character from them, and comparing in a case insensitive way.
 */
bool Identifier::isEquivalentName(const char *a, const char *b) noexcept {
    return isEquivalentName(a, b, /* biggerDifferencesAllowed = */ true);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct PositionalAccuracy::Private {
    std::string value_{};
};
//! @endcond

// ---------------------------------------------------------------------------

PositionalAccuracy::PositionalAccuracy(const std::string &valueIn)
    : d(std::make_unique<Private>()) {
    d->value_ = valueIn;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
PositionalAccuracy::~PositionalAccuracy() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the value of the positional accuracy.
 */
const std::string &PositionalAccuracy::value() PROJ_PURE_DEFN {
    return d->value_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a PositionalAccuracy.
 *
 * @param valueIn positional accuracy value.
 * @return a new PositionalAccuracy.
 */
PositionalAccuracyNNPtr PositionalAccuracy::create(const std::string &valueIn) {
    return PositionalAccuracy::nn_make_shared<PositionalAccuracy>(valueIn);
}

} // namespace metadata
NS_PROJ_END
