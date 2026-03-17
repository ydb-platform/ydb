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

#ifndef METADATA_HH_INCLUDED
#define METADATA_HH_INCLUDED

#include <memory>
#include <string>
#include <vector>

#include "io.hpp"
#include "util.hpp"

NS_PROJ_START

namespace common {
class UnitOfMeasure;
using UnitOfMeasurePtr = std::shared_ptr<UnitOfMeasure>;
using UnitOfMeasureNNPtr = util::nn<UnitOfMeasurePtr>;
class IdentifiedObject;
} // namespace common

/** osgeo.proj.metadata namespace
 *
 * \brief Common classes from \ref ISO_19115 standard
 */
namespace metadata {

// ---------------------------------------------------------------------------

/** \brief Standardized resource reference.
 *
 * A citation contains a title.
 *
 * \remark Simplified version of [Citation]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/metadata/citation/Citation.html)
 * from \ref GeoAPI
 */
class PROJ_GCC_DLL Citation : public util::BaseObject {
  public:
    PROJ_DLL explicit Citation(const std::string &titleIn);
    //! @cond Doxygen_Suppress
    PROJ_DLL Citation();
    PROJ_DLL Citation(const Citation &other);
    PROJ_DLL ~Citation() override;
    //! @endcond

    PROJ_DLL const util::optional<std::string> &title() PROJ_PURE_DECL;

  protected:
    PROJ_FRIEND_OPTIONAL(Citation);
    PROJ_INTERNAL Citation &operator=(const Citation &other);

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class GeographicExtent;
/** Shared pointer of GeographicExtent. */
using GeographicExtentPtr = std::shared_ptr<GeographicExtent>;
/** Non-null shared pointer of GeographicExtent. */
using GeographicExtentNNPtr = util::nn<GeographicExtentPtr>;

/** \brief Base interface for geographic area of the dataset.
 *
 * \remark Simplified version of [GeographicExtent]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/metadata/extent/GeographicExtent.html)
 * from \ref GeoAPI
 */
class PROJ_GCC_DLL GeographicExtent : public util::BaseObject,
                                      public util::IComparable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~GeographicExtent() override;
    //! @endcond

    // GeoAPI has a getInclusion() method. We assume that it is included for our
    // use

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override = 0;
    //! @endcond

    /** \brief Returns whether this extent contains the other one. */
    PROJ_DLL virtual bool
    contains(const GeographicExtentNNPtr &other) const = 0;

    /** \brief Returns whether this extent intersects the other one. */
    PROJ_DLL virtual bool
    intersects(const GeographicExtentNNPtr &other) const = 0;

    /** \brief Returns the intersection of this extent with another one. */
    PROJ_DLL virtual GeographicExtentPtr
    intersection(const GeographicExtentNNPtr &other) const = 0;

  protected:
    PROJ_INTERNAL GeographicExtent();

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class GeographicBoundingBox;
/** Shared pointer of GeographicBoundingBox. */
using GeographicBoundingBoxPtr = std::shared_ptr<GeographicBoundingBox>;
/** Non-null shared pointer of GeographicBoundingBox. */
using GeographicBoundingBoxNNPtr = util::nn<GeographicBoundingBoxPtr>;

/** \brief Geographic position of the dataset.
 *
 * This is only an approximate so specifying the coordinate reference system is
 * unnecessary.
 *
 * \remark Implements [GeographicBoundingBox]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/metadata/extent/GeographicBoundingBox.html)
 * from \ref GeoAPI
 */
class PROJ_GCC_DLL GeographicBoundingBox : public GeographicExtent {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~GeographicBoundingBox() override;
    //! @endcond

    PROJ_DLL double westBoundLongitude() PROJ_PURE_DECL;
    PROJ_DLL double southBoundLatitude() PROJ_PURE_DECL;
    PROJ_DLL double eastBoundLongitude() PROJ_PURE_DECL;
    PROJ_DLL double northBoundLatitude() PROJ_PURE_DECL;

    PROJ_DLL static GeographicBoundingBoxNNPtr
    create(double west, double south, double east, double north);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

    PROJ_INTERNAL bool
    contains(const GeographicExtentNNPtr &other) const override;

    PROJ_INTERNAL bool
    intersects(const GeographicExtentNNPtr &other) const override;

    PROJ_INTERNAL GeographicExtentPtr
    intersection(const GeographicExtentNNPtr &other) const override;

  protected:
    PROJ_INTERNAL GeographicBoundingBox(double west, double south, double east,
                                        double north);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class TemporalExtent;
/** Shared pointer of TemporalExtent. */
using TemporalExtentPtr = std::shared_ptr<TemporalExtent>;
/** Non-null shared pointer of TemporalExtent. */
using TemporalExtentNNPtr = util::nn<TemporalExtentPtr>;

/** \brief Time period covered by the content of the dataset.
 *
 * \remark Simplified version of [TemporalExtent]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/metadata/extent/TemporalExtent.html)
 * from \ref GeoAPI
 */
class PROJ_GCC_DLL TemporalExtent : public util::BaseObject,
                                    public util::IComparable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~TemporalExtent() override;
    //! @endcond

    PROJ_DLL const std::string &start() PROJ_PURE_DECL;
    PROJ_DLL const std::string &stop() PROJ_PURE_DECL;

    PROJ_DLL static TemporalExtentNNPtr create(const std::string &start,
                                               const std::string &stop);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

    PROJ_DLL bool contains(const TemporalExtentNNPtr &other) const;

    PROJ_DLL bool intersects(const TemporalExtentNNPtr &other) const;

  protected:
    PROJ_INTERNAL TemporalExtent(const std::string &start,
                                 const std::string &stop);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class VerticalExtent;
/** Shared pointer of VerticalExtent. */
using VerticalExtentPtr = std::shared_ptr<VerticalExtent>;
/** Non-null shared pointer of VerticalExtent. */
using VerticalExtentNNPtr = util::nn<VerticalExtentPtr>;

/** \brief Vertical domain of dataset.
 *
 * \remark Simplified version of [VerticalExtent]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/metadata/extent/VerticalExtent.html)
 * from \ref GeoAPI
 */
class PROJ_GCC_DLL VerticalExtent : public util::BaseObject,
                                    public util::IComparable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~VerticalExtent() override;
    //! @endcond

    PROJ_DLL double minimumValue() PROJ_PURE_DECL;
    PROJ_DLL double maximumValue() PROJ_PURE_DECL;
    PROJ_DLL common::UnitOfMeasureNNPtr &unit() PROJ_PURE_DECL;

    PROJ_DLL static VerticalExtentNNPtr
    create(double minimumValue, double maximumValue,
           const common::UnitOfMeasureNNPtr &unitIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

    PROJ_DLL bool contains(const VerticalExtentNNPtr &other) const;

    PROJ_DLL bool intersects(const VerticalExtentNNPtr &other) const;

  protected:
    PROJ_INTERNAL VerticalExtent(double minimumValue, double maximumValue,
                                 const common::UnitOfMeasureNNPtr &unitIn);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class Extent;
/** Shared pointer of Extent. */
using ExtentPtr = std::shared_ptr<Extent>;
/** Non-null shared pointer of Extent. */
using ExtentNNPtr = util::nn<ExtentPtr>;

/** \brief Information about spatial, vertical, and temporal extent.
 *
 * \remark Simplified version of [Extent]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/metadata/extent/Extent.html)
 * from \ref GeoAPI
 */
class PROJ_GCC_DLL Extent : public util::BaseObject, public util::IComparable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL Extent(const Extent &other);
    PROJ_DLL ~Extent() override;
    //! @endcond

    PROJ_DLL const util::optional<std::string> &description() PROJ_PURE_DECL;
    PROJ_DLL const std::vector<GeographicExtentNNPtr> &
    geographicElements() PROJ_PURE_DECL;
    PROJ_DLL const std::vector<TemporalExtentNNPtr> &
    temporalElements() PROJ_PURE_DECL;
    PROJ_DLL const std::vector<VerticalExtentNNPtr> &
    verticalElements() PROJ_PURE_DECL;

    PROJ_DLL static ExtentNNPtr
    create(const util::optional<std::string> &descriptionIn,
           const std::vector<GeographicExtentNNPtr> &geographicElementsIn,
           const std::vector<VerticalExtentNNPtr> &verticalElementsIn,
           const std::vector<TemporalExtentNNPtr> &temporalElementsIn);

    PROJ_DLL static ExtentNNPtr
    createFromBBOX(double west, double south, double east, double north,
                   const util::optional<std::string> &descriptionIn =
                       util::optional<std::string>());

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

    PROJ_DLL bool contains(const ExtentNNPtr &other) const;

    PROJ_DLL bool intersects(const ExtentNNPtr &other) const;

    PROJ_DLL ExtentPtr intersection(const ExtentNNPtr &other) const;

    PROJ_DLL static const ExtentNNPtr WORLD;

  protected:
    PROJ_INTERNAL Extent();
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    Extent &operator=(const Extent &other) = delete;
};

// ---------------------------------------------------------------------------

class Identifier;
/** Shared pointer of Identifier. */
using IdentifierPtr = std::shared_ptr<Identifier>;
/** Non-null shared pointer of Identifier. */
using IdentifierNNPtr = util::nn<IdentifierPtr>;

/** \brief Value uniquely identifying an object within a namespace.
 *
 * \remark Implements Identifier as described in \ref ISO_19111_2019 but which
 * originates from \ref ISO_19115
 */
class PROJ_GCC_DLL Identifier : public util::BaseObject,
                                public io::IWKTExportable,
                                public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL Identifier(const Identifier &other);
    PROJ_DLL ~Identifier() override;
    //! @endcond

    PROJ_DLL static IdentifierNNPtr
    create(const std::string &codeIn = std::string(),
           const util::PropertyMap &properties =
               util::PropertyMap()); // throw(InvalidValueTypeException)

    PROJ_DLL static const std::string AUTHORITY_KEY;
    PROJ_DLL static const std::string CODE_KEY;
    PROJ_DLL static const std::string CODESPACE_KEY;
    PROJ_DLL static const std::string VERSION_KEY;
    PROJ_DLL static const std::string DESCRIPTION_KEY;
    PROJ_DLL static const std::string URI_KEY;

    PROJ_DLL static const std::string EPSG;
    PROJ_DLL static const std::string OGC;

    PROJ_DLL const util::optional<Citation> &authority() PROJ_PURE_DECL;
    PROJ_DLL const std::string &code() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<std::string> &codeSpace() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<std::string> &version() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<std::string> &description() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<std::string> &uri() PROJ_PURE_DECL;

    PROJ_DLL static bool isEquivalentName(const char *a,
                                          const char *b) noexcept;
    PROJ_DLL static bool
    isEquivalentName(const char *a, const char *b,
                     bool biggerDifferencesAllowed) noexcept;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL static std::string
        canonicalizeName(const std::string &str,
                         bool biggerDifferencesAllowed = true);

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(io::FormattingException)

    //! @endcond

  protected:
    PROJ_INTERNAL explicit Identifier(const std::string &codeIn,
                                      const util::PropertyMap &properties);
    PROJ_INTERNAL explicit Identifier();

    PROJ_FRIEND_OPTIONAL(Identifier);
    INLINED_MAKE_SHARED
    Identifier &operator=(const Identifier &other) = delete;

    PROJ_FRIEND(common::IdentifiedObject);

    PROJ_INTERNAL static IdentifierNNPtr
    createFromDescription(const std::string &descriptionIn);

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class PositionalAccuracy;
/** Shared pointer of PositionalAccuracy. */
using PositionalAccuracyPtr = std::shared_ptr<PositionalAccuracy>;
/** Non-null shared pointer of PositionalAccuracy. */
using PositionalAccuracyNNPtr = util::nn<PositionalAccuracyPtr>;

/** \brief Accuracy of the position of features.
 *
 * \remark Simplified version of [PositionalAccuracy]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/metadata/quality/PositionalAccuracy.html)
 * from \ref GeoAPI, which originates from \ref ISO_19115
 */
class PROJ_GCC_DLL PositionalAccuracy : public util::BaseObject {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~PositionalAccuracy() override;
    //! @endcond

    PROJ_DLL const std::string &value() PROJ_PURE_DECL;

    PROJ_DLL static PositionalAccuracyNNPtr create(const std::string &valueIn);

  protected:
    PROJ_INTERNAL explicit PositionalAccuracy(const std::string &valueIn);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    PositionalAccuracy(const PositionalAccuracy &other) = delete;
    PositionalAccuracy &operator=(const PositionalAccuracy &other) = delete;
};

} // namespace metadata

NS_PROJ_END

#endif //  METADATA_HH_INCLUDED
