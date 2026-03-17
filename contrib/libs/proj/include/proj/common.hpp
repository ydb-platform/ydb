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

#ifndef COMMON_HH_INCLUDED
#define COMMON_HH_INCLUDED

#include <memory>
#include <string>
#include <vector>

#include "io.hpp"
#include "metadata.hpp"
#include "util.hpp"

NS_PROJ_START

/** osgeo.proj.common namespace
 *
 * \brief Common classes.
 */
namespace common {

// ---------------------------------------------------------------------------

class UnitOfMeasure;
/** Shared pointer of UnitOfMeasure. */
using UnitOfMeasurePtr = std::shared_ptr<UnitOfMeasure>;
/** Non-null shared pointer of UnitOfMeasure. */
using UnitOfMeasureNNPtr = util::nn<UnitOfMeasurePtr>;

/** \brief Unit of measure.
 *
 * This is a mutable object.
 */
class PROJ_GCC_DLL UnitOfMeasure : public util::BaseObject {
  public:
    /** \brief Type of unit of measure. */
    enum class PROJ_MSVC_DLL Type {
        /** Unknown unit of measure */
        UNKNOWN,
        /** No unit of measure */
        NONE,
        /** Angular unit of measure */
        ANGULAR,
        /** Linear unit of measure */
        LINEAR,
        /** Scale unit of measure */
        SCALE,
        /** Time unit of measure */
        TIME,
        /** Parametric unit of measure */
        PARAMETRIC,
    };

    PROJ_DLL explicit UnitOfMeasure(
        const std::string &nameIn = std::string(), double toSIIn = 1.0,
        Type typeIn = Type::UNKNOWN,
        const std::string &codeSpaceIn = std::string(),
        const std::string &codeIn = std::string());

    //! @cond Doxygen_Suppress
    PROJ_DLL UnitOfMeasure(const UnitOfMeasure &other);
    PROJ_DLL ~UnitOfMeasure() override;
    PROJ_DLL UnitOfMeasure &operator=(const UnitOfMeasure &other);
    PROJ_DLL UnitOfMeasure &operator=(UnitOfMeasure &&other);
    PROJ_INTERNAL static UnitOfMeasureNNPtr create(const UnitOfMeasure &other);
    //! @endcond

    PROJ_DLL const std::string &name() PROJ_PURE_DECL;
    PROJ_DLL double conversionToSI() PROJ_PURE_DECL;
    PROJ_DLL Type type() PROJ_PURE_DECL;

    PROJ_DLL const std::string &codeSpace() PROJ_PURE_DECL;
    PROJ_DLL const std::string &code() PROJ_PURE_DECL;

    PROJ_DLL bool operator==(const UnitOfMeasure &other) PROJ_PURE_DECL;
    PROJ_DLL bool operator!=(const UnitOfMeasure &other) PROJ_PURE_DECL;

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter,
                                    const std::string &unitType = std::string())
        const; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(
        io::JSONFormatter *formatter) const; // throw(io::FormattingException)

    PROJ_INTERNAL std::string exportToPROJString() const;

    PROJ_INTERNAL bool
    _isEquivalentTo(const UnitOfMeasure &other,
                    util::IComparable::Criterion criterion =
                        util::IComparable::Criterion::STRICT) const;

    //! @endcond

    PROJ_DLL static const UnitOfMeasure NONE;

    PROJ_DLL static const UnitOfMeasure SCALE_UNITY;
    PROJ_DLL static const UnitOfMeasure PARTS_PER_MILLION;
    PROJ_DLL static const UnitOfMeasure PPM_PER_YEAR;

    PROJ_DLL static const UnitOfMeasure METRE;
    PROJ_DLL static const UnitOfMeasure METRE_PER_YEAR;
    PROJ_DLL static const UnitOfMeasure FOOT;
    PROJ_DLL static const UnitOfMeasure US_FOOT;

    PROJ_DLL static const UnitOfMeasure RADIAN;
    PROJ_DLL static const UnitOfMeasure MICRORADIAN;
    PROJ_DLL static const UnitOfMeasure DEGREE;
    PROJ_DLL static const UnitOfMeasure ARC_SECOND;
    PROJ_DLL static const UnitOfMeasure GRAD;
    PROJ_DLL static const UnitOfMeasure ARC_SECOND_PER_YEAR;

    PROJ_DLL static const UnitOfMeasure SECOND;
    PROJ_DLL static const UnitOfMeasure YEAR;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

/** \brief Numeric value associated with a UnitOfMeasure. */
class Measure : public util::BaseObject {
  public:
    // cppcheck-suppress noExplicitConstructor
    PROJ_DLL Measure(double valueIn = 0.0,
                     const UnitOfMeasure &unitIn = UnitOfMeasure());

    //! @cond Doxygen_Suppress
    PROJ_DLL Measure(const Measure &other);
    PROJ_DLL ~Measure() override;
    //! @endcond

    PROJ_DLL const UnitOfMeasure &unit() PROJ_PURE_DECL;
    PROJ_DLL double getSIValue() PROJ_PURE_DECL;
    PROJ_DLL double value() PROJ_PURE_DECL;

    PROJ_DLL double
    convertToUnit(const UnitOfMeasure &otherUnit) PROJ_PURE_DECL;

    PROJ_DLL bool operator==(const Measure &other) PROJ_PURE_DECL;

    /** Default maximum resulative error. */
    static constexpr double DEFAULT_MAX_REL_ERROR = 1e-10;

    PROJ_INTERNAL bool
    _isEquivalentTo(const Measure &other,
                    util::IComparable::Criterion criterion =
                        util::IComparable::Criterion::STRICT,
                    double maxRelativeError = DEFAULT_MAX_REL_ERROR) const;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    Measure &operator=(const Measure &) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Numeric value, without a physical unit of measure. */
class Scale : public Measure {
  public:
    PROJ_DLL explicit Scale(double valueIn = 0.0);
    PROJ_DLL explicit Scale(double valueIn, const UnitOfMeasure &unitIn);

    //! @cond Doxygen_Suppress
    explicit Scale(const Measure &other) : Scale(other.value(), other.unit()) {}
    PROJ_DLL Scale(const Scale &other);
    PROJ_DLL ~Scale() override;
    //! @endcond

  protected:
    PROJ_FRIEND_OPTIONAL(Scale);
    Scale &operator=(const Scale &) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Numeric value, with a angular unit of measure. */
class Angle : public Measure {
  public:
    PROJ_DLL explicit Angle(double valueIn = 0.0);
    PROJ_DLL Angle(double valueIn, const UnitOfMeasure &unitIn);

    //! @cond Doxygen_Suppress
    explicit Angle(const Measure &other) : Angle(other.value(), other.unit()) {}
    PROJ_DLL Angle(const Angle &other);
    PROJ_DLL ~Angle() override;
    //! @endcond

  protected:
    PROJ_FRIEND_OPTIONAL(Angle);
    Angle &operator=(const Angle &) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Numeric value, with a linear unit of measure. */
class Length : public Measure {
  public:
    PROJ_DLL explicit Length(double valueIn = 0.0);
    PROJ_DLL Length(double valueIn, const UnitOfMeasure &unitIn);

    //! @cond Doxygen_Suppress
    explicit Length(const Measure &other)
        : Length(other.value(), other.unit()) {}
    PROJ_DLL Length(const Length &other);
    PROJ_DLL ~Length() override;
    //! @endcond

  protected:
    PROJ_FRIEND_OPTIONAL(Length);
    Length &operator=(const Length &) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Date-time value, as a ISO:8601 encoded string, or other string
 * encoding */
class DateTime {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL DateTime(const DateTime &other);
    PROJ_DLL ~DateTime();
    //! @endcond

    PROJ_DLL bool isISO_8601() const;
    PROJ_DLL std::string toString() const;

    PROJ_DLL static DateTime
    create(const std::string &str); // may throw Exception

  protected:
    DateTime();
    PROJ_FRIEND_OPTIONAL(DateTime);
    DateTime &operator=(const DateTime &other);

  private:
    explicit DateTime(const std::string &str);

    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

/** \brief Data epoch */
class DataEpoch {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL explicit DataEpoch(const Measure &coordinateEpochIn);
    PROJ_DLL DataEpoch(const DataEpoch &other);
    PROJ_DLL ~DataEpoch();
    //! @endcond

    PROJ_DLL const Measure &coordinateEpoch() const;

  protected:
    DataEpoch();
    PROJ_FRIEND_OPTIONAL(DataEpoch);

  private:
    DataEpoch &operator=(const DataEpoch &other) = delete;
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class IdentifiedObject;
/** Shared pointer of IdentifiedObject. */
using IdentifiedObjectPtr = std::shared_ptr<IdentifiedObject>;
/** Non-null shared pointer of IdentifiedObject. */
using IdentifiedObjectNNPtr = util::nn<IdentifiedObjectPtr>;

/** \brief Abstract class representing a CRS-related object that has an
 * identification.
 *
 * \remark Implements IdentifiedObject from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL IdentifiedObject : public util::BaseObject,
                                      public util::IComparable,
                                      public io::IWKTExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~IdentifiedObject() override;
    //! @endcond

    PROJ_DLL static const std::string NAME_KEY;
    PROJ_DLL static const std::string IDENTIFIERS_KEY;
    PROJ_DLL static const std::string ALIAS_KEY;
    PROJ_DLL static const std::string REMARKS_KEY;
    PROJ_DLL static const std::string DEPRECATED_KEY;

    // in practice only name().description() is used
    PROJ_DLL const metadata::IdentifierNNPtr &name() PROJ_PURE_DECL;
    PROJ_DLL const std::string &nameStr() PROJ_PURE_DECL;
    PROJ_DLL const std::vector<metadata::IdentifierNNPtr> &
    identifiers() PROJ_PURE_DECL;
    PROJ_DLL const std::vector<util::GenericNameNNPtr> &
    aliases() PROJ_PURE_DECL;
    PROJ_DLL const std::string &remarks() PROJ_PURE_DECL;

    // from Apache SIS AbstractIdentifiedObject
    PROJ_DLL bool isDeprecated() PROJ_PURE_DECL;

    // Non-standard
    PROJ_DLL std::string alias() PROJ_PURE_DECL;
    PROJ_DLL int getEPSGCode() PROJ_PURE_DECL;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        void
        formatID(io::WKTFormatter *formatter) const;

    PROJ_INTERNAL void formatID(io::JSONFormatter *formatter) const;

    PROJ_INTERNAL void formatRemarks(io::WKTFormatter *formatter) const;

    PROJ_INTERNAL void formatRemarks(io::JSONFormatter *formatter) const;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const IdentifiedObject *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) PROJ_PURE_DECL;
    //! @endcond

  protected:
    PROJ_FRIEND_OPTIONAL(IdentifiedObject);
    INLINED_MAKE_SHARED
    IdentifiedObject();
    IdentifiedObject(const IdentifiedObject &other);

    void setProperties(const util::PropertyMap
                           &properties); // throw(InvalidValueTypeException)

    virtual bool hasEquivalentNameToUsingAlias(
        const IdentifiedObject *other,
        const io::DatabaseContextPtr &dbContext) const;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    IdentifiedObject &operator=(const IdentifiedObject &other) = delete;
};

// ---------------------------------------------------------------------------

class ObjectDomain;
/** Shared pointer of ObjectDomain. */
using ObjectDomainPtr = std::shared_ptr<ObjectDomain>;
/** Non-null shared pointer of ObjectDomain. */
using ObjectDomainNNPtr = util::nn<ObjectDomainPtr>;

/** \brief The scope and validity of a CRS-related object.
 *
 * \remark Implements ObjectDomain from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL ObjectDomain : public util::BaseObject,
                                  public util::IComparable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~ObjectDomain() override;
    //! @endcond

    // In ISO_19111:2018, scope and domain are compulsory, but in WKT2:2015,
    // they
    // are not necessarily both specified
    PROJ_DLL const util::optional<std::string> &scope() PROJ_PURE_DECL;
    PROJ_DLL const metadata::ExtentPtr &domainOfValidity() PROJ_PURE_DECL;

    PROJ_DLL static ObjectDomainNNPtr
    create(const util::optional<std::string> &scopeIn,
           const metadata::ExtentPtr &extent);

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        void
        _exportToWKT(io::WKTFormatter *formatter)
            const; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(
        io::JSONFormatter *formatter) const; // throw(FormattingException)

    bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    //! @cond Doxygen_Suppress
    ObjectDomain(const util::optional<std::string> &scopeIn,
                 const metadata::ExtentPtr &extent);
    //! @endcond

    ObjectDomain(const ObjectDomain &other);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    ObjectDomain &operator=(const ObjectDomain &other) = delete;
};

// ---------------------------------------------------------------------------

class ObjectUsage;
/** Shared pointer of ObjectUsage. */
using ObjectUsagePtr = std::shared_ptr<ObjectUsage>;
/** Non-null shared pointer of ObjectUsage. */
using ObjectUsageNNPtr = util::nn<ObjectUsagePtr>;

/** \brief Abstract class of a CRS-related object that has usages.
 *
 * \remark Implements ObjectUsage from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL ObjectUsage : public IdentifiedObject {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~ObjectUsage() override;
    //! @endcond

    PROJ_DLL const std::vector<ObjectDomainNNPtr> &domains() PROJ_PURE_DECL;

    PROJ_DLL static const std::string SCOPE_KEY;
    PROJ_DLL static const std::string DOMAIN_OF_VALIDITY_KEY;

    PROJ_DLL static const std::string OBJECT_DOMAIN_KEY;

    //! @cond Doxygen_Suppress
    bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    ObjectUsage();
    ObjectUsage(const ObjectUsage &other);
    void setProperties(const util::PropertyMap
                           &properties); // throw(InvalidValueTypeException)

    void baseExportToWKT(
        io::WKTFormatter *formatter) const; // throw(io::FormattingException)

    void baseExportToJSON(
        io::JSONFormatter *formatter) const; // throw(io::FormattingException)

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    ObjectUsage &operator=(const ObjectUsage &other) = delete;
};

} // namespace common

NS_PROJ_END

#endif //  COMMON_HH_INCLUDED
