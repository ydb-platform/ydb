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

#ifndef COORDINATEOPERATION_HH_INCLUDED
#define COORDINATEOPERATION_HH_INCLUDED

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common.hpp"
#include "io.hpp"
#include "metadata.hpp"

#include "proj.h"

NS_PROJ_START

namespace crs {
class CRS;
using CRSPtr = std::shared_ptr<CRS>;
using CRSNNPtr = util::nn<CRSPtr>;

class DerivedCRS;
class ProjectedCRS;
} // namespace crs

namespace io {
class JSONParser;
} // namespace io

namespace coordinates {
class CoordinateMetadata;
using CoordinateMetadataPtr = std::shared_ptr<CoordinateMetadata>;
using CoordinateMetadataNNPtr = util::nn<CoordinateMetadataPtr>;
} // namespace coordinates

/** osgeo.proj.operation namespace

  \brief Coordinate operations (relationship between any two coordinate
  reference systems).

  This covers Conversion, Transformation,
  PointMotionOperation or ConcatenatedOperation.
*/
namespace operation {

// ---------------------------------------------------------------------------

/** \brief Grid description */
struct GridDescription {
    std::string shortName;   /**< Grid short filename */
    std::string fullName;    /**< Grid full path name (if found) */
    std::string packageName; /**< Package name (or empty) */
    std::string url;         /**< Grid URL (if packageName is empty), or package
                                    URL (or empty) */
    bool directDownload;     /**< Whether url can be fetched directly. */
    /** Whether the grid is released with an open license. */
    bool openLicense;
    bool available; /**< Whether GRID is available. */

    //! @cond Doxygen_Suppress
    bool operator<(const GridDescription &other) const {
        return shortName < other.shortName;
    }

    PROJ_DLL GridDescription();
    PROJ_DLL ~GridDescription();
    PROJ_DLL GridDescription(const GridDescription &);
    PROJ_DLL GridDescription(GridDescription &&) noexcept;
    //! @endcond
};

// ---------------------------------------------------------------------------

class CoordinateOperation;
/** Shared pointer of CoordinateOperation */
using CoordinateOperationPtr = std::shared_ptr<CoordinateOperation>;
/** Non-null shared pointer of CoordinateOperation */
using CoordinateOperationNNPtr = util::nn<CoordinateOperationPtr>;

// ---------------------------------------------------------------------------

class CoordinateTransformer;
/** Shared pointer of CoordinateTransformer */
using CoordinateTransformerPtr = std::unique_ptr<CoordinateTransformer>;
/** Non-null shared pointer of CoordinateTransformer */
using CoordinateTransformerNNPtr = util::nn<CoordinateTransformerPtr>;

/** \brief Coordinate transformer.
 *
 * Performs coordinate transformation of coordinate tuplies.
 *
 * @since 9.3
 */
class PROJ_GCC_DLL CoordinateTransformer {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~CoordinateTransformer();
    //! @endcond

    PROJ_DLL PJ_COORD transform(PJ_COORD coord);

  protected:
    PROJ_FRIEND(CoordinateOperation);

    PROJ_INTERNAL CoordinateTransformer();

    PROJ_INTERNAL static CoordinateTransformerNNPtr
    create(const CoordinateOperationNNPtr &op, PJ_CONTEXT *ctx);

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    INLINED_MAKE_UNIQUE
    CoordinateTransformer &
    operator=(const CoordinateTransformer &other) = delete;
};

// ---------------------------------------------------------------------------

class Transformation;
/** Shared pointer of Transformation */
using TransformationPtr = std::shared_ptr<Transformation>;
/** Non-null shared pointer of Transformation */
using TransformationNNPtr = util::nn<TransformationPtr>;

/** \brief Abstract class for a mathematical operation on coordinates.
 *
 * A mathematical operation:
 * <ul>
 * <li>on coordinates that transforms or converts them from one coordinate
 * reference system to another coordinate reference system</li>
 * <li>or that describes the change of coordinate values within one coordinate
 * reference system due to the motion of the point between one coordinate epoch
 * and another coordinate epoch.</li>
 * </ul>
 * Many but not all coordinate operations (from CRS A to CRS B) also uniquely
 * define the inverse coordinate operation (from CRS B to CRS A). In some cases,
 * the coordinate operation method algorithm for the inverse coordinate
 * operation is the same as for the forward algorithm, but the signs of some
 * coordinate operation parameter values have to be reversed. In other cases,
 * different algorithms are required for the forward and inverse coordinate
 * operations, but the same coordinate operation parameter values are used. If
 * (some) entirely different parameter values are needed, a different coordinate
 * operation shall be defined.
 *
 * \remark Implements CoordinateOperation from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL CoordinateOperation : public common::ObjectUsage,
                                         public io::IPROJStringExportable,
                                         public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~CoordinateOperation() override;
    //! @endcond

    PROJ_DLL const util::optional<std::string> &operationVersion() const;
    PROJ_DLL const std::vector<metadata::PositionalAccuracyNNPtr> &
    coordinateOperationAccuracies() const;

    PROJ_DLL const crs::CRSPtr sourceCRS() const;
    PROJ_DLL const crs::CRSPtr targetCRS() const;
    PROJ_DLL const crs::CRSPtr &interpolationCRS() const;
    PROJ_DLL const util::optional<common::DataEpoch> &
    sourceCoordinateEpoch() const;
    PROJ_DLL const util::optional<common::DataEpoch> &
    targetCoordinateEpoch() const;

    PROJ_DLL CoordinateTransformerNNPtr
    coordinateTransformer(PJ_CONTEXT *ctx) const;

    /** \brief Return the inverse of the coordinate operation.
     *
     * \throw util::UnsupportedOperationException if inverse is not available
     */
    PROJ_DLL virtual CoordinateOperationNNPtr inverse() const = 0;

    /** \brief Return grids needed by an operation. */
    PROJ_DLL virtual std::set<GridDescription>
    gridsNeeded(const io::DatabaseContextPtr &databaseContext,
                bool considerKnownGridsAsAvailable) const = 0;

    PROJ_DLL bool
    isPROJInstantiable(const io::DatabaseContextPtr &databaseContext,
                       bool considerKnownGridsAsAvailable) const;

    PROJ_DLL bool hasBallparkTransformation() const;

    PROJ_DLL bool requiresPerCoordinateInputTime() const;

    PROJ_DLL static const std::string OPERATION_VERSION_KEY;

    PROJ_DLL CoordinateOperationNNPtr normalizeForVisualization() const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_FOR_TEST CoordinateOperationNNPtr
        shallowClone() const;
    //! @endcond

  protected:
    PROJ_INTERNAL CoordinateOperation();
    PROJ_INTERNAL CoordinateOperation(const CoordinateOperation &other);

    PROJ_FRIEND(crs::DerivedCRS);
    PROJ_FRIEND(io::AuthorityFactory);
    PROJ_FRIEND(CoordinateOperationFactory);
    PROJ_FRIEND(ConcatenatedOperation);
    PROJ_FRIEND(io::WKTParser);
    PROJ_FRIEND(io::JSONParser);
    PROJ_INTERNAL void
    setWeakSourceTargetCRS(std::weak_ptr<crs::CRS> sourceCRSIn,
                           std::weak_ptr<crs::CRS> targetCRSIn);
    PROJ_INTERNAL void setCRSs(const crs::CRSNNPtr &sourceCRSIn,
                               const crs::CRSNNPtr &targetCRSIn,
                               const crs::CRSPtr &interpolationCRSIn);
    PROJ_INTERNAL void
    setCRSsUpdateInverse(const crs::CRSNNPtr &sourceCRSIn,
                         const crs::CRSNNPtr &targetCRSIn,
                         const crs::CRSPtr &interpolationCRSIn);
    PROJ_INTERNAL void
    setInterpolationCRS(const crs::CRSPtr &interpolationCRSIn);
    PROJ_INTERNAL void setCRSs(const CoordinateOperation *in,
                               bool inverseSourceTarget);
    PROJ_INTERNAL
    void setAccuracies(
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);
    PROJ_INTERNAL void setHasBallparkTransformation(bool b);

    PROJ_INTERNAL void setRequiresPerCoordinateInputTime(bool b);

    PROJ_INTERNAL void
    setSourceCoordinateEpoch(const util::optional<common::DataEpoch> &epoch);
    PROJ_INTERNAL void
    setTargetCoordinateEpoch(const util::optional<common::DataEpoch> &epoch);

    PROJ_INTERNAL void
    setProperties(const util::PropertyMap
                      &properties); // throw(InvalidValueTypeException)

    PROJ_INTERNAL virtual CoordinateOperationNNPtr _shallowClone() const = 0;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    CoordinateOperation &operator=(const CoordinateOperation &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Abstract class modelling a parameter value (OperationParameter)
 * or group of parameters.
 *
 * \remark Implements GeneralOperationParameter from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL GeneralOperationParameter : public common::IdentifiedObject {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~GeneralOperationParameter() override;
    //! @endcond

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override = 0;
    //! @endcond

  protected:
    PROJ_INTERNAL GeneralOperationParameter();
    PROJ_INTERNAL
    GeneralOperationParameter(const GeneralOperationParameter &other);

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    GeneralOperationParameter &
    operator=(const GeneralOperationParameter &other) = delete;
};

/** Shared pointer of GeneralOperationParameter */
using GeneralOperationParameterPtr = std::shared_ptr<GeneralOperationParameter>;
/** Non-null shared pointer of GeneralOperationParameter */
using GeneralOperationParameterNNPtr = util::nn<GeneralOperationParameterPtr>;

// ---------------------------------------------------------------------------

class OperationParameter;
/** Shared pointer of OperationParameter */
using OperationParameterPtr = std::shared_ptr<OperationParameter>;
/** Non-null shared pointer of OperationParameter */
using OperationParameterNNPtr = util::nn<OperationParameterPtr>;

/** \brief The definition of a parameter used by a coordinate operation method.
 *
 * Most parameter values are numeric, but other types of parameter values are
 * possible.
 *
 * \remark Implements OperationParameter from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL OperationParameter final : public GeneralOperationParameter {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~OperationParameter() override;
    //! @endcond

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

    // non-standard
    PROJ_DLL static OperationParameterNNPtr
    create(const util::PropertyMap &properties);

    PROJ_DLL int getEPSGCode() PROJ_PURE_DECL;

    PROJ_DLL static const char *getNameForEPSGCode(int epsg_code) noexcept;

  protected:
    PROJ_INTERNAL OperationParameter();
    PROJ_INTERNAL OperationParameter(const OperationParameter &other);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    OperationParameter &operator=(const OperationParameter &other) = delete;

    // cppcheck-suppress functionStatic
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)
};

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct MethodMapping;
//! @endcond

/** \brief Abstract class modelling a parameter value (OperationParameterValue)
 * or group of parameter values.
 *
 * \remark Implements GeneralParameterValue from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL GeneralParameterValue : public util::BaseObject,
                                           public io::IWKTExportable,
                                           public io::IJSONExportable,
                                           public util::IComparable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~GeneralParameterValue() override;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override = 0; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override = 0; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override = 0;
    //! @endcond

  protected:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL GeneralParameterValue();
    PROJ_INTERNAL GeneralParameterValue(const GeneralParameterValue &other);

    friend class Conversion;
    friend class SingleOperation;
    friend class PointMotionOperation;
    PROJ_INTERNAL virtual void _exportToWKT(io::WKTFormatter *formatter,
                                            const MethodMapping *mapping)
        const = 0; // throw(io::FormattingException)
                   //! @endcond

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    GeneralParameterValue &
    operator=(const GeneralParameterValue &other) = delete;
};

/** Shared pointer of GeneralParameterValue */
using GeneralParameterValuePtr = std::shared_ptr<GeneralParameterValue>;
/** Non-null shared pointer of GeneralParameterValue */
using GeneralParameterValueNNPtr = util::nn<GeneralParameterValuePtr>;

// ---------------------------------------------------------------------------

class ParameterValue;
/** Shared pointer of ParameterValue */
using ParameterValuePtr = std::shared_ptr<ParameterValue>;
/** Non-null shared pointer of ParameterValue */
using ParameterValueNNPtr = util::nn<ParameterValuePtr>;

/** \brief The value of the coordinate operation parameter.
 *
 * Most parameter values are numeric, but other types of parameter values are
 * possible.
 *
 * \remark Implements ParameterValue from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL ParameterValue final : public util::BaseObject,
                                          public io::IWKTExportable,
                                          public util::IComparable {
  public:
    /** Type of the value. */
    enum class Type {
        /** Measure (i.e. value with a unit) */
        MEASURE,
        /** String */
        STRING,
        /** Integer */
        INTEGER,
        /** Boolean */
        BOOLEAN,
        /** Filename */
        FILENAME
    };
    //! @cond Doxygen_Suppress
    PROJ_DLL ~ParameterValue() override;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)
    //! @endcond

    PROJ_DLL static ParameterValueNNPtr
    create(const common::Measure &measureIn);
    PROJ_DLL static ParameterValueNNPtr create(const char *stringValueIn);
    PROJ_DLL static ParameterValueNNPtr
    create(const std::string &stringValueIn);
    PROJ_DLL static ParameterValueNNPtr create(int integerValueIn);
    PROJ_DLL static ParameterValueNNPtr create(bool booleanValueIn);
    PROJ_DLL static ParameterValueNNPtr
    createFilename(const std::string &stringValueIn);

    PROJ_DLL const Type &type() PROJ_PURE_DECL;
    PROJ_DLL const common::Measure &value() PROJ_PURE_DECL;
    PROJ_DLL const std::string &stringValue() PROJ_PURE_DECL;
    PROJ_DLL const std::string &valueFile() PROJ_PURE_DECL;
    PROJ_DLL int integerValue() PROJ_PURE_DECL;
    PROJ_DLL bool booleanValue() PROJ_PURE_DECL;

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    PROJ_INTERNAL explicit ParameterValue(const common::Measure &measureIn);
    PROJ_INTERNAL explicit ParameterValue(const std::string &stringValueIn,
                                          Type typeIn);
    PROJ_INTERNAL explicit ParameterValue(int integerValueIn);
    PROJ_INTERNAL explicit ParameterValue(bool booleanValueIn);
    INLINED_MAKE_SHARED
  private:
    PROJ_OPAQUE_PRIVATE_DATA
    ParameterValue &operator=(const ParameterValue &other) = delete;
};

// ---------------------------------------------------------------------------

class OperationParameterValue;
/** Shared pointer of OperationParameterValue */
using OperationParameterValuePtr = std::shared_ptr<OperationParameterValue>;
/** Non-null shared pointer of OperationParameterValue */
using OperationParameterValueNNPtr = util::nn<OperationParameterValuePtr>;

/** \brief A parameter value, ordered sequence of values, or reference to a
 * file of parameter values.
 *
 * This combines a OperationParameter with the corresponding ParameterValue.
 *
 * \remark Implements OperationParameterValue from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL OperationParameterValue final
    : public GeneralParameterValue {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~OperationParameterValue() override;
    //! @endcond

    PROJ_DLL const OperationParameterNNPtr &parameter() PROJ_PURE_DECL;
    PROJ_DLL const ParameterValueNNPtr &parameterValue() PROJ_PURE_DECL;

    PROJ_DLL static OperationParameterValueNNPtr
    create(const OperationParameterNNPtr &parameterIn,
           const ParameterValueNNPtr &valueIn);

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL static bool
        convertFromAbridged(const std::string &paramName, double &val,
                            const common::UnitOfMeasure *&unit,
                            int &paramEPSGCode);

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    PROJ_INTERNAL
    OperationParameterValue(const OperationParameterNNPtr &parameterIn,
                            const ParameterValueNNPtr &valueIn);
    PROJ_INTERNAL OperationParameterValue(const OperationParameterValue &other);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter,
                                    const MethodMapping *mapping)
        const override; // throw(io::FormattingException)

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    OperationParameterValue &
    operator=(const OperationParameterValue &other) = delete;
};

// ---------------------------------------------------------------------------

class OperationMethod;
/** Shared pointer of OperationMethod */
using OperationMethodPtr = std::shared_ptr<OperationMethod>;
/** Non-null shared pointer of OperationMethod */
using OperationMethodNNPtr = util::nn<OperationMethodPtr>;

/** \brief The method (algorithm or procedure) used to perform the
 * coordinate operation.
 *
 * For a projection method, this contains the name of the projection method
 * and the name of the projection parameters.
 *
 * \remark Implements OperationMethod from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL OperationMethod : public common::IdentifiedObject,
                                     public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~OperationMethod() override;
    //! @endcond

    PROJ_DLL const util::optional<std::string> &formula() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<metadata::Citation> &
    formulaCitation() PROJ_PURE_DECL;
    PROJ_DLL const std::vector<GeneralOperationParameterNNPtr> &
    parameters() PROJ_PURE_DECL;

    PROJ_DLL static OperationMethodNNPtr
    create(const util::PropertyMap &properties,
           const std::vector<GeneralOperationParameterNNPtr> &parameters);

    PROJ_DLL static OperationMethodNNPtr
    create(const util::PropertyMap &properties,
           const std::vector<OperationParameterNNPtr> &parameters);

    PROJ_DLL int getEPSGCode() PROJ_PURE_DECL;

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    PROJ_INTERNAL OperationMethod();
    PROJ_INTERNAL OperationMethod(const OperationMethod &other);
    INLINED_MAKE_SHARED
    friend class Conversion;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    OperationMethod &operator=(const OperationMethod &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Exception that can be thrown when an invalid operation is attempted
 * to be constructed.
 */
class PROJ_GCC_DLL InvalidOperation : public util::Exception {
  public:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit InvalidOperation(const char *message);
    PROJ_INTERNAL explicit InvalidOperation(const std::string &message);
    PROJ_DLL InvalidOperation(const InvalidOperation &other);
    PROJ_DLL ~InvalidOperation() override;
    //! @endcond
};

// ---------------------------------------------------------------------------

class SingleOperation;
/** Shared pointer of SingleOperation */
using SingleOperationPtr = std::shared_ptr<SingleOperation>;
/** Non-null shared pointer of SingleOperation */
using SingleOperationNNPtr = util::nn<SingleOperationPtr>;

/** \brief A single (not concatenated) coordinate operation
 * (CoordinateOperation)
 *
 * \remark Implements SingleOperation from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL SingleOperation : virtual public CoordinateOperation {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~SingleOperation() override;
    //! @endcond

    PROJ_DLL const std::vector<GeneralParameterValueNNPtr> &
    parameterValues() PROJ_PURE_DECL;
    PROJ_DLL const OperationMethodNNPtr &method() PROJ_PURE_DECL;

    PROJ_DLL const ParameterValuePtr &
    parameterValue(const std::string &paramName,
                   int epsg_code = 0) const noexcept;

    PROJ_DLL const ParameterValuePtr &
    parameterValue(int epsg_code) const noexcept;

    PROJ_DLL const common::Measure &
    parameterValueMeasure(const std::string &paramName,
                          int epsg_code = 0) const noexcept;

    PROJ_DLL const common::Measure &
    parameterValueMeasure(int epsg_code) const noexcept;

    PROJ_DLL static SingleOperationNNPtr createPROJBased(
        const util::PropertyMap &properties, const std::string &PROJString,
        const crs::CRSPtr &sourceCRS, const crs::CRSPtr &targetCRS,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies =
            std::vector<metadata::PositionalAccuracyNNPtr>());

    PROJ_DLL std::set<GridDescription>
    gridsNeeded(const io::DatabaseContextPtr &databaseContext,
                bool considerKnownGridsAsAvailable) const override;

    PROJ_DLL std::list<std::string> validateParameters() const;

    PROJ_DLL TransformationNNPtr substitutePROJAlternativeGridNames(
        io::DatabaseContextNNPtr databaseContext) const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress

        PROJ_DLL double
        parameterValueNumeric(
            int epsg_code,
            const common::UnitOfMeasure &targetUnit) const noexcept;

    PROJ_INTERNAL double parameterValueNumeric(
        const char *param_name,
        const common::UnitOfMeasure &targetUnit) const noexcept;

    PROJ_INTERNAL double
    parameterValueNumericAsSI(int epsg_code) const noexcept;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL bool isLongitudeRotation() const;

    //! @endcond

  protected:
    PROJ_INTERNAL explicit SingleOperation(
        const OperationMethodNNPtr &methodIn);
    PROJ_INTERNAL SingleOperation(const SingleOperation &other);

    PROJ_INTERNAL void
    setParameterValues(const std::vector<GeneralParameterValueNNPtr> &values);

    PROJ_INTERNAL void
    exportTransformationToWKT(io::WKTFormatter *formatter) const;

    PROJ_INTERNAL bool
    exportToPROJStringGeneric(io::PROJStringFormatter *formatter) const;

    PROJ_INTERNAL bool _isEquivalentTo(const util::IComparable *other,
                                       util::IComparable::Criterion criterion,
                                       const io::DatabaseContextPtr &dbContext,
                                       bool inOtherDirection) const;

    PROJ_INTERNAL static GeneralParameterValueNNPtr
    createOperationParameterValueFromInterpolationCRS(int methodEPSGCode,
                                                      int crsEPSGCode);

    PROJ_INTERNAL static void
    exportToPROJStringChangeVerticalUnit(io::PROJStringFormatter *formatter,
                                         double convFactor);

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    SingleOperation &operator=(const SingleOperation &other) = delete;
};

// ---------------------------------------------------------------------------

class Conversion;
/** Shared pointer of Conversion */
using ConversionPtr = std::shared_ptr<Conversion>;
/** Non-null shared pointer of Conversion */
using ConversionNNPtr = util::nn<ConversionPtr>;

/** \brief A mathematical operation on coordinates in which the parameter
 * values are defined rather than empirically derived.
 *
 * Application of the coordinate conversion introduces no error into output
 * coordinates. The best-known example of a coordinate conversion is a map
 * projection. For coordinate conversions the output coordinates are referenced
 * to the same datum as are the input coordinates.
 *
 * Coordinate conversions forming a component of a derived CRS have a source
 * crs::CRS and a target crs::CRS that are NOT specified through the source and
 * target
 * associations, but through associations from crs::DerivedCRS to
 * crs::SingleCRS.
 *
 * \remark Implements Conversion from \ref ISO_19111_2019
 */

/*!

\section projection_parameters Projection parameters

\subsection colatitude_cone_axis Co-latitude of cone axis

The rotation applied to spherical coordinates for the oblique projection,
measured on the conformal sphere in the plane of the meridian of origin.

EPSG:1036

\subsection center_latitude Latitude of natural origin/Center Latitude

The latitude of the point from which the values of both the geographical
coordinates on the ellipsoid and the grid coordinates on the projection are
deemed to increment or decrement for computational purposes. Alternatively it
may be considered as the latitude of the point which in the absence of
application of false coordinates has grid coordinates of (0,0).

EPSG:8801

\subsection center_longitude Longitude of natural origin/Central Meridian

The longitude of the point from which the values of both the geographical
coordinates on the ellipsoid and the grid coordinates on the projection are
deemed to increment or decrement for computational purposes. Alternatively it
may be considered as the longitude of the point which in the absence of
application of false coordinates has grid coordinates of (0,0).  Sometimes known
as "central meridian (CM)".

EPSG:8802

\subsection scale Scale Factor

The factor by which the map grid is reduced or enlarged during the projection
process, defined by its value at the natural origin.

EPSG:8805

\subsection false_easting False Easting

Since the natural origin may be at or near the centre of the projection and
under normal coordinate circumstances would thus give rise to negative
coordinates over parts of the mapped area, this origin is usually given false
coordinates which are large enough to avoid this inconvenience. The False
Easting, FE, is the value assigned to the abscissa (east or west) axis of the
projection grid at the natural origin.

EPSG:8806

\subsection false_northing False Northing

Since the natural origin may be at or near the centre of the projection and
under normal coordinate circumstances would thus give rise to negative
coordinates over parts of the mapped area, this origin is usually given false
coordinates which are large enough to avoid this inconvenience. The False
Northing, FN, is the value assigned to the ordinate (north or south) axis of the
projection grid at the natural origin.

EPSG:8807

\subsection latitude_projection_centre Latitude of projection centre

For an oblique projection, this is the latitude of the point at which the
azimuth of the central line is defined.

EPSG:8811

\subsection longitude_projection_centre Longitude of projection centre

For an oblique projection, this is the longitude of the point at which the
azimuth of the central line is defined.

EPSG:8812

\subsection azimuth_initial_line Azimuth of initial line

The azimuthal direction (north zero, east of north being positive) of the great
circle which is the centre line of an oblique projection. The azimuth is given
at the projection centre.

EPSG:8813

\subsection angle_from_recitfied_to_skrew_grid Angle from Rectified to Skew Grid

The angle at the natural origin of an oblique projection through which the
natural coordinate reference system is rotated to make the projection north
axis parallel with true north.

EPSG:8814

\subsection scale_factor_initial_line Scale factor on initial line

The factor by which the map grid is reduced or enlarged during the projection
process, defined by its value at the projection center.

EPSG:8815

\subsection easting_projection_centre Easting at projection centre

The easting value assigned to the projection centre.

EPSG:8816

\subsection northing_projection_centre Northing at projection centre

The northing value assigned to the projection centre.

EPSG:8817

\subsection latitude_pseudo_standard_parallel Latitude of pseudo standard
parallel

Latitude of the parallel on which the conic or cylindrical projection is based.
This latitude is not geographic, but is defined on the conformal sphere AFTER
its rotation to obtain the oblique aspect of the projection.

EPSG:8818

\subsection scale_factor_pseudo_standard_parallel Scale factor on pseudo
standard parallel

The factor by which the map grid is reduced or enlarged during the projection
process, defined by its value at the pseudo-standard parallel.
EPSG:8819

\subsection latitude_false_origin Latitude of false origin

The latitude of the point which is not the natural origin and at which grid
coordinate values false easting and false northing are defined.

EPSG:8821

\subsection longitude_false_origin Longitude of false origin

The longitude of the point which is not the natural origin and at which grid
coordinate values false easting and false northing are defined.

EPSG:8822

\subsection latitude_first_std_parallel Latitude of 1st standard parallel

For a conic projection with two standard parallels, this is the latitude of one
of the parallels of intersection of the cone with the ellipsoid. It is normally
but not necessarily that nearest to the pole. Scale is true along this parallel.

EPSG:8823

\subsection latitude_second_std_parallel Latitude of 2nd standard parallel

For a conic projection with two standard parallels, this is the latitude of one
of the parallels at which the cone intersects with the ellipsoid. It is normally
but not necessarily that nearest to the equator. Scale is true along this
parallel.

EPSG:8824

\subsection easting_false_origin Easting of false origin

The easting value assigned to the false origin.

EPSG:8826

\subsection northing_false_origin Northing of false origin

The northing value assigned to the false origin.

EPSG:8827

\subsection latitude_std_parallel Latitude of standard parallel

For polar aspect azimuthal projections, the parallel on which the scale factor
is defined to be unity.

EPSG:8832

\subsection longitude_of_origin Longitude of origin

For polar aspect azimuthal projections, the meridian along which the
northing axis increments and also across which parallels of latitude
increment towards the north pole.

EPSG:8833

*/

class PROJ_GCC_DLL Conversion : public SingleOperation {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~Conversion() override;
    //! @endcond

    PROJ_DLL CoordinateOperationNNPtr inverse() const override;

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)
    //! @endcond

    PROJ_DLL bool isUTM(int &zone, bool &north) const;

    PROJ_DLL ConversionNNPtr identify() const;

    PROJ_DLL static ConversionNNPtr
    create(const util::PropertyMap &properties,
           const OperationMethodNNPtr &methodIn,
           const std::vector<GeneralParameterValueNNPtr>
               &values); // throw InvalidOperation

    PROJ_DLL static ConversionNNPtr
    create(const util::PropertyMap &propertiesConversion,
           const util::PropertyMap &propertiesOperationMethod,
           const std::vector<OperationParameterNNPtr> &parameters,
           const std::vector<ParameterValueNNPtr>
               &values); // throw InvalidOperation

    PROJ_DLL static ConversionNNPtr
    createUTM(const util::PropertyMap &properties, int zone, bool north);

    PROJ_DLL static ConversionNNPtr createTransverseMercator(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Scale &scale,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createGaussSchreiberTransverseMercator(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Scale &scale,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createTransverseMercatorSouthOriented(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Scale &scale,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createTwoPointEquidistant(const util::PropertyMap &properties,
                              const common::Angle &latitudeFirstPoint,
                              const common::Angle &longitudeFirstPoint,
                              const common::Angle &latitudeSecondPoint,
                              const common::Angle &longitudeSeconPoint,
                              const common::Length &falseEasting,
                              const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createTunisiaMappingGrid(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createTunisiaMiningGrid(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createAlbersEqualArea(const util::PropertyMap &properties,
                          const common::Angle &latitudeFalseOrigin,
                          const common::Angle &longitudeFalseOrigin,
                          const common::Angle &latitudeFirstParallel,
                          const common::Angle &latitudeSecondParallel,
                          const common::Length &eastingFalseOrigin,
                          const common::Length &northingFalseOrigin);

    PROJ_DLL static ConversionNNPtr createLambertConicConformal_1SP(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Scale &scale,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createLambertConicConformal_1SP_VariantB(
        const util::PropertyMap &properties,
        const common::Angle &latitudeNatOrigin, const common::Scale &scale,
        const common::Angle &latitudeFalseOrigin,
        const common::Angle &longitudeFalseOrigin,
        const common::Length &eastingFalseOrigin,
        const common::Length &northingFalseOrigin);

    PROJ_DLL static ConversionNNPtr
    createLambertConicConformal_2SP(const util::PropertyMap &properties,
                                    const common::Angle &latitudeFalseOrigin,
                                    const common::Angle &longitudeFalseOrigin,
                                    const common::Angle &latitudeFirstParallel,
                                    const common::Angle &latitudeSecondParallel,
                                    const common::Length &eastingFalseOrigin,
                                    const common::Length &northingFalseOrigin);

    PROJ_DLL static ConversionNNPtr createLambertConicConformal_2SP_Michigan(
        const util::PropertyMap &properties,
        const common::Angle &latitudeFalseOrigin,
        const common::Angle &longitudeFalseOrigin,
        const common::Angle &latitudeFirstParallel,
        const common::Angle &latitudeSecondParallel,
        const common::Length &eastingFalseOrigin,
        const common::Length &northingFalseOrigin,
        const common::Scale &ellipsoidScalingFactor);

    PROJ_DLL static ConversionNNPtr createLambertConicConformal_2SP_Belgium(
        const util::PropertyMap &properties,
        const common::Angle &latitudeFalseOrigin,
        const common::Angle &longitudeFalseOrigin,
        const common::Angle &latitudeFirstParallel,
        const common::Angle &latitudeSecondParallel,
        const common::Length &eastingFalseOrigin,
        const common::Length &northingFalseOrigin);

    PROJ_DLL static ConversionNNPtr
    createAzimuthalEquidistant(const util::PropertyMap &properties,
                               const common::Angle &latitudeNatOrigin,
                               const common::Angle &longitudeNatOrigin,
                               const common::Length &falseEasting,
                               const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createGuamProjection(const util::PropertyMap &properties,
                         const common::Angle &latitudeNatOrigin,
                         const common::Angle &longitudeNatOrigin,
                         const common::Length &falseEasting,
                         const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createBonne(const util::PropertyMap &properties,
                const common::Angle &latitudeNatOrigin,
                const common::Angle &longitudeNatOrigin,
                const common::Length &falseEasting,
                const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createLambertCylindricalEqualAreaSpherical(
        const util::PropertyMap &properties,
        const common::Angle &latitudeFirstParallel,
        const common::Angle &longitudeNatOrigin,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createLambertCylindricalEqualArea(
        const util::PropertyMap &properties,
        const common::Angle &latitudeFirstParallel,
        const common::Angle &longitudeNatOrigin,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createCassiniSoldner(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createEquidistantConic(const util::PropertyMap &properties,
                           const common::Angle &latitudeFalseOrigin,
                           const common::Angle &longitudeFalseOrigin,
                           const common::Angle &latitudeFirstParallel,
                           const common::Angle &latitudeSecondParallel,
                           const common::Length &eastingFalseOrigin,
                           const common::Length &northingFalseOrigin);

    PROJ_DLL static ConversionNNPtr
    createEckertI(const util::PropertyMap &properties,
                  const common::Angle &centerLong,
                  const common::Length &falseEasting,
                  const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createEckertII(const util::PropertyMap &properties,
                   const common::Angle &centerLong,
                   const common::Length &falseEasting,
                   const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createEckertIII(const util::PropertyMap &properties,
                    const common::Angle &centerLong,
                    const common::Length &falseEasting,
                    const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createEckertIV(const util::PropertyMap &properties,
                   const common::Angle &centerLong,
                   const common::Length &falseEasting,
                   const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createEckertV(const util::PropertyMap &properties,
                  const common::Angle &centerLong,
                  const common::Length &falseEasting,
                  const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createEckertVI(const util::PropertyMap &properties,
                   const common::Angle &centerLong,
                   const common::Length &falseEasting,
                   const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createEquidistantCylindrical(const util::PropertyMap &properties,
                                 const common::Angle &latitudeFirstParallel,
                                 const common::Angle &longitudeNatOrigin,
                                 const common::Length &falseEasting,
                                 const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createEquidistantCylindricalSpherical(
        const util::PropertyMap &properties,
        const common::Angle &latitudeFirstParallel,
        const common::Angle &longitudeNatOrigin,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createGall(const util::PropertyMap &properties,
               const common::Angle &centerLong,
               const common::Length &falseEasting,
               const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createGoodeHomolosine(const util::PropertyMap &properties,
                          const common::Angle &centerLong,
                          const common::Length &falseEasting,
                          const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createInterruptedGoodeHomolosine(const util::PropertyMap &properties,
                                     const common::Angle &centerLong,
                                     const common::Length &falseEasting,
                                     const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createGeostationarySatelliteSweepX(
        const util::PropertyMap &properties, const common::Angle &centerLong,
        const common::Length &height, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createGeostationarySatelliteSweepY(
        const util::PropertyMap &properties, const common::Angle &centerLong,
        const common::Length &height, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createGnomonic(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createHotineObliqueMercatorVariantA(
        const util::PropertyMap &properties,
        const common::Angle &latitudeProjectionCentre,
        const common::Angle &longitudeProjectionCentre,
        const common::Angle &azimuthInitialLine,
        const common::Angle &angleFromRectifiedToSkrewGrid,
        const common::Scale &scale, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createHotineObliqueMercatorVariantB(
        const util::PropertyMap &properties,
        const common::Angle &latitudeProjectionCentre,
        const common::Angle &longitudeProjectionCentre,
        const common::Angle &azimuthInitialLine,
        const common::Angle &angleFromRectifiedToSkrewGrid,
        const common::Scale &scale,
        const common::Length &eastingProjectionCentre,
        const common::Length &northingProjectionCentre);

    PROJ_DLL static ConversionNNPtr
    createHotineObliqueMercatorTwoPointNaturalOrigin(
        const util::PropertyMap &properties,
        const common::Angle &latitudeProjectionCentre,
        const common::Angle &latitudePoint1,
        const common::Angle &longitudePoint1,
        const common::Angle &latitudePoint2,
        const common::Angle &longitudePoint2, const common::Scale &scale,
        const common::Length &eastingProjectionCentre,
        const common::Length &northingProjectionCentre);

    PROJ_DLL static ConversionNNPtr
    createLabordeObliqueMercator(const util::PropertyMap &properties,
                                 const common::Angle &latitudeProjectionCentre,
                                 const common::Angle &longitudeProjectionCentre,
                                 const common::Angle &azimuthInitialLine,
                                 const common::Scale &scale,
                                 const common::Length &falseEasting,
                                 const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createInternationalMapWorldPolyconic(
        const util::PropertyMap &properties, const common::Angle &centerLong,
        const common::Angle &latitudeFirstParallel,
        const common::Angle &latitudeSecondParallel,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createKrovakNorthOriented(
        const util::PropertyMap &properties,
        const common::Angle &latitudeProjectionCentre,
        const common::Angle &longitudeOfOrigin,
        const common::Angle &colatitudeConeAxis,
        const common::Angle &latitudePseudoStandardParallel,
        const common::Scale &scaleFactorPseudoStandardParallel,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createKrovak(const util::PropertyMap &properties,
                 const common::Angle &latitudeProjectionCentre,
                 const common::Angle &longitudeOfOrigin,
                 const common::Angle &colatitudeConeAxis,
                 const common::Angle &latitudePseudoStandardParallel,
                 const common::Scale &scaleFactorPseudoStandardParallel,
                 const common::Length &falseEasting,
                 const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createLambertAzimuthalEqualArea(const util::PropertyMap &properties,
                                    const common::Angle &latitudeNatOrigin,
                                    const common::Angle &longitudeNatOrigin,
                                    const common::Length &falseEasting,
                                    const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createMillerCylindrical(const util::PropertyMap &properties,
                            const common::Angle &centerLong,
                            const common::Length &falseEasting,
                            const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createMercatorVariantA(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Scale &scale,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createMercatorVariantB(const util::PropertyMap &properties,
                           const common::Angle &latitudeFirstParallel,
                           const common::Angle &centerLong,
                           const common::Length &falseEasting,
                           const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createPopularVisualisationPseudoMercator(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createMercatorSpherical(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createMollweide(const util::PropertyMap &properties,
                    const common::Angle &centerLong,
                    const common::Length &falseEasting,
                    const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createNewZealandMappingGrid(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createObliqueStereographic(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Scale &scale,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createOrthographic(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createLocalOrthographic(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong,
        const common::Angle &azimuthInitialLine, const common::Scale &scale,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createAmericanPolyconic(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createPolarStereographicVariantA(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Scale &scale,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createPolarStereographicVariantB(
        const util::PropertyMap &properties,
        const common::Angle &latitudeStandardParallel,
        const common::Angle &longitudeOfOrigin,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createRobinson(const util::PropertyMap &properties,
                   const common::Angle &centerLong,
                   const common::Length &falseEasting,
                   const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createSinusoidal(const util::PropertyMap &properties,
                     const common::Angle &centerLong,
                     const common::Length &falseEasting,
                     const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createStereographic(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Scale &scale,
        const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createVanDerGrinten(const util::PropertyMap &properties,
                        const common::Angle &centerLong,
                        const common::Length &falseEasting,
                        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createWagnerI(const util::PropertyMap &properties,
                  const common::Angle &centerLong,
                  const common::Length &falseEasting,
                  const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createWagnerII(const util::PropertyMap &properties,
                   const common::Angle &centerLong,
                   const common::Length &falseEasting,
                   const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createWagnerIII(const util::PropertyMap &properties,
                    const common::Angle &latitudeTrueScale,
                    const common::Angle &centerLong,
                    const common::Length &falseEasting,
                    const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createWagnerIV(const util::PropertyMap &properties,
                   const common::Angle &centerLong,
                   const common::Length &falseEasting,
                   const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createWagnerV(const util::PropertyMap &properties,
                  const common::Angle &centerLong,
                  const common::Length &falseEasting,
                  const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createWagnerVI(const util::PropertyMap &properties,
                   const common::Angle &centerLong,
                   const common::Length &falseEasting,
                   const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createWagnerVII(const util::PropertyMap &properties,
                    const common::Angle &centerLong,
                    const common::Length &falseEasting,
                    const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createQuadrilateralizedSphericalCube(
        const util::PropertyMap &properties, const common::Angle &centerLat,
        const common::Angle &centerLong, const common::Length &falseEasting,
        const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createSphericalCrossTrackHeight(
        const util::PropertyMap &properties, const common::Angle &pegPointLat,
        const common::Angle &pegPointLong, const common::Angle &pegPointHeading,
        const common::Length &pegPointHeight);

    PROJ_DLL static ConversionNNPtr
    createEqualEarth(const util::PropertyMap &properties,
                     const common::Angle &centerLong,
                     const common::Length &falseEasting,
                     const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr
    createVerticalPerspective(const util::PropertyMap &properties,
                              const common::Angle &topoOriginLat,
                              const common::Angle &topoOriginLong,
                              const common::Length &topoOriginHeight,
                              const common::Length &viewPointHeight,
                              const common::Length &falseEasting,
                              const common::Length &falseNorthing);

    PROJ_DLL static ConversionNNPtr createPoleRotationGRIBConvention(
        const util::PropertyMap &properties,
        const common::Angle &southPoleLatInUnrotatedCRS,
        const common::Angle &southPoleLongInUnrotatedCRS,
        const common::Angle &axisRotation);

    PROJ_DLL static ConversionNNPtr createPoleRotationNetCDFCFConvention(
        const util::PropertyMap &properties,
        const common::Angle &gridNorthPoleLatitude,
        const common::Angle &gridNorthPoleLongitude,
        const common::Angle &northPoleGridLongitude);

    PROJ_DLL static ConversionNNPtr
    createChangeVerticalUnit(const util::PropertyMap &properties,
                             const common::Scale &factor);

    PROJ_DLL static ConversionNNPtr
    createChangeVerticalUnit(const util::PropertyMap &properties);

    PROJ_DLL static ConversionNNPtr
    createHeightDepthReversal(const util::PropertyMap &properties);

    PROJ_DLL static ConversionNNPtr createAxisOrderReversal(bool is3D);

    PROJ_DLL static ConversionNNPtr
    createGeographicGeocentric(const util::PropertyMap &properties);

    PROJ_DLL static ConversionNNPtr
    createGeographic2DOffsets(const util::PropertyMap &properties,
                              const common::Angle &offsetLat,
                              const common::Angle &offsetLong);

    PROJ_DLL static ConversionNNPtr createGeographic3DOffsets(
        const util::PropertyMap &properties, const common::Angle &offsetLat,
        const common::Angle &offsetLong, const common::Length &offsetHeight);

    PROJ_DLL static ConversionNNPtr createGeographic2DWithHeightOffsets(
        const util::PropertyMap &properties, const common::Angle &offsetLat,
        const common::Angle &offsetLong, const common::Length &offsetHeight);

    PROJ_DLL static ConversionNNPtr
    createVerticalOffset(const util::PropertyMap &properties,
                         const common::Length &offsetHeight);

    PROJ_DLL ConversionPtr convertToOtherMethod(int targetEPSGCode) const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL void
        _exportToPROJString(io::PROJStringFormatter *formatter)
            const override; // throw(FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL const char *getESRIMethodName() const;

    PROJ_INTERNAL const char *getWKT1GDALMethodName() const;

    PROJ_INTERNAL ConversionNNPtr shallowClone() const;

    PROJ_INTERNAL ConversionNNPtr alterParametersLinearUnit(
        const common::UnitOfMeasure &unit, bool convertToNewUnit) const;

    PROJ_INTERNAL static ConversionNNPtr
    createGeographicGeocentric(const crs::CRSNNPtr &sourceCRS,
                               const crs::CRSNNPtr &targetCRS);

    PROJ_INTERNAL static ConversionNNPtr
    createGeographicGeocentricLatitude(const crs::CRSNNPtr &sourceCRS,
                                       const crs::CRSNNPtr &targetCRS);

    //! @endcond

  protected:
    PROJ_INTERNAL
    Conversion(const OperationMethodNNPtr &methodIn,
               const std::vector<GeneralParameterValueNNPtr> &values);
    PROJ_INTERNAL Conversion(const Conversion &other);
    INLINED_MAKE_SHARED

    PROJ_FRIEND(crs::ProjectedCRS);
    PROJ_INTERNAL bool addWKTExtensionNode(io::WKTFormatter *formatter) const;

    PROJ_INTERNAL CoordinateOperationNNPtr _shallowClone() const override;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    Conversion &operator=(const Conversion &other) = delete;

    PROJ_INTERNAL static ConversionNNPtr
    create(const util::PropertyMap &properties, int method_epsg_code,
           const std::vector<ParameterValueNNPtr> &values);

    PROJ_INTERNAL static ConversionNNPtr
    create(const util::PropertyMap &properties, const char *method_wkt2_name,
           const std::vector<ParameterValueNNPtr> &values);
};

// ---------------------------------------------------------------------------

/** \brief A mathematical operation on coordinates in which parameters are
 * empirically derived from data containing the coordinates of a series of
 * points in both coordinate reference systems.
 *
 * This computational process is usually "over-determined", allowing derivation
 * of error (or accuracy) estimates for the coordinate transformation. Also,
 * the stochastic nature of the parameters may result in multiple (different)
 * versions of the same coordinate transformations between the same source and
 * target CRSs. Any single coordinate operation in which the input and output
 * coordinates are referenced to different datums (reference frames) will be a
 * coordinate transformation.
 *
 * \remark Implements Transformation from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL Transformation : public SingleOperation {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~Transformation() override;
    //! @endcond

    PROJ_DLL const crs::CRSNNPtr &sourceCRS() PROJ_PURE_DECL;
    PROJ_DLL const crs::CRSNNPtr &targetCRS() PROJ_PURE_DECL;

    PROJ_DLL CoordinateOperationNNPtr inverse() const override;

    PROJ_DLL static TransformationNNPtr
    create(const util::PropertyMap &properties,
           const crs::CRSNNPtr &sourceCRSIn, const crs::CRSNNPtr &targetCRSIn,
           const crs::CRSPtr &interpolationCRSIn,
           const OperationMethodNNPtr &methodIn,
           const std::vector<GeneralParameterValueNNPtr> &values,
           const std::vector<metadata::PositionalAccuracyNNPtr>
               &accuracies); // throw InvalidOperation

    PROJ_DLL static TransformationNNPtr
    create(const util::PropertyMap &propertiesTransformation,
           const crs::CRSNNPtr &sourceCRSIn, const crs::CRSNNPtr &targetCRSIn,
           const crs::CRSPtr &interpolationCRSIn,
           const util::PropertyMap &propertiesOperationMethod,
           const std::vector<OperationParameterNNPtr> &parameters,
           const std::vector<ParameterValueNNPtr> &values,
           const std::vector<metadata::PositionalAccuracyNNPtr>
               &accuracies); // throw InvalidOperation

    PROJ_DLL static TransformationNNPtr createGeocentricTranslations(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, double translationXMetre,
        double translationYMetre, double translationZMetre,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createPositionVector(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, double translationXMetre,
        double translationYMetre, double translationZMetre,
        double rotationXArcSecond, double rotationYArcSecond,
        double rotationZArcSecond, double scaleDifferencePPM,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createCoordinateFrameRotation(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, double translationXMetre,
        double translationYMetre, double translationZMetre,
        double rotationXArcSecond, double rotationYArcSecond,
        double rotationZArcSecond, double scaleDifferencePPM,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createTimeDependentPositionVector(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, double translationXMetre,
        double translationYMetre, double translationZMetre,
        double rotationXArcSecond, double rotationYArcSecond,
        double rotationZArcSecond, double scaleDifferencePPM,
        double rateTranslationX, double rateTranslationY,
        double rateTranslationZ, double rateRotationX, double rateRotationY,
        double rateRotationZ, double rateScaleDifference,
        double referenceEpochYear,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr
    createTimeDependentCoordinateFrameRotation(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, double translationXMetre,
        double translationYMetre, double translationZMetre,
        double rotationXArcSecond, double rotationYArcSecond,
        double rotationZArcSecond, double scaleDifferencePPM,
        double rateTranslationX, double rateTranslationY,
        double rateTranslationZ, double rateRotationX, double rateRotationY,
        double rateRotationZ, double rateScaleDifference,
        double referenceEpochYear,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createTOWGS84(
        const crs::CRSNNPtr &sourceCRSIn,
        const std::vector<double> &TOWGS84Parameters); // throw InvalidOperation

    PROJ_DLL static TransformationNNPtr createNTv2(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const std::string &filename,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createMolodensky(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, double translationXMetre,
        double translationYMetre, double translationZMetre,
        double semiMajorAxisDifferenceMetre, double flattingDifference,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createAbridgedMolodensky(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, double translationXMetre,
        double translationYMetre, double translationZMetre,
        double semiMajorAxisDifferenceMetre, double flattingDifference,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr
    createGravityRelatedHeightToGeographic3D(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const crs::CRSPtr &interpolationCRSIn,
        const std::string &filename,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createVERTCON(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const std::string &filename,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createLongitudeRotation(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const common::Angle &offset);

    PROJ_DLL static TransformationNNPtr createGeographic2DOffsets(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const common::Angle &offsetLat,
        const common::Angle &offsetLong,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createGeographic3DOffsets(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const common::Angle &offsetLat,
        const common::Angle &offsetLong, const common::Length &offsetHeight,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createGeographic2DWithHeightOffsets(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const common::Angle &offsetLat,
        const common::Angle &offsetLong, const common::Length &offsetHeight,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createCartesianGridOffsets(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const common::Length &eastingOffset,
        const common::Length &northingOffset,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createVerticalOffset(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const common::Length &offsetHeight,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_DLL static TransformationNNPtr createChangeVerticalUnit(
        const util::PropertyMap &properties, const crs::CRSNNPtr &sourceCRSIn,
        const crs::CRSNNPtr &targetCRSIn, const common::Scale &factor,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL const std::string &
        getPROJ4NadgridsCompatibleFilename() const;

    PROJ_FOR_TEST std::vector<double> getTOWGS84Parameters(
        bool canThrowException) const; // throw(io::FormattingException)

    PROJ_INTERNAL const std::string &getHeightToGeographic3DFilename() const;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL TransformationNNPtr shallowClone() const;

    PROJ_INTERNAL TransformationNNPtr
    promoteTo3D(const std::string &newName,
                const io::DatabaseContextPtr &dbContext) const;

    PROJ_INTERNAL TransformationNNPtr
    demoteTo2D(const std::string &newName,
               const io::DatabaseContextPtr &dbContext) const;

    PROJ_INTERNAL static bool
    isGeographic3DToGravityRelatedHeight(const OperationMethodNNPtr &method,
                                         bool allowInverse);
    //! @endcond

  protected:
    PROJ_INTERNAL Transformation(
        const crs::CRSNNPtr &sourceCRSIn, const crs::CRSNNPtr &targetCRSIn,
        const crs::CRSPtr &interpolationCRSIn,
        const OperationMethodNNPtr &methodIn,
        const std::vector<GeneralParameterValueNNPtr> &values,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);
    PROJ_INTERNAL Transformation(const Transformation &other);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_FRIEND(CoordinateOperation);
    PROJ_FRIEND(CoordinateOperationFactory);
    PROJ_FRIEND(SingleOperation);
    PROJ_INTERNAL TransformationNNPtr inverseAsTransformation() const;

    PROJ_INTERNAL CoordinateOperationNNPtr _shallowClone() const override;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class PointMotionOperation;
/** Shared pointer of PointMotionOperation */
using PointMotionOperationPtr = std::shared_ptr<PointMotionOperation>;
/** Non-null shared pointer of PointMotionOperation */
using PointMotionOperationNNPtr = util::nn<PointMotionOperationPtr>;

/** \brief A mathematical operation that describes the change of coordinate
 * values within one coordinate reference system due to the motion of the
 * point between one coordinate epoch and another coordinate epoch.
 *
 * The motion is due to tectonic plate movement or deformation.
 *
 * \remark Implements PointMotionOperation from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL PointMotionOperation : public SingleOperation {
  public:
    // TODO
    //! @cond Doxygen_Suppress
    PROJ_DLL ~PointMotionOperation() override;
    //! @endcond

    PROJ_DLL const crs::CRSNNPtr &sourceCRS() PROJ_PURE_DECL;

    PROJ_DLL CoordinateOperationNNPtr inverse() const override;

    PROJ_DLL static PointMotionOperationNNPtr
    create(const util::PropertyMap &properties, const crs::CRSNNPtr &crsIn,
           const OperationMethodNNPtr &methodIn,
           const std::vector<GeneralParameterValueNNPtr> &values,
           const std::vector<metadata::PositionalAccuracyNNPtr>
               &accuracies); // throw InvalidOperation

    PROJ_DLL static PointMotionOperationNNPtr
    create(const util::PropertyMap &propertiesOperation,
           const crs::CRSNNPtr &crsIn,
           const util::PropertyMap &propertiesOperationMethod,
           const std::vector<OperationParameterNNPtr> &parameters,
           const std::vector<ParameterValueNNPtr> &values,
           const std::vector<metadata::PositionalAccuracyNNPtr>
               &accuracies); // throw InvalidOperation

    PROJ_DLL PointMotionOperationNNPtr substitutePROJAlternativeGridNames(
        io::DatabaseContextNNPtr databaseContext) const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL PointMotionOperationNNPtr
        shallowClone() const;

    PROJ_INTERNAL PointMotionOperationNNPtr
    cloneWithEpochs(const common::DataEpoch &sourceEpoch,
                    const common::DataEpoch &targetEpoch) const;

    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    //! @endcond

  protected:
    PROJ_INTERNAL PointMotionOperation(
        const crs::CRSNNPtr &crsIn, const OperationMethodNNPtr &methodIn,
        const std::vector<GeneralParameterValueNNPtr> &values,
        const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);
    PROJ_INTERNAL PointMotionOperation(const PointMotionOperation &other);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL CoordinateOperationNNPtr _shallowClone() const override;

  private:
    PointMotionOperation &operator=(const PointMotionOperation &) = delete;
};

// ---------------------------------------------------------------------------

class ConcatenatedOperation;
/** Shared pointer of ConcatenatedOperation */
using ConcatenatedOperationPtr = std::shared_ptr<ConcatenatedOperation>;
/** Non-null shared pointer of ConcatenatedOperation */
using ConcatenatedOperationNNPtr = util::nn<ConcatenatedOperationPtr>;

/** \brief An ordered sequence of two or more single coordinate operations
 * (SingleOperation).
 *
 * The sequence of coordinate operations is constrained by the requirement
 * that
 * the source coordinate reference system of step n+1 shall be the same as
 * the target coordinate reference system of step n.
 *
 * \remark Implements ConcatenatedOperation from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL ConcatenatedOperation final : public CoordinateOperation {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~ConcatenatedOperation() override;
    //! @endcond

    PROJ_DLL const std::vector<CoordinateOperationNNPtr> &operations() const;

    PROJ_DLL CoordinateOperationNNPtr inverse() const override;

    PROJ_DLL static ConcatenatedOperationNNPtr
    create(const util::PropertyMap &properties,
           const std::vector<CoordinateOperationNNPtr> &operationsIn,
           const std::vector<metadata::PositionalAccuracyNNPtr>
               &accuracies); // throw InvalidOperation

    PROJ_DLL static CoordinateOperationNNPtr createComputeMetadata(
        const std::vector<CoordinateOperationNNPtr> &operationsIn,
        bool checkExtent); // throw InvalidOperation

    PROJ_DLL std::set<GridDescription>
    gridsNeeded(const io::DatabaseContextPtr &databaseContext,
                bool considerKnownGridsAsAvailable) const override;

    PROJ_PRIVATE :

        //! @cond Doxygen_Suppress
        PROJ_INTERNAL void
        _exportToWKT(io::WKTFormatter *formatter)
            const override; // throw(io::FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL static void
    fixSteps(const crs::CRSNNPtr &concatOpSourceCRS,
             const crs::CRSNNPtr &concatOpTargetCRS,
             std::vector<CoordinateOperationNNPtr> &operationsInOut,
             const io::DatabaseContextPtr &dbContext, bool fixDirectionAllowed);
    //! @endcond

  protected:
    PROJ_INTERNAL ConcatenatedOperation(const ConcatenatedOperation &other);
    PROJ_INTERNAL explicit ConcatenatedOperation(
        const std::vector<CoordinateOperationNNPtr> &operationsIn);

    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL CoordinateOperationNNPtr _shallowClone() const override;

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    ConcatenatedOperation &
    operator=(const ConcatenatedOperation &other) = delete;

    PROJ_INTERNAL
    static void setCRSsUpdateInverse(CoordinateOperation *co,
                                     const crs::CRSNNPtr &sourceCRS,
                                     const crs::CRSNNPtr &targetCRS);
};

// ---------------------------------------------------------------------------

class CoordinateOperationContext;
/** Unique pointer of CoordinateOperationContext */
using CoordinateOperationContextPtr =
    std::unique_ptr<CoordinateOperationContext>;
/** Non-null unique pointer of CoordinateOperationContext */
using CoordinateOperationContextNNPtr = util::nn<CoordinateOperationContextPtr>;

/** \brief Context in which a coordinate operation is to be used.
 *
 * \remark Implements [CoordinateOperationFactory
 * https://sis.apache.org/apidocs/org/apache/sis/referencing/operation/CoordinateOperationContext.html]
 * from
 * Apache SIS
 */

class PROJ_GCC_DLL CoordinateOperationContext {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL virtual ~CoordinateOperationContext();
    //! @endcond

    PROJ_DLL const io::AuthorityFactoryPtr &getAuthorityFactory() const;

    PROJ_DLL const metadata::ExtentPtr &getAreaOfInterest() const;

    PROJ_DLL void setAreaOfInterest(const metadata::ExtentPtr &extent);

    PROJ_DLL double getDesiredAccuracy() const;

    PROJ_DLL void setDesiredAccuracy(double accuracy);

    PROJ_DLL void setAllowBallparkTransformations(bool allow);

    PROJ_DLL bool getAllowBallparkTransformations() const;

    /** Specify how source and target CRS extent should be used to restrict
     * candidate operations (only taken into account if no explicit area of
     * interest is specified. */
    enum class SourceTargetCRSExtentUse {
        /** Ignore CRS extent */
        NONE,
        /** Test coordinate operation extent against both CRS extent. */
        BOTH,
        /** Test coordinate operation extent against the intersection of both
           CRS extent. */
        INTERSECTION,
        /** Test coordinate operation against the smallest of both CRS extent.
         */
        SMALLEST,
    };

    PROJ_DLL void setSourceAndTargetCRSExtentUse(SourceTargetCRSExtentUse use);

    PROJ_DLL SourceTargetCRSExtentUse getSourceAndTargetCRSExtentUse() const;

    /** Spatial criterion to restrict candidate operations. */
    enum class SpatialCriterion {
        /** The area of validity of transforms should strictly contain the
         * are of interest. */
        STRICT_CONTAINMENT,

        /** The area of validity of transforms should at least intersect the
         * area of interest. */
        PARTIAL_INTERSECTION
    };

    PROJ_DLL void setSpatialCriterion(SpatialCriterion criterion);

    PROJ_DLL SpatialCriterion getSpatialCriterion() const;

    PROJ_DLL void setUsePROJAlternativeGridNames(bool usePROJNames);

    PROJ_DLL bool getUsePROJAlternativeGridNames() const;

    PROJ_DLL void setDiscardSuperseded(bool discard);

    PROJ_DLL bool getDiscardSuperseded() const;

    /** Describe how grid availability is used. */
    enum class GridAvailabilityUse {
        /** Grid availability is only used for sorting results. Operations
         * where some grids are missing will be sorted last. */
        USE_FOR_SORTING,

        /** Completely discard an operation if a required grid is missing. */
        DISCARD_OPERATION_IF_MISSING_GRID,

        /** Ignore grid availability at all. Results will be presented as if
         * all grids were available. */
        IGNORE_GRID_AVAILABILITY,

        /** Results will be presented as if grids known to PROJ (that is
         * registered in the grid_alternatives table of its database) were
         * available. Used typically when networking is enabled.
         */
        KNOWN_AVAILABLE,
    };

    PROJ_DLL void setGridAvailabilityUse(GridAvailabilityUse use);

    PROJ_DLL GridAvailabilityUse getGridAvailabilityUse() const;

    /** Describe if and how intermediate CRS should be used */
    enum class IntermediateCRSUse {
        /** Always search for intermediate CRS. */
        ALWAYS,

        /** Only attempt looking for intermediate CRS if there is no direct
         * transformation available. */
        IF_NO_DIRECT_TRANSFORMATION,

        /* Do not attempt looking for intermediate CRS. */
        NEVER,
    };

    PROJ_DLL void setAllowUseIntermediateCRS(IntermediateCRSUse use);

    PROJ_DLL IntermediateCRSUse getAllowUseIntermediateCRS() const;

    PROJ_DLL void
    setIntermediateCRS(const std::vector<std::pair<std::string, std::string>>
                           &intermediateCRSAuthCodes);

    PROJ_DLL const std::vector<std::pair<std::string, std::string>> &
    getIntermediateCRS() const;

    PROJ_DLL void
    setSourceCoordinateEpoch(const util::optional<common::DataEpoch> &epoch);

    PROJ_DLL const util::optional<common::DataEpoch> &
    getSourceCoordinateEpoch() const;

    PROJ_DLL void
    setTargetCoordinateEpoch(const util::optional<common::DataEpoch> &epoch);

    PROJ_DLL const util::optional<common::DataEpoch> &
    getTargetCoordinateEpoch() const;

    PROJ_DLL static CoordinateOperationContextNNPtr
    create(const io::AuthorityFactoryPtr &authorityFactory,
           const metadata::ExtentPtr &extent, double accuracy);

    PROJ_DLL CoordinateOperationContextNNPtr clone() const;

  protected:
    PROJ_INTERNAL CoordinateOperationContext();
    PROJ_INTERNAL
    CoordinateOperationContext(const CoordinateOperationContext &);
    INLINED_MAKE_UNIQUE

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class CoordinateOperationFactory;
/** Unique pointer of CoordinateOperationFactory */
using CoordinateOperationFactoryPtr =
    std::unique_ptr<CoordinateOperationFactory>;
/** Non-null unique pointer of CoordinateOperationFactory */
using CoordinateOperationFactoryNNPtr = util::nn<CoordinateOperationFactoryPtr>;

/** \brief Creates coordinate operations. This factory is capable to find
 * coordinate transformations or conversions between two coordinate
 * reference
 * systems.
 *
 * \remark Implements (partially) CoordinateOperationFactory from \ref
 * GeoAPI
 */
class PROJ_GCC_DLL CoordinateOperationFactory {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL virtual ~CoordinateOperationFactory();
    //! @endcond

    PROJ_DLL CoordinateOperationPtr createOperation(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS) const;

    PROJ_DLL std::vector<CoordinateOperationNNPtr>
    createOperations(const crs::CRSNNPtr &sourceCRS,
                     const crs::CRSNNPtr &targetCRS,
                     const CoordinateOperationContextNNPtr &context) const;

    PROJ_DLL std::vector<CoordinateOperationNNPtr> createOperations(
        const coordinates::CoordinateMetadataNNPtr &sourceCoordinateMetadata,
        const crs::CRSNNPtr &targetCRS,
        const CoordinateOperationContextNNPtr &context) const;

    PROJ_DLL std::vector<CoordinateOperationNNPtr> createOperations(
        const crs::CRSNNPtr &sourceCRS,
        const coordinates::CoordinateMetadataNNPtr &targetCoordinateMetadata,
        const CoordinateOperationContextNNPtr &context) const;

    PROJ_DLL std::vector<CoordinateOperationNNPtr> createOperations(
        const coordinates::CoordinateMetadataNNPtr &sourceCoordinateMetadata,
        const coordinates::CoordinateMetadataNNPtr &targetCoordinateMetadata,
        const CoordinateOperationContextNNPtr &context) const;

    PROJ_DLL static CoordinateOperationFactoryNNPtr create();

  protected:
    PROJ_INTERNAL CoordinateOperationFactory();
    INLINED_MAKE_UNIQUE

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

} // namespace operation

NS_PROJ_END

#endif //  COORDINATEOPERATION_HH_INCLUDED
