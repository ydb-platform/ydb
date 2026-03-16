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
#error This file should only be included from a PROJ cpp file
#endif

#ifndef COORDINATEOPERATION_INTERNAL_HH_INCLUDED
#define COORDINATEOPERATION_INTERNAL_HH_INCLUDED

#include "proj/coordinateoperation.hpp"

#include <vector>

//! @cond Doxygen_Suppress

NS_PROJ_START

namespace operation {

// ---------------------------------------------------------------------------

bool isAxisOrderReversal(int methodEPSGCode);

// ---------------------------------------------------------------------------

class InverseCoordinateOperation;
/** Shared pointer of InverseCoordinateOperation */
using InverseCoordinateOperationPtr =
    std::shared_ptr<InverseCoordinateOperation>;
/** Non-null shared pointer of InverseCoordinateOperation */
using InverseCoordinateOperationNNPtr = util::nn<InverseCoordinateOperationPtr>;

/** \brief Inverse operation of a CoordinateOperation.
 *
 * This is used when there is no straightforward way of building another
 * subclass of CoordinateOperation that models the inverse operation.
 */
class InverseCoordinateOperation : virtual public CoordinateOperation {
  public:
    InverseCoordinateOperation(
        const CoordinateOperationNNPtr &forwardOperationIn,
        bool wktSupportsInversion);

    ~InverseCoordinateOperation() override;

    void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    CoordinateOperationNNPtr inverse() const override;

    const CoordinateOperationNNPtr &forwardOperation() const {
        return forwardOperation_;
    }

  protected:
    CoordinateOperationNNPtr forwardOperation_;
    bool wktSupportsInversion_;

    void setPropertiesFromForward();
};

// ---------------------------------------------------------------------------

/** \brief Inverse of a conversion. */
class InverseConversion : public Conversion, public InverseCoordinateOperation {
  public:
    explicit InverseConversion(const ConversionNNPtr &forward);

    ~InverseConversion() override;

    void _exportToWKT(io::WKTFormatter *formatter) const override {
        Conversion::_exportToWKT(formatter);
    }

    void _exportToJSON(io::JSONFormatter *formatter) const override {
        Conversion::_exportToJSON(formatter);
    }

    void
    _exportToPROJString(io::PROJStringFormatter *formatter) const override {
        InverseCoordinateOperation::_exportToPROJString(formatter);
    }

    bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override {
        return InverseCoordinateOperation::_isEquivalentTo(other, criterion,
                                                           dbContext);
    }

    CoordinateOperationNNPtr inverse() const override {
        return InverseCoordinateOperation::inverse();
    }

    ConversionNNPtr inverseAsConversion() const;

#ifdef _MSC_VER
    // To avoid a warning C4250: 'osgeo::proj::operation::InverseConversion':
    // inherits
    // 'osgeo::proj::operation::SingleOperation::osgeo::proj::operation::SingleOperation::gridsNeeded'
    // via dominance
    std::set<GridDescription>
    gridsNeeded(const io::DatabaseContextPtr &databaseContext,
                bool considerKnownGridsAsAvailable) const override {
        return SingleOperation::gridsNeeded(databaseContext,
                                            considerKnownGridsAsAvailable);
    }
#endif

    static CoordinateOperationNNPtr create(const ConversionNNPtr &forward);

    CoordinateOperationNNPtr _shallowClone() const override;
};

// ---------------------------------------------------------------------------

/** \brief Inverse of a transformation. */
class InverseTransformation : public Transformation,
                              public InverseCoordinateOperation {
  public:
    explicit InverseTransformation(const TransformationNNPtr &forward);

    ~InverseTransformation() override;

    void _exportToWKT(io::WKTFormatter *formatter) const override;

    void
    _exportToPROJString(io::PROJStringFormatter *formatter) const override {
        return InverseCoordinateOperation::_exportToPROJString(formatter);
    }

    void _exportToJSON(io::JSONFormatter *formatter) const override {
        Transformation::_exportToJSON(formatter);
    }

    bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override {
        return InverseCoordinateOperation::_isEquivalentTo(other, criterion,
                                                           dbContext);
    }

    CoordinateOperationNNPtr inverse() const override {
        return InverseCoordinateOperation::inverse();
    }

    TransformationNNPtr inverseAsTransformation() const;

#ifdef _MSC_VER
    // To avoid a warning C4250:
    // 'osgeo::proj::operation::InverseTransformation': inherits
    // 'osgeo::proj::operation::SingleOperation::osgeo::proj::operation::SingleOperation::gridsNeeded'
    // via dominance
    std::set<GridDescription>
    gridsNeeded(const io::DatabaseContextPtr &databaseContext,
                bool considerKnownGridsAsAvailable) const override {
        return SingleOperation::gridsNeeded(databaseContext,
                                            considerKnownGridsAsAvailable);
    }
#endif

    static TransformationNNPtr create(const TransformationNNPtr &forward);

    CoordinateOperationNNPtr _shallowClone() const override;
};

// ---------------------------------------------------------------------------

class PROJBasedOperation;
/** Shared pointer of PROJBasedOperation */
using PROJBasedOperationPtr = std::shared_ptr<PROJBasedOperation>;
/** Non-null shared pointer of PROJBasedOperation */
using PROJBasedOperationNNPtr = util::nn<PROJBasedOperationPtr>;

/** \brief A PROJ-string based coordinate operation.
 */
class PROJBasedOperation : public SingleOperation {
  public:
    ~PROJBasedOperation() override;

    void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    CoordinateOperationNNPtr inverse() const override;

    static PROJBasedOperationNNPtr
    create(const util::PropertyMap &properties, const std::string &PROJString,
           const crs::CRSPtr &sourceCRS, const crs::CRSPtr &targetCRS,
           const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies);

    static PROJBasedOperationNNPtr
    create(const util::PropertyMap &properties,
           const io::IPROJStringExportableNNPtr &projExportable, bool inverse,
           const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
           const crs::CRSPtr &interpolationCRS,
           const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies,
           bool hasRoughTransformation);

    std::set<GridDescription>
    gridsNeeded(const io::DatabaseContextPtr &databaseContext,
                bool considerKnownGridsAsAvailable) const override;

  protected:
    PROJBasedOperation(const PROJBasedOperation &) = default;
    explicit PROJBasedOperation(const OperationMethodNNPtr &methodIn);

    void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    CoordinateOperationNNPtr _shallowClone() const override;

    INLINED_MAKE_SHARED

  private:
    std::string projString_{};
    io::IPROJStringExportablePtr projStringExportable_{};
    bool inverse_ = false;
};

// ---------------------------------------------------------------------------

class InvalidOperationEmptyIntersection : public InvalidOperation {
  public:
    explicit InvalidOperationEmptyIntersection(const std::string &message);
    InvalidOperationEmptyIntersection(
        const InvalidOperationEmptyIntersection &other);
    ~InvalidOperationEmptyIntersection() override;
};
} // namespace operation

NS_PROJ_END

//! @endcond

#endif // COORDINATEOPERATION_INTERNAL_HH_INCLUDED
