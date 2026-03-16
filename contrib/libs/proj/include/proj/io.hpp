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

#ifndef IO_HH_INCLUDED
#define IO_HH_INCLUDED

#include <list>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "proj.h"

#include "util.hpp"

NS_PROJ_START

class CPLJSonStreamingWriter;

namespace common {
class UnitOfMeasure;
using UnitOfMeasurePtr = std::shared_ptr<UnitOfMeasure>;
using UnitOfMeasureNNPtr = util::nn<UnitOfMeasurePtr>;

class IdentifiedObject;
using IdentifiedObjectPtr = std::shared_ptr<IdentifiedObject>;
using IdentifiedObjectNNPtr = util::nn<IdentifiedObjectPtr>;
} // namespace common

namespace cs {
class CoordinateSystem;
using CoordinateSystemPtr = std::shared_ptr<CoordinateSystem>;
using CoordinateSystemNNPtr = util::nn<CoordinateSystemPtr>;
} // namespace cs

namespace metadata {
class Extent;
using ExtentPtr = std::shared_ptr<Extent>;
using ExtentNNPtr = util::nn<ExtentPtr>;
} // namespace metadata

namespace datum {
class Datum;
using DatumPtr = std::shared_ptr<Datum>;
using DatumNNPtr = util::nn<DatumPtr>;

class DatumEnsemble;
using DatumEnsemblePtr = std::shared_ptr<DatumEnsemble>;
using DatumEnsembleNNPtr = util::nn<DatumEnsemblePtr>;

class Ellipsoid;
using EllipsoidPtr = std::shared_ptr<Ellipsoid>;
using EllipsoidNNPtr = util::nn<EllipsoidPtr>;

class PrimeMeridian;
using PrimeMeridianPtr = std::shared_ptr<PrimeMeridian>;
using PrimeMeridianNNPtr = util::nn<PrimeMeridianPtr>;

class GeodeticReferenceFrame;
using GeodeticReferenceFramePtr = std::shared_ptr<GeodeticReferenceFrame>;
using GeodeticReferenceFrameNNPtr = util::nn<GeodeticReferenceFramePtr>;

class VerticalReferenceFrame;
using VerticalReferenceFramePtr = std::shared_ptr<VerticalReferenceFrame>;
using VerticalReferenceFrameNNPtr = util::nn<VerticalReferenceFramePtr>;

class EngineeringDatum;
using EngineeringDatumPtr = std::shared_ptr<EngineeringDatum>;
using EngineeringDatumNNPtr = util::nn<EngineeringDatumPtr>;
} // namespace datum

namespace crs {
class CRS;
using CRSPtr = std::shared_ptr<CRS>;
using CRSNNPtr = util::nn<CRSPtr>;

class GeodeticCRS;
using GeodeticCRSPtr = std::shared_ptr<GeodeticCRS>;
using GeodeticCRSNNPtr = util::nn<GeodeticCRSPtr>;

class GeographicCRS;
using GeographicCRSPtr = std::shared_ptr<GeographicCRS>;
using GeographicCRSNNPtr = util::nn<GeographicCRSPtr>;

class VerticalCRS;
using VerticalCRSPtr = std::shared_ptr<VerticalCRS>;
using VerticalCRSNNPtr = util::nn<VerticalCRSPtr>;

class ProjectedCRS;
using ProjectedCRSPtr = std::shared_ptr<ProjectedCRS>;
using ProjectedCRSNNPtr = util::nn<ProjectedCRSPtr>;

class CompoundCRS;
using CompoundCRSPtr = std::shared_ptr<CompoundCRS>;
using CompoundCRSNNPtr = util::nn<CompoundCRSPtr>;

class EngineeringCRS;
using EngineeringCRSPtr = std::shared_ptr<EngineeringCRS>;
using EngineeringCRSNNPtr = util::nn<EngineeringCRSPtr>;
} // namespace crs

namespace coordinates {
class CoordinateMetadata;
/** Shared pointer of CoordinateMetadata */
using CoordinateMetadataPtr = std::shared_ptr<CoordinateMetadata>;
/** Non-null shared pointer of CoordinateMetadata */
using CoordinateMetadataNNPtr = util::nn<CoordinateMetadataPtr>;
} // namespace coordinates

namespace operation {
class Conversion;
using ConversionPtr = std::shared_ptr<Conversion>;
using ConversionNNPtr = util::nn<ConversionPtr>;

class CoordinateOperation;
using CoordinateOperationPtr = std::shared_ptr<CoordinateOperation>;
using CoordinateOperationNNPtr = util::nn<CoordinateOperationPtr>;

class PointMotionOperation;
using PointMotionOperationPtr = std::shared_ptr<PointMotionOperation>;
using PointMotionOperationNNPtr = util::nn<PointMotionOperationPtr>;
} // namespace operation

/** osgeo.proj.io namespace.
 *
 * \brief I/O classes
 */
namespace io {

class DatabaseContext;
/** Shared pointer of DatabaseContext. */
using DatabaseContextPtr = std::shared_ptr<DatabaseContext>;
/** Non-null shared pointer of DatabaseContext. */
using DatabaseContextNNPtr = util::nn<DatabaseContextPtr>;

// ---------------------------------------------------------------------------

class WKTNode;
/** Unique pointer of WKTNode. */
using WKTNodePtr = std::unique_ptr<WKTNode>;
/** Non-null unique pointer of WKTNode. */
using WKTNodeNNPtr = util::nn<WKTNodePtr>;

// ---------------------------------------------------------------------------

class WKTFormatter;
/** WKTFormatter unique pointer. */
using WKTFormatterPtr = std::unique_ptr<WKTFormatter>;
/** Non-null WKTFormatter unique pointer. */
using WKTFormatterNNPtr = util::nn<WKTFormatterPtr>;

/** \brief Formatter to WKT strings.
 *
 * An instance of this class can only be used by a single
 * thread at a time.
 */
class PROJ_GCC_DLL WKTFormatter {
  public:
    /** WKT variant. */
    enum class PROJ_MSVC_DLL Convention {
        /** Full WKT2 string, conforming to ISO 19162:2015(E) / OGC 12-063r5
         * (\ref WKT2_2015) with all possible nodes and new keyword names.
         */
        WKT2,
        WKT2_2015 = WKT2,

        /** Same as WKT2 with the following exceptions:
         * <ul>
         *      <li>UNIT keyword used.</li>
         *      <li>ID node only on top element.</li>
         *      <li>No ORDER element in AXIS element.</li>
         *      <li>PRIMEM node omitted if it is Greenwich.</li>
         *      <li>ELLIPSOID.UNIT node omitted if it is
         * UnitOfMeasure::METRE.</li>
         *      <li>PARAMETER.UNIT / PRIMEM.UNIT omitted if same as AXIS.</li>
         *      <li>AXIS.UNIT omitted and replaced by a common GEODCRS.UNIT if
         * they are all the same on all axis.</li>
         * </ul>
         */
        WKT2_SIMPLIFIED,
        WKT2_2015_SIMPLIFIED = WKT2_SIMPLIFIED,

        /** Full WKT2 string, conforming to ISO 19162:2019 / OGC 18-010, with
         * (\ref WKT2_2019) all possible nodes and new keyword names.
         * Non-normative list of differences:
         * <ul>
         *      <li>WKT2_2019 uses GEOGCRS / BASEGEOGCRS keywords for
         * GeographicCRS.</li>
         * </ul>
         */
        WKT2_2019,

        /** Deprecated alias for WKT2_2019 */
        WKT2_2018 = WKT2_2019,

        /** WKT2_2019 with the simplification rule of WKT2_SIMPLIFIED */
        WKT2_2019_SIMPLIFIED,

        /** Deprecated alias for WKT2_2019_SIMPLIFIED */
        WKT2_2018_SIMPLIFIED = WKT2_2019_SIMPLIFIED,

        /** WKT1 as traditionally output by GDAL, deriving from OGC 01-009.
            A notable departure from WKT1_GDAL with respect to OGC 01-009 is
            that in WKT1_GDAL, the unit of the PRIMEM value is always degrees.
           */
        WKT1_GDAL,

        /** WKT1 as traditionally output by ESRI software,
         * deriving from OGC 99-049. */
        WKT1_ESRI,
    };

    PROJ_DLL static WKTFormatterNNPtr
    create(Convention convention = Convention::WKT2,
           DatabaseContextPtr dbContext = nullptr);
    PROJ_DLL static WKTFormatterNNPtr create(const WKTFormatterNNPtr &other);
    //! @cond Doxygen_Suppress
    PROJ_DLL ~WKTFormatter();
    //! @endcond

    PROJ_DLL WKTFormatter &setMultiLine(bool multiLine) noexcept;
    PROJ_DLL WKTFormatter &setIndentationWidth(int width) noexcept;

    /** Rule for output AXIS nodes */
    enum class OutputAxisRule {
        /** Always include AXIS nodes */
        YES,
        /** Never include AXIS nodes */
        NO,
        /** Includes them only on PROJCS node if it uses Easting/Northing
         *ordering. Typically used for WKT1_GDAL */
        WKT1_GDAL_EPSG_STYLE,
    };

    PROJ_DLL WKTFormatter &setOutputAxis(OutputAxisRule outputAxis) noexcept;
    PROJ_DLL WKTFormatter &setStrict(bool strict) noexcept;
    PROJ_DLL bool isStrict() const noexcept;

    PROJ_DLL WKTFormatter &
    setAllowEllipsoidalHeightAsVerticalCRS(bool allow) noexcept;
    PROJ_DLL bool isAllowedEllipsoidalHeightAsVerticalCRS() const noexcept;

    PROJ_DLL WKTFormatter &setAllowLINUNITNode(bool allow) noexcept;
    PROJ_DLL bool isAllowedLINUNITNode() const noexcept;

    PROJ_DLL const std::string &toString() const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_DLL WKTFormatter &
        setOutputId(bool outputIdIn);

    PROJ_INTERNAL void enter();
    PROJ_INTERNAL void leave();

    PROJ_INTERNAL void startNode(const std::string &keyword, bool hasId);
    PROJ_INTERNAL void endNode();

    PROJ_INTERNAL bool isAtTopLevel() const;

    PROJ_DLL WKTFormatter &simulCurNodeHasId();

    PROJ_INTERNAL void addQuotedString(const char *str);
    PROJ_INTERNAL void addQuotedString(const std::string &str);
    PROJ_INTERNAL void add(const std::string &str);
    PROJ_INTERNAL void add(int number);
    PROJ_INTERNAL void add(size_t number) = delete;
    PROJ_INTERNAL void add(double number, int precision = 15);

    PROJ_INTERNAL void pushOutputUnit(bool outputUnitIn);
    PROJ_INTERNAL void popOutputUnit();
    PROJ_INTERNAL bool outputUnit() const;

    PROJ_INTERNAL void pushOutputId(bool outputIdIn);
    PROJ_INTERNAL void popOutputId();
    PROJ_INTERNAL bool outputId() const;

    PROJ_INTERNAL void pushHasId(bool hasId);
    PROJ_INTERNAL void popHasId();

    PROJ_INTERNAL void pushDisableUsage();
    PROJ_INTERNAL void popDisableUsage();
    PROJ_INTERNAL bool outputUsage() const;

    PROJ_INTERNAL void
    pushAxisLinearUnit(const common::UnitOfMeasureNNPtr &unit);
    PROJ_INTERNAL void popAxisLinearUnit();
    PROJ_INTERNAL const common::UnitOfMeasureNNPtr &axisLinearUnit() const;

    PROJ_INTERNAL void
    pushAxisAngularUnit(const common::UnitOfMeasureNNPtr &unit);
    PROJ_INTERNAL void popAxisAngularUnit();
    PROJ_INTERNAL const common::UnitOfMeasureNNPtr &axisAngularUnit() const;

    PROJ_INTERNAL void setAbridgedTransformation(bool abriged);
    PROJ_INTERNAL bool abridgedTransformation() const;

    PROJ_INTERNAL void setUseDerivingConversion(bool useDerivingConversionIn);
    PROJ_INTERNAL bool useDerivingConversion() const;

    PROJ_INTERNAL void setTOWGS84Parameters(const std::vector<double> &params);
    PROJ_INTERNAL const std::vector<double> &getTOWGS84Parameters() const;

    PROJ_INTERNAL void setVDatumExtension(const std::string &filename);
    PROJ_INTERNAL const std::string &getVDatumExtension() const;

    PROJ_INTERNAL void setHDatumExtension(const std::string &filename);
    PROJ_INTERNAL const std::string &getHDatumExtension() const;

    PROJ_INTERNAL void
    setGeogCRSOfCompoundCRS(const crs::GeographicCRSPtr &crs);
    PROJ_INTERNAL const crs::GeographicCRSPtr &getGeogCRSOfCompoundCRS() const;

    PROJ_INTERNAL static std::string morphNameToESRI(const std::string &name);

#ifdef unused
    PROJ_INTERNAL void startInversion();
    PROJ_INTERNAL void stopInversion();
    PROJ_INTERNAL bool isInverted() const;
#endif

    PROJ_INTERNAL OutputAxisRule outputAxis() const;
    PROJ_INTERNAL bool outputAxisOrder() const;
    PROJ_INTERNAL bool primeMeridianOmittedIfGreenwich() const;
    PROJ_INTERNAL bool ellipsoidUnitOmittedIfMetre() const;
    PROJ_INTERNAL bool forceUNITKeyword() const;
    PROJ_INTERNAL bool primeMeridianOrParameterUnitOmittedIfSameAsAxis() const;
    PROJ_INTERNAL bool primeMeridianInDegree() const;
    PROJ_INTERNAL bool outputCSUnitOnlyOnceIfSame() const;
    PROJ_INTERNAL bool idOnTopLevelOnly() const;
    PROJ_INTERNAL bool topLevelHasId() const;

    /** WKT version. */
    enum class Version {
        /** WKT1 */
        WKT1,
        /** WKT2 / ISO 19162 */
        WKT2
    };

    PROJ_INTERNAL Version version() const;
    PROJ_INTERNAL bool use2019Keywords() const;
    PROJ_INTERNAL bool useESRIDialect() const;

    PROJ_INTERNAL const DatabaseContextPtr &databaseContext() const;

    PROJ_INTERNAL void ingestWKTNode(const WKTNodeNNPtr &node);

    //! @endcond

  protected:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit WKTFormatter(Convention convention);
    WKTFormatter(const WKTFormatter &other) = delete;

    INLINED_MAKE_UNIQUE
    //! @endcond

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class PROJStringFormatter;
/** PROJStringFormatter unique pointer. */
using PROJStringFormatterPtr = std::unique_ptr<PROJStringFormatter>;
/** Non-null PROJStringFormatter unique pointer. */
using PROJStringFormatterNNPtr = util::nn<PROJStringFormatterPtr>;

/** \brief Formatter to PROJ strings.
 *
 * An instance of this class can only be used by a single
 * thread at a time.
 */
class PROJ_GCC_DLL PROJStringFormatter {
  public:
    /** PROJ variant. */
    enum class PROJ_MSVC_DLL Convention {
        /** PROJ v5 (or later versions) string. */
        PROJ_5,

        /** PROJ v4 string as output by GDAL exportToProj4() */
        PROJ_4
    };

    PROJ_DLL static PROJStringFormatterNNPtr
    create(Convention conventionIn = Convention::PROJ_5,
           DatabaseContextPtr dbContext = nullptr);
    //! @cond Doxygen_Suppress
    PROJ_DLL ~PROJStringFormatter();
    //! @endcond

    PROJ_DLL PROJStringFormatter &setMultiLine(bool multiLine) noexcept;
    PROJ_DLL PROJStringFormatter &setIndentationWidth(int width) noexcept;
    PROJ_DLL PROJStringFormatter &setMaxLineLength(int maxLineLength) noexcept;

    PROJ_DLL void setUseApproxTMerc(bool flag);

    PROJ_DLL const std::string &toString() const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress

        PROJ_DLL void
        setCRSExport(bool b);
    PROJ_INTERNAL bool getCRSExport() const;
    PROJ_DLL void startInversion();
    PROJ_DLL void stopInversion();
    PROJ_INTERNAL bool isInverted() const;
    PROJ_INTERNAL bool getUseApproxTMerc() const;
    PROJ_INTERNAL void setCoordinateOperationOptimizations(bool enable);

    PROJ_DLL void
    ingestPROJString(const std::string &str); // throw ParsingException

    PROJ_DLL void addStep(const char *step);
    PROJ_DLL void addStep(const std::string &step);
    PROJ_DLL void setCurrentStepInverted(bool inverted);
    PROJ_DLL void addParam(const std::string &paramName);
    PROJ_DLL void addParam(const char *paramName, double val);
    PROJ_DLL void addParam(const std::string &paramName, double val);
    PROJ_DLL void addParam(const char *paramName, int val);
    PROJ_DLL void addParam(const std::string &paramName, int val);
    PROJ_DLL void addParam(const char *paramName, const char *val);
    PROJ_DLL void addParam(const char *paramName, const std::string &val);
    PROJ_DLL void addParam(const std::string &paramName, const char *val);
    PROJ_DLL void addParam(const std::string &paramName,
                           const std::string &val);
    PROJ_DLL void addParam(const char *paramName,
                           const std::vector<double> &vals);

    PROJ_INTERNAL bool hasParam(const char *paramName) const;

    PROJ_INTERNAL void addNoDefs(bool b);
    PROJ_INTERNAL bool getAddNoDefs() const;

    PROJ_INTERNAL std::set<std::string> getUsedGridNames() const;

    PROJ_INTERNAL bool requiresPerCoordinateInputTime() const;

    PROJ_INTERNAL void setTOWGS84Parameters(const std::vector<double> &params);
    PROJ_INTERNAL const std::vector<double> &getTOWGS84Parameters() const;

    PROJ_INTERNAL void setVDatumExtension(const std::string &filename,
                                          const std::string &geoidCRSValue);
    PROJ_INTERNAL const std::string &getVDatumExtension() const;
    PROJ_INTERNAL const std::string &getGeoidCRSValue() const;

    PROJ_INTERNAL void setHDatumExtension(const std::string &filename);
    PROJ_INTERNAL const std::string &getHDatumExtension() const;

    PROJ_INTERNAL void
    setGeogCRSOfCompoundCRS(const crs::GeographicCRSPtr &crs);
    PROJ_INTERNAL const crs::GeographicCRSPtr &getGeogCRSOfCompoundCRS() const;

    PROJ_INTERNAL void setOmitProjLongLatIfPossible(bool omit);
    PROJ_INTERNAL bool omitProjLongLatIfPossible() const;

    PROJ_INTERNAL void pushOmitZUnitConversion();
    PROJ_INTERNAL void popOmitZUnitConversion();
    PROJ_INTERNAL bool omitZUnitConversion() const;

    PROJ_INTERNAL void pushOmitHorizontalConversionInVertTransformation();
    PROJ_INTERNAL void popOmitHorizontalConversionInVertTransformation();
    PROJ_INTERNAL bool omitHorizontalConversionInVertTransformation() const;

    PROJ_INTERNAL void setLegacyCRSToCRSContext(bool legacyContext);
    PROJ_INTERNAL bool getLegacyCRSToCRSContext() const;

    PROJ_INTERNAL PROJStringFormatter &setNormalizeOutput();

    PROJ_INTERNAL const DatabaseContextPtr &databaseContext() const;

    PROJ_INTERNAL Convention convention() const;

    PROJ_INTERNAL size_t getStepCount() const;

    //! @endcond

  protected:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit PROJStringFormatter(
        Convention conventionIn, const DatabaseContextPtr &dbContext);
    PROJStringFormatter(const PROJStringFormatter &other) = delete;

    INLINED_MAKE_UNIQUE
    //! @endcond

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class JSONFormatter;
/** JSONFormatter unique pointer. */
using JSONFormatterPtr = std::unique_ptr<JSONFormatter>;
/** Non-null JSONFormatter unique pointer. */
using JSONFormatterNNPtr = util::nn<JSONFormatterPtr>;

/** \brief Formatter to JSON strings.
 *
 * An instance of this class can only be used by a single
 * thread at a time.
 */
class PROJ_GCC_DLL JSONFormatter {
  public:
    PROJ_DLL static JSONFormatterNNPtr
    create(DatabaseContextPtr dbContext = nullptr);
    //! @cond Doxygen_Suppress
    PROJ_DLL ~JSONFormatter();
    //! @endcond

    PROJ_DLL JSONFormatter &setMultiLine(bool multiLine) noexcept;
    PROJ_DLL JSONFormatter &setIndentationWidth(int width) noexcept;
    PROJ_DLL JSONFormatter &setSchema(const std::string &schema) noexcept;

    PROJ_DLL const std::string &toString() const;

    PROJ_PRIVATE :

        //! @cond Doxygen_Suppress
        PROJ_INTERNAL CPLJSonStreamingWriter *
        writer() const;

    PROJ_INTERNAL const DatabaseContextPtr &databaseContext() const;

    struct ObjectContext {
        JSONFormatter &m_formatter;

        ObjectContext(const ObjectContext &) = delete;
        ObjectContext(ObjectContext &&) = default;

        explicit ObjectContext(JSONFormatter &formatter, const char *objectType,
                               bool hasId);
        ~ObjectContext();
    };
    PROJ_INTERNAL inline ObjectContext MakeObjectContext(const char *objectType,
                                                         bool hasId) {
        return ObjectContext(*this, objectType, hasId);
    }

    PROJ_INTERNAL void setAllowIDInImmediateChild();

    PROJ_INTERNAL void setOmitTypeInImmediateChild();

    PROJ_INTERNAL void setAbridgedTransformation(bool abriged);
    PROJ_INTERNAL bool abridgedTransformation() const;

    PROJ_INTERNAL void setAbridgedTransformationWriteSourceCRS(bool writeCRS);
    PROJ_INTERNAL bool abridgedTransformationWriteSourceCRS() const;

    // cppcheck-suppress functionStatic
    PROJ_INTERNAL bool outputId() const;

    PROJ_INTERNAL bool
    outputUsage(bool calledBeforeObjectContext = false) const;

    PROJ_INTERNAL static const char *PROJJSON_v0_7;

    //! @endcond

  protected:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit JSONFormatter();
    JSONFormatter(const JSONFormatter &other) = delete;

    INLINED_MAKE_UNIQUE
    //! @endcond

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

/** \brief Interface for an object that can be exported to JSON. */
class PROJ_GCC_DLL IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL virtual ~IJSONExportable();
    //! @endcond

    /** Builds a JSON representation. May throw a FormattingException */
    PROJ_DLL std::string
    exportToJSON(JSONFormatter *formatter) const; // throw(FormattingException)

    PROJ_PRIVATE :

        //! @cond Doxygen_Suppress
        PROJ_INTERNAL virtual void
        _exportToJSON(
            JSONFormatter *formatter) const = 0; // throw(FormattingException)
                                                 //! @endcond
};

// ---------------------------------------------------------------------------

/** \brief Exception possibly thrown by IWKTExportable::exportToWKT() or
 * IPROJStringExportable::exportToPROJString(). */
class PROJ_GCC_DLL FormattingException : public util::Exception {
  public:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit FormattingException(const char *message);
    PROJ_INTERNAL explicit FormattingException(const std::string &message);
    PROJ_DLL FormattingException(const FormattingException &other);
    PROJ_DLL virtual ~FormattingException() override;

    PROJ_INTERNAL static void Throw(const char *msg) PROJ_NO_RETURN;
    PROJ_INTERNAL static void Throw(const std::string &msg) PROJ_NO_RETURN;
    //! @endcond
};

// ---------------------------------------------------------------------------

/** \brief Exception possibly thrown by WKTNode::createFrom() or
 * WKTParser::createFromWKT(). */
class PROJ_GCC_DLL ParsingException : public util::Exception {
  public:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit ParsingException(const char *message);
    PROJ_INTERNAL explicit ParsingException(const std::string &message);
    PROJ_DLL ParsingException(const ParsingException &other);
    PROJ_DLL virtual ~ParsingException() override;
    //! @endcond
};

// ---------------------------------------------------------------------------

/** \brief Interface for an object that can be exported to WKT. */
class PROJ_GCC_DLL IWKTExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL virtual ~IWKTExportable();
    //! @endcond

    /** Builds a WKT representation. May throw a FormattingException */
    PROJ_DLL std::string
    exportToWKT(WKTFormatter *formatter) const; // throw(FormattingException)

    PROJ_PRIVATE :

        //! @cond Doxygen_Suppress
        PROJ_INTERNAL virtual void
        _exportToWKT(
            WKTFormatter *formatter) const = 0; // throw(FormattingException)
                                                //! @endcond
};

// ---------------------------------------------------------------------------

class IPROJStringExportable;
/** Shared pointer of IPROJStringExportable. */
using IPROJStringExportablePtr = std::shared_ptr<IPROJStringExportable>;
/** Non-null shared pointer of IPROJStringExportable. */
using IPROJStringExportableNNPtr = util::nn<IPROJStringExportablePtr>;

/** \brief Interface for an object that can be exported to a PROJ string. */
class PROJ_GCC_DLL IPROJStringExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL virtual ~IPROJStringExportable();
    //! @endcond

    /** \brief Builds a PROJ string representation.
     *
     * <ul>
     * <li>For PROJStringFormatter::Convention::PROJ_5 (the default),
     * <ul>
     * <li>For a crs::CRS, returns the same as
     * PROJStringFormatter::Convention::PROJ_4. It should be noted that the
     * export of a CRS as a PROJ string may cause loss of many important aspects
     * of a CRS definition. Consequently it is discouraged to use it for
     * interoperability in newer projects. The choice of a WKT representation
     * will be a better option.</li>
     * <li>For operation::CoordinateOperation, returns a PROJ
     * pipeline.</li>
     * </ul>
     *
     * <li>For PROJStringFormatter::Convention::PROJ_4, format a string
     * compatible with the OGRSpatialReference::exportToProj4() of GDAL
     * &lt;=2.3. It is only compatible of a few CRS objects. The PROJ string
     * will also contain a +type=crs parameter to disambiguate the nature of
     * the string from a CoordinateOperation.
     * <ul>
     * <li>For a crs::GeographicCRS, returns a proj=longlat string, with
     * ellipsoid / datum / prime meridian information, ignoring axis order
     * and unit information.</li>
     * <li>For a geocentric crs::GeodeticCRS, returns the transformation from
     * geographic coordinates into geocentric coordinates.</li>
     * <li>For a crs::ProjectedCRS, returns the projection method, ignoring
     * axis order.</li>
     * <li>For a crs::BoundCRS, returns the PROJ string of its source/base CRS,
     * amended with towgs84 / nadgrids parameter when the deriving conversion
     * can be expressed in that way.</li>
     * </ul>
     * </li>
     *
     * </ul>
     *
     * @param formatter PROJ string formatter.
     * @return a PROJ string.
     * @throw FormattingException if cannot be exported as a PROJ string */
    PROJ_DLL std::string exportToPROJString(
        PROJStringFormatter *formatter) const; // throw(FormattingException)

    PROJ_PRIVATE :

        //! @cond Doxygen_Suppress
        PROJ_INTERNAL virtual void
        _exportToPROJString(PROJStringFormatter *formatter)
            const = 0; // throw(FormattingException)
                       //! @endcond
};

// ---------------------------------------------------------------------------

/** \brief Node in the tree-splitted WKT representation.
 */
class PROJ_GCC_DLL WKTNode {
  public:
    PROJ_DLL explicit WKTNode(const std::string &valueIn);
    //! @cond Doxygen_Suppress
    PROJ_DLL ~WKTNode();
    //! @endcond

    PROJ_DLL const std::string &value() const;
    PROJ_DLL const std::vector<WKTNodeNNPtr> &children() const;

    PROJ_DLL void addChild(WKTNodeNNPtr &&child);
    PROJ_DLL const WKTNodePtr &lookForChild(const std::string &childName,
                                            int occurrence = 0) const noexcept;
    PROJ_DLL int
    countChildrenOfName(const std::string &childName) const noexcept;

    PROJ_DLL std::string toString() const;

    PROJ_DLL static WKTNodeNNPtr createFrom(const std::string &wkt,
                                            size_t indexStart = 0);

  protected:
    PROJ_INTERNAL static WKTNodeNNPtr
    createFrom(const std::string &wkt, size_t indexStart, int recLevel,
               size_t &indexEnd); // throw(ParsingException)

  private:
    friend class WKTParser;
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

PROJ_DLL util::BaseObjectNNPtr
createFromUserInput(const std::string &text,
                    const DatabaseContextPtr &dbContext,
                    bool usePROJ4InitRules = false);

PROJ_DLL util::BaseObjectNNPtr createFromUserInput(const std::string &text,
                                                   PJ_CONTEXT *ctx);

// ---------------------------------------------------------------------------

/** \brief Parse a WKT string into the appropriate subclass of util::BaseObject.
 */
class PROJ_GCC_DLL WKTParser {
  public:
    PROJ_DLL WKTParser();
    //! @cond Doxygen_Suppress
    PROJ_DLL ~WKTParser();
    //! @endcond

    PROJ_DLL WKTParser &
    attachDatabaseContext(const DatabaseContextPtr &dbContext);

    PROJ_DLL WKTParser &setStrict(bool strict);
    PROJ_DLL std::list<std::string> warningList() const;
    PROJ_DLL std::list<std::string> grammarErrorList() const;

    PROJ_DLL WKTParser &setUnsetIdentifiersIfIncompatibleDef(bool unset);

    PROJ_DLL util::BaseObjectNNPtr
    createFromWKT(const std::string &wkt); // throw(ParsingException)

    /** Guessed WKT "dialect" */
    enum class PROJ_MSVC_DLL WKTGuessedDialect {
        /** \ref WKT2_2019 */
        WKT2_2019,
        /** Deprecated alias for WKT2_2019 */
        WKT2_2018 = WKT2_2019,
        /** \ref WKT2_2015 */
        WKT2_2015,
        /** \ref WKT1 */
        WKT1_GDAL,
        /** ESRI variant of WKT1 */
        WKT1_ESRI,
        /** Not WKT / unrecognized */
        NOT_WKT
    };

    // cppcheck-suppress functionStatic
    PROJ_DLL WKTGuessedDialect guessDialect(const std::string &wkt) noexcept;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

/** \brief Parse a PROJ string into the appropriate subclass of
 * util::BaseObject.
 */
class PROJ_GCC_DLL PROJStringParser {
  public:
    PROJ_DLL PROJStringParser();
    //! @cond Doxygen_Suppress
    PROJ_DLL ~PROJStringParser();
    //! @endcond

    PROJ_DLL PROJStringParser &
    attachDatabaseContext(const DatabaseContextPtr &dbContext);

    PROJ_DLL PROJStringParser &setUsePROJ4InitRules(bool enable);

    PROJ_DLL std::vector<std::string> warningList() const;

    PROJ_DLL util::BaseObjectNNPtr createFromPROJString(
        const std::string &projString); // throw(ParsingException)

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJStringParser &
        attachContext(PJ_CONTEXT *ctx);
    //! @endcond
  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

/** \brief Database context.
 *
 * A database context should be used only by one thread at a time.
 */
class PROJ_GCC_DLL DatabaseContext {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DatabaseContext();
    //! @endcond

    PROJ_DLL static DatabaseContextNNPtr
    create(const std::string &databasePath = std::string(),
           const std::vector<std::string> &auxiliaryDatabasePaths =
               std::vector<std::string>(),
           PJ_CONTEXT *ctx = nullptr);

    PROJ_DLL const std::string &getPath() const;

    PROJ_DLL const char *getMetadata(const char *key) const;

    PROJ_DLL std::set<std::string> getAuthorities() const;

    PROJ_DLL std::vector<std::string> getDatabaseStructure() const;

    PROJ_DLL void startInsertStatementsSession();

    PROJ_DLL std::string
    suggestsCodeFor(const common::IdentifiedObjectNNPtr &object,
                    const std::string &authName, bool numericCode);

    PROJ_DLL std::vector<std::string> getInsertStatementsFor(
        const common::IdentifiedObjectNNPtr &object,
        const std::string &authName, const std::string &code, bool numericCode,
        const std::vector<std::string> &allowedAuthorities = {"EPSG", "PROJ"});

    PROJ_DLL void stopInsertStatementsSession();

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_DLL void *
        getSqliteHandle() const;

    PROJ_DLL static DatabaseContextNNPtr create(void *sqlite_handle);

    PROJ_INTERNAL bool lookForGridAlternative(const std::string &officialName,
                                              std::string &projFilename,
                                              std::string &projFormat,
                                              bool &inverse) const;

    PROJ_DLL bool lookForGridInfo(const std::string &projFilename,
                                  bool considerKnownGridsAsAvailable,
                                  std::string &fullFilename,
                                  std::string &packageName, std::string &url,
                                  bool &directDownload, bool &openLicense,
                                  bool &gridAvailable) const;

    PROJ_DLL unsigned int getQueryCounter() const;

    PROJ_INTERNAL std::string
    getProjGridName(const std::string &oldProjGridName);

    PROJ_INTERNAL void invalidateGridInfo(const std::string &projFilename);

    PROJ_INTERNAL std::string getOldProjGridName(const std::string &gridName);

    PROJ_INTERNAL std::string
    getAliasFromOfficialName(const std::string &officialName,
                             const std::string &tableName,
                             const std::string &source) const;

    PROJ_INTERNAL std::list<std::string>
    getAliases(const std::string &authName, const std::string &code,
               const std::string &officialName, const std::string &tableName,
               const std::string &source) const;

    PROJ_INTERNAL bool isKnownName(const std::string &name,
                                   const std::string &tableName) const;

    PROJ_INTERNAL std::string getName(const std::string &tableName,
                                      const std::string &authName,
                                      const std::string &code) const;

    PROJ_INTERNAL std::string getTextDefinition(const std::string &tableName,
                                                const std::string &authName,
                                                const std::string &code) const;

    PROJ_INTERNAL std::vector<std::string>
    getAllowedAuthorities(const std::string &sourceAuthName,
                          const std::string &targetAuthName) const;

    PROJ_INTERNAL std::list<std::pair<std::string, std::string>>
    getNonDeprecated(const std::string &tableName, const std::string &authName,
                     const std::string &code) const;

    PROJ_INTERNAL static std::vector<operation::CoordinateOperationNNPtr>
    getTransformationsForGridName(const DatabaseContextNNPtr &databaseContext,
                                  const std::string &gridName);

    PROJ_INTERNAL bool
    getAuthorityAndVersion(const std::string &versionedAuthName,
                           std::string &authNameOut, std::string &versionOut);

    PROJ_INTERNAL bool getVersionedAuthority(const std::string &authName,
                                             const std::string &version,
                                             std::string &versionedAuthNameOut);

    PROJ_DLL std::vector<std::string>
    getVersionedAuthoritiesFromName(const std::string &authName);

    PROJ_FOR_TEST bool
    toWGS84AutocorrectWrongValues(double &tx, double &ty, double &tz,
                                  double &rx, double &ry, double &rz,
                                  double &scale_difference) const;

    //! @endcond

  protected:
    PROJ_INTERNAL DatabaseContext();
    INLINED_MAKE_SHARED
    PROJ_FRIEND(AuthorityFactory);

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    DatabaseContext(const DatabaseContext &) = delete;
    DatabaseContext &operator=(const DatabaseContext &other) = delete;
};

// ---------------------------------------------------------------------------

class AuthorityFactory;
/** Shared pointer of AuthorityFactory. */
using AuthorityFactoryPtr = std::shared_ptr<AuthorityFactory>;
/** Non-null shared pointer of AuthorityFactory. */
using AuthorityFactoryNNPtr = util::nn<AuthorityFactoryPtr>;

/** \brief Builds object from an authority database.
 *
 * A AuthorityFactory should be used only by one thread at a time.
 *
 * \remark Implements [AuthorityFactory]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/referencing/AuthorityFactory.html)
 * from \ref GeoAPI
 */
class PROJ_GCC_DLL AuthorityFactory {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~AuthorityFactory();
    //! @endcond

    PROJ_DLL util::BaseObjectNNPtr createObject(const std::string &code) const;

    PROJ_DLL common::UnitOfMeasureNNPtr
    createUnitOfMeasure(const std::string &code) const;

    PROJ_DLL metadata::ExtentNNPtr createExtent(const std::string &code) const;

    PROJ_DLL datum::PrimeMeridianNNPtr
    createPrimeMeridian(const std::string &code) const;

    PROJ_DLL std::string identifyBodyFromSemiMajorAxis(double a,
                                                       double tolerance) const;

    PROJ_DLL datum::EllipsoidNNPtr
    createEllipsoid(const std::string &code) const;

    PROJ_DLL datum::DatumNNPtr createDatum(const std::string &code) const;

    PROJ_DLL datum::DatumEnsembleNNPtr
    createDatumEnsemble(const std::string &code,
                        const std::string &type = std::string()) const;

    PROJ_DLL datum::GeodeticReferenceFrameNNPtr
    createGeodeticDatum(const std::string &code) const;

    PROJ_DLL datum::VerticalReferenceFrameNNPtr
    createVerticalDatum(const std::string &code) const;

    PROJ_DLL datum::EngineeringDatumNNPtr
    createEngineeringDatum(const std::string &code) const;

    PROJ_DLL cs::CoordinateSystemNNPtr
    createCoordinateSystem(const std::string &code) const;

    PROJ_DLL crs::GeodeticCRSNNPtr
    createGeodeticCRS(const std::string &code) const;

    PROJ_DLL crs::GeographicCRSNNPtr
    createGeographicCRS(const std::string &code) const;

    PROJ_DLL crs::VerticalCRSNNPtr
    createVerticalCRS(const std::string &code) const;

    PROJ_DLL crs::EngineeringCRSNNPtr
    createEngineeringCRS(const std::string &code) const;

    PROJ_DLL operation::ConversionNNPtr
    createConversion(const std::string &code) const;

    PROJ_DLL crs::ProjectedCRSNNPtr
    createProjectedCRS(const std::string &code) const;

    PROJ_DLL crs::CompoundCRSNNPtr
    createCompoundCRS(const std::string &code) const;

    PROJ_DLL crs::CRSNNPtr
    createCoordinateReferenceSystem(const std::string &code) const;

    PROJ_DLL coordinates::CoordinateMetadataNNPtr
    createCoordinateMetadata(const std::string &code) const;

    PROJ_DLL operation::CoordinateOperationNNPtr
    createCoordinateOperation(const std::string &code,
                              bool usePROJAlternativeGridNames) const;

    PROJ_DLL std::vector<operation::CoordinateOperationNNPtr>
    createFromCoordinateReferenceSystemCodes(
        const std::string &sourceCRSCode,
        const std::string &targetCRSCode) const;

    PROJ_DLL std::list<std::string>
    getGeoidModels(const std::string &code) const;

    PROJ_DLL const std::string &getAuthority() PROJ_PURE_DECL;

    /** Object type. */
    enum class ObjectType {
        /** Object of type datum::PrimeMeridian */
        PRIME_MERIDIAN,
        /** Object of type datum::Ellipsoid */
        ELLIPSOID,
        /** Object of type datum::Datum (and derived classes) */
        DATUM,
        /** Object of type datum::GeodeticReferenceFrame (and derived
           classes) */
        GEODETIC_REFERENCE_FRAME,
        /** Object of type datum::VerticalReferenceFrame (and derived
           classes) */
        VERTICAL_REFERENCE_FRAME,
        /** Object of type datum::EngineeringDatum */
        ENGINEERING_DATUM,
        /** Object of type crs::CRS (and derived classes) */
        CRS,
        /** Object of type crs::GeodeticCRS (and derived classes) */
        GEODETIC_CRS,
        /** GEODETIC_CRS of type geocentric */
        GEOCENTRIC_CRS,
        /** Object of type crs::GeographicCRS (and derived classes) */
        GEOGRAPHIC_CRS,
        /** GEOGRAPHIC_CRS of type Geographic 2D */
        GEOGRAPHIC_2D_CRS,
        /** GEOGRAPHIC_CRS of type Geographic 3D */
        GEOGRAPHIC_3D_CRS,
        /** Object of type crs::ProjectedCRS (and derived classes) */
        PROJECTED_CRS,
        /** Object of type crs::VerticalCRS (and derived classes) */
        VERTICAL_CRS,
        /** Object of type crs::CompoundCRS (and derived classes) */
        ENGINEERING_CRS,
        /** Object of type crs::EngineeringCRS */
        COMPOUND_CRS,
        /** Object of type operation::CoordinateOperation (and derived
           classes) */
        COORDINATE_OPERATION,
        /** Object of type operation::Conversion (and derived classes) */
        CONVERSION,
        /** Object of type operation::Transformation (and derived classes)
         */
        TRANSFORMATION,
        /** Object of type operation::ConcatenatedOperation (and derived
           classes) */
        CONCATENATED_OPERATION,
        /** Object of type datum::DynamicGeodeticReferenceFrame */
        DYNAMIC_GEODETIC_REFERENCE_FRAME,
        /** Object of type datum::DynamicVerticalReferenceFrame */
        DYNAMIC_VERTICAL_REFERENCE_FRAME,
        /** Object of type datum::DatumEnsemble */
        DATUM_ENSEMBLE,
    };

    PROJ_DLL std::set<std::string>
    getAuthorityCodes(const ObjectType &type,
                      bool allowDeprecated = true) const;

    PROJ_DLL std::string getDescriptionText(const std::string &code) const;

    // non-standard

    /** CRS information */
    struct CRSInfo {
        /** Authority name */
        std::string authName;
        /** Code */
        std::string code;
        /** Name */
        std::string name;
        /** Type */
        ObjectType type;
        /** Whether the object is deprecated */
        bool deprecated;
        /** Whereas the west_lon_degree, south_lat_degree, east_lon_degree and
         * north_lat_degree fields are valid. */
        bool bbox_valid;
        /** Western-most longitude of the area of use, in degrees. */
        double west_lon_degree;
        /** Southern-most latitude of the area of use, in degrees. */
        double south_lat_degree;
        /** Eastern-most longitude of the area of use, in degrees. */
        double east_lon_degree;
        /** Northern-most latitude of the area of use, in degrees. */
        double north_lat_degree;
        /** Name of the area of use. */
        std::string areaName;
        /** Name of the projection method for a projected CRS. Might be empty
         * even for projected CRS in some cases. */
        std::string projectionMethodName;
        /** Name of the celestial body of the CRS (e.g. "Earth") */
        std::string celestialBodyName;

        //! @cond Doxygen_Suppress
        CRSInfo();
        //! @endcond
    };

    PROJ_DLL std::list<CRSInfo> getCRSInfoList() const;

    /** Unit information */
    struct UnitInfo {
        /** Authority name */
        std::string authName;
        /** Code */
        std::string code;
        /** Name */
        std::string name;
        /** Category: one of "linear", "linear_per_time", "angular",
         * "angular_per_time", "scale", "scale_per_time" or "time" */
        std::string category;
        /** Conversion factor to the SI unit.
         * It might be 0 in some cases to indicate no known conversion factor.
         */
        double convFactor;
        /** PROJ short name (may be empty) */
        std::string projShortName;
        /** Whether the object is deprecated */
        bool deprecated;

        //! @cond Doxygen_Suppress
        UnitInfo();
        //! @endcond
    };

    PROJ_DLL std::list<UnitInfo> getUnitList() const;

    /** Celestial Body information */
    struct CelestialBodyInfo {
        /** Authority name */
        std::string authName;
        /** Name */
        std::string name;
        //! @cond Doxygen_Suppress
        CelestialBodyInfo();
        //! @endcond
    };

    PROJ_DLL std::list<CelestialBodyInfo> getCelestialBodyList() const;

    PROJ_DLL static AuthorityFactoryNNPtr
    create(const DatabaseContextNNPtr &context,
           const std::string &authorityName);

    PROJ_DLL const DatabaseContextNNPtr &databaseContext() const;

    PROJ_DLL std::vector<operation::CoordinateOperationNNPtr>
    createFromCoordinateReferenceSystemCodes(
        const std::string &sourceCRSAuthName, const std::string &sourceCRSCode,
        const std::string &targetCRSAuthName, const std::string &targetCRSCode,
        bool usePROJAlternativeGridNames, bool discardIfMissingGrid,
        bool considerKnownGridsAsAvailable, bool discardSuperseded,
        bool tryReverseOrder = false,
        bool reportOnlyIntersectingTransformations = false,
        const metadata::ExtentPtr &intersectingExtent1 = nullptr,
        const metadata::ExtentPtr &intersectingExtent2 = nullptr) const;

    PROJ_DLL std::vector<operation::CoordinateOperationNNPtr>
    createFromCRSCodesWithIntermediates(
        const std::string &sourceCRSAuthName, const std::string &sourceCRSCode,
        const std::string &targetCRSAuthName, const std::string &targetCRSCode,
        bool usePROJAlternativeGridNames, bool discardIfMissingGrid,
        bool considerKnownGridsAsAvailable, bool discardSuperseded,
        const std::vector<std::pair<std::string, std::string>>
            &intermediateCRSAuthCodes,
        ObjectType allowedIntermediateObjectType = ObjectType::CRS,
        const std::vector<std::string> &allowedAuthorities =
            std::vector<std::string>(),
        const metadata::ExtentPtr &intersectingExtent1 = nullptr,
        const metadata::ExtentPtr &intersectingExtent2 = nullptr) const;

    PROJ_DLL std::string getOfficialNameFromAlias(
        const std::string &aliasedName, const std::string &tableName,
        const std::string &source, bool tryEquivalentNameSpelling,
        std::string &outTableName, std::string &outAuthName,
        std::string &outCode) const;

    PROJ_DLL std::list<common::IdentifiedObjectNNPtr>
    createObjectsFromName(const std::string &name,
                          const std::vector<ObjectType> &allowedObjectTypes =
                              std::vector<ObjectType>(),
                          bool approximateMatch = true,
                          size_t limitResultCount = 0) const;

    PROJ_DLL std::list<std::pair<std::string, std::string>>
    listAreaOfUseFromName(const std::string &name, bool approximateMatch) const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress

        PROJ_INTERNAL std::list<datum::EllipsoidNNPtr>
        createEllipsoidFromExisting(
            const datum::EllipsoidNNPtr &ellipsoid) const;

    PROJ_INTERNAL std::list<crs::GeodeticCRSNNPtr>
    createGeodeticCRSFromDatum(const std::string &datum_auth_name,
                               const std::string &datum_code,
                               const std::string &geodetic_crs_type) const;

    PROJ_INTERNAL std::list<crs::GeodeticCRSNNPtr>
    createGeodeticCRSFromDatum(const datum::GeodeticReferenceFrameNNPtr &datum,
                               const std::string &preferredAuthName,
                               const std::string &geodetic_crs_type) const;

    PROJ_INTERNAL std::list<crs::VerticalCRSNNPtr>
    createVerticalCRSFromDatum(const std::string &datum_auth_name,
                               const std::string &datum_code) const;

    PROJ_INTERNAL std::list<crs::GeodeticCRSNNPtr>
    createGeodeticCRSFromEllipsoid(const std::string &ellipsoid_auth_name,
                                   const std::string &ellipsoid_code,
                                   const std::string &geodetic_crs_type) const;

    PROJ_INTERNAL std::list<crs::ProjectedCRSNNPtr>
    createProjectedCRSFromExisting(const crs::ProjectedCRSNNPtr &crs) const;

    PROJ_INTERNAL std::list<crs::CompoundCRSNNPtr>
    createCompoundCRSFromExisting(const crs::CompoundCRSNNPtr &crs) const;

    PROJ_INTERNAL crs::CRSNNPtr
    createCoordinateReferenceSystem(const std::string &code,
                                    bool allowCompound) const;

    PROJ_INTERNAL std::vector<operation::CoordinateOperationNNPtr>
    getTransformationsForGeoid(const std::string &geoidName,
                               bool usePROJAlternativeGridNames) const;

    PROJ_INTERNAL std::vector<operation::CoordinateOperationNNPtr>
    createBetweenGeodeticCRSWithDatumBasedIntermediates(
        const crs::CRSNNPtr &sourceCRS, const std::string &sourceCRSAuthName,
        const std::string &sourceCRSCode, const crs::CRSNNPtr &targetCRS,
        const std::string &targetCRSAuthName, const std::string &targetCRSCode,
        bool usePROJAlternativeGridNames, bool discardIfMissingGrid,
        bool considerKnownGridsAsAvailable, bool discardSuperseded,
        const std::vector<std::string> &allowedAuthorities,
        const metadata::ExtentPtr &intersectingExtent1,
        const metadata::ExtentPtr &intersectingExtent2) const;

    typedef std::pair<common::IdentifiedObjectNNPtr, std::string>
        PairObjectName;
    PROJ_INTERNAL std::list<PairObjectName>
    createObjectsFromNameEx(const std::string &name,
                            const std::vector<ObjectType> &allowedObjectTypes =
                                std::vector<ObjectType>(),
                            bool approximateMatch = true,
                            size_t limitResultCount = 0) const;

    PROJ_FOR_TEST std::vector<operation::PointMotionOperationNNPtr>
    getPointMotionOperationsFor(const crs::GeodeticCRSNNPtr &crs,
                                bool usePROJAlternativeGridNames) const;

    //! @endcond

  protected:
    PROJ_INTERNAL AuthorityFactory(const DatabaseContextNNPtr &context,
                                   const std::string &authorityName);

    PROJ_INTERNAL crs::GeodeticCRSNNPtr
    createGeodeticCRS(const std::string &code, bool geographicOnly) const;

    PROJ_INTERNAL operation::CoordinateOperationNNPtr
    createCoordinateOperation(const std::string &code, bool allowConcatenated,
                              bool usePROJAlternativeGridNames,
                              const std::string &type) const;

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA

    PROJ_INTERNAL void
    createGeodeticDatumOrEnsemble(const std::string &code,
                                  datum::GeodeticReferenceFramePtr &outDatum,
                                  datum::DatumEnsemblePtr &outDatumEnsemble,
                                  bool turnEnsembleAsDatum) const;

    PROJ_INTERNAL void
    createVerticalDatumOrEnsemble(const std::string &code,
                                  datum::VerticalReferenceFramePtr &outDatum,
                                  datum::DatumEnsemblePtr &outDatumEnsemble,
                                  bool turnEnsembleAsDatum) const;
};

// ---------------------------------------------------------------------------

/** \brief Exception thrown when a factory can't create an instance of the
 * requested object.
 */
class PROJ_GCC_DLL FactoryException : public util::Exception {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL explicit FactoryException(const char *message);
    PROJ_DLL explicit FactoryException(const std::string &message);
    PROJ_DLL
    FactoryException(const FactoryException &other);
    PROJ_DLL ~FactoryException() override;
    //! @endcond
};

// ---------------------------------------------------------------------------

/** \brief Exception thrown when an authority factory can't find the requested
 * authority code.
 */
class PROJ_GCC_DLL NoSuchAuthorityCodeException : public FactoryException {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL explicit NoSuchAuthorityCodeException(const std::string &message,
                                                   const std::string &authority,
                                                   const std::string &code);
    PROJ_DLL
    NoSuchAuthorityCodeException(const NoSuchAuthorityCodeException &other);
    PROJ_DLL ~NoSuchAuthorityCodeException() override;
    //! @endcond

    PROJ_DLL const std::string &getAuthority() const;
    PROJ_DLL const std::string &getAuthorityCode() const;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

} // namespace io

NS_PROJ_END

#endif // IO_HH_INCLUDED
