/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  ISO19111:2019 implementation
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2023, Even Rouault <even dot rouault at spatialys dot com>
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

#ifndef COORDINATES_HH_INCLUDED
#define COORDINATES_HH_INCLUDED

#include <memory>

#include "common.hpp"
#include "crs.hpp"
#include "io.hpp"
#include "util.hpp"

NS_PROJ_START

/** osgeo.proj.coordinates namespace

    \brief Coordinates package
*/
namespace coordinates {

class CoordinateMetadata;
/** Shared pointer of CoordinateMetadata */
using CoordinateMetadataPtr = std::shared_ptr<CoordinateMetadata>;
/** Non-null shared pointer of CoordinateMetadata */
using CoordinateMetadataNNPtr = util::nn<CoordinateMetadataPtr>;

// ---------------------------------------------------------------------------

/** \brief Associates a CRS with a coordinate epoch.
 *
 * \remark Implements CoordinateMetadata from \ref ISO_19111_2019
 * \since 9.2
 */

class PROJ_GCC_DLL CoordinateMetadata : public util::BaseObject,
                                        public io::IWKTExportable,
                                        public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~CoordinateMetadata() override;
    //! @endcond

    PROJ_DLL const crs::CRSNNPtr &crs() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<common::DataEpoch> &
    coordinateEpoch() PROJ_PURE_DECL;
    PROJ_DLL double coordinateEpochAsDecimalYear() PROJ_PURE_DECL;

    PROJ_DLL static CoordinateMetadataNNPtr create(const crs::CRSNNPtr &crsIn);
    PROJ_DLL static CoordinateMetadataNNPtr
    create(const crs::CRSNNPtr &crsIn, double coordinateEpochAsDecimalYear);
    PROJ_DLL static CoordinateMetadataNNPtr
    create(const crs::CRSNNPtr &crsIn, double coordinateEpochAsDecimalYear,
           const io::DatabaseContextPtr &dbContext);

    PROJ_DLL CoordinateMetadataNNPtr
    promoteTo3D(const std::string &newName,
                const io::DatabaseContextPtr &dbContext) const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress

        PROJ_INTERNAL void
        _exportToWKT(io::WKTFormatter *formatter)
            const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    //! @endcond

  protected:
    PROJ_INTERNAL explicit CoordinateMetadata(const crs::CRSNNPtr &crsIn);
    PROJ_INTERNAL CoordinateMetadata(const crs::CRSNNPtr &crsIn,
                                     double coordinateEpochAsDecimalYear);

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    CoordinateMetadata &operator=(const CoordinateMetadata &other) = delete;
};

// ---------------------------------------------------------------------------

} // namespace coordinates

NS_PROJ_END

#endif //  COORDINATES_HH_INCLUDED
