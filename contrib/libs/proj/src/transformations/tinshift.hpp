/******************************************************************************
 * Project:  PROJ
 * Purpose:  Functionality related to TIN based transformations
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2020, Even Rouault, <even.rouault at spatialys.com>
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

#ifndef TINSHIFT_HPP
#define TINSHIFT_HPP

#ifdef PROJ_COMPILATION
#include "proj/internal/include_nlohmann_json.hpp"
#else
#include "nlohmann/json.hpp"
#endif

#include <algorithm>
#include <cmath>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "quadtree.hpp"

#ifndef TINSHIFT_NAMESPACE
#define TINSHIFT_NAMESPACE TINShift
#endif

#include "tinshift_exceptions.hpp"

namespace TINSHIFT_NAMESPACE {

enum FallbackStrategy {
    FALLBACK_NONE,
    FALLBACK_NEAREST_SIDE,
    FALLBACK_NEAREST_CENTROID,
};

using json = nlohmann::json;

// ---------------------------------------------------------------------------

/** Content of a TINShift file. */
class TINShiftFile {
  public:
    /** Parse the provided serialized JSON content and return an object.
     *
     * @throws ParsingException in case of error.
     */
    static std::unique_ptr<TINShiftFile> parse(const std::string &text);

    /** Get file type. Should always be "triangulation_file" */
    const std::string &fileType() const { return mFileType; }

    /** Get the version of the format. At time of writing, only "1.0" is known
     */
    const std::string &formatVersion() const { return mFormatVersion; }

    /** Get brief descriptive name of the deformation model. */
    const std::string &name() const { return mName; }

    /** Get a string identifying the version of the deformation model.
     * The format for specifying version is defined by the agency
     * responsible for the deformation model. */
    const std::string &version() const { return mVersion; }

    /** Get a string identifying the license of the file.
     * e.g "Create Commons Attribution 4.0 International" */
    const std::string &license() const { return mLicense; }

    /** Get a text description of the model. Intended to be longer than name()
     */
    const std::string &description() const { return mDescription; }

    /** Get a text description of the model. Intended to be longer than name()
     */
    const std::string &publicationDate() const { return mPublicationDate; }

    const enum FallbackStrategy &fallbackStrategy() const {
        return mFallbackStrategy;
    }

    /** Basic information on the agency responsible for the model. */
    struct Authority {
        std::string name{};
        std::string url{};
        std::string address{};
        std::string email{};
    };

    /** Get basic information on the agency responsible for the model. */
    const Authority &authority() const { return mAuthority; }

    /** Hyperlink related to the model. */
    struct Link {
        /** URL holding the information */
        std::string href{};

        /** Relationship to the dataset. e.g. "about", "source", "license",
         * "metadata" */
        std::string rel{};

        /** Mime type */
        std::string type{};

        /** Description of the link */
        std::string title{};
    };

    /** Get links to related information. */
    const std::vector<Link> links() const { return mLinks; }

    /** Get a string identifying the CRS of source coordinates in the
     * vertices. Typically "EPSG:XXXX". If the transformation is for vertical
     * component, this should be the code for a compound CRS (can be
     * EPSG:XXXX+YYYY where XXXX is the code of the horizontal CRS and YYYY
     * the code of the vertical CRS).
     * For example, for the KKJ->ETRS89 transformation, this is EPSG:2393
     * ("KKJ / Finland Uniform Coordinate System").
     * The input coordinates are assumed to
     * be passed in the "normalized for visualisation" / "GIS friendly" order,
     * that is longitude, latitude for geographic coordinates and
     * easting, northing for projected coordinates.
     * This may be empty for unspecified CRS.
     */
    const std::string &inputCRS() const { return mInputCRS; }

    /** Get a string identifying the CRS of target coordinates in the
     * vertices. Typically "EPSG:XXXX". If the transformation is for vertical
     * component, this should be the code for a compound CRS (can be
     * EPSG:XXXX+YYYY where XXXX is the code of the horizontal CRS and YYYY
     * the code of the vertical CRS).
     * For example, for the KKJ->ETRS89 transformation, this is EPSG:3067
     * ("ETRS89 / TM35FIN(E,N)").
     * The output coordinates will be
     * returned in the "normalized for visualisation" / "GIS friendly" order,
     * that is longitude, latitude for geographic coordinates and
     * easting, northing for projected coordinates.
     * This may be empty for unspecified CRS.
     */
    const std::string &outputCRS() const { return mOutputCRS; }

    /** Return whether horizontal coordinates are transformed. */
    bool transformHorizontalComponent() const {
        return mTransformHorizontalComponent;
    }

    /** Return whether vertical coordinates are transformed. */
    bool transformVerticalComponent() const {
        return mTransformVerticalComponent;
    }

    /** Indices of vertices of a triangle */
    struct VertexIndices {
        /** Index of first vertex */
        unsigned idx1;
        /** Index of second vertex */
        unsigned idx2;
        /** Index of third vertex */
        unsigned idx3;
    };

    /** Return number of elements per vertex of vertices() */
    unsigned verticesColumnCount() const { return mVerticesColumnCount; }

    /** Return description of triangulation vertices.
     * Each vertex is described by verticesColumnCount() consecutive values.
     * They are respectively:
     * - the source X value
     * - the source Y value
     * - (if transformHorizontalComponent() is true) the target X value
     * - (if transformHorizontalComponent() is true) the target Y value
     * - (if transformVerticalComponent() is true) the delta Z value (to go from
     * source to target Z)
     *
     * X is assumed to be a longitude (in degrees) or easting value.
     * Y is assumed to be a latitude (in degrees) or northing value.
     */
    const std::vector<double> &vertices() const { return mVertices; }

    /** Return triangles*/
    const std::vector<VertexIndices> &triangles() const { return mTriangles; }

  private:
    TINShiftFile() = default;

    std::string mFileType{};
    std::string mFormatVersion{};
    std::string mName{};
    std::string mVersion{};
    std::string mLicense{};
    std::string mDescription{};
    std::string mPublicationDate{};
    enum FallbackStrategy mFallbackStrategy {};
    Authority mAuthority{};
    std::vector<Link> mLinks{};
    std::string mInputCRS{};
    std::string mOutputCRS{};
    bool mTransformHorizontalComponent = false;
    bool mTransformVerticalComponent = false;
    unsigned mVerticesColumnCount = 0;
    std::vector<double> mVertices{};
    std::vector<VertexIndices> mTriangles{};
};

// ---------------------------------------------------------------------------

/** Class to evaluate the transformation of a coordinate */
class Evaluator {
  public:
    /** Constructor. */
    explicit Evaluator(std::unique_ptr<TINShiftFile> &&fileIn);

    /** Get file */
    const TINShiftFile &file() const { return *(mFile.get()); }

    /** Evaluate displacement of a position given by (x,y,z,t) and
     * return it in (x_out,y_out_,z_out).
     */
    bool forward(double x, double y, double z, double &x_out, double &y_out,
                 double &z_out);

    /** Apply inverse transformation. */
    bool inverse(double x, double y, double z, double &x_out, double &y_out,
                 double &z_out);

  private:
    std::unique_ptr<TINShiftFile> mFile;

    // Reused between invocations to save memory allocations
    std::vector<unsigned> mTriangleIndices{};

    std::unique_ptr<NS_PROJ::QuadTree::QuadTree<unsigned>> mQuadTreeForward{};
    std::unique_ptr<NS_PROJ::QuadTree::QuadTree<unsigned>> mQuadTreeInverse{};
};

// ---------------------------------------------------------------------------

} // namespace TINSHIFT_NAMESPACE

// ---------------------------------------------------------------------------

#include "tinshift_impl.hpp"

#endif // TINSHIFT_HPP
