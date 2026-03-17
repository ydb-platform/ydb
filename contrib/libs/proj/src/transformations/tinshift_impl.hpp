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

#ifndef TINSHIFT_NAMESPACE
#error "Should be included only by tinshift.hpp"
#endif

#include <algorithm>
#include <cmath>
#include <limits>

namespace TINSHIFT_NAMESPACE {

// ---------------------------------------------------------------------------

static std::string getString(const json &j, const char *key, bool optional) {
    if (!j.contains(key)) {
        if (optional) {
            return std::string();
        }
        throw ParsingException(std::string("Missing \"") + key + "\" key");
    }
    const json v = j[key];
    if (!v.is_string()) {
        throw ParsingException(std::string("The value of \"") + key +
                               "\" should be a string");
    }
    return v.get<std::string>();
}

static std::string getReqString(const json &j, const char *key) {
    return getString(j, key, false);
}

static std::string getOptString(const json &j, const char *key) {
    return getString(j, key, true);
}

// ---------------------------------------------------------------------------

static json getArrayMember(const json &j, const char *key) {
    if (!j.contains(key)) {
        throw ParsingException(std::string("Missing \"") + key + "\" key");
    }
    const json obj = j[key];
    if (!obj.is_array()) {
        throw ParsingException(std::string("The value of \"") + key +
                               "\" should be a array");
    }
    return obj;
}

// ---------------------------------------------------------------------------

std::unique_ptr<TINShiftFile> TINShiftFile::parse(const std::string &text) {
    std::unique_ptr<TINShiftFile> tinshiftFile(new TINShiftFile());
    json j;
    try {
        j = json::parse(text);
    } catch (const std::exception &e) {
        throw ParsingException(e.what());
    }
    if (!j.is_object()) {
        throw ParsingException("Not an object");
    }
    tinshiftFile->mFileType = getReqString(j, "file_type");
    tinshiftFile->mFormatVersion = getReqString(j, "format_version");
    tinshiftFile->mName = getOptString(j, "name");
    tinshiftFile->mVersion = getOptString(j, "version");
    tinshiftFile->mLicense = getOptString(j, "license");
    tinshiftFile->mDescription = getOptString(j, "description");
    tinshiftFile->mPublicationDate = getOptString(j, "publication_date");

    tinshiftFile->mFallbackStrategy = FALLBACK_NONE;
    if (j.contains("fallback_strategy")) {
        if (tinshiftFile->mFormatVersion != "1.1") {
            throw ParsingException(
                "fallback_strategy needs format_version 1.1");
        }
        const auto fallback_strategy = getOptString(j, "fallback_strategy");
        if (fallback_strategy == "nearest_side") {
            tinshiftFile->mFallbackStrategy = FALLBACK_NEAREST_SIDE;
        } else if (fallback_strategy == "nearest_centroid") {
            tinshiftFile->mFallbackStrategy = FALLBACK_NEAREST_CENTROID;
        } else if (fallback_strategy == "none") {
            tinshiftFile->mFallbackStrategy = FALLBACK_NONE;
        } else {
            throw ParsingException("invalid fallback_strategy");
        }
    }

    if (j.contains("authority")) {
        const json jAuthority = j["authority"];
        if (!jAuthority.is_object()) {
            throw ParsingException("authority is not a object");
        }
        tinshiftFile->mAuthority.name = getOptString(jAuthority, "name");
        tinshiftFile->mAuthority.url = getOptString(jAuthority, "url");
        tinshiftFile->mAuthority.address = getOptString(jAuthority, "address");
        tinshiftFile->mAuthority.email = getOptString(jAuthority, "email");
    }

    if (j.contains("links")) {
        const json jLinks = j["links"];
        if (!jLinks.is_array()) {
            throw ParsingException("links is not an array");
        }
        for (const json &jLink : jLinks) {
            if (!jLink.is_object()) {
                throw ParsingException("links[] item is not an object");
            }
            Link link;
            link.href = getOptString(jLink, "href");
            link.rel = getOptString(jLink, "rel");
            link.type = getOptString(jLink, "type");
            link.title = getOptString(jLink, "title");
            tinshiftFile->mLinks.emplace_back(std::move(link));
        }
    }
    tinshiftFile->mInputCRS = getOptString(j, "input_crs");
    tinshiftFile->mOutputCRS = getOptString(j, "output_crs");

    const auto jTransformedComponents =
        getArrayMember(j, "transformed_components");
    for (const json &jComp : jTransformedComponents) {
        if (!jComp.is_string()) {
            throw ParsingException(
                "transformed_components[] item is not a string");
        }
        const auto jCompStr = jComp.get<std::string>();
        if (jCompStr == "horizontal") {
            tinshiftFile->mTransformHorizontalComponent = true;
        } else if (jCompStr == "vertical") {
            tinshiftFile->mTransformVerticalComponent = true;
        } else {
            throw ParsingException("transformed_components[] = " + jCompStr +
                                   " is not handled");
        }
    }

    const auto jVerticesColumns = getArrayMember(j, "vertices_columns");
    int sourceXCol = -1;
    int sourceYCol = -1;
    int sourceZCol = -1;
    int targetXCol = -1;
    int targetYCol = -1;
    int targetZCol = -1;
    int offsetZCol = -1;
    for (size_t i = 0; i < jVerticesColumns.size(); ++i) {
        const json &jColumn = jVerticesColumns[i];
        if (!jColumn.is_string()) {
            throw ParsingException("vertices_columns[] item is not a string");
        }
        const auto jColumnStr = jColumn.get<std::string>();
        if (jColumnStr == "source_x") {
            sourceXCol = static_cast<int>(i);
        } else if (jColumnStr == "source_y") {
            sourceYCol = static_cast<int>(i);
        } else if (jColumnStr == "source_z") {
            sourceZCol = static_cast<int>(i);
        } else if (jColumnStr == "target_x") {
            targetXCol = static_cast<int>(i);
        } else if (jColumnStr == "target_y") {
            targetYCol = static_cast<int>(i);
        } else if (jColumnStr == "target_z") {
            targetZCol = static_cast<int>(i);
        } else if (jColumnStr == "offset_z") {
            offsetZCol = static_cast<int>(i);
        }
    }
    if (sourceXCol < 0) {
        throw ParsingException(
            "source_x must be specified in vertices_columns[]");
    }
    if (sourceYCol < 0) {
        throw ParsingException(
            "source_y must be specified in vertices_columns[]");
    }
    if (tinshiftFile->mTransformHorizontalComponent) {
        if (targetXCol < 0) {
            throw ParsingException(
                "target_x must be specified in vertices_columns[]");
        }
        if (targetYCol < 0) {
            throw ParsingException(
                "target_y must be specified in vertices_columns[]");
        }
    }
    if (tinshiftFile->mTransformVerticalComponent) {
        if (offsetZCol >= 0) {
            // do nothing
        } else {
            if (sourceZCol < 0) {
                throw ParsingException("source_z or delta_z must be specified "
                                       "in vertices_columns[]");
            }
            if (targetZCol < 0) {
                throw ParsingException(
                    "target_z must be specified in vertices_columns[]");
            }
        }
    }

    const auto jTrianglesColumns = getArrayMember(j, "triangles_columns");
    int idxVertex1Col = -1;
    int idxVertex2Col = -1;
    int idxVertex3Col = -1;
    for (size_t i = 0; i < jTrianglesColumns.size(); ++i) {
        const json &jColumn = jTrianglesColumns[i];
        if (!jColumn.is_string()) {
            throw ParsingException("triangles_columns[] item is not a string");
        }
        const auto jColumnStr = jColumn.get<std::string>();
        if (jColumnStr == "idx_vertex1") {
            idxVertex1Col = static_cast<int>(i);
        } else if (jColumnStr == "idx_vertex2") {
            idxVertex2Col = static_cast<int>(i);
        } else if (jColumnStr == "idx_vertex3") {
            idxVertex3Col = static_cast<int>(i);
        }
    }
    if (idxVertex1Col < 0) {
        throw ParsingException(
            "idx_vertex1 must be specified in triangles_columns[]");
    }
    if (idxVertex2Col < 0) {
        throw ParsingException(
            "idx_vertex2 must be specified in triangles_columns[]");
    }
    if (idxVertex3Col < 0) {
        throw ParsingException(
            "idx_vertex3 must be specified in triangles_columns[]");
    }

    const auto jVertices = getArrayMember(j, "vertices");
    tinshiftFile->mVerticesColumnCount = 2;
    if (tinshiftFile->mTransformHorizontalComponent)
        tinshiftFile->mVerticesColumnCount += 2;
    if (tinshiftFile->mTransformVerticalComponent)
        tinshiftFile->mVerticesColumnCount += 1;

    tinshiftFile->mVertices.reserve(tinshiftFile->mVerticesColumnCount *
                                    jVertices.size());
    for (const auto &jVertex : jVertices) {
        if (!jVertex.is_array()) {
            throw ParsingException("vertices[] item is not an array");
        }
        if (jVertex.size() != jVerticesColumns.size()) {
            throw ParsingException(
                "vertices[] item has not expected number of elements");
        }
        if (!jVertex[sourceXCol].is_number()) {
            throw ParsingException("vertices[][] item is not a number");
        }
        tinshiftFile->mVertices.push_back(jVertex[sourceXCol].get<double>());
        if (!jVertex[sourceYCol].is_number()) {
            throw ParsingException("vertices[][] item is not a number");
        }
        tinshiftFile->mVertices.push_back(jVertex[sourceYCol].get<double>());
        if (tinshiftFile->mTransformHorizontalComponent) {
            if (!jVertex[targetXCol].is_number()) {
                throw ParsingException("vertices[][] item is not a number");
            }
            tinshiftFile->mVertices.push_back(
                jVertex[targetXCol].get<double>());
            if (!jVertex[targetYCol].is_number()) {
                throw ParsingException("vertices[][] item is not a number");
            }
            tinshiftFile->mVertices.push_back(
                jVertex[targetYCol].get<double>());
        }
        if (tinshiftFile->mTransformVerticalComponent) {
            if (offsetZCol >= 0) {
                if (!jVertex[offsetZCol].is_number()) {
                    throw ParsingException("vertices[][] item is not a number");
                }
                tinshiftFile->mVertices.push_back(
                    jVertex[offsetZCol].get<double>());
            } else {
                if (!jVertex[sourceZCol].is_number()) {
                    throw ParsingException("vertices[][] item is not a number");
                }
                const double sourceZ = jVertex[sourceZCol].get<double>();
                if (!jVertex[targetZCol].is_number()) {
                    throw ParsingException("vertices[][] item is not a number");
                }
                const double targetZ = jVertex[targetZCol].get<double>();
                tinshiftFile->mVertices.push_back(targetZ - sourceZ);
            }
        }
    }

    const auto jTriangles = getArrayMember(j, "triangles");
    tinshiftFile->mTriangles.reserve(jTriangles.size());
    for (const auto &jTriangle : jTriangles) {
        if (!jTriangle.is_array()) {
            throw ParsingException("triangles[] item is not an array");
        }
        if (jTriangle.size() != jTrianglesColumns.size()) {
            throw ParsingException(
                "triangles[] item has not expected number of elements");
        }

        if (jTriangle[idxVertex1Col].type() != json::value_t::number_unsigned) {
            throw ParsingException("triangles[][] item is not an integer");
        }
        const unsigned vertex1 = jTriangle[idxVertex1Col].get<unsigned>();
        if (vertex1 >= jVertices.size()) {
            throw ParsingException("Invalid value for a vertex index");
        }

        if (jTriangle[idxVertex2Col].type() != json::value_t::number_unsigned) {
            throw ParsingException("triangles[][] item is not an integer");
        }
        const unsigned vertex2 = jTriangle[idxVertex2Col].get<unsigned>();
        if (vertex2 >= jVertices.size()) {
            throw ParsingException("Invalid value for a vertex index");
        }

        if (jTriangle[idxVertex3Col].type() != json::value_t::number_unsigned) {
            throw ParsingException("triangles[][] item is not an integer");
        }
        const unsigned vertex3 = jTriangle[idxVertex3Col].get<unsigned>();
        if (vertex3 >= jVertices.size()) {
            throw ParsingException("Invalid value for a vertex index");
        }

        VertexIndices vi;
        vi.idx1 = vertex1;
        vi.idx2 = vertex2;
        vi.idx3 = vertex3;
        tinshiftFile->mTriangles.push_back(vi);
    }

    return tinshiftFile;
}

// ---------------------------------------------------------------------------

static NS_PROJ::QuadTree::RectObj GetBounds(const TINShiftFile &file,
                                            bool forward) {
    NS_PROJ::QuadTree::RectObj rect;
    rect.minx = std::numeric_limits<double>::max();
    rect.miny = std::numeric_limits<double>::max();
    rect.maxx = -std::numeric_limits<double>::max();
    rect.maxy = -std::numeric_limits<double>::max();
    const auto &vertices = file.vertices();
    const unsigned colCount = file.verticesColumnCount();
    const int idxX = file.transformHorizontalComponent() && !forward ? 2 : 0;
    const int idxY = file.transformHorizontalComponent() && !forward ? 3 : 1;
    for (size_t i = 0; i + colCount - 1 < vertices.size(); i += colCount) {
        const double x = vertices[i + idxX];
        const double y = vertices[i + idxY];
        rect.minx = std::min(rect.minx, x);
        rect.miny = std::min(rect.miny, y);
        rect.maxx = std::max(rect.maxx, x);
        rect.maxy = std::max(rect.maxy, y);
    }
    return rect;
}

// ---------------------------------------------------------------------------

static std::unique_ptr<NS_PROJ::QuadTree::QuadTree<unsigned>>
BuildQuadTree(const TINShiftFile &file, bool forward) {
    auto quadtree = std::unique_ptr<NS_PROJ::QuadTree::QuadTree<unsigned>>(
        new NS_PROJ::QuadTree::QuadTree<unsigned>(GetBounds(file, forward)));
    const auto &triangles = file.triangles();
    const auto &vertices = file.vertices();
    const int idxX = file.transformHorizontalComponent() && !forward ? 2 : 0;
    const int idxY = file.transformHorizontalComponent() && !forward ? 3 : 1;
    const unsigned colCount = file.verticesColumnCount();
    for (size_t i = 0; i < triangles.size(); ++i) {
        const unsigned i1 = triangles[i].idx1;
        const unsigned i2 = triangles[i].idx2;
        const unsigned i3 = triangles[i].idx3;
        const double x1 = vertices[i1 * colCount + idxX];
        const double y1 = vertices[i1 * colCount + idxY];
        const double x2 = vertices[i2 * colCount + idxX];
        const double y2 = vertices[i2 * colCount + idxY];
        const double x3 = vertices[i3 * colCount + idxX];
        const double y3 = vertices[i3 * colCount + idxY];
        NS_PROJ::QuadTree::RectObj rect;
        rect.minx = x1;
        rect.miny = y1;
        rect.maxx = x1;
        rect.maxy = y1;
        rect.minx = std::min(rect.minx, x2);
        rect.miny = std::min(rect.miny, y2);
        rect.maxx = std::max(rect.maxx, x2);
        rect.maxy = std::max(rect.maxy, y2);
        rect.minx = std::min(rect.minx, x3);
        rect.miny = std::min(rect.miny, y3);
        rect.maxx = std::max(rect.maxx, x3);
        rect.maxy = std::max(rect.maxy, y3);
        quadtree->insert(static_cast<unsigned>(i), rect);
    }

    return quadtree;
}

// ---------------------------------------------------------------------------

Evaluator::Evaluator(std::unique_ptr<TINShiftFile> &&fileIn)
    : mFile(std::move(fileIn)) {}

// ---------------------------------------------------------------------------

static inline double sqr(double x) { return x * x; }
static inline double squared_distance(double x1, double y1, double x2,
                                      double y2) {
    return sqr(x1 - x2) + sqr(y1 - y2);
}
static double distance_point_segment(double x, double y, double x1, double y1,
                                     double x2, double y2, double dist12) {
    // squared distance of point x/y to line segment x1/y1 -- x2/y2
    double t = ((x - x1) * (x2 - x1) + (y - y1) * (y2 - y1)) / dist12;
    if (t <= 0.0) {
        // closest to x1/y1
        return squared_distance(x, y, x1, y1);
    }
    if (t >= 1.0) {
        // closest to y2/y2
        return squared_distance(x, y, x2, y2);
    }

    // closest to line segment x1/y1 -- x2/y2
    return squared_distance(x, y, x1 + t * (x2 - x1), y1 + t * (y2 - y1));
}

static const TINShiftFile::VertexIndices *
FindTriangle(const TINShiftFile &file,
             const NS_PROJ::QuadTree::QuadTree<unsigned> &quadtree,
             std::vector<unsigned> &triangleIndices, double x, double y,
             bool forward, double &lambda1, double &lambda2, double &lambda3) {
#define USE_QUADTREE
#ifdef USE_QUADTREE
    triangleIndices.clear();
    quadtree.search(x, y, triangleIndices);
#endif
    const auto &triangles = file.triangles();
    const auto &vertices = file.vertices();
    constexpr double EPS = 1e-10;
    const int idxX = file.transformHorizontalComponent() && !forward ? 2 : 0;
    const int idxY = file.transformHorizontalComponent() && !forward ? 3 : 1;
    const unsigned colCount = file.verticesColumnCount();
#ifdef USE_QUADTREE
    for (unsigned i : triangleIndices)
#else
    for (size_t i = 0; i < triangles.size(); ++i)
#endif
    {
        const auto &triangle = triangles[i];
        const unsigned i1 = triangle.idx1;
        const unsigned i2 = triangle.idx2;
        const unsigned i3 = triangle.idx3;
        const double x1 = vertices[i1 * colCount + idxX];
        const double y1 = vertices[i1 * colCount + idxY];
        const double x2 = vertices[i2 * colCount + idxX];
        const double y2 = vertices[i2 * colCount + idxY];
        const double x3 = vertices[i3 * colCount + idxX];
        const double y3 = vertices[i3 * colCount + idxY];
        const double det_T = (y2 - y3) * (x1 - x3) + (x3 - x2) * (y1 - y3);
        lambda1 = ((y2 - y3) * (x - x3) + (x3 - x2) * (y - y3)) / det_T;
        lambda2 = ((y3 - y1) * (x - x3) + (x1 - x3) * (y - y3)) / det_T;
        if (lambda1 >= -EPS && lambda1 <= 1 + EPS && lambda2 >= -EPS &&
            lambda2 <= 1 + EPS) {
            lambda3 = 1 - lambda1 - lambda2;
            if (lambda3 >= 0) {
                return &triangle;
            }
        }
    }
    if (file.fallbackStrategy() == FALLBACK_NONE) {
        return nullptr;
    }
    // find triangle with the shortest squared distance
    //
    // TODO: extend quadtree to support nearest neighbor search
    double closest_dist = std::numeric_limits<double>::infinity();
    double closest_dist2 = std::numeric_limits<double>::infinity();
    size_t closest_i = 0;
    for (size_t i = 0; i < triangles.size(); ++i) {
        const auto &triangle = triangles[i];
        const unsigned i1 = triangle.idx1;
        const unsigned i2 = triangle.idx2;
        const unsigned i3 = triangle.idx3;
        const double x1 = vertices[i1 * colCount + idxX];
        const double y1 = vertices[i1 * colCount + idxY];
        const double x2 = vertices[i2 * colCount + idxX];
        const double y2 = vertices[i2 * colCount + idxY];
        const double x3 = vertices[i3 * colCount + idxX];
        const double y3 = vertices[i3 * colCount + idxY];

        // don't check this triangle if the query point plusminus the
        // currently closest found distance is outside the triangle's AABB
        if (x + closest_dist < std::min(x1, std::min(x2, x3)) ||
            x - closest_dist > std::max(x1, std::max(x2, x3)) ||
            y + closest_dist < std::min(y1, std::min(y2, y3)) ||
            y - closest_dist > std::max(y1, std::max(y2, y3))) {
            continue;
        }

        double dist12 = squared_distance(x1, y1, x2, y2);
        double dist23 = squared_distance(x2, y2, x3, y3);
        double dist13 = squared_distance(x1, y1, x3, y3);
        if (dist12 < EPS || dist23 < EPS || dist13 < EPS) {
            // do not use degenerate triangles
            continue;
        }
        double dist2;
        if (file.fallbackStrategy() == FALLBACK_NEAREST_SIDE) {
            // we don't know whether the points of the triangle are given
            // clockwise or counter-clockwise, so we have to check the distance
            // of the point to all three sides of the triangle
            dist2 = distance_point_segment(x, y, x1, y1, x2, y2, dist12);
            if (dist2 < closest_dist2) {
                closest_dist2 = dist2;
                closest_dist = sqrt(dist2);
                closest_i = i;
            }
            dist2 = distance_point_segment(x, y, x2, y2, x3, y3, dist23);
            if (dist2 < closest_dist2) {
                closest_dist2 = dist2;
                closest_dist = sqrt(dist2);
                closest_i = i;
            }
            dist2 = distance_point_segment(x, y, x1, y1, x3, y3, dist13);
            if (dist2 < closest_dist2) {
                closest_dist2 = dist2;
                closest_dist = sqrt(dist2);
                closest_i = i;
            }
        } else if (file.fallbackStrategy() == FALLBACK_NEAREST_CENTROID) {
            double c_x = (x1 + x2 + x3) / 3.0;
            double c_y = (y1 + y2 + y3) / 3.0;
            dist2 = squared_distance(x, y, c_x, c_y);
            if (dist2 < closest_dist2) {
                closest_dist2 = dist2;
                closest_dist = sqrt(dist2);
                closest_i = i;
            }
        }
    }
    if (std::isinf(closest_dist)) {
        // nothing was found due to empty triangle list or only degenerate
        // triangles
        return nullptr;
    }
    const auto &triangle = triangles[closest_i];
    const unsigned i1 = triangle.idx1;
    const unsigned i2 = triangle.idx2;
    const unsigned i3 = triangle.idx3;
    const double x1 = vertices[i1 * colCount + idxX];
    const double y1 = vertices[i1 * colCount + idxY];
    const double x2 = vertices[i2 * colCount + idxX];
    const double y2 = vertices[i2 * colCount + idxY];
    const double x3 = vertices[i3 * colCount + idxX];
    const double y3 = vertices[i3 * colCount + idxY];
    const double det_T = (y2 - y3) * (x1 - x3) + (x3 - x2) * (y1 - y3);
    if (std::fabs(det_T) < EPS) {
        // the nearest triangle is degenerate
        return nullptr;
    }
    lambda1 = ((y2 - y3) * (x - x3) + (x3 - x2) * (y - y3)) / det_T;
    lambda2 = ((y3 - y1) * (x - x3) + (x1 - x3) * (y - y3)) / det_T;
    lambda3 = 1 - lambda1 - lambda2;
    return &triangle;
}

// ---------------------------------------------------------------------------

bool Evaluator::forward(double x, double y, double z, double &x_out,
                        double &y_out, double &z_out) {
    if (!mQuadTreeForward)
        mQuadTreeForward = BuildQuadTree(*(mFile.get()), true);

    double lambda1 = 0.0;
    double lambda2 = 0.0;
    double lambda3 = 0.0;
    const auto *triangle =
        FindTriangle(*mFile, *mQuadTreeForward, mTriangleIndices, x, y, true,
                     lambda1, lambda2, lambda3);
    if (!triangle)
        return false;
    const auto &vertices = mFile->vertices();
    const unsigned i1 = triangle->idx1;
    const unsigned i2 = triangle->idx2;
    const unsigned i3 = triangle->idx3;
    const unsigned colCount = mFile->verticesColumnCount();
    if (mFile->transformHorizontalComponent()) {
        constexpr unsigned idxTargetColX = 2;
        constexpr unsigned idxTargetColY = 3;
        x_out = vertices[i1 * colCount + idxTargetColX] * lambda1 +
                vertices[i2 * colCount + idxTargetColX] * lambda2 +
                vertices[i3 * colCount + idxTargetColX] * lambda3;
        y_out = vertices[i1 * colCount + idxTargetColY] * lambda1 +
                vertices[i2 * colCount + idxTargetColY] * lambda2 +
                vertices[i3 * colCount + idxTargetColY] * lambda3;
    } else {
        x_out = x;
        y_out = y;
    }
    if (mFile->transformVerticalComponent()) {
        const int idxCol = mFile->transformHorizontalComponent() ? 4 : 2;
        z_out = z + (vertices[i1 * colCount + idxCol] * lambda1 +
                     vertices[i2 * colCount + idxCol] * lambda2 +
                     vertices[i3 * colCount + idxCol] * lambda3);
    } else {
        z_out = z;
    }
    return true;
}

// ---------------------------------------------------------------------------

bool Evaluator::inverse(double x, double y, double z, double &x_out,
                        double &y_out, double &z_out) {
    NS_PROJ::QuadTree::QuadTree<unsigned> *quadtree;
    if (!mFile->transformHorizontalComponent() &&
        mFile->transformVerticalComponent()) {
        if (!mQuadTreeForward)
            mQuadTreeForward = BuildQuadTree(*(mFile.get()), true);
        quadtree = mQuadTreeForward.get();
    } else {
        if (!mQuadTreeInverse)
            mQuadTreeInverse = BuildQuadTree(*(mFile.get()), false);
        quadtree = mQuadTreeInverse.get();
    }

    double lambda1 = 0.0;
    double lambda2 = 0.0;
    double lambda3 = 0.0;
    const auto *triangle = FindTriangle(*mFile, *quadtree, mTriangleIndices, x,
                                        y, false, lambda1, lambda2, lambda3);
    if (!triangle)
        return false;
    const auto &vertices = mFile->vertices();
    const unsigned i1 = triangle->idx1;
    const unsigned i2 = triangle->idx2;
    const unsigned i3 = triangle->idx3;
    const unsigned colCount = mFile->verticesColumnCount();
    if (mFile->transformHorizontalComponent()) {
        constexpr unsigned idxTargetColX = 0;
        constexpr unsigned idxTargetColY = 1;
        x_out = vertices[i1 * colCount + idxTargetColX] * lambda1 +
                vertices[i2 * colCount + idxTargetColX] * lambda2 +
                vertices[i3 * colCount + idxTargetColX] * lambda3;
        y_out = vertices[i1 * colCount + idxTargetColY] * lambda1 +
                vertices[i2 * colCount + idxTargetColY] * lambda2 +
                vertices[i3 * colCount + idxTargetColY] * lambda3;
    } else {
        x_out = x;
        y_out = y;
    }
    if (mFile->transformVerticalComponent()) {
        const int idxCol = mFile->transformHorizontalComponent() ? 4 : 2;
        z_out = z - (vertices[i1 * colCount + idxCol] * lambda1 +
                     vertices[i2 * colCount + idxCol] * lambda2 +
                     vertices[i3 * colCount + idxCol] * lambda3);
    } else {
        z_out = z;
    }
    return true;
}

} // namespace TINSHIFT_NAMESPACE
