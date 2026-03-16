/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  C API wrapper of C++ API
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

#include <algorithm>
#include <cassert>
#include <cstdarg>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <utility>
#include <vector>

#include "proj/common.hpp"
#include "proj/coordinateoperation.hpp"
#include "proj/coordinates.hpp"
#include "proj/coordinatesystem.hpp"
#include "proj/crs.hpp"
#include "proj/datum.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "proj/internal/datum_internal.hpp"
#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"

// PROJ include order is sensitive
// clang-format off
#include "proj.h"
#include "proj_internal.h"
#include "proj_experimental.h"
// clang-format on
#include "geodesic.h"
#include "proj_constants.h"

using namespace NS_PROJ::common;
using namespace NS_PROJ::coordinates;
using namespace NS_PROJ::crs;
using namespace NS_PROJ::cs;
using namespace NS_PROJ::datum;
using namespace NS_PROJ::io;
using namespace NS_PROJ::internal;
using namespace NS_PROJ::metadata;
using namespace NS_PROJ::operation;
using namespace NS_PROJ::util;
using namespace NS_PROJ;

// ---------------------------------------------------------------------------

static void PROJ_NO_INLINE proj_log_error(PJ_CONTEXT *ctx, const char *function,
                                          const char *text) {
    if (ctx->debug_level != PJ_LOG_NONE) {
        std::string msg(function);
        msg += ": ";
        msg += text;
        ctx->logger(ctx->logger_app_data, PJ_LOG_ERROR, msg.c_str());
    }
    auto previous_errno = proj_context_errno(ctx);
    if (previous_errno == 0) {
        // only set errno if it wasn't set deeper down the call stack
        proj_context_errno_set(ctx, PROJ_ERR_OTHER);
    }
}

// ---------------------------------------------------------------------------

static void PROJ_NO_INLINE proj_log_debug(PJ_CONTEXT *ctx, const char *function,
                                          const char *text) {
    std::string msg(function);
    msg += ": ";
    msg += text;
    ctx->logger(ctx->logger_app_data, PJ_LOG_DEBUG, msg.c_str());
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

template <class T> static PROJ_STRING_LIST to_string_list(T &&set) {
    auto ret = new char *[set.size() + 1];
    size_t i = 0;
    for (const auto &str : set) {
        try {
            ret[i] = new char[str.size() + 1];
        } catch (const std::exception &) {
            while (--i > 0) {
                delete[] ret[i];
            }
            delete[] ret;
            throw;
        }
        std::memcpy(ret[i], str.c_str(), str.size() + 1);
        i++;
    }
    ret[i] = nullptr;
    return ret;
}

// ---------------------------------------------------------------------------

void proj_context_delete_cpp_context(struct projCppContext *cppContext) {
    delete cppContext;
}
// ---------------------------------------------------------------------------

projCppContext::projCppContext(PJ_CONTEXT *ctx, const char *dbPath,
                               const std::vector<std::string> &auxDbPaths)
    : ctx_(ctx), dbPath_(dbPath ? dbPath : std::string()),
      auxDbPaths_(auxDbPaths) {}

// ---------------------------------------------------------------------------

std::vector<std::string>
projCppContext::toVector(const char *const *auxDbPaths) {
    std::vector<std::string> res;
    for (auto iter = auxDbPaths; iter && *iter; ++iter) {
        res.emplace_back(std::string(*iter));
    }
    return res;
}

// ---------------------------------------------------------------------------

projCppContext *projCppContext::clone(PJ_CONTEXT *ctx) const {
    projCppContext *newContext =
        new projCppContext(ctx, getDbPath().c_str(), getAuxDbPaths());
    return newContext;
}

// ---------------------------------------------------------------------------

NS_PROJ::io::DatabaseContextNNPtr projCppContext::getDatabaseContext() {
    if (databaseContext_) {
        return NN_NO_CHECK(databaseContext_);
    }
    auto dbContext =
        NS_PROJ::io::DatabaseContext::create(dbPath_, auxDbPaths_, ctx_);
    databaseContext_ = dbContext;
    return dbContext;
}

// ---------------------------------------------------------------------------

static PROJ_NO_INLINE DatabaseContextNNPtr getDBcontext(PJ_CONTEXT *ctx) {
    return ctx->get_cpp_context()->getDatabaseContext();
}

// ---------------------------------------------------------------------------

static PROJ_NO_INLINE DatabaseContextPtr
getDBcontextNoException(PJ_CONTEXT *ctx, const char *function) {
    try {
        return getDBcontext(ctx).as_nullable();
    } catch (const std::exception &e) {
        proj_log_debug(ctx, function, e.what());
        return nullptr;
    }
}
// ---------------------------------------------------------------------------

PJ *pj_obj_create(PJ_CONTEXT *ctx, const BaseObjectNNPtr &objIn) {
    auto coordop = dynamic_cast<const CoordinateOperation *>(objIn.get());
    if (coordop) {
        auto singleOp = dynamic_cast<const SingleOperation *>(coordop);
        bool bTryToExportToProj = true;
        if (singleOp && singleOp->method()->nameStr() == "unnamed") {
            // Can happen for example when the GDAL GeoTIFF SRS builder
            // creates a dummy conversion when building the SRS, before setting
            // the final map projection. This avoids exportToPROJString() from
            // throwing an exception.
            bTryToExportToProj = false;
        }
        if (bTryToExportToProj) {
            auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
            try {
                auto formatter = PROJStringFormatter::create(
                    PROJStringFormatter::Convention::PROJ_5,
                    std::move(dbContext));
                auto projString = coordop->exportToPROJString(formatter.get());
                const bool defer_grid_opening_backup = ctx->defer_grid_opening;
                if (!defer_grid_opening_backup &&
                    proj_context_is_network_enabled(ctx)) {
                    ctx->defer_grid_opening = true;
                }
                auto pj = pj_create_internal(ctx, projString.c_str());
                ctx->defer_grid_opening = defer_grid_opening_backup;
                if (pj) {
                    pj->iso_obj = objIn;
                    pj->iso_obj_is_coordinate_operation = true;
                    auto sourceEpoch = coordop->sourceCoordinateEpoch();
                    auto targetEpoch = coordop->targetCoordinateEpoch();
                    if (sourceEpoch.has_value()) {
                        if (!targetEpoch.has_value()) {
                            pj->hasCoordinateEpoch = true;
                            pj->coordinateEpoch =
                                sourceEpoch->coordinateEpoch().convertToUnit(
                                    common::UnitOfMeasure::YEAR);
                        }
                    } else {
                        if (targetEpoch.has_value()) {
                            pj->hasCoordinateEpoch = true;
                            pj->coordinateEpoch =
                                targetEpoch->coordinateEpoch().convertToUnit(
                                    common::UnitOfMeasure::YEAR);
                        }
                    }
                    return pj;
                }
            } catch (const std::exception &) {
                // Silence, since we may not always be able to export as a
                // PROJ string.
            }
        }
    }
    auto pj = pj_new();
    if (pj) {
        pj->ctx = ctx;
        pj->descr = "ISO-19111 object";
        pj->iso_obj = objIn;
        pj->iso_obj_is_coordinate_operation = coordop != nullptr;
        try {
            auto crs = dynamic_cast<const CRS *>(objIn.get());
            if (crs) {
                auto geodCRS = crs->extractGeodeticCRS();
                if (geodCRS) {
                    const auto &ellps = geodCRS->ellipsoid();
                    const double a = ellps->semiMajorAxis().getSIValue();
                    const double es = ellps->squaredEccentricity();
                    if (!(a > 0 && es >= 0 && es < 1)) {
                        proj_log_error(pj, _("Invalid ellipsoid parameters"));
                        proj_errno_set(pj,
                                       PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
                        proj_destroy(pj);
                        return nullptr;
                    }
                    pj_calc_ellipsoid_params(pj, a, es);
                    assert(pj->geod == nullptr);
                    pj->geod = static_cast<struct geod_geodesic *>(
                        calloc(1, sizeof(struct geod_geodesic)));
                    if (pj->geod) {
                        geod_init(pj->geod, pj->a,
                                  pj->es / (1 + sqrt(pj->one_es)));
                    }
                }
            }
        } catch (const std::exception &) {
        }
    }
    return pj;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Opaque object representing a set of operation results. */
struct PJ_OBJ_LIST {
    //! @cond Doxygen_Suppress
    std::vector<IdentifiedObjectNNPtr> objects;

    explicit PJ_OBJ_LIST(std::vector<IdentifiedObjectNNPtr> &&objectsIn)
        : objects(std::move(objectsIn)) {}
    virtual ~PJ_OBJ_LIST();

    PJ_OBJ_LIST(const PJ_OBJ_LIST &) = delete;
    PJ_OBJ_LIST &operator=(const PJ_OBJ_LIST &) = delete;
    //! @endcond
};

//! @cond Doxygen_Suppress
PJ_OBJ_LIST::~PJ_OBJ_LIST() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

#define SANITIZE_CTX(ctx)                                                      \
    do {                                                                       \
        if (ctx == nullptr) {                                                  \
            ctx = pj_get_default_ctx();                                        \
        }                                                                      \
    } while (0)

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Starting with PROJ 8.1, this function does nothing.
 *
 * If you want to take into account changes to the PROJ database, you need to
 * re-create a new context.
 *
 * @param ctx Ignored
 * @param autoclose Ignored
 * @since 6.2
 * deprecated Since 8.1
 */
void proj_context_set_autoclose_database(PJ_CONTEXT *ctx, int autoclose) {
    (void)ctx;
    (void)autoclose;
}

// ---------------------------------------------------------------------------

/** \brief Explicitly point to the main PROJ CRS and coordinate operation
 * definition database ("proj.db"), and potentially auxiliary databases with
 * same structure.
 *
 * Starting with PROJ 8.1, if the auxDbPaths parameter is an empty array,
 * the PROJ_AUX_DB environment variable will be used, if set.
 * It must contain one or several paths. If several paths are
 * provided, they must be separated by the colon (:) character on Unix, and
 * on Windows, by the semi-colon (;) character.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param dbPath Path to main database, or NULL for default.
 * @param auxDbPaths NULL-terminated list of auxiliary database filenames, or
 * NULL.
 * @param options should be set to NULL for now
 * @return TRUE in case of success
 */
int proj_context_set_database_path(PJ_CONTEXT *ctx, const char *dbPath,
                                   const char *const *auxDbPaths,
                                   const char *const *options) {
    SANITIZE_CTX(ctx);
    (void)options;
    std::string osPrevDbPath;
    std::vector<std::string> osPrevAuxDbPaths;
    if (ctx->cpp_context) {
        osPrevDbPath = ctx->cpp_context->getDbPath();
        osPrevAuxDbPaths = ctx->cpp_context->getAuxDbPaths();
    }
    delete ctx->cpp_context;
    ctx->cpp_context = nullptr;
    try {
        ctx->cpp_context = new projCppContext(
            ctx, dbPath, projCppContext::toVector(auxDbPaths));
        ctx->cpp_context->getDatabaseContext();
        return true;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        delete ctx->cpp_context;
        ctx->cpp_context =
            new projCppContext(ctx, osPrevDbPath.c_str(), osPrevAuxDbPaths);
        return false;
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns the path to the database.
 *
 * The returned pointer remains valid while ctx is valid, and until
 * proj_context_set_database_path() is called.
 *
 * @param ctx PROJ context, or NULL for default context
 * @return path, or nullptr
 */
const char *proj_context_get_database_path(PJ_CONTEXT *ctx) {
    SANITIZE_CTX(ctx);
    try {
        // temporary variable must be used as getDBcontext() might create
        // ctx->cpp_context
        const std::string osPath(getDBcontext(ctx)->getPath());
        ctx->get_cpp_context()->lastDbPath_ = osPath;
        return ctx->cpp_context->lastDbPath_.c_str();
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Return a metadata from the database.
 *
 * The returned pointer remains valid while ctx is valid, and until
 * proj_context_get_database_metadata() is called.
 *
 * Available keys:
 *
 * - DATABASE.LAYOUT.VERSION.MAJOR
 * - DATABASE.LAYOUT.VERSION.MINOR
 * - EPSG.VERSION
 * - EPSG.DATE
 * - ESRI.VERSION
 * - ESRI.DATE
 * - IGNF.SOURCE
 * - IGNF.VERSION
 * - IGNF.DATE
 * - NKG.SOURCE
 * - NKG.VERSION
 * - NKG.DATE
 * - PROJ.VERSION
 * - PROJ_DATA.VERSION : PROJ-data version most compatible with this database.
 *
 *
 * @param ctx PROJ context, or NULL for default context
 * @param key Metadata key. Must not be NULL
 * @return value, or nullptr
 */
const char *proj_context_get_database_metadata(PJ_CONTEXT *ctx,
                                               const char *key) {
    SANITIZE_CTX(ctx);
    if (!key) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    try {
        // temporary variable must be used as getDBcontext() might create
        // ctx->cpp_context
        auto osVal(getDBcontext(ctx)->getMetadata(key));
        if (osVal == nullptr) {
            return nullptr;
        }
        ctx->get_cpp_context()->lastDbMetadataItem_ = osVal;
        return ctx->cpp_context->lastDbMetadataItem_.c_str();
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Return the database structure
 *
 * Return SQL statements to run to initiate a new valid auxiliary empty
 * database. It contains definitions of tables, views and triggers, as well
 * as metadata for the version of the layout of the database.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param options null-terminated list of options, or NULL. None currently.
 * @return list of SQL statements (to be freed with proj_string_list_destroy()),
 *         or NULL in case of error.
 * @since 8.1
 */
PROJ_STRING_LIST
proj_context_get_database_structure(PJ_CONTEXT *ctx,
                                    const char *const *options) {
    SANITIZE_CTX(ctx);
    (void)options;
    try {
        auto ret = to_string_list(getDBcontext(ctx)->getDatabaseStructure());
        return ret;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Guess the "dialect" of the WKT string.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param wkt String (must not be NULL)
 */
PJ_GUESSED_WKT_DIALECT proj_context_guess_wkt_dialect(PJ_CONTEXT *ctx,
                                                      const char *wkt) {
    (void)ctx;
    if (!wkt) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return PJ_GUESSED_NOT_WKT;
    }
    switch (WKTParser().guessDialect(wkt)) {
    case WKTParser::WKTGuessedDialect::WKT2_2019:
        return PJ_GUESSED_WKT2_2019;
    case WKTParser::WKTGuessedDialect::WKT2_2015:
        return PJ_GUESSED_WKT2_2015;
    case WKTParser::WKTGuessedDialect::WKT1_GDAL:
        return PJ_GUESSED_WKT1_GDAL;
    case WKTParser::WKTGuessedDialect::WKT1_ESRI:
        return PJ_GUESSED_WKT1_ESRI;
    case WKTParser::WKTGuessedDialect::NOT_WKT:
        break;
    }
    return PJ_GUESSED_NOT_WKT;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static const char *getOptionValue(const char *option,
                                  const char *keyWithEqual) noexcept {
    if (ci_starts_with(option, keyWithEqual)) {
        return option + strlen(keyWithEqual);
    }
    return nullptr;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief "Clone" an object.
 *
 * The object might be used independently of the original object, provided that
 * the use of context is compatible. In particular if you intend to use a
 * clone in a different thread than the original object, you should pass a
 * context that is different from the one of the original object (or later
 * assign a different context with proj_assign_context()).
 *
 * The returned object must be unreferenced with proj_destroy() after use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object to clone. Must not be NULL.
 * @return Object that must be unreferenced with proj_destroy(), or NULL in
 * case of error.
 */
PJ *proj_clone(PJ_CONTEXT *ctx, const PJ *obj) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    if (!obj->iso_obj) {
        if (!obj->alternativeCoordinateOperations.empty()) {
            auto newPj = pj_new();
            if (newPj) {
                newPj->descr = "Set of coordinate operations";
                newPj->ctx = ctx;
                newPj->copyStateFrom(*obj);
                ctx->forceOver = obj->over != 0;
                const int old_debug_level = ctx->debug_level;
                ctx->debug_level = PJ_LOG_NONE;
                for (const auto &altOp : obj->alternativeCoordinateOperations) {
                    newPj->alternativeCoordinateOperations.emplace_back(
                        PJCoordOperation(ctx, altOp));
                }
                ctx->forceOver = false;
                ctx->debug_level = old_debug_level;
            }
            return newPj;
        }
        return nullptr;
    }
    try {
        ctx->forceOver = obj->over != 0;
        PJ *newPj = pj_obj_create(ctx, NN_NO_CHECK(obj->iso_obj));
        ctx->forceOver = false;
        if (newPj) {
            newPj->copyStateFrom(*obj);
        }
        return newPj;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate an object from a WKT string, PROJ string, object code
 * (like "EPSG:4326", "urn:ogc:def:crs:EPSG::4326",
 * "urn:ogc:def:coordinateOperation:EPSG::1671"), a PROJJSON string, an object
 * name (e.g "WGS 84") of a compound CRS build from object names
 * (e.g "WGS 84 + EGM96 height")
 *
 * This function calls osgeo::proj::io::createFromUserInput()
 *
 * The returned object must be unreferenced with proj_destroy() after use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param text String (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL in
 * case of error.
 */
PJ *proj_create(PJ_CONTEXT *ctx, const char *text) {
    SANITIZE_CTX(ctx);
    if (!text) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }

    // Only connect to proj.db if needed
    if (strstr(text, "proj=") == nullptr || strstr(text, "init=") != nullptr) {
        getDBcontextNoException(ctx, __FUNCTION__);
    }
    try {
        auto obj =
            nn_dynamic_pointer_cast<BaseObject>(createFromUserInput(text, ctx));
        if (obj) {
            return pj_obj_create(ctx, NN_NO_CHECK(obj));
        }
    } catch (const io::ParsingException &e) {
        if (proj_context_errno(ctx) == 0) {
            proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_WRONG_SYNTAX);
        }
        proj_log_error(ctx, __FUNCTION__, e.what());
    } catch (const NoSuchAuthorityCodeException &e) {
        proj_log_error(ctx, __FUNCTION__,
                       std::string(e.what())
                           .append(": ")
                           .append(e.getAuthority())
                           .append(":")
                           .append(e.getAuthorityCode())
                           .c_str());
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate an object from a WKT string.
 *
 * This function calls osgeo::proj::io::WKTParser::createFromWKT()
 *
 * The returned object must be unreferenced with proj_destroy() after use.
 * It should be used by at most one thread at a time.
 *
 * The distinction between warnings and grammar errors is somewhat artificial
 * and does not tell much about the real criticity of the non-compliance.
 * Some warnings may be more concerning than some grammar errors. Human
 * expertise (or, by the time this comment will be read, specialized AI) is
 * generally needed to perform that assessment.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param wkt WKT string (must not be NULL)
 * @param options null-terminated list of options, or NULL. Currently
 * supported options are:
 * <ul>
 * <li>STRICT=YES/NO. Defaults to NO. When set to YES, strict validation will
 * be enabled.</li>
 * <li>UNSET_IDENTIFIERS_IF_INCOMPATIBLE_DEF=YES/NO. Defaults to YES.
 *     When set to YES, object identifiers are unset when there is
 *     a contradiction between the definition from WKT and the one from
 *     the database./<li>
 * </ul>
 * @param out_warnings Pointer to a PROJ_STRING_LIST object, or NULL.
 * If provided, *out_warnings will contain a list of warnings, typically for
 * non recognized projection method or parameters, or other issues found during
 * WKT analys. It must be freed with proj_string_list_destroy().
 * @param out_grammar_errors Pointer to a PROJ_STRING_LIST object, or NULL.
 * If provided, *out_grammar_errors will contain a list of errors regarding the
 * WKT grammar. It must be freed with proj_string_list_destroy().
 * @return Object that must be unreferenced with proj_destroy(), or NULL in
 * case of error.
 */
PJ *proj_create_from_wkt(PJ_CONTEXT *ctx, const char *wkt,
                         const char *const *options,
                         PROJ_STRING_LIST *out_warnings,
                         PROJ_STRING_LIST *out_grammar_errors) {
    SANITIZE_CTX(ctx);
    if (!wkt) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }

    if (out_warnings) {
        *out_warnings = nullptr;
    }
    if (out_grammar_errors) {
        *out_grammar_errors = nullptr;
    }

    try {
        WKTParser parser;
        auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
        if (dbContext) {
            parser.attachDatabaseContext(NN_NO_CHECK(dbContext));
        }
        parser.setStrict(false);
        for (auto iter = options; iter && iter[0]; ++iter) {
            const char *value;
            if ((value = getOptionValue(*iter, "STRICT="))) {
                parser.setStrict(ci_equal(value, "YES"));
            } else if ((value = getOptionValue(
                            *iter, "UNSET_IDENTIFIERS_IF_INCOMPATIBLE_DEF="))) {
                parser.setUnsetIdentifiersIfIncompatibleDef(
                    ci_equal(value, "YES"));
            } else {
                std::string msg("Unknown option :");
                msg += *iter;
                proj_log_error(ctx, __FUNCTION__, msg.c_str());
                return nullptr;
            }
        }
        auto obj = parser.createFromWKT(wkt);

        if (out_grammar_errors) {
            auto grammarErrors = parser.grammarErrorList();
            if (!grammarErrors.empty()) {
                *out_grammar_errors = to_string_list(grammarErrors);
            }
        }

        if (out_warnings) {
            auto warnings = parser.warningList();
            auto derivedCRS = dynamic_cast<const crs::DerivedCRS *>(obj.get());
            if (derivedCRS) {
                auto extraWarnings =
                    derivedCRS->derivingConversionRef()->validateParameters();
                warnings.insert(warnings.end(), extraWarnings.begin(),
                                extraWarnings.end());
            } else {
                auto singleOp =
                    dynamic_cast<const operation::SingleOperation *>(obj.get());
                if (singleOp) {
                    auto extraWarnings = singleOp->validateParameters();
                    warnings.insert(warnings.end(), extraWarnings.begin(),
                                    extraWarnings.end());
                }
            }
            if (!warnings.empty()) {
                *out_warnings = to_string_list(warnings);
            }
        }

        return pj_obj_create(ctx, NN_NO_CHECK(obj));
    } catch (const std::exception &e) {
        if (out_grammar_errors) {
            std::list<std::string> exc{e.what()};
            try {
                *out_grammar_errors = to_string_list(exc);
            } catch (const std::exception &) {
                proj_log_error(ctx, __FUNCTION__, e.what());
            }
        } else {
            proj_log_error(ctx, __FUNCTION__, e.what());
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate an object from a database lookup.
 *
 * The returned object must be unreferenced with proj_destroy() after use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx Context, or NULL for default context.
 * @param auth_name Authority name (must not be NULL)
 * @param code Object code (must not be NULL)
 * @param category Object category
 * @param usePROJAlternativeGridNames Whether PROJ alternative grid names
 * should be substituted to the official grid names. Only used on
 * transformations
 * @param options should be set to NULL for now
 * @return Object that must be unreferenced with proj_destroy(), or NULL in
 * case of error.
 */
PJ *proj_create_from_database(PJ_CONTEXT *ctx, const char *auth_name,
                              const char *code, PJ_CATEGORY category,
                              int usePROJAlternativeGridNames,
                              const char *const *options) {
    SANITIZE_CTX(ctx);
    if (!auth_name || !code) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    (void)options;
    try {
        const std::string codeStr(code);
        auto factory = AuthorityFactory::create(getDBcontext(ctx), auth_name);
        IdentifiedObjectPtr obj;
        switch (category) {
        case PJ_CATEGORY_ELLIPSOID:
            obj = factory->createEllipsoid(codeStr).as_nullable();
            break;
        case PJ_CATEGORY_PRIME_MERIDIAN:
            obj = factory->createPrimeMeridian(codeStr).as_nullable();
            break;
        case PJ_CATEGORY_DATUM:
            obj = factory->createDatum(codeStr).as_nullable();
            break;
        case PJ_CATEGORY_CRS:
            obj =
                factory->createCoordinateReferenceSystem(codeStr).as_nullable();
            break;
        case PJ_CATEGORY_COORDINATE_OPERATION:
            obj = factory
                      ->createCoordinateOperation(
                          codeStr, usePROJAlternativeGridNames != 0)
                      .as_nullable();
            break;
        case PJ_CATEGORY_DATUM_ENSEMBLE:
            obj = factory->createDatumEnsemble(codeStr).as_nullable();
            break;
        }
        return pj_obj_create(ctx, NN_NO_CHECK(obj));
    } catch (const NoSuchAuthorityCodeException &e) {
        proj_log_error(ctx, __FUNCTION__,
                       std::string(e.what())
                           .append(": ")
                           .append(e.getAuthority())
                           .append(":")
                           .append(e.getAuthorityCode())
                           .c_str());
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static const char *get_unit_category(const std::string &unit_name,
                                     UnitOfMeasure::Type type) {
    const char *ret = nullptr;
    switch (type) {
    case UnitOfMeasure::Type::UNKNOWN:
        ret = "unknown";
        break;
    case UnitOfMeasure::Type::NONE:
        ret = "none";
        break;
    case UnitOfMeasure::Type::ANGULAR:
        ret = unit_name.find(" per ") != std::string::npos ? "angular_per_time"
                                                           : "angular";
        break;
    case UnitOfMeasure::Type::LINEAR:
        ret = unit_name.find(" per ") != std::string::npos ? "linear_per_time"
                                                           : "linear";
        break;
    case UnitOfMeasure::Type::SCALE:
        ret = unit_name.find(" per year") != std::string::npos ||
                      unit_name.find(" per second") != std::string::npos
                  ? "scale_per_time"
                  : "scale";
        break;
    case UnitOfMeasure::Type::TIME:
        ret = "time";
        break;
    case UnitOfMeasure::Type::PARAMETRIC:
        ret = unit_name.find(" per ") != std::string::npos
                  ? "parametric_per_time"
                  : "parametric";
        break;
    }
    return ret;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Get information for a unit of measure from a database lookup.
 *
 * @param ctx Context, or NULL for default context.
 * @param auth_name Authority name (must not be NULL)
 * @param code Unit of measure code (must not be NULL)
 * @param out_name Pointer to a string value to store the parameter name. or
 * NULL. This value remains valid until the next call to
 * proj_uom_get_info_from_database() or the context destruction.
 * @param out_conv_factor Pointer to a value to store the conversion
 * factor of the prime meridian longitude unit to radian. or NULL
 * @param out_category Pointer to a string value to store the parameter name. or
 * NULL. This value might be "unknown", "none", "linear", "linear_per_time",
 * "angular", "angular_per_time", "scale", "scale_per_time", "time",
 * "parametric" or "parametric_per_time"
 * @return TRUE in case of success
 */
int proj_uom_get_info_from_database(PJ_CONTEXT *ctx, const char *auth_name,
                                    const char *code, const char **out_name,
                                    double *out_conv_factor,
                                    const char **out_category) {

    SANITIZE_CTX(ctx);
    if (!auth_name || !code) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    try {
        auto factory = AuthorityFactory::create(getDBcontext(ctx), auth_name);
        auto obj = factory->createUnitOfMeasure(code);
        if (out_name) {
            ctx->get_cpp_context()->lastUOMName_ = obj->name();
            *out_name = ctx->cpp_context->lastUOMName_.c_str();
        }
        if (out_conv_factor) {
            *out_conv_factor = obj->conversionToSI();
        }
        if (out_category) {
            *out_category = get_unit_category(obj->name(), obj->type());
        }
        return true;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return false;
}

// ---------------------------------------------------------------------------

/** \brief Get information for a grid from a database lookup.
 *
 * @param ctx Context, or NULL for default context.
 * @param grid_name Grid name (must not be NULL)
 * @param out_full_name Pointer to a string value to store the grid full
 * filename. or NULL
 * @param out_package_name Pointer to a string value to store the package name
 * where
 * the grid might be found. or NULL
 * @param out_url Pointer to a string value to store the grid URL or the
 * package URL where the grid might be found. or NULL
 * @param out_direct_download Pointer to a int (boolean) value to store whether
 * *out_url can be downloaded directly. or NULL
 * @param out_open_license Pointer to a int (boolean) value to store whether
 * the grid is released with an open license. or NULL
 * @param out_available Pointer to a int (boolean) value to store whether the
 * grid is available at runtime. or NULL
 * @return TRUE in case of success.
 */
int proj_grid_get_info_from_database(
    PJ_CONTEXT *ctx, const char *grid_name, const char **out_full_name,
    const char **out_package_name, const char **out_url,
    int *out_direct_download, int *out_open_license, int *out_available) {
    SANITIZE_CTX(ctx);
    if (!grid_name) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    try {
        auto db_context = getDBcontext(ctx);
        bool direct_download;
        bool open_license;
        bool available;
        if (!db_context->lookForGridInfo(
                grid_name, false, ctx->get_cpp_context()->lastGridFullName_,
                ctx->get_cpp_context()->lastGridPackageName_,
                ctx->get_cpp_context()->lastGridUrl_, direct_download,
                open_license, available)) {
            return false;
        }

        if (out_full_name)
            *out_full_name = ctx->get_cpp_context()->lastGridFullName_.c_str();
        if (out_package_name)
            *out_package_name =
                ctx->get_cpp_context()->lastGridPackageName_.c_str();
        if (out_url)
            *out_url = ctx->get_cpp_context()->lastGridUrl_.c_str();
        if (out_direct_download)
            *out_direct_download = direct_download ? 1 : 0;
        if (out_open_license)
            *out_open_license = open_license ? 1 : 0;
        if (out_available)
            *out_available = available ? 1 : 0;

        return true;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return false;
}

// ---------------------------------------------------------------------------

/** \brief Return GeodeticCRS that use the specified datum.
 *
 * @param ctx Context, or NULL for default context.
 * @param crs_auth_name CRS authority name, or NULL.
 * @param datum_auth_name Datum authority name (must not be NULL)
 * @param datum_code Datum code (must not be NULL)
 * @param crs_type "geographic 2D", "geographic 3D", "geocentric" or NULL
 * @return a result set that must be unreferenced with
 * proj_list_destroy(), or NULL in case of error.
 */
PJ_OBJ_LIST *proj_query_geodetic_crs_from_datum(PJ_CONTEXT *ctx,
                                                const char *crs_auth_name,
                                                const char *datum_auth_name,
                                                const char *datum_code,
                                                const char *crs_type) {
    SANITIZE_CTX(ctx);
    if (!datum_auth_name || !datum_code) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    try {
        auto factory = AuthorityFactory::create(
            getDBcontext(ctx), crs_auth_name ? crs_auth_name : "");
        auto res = factory->createGeodeticCRSFromDatum(
            datum_auth_name, datum_code, crs_type ? crs_type : "");
        std::vector<IdentifiedObjectNNPtr> objects;
        for (const auto &obj : res) {
            objects.push_back(obj);
        }
        return new PJ_OBJ_LIST(std::move(objects));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static AuthorityFactory::ObjectType
convertPJObjectTypeToObjectType(PJ_TYPE type, bool &valid) {
    valid = true;
    AuthorityFactory::ObjectType cppType = AuthorityFactory::ObjectType::CRS;
    switch (type) {
    case PJ_TYPE_ELLIPSOID:
        cppType = AuthorityFactory::ObjectType::ELLIPSOID;
        break;

    case PJ_TYPE_PRIME_MERIDIAN:
        cppType = AuthorityFactory::ObjectType::PRIME_MERIDIAN;
        break;

    case PJ_TYPE_GEODETIC_REFERENCE_FRAME:
        cppType = AuthorityFactory::ObjectType::GEODETIC_REFERENCE_FRAME;
        break;

    case PJ_TYPE_DYNAMIC_GEODETIC_REFERENCE_FRAME:
        cppType =
            AuthorityFactory::ObjectType::DYNAMIC_GEODETIC_REFERENCE_FRAME;
        break;

    case PJ_TYPE_VERTICAL_REFERENCE_FRAME:
        cppType = AuthorityFactory::ObjectType::VERTICAL_REFERENCE_FRAME;
        break;

    case PJ_TYPE_DYNAMIC_VERTICAL_REFERENCE_FRAME:
        cppType =
            AuthorityFactory::ObjectType::DYNAMIC_VERTICAL_REFERENCE_FRAME;
        break;

    case PJ_TYPE_DATUM_ENSEMBLE:
        cppType = AuthorityFactory::ObjectType::DATUM_ENSEMBLE;
        break;

    case PJ_TYPE_TEMPORAL_DATUM:
        valid = false;
        break;

    case PJ_TYPE_ENGINEERING_DATUM:
        cppType = AuthorityFactory::ObjectType::ENGINEERING_DATUM;
        break;

    case PJ_TYPE_PARAMETRIC_DATUM:
        valid = false;
        break;

    case PJ_TYPE_CRS:
        cppType = AuthorityFactory::ObjectType::CRS;
        break;

    case PJ_TYPE_GEODETIC_CRS:
        cppType = AuthorityFactory::ObjectType::GEODETIC_CRS;
        break;

    case PJ_TYPE_GEOCENTRIC_CRS:
        cppType = AuthorityFactory::ObjectType::GEOCENTRIC_CRS;
        break;

    case PJ_TYPE_GEOGRAPHIC_CRS:
        cppType = AuthorityFactory::ObjectType::GEOGRAPHIC_CRS;
        break;

    case PJ_TYPE_GEOGRAPHIC_2D_CRS:
        cppType = AuthorityFactory::ObjectType::GEOGRAPHIC_2D_CRS;
        break;

    case PJ_TYPE_GEOGRAPHIC_3D_CRS:
        cppType = AuthorityFactory::ObjectType::GEOGRAPHIC_3D_CRS;
        break;

    case PJ_TYPE_VERTICAL_CRS:
        cppType = AuthorityFactory::ObjectType::VERTICAL_CRS;
        break;

    case PJ_TYPE_PROJECTED_CRS:
        cppType = AuthorityFactory::ObjectType::PROJECTED_CRS;
        break;

    case PJ_TYPE_DERIVED_PROJECTED_CRS:
        valid = false;
        break;

    case PJ_TYPE_COMPOUND_CRS:
        cppType = AuthorityFactory::ObjectType::COMPOUND_CRS;
        break;

    case PJ_TYPE_ENGINEERING_CRS:
        cppType = AuthorityFactory::ObjectType::ENGINEERING_CRS;
        break;

    case PJ_TYPE_TEMPORAL_CRS:
        valid = false;
        break;

    case PJ_TYPE_BOUND_CRS:
        valid = false;
        break;

    case PJ_TYPE_OTHER_CRS:
        cppType = AuthorityFactory::ObjectType::CRS;
        break;

    case PJ_TYPE_CONVERSION:
        cppType = AuthorityFactory::ObjectType::CONVERSION;
        break;

    case PJ_TYPE_TRANSFORMATION:
        cppType = AuthorityFactory::ObjectType::TRANSFORMATION;
        break;

    case PJ_TYPE_CONCATENATED_OPERATION:
        cppType = AuthorityFactory::ObjectType::CONCATENATED_OPERATION;
        break;

    case PJ_TYPE_OTHER_COORDINATE_OPERATION:
        cppType = AuthorityFactory::ObjectType::COORDINATE_OPERATION;
        break;

    case PJ_TYPE_UNKNOWN:
        valid = false;
        break;

    case PJ_TYPE_COORDINATE_METADATA:
        valid = false;
        break;
    }
    return cppType;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return a list of objects by their name.
 *
 * @param ctx Context, or NULL for default context.
 * @param auth_name Authority name, used to restrict the search.
 * Or NULL for all authorities.
 * @param searchedName Searched name. Must be at least 2 character long.
 * @param types List of object types into which to search. If
 * NULL, all object types will be searched.
 * @param typesCount Number of elements in types, or 0 if types is NULL
 * @param approximateMatch Whether approximate name identification is allowed.
 * @param limitResultCount Maximum number of results to return.
 * Or 0 for unlimited.
 * @param options should be set to NULL for now
 * @return a result set that must be unreferenced with
 * proj_list_destroy(), or NULL in case of error.
 */
PJ_OBJ_LIST *proj_create_from_name(PJ_CONTEXT *ctx, const char *auth_name,
                                   const char *searchedName,
                                   const PJ_TYPE *types, size_t typesCount,
                                   int approximateMatch,
                                   size_t limitResultCount,
                                   const char *const *options) {
    SANITIZE_CTX(ctx);
    if (!searchedName || (types != nullptr && typesCount == 0) ||
        (types == nullptr && typesCount > 0)) {
        proj_log_error(ctx, __FUNCTION__, "invalid input");
        return nullptr;
    }
    (void)options;
    try {
        auto factory = AuthorityFactory::create(getDBcontext(ctx),
                                                auth_name ? auth_name : "");
        std::vector<AuthorityFactory::ObjectType> allowedTypes;
        for (size_t i = 0; i < typesCount; ++i) {
            bool valid = false;
            auto type = convertPJObjectTypeToObjectType(types[i], valid);
            if (valid) {
                allowedTypes.push_back(type);
            }
        }
        auto res = factory->createObjectsFromName(searchedName, allowedTypes,
                                                  approximateMatch != 0,
                                                  limitResultCount);
        std::vector<IdentifiedObjectNNPtr> objects;
        for (const auto &obj : res) {
            objects.push_back(obj);
        }
        return new PJ_OBJ_LIST(std::move(objects));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Return the type of an object.
 *
 * @param obj Object (must not be NULL)
 * @return its type.
 */
PJ_TYPE proj_get_type(const PJ *obj) {
    if (!obj || !obj->iso_obj) {
        return PJ_TYPE_UNKNOWN;
    }
    if (obj->type != PJ_TYPE_UNKNOWN)
        return obj->type;

    const auto getType = [&obj]() {
        auto ptr = obj->iso_obj.get();
        if (dynamic_cast<Ellipsoid *>(ptr)) {
            return PJ_TYPE_ELLIPSOID;
        }

        if (dynamic_cast<PrimeMeridian *>(ptr)) {
            return PJ_TYPE_PRIME_MERIDIAN;
        }

        if (dynamic_cast<DynamicGeodeticReferenceFrame *>(ptr)) {
            return PJ_TYPE_DYNAMIC_GEODETIC_REFERENCE_FRAME;
        }
        if (dynamic_cast<GeodeticReferenceFrame *>(ptr)) {
            return PJ_TYPE_GEODETIC_REFERENCE_FRAME;
        }
        if (dynamic_cast<DynamicVerticalReferenceFrame *>(ptr)) {
            return PJ_TYPE_DYNAMIC_VERTICAL_REFERENCE_FRAME;
        }
        if (dynamic_cast<VerticalReferenceFrame *>(ptr)) {
            return PJ_TYPE_VERTICAL_REFERENCE_FRAME;
        }
        if (dynamic_cast<DatumEnsemble *>(ptr)) {
            return PJ_TYPE_DATUM_ENSEMBLE;
        }
        if (dynamic_cast<TemporalDatum *>(ptr)) {
            return PJ_TYPE_TEMPORAL_DATUM;
        }
        if (dynamic_cast<EngineeringDatum *>(ptr)) {
            return PJ_TYPE_ENGINEERING_DATUM;
        }
        if (dynamic_cast<ParametricDatum *>(ptr)) {
            return PJ_TYPE_PARAMETRIC_DATUM;
        }

        if (auto crs = dynamic_cast<GeographicCRS *>(ptr)) {
            if (crs->coordinateSystem()->axisList().size() == 2) {
                return PJ_TYPE_GEOGRAPHIC_2D_CRS;
            } else {
                return PJ_TYPE_GEOGRAPHIC_3D_CRS;
            }
        }

        if (auto crs = dynamic_cast<GeodeticCRS *>(ptr)) {
            if (crs->isGeocentric()) {
                return PJ_TYPE_GEOCENTRIC_CRS;
            } else {
                return PJ_TYPE_GEODETIC_CRS;
            }
        }

        if (dynamic_cast<VerticalCRS *>(ptr)) {
            return PJ_TYPE_VERTICAL_CRS;
        }
        if (dynamic_cast<ProjectedCRS *>(ptr)) {
            return PJ_TYPE_PROJECTED_CRS;
        }
        if (dynamic_cast<DerivedProjectedCRS *>(ptr)) {
            return PJ_TYPE_DERIVED_PROJECTED_CRS;
        }
        if (dynamic_cast<CompoundCRS *>(ptr)) {
            return PJ_TYPE_COMPOUND_CRS;
        }
        if (dynamic_cast<TemporalCRS *>(ptr)) {
            return PJ_TYPE_TEMPORAL_CRS;
        }
        if (dynamic_cast<EngineeringCRS *>(ptr)) {
            return PJ_TYPE_ENGINEERING_CRS;
        }
        if (dynamic_cast<BoundCRS *>(ptr)) {
            return PJ_TYPE_BOUND_CRS;
        }
        if (dynamic_cast<CRS *>(ptr)) {
            return PJ_TYPE_OTHER_CRS;
        }

        if (dynamic_cast<Conversion *>(ptr)) {
            return PJ_TYPE_CONVERSION;
        }
        if (dynamic_cast<Transformation *>(ptr)) {
            return PJ_TYPE_TRANSFORMATION;
        }
        if (dynamic_cast<ConcatenatedOperation *>(ptr)) {
            return PJ_TYPE_CONCATENATED_OPERATION;
        }
        if (dynamic_cast<CoordinateOperation *>(ptr)) {
            return PJ_TYPE_OTHER_COORDINATE_OPERATION;
        }

        if (dynamic_cast<CoordinateMetadata *>(ptr)) {
            return PJ_TYPE_COORDINATE_METADATA;
        }

        return PJ_TYPE_UNKNOWN;
    };

    obj->type = getType();
    return obj->type;
}

// ---------------------------------------------------------------------------

/** \brief Return whether an object is deprecated.
 *
 * @param obj Object (must not be NULL)
 * @return TRUE if it is deprecated, FALSE otherwise
 */
int proj_is_deprecated(const PJ *obj) {
    if (!obj) {
        return false;
    }
    auto identifiedObj = dynamic_cast<IdentifiedObject *>(obj->iso_obj.get());
    if (!identifiedObj) {
        return false;
    }
    return identifiedObj->isDeprecated();
}

// ---------------------------------------------------------------------------

/** \brief Return a list of non-deprecated objects related to the passed one
 *
 * @param ctx Context, or NULL for default context.
 * @param obj Object (of type CRS for now) for which non-deprecated objects
 * must be searched. Must not be NULL
 * @return a result set that must be unreferenced with
 * proj_list_destroy(), or NULL in case of error.
 */
PJ_OBJ_LIST *proj_get_non_deprecated(PJ_CONTEXT *ctx, const PJ *obj) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto crs = dynamic_cast<const CRS *>(obj->iso_obj.get());
    if (!crs) {
        return nullptr;
    }
    try {
        std::vector<IdentifiedObjectNNPtr> objects;
        auto res = crs->getNonDeprecated(getDBcontext(ctx));
        for (const auto &resObj : res) {
            objects.push_back(resObj);
        }
        return new PJ_OBJ_LIST(std::move(objects));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

static int proj_is_equivalent_to_internal(PJ_CONTEXT *ctx, const PJ *obj,
                                          const PJ *other,
                                          PJ_COMPARISON_CRITERION criterion) {

    if (!obj || !other) {
        if (ctx) {
            proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
            proj_log_error(ctx, __FUNCTION__, "missing required input");
        }
        return false;
    }

    if (obj->iso_obj == nullptr && other->iso_obj == nullptr &&
        !obj->alternativeCoordinateOperations.empty() &&
        obj->alternativeCoordinateOperations.size() ==
            other->alternativeCoordinateOperations.size()) {
        for (size_t i = 0; i < obj->alternativeCoordinateOperations.size();
             ++i) {
            if (obj->alternativeCoordinateOperations[i] !=
                other->alternativeCoordinateOperations[i]) {
                return false;
            }
        }
        return true;
    }

    auto identifiedObj = dynamic_cast<IdentifiedObject *>(obj->iso_obj.get());
    if (!identifiedObj) {
        return false;
    }
    auto otherIdentifiedObj =
        dynamic_cast<IdentifiedObject *>(other->iso_obj.get());
    if (!otherIdentifiedObj) {
        return false;
    }
    const auto cppCriterion = ([](PJ_COMPARISON_CRITERION l_criterion) {
        switch (l_criterion) {
        case PJ_COMP_STRICT:
            return IComparable::Criterion::STRICT;
        case PJ_COMP_EQUIVALENT:
            return IComparable::Criterion::EQUIVALENT;
        case PJ_COMP_EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS:
            break;
        }
        return IComparable::Criterion::EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS;
    })(criterion);

    int res = identifiedObj->isEquivalentTo(
        otherIdentifiedObj, cppCriterion,
        ctx ? getDBcontextNoException(ctx, "proj_is_equivalent_to_with_ctx")
            : nullptr);
    return res;
}

// ---------------------------------------------------------------------------

/** \brief Return whether two objects are equivalent.
 *
 * Use proj_is_equivalent_to_with_ctx() to be able to use database information.
 *
 * @param obj Object (must not be NULL)
 * @param other Other object (must not be NULL)
 * @param criterion Comparison criterion
 * @return TRUE if they are equivalent
 */
int proj_is_equivalent_to(const PJ *obj, const PJ *other,
                          PJ_COMPARISON_CRITERION criterion) {
    return proj_is_equivalent_to_internal(nullptr, obj, other, criterion);
}

// ---------------------------------------------------------------------------

/** \brief Return whether two objects are equivalent
 *
 * Possibly using database to check for name aliases.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object (must not be NULL)
 * @param other Other object (must not be NULL)
 * @param criterion Comparison criterion
 * @return TRUE if they are equivalent
 * @since 6.3
 */
int proj_is_equivalent_to_with_ctx(PJ_CONTEXT *ctx, const PJ *obj,
                                   const PJ *other,
                                   PJ_COMPARISON_CRITERION criterion) {
    SANITIZE_CTX(ctx);
    return proj_is_equivalent_to_internal(ctx, obj, other, criterion);
}

// ---------------------------------------------------------------------------

/** \brief Return whether an object is a CRS
 *
 * @param obj Object (must not be NULL)
 */
int proj_is_crs(const PJ *obj) {
    if (!obj) {
        return false;
    }
    return dynamic_cast<CRS *>(obj->iso_obj.get()) != nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Get the name of an object.
 *
 * The lifetime of the returned string is the same as the input obj parameter.
 *
 * @param obj Object (must not be NULL)
 * @return a string, or NULL in case of error or missing name.
 */
const char *proj_get_name(const PJ *obj) {
    if (!obj) {
        return nullptr;
    }
    auto identifiedObj = dynamic_cast<IdentifiedObject *>(obj->iso_obj.get());
    if (!identifiedObj) {
        return nullptr;
    }
    const auto &desc = identifiedObj->name()->description();
    if (!desc.has_value()) {
        return nullptr;
    }
    // The object will still be alive after the function call.
    // cppcheck-suppress stlcstr
    return desc->c_str();
}

// ---------------------------------------------------------------------------

/** \brief Get the remarks of an object.
 *
 * The lifetime of the returned string is the same as the input obj parameter.
 *
 * @param obj Object (must not be NULL)
 * @return a string, or NULL in case of error.
 */
const char *proj_get_remarks(const PJ *obj) {
    if (!obj) {
        return nullptr;
    }
    auto identifiedObj = dynamic_cast<IdentifiedObject *>(obj->iso_obj.get());
    if (!identifiedObj) {
        return nullptr;
    }
    // The object will still be alive after the function call.
    // cppcheck-suppress stlcstr
    return identifiedObj->remarks().c_str();
}

// ---------------------------------------------------------------------------

/** \brief Get the authority name / codespace of an identifier of an object.
 *
 * The lifetime of the returned string is the same as the input obj parameter.
 *
 * @param obj Object (must not be NULL)
 * @param index Index of the identifier. 0 = first identifier
 * @return a string, or NULL in case of error or missing name.
 */
const char *proj_get_id_auth_name(const PJ *obj, int index) {
    if (!obj) {
        return nullptr;
    }
    auto identifiedObj = dynamic_cast<IdentifiedObject *>(obj->iso_obj.get());
    if (!identifiedObj) {
        return nullptr;
    }
    const auto &ids = identifiedObj->identifiers();
    if (static_cast<size_t>(index) >= ids.size()) {
        return nullptr;
    }
    const auto &codeSpace = ids[index]->codeSpace();
    if (!codeSpace.has_value()) {
        return nullptr;
    }
    // The object will still be alive after the function call.
    // cppcheck-suppress stlcstr
    return codeSpace->c_str();
}

// ---------------------------------------------------------------------------

/** \brief Get the code of an identifier of an object.
 *
 * The lifetime of the returned string is the same as the input obj parameter.
 *
 * @param obj Object (must not be NULL)
 * @param index Index of the identifier. 0 = first identifier
 * @return a string, or NULL in case of error or missing name.
 */
const char *proj_get_id_code(const PJ *obj, int index) {
    if (!obj) {
        return nullptr;
    }
    auto identifiedObj = dynamic_cast<IdentifiedObject *>(obj->iso_obj.get());
    if (!identifiedObj) {
        return nullptr;
    }
    const auto &ids = identifiedObj->identifiers();
    if (static_cast<size_t>(index) >= ids.size()) {
        return nullptr;
    }
    return ids[index]->code().c_str();
}

// ---------------------------------------------------------------------------

/** \brief Get a WKT representation of an object.
 *
 * The returned string is valid while the input obj parameter is valid,
 * and until a next call to proj_as_wkt() with the same input object.
 *
 * This function calls osgeo::proj::io::IWKTExportable::exportToWKT().
 *
 * This function may return NULL if the object is not compatible with an
 * export to the requested type.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object (must not be NULL)
 * @param type WKT version.
 * @param options null-terminated list of options, or NULL. Currently
 * supported options are:
 * <ul>
 * <li>MULTILINE=YES/NO. Defaults to YES, except for WKT1_ESRI</li>
 * <li>INDENTATION_WIDTH=number. Defaults to 4 (when multiline output is
 * on).</li>
 * <li>OUTPUT_AXIS=AUTO/YES/NO. In AUTO mode, axis will be output for WKT2
 * variants, for WKT1_GDAL for ProjectedCRS with easting/northing ordering
 * (otherwise stripped), but not for WKT1_ESRI. Setting to YES will output
 * them unconditionally, and to NO will omit them unconditionally.</li>
 * <li>STRICT=YES/NO. Default is YES. If NO, a Geographic 3D CRS can be for
 * example exported as WKT1_GDAL with 3 axes, whereas this is normally not
 * allowed.</li>
 * <li>ALLOW_ELLIPSOIDAL_HEIGHT_AS_VERTICAL_CRS=YES/NO. Default is NO. If set
 * to YES and type == PJ_WKT1_GDAL, a Geographic 3D CRS or a Projected 3D CRS
 * will be exported as a compound CRS whose vertical part represents an
 * ellipsoidal height (for example for use with LAS 1.4 WKT1).</li>
 * <li>ALLOW_LINUNIT_NODE=YES/NO. Default is YES starting with PROJ 9.1.
 * Only taken into account with type == PJ_WKT1_ESRI on a Geographic 3D
 * CRS.</li>
 * </ul>
 * @return a string, or NULL in case of error.
 */
const char *proj_as_wkt(PJ_CONTEXT *ctx, const PJ *obj, PJ_WKT_TYPE type,
                        const char *const *options) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto iWKTExportable = dynamic_cast<IWKTExportable *>(obj->iso_obj.get());
    if (!iWKTExportable) {
        return nullptr;
    }

    const auto convention = ([](PJ_WKT_TYPE l_type) {
        switch (l_type) {
        case PJ_WKT2_2015:
            return WKTFormatter::Convention::WKT2_2015;
        case PJ_WKT2_2015_SIMPLIFIED:
            return WKTFormatter::Convention::WKT2_2015_SIMPLIFIED;
        case PJ_WKT2_2019:
            return WKTFormatter::Convention::WKT2_2019;
        case PJ_WKT2_2019_SIMPLIFIED:
            return WKTFormatter::Convention::WKT2_2019_SIMPLIFIED;
        case PJ_WKT1_GDAL:
            return WKTFormatter::Convention::WKT1_GDAL;
        case PJ_WKT1_ESRI:
            break;
        }
        return WKTFormatter::Convention::WKT1_ESRI;
    })(type);

    try {
        auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
        auto formatter = WKTFormatter::create(convention, std::move(dbContext));
        for (auto iter = options; iter && iter[0]; ++iter) {
            const char *value;
            if ((value = getOptionValue(*iter, "MULTILINE="))) {
                formatter->setMultiLine(ci_equal(value, "YES"));
            } else if ((value = getOptionValue(*iter, "INDENTATION_WIDTH="))) {
                formatter->setIndentationWidth(std::atoi(value));
            } else if ((value = getOptionValue(*iter, "OUTPUT_AXIS="))) {
                if (!ci_equal(value, "AUTO")) {
                    formatter->setOutputAxis(
                        ci_equal(value, "YES")
                            ? WKTFormatter::OutputAxisRule::YES
                            : WKTFormatter::OutputAxisRule::NO);
                }
            } else if ((value = getOptionValue(*iter, "STRICT="))) {
                formatter->setStrict(ci_equal(value, "YES"));
            } else if ((value = getOptionValue(
                            *iter,
                            "ALLOW_ELLIPSOIDAL_HEIGHT_AS_VERTICAL_CRS="))) {
                formatter->setAllowEllipsoidalHeightAsVerticalCRS(
                    ci_equal(value, "YES"));
            } else if ((value = getOptionValue(*iter, "ALLOW_LINUNIT_NODE="))) {
                formatter->setAllowLINUNITNode(ci_equal(value, "YES"));
            } else {
                std::string msg("Unknown option :");
                msg += *iter;
                proj_log_error(ctx, __FUNCTION__, msg.c_str());
                return nullptr;
            }
        }
        obj->lastWKT = iWKTExportable->exportToWKT(formatter.get());
        return obj->lastWKT.c_str();
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Get a PROJ string representation of an object.
 *
 * The returned string is valid while the input obj parameter is valid,
 * and until a next call to proj_as_proj_string() with the same input
 * object.
 *
 * \warning If a CRS object was not created from a PROJ string,
 *          exporting to a PROJ string will in most cases
 *          cause a loss of information. This can potentially lead to
 *          erroneous transformations.
 *
 * This function calls
 * osgeo::proj::io::IPROJStringExportable::exportToPROJString().
 *
 * This function may return NULL if the object is not compatible with an
 * export to the requested type.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object (must not be NULL)
 * @param type PROJ String version.
 * @param options NULL-terminated list of strings with "KEY=VALUE" format. or
 * NULL. Currently supported options are:
 * <ul>
 * <li>USE_APPROX_TMERC=YES to add the +approx flag to +proj=tmerc or
 * +proj=utm.</li>
 * <li>MULTILINE=YES/NO. Defaults to NO</li>
 * <li>INDENTATION_WIDTH=number. Defaults to 2 (when multiline output is
 * on).</li>
 * <li>MAX_LINE_LENGTH=number. Defaults to 80 (when multiline output is
 * on).</li>
 * </ul>
 * @return a string, or NULL in case of error.
 */
const char *proj_as_proj_string(PJ_CONTEXT *ctx, const PJ *obj,
                                PJ_PROJ_STRING_TYPE type,
                                const char *const *options) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto exportable =
        dynamic_cast<const IPROJStringExportable *>(obj->iso_obj.get());
    if (!exportable) {
        proj_log_error(ctx, __FUNCTION__, "Object type not exportable to PROJ");
        return nullptr;
    }
    // Make sure that the C and C++ enumeration match
    static_assert(static_cast<int>(PJ_PROJ_5) ==
                      static_cast<int>(PROJStringFormatter::Convention::PROJ_5),
                  "");
    static_assert(static_cast<int>(PJ_PROJ_4) ==
                      static_cast<int>(PROJStringFormatter::Convention::PROJ_4),
                  "");
    // Make sure we enumerate all values. If adding a new value, as we
    // don't have a default clause, the compiler will warn.
    switch (type) {
    case PJ_PROJ_5:
    case PJ_PROJ_4:
        break;
    }
    const PROJStringFormatter::Convention convention =
        static_cast<PROJStringFormatter::Convention>(type);
    auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
    try {
        auto formatter =
            PROJStringFormatter::create(convention, std::move(dbContext));
        for (auto iter = options; iter && iter[0]; ++iter) {
            const char *value;
            if ((value = getOptionValue(*iter, "MULTILINE="))) {
                formatter->setMultiLine(ci_equal(value, "YES"));
            } else if ((value = getOptionValue(*iter, "INDENTATION_WIDTH="))) {
                formatter->setIndentationWidth(std::atoi(value));
            } else if ((value = getOptionValue(*iter, "MAX_LINE_LENGTH="))) {
                formatter->setMaxLineLength(std::atoi(value));
            } else if ((value = getOptionValue(*iter, "USE_APPROX_TMERC="))) {
                formatter->setUseApproxTMerc(ci_equal(value, "YES"));
            } else {
                std::string msg("Unknown option :");
                msg += *iter;
                proj_log_error(ctx, __FUNCTION__, msg.c_str());
                return nullptr;
            }
        }
        obj->lastPROJString = exportable->exportToPROJString(formatter.get());
        return obj->lastPROJString.c_str();
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Get a PROJJSON string representation of an object.
 *
 * The returned string is valid while the input obj parameter is valid,
 * and until a next call to proj_as_proj_string() with the same input
 * object.
 *
 * This function calls
 * osgeo::proj::io::IJSONExportable::exportToJSON().
 *
 * This function may return NULL if the object is not compatible with an
 * export to the requested type.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object (must not be NULL)
 * @param options NULL-terminated list of strings with "KEY=VALUE" format. or
 * NULL. Currently
 * supported options are:
 * <ul>
 * <li>MULTILINE=YES/NO. Defaults to YES</li>
 * <li>INDENTATION_WIDTH=number. Defaults to 2 (when multiline output is
 * on).</li>
 * <li>SCHEMA=string. URL to PROJJSON schema. Can be set to empty string to
 * disable it.</li>
 * </ul>
 * @return a string, or NULL in case of error.
 *
 * @since 6.2
 */
const char *proj_as_projjson(PJ_CONTEXT *ctx, const PJ *obj,
                             const char *const *options) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto exportable = dynamic_cast<const IJSONExportable *>(obj->iso_obj.get());
    if (!exportable) {
        proj_log_error(ctx, __FUNCTION__, "Object type not exportable to JSON");
        return nullptr;
    }

    auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
    try {
        auto formatter = JSONFormatter::create(std::move(dbContext));
        for (auto iter = options; iter && iter[0]; ++iter) {
            const char *value;
            if ((value = getOptionValue(*iter, "MULTILINE="))) {
                formatter->setMultiLine(ci_equal(value, "YES"));
            } else if ((value = getOptionValue(*iter, "INDENTATION_WIDTH="))) {
                formatter->setIndentationWidth(std::atoi(value));
            } else if ((value = getOptionValue(*iter, "SCHEMA="))) {
                formatter->setSchema(value);
            } else {
                std::string msg("Unknown option :");
                msg += *iter;
                proj_log_error(ctx, __FUNCTION__, msg.c_str());
                return nullptr;
            }
        }
        obj->lastJSONString = exportable->exportToJSON(formatter.get());
        return obj->lastJSONString.c_str();
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Get the number of domains/usages for a given object.
 *
 * Most objects have a single domain/usage, but for some of them, there might
 * be multiple.
 *
 * @param obj Object (must not be NULL)
 * @return the number of domains, or 0 in case of error.
 * @since 9.2
 */
int proj_get_domain_count(const PJ *obj) {
    if (!obj || !obj->iso_obj) {
        return 0;
    }
    auto objectUsage = dynamic_cast<const ObjectUsage *>(obj->iso_obj.get());
    if (!objectUsage) {
        return 0;
    }
    const auto &domains = objectUsage->domains();
    return static_cast<int>(domains.size());
}

// ---------------------------------------------------------------------------

/** \brief Get the scope of an object.
 *
 * In case of multiple usages, this will be the one of first usage.
 *
 * The lifetime of the returned string is the same as the input obj parameter.
 *
 * @param obj Object (must not be NULL)
 * @return a string, or NULL in case of error or missing scope.
 */
const char *proj_get_scope(const PJ *obj) { return proj_get_scope_ex(obj, 0); }

// ---------------------------------------------------------------------------

/** \brief Get the scope of an object.
 *
 * The lifetime of the returned string is the same as the input obj parameter.
 *
 * @param obj Object (must not be NULL)
 * @param domainIdx Index of the domain/usage. In [0,proj_get_domain_count(obj)[
 * @return a string, or NULL in case of error or missing scope.
 * @since 9.2
 */
const char *proj_get_scope_ex(const PJ *obj, int domainIdx) {
    if (!obj || !obj->iso_obj) {
        return nullptr;
    }
    auto objectUsage = dynamic_cast<const ObjectUsage *>(obj->iso_obj.get());
    if (!objectUsage) {
        return nullptr;
    }
    const auto &domains = objectUsage->domains();
    if (domainIdx < 0 || static_cast<size_t>(domainIdx) >= domains.size()) {
        return nullptr;
    }
    const auto &scope = domains[domainIdx]->scope();
    if (!scope.has_value()) {
        return nullptr;
    }
    // The object will still be alive after the function call.
    // cppcheck-suppress stlcstr
    return scope->c_str();
}

// ---------------------------------------------------------------------------

/** \brief Return the area of use of an object.
 *
 * In case of multiple usages, this will be the one of first usage.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object (must not be NULL)
 * @param out_west_lon_degree Pointer to a double to receive the west longitude
 * (in degrees). Or NULL. If the returned value is -1000, the bounding box is
 * unknown.
 * @param out_south_lat_degree Pointer to a double to receive the south latitude
 * (in degrees). Or NULL. If the returned value is -1000, the bounding box is
 * unknown.
 * @param out_east_lon_degree Pointer to a double to receive the east longitude
 * (in degrees). Or NULL. If the returned value is -1000, the bounding box is
 * unknown.
 * @param out_north_lat_degree Pointer to a double to receive the north latitude
 * (in degrees). Or NULL. If the returned value is -1000, the bounding box is
 * unknown.
 * @param out_area_name Pointer to a string to receive the name of the area of
 * use. Or NULL. *p_area_name is valid while obj is valid itself.
 * @return TRUE in case of success, FALSE in case of error or if the area
 * of use is unknown.
 */
int proj_get_area_of_use(PJ_CONTEXT *ctx, const PJ *obj,
                         double *out_west_lon_degree,
                         double *out_south_lat_degree,
                         double *out_east_lon_degree,
                         double *out_north_lat_degree,
                         const char **out_area_name) {
    return proj_get_area_of_use_ex(ctx, obj, 0, out_west_lon_degree,
                                   out_south_lat_degree, out_east_lon_degree,
                                   out_north_lat_degree, out_area_name);
}

// ---------------------------------------------------------------------------

/** \brief Return the area of use of an object.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object (must not be NULL)
 * @param domainIdx Index of the domain/usage. In [0,proj_get_domain_count(obj)[
 * @param out_west_lon_degree Pointer to a double to receive the west longitude
 * (in degrees). Or NULL. If the returned value is -1000, the bounding box is
 * unknown.
 * @param out_south_lat_degree Pointer to a double to receive the south latitude
 * (in degrees). Or NULL. If the returned value is -1000, the bounding box is
 * unknown.
 * @param out_east_lon_degree Pointer to a double to receive the east longitude
 * (in degrees). Or NULL. If the returned value is -1000, the bounding box is
 * unknown.
 * @param out_north_lat_degree Pointer to a double to receive the north latitude
 * (in degrees). Or NULL. If the returned value is -1000, the bounding box is
 * unknown.
 * @param out_area_name Pointer to a string to receive the name of the area of
 * use. Or NULL. *p_area_name is valid while obj is valid itself.
 * @return TRUE in case of success, FALSE in case of error or if the area
 * of use is unknown.
 */
int proj_get_area_of_use_ex(PJ_CONTEXT *ctx, const PJ *obj, int domainIdx,
                            double *out_west_lon_degree,
                            double *out_south_lat_degree,
                            double *out_east_lon_degree,
                            double *out_north_lat_degree,
                            const char **out_area_name) {
    (void)ctx;
    if (out_area_name) {
        *out_area_name = nullptr;
    }
    auto objectUsage = dynamic_cast<const ObjectUsage *>(obj->iso_obj.get());
    if (!objectUsage) {
        return false;
    }
    const auto &domains = objectUsage->domains();
    if (domainIdx < 0 || static_cast<size_t>(domainIdx) >= domains.size()) {
        return false;
    }
    const auto &extent = domains[domainIdx]->domainOfValidity();
    if (!extent) {
        return false;
    }
    const auto &desc = extent->description();
    if (desc.has_value() && out_area_name) {
        *out_area_name = desc->c_str();
    }

    const auto &geogElements = extent->geographicElements();
    if (!geogElements.empty()) {
        auto bbox =
            dynamic_cast<const GeographicBoundingBox *>(geogElements[0].get());
        if (bbox) {
            if (out_west_lon_degree) {
                *out_west_lon_degree = bbox->westBoundLongitude();
            }
            if (out_south_lat_degree) {
                *out_south_lat_degree = bbox->southBoundLatitude();
            }
            if (out_east_lon_degree) {
                *out_east_lon_degree = bbox->eastBoundLongitude();
            }
            if (out_north_lat_degree) {
                *out_north_lat_degree = bbox->northBoundLatitude();
            }
            return true;
        }
    }
    if (out_west_lon_degree) {
        *out_west_lon_degree = -1000;
    }
    if (out_south_lat_degree) {
        *out_south_lat_degree = -1000;
    }
    if (out_east_lon_degree) {
        *out_east_lon_degree = -1000;
    }
    if (out_north_lat_degree) {
        *out_north_lat_degree = -1000;
    }
    return true;
}

// ---------------------------------------------------------------------------

static const GeodeticCRS *extractGeodeticCRS(PJ_CONTEXT *ctx, const PJ *crs,
                                             const char *fname) {
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, fname, "missing required input");
        return nullptr;
    }
    auto l_crs = dynamic_cast<const CRS *>(crs->iso_obj.get());
    if (!l_crs) {
        proj_log_error(ctx, fname, "Object is not a CRS");
        return nullptr;
    }
    auto geodCRS = l_crs->extractGeodeticCRSRaw();
    if (!geodCRS) {
        proj_log_error(ctx, fname, "CRS has no geodetic CRS");
    }
    return geodCRS;
}

// ---------------------------------------------------------------------------

/** \brief Get the geodeticCRS / geographicCRS from a CRS
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type CRS (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 */
PJ *proj_crs_get_geodetic_crs(PJ_CONTEXT *ctx, const PJ *crs) {
    SANITIZE_CTX(ctx);
    auto geodCRS = extractGeodeticCRS(ctx, crs, __FUNCTION__);
    if (!geodCRS) {
        return nullptr;
    }
    return pj_obj_create(ctx,
                         NN_NO_CHECK(nn_dynamic_pointer_cast<IdentifiedObject>(
                             geodCRS->shared_from_this())));
}

// ---------------------------------------------------------------------------

/** \brief Returns whether a CRS is a derived CRS.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type CRS (must not be NULL)
 * @return TRUE if the CRS is a derived CRS.
 * @since 8.0
 */
int proj_crs_is_derived(PJ_CONTEXT *ctx, const PJ *crs) {
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto l_crs = dynamic_cast<const CRS *>(crs->iso_obj.get());
    if (!l_crs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a CRS");
        return false;
    }
    return dynamic_cast<const DerivedCRS *>(l_crs) != nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Get a CRS component from a CompoundCRS
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type CRS (must not be NULL)
 * @param index Index of the CRS component (typically 0 = horizontal, 1 =
 * vertical)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 */
PJ *proj_crs_get_sub_crs(PJ_CONTEXT *ctx, const PJ *crs, int index) {
    SANITIZE_CTX(ctx);
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_crs = dynamic_cast<CompoundCRS *>(crs->iso_obj.get());
    if (!l_crs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a CompoundCRS");
        return nullptr;
    }
    const auto &components = l_crs->componentReferenceSystems();
    if (static_cast<size_t>(index) >= components.size()) {
        return nullptr;
    }
    return pj_obj_create(ctx, components[index]);
}

// ---------------------------------------------------------------------------

/** \brief Returns a BoundCRS
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param base_crs Base CRS (must not be NULL)
 * @param hub_crs Hub CRS (must not be NULL)
 * @param transformation Transformation (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 */
PJ *proj_crs_create_bound_crs(PJ_CONTEXT *ctx, const PJ *base_crs,
                              const PJ *hub_crs, const PJ *transformation) {
    SANITIZE_CTX(ctx);
    if (!base_crs || !hub_crs || !transformation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_base_crs = std::dynamic_pointer_cast<CRS>(base_crs->iso_obj);
    if (!l_base_crs) {
        proj_log_error(ctx, __FUNCTION__, "base_crs is not a CRS");
        return nullptr;
    }
    auto l_hub_crs = std::dynamic_pointer_cast<CRS>(hub_crs->iso_obj);
    if (!l_hub_crs) {
        proj_log_error(ctx, __FUNCTION__, "hub_crs is not a CRS");
        return nullptr;
    }
    auto l_transformation =
        std::dynamic_pointer_cast<Transformation>(transformation->iso_obj);
    if (!l_transformation) {
        proj_log_error(ctx, __FUNCTION__, "transformation is not a CRS");
        return nullptr;
    }
    try {
        return pj_obj_create(ctx,
                             BoundCRS::create(NN_NO_CHECK(l_base_crs),
                                              NN_NO_CHECK(l_hub_crs),
                                              NN_NO_CHECK(l_transformation)));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns potentially
 * a BoundCRS, with a transformation to EPSG:4326, wrapping this CRS
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * This is the same as method
 * osgeo::proj::crs::CRS::createBoundCRSToWGS84IfPossible()
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type CRS (must not be NULL)
 * @param options null-terminated list of options, or NULL. Currently
 * supported options are:
 * <ul>
 * <li>ALLOW_INTERMEDIATE_CRS=ALWAYS/IF_NO_DIRECT_TRANSFORMATION/NEVER. Defaults
 * to NEVER. When set to ALWAYS/IF_NO_DIRECT_TRANSFORMATION,
 * intermediate CRS may be considered when computing the possible
 * transformations. Slower.</li>
 * </ul>
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 */
PJ *proj_crs_create_bound_crs_to_WGS84(PJ_CONTEXT *ctx, const PJ *crs,
                                       const char *const *options) {
    SANITIZE_CTX(ctx);
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_crs = dynamic_cast<const CRS *>(crs->iso_obj.get());
    if (!l_crs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a CRS");
        return nullptr;
    }
    auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
    try {
        CoordinateOperationContext::IntermediateCRSUse allowIntermediateCRS =
            CoordinateOperationContext::IntermediateCRSUse::NEVER;
        for (auto iter = options; iter && iter[0]; ++iter) {
            const char *value;
            if ((value = getOptionValue(*iter, "ALLOW_INTERMEDIATE_CRS="))) {
                if (ci_equal(value, "YES") || ci_equal(value, "ALWAYS")) {
                    allowIntermediateCRS =
                        CoordinateOperationContext::IntermediateCRSUse::ALWAYS;
                } else if (ci_equal(value, "IF_NO_DIRECT_TRANSFORMATION")) {
                    allowIntermediateCRS = CoordinateOperationContext::
                        IntermediateCRSUse::IF_NO_DIRECT_TRANSFORMATION;
                }
            } else {
                std::string msg("Unknown option :");
                msg += *iter;
                proj_log_error(ctx, __FUNCTION__, msg.c_str());
                return nullptr;
            }
        }
        return pj_obj_create(ctx, l_crs->createBoundCRSToWGS84IfPossible(
                                      dbContext, allowIntermediateCRS));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a BoundCRS, with a transformation to a hub geographic 3D crs
 * (use EPSG:4979 for WGS84 for example), using a grid.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param vert_crs Object of type VerticalCRS (must not be NULL)
 * @param hub_geographic_3D_crs Object of type Geographic 3D CRS (must not be
 * NULL)
 * @param grid_name Grid name (typically a .gtx file)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 * @since 6.3
 */
PJ *proj_crs_create_bound_vertical_crs(PJ_CONTEXT *ctx, const PJ *vert_crs,
                                       const PJ *hub_geographic_3D_crs,
                                       const char *grid_name) {
    SANITIZE_CTX(ctx);
    if (!vert_crs || !hub_geographic_3D_crs || !grid_name) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_crs = std::dynamic_pointer_cast<VerticalCRS>(vert_crs->iso_obj);
    if (!l_crs) {
        proj_log_error(ctx, __FUNCTION__, "vert_crs is not a VerticalCRS");
        return nullptr;
    }
    auto hub_crs =
        std::dynamic_pointer_cast<CRS>(hub_geographic_3D_crs->iso_obj);
    if (!hub_crs) {
        proj_log_error(ctx, __FUNCTION__, "hub_geographic_3D_crs is not a CRS");
        return nullptr;
    }
    try {
        auto nnCRS = NN_NO_CHECK(l_crs);
        auto nnHubCRS = NN_NO_CHECK(hub_crs);
        auto transformation =
            Transformation::createGravityRelatedHeightToGeographic3D(
                PropertyMap().set(IdentifiedObject::NAME_KEY,
                                  "unknown to " + hub_crs->nameStr() +
                                      " ellipsoidal height"),
                nnCRS, nnHubCRS, nullptr, std::string(grid_name),
                std::vector<PositionalAccuracyNNPtr>());
        return pj_obj_create(ctx,
                             BoundCRS::create(nnCRS, nnHubCRS, transformation));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Get the ellipsoid from a CRS or a GeodeticReferenceFrame.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS or GeodeticReferenceFrame (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 */
PJ *proj_get_ellipsoid(PJ_CONTEXT *ctx, const PJ *obj) {
    SANITIZE_CTX(ctx);
    auto ptr = obj->iso_obj.get();
    if (dynamic_cast<const CRS *>(ptr)) {
        auto geodCRS = extractGeodeticCRS(ctx, obj, __FUNCTION__);
        if (geodCRS) {
            return pj_obj_create(ctx, geodCRS->ellipsoid());
        }
    } else {
        auto datum = dynamic_cast<const GeodeticReferenceFrame *>(ptr);
        if (datum) {
            return pj_obj_create(ctx, datum->ellipsoid());
        }
    }
    proj_log_error(ctx, __FUNCTION__,
                   "Object is not a CRS or GeodeticReferenceFrame");
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Get the name of the celestial body of this object.
 *
 * Object should be a CRS, Datum or Ellipsoid.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS, Datum or Ellipsoid.(must not be NULL)
 * @return the name of the celestial body, or NULL.
 * @since 8.1
 */
const char *proj_get_celestial_body_name(PJ_CONTEXT *ctx, const PJ *obj) {
    SANITIZE_CTX(ctx);
    const BaseObject *ptr = obj->iso_obj.get();
    if (dynamic_cast<const CRS *>(ptr)) {
        const auto geodCRS = extractGeodeticCRS(ctx, obj, __FUNCTION__);
        if (!geodCRS) {
            // FIXME when vertical CRS can be non-EARTH...
            return datum::Ellipsoid::EARTH.c_str();
        }
        return geodCRS->ellipsoid()->celestialBody().c_str();
    }
    const auto ensemble = dynamic_cast<const DatumEnsemble *>(ptr);
    if (ensemble) {
        ptr = ensemble->datums().front().get();
        // Go on
    }
    const auto geodetic_datum =
        dynamic_cast<const GeodeticReferenceFrame *>(ptr);
    if (geodetic_datum) {
        return geodetic_datum->ellipsoid()->celestialBody().c_str();
    }
    const auto vertical_datum =
        dynamic_cast<const VerticalReferenceFrame *>(ptr);
    if (vertical_datum) {
        // FIXME when vertical CRS can be non-EARTH...
        return datum::Ellipsoid::EARTH.c_str();
    }
    const auto ellipsoid = dynamic_cast<const Ellipsoid *>(ptr);
    if (ellipsoid) {
        return ellipsoid->celestialBody().c_str();
    }
    proj_log_error(ctx, __FUNCTION__,
                   "Object is not a CRS, Datum or Ellipsoid");
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Get the horizontal datum from a CRS
 *
 * This function may return a Datum or DatumEnsemble object.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type CRS (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 */
PJ *proj_crs_get_horizontal_datum(PJ_CONTEXT *ctx, const PJ *crs) {
    SANITIZE_CTX(ctx);
    auto geodCRS = extractGeodeticCRS(ctx, crs, __FUNCTION__);
    if (!geodCRS) {
        return nullptr;
    }
    const auto &datum = geodCRS->datum();
    if (datum) {
        return pj_obj_create(ctx, NN_NO_CHECK(datum));
    }

    const auto &datumEnsemble = geodCRS->datumEnsemble();
    if (datumEnsemble) {
        return pj_obj_create(ctx, NN_NO_CHECK(datumEnsemble));
    }
    proj_log_error(ctx, __FUNCTION__, "CRS has no datum");
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Return ellipsoid parameters.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param ellipsoid Object of type Ellipsoid (must not be NULL)
 * @param out_semi_major_metre Pointer to a value to store the semi-major axis
 * in
 * metre. or NULL
 * @param out_semi_minor_metre Pointer to a value to store the semi-minor axis
 * in
 * metre. or NULL
 * @param out_is_semi_minor_computed Pointer to a boolean value to indicate if
 * the
 * semi-minor value was computed. If FALSE, its value comes from the
 * definition. or NULL
 * @param out_inv_flattening Pointer to a value to store the inverse
 * flattening. or NULL
 * @return TRUE in case of success.
 */
int proj_ellipsoid_get_parameters(PJ_CONTEXT *ctx, const PJ *ellipsoid,
                                  double *out_semi_major_metre,
                                  double *out_semi_minor_metre,
                                  int *out_is_semi_minor_computed,
                                  double *out_inv_flattening) {
    SANITIZE_CTX(ctx);
    if (!ellipsoid) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return FALSE;
    }
    auto l_ellipsoid =
        dynamic_cast<const Ellipsoid *>(ellipsoid->iso_obj.get());
    if (!l_ellipsoid) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a Ellipsoid");
        return FALSE;
    }

    if (out_semi_major_metre) {
        *out_semi_major_metre = l_ellipsoid->semiMajorAxis().getSIValue();
    }
    if (out_semi_minor_metre) {
        *out_semi_minor_metre =
            l_ellipsoid->computeSemiMinorAxis().getSIValue();
    }
    if (out_is_semi_minor_computed) {
        *out_is_semi_minor_computed =
            !(l_ellipsoid->semiMinorAxis().has_value());
    }
    if (out_inv_flattening) {
        *out_inv_flattening = l_ellipsoid->computedInverseFlattening();
    }
    return TRUE;
}

// ---------------------------------------------------------------------------

/** \brief Get the prime meridian of a CRS or a GeodeticReferenceFrame.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS or GeodeticReferenceFrame (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 */

PJ *proj_get_prime_meridian(PJ_CONTEXT *ctx, const PJ *obj) {
    SANITIZE_CTX(ctx);
    auto ptr = obj->iso_obj.get();
    if (dynamic_cast<CRS *>(ptr)) {
        auto geodCRS = extractGeodeticCRS(ctx, obj, __FUNCTION__);
        if (geodCRS) {
            return pj_obj_create(ctx, geodCRS->primeMeridian());
        }
    } else {
        auto datum = dynamic_cast<const GeodeticReferenceFrame *>(ptr);
        if (datum) {
            return pj_obj_create(ctx, datum->primeMeridian());
        }
    }
    proj_log_error(ctx, __FUNCTION__,
                   "Object is not a CRS or GeodeticReferenceFrame");
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Return prime meridian parameters.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param prime_meridian Object of type PrimeMeridian (must not be NULL)
 * @param out_longitude Pointer to a value to store the longitude of the prime
 * meridian, in its native unit. or NULL
 * @param out_unit_conv_factor Pointer to a value to store the conversion
 * factor of the prime meridian longitude unit to radian. or NULL
 * @param out_unit_name Pointer to a string value to store the unit name.
 * or NULL
 * @return TRUE in case of success.
 */
int proj_prime_meridian_get_parameters(PJ_CONTEXT *ctx,
                                       const PJ *prime_meridian,
                                       double *out_longitude,
                                       double *out_unit_conv_factor,
                                       const char **out_unit_name) {
    SANITIZE_CTX(ctx);
    if (!prime_meridian) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto l_pm =
        dynamic_cast<const PrimeMeridian *>(prime_meridian->iso_obj.get());
    if (!l_pm) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a PrimeMeridian");
        return false;
    }
    const auto &longitude = l_pm->longitude();
    if (out_longitude) {
        *out_longitude = longitude.value();
    }
    const auto &unit = longitude.unit();
    if (out_unit_conv_factor) {
        *out_unit_conv_factor = unit.conversionToSI();
    }
    if (out_unit_name) {
        *out_unit_name = unit.name().c_str();
    }
    return true;
}

// ---------------------------------------------------------------------------

/** \brief Return the base CRS of a BoundCRS or a DerivedCRS/ProjectedCRS, or
 * the source CRS of a CoordinateOperation, or the CRS of a CoordinateMetadata.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type BoundCRS or CoordinateOperation (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error, or missing source CRS.
 */
PJ *proj_get_source_crs(PJ_CONTEXT *ctx, const PJ *obj) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        return nullptr;
    }
    auto ptr = obj->iso_obj.get();
    auto boundCRS = dynamic_cast<const BoundCRS *>(ptr);
    if (boundCRS) {
        return pj_obj_create(ctx, boundCRS->baseCRS());
    }
    auto derivedCRS = dynamic_cast<const DerivedCRS *>(ptr);
    if (derivedCRS) {
        return pj_obj_create(ctx, derivedCRS->baseCRS());
    }
    auto co = dynamic_cast<const CoordinateOperation *>(ptr);
    if (co) {
        auto sourceCRS = co->sourceCRS();
        if (sourceCRS) {
            return pj_obj_create(ctx, NN_NO_CHECK(sourceCRS));
        }
        return nullptr;
    }
    if (!obj->alternativeCoordinateOperations.empty()) {
        return proj_get_source_crs(ctx,
                                   obj->alternativeCoordinateOperations[0].pj);
    }
    auto coordinateMetadata = dynamic_cast<const CoordinateMetadata *>(ptr);
    if (coordinateMetadata) {
        return pj_obj_create(ctx, coordinateMetadata->crs());
    }

    proj_log_error(ctx, __FUNCTION__,
                   "Object is not a BoundCRS, a CoordinateOperation or a "
                   "CoordinateMetadata");
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Return the hub CRS of a BoundCRS or the target CRS of a
 * CoordinateOperation.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type BoundCRS or CoordinateOperation (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error, or missing target CRS.
 */
PJ *proj_get_target_crs(PJ_CONTEXT *ctx, const PJ *obj) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto ptr = obj->iso_obj.get();
    auto boundCRS = dynamic_cast<const BoundCRS *>(ptr);
    if (boundCRS) {
        return pj_obj_create(ctx, boundCRS->hubCRS());
    }
    auto co = dynamic_cast<const CoordinateOperation *>(ptr);
    if (co) {
        auto targetCRS = co->targetCRS();
        if (targetCRS) {
            return pj_obj_create(ctx, NN_NO_CHECK(targetCRS));
        }
        return nullptr;
    }
    if (!obj->alternativeCoordinateOperations.empty()) {
        return proj_get_target_crs(ctx,
                                   obj->alternativeCoordinateOperations[0].pj);
    }
    proj_log_error(ctx, __FUNCTION__,
                   "Object is not a BoundCRS or a CoordinateOperation");
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Identify the CRS with reference CRSs.
 *
 * The candidate CRSs are either hard-coded, or looked in the database when
 * it is available.
 *
 * Note that the implementation uses a set of heuristics to have a good
 * compromise of successful identifications over execution time. It might miss
 * legitimate matches in some circumstances.
 *
 * The method returns a list of matching reference CRS, and the percentage
 * (0-100) of confidence in the match. The list is sorted by decreasing
 * confidence.
 * <ul>
 * <li>100% means that the name of the reference entry
 * perfectly matches the CRS name, and both are equivalent. In which case a
 * single result is returned.
 * Note: in the case of a GeographicCRS whose axis
 * order is implicit in the input definition (for example ESRI WKT), then axis
 * order is ignored for the purpose of identification. That is the CRS built
 * from
 * GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],
 * PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]
 * will be identified to EPSG:4326, but will not pass a
 * isEquivalentTo(EPSG_4326, util::IComparable::Criterion::EQUIVALENT) test,
 * but rather isEquivalentTo(EPSG_4326,
 * util::IComparable::Criterion::EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS)
 * </li>
 * <li>90% means that CRS are equivalent, but the names are not exactly the
 * same.</li>
 * <li>70% means that CRS are equivalent, but the names are not equivalent.
 * </li>
 * <li>25% means that the CRS are not equivalent, but there is some similarity
 * in
 * the names.</li>
 * </ul>
 * Other confidence values may be returned by some specialized implementations.
 *
 * This is implemented for GeodeticCRS, ProjectedCRS, VerticalCRS and
 * CompoundCRS.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS. Must not be NULL
 * @param auth_name Authority name, or NULL for all authorities
 * @param options Placeholder for future options. Should be set to NULL.
 * @param out_confidence Output parameter. Pointer to an array of integers that
 * will be allocated by the function and filled with the confidence values
 * (0-100). There are as many elements in this array as
 * proj_list_get_count()
 * returns on the return value of this function. *confidence should be
 * released with proj_int_list_destroy().
 * @return a list of matching reference CRS, or nullptr in case of error.
 */
PJ_OBJ_LIST *proj_identify(PJ_CONTEXT *ctx, const PJ *obj,
                           const char *auth_name, const char *const *options,
                           int **out_confidence) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    (void)options;
    if (out_confidence) {
        *out_confidence = nullptr;
    }
    auto ptr = obj->iso_obj.get();
    auto crs = dynamic_cast<const CRS *>(ptr);
    if (!crs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a CRS");
    } else {
        int *confidenceTemp = nullptr;
        try {
            auto factory = AuthorityFactory::create(getDBcontext(ctx),
                                                    auth_name ? auth_name : "");
            auto res = crs->identify(factory);
            std::vector<IdentifiedObjectNNPtr> objects;
            confidenceTemp = out_confidence ? new int[res.size()] : nullptr;
            size_t i = 0;
            for (const auto &pair : res) {
                objects.push_back(pair.first);
                if (confidenceTemp) {
                    confidenceTemp[i] = pair.second;
                    ++i;
                }
            }
            auto ret = std::make_unique<PJ_OBJ_LIST>(std::move(objects));
            if (out_confidence) {
                *out_confidence = confidenceTemp;
                confidenceTemp = nullptr;
            }
            return ret.release();
        } catch (const std::exception &e) {
            delete[] confidenceTemp;
            proj_log_error(ctx, __FUNCTION__, e.what());
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Free an array of integer. */
void proj_int_list_destroy(int *list) { delete[] list; }

// ---------------------------------------------------------------------------

/** \brief Return the list of authorities used in the database.
 *
 * The returned list is NULL terminated and must be freed with
 * proj_string_list_destroy().
 *
 * @param ctx PROJ context, or NULL for default context
 *
 * @return a NULL terminated list of NUL-terminated strings that must be
 * freed with proj_string_list_destroy(), or NULL in case of error.
 */
PROJ_STRING_LIST proj_get_authorities_from_database(PJ_CONTEXT *ctx) {
    SANITIZE_CTX(ctx);
    try {
        auto ret = to_string_list(getDBcontext(ctx)->getAuthorities());
        return ret;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Returns the set of authority codes of the given object type.
 *
 * The returned list is NULL terminated and must be freed with
 * proj_string_list_destroy().
 *
 * @param ctx PROJ context, or NULL for default context.
 * @param auth_name Authority name (must not be NULL)
 * @param type Object type.
 * @param allow_deprecated whether we should return deprecated objects as well.
 *
 * @return a NULL terminated list of NUL-terminated strings that must be
 * freed with proj_string_list_destroy(), or NULL in case of error.
 *
 * @see proj_get_crs_info_list_from_database()
 */
PROJ_STRING_LIST proj_get_codes_from_database(PJ_CONTEXT *ctx,
                                              const char *auth_name,
                                              PJ_TYPE type,
                                              int allow_deprecated) {
    SANITIZE_CTX(ctx);
    if (!auth_name) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    try {
        auto factory = AuthorityFactory::create(getDBcontext(ctx), auth_name);
        bool valid = false;
        auto typeInternal = convertPJObjectTypeToObjectType(type, valid);
        if (!valid) {
            return nullptr;
        }
        auto ret = to_string_list(
            factory->getAuthorityCodes(typeInternal, allow_deprecated != 0));
        return ret;

    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Enumerate celestial bodies from the database.
 *
 * The returned object is an array of PROJ_CELESTIAL_BODY_INFO* pointers, whose
 * last entry is NULL. This array should be freed with
 * proj_celestial_body_list_destroy()
 *
 * @param ctx PROJ context, or NULL for default context
 * @param auth_name Authority name, used to restrict the search.
 * Or NULL for all authorities.
 * @param out_result_count Output parameter pointing to an integer to receive
 * the size of the result list. Might be NULL
 * @return an array of PROJ_CELESTIAL_BODY_INFO* pointers to be freed with
 * proj_celestial_body_list_destroy(), or NULL in case of error.
 * @since 8.1
 */
PROJ_CELESTIAL_BODY_INFO **proj_get_celestial_body_list_from_database(
    PJ_CONTEXT *ctx, const char *auth_name, int *out_result_count) {
    SANITIZE_CTX(ctx);
    PROJ_CELESTIAL_BODY_INFO **ret = nullptr;
    int i = 0;
    try {
        auto factory = AuthorityFactory::create(getDBcontext(ctx),
                                                auth_name ? auth_name : "");
        auto list = factory->getCelestialBodyList();
        ret = new PROJ_CELESTIAL_BODY_INFO *[list.size() + 1];
        for (const auto &info : list) {
            ret[i] = new PROJ_CELESTIAL_BODY_INFO;
            ret[i]->auth_name = pj_strdup(info.authName.c_str());
            ret[i]->name = pj_strdup(info.name.c_str());
            i++;
        }
        ret[i] = nullptr;
        if (out_result_count)
            *out_result_count = i;
        return ret;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        if (ret) {
            ret[i + 1] = nullptr;
            proj_celestial_body_list_destroy(ret);
        }
        if (out_result_count)
            *out_result_count = 0;
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Destroy the result returned by
 * proj_get_celestial_body_list_from_database().
 *
 * @since 8.1
 */
void proj_celestial_body_list_destroy(PROJ_CELESTIAL_BODY_INFO **list) {
    if (list) {
        for (int i = 0; list[i] != nullptr; i++) {
            free(list[i]->auth_name);
            free(list[i]->name);
            delete list[i];
        }
        delete[] list;
    }
}

// ---------------------------------------------------------------------------

/** Free a list of NULL terminated strings. */
void proj_string_list_destroy(PROJ_STRING_LIST list) {
    if (list) {
        for (size_t i = 0; list[i] != nullptr; i++) {
            delete[] list[i];
        }
        delete[] list;
    }
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a default set of parameters to be used by
 * proj_get_crs_list().
 *
 * @return a new object to free with proj_get_crs_list_parameters_destroy() */
PROJ_CRS_LIST_PARAMETERS *proj_get_crs_list_parameters_create() {
    auto ret = new (std::nothrow) PROJ_CRS_LIST_PARAMETERS();
    if (ret) {
        ret->types = nullptr;
        ret->typesCount = 0;
        ret->crs_area_of_use_contains_bbox = TRUE;
        ret->bbox_valid = FALSE;
        ret->west_lon_degree = 0.0;
        ret->south_lat_degree = 0.0;
        ret->east_lon_degree = 0.0;
        ret->north_lat_degree = 0.0;
        ret->allow_deprecated = FALSE;
        ret->celestial_body_name = nullptr;
    }
    return ret;
}

// ---------------------------------------------------------------------------

/** \brief Destroy an object returned by proj_get_crs_list_parameters_create()
 */
void proj_get_crs_list_parameters_destroy(PROJ_CRS_LIST_PARAMETERS *params) {
    delete params;
}

// ---------------------------------------------------------------------------

/** \brief Enumerate CRS objects from the database, taking into account various
 * criteria.
 *
 * The returned object is an array of PROJ_CRS_INFO* pointers, whose last
 * entry is NULL. This array should be freed with proj_crs_info_list_destroy()
 *
 * When no filter parameters are set, this is functionally equivalent to
 * proj_get_codes_from_database(), instantiating a PJ* object for each
 * of the codes with proj_create_from_database() and retrieving information
 * with the various getters. However this function will be much faster.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param auth_name Authority name, used to restrict the search.
 * Or NULL for all authorities.
 * @param params Additional criteria, or NULL. If not-NULL, params SHOULD
 * have been allocated by proj_get_crs_list_parameters_create(), as the
 * PROJ_CRS_LIST_PARAMETERS structure might grow over time.
 * @param out_result_count Output parameter pointing to an integer to receive
 * the size of the result list. Might be NULL
 * @return an array of PROJ_CRS_INFO* pointers to be freed with
 * proj_crs_info_list_destroy(), or NULL in case of error.
 */
PROJ_CRS_INFO **
proj_get_crs_info_list_from_database(PJ_CONTEXT *ctx, const char *auth_name,
                                     const PROJ_CRS_LIST_PARAMETERS *params,
                                     int *out_result_count) {
    SANITIZE_CTX(ctx);
    PROJ_CRS_INFO **ret = nullptr;
    int i = 0;
    try {
        auto dbContext = getDBcontext(ctx);
        std::string authName = auth_name ? auth_name : "";
        auto actualAuthNames =
            dbContext->getVersionedAuthoritiesFromName(authName);
        if (actualAuthNames.empty())
            actualAuthNames.push_back(std::move(authName));
        std::list<AuthorityFactory::CRSInfo> concatList;
        for (const auto &actualAuthName : actualAuthNames) {
            auto factory = AuthorityFactory::create(dbContext, actualAuthName);
            auto list = factory->getCRSInfoList();
            concatList.splice(concatList.end(), std::move(list));
        }
        ret = new PROJ_CRS_INFO *[concatList.size() + 1];
        GeographicBoundingBoxPtr bbox;
        if (params && params->bbox_valid) {
            bbox = GeographicBoundingBox::create(
                       params->west_lon_degree, params->south_lat_degree,
                       params->east_lon_degree, params->north_lat_degree)
                       .as_nullable();
        }
        for (const auto &info : concatList) {
            auto type = PJ_TYPE_CRS;
            if (info.type == AuthorityFactory::ObjectType::GEOGRAPHIC_2D_CRS) {
                type = PJ_TYPE_GEOGRAPHIC_2D_CRS;
            } else if (info.type ==
                       AuthorityFactory::ObjectType::GEOGRAPHIC_3D_CRS) {
                type = PJ_TYPE_GEOGRAPHIC_3D_CRS;
            } else if (info.type ==
                       AuthorityFactory::ObjectType::GEOCENTRIC_CRS) {
                type = PJ_TYPE_GEOCENTRIC_CRS;
            } else if (info.type ==
                       AuthorityFactory::ObjectType::GEODETIC_CRS) {
                type = PJ_TYPE_GEODETIC_CRS;
            } else if (info.type ==
                       AuthorityFactory::ObjectType::PROJECTED_CRS) {
                type = PJ_TYPE_PROJECTED_CRS;
            } else if (info.type ==
                       AuthorityFactory::ObjectType::VERTICAL_CRS) {
                type = PJ_TYPE_VERTICAL_CRS;
            } else if (info.type ==
                       AuthorityFactory::ObjectType::COMPOUND_CRS) {
                type = PJ_TYPE_COMPOUND_CRS;
            }
            if (params && params->typesCount) {
                bool typeValid = false;
                for (size_t j = 0; j < params->typesCount; j++) {
                    if (params->types[j] == type) {
                        typeValid = true;
                        break;
                    } else if (params->types[j] == PJ_TYPE_GEOGRAPHIC_CRS &&
                               (type == PJ_TYPE_GEOGRAPHIC_2D_CRS ||
                                type == PJ_TYPE_GEOGRAPHIC_3D_CRS)) {
                        typeValid = true;
                        break;
                    } else if (params->types[j] == PJ_TYPE_GEODETIC_CRS &&
                               (type == PJ_TYPE_GEOCENTRIC_CRS ||
                                type == PJ_TYPE_GEOGRAPHIC_2D_CRS ||
                                type == PJ_TYPE_GEOGRAPHIC_3D_CRS)) {
                        typeValid = true;
                        break;
                    }
                }
                if (!typeValid) {
                    continue;
                }
            }
            if (params && !params->allow_deprecated && info.deprecated) {
                continue;
            }
            if (params && params->bbox_valid) {
                if (!info.bbox_valid) {
                    continue;
                }
                if (info.west_lon_degree <= info.east_lon_degree &&
                    params->west_lon_degree <= params->east_lon_degree) {
                    if (params->crs_area_of_use_contains_bbox) {
                        if (params->west_lon_degree < info.west_lon_degree ||
                            params->east_lon_degree > info.east_lon_degree ||
                            params->south_lat_degree < info.south_lat_degree ||
                            params->north_lat_degree > info.north_lat_degree) {
                            continue;
                        }
                    } else {
                        if (info.east_lon_degree < params->west_lon_degree ||
                            info.west_lon_degree > params->east_lon_degree ||
                            info.north_lat_degree < params->south_lat_degree ||
                            info.south_lat_degree > params->north_lat_degree) {
                            continue;
                        }
                    }
                } else {
                    auto crsExtent = GeographicBoundingBox::create(
                        info.west_lon_degree, info.south_lat_degree,
                        info.east_lon_degree, info.north_lat_degree);
                    if (params->crs_area_of_use_contains_bbox) {
                        if (!crsExtent->contains(NN_NO_CHECK(bbox))) {
                            continue;
                        }
                    } else {
                        if (!bbox->intersects(crsExtent)) {
                            continue;
                        }
                    }
                }
            }
            if (params && params->celestial_body_name &&
                params->celestial_body_name != info.celestialBodyName) {
                continue;
            }

            ret[i] = new PROJ_CRS_INFO;
            ret[i]->auth_name = pj_strdup(info.authName.c_str());
            ret[i]->code = pj_strdup(info.code.c_str());
            ret[i]->name = pj_strdup(info.name.c_str());
            ret[i]->type = type;
            ret[i]->deprecated = info.deprecated;
            ret[i]->bbox_valid = info.bbox_valid;
            ret[i]->west_lon_degree = info.west_lon_degree;
            ret[i]->south_lat_degree = info.south_lat_degree;
            ret[i]->east_lon_degree = info.east_lon_degree;
            ret[i]->north_lat_degree = info.north_lat_degree;
            ret[i]->area_name = pj_strdup(info.areaName.c_str());
            ret[i]->projection_method_name =
                info.projectionMethodName.empty()
                    ? nullptr
                    : pj_strdup(info.projectionMethodName.c_str());
            ret[i]->celestial_body_name =
                pj_strdup(info.celestialBodyName.c_str());
            i++;
        }
        ret[i] = nullptr;
        if (out_result_count)
            *out_result_count = i;
        return ret;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        if (ret) {
            ret[i + 1] = nullptr;
            proj_crs_info_list_destroy(ret);
        }
        if (out_result_count)
            *out_result_count = 0;
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Destroy the result returned by
 * proj_get_crs_info_list_from_database().
 */
void proj_crs_info_list_destroy(PROJ_CRS_INFO **list) {
    if (list) {
        for (int i = 0; list[i] != nullptr; i++) {
            free(list[i]->auth_name);
            free(list[i]->code);
            free(list[i]->name);
            free(list[i]->area_name);
            free(list[i]->projection_method_name);
            free(list[i]->celestial_body_name);
            delete list[i];
        }
        delete[] list;
    }
}

// ---------------------------------------------------------------------------

/** \brief Enumerate units from the database, taking into account various
 * criteria.
 *
 * The returned object is an array of PROJ_UNIT_INFO* pointers, whose last
 * entry is NULL. This array should be freed with proj_unit_list_destroy()
 *
 * @param ctx PROJ context, or NULL for default context
 * @param auth_name Authority name, used to restrict the search.
 * Or NULL for all authorities.
 * @param category Filter by category, if this parameter is not NULL. Category
 * is one of "linear", "linear_per_time", "angular", "angular_per_time",
 * "scale", "scale_per_time" or "time"
 * @param allow_deprecated whether we should return deprecated objects as well.
 * @param out_result_count Output parameter pointing to an integer to receive
 * the size of the result list. Might be NULL
 * @return an array of PROJ_UNIT_INFO* pointers to be freed with
 * proj_unit_list_destroy(), or NULL in case of error.
 *
 * @since 7.1
 */
PROJ_UNIT_INFO **proj_get_units_from_database(PJ_CONTEXT *ctx,
                                              const char *auth_name,
                                              const char *category,
                                              int allow_deprecated,
                                              int *out_result_count) {
    SANITIZE_CTX(ctx);
    PROJ_UNIT_INFO **ret = nullptr;
    int i = 0;
    try {
        auto factory = AuthorityFactory::create(getDBcontext(ctx),
                                                auth_name ? auth_name : "");
        auto list = factory->getUnitList();
        ret = new PROJ_UNIT_INFO *[list.size() + 1];
        for (const auto &info : list) {
            if (category && info.category != category) {
                continue;
            }
            if (!allow_deprecated && info.deprecated) {
                continue;
            }
            ret[i] = new PROJ_UNIT_INFO;
            ret[i]->auth_name = pj_strdup(info.authName.c_str());
            ret[i]->code = pj_strdup(info.code.c_str());
            ret[i]->name = pj_strdup(info.name.c_str());
            ret[i]->category = pj_strdup(info.category.c_str());
            ret[i]->conv_factor = info.convFactor;
            ret[i]->proj_short_name =
                info.projShortName.empty()
                    ? nullptr
                    : pj_strdup(info.projShortName.c_str());
            ret[i]->deprecated = info.deprecated;
            i++;
        }
        ret[i] = nullptr;
        if (out_result_count)
            *out_result_count = i;
        return ret;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        if (ret) {
            ret[i + 1] = nullptr;
            proj_unit_list_destroy(ret);
        }
        if (out_result_count)
            *out_result_count = 0;
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Destroy the result returned by
 * proj_get_units_from_database().
 *
 * @since 7.1
 */
void proj_unit_list_destroy(PROJ_UNIT_INFO **list) {
    if (list) {
        for (int i = 0; list[i] != nullptr; i++) {
            free(list[i]->auth_name);
            free(list[i]->code);
            free(list[i]->name);
            free(list[i]->category);
            free(list[i]->proj_short_name);
            delete list[i];
        }
        delete[] list;
    }
}

// ---------------------------------------------------------------------------

/** \brief Return the Conversion of a DerivedCRS (such as a ProjectedCRS),
 * or the Transformation from the baseCRS to the hubCRS of a BoundCRS
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type DerivedCRS or BoundCRSs (must not be NULL)
 * @return Object of type SingleOperation that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_crs_get_coordoperation(PJ_CONTEXT *ctx, const PJ *crs) {
    SANITIZE_CTX(ctx);
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    SingleOperationPtr co;

    auto derivedCRS = dynamic_cast<const DerivedCRS *>(crs->iso_obj.get());
    if (derivedCRS) {
        co = derivedCRS->derivingConversion().as_nullable();
    } else {
        auto boundCRS = dynamic_cast<const BoundCRS *>(crs->iso_obj.get());
        if (boundCRS) {
            co = boundCRS->transformation().as_nullable();
        } else {
            proj_log_error(ctx, __FUNCTION__,
                           "Object is not a DerivedCRS or BoundCRS");
            return nullptr;
        }
    }

    return pj_obj_create(ctx, NN_NO_CHECK(co));
}

// ---------------------------------------------------------------------------

/** \brief Return information on the operation method of the SingleOperation.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type SingleOperation (typically a Conversion
 * or Transformation) (must not be NULL)
 * @param out_method_name Pointer to a string value to store the method
 * (projection) name. or NULL
 * @param out_method_auth_name Pointer to a string value to store the method
 * authority name. or NULL
 * @param out_method_code Pointer to a string value to store the method
 * code. or NULL
 * @return TRUE in case of success.
 */
int proj_coordoperation_get_method_info(PJ_CONTEXT *ctx,
                                        const PJ *coordoperation,
                                        const char **out_method_name,
                                        const char **out_method_auth_name,
                                        const char **out_method_code) {
    SANITIZE_CTX(ctx);
    if (!coordoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto singleOp =
        dynamic_cast<const SingleOperation *>(coordoperation->iso_obj.get());
    if (!singleOp) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a DerivedCRS or BoundCRS");
        return false;
    }

    const auto &method = singleOp->method();
    const auto &method_ids = method->identifiers();
    if (out_method_name) {
        *out_method_name = method->name()->description()->c_str();
    }
    if (out_method_auth_name) {
        if (!method_ids.empty()) {
            *out_method_auth_name = method_ids[0]->codeSpace()->c_str();
        } else {
            *out_method_auth_name = nullptr;
        }
    }
    if (out_method_code) {
        if (!method_ids.empty()) {
            *out_method_code = method_ids[0]->code().c_str();
        } else {
            *out_method_code = nullptr;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static PropertyMap createPropertyMapName(const char *c_name,
                                         const char *auth_name = nullptr,
                                         const char *code = nullptr) {
    std::string name(c_name ? c_name : "unnamed");
    PropertyMap properties;
    if (ends_with(name, " (deprecated)")) {
        name.resize(name.size() - strlen(" (deprecated)"));
        properties.set(common::IdentifiedObject::DEPRECATED_KEY, true);
    }
    if (auth_name && code) {
        properties.set(metadata::Identifier::CODESPACE_KEY, auth_name);
        properties.set(metadata::Identifier::CODE_KEY, code);
    }
    return properties.set(common::IdentifiedObject::NAME_KEY, name);
}

// ---------------------------------------------------------------------------

static UnitOfMeasure createLinearUnit(const char *name, double convFactor,
                                      const char *unit_auth_name = nullptr,
                                      const char *unit_code = nullptr) {
    return name == nullptr
               ? UnitOfMeasure::METRE
               : UnitOfMeasure(name, convFactor, UnitOfMeasure::Type::LINEAR,
                               unit_auth_name ? unit_auth_name : "",
                               unit_code ? unit_code : "");
}

// ---------------------------------------------------------------------------

static UnitOfMeasure createAngularUnit(const char *name, double convFactor,
                                       const char *unit_auth_name = nullptr,
                                       const char *unit_code = nullptr) {
    return name ? (ci_equal(name, "degree") ? UnitOfMeasure::DEGREE
                   : ci_equal(name, "grad")
                       ? UnitOfMeasure::GRAD
                       : UnitOfMeasure(name, convFactor,
                                       UnitOfMeasure::Type::ANGULAR,
                                       unit_auth_name ? unit_auth_name : "",
                                       unit_code ? unit_code : ""))
                : UnitOfMeasure::DEGREE;
}

// ---------------------------------------------------------------------------

static GeodeticReferenceFrameNNPtr createGeodeticReferenceFrame(
    PJ_CONTEXT *ctx, const char *datum_name, const char *ellps_name,
    double semi_major_metre, double inv_flattening,
    const char *prime_meridian_name, double prime_meridian_offset,
    const char *angular_units, double angular_units_conv) {
    const UnitOfMeasure angUnit(
        createAngularUnit(angular_units, angular_units_conv));
    auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
    auto body = Ellipsoid::guessBodyName(dbContext, semi_major_metre);
    auto ellpsName = createPropertyMapName(ellps_name);
    auto ellps = inv_flattening != 0.0
                     ? Ellipsoid::createFlattenedSphere(
                           ellpsName, Length(semi_major_metre),
                           Scale(inv_flattening), body)
                     : Ellipsoid::createSphere(ellpsName,
                                               Length(semi_major_metre), body);
    auto pm = PrimeMeridian::create(
        PropertyMap().set(
            common::IdentifiedObject::NAME_KEY,
            prime_meridian_name ? prime_meridian_name
            : prime_meridian_offset == 0.0
                ? (ellps->celestialBody() == Ellipsoid::EARTH
                       ? PrimeMeridian::GREENWICH->nameStr().c_str()
                       : PrimeMeridian::REFERENCE_MERIDIAN->nameStr().c_str())
                : "unnamed"),
        Angle(prime_meridian_offset, angUnit));

    std::string datumName(datum_name ? datum_name : "unnamed");
    if (datumName == "WGS_1984") {
        datumName = GeodeticReferenceFrame::EPSG_6326->nameStr();
    } else if (datumName.find('_') != std::string::npos) {
        // Likely coming from WKT1
        if (dbContext) {
            auto authFactory =
                AuthorityFactory::create(NN_NO_CHECK(dbContext), std::string());
            auto res = authFactory->createObjectsFromName(
                datumName,
                {AuthorityFactory::ObjectType::GEODETIC_REFERENCE_FRAME}, true,
                1);
            if (!res.empty()) {
                const auto &refDatum = res.front();
                if (metadata::Identifier::isEquivalentName(
                        datumName.c_str(), refDatum->nameStr().c_str())) {
                    datumName = refDatum->nameStr();
                } else if (refDatum->identifiers().size() == 1) {
                    const auto &id = refDatum->identifiers()[0];
                    const auto aliases =
                        authFactory->databaseContext()->getAliases(
                            *id->codeSpace(), id->code(), refDatum->nameStr(),
                            "geodetic_datum", std::string());
                    for (const auto &alias : aliases) {
                        if (metadata::Identifier::isEquivalentName(
                                datumName.c_str(), alias.c_str())) {
                            datumName = refDatum->nameStr();
                            break;
                        }
                    }
                }
            }
        }
    }

    return GeodeticReferenceFrame::create(
        createPropertyMapName(datumName.c_str()), ellps,
        util::optional<std::string>(), pm);
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Create a GeographicCRS.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name Name of the GeographicCRS. Or NULL
 * @param datum_name Name of the GeodeticReferenceFrame. Or NULL
 * @param ellps_name Name of the Ellipsoid. Or NULL
 * @param semi_major_metre Ellipsoid semi-major axis, in metres.
 * @param inv_flattening Ellipsoid inverse flattening. Or 0 for a sphere.
 * @param prime_meridian_name Name of the PrimeMeridian. Or NULL
 * @param prime_meridian_offset Offset of the prime meridian, expressed in the
 * specified angular units.
 * @param pm_angular_units Name of the angular units. Or NULL for Degree
 * @param pm_angular_units_conv Conversion factor from the angular unit to
 * radian.
 * Or
 * 0 for Degree if pm_angular_units == NULL. Otherwise should be not NULL
 * @param ellipsoidal_cs Coordinate system. Must not be NULL.
 *
 * @return Object of type GeographicCRS that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_create_geographic_crs(PJ_CONTEXT *ctx, const char *crs_name,
                               const char *datum_name, const char *ellps_name,
                               double semi_major_metre, double inv_flattening,
                               const char *prime_meridian_name,
                               double prime_meridian_offset,
                               const char *pm_angular_units,
                               double pm_angular_units_conv,
                               const PJ *ellipsoidal_cs) {

    SANITIZE_CTX(ctx);
    auto cs = std::dynamic_pointer_cast<EllipsoidalCS>(ellipsoidal_cs->iso_obj);
    if (!cs) {
        return nullptr;
    }
    try {
        auto datum = createGeodeticReferenceFrame(
            ctx, datum_name, ellps_name, semi_major_metre, inv_flattening,
            prime_meridian_name, prime_meridian_offset, pm_angular_units,
            pm_angular_units_conv);
        auto geogCRS = GeographicCRS::create(createPropertyMapName(crs_name),
                                             datum, NN_NO_CHECK(cs));
        return pj_obj_create(ctx, geogCRS);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Create a GeographicCRS.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name Name of the GeographicCRS. Or NULL
 * @param datum_or_datum_ensemble Datum or DatumEnsemble (DatumEnsemble possible
 * since 7.2). Must not be NULL.
 * @param ellipsoidal_cs Coordinate system. Must not be NULL.
 *
 * @return Object of type GeographicCRS that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_create_geographic_crs_from_datum(PJ_CONTEXT *ctx, const char *crs_name,
                                          const PJ *datum_or_datum_ensemble,
                                          const PJ *ellipsoidal_cs) {

    SANITIZE_CTX(ctx);
    if (datum_or_datum_ensemble == nullptr) {
        proj_log_error(ctx, __FUNCTION__,
                       "Missing input datum_or_datum_ensemble");
        return nullptr;
    }
    auto l_datum = std::dynamic_pointer_cast<GeodeticReferenceFrame>(
        datum_or_datum_ensemble->iso_obj);
    auto l_datum_ensemble = std::dynamic_pointer_cast<DatumEnsemble>(
        datum_or_datum_ensemble->iso_obj);
    auto cs = std::dynamic_pointer_cast<EllipsoidalCS>(ellipsoidal_cs->iso_obj);
    if (!cs) {
        return nullptr;
    }
    try {
        auto geogCRS =
            GeographicCRS::create(createPropertyMapName(crs_name), l_datum,
                                  l_datum_ensemble, NN_NO_CHECK(cs));
        return pj_obj_create(ctx, geogCRS);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Create a GeodeticCRS of geocentric type.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name Name of the GeographicCRS. Or NULL
 * @param datum_name Name of the GeodeticReferenceFrame. Or NULL
 * @param ellps_name Name of the Ellipsoid. Or NULL
 * @param semi_major_metre Ellipsoid semi-major axis, in metres.
 * @param inv_flattening Ellipsoid inverse flattening. Or 0 for a sphere.
 * @param prime_meridian_name Name of the PrimeMeridian. Or NULL
 * @param prime_meridian_offset Offset of the prime meridian, expressed in the
 * specified angular units.
 * @param angular_units Name of the angular units. Or NULL for Degree
 * @param angular_units_conv Conversion factor from the angular unit to radian.
 * Or
 * 0 for Degree if angular_units == NULL. Otherwise should be not NULL
 * @param linear_units Name of the linear units. Or NULL for Metre
 * @param linear_units_conv Conversion factor from the linear unit to metre. Or
 * 0 for Metre if linear_units == NULL. Otherwise should be not NULL
 *
 * @return Object of type GeodeticCRS that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_create_geocentric_crs(
    PJ_CONTEXT *ctx, const char *crs_name, const char *datum_name,
    const char *ellps_name, double semi_major_metre, double inv_flattening,
    const char *prime_meridian_name, double prime_meridian_offset,
    const char *angular_units, double angular_units_conv,
    const char *linear_units, double linear_units_conv) {

    SANITIZE_CTX(ctx);
    try {
        const UnitOfMeasure linearUnit(
            createLinearUnit(linear_units, linear_units_conv));
        auto datum = createGeodeticReferenceFrame(
            ctx, datum_name, ellps_name, semi_major_metre, inv_flattening,
            prime_meridian_name, prime_meridian_offset, angular_units,
            angular_units_conv);

        auto geodCRS =
            GeodeticCRS::create(createPropertyMapName(crs_name), datum,
                                cs::CartesianCS::createGeocentric(linearUnit));
        return pj_obj_create(ctx, geodCRS);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Create a GeodeticCRS of geocentric type.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name Name of the GeographicCRS. Or NULL
 * @param datum_or_datum_ensemble Datum or DatumEnsemble (DatumEnsemble possible
 * since 7.2). Must not be NULL.
 * @param linear_units Name of the linear units. Or NULL for Metre
 * @param linear_units_conv Conversion factor from the linear unit to metre. Or
 * 0 for Metre if linear_units == NULL. Otherwise should be not NULL
 *
 * @return Object of type GeodeticCRS that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_create_geocentric_crs_from_datum(PJ_CONTEXT *ctx, const char *crs_name,
                                          const PJ *datum_or_datum_ensemble,
                                          const char *linear_units,
                                          double linear_units_conv) {
    SANITIZE_CTX(ctx);
    if (datum_or_datum_ensemble == nullptr) {
        proj_log_error(ctx, __FUNCTION__,
                       "Missing input datum_or_datum_ensemble");
        return nullptr;
    }
    auto l_datum = std::dynamic_pointer_cast<GeodeticReferenceFrame>(
        datum_or_datum_ensemble->iso_obj);
    auto l_datum_ensemble = std::dynamic_pointer_cast<DatumEnsemble>(
        datum_or_datum_ensemble->iso_obj);
    try {
        const UnitOfMeasure linearUnit(
            createLinearUnit(linear_units, linear_units_conv));
        auto geodCRS = GeodeticCRS::create(
            createPropertyMapName(crs_name), l_datum, l_datum_ensemble,
            cs::CartesianCS::createGeocentric(linearUnit));
        return pj_obj_create(ctx, geodCRS);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Create a DerivedGeograhicCRS.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name Name of the GeographicCRS. Or NULL
 * @param base_geographic_crs Base Geographic CRS. Must not be NULL.
 * @param conversion Conversion from the base Geographic to the
 * DerivedGeograhicCRS. Must not be NULL.
 * @param ellipsoidal_cs Coordinate system. Must not be NULL.
 *
 * @return Object of type GeodeticCRS that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 *
 * @since 7.0
 */
PJ *proj_create_derived_geographic_crs(PJ_CONTEXT *ctx, const char *crs_name,
                                       const PJ *base_geographic_crs,
                                       const PJ *conversion,
                                       const PJ *ellipsoidal_cs) {
    SANITIZE_CTX(ctx);
    auto base_crs =
        std::dynamic_pointer_cast<GeographicCRS>(base_geographic_crs->iso_obj);
    auto conversion_cpp =
        std::dynamic_pointer_cast<Conversion>(conversion->iso_obj);
    auto cs = std::dynamic_pointer_cast<EllipsoidalCS>(ellipsoidal_cs->iso_obj);
    if (!base_crs || !conversion_cpp || !cs) {
        return nullptr;
    }
    try {
        auto derivedCRS = DerivedGeographicCRS::create(
            createPropertyMapName(crs_name), NN_NO_CHECK(base_crs),
            NN_NO_CHECK(conversion_cpp), NN_NO_CHECK(cs));
        return pj_obj_create(ctx, derivedCRS);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Return whether a CRS is a Derived CRS.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs CRS. Must not be NULL.
 *
 * @return whether a CRS is a Derived CRS.
 *
 * @since 7.0
 */
int proj_is_derived_crs(PJ_CONTEXT *ctx, const PJ *crs) {
    SANITIZE_CTX(ctx);
    return dynamic_cast<DerivedCRS *>(crs->iso_obj.get()) != nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Create a VerticalCRS
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name Name of the GeographicCRS. Or NULL
 * @param datum_name Name of the VerticalReferenceFrame. Or NULL
 * @param linear_units Name of the linear units. Or NULL for Metre
 * @param linear_units_conv Conversion factor from the linear unit to metre. Or
 * 0 for Metre if linear_units == NULL. Otherwise should be not NULL
 *
 * @return Object of type VerticalCRS that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_create_vertical_crs(PJ_CONTEXT *ctx, const char *crs_name,
                             const char *datum_name, const char *linear_units,
                             double linear_units_conv) {

    return proj_create_vertical_crs_ex(
        ctx, crs_name, datum_name, nullptr, nullptr, linear_units,
        linear_units_conv, nullptr, nullptr, nullptr, nullptr, nullptr);
}

// ---------------------------------------------------------------------------

/** \brief Create a VerticalCRS
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * This is an extended (_ex) version of proj_create_vertical_crs() that adds
 * the capability of defining a geoid model.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name Name of the GeographicCRS. Or NULL
 * @param datum_name Name of the VerticalReferenceFrame. Or NULL
 * @param datum_auth_name Authority name of the VerticalReferenceFrame. Or NULL
 * @param datum_code Code of the VerticalReferenceFrame. Or NULL
 * @param linear_units Name of the linear units. Or NULL for Metre
 * @param linear_units_conv Conversion factor from the linear unit to metre. Or
 * 0 for Metre if linear_units == NULL. Otherwise should be not NULL
 * @param geoid_model_name Geoid model name, or NULL. Can be a name from the
 * geoid_model name or a string "PROJ foo.gtx"
 * @param geoid_model_auth_name Authority name of the transformation for
 * the geoid model. or NULL
 * @param geoid_model_code Code of the transformation for
 * the geoid model. or NULL
 * @param geoid_geog_crs Geographic CRS for the geoid transformation, or NULL.
 * @param options NULL-terminated list of strings with "KEY=VALUE" format. or
 * NULL.
 * The currently recognized option is ACCURACY=value, where value is in metre.
 * @return Object of type VerticalCRS that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_create_vertical_crs_ex(
    PJ_CONTEXT *ctx, const char *crs_name, const char *datum_name,
    const char *datum_auth_name, const char *datum_code,
    const char *linear_units, double linear_units_conv,
    const char *geoid_model_name, const char *geoid_model_auth_name,
    const char *geoid_model_code, const PJ *geoid_geog_crs,
    const char *const *options) {
    SANITIZE_CTX(ctx);
    try {
        const UnitOfMeasure linearUnit(
            createLinearUnit(linear_units, linear_units_conv));
        auto datum = VerticalReferenceFrame::create(
            createPropertyMapName(datum_name, datum_auth_name, datum_code));
        auto props = createPropertyMapName(crs_name);
        auto cs = cs::VerticalCS::createGravityRelatedHeight(linearUnit);
        if (geoid_model_name) {
            auto propsModel = createPropertyMapName(
                geoid_model_name, geoid_model_auth_name, geoid_model_code);
            const auto vertCRSWithoutGeoid =
                VerticalCRS::create(props, datum, cs);
            const auto interpCRS =
                geoid_geog_crs && std::dynamic_pointer_cast<GeographicCRS>(
                                      geoid_geog_crs->iso_obj)
                    ? std::dynamic_pointer_cast<CRS>(geoid_geog_crs->iso_obj)
                    : nullptr;

            std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
            for (auto iter = options; iter && iter[0]; ++iter) {
                const char *value;
                if ((value = getOptionValue(*iter, "ACCURACY="))) {
                    accuracies.emplace_back(
                        metadata::PositionalAccuracy::create(value));
                }
            }
            const auto model(Transformation::create(
                propsModel, vertCRSWithoutGeoid,
                GeographicCRS::EPSG_4979, // arbitrarily chosen. Ignored
                interpCRS,
                OperationMethod::create(PropertyMap(),
                                        std::vector<OperationParameterNNPtr>()),
                {}, accuracies));
            props.set("GEOID_MODEL", model);
        }
        auto vertCRS = VerticalCRS::create(props, datum, cs);
        return pj_obj_create(ctx, vertCRS);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Create a CompoundCRS
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name Name of the GeographicCRS. Or NULL
 * @param horiz_crs Horizontal CRS. must not be NULL.
 * @param vert_crs Vertical CRS. must not be NULL.
 *
 * @return Object of type CompoundCRS that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_create_compound_crs(PJ_CONTEXT *ctx, const char *crs_name,
                             const PJ *horiz_crs, const PJ *vert_crs) {

    SANITIZE_CTX(ctx);
    if (!horiz_crs || !vert_crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_horiz_crs = std::dynamic_pointer_cast<CRS>(horiz_crs->iso_obj);
    if (!l_horiz_crs) {
        return nullptr;
    }
    auto l_vert_crs = std::dynamic_pointer_cast<CRS>(vert_crs->iso_obj);
    if (!l_vert_crs) {
        return nullptr;
    }
    try {
        auto compoundCRS = CompoundCRS::create(
            createPropertyMapName(crs_name),
            {NN_NO_CHECK(l_horiz_crs), NN_NO_CHECK(l_vert_crs)});
        return pj_obj_create(ctx, compoundCRS);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Return a copy of the object with its name changed
 *
 * Currently, only implemented on CRS objects.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS. Must not be NULL
 * @param name New name. Must not be NULL
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_alter_name(PJ_CONTEXT *ctx, const PJ *obj, const char *name) {
    SANITIZE_CTX(ctx);
    if (!obj || !name) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto crs = dynamic_cast<const CRS *>(obj->iso_obj.get());
    if (!crs) {
        return nullptr;
    }
    try {
        return pj_obj_create(ctx, crs->alterName(name));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Return a copy of the object with its identifier changed/set
 *
 * Currently, only implemented on CRS objects.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS. Must not be NULL
 * @param auth_name Authority name. Must not be NULL
 * @param code Code. Must not be NULL
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_alter_id(PJ_CONTEXT *ctx, const PJ *obj, const char *auth_name,
                  const char *code) {
    SANITIZE_CTX(ctx);
    if (!obj || !auth_name || !code) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto crs = dynamic_cast<const CRS *>(obj->iso_obj.get());
    if (!crs) {
        return nullptr;
    }
    try {
        return pj_obj_create(ctx, crs->alterId(auth_name, code));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Return a copy of the CRS with its geodetic CRS changed
 *
 * Currently, when obj is a GeodeticCRS, it returns a clone of new_geod_crs
 * When obj is a ProjectedCRS, it replaces its base CRS with new_geod_crs.
 * When obj is a CompoundCRS, it replaces the GeodeticCRS part of the horizontal
 * CRS with new_geod_crs.
 * In other cases, it returns a clone of obj.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS. Must not be NULL
 * @param new_geod_crs Object of type GeodeticCRS. Must not be NULL
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_crs_alter_geodetic_crs(PJ_CONTEXT *ctx, const PJ *obj,
                                const PJ *new_geod_crs) {
    SANITIZE_CTX(ctx);
    if (!obj || !new_geod_crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_new_geod_crs =
        std::dynamic_pointer_cast<GeodeticCRS>(new_geod_crs->iso_obj);
    if (!l_new_geod_crs) {
        proj_log_error(ctx, __FUNCTION__, "new_geod_crs is not a GeodeticCRS");
        return nullptr;
    }

    auto crs = dynamic_cast<const CRS *>(obj->iso_obj.get());
    if (!crs) {
        proj_log_error(ctx, __FUNCTION__, "obj is not a CRS");
        return nullptr;
    }

    try {
        return pj_obj_create(
            ctx, crs->alterGeodeticCRS(NN_NO_CHECK(l_new_geod_crs)));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Return a copy of the CRS with its angular units changed
 *
 * The CRS must be or contain a GeographicCRS.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS. Must not be NULL
 * @param angular_units Name of the angular units. Or NULL for Degree
 * @param angular_units_conv Conversion factor from the angular unit to radian.
 * Or 0 for Degree if angular_units == NULL. Otherwise should be not NULL
 * @param unit_auth_name Unit authority name. Or NULL.
 * @param unit_code Unit code. Or NULL.
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_crs_alter_cs_angular_unit(PJ_CONTEXT *ctx, const PJ *obj,
                                   const char *angular_units,
                                   double angular_units_conv,
                                   const char *unit_auth_name,
                                   const char *unit_code) {

    SANITIZE_CTX(ctx);
    auto geodCRS = proj_crs_get_geodetic_crs(ctx, obj);
    if (!geodCRS) {
        return nullptr;
    }
    auto geogCRS = dynamic_cast<const GeographicCRS *>(geodCRS->iso_obj.get());
    if (!geogCRS) {
        proj_destroy(geodCRS);
        return nullptr;
    }

    PJ *geogCRSAltered = nullptr;
    try {
        const UnitOfMeasure angUnit(createAngularUnit(
            angular_units, angular_units_conv, unit_auth_name, unit_code));
        geogCRSAltered = pj_obj_create(
            ctx, GeographicCRS::create(
                     createPropertyMapName(proj_get_name(geodCRS)),
                     geogCRS->datum(), geogCRS->datumEnsemble(),
                     geogCRS->coordinateSystem()->alterAngularUnit(angUnit)));
        proj_destroy(geodCRS);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        proj_destroy(geodCRS);
        return nullptr;
    }

    auto ret = proj_crs_alter_geodetic_crs(ctx, obj, geogCRSAltered);
    proj_destroy(geogCRSAltered);
    return ret;
}

// ---------------------------------------------------------------------------

/** \brief Return a copy of the CRS with the linear units of its coordinate
 * system changed
 *
 * The CRS must be or contain a ProjectedCRS, VerticalCRS or a GeocentricCRS.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS. Must not be NULL
 * @param linear_units Name of the linear units. Or NULL for Metre
 * @param linear_units_conv Conversion factor from the linear unit to metre. Or
 * 0 for Metre if linear_units == NULL. Otherwise should be not NULL
 * @param unit_auth_name Unit authority name. Or NULL.
 * @param unit_code Unit code. Or NULL.
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_crs_alter_cs_linear_unit(PJ_CONTEXT *ctx, const PJ *obj,
                                  const char *linear_units,
                                  double linear_units_conv,
                                  const char *unit_auth_name,
                                  const char *unit_code) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto crs = dynamic_cast<const CRS *>(obj->iso_obj.get());
    if (!crs) {
        return nullptr;
    }

    try {
        const UnitOfMeasure linearUnit(createLinearUnit(
            linear_units, linear_units_conv, unit_auth_name, unit_code));
        return pj_obj_create(ctx, crs->alterCSLinearUnit(linearUnit));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Return a copy of the CRS with the linear units of the parameters
 * of its conversion modified.
 *
 * The CRS must be or contain a ProjectedCRS, VerticalCRS or a GeocentricCRS.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type ProjectedCRS. Must not be NULL
 * @param linear_units Name of the linear units. Or NULL for Metre
 * @param linear_units_conv Conversion factor from the linear unit to metre. Or
 * 0 for Metre if linear_units == NULL. Otherwise should be not NULL
 * @param unit_auth_name Unit authority name. Or NULL.
 * @param unit_code Unit code. Or NULL.
 * @param convert_to_new_unit TRUE if existing values should be converted from
 * their current unit to the new unit. If FALSE, their value will be left
 * unchanged and the unit overridden (so the resulting CRS will not be
 * equivalent to the original one for reprojection purposes).
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_crs_alter_parameters_linear_unit(PJ_CONTEXT *ctx, const PJ *obj,
                                          const char *linear_units,
                                          double linear_units_conv,
                                          const char *unit_auth_name,
                                          const char *unit_code,
                                          int convert_to_new_unit) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto crs = dynamic_cast<const ProjectedCRS *>(obj->iso_obj.get());
    if (!crs) {
        return nullptr;
    }

    try {
        const UnitOfMeasure linearUnit(createLinearUnit(
            linear_units, linear_units_conv, unit_auth_name, unit_code));
        return pj_obj_create(ctx, crs->alterParametersLinearUnit(
                                      linearUnit, convert_to_new_unit == TRUE));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Create a 3D CRS from an existing 2D CRS.
 *
 * The new axis will be ellipsoidal height, oriented upwards, and with metre
 * units.
 *
 * See osgeo::proj::crs::CRS::promoteTo3D().
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_3D_name CRS name. Or NULL (in which case the name of crs_2D
 * will be used)
 * @param crs_2D 2D CRS to be "promoted" to 3D. Must not be NULL.
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 * @since 6.3
 */
PJ *proj_crs_promote_to_3D(PJ_CONTEXT *ctx, const char *crs_3D_name,
                           const PJ *crs_2D) {
    SANITIZE_CTX(ctx);
    if (!crs_2D) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto cpp_2D_crs = dynamic_cast<const CRS *>(crs_2D->iso_obj.get());
    if (!cpp_2D_crs) {
        auto coordinateMetadata =
            dynamic_cast<const CoordinateMetadata *>(crs_2D->iso_obj.get());
        if (!coordinateMetadata) {
            proj_log_error(ctx, __FUNCTION__,
                           "crs_2D is not a CRS or a CoordinateMetadata");
            return nullptr;
        }

        try {
            auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
            auto crs = coordinateMetadata->crs();
            auto crs_3D = crs->promoteTo3D(
                crs_3D_name ? std::string(crs_3D_name) : crs->nameStr(),
                dbContext);
            if (coordinateMetadata->coordinateEpoch().has_value()) {
                return pj_obj_create(
                    ctx, CoordinateMetadata::create(
                             crs_3D,
                             coordinateMetadata->coordinateEpochAsDecimalYear(),
                             dbContext));
            } else {
                return pj_obj_create(ctx, CoordinateMetadata::create(crs_3D));
            }
        } catch (const std::exception &e) {
            proj_log_error(ctx, __FUNCTION__, e.what());
            return nullptr;
        }
    } else {
        try {
            auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
            return pj_obj_create(ctx, cpp_2D_crs->promoteTo3D(
                                          crs_3D_name ? std::string(crs_3D_name)
                                                      : cpp_2D_crs->nameStr(),
                                          dbContext));
        } catch (const std::exception &e) {
            proj_log_error(ctx, __FUNCTION__, e.what());
            return nullptr;
        }
    }
}

// ---------------------------------------------------------------------------

/** \brief Create a projected 3D CRS from an existing projected 2D CRS.
 *
 * The passed projected_2D_crs is used so that its name is replaced by
 * crs_name and its base geographic CRS is replaced by geog_3D_crs. The vertical
 * axis of geog_3D_crs (ellipsoidal height) will be added as the 3rd axis of
 * the resulting projected 3D CRS.
 * Normally, the passed geog_3D_crs should be the 3D counterpart of the original
 * 2D base geographic CRS of projected_2D_crs, but such no check is done.
 *
 * It is also possible to invoke this function with a NULL geog_3D_crs. In which
 * case, the existing base geographic 2D CRS of projected_2D_crs will be
 * automatically promoted to 3D by assuming a 3rd axis being an ellipsoidal
 * height, oriented upwards, and with metre units. This is equivalent to using
 * proj_crs_promote_to_3D().
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name CRS name. Or NULL (in which case the name of projected_2D_crs
 * will be used)
 * @param projected_2D_crs Projected 2D CRS to be "promoted" to 3D. Must not be
 * NULL.
 * @param geog_3D_crs Base geographic 3D CRS for the new CRS. May be NULL.
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 * @since 6.3
 */
PJ *proj_crs_create_projected_3D_crs_from_2D(PJ_CONTEXT *ctx,
                                             const char *crs_name,
                                             const PJ *projected_2D_crs,
                                             const PJ *geog_3D_crs) {
    SANITIZE_CTX(ctx);
    if (!projected_2D_crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto cpp_projected_2D_crs =
        dynamic_cast<const ProjectedCRS *>(projected_2D_crs->iso_obj.get());
    if (!cpp_projected_2D_crs) {
        proj_log_error(ctx, __FUNCTION__,
                       "projected_2D_crs is not a Projected CRS");
        return nullptr;
    }
    const auto &oldCS = cpp_projected_2D_crs->coordinateSystem();
    const auto &oldCSAxisList = oldCS->axisList();

    if (geog_3D_crs && geog_3D_crs->iso_obj) {
        auto cpp_geog_3D_CRS =
            std::dynamic_pointer_cast<GeographicCRS>(geog_3D_crs->iso_obj);
        if (!cpp_geog_3D_CRS) {
            proj_log_error(ctx, __FUNCTION__,
                           "geog_3D_crs is not a Geographic CRS");
            return nullptr;
        }

        const auto &geogCS = cpp_geog_3D_CRS->coordinateSystem();
        const auto &geogCSAxisList = geogCS->axisList();
        if (geogCSAxisList.size() != 3) {
            proj_log_error(ctx, __FUNCTION__,
                           "geog_3D_crs is not a Geographic 3D CRS");
            return nullptr;
        }
        try {
            auto newCS =
                cs::CartesianCS::create(PropertyMap(), oldCSAxisList[0],
                                        oldCSAxisList[1], geogCSAxisList[2]);
            return pj_obj_create(
                ctx,
                ProjectedCRS::create(
                    createPropertyMapName(
                        crs_name ? crs_name
                                 : cpp_projected_2D_crs->nameStr().c_str()),
                    NN_NO_CHECK(cpp_geog_3D_CRS),
                    cpp_projected_2D_crs->derivingConversion(), newCS));
        } catch (const std::exception &e) {
            proj_log_error(ctx, __FUNCTION__, e.what());
            return nullptr;
        }
    } else {
        try {
            auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
            return pj_obj_create(ctx,
                                 cpp_projected_2D_crs->promoteTo3D(
                                     crs_name ? std::string(crs_name)
                                              : cpp_projected_2D_crs->nameStr(),
                                     dbContext));
        } catch (const std::exception &e) {
            proj_log_error(ctx, __FUNCTION__, e.what());
            return nullptr;
        }
    }
}

// ---------------------------------------------------------------------------

/** \brief Create a 2D CRS from an existing 3D CRS.
 *
 * See osgeo::proj::crs::CRS::demoteTo2D().
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_2D_name CRS name. Or NULL (in which case the name of crs_3D
 * will be used)
 * @param crs_3D 3D CRS to be "demoted" to 2D. Must not be NULL.
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 * @since 6.3
 */
PJ *proj_crs_demote_to_2D(PJ_CONTEXT *ctx, const char *crs_2D_name,
                          const PJ *crs_3D) {
    SANITIZE_CTX(ctx);
    if (!crs_3D) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto cpp_3D_crs = dynamic_cast<const CRS *>(crs_3D->iso_obj.get());
    if (!cpp_3D_crs) {
        proj_log_error(ctx, __FUNCTION__, "crs_3D is not a CRS");
        return nullptr;
    }
    try {
        auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
        return pj_obj_create(
            ctx, cpp_3D_crs->demoteTo2D(crs_2D_name ? std::string(crs_2D_name)
                                                    : cpp_3D_crs->nameStr(),
                                        dbContext));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a EngineeringCRS with just a name
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name CRS name. Or NULL.
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_create_engineering_crs(PJ_CONTEXT *ctx, const char *crs_name) {
    SANITIZE_CTX(ctx);
    try {
        return pj_obj_create(
            ctx, EngineeringCRS::create(
                     createPropertyMapName(crs_name),
                     EngineeringDatum::create(
                         createPropertyMapName(UNKNOWN_ENGINEERING_DATUM)),
                     CartesianCS::createEastingNorthing(UnitOfMeasure::METRE)));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static void setSingleOperationElements(
    const char *name, const char *auth_name, const char *code,
    const char *method_name, const char *method_auth_name,
    const char *method_code, int param_count,
    const PJ_PARAM_DESCRIPTION *params, PropertyMap &propSingleOp,
    PropertyMap &propMethod, std::vector<OperationParameterNNPtr> &parameters,
    std::vector<ParameterValueNNPtr> &values) {
    propSingleOp.set(common::IdentifiedObject::NAME_KEY,
                     name ? name : "unnamed");
    if (auth_name && code) {
        propSingleOp.set(metadata::Identifier::CODESPACE_KEY, auth_name)
            .set(metadata::Identifier::CODE_KEY, code);
    }

    propMethod.set(common::IdentifiedObject::NAME_KEY,
                   method_name ? method_name : "unnamed");
    if (method_auth_name && method_code) {
        propMethod.set(metadata::Identifier::CODESPACE_KEY, method_auth_name)
            .set(metadata::Identifier::CODE_KEY, method_code);
    }

    for (int i = 0; i < param_count; i++) {
        PropertyMap propParam;
        propParam.set(common::IdentifiedObject::NAME_KEY,
                      params[i].name ? params[i].name : "unnamed");
        if (params[i].auth_name && params[i].code) {
            propParam
                .set(metadata::Identifier::CODESPACE_KEY, params[i].auth_name)
                .set(metadata::Identifier::CODE_KEY, params[i].code);
        }
        parameters.emplace_back(OperationParameter::create(propParam));
        auto unit_type = UnitOfMeasure::Type::UNKNOWN;
        switch (params[i].unit_type) {
        case PJ_UT_ANGULAR:
            unit_type = UnitOfMeasure::Type::ANGULAR;
            break;
        case PJ_UT_LINEAR:
            unit_type = UnitOfMeasure::Type::LINEAR;
            break;
        case PJ_UT_SCALE:
            unit_type = UnitOfMeasure::Type::SCALE;
            break;
        case PJ_UT_TIME:
            unit_type = UnitOfMeasure::Type::TIME;
            break;
        case PJ_UT_PARAMETRIC:
            unit_type = UnitOfMeasure::Type::PARAMETRIC;
            break;
        }

        Measure measure(
            params[i].value,
            params[i].unit_type == PJ_UT_ANGULAR
                ? createAngularUnit(params[i].unit_name,
                                    params[i].unit_conv_factor)
            : params[i].unit_type == PJ_UT_LINEAR
                ? createLinearUnit(params[i].unit_name,
                                   params[i].unit_conv_factor)
                : UnitOfMeasure(params[i].unit_name ? params[i].unit_name
                                                    : "unnamed",
                                params[i].unit_conv_factor, unit_type));
        values.emplace_back(ParameterValue::create(measure));
    }
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a Conversion
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param name Conversion name. Or NULL.
 * @param auth_name Conversion authority name. Or NULL.
 * @param code Conversion code. Or NULL.
 * @param method_name Method name. Or NULL.
 * @param method_auth_name Method authority name. Or NULL.
 * @param method_code Method code. Or NULL.
 * @param param_count Number of parameters (size of params argument)
 * @param params Parameter descriptions (array of size param_count)
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */

PJ *proj_create_conversion(PJ_CONTEXT *ctx, const char *name,
                           const char *auth_name, const char *code,
                           const char *method_name,
                           const char *method_auth_name,
                           const char *method_code, int param_count,
                           const PJ_PARAM_DESCRIPTION *params) {
    SANITIZE_CTX(ctx);
    try {
        PropertyMap propSingleOp;
        PropertyMap propMethod;
        std::vector<OperationParameterNNPtr> parameters;
        std::vector<ParameterValueNNPtr> values;

        setSingleOperationElements(
            name, auth_name, code, method_name, method_auth_name, method_code,
            param_count, params, propSingleOp, propMethod, parameters, values);

        return pj_obj_create(ctx, Conversion::create(propSingleOp, propMethod,
                                                     parameters, values));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Transformation
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param name Transformation name. Or NULL.
 * @param auth_name Transformation authority name. Or NULL.
 * @param code Transformation code. Or NULL.
 * @param source_crs Object of type CRS representing the source CRS.
 * Must not be NULL.
 * @param target_crs Object of type CRS representing the target CRS.
 * Must not be NULL.
 * @param interpolation_crs Object of type CRS representing the interpolation
 * CRS. Or NULL.
 * @param method_name Method name. Or NULL.
 * @param method_auth_name Method authority name. Or NULL.
 * @param method_code Method code. Or NULL.
 * @param param_count Number of parameters (size of params argument)
 * @param params Parameter descriptions (array of size param_count)
 * @param accuracy Accuracy of the transformation in meters. A negative
 * values means unknown.
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */

PJ *proj_create_transformation(
    PJ_CONTEXT *ctx, const char *name, const char *auth_name, const char *code,
    const PJ *source_crs, const PJ *target_crs, const PJ *interpolation_crs,
    const char *method_name, const char *method_auth_name,
    const char *method_code, int param_count,
    const PJ_PARAM_DESCRIPTION *params, double accuracy) {
    SANITIZE_CTX(ctx);
    if (!source_crs || !target_crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }

    auto l_sourceCRS = std::dynamic_pointer_cast<CRS>(source_crs->iso_obj);
    if (!l_sourceCRS) {
        proj_log_error(ctx, __FUNCTION__, "source_crs is not a CRS");
        return nullptr;
    }

    auto l_targetCRS = std::dynamic_pointer_cast<CRS>(target_crs->iso_obj);
    if (!l_targetCRS) {
        proj_log_error(ctx, __FUNCTION__, "target_crs is not a CRS");
        return nullptr;
    }

    CRSPtr l_interpolationCRS;
    if (interpolation_crs) {
        l_interpolationCRS =
            std::dynamic_pointer_cast<CRS>(interpolation_crs->iso_obj);
        if (!l_interpolationCRS) {
            proj_log_error(ctx, __FUNCTION__, "interpolation_crs is not a CRS");
            return nullptr;
        }
    }

    try {
        PropertyMap propSingleOp;
        PropertyMap propMethod;
        std::vector<OperationParameterNNPtr> parameters;
        std::vector<ParameterValueNNPtr> values;

        setSingleOperationElements(
            name, auth_name, code, method_name, method_auth_name, method_code,
            param_count, params, propSingleOp, propMethod, parameters, values);

        std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
        if (accuracy >= 0.0) {
            accuracies.emplace_back(
                PositionalAccuracy::create(toString(accuracy)));
        }

        return pj_obj_create(
            ctx,
            Transformation::create(propSingleOp, NN_NO_CHECK(l_sourceCRS),
                                   NN_NO_CHECK(l_targetCRS), l_interpolationCRS,
                                   propMethod, parameters, values, accuracies));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/**
 * \brief Return an equivalent projection.
 *
 * Currently implemented:
 * <ul>
 * <li>EPSG_CODE_METHOD_MERCATOR_VARIANT_A (1SP) to
 * EPSG_CODE_METHOD_MERCATOR_VARIANT_B (2SP)</li>
 * <li>EPSG_CODE_METHOD_MERCATOR_VARIANT_B (2SP) to
 * EPSG_CODE_METHOD_MERCATOR_VARIANT_A (1SP)</li>
 * <li>EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP to
 * EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP</li>
 * <li>EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP to
 * EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP</li>
 * </ul>
 *
 * @param ctx PROJ context, or NULL for default context
 * @param conversion Object of type Conversion. Must not be NULL.
 * @param new_method_epsg_code EPSG code of the target method. Or 0 (in which
 * case new_method_name must be specified).
 * @param new_method_name EPSG or PROJ target method name. Or nullptr  (in which
 * case new_method_epsg_code must be specified).
 * @return new conversion that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */
PJ *proj_convert_conversion_to_other_method(PJ_CONTEXT *ctx,
                                            const PJ *conversion,
                                            int new_method_epsg_code,
                                            const char *new_method_name) {
    SANITIZE_CTX(ctx);
    if (!conversion) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto conv = dynamic_cast<const Conversion *>(conversion->iso_obj.get());
    if (!conv) {
        proj_log_error(ctx, __FUNCTION__, "not a Conversion");
        return nullptr;
    }
    if (new_method_epsg_code == 0) {
        if (!new_method_name) {
            return nullptr;
        }
        if (metadata::Identifier::isEquivalentName(
                new_method_name, EPSG_NAME_METHOD_MERCATOR_VARIANT_A)) {
            new_method_epsg_code = EPSG_CODE_METHOD_MERCATOR_VARIANT_A;
        } else if (metadata::Identifier::isEquivalentName(
                       new_method_name, EPSG_NAME_METHOD_MERCATOR_VARIANT_B)) {
            new_method_epsg_code = EPSG_CODE_METHOD_MERCATOR_VARIANT_B;
        } else if (metadata::Identifier::isEquivalentName(
                       new_method_name,
                       EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_1SP)) {
            new_method_epsg_code = EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP;
        } else if (metadata::Identifier::isEquivalentName(
                       new_method_name,
                       EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_2SP)) {
            new_method_epsg_code = EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP;
        }
    }
    try {
        auto new_conv = conv->convertToOtherMethod(new_method_epsg_code);
        if (!new_conv)
            return nullptr;
        return pj_obj_create(ctx, NN_NO_CHECK(new_conv));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static CoordinateSystemAxisNNPtr createAxis(const PJ_AXIS_DESCRIPTION &axis) {
    const auto dir =
        axis.direction ? AxisDirection::valueOf(axis.direction) : nullptr;
    if (dir == nullptr)
        throw Exception("invalid value for axis direction");
    auto unit_type = UnitOfMeasure::Type::UNKNOWN;
    switch (axis.unit_type) {
    case PJ_UT_ANGULAR:
        unit_type = UnitOfMeasure::Type::ANGULAR;
        break;
    case PJ_UT_LINEAR:
        unit_type = UnitOfMeasure::Type::LINEAR;
        break;
    case PJ_UT_SCALE:
        unit_type = UnitOfMeasure::Type::SCALE;
        break;
    case PJ_UT_TIME:
        unit_type = UnitOfMeasure::Type::TIME;
        break;
    case PJ_UT_PARAMETRIC:
        unit_type = UnitOfMeasure::Type::PARAMETRIC;
        break;
    }
    const common::UnitOfMeasure unit(
        axis.unit_type == PJ_UT_ANGULAR
            ? createAngularUnit(axis.unit_name, axis.unit_conv_factor)
        : axis.unit_type == PJ_UT_LINEAR
            ? createLinearUnit(axis.unit_name, axis.unit_conv_factor)
            : UnitOfMeasure(axis.unit_name ? axis.unit_name : "unnamed",
                            axis.unit_conv_factor, unit_type));

    return CoordinateSystemAxis::create(
        createPropertyMapName(axis.name),
        axis.abbreviation ? axis.abbreviation : std::string(), *dir, unit);
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a CoordinateSystem.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param type Coordinate system type.
 * @param axis_count Number of axis
 * @param axis Axis description (array of size axis_count)
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */

PJ *proj_create_cs(PJ_CONTEXT *ctx, PJ_COORDINATE_SYSTEM_TYPE type,
                   int axis_count, const PJ_AXIS_DESCRIPTION *axis) {
    SANITIZE_CTX(ctx);
    try {
        switch (type) {
        case PJ_CS_TYPE_UNKNOWN:
            return nullptr;

        case PJ_CS_TYPE_CARTESIAN: {
            if (axis_count == 2) {
                return pj_obj_create(
                    ctx, CartesianCS::create(PropertyMap(), createAxis(axis[0]),
                                             createAxis(axis[1])));
            } else if (axis_count == 3) {
                return pj_obj_create(
                    ctx, CartesianCS::create(PropertyMap(), createAxis(axis[0]),
                                             createAxis(axis[1]),
                                             createAxis(axis[2])));
            }
            break;
        }

        case PJ_CS_TYPE_ELLIPSOIDAL: {
            if (axis_count == 2) {
                return pj_obj_create(
                    ctx,
                    EllipsoidalCS::create(PropertyMap(), createAxis(axis[0]),
                                          createAxis(axis[1])));
            } else if (axis_count == 3) {
                return pj_obj_create(
                    ctx, EllipsoidalCS::create(
                             PropertyMap(), createAxis(axis[0]),
                             createAxis(axis[1]), createAxis(axis[2])));
            }
            break;
        }

        case PJ_CS_TYPE_VERTICAL: {
            if (axis_count == 1) {
                return pj_obj_create(
                    ctx,
                    VerticalCS::create(PropertyMap(), createAxis(axis[0])));
            }
            break;
        }

        case PJ_CS_TYPE_SPHERICAL: {
            if (axis_count == 3) {
                return pj_obj_create(
                    ctx, EllipsoidalCS::create(
                             PropertyMap(), createAxis(axis[0]),
                             createAxis(axis[1]), createAxis(axis[2])));
            }
            break;
        }

        case PJ_CS_TYPE_PARAMETRIC: {
            if (axis_count == 1) {
                return pj_obj_create(
                    ctx,
                    ParametricCS::create(PropertyMap(), createAxis(axis[0])));
            }
            break;
        }

        case PJ_CS_TYPE_ORDINAL: {
            std::vector<CoordinateSystemAxisNNPtr> axisVector;
            for (int i = 0; i < axis_count; i++) {
                axisVector.emplace_back(createAxis(axis[i]));
            }

            return pj_obj_create(ctx,
                                 OrdinalCS::create(PropertyMap(), axisVector));
        }

        case PJ_CS_TYPE_DATETIMETEMPORAL: {
            if (axis_count == 1) {
                return pj_obj_create(
                    ctx, DateTimeTemporalCS::create(PropertyMap(),
                                                    createAxis(axis[0])));
            }
            break;
        }

        case PJ_CS_TYPE_TEMPORALCOUNT: {
            if (axis_count == 1) {
                return pj_obj_create(
                    ctx, TemporalCountCS::create(PropertyMap(),
                                                 createAxis(axis[0])));
            }
            break;
        }

        case PJ_CS_TYPE_TEMPORALMEASURE: {
            if (axis_count == 1) {
                return pj_obj_create(
                    ctx, TemporalMeasureCS::create(PropertyMap(),
                                                   createAxis(axis[0])));
            }
            break;
        }
        }

    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
    proj_log_error(ctx, __FUNCTION__, "Wrong value for axis_count");
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CartesiansCS 2D
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param type Coordinate system type.
 * @param unit_name Unit name.
 * @param unit_conv_factor Unit conversion factor to SI.
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */

PJ *proj_create_cartesian_2D_cs(PJ_CONTEXT *ctx, PJ_CARTESIAN_CS_2D_TYPE type,
                                const char *unit_name,
                                double unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        switch (type) {
        case PJ_CART2D_EASTING_NORTHING:
            return pj_obj_create(
                ctx, CartesianCS::createEastingNorthing(
                         createLinearUnit(unit_name, unit_conv_factor)));

        case PJ_CART2D_NORTHING_EASTING:
            return pj_obj_create(
                ctx, CartesianCS::createNorthingEasting(
                         createLinearUnit(unit_name, unit_conv_factor)));

        case PJ_CART2D_NORTH_POLE_EASTING_SOUTH_NORTHING_SOUTH:
            return pj_obj_create(
                ctx, CartesianCS::createNorthPoleEastingSouthNorthingSouth(
                         createLinearUnit(unit_name, unit_conv_factor)));

        case PJ_CART2D_SOUTH_POLE_EASTING_NORTH_NORTHING_NORTH:
            return pj_obj_create(
                ctx, CartesianCS::createSouthPoleEastingNorthNorthingNorth(
                         createLinearUnit(unit_name, unit_conv_factor)));

        case PJ_CART2D_WESTING_SOUTHING:
            return pj_obj_create(
                ctx, CartesianCS::createWestingSouthing(
                         createLinearUnit(unit_name, unit_conv_factor)));
        }
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Ellipsoidal 2D
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param type Coordinate system type.
 * @param unit_name Name of the angular units. Or NULL for Degree
 * @param unit_conv_factor Conversion factor from the angular unit to radian.
 * Or 0 for Degree if unit_name == NULL. Otherwise should be not NULL
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */

PJ *proj_create_ellipsoidal_2D_cs(PJ_CONTEXT *ctx,
                                  PJ_ELLIPSOIDAL_CS_2D_TYPE type,
                                  const char *unit_name,
                                  double unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        switch (type) {
        case PJ_ELLPS2D_LONGITUDE_LATITUDE:
            return pj_obj_create(
                ctx, EllipsoidalCS::createLongitudeLatitude(
                         createAngularUnit(unit_name, unit_conv_factor)));

        case PJ_ELLPS2D_LATITUDE_LONGITUDE:
            return pj_obj_create(
                ctx, EllipsoidalCS::createLatitudeLongitude(
                         createAngularUnit(unit_name, unit_conv_factor)));
        }
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Ellipsoidal 3D
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param type Coordinate system type.
 * @param horizontal_angular_unit_name Name of the angular units. Or NULL for
 * Degree.
 * @param horizontal_angular_unit_conv_factor Conversion factor from the angular
 * unit to radian. Or 0 for Degree if horizontal_angular_unit_name == NULL.
 * Otherwise should be not NULL
 * @param vertical_linear_unit_name Vertical linear unit name. Or NULL for
 * Metre.
 * @param vertical_linear_unit_conv_factor Vertical linear unit conversion
 * factor to metre. Or 0 for Metre if vertical_linear_unit_name == NULL.
 * Otherwise should be not NULL

 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 * @since 6.3
 */

PJ *proj_create_ellipsoidal_3D_cs(PJ_CONTEXT *ctx,
                                  PJ_ELLIPSOIDAL_CS_3D_TYPE type,
                                  const char *horizontal_angular_unit_name,
                                  double horizontal_angular_unit_conv_factor,
                                  const char *vertical_linear_unit_name,
                                  double vertical_linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        switch (type) {
        case PJ_ELLPS3D_LONGITUDE_LATITUDE_HEIGHT:
            return pj_obj_create(
                ctx, EllipsoidalCS::createLongitudeLatitudeEllipsoidalHeight(
                         createAngularUnit(horizontal_angular_unit_name,
                                           horizontal_angular_unit_conv_factor),
                         createLinearUnit(vertical_linear_unit_name,
                                          vertical_linear_unit_conv_factor)));

        case PJ_ELLPS3D_LATITUDE_LONGITUDE_HEIGHT:
            return pj_obj_create(
                ctx, EllipsoidalCS::createLatitudeLongitudeEllipsoidalHeight(
                         createAngularUnit(horizontal_angular_unit_name,
                                           horizontal_angular_unit_conv_factor),
                         createLinearUnit(vertical_linear_unit_name,
                                          vertical_linear_unit_conv_factor)));
        }
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs_name CRS name. Or NULL
 * @param geodetic_crs Base GeodeticCRS. Must not be NULL.
 * @param conversion Conversion. Must not be NULL.
 * @param coordinate_system Cartesian coordinate system. Must not be NULL.
 *
 * @return Object that must be unreferenced with
 * proj_destroy(), or NULL in case of error.
 */

PJ *proj_create_projected_crs(PJ_CONTEXT *ctx, const char *crs_name,
                              const PJ *geodetic_crs, const PJ *conversion,
                              const PJ *coordinate_system) {
    SANITIZE_CTX(ctx);
    if (!geodetic_crs || !conversion || !coordinate_system) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto geodCRS =
        std::dynamic_pointer_cast<GeodeticCRS>(geodetic_crs->iso_obj);
    if (!geodCRS) {
        return nullptr;
    }
    auto conv = std::dynamic_pointer_cast<Conversion>(conversion->iso_obj);
    if (!conv) {
        return nullptr;
    }
    auto cs =
        std::dynamic_pointer_cast<CartesianCS>(coordinate_system->iso_obj);
    if (!cs) {
        return nullptr;
    }
    try {
        return pj_obj_create(
            ctx, ProjectedCRS::create(createPropertyMapName(crs_name),
                                      NN_NO_CHECK(geodCRS), NN_NO_CHECK(conv),
                                      NN_NO_CHECK(cs)));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static PJ *proj_create_conversion(PJ_CONTEXT *ctx,
                                  const ConversionNNPtr &conv) {
    return pj_obj_create(ctx, conv);
}

//! @endcond

/* BEGIN: Generated by scripts/create_c_api_projections.py*/

// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a Universal Transverse Mercator
 * conversion.
 *
 * See osgeo::proj::operation::Conversion::createUTM().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 */
PJ *proj_create_conversion_utm(PJ_CONTEXT *ctx, int zone, int north) {
    SANITIZE_CTX(ctx);
    try {
        auto conv = Conversion::createUTM(PropertyMap(), zone, north != 0);
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Transverse
 * Mercator projection method.
 *
 * See osgeo::proj::operation::Conversion::createTransverseMercator().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_transverse_mercator(
    PJ_CONTEXT *ctx, double center_lat, double center_long, double scale,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createTransverseMercator(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Gauss
 * Schreiber Transverse Mercator projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createGaussSchreiberTransverseMercator().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_gauss_schreiber_transverse_mercator(
    PJ_CONTEXT *ctx, double center_lat, double center_long, double scale,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createGaussSchreiberTransverseMercator(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Transverse
 * Mercator South Orientated projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createTransverseMercatorSouthOriented().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_transverse_mercator_south_oriented(
    PJ_CONTEXT *ctx, double center_lat, double center_long, double scale,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createTransverseMercatorSouthOriented(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Two Point
 * Equidistant projection method.
 *
 * See osgeo::proj::operation::Conversion::createTwoPointEquidistant().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_two_point_equidistant(
    PJ_CONTEXT *ctx, double latitude_first_point, double longitude_first_point,
    double latitude_second_point, double longitude_secon_point,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createTwoPointEquidistant(
            PropertyMap(), Angle(latitude_first_point, angUnit),
            Angle(longitude_first_point, angUnit),
            Angle(latitude_second_point, angUnit),
            Angle(longitude_secon_point, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Tunisia
 * Mining Grid projection method.
 *
 * See osgeo::proj::operation::Conversion::createTunisiaMiningGrid().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 *
 * @since 9.2
 */
PJ *proj_create_conversion_tunisia_mining_grid(
    PJ_CONTEXT *ctx, double center_lat, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createTunisiaMiningGrid(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Tunisia
 * Mining Grid projection method.
 *
 * See osgeo::proj::operation::Conversion::createTunisiaMiningGrid().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 *
 * @deprecated Replaced by proj_create_conversion_tunisia_mining_grid
 */
PJ *proj_create_conversion_tunisia_mapping_grid(
    PJ_CONTEXT *ctx, double center_lat, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createTunisiaMiningGrid(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Albers
 * Conic Equal Area projection method.
 *
 * See osgeo::proj::operation::Conversion::createAlbersEqualArea().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_albers_equal_area(
    PJ_CONTEXT *ctx, double latitude_false_origin,
    double longitude_false_origin, double latitude_first_parallel,
    double latitude_second_parallel, double easting_false_origin,
    double northing_false_origin, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createAlbersEqualArea(
            PropertyMap(), Angle(latitude_false_origin, angUnit),
            Angle(longitude_false_origin, angUnit),
            Angle(latitude_first_parallel, angUnit),
            Angle(latitude_second_parallel, angUnit),
            Length(easting_false_origin, linearUnit),
            Length(northing_false_origin, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Lambert
 * Conic Conformal 1SP projection method.
 *
 * See osgeo::proj::operation::Conversion::createLambertConicConformal_1SP().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_lambert_conic_conformal_1sp(
    PJ_CONTEXT *ctx, double center_lat, double center_long, double scale,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLambertConicConformal_1SP(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Lambert
 * Conic Conformal (1SP Variant B) projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createLambertConicConformal_1SP_VariantB().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 * @since 9.2.1
 */
PJ *proj_create_conversion_lambert_conic_conformal_1sp_variant_b(
    PJ_CONTEXT *ctx, double latitude_nat_origin, double scale,
    double latitude_false_origin, double longitude_false_origin,
    double easting_false_origin, double northing_false_origin,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLambertConicConformal_1SP_VariantB(
            PropertyMap(), Angle(latitude_nat_origin, angUnit), Scale(scale),
            Angle(latitude_false_origin, angUnit),
            Angle(longitude_false_origin, angUnit),
            Length(easting_false_origin, linearUnit),
            Length(northing_false_origin, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Lambert
 * Conic Conformal (2SP) projection method.
 *
 * See osgeo::proj::operation::Conversion::createLambertConicConformal_2SP().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_lambert_conic_conformal_2sp(
    PJ_CONTEXT *ctx, double latitude_false_origin,
    double longitude_false_origin, double latitude_first_parallel,
    double latitude_second_parallel, double easting_false_origin,
    double northing_false_origin, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLambertConicConformal_2SP(
            PropertyMap(), Angle(latitude_false_origin, angUnit),
            Angle(longitude_false_origin, angUnit),
            Angle(latitude_first_parallel, angUnit),
            Angle(latitude_second_parallel, angUnit),
            Length(easting_false_origin, linearUnit),
            Length(northing_false_origin, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Lambert
 * Conic Conformal (2SP Michigan) projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createLambertConicConformal_2SP_Michigan().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_lambert_conic_conformal_2sp_michigan(
    PJ_CONTEXT *ctx, double latitude_false_origin,
    double longitude_false_origin, double latitude_first_parallel,
    double latitude_second_parallel, double easting_false_origin,
    double northing_false_origin, double ellipsoid_scaling_factor,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLambertConicConformal_2SP_Michigan(
            PropertyMap(), Angle(latitude_false_origin, angUnit),
            Angle(longitude_false_origin, angUnit),
            Angle(latitude_first_parallel, angUnit),
            Angle(latitude_second_parallel, angUnit),
            Length(easting_false_origin, linearUnit),
            Length(northing_false_origin, linearUnit),
            Scale(ellipsoid_scaling_factor));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Lambert
 * Conic Conformal (2SP Belgium) projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createLambertConicConformal_2SP_Belgium().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_lambert_conic_conformal_2sp_belgium(
    PJ_CONTEXT *ctx, double latitude_false_origin,
    double longitude_false_origin, double latitude_first_parallel,
    double latitude_second_parallel, double easting_false_origin,
    double northing_false_origin, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLambertConicConformal_2SP_Belgium(
            PropertyMap(), Angle(latitude_false_origin, angUnit),
            Angle(longitude_false_origin, angUnit),
            Angle(latitude_first_parallel, angUnit),
            Angle(latitude_second_parallel, angUnit),
            Length(easting_false_origin, linearUnit),
            Length(northing_false_origin, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Modified
 * Azimuthal Equidistant projection method.
 *
 * See osgeo::proj::operation::Conversion::createAzimuthalEquidistant().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_azimuthal_equidistant(
    PJ_CONTEXT *ctx, double latitude_nat_origin, double longitude_nat_origin,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createAzimuthalEquidistant(
            PropertyMap(), Angle(latitude_nat_origin, angUnit),
            Angle(longitude_nat_origin, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Guam
 * Projection projection method.
 *
 * See osgeo::proj::operation::Conversion::createGuamProjection().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_guam_projection(
    PJ_CONTEXT *ctx, double latitude_nat_origin, double longitude_nat_origin,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createGuamProjection(
            PropertyMap(), Angle(latitude_nat_origin, angUnit),
            Angle(longitude_nat_origin, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Bonne
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createBonne().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_bonne(PJ_CONTEXT *ctx, double latitude_nat_origin,
                                 double longitude_nat_origin,
                                 double false_easting, double false_northing,
                                 const char *ang_unit_name,
                                 double ang_unit_conv_factor,
                                 const char *linear_unit_name,
                                 double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createBonne(
            PropertyMap(), Angle(latitude_nat_origin, angUnit),
            Angle(longitude_nat_origin, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Lambert
 * Cylindrical Equal Area (Spherical) projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createLambertCylindricalEqualAreaSpherical().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_lambert_cylindrical_equal_area_spherical(
    PJ_CONTEXT *ctx, double latitude_first_parallel,
    double longitude_nat_origin, double false_easting, double false_northing,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLambertCylindricalEqualAreaSpherical(
            PropertyMap(), Angle(latitude_first_parallel, angUnit),
            Angle(longitude_nat_origin, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Lambert
 * Cylindrical Equal Area (ellipsoidal form) projection method.
 *
 * See osgeo::proj::operation::Conversion::createLambertCylindricalEqualArea().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_lambert_cylindrical_equal_area(
    PJ_CONTEXT *ctx, double latitude_first_parallel,
    double longitude_nat_origin, double false_easting, double false_northing,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLambertCylindricalEqualArea(
            PropertyMap(), Angle(latitude_first_parallel, angUnit),
            Angle(longitude_nat_origin, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the
 * Cassini-Soldner projection method.
 *
 * See osgeo::proj::operation::Conversion::createCassiniSoldner().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_cassini_soldner(
    PJ_CONTEXT *ctx, double center_lat, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createCassiniSoldner(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Equidistant
 * Conic projection method.
 *
 * See osgeo::proj::operation::Conversion::createEquidistantConic().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_equidistant_conic(
    PJ_CONTEXT *ctx, double center_lat, double center_long,
    double latitude_first_parallel, double latitude_second_parallel,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEquidistantConic(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit),
            Angle(latitude_first_parallel, angUnit),
            Angle(latitude_second_parallel, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Eckert I
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createEckertI().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_eckert_i(PJ_CONTEXT *ctx, double center_long,
                                    double false_easting, double false_northing,
                                    const char *ang_unit_name,
                                    double ang_unit_conv_factor,
                                    const char *linear_unit_name,
                                    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEckertI(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Eckert II
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createEckertII().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_eckert_ii(PJ_CONTEXT *ctx, double center_long,
                                     double false_easting,
                                     double false_northing,
                                     const char *ang_unit_name,
                                     double ang_unit_conv_factor,
                                     const char *linear_unit_name,
                                     double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEckertII(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Eckert III
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createEckertIII().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_eckert_iii(PJ_CONTEXT *ctx, double center_long,
                                      double false_easting,
                                      double false_northing,
                                      const char *ang_unit_name,
                                      double ang_unit_conv_factor,
                                      const char *linear_unit_name,
                                      double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEckertIII(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Eckert IV
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createEckertIV().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_eckert_iv(PJ_CONTEXT *ctx, double center_long,
                                     double false_easting,
                                     double false_northing,
                                     const char *ang_unit_name,
                                     double ang_unit_conv_factor,
                                     const char *linear_unit_name,
                                     double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEckertIV(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Eckert V
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createEckertV().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_eckert_v(PJ_CONTEXT *ctx, double center_long,
                                    double false_easting, double false_northing,
                                    const char *ang_unit_name,
                                    double ang_unit_conv_factor,
                                    const char *linear_unit_name,
                                    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEckertV(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Eckert VI
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createEckertVI().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_eckert_vi(PJ_CONTEXT *ctx, double center_long,
                                     double false_easting,
                                     double false_northing,
                                     const char *ang_unit_name,
                                     double ang_unit_conv_factor,
                                     const char *linear_unit_name,
                                     double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEckertVI(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Equidistant
 * Cylindrical projection method.
 *
 * See osgeo::proj::operation::Conversion::createEquidistantCylindrical().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_equidistant_cylindrical(
    PJ_CONTEXT *ctx, double latitude_first_parallel,
    double longitude_nat_origin, double false_easting, double false_northing,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEquidistantCylindrical(
            PropertyMap(), Angle(latitude_first_parallel, angUnit),
            Angle(longitude_nat_origin, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Equidistant
 * Cylindrical (Spherical) projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createEquidistantCylindricalSpherical().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_equidistant_cylindrical_spherical(
    PJ_CONTEXT *ctx, double latitude_first_parallel,
    double longitude_nat_origin, double false_easting, double false_northing,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEquidistantCylindricalSpherical(
            PropertyMap(), Angle(latitude_first_parallel, angUnit),
            Angle(longitude_nat_origin, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Gall
 * (Stereographic) projection method.
 *
 * See osgeo::proj::operation::Conversion::createGall().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_gall(PJ_CONTEXT *ctx, double center_long,
                                double false_easting, double false_northing,
                                const char *ang_unit_name,
                                double ang_unit_conv_factor,
                                const char *linear_unit_name,
                                double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv =
            Conversion::createGall(PropertyMap(), Angle(center_long, angUnit),
                                   Length(false_easting, linearUnit),
                                   Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Goode
 * Homolosine projection method.
 *
 * See osgeo::proj::operation::Conversion::createGoodeHomolosine().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_goode_homolosine(PJ_CONTEXT *ctx, double center_long,
                                            double false_easting,
                                            double false_northing,
                                            const char *ang_unit_name,
                                            double ang_unit_conv_factor,
                                            const char *linear_unit_name,
                                            double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createGoodeHomolosine(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Interrupted
 * Goode Homolosine projection method.
 *
 * See osgeo::proj::operation::Conversion::createInterruptedGoodeHomolosine().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_interrupted_goode_homolosine(
    PJ_CONTEXT *ctx, double center_long, double false_easting,
    double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createInterruptedGoodeHomolosine(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the
 * Geostationary Satellite View projection method, with the sweep angle axis of
 * the viewing instrument being x.
 *
 * See osgeo::proj::operation::Conversion::createGeostationarySatelliteSweepX().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_geostationary_satellite_sweep_x(
    PJ_CONTEXT *ctx, double center_long, double height, double false_easting,
    double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createGeostationarySatelliteSweepX(
            PropertyMap(), Angle(center_long, angUnit),
            Length(height, linearUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the
 * Geostationary Satellite View projection method, with the sweep angle axis of
 * the viewing instrument being y.
 *
 * See osgeo::proj::operation::Conversion::createGeostationarySatelliteSweepY().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_geostationary_satellite_sweep_y(
    PJ_CONTEXT *ctx, double center_long, double height, double false_easting,
    double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createGeostationarySatelliteSweepY(
            PropertyMap(), Angle(center_long, angUnit),
            Length(height, linearUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Gnomonic
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createGnomonic().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_gnomonic(PJ_CONTEXT *ctx, double center_lat,
                                    double center_long, double false_easting,
                                    double false_northing,
                                    const char *ang_unit_name,
                                    double ang_unit_conv_factor,
                                    const char *linear_unit_name,
                                    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createGnomonic(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Hotine
 * Oblique Mercator (Variant A) projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createHotineObliqueMercatorVariantA().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_hotine_oblique_mercator_variant_a(
    PJ_CONTEXT *ctx, double latitude_projection_centre,
    double longitude_projection_centre, double azimuth_initial_line,
    double angle_from_rectified_to_skrew_grid, double scale,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createHotineObliqueMercatorVariantA(
            PropertyMap(), Angle(latitude_projection_centre, angUnit),
            Angle(longitude_projection_centre, angUnit),
            Angle(azimuth_initial_line, angUnit),
            Angle(angle_from_rectified_to_skrew_grid, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Hotine
 * Oblique Mercator (Variant B) projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createHotineObliqueMercatorVariantB().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_hotine_oblique_mercator_variant_b(
    PJ_CONTEXT *ctx, double latitude_projection_centre,
    double longitude_projection_centre, double azimuth_initial_line,
    double angle_from_rectified_to_skrew_grid, double scale,
    double easting_projection_centre, double northing_projection_centre,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createHotineObliqueMercatorVariantB(
            PropertyMap(), Angle(latitude_projection_centre, angUnit),
            Angle(longitude_projection_centre, angUnit),
            Angle(azimuth_initial_line, angUnit),
            Angle(angle_from_rectified_to_skrew_grid, angUnit), Scale(scale),
            Length(easting_projection_centre, linearUnit),
            Length(northing_projection_centre, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Hotine
 * Oblique Mercator Two Point Natural Origin projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createHotineObliqueMercatorTwoPointNaturalOrigin().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_hotine_oblique_mercator_two_point_natural_origin(
    PJ_CONTEXT *ctx, double latitude_projection_centre, double latitude_point1,
    double longitude_point1, double latitude_point2, double longitude_point2,
    double scale, double easting_projection_centre,
    double northing_projection_centre, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv =
            Conversion::createHotineObliqueMercatorTwoPointNaturalOrigin(
                PropertyMap(), Angle(latitude_projection_centre, angUnit),
                Angle(latitude_point1, angUnit),
                Angle(longitude_point1, angUnit),
                Angle(latitude_point2, angUnit),
                Angle(longitude_point2, angUnit), Scale(scale),
                Length(easting_projection_centre, linearUnit),
                Length(northing_projection_centre, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Laborde
 * Oblique Mercator projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createLabordeObliqueMercator().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_laborde_oblique_mercator(
    PJ_CONTEXT *ctx, double latitude_projection_centre,
    double longitude_projection_centre, double azimuth_initial_line,
    double scale, double false_easting, double false_northing,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLabordeObliqueMercator(
            PropertyMap(), Angle(latitude_projection_centre, angUnit),
            Angle(longitude_projection_centre, angUnit),
            Angle(azimuth_initial_line, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the
 * International Map of the World Polyconic projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createInternationalMapWorldPolyconic().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_international_map_world_polyconic(
    PJ_CONTEXT *ctx, double center_long, double latitude_first_parallel,
    double latitude_second_parallel, double false_easting,
    double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createInternationalMapWorldPolyconic(
            PropertyMap(), Angle(center_long, angUnit),
            Angle(latitude_first_parallel, angUnit),
            Angle(latitude_second_parallel, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Krovak
 * (north oriented) projection method.
 *
 * See osgeo::proj::operation::Conversion::createKrovakNorthOriented().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_krovak_north_oriented(
    PJ_CONTEXT *ctx, double latitude_projection_centre,
    double longitude_of_origin, double colatitude_cone_axis,
    double latitude_pseudo_standard_parallel,
    double scale_factor_pseudo_standard_parallel, double false_easting,
    double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createKrovakNorthOriented(
            PropertyMap(), Angle(latitude_projection_centre, angUnit),
            Angle(longitude_of_origin, angUnit),
            Angle(colatitude_cone_axis, angUnit),
            Angle(latitude_pseudo_standard_parallel, angUnit),
            Scale(scale_factor_pseudo_standard_parallel),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Krovak
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createKrovak().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_krovak(
    PJ_CONTEXT *ctx, double latitude_projection_centre,
    double longitude_of_origin, double colatitude_cone_axis,
    double latitude_pseudo_standard_parallel,
    double scale_factor_pseudo_standard_parallel, double false_easting,
    double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createKrovak(
            PropertyMap(), Angle(latitude_projection_centre, angUnit),
            Angle(longitude_of_origin, angUnit),
            Angle(colatitude_cone_axis, angUnit),
            Angle(latitude_pseudo_standard_parallel, angUnit),
            Scale(scale_factor_pseudo_standard_parallel),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Lambert
 * Azimuthal Equal Area projection method.
 *
 * See osgeo::proj::operation::Conversion::createLambertAzimuthalEqualArea().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_lambert_azimuthal_equal_area(
    PJ_CONTEXT *ctx, double latitude_nat_origin, double longitude_nat_origin,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLambertAzimuthalEqualArea(
            PropertyMap(), Angle(latitude_nat_origin, angUnit),
            Angle(longitude_nat_origin, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Miller
 * Cylindrical projection method.
 *
 * See osgeo::proj::operation::Conversion::createMillerCylindrical().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_miller_cylindrical(
    PJ_CONTEXT *ctx, double center_long, double false_easting,
    double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createMillerCylindrical(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Mercator
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createMercatorVariantA().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_mercator_variant_a(
    PJ_CONTEXT *ctx, double center_lat, double center_long, double scale,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createMercatorVariantA(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Mercator
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createMercatorVariantB().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_mercator_variant_b(
    PJ_CONTEXT *ctx, double latitude_first_parallel, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createMercatorVariantB(
            PropertyMap(), Angle(latitude_first_parallel, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Popular
 * Visualisation Pseudo Mercator projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createPopularVisualisationPseudoMercator().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_popular_visualisation_pseudo_mercator(
    PJ_CONTEXT *ctx, double center_lat, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createPopularVisualisationPseudoMercator(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Mollweide
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createMollweide().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_mollweide(PJ_CONTEXT *ctx, double center_long,
                                     double false_easting,
                                     double false_northing,
                                     const char *ang_unit_name,
                                     double ang_unit_conv_factor,
                                     const char *linear_unit_name,
                                     double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createMollweide(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the New Zealand
 * Map Grid projection method.
 *
 * See osgeo::proj::operation::Conversion::createNewZealandMappingGrid().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_new_zealand_mapping_grid(
    PJ_CONTEXT *ctx, double center_lat, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createNewZealandMappingGrid(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Oblique
 * Stereographic (Alternative) projection method.
 *
 * See osgeo::proj::operation::Conversion::createObliqueStereographic().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_oblique_stereographic(
    PJ_CONTEXT *ctx, double center_lat, double center_long, double scale,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createObliqueStereographic(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the
 * Orthographic projection method.
 *
 * See osgeo::proj::operation::Conversion::createOrthographic().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_orthographic(
    PJ_CONTEXT *ctx, double center_lat, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createOrthographic(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Local
 * Orthographic projection method.
 *
 * See osgeo::proj::operation::Conversion::createLocalOrthographic().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_local_orthographic(
    PJ_CONTEXT *ctx, double center_lat, double center_long, double azimuth,
    double scale, double false_easting, double false_northing,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createLocalOrthographic(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Angle(azimuth, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the American
 * Polyconic projection method.
 *
 * See osgeo::proj::operation::Conversion::createAmericanPolyconic().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_american_polyconic(
    PJ_CONTEXT *ctx, double center_lat, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createAmericanPolyconic(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Polar
 * Stereographic (Variant A) projection method.
 *
 * See osgeo::proj::operation::Conversion::createPolarStereographicVariantA().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_polar_stereographic_variant_a(
    PJ_CONTEXT *ctx, double center_lat, double center_long, double scale,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createPolarStereographicVariantA(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Polar
 * Stereographic (Variant B) projection method.
 *
 * See osgeo::proj::operation::Conversion::createPolarStereographicVariantB().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_polar_stereographic_variant_b(
    PJ_CONTEXT *ctx, double latitude_standard_parallel,
    double longitude_of_origin, double false_easting, double false_northing,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createPolarStereographicVariantB(
            PropertyMap(), Angle(latitude_standard_parallel, angUnit),
            Angle(longitude_of_origin, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Robinson
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createRobinson().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_robinson(PJ_CONTEXT *ctx, double center_long,
                                    double false_easting, double false_northing,
                                    const char *ang_unit_name,
                                    double ang_unit_conv_factor,
                                    const char *linear_unit_name,
                                    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createRobinson(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Sinusoidal
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createSinusoidal().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_sinusoidal(PJ_CONTEXT *ctx, double center_long,
                                      double false_easting,
                                      double false_northing,
                                      const char *ang_unit_name,
                                      double ang_unit_conv_factor,
                                      const char *linear_unit_name,
                                      double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createSinusoidal(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the
 * Stereographic projection method.
 *
 * See osgeo::proj::operation::Conversion::createStereographic().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_stereographic(
    PJ_CONTEXT *ctx, double center_lat, double center_long, double scale,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createStereographic(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Scale(scale),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Van der
 * Grinten projection method.
 *
 * See osgeo::proj::operation::Conversion::createVanDerGrinten().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_van_der_grinten(PJ_CONTEXT *ctx, double center_long,
                                           double false_easting,
                                           double false_northing,
                                           const char *ang_unit_name,
                                           double ang_unit_conv_factor,
                                           const char *linear_unit_name,
                                           double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createVanDerGrinten(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Wagner I
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createWagnerI().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_wagner_i(PJ_CONTEXT *ctx, double center_long,
                                    double false_easting, double false_northing,
                                    const char *ang_unit_name,
                                    double ang_unit_conv_factor,
                                    const char *linear_unit_name,
                                    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createWagnerI(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Wagner II
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createWagnerII().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_wagner_ii(PJ_CONTEXT *ctx, double center_long,
                                     double false_easting,
                                     double false_northing,
                                     const char *ang_unit_name,
                                     double ang_unit_conv_factor,
                                     const char *linear_unit_name,
                                     double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createWagnerII(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Wagner III
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createWagnerIII().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_wagner_iii(
    PJ_CONTEXT *ctx, double latitude_true_scale, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createWagnerIII(
            PropertyMap(), Angle(latitude_true_scale, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Wagner IV
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createWagnerIV().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_wagner_iv(PJ_CONTEXT *ctx, double center_long,
                                     double false_easting,
                                     double false_northing,
                                     const char *ang_unit_name,
                                     double ang_unit_conv_factor,
                                     const char *linear_unit_name,
                                     double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createWagnerIV(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Wagner V
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createWagnerV().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_wagner_v(PJ_CONTEXT *ctx, double center_long,
                                    double false_easting, double false_northing,
                                    const char *ang_unit_name,
                                    double ang_unit_conv_factor,
                                    const char *linear_unit_name,
                                    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createWagnerV(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Wagner VI
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createWagnerVI().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_wagner_vi(PJ_CONTEXT *ctx, double center_long,
                                     double false_easting,
                                     double false_northing,
                                     const char *ang_unit_name,
                                     double ang_unit_conv_factor,
                                     const char *linear_unit_name,
                                     double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createWagnerVI(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Wagner VII
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createWagnerVII().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_wagner_vii(PJ_CONTEXT *ctx, double center_long,
                                      double false_easting,
                                      double false_northing,
                                      const char *ang_unit_name,
                                      double ang_unit_conv_factor,
                                      const char *linear_unit_name,
                                      double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createWagnerVII(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the
 * Quadrilateralized Spherical Cube projection method.
 *
 * See
 * osgeo::proj::operation::Conversion::createQuadrilateralizedSphericalCube().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_quadrilateralized_spherical_cube(
    PJ_CONTEXT *ctx, double center_lat, double center_long,
    double false_easting, double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createQuadrilateralizedSphericalCube(
            PropertyMap(), Angle(center_lat, angUnit),
            Angle(center_long, angUnit), Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Spherical
 * Cross-Track Height projection method.
 *
 * See osgeo::proj::operation::Conversion::createSphericalCrossTrackHeight().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_spherical_cross_track_height(
    PJ_CONTEXT *ctx, double peg_point_lat, double peg_point_long,
    double peg_point_heading, double peg_point_height,
    const char *ang_unit_name, double ang_unit_conv_factor,
    const char *linear_unit_name, double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createSphericalCrossTrackHeight(
            PropertyMap(), Angle(peg_point_lat, angUnit),
            Angle(peg_point_long, angUnit), Angle(peg_point_heading, angUnit),
            Length(peg_point_height, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS with a conversion based on the Equal Earth
 * projection method.
 *
 * See osgeo::proj::operation::Conversion::createEqualEarth().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_equal_earth(PJ_CONTEXT *ctx, double center_long,
                                       double false_easting,
                                       double false_northing,
                                       const char *ang_unit_name,
                                       double ang_unit_conv_factor,
                                       const char *linear_unit_name,
                                       double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createEqualEarth(
            PropertyMap(), Angle(center_long, angUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Vertical Perspective projection
 * method.
 *
 * See osgeo::proj::operation::Conversion::createVerticalPerspective().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 *
 * @since 6.3
 */
PJ *proj_create_conversion_vertical_perspective(
    PJ_CONTEXT *ctx, double topo_origin_lat, double topo_origin_long,
    double topo_origin_height, double view_point_height, double false_easting,
    double false_northing, const char *ang_unit_name,
    double ang_unit_conv_factor, const char *linear_unit_name,
    double linear_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure linearUnit(
            createLinearUnit(linear_unit_name, linear_unit_conv_factor));
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createVerticalPerspective(
            PropertyMap(), Angle(topo_origin_lat, angUnit),
            Angle(topo_origin_long, angUnit),
            Length(topo_origin_height, linearUnit),
            Length(view_point_height, linearUnit),
            Length(false_easting, linearUnit),
            Length(false_northing, linearUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Pole Rotation method, using the
 * conventions of the GRIB 1 and GRIB 2 data formats.
 *
 * See osgeo::proj::operation::Conversion::createPoleRotationGRIBConvention().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_pole_rotation_grib_convention(
    PJ_CONTEXT *ctx, double south_pole_lat_in_unrotated_crs,
    double south_pole_long_in_unrotated_crs, double axis_rotation,
    const char *ang_unit_name, double ang_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createPoleRotationGRIBConvention(
            PropertyMap(), Angle(south_pole_lat_in_unrotated_crs, angUnit),
            Angle(south_pole_long_in_unrotated_crs, angUnit),
            Angle(axis_rotation, angUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Pole Rotation method, using
 * the conventions of the netCDF CF convention for the netCDF format.
 *
 * See
 * osgeo::proj::operation::Conversion::createPoleRotationNetCDFCFConvention().
 *
 * Linear parameters are expressed in (linear_unit_name,
 * linear_unit_conv_factor).
 * Angular parameters are expressed in (ang_unit_name, ang_unit_conv_factor).
 */
PJ *proj_create_conversion_pole_rotation_netcdf_cf_convention(
    PJ_CONTEXT *ctx, double grid_north_pole_latitude,
    double grid_north_pole_longitude, double north_pole_grid_longitude,
    const char *ang_unit_name, double ang_unit_conv_factor) {
    SANITIZE_CTX(ctx);
    try {
        UnitOfMeasure angUnit(
            createAngularUnit(ang_unit_name, ang_unit_conv_factor));
        auto conv = Conversion::createPoleRotationNetCDFCFConvention(
            PropertyMap(), Angle(grid_north_pole_latitude, angUnit),
            Angle(grid_north_pole_longitude, angUnit),
            Angle(north_pole_grid_longitude, angUnit));
        return proj_create_conversion(ctx, conv);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

/* END: Generated by scripts/create_c_api_projections.py*/

// ---------------------------------------------------------------------------

/** \brief Return whether a coordinate operation can be instantiated as
 * a PROJ pipeline, checking in particular that referenced grids are
 * available.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type CoordinateOperation or derived classes
 * (must not be NULL)
 * @return TRUE or FALSE.
 */

int proj_coordoperation_is_instantiable(PJ_CONTEXT *ctx,
                                        const PJ *coordoperation) {
    SANITIZE_CTX(ctx);
    if (!coordoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto op = dynamic_cast<const CoordinateOperation *>(
        coordoperation->iso_obj.get());
    if (!op) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a CoordinateOperation");
        return 0;
    }
    auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
    try {
        auto ret = op->isPROJInstantiable(
                       dbContext, proj_context_is_network_enabled(ctx) != FALSE)
                       ? 1
                       : 0;
        return ret;
    } catch (const std::exception &) {
        return 0;
    }
}

// ---------------------------------------------------------------------------

/** \brief Return whether a coordinate operation has a "ballpark"
 * transformation,
 * that is a very approximate one, due to lack of more accurate transformations.
 *
 * Typically a null geographic offset between two horizontal datum, or a
 * null vertical offset (or limited to unit changes) between two vertical
 * datum. Errors of several tens to one hundred meters might be expected,
 * compared to more accurate transformations.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type CoordinateOperation or derived classes
 * (must not be NULL)
 * @return TRUE or FALSE.
 */

int proj_coordoperation_has_ballpark_transformation(PJ_CONTEXT *ctx,
                                                    const PJ *coordoperation) {
    SANITIZE_CTX(ctx);
    if (!coordoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto op = dynamic_cast<const CoordinateOperation *>(
        coordoperation->iso_obj.get());
    if (!op) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a CoordinateOperation");
        return 0;
    }
    return op->hasBallparkTransformation();
}

// ---------------------------------------------------------------------------

/** \brief Return whether a coordinate operation requires coordinate tuples
 * to have a valid input time for the coordinate transformation to succeed.
 * (this applies for the forward direction)
 *
 * Note: in the case of a time-dependent Helmert transformation, this function
 * will return true, but when executing proj_trans(), execution will still
 * succeed if the time information is missing, due to the transformation central
 * epoch being used as a fallback.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type CoordinateOperation or derived classes
 * (must not be NULL)
 * @return TRUE or FALSE.
 * @since 9.5
 */

int proj_coordoperation_requires_per_coordinate_input_time(
    PJ_CONTEXT *ctx, const PJ *coordoperation) {
    SANITIZE_CTX(ctx);
    if (!coordoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto op = dynamic_cast<const CoordinateOperation *>(
        coordoperation->iso_obj.get());
    if (!op) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a CoordinateOperation");
        return false;
    }
    return op->requiresPerCoordinateInputTime();
}

// ---------------------------------------------------------------------------

/** \brief Return the number of parameters of a SingleOperation
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type SingleOperation or derived classes
 * (must not be NULL)
 */

int proj_coordoperation_get_param_count(PJ_CONTEXT *ctx,
                                        const PJ *coordoperation) {
    SANITIZE_CTX(ctx);
    if (!coordoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto op =
        dynamic_cast<const SingleOperation *>(coordoperation->iso_obj.get());
    if (!op) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a SingleOperation");
        return 0;
    }
    return static_cast<int>(op->parameterValues().size());
}

// ---------------------------------------------------------------------------

/** \brief Return the index of a parameter of a SingleOperation
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type SingleOperation or derived classes
 * (must not be NULL)
 * @param name Parameter name. Must not be NULL
 * @return index (>=0), or -1 in case of error.
 */

int proj_coordoperation_get_param_index(PJ_CONTEXT *ctx,
                                        const PJ *coordoperation,
                                        const char *name) {
    SANITIZE_CTX(ctx);
    if (!coordoperation || !name) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return -1;
    }
    auto op =
        dynamic_cast<const SingleOperation *>(coordoperation->iso_obj.get());
    if (!op) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a SingleOperation");
        return -1;
    }
    int index = 0;
    for (const auto &genParam : op->method()->parameters()) {
        if (Identifier::isEquivalentName(genParam->nameStr().c_str(), name)) {
            return index;
        }
        index++;
    }
    return -1;
}

// ---------------------------------------------------------------------------

/** \brief Return a parameter of a SingleOperation
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type SingleOperation or derived classes
 * (must not be NULL)
 * @param index Parameter index.
 * @param out_name Pointer to a string value to store the parameter name. or
 * NULL
 * @param out_auth_name Pointer to a string value to store the parameter
 * authority name. or NULL
 * @param out_code Pointer to a string value to store the parameter
 * code. or NULL
 * @param out_value Pointer to a double value to store the parameter
 * value (if numeric). or NULL
 * @param out_value_string Pointer to a string value to store the parameter
 * value (if of type string). or NULL
 * @param out_unit_conv_factor Pointer to a double value to store the parameter
 * unit conversion factor. or NULL
 * @param out_unit_name Pointer to a string value to store the parameter
 * unit name. or NULL
 * @param out_unit_auth_name Pointer to a string value to store the
 * unit authority name. or NULL
 * @param out_unit_code Pointer to a string value to store the
 * unit code. or NULL
 * @param out_unit_category Pointer to a string value to store the parameter
 * name. or
 * NULL. This value might be "unknown", "none", "linear", "linear_per_time",
 * "angular", "angular_per_time", "scale", "scale_per_time", "time",
 * "parametric" or "parametric_per_time"
 * @return TRUE in case of success.
 */

int proj_coordoperation_get_param(
    PJ_CONTEXT *ctx, const PJ *coordoperation, int index, const char **out_name,
    const char **out_auth_name, const char **out_code, double *out_value,
    const char **out_value_string, double *out_unit_conv_factor,
    const char **out_unit_name, const char **out_unit_auth_name,
    const char **out_unit_code, const char **out_unit_category) {
    SANITIZE_CTX(ctx);
    if (!coordoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto op =
        dynamic_cast<const SingleOperation *>(coordoperation->iso_obj.get());
    if (!op) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a SingleOperation");
        return false;
    }
    const auto &parameters = op->method()->parameters();
    const auto &values = op->parameterValues();
    if (static_cast<size_t>(index) >= parameters.size() ||
        static_cast<size_t>(index) >= values.size()) {
        proj_log_error(ctx, __FUNCTION__, "Invalid index");
        return false;
    }

    const auto &param = parameters[index];
    const auto &param_ids = param->identifiers();
    if (out_name) {
        *out_name = param->name()->description()->c_str();
    }
    if (out_auth_name) {
        if (!param_ids.empty()) {
            *out_auth_name = param_ids[0]->codeSpace()->c_str();
        } else {
            *out_auth_name = nullptr;
        }
    }
    if (out_code) {
        if (!param_ids.empty()) {
            *out_code = param_ids[0]->code().c_str();
        } else {
            *out_code = nullptr;
        }
    }

    const auto &value = values[index];
    ParameterValuePtr paramValue = nullptr;
    auto opParamValue =
        dynamic_cast<const OperationParameterValue *>(value.get());
    if (opParamValue) {
        paramValue = opParamValue->parameterValue().as_nullable();
    }
    if (out_value) {
        *out_value = 0;
        if (paramValue) {
            if (paramValue->type() == ParameterValue::Type::MEASURE) {
                *out_value = paramValue->value().value();
            }
        }
    }
    if (out_value_string) {
        *out_value_string = nullptr;
        if (paramValue) {
            if (paramValue->type() == ParameterValue::Type::FILENAME) {
                *out_value_string = paramValue->valueFile().c_str();
            } else if (paramValue->type() == ParameterValue::Type::STRING) {
                *out_value_string = paramValue->stringValue().c_str();
            }
        }
    }
    if (out_unit_conv_factor) {
        *out_unit_conv_factor = 0;
    }
    if (out_unit_name) {
        *out_unit_name = nullptr;
    }
    if (out_unit_auth_name) {
        *out_unit_auth_name = nullptr;
    }
    if (out_unit_code) {
        *out_unit_code = nullptr;
    }
    if (out_unit_category) {
        *out_unit_category = nullptr;
    }
    if (paramValue) {
        if (paramValue->type() == ParameterValue::Type::MEASURE) {
            const auto &unit = paramValue->value().unit();
            if (out_unit_conv_factor) {
                *out_unit_conv_factor = unit.conversionToSI();
            }
            if (out_unit_name) {
                *out_unit_name = unit.name().c_str();
            }
            if (out_unit_auth_name) {
                *out_unit_auth_name = unit.codeSpace().c_str();
            }
            if (out_unit_code) {
                *out_unit_code = unit.code().c_str();
            }
            if (out_unit_category) {
                *out_unit_category =
                    get_unit_category(unit.name(), unit.type());
            }
        }
    }

    return true;
}

// ---------------------------------------------------------------------------

/** \brief Return the parameters of a Helmert transformation as WKT1 TOWGS84
 * values.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type Transformation, that can be represented
 * as a WKT1 TOWGS84 node (must not be NULL)
 * @param out_values Pointer to an array of value_count double values.
 * @param value_count Size of out_values array. The suggested size is 7 to get
 * translation, rotation and scale difference parameters. Rotation and scale
 * difference terms might be zero if the transformation only includes
 * translation
 * parameters. In that case, value_count could be set to 3.
 * @param emit_error_if_incompatible Boolean to indicate if an error must be
 * logged if coordoperation is not compatible with a WKT1 TOWGS84
 * representation.
 * @return TRUE in case of success, or FALSE if coordoperation is not
 * compatible with a WKT1 TOWGS84 representation.
 */

int proj_coordoperation_get_towgs84_values(PJ_CONTEXT *ctx,
                                           const PJ *coordoperation,
                                           double *out_values, int value_count,
                                           int emit_error_if_incompatible) {
    SANITIZE_CTX(ctx);
    if (!coordoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto transf =
        dynamic_cast<const Transformation *>(coordoperation->iso_obj.get());
    if (!transf) {
        if (emit_error_if_incompatible) {
            proj_log_error(ctx, __FUNCTION__, "Object is not a Transformation");
        }
        return FALSE;
    }

    const auto values = transf->getTOWGS84Parameters(false);
    if (!values.empty()) {
        for (int i = 0;
             i < value_count && static_cast<size_t>(i) < values.size(); i++) {
            out_values[i] = values[i];
        }
        return TRUE;
    } else {
        if (emit_error_if_incompatible) {
            proj_log_error(ctx, __FUNCTION__,
                           "Transformation cannot be formatted as WKT1 TOWGS84 "
                           "parameters");
        }
        return FALSE;
    }
}

// ---------------------------------------------------------------------------

/** \brief Return the number of grids used by a CoordinateOperation
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type CoordinateOperation or derived classes
 * (must not be NULL)
 */

int proj_coordoperation_get_grid_used_count(PJ_CONTEXT *ctx,
                                            const PJ *coordoperation) {
    SANITIZE_CTX(ctx);
    if (!coordoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto co = dynamic_cast<const CoordinateOperation *>(
        coordoperation->iso_obj.get());
    if (!co) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a CoordinateOperation");
        return 0;
    }
    auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
    try {
        if (!coordoperation->gridsNeededAsked) {
            coordoperation->gridsNeededAsked = true;
            const auto gridsNeeded = co->gridsNeeded(
                dbContext, proj_context_is_network_enabled(ctx) != FALSE);
            for (const auto &gridDesc : gridsNeeded) {
                coordoperation->gridsNeeded.emplace_back(gridDesc);
            }
        }
        return static_cast<int>(coordoperation->gridsNeeded.size());
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return 0;
    }
}

// ---------------------------------------------------------------------------

/** \brief Return a parameter of a SingleOperation
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Object of type SingleOperation or derived classes
 * (must not be NULL)
 * @param index Parameter index.
 * @param out_short_name Pointer to a string value to store the grid short name.
 * or NULL
 * @param out_full_name Pointer to a string value to store the grid full
 * filename. or NULL
 * @param out_package_name Pointer to a string value to store the package name
 * where
 * the grid might be found. or NULL
 * @param out_url Pointer to a string value to store the grid URL or the
 * package URL where the grid might be found. or NULL
 * @param out_direct_download Pointer to a int (boolean) value to store whether
 * *out_url can be downloaded directly. or NULL
 * @param out_open_license Pointer to a int (boolean) value to store whether
 * the grid is released with an open license. or NULL
 * @param out_available Pointer to a int (boolean) value to store whether the
 * grid is available at runtime. or NULL
 * @return TRUE in case of success.
 */

int proj_coordoperation_get_grid_used(
    PJ_CONTEXT *ctx, const PJ *coordoperation, int index,
    const char **out_short_name, const char **out_full_name,
    const char **out_package_name, const char **out_url,
    int *out_direct_download, int *out_open_license, int *out_available) {
    SANITIZE_CTX(ctx);
    const int count =
        proj_coordoperation_get_grid_used_count(ctx, coordoperation);
    if (index < 0 || index >= count) {
        proj_log_error(ctx, __FUNCTION__, "Invalid index");
        return false;
    }

    const auto &gridDesc = coordoperation->gridsNeeded[index];
    if (out_short_name) {
        *out_short_name = gridDesc.shortName.c_str();
    }

    if (out_full_name) {
        *out_full_name = gridDesc.fullName.c_str();
    }

    if (out_package_name) {
        *out_package_name = gridDesc.packageName.c_str();
    }

    if (out_url) {
        *out_url = gridDesc.url.c_str();
    }

    if (out_direct_download) {
        *out_direct_download = gridDesc.directDownload;
    }

    if (out_open_license) {
        *out_open_license = gridDesc.openLicense;
    }

    if (out_available) {
        *out_available = gridDesc.available;
    }

    return true;
}

// ---------------------------------------------------------------------------

/** \brief Opaque object representing an operation factory context. */
struct PJ_OPERATION_FACTORY_CONTEXT {
    //! @cond Doxygen_Suppress
    CoordinateOperationContextNNPtr operationContext;

    explicit PJ_OPERATION_FACTORY_CONTEXT(
        CoordinateOperationContextNNPtr &&operationContextIn)
        : operationContext(std::move(operationContextIn)) {}

    PJ_OPERATION_FACTORY_CONTEXT(const PJ_OPERATION_FACTORY_CONTEXT &) = delete;
    PJ_OPERATION_FACTORY_CONTEXT &
    operator=(const PJ_OPERATION_FACTORY_CONTEXT &) = delete;
    //! @endcond
};

// ---------------------------------------------------------------------------

/** \brief Instantiate a context for building coordinate operations between
 * two CRS.
 *
 * The returned object must be unreferenced with
 * proj_operation_factory_context_destroy() after use.
 *
 * If authority is NULL or the empty string, then coordinate
 * operations from any authority will be searched, with the restrictions set
 * in the authority_to_authority_preference database table.
 * If authority is set to "any", then coordinate
 * operations from any authority will be searched
 * If authority is a non-empty string different of "any",
 * then coordinate operations will be searched only in that authority namespace.
 *
 * @param ctx Context, or NULL for default context.
 * @param authority Name of authority to which to restrict the search of
 *                  candidate operations.
 * @return Object that must be unreferenced with
 * proj_operation_factory_context_destroy(), or NULL in
 * case of error.
 */
PJ_OPERATION_FACTORY_CONTEXT *
proj_create_operation_factory_context(PJ_CONTEXT *ctx, const char *authority) {
    SANITIZE_CTX(ctx);
    auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
    try {
        if (dbContext) {
            auto factory = CoordinateOperationFactory::create();
            auto authFactory = AuthorityFactory::create(
                NN_NO_CHECK(dbContext),
                std::string(authority ? authority : ""));
            auto operationContext =
                CoordinateOperationContext::create(authFactory, nullptr, 0.0);
            return new PJ_OPERATION_FACTORY_CONTEXT(
                std::move(operationContext));
        } else {
            auto operationContext =
                CoordinateOperationContext::create(nullptr, nullptr, 0.0);
            return new PJ_OPERATION_FACTORY_CONTEXT(
                std::move(operationContext));
        }
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Drops a reference on an object.
 *
 * This method should be called one and exactly one for each function
 * returning a PJ_OPERATION_FACTORY_CONTEXT*
 *
 * @param ctx Object, or NULL.
 */
void proj_operation_factory_context_destroy(PJ_OPERATION_FACTORY_CONTEXT *ctx) {
    delete ctx;
}

// ---------------------------------------------------------------------------

/** \brief Set the desired accuracy of the resulting coordinate transformations.
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param accuracy Accuracy in meter (or 0 to disable the filter).
 */
void proj_operation_factory_context_set_desired_accuracy(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
    double accuracy) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        factory_ctx->operationContext->setDesiredAccuracy(accuracy);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Set the desired area of interest for the resulting coordinate
 * transformations.
 *
 * For an area of interest crossing the anti-meridian, west_lon_degree will be
 * greater than east_lon_degree.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param west_lon_degree West longitude (in degrees).
 * @param south_lat_degree South latitude (in degrees).
 * @param east_lon_degree East longitude (in degrees).
 * @param north_lat_degree North latitude (in degrees).
 */
void proj_operation_factory_context_set_area_of_interest(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
    double west_lon_degree, double south_lat_degree, double east_lon_degree,
    double north_lat_degree) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        factory_ctx->operationContext->setAreaOfInterest(
            Extent::createFromBBOX(west_lon_degree, south_lat_degree,
                                   east_lon_degree, north_lat_degree));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Set the name of the desired area of interest for the resulting
 * coordinate transformations.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param area_name Area name. Must be known of the database.
 */
void proj_operation_factory_context_set_area_of_interest_name(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
    const char *area_name) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx || !area_name) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        auto extent = factory_ctx->operationContext->getAreaOfInterest();
        if (extent == nullptr) {
            auto dbContext = getDBcontext(ctx);
            auto factory = AuthorityFactory::create(dbContext, std::string());
            auto res = factory->listAreaOfUseFromName(area_name, false);
            if (res.size() == 1) {
                factory_ctx->operationContext->setAreaOfInterest(
                    AuthorityFactory::create(dbContext, res.front().first)
                        ->createExtent(res.front().second)
                        .as_nullable());
            } else {
                proj_log_error(ctx, __FUNCTION__, "cannot find area");
                return;
            }
        } else {
            factory_ctx->operationContext->setAreaOfInterest(
                metadata::Extent::create(util::optional<std::string>(area_name),
                                         extent->geographicElements(),
                                         extent->verticalElements(),
                                         extent->temporalElements())
                    .as_nullable());
        }
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Set how source and target CRS extent should be used
 * when considering if a transformation can be used (only takes effect if
 * no area of interest is explicitly defined).
 *
 * The default is PJ_CRS_EXTENT_SMALLEST.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param use How source and target CRS extent should be used.
 */
void proj_operation_factory_context_set_crs_extent_use(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
    PROJ_CRS_EXTENT_USE use) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        switch (use) {
        case PJ_CRS_EXTENT_NONE:
            factory_ctx->operationContext->setSourceAndTargetCRSExtentUse(
                CoordinateOperationContext::SourceTargetCRSExtentUse::NONE);
            break;

        case PJ_CRS_EXTENT_BOTH:
            factory_ctx->operationContext->setSourceAndTargetCRSExtentUse(
                CoordinateOperationContext::SourceTargetCRSExtentUse::BOTH);
            break;

        case PJ_CRS_EXTENT_INTERSECTION:
            factory_ctx->operationContext->setSourceAndTargetCRSExtentUse(
                CoordinateOperationContext::SourceTargetCRSExtentUse::
                    INTERSECTION);
            break;

        case PJ_CRS_EXTENT_SMALLEST:
            factory_ctx->operationContext->setSourceAndTargetCRSExtentUse(
                CoordinateOperationContext::SourceTargetCRSExtentUse::SMALLEST);
            break;
        }
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Set the spatial criterion to use when comparing the area of
 * validity of coordinate operations with the area of interest / area of
 * validity of
 * source and target CRS.
 *
 * The default is PROJ_SPATIAL_CRITERION_STRICT_CONTAINMENT.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param criterion spatial criterion to use
 */
void proj_operation_factory_context_set_spatial_criterion(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
    PROJ_SPATIAL_CRITERION criterion) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        switch (criterion) {
        case PROJ_SPATIAL_CRITERION_STRICT_CONTAINMENT:
            factory_ctx->operationContext->setSpatialCriterion(
                CoordinateOperationContext::SpatialCriterion::
                    STRICT_CONTAINMENT);
            break;

        case PROJ_SPATIAL_CRITERION_PARTIAL_INTERSECTION:
            factory_ctx->operationContext->setSpatialCriterion(
                CoordinateOperationContext::SpatialCriterion::
                    PARTIAL_INTERSECTION);
            break;
        }
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Set how grid availability is used.
 *
 * The default is USE_FOR_SORTING.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param use how grid availability is used.
 */
void proj_operation_factory_context_set_grid_availability_use(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
    PROJ_GRID_AVAILABILITY_USE use) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        switch (use) {
        case PROJ_GRID_AVAILABILITY_USED_FOR_SORTING:
            factory_ctx->operationContext->setGridAvailabilityUse(
                CoordinateOperationContext::GridAvailabilityUse::
                    USE_FOR_SORTING);
            break;

        case PROJ_GRID_AVAILABILITY_DISCARD_OPERATION_IF_MISSING_GRID:
            factory_ctx->operationContext->setGridAvailabilityUse(
                CoordinateOperationContext::GridAvailabilityUse::
                    DISCARD_OPERATION_IF_MISSING_GRID);
            break;

        case PROJ_GRID_AVAILABILITY_IGNORED:
            factory_ctx->operationContext->setGridAvailabilityUse(
                CoordinateOperationContext::GridAvailabilityUse::
                    IGNORE_GRID_AVAILABILITY);
            break;

        case PROJ_GRID_AVAILABILITY_KNOWN_AVAILABLE:
            factory_ctx->operationContext->setGridAvailabilityUse(
                CoordinateOperationContext::GridAvailabilityUse::
                    KNOWN_AVAILABLE);
            break;
        }
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Set whether PROJ alternative grid names should be substituted to
 * the official authority names.
 *
 * The default is true.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param usePROJNames whether PROJ alternative grid names should be used
 */
void proj_operation_factory_context_set_use_proj_alternative_grid_names(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
    int usePROJNames) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        factory_ctx->operationContext->setUsePROJAlternativeGridNames(
            usePROJNames != 0);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Set whether an intermediate pivot CRS can be used for researching
 * coordinate operations between a source and target CRS.
 *
 * Concretely if in the database there is an operation from A to C
 * (or C to A), and another one from C to B (or B to C), but no direct
 * operation between A and B, setting this parameter to true, allow
 * chaining both operations.
 *
 * The current implementation is limited to researching one intermediate
 * step.
 *
 * By default, with the IF_NO_DIRECT_TRANSFORMATION strategy, all potential
 * C candidates will be used if there is no direct transformation.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param use whether and how intermediate CRS may be used.
 */
void proj_operation_factory_context_set_allow_use_intermediate_crs(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
    PROJ_INTERMEDIATE_CRS_USE use) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        switch (use) {
        case PROJ_INTERMEDIATE_CRS_USE_ALWAYS:
            factory_ctx->operationContext->setAllowUseIntermediateCRS(
                CoordinateOperationContext::IntermediateCRSUse::ALWAYS);
            break;

        case PROJ_INTERMEDIATE_CRS_USE_IF_NO_DIRECT_TRANSFORMATION:
            factory_ctx->operationContext->setAllowUseIntermediateCRS(
                CoordinateOperationContext::IntermediateCRSUse::
                    IF_NO_DIRECT_TRANSFORMATION);
            break;

        case PROJ_INTERMEDIATE_CRS_USE_NEVER:
            factory_ctx->operationContext->setAllowUseIntermediateCRS(
                CoordinateOperationContext::IntermediateCRSUse::NEVER);
            break;
        }
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Restrict the potential pivot CRSs that can be used when trying to
 * build a coordinate operation between two CRS that have no direct operation.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param list_of_auth_name_codes an array of strings NLL terminated,
 * with the format { "auth_name1", "code1", "auth_name2", "code2", ... NULL }
 */
void proj_operation_factory_context_set_allowed_intermediate_crs(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
    const char *const *list_of_auth_name_codes) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        std::vector<std::pair<std::string, std::string>> pivots;
        for (auto iter = list_of_auth_name_codes; iter && iter[0] && iter[1];
             iter += 2) {
            pivots.emplace_back(std::pair<std::string, std::string>(
                std::string(iter[0]), std::string(iter[1])));
        }
        factory_ctx->operationContext->setIntermediateCRS(pivots);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Set whether transformations that are superseded (but not deprecated)
 * should be discarded.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param discard superseded crs or not
 */
void proj_operation_factory_context_set_discard_superseded(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx, int discard) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        factory_ctx->operationContext->setDiscardSuperseded(discard != 0);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

/** \brief Set whether ballpark transformations are allowed.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param factory_ctx Operation factory context. must not be NULL
 * @param allow set to TRUE to allow ballpark transformations.
 * @since 7.1
 */
void proj_operation_factory_context_set_allow_ballpark_transformations(
    PJ_CONTEXT *ctx, PJ_OPERATION_FACTORY_CONTEXT *factory_ctx, int allow) {
    SANITIZE_CTX(ctx);
    if (!factory_ctx) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return;
    }
    try {
        factory_ctx->operationContext->setAllowBallparkTransformations(allow !=
                                                                       0);
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
/** \brief Opaque object representing a set of operation results. */
struct PJ_OPERATION_LIST : PJ_OBJ_LIST {

    PJ *source_crs;
    PJ *target_crs;
    bool hasPreparedOperation = false;
    std::vector<PJCoordOperation> preparedOperations{};

    explicit PJ_OPERATION_LIST(PJ_CONTEXT *ctx, const PJ *source_crsIn,
                               const PJ *target_crsIn,
                               std::vector<IdentifiedObjectNNPtr> &&objectsIn);
    ~PJ_OPERATION_LIST() override;

    PJ_OPERATION_LIST(const PJ_OPERATION_LIST &) = delete;
    PJ_OPERATION_LIST &operator=(const PJ_OPERATION_LIST &) = delete;

    const std::vector<PJCoordOperation> &getPreparedOperations(PJ_CONTEXT *ctx);
};

// ---------------------------------------------------------------------------

PJ_OPERATION_LIST::PJ_OPERATION_LIST(
    PJ_CONTEXT *ctx, const PJ *source_crsIn, const PJ *target_crsIn,
    std::vector<IdentifiedObjectNNPtr> &&objectsIn)
    : PJ_OBJ_LIST(std::move(objectsIn)),
      source_crs(proj_clone(ctx, source_crsIn)),
      target_crs(proj_clone(ctx, target_crsIn)) {}

// ---------------------------------------------------------------------------

PJ_OPERATION_LIST::~PJ_OPERATION_LIST() {
    auto tmpCtxt = proj_context_create();
    proj_assign_context(source_crs, tmpCtxt);
    proj_assign_context(target_crs, tmpCtxt);
    proj_destroy(source_crs);
    proj_destroy(target_crs);
    proj_context_destroy(tmpCtxt);
}

// ---------------------------------------------------------------------------

const std::vector<PJCoordOperation> &
PJ_OPERATION_LIST::getPreparedOperations(PJ_CONTEXT *ctx) {
    if (!hasPreparedOperation) {
        hasPreparedOperation = true;
        preparedOperations =
            pj_create_prepared_operations(ctx, source_crs, target_crs, this);
    }
    return preparedOperations;
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Find a list of CoordinateOperation from source_crs to target_crs.
 *
 * The operations are sorted with the most relevant ones first: by
 * descending
 * area (intersection of the transformation area with the area of interest,
 * or intersection of the transformation with the area of use of the CRS),
 * and
 * by increasing accuracy. Operations with unknown accuracy are sorted last,
 * whatever their area.
 *
 * Starting with PROJ 9.1, vertical transformations are only done if both
 * source CRS and target CRS are 3D CRS or Compound CRS with a vertical
 * component. You may need to use proj_crs_promote_to_3D().
 *
 * @param ctx PROJ context, or NULL for default context
 * @param source_crs source CRS. Must not be NULL.
 * @param target_crs source CRS. Must not be NULL.
 * @param operationContext Search context. Must not be NULL.
 * @return a result set that must be unreferenced with
 * proj_list_destroy(), or NULL in case of error.
 */
PJ_OBJ_LIST *
proj_create_operations(PJ_CONTEXT *ctx, const PJ *source_crs,
                       const PJ *target_crs,
                       const PJ_OPERATION_FACTORY_CONTEXT *operationContext) {
    SANITIZE_CTX(ctx);
    if (!source_crs || !target_crs || !operationContext) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto sourceCRS = std::dynamic_pointer_cast<CRS>(source_crs->iso_obj);
    CoordinateMetadataPtr sourceCoordinateMetadata;
    if (!sourceCRS) {
        sourceCoordinateMetadata =
            std::dynamic_pointer_cast<CoordinateMetadata>(source_crs->iso_obj);
        if (!sourceCoordinateMetadata) {
            proj_log_error(ctx, __FUNCTION__,
                           "source_crs is not a CRS or a CoordinateMetadata");
            return nullptr;
        }
        if (!sourceCoordinateMetadata->coordinateEpoch().has_value()) {
            sourceCRS = sourceCoordinateMetadata->crs().as_nullable();
            sourceCoordinateMetadata.reset();
        }
    }
    auto targetCRS = std::dynamic_pointer_cast<CRS>(target_crs->iso_obj);
    CoordinateMetadataPtr targetCoordinateMetadata;
    if (!targetCRS) {
        targetCoordinateMetadata =
            std::dynamic_pointer_cast<CoordinateMetadata>(target_crs->iso_obj);
        if (!targetCoordinateMetadata) {
            proj_log_error(ctx, __FUNCTION__,
                           "target_crs is not a CRS or a CoordinateMetadata");
            return nullptr;
        }
        if (!targetCoordinateMetadata->coordinateEpoch().has_value()) {
            targetCRS = targetCoordinateMetadata->crs().as_nullable();
            targetCoordinateMetadata.reset();
        }
    }

    try {
        auto factory = CoordinateOperationFactory::create();
        std::vector<IdentifiedObjectNNPtr> objects;
        auto ops = sourceCoordinateMetadata != nullptr
                       ? (targetCoordinateMetadata != nullptr
                              ? factory->createOperations(
                                    NN_NO_CHECK(sourceCoordinateMetadata),
                                    NN_NO_CHECK(targetCoordinateMetadata),
                                    operationContext->operationContext)
                              : factory->createOperations(
                                    NN_NO_CHECK(sourceCoordinateMetadata),
                                    NN_NO_CHECK(targetCRS),
                                    operationContext->operationContext))
                   : targetCoordinateMetadata != nullptr
                       ? factory->createOperations(
                             NN_NO_CHECK(sourceCRS),
                             NN_NO_CHECK(targetCoordinateMetadata),
                             operationContext->operationContext)
                       : factory->createOperations(
                             NN_NO_CHECK(sourceCRS), NN_NO_CHECK(targetCRS),
                             operationContext->operationContext);
        for (const auto &op : ops) {
            objects.emplace_back(op);
        }
        return new PJ_OPERATION_LIST(ctx, source_crs, target_crs,
                                     std::move(objects));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** Return the index of the operation that would be the most appropriate to
 * transform the specified coordinates.
 *
 * This operation may use resources that are not locally available, depending
 * on the search criteria used by proj_create_operations().
 *
 * This could be done by using proj_create_operations() with a punctual bounding
 * box, but this function is faster when one needs to evaluate on many points
 * with the same (source_crs, target_crs) tuple.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param operations List of operations returned by proj_create_operations()
 * @param direction Direction into which to transform the point.
 * @param coord Coordinate to transform
 * @return the index in operations that would be used to transform coord. Or -1
 * in case of error, or no match.
 *
 * @since 7.1
 */
int proj_get_suggested_operation(PJ_CONTEXT *ctx, PJ_OBJ_LIST *operations,
                                 // cppcheck-suppress passedByValue
                                 PJ_DIRECTION direction, PJ_COORD coord) {
    SANITIZE_CTX(ctx);
    auto opList = dynamic_cast<PJ_OPERATION_LIST *>(operations);
    if (opList == nullptr) {
        proj_log_error(ctx, __FUNCTION__,
                       "operations is not a list of operations");
        return -1;
    }

    // Special case:
    // proj_create_crs_to_crs_from_pj() always use the unique operation
    // if there's a single one
    if (opList->objects.size() == 1) {
        return 0;
    }

    int iExcluded[2] = {-1, -1};
    const auto &preparedOps = opList->getPreparedOperations(ctx);
    int idx = pj_get_suggested_operation(ctx, preparedOps, iExcluded,
                                         /* skipNonInstantiable= */ false,
                                         direction, coord);
    if (idx >= 0) {
        idx = preparedOps[idx].idxInOriginalList;
    }
    return idx;
}

// ---------------------------------------------------------------------------

/** \brief Return the number of objects in the result set
 *
 * @param result Object of type PJ_OBJ_LIST (must not be NULL)
 */
int proj_list_get_count(const PJ_OBJ_LIST *result) {
    if (!result) {
        return 0;
    }
    return static_cast<int>(result->objects.size());
}

// ---------------------------------------------------------------------------

/** \brief Return an object from the result set
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param result Object of type PJ_OBJ_LIST (must not be NULL)
 * @param index Index
 * @return a new object that must be unreferenced with proj_destroy(),
 * or nullptr in case of error.
 */

PJ *proj_list_get(PJ_CONTEXT *ctx, const PJ_OBJ_LIST *result, int index) {
    SANITIZE_CTX(ctx);
    if (!result) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    if (index < 0 || index >= proj_list_get_count(result)) {
        proj_log_error(ctx, __FUNCTION__, "Invalid index");
        return nullptr;
    }
    return pj_obj_create(ctx, result->objects[index]);
}

// ---------------------------------------------------------------------------

/** \brief Drops a reference on the result set.
 *
 * This method should be called one and exactly one for each function
 * returning a PJ_OBJ_LIST*
 *
 * @param result Object, or NULL.
 */
void proj_list_destroy(PJ_OBJ_LIST *result) { delete result; }

// ---------------------------------------------------------------------------

/** \brief Return the accuracy (in metre) of a coordinate operation.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param coordoperation Coordinate operation. Must not be NULL.
 * @return the accuracy, or a negative value if unknown or in case of error.
 */
double proj_coordoperation_get_accuracy(PJ_CONTEXT *ctx,
                                        const PJ *coordoperation) {
    SANITIZE_CTX(ctx);
    if (!coordoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return -1;
    }
    auto co = dynamic_cast<const CoordinateOperation *>(
        coordoperation->iso_obj.get());
    if (!co) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a CoordinateOperation");
        return -1;
    }
    const auto &accuracies = co->coordinateOperationAccuracies();
    if (accuracies.empty()) {
        return -1;
    }
    try {
        return c_locale_stod(accuracies[0]->value());
    } catch (const std::exception &) {
    }
    return -1;
}

// ---------------------------------------------------------------------------

/** \brief Returns the datum of a SingleCRS.
 *
 * If that function returns NULL, @see proj_crs_get_datum_ensemble() to
 * potentially get a DatumEnsemble instead.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type SingleCRS (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error (or if there is no datum)
 */
PJ *proj_crs_get_datum(PJ_CONTEXT *ctx, const PJ *crs) {
    SANITIZE_CTX(ctx);
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_crs = dynamic_cast<const SingleCRS *>(crs->iso_obj.get());
    if (!l_crs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a SingleCRS");
        return nullptr;
    }
    const auto &datum = l_crs->datum();
    if (!datum) {
        return nullptr;
    }
    return pj_obj_create(ctx, NN_NO_CHECK(datum));
}

// ---------------------------------------------------------------------------

/** \brief Returns the datum ensemble of a SingleCRS.
 *
 * If that function returns NULL, @see proj_crs_get_datum() to
 * potentially get a Datum instead.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type SingleCRS (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error (or if there is no datum ensemble)
 *
 * @since 7.2
 */
PJ *proj_crs_get_datum_ensemble(PJ_CONTEXT *ctx, const PJ *crs) {
    SANITIZE_CTX(ctx);
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_crs = dynamic_cast<const SingleCRS *>(crs->iso_obj.get());
    if (!l_crs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a SingleCRS");
        return nullptr;
    }
    const auto &datumEnsemble = l_crs->datumEnsemble();
    if (!datumEnsemble) {
        return nullptr;
    }
    return pj_obj_create(ctx, NN_NO_CHECK(datumEnsemble));
}

// ---------------------------------------------------------------------------

/** \brief Returns the number of members of a datum ensemble.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param datum_ensemble Object of type DatumEnsemble (must not be NULL)
 *
 * @since 7.2
 */
int proj_datum_ensemble_get_member_count(PJ_CONTEXT *ctx,
                                         const PJ *datum_ensemble) {
    SANITIZE_CTX(ctx);
    if (!datum_ensemble) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return 0;
    }
    auto l_datum_ensemble =
        dynamic_cast<const DatumEnsemble *>(datum_ensemble->iso_obj.get());
    if (!l_datum_ensemble) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a DatumEnsemble");
        return 0;
    }
    return static_cast<int>(l_datum_ensemble->datums().size());
}

// ---------------------------------------------------------------------------

/** \brief Returns the positional accuracy of the datum ensemble.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param datum_ensemble Object of type DatumEnsemble (must not be NULL)
 * @return the accuracy, or -1 in case of error.
 *
 * @since 7.2
 */
double proj_datum_ensemble_get_accuracy(PJ_CONTEXT *ctx,
                                        const PJ *datum_ensemble) {
    SANITIZE_CTX(ctx);
    if (!datum_ensemble) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return -1;
    }
    auto l_datum_ensemble =
        dynamic_cast<const DatumEnsemble *>(datum_ensemble->iso_obj.get());
    if (!l_datum_ensemble) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a DatumEnsemble");
        return -1;
    }
    const auto &accuracy = l_datum_ensemble->positionalAccuracy();
    try {
        return c_locale_stod(accuracy->value());
    } catch (const std::exception &) {
    }
    return -1;
}

// ---------------------------------------------------------------------------

/** \brief Returns a member from a datum ensemble.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param datum_ensemble Object of type DatumEnsemble (must not be NULL)
 * @param member_index Index of the datum member to extract (between 0 and
 * proj_datum_ensemble_get_member_count()-1)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error (or if there is no datum ensemble)
 *
 * @since 7.2
 */
PJ *proj_datum_ensemble_get_member(PJ_CONTEXT *ctx, const PJ *datum_ensemble,
                                   int member_index) {
    SANITIZE_CTX(ctx);
    if (!datum_ensemble) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_datum_ensemble =
        dynamic_cast<const DatumEnsemble *>(datum_ensemble->iso_obj.get());
    if (!l_datum_ensemble) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a DatumEnsemble");
        return nullptr;
    }
    if (member_index < 0 ||
        member_index >= static_cast<int>(l_datum_ensemble->datums().size())) {
        proj_log_error(ctx, __FUNCTION__, "Invalid member_index");
        return nullptr;
    }
    return pj_obj_create(ctx, l_datum_ensemble->datums()[member_index]);
}

// ---------------------------------------------------------------------------

/** \brief Returns a datum for a SingleCRS.
 *
 * If the SingleCRS has a datum, then this datum is returned.
 * Otherwise, the SingleCRS has a datum ensemble, and this datum ensemble is
 * returned as a regular datum instead of a datum ensemble.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type SingleCRS (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error (or if there is no datum)
 *
 * @since 7.2
 */
PJ *proj_crs_get_datum_forced(PJ_CONTEXT *ctx, const PJ *crs) {
    SANITIZE_CTX(ctx);
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_crs = dynamic_cast<const SingleCRS *>(crs->iso_obj.get());
    if (!l_crs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a SingleCRS");
        return nullptr;
    }
    const auto &datum = l_crs->datum();
    if (datum) {
        return pj_obj_create(ctx, NN_NO_CHECK(datum));
    }
    const auto &datumEnsemble = l_crs->datumEnsemble();
    assert(datumEnsemble);
    auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
    try {
        return pj_obj_create(ctx, datumEnsemble->asDatum(dbContext));
    } catch (const std::exception &e) {
        proj_log_debug(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns the frame reference epoch of a dynamic geodetic or vertical
 * reference frame.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param datum Object of type DynamicGeodeticReferenceFrame or
 * DynamicVerticalReferenceFrame (must not be NULL)
 * @return the frame reference epoch as decimal year, or -1 in case of error.
 *
 * @since 7.2
 */
double proj_dynamic_datum_get_frame_reference_epoch(PJ_CONTEXT *ctx,
                                                    const PJ *datum) {
    SANITIZE_CTX(ctx);
    if (!datum) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return -1;
    }
    auto dgrf = dynamic_cast<const DynamicGeodeticReferenceFrame *>(
        datum->iso_obj.get());
    auto dvrf = dynamic_cast<const DynamicVerticalReferenceFrame *>(
        datum->iso_obj.get());
    if (!dgrf && !dvrf) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a "
                       "DynamicGeodeticReferenceFrame or "
                       "DynamicVerticalReferenceFrame");
        return -1;
    }
    const auto &frameReferenceEpoch =
        dgrf ? dgrf->frameReferenceEpoch() : dvrf->frameReferenceEpoch();
    return frameReferenceEpoch.value();
}

// ---------------------------------------------------------------------------

/** \brief Returns the coordinate system of a SingleCRS.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param crs Object of type SingleCRS (must not be NULL)
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 */
PJ *proj_crs_get_coordinate_system(PJ_CONTEXT *ctx, const PJ *crs) {
    SANITIZE_CTX(ctx);
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_crs = dynamic_cast<const SingleCRS *>(crs->iso_obj.get());
    if (!l_crs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a SingleCRS");
        return nullptr;
    }
    return pj_obj_create(ctx, l_crs->coordinateSystem());
}

// ---------------------------------------------------------------------------

/** \brief Returns the type of the coordinate system.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param cs Object of type CoordinateSystem (must not be NULL)
 * @return type, or PJ_CS_TYPE_UNKNOWN in case of error.
 */
PJ_COORDINATE_SYSTEM_TYPE proj_cs_get_type(PJ_CONTEXT *ctx, const PJ *cs) {
    SANITIZE_CTX(ctx);
    if (!cs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return PJ_CS_TYPE_UNKNOWN;
    }
    auto l_cs = dynamic_cast<const CoordinateSystem *>(cs->iso_obj.get());
    if (!l_cs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a CoordinateSystem");
        return PJ_CS_TYPE_UNKNOWN;
    }
    if (dynamic_cast<const CartesianCS *>(l_cs)) {
        return PJ_CS_TYPE_CARTESIAN;
    }
    if (dynamic_cast<const EllipsoidalCS *>(l_cs)) {
        return PJ_CS_TYPE_ELLIPSOIDAL;
    }
    if (dynamic_cast<const VerticalCS *>(l_cs)) {
        return PJ_CS_TYPE_VERTICAL;
    }
    if (dynamic_cast<const SphericalCS *>(l_cs)) {
        return PJ_CS_TYPE_SPHERICAL;
    }
    if (dynamic_cast<const OrdinalCS *>(l_cs)) {
        return PJ_CS_TYPE_ORDINAL;
    }
    if (dynamic_cast<const ParametricCS *>(l_cs)) {
        return PJ_CS_TYPE_PARAMETRIC;
    }
    if (dynamic_cast<const DateTimeTemporalCS *>(l_cs)) {
        return PJ_CS_TYPE_DATETIMETEMPORAL;
    }
    if (dynamic_cast<const TemporalCountCS *>(l_cs)) {
        return PJ_CS_TYPE_TEMPORALCOUNT;
    }
    if (dynamic_cast<const TemporalMeasureCS *>(l_cs)) {
        return PJ_CS_TYPE_TEMPORALMEASURE;
    }
    return PJ_CS_TYPE_UNKNOWN;
}

// ---------------------------------------------------------------------------

/** \brief Returns the number of axis of the coordinate system.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param cs Object of type CoordinateSystem (must not be NULL)
 * @return number of axis, or -1 in case of error.
 */
int proj_cs_get_axis_count(PJ_CONTEXT *ctx, const PJ *cs) {
    SANITIZE_CTX(ctx);
    if (!cs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return -1;
    }
    auto l_cs = dynamic_cast<const CoordinateSystem *>(cs->iso_obj.get());
    if (!l_cs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a CoordinateSystem");
        return -1;
    }
    return static_cast<int>(l_cs->axisList().size());
}

// ---------------------------------------------------------------------------

/** \brief Returns information on an axis
 *
 * @param ctx PROJ context, or NULL for default context
 * @param cs Object of type CoordinateSystem (must not be NULL)
 * @param index Index of the coordinate system (between 0 and
 * proj_cs_get_axis_count() - 1)
 * @param out_name Pointer to a string value to store the axis name. or NULL
 * @param out_abbrev Pointer to a string value to store the axis abbreviation.
 * or NULL
 * @param out_direction Pointer to a string value to store the axis direction.
 * or NULL
 * @param out_unit_conv_factor Pointer to a double value to store the axis
 * unit conversion factor. or NULL
 * @param out_unit_name Pointer to a string value to store the axis
 * unit name. or NULL
 * @param out_unit_auth_name Pointer to a string value to store the axis
 * unit authority name. or NULL
 * @param out_unit_code Pointer to a string value to store the axis
 * unit code. or NULL
 * @return TRUE in case of success
 */
int proj_cs_get_axis_info(PJ_CONTEXT *ctx, const PJ *cs, int index,
                          const char **out_name, const char **out_abbrev,
                          const char **out_direction,
                          double *out_unit_conv_factor,
                          const char **out_unit_name,
                          const char **out_unit_auth_name,
                          const char **out_unit_code) {
    SANITIZE_CTX(ctx);
    if (!cs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto l_cs = dynamic_cast<const CoordinateSystem *>(cs->iso_obj.get());
    if (!l_cs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a CoordinateSystem");
        return false;
    }
    const auto &axisList = l_cs->axisList();
    if (index < 0 || static_cast<size_t>(index) >= axisList.size()) {
        proj_log_error(ctx, __FUNCTION__, "Invalid index");
        return false;
    }
    const auto &axis = axisList[index];
    if (out_name) {
        *out_name = axis->nameStr().c_str();
    }
    if (out_abbrev) {
        *out_abbrev = axis->abbreviation().c_str();
    }
    if (out_direction) {
        *out_direction = axis->direction().toString().c_str();
    }
    if (out_unit_conv_factor) {
        *out_unit_conv_factor = axis->unit().conversionToSI();
    }
    if (out_unit_name) {
        *out_unit_name = axis->unit().name().c_str();
    }
    if (out_unit_auth_name) {
        *out_unit_auth_name = axis->unit().codeSpace().c_str();
    }
    if (out_unit_code) {
        *out_unit_code = axis->unit().code().c_str();
    }
    return true;
}

// ---------------------------------------------------------------------------

/** \brief Returns a PJ* object whose axis order is the one expected for
 * visualization purposes.
 *
 * The input object must be either:
 * <ul>
 * <li>a coordinate operation, that has been created with
 *     proj_create_crs_to_crs(). If the axis order of its source or target CRS
 *     is northing,easting, then an axis swap operation will be inserted.</li>
 * <li>or a CRS. The axis order of geographic CRS will be longitude, latitude
 *     [,height], and the one of projected CRS will be easting, northing
 *     [, height]</li>
 * </ul>
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CRS, or CoordinateOperation created with
 * proj_create_crs_to_crs() (must not be NULL)
 * @return a new PJ* object to free with proj_destroy() in case of success, or
 * nullptr in case of error
 */
PJ *proj_normalize_for_visualization(PJ_CONTEXT *ctx, const PJ *obj) {

    SANITIZE_CTX(ctx);
    if (!obj->alternativeCoordinateOperations.empty()) {
        try {
            auto pjNew = std::unique_ptr<PJ>(pj_new());
            if (!pjNew)
                return nullptr;
            pjNew->ctx = ctx;
            pjNew->descr = "Set of coordinate operations";
            pjNew->left = obj->left;
            pjNew->right = obj->right;
            pjNew->copyStateFrom(*obj);

            for (const auto &alt : obj->alternativeCoordinateOperations) {
                auto co = dynamic_cast<const CoordinateOperation *>(
                    alt.pj->iso_obj.get());
                if (co) {
                    double minxSrc = alt.minxSrc;
                    double minySrc = alt.minySrc;
                    double maxxSrc = alt.maxxSrc;
                    double maxySrc = alt.maxySrc;
                    double minxDst = alt.minxDst;
                    double minyDst = alt.minyDst;
                    double maxxDst = alt.maxxDst;
                    double maxyDst = alt.maxyDst;

                    auto l_sourceCRS = co->sourceCRS();
                    auto l_targetCRS = co->targetCRS();
                    if (l_sourceCRS && l_targetCRS) {
                        const bool swapSource =
                            l_sourceCRS
                                ->mustAxisOrderBeSwitchedForVisualization();
                        if (swapSource) {
                            std::swap(minxSrc, minySrc);
                            std::swap(maxxSrc, maxySrc);
                        }
                        const bool swapTarget =
                            l_targetCRS
                                ->mustAxisOrderBeSwitchedForVisualization();
                        if (swapTarget) {
                            std::swap(minxDst, minyDst);
                            std::swap(maxxDst, maxyDst);
                        }
                    }
                    ctx->forceOver = alt.pj->over != 0;
                    auto pjNormalized =
                        pj_obj_create(ctx, co->normalizeForVisualization());
                    ctx->forceOver = false;

                    pjNormalized->copyStateFrom(*(alt.pj));

                    pjNew->alternativeCoordinateOperations.emplace_back(
                        alt.idxInOriginalList, minxSrc, minySrc, maxxSrc,
                        maxySrc, minxDst, minyDst, maxxDst, maxyDst,
                        pjNormalized, co->nameStr(), alt.accuracy,
                        alt.pseudoArea, alt.areaName.c_str(),
                        alt.pjSrcGeocentricToLonLat,
                        alt.pjDstGeocentricToLonLat);
                }
            }
            return pjNew.release();
        } catch (const std::exception &e) {
            ctx->forceOver = false;
            proj_log_debug(ctx, __FUNCTION__, e.what());
            return nullptr;
        }
    }

    auto crs = dynamic_cast<const CRS *>(obj->iso_obj.get());
    if (crs) {
        try {
            return pj_obj_create(ctx, crs->normalizeForVisualization());
        } catch (const std::exception &e) {
            proj_log_debug(ctx, __FUNCTION__, e.what());
            return nullptr;
        }
    }

    auto co = dynamic_cast<const CoordinateOperation *>(obj->iso_obj.get());
    if (!co) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a CoordinateOperation "
                       "created with "
                       "proj_create_crs_to_crs");
        return nullptr;
    }
    try {
        ctx->forceOver = obj->over != 0;
        auto pjNormalized = pj_obj_create(ctx, co->normalizeForVisualization());
        pjNormalized->over = obj->over;
        ctx->forceOver = false;
        return pjNormalized;
    } catch (const std::exception &e) {
        ctx->forceOver = false;
        proj_log_debug(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a PJ* coordinate operation object which represents the
 * inverse operation of the specified coordinate operation.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param obj Object of type CoordinateOperation (must not be NULL)
 * @return a new PJ* object to free with proj_destroy() in case of success, or
 * nullptr in case of error
 * @since 6.3
 */
PJ *proj_coordoperation_create_inverse(PJ_CONTEXT *ctx, const PJ *obj) {

    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto co = dynamic_cast<const CoordinateOperation *>(obj->iso_obj.get());
    if (!co) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a CoordinateOperation");
        return nullptr;
    }
    try {
        return pj_obj_create(ctx, co->inverse());
    } catch (const std::exception &e) {
        proj_log_debug(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns the number of steps of a concatenated operation.
 *
 * The input object must be a concatenated operation.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param concatoperation Concatenated operation (must not be NULL)
 * @return the number of steps, or 0 in case of error.
 */
int proj_concatoperation_get_step_count(PJ_CONTEXT *ctx,
                                        const PJ *concatoperation) {
    SANITIZE_CTX(ctx);
    if (!concatoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto l_co = dynamic_cast<const ConcatenatedOperation *>(
        concatoperation->iso_obj.get());
    if (!l_co) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a ConcatenatedOperation");
        return false;
    }
    return static_cast<int>(l_co->operations().size());
}
// ---------------------------------------------------------------------------

/** \brief Returns a step of a concatenated operation.
 *
 * The input object must be a concatenated operation.
 *
 * The returned object must be unreferenced with proj_destroy() after
 * use.
 * It should be used by at most one thread at a time.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param concatoperation Concatenated operation (must not be NULL)
 * @param i_step Index of the step to extract. Between 0 and
 *               proj_concatoperation_get_step_count()-1
 * @return Object that must be unreferenced with proj_destroy(), or NULL
 * in case of error.
 */
PJ *proj_concatoperation_get_step(PJ_CONTEXT *ctx, const PJ *concatoperation,
                                  int i_step) {
    SANITIZE_CTX(ctx);
    if (!concatoperation) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto l_co = dynamic_cast<const ConcatenatedOperation *>(
        concatoperation->iso_obj.get());
    if (!l_co) {
        proj_log_error(ctx, __FUNCTION__,
                       "Object is not a ConcatenatedOperation");
        return nullptr;
    }
    const auto &steps = l_co->operations();
    if (i_step < 0 || static_cast<size_t>(i_step) >= steps.size()) {
        proj_log_error(ctx, __FUNCTION__, "Invalid step index");
        return nullptr;
    }
    return pj_obj_create(ctx, steps[i_step]);
}
// ---------------------------------------------------------------------------

/** \brief Opaque object representing an insertion session. */
struct PJ_INSERT_SESSION {
    //! @cond Doxygen_Suppress
    PJ_CONTEXT *ctx = nullptr;
    //!  @endcond
};

// ---------------------------------------------------------------------------

/** \brief Starts a session for proj_get_insert_statements()
 *
 * Starts a new session for one or several calls to
 * proj_get_insert_statements().
 *
 * An insertion session guarantees that the inserted objects will not create
 * conflicting intermediate objects.
 *
 * The session must be stopped with proj_insert_object_session_destroy().
 *
 * Only one session may be active at a time for a given context.
 *
 * @param ctx PROJ context, or NULL for default context
 * @return the session, or NULL in case of error.
 *
 * @since 8.1
 */
PJ_INSERT_SESSION *proj_insert_object_session_create(PJ_CONTEXT *ctx) {
    SANITIZE_CTX(ctx);
    try {
        auto dbContext = getDBcontext(ctx);
        dbContext->startInsertStatementsSession();
        PJ_INSERT_SESSION *session = new PJ_INSERT_SESSION;
        session->ctx = ctx;
        return session;
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Stops an insertion session started with
 * proj_insert_object_session_create()
 *
 * @param ctx PROJ context, or NULL for default context
 * @param session The insertion session.
 * @since 8.1
 */
void proj_insert_object_session_destroy(PJ_CONTEXT *ctx,
                                        PJ_INSERT_SESSION *session) {
    SANITIZE_CTX(ctx);
    if (session) {
        try {
            if (session->ctx != ctx) {
                proj_log_error(ctx, __FUNCTION__,
                               "proj_insert_object_session_destroy() called "
                               "with a context different from the one of "
                               "proj_insert_object_session_create()");
            } else {
                auto dbContext = getDBcontext(ctx);
                dbContext->stopInsertStatementsSession();
            }
        } catch (const std::exception &e) {
            proj_log_error(ctx, __FUNCTION__, e.what());
        }
        delete session;
    }
}

// ---------------------------------------------------------------------------

/** \brief Suggests a database code for the passed object.
 *
 * Supported type of objects are PrimeMeridian, Ellipsoid, Datum, DatumEnsemble,
 * GeodeticCRS, ProjectedCRS, VerticalCRS, CompoundCRS, BoundCRS, Conversion.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param object Object for which to suggest a code.
 * @param authority Authority name into which the object will be inserted.
 * @param numeric_code Whether the code should be numeric, or derived from the
 * object name.
 * @param options NULL terminated list of options, or NULL.
 *                No options are supported currently.
 * @return the suggested code, that is guaranteed to not conflict with an
 * existing one (to be freed with proj_string_destroy),
 * or nullptr in case of error.
 *
 * @since 8.1
 */
char *proj_suggests_code_for(PJ_CONTEXT *ctx, const PJ *object,
                             const char *authority, int numeric_code,
                             const char *const *options) {
    SANITIZE_CTX(ctx);
    (void)options;

    if (!object || !authority) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto identifiedObject =
        std::dynamic_pointer_cast<IdentifiedObject>(object->iso_obj);
    if (!identifiedObject) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "Object is not a IdentifiedObject");
        return nullptr;
    }

    try {
        auto dbContext = getDBcontext(ctx);
        return pj_strdup(dbContext
                             ->suggestsCodeFor(NN_NO_CHECK(identifiedObject),
                                               std::string(authority),
                                               numeric_code != FALSE)
                             .c_str());
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Free a string.
 *
 * Only to be used with functions that document using this function.
 *
 * @param str String to free.
 *
 * @since 8.1
 */
void proj_string_destroy(char *str) { free(str); }

// ---------------------------------------------------------------------------

/** \brief Returns SQL statements needed to insert the passed object into the
 * database.
 *
 * proj_insert_object_session_create() may have been called previously.
 *
 * It is strongly recommended that new objects should not be added in common
 * registries, such as "EPSG", "ESRI", "IAU", etc. Users should use a custom
 * authority name instead. If a new object should be
 * added to the official EPSG registry, users are invited to follow the
 * procedure explained at https://epsg.org/dataset-change-requests.html.
 *
 * Combined with proj_context_get_database_structure(), users can create
 * auxiliary databases, instead of directly modifying the main proj.db database.
 * Those auxiliary databases can be specified through
 * proj_context_set_database_path() or the PROJ_AUX_DB environment variable.
 *
 * @param ctx PROJ context, or NULL for default context
 * @param session The insertion session. May be NULL if a single object must be
 *                inserted.
 * @param object The object to insert into the database. Currently only
 *               PrimeMeridian, Ellipsoid, Datum, GeodeticCRS, ProjectedCRS,
 *               VerticalCRS, CompoundCRS or BoundCRS are supported.
 * @param authority Authority name into which the object will be inserted.
 *                  Must not be NULL.
 * @param code Code with which the object will be inserted.Must not be NULL.
 * @param numeric_codes Whether intermediate objects that can be created should
 *                      use numeric codes (true), or may be alphanumeric (false)
 * @param allowed_authorities NULL terminated list of authority names, or NULL.
 *                            Authorities to which intermediate objects are
 *                            allowed to refer to. "authority" will be
 *                            implicitly added to it. Note that unit,
 *                            coordinate systems, projection methods and
 *                            parameters will in any case be allowed to refer
 *                            to EPSG.
 *                            If NULL, allowed_authorities defaults to
 *                            {"EPSG", "PROJ", nullptr}
 * @param options NULL terminated list of options, or NULL.
 *                No options are supported currently.
 *
 * @return a list of insert statements (to be freed with
 *         proj_string_list_destroy()), or NULL in case of error.
 * @since 8.1
 */
PROJ_STRING_LIST proj_get_insert_statements(
    PJ_CONTEXT *ctx, PJ_INSERT_SESSION *session, const PJ *object,
    const char *authority, const char *code, int numeric_codes,
    const char *const *allowed_authorities, const char *const *options) {
    SANITIZE_CTX(ctx);
    (void)options;

    struct TempSessionHolder {
      private:
        PJ_CONTEXT *m_ctx;
        PJ_INSERT_SESSION *m_tempSession;
        TempSessionHolder(const TempSessionHolder &) = delete;
        TempSessionHolder &operator=(const TempSessionHolder &) = delete;

      public:
        TempSessionHolder(PJ_CONTEXT *ctx, PJ_INSERT_SESSION *session)
            : m_ctx(ctx),
              m_tempSession(session ? nullptr
                                    : proj_insert_object_session_create(ctx)) {}

        ~TempSessionHolder() {
            if (m_tempSession) {
                proj_insert_object_session_destroy(m_ctx, m_tempSession);
            }
        }

        inline PJ_INSERT_SESSION *GetTempSession() const {
            return m_tempSession;
        }
    };

    try {
        TempSessionHolder oHolder(ctx, session);
        if (!session) {
            session = oHolder.GetTempSession();
            if (!session) {
                return nullptr;
            }
        }

        if (!object || !authority || !code) {
            proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
            proj_log_error(ctx, __FUNCTION__, "missing required input");
            return nullptr;
        }
        auto identifiedObject =
            std::dynamic_pointer_cast<IdentifiedObject>(object->iso_obj);
        if (!identifiedObject) {
            proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
            proj_log_error(ctx, __FUNCTION__,
                           "Object is not a IdentifiedObject");
            return nullptr;
        }

        auto dbContext = getDBcontext(ctx);
        std::vector<std::string> allowedAuthorities{"EPSG", "PROJ"};
        if (allowed_authorities) {
            allowedAuthorities.clear();
            for (auto iter = allowed_authorities; *iter; ++iter) {
                allowedAuthorities.emplace_back(*iter);
            }
        }
        auto statements = dbContext->getInsertStatementsFor(
            NN_NO_CHECK(identifiedObject), authority, code,
            numeric_codes != FALSE, allowedAuthorities);
        return to_string_list(std::move(statements));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Returns a list of geoid models available for that crs
 *
 * The list includes the geoid models connected directly with the crs,
 * or via "Height Depth Reversal" or "Change of Vertical Unit" transformations.
 * The returned list is NULL terminated and must be freed with
 * proj_string_list_destroy().
 *
 * @param ctx Context, or NULL for default context.
 * @param auth_name Authority name (must not be NULL)
 * @param code Object code (must not be NULL)
 * @param options should be set to NULL for now
 * @return list of geoid models names (to be freed with
 * proj_string_list_destroy()), or NULL in case of error.
 * @since 8.1
 */
PROJ_STRING_LIST
proj_get_geoid_models_from_database(PJ_CONTEXT *ctx, const char *auth_name,
                                    const char *code,
                                    const char *const *options) {
    SANITIZE_CTX(ctx);
    if (!auth_name || !code) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    (void)options;
    try {
        const std::string codeStr(code);
        auto factory = AuthorityFactory::create(getDBcontext(ctx), auth_name);
        auto geoidModels = factory->getGeoidModels(codeStr);
        return to_string_list(std::move(geoidModels));
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CoordinateMetadata object
 *
 * @since 9.4
 */

PJ *proj_coordinate_metadata_create(PJ_CONTEXT *ctx, const PJ *crs,
                                    double epoch) {
    SANITIZE_CTX(ctx);
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return nullptr;
    }
    auto crsCast = std::dynamic_pointer_cast<CRS>(crs->iso_obj);
    if (!crsCast) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a CRS");
        return nullptr;
    }
    try {
        auto dbContext = getDBcontextNoException(ctx, __FUNCTION__);
        return pj_obj_create(ctx, CoordinateMetadata::create(
                                      NN_NO_CHECK(crsCast), epoch, dbContext));
    } catch (const std::exception &e) {
        proj_log_debug(ctx, __FUNCTION__, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------

/** \brief Return the coordinate epoch associated with a CoordinateMetadata.
 *
 * It may return a NaN value if there is no associated coordinate epoch.
 *
 * @since 9.2
 */
double proj_coordinate_metadata_get_epoch(PJ_CONTEXT *ctx, const PJ *obj) {
    SANITIZE_CTX(ctx);
    if (!obj) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return std::numeric_limits<double>::quiet_NaN();
    }
    auto ptr = obj->iso_obj.get();
    auto coordinateMetadata = dynamic_cast<const CoordinateMetadata *>(ptr);
    if (coordinateMetadata) {
        if (coordinateMetadata->coordinateEpoch().has_value()) {
            return coordinateMetadata->coordinateEpochAsDecimalYear();
        }
        return std::numeric_limits<double>::quiet_NaN();
    }
    proj_log_error(ctx, __FUNCTION__, "Object is not a CoordinateMetadata");
    return std::numeric_limits<double>::quiet_NaN();
}

// ---------------------------------------------------------------------------

/** \brief Return whether a CRS has an associated PointMotionOperation
 *
 * @since 9.4
 */
int proj_crs_has_point_motion_operation(PJ_CONTEXT *ctx, const PJ *crs) {
    SANITIZE_CTX(ctx);
    if (!crs) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER_API_MISUSE);
        proj_log_error(ctx, __FUNCTION__, "missing required input");
        return false;
    }
    auto l_crs = dynamic_cast<const CRS *>(crs->iso_obj.get());
    if (!l_crs) {
        proj_log_error(ctx, __FUNCTION__, "Object is not a CRS");
        return false;
    }
    auto geodeticCRS = l_crs->extractGeodeticCRS();
    if (!geodeticCRS)
        return false;
    try {
        auto factory =
            AuthorityFactory::create(getDBcontext(ctx), std::string());
        return !factory
                    ->getPointMotionOperationsFor(NN_NO_CHECK(geodeticCRS),
                                                  false)
                    .empty();
    } catch (const std::exception &e) {
        proj_log_error(ctx, __FUNCTION__, e.what());
    }
    return false;
}
