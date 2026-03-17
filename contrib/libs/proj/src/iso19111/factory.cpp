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

#include "proj/common.hpp"
#include "proj/coordinateoperation.hpp"
#include "proj/coordinates.hpp"
#include "proj/coordinatesystem.hpp"
#include "proj/crs.hpp"
#include "proj/datum.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"
#include "proj/internal/lru_cache.hpp"
#include "proj/internal/tracing.hpp"

#include "operation/coordinateoperation_internal.hpp"
#include "operation/parammappings.hpp"

#include "filemanager.hpp"
#include "sqlite3_utils.hpp"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <limits>
#include <locale>
#include <map>
#include <memory>
#include <mutex>
#include <sstream> // std::ostringstream
#include <stdexcept>
#include <string>

#include "proj_constants.h"

// PROJ include order is sensitive
// clang-format off
#include "proj.h"
#include "proj_internal.h"
// clang-format on

#include <sqlite3.h>

#ifdef EMBED_RESOURCE_FILES
#error #include "embedded_resources.h"
#endif

#include <library/cpp/resource/resource.h>
#include <util/system/tempfile.h>

// Custom SQLite VFS as our database is not supposed to be modified in
// parallel. This is slightly faster
#define ENABLE_CUSTOM_LOCKLESS_VFS

#if defined(_WIN32) && defined(PROJ_HAS_PTHREADS)
#undef PROJ_HAS_PTHREADS
#endif

/* SQLite3 might use seak()+read() or pread[64]() to read data */
/* The later allows the same SQLite handle to be safely used in forked */
/* children of a parent process, while the former doesn't. */
/* So we use pthread_atfork() to set a flag in forked children, to ask them */
/* to close and reopen their database handle. */
#if defined(PROJ_HAS_PTHREADS) && !defined(SQLITE_USE_PREAD)
#include <pthread.h>
#define REOPEN_SQLITE_DB_AFTER_FORK
#endif

using namespace NS_PROJ::internal;
using namespace NS_PROJ::common;

NS_PROJ_START
namespace io {

//! @cond Doxygen_Suppress

// CRS subtypes
#define GEOG_2D "geographic 2D"
#define GEOG_3D "geographic 3D"
#define GEOCENTRIC "geocentric"
#define OTHER "other"
#define PROJECTED "projected"
#define ENGINEERING "engineering"
#define VERTICAL "vertical"
#define COMPOUND "compound"

#define GEOG_2D_SINGLE_QUOTED "'geographic 2D'"
#define GEOG_3D_SINGLE_QUOTED "'geographic 3D'"
#define GEOCENTRIC_SINGLE_QUOTED "'geocentric'"

// Coordinate system types
constexpr const char *CS_TYPE_ELLIPSOIDAL = cs::EllipsoidalCS::WKT2_TYPE;
constexpr const char *CS_TYPE_CARTESIAN = cs::CartesianCS::WKT2_TYPE;
constexpr const char *CS_TYPE_SPHERICAL = cs::SphericalCS::WKT2_TYPE;
constexpr const char *CS_TYPE_VERTICAL = cs::VerticalCS::WKT2_TYPE;
constexpr const char *CS_TYPE_ORDINAL = cs::OrdinalCS::WKT2_TYPE;

// See data/sql/metadata.sql for the semantics of those constants
constexpr int DATABASE_LAYOUT_VERSION_MAJOR = 1;
// If the code depends on the new additions, then DATABASE_LAYOUT_VERSION_MINOR
// must be incremented.
constexpr int DATABASE_LAYOUT_VERSION_MINOR = 6;

constexpr size_t N_MAX_PARAMS = 7;

#ifdef EMBED_RESOURCE_FILES
constexpr const char *EMBEDDED_PROJ_DB = "__embedded_proj_db__";
#endif

// ---------------------------------------------------------------------------

struct SQLValues {
    enum class Type { STRING, INT, DOUBLE };

    // cppcheck-suppress noExplicitConstructor
    SQLValues(const std::string &value) : type_(Type::STRING), str_(value) {}

    // cppcheck-suppress noExplicitConstructor
    SQLValues(int value) : type_(Type::INT), int_(value) {}

    // cppcheck-suppress noExplicitConstructor
    SQLValues(double value) : type_(Type::DOUBLE), double_(value) {}

    const Type &type() const { return type_; }

    // cppcheck-suppress functionStatic
    const std::string &stringValue() const { return str_; }

    // cppcheck-suppress functionStatic
    int intValue() const { return int_; }

    // cppcheck-suppress functionStatic
    double doubleValue() const { return double_; }

  private:
    Type type_;
    std::string str_{};
    int int_ = 0;
    double double_ = 0.0;
};

// ---------------------------------------------------------------------------

using SQLRow = std::vector<std::string>;
using SQLResultSet = std::list<SQLRow>;
using ListOfParams = std::list<SQLValues>;

// ---------------------------------------------------------------------------

static double PROJ_SQLITE_GetValAsDouble(sqlite3_value *val, bool &gotVal) {
    switch (sqlite3_value_type(val)) {
    case SQLITE_FLOAT:
        gotVal = true;
        return sqlite3_value_double(val);

    case SQLITE_INTEGER:
        gotVal = true;
        return static_cast<double>(sqlite3_value_int64(val));

    default:
        gotVal = false;
        return 0.0;
    }
}

// ---------------------------------------------------------------------------

static void PROJ_SQLITE_pseudo_area_from_swne(sqlite3_context *pContext,
                                              int /* argc */,
                                              sqlite3_value **argv) {
    bool b0, b1, b2, b3;
    double south_lat = PROJ_SQLITE_GetValAsDouble(argv[0], b0);
    double west_lon = PROJ_SQLITE_GetValAsDouble(argv[1], b1);
    double north_lat = PROJ_SQLITE_GetValAsDouble(argv[2], b2);
    double east_lon = PROJ_SQLITE_GetValAsDouble(argv[3], b3);
    if (!b0 || !b1 || !b2 || !b3) {
        sqlite3_result_null(pContext);
        return;
    }
    // Deal with area crossing antimeridian
    if (east_lon < west_lon) {
        east_lon += 360.0;
    }
    // Integrate cos(lat) between south_lat and north_lat
    double pseudo_area = (east_lon - west_lon) *
                         (std::sin(common::Angle(north_lat).getSIValue()) -
                          std::sin(common::Angle(south_lat).getSIValue()));
    sqlite3_result_double(pContext, pseudo_area);
}

// ---------------------------------------------------------------------------

static void PROJ_SQLITE_intersects_bbox(sqlite3_context *pContext,
                                        int /* argc */, sqlite3_value **argv) {
    bool b0, b1, b2, b3, b4, b5, b6, b7;
    double south_lat1 = PROJ_SQLITE_GetValAsDouble(argv[0], b0);
    double west_lon1 = PROJ_SQLITE_GetValAsDouble(argv[1], b1);
    double north_lat1 = PROJ_SQLITE_GetValAsDouble(argv[2], b2);
    double east_lon1 = PROJ_SQLITE_GetValAsDouble(argv[3], b3);
    double south_lat2 = PROJ_SQLITE_GetValAsDouble(argv[4], b4);
    double west_lon2 = PROJ_SQLITE_GetValAsDouble(argv[5], b5);
    double north_lat2 = PROJ_SQLITE_GetValAsDouble(argv[6], b6);
    double east_lon2 = PROJ_SQLITE_GetValAsDouble(argv[7], b7);
    if (!b0 || !b1 || !b2 || !b3 || !b4 || !b5 || !b6 || !b7) {
        sqlite3_result_null(pContext);
        return;
    }
    auto bbox1 = metadata::GeographicBoundingBox::create(west_lon1, south_lat1,
                                                         east_lon1, north_lat1);
    auto bbox2 = metadata::GeographicBoundingBox::create(west_lon2, south_lat2,
                                                         east_lon2, north_lat2);
    sqlite3_result_int(pContext, bbox1->intersects(bbox2) ? 1 : 0);
}

// ---------------------------------------------------------------------------

class SQLiteHandle {
    std::string path_{};
    sqlite3 *sqlite_handle_ = nullptr;
    bool close_handle_ = true;

#ifdef REOPEN_SQLITE_DB_AFTER_FORK
    bool is_valid_ = true;
#endif

    int nLayoutVersionMajor_ = 0;
    int nLayoutVersionMinor_ = 0;

#if defined(ENABLE_CUSTOM_LOCKLESS_VFS) || defined(EMBED_RESOURCE_FILES)
    std::unique_ptr<SQLite3VFS> vfs_{};
#endif

    SQLiteHandle(const SQLiteHandle &) = delete;
    SQLiteHandle &operator=(const SQLiteHandle &) = delete;

    SQLiteHandle(sqlite3 *sqlite_handle, bool close_handle)
        : sqlite_handle_(sqlite_handle), close_handle_(close_handle) {
        assert(sqlite_handle_);
    }

    // cppcheck-suppress functionStatic
    void initialize();

    SQLResultSet run(const std::string &sql,
                     const ListOfParams &parameters = ListOfParams(),
                     bool useMaxFloatPrecision = false);

  public:
    ~SQLiteHandle();

    const std::string &path() const { return path_; }

    sqlite3 *handle() { return sqlite_handle_; }

#ifdef REOPEN_SQLITE_DB_AFTER_FORK
    bool isValid() const { return is_valid_; }

    void invalidate() { is_valid_ = false; }
#endif

    static std::shared_ptr<SQLiteHandle> open(PJ_CONTEXT *ctx,
                                              const std::string &path);

    // might not be shared between thread depending how the handle was opened!
    static std::shared_ptr<SQLiteHandle>
    initFromExisting(sqlite3 *sqlite_handle, bool close_handle,
                     int nLayoutVersionMajor, int nLayoutVersionMinor);

    static std::unique_ptr<SQLiteHandle>
    initFromExistingUniquePtr(sqlite3 *sqlite_handle, bool close_handle);

    void checkDatabaseLayout(const std::string &mainDbPath,
                             const std::string &path,
                             const std::string &dbNamePrefix);

    SQLResultSet run(sqlite3_stmt *stmt, const std::string &sql,
                     const ListOfParams &parameters = ListOfParams(),
                     bool useMaxFloatPrecision = false);

    inline int getLayoutVersionMajor() const { return nLayoutVersionMajor_; }
    inline int getLayoutVersionMinor() const { return nLayoutVersionMinor_; }
};

// ---------------------------------------------------------------------------

SQLiteHandle::~SQLiteHandle() {
    if (close_handle_) {
        sqlite3_close(sqlite_handle_);
    }
}

// ---------------------------------------------------------------------------

std::shared_ptr<SQLiteHandle> SQLiteHandle::open(PJ_CONTEXT *ctx,
                                                 const std::string &pathIn) {

    std::string path(pathIn);
    const int sqlite3VersionNumber = sqlite3_libversion_number();
    // Minimum version for correct performance: 3.11
    if (sqlite3VersionNumber < 3 * 1000000 + 11 * 1000) {
        pj_log(ctx, PJ_LOG_ERROR,
               "SQLite3 version is %s, whereas at least 3.11 should be used",
               sqlite3_libversion());
    }

    std::string vfsName;
#if defined(ENABLE_CUSTOM_LOCKLESS_VFS) || defined(EMBED_RESOURCE_FILES)
    std::unique_ptr<SQLite3VFS> vfs;
#endif

#ifdef EMBED_RESOURCE_FILES
    if (path == EMBEDDED_PROJ_DB && ctx->custom_sqlite3_vfs_name.empty()) {
        unsigned int proj_db_size = 0;
        const unsigned char *proj_db = pj_get_embedded_proj_db(&proj_db_size);

        vfs = SQLite3VFS::createMem(proj_db, proj_db_size);
        if (vfs == nullptr) {
            throw FactoryException("Open of " + path + " failed");
        }

        std::ostringstream buffer;
        buffer << "file:/proj.db?immutable=1&ptr=";
        buffer << reinterpret_cast<uintptr_t>(proj_db);
        buffer << "&sz=";
        buffer << proj_db_size;
        buffer << "&max=";
        buffer << proj_db_size;
        buffer << "&vfs=";
        buffer << vfs->name();
        path = buffer.str();
    } else
#endif

#ifdef ENABLE_CUSTOM_LOCKLESS_VFS
        if (ctx->custom_sqlite3_vfs_name.empty()) {
        vfs = SQLite3VFS::create(false, true, true);
        if (vfs == nullptr) {
            throw FactoryException("Open of " + path + " failed");
        }
        vfsName = vfs->name();
    } else
#endif
    {
        vfsName = ctx->custom_sqlite3_vfs_name;
    }
    sqlite3 *sqlite_handle = nullptr;
    // SQLITE_OPEN_FULLMUTEX as this will be used from concurrent threads
    if (sqlite3_open_v2(
            path.c_str(), &sqlite_handle,
            SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_URI,
            vfsName.empty() ? nullptr : vfsName.c_str()) != SQLITE_OK ||
        !sqlite_handle) {
        if (sqlite_handle != nullptr) {
            sqlite3_close(sqlite_handle);
        }
        throw FactoryException("Open of " + path + " failed");
    }
    auto handle =
        std::shared_ptr<SQLiteHandle>(new SQLiteHandle(sqlite_handle, true));
#if defined(ENABLE_CUSTOM_LOCKLESS_VFS) || defined(EMBED_RESOURCE_FILES)
    handle->vfs_ = std::move(vfs);
#endif
    handle->initialize();
    handle->path_ = path;
    handle->checkDatabaseLayout(path, path, std::string());
    return handle;
}

// ---------------------------------------------------------------------------

std::shared_ptr<SQLiteHandle>
SQLiteHandle::initFromExisting(sqlite3 *sqlite_handle, bool close_handle,
                               int nLayoutVersionMajor,
                               int nLayoutVersionMinor) {
    auto handle = std::shared_ptr<SQLiteHandle>(
        new SQLiteHandle(sqlite_handle, close_handle));
    handle->nLayoutVersionMajor_ = nLayoutVersionMajor;
    handle->nLayoutVersionMinor_ = nLayoutVersionMinor;
    handle->initialize();
    return handle;
}

// ---------------------------------------------------------------------------

std::unique_ptr<SQLiteHandle>
SQLiteHandle::initFromExistingUniquePtr(sqlite3 *sqlite_handle,
                                        bool close_handle) {
    auto handle = std::unique_ptr<SQLiteHandle>(
        new SQLiteHandle(sqlite_handle, close_handle));
    handle->initialize();
    return handle;
}

// ---------------------------------------------------------------------------

SQLResultSet SQLiteHandle::run(sqlite3_stmt *stmt, const std::string &sql,
                               const ListOfParams &parameters,
                               bool useMaxFloatPrecision) {
    int nBindField = 1;
    for (const auto &param : parameters) {
        const auto &paramType = param.type();
        if (paramType == SQLValues::Type::STRING) {
            const auto &strValue = param.stringValue();
            sqlite3_bind_text(stmt, nBindField, strValue.c_str(),
                              static_cast<int>(strValue.size()),
                              SQLITE_TRANSIENT);
        } else if (paramType == SQLValues::Type::INT) {
            sqlite3_bind_int(stmt, nBindField, param.intValue());
        } else {
            assert(paramType == SQLValues::Type::DOUBLE);
            sqlite3_bind_double(stmt, nBindField, param.doubleValue());
        }
        nBindField++;
    }

#ifdef TRACE_DATABASE
    size_t nPos = 0;
    std::string sqlSubst(sql);
    for (const auto &param : parameters) {
        nPos = sqlSubst.find('?', nPos);
        assert(nPos != std::string::npos);
        std::string strValue;
        const auto paramType = param.type();
        if (paramType == SQLValues::Type::STRING) {
            strValue = '\'' + param.stringValue() + '\'';
        } else if (paramType == SQLValues::Type::INT) {
            strValue = toString(param.intValue());
        } else {
            strValue = toString(param.doubleValue());
        }
        sqlSubst =
            sqlSubst.substr(0, nPos) + strValue + sqlSubst.substr(nPos + 1);
        nPos += strValue.size();
    }
    logTrace(sqlSubst, "DATABASE");
#endif

    SQLResultSet result;
    const int column_count = sqlite3_column_count(stmt);
    while (true) {
        int ret = sqlite3_step(stmt);
        if (ret == SQLITE_ROW) {
            SQLRow row(column_count);
            for (int i = 0; i < column_count; i++) {
                if (useMaxFloatPrecision &&
                    sqlite3_column_type(stmt, i) == SQLITE_FLOAT) {
                    // sqlite3_column_text() does not use maximum precision
                    std::ostringstream buffer;
                    buffer.imbue(std::locale::classic());
                    buffer << std::setprecision(18);
                    buffer << sqlite3_column_double(stmt, i);
                    row[i] = buffer.str();
                } else {
                    const char *txt = reinterpret_cast<const char *>(
                        sqlite3_column_text(stmt, i));
                    if (txt) {
                        row[i] = txt;
                    }
                }
            }
            result.emplace_back(std::move(row));
        } else if (ret == SQLITE_DONE) {
            break;
        } else {
            throw FactoryException(std::string("SQLite error [ ")
                                       .append("code = ")
                                       .append(internal::toString(ret))
                                       .append(", msg = ")
                                       .append(sqlite3_errmsg(sqlite_handle_))
                                       .append(" ] on ")
                                       .append(sql));
        }
    }
    return result;
}

// ---------------------------------------------------------------------------

SQLResultSet SQLiteHandle::run(const std::string &sql,
                               const ListOfParams &parameters,
                               bool useMaxFloatPrecision) {
    sqlite3_stmt *stmt = nullptr;
    try {
        if (sqlite3_prepare_v2(sqlite_handle_, sql.c_str(),
                               static_cast<int>(sql.size()), &stmt,
                               nullptr) != SQLITE_OK) {
            throw FactoryException(std::string("SQLite error [ ")
                                       .append(sqlite3_errmsg(sqlite_handle_))
                                       .append(" ] on ")
                                       .append(sql));
        }
        auto ret = run(stmt, sql, parameters, useMaxFloatPrecision);
        sqlite3_finalize(stmt);
        return ret;
    } catch (const std::exception &) {
        if (stmt)
            sqlite3_finalize(stmt);
        throw;
    }
}

// ---------------------------------------------------------------------------

void SQLiteHandle::checkDatabaseLayout(const std::string &mainDbPath,
                                       const std::string &path,
                                       const std::string &dbNamePrefix) {
    if (!dbNamePrefix.empty() && run("SELECT 1 FROM " + dbNamePrefix +
                                     "sqlite_master WHERE name = 'metadata'")
                                     .empty()) {
        // Accept auxiliary databases without metadata table (sparse DBs)
        return;
    }
    auto res = run("SELECT key, value FROM " + dbNamePrefix +
                   "metadata WHERE key IN "
                   "('DATABASE.LAYOUT.VERSION.MAJOR', "
                   "'DATABASE.LAYOUT.VERSION.MINOR')");
    if (res.empty() && !dbNamePrefix.empty()) {
        // Accept auxiliary databases without layout metadata.
        return;
    }
    if (res.size() != 2) {
        throw FactoryException(
            path + " lacks DATABASE.LAYOUT.VERSION.MAJOR / "
                   "DATABASE.LAYOUT.VERSION.MINOR "
                   "metadata. It comes from another PROJ installation.");
    }
    int major = 0;
    int minor = 0;
    for (const auto &row : res) {
        if (row[0] == "DATABASE.LAYOUT.VERSION.MAJOR") {
            major = atoi(row[1].c_str());
        } else if (row[0] == "DATABASE.LAYOUT.VERSION.MINOR") {
            minor = atoi(row[1].c_str());
        }
    }
    if (major != DATABASE_LAYOUT_VERSION_MAJOR) {
        throw FactoryException(
            path +
            " contains DATABASE.LAYOUT.VERSION.MAJOR = " + toString(major) +
            " whereas " + toString(DATABASE_LAYOUT_VERSION_MAJOR) +
            " is expected. "
            "It comes from another PROJ installation.");
    }

    if (minor < DATABASE_LAYOUT_VERSION_MINOR) {
        throw FactoryException(
            path +
            " contains DATABASE.LAYOUT.VERSION.MINOR = " + toString(minor) +
            " whereas a number >= " + toString(DATABASE_LAYOUT_VERSION_MINOR) +
            " is expected. "
            "It comes from another PROJ installation.");
    }

    if (dbNamePrefix.empty()) {
        nLayoutVersionMajor_ = major;
        nLayoutVersionMinor_ = minor;
    } else if (nLayoutVersionMajor_ != major || nLayoutVersionMinor_ != minor) {
        throw FactoryException(
            "Auxiliary database " + path +
            " contains a DATABASE.LAYOUT.VERSION =  " + toString(major) + '.' +
            toString(minor) +
            " which is different from the one from the main database " +
            mainDbPath + " which is " + toString(nLayoutVersionMajor_) + '.' +
            toString(nLayoutVersionMinor_));
    }
}

// ---------------------------------------------------------------------------

#ifndef SQLITE_DETERMINISTIC
#define SQLITE_DETERMINISTIC 0
#endif

void SQLiteHandle::initialize() {

    // There is a bug in sqlite 3.38.0 with some complex queries.
    // Cf https://github.com/OSGeo/PROJ/issues/3077
    // Disabling Bloom-filter pull-down optimization as suggested in
    // https://sqlite.org/forum/forumpost/7d3a75438c
    const int sqlite3VersionNumber = sqlite3_libversion_number();
    if (sqlite3VersionNumber == 3 * 1000000 + 38 * 1000) {
        sqlite3_test_control(SQLITE_TESTCTRL_OPTIMIZATIONS, sqlite_handle_,
                             0x100000);
    }

    sqlite3_create_function(sqlite_handle_, "pseudo_area_from_swne", 4,
                            SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr,
                            PROJ_SQLITE_pseudo_area_from_swne, nullptr,
                            nullptr);

    sqlite3_create_function(sqlite_handle_, "intersects_bbox", 8,
                            SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr,
                            PROJ_SQLITE_intersects_bbox, nullptr, nullptr);
}

// ---------------------------------------------------------------------------

class SQLiteHandleCache {
#ifdef REOPEN_SQLITE_DB_AFTER_FORK
    bool firstTime_ = true;
#endif

    std::mutex sMutex_{};

    // Map dbname to SQLiteHandle
    lru11::Cache<std::string, std::shared_ptr<SQLiteHandle>> cache_{};

  public:
    static SQLiteHandleCache &get();

    std::shared_ptr<SQLiteHandle> getHandle(const std::string &path,
                                            PJ_CONTEXT *ctx);

    void clear();

#ifdef REOPEN_SQLITE_DB_AFTER_FORK
    void invalidateHandles();
#endif
};

// ---------------------------------------------------------------------------

SQLiteHandleCache &SQLiteHandleCache::get() {
    // Global cache
    static SQLiteHandleCache gSQLiteHandleCache;
    return gSQLiteHandleCache;
}

// ---------------------------------------------------------------------------

void SQLiteHandleCache::clear() {
    std::lock_guard<std::mutex> lock(sMutex_);
    cache_.clear();
}

// ---------------------------------------------------------------------------

std::shared_ptr<SQLiteHandle>
SQLiteHandleCache::getHandle(const std::string &path, PJ_CONTEXT *ctx) {
    std::lock_guard<std::mutex> lock(sMutex_);

#ifdef REOPEN_SQLITE_DB_AFTER_FORK
    if (firstTime_) {
        firstTime_ = false;
        pthread_atfork(
            []() {
                // This mutex needs to be acquired by 'invalidateHandles()'.
                // The forking thread needs to own this mutex during the fork.
                // Otherwise there's an opporunity for another thread to own
                // the mutex during the fork, leaving the child process unable
                // to acquire the mutex in invalidateHandles().
                SQLiteHandleCache::get().sMutex_.lock();
            },
            []() { SQLiteHandleCache::get().sMutex_.unlock(); },
            []() {
                SQLiteHandleCache::get().sMutex_.unlock();
                SQLiteHandleCache::get().invalidateHandles();
            });
    }
#endif

    std::shared_ptr<SQLiteHandle> handle;
    std::string key = path + ctx->custom_sqlite3_vfs_name;
    if (!cache_.tryGet(key, handle)) {
        handle = SQLiteHandle::open(ctx, path);
        cache_.insert(key, handle);
    }
    return handle;
}

#ifdef REOPEN_SQLITE_DB_AFTER_FORK
// ---------------------------------------------------------------------------

void SQLiteHandleCache::invalidateHandles() {
    std::lock_guard<std::mutex> lock(sMutex_);
    const auto lambda =
        [](const lru11::KeyValuePair<std::string, std::shared_ptr<SQLiteHandle>>
               &kvp) { kvp.value->invalidate(); };
    cache_.cwalk(lambda);
    cache_.clear();
}
#endif

// ---------------------------------------------------------------------------

struct DatabaseContext::Private {
    Private();
    ~Private();

    void open(const std::string &databasePath, PJ_CONTEXT *ctx);
    void setHandle(sqlite3 *sqlite_handle);

    const std::shared_ptr<SQLiteHandle> &handle();

    PJ_CONTEXT *pjCtxt() const { return pjCtxt_; }
    void setPjCtxt(PJ_CONTEXT *ctxt) { pjCtxt_ = ctxt; }

    SQLResultSet run(const std::string &sql,
                     const ListOfParams &parameters = ListOfParams(),
                     bool useMaxFloatPrecision = false);

    std::vector<std::string> getDatabaseStructure();

    // cppcheck-suppress functionStatic
    const std::string &getPath() const { return databasePath_; }

    void attachExtraDatabases(
        const std::vector<std::string> &auxiliaryDatabasePaths);

    // Mechanism to detect recursion in calls from
    // AuthorityFactory::createXXX() -> createFromUserInput() ->
    // AuthorityFactory::createXXX()
    struct RecursionDetector {
        explicit RecursionDetector(const DatabaseContextNNPtr &context)
            : dbContext_(context) {
            if (dbContext_->getPrivate()->recLevel_ == 2) {
                // Throw exception before incrementing, since the destructor
                // will not be called
                throw FactoryException("Too many recursive calls");
            }
            ++dbContext_->getPrivate()->recLevel_;
        }

        ~RecursionDetector() { --dbContext_->getPrivate()->recLevel_; }

      private:
        DatabaseContextNNPtr dbContext_;
    };

    std::map<std::string, std::list<SQLRow>> &getMapCanonicalizeGRFName() {
        return mapCanonicalizeGRFName_;
    }

    // cppcheck-suppress functionStatic
    common::UnitOfMeasurePtr getUOMFromCache(const std::string &code);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code, const common::UnitOfMeasureNNPtr &uom);

    // cppcheck-suppress functionStatic
    crs::CRSPtr getCRSFromCache(const std::string &code);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code, const crs::CRSNNPtr &crs);

    datum::GeodeticReferenceFramePtr
    // cppcheck-suppress functionStatic
    getGeodeticDatumFromCache(const std::string &code);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code,
               const datum::GeodeticReferenceFrameNNPtr &datum);

    datum::DatumEnsemblePtr
    // cppcheck-suppress functionStatic
    getDatumEnsembleFromCache(const std::string &code);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code,
               const datum::DatumEnsembleNNPtr &datumEnsemble);

    datum::EllipsoidPtr
    // cppcheck-suppress functionStatic
    getEllipsoidFromCache(const std::string &code);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code, const datum::EllipsoidNNPtr &ellipsoid);

    datum::PrimeMeridianPtr
    // cppcheck-suppress functionStatic
    getPrimeMeridianFromCache(const std::string &code);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code, const datum::PrimeMeridianNNPtr &pm);

    // cppcheck-suppress functionStatic
    cs::CoordinateSystemPtr
    getCoordinateSystemFromCache(const std::string &code);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code, const cs::CoordinateSystemNNPtr &cs);

    // cppcheck-suppress functionStatic
    metadata::ExtentPtr getExtentFromCache(const std::string &code);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code, const metadata::ExtentNNPtr &extent);

    // cppcheck-suppress functionStatic
    bool getCRSToCRSCoordOpFromCache(
        const std::string &code,
        std::vector<operation::CoordinateOperationNNPtr> &list);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code,
               const std::vector<operation::CoordinateOperationNNPtr> &list);

    struct GridInfoCache {
        std::string fullFilename{};
        std::string packageName{};
        std::string url{};
        bool found = false;
        bool directDownload = false;
        bool openLicense = false;
        bool gridAvailable = false;
    };

    // cppcheck-suppress functionStatic
    bool getGridInfoFromCache(const std::string &code, GridInfoCache &info);
    // cppcheck-suppress functionStatic
    void evictGridInfoFromCache(const std::string &code);
    // cppcheck-suppress functionStatic
    void cache(const std::string &code, const GridInfoCache &info);

    struct VersionedAuthName {
        std::string versionedAuthName{};
        std::string authName{};
        std::string version{};
        int priority = 0;
    };
    const std::vector<VersionedAuthName> &getCacheAuthNameWithVersion();

  private:
    friend class DatabaseContext;

    // This is a manual implementation of std::enable_shared_from_this<> that
    // avoids publicly deriving from it.
    std::weak_ptr<DatabaseContext> self_{};

    std::string databasePath_{};
    std::vector<std::string> auxiliaryDatabasePaths_{};
    std::shared_ptr<SQLiteHandle> sqlite_handle_{};
    unsigned int queryCounter_ = 0;
    std::map<std::string, sqlite3_stmt *> mapSqlToStatement_{};
    PJ_CONTEXT *pjCtxt_ = nullptr;
    int recLevel_ = 0;
    bool detach_ = false;
    std::string lastMetadataValue_{};
    std::map<std::string, std::list<SQLRow>> mapCanonicalizeGRFName_{};

    // Used by startInsertStatementsSession() and related functions
    std::string memoryDbForInsertPath_{};
    std::unique_ptr<SQLiteHandle> memoryDbHandle_{};

    using LRUCacheOfObjects = lru11::Cache<std::string, util::BaseObjectPtr>;

    static constexpr size_t CACHE_SIZE = 128;
    LRUCacheOfObjects cacheUOM_{CACHE_SIZE};
    LRUCacheOfObjects cacheCRS_{CACHE_SIZE};
    LRUCacheOfObjects cacheEllipsoid_{CACHE_SIZE};
    LRUCacheOfObjects cacheGeodeticDatum_{CACHE_SIZE};
    LRUCacheOfObjects cacheDatumEnsemble_{CACHE_SIZE};
    LRUCacheOfObjects cachePrimeMeridian_{CACHE_SIZE};
    LRUCacheOfObjects cacheCS_{CACHE_SIZE};
    LRUCacheOfObjects cacheExtent_{CACHE_SIZE};
    lru11::Cache<std::string, std::vector<operation::CoordinateOperationNNPtr>>
        cacheCRSToCrsCoordOp_{CACHE_SIZE};
    lru11::Cache<std::string, GridInfoCache> cacheGridInfo_{CACHE_SIZE};

    std::map<std::string, std::vector<std::string>> cacheAllowedAuthorities_{};

    lru11::Cache<std::string, std::list<std::string>> cacheAliasNames_{
        CACHE_SIZE};
    lru11::Cache<std::string, std::string> cacheNames_{CACHE_SIZE};

    std::vector<VersionedAuthName> cacheAuthNameWithVersion_{};

    static void insertIntoCache(LRUCacheOfObjects &cache,
                                const std::string &code,
                                const util::BaseObjectPtr &obj);

    static void getFromCache(LRUCacheOfObjects &cache, const std::string &code,
                             util::BaseObjectPtr &obj);

    void closeDB() noexcept;

    void clearCaches();

    std::string findFreeCode(const std::string &tableName,
                             const std::string &authName,
                             const std::string &codePrototype);

    void identify(const DatabaseContextNNPtr &dbContext,
                  const cs::CoordinateSystemNNPtr &obj, std::string &authName,
                  std::string &code);
    void identifyOrInsert(const DatabaseContextNNPtr &dbContext,
                          const cs::CoordinateSystemNNPtr &obj,
                          const std::string &ownerType,
                          const std::string &ownerAuthName,
                          const std::string &ownerCode, std::string &authName,
                          std::string &code,
                          std::vector<std::string> &sqlStatements);

    void identify(const DatabaseContextNNPtr &dbContext,
                  const common::UnitOfMeasure &obj, std::string &authName,
                  std::string &code);
    void identifyOrInsert(const DatabaseContextNNPtr &dbContext,
                          const common::UnitOfMeasure &unit,
                          const std::string &ownerAuthName,
                          std::string &authName, std::string &code,
                          std::vector<std::string> &sqlStatements);

    void appendSql(std::vector<std::string> &sqlStatements,
                   const std::string &sql);

    void
    identifyOrInsertUsages(const common::ObjectUsageNNPtr &obj,
                           const std::string &tableName,
                           const std::string &authName, const std::string &code,
                           const std::vector<std::string> &allowedAuthorities,
                           std::vector<std::string> &sqlStatements);

    std::vector<std::string>
    getInsertStatementsFor(const datum::PrimeMeridianNNPtr &pm,
                           const std::string &authName, const std::string &code,
                           bool numericCode,
                           const std::vector<std::string> &allowedAuthorities);

    std::vector<std::string>
    getInsertStatementsFor(const datum::EllipsoidNNPtr &ellipsoid,
                           const std::string &authName, const std::string &code,
                           bool numericCode,
                           const std::vector<std::string> &allowedAuthorities);

    std::vector<std::string>
    getInsertStatementsFor(const datum::GeodeticReferenceFrameNNPtr &datum,
                           const std::string &authName, const std::string &code,
                           bool numericCode,
                           const std::vector<std::string> &allowedAuthorities);

    std::vector<std::string>
    getInsertStatementsFor(const datum::DatumEnsembleNNPtr &ensemble,
                           const std::string &authName, const std::string &code,
                           bool numericCode,
                           const std::vector<std::string> &allowedAuthorities);

    std::vector<std::string>
    getInsertStatementsFor(const crs::GeodeticCRSNNPtr &crs,
                           const std::string &authName, const std::string &code,
                           bool numericCode,
                           const std::vector<std::string> &allowedAuthorities);

    std::vector<std::string>
    getInsertStatementsFor(const crs::ProjectedCRSNNPtr &crs,
                           const std::string &authName, const std::string &code,
                           bool numericCode,
                           const std::vector<std::string> &allowedAuthorities);

    std::vector<std::string>
    getInsertStatementsFor(const datum::VerticalReferenceFrameNNPtr &datum,
                           const std::string &authName, const std::string &code,
                           bool numericCode,
                           const std::vector<std::string> &allowedAuthorities);

    std::vector<std::string>
    getInsertStatementsFor(const crs::VerticalCRSNNPtr &crs,
                           const std::string &authName, const std::string &code,
                           bool numericCode,
                           const std::vector<std::string> &allowedAuthorities);

    std::vector<std::string>
    getInsertStatementsFor(const crs::CompoundCRSNNPtr &crs,
                           const std::string &authName, const std::string &code,
                           bool numericCode,
                           const std::vector<std::string> &allowedAuthorities);

    Private(const Private &) = delete;
    Private &operator=(const Private &) = delete;
};

// ---------------------------------------------------------------------------

DatabaseContext::Private::Private() = default;

// ---------------------------------------------------------------------------

DatabaseContext::Private::~Private() {
    assert(recLevel_ == 0);

    closeDB();
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::closeDB() noexcept {

    if (detach_) {
        // Workaround a bug visible in SQLite 3.8.1 and 3.8.2 that causes
        // a crash in TEST(factory, attachExtraDatabases_auxiliary)
        // due to possible wrong caching of key info.
        // The bug is specific to using a memory file with shared cache as an
        // auxiliary DB.
        // The fix was likely in 3.8.8
        // https://github.com/mackyle/sqlite/commit/d412d4b8731991ecbd8811874aa463d0821673eb
        // But just after 3.8.2,
        // https://github.com/mackyle/sqlite/commit/ccf328c4318eacedab9ed08c404bc4f402dcad19
        // also seemed to hide the issue.
        // Detaching a database hides the issue, not sure if it is by chance...
        try {
            run("DETACH DATABASE db_0");
        } catch (...) {
        }
        detach_ = false;
    }

    for (auto &pair : mapSqlToStatement_) {
        sqlite3_finalize(pair.second);
    }
    mapSqlToStatement_.clear();

    sqlite_handle_.reset();
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::clearCaches() {

    cacheUOM_.clear();
    cacheCRS_.clear();
    cacheEllipsoid_.clear();
    cacheGeodeticDatum_.clear();
    cacheDatumEnsemble_.clear();
    cachePrimeMeridian_.clear();
    cacheCS_.clear();
    cacheExtent_.clear();
    cacheCRSToCrsCoordOp_.clear();
    cacheGridInfo_.clear();
    cacheAllowedAuthorities_.clear();
    cacheAliasNames_.clear();
    cacheNames_.clear();
}

// ---------------------------------------------------------------------------

const std::shared_ptr<SQLiteHandle> &DatabaseContext::Private::handle() {
#ifdef REOPEN_SQLITE_DB_AFTER_FORK
    if (sqlite_handle_ && !sqlite_handle_->isValid()) {
        closeDB();
        open(databasePath_, pjCtxt_);
        if (!auxiliaryDatabasePaths_.empty()) {
            attachExtraDatabases(auxiliaryDatabasePaths_);
        }
    }
#endif
    return sqlite_handle_;
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::insertIntoCache(LRUCacheOfObjects &cache,
                                               const std::string &code,
                                               const util::BaseObjectPtr &obj) {
    cache.insert(code, obj);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::getFromCache(LRUCacheOfObjects &cache,
                                            const std::string &code,
                                            util::BaseObjectPtr &obj) {
    cache.tryGet(code, obj);
}

// ---------------------------------------------------------------------------

bool DatabaseContext::Private::getCRSToCRSCoordOpFromCache(
    const std::string &code,
    std::vector<operation::CoordinateOperationNNPtr> &list) {
    return cacheCRSToCrsCoordOp_.tryGet(code, list);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(
    const std::string &code,
    const std::vector<operation::CoordinateOperationNNPtr> &list) {
    cacheCRSToCrsCoordOp_.insert(code, list);
}

// ---------------------------------------------------------------------------

crs::CRSPtr DatabaseContext::Private::getCRSFromCache(const std::string &code) {
    util::BaseObjectPtr obj;
    getFromCache(cacheCRS_, code, obj);
    return std::static_pointer_cast<crs::CRS>(obj);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(const std::string &code,
                                     const crs::CRSNNPtr &crs) {
    insertIntoCache(cacheCRS_, code, crs.as_nullable());
}

// ---------------------------------------------------------------------------

common::UnitOfMeasurePtr
DatabaseContext::Private::getUOMFromCache(const std::string &code) {
    util::BaseObjectPtr obj;
    getFromCache(cacheUOM_, code, obj);
    return std::static_pointer_cast<common::UnitOfMeasure>(obj);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(const std::string &code,
                                     const common::UnitOfMeasureNNPtr &uom) {
    insertIntoCache(cacheUOM_, code, uom.as_nullable());
}

// ---------------------------------------------------------------------------

datum::GeodeticReferenceFramePtr
DatabaseContext::Private::getGeodeticDatumFromCache(const std::string &code) {
    util::BaseObjectPtr obj;
    getFromCache(cacheGeodeticDatum_, code, obj);
    return std::static_pointer_cast<datum::GeodeticReferenceFrame>(obj);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(
    const std::string &code, const datum::GeodeticReferenceFrameNNPtr &datum) {
    insertIntoCache(cacheGeodeticDatum_, code, datum.as_nullable());
}

// ---------------------------------------------------------------------------

datum::DatumEnsemblePtr
DatabaseContext::Private::getDatumEnsembleFromCache(const std::string &code) {
    util::BaseObjectPtr obj;
    getFromCache(cacheDatumEnsemble_, code, obj);
    return std::static_pointer_cast<datum::DatumEnsemble>(obj);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(
    const std::string &code, const datum::DatumEnsembleNNPtr &datumEnsemble) {
    insertIntoCache(cacheDatumEnsemble_, code, datumEnsemble.as_nullable());
}

// ---------------------------------------------------------------------------

datum::EllipsoidPtr
DatabaseContext::Private::getEllipsoidFromCache(const std::string &code) {
    util::BaseObjectPtr obj;
    getFromCache(cacheEllipsoid_, code, obj);
    return std::static_pointer_cast<datum::Ellipsoid>(obj);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(const std::string &code,
                                     const datum::EllipsoidNNPtr &ellps) {
    insertIntoCache(cacheEllipsoid_, code, ellps.as_nullable());
}

// ---------------------------------------------------------------------------

datum::PrimeMeridianPtr
DatabaseContext::Private::getPrimeMeridianFromCache(const std::string &code) {
    util::BaseObjectPtr obj;
    getFromCache(cachePrimeMeridian_, code, obj);
    return std::static_pointer_cast<datum::PrimeMeridian>(obj);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(const std::string &code,
                                     const datum::PrimeMeridianNNPtr &pm) {
    insertIntoCache(cachePrimeMeridian_, code, pm.as_nullable());
}

// ---------------------------------------------------------------------------

cs::CoordinateSystemPtr DatabaseContext::Private::getCoordinateSystemFromCache(
    const std::string &code) {
    util::BaseObjectPtr obj;
    getFromCache(cacheCS_, code, obj);
    return std::static_pointer_cast<cs::CoordinateSystem>(obj);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(const std::string &code,
                                     const cs::CoordinateSystemNNPtr &cs) {
    insertIntoCache(cacheCS_, code, cs.as_nullable());
}

// ---------------------------------------------------------------------------

metadata::ExtentPtr
DatabaseContext::Private::getExtentFromCache(const std::string &code) {
    util::BaseObjectPtr obj;
    getFromCache(cacheExtent_, code, obj);
    return std::static_pointer_cast<metadata::Extent>(obj);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(const std::string &code,
                                     const metadata::ExtentNNPtr &extent) {
    insertIntoCache(cacheExtent_, code, extent.as_nullable());
}

// ---------------------------------------------------------------------------

bool DatabaseContext::Private::getGridInfoFromCache(const std::string &code,
                                                    GridInfoCache &info) {
    return cacheGridInfo_.tryGet(code, info);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::evictGridInfoFromCache(const std::string &code) {
    cacheGridInfo_.remove(code);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::cache(const std::string &code,
                                     const GridInfoCache &info) {
    cacheGridInfo_.insert(code, info);
}

// ---------------------------------------------------------------------------

std::string loadProjDbFromResource() {
    struct ProjDbTmpFile {
        TTempFileHandle file;
        ProjDbTmpFile() {
            auto buff = NResource::Find("/proj.db");
            file.Write(buff.data(), buff.size());
            file.Close();
        }
    };
    static ProjDbTmpFile db;
    return db.file.Name();
}

void DatabaseContext::Private::open(const std::string &databasePath,
                                    PJ_CONTEXT *ctx) {
    if (!ctx) {
        ctx = pj_get_default_ctx();
    }

    setPjCtxt(ctx);
    std::string path(databasePath);
    if (path.empty()) {
#ifndef USE_ONLY_EMBEDDED_RESOURCE_FILES
        path.resize(2048);
        const bool found =
            pj_find_file(pjCtxt(), "proj.db", &path[0], path.size() - 1) != 0;
        path.resize(strlen(path.c_str()));
        if (!found)
#endif
        {
#ifdef EMBED_RESOURCE_FILES
            path = EMBEDDED_PROJ_DB;
#else
            path = loadProjDbFromResource();
#endif
        }
    }

    sqlite_handle_ = SQLiteHandleCache::get().getHandle(path, ctx);

    databasePath_ = sqlite_handle_->path();
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::setHandle(sqlite3 *sqlite_handle) {

    assert(sqlite_handle);
    assert(!sqlite_handle_);
    sqlite_handle_ = SQLiteHandle::initFromExisting(sqlite_handle, false, 0, 0);
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getDatabaseStructure() {
    const std::string dbNamePrefix(auxiliaryDatabasePaths_.empty() &&
                                           memoryDbForInsertPath_.empty()
                                       ? ""
                                       : "db_0.");
    const auto sqlBegin("SELECT sql||';' FROM " + dbNamePrefix +
                        "sqlite_master WHERE type = ");
    const char *tableType = "'table' AND name NOT LIKE 'sqlite_stat%'";
    const char *const objectTypes[] = {tableType, "'view'", "'trigger'"};
    std::vector<std::string> res;
    for (const auto &objectType : objectTypes) {
        const auto sqlRes = run(sqlBegin + objectType);
        for (const auto &row : sqlRes) {
            res.emplace_back(row[0]);
        }
    }
    if (sqlite_handle_->getLayoutVersionMajor() > 0) {
        res.emplace_back(
            "INSERT INTO metadata VALUES('DATABASE.LAYOUT.VERSION.MAJOR'," +
            toString(sqlite_handle_->getLayoutVersionMajor()) + ");");
        res.emplace_back(
            "INSERT INTO metadata VALUES('DATABASE.LAYOUT.VERSION.MINOR'," +
            toString(sqlite_handle_->getLayoutVersionMinor()) + ");");
    }
    return res;
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::attachExtraDatabases(
    const std::vector<std::string> &auxiliaryDatabasePaths) {

    auto l_handle = handle();
    assert(l_handle);

    auto tables = run("SELECT name, type, sql FROM sqlite_master WHERE type IN "
                      "('table', 'view') "
                      "AND name NOT LIKE 'sqlite_stat%'");

    struct TableStructure {
        std::string name{};
        bool isTable = false;
        std::string sql{};
        std::vector<std::string> columns{};
    };
    std::vector<TableStructure> tablesStructure;
    for (const auto &rowTable : tables) {
        TableStructure tableStructure;
        tableStructure.name = rowTable[0];
        tableStructure.isTable = rowTable[1] == "table";
        tableStructure.sql = rowTable[2];
        auto tableInfo =
            run("PRAGMA table_info(\"" +
                replaceAll(tableStructure.name, "\"", "\"\"") + "\")");
        for (const auto &rowCol : tableInfo) {
            const auto &colName = rowCol[1];
            tableStructure.columns.push_back(colName);
        }
        tablesStructure.push_back(std::move(tableStructure));
    }

    const int nLayoutVersionMajor = l_handle->getLayoutVersionMajor();
    const int nLayoutVersionMinor = l_handle->getLayoutVersionMinor();

    closeDB();
    if (auxiliaryDatabasePaths.empty()) {
        open(databasePath_, pjCtxt());
        return;
    }

    sqlite3 *sqlite_handle = nullptr;
    sqlite3_open_v2(
        ":memory:", &sqlite_handle,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_URI, nullptr);
    if (!sqlite_handle) {
        throw FactoryException("cannot create in memory database");
    }
    sqlite_handle_ = SQLiteHandle::initFromExisting(
        sqlite_handle, true, nLayoutVersionMajor, nLayoutVersionMinor);
    l_handle = sqlite_handle_;

    run("ATTACH DATABASE ? AS db_0", {databasePath_});
    detach_ = true;
    int count = 1;
    for (const auto &otherDbPath : auxiliaryDatabasePaths) {
        const auto attachedDbName("db_" + toString(static_cast<int>(count)));
        std::string sql = "ATTACH DATABASE ? AS ";
        sql += attachedDbName;
        count++;
        run(sql, {otherDbPath});

        l_handle->checkDatabaseLayout(databasePath_, otherDbPath,
                                      attachedDbName + '.');
    }

    for (const auto &tableStructure : tablesStructure) {
        if (tableStructure.isTable) {
            std::string sql("CREATE TEMP VIEW ");
            sql += tableStructure.name;
            sql += " AS ";
            for (size_t i = 0; i <= auxiliaryDatabasePaths.size(); ++i) {
                std::string selectFromAux("SELECT ");
                bool firstCol = true;
                for (const auto &colName : tableStructure.columns) {
                    if (!firstCol) {
                        selectFromAux += ", ";
                    }
                    firstCol = false;
                    selectFromAux += colName;
                }
                selectFromAux += " FROM db_";
                selectFromAux += toString(static_cast<int>(i));
                selectFromAux += ".";
                selectFromAux += tableStructure.name;

                try {
                    // Check that the request will succeed. In case of 'sparse'
                    // databases...
                    run(selectFromAux + " LIMIT 0");

                    if (i > 0) {
                        if (tableStructure.name == "conversion_method")
                            sql += " UNION ";
                        else
                            sql += " UNION ALL ";
                    }
                    sql += selectFromAux;
                } catch (const std::exception &) {
                }
            }
            run(sql);
        } else {
            run(replaceAll(tableStructure.sql, "CREATE VIEW",
                           "CREATE TEMP VIEW"));
        }
    }
}

// ---------------------------------------------------------------------------

SQLResultSet DatabaseContext::Private::run(const std::string &sql,
                                           const ListOfParams &parameters,
                                           bool useMaxFloatPrecision) {

    auto l_handle = handle();
    assert(l_handle);

    sqlite3_stmt *stmt = nullptr;
    auto iter = mapSqlToStatement_.find(sql);
    if (iter != mapSqlToStatement_.end()) {
        stmt = iter->second;
        sqlite3_reset(stmt);
    } else {
        if (sqlite3_prepare_v2(l_handle->handle(), sql.c_str(),
                               static_cast<int>(sql.size()), &stmt,
                               nullptr) != SQLITE_OK) {
            throw FactoryException(
                std::string("SQLite error [ ")
                    .append(sqlite3_errmsg(l_handle->handle()))
                    .append(" ] on ")
                    .append(sql));
        }
        mapSqlToStatement_.insert(
            std::pair<std::string, sqlite3_stmt *>(sql, stmt));
    }

    ++queryCounter_;

    return l_handle->run(stmt, sql, parameters, useMaxFloatPrecision);
}

// ---------------------------------------------------------------------------

static std::string formatStatement(const char *fmt, ...) {
    std::string res;
    va_list args;
    va_start(args, fmt);
    for (int i = 0; fmt[i] != '\0'; ++i) {
        if (fmt[i] == '%') {
            if (fmt[i + 1] == '%') {
                res += '%';
            } else if (fmt[i + 1] == 'q') {
                const char *arg = va_arg(args, const char *);
                for (int j = 0; arg[j] != '\0'; ++j) {
                    if (arg[j] == '\'')
                        res += arg[j];
                    res += arg[j];
                }
            } else if (fmt[i + 1] == 'Q') {
                const char *arg = va_arg(args, const char *);
                if (arg == nullptr)
                    res += "NULL";
                else {
                    res += '\'';
                    for (int j = 0; arg[j] != '\0'; ++j) {
                        if (arg[j] == '\'')
                            res += arg[j];
                        res += arg[j];
                    }
                    res += '\'';
                }
            } else if (fmt[i + 1] == 's') {
                const char *arg = va_arg(args, const char *);
                res += arg;
            } else if (fmt[i + 1] == 'f') {
                const double arg = va_arg(args, double);
                res += toString(arg);
            } else if (fmt[i + 1] == 'd') {
                const int arg = va_arg(args, int);
                res += toString(arg);
            } else {
                va_end(args);
                throw FactoryException(
                    "Unsupported formatter in formatStatement()");
            }
            ++i;
        } else {
            res += fmt[i];
        }
    }
    va_end(args);
    return res;
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::appendSql(
    std::vector<std::string> &sqlStatements, const std::string &sql) {
    sqlStatements.emplace_back(sql);
    char *errMsg = nullptr;
    if (sqlite3_exec(memoryDbHandle_->handle(), sql.c_str(), nullptr, nullptr,
                     &errMsg) != SQLITE_OK) {
        std::string s("Cannot execute " + sql);
        if (errMsg) {
            s += " : ";
            s += errMsg;
        }
        sqlite3_free(errMsg);
        throw FactoryException(s);
    }
    sqlite3_free(errMsg);
}

// ---------------------------------------------------------------------------

static void identifyFromNameOrCode(
    const DatabaseContextNNPtr &dbContext,
    const std::vector<std::string> &allowedAuthorities,
    const std::string &authNameParent, const common::IdentifiedObjectNNPtr &obj,
    std::function<std::shared_ptr<util::IComparable>(
        const AuthorityFactoryNNPtr &authFactory, const std::string &)>
        instantiateFunc,
    AuthorityFactory::ObjectType objType, std::string &authName,
    std::string &code) {

    auto allowedAuthoritiesTmp(allowedAuthorities);
    allowedAuthoritiesTmp.emplace_back(authNameParent);

    for (const auto &id : obj->identifiers()) {
        try {
            const auto &idAuthName = *(id->codeSpace());
            if (std::find(allowedAuthoritiesTmp.begin(),
                          allowedAuthoritiesTmp.end(),
                          idAuthName) != allowedAuthoritiesTmp.end()) {
                const auto factory =
                    AuthorityFactory::create(dbContext, idAuthName);
                if (instantiateFunc(factory, id->code())
                        ->isEquivalentTo(
                            obj.get(),
                            util::IComparable::Criterion::EQUIVALENT)) {
                    authName = idAuthName;
                    code = id->code();
                    return;
                }
            }
        } catch (const std::exception &) {
        }
    }

    for (const auto &allowedAuthority : allowedAuthoritiesTmp) {
        const auto factory =
            AuthorityFactory::create(dbContext, allowedAuthority);
        const auto candidates =
            factory->createObjectsFromName(obj->nameStr(), {objType}, false, 0);
        for (const auto &candidate : candidates) {
            const auto &ids = candidate->identifiers();
            if (!ids.empty() &&
                candidate->isEquivalentTo(
                    obj.get(), util::IComparable::Criterion::EQUIVALENT)) {
                const auto &id = ids.front();
                authName = *(id->codeSpace());
                code = id->code();
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------

static void
identifyFromNameOrCode(const DatabaseContextNNPtr &dbContext,
                       const std::vector<std::string> &allowedAuthorities,
                       const std::string &authNameParent,
                       const datum::DatumEnsembleNNPtr &obj,
                       std::string &authName, std::string &code) {
    const char *type = "geodetic_datum";
    if (!obj->datums().empty() &&
        dynamic_cast<const datum::VerticalReferenceFrame *>(
            obj->datums().front().get())) {
        type = "vertical_datum";
    }
    const auto instantiateFunc =
        [&type](const AuthorityFactoryNNPtr &authFactory,
                const std::string &lCode) {
            return util::nn_static_pointer_cast<util::IComparable>(
                authFactory->createDatumEnsemble(lCode, type));
        };
    identifyFromNameOrCode(
        dbContext, allowedAuthorities, authNameParent, obj, instantiateFunc,
        AuthorityFactory::ObjectType::DATUM_ENSEMBLE, authName, code);
}

// ---------------------------------------------------------------------------

static void
identifyFromNameOrCode(const DatabaseContextNNPtr &dbContext,
                       const std::vector<std::string> &allowedAuthorities,
                       const std::string &authNameParent,
                       const datum::GeodeticReferenceFrameNNPtr &obj,
                       std::string &authName, std::string &code) {
    const auto instantiateFunc = [](const AuthorityFactoryNNPtr &authFactory,
                                    const std::string &lCode) {
        return util::nn_static_pointer_cast<util::IComparable>(
            authFactory->createGeodeticDatum(lCode));
    };
    identifyFromNameOrCode(
        dbContext, allowedAuthorities, authNameParent, obj, instantiateFunc,
        AuthorityFactory::ObjectType::GEODETIC_REFERENCE_FRAME, authName, code);
}

// ---------------------------------------------------------------------------

static void
identifyFromNameOrCode(const DatabaseContextNNPtr &dbContext,
                       const std::vector<std::string> &allowedAuthorities,
                       const std::string &authNameParent,
                       const datum::EllipsoidNNPtr &obj, std::string &authName,
                       std::string &code) {
    const auto instantiateFunc = [](const AuthorityFactoryNNPtr &authFactory,
                                    const std::string &lCode) {
        return util::nn_static_pointer_cast<util::IComparable>(
            authFactory->createEllipsoid(lCode));
    };
    identifyFromNameOrCode(
        dbContext, allowedAuthorities, authNameParent, obj, instantiateFunc,
        AuthorityFactory::ObjectType::ELLIPSOID, authName, code);
}

// ---------------------------------------------------------------------------

static void
identifyFromNameOrCode(const DatabaseContextNNPtr &dbContext,
                       const std::vector<std::string> &allowedAuthorities,
                       const std::string &authNameParent,
                       const datum::PrimeMeridianNNPtr &obj,
                       std::string &authName, std::string &code) {
    const auto instantiateFunc = [](const AuthorityFactoryNNPtr &authFactory,
                                    const std::string &lCode) {
        return util::nn_static_pointer_cast<util::IComparable>(
            authFactory->createPrimeMeridian(lCode));
    };
    identifyFromNameOrCode(
        dbContext, allowedAuthorities, authNameParent, obj, instantiateFunc,
        AuthorityFactory::ObjectType::PRIME_MERIDIAN, authName, code);
}

// ---------------------------------------------------------------------------

static void
identifyFromNameOrCode(const DatabaseContextNNPtr &dbContext,
                       const std::vector<std::string> &allowedAuthorities,
                       const std::string &authNameParent,
                       const datum::VerticalReferenceFrameNNPtr &obj,
                       std::string &authName, std::string &code) {
    const auto instantiateFunc = [](const AuthorityFactoryNNPtr &authFactory,
                                    const std::string &lCode) {
        return util::nn_static_pointer_cast<util::IComparable>(
            authFactory->createVerticalDatum(lCode));
    };
    identifyFromNameOrCode(
        dbContext, allowedAuthorities, authNameParent, obj, instantiateFunc,
        AuthorityFactory::ObjectType::VERTICAL_REFERENCE_FRAME, authName, code);
}

// ---------------------------------------------------------------------------

static void
identifyFromNameOrCode(const DatabaseContextNNPtr &dbContext,
                       const std::vector<std::string> &allowedAuthorities,
                       const std::string &authNameParent,
                       const datum::DatumNNPtr &obj, std::string &authName,
                       std::string &code) {
    if (const auto geodeticDatum =
            util::nn_dynamic_pointer_cast<datum::GeodeticReferenceFrame>(obj)) {
        identifyFromNameOrCode(dbContext, allowedAuthorities, authNameParent,
                               NN_NO_CHECK(geodeticDatum), authName, code);
    } else if (const auto verticalDatum =
                   util::nn_dynamic_pointer_cast<datum::VerticalReferenceFrame>(
                       obj)) {
        identifyFromNameOrCode(dbContext, allowedAuthorities, authNameParent,
                               NN_NO_CHECK(verticalDatum), authName, code);
    } else {
        throw FactoryException("Unhandled type of datum");
    }
}

// ---------------------------------------------------------------------------

static const char *getCSDatabaseType(const cs::CoordinateSystemNNPtr &obj) {
    if (dynamic_cast<const cs::EllipsoidalCS *>(obj.get())) {
        return CS_TYPE_ELLIPSOIDAL;
    } else if (dynamic_cast<const cs::CartesianCS *>(obj.get())) {
        return CS_TYPE_CARTESIAN;
    } else if (dynamic_cast<const cs::VerticalCS *>(obj.get())) {
        return CS_TYPE_VERTICAL;
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

std::string
DatabaseContext::Private::findFreeCode(const std::string &tableName,
                                       const std::string &authName,
                                       const std::string &codePrototype) {
    std::string code(codePrototype);
    if (run("SELECT 1 FROM " + tableName + " WHERE auth_name = ? AND code = ?",
            {authName, code})
            .empty()) {
        return code;
    }

    for (int counter = 2; counter < 10; counter++) {
        code = codePrototype + '_' + toString(counter);
        if (run("SELECT 1 FROM " + tableName +
                    " WHERE auth_name = ? AND code = ?",
                {authName, code})
                .empty()) {
            return code;
        }
    }

    // shouldn't happen hopefully...
    throw FactoryException("Cannot insert " + tableName +
                           ": too many similar codes");
}

// ---------------------------------------------------------------------------

static const char *getUnitDatabaseType(const common::UnitOfMeasure &unit) {
    switch (unit.type()) {
    case common::UnitOfMeasure::Type::LINEAR:
        return "length";

    case common::UnitOfMeasure::Type::ANGULAR:
        return "angle";

    case common::UnitOfMeasure::Type::SCALE:
        return "scale";

    case common::UnitOfMeasure::Type::TIME:
        return "time";

    default:
        break;
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::identify(const DatabaseContextNNPtr &dbContext,
                                        const common::UnitOfMeasure &obj,
                                        std::string &authName,
                                        std::string &code) {
    // Identify quickly a few well-known units
    const double convFactor = obj.conversionToSI();
    switch (obj.type()) {
    case common::UnitOfMeasure::Type::LINEAR: {
        if (convFactor == 1.0) {
            authName = metadata::Identifier::EPSG;
            code = "9001";
            return;
        }
        break;
    }
    case common::UnitOfMeasure::Type::ANGULAR: {
        constexpr double CONV_FACTOR_DEGREE = 1.74532925199432781271e-02;
        if (std::abs(convFactor - CONV_FACTOR_DEGREE) <=
            1e-10 * CONV_FACTOR_DEGREE) {
            authName = metadata::Identifier::EPSG;
            code = "9102";
            return;
        }
        break;
    }
    case common::UnitOfMeasure::Type::SCALE: {
        if (convFactor == 1.0) {
            authName = metadata::Identifier::EPSG;
            code = "9201";
            return;
        }
        break;
    }
    default:
        break;
    }

    std::string sql("SELECT auth_name, code FROM unit_of_measure "
                    "WHERE abs(conv_factor - ?) <= 1e-10 * conv_factor");
    ListOfParams params{convFactor};
    const char *type = getUnitDatabaseType(obj);
    if (type) {
        sql += " AND type = ?";
        params.emplace_back(std::string(type));
    }
    sql += " ORDER BY auth_name, code";
    const auto res = run(sql, params);
    for (const auto &row : res) {
        const auto &rowAuthName = row[0];
        const auto &rowCode = row[1];
        const auto tmpAuthFactory =
            AuthorityFactory::create(dbContext, rowAuthName);
        try {
            tmpAuthFactory->createUnitOfMeasure(rowCode);
            authName = rowAuthName;
            code = rowCode;
            return;
        } catch (const std::exception &) {
        }
    }
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::identifyOrInsert(
    const DatabaseContextNNPtr &dbContext, const common::UnitOfMeasure &unit,
    const std::string &ownerAuthName, std::string &authName, std::string &code,
    std::vector<std::string> &sqlStatements) {
    authName = unit.codeSpace();
    code = unit.code();
    if (authName.empty()) {
        identify(dbContext, unit, authName, code);
    }
    if (!authName.empty()) {
        return;
    }
    const char *type = getUnitDatabaseType(unit);
    if (type == nullptr) {
        throw FactoryException("Cannot insert this type of UnitOfMeasure");
    }

    // Insert new record
    authName = ownerAuthName;
    const std::string codePrototype(replaceAll(toupper(unit.name()), " ", "_"));
    code = findFreeCode("unit_of_measure", authName, codePrototype);

    const auto sql = formatStatement(
        "INSERT INTO unit_of_measure VALUES('%q','%q','%q','%q',%f,NULL,0);",
        authName.c_str(), code.c_str(), unit.name().c_str(), type,
        unit.conversionToSI());
    appendSql(sqlStatements, sql);
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::identify(const DatabaseContextNNPtr &dbContext,
                                        const cs::CoordinateSystemNNPtr &obj,
                                        std::string &authName,
                                        std::string &code) {

    const auto &axisList = obj->axisList();
    if (axisList.size() == 1U &&
        axisList[0]->unit()._isEquivalentTo(UnitOfMeasure::METRE) &&
        &(axisList[0]->direction()) == &cs::AxisDirection::UP &&
        (axisList[0]->nameStr() == "Up" ||
         axisList[0]->nameStr() == "Gravity-related height")) {
        // preferred coordinate system for gravity-related height
        authName = metadata::Identifier::EPSG;
        code = "6499";
        return;
    }

    std::string sql(
        "SELECT auth_name, code FROM coordinate_system WHERE dimension = ?");
    ListOfParams params{static_cast<int>(axisList.size())};
    const char *type = getCSDatabaseType(obj);
    if (type) {
        sql += " AND type = ?";
        params.emplace_back(std::string(type));
    }
    sql += " ORDER BY auth_name, code";
    const auto res = run(sql, params);
    for (const auto &row : res) {
        const auto &rowAuthName = row[0];
        const auto &rowCode = row[1];
        const auto tmpAuthFactory =
            AuthorityFactory::create(dbContext, rowAuthName);
        try {
            const auto cs = tmpAuthFactory->createCoordinateSystem(rowCode);
            if (cs->_isEquivalentTo(obj.get(),
                                    util::IComparable::Criterion::EQUIVALENT)) {
                authName = rowAuthName;
                code = rowCode;
                if (authName == metadata::Identifier::EPSG && code == "4400") {
                    // preferred coordinate system for cartesian
                    // Easting, Northing
                    return;
                }
                if (authName == metadata::Identifier::EPSG && code == "6422") {
                    // preferred coordinate system for geographic lat, long
                    return;
                }
                if (authName == metadata::Identifier::EPSG && code == "6423") {
                    // preferred coordinate system for geographic lat, long, h
                    return;
                }
            }
        } catch (const std::exception &) {
        }
    }
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::identifyOrInsert(
    const DatabaseContextNNPtr &dbContext, const cs::CoordinateSystemNNPtr &obj,
    const std::string &ownerType, const std::string &ownerAuthName,
    const std::string &ownerCode, std::string &authName, std::string &code,
    std::vector<std::string> &sqlStatements) {

    identify(dbContext, obj, authName, code);
    if (!authName.empty()) {
        return;
    }

    const char *type = getCSDatabaseType(obj);
    if (type == nullptr) {
        throw FactoryException("Cannot insert this type of CoordinateSystem");
    }

    // Insert new record in coordinate_system
    authName = ownerAuthName;
    const std::string codePrototype("CS_" + ownerType + '_' + ownerCode);
    code = findFreeCode("coordinate_system", authName, codePrototype);

    const auto &axisList = obj->axisList();
    {
        const auto sql = formatStatement(
            "INSERT INTO coordinate_system VALUES('%q','%q','%q',%d);",
            authName.c_str(), code.c_str(), type,
            static_cast<int>(axisList.size()));
        appendSql(sqlStatements, sql);
    }

    // Insert new records for the axis
    for (int i = 0; i < static_cast<int>(axisList.size()); ++i) {
        const auto &axis = axisList[i];
        std::string uomAuthName;
        std::string uomCode;
        identifyOrInsert(dbContext, axis->unit(), ownerAuthName, uomAuthName,
                         uomCode, sqlStatements);
        const auto sql = formatStatement(
            "INSERT INTO axis VALUES("
            "'%q','%q','%q','%q','%q','%q','%q',%d,'%q','%q');",
            authName.c_str(), (code + "_AXIS_" + toString(i + 1)).c_str(),
            axis->nameStr().c_str(), axis->abbreviation().c_str(),
            axis->direction().toString().c_str(), authName.c_str(),
            code.c_str(), i + 1, uomAuthName.c_str(), uomCode.c_str());
        appendSql(sqlStatements, sql);
    }
}

// ---------------------------------------------------------------------------

static void
addAllowedAuthoritiesCond(const std::vector<std::string> &allowedAuthorities,
                          const std::string &authName, std::string &sql,
                          ListOfParams &params) {
    sql += "auth_name IN (?";
    params.emplace_back(authName);
    for (const auto &allowedAuthority : allowedAuthorities) {
        sql += ",?";
        params.emplace_back(allowedAuthority);
    }
    sql += ')';
}

// ---------------------------------------------------------------------------

void DatabaseContext::Private::identifyOrInsertUsages(
    const common::ObjectUsageNNPtr &obj, const std::string &tableName,
    const std::string &authName, const std::string &code,
    const std::vector<std::string> &allowedAuthorities,
    std::vector<std::string> &sqlStatements) {

    std::string usageCode("USAGE_");
    const std::string upperTableName(toupper(tableName));
    if (!starts_with(code, upperTableName)) {
        usageCode += upperTableName;
        usageCode += '_';
    }
    usageCode += code;

    const auto &domains = obj->domains();
    if (domains.empty()) {
        const auto sql =
            formatStatement("INSERT INTO usage VALUES('%q','%q','%q','%q','%q',"
                            "'PROJ','EXTENT_UNKNOWN','PROJ','SCOPE_UNKNOWN');",
                            authName.c_str(), usageCode.c_str(),
                            tableName.c_str(), authName.c_str(), code.c_str());
        appendSql(sqlStatements, sql);
        return;
    }

    int usageCounter = 1;
    for (const auto &domain : domains) {
        std::string scopeAuthName;
        std::string scopeCode;
        const auto &scope = domain->scope();
        if (scope.has_value()) {
            std::string sql =
                "SELECT auth_name, code, "
                "(CASE WHEN auth_name = 'EPSG' THEN 0 ELSE 1 END) "
                "AS order_idx "
                "FROM scope WHERE scope = ? AND deprecated = 0 AND ";
            ListOfParams params{*scope};
            addAllowedAuthoritiesCond(allowedAuthorities, authName, sql,
                                      params);
            sql += " ORDER BY order_idx, auth_name, code";
            const auto rows = run(sql, params);
            if (!rows.empty()) {
                const auto &row = rows.front();
                scopeAuthName = row[0];
                scopeCode = row[1];
            } else {
                scopeAuthName = authName;
                scopeCode = "SCOPE_";
                scopeCode += tableName;
                scopeCode += '_';
                scopeCode += code;
                const auto sqlToInsert = formatStatement(
                    "INSERT INTO scope VALUES('%q','%q','%q',0);",
                    scopeAuthName.c_str(), scopeCode.c_str(), scope->c_str());
                appendSql(sqlStatements, sqlToInsert);
            }
        } else {
            scopeAuthName = "PROJ";
            scopeCode = "SCOPE_UNKNOWN";
        }

        std::string extentAuthName("PROJ");
        std::string extentCode("EXTENT_UNKNOWN");
        const auto &extent = domain->domainOfValidity();
        if (extent) {
            const auto &geogElts = extent->geographicElements();
            if (!geogElts.empty()) {
                const auto bbox =
                    dynamic_cast<const metadata::GeographicBoundingBox *>(
                        geogElts.front().get());
                if (bbox) {
                    std::string sql =
                        "SELECT auth_name, code, "
                        "(CASE WHEN auth_name = 'EPSG' THEN 0 ELSE 1 END) "
                        "AS order_idx "
                        "FROM extent WHERE south_lat = ? AND north_lat = ? "
                        "AND west_lon = ? AND east_lon = ? AND deprecated = 0 "
                        "AND ";
                    ListOfParams params{
                        bbox->southBoundLatitude(), bbox->northBoundLatitude(),
                        bbox->westBoundLongitude(), bbox->eastBoundLongitude()};
                    addAllowedAuthoritiesCond(allowedAuthorities, authName, sql,
                                              params);
                    sql += " ORDER BY order_idx, auth_name, code";
                    const auto rows = run(sql, params);
                    if (!rows.empty()) {
                        const auto &row = rows.front();
                        extentAuthName = row[0];
                        extentCode = row[1];
                    } else {
                        extentAuthName = authName;
                        extentCode = "EXTENT_";
                        extentCode += tableName;
                        extentCode += '_';
                        extentCode += code;
                        std::string description(*(extent->description()));
                        if (description.empty()) {
                            description = "unknown";
                        }
                        const auto sqlToInsert = formatStatement(
                            "INSERT INTO extent "
                            "VALUES('%q','%q','%q','%q',%f,%f,%f,%f,0);",
                            extentAuthName.c_str(), extentCode.c_str(),
                            description.c_str(), description.c_str(),
                            bbox->southBoundLatitude(),
                            bbox->northBoundLatitude(),
                            bbox->westBoundLongitude(),
                            bbox->eastBoundLongitude());
                        appendSql(sqlStatements, sqlToInsert);
                    }
                }
            }
        }

        if (domains.size() > 1) {
            usageCode += '_';
            usageCode += toString(usageCounter);
        }
        const auto sql = formatStatement(
            "INSERT INTO usage VALUES('%q','%q','%q','%q','%q',"
            "'%q','%q','%q','%q');",
            authName.c_str(), usageCode.c_str(), tableName.c_str(),
            authName.c_str(), code.c_str(), extentAuthName.c_str(),
            extentCode.c_str(), scopeAuthName.c_str(), scopeCode.c_str());
        appendSql(sqlStatements, sql);

        usageCounter++;
    }
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getInsertStatementsFor(
    const datum::PrimeMeridianNNPtr &pm, const std::string &authName,
    const std::string &code, bool /*numericCode*/,
    const std::vector<std::string> &allowedAuthorities) {

    const auto self = NN_NO_CHECK(self_.lock());

    // Check if the object is already known under that code
    std::string pmAuthName;
    std::string pmCode;
    identifyFromNameOrCode(self, allowedAuthorities, authName, pm, pmAuthName,
                           pmCode);
    if (pmAuthName == authName && pmCode == code) {
        return {};
    }

    std::vector<std::string> sqlStatements;

    // Insert new record in prime_meridian table
    std::string uomAuthName;
    std::string uomCode;
    identifyOrInsert(self, pm->longitude().unit(), authName, uomAuthName,
                     uomCode, sqlStatements);

    const auto sql = formatStatement(
        "INSERT INTO prime_meridian VALUES("
        "'%q','%q','%q',%f,'%q','%q',0);",
        authName.c_str(), code.c_str(), pm->nameStr().c_str(),
        pm->longitude().value(), uomAuthName.c_str(), uomCode.c_str());
    appendSql(sqlStatements, sql);

    return sqlStatements;
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getInsertStatementsFor(
    const datum::EllipsoidNNPtr &ellipsoid, const std::string &authName,
    const std::string &code, bool /*numericCode*/,
    const std::vector<std::string> &allowedAuthorities) {

    const auto self = NN_NO_CHECK(self_.lock());

    // Check if the object is already known under that code
    std::string ellipsoidAuthName;
    std::string ellipsoidCode;
    identifyFromNameOrCode(self, allowedAuthorities, authName, ellipsoid,
                           ellipsoidAuthName, ellipsoidCode);
    if (ellipsoidAuthName == authName && ellipsoidCode == code) {
        return {};
    }

    std::vector<std::string> sqlStatements;

    // Find or insert celestial body
    const auto &semiMajorAxis = ellipsoid->semiMajorAxis();
    const double semiMajorAxisMetre = semiMajorAxis.getSIValue();
    constexpr double tolerance = 0.005;
    std::string bodyAuthName;
    std::string bodyCode;
    auto res = run("SELECT auth_name, code, "
                   "(ABS(semi_major_axis - ?) / semi_major_axis ) "
                   "AS rel_error FROM celestial_body WHERE rel_error <= ?",
                   {semiMajorAxisMetre, tolerance});
    if (!res.empty()) {
        const auto &row = res.front();
        bodyAuthName = row[0];
        bodyCode = row[1];
    } else {
        bodyAuthName = authName;
        bodyCode = "BODY_" + code;
        const auto bodyName = "Body of " + ellipsoid->nameStr();
        const auto sql = formatStatement(
            "INSERT INTO celestial_body VALUES('%q','%q','%q',%f);",
            bodyAuthName.c_str(), bodyCode.c_str(), bodyName.c_str(),
            semiMajorAxisMetre);
        appendSql(sqlStatements, sql);
    }

    // Insert new record in ellipsoid table
    std::string uomAuthName;
    std::string uomCode;
    identifyOrInsert(self, semiMajorAxis.unit(), authName, uomAuthName, uomCode,
                     sqlStatements);
    std::string invFlattening("NULL");
    std::string semiMinorAxis("NULL");
    if (ellipsoid->isSphere() || ellipsoid->semiMinorAxis().has_value()) {
        semiMinorAxis = toString(ellipsoid->computeSemiMinorAxis().value());
    } else {
        invFlattening = toString(ellipsoid->computedInverseFlattening());
    }

    const auto sql = formatStatement(
        "INSERT INTO ellipsoid VALUES("
        "'%q','%q','%q','%q','%q','%q',%f,'%q','%q',%s,%s,0);",
        authName.c_str(), code.c_str(), ellipsoid->nameStr().c_str(),
        "", // description
        bodyAuthName.c_str(), bodyCode.c_str(), semiMajorAxis.value(),
        uomAuthName.c_str(), uomCode.c_str(), invFlattening.c_str(),
        semiMinorAxis.c_str());
    appendSql(sqlStatements, sql);

    return sqlStatements;
}

// ---------------------------------------------------------------------------

static std::string anchorEpochToStr(double val) {
    constexpr int BUF_SIZE = 16;
    char szBuffer[BUF_SIZE];
    sqlite3_snprintf(BUF_SIZE, szBuffer, "%.3f", val);
    return szBuffer;
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getInsertStatementsFor(
    const datum::GeodeticReferenceFrameNNPtr &datum,
    const std::string &authName, const std::string &code, bool numericCode,
    const std::vector<std::string> &allowedAuthorities) {

    const auto self = NN_NO_CHECK(self_.lock());

    // Check if the object is already known under that code
    std::string datumAuthName;
    std::string datumCode;
    identifyFromNameOrCode(self, allowedAuthorities, authName, datum,
                           datumAuthName, datumCode);
    if (datumAuthName == authName && datumCode == code) {
        return {};
    }

    std::vector<std::string> sqlStatements;

    // Find or insert ellipsoid
    std::string ellipsoidAuthName;
    std::string ellipsoidCode;
    const auto &ellipsoidOfDatum = datum->ellipsoid();
    identifyFromNameOrCode(self, allowedAuthorities, authName, ellipsoidOfDatum,
                           ellipsoidAuthName, ellipsoidCode);
    if (ellipsoidAuthName.empty()) {
        ellipsoidAuthName = authName;
        if (numericCode) {
            ellipsoidCode = self->suggestsCodeFor(ellipsoidOfDatum,
                                                  ellipsoidAuthName, true);
        } else {
            ellipsoidCode = "ELLPS_" + code;
        }
        sqlStatements = self->getInsertStatementsFor(
            ellipsoidOfDatum, ellipsoidAuthName, ellipsoidCode, numericCode,
            allowedAuthorities);
    }

    // Find or insert prime meridian
    std::string pmAuthName;
    std::string pmCode;
    const auto &pmOfDatum = datum->primeMeridian();
    identifyFromNameOrCode(self, allowedAuthorities, authName, pmOfDatum,
                           pmAuthName, pmCode);
    if (pmAuthName.empty()) {
        pmAuthName = authName;
        if (numericCode) {
            pmCode = self->suggestsCodeFor(pmOfDatum, pmAuthName, true);
        } else {
            pmCode = "PM_" + code;
        }
        const auto sqlStatementsTmp = self->getInsertStatementsFor(
            pmOfDatum, pmAuthName, pmCode, numericCode, allowedAuthorities);
        sqlStatements.insert(sqlStatements.end(), sqlStatementsTmp.begin(),
                             sqlStatementsTmp.end());
    }

    // Insert new record in geodetic_datum table
    std::string publicationDate("NULL");
    if (datum->publicationDate().has_value()) {
        publicationDate = '\'';
        publicationDate +=
            replaceAll(datum->publicationDate()->toString(), "'", "''");
        publicationDate += '\'';
    }
    std::string frameReferenceEpoch("NULL");
    const auto dynamicDatum =
        dynamic_cast<const datum::DynamicGeodeticReferenceFrame *>(datum.get());
    if (dynamicDatum) {
        frameReferenceEpoch =
            toString(dynamicDatum->frameReferenceEpoch().value());
    }
    const std::string anchor(*(datum->anchorDefinition()));
    const util::optional<common::Measure> &anchorEpoch = datum->anchorEpoch();
    const auto sql = formatStatement(
        "INSERT INTO geodetic_datum VALUES("
        "'%q','%q','%q','%q','%q','%q','%q','%q',%s,%s,NULL,%Q,%s,0);",
        authName.c_str(), code.c_str(), datum->nameStr().c_str(),
        "", // description
        ellipsoidAuthName.c_str(), ellipsoidCode.c_str(), pmAuthName.c_str(),
        pmCode.c_str(), publicationDate.c_str(), frameReferenceEpoch.c_str(),
        anchor.empty() ? nullptr : anchor.c_str(),
        anchorEpoch.has_value()
            ? anchorEpochToStr(
                  anchorEpoch->convertToUnit(common::UnitOfMeasure::YEAR))
                  .c_str()
            : "NULL");
    appendSql(sqlStatements, sql);

    identifyOrInsertUsages(datum, "geodetic_datum", authName, code,
                           allowedAuthorities, sqlStatements);

    return sqlStatements;
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getInsertStatementsFor(
    const datum::DatumEnsembleNNPtr &ensemble, const std::string &authName,
    const std::string &code, bool numericCode,
    const std::vector<std::string> &allowedAuthorities) {
    const auto self = NN_NO_CHECK(self_.lock());

    // Check if the object is already known under that code
    std::string datumAuthName;
    std::string datumCode;
    identifyFromNameOrCode(self, allowedAuthorities, authName, ensemble,
                           datumAuthName, datumCode);
    if (datumAuthName == authName && datumCode == code) {
        return {};
    }

    std::vector<std::string> sqlStatements;

    const auto &members = ensemble->datums();
    assert(!members.empty());

    int counter = 1;
    std::vector<std::pair<std::string, std::string>> membersId;
    for (const auto &member : members) {
        std::string memberAuthName;
        std::string memberCode;
        identifyFromNameOrCode(self, allowedAuthorities, authName, member,
                               memberAuthName, memberCode);
        if (memberAuthName.empty()) {
            memberAuthName = authName;
            if (numericCode) {
                memberCode =
                    self->suggestsCodeFor(member, memberAuthName, true);
            } else {
                memberCode = "MEMBER_" + toString(counter) + "_OF_" + code;
            }
            const auto sqlStatementsTmp =
                self->getInsertStatementsFor(member, memberAuthName, memberCode,
                                             numericCode, allowedAuthorities);
            sqlStatements.insert(sqlStatements.end(), sqlStatementsTmp.begin(),
                                 sqlStatementsTmp.end());
        }

        membersId.emplace_back(
            std::pair<std::string, std::string>(memberAuthName, memberCode));

        ++counter;
    }

    const bool isGeodetic =
        util::nn_dynamic_pointer_cast<datum::GeodeticReferenceFrame>(
            members.front()) != nullptr;

    // Insert new record in geodetic_datum/vertical_datum table
    const double accuracy =
        c_locale_stod(ensemble->positionalAccuracy()->value());
    if (isGeodetic) {
        const auto firstDatum =
            AuthorityFactory::create(self, membersId.front().first)
                ->createGeodeticDatum(membersId.front().second);
        const auto &ellipsoid = firstDatum->ellipsoid();
        const auto &ellipsoidIds = ellipsoid->identifiers();
        assert(!ellipsoidIds.empty());
        const std::string &ellipsoidAuthName =
            *(ellipsoidIds.front()->codeSpace());
        const std::string &ellipsoidCode = ellipsoidIds.front()->code();
        const auto &pm = firstDatum->primeMeridian();
        const auto &pmIds = pm->identifiers();
        assert(!pmIds.empty());
        const std::string &pmAuthName = *(pmIds.front()->codeSpace());
        const std::string &pmCode = pmIds.front()->code();
        const std::string anchor(*(firstDatum->anchorDefinition()));
        const util::optional<common::Measure> &anchorEpoch =
            firstDatum->anchorEpoch();
        const auto sql = formatStatement(
            "INSERT INTO geodetic_datum VALUES("
            "'%q','%q','%q','%q','%q','%q','%q','%q',NULL,NULL,%f,%Q,%s,0);",
            authName.c_str(), code.c_str(), ensemble->nameStr().c_str(),
            "", // description
            ellipsoidAuthName.c_str(), ellipsoidCode.c_str(),
            pmAuthName.c_str(), pmCode.c_str(), accuracy,
            anchor.empty() ? nullptr : anchor.c_str(),
            anchorEpoch.has_value()
                ? anchorEpochToStr(
                      anchorEpoch->convertToUnit(common::UnitOfMeasure::YEAR))
                      .c_str()
                : "NULL");
        appendSql(sqlStatements, sql);
    } else {
        const auto firstDatum =
            AuthorityFactory::create(self, membersId.front().first)
                ->createVerticalDatum(membersId.front().second);
        const std::string anchor(*(firstDatum->anchorDefinition()));
        const util::optional<common::Measure> &anchorEpoch =
            firstDatum->anchorEpoch();
        const auto sql = formatStatement(
            "INSERT INTO vertical_datum VALUES("
            "'%q','%q','%q','%q',NULL,NULL,%f,%Q,%s,0);",
            authName.c_str(), code.c_str(), ensemble->nameStr().c_str(),
            "", // description
            accuracy, anchor.empty() ? nullptr : anchor.c_str(),
            anchorEpoch.has_value()
                ? anchorEpochToStr(
                      anchorEpoch->convertToUnit(common::UnitOfMeasure::YEAR))
                      .c_str()
                : "NULL");
        appendSql(sqlStatements, sql);
    }
    identifyOrInsertUsages(ensemble,
                           isGeodetic ? "geodetic_datum" : "vertical_datum",
                           authName, code, allowedAuthorities, sqlStatements);

    const char *tableName = isGeodetic ? "geodetic_datum_ensemble_member"
                                       : "vertical_datum_ensemble_member";
    counter = 1;
    for (const auto &authCodePair : membersId) {
        const auto sql = formatStatement(
            "INSERT INTO %s VALUES("
            "'%q','%q','%q','%q',%d);",
            tableName, authName.c_str(), code.c_str(),
            authCodePair.first.c_str(), authCodePair.second.c_str(), counter);
        appendSql(sqlStatements, sql);
        ++counter;
    }

    return sqlStatements;
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getInsertStatementsFor(
    const crs::GeodeticCRSNNPtr &crs, const std::string &authName,
    const std::string &code, bool numericCode,
    const std::vector<std::string> &allowedAuthorities) {

    const auto self = NN_NO_CHECK(self_.lock());

    std::vector<std::string> sqlStatements;

    // Find or insert datum/datum ensemble
    std::string datumAuthName;
    std::string datumCode;
    const auto &ensemble = crs->datumEnsemble();
    if (ensemble) {
        const auto ensembleNN = NN_NO_CHECK(ensemble);
        identifyFromNameOrCode(self, allowedAuthorities, authName, ensembleNN,
                               datumAuthName, datumCode);
        if (datumAuthName.empty()) {
            datumAuthName = authName;
            if (numericCode) {
                datumCode =
                    self->suggestsCodeFor(ensembleNN, datumAuthName, true);
            } else {
                datumCode = "GEODETIC_DATUM_" + code;
            }
            sqlStatements = self->getInsertStatementsFor(
                ensembleNN, datumAuthName, datumCode, numericCode,
                allowedAuthorities);
        }
    } else {
        const auto &datum = crs->datum();
        assert(datum);
        const auto datumNN = NN_NO_CHECK(datum);
        identifyFromNameOrCode(self, allowedAuthorities, authName, datumNN,
                               datumAuthName, datumCode);
        if (datumAuthName.empty()) {
            datumAuthName = authName;
            if (numericCode) {
                datumCode = self->suggestsCodeFor(datumNN, datumAuthName, true);
            } else {
                datumCode = "GEODETIC_DATUM_" + code;
            }
            sqlStatements =
                self->getInsertStatementsFor(datumNN, datumAuthName, datumCode,
                                             numericCode, allowedAuthorities);
        }
    }

    // Find or insert coordinate system
    const auto &coordinateSystem = crs->coordinateSystem();
    std::string csAuthName;
    std::string csCode;
    identifyOrInsert(self, coordinateSystem, "GEODETIC_CRS", authName, code,
                     csAuthName, csCode, sqlStatements);

    const char *type = GEOG_2D;
    if (coordinateSystem->axisList().size() == 3) {
        if (dynamic_cast<const crs::GeographicCRS *>(crs.get())) {
            type = GEOG_3D;
        } else {
            type = GEOCENTRIC;
        }
    }

    // Insert new record in geodetic_crs table
    const auto sql =
        formatStatement("INSERT INTO geodetic_crs VALUES("
                        "'%q','%q','%q','%q','%q','%q','%q','%q','%q',NULL,0);",
                        authName.c_str(), code.c_str(), crs->nameStr().c_str(),
                        "", // description
                        type, csAuthName.c_str(), csCode.c_str(),
                        datumAuthName.c_str(), datumCode.c_str());
    appendSql(sqlStatements, sql);

    identifyOrInsertUsages(crs, "geodetic_crs", authName, code,
                           allowedAuthorities, sqlStatements);
    return sqlStatements;
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getInsertStatementsFor(
    const crs::ProjectedCRSNNPtr &crs, const std::string &authName,
    const std::string &code, bool numericCode,
    const std::vector<std::string> &allowedAuthorities) {

    const auto self = NN_NO_CHECK(self_.lock());

    std::vector<std::string> sqlStatements;

    // Find or insert base geodetic CRS
    const auto &baseCRS = crs->baseCRS();
    std::string geodAuthName;
    std::string geodCode;

    auto allowedAuthoritiesTmp(allowedAuthorities);
    allowedAuthoritiesTmp.emplace_back(authName);
    for (const auto &allowedAuthority : allowedAuthoritiesTmp) {
        const auto factory = AuthorityFactory::create(self, allowedAuthority);
        const auto candidates = baseCRS->identify(factory);
        for (const auto &candidate : candidates) {
            if (candidate.second == 100) {
                const auto &ids = candidate.first->identifiers();
                if (!ids.empty()) {
                    const auto &id = ids.front();
                    geodAuthName = *(id->codeSpace());
                    geodCode = id->code();
                    break;
                }
            }
            if (!geodAuthName.empty()) {
                break;
            }
        }
    }
    if (geodAuthName.empty()) {
        geodAuthName = authName;
        geodCode = "GEODETIC_CRS_" + code;
        sqlStatements = self->getInsertStatementsFor(
            baseCRS, geodAuthName, geodCode, numericCode, allowedAuthorities);
    }

    // Insert new record in conversion table
    const auto &conversion = crs->derivingConversionRef();
    std::string convAuthName(authName);
    std::string convCode("CONVERSION_" + code);
    if (numericCode) {
        convCode = self->suggestsCodeFor(conversion, convAuthName, true);
    }
    {
        const auto &method = conversion->method();
        const auto &methodIds = method->identifiers();
        std::string methodAuthName;
        std::string methodCode;
        const operation::MethodMapping *methodMapping = nullptr;
        if (methodIds.empty()) {
            const int epsgCode = method->getEPSGCode();
            if (epsgCode > 0) {
                methodAuthName = metadata::Identifier::EPSG;
                methodCode = toString(epsgCode);
            } else {
                const auto &methodName = method->nameStr();
                size_t nProjectionMethodMappings = 0;
                const auto projectionMethodMappings =
                    operation::getProjectionMethodMappings(
                        nProjectionMethodMappings);
                for (size_t i = 0; i < nProjectionMethodMappings; ++i) {
                    const auto &mapping = projectionMethodMappings[i];
                    if (metadata::Identifier::isEquivalentName(
                            mapping.wkt2_name, methodName.c_str())) {
                        methodMapping = &mapping;
                    }
                }
                if (methodMapping == nullptr ||
                    methodMapping->proj_name_main == nullptr) {
                    throw FactoryException("Cannot insert projection with "
                                           "method without identifier");
                }
                methodAuthName = "PROJ";
                methodCode = methodMapping->proj_name_main;
                if (methodMapping->proj_name_aux) {
                    methodCode += ' ';
                    methodCode += methodMapping->proj_name_aux;
                }
            }
        } else {
            const auto &methodId = methodIds.front();
            methodAuthName = *(methodId->codeSpace());
            methodCode = methodId->code();
        }

        auto sql = formatStatement("INSERT INTO conversion VALUES("
                                   "'%q','%q','%q','','%q','%q','%q'",
                                   convAuthName.c_str(), convCode.c_str(),
                                   conversion->nameStr().c_str(),
                                   methodAuthName.c_str(), methodCode.c_str(),
                                   method->nameStr().c_str());
        const auto &srcValues = conversion->parameterValues();
        if (srcValues.size() > N_MAX_PARAMS) {
            throw FactoryException("Cannot insert projection with more than " +
                                   toString(static_cast<int>(N_MAX_PARAMS)) +
                                   " parameters");
        }

        std::vector<operation::GeneralParameterValueNNPtr> values;
        if (methodMapping == nullptr) {
            if (methodAuthName == metadata::Identifier::EPSG) {
                methodMapping = operation::getMapping(atoi(methodCode.c_str()));
            } else {
                methodMapping =
                    operation::getMapping(method->nameStr().c_str());
            }
        }
        if (methodMapping != nullptr) {
            // Re-order projection parameters in their reference order
            for (size_t j = 0; methodMapping->params[j] != nullptr; ++j) {
                for (size_t i = 0; i < srcValues.size(); ++i) {
                    auto opParamValue = dynamic_cast<
                        const operation::OperationParameterValue *>(
                        srcValues[i].get());
                    if (!opParamValue) {
                        throw FactoryException("Cannot insert projection with "
                                               "non-OperationParameterValue");
                    }
                    if (methodMapping->params[j]->wkt2_name &&
                        opParamValue->parameter()->nameStr() ==
                            methodMapping->params[j]->wkt2_name) {
                        values.emplace_back(srcValues[i]);
                    }
                }
            }
        }
        if (values.size() != srcValues.size()) {
            values = srcValues;
        }

        for (const auto &genOpParamvalue : values) {
            auto opParamValue =
                dynamic_cast<const operation::OperationParameterValue *>(
                    genOpParamvalue.get());
            if (!opParamValue) {
                throw FactoryException("Cannot insert projection with "
                                       "non-OperationParameterValue");
            }
            const auto &param = opParamValue->parameter();
            const auto &paramIds = param->identifiers();
            std::string paramAuthName;
            std::string paramCode;
            if (paramIds.empty()) {
                const int paramEPSGCode = param->getEPSGCode();
                if (paramEPSGCode == 0) {
                    throw FactoryException(
                        "Cannot insert projection with method parameter "
                        "without identifier");
                }
                paramAuthName = metadata::Identifier::EPSG;
                paramCode = toString(paramEPSGCode);
            } else {
                const auto &paramId = paramIds.front();
                paramAuthName = *(paramId->codeSpace());
                paramCode = paramId->code();
            }
            const auto &value = opParamValue->parameterValue()->value();
            const auto &unit = value.unit();
            std::string uomAuthName;
            std::string uomCode;
            identifyOrInsert(self, unit, authName, uomAuthName, uomCode,
                             sqlStatements);
            sql += formatStatement(",'%q','%q','%q',%f,'%q','%q'",
                                   paramAuthName.c_str(), paramCode.c_str(),
                                   param->nameStr().c_str(), value.value(),
                                   uomAuthName.c_str(), uomCode.c_str());
        }
        for (size_t i = values.size(); i < N_MAX_PARAMS; ++i) {
            sql += ",NULL,NULL,NULL,NULL,NULL,NULL";
        }
        sql += ",0);";
        appendSql(sqlStatements, sql);
        identifyOrInsertUsages(crs, "conversion", convAuthName, convCode,
                               allowedAuthorities, sqlStatements);
    }

    // Find or insert coordinate system
    const auto &coordinateSystem = crs->coordinateSystem();
    std::string csAuthName;
    std::string csCode;
    identifyOrInsert(self, coordinateSystem, "PROJECTED_CRS", authName, code,
                     csAuthName, csCode, sqlStatements);

    // Insert new record in projected_crs table
    const auto sql = formatStatement(
        "INSERT INTO projected_crs VALUES("
        "'%q','%q','%q','%q','%q','%q','%q','%q','%q','%q',NULL,0);",
        authName.c_str(), code.c_str(), crs->nameStr().c_str(),
        "", // description
        csAuthName.c_str(), csCode.c_str(), geodAuthName.c_str(),
        geodCode.c_str(), convAuthName.c_str(), convCode.c_str());
    appendSql(sqlStatements, sql);

    identifyOrInsertUsages(crs, "projected_crs", authName, code,
                           allowedAuthorities, sqlStatements);

    return sqlStatements;
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getInsertStatementsFor(
    const datum::VerticalReferenceFrameNNPtr &datum,
    const std::string &authName, const std::string &code,
    bool /* numericCode */,
    const std::vector<std::string> &allowedAuthorities) {

    const auto self = NN_NO_CHECK(self_.lock());

    std::vector<std::string> sqlStatements;

    // Check if the object is already known under that code
    std::string datumAuthName;
    std::string datumCode;
    identifyFromNameOrCode(self, allowedAuthorities, authName, datum,
                           datumAuthName, datumCode);
    if (datumAuthName == authName && datumCode == code) {
        return {};
    }

    // Insert new record in vertical_datum table
    std::string publicationDate("NULL");
    if (datum->publicationDate().has_value()) {
        publicationDate = '\'';
        publicationDate +=
            replaceAll(datum->publicationDate()->toString(), "'", "''");
        publicationDate += '\'';
    }
    std::string frameReferenceEpoch("NULL");
    const auto dynamicDatum =
        dynamic_cast<const datum::DynamicVerticalReferenceFrame *>(datum.get());
    if (dynamicDatum) {
        frameReferenceEpoch =
            toString(dynamicDatum->frameReferenceEpoch().value());
    }
    const std::string anchor(*(datum->anchorDefinition()));
    const util::optional<common::Measure> &anchorEpoch = datum->anchorEpoch();
    const auto sql = formatStatement(
        "INSERT INTO vertical_datum VALUES("
        "'%q','%q','%q','%q',%s,%s,NULL,%Q,%s,0);",
        authName.c_str(), code.c_str(), datum->nameStr().c_str(),
        "", // description
        publicationDate.c_str(), frameReferenceEpoch.c_str(),
        anchor.empty() ? nullptr : anchor.c_str(),
        anchorEpoch.has_value()
            ? anchorEpochToStr(
                  anchorEpoch->convertToUnit(common::UnitOfMeasure::YEAR))
                  .c_str()
            : "NULL");
    appendSql(sqlStatements, sql);

    identifyOrInsertUsages(datum, "vertical_datum", authName, code,
                           allowedAuthorities, sqlStatements);

    return sqlStatements;
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getInsertStatementsFor(
    const crs::VerticalCRSNNPtr &crs, const std::string &authName,
    const std::string &code, bool numericCode,
    const std::vector<std::string> &allowedAuthorities) {

    const auto self = NN_NO_CHECK(self_.lock());

    std::vector<std::string> sqlStatements;

    // Find or insert datum/datum ensemble
    std::string datumAuthName;
    std::string datumCode;
    const auto &ensemble = crs->datumEnsemble();
    if (ensemble) {
        const auto ensembleNN = NN_NO_CHECK(ensemble);
        identifyFromNameOrCode(self, allowedAuthorities, authName, ensembleNN,
                               datumAuthName, datumCode);
        if (datumAuthName.empty()) {
            datumAuthName = authName;
            if (numericCode) {
                datumCode =
                    self->suggestsCodeFor(ensembleNN, datumAuthName, true);
            } else {
                datumCode = "VERTICAL_DATUM_" + code;
            }
            sqlStatements = self->getInsertStatementsFor(
                ensembleNN, datumAuthName, datumCode, numericCode,
                allowedAuthorities);
        }
    } else {
        const auto &datum = crs->datum();
        assert(datum);
        const auto datumNN = NN_NO_CHECK(datum);
        identifyFromNameOrCode(self, allowedAuthorities, authName, datumNN,
                               datumAuthName, datumCode);
        if (datumAuthName.empty()) {
            datumAuthName = authName;
            if (numericCode) {
                datumCode = self->suggestsCodeFor(datumNN, datumAuthName, true);
            } else {
                datumCode = "VERTICAL_DATUM_" + code;
            }
            sqlStatements =
                self->getInsertStatementsFor(datumNN, datumAuthName, datumCode,
                                             numericCode, allowedAuthorities);
        }
    }

    // Find or insert coordinate system
    const auto &coordinateSystem = crs->coordinateSystem();
    std::string csAuthName;
    std::string csCode;
    identifyOrInsert(self, coordinateSystem, "VERTICAL_CRS", authName, code,
                     csAuthName, csCode, sqlStatements);

    // Insert new record in vertical_crs table
    const auto sql =
        formatStatement("INSERT INTO vertical_crs VALUES("
                        "'%q','%q','%q','%q','%q','%q','%q','%q',0);",
                        authName.c_str(), code.c_str(), crs->nameStr().c_str(),
                        "", // description
                        csAuthName.c_str(), csCode.c_str(),
                        datumAuthName.c_str(), datumCode.c_str());
    appendSql(sqlStatements, sql);

    identifyOrInsertUsages(crs, "vertical_crs", authName, code,
                           allowedAuthorities, sqlStatements);

    return sqlStatements;
}

// ---------------------------------------------------------------------------

std::vector<std::string> DatabaseContext::Private::getInsertStatementsFor(
    const crs::CompoundCRSNNPtr &crs, const std::string &authName,
    const std::string &code, bool numericCode,
    const std::vector<std::string> &allowedAuthorities) {

    const auto self = NN_NO_CHECK(self_.lock());

    std::vector<std::string> sqlStatements;

    int counter = 1;
    std::vector<std::pair<std::string, std::string>> componentsId;
    const auto &components = crs->componentReferenceSystems();
    if (components.size() != 2) {
        throw FactoryException(
            "Cannot insert compound CRS with number of components != 2");
    }

    auto allowedAuthoritiesTmp(allowedAuthorities);
    allowedAuthoritiesTmp.emplace_back(authName);

    for (const auto &component : components) {
        std::string compAuthName;
        std::string compCode;

        for (const auto &allowedAuthority : allowedAuthoritiesTmp) {
            const auto factory =
                AuthorityFactory::create(self, allowedAuthority);
            const auto candidates = component->identify(factory);
            for (const auto &candidate : candidates) {
                if (candidate.second == 100) {
                    const auto &ids = candidate.first->identifiers();
                    if (!ids.empty()) {
                        const auto &id = ids.front();
                        compAuthName = *(id->codeSpace());
                        compCode = id->code();
                        break;
                    }
                }
                if (!compAuthName.empty()) {
                    break;
                }
            }
        }

        if (compAuthName.empty()) {
            compAuthName = authName;
            if (numericCode) {
                compCode = self->suggestsCodeFor(component, compAuthName, true);
            } else {
                compCode = "COMPONENT_" + code + '_' + toString(counter);
            }
            const auto sqlStatementsTmp =
                self->getInsertStatementsFor(component, compAuthName, compCode,
                                             numericCode, allowedAuthorities);
            sqlStatements.insert(sqlStatements.end(), sqlStatementsTmp.begin(),
                                 sqlStatementsTmp.end());
        }

        componentsId.emplace_back(
            std::pair<std::string, std::string>(compAuthName, compCode));

        ++counter;
    }

    // Insert new record in compound_crs table
    const auto sql = formatStatement(
        "INSERT INTO compound_crs VALUES("
        "'%q','%q','%q','%q','%q','%q','%q','%q',0);",
        authName.c_str(), code.c_str(), crs->nameStr().c_str(),
        "", // description
        componentsId[0].first.c_str(), componentsId[0].second.c_str(),
        componentsId[1].first.c_str(), componentsId[1].second.c_str());
    appendSql(sqlStatements, sql);

    identifyOrInsertUsages(crs, "compound_crs", authName, code,
                           allowedAuthorities, sqlStatements);

    return sqlStatements;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DatabaseContext::~DatabaseContext() {
    try {
        stopInsertStatementsSession();
    } catch (const std::exception &) {
    }
}
//! @endcond

// ---------------------------------------------------------------------------

DatabaseContext::DatabaseContext() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

/** \brief Instantiate a database context.
 *
 * This database context should be used only by one thread at a time.
 *
 * @param databasePath Path and filename of the database. Might be empty
 * string for the default rules to locate the default proj.db
 * @param auxiliaryDatabasePaths Path and filename of auxiliary databases.
 * Might be empty.
 * Starting with PROJ 8.1, if this parameter is an empty array,
 * the PROJ_AUX_DB environment variable will be used, if set.
 * It must contain one or several paths. If several paths are
 * provided, they must be separated by the colon (:) character on Unix, and
 * on Windows, by the semi-colon (;) character.
 * @param ctx Context used for file search.
 * @throw FactoryException if the database cannot be opened.
 */
DatabaseContextNNPtr
DatabaseContext::create(const std::string &databasePath,
                        const std::vector<std::string> &auxiliaryDatabasePaths,
                        PJ_CONTEXT *ctx) {
    auto dbCtx = DatabaseContext::nn_make_shared<DatabaseContext>();
    auto dbCtxPrivate = dbCtx->getPrivate();
    dbCtxPrivate->open(databasePath, ctx);
    auto auxDbs(auxiliaryDatabasePaths);
    if (auxDbs.empty()) {
        const char *auxDbStr = getenv("PROJ_AUX_DB");
        if (auxDbStr) {
#ifdef _WIN32
            const char *delim = ";";
#else
            const char *delim = ":";
#endif
            auxDbs = split(auxDbStr, delim);
        }
    }
    if (!auxDbs.empty()) {
        dbCtxPrivate->attachExtraDatabases(auxDbs);
        dbCtxPrivate->auxiliaryDatabasePaths_ = std::move(auxDbs);
    }
    dbCtxPrivate->self_ = dbCtx.as_nullable();
    return dbCtx;
}

// ---------------------------------------------------------------------------

/** \brief Return the list of authorities used in the database.
 */
std::set<std::string> DatabaseContext::getAuthorities() const {
    auto res = d->run("SELECT auth_name FROM authority_list");
    std::set<std::string> list;
    for (const auto &row : res) {
        list.insert(row[0]);
    }
    return list;
}

// ---------------------------------------------------------------------------

/** \brief Return the list of SQL commands (CREATE TABLE, CREATE TRIGGER,
 * CREATE VIEW) needed to initialize a new database.
 */
std::vector<std::string> DatabaseContext::getDatabaseStructure() const {
    return d->getDatabaseStructure();
}

// ---------------------------------------------------------------------------

/** \brief Return the path to the database.
 */
const std::string &DatabaseContext::getPath() const { return d->getPath(); }

// ---------------------------------------------------------------------------

/** \brief Return a metadata item.
 *
 * Value remains valid while this is alive and to the next call to getMetadata
 */
const char *DatabaseContext::getMetadata(const char *key) const {
    auto res =
        d->run("SELECT value FROM metadata WHERE key = ?", {std::string(key)});
    if (res.empty()) {
        return nullptr;
    }
    d->lastMetadataValue_ = res.front()[0];
    return d->lastMetadataValue_.c_str();
}

// ---------------------------------------------------------------------------

/** \brief Starts a session for getInsertStatementsFor()
 *
 * Starts a new session for one or several calls to getInsertStatementsFor().
 * An insertion session guarantees that the inserted objects will not create
 * conflicting intermediate objects.
 *
 * The session must be stopped with stopInsertStatementsSession().
 *
 * Only one session may be active at a time for a given database context.
 *
 * @throw FactoryException in case of error.
 * @since 8.1
 */
void DatabaseContext::startInsertStatementsSession() {
    if (d->memoryDbHandle_) {
        throw FactoryException(
            "startInsertStatementsSession() cannot be invoked until "
            "stopInsertStatementsSession() is.");
    }

    d->memoryDbForInsertPath_.clear();
    const auto sqlStatements = getDatabaseStructure();

    // Create a in-memory temporary sqlite3 database
    std::ostringstream buffer;
    buffer << "file:temp_db_for_insert_statements_";
    buffer << this;
    buffer << ".db?mode=memory&cache=shared";
    d->memoryDbForInsertPath_ = buffer.str();
    sqlite3 *memoryDbHandle = nullptr;
    sqlite3_open_v2(
        d->memoryDbForInsertPath_.c_str(), &memoryDbHandle,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, nullptr);
    if (memoryDbHandle == nullptr) {
        throw FactoryException("Cannot create in-memory database");
    }
    d->memoryDbHandle_ =
        SQLiteHandle::initFromExistingUniquePtr(memoryDbHandle, true);

    // Fill the structure of this database
    for (const auto &sql : sqlStatements) {
        char *errmsg = nullptr;
        if (sqlite3_exec(d->memoryDbHandle_->handle(), sql.c_str(), nullptr,
                         nullptr, &errmsg) != SQLITE_OK) {
            const auto sErrMsg =
                "Cannot execute " + sql + ": " + (errmsg ? errmsg : "");
            sqlite3_free(errmsg);
            throw FactoryException(sErrMsg);
        }
        sqlite3_free(errmsg);
    }

    // Attach this database to the current one(s)
    auto auxiliaryDatabasePaths(d->auxiliaryDatabasePaths_);
    auxiliaryDatabasePaths.push_back(d->memoryDbForInsertPath_);
    d->attachExtraDatabases(auxiliaryDatabasePaths);
}

// ---------------------------------------------------------------------------

/** \brief Suggests a database code for the passed object.
 *
 * Supported type of objects are PrimeMeridian, Ellipsoid, Datum, DatumEnsemble,
 * GeodeticCRS, ProjectedCRS, VerticalCRS, CompoundCRS, BoundCRS, Conversion.
 *
 * @param object Object for which to suggest a code.
 * @param authName Authority name into which the object will be inserted.
 * @param numericCode Whether the code should be numeric, or derived from the
 * object name.
 * @return the suggested code, that is guaranteed to not conflict with an
 * existing one.
 *
 * @throw FactoryException in case of error.
 * @since 8.1
 */
std::string
DatabaseContext::suggestsCodeFor(const common::IdentifiedObjectNNPtr &object,
                                 const std::string &authName,
                                 bool numericCode) {
    const char *tableName = "prime_meridian";
    if (dynamic_cast<const datum::PrimeMeridian *>(object.get())) {
        // tableName = "prime_meridian";
    } else if (dynamic_cast<const datum::Ellipsoid *>(object.get())) {
        tableName = "ellipsoid";
    } else if (dynamic_cast<const datum::GeodeticReferenceFrame *>(
                   object.get())) {
        tableName = "geodetic_datum";
    } else if (dynamic_cast<const datum::VerticalReferenceFrame *>(
                   object.get())) {
        tableName = "vertical_datum";
    } else if (const auto ensemble =
                   dynamic_cast<const datum::DatumEnsemble *>(object.get())) {
        const auto &datums = ensemble->datums();
        if (!datums.empty() &&
            dynamic_cast<const datum::GeodeticReferenceFrame *>(
                datums[0].get())) {
            tableName = "geodetic_datum";
        } else {
            tableName = "vertical_datum";
        }
    } else if (const auto boundCRS =
                   dynamic_cast<const crs::BoundCRS *>(object.get())) {
        return suggestsCodeFor(boundCRS->baseCRS(), authName, numericCode);
    } else if (dynamic_cast<const crs::CRS *>(object.get())) {
        tableName = "crs_view";
    } else if (dynamic_cast<const operation::Conversion *>(object.get())) {
        tableName = "conversion";
    } else {
        throw FactoryException("suggestsCodeFor(): unhandled type of object");
    }

    if (numericCode) {
        std::string sql("SELECT MAX(code) FROM ");
        sql += tableName;
        sql += " WHERE auth_name = ? AND code >= '1' AND code <= '999999999' "
               "AND upper(code) = lower(code)";
        const auto res = d->run(sql, {authName});
        if (res.empty()) {
            return "1";
        }
        return toString(atoi(res.front()[0].c_str()) + 1);
    }

    std::string code;
    code.reserve(object->nameStr().size());
    bool insertUnderscore = false;
    for (const auto ch : toupper(object->nameStr())) {
        if ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z')) {
            if (insertUnderscore && code.back() != '_')
                code += '_';
            code += ch;
            insertUnderscore = false;
        } else {
            insertUnderscore = true;
        }
    }
    return d->findFreeCode(tableName, authName, code);
}

// ---------------------------------------------------------------------------

/** \brief Returns SQL statements needed to insert the passed object into the
 * database.
 *
 * startInsertStatementsSession() must have been called previously.
 *
 * @param object The object to insert into the database. Currently only
 *               PrimeMeridian, Ellipsoid, Datum, GeodeticCRS, ProjectedCRS,
 *               VerticalCRS, CompoundCRS or BoundCRS are supported.
 * @param authName Authority name into which the object will be inserted.
 * @param code Code with which the object will be inserted.
 * @param numericCode Whether intermediate objects that can be created should
 *                    use numeric codes (true), or may be alphanumeric (false)
 * @param allowedAuthorities Authorities to which intermediate objects are
 *                           allowed to refer to. authName will be implicitly
 *                           added to it. Note that unit, coordinate
 *                           systems, projection methods and parameters will in
 *                           any case be allowed to refer to EPSG.
 * @throw FactoryException in case of error.
 * @since 8.1
 */
std::vector<std::string> DatabaseContext::getInsertStatementsFor(
    const common::IdentifiedObjectNNPtr &object, const std::string &authName,
    const std::string &code, bool numericCode,
    const std::vector<std::string> &allowedAuthorities) {
    if (d->memoryDbHandle_ == nullptr) {
        throw FactoryException(
            "startInsertStatementsSession() should be invoked first");
    }

    const auto crs = util::nn_dynamic_pointer_cast<crs::CRS>(object);
    if (crs) {
        // Check if the object is already known under that code
        const auto self = NN_NO_CHECK(d->self_.lock());
        auto allowedAuthoritiesTmp(allowedAuthorities);
        allowedAuthoritiesTmp.emplace_back(authName);
        for (const auto &allowedAuthority : allowedAuthoritiesTmp) {
            const auto factory =
                AuthorityFactory::create(self, allowedAuthority);
            const auto candidates = crs->identify(factory);
            for (const auto &candidate : candidates) {
                if (candidate.second == 100) {
                    const auto &ids = candidate.first->identifiers();
                    for (const auto &id : ids) {
                        if (*(id->codeSpace()) == authName &&
                            id->code() == code) {
                            return {};
                        }
                    }
                }
            }
        }
    }

    if (const auto pm =
            util::nn_dynamic_pointer_cast<datum::PrimeMeridian>(object)) {
        return d->getInsertStatementsFor(NN_NO_CHECK(pm), authName, code,
                                         numericCode, allowedAuthorities);
    }

    else if (const auto ellipsoid =
                 util::nn_dynamic_pointer_cast<datum::Ellipsoid>(object)) {
        return d->getInsertStatementsFor(NN_NO_CHECK(ellipsoid), authName, code,
                                         numericCode, allowedAuthorities);
    }

    else if (const auto geodeticDatum =
                 util::nn_dynamic_pointer_cast<datum::GeodeticReferenceFrame>(
                     object)) {
        return d->getInsertStatementsFor(NN_NO_CHECK(geodeticDatum), authName,
                                         code, numericCode, allowedAuthorities);
    }

    else if (const auto ensemble =
                 util::nn_dynamic_pointer_cast<datum::DatumEnsemble>(object)) {
        return d->getInsertStatementsFor(NN_NO_CHECK(ensemble), authName, code,
                                         numericCode, allowedAuthorities);
    }

    else if (const auto geodCRS =
                 std::dynamic_pointer_cast<crs::GeodeticCRS>(crs)) {
        return d->getInsertStatementsFor(NN_NO_CHECK(geodCRS), authName, code,
                                         numericCode, allowedAuthorities);
    }

    else if (const auto projCRS =
                 std::dynamic_pointer_cast<crs::ProjectedCRS>(crs)) {
        return d->getInsertStatementsFor(NN_NO_CHECK(projCRS), authName, code,
                                         numericCode, allowedAuthorities);
    }

    else if (const auto verticalDatum =
                 util::nn_dynamic_pointer_cast<datum::VerticalReferenceFrame>(
                     object)) {
        return d->getInsertStatementsFor(NN_NO_CHECK(verticalDatum), authName,
                                         code, numericCode, allowedAuthorities);
    }

    else if (const auto vertCRS =
                 std::dynamic_pointer_cast<crs::VerticalCRS>(crs)) {
        return d->getInsertStatementsFor(NN_NO_CHECK(vertCRS), authName, code,
                                         numericCode, allowedAuthorities);
    }

    else if (const auto compoundCRS =
                 std::dynamic_pointer_cast<crs::CompoundCRS>(crs)) {
        return d->getInsertStatementsFor(NN_NO_CHECK(compoundCRS), authName,
                                         code, numericCode, allowedAuthorities);
    }

    else if (const auto boundCRS =
                 std::dynamic_pointer_cast<crs::BoundCRS>(crs)) {
        return getInsertStatementsFor(boundCRS->baseCRS(), authName, code,
                                      numericCode, allowedAuthorities);
    }

    else {
        throw FactoryException(
            "getInsertStatementsFor(): unhandled type of object");
    }
}

// ---------------------------------------------------------------------------

/** \brief Stops an insertion session started with
 * startInsertStatementsSession()
 *
 * @since 8.1
 */
void DatabaseContext::stopInsertStatementsSession() {
    if (d->memoryDbHandle_) {
        d->clearCaches();
        d->attachExtraDatabases(d->auxiliaryDatabasePaths_);
        d->memoryDbHandle_.reset();
        d->memoryDbForInsertPath_.clear();
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

DatabaseContextNNPtr DatabaseContext::create(void *sqlite_handle) {
    auto ctxt = DatabaseContext::nn_make_shared<DatabaseContext>();
    ctxt->getPrivate()->setHandle(static_cast<sqlite3 *>(sqlite_handle));
    return ctxt;
}

// ---------------------------------------------------------------------------

void *DatabaseContext::getSqliteHandle() const { return d->handle()->handle(); }

// ---------------------------------------------------------------------------

bool DatabaseContext::lookForGridAlternative(const std::string &officialName,
                                             std::string &projFilename,
                                             std::string &projFormat,
                                             bool &inverse) const {
    auto res = d->run(
        "SELECT proj_grid_name, proj_grid_format, inverse_direction FROM "
        "grid_alternatives WHERE original_grid_name = ? AND "
        "proj_grid_name <> ''",
        {officialName});
    if (res.empty()) {
        return false;
    }
    const auto &row = res.front();
    projFilename = row[0];
    projFormat = row[1];
    inverse = row[2] == "1";
    return true;
}

// ---------------------------------------------------------------------------

static std::string makeCachedGridKey(const std::string &projFilename,
                                     bool networkEnabled,
                                     bool considerKnownGridsAsAvailable) {
    std::string key(projFilename);
    key += networkEnabled ? "true" : "false";
    key += considerKnownGridsAsAvailable ? "true" : "false";
    return key;
}

// ---------------------------------------------------------------------------

/** Invalidates information related to projFilename that might have been
 * previously cached by lookForGridInfo().
 *
 * This is useful when downloading a new grid during a session.
 */
void DatabaseContext::invalidateGridInfo(const std::string &projFilename) {
    d->evictGridInfoFromCache(makeCachedGridKey(projFilename, false, false));
    d->evictGridInfoFromCache(makeCachedGridKey(projFilename, false, true));
    d->evictGridInfoFromCache(makeCachedGridKey(projFilename, true, false));
    d->evictGridInfoFromCache(makeCachedGridKey(projFilename, true, true));
}

// ---------------------------------------------------------------------------

bool DatabaseContext::lookForGridInfo(
    const std::string &projFilename, bool considerKnownGridsAsAvailable,
    std::string &fullFilename, std::string &packageName, std::string &url,
    bool &directDownload, bool &openLicense, bool &gridAvailable) const {
    Private::GridInfoCache info;

    if (projFilename == "null") {
        // Special case for implicit "null" grid.
        fullFilename.clear();
        packageName.clear();
        url.clear();
        directDownload = false;
        openLicense = true;
        gridAvailable = true;
        return true;
    }

    auto ctxt = d->pjCtxt();
    if (ctxt == nullptr) {
        ctxt = pj_get_default_ctx();
        d->setPjCtxt(ctxt);
    }

    const std::string key(makeCachedGridKey(
        projFilename, proj_context_is_network_enabled(ctxt) != false,
        considerKnownGridsAsAvailable));
    if (d->getGridInfoFromCache(key, info)) {
        fullFilename = info.fullFilename;
        packageName = info.packageName;
        url = info.url;
        directDownload = info.directDownload;
        openLicense = info.openLicense;
        gridAvailable = info.gridAvailable;
        return info.found;
    }

    fullFilename.clear();
    packageName.clear();
    url.clear();
    openLicense = false;
    directDownload = false;
    gridAvailable = false;

    const auto resolveFullFilename = [ctxt, &fullFilename, &projFilename]() {
        fullFilename.resize(2048);
        const int errno_before = proj_context_errno(ctxt);
        bool lGridAvailable = NS_PROJ::FileManager::open_resource_file(
                                  ctxt, projFilename.c_str(), &fullFilename[0],
                                  fullFilename.size() - 1) != nullptr;
        proj_context_errno_set(ctxt, errno_before);
        fullFilename.resize(strlen(fullFilename.c_str()));
        return lGridAvailable;
    };

    auto res =
        d->run("SELECT "
               "grid_packages.package_name, "
               "grid_alternatives.url, "
               "grid_packages.url AS package_url, "
               "grid_alternatives.open_license, "
               "grid_packages.open_license AS package_open_license, "
               "grid_alternatives.direct_download, "
               "grid_packages.direct_download AS package_direct_download, "
               "grid_alternatives.proj_grid_name, "
               "grid_alternatives.old_proj_grid_name "
               "FROM grid_alternatives "
               "LEFT JOIN grid_packages ON "
               "grid_alternatives.package_name = grid_packages.package_name "
               "WHERE proj_grid_name = ? OR old_proj_grid_name = ?",
               {projFilename, projFilename});
    bool ret = !res.empty();
    if (ret) {
        const auto &row = res.front();
        packageName = std::move(row[0]);
        url = row[1].empty() ? std::move(row[2]) : std::move(row[1]);
        openLicense = (row[3].empty() ? row[4] : row[3]) == "1";
        directDownload = (row[5].empty() ? row[6] : row[5]) == "1";

        const auto &proj_grid_name = row[7];
        const auto &old_proj_grid_name = row[8];
        if (proj_grid_name != old_proj_grid_name &&
            old_proj_grid_name == projFilename) {
            std::string fullFilenameNewName;
            fullFilenameNewName.resize(2048);
            const int errno_before = proj_context_errno(ctxt);
            bool gridAvailableWithNewName =
                pj_find_file(ctxt, proj_grid_name.c_str(),
                             &fullFilenameNewName[0],
                             fullFilenameNewName.size() - 1) != 0;
            proj_context_errno_set(ctxt, errno_before);
            fullFilenameNewName.resize(strlen(fullFilenameNewName.c_str()));
            if (gridAvailableWithNewName) {
                gridAvailable = true;
                fullFilename = std::move(fullFilenameNewName);
            }
        }

        if (!gridAvailable && considerKnownGridsAsAvailable &&
            (!packageName.empty() || (!url.empty() && openLicense))) {

            // In considerKnownGridsAsAvailable mode, try to fetch the local
            // file name if it exists, but do not attempt network access.
            const auto network_was_enabled =
                proj_context_is_network_enabled(ctxt);
            proj_context_set_enable_network(ctxt, false);
            (void)resolveFullFilename();
            proj_context_set_enable_network(ctxt, network_was_enabled);

            gridAvailable = true;
        }

        info.packageName = packageName;
        std::string endpoint(proj_context_get_url_endpoint(d->pjCtxt()));
        if (!endpoint.empty() && starts_with(url, "https://cdn.proj.org/")) {
            if (endpoint.back() != '/') {
                endpoint += '/';
            }
            url = endpoint + url.substr(strlen("https://cdn.proj.org/"));
        }
        info.directDownload = directDownload;
        info.openLicense = openLicense;

        if (!gridAvailable) {
            gridAvailable = resolveFullFilename();
        }
    } else {
        gridAvailable = resolveFullFilename();

        if (starts_with(fullFilename, "http://") ||
            starts_with(fullFilename, "https://")) {
            url = fullFilename;
            fullFilename.clear();
        }
    }

    info.fullFilename = fullFilename;
    info.url = url;
    info.gridAvailable = gridAvailable;
    info.found = ret;
    d->cache(key, info);
    return ret;
}

// ---------------------------------------------------------------------------

/** Returns the number of queries to the database since the creation of this
 * instance.
 */
unsigned int DatabaseContext::getQueryCounter() const {
    return d->queryCounter_;
}

// ---------------------------------------------------------------------------

bool DatabaseContext::isKnownName(const std::string &name,
                                  const std::string &tableName) const {
    std::string sql("SELECT 1 FROM \"");
    sql += replaceAll(tableName, "\"", "\"\"");
    sql += "\" WHERE name = ? LIMIT 1";
    return !d->run(sql, {name}).empty();
}
// ---------------------------------------------------------------------------

std::string
DatabaseContext::getProjGridName(const std::string &oldProjGridName) {
    auto res = d->run("SELECT proj_grid_name FROM grid_alternatives WHERE "
                      "old_proj_grid_name = ?",
                      {oldProjGridName});
    if (res.empty()) {
        return std::string();
    }
    return res.front()[0];
}

// ---------------------------------------------------------------------------

std::string DatabaseContext::getOldProjGridName(const std::string &gridName) {
    auto res = d->run("SELECT old_proj_grid_name FROM grid_alternatives WHERE "
                      "proj_grid_name = ?",
                      {gridName});
    if (res.empty()) {
        return std::string();
    }
    return res.front()[0];
}

// ---------------------------------------------------------------------------

// scripts/build_db_from_esri.py adds a second alias for
// names that have '[' in them. See get_old_esri_name()
// in scripts/build_db_from_esri.py
// So if we only have two aliases detect that situation to get the official
// new name
static std::string getUniqueEsriAlias(const std::list<std::string> &l) {
    std::string first = l.front();
    std::string second = *(std::next(l.begin()));
    if (second.find('[') != std::string::npos)
        std::swap(first, second);
    if (replaceAll(replaceAll(replaceAll(first, "[", ""), "]", ""), "-", "_") ==
        second) {
        return first;
    }
    return std::string();
}

// ---------------------------------------------------------------------------

/** \brief Gets the alias name from an official name.
 *
 * @param officialName Official name. Mandatory
 * @param tableName Table name/category. Mandatory.
 *                  "geographic_2D_crs" and "geographic_3D_crs" are also
 *                  accepted as special names to add a constraint on the "type"
 *                  column of the "geodetic_crs" table.
 * @param source Source of the alias. Mandatory
 * @return Alias name (or empty if not found).
 * @throw FactoryException in case of error.
 */
std::string
DatabaseContext::getAliasFromOfficialName(const std::string &officialName,
                                          const std::string &tableName,
                                          const std::string &source) const {
    std::string sql("SELECT auth_name, code FROM \"");
    const auto genuineTableName =
        tableName == "geographic_2D_crs" || tableName == "geographic_3D_crs"
            ? "geodetic_crs"
            : tableName;
    sql += replaceAll(genuineTableName, "\"", "\"\"");
    sql += "\" WHERE name = ?";
    if (tableName == "geodetic_crs" || tableName == "geographic_2D_crs") {
        sql += " AND type = " GEOG_2D_SINGLE_QUOTED;
    } else if (tableName == "geographic_3D_crs") {
        sql += " AND type = " GEOG_3D_SINGLE_QUOTED;
    }
    sql += " ORDER BY deprecated";
    auto res = d->run(sql, {officialName});
    // Sorry for the hack excluding NAD83 + geographic_3D_crs, but otherwise
    // EPSG has a weird alias from NAD83 to EPSG:4152 which happens to be
    // NAD83(HARN), and that's definitely not desirable.
    if (res.empty() &&
        !(officialName == "NAD83" && tableName == "geographic_3D_crs")) {
        res = d->run(
            "SELECT auth_name, code FROM alias_name WHERE table_name = ? AND "
            "alt_name = ? AND source IN ('EPSG', 'PROJ')",
            {genuineTableName, officialName});
        if (res.size() != 1) {
            return std::string();
        }
    }
    for (const auto &row : res) {
        auto res2 =
            d->run("SELECT alt_name FROM alias_name WHERE table_name = ? AND "
                   "auth_name = ? AND code = ? AND source = ?",
                   {genuineTableName, row[0], row[1], source});
        if (!res2.empty()) {
            if (res2.size() == 2 && source == "ESRI") {
                std::list<std::string> l;
                l.emplace_back(res2.front()[0]);
                l.emplace_back((*(std::next(res2.begin())))[0]);
                std::string uniqueEsriAlias = getUniqueEsriAlias(l);
                if (!uniqueEsriAlias.empty())
                    return uniqueEsriAlias;
            }
            return res2.front()[0];
        }
    }
    return std::string();
}

// ---------------------------------------------------------------------------

/** \brief Gets the alias names for an object.
 *
 * Either authName + code or officialName must be non empty.
 *
 * @param authName Authority.
 * @param code Code.
 * @param officialName Official name.
 * @param tableName Table name/category. Mandatory.
 *                  "geographic_2D_crs" and "geographic_3D_crs" are also
 *                  accepted as special names to add a constraint on the "type"
 *                  column of the "geodetic_crs" table.
 * @param source Source of the alias. May be empty.
 * @return Aliases
 */
std::list<std::string> DatabaseContext::getAliases(
    const std::string &authName, const std::string &code,
    const std::string &officialName, const std::string &tableName,
    const std::string &source) const {

    std::list<std::string> res;
    const auto key(authName + code + officialName + tableName + source);
    if (d->cacheAliasNames_.tryGet(key, res)) {
        return res;
    }

    std::string resolvedAuthName(authName);
    std::string resolvedCode(code);
    const auto genuineTableName =
        tableName == "geographic_2D_crs" || tableName == "geographic_3D_crs"
            ? "geodetic_crs"
            : tableName;
    if (authName.empty() || code.empty()) {
        std::string sql("SELECT auth_name, code FROM \"");
        sql += replaceAll(genuineTableName, "\"", "\"\"");
        sql += "\" WHERE name = ?";
        if (tableName == "geodetic_crs" || tableName == "geographic_2D_crs") {
            sql += " AND type = " GEOG_2D_SINGLE_QUOTED;
        } else if (tableName == "geographic_3D_crs") {
            sql += " AND type = " GEOG_3D_SINGLE_QUOTED;
        }
        sql += " ORDER BY deprecated";
        auto resSql = d->run(sql, {officialName});
        if (resSql.empty()) {
            resSql = d->run("SELECT auth_name, code FROM alias_name WHERE "
                            "table_name = ? AND "
                            "alt_name = ? AND source IN ('EPSG', 'PROJ')",
                            {genuineTableName, officialName});
            if (resSql.size() != 1) {
                d->cacheAliasNames_.insert(key, res);
                return res;
            }
        }
        const auto &row = resSql.front();
        resolvedAuthName = row[0];
        resolvedCode = row[1];
    }
    std::string sql("SELECT alt_name FROM alias_name WHERE table_name = ? AND "
                    "auth_name = ? AND code = ?");
    ListOfParams params{genuineTableName, resolvedAuthName, resolvedCode};
    if (!source.empty()) {
        sql += " AND source = ?";
        params.emplace_back(source);
    }
    auto resSql = d->run(sql, params);
    for (const auto &row : resSql) {
        res.emplace_back(row[0]);
    }

    if (res.size() == 2 && source == "ESRI") {
        const auto uniqueEsriAlias = getUniqueEsriAlias(res);
        if (!uniqueEsriAlias.empty()) {
            res.clear();
            res.emplace_back(uniqueEsriAlias);
        }
    }

    d->cacheAliasNames_.insert(key, res);
    return res;
}

// ---------------------------------------------------------------------------

/** \brief Return the 'name' column of a table for an object
 *
 * @param tableName Table name/category.
 * @param authName Authority name of the object.
 * @param code Code of the object
 * @return Name (or empty)
 * @throw FactoryException in case of error.
 */
std::string DatabaseContext::getName(const std::string &tableName,
                                     const std::string &authName,
                                     const std::string &code) const {
    std::string res;
    const auto key(tableName + authName + code);
    if (d->cacheNames_.tryGet(key, res)) {
        return res;
    }

    std::string sql("SELECT name FROM \"");
    sql += replaceAll(tableName, "\"", "\"\"");
    sql += "\" WHERE auth_name = ? AND code = ?";
    auto sqlRes = d->run(sql, {authName, code});
    if (sqlRes.empty()) {
        res.clear();
    } else {
        res = sqlRes.front()[0];
    }
    d->cacheNames_.insert(key, res);
    return res;
}

// ---------------------------------------------------------------------------

/** \brief Return the 'text_definition' column of a table for an object
 *
 * @param tableName Table name/category.
 * @param authName Authority name of the object.
 * @param code Code of the object
 * @return Text definition (or empty)
 * @throw FactoryException in case of error.
 */
std::string DatabaseContext::getTextDefinition(const std::string &tableName,
                                               const std::string &authName,
                                               const std::string &code) const {
    std::string sql("SELECT text_definition FROM \"");
    sql += replaceAll(tableName, "\"", "\"\"");
    sql += "\" WHERE auth_name = ? AND code = ?";
    auto res = d->run(sql, {authName, code});
    if (res.empty()) {
        return std::string();
    }
    return res.front()[0];
}

// ---------------------------------------------------------------------------

/** \brief Return the allowed authorities when researching transformations
 * between different authorities.
 *
 * @throw FactoryException in case of error.
 */
std::vector<std::string> DatabaseContext::getAllowedAuthorities(
    const std::string &sourceAuthName,
    const std::string &targetAuthName) const {

    const auto key(sourceAuthName + targetAuthName);
    auto hit = d->cacheAllowedAuthorities_.find(key);
    if (hit != d->cacheAllowedAuthorities_.end()) {
        return hit->second;
    }

    auto sqlRes = d->run(
        "SELECT allowed_authorities FROM authority_to_authority_preference "
        "WHERE source_auth_name = ? AND target_auth_name = ?",
        {sourceAuthName, targetAuthName});
    if (sqlRes.empty()) {
        sqlRes = d->run(
            "SELECT allowed_authorities FROM authority_to_authority_preference "
            "WHERE source_auth_name = ? AND target_auth_name = 'any'",
            {sourceAuthName});
    }
    if (sqlRes.empty()) {
        sqlRes = d->run(
            "SELECT allowed_authorities FROM authority_to_authority_preference "
            "WHERE source_auth_name = 'any' AND target_auth_name = ?",
            {targetAuthName});
    }
    if (sqlRes.empty()) {
        sqlRes = d->run(
            "SELECT allowed_authorities FROM authority_to_authority_preference "
            "WHERE source_auth_name = 'any' AND target_auth_name = 'any'",
            {});
    }
    if (sqlRes.empty()) {
        d->cacheAllowedAuthorities_[key] = std::vector<std::string>();
        return std::vector<std::string>();
    }
    auto res = split(sqlRes.front()[0], ',');
    d->cacheAllowedAuthorities_[key] = res;
    return res;
}

// ---------------------------------------------------------------------------

std::list<std::pair<std::string, std::string>>
DatabaseContext::getNonDeprecated(const std::string &tableName,
                                  const std::string &authName,
                                  const std::string &code) const {
    auto sqlRes =
        d->run("SELECT replacement_auth_name, replacement_code, source "
               "FROM deprecation "
               "WHERE table_name = ? AND deprecated_auth_name = ? "
               "AND deprecated_code = ?",
               {tableName, authName, code});
    std::list<std::pair<std::string, std::string>> res;
    for (const auto &row : sqlRes) {
        const auto &source = row[2];
        if (source == "PROJ") {
            const auto &replacement_auth_name = row[0];
            const auto &replacement_code = row[1];
            res.emplace_back(replacement_auth_name, replacement_code);
        }
    }
    if (!res.empty()) {
        return res;
    }
    for (const auto &row : sqlRes) {
        const auto &replacement_auth_name = row[0];
        const auto &replacement_code = row[1];
        res.emplace_back(replacement_auth_name, replacement_code);
    }
    return res;
}

// ---------------------------------------------------------------------------

const std::vector<DatabaseContext::Private::VersionedAuthName> &
DatabaseContext::Private::getCacheAuthNameWithVersion() {
    if (cacheAuthNameWithVersion_.empty()) {
        const auto sqlRes =
            run("SELECT versioned_auth_name, auth_name, version, priority "
                "FROM versioned_auth_name_mapping");
        for (const auto &row : sqlRes) {
            VersionedAuthName van;
            van.versionedAuthName = row[0];
            van.authName = row[1];
            van.version = row[2];
            van.priority = atoi(row[3].c_str());
            cacheAuthNameWithVersion_.emplace_back(std::move(van));
        }
    }
    return cacheAuthNameWithVersion_;
}

// ---------------------------------------------------------------------------

// From IAU_2015 returns (IAU,2015)
bool DatabaseContext::getAuthorityAndVersion(
    const std::string &versionedAuthName, std::string &authNameOut,
    std::string &versionOut) {

    for (const auto &van : d->getCacheAuthNameWithVersion()) {
        if (van.versionedAuthName == versionedAuthName) {
            authNameOut = van.authName;
            versionOut = van.version;
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------

// From IAU and 2015, returns IAU_2015
bool DatabaseContext::getVersionedAuthority(const std::string &authName,
                                            const std::string &version,
                                            std::string &versionedAuthNameOut) {

    for (const auto &van : d->getCacheAuthNameWithVersion()) {
        if (van.authName == authName && van.version == version) {
            versionedAuthNameOut = van.versionedAuthName;
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------

// From IAU returns IAU_latest, ... IAU_2015
std::vector<std::string>
DatabaseContext::getVersionedAuthoritiesFromName(const std::string &authName) {

    typedef std::pair<std::string, int> VersionedAuthNamePriority;
    std::vector<VersionedAuthNamePriority> tmp;
    for (const auto &van : d->getCacheAuthNameWithVersion()) {
        if (van.authName == authName) {
            tmp.emplace_back(
                VersionedAuthNamePriority(van.versionedAuthName, van.priority));
        }
    }
    std::vector<std::string> res;
    if (!tmp.empty()) {
        // Sort by decreasing priority
        std::sort(tmp.begin(), tmp.end(),
                  [](const VersionedAuthNamePriority &a,
                     const VersionedAuthNamePriority &b) {
                      return b.second > a.second;
                  });
        for (const auto &pair : tmp)
            res.emplace_back(pair.first);
    }
    return res;
}

// ---------------------------------------------------------------------------

std::vector<operation::CoordinateOperationNNPtr>
DatabaseContext::getTransformationsForGridName(
    const DatabaseContextNNPtr &databaseContext, const std::string &gridName) {
    auto sqlRes = databaseContext->d->run(
        "SELECT auth_name, code FROM grid_transformation "
        "WHERE grid_name = ? OR grid_name IN "
        "(SELECT original_grid_name FROM grid_alternatives "
        "WHERE proj_grid_name = ?) ORDER BY auth_name, code",
        {gridName, gridName});
    std::vector<operation::CoordinateOperationNNPtr> res;
    for (const auto &row : sqlRes) {
        res.emplace_back(AuthorityFactory::create(databaseContext, row[0])
                             ->createCoordinateOperation(row[1], true));
    }
    return res;
}

// ---------------------------------------------------------------------------

// Fixes wrong towgs84 values returned by epsg.io when using a Coordinate Frame
// transformation, where they neglect to reverse the sign of the rotation terms.
// Cf https://github.com/OSGeo/PROJ/issues/4170 and
// https://github.com/maptiler/epsg.io/issues/194
// We do that only when we found a valid Coordinate Frame rotation that
// has the same numeric values (and no corresponding Position Vector
// transformation with same values, or Coordinate Frame transformation with
// opposite sign for rotation terms, both are highly unlikely)
bool DatabaseContext::toWGS84AutocorrectWrongValues(
    double &tx, double &ty, double &tz, double &rx, double &ry, double &rz,
    double &scale_difference) const {
    if (rx == 0 && ry == 0 && rz == 0)
        return false;
    // 9606: Position Vector transformation (geog2D domain)
    // 9607: Coordinate Frame rotation (geog2D domain)
    std::string sql(
        "SELECT DISTINCT method_code "
        "FROM helmert_transformation_table WHERE "
        "abs(tx - ?) <= 1e-8 * abs(tx) AND "
        "abs(ty - ?) <= 1e-8 * abs(ty) AND "
        "abs(tz - ?) <= 1e-8 * abs(tz) AND "
        "abs(rx - ?) <= 1e-8 * abs(rx) AND "
        "abs(ry - ?) <= 1e-8 * abs(ry) AND "
        "abs(rz - ?) <= 1e-8 * abs(rz) AND "
        "abs(scale_difference - ?) <= 1e-8 * abs(scale_difference) AND "
        "method_auth_name = 'EPSG' AND "
        "method_code IN (9606, 9607) AND "
        "translation_uom_auth_name = 'EPSG' AND "
        "translation_uom_code = 9001 AND " // metre
        "rotation_uom_auth_name = 'EPSG' AND "
        "rotation_uom_code = 9104 AND " // arc-second
        "scale_difference_uom_auth_name = 'EPSG' AND "
        "scale_difference_uom_code = 9202 AND " // parts per million
        "deprecated = 0");
    ListOfParams params;
    params.emplace_back(tx);
    params.emplace_back(ty);
    params.emplace_back(tz);
    params.emplace_back(rx);
    params.emplace_back(ry);
    params.emplace_back(rz);
    params.emplace_back(scale_difference);
    bool bFound9606 = false;
    bool bFound9607 = false;
    for (const auto &row : d->run(sql, params)) {
        if (row[0] == "9606") {
            bFound9606 = true;
        } else if (row[0] == "9607") {
            bFound9607 = true;
        }
    }
    if (bFound9607 && !bFound9606) {
        params.clear();
        params.emplace_back(tx);
        params.emplace_back(ty);
        params.emplace_back(tz);
        params.emplace_back(-rx);
        params.emplace_back(-ry);
        params.emplace_back(-rz);
        params.emplace_back(scale_difference);
        if (d->run(sql, params).empty()) {
            if (d->pjCtxt()) {
                pj_log(d->pjCtxt(), PJ_LOG_ERROR,
                       "Auto-correcting wrong sign of rotation terms of "
                       "TOWGS84 clause from %s,%s,%s,%s,%s,%s,%s to "
                       "%s,%s,%s,%s,%s,%s,%s",
                       internal::toString(tx).c_str(),
                       internal::toString(ty).c_str(),
                       internal::toString(tz).c_str(),
                       internal::toString(rx).c_str(),
                       internal::toString(ry).c_str(),
                       internal::toString(rz).c_str(),
                       internal::toString(scale_difference).c_str(),
                       internal::toString(tx).c_str(),
                       internal::toString(ty).c_str(),
                       internal::toString(tz).c_str(),
                       internal::toString(-rx).c_str(),
                       internal::toString(-ry).c_str(),
                       internal::toString(-rz).c_str(),
                       internal::toString(scale_difference).c_str());
            }
            rx = -rx;
            ry = -ry;
            rz = -rz;
            return true;
        }
    }
    return false;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct AuthorityFactory::Private {
    Private(const DatabaseContextNNPtr &contextIn,
            const std::string &authorityName)
        : context_(contextIn), authority_(authorityName) {}

    inline const std::string &authority() PROJ_PURE_DEFN { return authority_; }

    inline const DatabaseContextNNPtr &context() PROJ_PURE_DEFN {
        return context_;
    }

    // cppcheck-suppress functionStatic
    void setThis(AuthorityFactoryNNPtr factory) {
        thisFactory_ = factory.as_nullable();
    }

    // cppcheck-suppress functionStatic
    AuthorityFactoryPtr getSharedFromThis() { return thisFactory_.lock(); }

    inline AuthorityFactoryNNPtr createFactory(const std::string &auth_name) {
        if (auth_name == authority_) {
            return NN_NO_CHECK(thisFactory_.lock());
        }
        return AuthorityFactory::create(context_, auth_name);
    }

    bool rejectOpDueToMissingGrid(const operation::CoordinateOperationNNPtr &op,
                                  bool considerKnownGridsAsAvailable);

    UnitOfMeasure createUnitOfMeasure(const std::string &auth_name,
                                      const std::string &code);

    util::PropertyMap
    createProperties(const std::string &code, const std::string &name,
                     bool deprecated,
                     const std::vector<ObjectDomainNNPtr> &usages);

    util::PropertyMap
    createPropertiesSearchUsages(const std::string &table_name,
                                 const std::string &code,
                                 const std::string &name, bool deprecated);

    util::PropertyMap createPropertiesSearchUsages(
        const std::string &table_name, const std::string &code,
        const std::string &name, bool deprecated, const std::string &remarks);

    SQLResultSet run(const std::string &sql,
                     const ListOfParams &parameters = ListOfParams());

    SQLResultSet runWithCodeParam(const std::string &sql,
                                  const std::string &code);

    SQLResultSet runWithCodeParam(const char *sql, const std::string &code);

    bool hasAuthorityRestriction() const {
        return !authority_.empty() && authority_ != "any";
    }

    SQLResultSet createProjectedCRSBegin(const std::string &code);
    crs::ProjectedCRSNNPtr createProjectedCRSEnd(const std::string &code,
                                                 const SQLResultSet &res);

  private:
    DatabaseContextNNPtr context_;
    std::string authority_;
    std::weak_ptr<AuthorityFactory> thisFactory_{};
};

// ---------------------------------------------------------------------------

SQLResultSet AuthorityFactory::Private::run(const std::string &sql,
                                            const ListOfParams &parameters) {
    return context()->getPrivate()->run(sql, parameters);
}

// ---------------------------------------------------------------------------

SQLResultSet
AuthorityFactory::Private::runWithCodeParam(const std::string &sql,
                                            const std::string &code) {
    return run(sql, {authority(), code});
}

// ---------------------------------------------------------------------------

SQLResultSet
AuthorityFactory::Private::runWithCodeParam(const char *sql,
                                            const std::string &code) {
    return runWithCodeParam(std::string(sql), code);
}

// ---------------------------------------------------------------------------

UnitOfMeasure
AuthorityFactory::Private::createUnitOfMeasure(const std::string &auth_name,
                                               const std::string &code) {
    return *(createFactory(auth_name)->createUnitOfMeasure(code));
}

// ---------------------------------------------------------------------------

util::PropertyMap AuthorityFactory::Private::createProperties(
    const std::string &code, const std::string &name, bool deprecated,
    const std::vector<ObjectDomainNNPtr> &usages) {
    auto props = util::PropertyMap()
                     .set(metadata::Identifier::CODESPACE_KEY, authority())
                     .set(metadata::Identifier::CODE_KEY, code)
                     .set(common::IdentifiedObject::NAME_KEY, name);
    if (deprecated) {
        props.set(common::IdentifiedObject::DEPRECATED_KEY, true);
    }
    if (!usages.empty()) {

        auto array(util::ArrayOfBaseObject::create());
        for (const auto &usage : usages) {
            array->add(usage);
        }
        props.set(common::ObjectUsage::OBJECT_DOMAIN_KEY,
                  util::nn_static_pointer_cast<util::BaseObject>(array));
    }
    return props;
}

// ---------------------------------------------------------------------------

util::PropertyMap AuthorityFactory::Private::createPropertiesSearchUsages(
    const std::string &table_name, const std::string &code,
    const std::string &name, bool deprecated) {

    SQLResultSet res;
    if (table_name == "geodetic_crs" && code == "4326" &&
        authority() == "EPSG") {
        // EPSG v10.077 has changed the extent from 1262 to 2830, whose
        // description is super verbose.
        // Cf https://epsg.org/closed-change-request/browse/id/2022.086
        // To avoid churn in our WKT2 output, hot patch to the usage of
        // 10.076 and earlier
        res = run("SELECT extent.description, extent.south_lat, "
                  "extent.north_lat, extent.west_lon, extent.east_lon, "
                  "scope.scope, 0 AS score FROM extent, scope WHERE "
                  "extent.code = 1262 and scope.code = 1183");
    } else {
        const std::string sql(
            "SELECT extent.description, extent.south_lat, "
            "extent.north_lat, extent.west_lon, extent.east_lon, "
            "scope.scope, "
            "(CASE WHEN scope.scope LIKE '%large scale%' THEN 0 ELSE 1 END) "
            "AS score "
            "FROM usage "
            "JOIN extent ON usage.extent_auth_name = extent.auth_name AND "
            "usage.extent_code = extent.code "
            "JOIN scope ON usage.scope_auth_name = scope.auth_name AND "
            "usage.scope_code = scope.code "
            "WHERE object_table_name = ? AND object_auth_name = ? AND "
            "object_code = ? AND "
            // We voluntary exclude extent and scope with a specific code
            "NOT (usage.extent_auth_name = 'PROJ' AND "
            "usage.extent_code = 'EXTENT_UNKNOWN') AND "
            "NOT (usage.scope_auth_name = 'PROJ' AND "
            "usage.scope_code = 'SCOPE_UNKNOWN') "
            "ORDER BY score, usage.auth_name, usage.code");
        res = run(sql, {table_name, authority(), code});
    }
    std::vector<ObjectDomainNNPtr> usages;
    for (const auto &row : res) {
        try {
            size_t idx = 0;
            const auto &extent_description = row[idx++];
            const auto &south_lat_str = row[idx++];
            const auto &north_lat_str = row[idx++];
            const auto &west_lon_str = row[idx++];
            const auto &east_lon_str = row[idx++];
            const auto &scope = row[idx];

            util::optional<std::string> scopeOpt;
            if (!scope.empty()) {
                scopeOpt = scope;
            }

            metadata::ExtentPtr extent;
            if (south_lat_str.empty()) {
                extent = metadata::Extent::create(
                             util::optional<std::string>(extent_description),
                             {}, {}, {})
                             .as_nullable();
            } else {
                double south_lat = c_locale_stod(south_lat_str);
                double north_lat = c_locale_stod(north_lat_str);
                double west_lon = c_locale_stod(west_lon_str);
                double east_lon = c_locale_stod(east_lon_str);
                auto bbox = metadata::GeographicBoundingBox::create(
                    west_lon, south_lat, east_lon, north_lat);
                extent = metadata::Extent::create(
                             util::optional<std::string>(extent_description),
                             std::vector<metadata::GeographicExtentNNPtr>{bbox},
                             std::vector<metadata::VerticalExtentNNPtr>(),
                             std::vector<metadata::TemporalExtentNNPtr>())
                             .as_nullable();
            }

            usages.emplace_back(ObjectDomain::create(scopeOpt, extent));
        } catch (const std::exception &) {
        }
    }
    return createProperties(code, name, deprecated, std::move(usages));
}

// ---------------------------------------------------------------------------

util::PropertyMap AuthorityFactory::Private::createPropertiesSearchUsages(
    const std::string &table_name, const std::string &code,
    const std::string &name, bool deprecated, const std::string &remarks) {
    auto props =
        createPropertiesSearchUsages(table_name, code, name, deprecated);
    if (!remarks.empty())
        props.set(common::IdentifiedObject::REMARKS_KEY, remarks);
    return props;
}

// ---------------------------------------------------------------------------

bool AuthorityFactory::Private::rejectOpDueToMissingGrid(
    const operation::CoordinateOperationNNPtr &op,
    bool considerKnownGridsAsAvailable) {

    struct DisableNetwork {
        const DatabaseContextNNPtr &m_dbContext;
        bool m_old_network_enabled = false;

        explicit DisableNetwork(const DatabaseContextNNPtr &l_context)
            : m_dbContext(l_context) {
            auto ctxt = m_dbContext->d->pjCtxt();
            if (ctxt == nullptr) {
                ctxt = pj_get_default_ctx();
                m_dbContext->d->setPjCtxt(ctxt);
            }
            m_old_network_enabled =
                proj_context_is_network_enabled(ctxt) != FALSE;
            if (m_old_network_enabled)
                proj_context_set_enable_network(ctxt, false);
        }

        ~DisableNetwork() {
            if (m_old_network_enabled) {
                auto ctxt = m_dbContext->d->pjCtxt();
                proj_context_set_enable_network(ctxt, true);
            }
        }
    };

    auto &l_context = context();
    // Temporarily disable networking as we are only interested in known grids
    DisableNetwork disabler(l_context);

    for (const auto &gridDesc :
         op->gridsNeeded(l_context, considerKnownGridsAsAvailable)) {
        if (!gridDesc.available) {
            return true;
        }
    }
    return false;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
AuthorityFactory::~AuthorityFactory() = default;
//! @endcond

// ---------------------------------------------------------------------------

AuthorityFactory::AuthorityFactory(const DatabaseContextNNPtr &context,
                                   const std::string &authorityName)
    : d(std::make_unique<Private>(context, authorityName)) {}

// ---------------------------------------------------------------------------

// clang-format off
/** \brief Instantiate a AuthorityFactory.
 *
 * The authority name might be set to the empty string in the particular case
 * where createFromCoordinateReferenceSystemCodes(const std::string&,const std::string&,const std::string&,const std::string&) const
 * is called.
 *
 * @param context Context.
 * @param authorityName Authority name.
 * @return new AuthorityFactory.
 */
// clang-format on

AuthorityFactoryNNPtr
AuthorityFactory::create(const DatabaseContextNNPtr &context,
                         const std::string &authorityName) {
    const auto getFactory = [&context, &authorityName]() {
        for (const auto &knownName :
             {metadata::Identifier::EPSG.c_str(), "ESRI", "PROJ"}) {
            if (ci_equal(authorityName, knownName)) {
                return AuthorityFactory::nn_make_shared<AuthorityFactory>(
                    context, knownName);
            }
        }
        return AuthorityFactory::nn_make_shared<AuthorityFactory>(
            context, authorityName);
    };
    auto factory = getFactory();
    factory->d->setThis(factory);
    return factory;
}

// ---------------------------------------------------------------------------

/** \brief Returns the database context. */
const DatabaseContextNNPtr &AuthorityFactory::databaseContext() const {
    return d->context();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
AuthorityFactory::CRSInfo::CRSInfo()
    : authName{}, code{}, name{}, type{ObjectType::CRS}, deprecated{},
      bbox_valid{}, west_lon_degree{}, south_lat_degree{}, east_lon_degree{},
      north_lat_degree{}, areaName{}, projectionMethodName{},
      celestialBodyName{} {}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns an arbitrary object from a code.
 *
 * The returned object will typically be an instance of Datum,
 * CoordinateSystem, ReferenceSystem or CoordinateOperation. If the type of
 * the object is know at compile time, it is recommended to invoke the most
 * precise method instead of this one (for example
 * createCoordinateReferenceSystem(code) instead of createObject(code)
 * if the caller know he is asking for a coordinate reference system).
 *
 * If there are several objects with the same code, a FactoryException is
 * thrown.
 *
 * @param code Object code allocated by authority. (e.g. "4326")
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

util::BaseObjectNNPtr
AuthorityFactory::createObject(const std::string &code) const {

    auto res = d->runWithCodeParam("SELECT table_name, type FROM object_view "
                                   "WHERE auth_name = ? AND code = ?",
                                   code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("not found", d->authority(), code);
    }
    if (res.size() != 1) {
        std::string msg(
            "More than one object matching specified code. Objects found in ");
        bool first = true;
        for (const auto &row : res) {
            if (!first)
                msg += ", ";
            msg += row[0];
            first = false;
        }
        throw FactoryException(msg);
    }
    const auto &first_row = res.front();
    const auto &table_name = first_row[0];
    const auto &type = first_row[1];
    if (table_name == "extent") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createExtent(code));
    }
    if (table_name == "unit_of_measure") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createUnitOfMeasure(code));
    }
    if (table_name == "prime_meridian") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createPrimeMeridian(code));
    }
    if (table_name == "ellipsoid") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createEllipsoid(code));
    }
    if (table_name == "geodetic_datum") {
        if (type == "ensemble") {
            return util::nn_static_pointer_cast<util::BaseObject>(
                createDatumEnsemble(code, table_name));
        }
        return util::nn_static_pointer_cast<util::BaseObject>(
            createGeodeticDatum(code));
    }
    if (table_name == "vertical_datum") {
        if (type == "ensemble") {
            return util::nn_static_pointer_cast<util::BaseObject>(
                createDatumEnsemble(code, table_name));
        }
        return util::nn_static_pointer_cast<util::BaseObject>(
            createVerticalDatum(code));
    }
    if (table_name == "engineering_datum") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createEngineeringDatum(code));
    }
    if (table_name == "geodetic_crs") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createGeodeticCRS(code));
    }
    if (table_name == "vertical_crs") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createVerticalCRS(code));
    }
    if (table_name == "projected_crs") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createProjectedCRS(code));
    }
    if (table_name == "compound_crs") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createCompoundCRS(code));
    }
    if (table_name == "engineering_crs") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createEngineeringCRS(code));
    }
    if (table_name == "conversion") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createConversion(code));
    }
    if (table_name == "helmert_transformation" ||
        table_name == "grid_transformation" ||
        table_name == "other_transformation" ||
        table_name == "concatenated_operation") {
        return util::nn_static_pointer_cast<util::BaseObject>(
            createCoordinateOperation(code, false));
    }
    throw FactoryException("unimplemented factory for " + res.front()[0]);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static FactoryException buildFactoryException(const char *type,
                                              const std::string &authName,
                                              const std::string &code,
                                              const std::exception &ex) {
    return FactoryException(std::string("cannot build ") + type + " " +
                            authName + ":" + code + ": " + ex.what());
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns a metadata::Extent from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

metadata::ExtentNNPtr
AuthorityFactory::createExtent(const std::string &code) const {
    const auto cacheKey(d->authority() + code);
    {
        auto extent = d->context()->d->getExtentFromCache(cacheKey);
        if (extent) {
            return NN_NO_CHECK(extent);
        }
    }
    auto sql = "SELECT description, south_lat, north_lat, west_lon, east_lon, "
               "deprecated FROM extent WHERE auth_name = ? AND code = ?";
    auto res = d->runWithCodeParam(sql, code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("extent not found", d->authority(),
                                           code);
    }
    try {
        const auto &row = res.front();
        const auto &description = row[0];
        if (row[1].empty()) {
            auto extent = metadata::Extent::create(
                util::optional<std::string>(description), {}, {}, {});
            d->context()->d->cache(cacheKey, extent);
            return extent;
        }
        double south_lat = c_locale_stod(row[1]);
        double north_lat = c_locale_stod(row[2]);
        double west_lon = c_locale_stod(row[3]);
        double east_lon = c_locale_stod(row[4]);
        auto bbox = metadata::GeographicBoundingBox::create(
            west_lon, south_lat, east_lon, north_lat);

        auto extent = metadata::Extent::create(
            util::optional<std::string>(description),
            std::vector<metadata::GeographicExtentNNPtr>{bbox},
            std::vector<metadata::VerticalExtentNNPtr>(),
            std::vector<metadata::TemporalExtentNNPtr>());
        d->context()->d->cache(cacheKey, extent);
        return extent;

    } catch (const std::exception &ex) {
        throw buildFactoryException("extent", d->authority(), code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a common::UnitOfMeasure from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

UnitOfMeasureNNPtr
AuthorityFactory::createUnitOfMeasure(const std::string &code) const {
    const auto cacheKey(d->authority() + code);
    {
        auto uom = d->context()->d->getUOMFromCache(cacheKey);
        if (uom) {
            return NN_NO_CHECK(uom);
        }
    }
    auto res = d->context()->d->run(
        "SELECT name, conv_factor, type, deprecated FROM unit_of_measure WHERE "
        "auth_name = ? AND code = ?",
        {d->authority(), code}, true);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("unit of measure not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name =
            (row[0] == "degree (supplier to define representation)")
                ? UnitOfMeasure::DEGREE.name()
                : row[0];
        double conv_factor = (code == "9107" || code == "9108")
                                 ? UnitOfMeasure::DEGREE.conversionToSI()
                                 : c_locale_stod(row[1]);
        constexpr double EPS = 1e-10;
        if (std::fabs(conv_factor - UnitOfMeasure::DEGREE.conversionToSI()) <
            EPS * UnitOfMeasure::DEGREE.conversionToSI()) {
            conv_factor = UnitOfMeasure::DEGREE.conversionToSI();
        }
        if (std::fabs(conv_factor -
                      UnitOfMeasure::ARC_SECOND.conversionToSI()) <
            EPS * UnitOfMeasure::ARC_SECOND.conversionToSI()) {
            conv_factor = UnitOfMeasure::ARC_SECOND.conversionToSI();
        }
        const auto &type_str = row[2];
        UnitOfMeasure::Type unitType = UnitOfMeasure::Type::UNKNOWN;
        if (type_str == "length")
            unitType = UnitOfMeasure::Type::LINEAR;
        else if (type_str == "angle")
            unitType = UnitOfMeasure::Type::ANGULAR;
        else if (type_str == "scale")
            unitType = UnitOfMeasure::Type::SCALE;
        else if (type_str == "time")
            unitType = UnitOfMeasure::Type::TIME;
        auto uom = util::nn_make_shared<UnitOfMeasure>(
            name, conv_factor, unitType, d->authority(), code);
        d->context()->d->cache(cacheKey, uom);
        return uom;
    } catch (const std::exception &ex) {
        throw buildFactoryException("unit of measure", d->authority(), code,
                                    ex);
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static double normalizeMeasure(const std::string &uom_code,
                               const std::string &value,
                               std::string &normalized_uom_code) {
    if (uom_code == "9110") // DDD.MMSSsss.....
    {
        double normalized_value = c_locale_stod(value);
        std::ostringstream buffer;
        buffer.imbue(std::locale::classic());
        constexpr size_t precision = 12;
        buffer << std::fixed << std::setprecision(precision)
               << normalized_value;
        auto formatted = buffer.str();
        size_t dotPos = formatted.find('.');
        assert(dotPos + 1 + precision == formatted.size());
        auto minutes = formatted.substr(dotPos + 1, 2);
        auto seconds = formatted.substr(dotPos + 3);
        assert(seconds.size() == precision - 2);
        normalized_value =
            (normalized_value < 0 ? -1.0 : 1.0) *
            (std::floor(std::fabs(normalized_value)) +
             c_locale_stod(minutes) / 60. +
             (c_locale_stod(seconds) / std::pow(10, seconds.size() - 2)) /
                 3600.);
        normalized_uom_code = common::UnitOfMeasure::DEGREE.code();
        /* coverity[overflow_sink] */
        return normalized_value;
    } else {
        normalized_uom_code = uom_code;
        return c_locale_stod(value);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns a datum::PrimeMeridian from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

datum::PrimeMeridianNNPtr
AuthorityFactory::createPrimeMeridian(const std::string &code) const {
    const auto cacheKey(d->authority() + code);
    {
        auto pm = d->context()->d->getPrimeMeridianFromCache(cacheKey);
        if (pm) {
            return NN_NO_CHECK(pm);
        }
    }
    auto res = d->runWithCodeParam(
        "SELECT name, longitude, uom_auth_name, uom_code, deprecated FROM "
        "prime_meridian WHERE "
        "auth_name = ? AND code = ?",
        code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("prime meridian not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &longitude = row[1];
        const auto &uom_auth_name = row[2];
        const auto &uom_code = row[3];
        const bool deprecated = row[4] == "1";

        std::string normalized_uom_code(uom_code);
        const double normalized_value =
            normalizeMeasure(uom_code, longitude, normalized_uom_code);

        auto uom = d->createUnitOfMeasure(uom_auth_name, normalized_uom_code);
        auto props = d->createProperties(code, name, deprecated, {});
        auto pm = datum::PrimeMeridian::create(
            props, common::Angle(normalized_value, uom));
        d->context()->d->cache(cacheKey, pm);
        return pm;
    } catch (const std::exception &ex) {
        throw buildFactoryException("prime meridian", d->authority(), code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Identify a celestial body from an approximate radius.
 *
 * @param semi_major_axis Approximate semi-major axis.
 * @param tolerance Relative error allowed.
 * @return celestial body name if one single match found.
 * @throw FactoryException in case of error.
 */

std::string
AuthorityFactory::identifyBodyFromSemiMajorAxis(double semi_major_axis,
                                                double tolerance) const {
    auto res =
        d->run("SELECT DISTINCT name, "
               "(ABS(semi_major_axis - ?) / semi_major_axis ) AS rel_error "
               "FROM celestial_body WHERE rel_error <= ? "
               "ORDER BY rel_error, name",
               {semi_major_axis, tolerance});
    if (res.empty()) {
        throw FactoryException("no match found");
    }
    constexpr int IDX_NAME = 0;
    if (res.size() > 1) {
        constexpr int IDX_REL_ERROR = 1;
        // If the first object has a relative error of 0 and the next one
        // a non-zero error, then use the first one.
        if (res.front()[IDX_REL_ERROR] == "0" &&
            (*std::next(res.begin()))[IDX_REL_ERROR] != "0") {
            return res.front()[IDX_NAME];
        }
        for (const auto &row : res) {
            if (row[IDX_NAME] != res.front()[IDX_NAME]) {
                throw FactoryException("more than one match found");
            }
        }
    }
    return res.front()[IDX_NAME];
}

// ---------------------------------------------------------------------------

/** \brief Returns a datum::Ellipsoid from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

datum::EllipsoidNNPtr
AuthorityFactory::createEllipsoid(const std::string &code) const {
    const auto cacheKey(d->authority() + code);
    {
        auto ellps = d->context()->d->getEllipsoidFromCache(cacheKey);
        if (ellps) {
            return NN_NO_CHECK(ellps);
        }
    }
    auto res = d->runWithCodeParam(
        "SELECT ellipsoid.name, ellipsoid.semi_major_axis, "
        "ellipsoid.uom_auth_name, ellipsoid.uom_code, "
        "ellipsoid.inv_flattening, ellipsoid.semi_minor_axis, "
        "celestial_body.name AS body_name, ellipsoid.deprecated FROM "
        "ellipsoid JOIN celestial_body "
        "ON ellipsoid.celestial_body_auth_name = celestial_body.auth_name AND "
        "ellipsoid.celestial_body_code = celestial_body.code WHERE "
        "ellipsoid.auth_name = ? AND ellipsoid.code = ?",
        code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("ellipsoid not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &semi_major_axis_str = row[1];
        double semi_major_axis = c_locale_stod(semi_major_axis_str);
        const auto &uom_auth_name = row[2];
        const auto &uom_code = row[3];
        const auto &inv_flattening_str = row[4];
        const auto &semi_minor_axis_str = row[5];
        const auto &body = row[6];
        const bool deprecated = row[7] == "1";
        auto uom = d->createUnitOfMeasure(uom_auth_name, uom_code);
        auto props = d->createProperties(code, name, deprecated, {});
        if (!inv_flattening_str.empty()) {
            auto ellps = datum::Ellipsoid::createFlattenedSphere(
                props, common::Length(semi_major_axis, uom),
                common::Scale(c_locale_stod(inv_flattening_str)), body);
            d->context()->d->cache(cacheKey, ellps);
            return ellps;
        } else if (semi_major_axis_str == semi_minor_axis_str) {
            auto ellps = datum::Ellipsoid::createSphere(
                props, common::Length(semi_major_axis, uom), body);
            d->context()->d->cache(cacheKey, ellps);
            return ellps;
        } else {
            auto ellps = datum::Ellipsoid::createTwoAxis(
                props, common::Length(semi_major_axis, uom),
                common::Length(c_locale_stod(semi_minor_axis_str), uom), body);
            d->context()->d->cache(cacheKey, ellps);
            return ellps;
        }
    } catch (const std::exception &ex) {
        throw buildFactoryException("ellipsoid", d->authority(), code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a datum::GeodeticReferenceFrame from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

datum::GeodeticReferenceFrameNNPtr
AuthorityFactory::createGeodeticDatum(const std::string &code) const {

    datum::GeodeticReferenceFramePtr datum;
    datum::DatumEnsemblePtr datumEnsemble;
    constexpr bool turnEnsembleAsDatum = true;
    createGeodeticDatumOrEnsemble(code, datum, datumEnsemble,
                                  turnEnsembleAsDatum);
    return NN_NO_CHECK(datum);
}

// ---------------------------------------------------------------------------

void AuthorityFactory::createGeodeticDatumOrEnsemble(
    const std::string &code, datum::GeodeticReferenceFramePtr &outDatum,
    datum::DatumEnsemblePtr &outDatumEnsemble, bool turnEnsembleAsDatum) const {
    const auto cacheKey(d->authority() + code);
    {
        outDatumEnsemble = d->context()->d->getDatumEnsembleFromCache(cacheKey);
        if (outDatumEnsemble) {
            if (!turnEnsembleAsDatum)
                return;
            outDatumEnsemble = nullptr;
        }
        outDatum = d->context()->d->getGeodeticDatumFromCache(cacheKey);
        if (outDatum) {
            return;
        }
    }
    auto res = d->runWithCodeParam(
        "SELECT name, ellipsoid_auth_name, ellipsoid_code, "
        "prime_meridian_auth_name, prime_meridian_code, "
        "publication_date, frame_reference_epoch, "
        "ensemble_accuracy, anchor, anchor_epoch, deprecated "
        "FROM geodetic_datum "
        "WHERE "
        "auth_name = ? AND code = ?",
        code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("geodetic datum not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &ellipsoid_auth_name = row[1];
        const auto &ellipsoid_code = row[2];
        const auto &prime_meridian_auth_name = row[3];
        const auto &prime_meridian_code = row[4];
        const auto &publication_date = row[5];
        const auto &frame_reference_epoch = row[6];
        const auto &ensemble_accuracy = row[7];
        const auto &anchor = row[8];
        const auto &anchor_epoch = row[9];
        const bool deprecated = row[10] == "1";

        std::string massagedName = name;
        if (turnEnsembleAsDatum) {
            if (name == "World Geodetic System 1984 ensemble") {
                massagedName = "World Geodetic System 1984";
            } else if (name ==
                       "European Terrestrial Reference System 1989 ensemble") {
                massagedName = "European Terrestrial Reference System 1989";
            }
        }
        auto props = d->createPropertiesSearchUsages("geodetic_datum", code,
                                                     massagedName, deprecated);

        if (!turnEnsembleAsDatum && !ensemble_accuracy.empty()) {
            auto resMembers =
                d->run("SELECT member_auth_name, member_code FROM "
                       "geodetic_datum_ensemble_member WHERE "
                       "ensemble_auth_name = ? AND ensemble_code = ? "
                       "ORDER BY sequence",
                       {d->authority(), code});

            std::vector<datum::DatumNNPtr> members;
            for (const auto &memberRow : resMembers) {
                members.push_back(
                    d->createFactory(memberRow[0])->createDatum(memberRow[1]));
            }
            auto datumEnsemble = datum::DatumEnsemble::create(
                props, std::move(members),
                metadata::PositionalAccuracy::create(ensemble_accuracy));
            d->context()->d->cache(cacheKey, datumEnsemble);
            outDatumEnsemble = datumEnsemble.as_nullable();
        } else {
            auto ellipsoid = d->createFactory(ellipsoid_auth_name)
                                 ->createEllipsoid(ellipsoid_code);
            auto pm = d->createFactory(prime_meridian_auth_name)
                          ->createPrimeMeridian(prime_meridian_code);

            auto anchorOpt = util::optional<std::string>();
            if (!anchor.empty())
                anchorOpt = anchor;
            if (!publication_date.empty()) {
                props.set("PUBLICATION_DATE", publication_date);
            }
            if (!anchor_epoch.empty()) {
                props.set("ANCHOR_EPOCH", anchor_epoch);
            }
            auto datum = frame_reference_epoch.empty()
                             ? datum::GeodeticReferenceFrame::create(
                                   props, ellipsoid, anchorOpt, pm)
                             : util::nn_static_pointer_cast<
                                   datum::GeodeticReferenceFrame>(
                                   datum::DynamicGeodeticReferenceFrame::create(
                                       props, ellipsoid, anchorOpt, pm,
                                       common::Measure(
                                           c_locale_stod(frame_reference_epoch),
                                           common::UnitOfMeasure::YEAR),
                                       util::optional<std::string>()));
            d->context()->d->cache(cacheKey, datum);
            outDatum = datum.as_nullable();
        }
    } catch (const std::exception &ex) {
        throw buildFactoryException("geodetic reference frame", d->authority(),
                                    code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a datum::VerticalReferenceFrame from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

datum::VerticalReferenceFrameNNPtr
AuthorityFactory::createVerticalDatum(const std::string &code) const {
    datum::VerticalReferenceFramePtr datum;
    datum::DatumEnsemblePtr datumEnsemble;
    constexpr bool turnEnsembleAsDatum = true;
    createVerticalDatumOrEnsemble(code, datum, datumEnsemble,
                                  turnEnsembleAsDatum);
    return NN_NO_CHECK(datum);
}

// ---------------------------------------------------------------------------

void AuthorityFactory::createVerticalDatumOrEnsemble(
    const std::string &code, datum::VerticalReferenceFramePtr &outDatum,
    datum::DatumEnsemblePtr &outDatumEnsemble, bool turnEnsembleAsDatum) const {
    auto res =
        d->runWithCodeParam("SELECT name, publication_date, "
                            "frame_reference_epoch, ensemble_accuracy, anchor, "
                            "anchor_epoch, deprecated FROM "
                            "vertical_datum WHERE auth_name = ? AND code = ?",
                            code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("vertical datum not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &publication_date = row[1];
        const auto &frame_reference_epoch = row[2];
        const auto &ensemble_accuracy = row[3];
        const auto &anchor = row[4];
        const auto &anchor_epoch = row[5];
        const bool deprecated = row[6] == "1";
        auto props = d->createPropertiesSearchUsages("vertical_datum", code,
                                                     name, deprecated);
        if (!turnEnsembleAsDatum && !ensemble_accuracy.empty()) {
            auto resMembers =
                d->run("SELECT member_auth_name, member_code FROM "
                       "vertical_datum_ensemble_member WHERE "
                       "ensemble_auth_name = ? AND ensemble_code = ? "
                       "ORDER BY sequence",
                       {d->authority(), code});

            std::vector<datum::DatumNNPtr> members;
            for (const auto &memberRow : resMembers) {
                members.push_back(
                    d->createFactory(memberRow[0])->createDatum(memberRow[1]));
            }
            auto datumEnsemble = datum::DatumEnsemble::create(
                props, std::move(members),
                metadata::PositionalAccuracy::create(ensemble_accuracy));
            outDatumEnsemble = datumEnsemble.as_nullable();
        } else {
            if (!publication_date.empty()) {
                props.set("PUBLICATION_DATE", publication_date);
            }
            if (!anchor_epoch.empty()) {
                props.set("ANCHOR_EPOCH", anchor_epoch);
            }
            if (d->authority() == "ESRI" &&
                starts_with(code, "from_geogdatum_")) {
                props.set("VERT_DATUM_TYPE", "2002");
            }
            auto anchorOpt = util::optional<std::string>();
            if (!anchor.empty())
                anchorOpt = anchor;
            if (frame_reference_epoch.empty()) {
                outDatum =
                    datum::VerticalReferenceFrame::create(props, anchorOpt)
                        .as_nullable();
            } else {
                outDatum =
                    datum::DynamicVerticalReferenceFrame::create(
                        props, anchorOpt,
                        util::optional<datum::RealizationMethod>(),
                        common::Measure(c_locale_stod(frame_reference_epoch),
                                        common::UnitOfMeasure::YEAR),
                        util::optional<std::string>())
                        .as_nullable();
            }
        }
    } catch (const std::exception &ex) {
        throw buildFactoryException("vertical reference frame", d->authority(),
                                    code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a datum::EngineeringDatum from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 * @since 9.6
 */

datum::EngineeringDatumNNPtr
AuthorityFactory::createEngineeringDatum(const std::string &code) const {
    auto res = d->runWithCodeParam(
        "SELECT name, publication_date, "
        "anchor, anchor_epoch, deprecated FROM "
        "engineering_datum WHERE auth_name = ? AND code = ?",
        code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("engineering datum not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &publication_date = row[1];
        const auto &anchor = row[2];
        const auto &anchor_epoch = row[3];
        const bool deprecated = row[4] == "1";
        auto props = d->createPropertiesSearchUsages("engineering_datum", code,
                                                     name, deprecated);

        if (!publication_date.empty()) {
            props.set("PUBLICATION_DATE", publication_date);
        }
        if (!anchor_epoch.empty()) {
            props.set("ANCHOR_EPOCH", anchor_epoch);
        }
        auto anchorOpt = util::optional<std::string>();
        if (!anchor.empty())
            anchorOpt = anchor;
        return datum::EngineeringDatum::create(props, anchorOpt);
    } catch (const std::exception &ex) {
        throw buildFactoryException("engineering datum", d->authority(), code,
                                    ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a datum::DatumEnsemble from the specified code.
 *
 * @param code Object code allocated by authority.
 * @param type "geodetic_datum", "vertical_datum" or empty string if unknown
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

datum::DatumEnsembleNNPtr
AuthorityFactory::createDatumEnsemble(const std::string &code,
                                      const std::string &type) const {
    auto res = d->run(
        "SELECT 'geodetic_datum', name, ensemble_accuracy, deprecated FROM "
        "geodetic_datum WHERE "
        "auth_name = ? AND code = ? AND ensemble_accuracy IS NOT NULL "
        "UNION ALL "
        "SELECT 'vertical_datum', name, ensemble_accuracy, deprecated FROM "
        "vertical_datum WHERE "
        "auth_name = ? AND code = ? AND ensemble_accuracy IS NOT NULL",
        {d->authority(), code, d->authority(), code});
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("datum ensemble not found",
                                           d->authority(), code);
    }
    for (const auto &row : res) {
        const std::string &gotType = row[0];
        const std::string &name = row[1];
        const std::string &ensembleAccuracy = row[2];
        const bool deprecated = row[3] == "1";
        if (type.empty() || type == gotType) {
            auto resMembers =
                d->run("SELECT member_auth_name, member_code FROM " + gotType +
                           "_ensemble_member WHERE "
                           "ensemble_auth_name = ? AND ensemble_code = ? "
                           "ORDER BY sequence",
                       {d->authority(), code});

            std::vector<datum::DatumNNPtr> members;
            for (const auto &memberRow : resMembers) {
                members.push_back(
                    d->createFactory(memberRow[0])->createDatum(memberRow[1]));
            }
            auto props = d->createPropertiesSearchUsages(gotType, code, name,
                                                         deprecated);
            return datum::DatumEnsemble::create(
                props, std::move(members),
                metadata::PositionalAccuracy::create(ensembleAccuracy));
        }
    }
    throw NoSuchAuthorityCodeException("datum ensemble not found",
                                       d->authority(), code);
}

// ---------------------------------------------------------------------------

/** \brief Returns a datum::Datum from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

datum::DatumNNPtr AuthorityFactory::createDatum(const std::string &code) const {
    auto res = d->run(
        "SELECT 'geodetic_datum' FROM geodetic_datum WHERE "
        "auth_name = ? AND code = ? "
        "UNION ALL SELECT 'vertical_datum' FROM vertical_datum WHERE "
        "auth_name = ? AND code = ? "
        "UNION ALL SELECT 'engineering_datum' FROM engineering_datum "
        "WHERE "
        "auth_name = ? AND code = ?",
        {d->authority(), code, d->authority(), code, d->authority(), code});
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("datum not found", d->authority(),
                                           code);
    }
    const auto &type = res.front()[0];
    if (type == "geodetic_datum") {
        return createGeodeticDatum(code);
    }
    if (type == "vertical_datum") {
        return createVerticalDatum(code);
    }
    return createEngineeringDatum(code);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static cs::MeridianPtr createMeridian(const std::string &val) {
    try {
        const std::string degW(std::string("\xC2\xB0") + "W");
        if (ends_with(val, degW)) {
            return cs::Meridian::create(common::Angle(
                -c_locale_stod(val.substr(0, val.size() - degW.size()))));
        }
        const std::string degE(std::string("\xC2\xB0") + "E");
        if (ends_with(val, degE)) {
            return cs::Meridian::create(common::Angle(
                c_locale_stod(val.substr(0, val.size() - degE.size()))));
        }
    } catch (const std::exception &) {
    }
    return nullptr;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns a cs::CoordinateSystem from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

cs::CoordinateSystemNNPtr
AuthorityFactory::createCoordinateSystem(const std::string &code) const {
    const auto cacheKey(d->authority() + code);
    {
        auto cs = d->context()->d->getCoordinateSystemFromCache(cacheKey);
        if (cs) {
            return NN_NO_CHECK(cs);
        }
    }
    auto res = d->runWithCodeParam(
        "SELECT axis.name, abbrev, orientation, uom_auth_name, uom_code, "
        "cs.type FROM "
        "axis LEFT JOIN coordinate_system cs ON "
        "axis.coordinate_system_auth_name = cs.auth_name AND "
        "axis.coordinate_system_code = cs.code WHERE "
        "coordinate_system_auth_name = ? AND coordinate_system_code = ? ORDER "
        "BY coordinate_system_order",
        code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("coordinate system not found",
                                           d->authority(), code);
    }

    const auto &csType = res.front()[5];
    std::vector<cs::CoordinateSystemAxisNNPtr> axisList;
    for (const auto &row : res) {
        const auto &name = row[0];
        const auto &abbrev = row[1];
        const auto &orientation = row[2];
        const auto &uom_auth_name = row[3];
        const auto &uom_code = row[4];
        if (uom_auth_name.empty() && csType != CS_TYPE_ORDINAL) {
            throw FactoryException("no unit of measure for an axis is only "
                                   "supported for ordinatal CS");
        }
        auto uom = uom_auth_name.empty()
                       ? common::UnitOfMeasure::NONE
                       : d->createUnitOfMeasure(uom_auth_name, uom_code);
        auto props =
            util::PropertyMap().set(common::IdentifiedObject::NAME_KEY, name);
        const cs::AxisDirection *direction =
            cs::AxisDirection::valueOf(orientation);
        cs::MeridianPtr meridian;
        if (direction == nullptr) {
            if (orientation == "Geocentre > equator/0"
                               "\xC2\xB0"
                               "E") {
                direction = &(cs::AxisDirection::GEOCENTRIC_X);
            } else if (orientation == "Geocentre > equator/90"
                                      "\xC2\xB0"
                                      "E") {
                direction = &(cs::AxisDirection::GEOCENTRIC_Y);
            } else if (orientation == "Geocentre > north pole") {
                direction = &(cs::AxisDirection::GEOCENTRIC_Z);
            } else if (starts_with(orientation, "North along ")) {
                direction = &(cs::AxisDirection::NORTH);
                meridian =
                    createMeridian(orientation.substr(strlen("North along ")));
            } else if (starts_with(orientation, "South along ")) {
                direction = &(cs::AxisDirection::SOUTH);
                meridian =
                    createMeridian(orientation.substr(strlen("South along ")));
            } else {
                throw FactoryException("unknown axis direction: " +
                                       orientation);
            }
        }
        axisList.emplace_back(cs::CoordinateSystemAxis::create(
            props, abbrev, *direction, uom, meridian));
    }

    const auto cacheAndRet = [this,
                              &cacheKey](const cs::CoordinateSystemNNPtr &cs) {
        d->context()->d->cache(cacheKey, cs);
        return cs;
    };

    auto props = util::PropertyMap()
                     .set(metadata::Identifier::CODESPACE_KEY, d->authority())
                     .set(metadata::Identifier::CODE_KEY, code);
    if (csType == CS_TYPE_ELLIPSOIDAL) {
        if (axisList.size() == 2) {
            return cacheAndRet(
                cs::EllipsoidalCS::create(props, axisList[0], axisList[1]));
        }
        if (axisList.size() == 3) {
            return cacheAndRet(cs::EllipsoidalCS::create(
                props, axisList[0], axisList[1], axisList[2]));
        }
        throw FactoryException("invalid number of axis for EllipsoidalCS");
    }
    if (csType == CS_TYPE_CARTESIAN) {
        if (axisList.size() == 2) {
            return cacheAndRet(
                cs::CartesianCS::create(props, axisList[0], axisList[1]));
        }
        if (axisList.size() == 3) {
            return cacheAndRet(cs::CartesianCS::create(
                props, axisList[0], axisList[1], axisList[2]));
        }
        throw FactoryException("invalid number of axis for CartesianCS");
    }
    if (csType == CS_TYPE_SPHERICAL) {
        if (axisList.size() == 2) {
            return cacheAndRet(
                cs::SphericalCS::create(props, axisList[0], axisList[1]));
        }
        if (axisList.size() == 3) {
            return cacheAndRet(cs::SphericalCS::create(
                props, axisList[0], axisList[1], axisList[2]));
        }
        throw FactoryException("invalid number of axis for SphericalCS");
    }
    if (csType == CS_TYPE_VERTICAL) {
        if (axisList.size() == 1) {
            return cacheAndRet(cs::VerticalCS::create(props, axisList[0]));
        }
        throw FactoryException("invalid number of axis for VerticalCS");
    }
    if (csType == CS_TYPE_ORDINAL) {
        return cacheAndRet(cs::OrdinalCS::create(props, axisList));
    }
    throw FactoryException("unhandled coordinate system type: " + csType);
}

// ---------------------------------------------------------------------------

/** \brief Returns a crs::GeodeticCRS from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

crs::GeodeticCRSNNPtr
AuthorityFactory::createGeodeticCRS(const std::string &code) const {
    return createGeodeticCRS(code, false);
}

// ---------------------------------------------------------------------------

/** \brief Returns a crs::GeographicCRS from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

crs::GeographicCRSNNPtr
AuthorityFactory::createGeographicCRS(const std::string &code) const {
    auto crs(util::nn_dynamic_pointer_cast<crs::GeographicCRS>(
        createGeodeticCRS(code, true)));
    if (!crs) {
        throw NoSuchAuthorityCodeException("geographicCRS not found",
                                           d->authority(), code);
    }
    return NN_NO_CHECK(crs);
}

// ---------------------------------------------------------------------------

static crs::GeodeticCRSNNPtr
cloneWithProps(const crs::GeodeticCRSNNPtr &geodCRS,
               const util::PropertyMap &props) {
    auto cs = geodCRS->coordinateSystem();
    auto ellipsoidalCS = util::nn_dynamic_pointer_cast<cs::EllipsoidalCS>(cs);
    if (ellipsoidalCS) {
        return crs::GeographicCRS::create(props, geodCRS->datum(),
                                          geodCRS->datumEnsemble(),
                                          NN_NO_CHECK(ellipsoidalCS));
    }
    auto geocentricCS = util::nn_dynamic_pointer_cast<cs::CartesianCS>(cs);
    if (geocentricCS) {
        return crs::GeodeticCRS::create(props, geodCRS->datum(),
                                        geodCRS->datumEnsemble(),
                                        NN_NO_CHECK(geocentricCS));
    }
    return geodCRS;
}

// ---------------------------------------------------------------------------

crs::GeodeticCRSNNPtr
AuthorityFactory::createGeodeticCRS(const std::string &code,
                                    bool geographicOnly) const {
    const auto cacheKey(d->authority() + code);
    auto crs = d->context()->d->getCRSFromCache(cacheKey);
    if (crs) {
        auto geogCRS = std::dynamic_pointer_cast<crs::GeodeticCRS>(crs);
        if (geogCRS) {
            return NN_NO_CHECK(geogCRS);
        }
        throw NoSuchAuthorityCodeException("geodeticCRS not found",
                                           d->authority(), code);
    }
    std::string sql("SELECT name, type, coordinate_system_auth_name, "
                    "coordinate_system_code, datum_auth_name, datum_code, "
                    "text_definition, deprecated, description FROM "
                    "geodetic_crs WHERE auth_name = ? AND code = ?");
    if (geographicOnly) {
        sql += " AND type in (" GEOG_2D_SINGLE_QUOTED "," GEOG_3D_SINGLE_QUOTED
               ")";
    }
    auto res = d->runWithCodeParam(sql, code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("geodeticCRS not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &type = row[1];
        const auto &cs_auth_name = row[2];
        const auto &cs_code = row[3];
        const auto &datum_auth_name = row[4];
        const auto &datum_code = row[5];
        const auto &text_definition = row[6];
        const bool deprecated = row[7] == "1";
        const auto &remarks = row[8];

        auto props = d->createPropertiesSearchUsages("geodetic_crs", code, name,
                                                     deprecated, remarks);

        if (!text_definition.empty()) {
            DatabaseContext::Private::RecursionDetector detector(d->context());
            auto obj = createFromUserInput(
                pj_add_type_crs_if_needed(text_definition), d->context());
            auto geodCRS = util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(obj);
            if (geodCRS) {
                auto crsRet = cloneWithProps(NN_NO_CHECK(geodCRS), props);
                d->context()->d->cache(cacheKey, crsRet);
                return crsRet;
            }

            auto boundCRS = dynamic_cast<const crs::BoundCRS *>(obj.get());
            if (boundCRS) {
                geodCRS = util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(
                    boundCRS->baseCRS());
                if (geodCRS) {
                    auto newBoundCRS = crs::BoundCRS::create(
                        cloneWithProps(NN_NO_CHECK(geodCRS), props),
                        boundCRS->hubCRS(), boundCRS->transformation());
                    return NN_NO_CHECK(
                        util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(
                            newBoundCRS->baseCRSWithCanonicalBoundCRS()));
                }
            }

            throw FactoryException(
                "text_definition does not define a GeodeticCRS");
        }

        auto cs =
            d->createFactory(cs_auth_name)->createCoordinateSystem(cs_code);
        datum::GeodeticReferenceFramePtr datum;
        datum::DatumEnsemblePtr datumEnsemble;
        constexpr bool turnEnsembleAsDatum = false;
        d->createFactory(datum_auth_name)
            ->createGeodeticDatumOrEnsemble(datum_code, datum, datumEnsemble,
                                            turnEnsembleAsDatum);

        auto ellipsoidalCS =
            util::nn_dynamic_pointer_cast<cs::EllipsoidalCS>(cs);
        if ((type == GEOG_2D || type == GEOG_3D) && ellipsoidalCS) {
            auto crsRet = crs::GeographicCRS::create(
                props, datum, datumEnsemble, NN_NO_CHECK(ellipsoidalCS));
            d->context()->d->cache(cacheKey, crsRet);
            return crsRet;
        }

        auto geocentricCS = util::nn_dynamic_pointer_cast<cs::CartesianCS>(cs);
        if (type == GEOCENTRIC && geocentricCS) {
            auto crsRet = crs::GeodeticCRS::create(props, datum, datumEnsemble,
                                                   NN_NO_CHECK(geocentricCS));
            d->context()->d->cache(cacheKey, crsRet);
            return crsRet;
        }

        auto sphericalCS = util::nn_dynamic_pointer_cast<cs::SphericalCS>(cs);
        if (type == OTHER && sphericalCS) {
            auto crsRet = crs::GeodeticCRS::create(props, datum, datumEnsemble,
                                                   NN_NO_CHECK(sphericalCS));
            d->context()->d->cache(cacheKey, crsRet);
            return crsRet;
        }

        throw FactoryException("unsupported (type, CS type) for geodeticCRS: " +
                               type + ", " + cs->getWKT2Type(true));
    } catch (const std::exception &ex) {
        throw buildFactoryException("geodeticCRS", d->authority(), code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a crs::VerticalCRS from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

crs::VerticalCRSNNPtr
AuthorityFactory::createVerticalCRS(const std::string &code) const {
    const auto cacheKey(d->authority() + code);
    auto crs = d->context()->d->getCRSFromCache(cacheKey);
    if (crs) {
        auto projCRS = std::dynamic_pointer_cast<crs::VerticalCRS>(crs);
        if (projCRS) {
            return NN_NO_CHECK(projCRS);
        }
        throw NoSuchAuthorityCodeException("verticalCRS not found",
                                           d->authority(), code);
    }
    auto res = d->runWithCodeParam(
        "SELECT name, coordinate_system_auth_name, "
        "coordinate_system_code, datum_auth_name, datum_code, "
        "deprecated FROM "
        "vertical_crs WHERE auth_name = ? AND code = ?",
        code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("verticalCRS not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &cs_auth_name = row[1];
        const auto &cs_code = row[2];
        const auto &datum_auth_name = row[3];
        const auto &datum_code = row[4];
        const bool deprecated = row[5] == "1";
        auto cs =
            d->createFactory(cs_auth_name)->createCoordinateSystem(cs_code);
        datum::VerticalReferenceFramePtr datum;
        datum::DatumEnsemblePtr datumEnsemble;
        constexpr bool turnEnsembleAsDatum = false;
        d->createFactory(datum_auth_name)
            ->createVerticalDatumOrEnsemble(datum_code, datum, datumEnsemble,
                                            turnEnsembleAsDatum);
        auto props = d->createPropertiesSearchUsages("vertical_crs", code, name,
                                                     deprecated);

        auto verticalCS = util::nn_dynamic_pointer_cast<cs::VerticalCS>(cs);
        if (verticalCS) {
            auto crsRet = crs::VerticalCRS::create(props, datum, datumEnsemble,
                                                   NN_NO_CHECK(verticalCS));
            d->context()->d->cache(cacheKey, crsRet);
            return crsRet;
        }
        throw FactoryException("unsupported CS type for verticalCRS: " +
                               cs->getWKT2Type(true));
    } catch (const std::exception &ex) {
        throw buildFactoryException("verticalCRS", d->authority(), code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a crs::EngineeringCRS from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 * @since 9.6
 */

crs::EngineeringCRSNNPtr
AuthorityFactory::createEngineeringCRS(const std::string &code) const {
    const auto cacheKey(d->authority() + code);
    auto crs = d->context()->d->getCRSFromCache(cacheKey);
    if (crs) {
        auto engCRS = std::dynamic_pointer_cast<crs::EngineeringCRS>(crs);
        if (engCRS) {
            return NN_NO_CHECK(engCRS);
        }
        throw NoSuchAuthorityCodeException("engineeringCRS not found",
                                           d->authority(), code);
    }
    auto res = d->runWithCodeParam(
        "SELECT name, coordinate_system_auth_name, "
        "coordinate_system_code, datum_auth_name, datum_code, "
        "deprecated FROM "
        "engineering_crs WHERE auth_name = ? AND code = ?",
        code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("engineeringCRS not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &cs_auth_name = row[1];
        const auto &cs_code = row[2];
        const auto &datum_auth_name = row[3];
        const auto &datum_code = row[4];
        const bool deprecated = row[5] == "1";
        auto cs =
            d->createFactory(cs_auth_name)->createCoordinateSystem(cs_code);
        auto datum = d->createFactory(datum_auth_name)
                         ->createEngineeringDatum(datum_code);
        auto props = d->createPropertiesSearchUsages("engineering_crs", code,
                                                     name, deprecated);
        auto crsRet = crs::EngineeringCRS::create(props, datum, cs);
        d->context()->d->cache(cacheKey, crsRet);
        return crsRet;
    } catch (const std::exception &ex) {
        throw buildFactoryException("engineeringCRS", d->authority(), code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a operation::Conversion from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

operation::ConversionNNPtr
AuthorityFactory::createConversion(const std::string &code) const {

    static const char *sql =
        "SELECT name, description, "
        "method_auth_name, method_code, method_name, "

        "param1_auth_name, param1_code, param1_name, param1_value, "
        "param1_uom_auth_name, param1_uom_code, "

        "param2_auth_name, param2_code, param2_name, param2_value, "
        "param2_uom_auth_name, param2_uom_code, "

        "param3_auth_name, param3_code, param3_name, param3_value, "
        "param3_uom_auth_name, param3_uom_code, "

        "param4_auth_name, param4_code, param4_name, param4_value, "
        "param4_uom_auth_name, param4_uom_code, "

        "param5_auth_name, param5_code, param5_name, param5_value, "
        "param5_uom_auth_name, param5_uom_code, "

        "param6_auth_name, param6_code, param6_name, param6_value, "
        "param6_uom_auth_name, param6_uom_code, "

        "param7_auth_name, param7_code, param7_name, param7_value, "
        "param7_uom_auth_name, param7_uom_code, "

        "deprecated FROM conversion WHERE auth_name = ? AND code = ?";

    auto res = d->runWithCodeParam(sql, code);
    if (res.empty()) {
        try {
            // Conversions using methods Change of Vertical Unit or
            // Height Depth Reversal are stored in other_transformation
            auto op = createCoordinateOperation(
                code, false /* allowConcatenated */,
                false /* usePROJAlternativeGridNames */,
                "other_transformation");
            auto conv =
                util::nn_dynamic_pointer_cast<operation::Conversion>(op);
            if (conv) {
                return NN_NO_CHECK(conv);
            }
        } catch (const std::exception &) {
        }
        throw NoSuchAuthorityCodeException("conversion not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        size_t idx = 0;
        const auto &name = row[idx++];
        const auto &description = row[idx++];
        const auto &method_auth_name = row[idx++];
        const auto &method_code = row[idx++];
        const auto &method_name = row[idx++];
        const size_t base_param_idx = idx;
        std::vector<operation::OperationParameterNNPtr> parameters;
        std::vector<operation::ParameterValueNNPtr> values;
        for (size_t i = 0; i < N_MAX_PARAMS; ++i) {
            const auto &param_auth_name = row[base_param_idx + i * 6 + 0];
            if (param_auth_name.empty()) {
                break;
            }
            const auto &param_code = row[base_param_idx + i * 6 + 1];
            const auto &param_name = row[base_param_idx + i * 6 + 2];
            const auto &param_value = row[base_param_idx + i * 6 + 3];
            const auto &param_uom_auth_name = row[base_param_idx + i * 6 + 4];
            const auto &param_uom_code = row[base_param_idx + i * 6 + 5];
            parameters.emplace_back(operation::OperationParameter::create(
                util::PropertyMap()
                    .set(metadata::Identifier::CODESPACE_KEY, param_auth_name)
                    .set(metadata::Identifier::CODE_KEY, param_code)
                    .set(common::IdentifiedObject::NAME_KEY, param_name)));
            std::string normalized_uom_code(param_uom_code);
            const double normalized_value = normalizeMeasure(
                param_uom_code, param_value, normalized_uom_code);
            auto uom = d->createUnitOfMeasure(param_uom_auth_name,
                                              normalized_uom_code);
            values.emplace_back(operation::ParameterValue::create(
                common::Measure(normalized_value, uom)));
        }
        const bool deprecated = row[base_param_idx + N_MAX_PARAMS * 6] == "1";

        auto propConversion = d->createPropertiesSearchUsages(
            "conversion", code, name, deprecated);
        if (!description.empty())
            propConversion.set(common::IdentifiedObject::REMARKS_KEY,
                               description);

        auto propMethod = util::PropertyMap().set(
            common::IdentifiedObject::NAME_KEY, method_name);
        if (!method_auth_name.empty()) {
            propMethod
                .set(metadata::Identifier::CODESPACE_KEY, method_auth_name)
                .set(metadata::Identifier::CODE_KEY, method_code);
        }

        return operation::Conversion::create(propConversion, propMethod,
                                             parameters, values);
    } catch (const std::exception &ex) {
        throw buildFactoryException("conversion", d->authority(), code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a crs::ProjectedCRS from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

crs::ProjectedCRSNNPtr
AuthorityFactory::createProjectedCRS(const std::string &code) const {
    const auto cacheKey(d->authority() + code);
    auto crs = d->context()->d->getCRSFromCache(cacheKey);
    if (crs) {
        auto projCRS = std::dynamic_pointer_cast<crs::ProjectedCRS>(crs);
        if (projCRS) {
            return NN_NO_CHECK(projCRS);
        }
        throw NoSuchAuthorityCodeException("projectedCRS not found",
                                           d->authority(), code);
    }
    return d->createProjectedCRSEnd(code, d->createProjectedCRSBegin(code));
}

// ---------------------------------------------------------------------------
//! @cond Doxygen_Suppress

/** Returns the result of the SQL query needed by createProjectedCRSEnd
 *
 * The split in two functions is for createFromCoordinateReferenceSystemCodes()
 * convenience, to avoid throwing exceptions.
 */
SQLResultSet
AuthorityFactory::Private::createProjectedCRSBegin(const std::string &code) {
    return runWithCodeParam(
        "SELECT name, coordinate_system_auth_name, "
        "coordinate_system_code, geodetic_crs_auth_name, geodetic_crs_code, "
        "conversion_auth_name, conversion_code, "
        "text_definition, "
        "deprecated FROM projected_crs WHERE auth_name = ? AND code = ?",
        code);
}

// ---------------------------------------------------------------------------

/** Build a ProjectedCRS from the result of createProjectedCRSBegin() */
crs::ProjectedCRSNNPtr
AuthorityFactory::Private::createProjectedCRSEnd(const std::string &code,
                                                 const SQLResultSet &res) {
    const auto cacheKey(authority() + code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("projectedCRS not found",
                                           authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &cs_auth_name = row[1];
        const auto &cs_code = row[2];
        const auto &geodetic_crs_auth_name = row[3];
        const auto &geodetic_crs_code = row[4];
        const auto &conversion_auth_name = row[5];
        const auto &conversion_code = row[6];
        const auto &text_definition = row[7];
        const bool deprecated = row[8] == "1";

        auto props = createPropertiesSearchUsages("projected_crs", code, name,
                                                  deprecated);

        if (!text_definition.empty()) {
            DatabaseContext::Private::RecursionDetector detector(context());
            auto obj = createFromUserInput(
                pj_add_type_crs_if_needed(text_definition), context());
            auto projCRS = dynamic_cast<const crs::ProjectedCRS *>(obj.get());
            if (projCRS) {
                auto conv = projCRS->derivingConversion();
                auto newConv =
                    (conv->nameStr() == "unnamed")
                        ? operation::Conversion::create(
                              util::PropertyMap().set(
                                  common::IdentifiedObject::NAME_KEY, name),
                              conv->method(), conv->parameterValues())
                        : std::move(conv);
                auto crsRet = crs::ProjectedCRS::create(
                    props, projCRS->baseCRS(), newConv,
                    projCRS->coordinateSystem());
                context()->d->cache(cacheKey, crsRet);
                return crsRet;
            }

            auto boundCRS = dynamic_cast<const crs::BoundCRS *>(obj.get());
            if (boundCRS) {
                projCRS = dynamic_cast<const crs::ProjectedCRS *>(
                    boundCRS->baseCRS().get());
                if (projCRS) {
                    auto newBoundCRS = crs::BoundCRS::create(
                        crs::ProjectedCRS::create(props, projCRS->baseCRS(),
                                                  projCRS->derivingConversion(),
                                                  projCRS->coordinateSystem()),
                        boundCRS->hubCRS(), boundCRS->transformation());
                    return NN_NO_CHECK(
                        util::nn_dynamic_pointer_cast<crs::ProjectedCRS>(
                            newBoundCRS->baseCRSWithCanonicalBoundCRS()));
                }
            }

            throw FactoryException(
                "text_definition does not define a ProjectedCRS");
        }

        auto cs = createFactory(cs_auth_name)->createCoordinateSystem(cs_code);

        auto baseCRS = createFactory(geodetic_crs_auth_name)
                           ->createGeodeticCRS(geodetic_crs_code);

        auto conv = createFactory(conversion_auth_name)
                        ->createConversion(conversion_code);
        if (conv->nameStr() == "unnamed") {
            conv = conv->shallowClone();
            conv->setProperties(util::PropertyMap().set(
                common::IdentifiedObject::NAME_KEY, name));
        }

        auto cartesianCS = util::nn_dynamic_pointer_cast<cs::CartesianCS>(cs);
        if (cartesianCS) {
            auto crsRet = crs::ProjectedCRS::create(props, baseCRS, conv,
                                                    NN_NO_CHECK(cartesianCS));
            context()->d->cache(cacheKey, crsRet);
            return crsRet;
        }
        throw FactoryException("unsupported CS type for projectedCRS: " +
                               cs->getWKT2Type(true));
    } catch (const std::exception &ex) {
        throw buildFactoryException("projectedCRS", authority(), code, ex);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns a crs::CompoundCRS from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

crs::CompoundCRSNNPtr
AuthorityFactory::createCompoundCRS(const std::string &code) const {
    auto res =
        d->runWithCodeParam("SELECT name, horiz_crs_auth_name, horiz_crs_code, "
                            "vertical_crs_auth_name, vertical_crs_code, "
                            "deprecated FROM "
                            "compound_crs WHERE auth_name = ? AND code = ?",
                            code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("compoundCRS not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &name = row[0];
        const auto &horiz_crs_auth_name = row[1];
        const auto &horiz_crs_code = row[2];
        const auto &vertical_crs_auth_name = row[3];
        const auto &vertical_crs_code = row[4];
        const bool deprecated = row[5] == "1";

        auto horizCRS =
            d->createFactory(horiz_crs_auth_name)
                ->createCoordinateReferenceSystem(horiz_crs_code, false);
        auto vertCRS = d->createFactory(vertical_crs_auth_name)
                           ->createVerticalCRS(vertical_crs_code);

        auto props = d->createPropertiesSearchUsages("compound_crs", code, name,
                                                     deprecated);
        return crs::CompoundCRS::create(
            props, std::vector<crs::CRSNNPtr>{std::move(horizCRS),
                                              std::move(vertCRS)});
    } catch (const std::exception &ex) {
        throw buildFactoryException("compoundCRS", d->authority(), code, ex);
    }
}

// ---------------------------------------------------------------------------

/** \brief Returns a crs::CRS from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

crs::CRSNNPtr AuthorityFactory::createCoordinateReferenceSystem(
    const std::string &code) const {
    return createCoordinateReferenceSystem(code, true);
}

//! @cond Doxygen_Suppress

crs::CRSNNPtr
AuthorityFactory::createCoordinateReferenceSystem(const std::string &code,
                                                  bool allowCompound) const {
    const auto cacheKey(d->authority() + code);
    auto crs = d->context()->d->getCRSFromCache(cacheKey);
    if (crs) {
        return NN_NO_CHECK(crs);
    }

    if (d->authority() == metadata::Identifier::OGC) {
        if (code == "AnsiDate") {
            // Derived from http://www.opengis.net/def/crs/OGC/0/AnsiDate
            return crs::TemporalCRS::create(
                util::PropertyMap()
                    // above URL indicates Julian Date" as name... likely wrong
                    .set(common::IdentifiedObject::NAME_KEY, "Ansi Date")
                    .set(metadata::Identifier::CODESPACE_KEY, d->authority())
                    .set(metadata::Identifier::CODE_KEY, code),
                datum::TemporalDatum::create(
                    util::PropertyMap().set(
                        common::IdentifiedObject::NAME_KEY,
                        "Epoch time for the ANSI date (1-Jan-1601, 00h00 UTC) "
                        "as day 1."),
                    common::DateTime::create("1600-12-31T00:00:00Z"),
                    datum::TemporalDatum::CALENDAR_PROLEPTIC_GREGORIAN),
                cs::TemporalCountCS::create(
                    util::PropertyMap(),
                    cs::CoordinateSystemAxis::create(
                        util::PropertyMap().set(
                            common::IdentifiedObject::NAME_KEY, "Time"),
                        "T", cs::AxisDirection::FUTURE,
                        common::UnitOfMeasure("day", 0,
                                              UnitOfMeasure::Type::TIME))));
        }
        if (code == "JulianDate") {
            // Derived from http://www.opengis.net/def/crs/OGC/0/JulianDate
            return crs::TemporalCRS::create(
                util::PropertyMap()
                    .set(common::IdentifiedObject::NAME_KEY, "Julian Date")
                    .set(metadata::Identifier::CODESPACE_KEY, d->authority())
                    .set(metadata::Identifier::CODE_KEY, code),
                datum::TemporalDatum::create(
                    util::PropertyMap().set(
                        common::IdentifiedObject::NAME_KEY,
                        "The beginning of the Julian period."),
                    common::DateTime::create("-4714-11-24T12:00:00Z"),
                    datum::TemporalDatum::CALENDAR_PROLEPTIC_GREGORIAN),
                cs::TemporalCountCS::create(
                    util::PropertyMap(),
                    cs::CoordinateSystemAxis::create(
                        util::PropertyMap().set(
                            common::IdentifiedObject::NAME_KEY, "Time"),
                        "T", cs::AxisDirection::FUTURE,
                        common::UnitOfMeasure("day", 0,
                                              UnitOfMeasure::Type::TIME))));
        }
        if (code == "UnixTime") {
            // Derived from http://www.opengis.net/def/crs/OGC/0/UnixTime
            return crs::TemporalCRS::create(
                util::PropertyMap()
                    .set(common::IdentifiedObject::NAME_KEY, "Unix Time")
                    .set(metadata::Identifier::CODESPACE_KEY, d->authority())
                    .set(metadata::Identifier::CODE_KEY, code),
                datum::TemporalDatum::create(
                    util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                            "Unix epoch"),
                    common::DateTime::create("1970-01-01T00:00:00Z"),
                    datum::TemporalDatum::CALENDAR_PROLEPTIC_GREGORIAN),
                cs::TemporalCountCS::create(
                    util::PropertyMap(),
                    cs::CoordinateSystemAxis::create(
                        util::PropertyMap().set(
                            common::IdentifiedObject::NAME_KEY, "Time"),
                        "T", cs::AxisDirection::FUTURE,
                        common::UnitOfMeasure::SECOND)));
        }
        if (code == "84") {
            return createCoordinateReferenceSystem("CRS84", false);
        }
    }

    auto res = d->runWithCodeParam(
        "SELECT type FROM crs_view WHERE auth_name = ? AND code = ?", code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("crs not found", d->authority(),
                                           code);
    }
    const auto &type = res.front()[0];
    if (type == GEOG_2D || type == GEOG_3D || type == GEOCENTRIC ||
        type == OTHER) {
        return createGeodeticCRS(code);
    }
    if (type == VERTICAL) {
        return createVerticalCRS(code);
    }
    if (type == PROJECTED) {
        return createProjectedCRS(code);
    }
    if (type == ENGINEERING) {
        return createEngineeringCRS(code);
    }
    if (allowCompound && type == COMPOUND) {
        return createCompoundCRS(code);
    }
    throw FactoryException("unhandled CRS type: " + type);
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns a coordinates::CoordinateMetadata from the specified code.
 *
 * @param code Object code allocated by authority.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 * @since 9.4
 */

coordinates::CoordinateMetadataNNPtr
AuthorityFactory::createCoordinateMetadata(const std::string &code) const {
    auto res = d->runWithCodeParam(
        "SELECT crs_auth_name, crs_code, crs_text_definition, coordinate_epoch "
        "FROM coordinate_metadata WHERE auth_name = ? AND code = ?",
        code);
    if (res.empty()) {
        throw NoSuchAuthorityCodeException("coordinate_metadata not found",
                                           d->authority(), code);
    }
    try {
        const auto &row = res.front();
        const auto &crs_auth_name = row[0];
        const auto &crs_code = row[1];
        const auto &crs_text_definition = row[2];
        const auto &coordinate_epoch = row[3];

        auto l_context = d->context();
        DatabaseContext::Private::RecursionDetector detector(l_context);
        auto crs =
            !crs_auth_name.empty()
                ? d->createFactory(crs_auth_name)
                      ->createCoordinateReferenceSystem(crs_code)
                      .as_nullable()
                : util::nn_dynamic_pointer_cast<crs::CRS>(
                      createFromUserInput(crs_text_definition, l_context));
        if (!crs) {
            throw FactoryException(
                std::string("cannot build CoordinateMetadata ") +
                d->authority() + ":" + code + ": cannot build CRS");
        }
        if (coordinate_epoch.empty()) {
            return coordinates::CoordinateMetadata::create(NN_NO_CHECK(crs));
        } else {
            return coordinates::CoordinateMetadata::create(
                NN_NO_CHECK(crs), c_locale_stod(coordinate_epoch),
                l_context.as_nullable());
        }
    } catch (const std::exception &ex) {
        throw buildFactoryException("CoordinateMetadata", d->authority(), code,
                                    ex);
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static util::PropertyMap createMapNameEPSGCode(const std::string &name,
                                               int code) {
    return util::PropertyMap()
        .set(common::IdentifiedObject::NAME_KEY, name)
        .set(metadata::Identifier::CODESPACE_KEY, metadata::Identifier::EPSG)
        .set(metadata::Identifier::CODE_KEY, code);
}

// ---------------------------------------------------------------------------

static operation::OperationParameterNNPtr createOpParamNameEPSGCode(int code) {
    const char *name = operation::OperationParameter::getNameForEPSGCode(code);
    assert(name);
    return operation::OperationParameter::create(
        createMapNameEPSGCode(name, code));
}

static operation::ParameterValueNNPtr createLength(const std::string &value,
                                                   const UnitOfMeasure &uom) {
    return operation::ParameterValue::create(
        common::Length(c_locale_stod(value), uom));
}

static operation::ParameterValueNNPtr createAngle(const std::string &value,
                                                  const UnitOfMeasure &uom) {
    return operation::ParameterValue::create(
        common::Angle(c_locale_stod(value), uom));
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns a operation::CoordinateOperation from the specified code.
 *
 * @param code Object code allocated by authority.
 * @param usePROJAlternativeGridNames Whether PROJ alternative grid names
 * should be substituted to the official grid names.
 * @return object.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

operation::CoordinateOperationNNPtr AuthorityFactory::createCoordinateOperation(
    const std::string &code, bool usePROJAlternativeGridNames) const {
    return createCoordinateOperation(code, true, usePROJAlternativeGridNames,
                                     std::string());
}

operation::CoordinateOperationNNPtr AuthorityFactory::createCoordinateOperation(
    const std::string &code, bool allowConcatenated,
    bool usePROJAlternativeGridNames, const std::string &typeIn) const {
    std::string type(typeIn);
    if (type.empty()) {
        auto res = d->runWithCodeParam(
            "SELECT type FROM coordinate_operation_with_conversion_view "
            "WHERE auth_name = ? AND code = ?",
            code);
        if (res.empty()) {
            throw NoSuchAuthorityCodeException("coordinate operation not found",
                                               d->authority(), code);
        }
        type = res.front()[0];
    }

    if (type == "conversion") {
        return createConversion(code);
    }

    if (type == "helmert_transformation") {

        auto res = d->runWithCodeParam(
            "SELECT name, description, "
            "method_auth_name, method_code, method_name, "
            "source_crs_auth_name, source_crs_code, target_crs_auth_name, "
            "target_crs_code, "
            "accuracy, tx, ty, tz, translation_uom_auth_name, "
            "translation_uom_code, rx, ry, rz, rotation_uom_auth_name, "
            "rotation_uom_code, scale_difference, "
            "scale_difference_uom_auth_name, scale_difference_uom_code, "
            "rate_tx, rate_ty, rate_tz, rate_translation_uom_auth_name, "
            "rate_translation_uom_code, rate_rx, rate_ry, rate_rz, "
            "rate_rotation_uom_auth_name, rate_rotation_uom_code, "
            "rate_scale_difference, rate_scale_difference_uom_auth_name, "
            "rate_scale_difference_uom_code, epoch, epoch_uom_auth_name, "
            "epoch_uom_code, px, py, pz, pivot_uom_auth_name, pivot_uom_code, "
            "operation_version, deprecated FROM "
            "helmert_transformation WHERE auth_name = ? AND code = ?",
            code);
        if (res.empty()) {
            // shouldn't happen if foreign keys are OK
            throw NoSuchAuthorityCodeException(
                "helmert_transformation not found", d->authority(), code);
        }
        try {
            const auto &row = res.front();
            size_t idx = 0;
            const auto &name = row[idx++];
            const auto &description = row[idx++];
            const auto &method_auth_name = row[idx++];
            const auto &method_code = row[idx++];
            const auto &method_name = row[idx++];
            const auto &source_crs_auth_name = row[idx++];
            const auto &source_crs_code = row[idx++];
            const auto &target_crs_auth_name = row[idx++];
            const auto &target_crs_code = row[idx++];
            const auto &accuracy = row[idx++];

            const auto &tx = row[idx++];
            const auto &ty = row[idx++];
            const auto &tz = row[idx++];
            const auto &translation_uom_auth_name = row[idx++];
            const auto &translation_uom_code = row[idx++];
            const auto &rx = row[idx++];
            const auto &ry = row[idx++];
            const auto &rz = row[idx++];
            const auto &rotation_uom_auth_name = row[idx++];
            const auto &rotation_uom_code = row[idx++];
            const auto &scale_difference = row[idx++];
            const auto &scale_difference_uom_auth_name = row[idx++];
            const auto &scale_difference_uom_code = row[idx++];

            const auto &rate_tx = row[idx++];
            const auto &rate_ty = row[idx++];
            const auto &rate_tz = row[idx++];
            const auto &rate_translation_uom_auth_name = row[idx++];
            const auto &rate_translation_uom_code = row[idx++];
            const auto &rate_rx = row[idx++];
            const auto &rate_ry = row[idx++];
            const auto &rate_rz = row[idx++];
            const auto &rate_rotation_uom_auth_name = row[idx++];
            const auto &rate_rotation_uom_code = row[idx++];
            const auto &rate_scale_difference = row[idx++];
            const auto &rate_scale_difference_uom_auth_name = row[idx++];
            const auto &rate_scale_difference_uom_code = row[idx++];

            const auto &epoch = row[idx++];
            const auto &epoch_uom_auth_name = row[idx++];
            const auto &epoch_uom_code = row[idx++];

            const auto &px = row[idx++];
            const auto &py = row[idx++];
            const auto &pz = row[idx++];
            const auto &pivot_uom_auth_name = row[idx++];
            const auto &pivot_uom_code = row[idx++];

            const auto &operation_version = row[idx++];
            const auto &deprecated_str = row[idx++];
            const bool deprecated = deprecated_str == "1";
            assert(idx == row.size());

            auto uom_translation = d->createUnitOfMeasure(
                translation_uom_auth_name, translation_uom_code);

            auto uom_epoch = epoch_uom_auth_name.empty()
                                 ? common::UnitOfMeasure::NONE
                                 : d->createUnitOfMeasure(epoch_uom_auth_name,
                                                          epoch_uom_code);

            auto sourceCRS =
                d->createFactory(source_crs_auth_name)
                    ->createCoordinateReferenceSystem(source_crs_code);
            auto targetCRS =
                d->createFactory(target_crs_auth_name)
                    ->createCoordinateReferenceSystem(target_crs_code);

            std::vector<operation::OperationParameterNNPtr> parameters;
            std::vector<operation::ParameterValueNNPtr> values;

            parameters.emplace_back(createOpParamNameEPSGCode(
                EPSG_CODE_PARAMETER_X_AXIS_TRANSLATION));
            values.emplace_back(createLength(tx, uom_translation));

            parameters.emplace_back(createOpParamNameEPSGCode(
                EPSG_CODE_PARAMETER_Y_AXIS_TRANSLATION));
            values.emplace_back(createLength(ty, uom_translation));

            parameters.emplace_back(createOpParamNameEPSGCode(
                EPSG_CODE_PARAMETER_Z_AXIS_TRANSLATION));
            values.emplace_back(createLength(tz, uom_translation));

            if (!rx.empty()) {
                // Helmert 7-, 8-, 10- or 15- parameter cases
                auto uom_rotation = d->createUnitOfMeasure(
                    rotation_uom_auth_name, rotation_uom_code);

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_X_AXIS_ROTATION));
                values.emplace_back(createAngle(rx, uom_rotation));

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_Y_AXIS_ROTATION));
                values.emplace_back(createAngle(ry, uom_rotation));

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_Z_AXIS_ROTATION));
                values.emplace_back(createAngle(rz, uom_rotation));

                auto uom_scale_difference =
                    scale_difference_uom_auth_name.empty()
                        ? common::UnitOfMeasure::NONE
                        : d->createUnitOfMeasure(scale_difference_uom_auth_name,
                                                 scale_difference_uom_code);

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_SCALE_DIFFERENCE));
                values.emplace_back(operation::ParameterValue::create(
                    common::Scale(c_locale_stod(scale_difference),
                                  uom_scale_difference)));
            }

            if (!rate_tx.empty()) {
                // Helmert 15-parameter

                auto uom_rate_translation = d->createUnitOfMeasure(
                    rate_translation_uom_auth_name, rate_translation_uom_code);

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_RATE_X_AXIS_TRANSLATION));
                values.emplace_back(
                    createLength(rate_tx, uom_rate_translation));

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_RATE_Y_AXIS_TRANSLATION));
                values.emplace_back(
                    createLength(rate_ty, uom_rate_translation));

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_RATE_Z_AXIS_TRANSLATION));
                values.emplace_back(
                    createLength(rate_tz, uom_rate_translation));

                auto uom_rate_rotation = d->createUnitOfMeasure(
                    rate_rotation_uom_auth_name, rate_rotation_uom_code);

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_RATE_X_AXIS_ROTATION));
                values.emplace_back(createAngle(rate_rx, uom_rate_rotation));

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_RATE_Y_AXIS_ROTATION));
                values.emplace_back(createAngle(rate_ry, uom_rate_rotation));

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_RATE_Z_AXIS_ROTATION));
                values.emplace_back(createAngle(rate_rz, uom_rate_rotation));

                auto uom_rate_scale_difference =
                    d->createUnitOfMeasure(rate_scale_difference_uom_auth_name,
                                           rate_scale_difference_uom_code);
                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_RATE_SCALE_DIFFERENCE));
                values.emplace_back(operation::ParameterValue::create(
                    common::Scale(c_locale_stod(rate_scale_difference),
                                  uom_rate_scale_difference)));

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_REFERENCE_EPOCH));
                values.emplace_back(operation::ParameterValue::create(
                    common::Measure(c_locale_stod(epoch), uom_epoch)));
            } else if (uom_epoch != common::UnitOfMeasure::NONE) {
                // Helmert 8-parameter
                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_TRANSFORMATION_REFERENCE_EPOCH));
                values.emplace_back(operation::ParameterValue::create(
                    common::Measure(c_locale_stod(epoch), uom_epoch)));
            } else if (!px.empty()) {
                // Molodensky-Badekas case
                auto uom_pivot =
                    d->createUnitOfMeasure(pivot_uom_auth_name, pivot_uom_code);

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_ORDINATE_1_EVAL_POINT));
                values.emplace_back(createLength(px, uom_pivot));

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_ORDINATE_2_EVAL_POINT));
                values.emplace_back(createLength(py, uom_pivot));

                parameters.emplace_back(createOpParamNameEPSGCode(
                    EPSG_CODE_PARAMETER_ORDINATE_3_EVAL_POINT));
                values.emplace_back(createLength(pz, uom_pivot));
            }

            auto props = d->createPropertiesSearchUsages(
                type, code, name, deprecated, description);
            if (!operation_version.empty()) {
                props.set(operation::CoordinateOperation::OPERATION_VERSION_KEY,
                          operation_version);
            }

            auto propsMethod =
                util::PropertyMap()
                    .set(metadata::Identifier::CODESPACE_KEY, method_auth_name)
                    .set(metadata::Identifier::CODE_KEY, method_code)
                    .set(common::IdentifiedObject::NAME_KEY, method_name);

            std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
            if (!accuracy.empty() && accuracy != "999.0") {
                accuracies.emplace_back(
                    metadata::PositionalAccuracy::create(accuracy));
            }
            return operation::Transformation::create(
                props, sourceCRS, targetCRS, nullptr, propsMethod, parameters,
                values, accuracies);

        } catch (const std::exception &ex) {
            throw buildFactoryException("transformation", d->authority(), code,
                                        ex);
        }
    }

    if (type == "grid_transformation") {
        auto res = d->runWithCodeParam(
            "SELECT name, description, "
            "method_auth_name, method_code, method_name, "
            "source_crs_auth_name, source_crs_code, target_crs_auth_name, "
            "target_crs_code, "
            "accuracy, grid_param_auth_name, grid_param_code, grid_param_name, "
            "grid_name, "
            "grid2_param_auth_name, grid2_param_code, grid2_param_name, "
            "grid2_name, "
            "param1_auth_name, param1_code, param1_name, param1_value, "
            "param1_uom_auth_name, param1_uom_code, "
            "param2_auth_name, param2_code, param2_name, param2_value, "
            "param2_uom_auth_name, param2_uom_code, "
            "interpolation_crs_auth_name, interpolation_crs_code, "
            "operation_version, deprecated FROM "
            "grid_transformation WHERE auth_name = ? AND code = ?",
            code);
        if (res.empty()) {
            // shouldn't happen if foreign keys are OK
            throw NoSuchAuthorityCodeException("grid_transformation not found",
                                               d->authority(), code);
        }
        try {
            const auto &row = res.front();
            size_t idx = 0;
            const auto &name = row[idx++];
            const auto &description = row[idx++];
            const auto &method_auth_name = row[idx++];
            const auto &method_code = row[idx++];
            const auto &method_name = row[idx++];
            const auto &source_crs_auth_name = row[idx++];
            const auto &source_crs_code = row[idx++];
            const auto &target_crs_auth_name = row[idx++];
            const auto &target_crs_code = row[idx++];
            const auto &accuracy = row[idx++];
            const auto &grid_param_auth_name = row[idx++];
            const auto &grid_param_code = row[idx++];
            const auto &grid_param_name = row[idx++];
            const auto &grid_name = row[idx++];
            const auto &grid2_param_auth_name = row[idx++];
            const auto &grid2_param_code = row[idx++];
            const auto &grid2_param_name = row[idx++];
            const auto &grid2_name = row[idx++];
            std::vector<operation::OperationParameterNNPtr> parameters;
            std::vector<operation::ParameterValueNNPtr> values;

            parameters.emplace_back(operation::OperationParameter::create(
                util::PropertyMap()
                    .set(common::IdentifiedObject::NAME_KEY, grid_param_name)
                    .set(metadata::Identifier::CODESPACE_KEY,
                         grid_param_auth_name)
                    .set(metadata::Identifier::CODE_KEY, grid_param_code)));
            values.emplace_back(
                operation::ParameterValue::createFilename(grid_name));
            if (!grid2_name.empty()) {
                parameters.emplace_back(operation::OperationParameter::create(
                    util::PropertyMap()
                        .set(common::IdentifiedObject::NAME_KEY,
                             grid2_param_name)
                        .set(metadata::Identifier::CODESPACE_KEY,
                             grid2_param_auth_name)
                        .set(metadata::Identifier::CODE_KEY,
                             grid2_param_code)));
                values.emplace_back(
                    operation::ParameterValue::createFilename(grid2_name));
            }

            const size_t base_param_idx = idx;
            constexpr size_t N_MAX_PARAMS_GRID_TRANSFORMATION = 2;
            for (size_t i = 0; i < N_MAX_PARAMS_GRID_TRANSFORMATION; ++i) {
                const auto &param_auth_name = row[base_param_idx + i * 6 + 0];
                if (param_auth_name.empty()) {
                    break;
                }
                const auto &param_code = row[base_param_idx + i * 6 + 1];
                const auto &param_name = row[base_param_idx + i * 6 + 2];
                const auto &param_value = row[base_param_idx + i * 6 + 3];
                const auto &param_uom_auth_name =
                    row[base_param_idx + i * 6 + 4];
                const auto &param_uom_code = row[base_param_idx + i * 6 + 5];
                parameters.emplace_back(operation::OperationParameter::create(
                    util::PropertyMap()
                        .set(metadata::Identifier::CODESPACE_KEY,
                             param_auth_name)
                        .set(metadata::Identifier::CODE_KEY, param_code)
                        .set(common::IdentifiedObject::NAME_KEY, param_name)));
                std::string normalized_uom_code(param_uom_code);
                const double normalized_value = normalizeMeasure(
                    param_uom_code, param_value, normalized_uom_code);
                auto uom = d->createUnitOfMeasure(param_uom_auth_name,
                                                  normalized_uom_code);
                values.emplace_back(operation::ParameterValue::create(
                    common::Measure(normalized_value, uom)));
            }
            idx = base_param_idx + 6 * N_MAX_PARAMS_GRID_TRANSFORMATION;

            const auto &interpolation_crs_auth_name = row[idx++];
            const auto &interpolation_crs_code = row[idx++];
            const auto &operation_version = row[idx++];
            const auto &deprecated_str = row[idx++];
            const bool deprecated = deprecated_str == "1";
            assert(idx == row.size());

            auto sourceCRS =
                d->createFactory(source_crs_auth_name)
                    ->createCoordinateReferenceSystem(source_crs_code);
            auto targetCRS =
                d->createFactory(target_crs_auth_name)
                    ->createCoordinateReferenceSystem(target_crs_code);
            auto interpolationCRS =
                interpolation_crs_auth_name.empty()
                    ? nullptr
                    : d->createFactory(interpolation_crs_auth_name)
                          ->createCoordinateReferenceSystem(
                              interpolation_crs_code)
                          .as_nullable();

            auto props = d->createPropertiesSearchUsages(
                type, code, name, deprecated, description);
            if (!operation_version.empty()) {
                props.set(operation::CoordinateOperation::OPERATION_VERSION_KEY,
                          operation_version);
            }
            auto propsMethod =
                util::PropertyMap()
                    .set(metadata::Identifier::CODESPACE_KEY, method_auth_name)
                    .set(metadata::Identifier::CODE_KEY, method_code)
                    .set(common::IdentifiedObject::NAME_KEY, method_name);

            std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
            if (!accuracy.empty() && accuracy != "999.0") {
                accuracies.emplace_back(
                    metadata::PositionalAccuracy::create(accuracy));
            }

            // A bit fragile to detect the operation type with the method name,
            // but not worth changing the database model
            if (starts_with(method_name, "Point motion")) {
                if (!sourceCRS->isEquivalentTo(targetCRS.get())) {
                    throw operation::InvalidOperation(
                        "source_crs and target_crs should be the same for a "
                        "PointMotionOperation");
                }

                auto pmo = operation::PointMotionOperation::create(
                    props, sourceCRS, propsMethod, parameters, values,
                    accuracies);
                if (usePROJAlternativeGridNames) {
                    return pmo->substitutePROJAlternativeGridNames(
                        d->context());
                }
                return pmo;
            }

            auto transf = operation::Transformation::create(
                props, sourceCRS, targetCRS, interpolationCRS, propsMethod,
                parameters, values, accuracies);
            if (usePROJAlternativeGridNames) {
                return transf->substitutePROJAlternativeGridNames(d->context());
            }
            return transf;

        } catch (const std::exception &ex) {
            throw buildFactoryException("transformation", d->authority(), code,
                                        ex);
        }
    }

    if (type == "other_transformation") {
        std::ostringstream buffer;
        buffer.imbue(std::locale::classic());
        buffer
            << "SELECT name, description, "
               "method_auth_name, method_code, method_name, "
               "source_crs_auth_name, source_crs_code, target_crs_auth_name, "
               "target_crs_code, "
               "grid_param_auth_name, grid_param_code, grid_param_name, "
               "grid_name, "
               "interpolation_crs_auth_name, interpolation_crs_code, "
               "operation_version, accuracy, deprecated";
        constexpr int N_MAX_PARAMS_OTHER_TRANSFORMATION = 9;
        for (size_t i = 1; i <= N_MAX_PARAMS_OTHER_TRANSFORMATION; ++i) {
            buffer << ", param" << i << "_auth_name";
            buffer << ", param" << i << "_code";
            buffer << ", param" << i << "_name";
            buffer << ", param" << i << "_value";
            buffer << ", param" << i << "_uom_auth_name";
            buffer << ", param" << i << "_uom_code";
        }
        buffer << " FROM other_transformation "
                  "WHERE auth_name = ? AND code = ?";

        auto res = d->runWithCodeParam(buffer.str(), code);
        if (res.empty()) {
            // shouldn't happen if foreign keys are OK
            throw NoSuchAuthorityCodeException("other_transformation not found",
                                               d->authority(), code);
        }
        try {
            const auto &row = res.front();
            size_t idx = 0;
            const auto &name = row[idx++];
            const auto &description = row[idx++];
            const auto &method_auth_name = row[idx++];
            const auto &method_code = row[idx++];
            const auto &method_name = row[idx++];
            const auto &source_crs_auth_name = row[idx++];
            const auto &source_crs_code = row[idx++];
            const auto &target_crs_auth_name = row[idx++];
            const auto &target_crs_code = row[idx++];
            const auto &grid_param_auth_name = row[idx++];
            const auto &grid_param_code = row[idx++];
            const auto &grid_param_name = row[idx++];
            const auto &grid_name = row[idx++];
            const auto &interpolation_crs_auth_name = row[idx++];
            const auto &interpolation_crs_code = row[idx++];
            const auto &operation_version = row[idx++];
            const auto &accuracy = row[idx++];
            const auto &deprecated_str = row[idx++];
            const bool deprecated = deprecated_str == "1";

            const size_t base_param_idx = idx;
            std::vector<operation::OperationParameterNNPtr> parameters;
            std::vector<operation::ParameterValueNNPtr> values;
            for (size_t i = 0; i < N_MAX_PARAMS_OTHER_TRANSFORMATION; ++i) {
                const auto &param_auth_name = row[base_param_idx + i * 6 + 0];
                if (param_auth_name.empty()) {
                    break;
                }
                const auto &param_code = row[base_param_idx + i * 6 + 1];
                const auto &param_name = row[base_param_idx + i * 6 + 2];
                const auto &param_value = row[base_param_idx + i * 6 + 3];
                const auto &param_uom_auth_name =
                    row[base_param_idx + i * 6 + 4];
                const auto &param_uom_code = row[base_param_idx + i * 6 + 5];

                parameters.emplace_back(operation::OperationParameter::create(
                    util::PropertyMap()
                        .set(metadata::Identifier::CODESPACE_KEY,
                             param_auth_name)
                        .set(metadata::Identifier::CODE_KEY, param_code)
                        .set(common::IdentifiedObject::NAME_KEY, param_name)));
                std::string normalized_uom_code(param_uom_code);
                const double normalized_value = normalizeMeasure(
                    param_uom_code, param_value, normalized_uom_code);
                auto uom = d->createUnitOfMeasure(param_uom_auth_name,
                                                  normalized_uom_code);
                values.emplace_back(operation::ParameterValue::create(
                    common::Measure(normalized_value, uom)));
            }
            idx = base_param_idx + 6 * N_MAX_PARAMS_OTHER_TRANSFORMATION;
            (void)idx;
            assert(idx == row.size());

            if (!grid_name.empty()) {
                parameters.emplace_back(operation::OperationParameter::create(
                    util::PropertyMap()
                        .set(common::IdentifiedObject::NAME_KEY,
                             grid_param_name)
                        .set(metadata::Identifier::CODESPACE_KEY,
                             grid_param_auth_name)
                        .set(metadata::Identifier::CODE_KEY, grid_param_code)));
                values.emplace_back(
                    operation::ParameterValue::createFilename(grid_name));
            }

            auto sourceCRS =
                d->createFactory(source_crs_auth_name)
                    ->createCoordinateReferenceSystem(source_crs_code);
            auto targetCRS =
                d->createFactory(target_crs_auth_name)
                    ->createCoordinateReferenceSystem(target_crs_code);
            auto interpolationCRS =
                interpolation_crs_auth_name.empty()
                    ? nullptr
                    : d->createFactory(interpolation_crs_auth_name)
                          ->createCoordinateReferenceSystem(
                              interpolation_crs_code)
                          .as_nullable();

            auto props = d->createPropertiesSearchUsages(
                type, code, name, deprecated, description);
            if (!operation_version.empty()) {
                props.set(operation::CoordinateOperation::OPERATION_VERSION_KEY,
                          operation_version);
            }

            std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
            if (!accuracy.empty() && accuracy != "999.0") {
                accuracies.emplace_back(
                    metadata::PositionalAccuracy::create(accuracy));
            }

            if (method_auth_name == "PROJ") {
                if (method_code == "PROJString") {
                    auto op = operation::SingleOperation::createPROJBased(
                        props, method_name, sourceCRS, targetCRS, accuracies);
                    op->setCRSs(sourceCRS, targetCRS, interpolationCRS);
                    return op;
                } else if (method_code == "WKT") {
                    auto op = util::nn_dynamic_pointer_cast<
                        operation::CoordinateOperation>(
                        WKTParser().createFromWKT(method_name));
                    if (!op) {
                        throw FactoryException("WKT string does not express a "
                                               "coordinate operation");
                    }
                    op->setCRSs(sourceCRS, targetCRS, interpolationCRS);
                    return NN_NO_CHECK(op);
                }
            }

            auto propsMethod =
                util::PropertyMap()
                    .set(metadata::Identifier::CODESPACE_KEY, method_auth_name)
                    .set(metadata::Identifier::CODE_KEY, method_code)
                    .set(common::IdentifiedObject::NAME_KEY, method_name);

            if (method_auth_name == metadata::Identifier::EPSG) {
                int method_code_int = std::atoi(method_code.c_str());
                if (operation::isAxisOrderReversal(method_code_int) ||
                    method_code_int == EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT ||
                    method_code_int ==
                        EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR ||
                    method_code_int == EPSG_CODE_METHOD_HEIGHT_DEPTH_REVERSAL) {
                    auto op = operation::Conversion::create(props, propsMethod,
                                                            parameters, values);
                    op->setCRSs(sourceCRS, targetCRS, interpolationCRS);
                    return op;
                }
            }
            auto transf = operation::Transformation::create(
                props, sourceCRS, targetCRS, interpolationCRS, propsMethod,
                parameters, values, accuracies);
            if (usePROJAlternativeGridNames) {
                return transf->substitutePROJAlternativeGridNames(d->context());
            }
            return transf;

        } catch (const std::exception &ex) {
            throw buildFactoryException("transformation", d->authority(), code,
                                        ex);
        }
    }

    if (allowConcatenated && type == "concatenated_operation") {
        auto res = d->runWithCodeParam(
            "SELECT name, description, "
            "source_crs_auth_name, source_crs_code, "
            "target_crs_auth_name, target_crs_code, "
            "accuracy, "
            "operation_version, deprecated FROM "
            "concatenated_operation WHERE auth_name = ? AND code = ?",
            code);
        if (res.empty()) {
            // shouldn't happen if foreign keys are OK
            throw NoSuchAuthorityCodeException(
                "concatenated_operation not found", d->authority(), code);
        }

        auto resSteps = d->runWithCodeParam(
            "SELECT step_auth_name, step_code, step_direction FROM "
            "concatenated_operation_step WHERE operation_auth_name = ? "
            "AND operation_code = ? ORDER BY step_number",
            code);

        try {
            const auto &row = res.front();
            size_t idx = 0;
            const auto &name = row[idx++];
            const auto &description = row[idx++];
            const auto &source_crs_auth_name = row[idx++];
            const auto &source_crs_code = row[idx++];
            const auto &target_crs_auth_name = row[idx++];
            const auto &target_crs_code = row[idx++];
            const auto &accuracy = row[idx++];
            const auto &operation_version = row[idx++];
            const auto &deprecated_str = row[idx++];
            const bool deprecated = deprecated_str == "1";

            std::vector<operation::CoordinateOperationNNPtr> operations;
            size_t countExplicitDirection = 0;
            for (const auto &rowStep : resSteps) {
                const auto &step_auth_name = rowStep[0];
                const auto &step_code = rowStep[1];
                const auto &step_direction = rowStep[2];
                auto stepOp =
                    d->createFactory(step_auth_name)
                        ->createCoordinateOperation(step_code, false,
                                                    usePROJAlternativeGridNames,
                                                    std::string());
                if (step_direction == "forward") {
                    ++countExplicitDirection;
                    operations.push_back(std::move(stepOp));
                } else if (step_direction == "reverse") {
                    ++countExplicitDirection;
                    operations.push_back(stepOp->inverse());
                } else {
                    operations.push_back(std::move(stepOp));
                }
            }

            if (countExplicitDirection > 0 &&
                countExplicitDirection != resSteps.size()) {
                throw FactoryException("not all steps have a defined direction "
                                       "for concatenated operation " +
                                       code);
            }

            const bool fixDirectionAllowed = (countExplicitDirection == 0);
            operation::ConcatenatedOperation::fixSteps(
                d->createFactory(source_crs_auth_name)
                    ->createCoordinateReferenceSystem(source_crs_code),
                d->createFactory(target_crs_auth_name)
                    ->createCoordinateReferenceSystem(target_crs_code),
                operations, d->context(), fixDirectionAllowed);

            auto props = d->createPropertiesSearchUsages(
                type, code, name, deprecated, description);
            if (!operation_version.empty()) {
                props.set(operation::CoordinateOperation::OPERATION_VERSION_KEY,
                          operation_version);
            }

            std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
            if (!accuracy.empty()) {
                if (accuracy != "999.0") {
                    accuracies.emplace_back(
                        metadata::PositionalAccuracy::create(accuracy));
                }
            } else {
                // Try to compute a reasonable accuracy from the members
                double totalAcc = -1;
                try {
                    for (const auto &op : operations) {
                        auto accs = op->coordinateOperationAccuracies();
                        if (accs.size() == 1) {
                            double acc = c_locale_stod(accs[0]->value());
                            if (totalAcc < 0) {
                                totalAcc = acc;
                            } else {
                                totalAcc += acc;
                            }
                        } else if (dynamic_cast<const operation::Conversion *>(
                                       op.get())) {
                            // A conversion is perfectly accurate.
                            if (totalAcc < 0) {
                                totalAcc = 0;
                            }
                        } else {
                            totalAcc = -1;
                            break;
                        }
                    }
                    if (totalAcc >= 0) {
                        accuracies.emplace_back(
                            metadata::PositionalAccuracy::create(
                                toString(totalAcc)));
                    }
                } catch (const std::exception &) {
                }
            }
            return operation::ConcatenatedOperation::create(props, operations,
                                                            accuracies);

        } catch (const std::exception &ex) {
            throw buildFactoryException("transformation", d->authority(), code,
                                        ex);
        }
    }

    throw FactoryException("unhandled coordinate operation type: " + type);
}

// ---------------------------------------------------------------------------

/** \brief Returns a list operation::CoordinateOperation between two CRS.
 *
 * The list is ordered with preferred operations first. No attempt is made
 * at inferring operations that are not explicitly in the database.
 *
 * Deprecated operations are rejected.
 *
 * @param sourceCRSCode Source CRS code allocated by authority.
 * @param targetCRSCode Source CRS code allocated by authority.
 * @return list of coordinate operations
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

std::vector<operation::CoordinateOperationNNPtr>
AuthorityFactory::createFromCoordinateReferenceSystemCodes(
    const std::string &sourceCRSCode, const std::string &targetCRSCode) const {
    return createFromCoordinateReferenceSystemCodes(
        d->authority(), sourceCRSCode, d->authority(), targetCRSCode, false,
        false, false, false);
}

// ---------------------------------------------------------------------------

/** \brief Returns a list of geoid models available for that crs
 *
 * The list includes the geoid models connected directly with the crs,
 * or via "Height Depth Reversal" or "Change of Vertical Unit" transformations
 *
 * @param code crs code allocated by authority.
 * @return list of geoid model names
 * @throw FactoryException in case of error.
 */

std::list<std::string>
AuthorityFactory::getGeoidModels(const std::string &code) const {

    ListOfParams params;
    std::string sql;
    sql += "SELECT DISTINCT GM0.name "
           "  FROM geoid_model GM0 "
           "INNER JOIN grid_transformation GT0 "
           "  ON  GT0.code = GM0.operation_code "
           "  AND GT0.auth_name = GM0.operation_auth_name "
           "  AND GT0.deprecated = 0 "
           "INNER JOIN vertical_crs VC0 "
           "  ON VC0.code = GT0.target_crs_code "
           "  AND VC0.auth_name = GT0.target_crs_auth_name "
           "INNER JOIN vertical_crs VC1 "
           "  ON VC1.datum_code = VC0.datum_code "
           "  AND VC1.datum_auth_name = VC0.datum_auth_name "
           "  AND VC1.code = ? ";
    params.emplace_back(code);
    if (d->hasAuthorityRestriction()) {
        sql += " AND GT0.target_crs_auth_name = ? ";
        params.emplace_back(d->authority());
    }
    sql += " ORDER BY 1 ";

    auto sqlRes = d->run(sql, params);
    std::list<std::string> res;
    for (const auto &row : sqlRes) {
        res.push_back(row[0]);
    }
    return res;
}

// ---------------------------------------------------------------------------

/** \brief Returns a list operation::CoordinateOperation between two CRS.
 *
 * The list is ordered with preferred operations first. No attempt is made
 * at inferring operations that are not explicitly in the database (see
 * createFromCRSCodesWithIntermediates() for that), and only
 * source -> target operations are searched (i.e. if target -> source is
 * present, you need to call this method with the arguments reversed, and apply
 * the reverse transformations).
 *
 * Deprecated operations are rejected.
 *
 * If getAuthority() returns empty, then coordinate operations from all
 * authorities are considered.
 *
 * @param sourceCRSAuthName Authority name of sourceCRSCode
 * @param sourceCRSCode Source CRS code allocated by authority
 * sourceCRSAuthName.
 * @param targetCRSAuthName Authority name of targetCRSCode
 * @param targetCRSCode Source CRS code allocated by authority
 * targetCRSAuthName.
 * @param usePROJAlternativeGridNames Whether PROJ alternative grid names
 * should be substituted to the official grid names.
 * @param discardIfMissingGrid Whether coordinate operations that reference
 * missing grids should be removed from the result set.
 * @param considerKnownGridsAsAvailable Whether known grids should be considered
 * as available (typically when network is enabled).
 * @param discardSuperseded Whether coordinate operations that are superseded
 * (but not deprecated) should be removed from the result set.
 * @param tryReverseOrder whether to search in the reverse order too (and thus
 * inverse results found that way)
 * @param reportOnlyIntersectingTransformations if intersectingExtent1 and
 * intersectingExtent2 should be honored in a strict way.
 * @param intersectingExtent1 Optional extent that the resulting operations
 * must intersect.
 * @param intersectingExtent2 Optional extent that the resulting operations
 * must intersect.
 * @return list of coordinate operations
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

std::vector<operation::CoordinateOperationNNPtr>
AuthorityFactory::createFromCoordinateReferenceSystemCodes(
    const std::string &sourceCRSAuthName, const std::string &sourceCRSCode,
    const std::string &targetCRSAuthName, const std::string &targetCRSCode,
    bool usePROJAlternativeGridNames, bool discardIfMissingGrid,
    bool considerKnownGridsAsAvailable, bool discardSuperseded,
    bool tryReverseOrder, bool reportOnlyIntersectingTransformations,
    const metadata::ExtentPtr &intersectingExtent1,
    const metadata::ExtentPtr &intersectingExtent2) const {

    auto cacheKey(d->authority());
    cacheKey += sourceCRSAuthName.empty() ? "{empty}" : sourceCRSAuthName;
    cacheKey += sourceCRSCode;
    cacheKey += targetCRSAuthName.empty() ? "{empty}" : targetCRSAuthName;
    cacheKey += targetCRSCode;
    cacheKey += (usePROJAlternativeGridNames ? '1' : '0');
    cacheKey += (discardIfMissingGrid ? '1' : '0');
    cacheKey += (considerKnownGridsAsAvailable ? '1' : '0');
    cacheKey += (discardSuperseded ? '1' : '0');
    cacheKey += (tryReverseOrder ? '1' : '0');
    cacheKey += (reportOnlyIntersectingTransformations ? '1' : '0');
    for (const auto &extent : {intersectingExtent1, intersectingExtent2}) {
        if (extent) {
            const auto &geogExtent = extent->geographicElements();
            if (geogExtent.size() == 1) {
                auto bbox =
                    dynamic_cast<const metadata::GeographicBoundingBox *>(
                        geogExtent[0].get());
                if (bbox) {
                    cacheKey += toString(bbox->southBoundLatitude());
                    cacheKey += toString(bbox->westBoundLongitude());
                    cacheKey += toString(bbox->northBoundLatitude());
                    cacheKey += toString(bbox->eastBoundLongitude());
                }
            }
        }
    }

    std::vector<operation::CoordinateOperationNNPtr> list;

    if (d->context()->d->getCRSToCRSCoordOpFromCache(cacheKey, list)) {
        return list;
    }

    // Check if sourceCRS would be the base of a ProjectedCRS targetCRS
    // In which case use the conversion of the ProjectedCRS
    if (!targetCRSAuthName.empty()) {
        auto targetFactory = d->createFactory(targetCRSAuthName);
        const auto cacheKeyProjectedCRS(targetFactory->d->authority() +
                                        targetCRSCode);
        auto crs = targetFactory->d->context()->d->getCRSFromCache(
            cacheKeyProjectedCRS);
        crs::ProjectedCRSPtr targetProjCRS;
        if (crs) {
            targetProjCRS = std::dynamic_pointer_cast<crs::ProjectedCRS>(crs);
        } else {
            const auto sqlRes =
                targetFactory->d->createProjectedCRSBegin(targetCRSCode);
            if (!sqlRes.empty()) {
                try {
                    targetProjCRS =
                        targetFactory->d
                            ->createProjectedCRSEnd(targetCRSCode, sqlRes)
                            .as_nullable();
                } catch (const std::exception &) {
                }
            }
        }
        if (targetProjCRS) {
            const auto &baseIds = targetProjCRS->baseCRS()->identifiers();
            if (sourceCRSAuthName.empty() ||
                (!baseIds.empty() &&
                 *(baseIds.front()->codeSpace()) == sourceCRSAuthName &&
                 baseIds.front()->code() == sourceCRSCode)) {
                bool ok = true;
                auto conv = targetProjCRS->derivingConversion();
                if (d->hasAuthorityRestriction()) {
                    ok = *(conv->identifiers().front()->codeSpace()) ==
                         d->authority();
                }
                if (ok) {
                    list.emplace_back(conv);
                    d->context()->d->cache(cacheKey, list);
                    return list;
                }
            }
        }
    }

    std::string sql;
    if (discardSuperseded) {
        sql = "SELECT cov.source_crs_auth_name, cov.source_crs_code, "
              "cov.target_crs_auth_name, cov.target_crs_code, "
              "cov.auth_name, cov.code, cov.table_name, "
              "extent.south_lat, extent.west_lon, extent.north_lat, "
              "extent.east_lon, "
              "ss.replacement_auth_name, ss.replacement_code, "
              "(gt.auth_name IS NOT NULL) AS replacement_is_grid_transform, "
              "(ga.proj_grid_name IS NOT NULL) AS replacement_is_known_grid "
              "FROM "
              "coordinate_operation_view cov "
              "JOIN usage ON "
              "usage.object_table_name = cov.table_name AND "
              "usage.object_auth_name = cov.auth_name AND "
              "usage.object_code = cov.code "
              "JOIN extent "
              "ON extent.auth_name = usage.extent_auth_name AND "
              "extent.code = usage.extent_code "
              "LEFT JOIN supersession ss ON "
              "ss.superseded_table_name = cov.table_name AND "
              "ss.superseded_auth_name = cov.auth_name AND "
              "ss.superseded_code = cov.code AND "
              "ss.superseded_table_name = ss.replacement_table_name AND "
              "ss.same_source_target_crs = 1 "
              "LEFT JOIN grid_transformation gt ON "
              "gt.auth_name = ss.replacement_auth_name AND "
              "gt.code = ss.replacement_code "
              "LEFT JOIN grid_alternatives ga ON "
              "ga.original_grid_name = gt.grid_name "
              "WHERE ";
    } else {
        sql = "SELECT source_crs_auth_name, source_crs_code, "
              "target_crs_auth_name, target_crs_code, "
              "cov.auth_name, cov.code, cov.table_name, "
              "extent.south_lat, extent.west_lon, extent.north_lat, "
              "extent.east_lon "
              "FROM "
              "coordinate_operation_view cov "
              "JOIN usage ON "
              "usage.object_table_name = cov.table_name AND "
              "usage.object_auth_name = cov.auth_name AND "
              "usage.object_code = cov.code "
              "JOIN extent "
              "ON extent.auth_name = usage.extent_auth_name AND "
              "extent.code = usage.extent_code "
              "WHERE ";
    }
    ListOfParams params;
    if (!sourceCRSAuthName.empty() && !targetCRSAuthName.empty()) {
        if (tryReverseOrder) {
            sql += "((cov.source_crs_auth_name = ? AND cov.source_crs_code = ? "
                   "AND "
                   "cov.target_crs_auth_name = ? AND cov.target_crs_code = ?) "
                   "OR "
                   "(cov.source_crs_auth_name = ? AND cov.source_crs_code = ? "
                   "AND "
                   "cov.target_crs_auth_name = ? AND cov.target_crs_code = ?)) "
                   "AND ";
            params.emplace_back(sourceCRSAuthName);
            params.emplace_back(sourceCRSCode);
            params.emplace_back(targetCRSAuthName);
            params.emplace_back(targetCRSCode);
            params.emplace_back(targetCRSAuthName);
            params.emplace_back(targetCRSCode);
            params.emplace_back(sourceCRSAuthName);
            params.emplace_back(sourceCRSCode);
        } else {
            sql += "cov.source_crs_auth_name = ? AND cov.source_crs_code = ? "
                   "AND "
                   "cov.target_crs_auth_name = ? AND cov.target_crs_code = ? "
                   "AND ";
            params.emplace_back(sourceCRSAuthName);
            params.emplace_back(sourceCRSCode);
            params.emplace_back(targetCRSAuthName);
            params.emplace_back(targetCRSCode);
        }
    } else if (!sourceCRSAuthName.empty()) {
        if (tryReverseOrder) {
            sql += "((cov.source_crs_auth_name = ? AND cov.source_crs_code = ? "
                   ")OR "
                   "(cov.target_crs_auth_name = ? AND cov.target_crs_code = ?))"
                   " AND ";
            params.emplace_back(sourceCRSAuthName);
            params.emplace_back(sourceCRSCode);
            params.emplace_back(sourceCRSAuthName);
            params.emplace_back(sourceCRSCode);
        } else {
            sql += "cov.source_crs_auth_name = ? AND cov.source_crs_code = ? "
                   "AND ";
            params.emplace_back(sourceCRSAuthName);
            params.emplace_back(sourceCRSCode);
        }
    } else if (!targetCRSAuthName.empty()) {
        if (tryReverseOrder) {
            sql += "((cov.source_crs_auth_name = ? AND cov.source_crs_code = ?)"
                   " OR "
                   "(cov.target_crs_auth_name = ? AND cov.target_crs_code = ?))"
                   " AND ";
            params.emplace_back(targetCRSAuthName);
            params.emplace_back(targetCRSCode);
            params.emplace_back(targetCRSAuthName);
            params.emplace_back(targetCRSCode);
        } else {
            sql += "cov.target_crs_auth_name = ? AND cov.target_crs_code = ? "
                   "AND ";
            params.emplace_back(targetCRSAuthName);
            params.emplace_back(targetCRSCode);
        }
    }
    sql += "cov.deprecated = 0";
    if (d->hasAuthorityRestriction()) {
        sql += " AND cov.auth_name = ?";
        params.emplace_back(d->authority());
    }
    sql += " ORDER BY pseudo_area_from_swne(south_lat, west_lon, north_lat, "
           "east_lon) DESC, "
           "(CASE WHEN cov.accuracy is NULL THEN 1 ELSE 0 END), cov.accuracy";
    auto res = d->run(sql, params);
    std::set<std::pair<std::string, std::string>> setTransf;
    if (discardSuperseded) {
        for (const auto &row : res) {
            const auto &auth_name = row[4];
            const auto &code = row[5];
            setTransf.insert(
                std::pair<std::string, std::string>(auth_name, code));
        }
    }

    // Do a pass to determine if there are transformations that intersect
    // intersectingExtent1 & intersectingExtent2
    std::vector<bool> intersectingTransformations;
    intersectingTransformations.resize(res.size());
    bool hasIntersectingTransformations = false;
    size_t i = 0;
    for (const auto &row : res) {
        size_t thisI = i;
        ++i;
        if (discardSuperseded) {
            const auto &replacement_auth_name = row[11];
            const auto &replacement_code = row[12];
            const bool replacement_is_grid_transform = row[13] == "1";
            const bool replacement_is_known_grid = row[14] == "1";
            if (!replacement_auth_name.empty() &&
                // Ignore supersession if the replacement uses a unknown grid
                !(replacement_is_grid_transform &&
                  !replacement_is_known_grid) &&
                setTransf.find(std::pair<std::string, std::string>(
                    replacement_auth_name, replacement_code)) !=
                    setTransf.end()) {
                // Skip transformations that are superseded by others that got
                // returned in the result set.
                continue;
            }
        }

        bool intersecting = true;
        try {
            double south_lat = c_locale_stod(row[7]);
            double west_lon = c_locale_stod(row[8]);
            double north_lat = c_locale_stod(row[9]);
            double east_lon = c_locale_stod(row[10]);
            auto transf_extent = metadata::Extent::createFromBBOX(
                west_lon, south_lat, east_lon, north_lat);

            for (const auto &extent :
                 {intersectingExtent1, intersectingExtent2}) {
                if (extent) {
                    if (!transf_extent->intersects(NN_NO_CHECK(extent))) {
                        intersecting = false;
                        break;
                    }
                }
            }
        } catch (const std::exception &) {
        }

        intersectingTransformations[thisI] = intersecting;
        if (intersecting)
            hasIntersectingTransformations = true;
    }

    // If there are intersecting transformations, then only report those ones
    // If there are no intersecting transformations, report all of them
    // This is for the "projinfo -s EPSG:32631 -t EPSG:2171" use case where we
    // still want to be able to use the Pulkovo datum shift if EPSG:32631
    // coordinates are used
    i = 0;
    for (const auto &row : res) {
        size_t thisI = i;
        ++i;
        if ((hasIntersectingTransformations ||
             reportOnlyIntersectingTransformations) &&
            !intersectingTransformations[thisI]) {
            continue;
        }
        if (discardSuperseded) {
            const auto &replacement_auth_name = row[11];
            const auto &replacement_code = row[12];
            const bool replacement_is_grid_transform = row[13] == "1";
            const bool replacement_is_known_grid = row[14] == "1";
            if (!replacement_auth_name.empty() &&
                // Ignore supersession if the replacement uses a unknown grid
                !(replacement_is_grid_transform &&
                  !replacement_is_known_grid) &&
                setTransf.find(std::pair<std::string, std::string>(
                    replacement_auth_name, replacement_code)) !=
                    setTransf.end()) {
                // Skip transformations that are superseded by others that got
                // returned in the result set.
                continue;
            }
        }

        const auto &source_crs_auth_name = row[0];
        const auto &source_crs_code = row[1];
        const auto &target_crs_auth_name = row[2];
        const auto &target_crs_code = row[3];
        const auto &auth_name = row[4];
        const auto &code = row[5];
        const auto &table_name = row[6];
        try {
            auto op = d->createFactory(auth_name)->createCoordinateOperation(
                code, true, usePROJAlternativeGridNames, table_name);
            if (tryReverseOrder &&
                (!sourceCRSAuthName.empty()
                     ? (source_crs_auth_name != sourceCRSAuthName ||
                        source_crs_code != sourceCRSCode)
                     : (target_crs_auth_name != targetCRSAuthName ||
                        target_crs_code != targetCRSCode))) {
                op = op->inverse();
            }
            if (!discardIfMissingGrid ||
                !d->rejectOpDueToMissingGrid(op,
                                             considerKnownGridsAsAvailable)) {
                list.emplace_back(op);
            }
        } catch (const std::exception &e) {
            // Mostly for debugging purposes when using an inconsistent
            // database
            if (getenv("PROJ_IGNORE_INSTANTIATION_ERRORS")) {
                fprintf(stderr, "Ignoring invalid operation: %s\n", e.what());
            } else {
                throw;
            }
        }
    }
    d->context()->d->cache(cacheKey, list);
    return list;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static bool useIrrelevantPivot(const operation::CoordinateOperationNNPtr &op,
                               const std::string &sourceCRSAuthName,
                               const std::string &sourceCRSCode,
                               const std::string &targetCRSAuthName,
                               const std::string &targetCRSCode) {
    auto concat =
        dynamic_cast<const operation::ConcatenatedOperation *>(op.get());
    if (!concat) {
        return false;
    }
    auto ops = concat->operations();
    for (size_t i = 0; i + 1 < ops.size(); i++) {
        auto targetCRS = ops[i]->targetCRS();
        if (targetCRS) {
            const auto &ids = targetCRS->identifiers();
            if (ids.size() == 1 &&
                ((*ids[0]->codeSpace() == sourceCRSAuthName &&
                  ids[0]->code() == sourceCRSCode) ||
                 (*ids[0]->codeSpace() == targetCRSAuthName &&
                  ids[0]->code() == targetCRSCode))) {
                return true;
            }
        }
    }
    return false;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns a list operation::CoordinateOperation between two CRS,
 * using intermediate codes.
 *
 * The list is ordered with preferred operations first.
 *
 * Deprecated operations are rejected.
 *
 * The method will take care of considering all potential combinations (i.e.
 * contrary to createFromCoordinateReferenceSystemCodes(), you do not need to
 * call it with sourceCRS and targetCRS switched)
 *
 * If getAuthority() returns empty, then coordinate operations from all
 * authorities are considered.
 *
 * @param sourceCRSAuthName Authority name of sourceCRSCode
 * @param sourceCRSCode Source CRS code allocated by authority
 * sourceCRSAuthName.
 * @param targetCRSAuthName Authority name of targetCRSCode
 * @param targetCRSCode Source CRS code allocated by authority
 * targetCRSAuthName.
 * @param usePROJAlternativeGridNames Whether PROJ alternative grid names
 * should be substituted to the official grid names.
 * @param discardIfMissingGrid Whether coordinate operations that reference
 * missing grids should be removed from the result set.
 * @param considerKnownGridsAsAvailable Whether known grids should be considered
 * as available (typically when network is enabled).
 * @param discardSuperseded Whether coordinate operations that are superseded
 * (but not deprecated) should be removed from the result set.
 * @param intermediateCRSAuthCodes List of (auth_name, code) of CRS that can be
 * used as potential intermediate CRS. If the list is empty, the database will
 * be used to find common CRS in operations involving both the source and
 * target CRS.
 * @param allowedIntermediateObjectType Restrict the type of the intermediate
 * object considered.
 * Only ObjectType::CRS and ObjectType::GEOGRAPHIC_CRS supported currently
 * @param allowedAuthorities One or several authority name allowed for the two
 * coordinate operations that are going to be searched. When this vector is
 * no empty, it overrides the authority of this object. This is useful for
 * example when the coordinate operations to chain belong to two different
 * allowed authorities.
 * @param intersectingExtent1 Optional extent that the resulting operations
 * must intersect.
 * @param intersectingExtent2 Optional extent that the resulting operations
 * must intersect.
 * @return list of coordinate operations
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */

std::vector<operation::CoordinateOperationNNPtr>
AuthorityFactory::createFromCRSCodesWithIntermediates(
    const std::string &sourceCRSAuthName, const std::string &sourceCRSCode,
    const std::string &targetCRSAuthName, const std::string &targetCRSCode,
    bool usePROJAlternativeGridNames, bool discardIfMissingGrid,
    bool considerKnownGridsAsAvailable, bool discardSuperseded,
    const std::vector<std::pair<std::string, std::string>>
        &intermediateCRSAuthCodes,
    ObjectType allowedIntermediateObjectType,
    const std::vector<std::string> &allowedAuthorities,
    const metadata::ExtentPtr &intersectingExtent1,
    const metadata::ExtentPtr &intersectingExtent2) const {

    std::vector<operation::CoordinateOperationNNPtr> listTmp;

    if (sourceCRSAuthName == targetCRSAuthName &&
        sourceCRSCode == targetCRSCode) {
        return listTmp;
    }

    const auto CheckIfHasOperations = [this](const std::string &auth_name,
                                             const std::string &code) {
        return !(d->run("SELECT 1 FROM coordinate_operation_view WHERE "
                        "(source_crs_auth_name = ? AND source_crs_code = ?) OR "
                        "(target_crs_auth_name = ? AND target_crs_code = ?) "
                        "LIMIT 1",
                        {auth_name, code, auth_name, code})
                     .empty());
    };

    // If the source or target CRS are not the source or target of an operation,
    // do not run the next costly requests.
    if (!CheckIfHasOperations(sourceCRSAuthName, sourceCRSCode) ||
        !CheckIfHasOperations(targetCRSAuthName, targetCRSCode)) {
        return listTmp;
    }

    const std::string sqlProlog(
        discardSuperseded
            ?

            "SELECT v1.table_name as table1, "
            "v1.auth_name AS auth_name1, v1.code AS code1, "
            "v1.accuracy AS accuracy1, "
            "v2.table_name as table2, "
            "v2.auth_name AS auth_name2, v2.code AS code2, "
            "v2.accuracy as accuracy2, "
            "a1.south_lat AS south_lat1, "
            "a1.west_lon AS west_lon1, "
            "a1.north_lat AS north_lat1, "
            "a1.east_lon AS east_lon1, "
            "a2.south_lat AS south_lat2, "
            "a2.west_lon AS west_lon2, "
            "a2.north_lat AS north_lat2, "
            "a2.east_lon AS east_lon2, "
            "ss1.replacement_auth_name AS replacement_auth_name1, "
            "ss1.replacement_code AS replacement_code1, "
            "ss2.replacement_auth_name AS replacement_auth_name2, "
            "ss2.replacement_code AS replacement_code2 "
            "FROM coordinate_operation_view v1 "
            "JOIN coordinate_operation_view v2 "
            :

            "SELECT v1.table_name as table1, "
            "v1.auth_name AS auth_name1, v1.code AS code1, "
            "v1.accuracy AS accuracy1, "
            "v2.table_name as table2, "
            "v2.auth_name AS auth_name2, v2.code AS code2, "
            "v2.accuracy as accuracy2, "
            "a1.south_lat AS south_lat1, "
            "a1.west_lon AS west_lon1, "
            "a1.north_lat AS north_lat1, "
            "a1.east_lon AS east_lon1, "
            "a2.south_lat AS south_lat2, "
            "a2.west_lon AS west_lon2, "
            "a2.north_lat AS north_lat2, "
            "a2.east_lon AS east_lon2 "
            "FROM coordinate_operation_view v1 "
            "JOIN coordinate_operation_view v2 ");

    const char *joinSupersession =
        "LEFT JOIN supersession ss1 ON "
        "ss1.superseded_table_name = v1.table_name AND "
        "ss1.superseded_auth_name = v1.auth_name AND "
        "ss1.superseded_code = v1.code AND "
        "ss1.superseded_table_name = ss1.replacement_table_name AND "
        "ss1.same_source_target_crs = 1 "
        "LEFT JOIN supersession ss2 ON "
        "ss2.superseded_table_name = v2.table_name AND "
        "ss2.superseded_auth_name = v2.auth_name AND "
        "ss2.superseded_code = v2.code AND "
        "ss2.superseded_table_name = ss2.replacement_table_name AND "
        "ss2.same_source_target_crs = 1 ";
    const std::string joinArea(
        (discardSuperseded ? std::string(joinSupersession) : std::string())
            .append("JOIN usage u1 ON "
                    "u1.object_table_name = v1.table_name AND "
                    "u1.object_auth_name = v1.auth_name AND "
                    "u1.object_code = v1.code "
                    "JOIN extent a1 "
                    "ON a1.auth_name = u1.extent_auth_name AND "
                    "a1.code = u1.extent_code "
                    "JOIN usage u2 ON "
                    "u2.object_table_name = v2.table_name AND "
                    "u2.object_auth_name = v2.auth_name AND "
                    "u2.object_code = v2.code "
                    "JOIN extent a2 "
                    "ON a2.auth_name = u2.extent_auth_name AND "
                    "a2.code = u2.extent_code "));
    const std::string orderBy(
        "ORDER BY (CASE WHEN accuracy1 is NULL THEN 1 ELSE 0 END) + "
        "(CASE WHEN accuracy2 is NULL THEN 1 ELSE 0 END), "
        "accuracy1 + accuracy2");

    // Case (source->intermediate) and (intermediate->target)
    std::string sql(
        sqlProlog +
        "ON v1.target_crs_auth_name = v2.source_crs_auth_name "
        "AND v1.target_crs_code = v2.source_crs_code " +
        joinArea +
        "WHERE v1.source_crs_auth_name = ? AND v1.source_crs_code = ? "
        "AND v2.target_crs_auth_name = ? AND v2.target_crs_code = ? ");
    std::string minDate;
    std::string criterionOnIntermediateCRS;

    const auto sourceCRS = d->createFactory(sourceCRSAuthName)
                               ->createCoordinateReferenceSystem(sourceCRSCode);
    const auto targetCRS = d->createFactory(targetCRSAuthName)
                               ->createCoordinateReferenceSystem(targetCRSCode);

    const bool ETRFtoETRF = starts_with(sourceCRS->nameStr(), "ETRF") &&
                            starts_with(targetCRS->nameStr(), "ETRF");

    const bool NAD83_CSRS_to_NAD83_CSRS =
        starts_with(sourceCRS->nameStr(), "NAD83(CSRS)") &&
        starts_with(targetCRS->nameStr(), "NAD83(CSRS)");

    if (allowedIntermediateObjectType == ObjectType::GEOGRAPHIC_CRS) {
        const auto &sourceGeogCRS =
            dynamic_cast<const crs::GeographicCRS *>(sourceCRS.get());
        const auto &targetGeogCRS =
            dynamic_cast<const crs::GeographicCRS *>(targetCRS.get());
        if (sourceGeogCRS && targetGeogCRS) {
            const auto &sourceDatum = sourceGeogCRS->datum();
            const auto &targetDatum = targetGeogCRS->datum();
            if (sourceDatum && sourceDatum->publicationDate().has_value() &&
                targetDatum && targetDatum->publicationDate().has_value()) {
                const auto sourceDate(
                    sourceDatum->publicationDate()->toString());
                const auto targetDate(
                    targetDatum->publicationDate()->toString());
                minDate = std::min(sourceDate, targetDate);
                // Check that the datum of the intermediateCRS has a publication
                // date most recent that the one of the source and the target
                // CRS Except when using the usual WGS84 pivot which happens to
                // have a NULL publication date.
                criterionOnIntermediateCRS =
                    "AND EXISTS(SELECT 1 FROM geodetic_crs x "
                    "JOIN geodetic_datum y "
                    "ON "
                    "y.auth_name = x.datum_auth_name AND "
                    "y.code = x.datum_code "
                    "WHERE "
                    "x.auth_name = v1.target_crs_auth_name AND "
                    "x.code = v1.target_crs_code AND "
                    "x.type IN ('geographic 2D', 'geographic 3D') AND "
                    "(y.publication_date IS NULL OR "
                    "(y.publication_date >= '" +
                    minDate + "'))) ";
            }
        }
        if (criterionOnIntermediateCRS.empty()) {
            criterionOnIntermediateCRS =
                "AND EXISTS(SELECT 1 FROM geodetic_crs x WHERE "
                "x.auth_name = v1.target_crs_auth_name AND "
                "x.code = v1.target_crs_code AND "
                "x.type IN ('geographic 2D', 'geographic 3D')) ";
        }
        sql += criterionOnIntermediateCRS;
    }
    auto params = ListOfParams{sourceCRSAuthName, sourceCRSCode,
                               targetCRSAuthName, targetCRSCode};
    std::string additionalWhere(
        "AND v1.deprecated = 0 AND v2.deprecated = 0 "
        "AND intersects_bbox(south_lat1, west_lon1, north_lat1, east_lon1, "
        "south_lat2, west_lon2, north_lat2, east_lon2) = 1 ");
    if (!allowedAuthorities.empty()) {
        additionalWhere += "AND v1.auth_name IN (";
        for (size_t i = 0; i < allowedAuthorities.size(); i++) {
            if (i > 0)
                additionalWhere += ',';
            additionalWhere += '?';
        }
        additionalWhere += ") AND v2.auth_name IN (";
        for (size_t i = 0; i < allowedAuthorities.size(); i++) {
            if (i > 0)
                additionalWhere += ',';
            additionalWhere += '?';
        }
        additionalWhere += ')';
        for (const auto &allowedAuthority : allowedAuthorities) {
            params.emplace_back(allowedAuthority);
        }
        for (const auto &allowedAuthority : allowedAuthorities) {
            params.emplace_back(allowedAuthority);
        }
    }
    if (d->hasAuthorityRestriction()) {
        additionalWhere += "AND v1.auth_name = ? AND v2.auth_name = ? ";
        params.emplace_back(d->authority());
        params.emplace_back(d->authority());
    }
    for (const auto &extent : {intersectingExtent1, intersectingExtent2}) {
        if (extent) {
            const auto &geogExtent = extent->geographicElements();
            if (geogExtent.size() == 1) {
                auto bbox =
                    dynamic_cast<const metadata::GeographicBoundingBox *>(
                        geogExtent[0].get());
                if (bbox) {
                    const double south_lat = bbox->southBoundLatitude();
                    const double west_lon = bbox->westBoundLongitude();
                    const double north_lat = bbox->northBoundLatitude();
                    const double east_lon = bbox->eastBoundLongitude();
                    if (south_lat != -90.0 || west_lon != -180.0 ||
                        north_lat != 90.0 || east_lon != 180.0) {
                        additionalWhere +=
                            "AND intersects_bbox(south_lat1, "
                            "west_lon1, north_lat1, east_lon1, ?, ?, ?, ?) AND "
                            "intersects_bbox(south_lat2, west_lon2, "
                            "north_lat2, east_lon2, ?, ?, ?, ?) ";
                        params.emplace_back(south_lat);
                        params.emplace_back(west_lon);
                        params.emplace_back(north_lat);
                        params.emplace_back(east_lon);
                        params.emplace_back(south_lat);
                        params.emplace_back(west_lon);
                        params.emplace_back(north_lat);
                        params.emplace_back(east_lon);
                    }
                }
            }
        }
    }

    const auto buildIntermediateWhere =
        [&intermediateCRSAuthCodes](const std::string &first_field,
                                    const std::string &second_field) {
            if (intermediateCRSAuthCodes.empty()) {
                return std::string();
            }
            std::string l_sql(" AND (");
            for (size_t i = 0; i < intermediateCRSAuthCodes.size(); ++i) {
                if (i > 0) {
                    l_sql += " OR";
                }
                l_sql += "(v1." + first_field + "_crs_auth_name = ? AND ";
                l_sql += "v1." + first_field + "_crs_code = ? AND ";
                l_sql += "v2." + second_field + "_crs_auth_name = ? AND ";
                l_sql += "v2." + second_field + "_crs_code = ?) ";
            }
            l_sql += ')';
            return l_sql;
        };

    std::string intermediateWhere = buildIntermediateWhere("target", "source");
    for (const auto &pair : intermediateCRSAuthCodes) {
        params.emplace_back(pair.first);
        params.emplace_back(pair.second);
        params.emplace_back(pair.first);
        params.emplace_back(pair.second);
    }
    auto res =
        d->run(sql + additionalWhere + intermediateWhere + orderBy, params);

    const auto filterOutSuperseded = [](SQLResultSet &&resultSet) {
        std::set<std::pair<std::string, std::string>> setTransf1;
        std::set<std::pair<std::string, std::string>> setTransf2;
        for (const auto &row : resultSet) {
            // table1
            const auto &auth_name1 = row[1];
            const auto &code1 = row[2];
            // accuracy1
            // table2
            const auto &auth_name2 = row[5];
            const auto &code2 = row[6];
            setTransf1.insert(
                std::pair<std::string, std::string>(auth_name1, code1));
            setTransf2.insert(
                std::pair<std::string, std::string>(auth_name2, code2));
        }
        SQLResultSet filteredResultSet;
        for (const auto &row : resultSet) {
            const auto &replacement_auth_name1 = row[16];
            const auto &replacement_code1 = row[17];
            const auto &replacement_auth_name2 = row[18];
            const auto &replacement_code2 = row[19];
            if (!replacement_auth_name1.empty() &&
                setTransf1.find(std::pair<std::string, std::string>(
                    replacement_auth_name1, replacement_code1)) !=
                    setTransf1.end()) {
                // Skip transformations that are superseded by others that got
                // returned in the result set.
                continue;
            }
            if (!replacement_auth_name2.empty() &&
                setTransf2.find(std::pair<std::string, std::string>(
                    replacement_auth_name2, replacement_code2)) !=
                    setTransf2.end()) {
                // Skip transformations that are superseded by others that got
                // returned in the result set.
                continue;
            }
            filteredResultSet.emplace_back(row);
        }
        return filteredResultSet;
    };

    if (discardSuperseded) {
        res = filterOutSuperseded(std::move(res));
    }

    const auto checkPivot = [ETRFtoETRF, NAD83_CSRS_to_NAD83_CSRS, &sourceCRS,
                             &targetCRS](const crs::CRSPtr &intermediateCRS) {
        // Make sure that ETRF2000 to ETRF2014 doesn't go through ITRF9x or
        // ITRF>2014
        if (ETRFtoETRF && intermediateCRS &&
            starts_with(intermediateCRS->nameStr(), "ITRF")) {
            const auto normalizeDate = [](int v) {
                return (v >= 80 && v <= 99) ? v + 1900 : v;
            };
            const int srcDate = normalizeDate(
                atoi(sourceCRS->nameStr().c_str() + strlen("ETRF")));
            const int tgtDate = normalizeDate(
                atoi(targetCRS->nameStr().c_str() + strlen("ETRF")));
            const int intermDate = normalizeDate(
                atoi(intermediateCRS->nameStr().c_str() + strlen("ITRF")));
            if (srcDate > 0 && tgtDate > 0 && intermDate > 0) {
                if (intermDate < std::min(srcDate, tgtDate) ||
                    intermDate > std::max(srcDate, tgtDate)) {
                    return false;
                }
            }
        }

        // Make sure that NAD83(CSRS)[x] to NAD83(CSRS)[y) doesn't go through
        // NAD83 generic. Cf https://github.com/OSGeo/PROJ/issues/4464
        if (NAD83_CSRS_to_NAD83_CSRS && intermediateCRS &&
            (intermediateCRS->nameStr() == "NAD83" ||
             intermediateCRS->nameStr() == "WGS 84")) {
            return false;
        }

        return true;
    };

    for (const auto &row : res) {
        const auto &table1 = row[0];
        const auto &auth_name1 = row[1];
        const auto &code1 = row[2];
        // const auto &accuracy1 = row[3];
        const auto &table2 = row[4];
        const auto &auth_name2 = row[5];
        const auto &code2 = row[6];
        // const auto &accuracy2 = row[7];
        try {
            auto op1 =
                d->createFactory(auth_name1)
                    ->createCoordinateOperation(
                        code1, true, usePROJAlternativeGridNames, table1);
            if (useIrrelevantPivot(op1, sourceCRSAuthName, sourceCRSCode,
                                   targetCRSAuthName, targetCRSCode)) {
                continue;
            }
            if (!checkPivot(op1->targetCRS())) {
                continue;
            }
            auto op2 =
                d->createFactory(auth_name2)
                    ->createCoordinateOperation(
                        code2, true, usePROJAlternativeGridNames, table2);
            if (useIrrelevantPivot(op2, sourceCRSAuthName, sourceCRSCode,
                                   targetCRSAuthName, targetCRSCode)) {
                continue;
            }

            listTmp.emplace_back(
                operation::ConcatenatedOperation::createComputeMetadata(
                    {std::move(op1), std::move(op2)}, false));
        } catch (const std::exception &e) {
            // Mostly for debugging purposes when using an inconsistent
            // database
            if (getenv("PROJ_IGNORE_INSTANTIATION_ERRORS")) {
                fprintf(stderr, "Ignoring invalid operation: %s\n", e.what());
            } else {
                throw;
            }
        }
    }

    // Case (source->intermediate) and (target->intermediate)
    sql = sqlProlog +
          "ON v1.target_crs_auth_name = v2.target_crs_auth_name "
          "AND v1.target_crs_code = v2.target_crs_code " +
          joinArea +
          "WHERE v1.source_crs_auth_name = ? AND v1.source_crs_code = ? "
          "AND v2.source_crs_auth_name = ? AND v2.source_crs_code = ? ";
    if (allowedIntermediateObjectType == ObjectType::GEOGRAPHIC_CRS) {
        sql += criterionOnIntermediateCRS;
    }
    intermediateWhere = buildIntermediateWhere("target", "target");
    res = d->run(sql + additionalWhere + intermediateWhere + orderBy, params);
    if (discardSuperseded) {
        res = filterOutSuperseded(std::move(res));
    }

    for (const auto &row : res) {
        const auto &table1 = row[0];
        const auto &auth_name1 = row[1];
        const auto &code1 = row[2];
        // const auto &accuracy1 = row[3];
        const auto &table2 = row[4];
        const auto &auth_name2 = row[5];
        const auto &code2 = row[6];
        // const auto &accuracy2 = row[7];
        try {
            auto op1 =
                d->createFactory(auth_name1)
                    ->createCoordinateOperation(
                        code1, true, usePROJAlternativeGridNames, table1);
            if (useIrrelevantPivot(op1, sourceCRSAuthName, sourceCRSCode,
                                   targetCRSAuthName, targetCRSCode)) {
                continue;
            }
            if (!checkPivot(op1->targetCRS())) {
                continue;
            }
            auto op2 =
                d->createFactory(auth_name2)
                    ->createCoordinateOperation(
                        code2, true, usePROJAlternativeGridNames, table2);
            if (useIrrelevantPivot(op2, sourceCRSAuthName, sourceCRSCode,
                                   targetCRSAuthName, targetCRSCode)) {
                continue;
            }

            listTmp.emplace_back(
                operation::ConcatenatedOperation::createComputeMetadata(
                    {std::move(op1), op2->inverse()}, false));
        } catch (const std::exception &e) {
            // Mostly for debugging purposes when using an inconsistent
            // database
            if (getenv("PROJ_IGNORE_INSTANTIATION_ERRORS")) {
                fprintf(stderr, "Ignoring invalid operation: %s\n", e.what());
            } else {
                throw;
            }
        }
    }

    // Case (intermediate->source) and (intermediate->target)
    sql = sqlProlog +
          "ON v1.source_crs_auth_name = v2.source_crs_auth_name "
          "AND v1.source_crs_code = v2.source_crs_code " +
          joinArea +
          "WHERE v1.target_crs_auth_name = ? AND v1.target_crs_code = ? "
          "AND v2.target_crs_auth_name = ? AND v2.target_crs_code = ? ";
    if (allowedIntermediateObjectType == ObjectType::GEOGRAPHIC_CRS) {
        if (!minDate.empty()) {
            criterionOnIntermediateCRS =
                "AND EXISTS(SELECT 1 FROM geodetic_crs x "
                "JOIN geodetic_datum y "
                "ON "
                "y.auth_name = x.datum_auth_name AND "
                "y.code = x.datum_code "
                "WHERE "
                "x.auth_name = v1.source_crs_auth_name AND "
                "x.code = v1.source_crs_code AND "
                "x.type IN ('geographic 2D', 'geographic 3D') AND "
                "(y.publication_date IS NULL OR "
                "(y.publication_date >= '" +
                minDate + "'))) ";
        } else {
            criterionOnIntermediateCRS =
                "AND EXISTS(SELECT 1 FROM geodetic_crs x WHERE "
                "x.auth_name = v1.source_crs_auth_name AND "
                "x.code = v1.source_crs_code AND "
                "x.type IN ('geographic 2D', 'geographic 3D')) ";
        }
        sql += criterionOnIntermediateCRS;
    }
    intermediateWhere = buildIntermediateWhere("source", "source");
    res = d->run(sql + additionalWhere + intermediateWhere + orderBy, params);
    if (discardSuperseded) {
        res = filterOutSuperseded(std::move(res));
    }
    for (const auto &row : res) {
        const auto &table1 = row[0];
        const auto &auth_name1 = row[1];
        const auto &code1 = row[2];
        // const auto &accuracy1 = row[3];
        const auto &table2 = row[4];
        const auto &auth_name2 = row[5];
        const auto &code2 = row[6];
        // const auto &accuracy2 = row[7];
        try {
            auto op1 =
                d->createFactory(auth_name1)
                    ->createCoordinateOperation(
                        code1, true, usePROJAlternativeGridNames, table1);
            if (useIrrelevantPivot(op1, sourceCRSAuthName, sourceCRSCode,
                                   targetCRSAuthName, targetCRSCode)) {
                continue;
            }
            if (!checkPivot(op1->sourceCRS())) {
                continue;
            }
            auto op2 =
                d->createFactory(auth_name2)
                    ->createCoordinateOperation(
                        code2, true, usePROJAlternativeGridNames, table2);
            if (useIrrelevantPivot(op2, sourceCRSAuthName, sourceCRSCode,
                                   targetCRSAuthName, targetCRSCode)) {
                continue;
            }

            listTmp.emplace_back(
                operation::ConcatenatedOperation::createComputeMetadata(
                    {op1->inverse(), std::move(op2)}, false));
        } catch (const std::exception &e) {
            // Mostly for debugging purposes when using an inconsistent
            // database
            if (getenv("PROJ_IGNORE_INSTANTIATION_ERRORS")) {
                fprintf(stderr, "Ignoring invalid operation: %s\n", e.what());
            } else {
                throw;
            }
        }
    }

    // Case (intermediate->source) and (target->intermediate)
    sql = sqlProlog +
          "ON v1.source_crs_auth_name = v2.target_crs_auth_name "
          "AND v1.source_crs_code = v2.target_crs_code " +
          joinArea +
          "WHERE v1.target_crs_auth_name = ? AND v1.target_crs_code = ? "
          "AND v2.source_crs_auth_name = ? AND v2.source_crs_code = ? ";
    if (allowedIntermediateObjectType == ObjectType::GEOGRAPHIC_CRS) {
        sql += criterionOnIntermediateCRS;
    }
    intermediateWhere = buildIntermediateWhere("source", "target");
    res = d->run(sql + additionalWhere + intermediateWhere + orderBy, params);
    if (discardSuperseded) {
        res = filterOutSuperseded(std::move(res));
    }
    for (const auto &row : res) {
        const auto &table1 = row[0];
        const auto &auth_name1 = row[1];
        const auto &code1 = row[2];
        // const auto &accuracy1 = row[3];
        const auto &table2 = row[4];
        const auto &auth_name2 = row[5];
        const auto &code2 = row[6];
        // const auto &accuracy2 = row[7];
        try {
            auto op1 =
                d->createFactory(auth_name1)
                    ->createCoordinateOperation(
                        code1, true, usePROJAlternativeGridNames, table1);
            if (useIrrelevantPivot(op1, sourceCRSAuthName, sourceCRSCode,
                                   targetCRSAuthName, targetCRSCode)) {
                continue;
            }
            if (!checkPivot(op1->sourceCRS())) {
                continue;
            }
            auto op2 =
                d->createFactory(auth_name2)
                    ->createCoordinateOperation(
                        code2, true, usePROJAlternativeGridNames, table2);
            if (useIrrelevantPivot(op2, sourceCRSAuthName, sourceCRSCode,
                                   targetCRSAuthName, targetCRSCode)) {
                continue;
            }

            listTmp.emplace_back(
                operation::ConcatenatedOperation::createComputeMetadata(
                    {op1->inverse(), op2->inverse()}, false));
        } catch (const std::exception &e) {
            // Mostly for debugging purposes when using an inconsistent
            // database
            if (getenv("PROJ_IGNORE_INSTANTIATION_ERRORS")) {
                fprintf(stderr, "Ignoring invalid operation: %s\n", e.what());
            } else {
                throw;
            }
        }
    }

    std::vector<operation::CoordinateOperationNNPtr> list;
    for (const auto &op : listTmp) {
        if (!discardIfMissingGrid ||
            !d->rejectOpDueToMissingGrid(op, considerKnownGridsAsAvailable)) {
            list.emplace_back(op);
        }
    }

    return list;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

struct TrfmInfo {
    std::string situation{};
    std::string table_name{};
    std::string auth_name{};
    std::string code{};
    std::string name{};
    double west = 0;
    double south = 0;
    double east = 0;
    double north = 0;
};

// ---------------------------------------------------------------------------

std::vector<operation::CoordinateOperationNNPtr>
AuthorityFactory::createBetweenGeodeticCRSWithDatumBasedIntermediates(
    const crs::CRSNNPtr &sourceCRS, const std::string &sourceCRSAuthName,
    const std::string &sourceCRSCode, const crs::CRSNNPtr &targetCRS,
    const std::string &targetCRSAuthName, const std::string &targetCRSCode,
    bool usePROJAlternativeGridNames, bool discardIfMissingGrid,
    bool considerKnownGridsAsAvailable, bool discardSuperseded,
    const std::vector<std::string> &allowedAuthorities,
    const metadata::ExtentPtr &intersectingExtent1,
    const metadata::ExtentPtr &intersectingExtent2) const {

    std::vector<operation::CoordinateOperationNNPtr> listTmp;

    if (sourceCRSAuthName == targetCRSAuthName &&
        sourceCRSCode == targetCRSCode) {
        return listTmp;
    }
    const auto sourceGeodCRS =
        dynamic_cast<crs::GeodeticCRS *>(sourceCRS.get());
    const auto targetGeodCRS =
        dynamic_cast<crs::GeodeticCRS *>(targetCRS.get());
    if (!sourceGeodCRS || !targetGeodCRS) {
        return listTmp;
    }

    const bool NAD83_CSRS_to_NAD83_CSRS =
        starts_with(sourceGeodCRS->nameStr(), "NAD83(CSRS)") &&
        starts_with(targetGeodCRS->nameStr(), "NAD83(CSRS)");

    const auto GetListCRSWithSameDatum = [this](const crs::GeodeticCRS *crs,
                                                const std::string &crsAuthName,
                                                const std::string &crsCode) {
        // Find all geodetic CRS that share the same datum as the CRS
        SQLResultSet listCRS;

        const common::IdentifiedObject *obj = crs->datum().get();
        if (obj == nullptr)
            obj = crs->datumEnsemble().get();
        assert(obj != nullptr);
        const auto &ids = obj->identifiers();
        std::string datumAuthName;
        std::string datumCode;
        if (!ids.empty()) {
            const auto &id = ids.front();
            datumAuthName = *(id->codeSpace());
            datumCode = id->code();
        } else {
            const auto res =
                d->run("SELECT datum_auth_name, datum_code FROM "
                       "geodetic_crs WHERE auth_name = ? AND code = ?",
                       {crsAuthName, crsCode});
            if (res.size() != 1) {
                return listCRS;
            }
            const auto &row = res.front();
            datumAuthName = row[0];
            datumCode = row[1];
        }

        listCRS =
            d->run("SELECT auth_name, code FROM geodetic_crs WHERE "
                   "datum_auth_name = ? AND datum_code = ? AND deprecated = 0",
                   {datumAuthName, datumCode});
        if (listCRS.empty()) {
            // Can happen if the CRS is deprecated
            listCRS.emplace_back(SQLRow{crsAuthName, crsCode});
        }
        return listCRS;
    };

    const SQLResultSet listSourceCRS = GetListCRSWithSameDatum(
        sourceGeodCRS, sourceCRSAuthName, sourceCRSCode);
    const SQLResultSet listTargetCRS = GetListCRSWithSameDatum(
        targetGeodCRS, targetCRSAuthName, targetCRSCode);
    if (listSourceCRS.empty() || listTargetCRS.empty()) {
        // would happen only if we had CRS objects in the database without a
        // link to a datum.
        return listTmp;
    }

    ListOfParams params;
    const auto BuildSQLPart = [this, NAD83_CSRS_to_NAD83_CSRS,
                               &allowedAuthorities, &params, &listSourceCRS,
                               &listTargetCRS](bool isSourceCRS,
                                               bool selectOnTarget) {
        std::string situation;
        if (isSourceCRS)
            situation = "src";
        else
            situation = "tgt";
        if (selectOnTarget)
            situation += "_is_tgt";
        else
            situation += "_is_src";
        const std::string prefix1(selectOnTarget ? "source" : "target");
        const std::string prefix2(selectOnTarget ? "target" : "source");
        std::string sql("SELECT '");
        sql += situation;
        sql += "' as situation, v.table_name, v.auth_name, "
               "v.code, v.name, gcrs.datum_auth_name, gcrs.datum_code, "
               "a.west_lon, a.south_lat, a.east_lon, a.north_lat "
               "FROM coordinate_operation_view v "
               "JOIN geodetic_crs gcrs on gcrs.auth_name = ";
        sql += prefix1;
        sql += "_crs_auth_name AND gcrs.code = ";
        sql += prefix1;
        sql += "_crs_code "

               "LEFT JOIN usage u ON "
               "u.object_table_name = v.table_name AND "
               "u.object_auth_name = v.auth_name AND "
               "u.object_code = v.code "
               "LEFT JOIN extent a "
               "ON a.auth_name = u.extent_auth_name AND "
               "a.code = u.extent_code "
               "WHERE v.deprecated = 0 AND (";

        std::string cond;

        const auto &list = isSourceCRS ? listSourceCRS : listTargetCRS;
        for (const auto &row : list) {
            if (!cond.empty())
                cond += " OR ";
            cond += '(';
            cond += prefix2;
            cond += "_crs_auth_name = ? AND ";
            cond += prefix2;
            cond += "_crs_code = ?)";
            params.emplace_back(row[0]);
            params.emplace_back(row[1]);
        }

        sql += cond;
        sql += ") ";

        if (!allowedAuthorities.empty()) {
            sql += "AND v.auth_name IN (";
            for (size_t i = 0; i < allowedAuthorities.size(); i++) {
                if (i > 0)
                    sql += ',';
                sql += '?';
            }
            sql += ") ";
            for (const auto &allowedAuthority : allowedAuthorities) {
                params.emplace_back(allowedAuthority);
            }
        }
        if (d->hasAuthorityRestriction()) {
            sql += "AND v.auth_name = ? ";
            params.emplace_back(d->authority());
        }
        if (NAD83_CSRS_to_NAD83_CSRS) {
            // Make sure that NAD83(CSRS)[x] to NAD83(CSRS)[y) doesn't go
            // through NAD83 generic. Cf
            // https://github.com/OSGeo/PROJ/issues/4464
            sql += "AND gcrs.name NOT IN ('NAD83', 'WGS 84') ";
        }

        return sql;
    };

    std::string sql(BuildSQLPart(true, true));
    sql += "UNION ALL ";
    sql += BuildSQLPart(false, true);
    sql += "UNION ALL ";
    sql += BuildSQLPart(true, false);
    sql += "UNION ALL ";
    sql += BuildSQLPart(false, false);
    // fprintf(stderr, "sql : %s\n", sql.c_str());

    // Find all operations that have as source/target CRS a CRS that
    // share the same datum as the source or targetCRS
    const auto res = d->run(sql, params);

    std::map<std::string, std::list<TrfmInfo>> mapIntermDatumOfSource;
    std::map<std::string, std::list<TrfmInfo>> mapIntermDatumOfTarget;

    for (const auto &row : res) {
        try {
            TrfmInfo trfm;
            trfm.situation = row[0];
            trfm.table_name = row[1];
            trfm.auth_name = row[2];
            trfm.code = row[3];
            trfm.name = row[4];
            const auto &datum_auth_name = row[5];
            const auto &datum_code = row[6];
            trfm.west = c_locale_stod(row[7]);
            trfm.south = c_locale_stod(row[8]);
            trfm.east = c_locale_stod(row[9]);
            trfm.north = c_locale_stod(row[10]);
            const std::string key =
                std::string(datum_auth_name).append(":").append(datum_code);
            if (trfm.situation == "src_is_tgt" ||
                trfm.situation == "src_is_src")
                mapIntermDatumOfSource[key].emplace_back(std::move(trfm));
            else
                mapIntermDatumOfTarget[key].emplace_back(std::move(trfm));
        } catch (const std::exception &) {
        }
    }

    std::vector<const metadata::GeographicBoundingBox *> extraBbox;
    for (const auto &extent : {intersectingExtent1, intersectingExtent2}) {
        if (extent) {
            const auto &geogExtent = extent->geographicElements();
            if (geogExtent.size() == 1) {
                auto bbox =
                    dynamic_cast<const metadata::GeographicBoundingBox *>(
                        geogExtent[0].get());
                if (bbox) {
                    const double south_lat = bbox->southBoundLatitude();
                    const double west_lon = bbox->westBoundLongitude();
                    const double north_lat = bbox->northBoundLatitude();
                    const double east_lon = bbox->eastBoundLongitude();
                    if (south_lat != -90.0 || west_lon != -180.0 ||
                        north_lat != 90.0 || east_lon != 180.0) {
                        extraBbox.emplace_back(bbox);
                    }
                }
            }
        }
    }

    std::map<std::string, operation::CoordinateOperationPtr> oMapTrfmKeyToOp;
    std::list<std::pair<TrfmInfo, TrfmInfo>> candidates;
    std::map<std::string, TrfmInfo> setOfTransformations;

    const auto MakeKey = [](const TrfmInfo &trfm) {
        return trfm.table_name + '_' + trfm.auth_name + '_' + trfm.code;
    };

    // Find transformations that share a pivot datum, and do bbox filtering
    for (const auto &kvSource : mapIntermDatumOfSource) {
        const auto &listTrmfSource = kvSource.second;
        auto iter = mapIntermDatumOfTarget.find(kvSource.first);
        if (iter == mapIntermDatumOfTarget.end())
            continue;

        const auto &listTrfmTarget = iter->second;
        for (const auto &trfmSource : listTrmfSource) {
            auto bbox1 = metadata::GeographicBoundingBox::create(
                trfmSource.west, trfmSource.south, trfmSource.east,
                trfmSource.north);
            bool okBbox1 = true;
            for (const auto bbox : extraBbox)
                okBbox1 &= bbox->intersects(bbox1);
            if (!okBbox1)
                continue;

            const std::string key1 = MakeKey(trfmSource);

            for (const auto &trfmTarget : listTrfmTarget) {
                auto bbox2 = metadata::GeographicBoundingBox::create(
                    trfmTarget.west, trfmTarget.south, trfmTarget.east,
                    trfmTarget.north);
                if (!bbox1->intersects(bbox2))
                    continue;
                bool okBbox2 = true;
                for (const auto bbox : extraBbox)
                    okBbox2 &= bbox->intersects(bbox2);
                if (!okBbox2)
                    continue;

                operation::CoordinateOperationPtr op1;
                if (oMapTrfmKeyToOp.find(key1) == oMapTrfmKeyToOp.end()) {
                    auto op1NN = d->createFactory(trfmSource.auth_name)
                                     ->createCoordinateOperation(
                                         trfmSource.code, true,
                                         usePROJAlternativeGridNames,
                                         trfmSource.table_name);
                    op1 = op1NN.as_nullable();
                    if (useIrrelevantPivot(op1NN, sourceCRSAuthName,
                                           sourceCRSCode, targetCRSAuthName,
                                           targetCRSCode)) {
                        op1.reset();
                    }
                    oMapTrfmKeyToOp[key1] = op1;
                } else {
                    op1 = oMapTrfmKeyToOp[key1];
                }
                if (op1 == nullptr)
                    continue;

                const std::string key2 = MakeKey(trfmTarget);

                operation::CoordinateOperationPtr op2;
                if (oMapTrfmKeyToOp.find(key2) == oMapTrfmKeyToOp.end()) {
                    auto op2NN = d->createFactory(trfmTarget.auth_name)
                                     ->createCoordinateOperation(
                                         trfmTarget.code, true,
                                         usePROJAlternativeGridNames,
                                         trfmTarget.table_name);
                    op2 = op2NN.as_nullable();
                    if (useIrrelevantPivot(op2NN, sourceCRSAuthName,
                                           sourceCRSCode, targetCRSAuthName,
                                           targetCRSCode)) {
                        op2.reset();
                    }
                    oMapTrfmKeyToOp[key2] = op2;
                } else {
                    op2 = oMapTrfmKeyToOp[key2];
                }
                if (op2 == nullptr)
                    continue;

                candidates.emplace_back(
                    std::pair<TrfmInfo, TrfmInfo>(trfmSource, trfmTarget));
                setOfTransformations[key1] = trfmSource;
                setOfTransformations[key2] = trfmTarget;
            }
        }
    }

    std::set<std::string> setSuperseded;
    if (discardSuperseded && !setOfTransformations.empty()) {
        std::string findSupersededSql(
            "SELECT superseded_table_name, "
            "superseded_auth_name, superseded_code, "
            "replacement_auth_name, replacement_code "
            "FROM supersession WHERE same_source_target_crs = 1 AND (");
        bool findSupersededFirstWhere = true;
        ListOfParams findSupersededParams;

        const auto keyMapSupersession = [](const std::string &table_name,
                                           const std::string &auth_name,
                                           const std::string &code) {
            return table_name + auth_name + code;
        };

        std::set<std::pair<std::string, std::string>> setTransf;
        for (const auto &kv : setOfTransformations) {
            const auto &table = kv.second.table_name;
            const auto &auth_name = kv.second.auth_name;
            const auto &code = kv.second.code;

            if (!findSupersededFirstWhere)
                findSupersededSql += " OR ";
            findSupersededFirstWhere = false;
            findSupersededSql +=
                "(superseded_table_name = ? AND replacement_table_name = "
                "superseded_table_name AND superseded_auth_name = ? AND "
                "superseded_code = ?)";
            findSupersededParams.push_back(table);
            findSupersededParams.push_back(auth_name);
            findSupersededParams.push_back(code);

            setTransf.insert(
                std::pair<std::string, std::string>(auth_name, code));
        }
        findSupersededSql += ')';

        std::map<std::string, std::vector<std::pair<std::string, std::string>>>
            mapSupersession;

        const auto resSuperseded =
            d->run(findSupersededSql, findSupersededParams);
        for (const auto &row : resSuperseded) {
            const auto &superseded_table_name = row[0];
            const auto &superseded_auth_name = row[1];
            const auto &superseded_code = row[2];
            const auto &replacement_auth_name = row[3];
            const auto &replacement_code = row[4];
            mapSupersession[keyMapSupersession(superseded_table_name,
                                               superseded_auth_name,
                                               superseded_code)]
                .push_back(std::pair<std::string, std::string>(
                    replacement_auth_name, replacement_code));
        }

        for (const auto &kv : setOfTransformations) {
            const auto &table = kv.second.table_name;
            const auto &auth_name = kv.second.auth_name;
            const auto &code = kv.second.code;

            const auto iter = mapSupersession.find(
                keyMapSupersession(table, auth_name, code));
            if (iter != mapSupersession.end()) {
                bool foundReplacement = false;
                for (const auto &replacement : iter->second) {
                    const auto &replacement_auth_name = replacement.first;
                    const auto &replacement_code = replacement.second;
                    if (setTransf.find(std::pair<std::string, std::string>(
                            replacement_auth_name, replacement_code)) !=
                        setTransf.end()) {
                        // Skip transformations that are superseded by others
                        // that got
                        // returned in the result set.
                        foundReplacement = true;
                        break;
                    }
                }
                if (foundReplacement) {
                    setSuperseded.insert(kv.first);
                }
            }
        }
    }

    std::string sourceDatumPubDate;
    const auto sourceDatum = sourceGeodCRS->datumNonNull(d->context());
    if (sourceDatum->publicationDate().has_value()) {
        sourceDatumPubDate = sourceDatum->publicationDate()->toString();
    }

    std::string targetDatumPubDate;
    const auto targetDatum = targetGeodCRS->datumNonNull(d->context());
    if (targetDatum->publicationDate().has_value()) {
        targetDatumPubDate = targetDatum->publicationDate()->toString();
    }

    const std::string mostAncientDatumPubDate =
        (!targetDatumPubDate.empty() &&
         (sourceDatumPubDate.empty() ||
          targetDatumPubDate < sourceDatumPubDate))
            ? targetDatumPubDate
            : sourceDatumPubDate;

    auto opFactory = operation::CoordinateOperationFactory::create();
    for (const auto &pair : candidates) {
        const auto &trfmSource = pair.first;
        const auto &trfmTarget = pair.second;
        const std::string key1 = MakeKey(trfmSource);
        const std::string key2 = MakeKey(trfmTarget);
        if (setSuperseded.find(key1) != setSuperseded.end() ||
            setSuperseded.find(key2) != setSuperseded.end()) {
            continue;
        }
        auto op1 = oMapTrfmKeyToOp[key1];
        auto op2 = oMapTrfmKeyToOp[key2];
        auto op1NN = NN_NO_CHECK(op1);
        auto op2NN = NN_NO_CHECK(op2);
        if (trfmSource.situation == "src_is_tgt")
            op1NN = op1NN->inverse();
        if (trfmTarget.situation == "tgt_is_src")
            op2NN = op2NN->inverse();

        const auto &op1Source = op1NN->sourceCRS();
        const auto &op1Target = op1NN->targetCRS();
        const auto &op2Source = op2NN->sourceCRS();
        const auto &op2Target = op2NN->targetCRS();
        if (!(op1Source && op1Target && op2Source && op2Target)) {
            continue;
        }

        // Skip operations using a datum that is older than the source or
        // target datum (e.g to avoid ED50 to WGS84 to go through NTF)
        if (!mostAncientDatumPubDate.empty()) {
            const auto isOlderCRS = [this, &mostAncientDatumPubDate](
                                        const crs::CRSPtr &crs) {
                const auto geogCRS =
                    dynamic_cast<const crs::GeodeticCRS *>(crs.get());
                if (geogCRS) {
                    const auto datum = geogCRS->datumNonNull(d->context());
                    // Hum, theoretically we'd want to check
                    // datum->publicationDate()->toString() <
                    // mostAncientDatumPubDate but that would exclude doing
                    // IG05/12 Intermediate CRS to ITRF2014 through ITRF2008,
                    // since IG05/12 Intermediate CRS has been published later
                    // than ITRF2008. So use a cut of date for ancient vs
                    // "modern" era.
                    constexpr const char *CUT_OFF_DATE = "1900-01-01";
                    if (datum->publicationDate().has_value() &&
                        datum->publicationDate()->toString() < CUT_OFF_DATE &&
                        mostAncientDatumPubDate > CUT_OFF_DATE) {
                        return true;
                    }
                }
                return false;
            };

            if (isOlderCRS(op1Source) || isOlderCRS(op1Target) ||
                isOlderCRS(op2Source) || isOlderCRS(op2Target))
                continue;
        }

        std::vector<operation::CoordinateOperationNNPtr> steps;

        if (!sourceCRS->isEquivalentTo(
                op1Source.get(), util::IComparable::Criterion::EQUIVALENT)) {
            auto opFirst =
                opFactory->createOperation(sourceCRS, NN_NO_CHECK(op1Source));
            assert(opFirst);
            steps.emplace_back(NN_NO_CHECK(opFirst));
        }

        steps.emplace_back(op1NN);

        if (!op1Target->isEquivalentTo(
                op2Source.get(), util::IComparable::Criterion::EQUIVALENT)) {
            auto opMiddle = opFactory->createOperation(NN_NO_CHECK(op1Target),
                                                       NN_NO_CHECK(op2Source));
            assert(opMiddle);
            steps.emplace_back(NN_NO_CHECK(opMiddle));
        }

        steps.emplace_back(op2NN);

        if (!op2Target->isEquivalentTo(
                targetCRS.get(), util::IComparable::Criterion::EQUIVALENT)) {
            auto opLast =
                opFactory->createOperation(NN_NO_CHECK(op2Target), targetCRS);
            assert(opLast);
            steps.emplace_back(NN_NO_CHECK(opLast));
        }

        listTmp.emplace_back(
            operation::ConcatenatedOperation::createComputeMetadata(steps,
                                                                    false));
    }

    std::vector<operation::CoordinateOperationNNPtr> list;
    for (const auto &op : listTmp) {
        if (!discardIfMissingGrid ||
            !d->rejectOpDueToMissingGrid(op, considerKnownGridsAsAvailable)) {
            list.emplace_back(op);
        }
    }

    return list;
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns the authority name associated to this factory.
 * @return name.
 */
const std::string &AuthorityFactory::getAuthority() PROJ_PURE_DEFN {
    return d->authority();
}

// ---------------------------------------------------------------------------

/** \brief Returns the set of authority codes of the given object type.
 *
 * @param type Object type.
 * @param allowDeprecated whether we should return deprecated objects as well.
 * @return the set of authority codes for spatial reference objects of the given
 * type
 * @throw FactoryException in case of error.
 */
std::set<std::string>
AuthorityFactory::getAuthorityCodes(const ObjectType &type,
                                    bool allowDeprecated) const {
    std::string sql;
    switch (type) {
    case ObjectType::PRIME_MERIDIAN:
        sql = "SELECT code FROM prime_meridian WHERE ";
        break;
    case ObjectType::ELLIPSOID:
        sql = "SELECT code FROM ellipsoid WHERE ";
        break;
    case ObjectType::DATUM:
        sql = "SELECT code FROM object_view WHERE table_name IN "
              "('geodetic_datum', 'vertical_datum', 'engineering_datum') AND ";
        break;
    case ObjectType::GEODETIC_REFERENCE_FRAME:
        sql = "SELECT code FROM geodetic_datum WHERE ";
        break;
    case ObjectType::DYNAMIC_GEODETIC_REFERENCE_FRAME:
        sql = "SELECT code FROM geodetic_datum WHERE "
              "frame_reference_epoch IS NOT NULL AND ";
        break;
    case ObjectType::VERTICAL_REFERENCE_FRAME:
        sql = "SELECT code FROM vertical_datum WHERE ";
        break;
    case ObjectType::DYNAMIC_VERTICAL_REFERENCE_FRAME:
        sql = "SELECT code FROM vertical_datum WHERE "
              "frame_reference_epoch IS NOT NULL AND ";
        break;
    case ObjectType::ENGINEERING_DATUM:
        sql = "SELECT code FROM engineering_datum WHERE ";
        break;
    case ObjectType::CRS:
        sql = "SELECT code FROM crs_view WHERE ";
        break;
    case ObjectType::GEODETIC_CRS:
        sql = "SELECT code FROM geodetic_crs WHERE ";
        break;
    case ObjectType::GEOCENTRIC_CRS:
        sql = "SELECT code FROM geodetic_crs WHERE type "
              "= " GEOCENTRIC_SINGLE_QUOTED " AND ";
        break;
    case ObjectType::GEOGRAPHIC_CRS:
        sql = "SELECT code FROM geodetic_crs WHERE type IN "
              "(" GEOG_2D_SINGLE_QUOTED "," GEOG_3D_SINGLE_QUOTED ") AND ";
        break;
    case ObjectType::GEOGRAPHIC_2D_CRS:
        sql =
            "SELECT code FROM geodetic_crs WHERE type = " GEOG_2D_SINGLE_QUOTED
            " AND ";
        break;
    case ObjectType::GEOGRAPHIC_3D_CRS:
        sql =
            "SELECT code FROM geodetic_crs WHERE type = " GEOG_3D_SINGLE_QUOTED
            " AND ";
        break;
    case ObjectType::VERTICAL_CRS:
        sql = "SELECT code FROM vertical_crs WHERE ";
        break;
    case ObjectType::PROJECTED_CRS:
        sql = "SELECT code FROM projected_crs WHERE ";
        break;
    case ObjectType::COMPOUND_CRS:
        sql = "SELECT code FROM compound_crs WHERE ";
        break;
    case ObjectType::ENGINEERING_CRS:
        sql = "SELECT code FROM engineering_crs WHERE ";
        break;
    case ObjectType::COORDINATE_OPERATION:
        sql =
            "SELECT code FROM coordinate_operation_with_conversion_view WHERE ";
        break;
    case ObjectType::CONVERSION:
        sql = "SELECT code FROM conversion WHERE ";
        break;
    case ObjectType::TRANSFORMATION:
        sql = "SELECT code FROM coordinate_operation_view WHERE table_name != "
              "'concatenated_operation' AND ";
        break;
    case ObjectType::CONCATENATED_OPERATION:
        sql = "SELECT code FROM concatenated_operation WHERE ";
        break;
    case ObjectType::DATUM_ENSEMBLE:
        sql = "SELECT code FROM object_view WHERE table_name IN "
              "('geodetic_datum', 'vertical_datum') AND "
              "type = 'ensemble' AND ";
        break;
    }

    sql += "auth_name = ?";
    if (!allowDeprecated) {
        sql += " AND deprecated = 0";
    }

    auto res = d->run(sql, {d->authority()});
    std::set<std::string> set;
    for (const auto &row : res) {
        set.insert(row[0]);
    }
    return set;
}

// ---------------------------------------------------------------------------

/** \brief Gets a description of the object corresponding to a code.
 *
 * \note In case of several objects of different types with the same code,
 * one of them will be arbitrarily selected. But if a CRS object is found, it
 * will be selected.
 *
 * @param code Object code allocated by authority. (e.g. "4326")
 * @return description.
 * @throw NoSuchAuthorityCodeException if there is no matching object.
 * @throw FactoryException in case of other errors.
 */
std::string
AuthorityFactory::getDescriptionText(const std::string &code) const {
    auto sql = "SELECT name, table_name FROM object_view WHERE auth_name = ? "
               "AND code = ? ORDER BY table_name";
    auto sqlRes = d->runWithCodeParam(sql, code);
    if (sqlRes.empty()) {
        throw NoSuchAuthorityCodeException("object not found", d->authority(),
                                           code);
    }
    std::string text;
    for (const auto &row : sqlRes) {
        const auto &tableName = row[1];
        if (tableName == "geodetic_crs" || tableName == "projected_crs" ||
            tableName == "vertical_crs" || tableName == "compound_crs" ||
            tableName == "engineering_crs") {
            return row[0];
        } else if (text.empty()) {
            text = row[0];
        }
    }
    return text;
}

// ---------------------------------------------------------------------------

/** \brief Return a list of information on CRS objects
 *
 * This is functionally equivalent of listing the codes from an authority,
 * instantiating
 * a CRS object for each of them and getting the information from this CRS
 * object, but this implementation has much less overhead.
 *
 * @throw FactoryException in case of error.
 */
std::list<AuthorityFactory::CRSInfo> AuthorityFactory::getCRSInfoList() const {

    const auto getSqlArea = [](const char *table_name) {
        std::string sql("LEFT JOIN usage u ON u.object_table_name = '");
        sql += table_name;
        sql += "' AND "
               "u.object_auth_name = c.auth_name AND "
               "u.object_code = c.code "
               "LEFT JOIN extent a "
               "ON a.auth_name = u.extent_auth_name AND "
               "a.code = u.extent_code ";
        return sql;
    };

    const auto getJoinCelestialBody = [](const char *crs_alias) {
        std::string sql("LEFT JOIN geodetic_datum gd ON gd.auth_name = ");
        sql += crs_alias;
        sql += ".datum_auth_name AND gd.code = ";
        sql += crs_alias;
        sql += ".datum_code "
               "LEFT JOIN ellipsoid e ON e.auth_name = gd.ellipsoid_auth_name "
               "AND e.code = gd.ellipsoid_code "
               "LEFT JOIN celestial_body cb ON "
               "cb.auth_name = e.celestial_body_auth_name "
               "AND cb.code = e.celestial_body_code ";
        return sql;
    };

    std::string sql = "SELECT * FROM ("
                      "SELECT c.auth_name, c.code, c.name, c.type, "
                      "c.deprecated, "
                      "a.west_lon, a.south_lat, a.east_lon, a.north_lat, "
                      "a.description, NULL, cb.name FROM geodetic_crs c ";
    sql += getSqlArea("geodetic_crs");
    sql += getJoinCelestialBody("c");
    ListOfParams params;
    if (d->hasAuthorityRestriction()) {
        sql += "WHERE c.auth_name = ? ";
        params.emplace_back(d->authority());
    }
    sql += "UNION ALL SELECT c.auth_name, c.code, c.name, 'projected', "
           "c.deprecated, "
           "a.west_lon, a.south_lat, a.east_lon, a.north_lat, "
           "a.description, cm.name, cb.name AS conversion_method_name FROM "
           "projected_crs c "
           "LEFT JOIN conversion_table conv ON "
           "c.conversion_auth_name = conv.auth_name AND "
           "c.conversion_code = conv.code "
           "LEFT JOIN conversion_method cm ON "
           "conv.method_auth_name = cm.auth_name AND "
           "conv.method_code = cm.code "
           "LEFT JOIN geodetic_crs gcrs ON "
           "gcrs.auth_name = c.geodetic_crs_auth_name "
           "AND gcrs.code = c.geodetic_crs_code ";
    sql += getSqlArea("projected_crs");
    sql += getJoinCelestialBody("gcrs");
    if (d->hasAuthorityRestriction()) {
        sql += "WHERE c.auth_name = ? ";
        params.emplace_back(d->authority());
    }
    // FIXME: we can't handle non-EARTH vertical CRS for now
    sql += "UNION ALL SELECT c.auth_name, c.code, c.name, 'vertical', "
           "c.deprecated, "
           "a.west_lon, a.south_lat, a.east_lon, a.north_lat, "
           "a.description, NULL, 'Earth' FROM vertical_crs c ";
    sql += getSqlArea("vertical_crs");
    if (d->hasAuthorityRestriction()) {
        sql += "WHERE c.auth_name = ? ";
        params.emplace_back(d->authority());
    }
    // FIXME: we can't handle non-EARTH compound CRS for now
    sql += "UNION ALL SELECT c.auth_name, c.code, c.name, 'compound', "
           "c.deprecated, "
           "a.west_lon, a.south_lat, a.east_lon, a.north_lat, "
           "a.description, NULL, 'Earth' FROM compound_crs c ";
    sql += getSqlArea("compound_crs");
    if (d->hasAuthorityRestriction()) {
        sql += "WHERE c.auth_name = ? ";
        params.emplace_back(d->authority());
    }
    // FIXME: we can't handle non-EARTH compound CRS for now
    sql += "UNION ALL SELECT c.auth_name, c.code, c.name, 'engineering', "
           "c.deprecated, "
           "a.west_lon, a.south_lat, a.east_lon, a.north_lat, "
           "a.description, NULL, 'Earth' FROM engineering_crs c ";
    sql += getSqlArea("engineering_crs");
    if (d->hasAuthorityRestriction()) {
        sql += "WHERE c.auth_name = ? ";
        params.emplace_back(d->authority());
    }
    sql += ") r ORDER BY auth_name, code";
    auto sqlRes = d->run(sql, params);
    std::list<AuthorityFactory::CRSInfo> res;
    for (const auto &row : sqlRes) {
        AuthorityFactory::CRSInfo info;
        info.authName = row[0];
        info.code = row[1];
        info.name = row[2];
        const auto &type = row[3];
        if (type == GEOG_2D) {
            info.type = AuthorityFactory::ObjectType::GEOGRAPHIC_2D_CRS;
        } else if (type == GEOG_3D) {
            info.type = AuthorityFactory::ObjectType::GEOGRAPHIC_3D_CRS;
        } else if (type == GEOCENTRIC) {
            info.type = AuthorityFactory::ObjectType::GEOCENTRIC_CRS;
        } else if (type == OTHER) {
            info.type = AuthorityFactory::ObjectType::GEODETIC_CRS;
        } else if (type == PROJECTED) {
            info.type = AuthorityFactory::ObjectType::PROJECTED_CRS;
        } else if (type == VERTICAL) {
            info.type = AuthorityFactory::ObjectType::VERTICAL_CRS;
        } else if (type == COMPOUND) {
            info.type = AuthorityFactory::ObjectType::COMPOUND_CRS;
        } else if (type == ENGINEERING) {
            info.type = AuthorityFactory::ObjectType::ENGINEERING_CRS;
        }
        info.deprecated = row[4] == "1";
        if (row[5].empty()) {
            info.bbox_valid = false;
        } else {
            info.bbox_valid = true;
            info.west_lon_degree = c_locale_stod(row[5]);
            info.south_lat_degree = c_locale_stod(row[6]);
            info.east_lon_degree = c_locale_stod(row[7]);
            info.north_lat_degree = c_locale_stod(row[8]);
        }
        info.areaName = row[9];
        info.projectionMethodName = row[10];
        info.celestialBodyName = row[11];
        res.emplace_back(info);
    }
    return res;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
AuthorityFactory::UnitInfo::UnitInfo()
    : authName{}, code{}, name{}, category{}, convFactor{}, projShortName{},
      deprecated{} {}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
AuthorityFactory::CelestialBodyInfo::CelestialBodyInfo() : authName{}, name{} {}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the list of units.
 * @throw FactoryException in case of error.
 *
 * @since 7.1
 */
std::list<AuthorityFactory::UnitInfo> AuthorityFactory::getUnitList() const {
    std::string sql = "SELECT auth_name, code, name, type, conv_factor, "
                      "proj_short_name, deprecated FROM unit_of_measure";
    ListOfParams params;
    if (d->hasAuthorityRestriction()) {
        sql += " WHERE auth_name = ?";
        params.emplace_back(d->authority());
    }
    sql += " ORDER BY auth_name, code";

    auto sqlRes = d->run(sql, params);
    std::list<AuthorityFactory::UnitInfo> res;
    for (const auto &row : sqlRes) {
        AuthorityFactory::UnitInfo info;
        info.authName = row[0];
        info.code = row[1];
        info.name = row[2];
        const std::string &raw_category(row[3]);
        if (raw_category == "length") {
            info.category = info.name.find(" per ") != std::string::npos
                                ? "linear_per_time"
                                : "linear";
        } else if (raw_category == "angle") {
            info.category = info.name.find(" per ") != std::string::npos
                                ? "angular_per_time"
                                : "angular";
        } else if (raw_category == "scale") {
            info.category =
                info.name.find(" per year") != std::string::npos ||
                        info.name.find(" per second") != std::string::npos
                    ? "scale_per_time"
                    : "scale";
        } else {
            info.category = raw_category;
        }
        info.convFactor = row[4].empty() ? 0 : c_locale_stod(row[4]);
        info.projShortName = row[5];
        info.deprecated = row[6] == "1";
        res.emplace_back(info);
    }
    return res;
}

// ---------------------------------------------------------------------------

/** \brief Return the list of celestial bodies.
 * @throw FactoryException in case of error.
 *
 * @since 8.1
 */
std::list<AuthorityFactory::CelestialBodyInfo>
AuthorityFactory::getCelestialBodyList() const {
    std::string sql = "SELECT auth_name, name FROM celestial_body";
    ListOfParams params;
    if (d->hasAuthorityRestriction()) {
        sql += " WHERE auth_name = ?";
        params.emplace_back(d->authority());
    }
    sql += " ORDER BY auth_name, name";

    auto sqlRes = d->run(sql, params);
    std::list<AuthorityFactory::CelestialBodyInfo> res;
    for (const auto &row : sqlRes) {
        AuthorityFactory::CelestialBodyInfo info;
        info.authName = row[0];
        info.name = row[1];
        res.emplace_back(info);
    }
    return res;
}

// ---------------------------------------------------------------------------

/** \brief Gets the official name from a possibly alias name.
 *
 * @param aliasedName Alias name.
 * @param tableName Table name/category. Can help in case of ambiguities.
 * Or empty otherwise.
 * @param source Source of the alias. Can help in case of ambiguities.
 * Or empty otherwise.
 * @param tryEquivalentNameSpelling whether the comparison of aliasedName with
 * the alt_name column of the alias_name table should be done with using
 * metadata::Identifier::isEquivalentName() rather than strict string
 * comparison;
 * @param outTableName Table name in which the official name has been found.
 * @param outAuthName Authority name of the official name that has been found.
 * @param outCode Code of the official name that has been found.
 * @return official name (or empty if not found).
 * @throw FactoryException in case of error.
 */
std::string AuthorityFactory::getOfficialNameFromAlias(
    const std::string &aliasedName, const std::string &tableName,
    const std::string &source, bool tryEquivalentNameSpelling,
    std::string &outTableName, std::string &outAuthName,
    std::string &outCode) const {

    if (tryEquivalentNameSpelling) {
        std::string sql(
            "SELECT table_name, auth_name, code, alt_name FROM alias_name");
        ListOfParams params;
        if (!tableName.empty()) {
            sql += " WHERE table_name = ?";
            params.push_back(tableName);
        }
        if (!source.empty()) {
            if (!tableName.empty()) {
                sql += " AND ";
            } else {
                sql += " WHERE ";
            }
            sql += "source = ?";
            params.push_back(source);
        }
        auto res = d->run(sql, params);
        if (res.empty()) {
            return std::string();
        }
        for (const auto &row : res) {
            const auto &alt_name = row[3];
            if (metadata::Identifier::isEquivalentName(alt_name.c_str(),
                                                       aliasedName.c_str())) {
                outTableName = row[0];
                outAuthName = row[1];
                outCode = row[2];
                sql = "SELECT name FROM \"";
                sql += replaceAll(outTableName, "\"", "\"\"");
                sql += "\" WHERE auth_name = ? AND code = ?";
                res = d->run(sql, {outAuthName, outCode});
                if (res.empty()) { // shouldn't happen normally
                    return std::string();
                }
                return res.front()[0];
            }
        }
        return std::string();
    } else {
        std::string sql(
            "SELECT table_name, auth_name, code FROM alias_name WHERE "
            "alt_name = ?");
        ListOfParams params{aliasedName};
        if (!tableName.empty()) {
            sql += " AND table_name = ?";
            params.push_back(tableName);
        }
        if (!source.empty()) {
            if (source == "ESRI") {
                sql += " AND source IN ('ESRI', 'ESRI_OLD')";
            } else {
                sql += " AND source = ?";
                params.push_back(source);
            }
        }
        auto res = d->run(sql, params);
        if (res.empty()) {
            return std::string();
        }

        params.clear();
        sql.clear();
        bool first = true;
        for (const auto &row : res) {
            if (!first)
                sql += " UNION ALL ";
            first = false;
            outTableName = row[0];
            outAuthName = row[1];
            outCode = row[2];
            sql += "SELECT name, ? AS table_name, auth_name, code, deprecated "
                   "FROM \"";
            sql += replaceAll(outTableName, "\"", "\"\"");
            sql += "\" WHERE auth_name = ? AND code = ?";
            params.emplace_back(outTableName);
            params.emplace_back(outAuthName);
            params.emplace_back(outCode);
        }
        sql = "SELECT name, table_name, auth_name, code FROM (" + sql +
              ") x ORDER BY deprecated LIMIT 1";
        res = d->run(sql, params);
        if (res.empty()) { // shouldn't happen normally
            return std::string();
        }
        const auto &row = res.front();
        outTableName = row[1];
        outAuthName = row[2];
        outCode = row[3];
        return row[0];
    }
}

// ---------------------------------------------------------------------------

/** \brief Return a list of objects, identified by their name
 *
 * @param searchedName Searched name. Must be at least 2 character long.
 * @param allowedObjectTypes List of object types into which to search. If
 * empty, all object types will be searched.
 * @param approximateMatch Whether approximate name identification is allowed.
 * @param limitResultCount Maximum number of results to return.
 * Or 0 for unlimited.
 * @return list of matched objects.
 * @throw FactoryException in case of error.
 */
std::list<common::IdentifiedObjectNNPtr>
AuthorityFactory::createObjectsFromName(
    const std::string &searchedName,
    const std::vector<ObjectType> &allowedObjectTypes, bool approximateMatch,
    size_t limitResultCount) const {
    std::list<common::IdentifiedObjectNNPtr> res;
    const auto resTmp(createObjectsFromNameEx(
        searchedName, allowedObjectTypes, approximateMatch, limitResultCount));
    for (const auto &pair : resTmp) {
        res.emplace_back(pair.first);
    }
    return res;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

/** \brief Return a list of objects, identifier by their name, with the name
 * on which the match occurred.
 *
 * The name on which the match occurred might be different from the object name,
 * if the match has been done on an alias name of that object.
 *
 * @param searchedName Searched name. Must be at least 2 character long.
 * @param allowedObjectTypes List of object types into which to search. If
 * empty, all object types will be searched.
 * @param approximateMatch Whether approximate name identification is allowed.
 * @param limitResultCount Maximum number of results to return.
 * Or 0 for unlimited.
 * @return list of matched objects.
 * @throw FactoryException in case of error.
 */
std::list<AuthorityFactory::PairObjectName>
AuthorityFactory::createObjectsFromNameEx(
    const std::string &searchedName,
    const std::vector<ObjectType> &allowedObjectTypes, bool approximateMatch,
    size_t limitResultCount) const {
    std::string searchedNameWithoutDeprecated(searchedName);
    bool deprecated = false;
    if (ends_with(searchedNameWithoutDeprecated, " (deprecated)")) {
        deprecated = true;
        searchedNameWithoutDeprecated.resize(
            searchedNameWithoutDeprecated.size() - strlen(" (deprecated)"));
    }

    const std::string canonicalizedSearchedName(
        metadata::Identifier::canonicalizeName(searchedNameWithoutDeprecated));
    if (canonicalizedSearchedName.size() <= 1) {
        return {};
    }

    std::string sql(
        "SELECT table_name, auth_name, code, name, deprecated, is_alias "
        "FROM (");

    const auto getTableAndTypeConstraints = [&allowedObjectTypes,
                                             &searchedName]() {
        typedef std::pair<std::string, std::string> TableType;
        std::list<TableType> res;
        // Hide ESRI D_ vertical datums
        const bool startsWithDUnderscore = starts_with(searchedName, "D_");
        if (allowedObjectTypes.empty()) {
            for (const auto &tableName :
                 {"prime_meridian", "ellipsoid", "geodetic_datum",
                  "vertical_datum", "engineering_datum", "geodetic_crs",
                  "projected_crs", "vertical_crs", "compound_crs",
                  "engineering_crs", "conversion", "helmert_transformation",
                  "grid_transformation", "other_transformation",
                  "concatenated_operation"}) {
                if (!(startsWithDUnderscore &&
                      strcmp(tableName, "vertical_datum") == 0)) {
                    res.emplace_back(TableType(tableName, std::string()));
                }
            }
        } else {
            for (const auto type : allowedObjectTypes) {
                switch (type) {
                case ObjectType::PRIME_MERIDIAN:
                    res.emplace_back(
                        TableType("prime_meridian", std::string()));
                    break;
                case ObjectType::ELLIPSOID:
                    res.emplace_back(TableType("ellipsoid", std::string()));
                    break;
                case ObjectType::DATUM:
                    res.emplace_back(
                        TableType("geodetic_datum", std::string()));
                    if (!startsWithDUnderscore) {
                        res.emplace_back(
                            TableType("vertical_datum", std::string()));
                        res.emplace_back(
                            TableType("engineering_datum", std::string()));
                    }
                    break;
                case ObjectType::GEODETIC_REFERENCE_FRAME:
                    res.emplace_back(
                        TableType("geodetic_datum", std::string()));
                    break;
                case ObjectType::DYNAMIC_GEODETIC_REFERENCE_FRAME:
                    res.emplace_back(
                        TableType("geodetic_datum", "frame_reference_epoch"));
                    break;
                case ObjectType::VERTICAL_REFERENCE_FRAME:
                    res.emplace_back(
                        TableType("vertical_datum", std::string()));
                    break;
                case ObjectType::DYNAMIC_VERTICAL_REFERENCE_FRAME:
                    res.emplace_back(
                        TableType("vertical_datum", "frame_reference_epoch"));
                    break;
                case ObjectType::ENGINEERING_DATUM:
                    res.emplace_back(
                        TableType("engineering_datum", std::string()));
                    break;
                case ObjectType::CRS:
                    res.emplace_back(TableType("geodetic_crs", std::string()));
                    res.emplace_back(TableType("projected_crs", std::string()));
                    res.emplace_back(TableType("vertical_crs", std::string()));
                    res.emplace_back(TableType("compound_crs", std::string()));
                    res.emplace_back(
                        TableType("engineering_crs", std::string()));
                    break;
                case ObjectType::GEODETIC_CRS:
                    res.emplace_back(TableType("geodetic_crs", std::string()));
                    break;
                case ObjectType::GEOCENTRIC_CRS:
                    res.emplace_back(TableType("geodetic_crs", GEOCENTRIC));
                    break;
                case ObjectType::GEOGRAPHIC_CRS:
                    res.emplace_back(TableType("geodetic_crs", GEOG_2D));
                    res.emplace_back(TableType("geodetic_crs", GEOG_3D));
                    break;
                case ObjectType::GEOGRAPHIC_2D_CRS:
                    res.emplace_back(TableType("geodetic_crs", GEOG_2D));
                    break;
                case ObjectType::GEOGRAPHIC_3D_CRS:
                    res.emplace_back(TableType("geodetic_crs", GEOG_3D));
                    break;
                case ObjectType::PROJECTED_CRS:
                    res.emplace_back(TableType("projected_crs", std::string()));
                    break;
                case ObjectType::VERTICAL_CRS:
                    res.emplace_back(TableType("vertical_crs", std::string()));
                    break;
                case ObjectType::COMPOUND_CRS:
                    res.emplace_back(TableType("compound_crs", std::string()));
                    break;
                case ObjectType::ENGINEERING_CRS:
                    res.emplace_back(
                        TableType("engineering_crs", std::string()));
                    break;
                case ObjectType::COORDINATE_OPERATION:
                    res.emplace_back(TableType("conversion", std::string()));
                    res.emplace_back(
                        TableType("helmert_transformation", std::string()));
                    res.emplace_back(
                        TableType("grid_transformation", std::string()));
                    res.emplace_back(
                        TableType("other_transformation", std::string()));
                    res.emplace_back(
                        TableType("concatenated_operation", std::string()));
                    break;
                case ObjectType::CONVERSION:
                    res.emplace_back(TableType("conversion", std::string()));
                    break;
                case ObjectType::TRANSFORMATION:
                    res.emplace_back(
                        TableType("helmert_transformation", std::string()));
                    res.emplace_back(
                        TableType("grid_transformation", std::string()));
                    res.emplace_back(
                        TableType("other_transformation", std::string()));
                    break;
                case ObjectType::CONCATENATED_OPERATION:
                    res.emplace_back(
                        TableType("concatenated_operation", std::string()));
                    break;
                case ObjectType::DATUM_ENSEMBLE:
                    res.emplace_back(TableType("geodetic_datum", "ensemble"));
                    res.emplace_back(TableType("vertical_datum", "ensemble"));
                    break;
                }
            }
        }
        return res;
    };

    bool datumEnsembleAllowed = false;
    if (allowedObjectTypes.empty()) {
        datumEnsembleAllowed = true;
    } else {
        for (const auto type : allowedObjectTypes) {
            if (type == ObjectType::DATUM_ENSEMBLE) {
                datumEnsembleAllowed = true;
                break;
            }
        }
    }

    const auto listTableNameType = getTableAndTypeConstraints();
    bool first = true;
    ListOfParams params;
    for (const auto &tableNameTypePair : listTableNameType) {
        if (!first) {
            sql += " UNION ";
        }
        first = false;
        sql += "SELECT '";
        sql += tableNameTypePair.first;
        sql += "' AS table_name, auth_name, code, name, deprecated, "
               "0 AS is_alias FROM ";
        sql += tableNameTypePair.first;
        sql += " WHERE 1 = 1 ";
        if (!tableNameTypePair.second.empty()) {
            if (tableNameTypePair.second == "frame_reference_epoch") {
                sql += "AND frame_reference_epoch IS NOT NULL ";
            } else if (tableNameTypePair.second == "ensemble") {
                sql += "AND ensemble_accuracy IS NOT NULL ";
            } else {
                sql += "AND type = '";
                sql += tableNameTypePair.second;
                sql += "' ";
            }
        }
        if (deprecated) {
            sql += "AND deprecated = 1 ";
        }
        if (!approximateMatch) {
            sql += "AND name = ? COLLATE NOCASE ";
            params.push_back(searchedNameWithoutDeprecated);
        }
        if (d->hasAuthorityRestriction()) {
            sql += "AND auth_name = ? ";
            params.emplace_back(d->authority());
        }

        sql += " UNION SELECT '";
        sql += tableNameTypePair.first;
        sql += "' AS table_name, "
               "ov.auth_name AS auth_name, "
               "ov.code AS code, a.alt_name AS name, "
               "ov.deprecated AS deprecated, 1 as is_alias FROM ";
        sql += tableNameTypePair.first;
        sql += " ov "
               "JOIN alias_name a ON "
               "ov.auth_name = a.auth_name AND ov.code = a.code WHERE "
               "a.table_name = '";
        sql += tableNameTypePair.first;
        sql += "' ";
        if (!tableNameTypePair.second.empty()) {
            if (tableNameTypePair.second == "frame_reference_epoch") {
                sql += "AND ov.frame_reference_epoch IS NOT NULL ";
            } else if (tableNameTypePair.second == "ensemble") {
                sql += "AND ov.ensemble_accuracy IS NOT NULL ";
            } else {
                sql += "AND ov.type = '";
                sql += tableNameTypePair.second;
                sql += "' ";
            }
        }
        if (deprecated) {
            sql += "AND ov.deprecated = 1 ";
        }
        if (!approximateMatch) {
            sql += "AND a.alt_name = ? COLLATE NOCASE ";
            params.push_back(searchedNameWithoutDeprecated);
        }
        if (d->hasAuthorityRestriction()) {
            sql += "AND ov.auth_name = ? ";
            params.emplace_back(d->authority());
        }
    }

    sql += ") ORDER BY deprecated, is_alias, length(name), name";
    if (limitResultCount > 0 &&
        limitResultCount <
            static_cast<size_t>(std::numeric_limits<int>::max()) &&
        !approximateMatch) {
        sql += " LIMIT ";
        sql += toString(static_cast<int>(limitResultCount));
    }

    std::list<PairObjectName> res;
    std::set<std::pair<std::string, std::string>> setIdentified;

    // Querying geodetic datum is a super hot path when importing from WKT1
    // so cache results.
    if (allowedObjectTypes.size() == 1 &&
        allowedObjectTypes[0] == ObjectType::GEODETIC_REFERENCE_FRAME &&
        approximateMatch && d->authority().empty()) {
        auto &mapCanonicalizeGRFName =
            d->context()->getPrivate()->getMapCanonicalizeGRFName();
        if (mapCanonicalizeGRFName.empty()) {
            auto sqlRes = d->run(sql, params);
            for (const auto &row : sqlRes) {
                const auto &name = row[3];
                const auto &deprecatedStr = row[4];
                const auto canonicalizedName(
                    metadata::Identifier::canonicalizeName(name));
                auto &v = mapCanonicalizeGRFName[canonicalizedName];
                if (deprecatedStr == "0" || v.empty() || v.front()[4] == "1") {
                    v.push_back(row);
                }
            }
        }
        auto iter = mapCanonicalizeGRFName.find(canonicalizedSearchedName);
        if (iter != mapCanonicalizeGRFName.end()) {
            const auto &listOfRow = iter->second;
            for (const auto &row : listOfRow) {
                const auto &auth_name = row[1];
                const auto &code = row[2];
                auto key = std::pair<std::string, std::string>(auth_name, code);
                if (setIdentified.find(key) != setIdentified.end()) {
                    continue;
                }
                setIdentified.insert(std::move(key));
                auto factory = d->createFactory(auth_name);
                const auto &name = row[3];
                res.emplace_back(
                    PairObjectName(factory->createGeodeticDatum(code), name));
                if (limitResultCount > 0 && res.size() == limitResultCount) {
                    break;
                }
            }
        } else {
            for (const auto &pair : mapCanonicalizeGRFName) {
                const auto &listOfRow = pair.second;
                for (const auto &row : listOfRow) {
                    const auto &name = row[3];
                    bool match = ci_find(name, searchedNameWithoutDeprecated) !=
                                 std::string::npos;
                    if (!match) {
                        const auto &canonicalizedName(pair.first);
                        match = ci_find(canonicalizedName,
                                        canonicalizedSearchedName) !=
                                std::string::npos;
                    }
                    if (!match) {
                        continue;
                    }

                    const auto &auth_name = row[1];
                    const auto &code = row[2];
                    auto key =
                        std::pair<std::string, std::string>(auth_name, code);
                    if (setIdentified.find(key) != setIdentified.end()) {
                        continue;
                    }
                    setIdentified.insert(std::move(key));
                    auto factory = d->createFactory(auth_name);
                    res.emplace_back(PairObjectName(
                        factory->createGeodeticDatum(code), name));
                    if (limitResultCount > 0 &&
                        res.size() == limitResultCount) {
                        break;
                    }
                }
                if (limitResultCount > 0 && res.size() == limitResultCount) {
                    break;
                }
            }
        }
    } else {
        auto sqlRes = d->run(sql, params);
        bool isFirst = true;
        bool firstIsDeprecated = false;
        size_t countExactMatch = 0;
        size_t countExactMatchOnAlias = 0;
        std::size_t hashCodeFirstMatch = 0;
        for (const auto &row : sqlRes) {
            const auto &name = row[3];
            if (approximateMatch) {
                bool match = ci_find(name, searchedNameWithoutDeprecated) !=
                             std::string::npos;
                if (!match) {
                    const auto canonicalizedName(
                        metadata::Identifier::canonicalizeName(name));
                    match =
                        ci_find(canonicalizedName, canonicalizedSearchedName) !=
                        std::string::npos;
                }
                if (!match) {
                    continue;
                }
            }
            const auto &table_name = row[0];
            const auto &auth_name = row[1];
            const auto &code = row[2];
            auto key = std::pair<std::string, std::string>(auth_name, code);
            if (setIdentified.find(key) != setIdentified.end()) {
                continue;
            }
            setIdentified.insert(std::move(key));
            const auto &deprecatedStr = row[4];
            if (isFirst) {
                firstIsDeprecated = deprecatedStr == "1";
                isFirst = false;
            }
            if (deprecatedStr == "1" && !res.empty() && !firstIsDeprecated) {
                break;
            }
            auto factory = d->createFactory(auth_name);
            auto getObject = [&factory, datumEnsembleAllowed](
                                 const std::string &l_table_name,
                                 const std::string &l_code)
                -> common::IdentifiedObjectNNPtr {
                if (l_table_name == "prime_meridian") {
                    return factory->createPrimeMeridian(l_code);
                } else if (l_table_name == "ellipsoid") {
                    return factory->createEllipsoid(l_code);
                } else if (l_table_name == "geodetic_datum") {
                    if (datumEnsembleAllowed) {
                        datum::GeodeticReferenceFramePtr datum;
                        datum::DatumEnsemblePtr datumEnsemble;
                        constexpr bool turnEnsembleAsDatum = false;
                        factory->createGeodeticDatumOrEnsemble(
                            l_code, datum, datumEnsemble, turnEnsembleAsDatum);
                        if (datum) {
                            return NN_NO_CHECK(datum);
                        }
                        assert(datumEnsemble);
                        return NN_NO_CHECK(datumEnsemble);
                    }
                    return factory->createGeodeticDatum(l_code);
                } else if (l_table_name == "vertical_datum") {
                    if (datumEnsembleAllowed) {
                        datum::VerticalReferenceFramePtr datum;
                        datum::DatumEnsemblePtr datumEnsemble;
                        constexpr bool turnEnsembleAsDatum = false;
                        factory->createVerticalDatumOrEnsemble(
                            l_code, datum, datumEnsemble, turnEnsembleAsDatum);
                        if (datum) {
                            return NN_NO_CHECK(datum);
                        }
                        assert(datumEnsemble);
                        return NN_NO_CHECK(datumEnsemble);
                    }
                    return factory->createVerticalDatum(l_code);
                } else if (l_table_name == "engineering_datum") {
                    return factory->createEngineeringDatum(l_code);
                } else if (l_table_name == "geodetic_crs") {
                    return factory->createGeodeticCRS(l_code);
                } else if (l_table_name == "projected_crs") {
                    return factory->createProjectedCRS(l_code);
                } else if (l_table_name == "vertical_crs") {
                    return factory->createVerticalCRS(l_code);
                } else if (l_table_name == "compound_crs") {
                    return factory->createCompoundCRS(l_code);
                } else if (l_table_name == "engineering_crs") {
                    return factory->createEngineeringCRS(l_code);
                } else if (l_table_name == "conversion") {
                    return factory->createConversion(l_code);
                } else if (l_table_name == "grid_transformation" ||
                           l_table_name == "helmert_transformation" ||
                           l_table_name == "other_transformation" ||
                           l_table_name == "concatenated_operation") {
                    return factory->createCoordinateOperation(l_code, true);
                }
                throw std::runtime_error("Unsupported table_name");
            };
            const auto obj = getObject(table_name, code);
            if (metadata::Identifier::isEquivalentName(
                    obj->nameStr().c_str(), searchedName.c_str(), false)) {
                countExactMatch++;
            } else if (metadata::Identifier::isEquivalentName(
                           name.c_str(), searchedName.c_str(), false)) {
                countExactMatchOnAlias++;
            }

            const auto objPtr = obj.get();
            if (res.empty()) {
                hashCodeFirstMatch = typeid(*objPtr).hash_code();
            } else if (hashCodeFirstMatch != typeid(*objPtr).hash_code()) {
                hashCodeFirstMatch = 0;
            }

            res.emplace_back(PairObjectName(obj, name));
            if (limitResultCount > 0 && res.size() == limitResultCount) {
                break;
            }
        }

        // If we found several objects that are an exact match, and all objects
        // have the same type, and we are not in approximate mode, only keep the
        // objects with the exact name match.
        if ((countExactMatch + countExactMatchOnAlias) >= 1 &&
            hashCodeFirstMatch != 0 && !approximateMatch) {
            std::list<PairObjectName> resTmp;
            bool biggerDifferencesAllowed = (countExactMatch == 0);
            for (const auto &pair : res) {
                if (metadata::Identifier::isEquivalentName(
                        pair.first->nameStr().c_str(), searchedName.c_str(),
                        biggerDifferencesAllowed) ||
                    (countExactMatch == 0 &&
                     metadata::Identifier::isEquivalentName(
                         pair.second.c_str(), searchedName.c_str(),
                         biggerDifferencesAllowed))) {
                    resTmp.emplace_back(pair);
                }
            }
            res = std::move(resTmp);
        }
    }

    auto sortLambda = [](const PairObjectName &a, const PairObjectName &b) {
        const auto &aName = a.first->nameStr();
        const auto &bName = b.first->nameStr();

        if (aName.size() < bName.size()) {
            return true;
        }
        if (aName.size() > bName.size()) {
            return false;
        }

        const auto &aIds = a.first->identifiers();
        const auto &bIds = b.first->identifiers();
        if (aIds.size() < bIds.size()) {
            return true;
        }
        if (aIds.size() > bIds.size()) {
            return false;
        }
        for (size_t idx = 0; idx < aIds.size(); idx++) {
            const auto &aCodeSpace = *aIds[idx]->codeSpace();
            const auto &bCodeSpace = *bIds[idx]->codeSpace();
            const auto codeSpaceComparison = aCodeSpace.compare(bCodeSpace);
            if (codeSpaceComparison < 0) {
                return true;
            }
            if (codeSpaceComparison > 0) {
                return false;
            }
            const auto &aCode = aIds[idx]->code();
            const auto &bCode = bIds[idx]->code();
            const auto codeComparison = aCode.compare(bCode);
            if (codeComparison < 0) {
                return true;
            }
            if (codeComparison > 0) {
                return false;
            }
        }
        return strcmp(typeid(a.first.get()).name(),
                      typeid(b.first.get()).name()) < 0;
    };

    res.sort(sortLambda);

    return res;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return a list of area of use from their name
 *
 * @param name Searched name.
 * @param approximateMatch Whether approximate name identification is allowed.
 * @return list of (auth_name, code) of matched objects.
 * @throw FactoryException in case of error.
 */
std::list<std::pair<std::string, std::string>>
AuthorityFactory::listAreaOfUseFromName(const std::string &name,
                                        bool approximateMatch) const {
    std::string sql(
        "SELECT auth_name, code FROM extent WHERE deprecated = 0 AND ");
    ListOfParams params;
    if (d->hasAuthorityRestriction()) {
        sql += " auth_name = ? AND ";
        params.emplace_back(d->authority());
    }
    sql += "name LIKE ?";
    if (!approximateMatch) {
        params.push_back(name);
    } else {
        params.push_back('%' + name + '%');
    }
    auto sqlRes = d->run(sql, params);
    std::list<std::pair<std::string, std::string>> res;
    for (const auto &row : sqlRes) {
        res.emplace_back(row[0], row[1]);
    }
    return res;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::list<datum::EllipsoidNNPtr> AuthorityFactory::createEllipsoidFromExisting(
    const datum::EllipsoidNNPtr &ellipsoid) const {
    std::string sql(
        "SELECT auth_name, code FROM ellipsoid WHERE "
        "abs(semi_major_axis - ?) < 1e-10 * abs(semi_major_axis) AND "
        "((semi_minor_axis IS NOT NULL AND "
        "abs(semi_minor_axis - ?) < 1e-10 * abs(semi_minor_axis)) OR "
        "((inv_flattening IS NOT NULL AND "
        "abs(inv_flattening - ?) < 1e-10 * abs(inv_flattening))))");
    ListOfParams params{ellipsoid->semiMajorAxis().getSIValue(),
                        ellipsoid->computeSemiMinorAxis().getSIValue(),
                        ellipsoid->computedInverseFlattening()};
    auto sqlRes = d->run(sql, params);
    std::list<datum::EllipsoidNNPtr> res;
    for (const auto &row : sqlRes) {
        const auto &auth_name = row[0];
        const auto &code = row[1];
        res.emplace_back(d->createFactory(auth_name)->createEllipsoid(code));
    }
    return res;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::list<crs::GeodeticCRSNNPtr> AuthorityFactory::createGeodeticCRSFromDatum(
    const std::string &datum_auth_name, const std::string &datum_code,
    const std::string &geodetic_crs_type) const {
    std::string sql(
        "SELECT auth_name, code FROM geodetic_crs WHERE "
        "datum_auth_name = ? AND datum_code = ? AND deprecated = 0");
    ListOfParams params{datum_auth_name, datum_code};
    if (d->hasAuthorityRestriction()) {
        sql += " AND auth_name = ?";
        params.emplace_back(d->authority());
    }
    if (!geodetic_crs_type.empty()) {
        sql += " AND type = ?";
        params.emplace_back(geodetic_crs_type);
    }
    sql += " ORDER BY auth_name, code";
    auto sqlRes = d->run(sql, params);
    std::list<crs::GeodeticCRSNNPtr> res;
    for (const auto &row : sqlRes) {
        const auto &auth_name = row[0];
        const auto &code = row[1];
        res.emplace_back(d->createFactory(auth_name)->createGeodeticCRS(code));
    }
    return res;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::list<crs::GeodeticCRSNNPtr> AuthorityFactory::createGeodeticCRSFromDatum(
    const datum::GeodeticReferenceFrameNNPtr &datum,
    const std::string &preferredAuthName,
    const std::string &geodetic_crs_type) const {
    std::list<crs::GeodeticCRSNNPtr> candidates;
    const auto &ids = datum->identifiers();
    const auto &datumName = datum->nameStr();
    if (!ids.empty()) {
        for (const auto &id : ids) {
            const auto &authName = *(id->codeSpace());
            const auto &code = id->code();
            if (!authName.empty()) {
                const auto tmpFactory =
                    (preferredAuthName == authName)
                        ? create(databaseContext(), authName)
                        : NN_NO_CHECK(d->getSharedFromThis());
                auto l_candidates = tmpFactory->createGeodeticCRSFromDatum(
                    authName, code, geodetic_crs_type);
                for (const auto &candidate : l_candidates) {
                    candidates.emplace_back(candidate);
                }
            }
        }
    } else if (datumName != "unknown" && datumName != "unnamed") {
        auto matches = createObjectsFromName(
            datumName,
            {io::AuthorityFactory::ObjectType::GEODETIC_REFERENCE_FRAME}, false,
            2);
        if (matches.size() == 1) {
            const auto &match = matches.front();
            if (datum->_isEquivalentTo(match.get(),
                                       util::IComparable::Criterion::EQUIVALENT,
                                       databaseContext().as_nullable()) &&
                !match->identifiers().empty()) {
                return createGeodeticCRSFromDatum(
                    util::nn_static_pointer_cast<datum::GeodeticReferenceFrame>(
                        match),
                    preferredAuthName, geodetic_crs_type);
            }
        }
    }
    return candidates;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::list<crs::VerticalCRSNNPtr> AuthorityFactory::createVerticalCRSFromDatum(
    const std::string &datum_auth_name, const std::string &datum_code) const {
    std::string sql(
        "SELECT auth_name, code FROM vertical_crs WHERE "
        "datum_auth_name = ? AND datum_code = ? AND deprecated = 0");
    ListOfParams params{datum_auth_name, datum_code};
    if (d->hasAuthorityRestriction()) {
        sql += " AND auth_name = ?";
        params.emplace_back(d->authority());
    }
    auto sqlRes = d->run(sql, params);
    std::list<crs::VerticalCRSNNPtr> res;
    for (const auto &row : sqlRes) {
        const auto &auth_name = row[0];
        const auto &code = row[1];
        res.emplace_back(d->createFactory(auth_name)->createVerticalCRS(code));
    }
    return res;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::list<crs::GeodeticCRSNNPtr>
AuthorityFactory::createGeodeticCRSFromEllipsoid(
    const std::string &ellipsoid_auth_name, const std::string &ellipsoid_code,
    const std::string &geodetic_crs_type) const {
    std::string sql(
        "SELECT geodetic_crs.auth_name, geodetic_crs.code FROM geodetic_crs "
        "JOIN geodetic_datum ON "
        "geodetic_crs.datum_auth_name = geodetic_datum.auth_name AND "
        "geodetic_crs.datum_code = geodetic_datum.code WHERE "
        "geodetic_datum.ellipsoid_auth_name = ? AND "
        "geodetic_datum.ellipsoid_code = ? AND "
        "geodetic_datum.deprecated = 0 AND "
        "geodetic_crs.deprecated = 0");
    ListOfParams params{ellipsoid_auth_name, ellipsoid_code};
    if (d->hasAuthorityRestriction()) {
        sql += " AND geodetic_crs.auth_name = ?";
        params.emplace_back(d->authority());
    }
    if (!geodetic_crs_type.empty()) {
        sql += " AND geodetic_crs.type = ?";
        params.emplace_back(geodetic_crs_type);
    }
    auto sqlRes = d->run(sql, params);
    std::list<crs::GeodeticCRSNNPtr> res;
    for (const auto &row : sqlRes) {
        const auto &auth_name = row[0];
        const auto &code = row[1];
        res.emplace_back(d->createFactory(auth_name)->createGeodeticCRS(code));
    }
    return res;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static std::string buildSqlLookForAuthNameCode(
    const std::list<std::pair<crs::CRSNNPtr, int>> &list, ListOfParams &params,
    const char *prefixField) {
    std::string sql("(");

    std::set<std::string> authorities;
    for (const auto &crs : list) {
        auto boundCRS = dynamic_cast<crs::BoundCRS *>(crs.first.get());
        const auto &ids = boundCRS ? boundCRS->baseCRS()->identifiers()
                                   : crs.first->identifiers();
        if (!ids.empty()) {
            authorities.insert(*(ids[0]->codeSpace()));
        }
    }
    bool firstAuth = true;
    for (const auto &auth_name : authorities) {
        if (!firstAuth) {
            sql += " OR ";
        }
        firstAuth = false;
        sql += "( ";
        sql += prefixField;
        sql += "auth_name = ? AND ";
        sql += prefixField;
        sql += "code IN (";
        params.emplace_back(auth_name);
        bool firstGeodCRSForAuth = true;
        for (const auto &crs : list) {
            auto boundCRS = dynamic_cast<crs::BoundCRS *>(crs.first.get());
            const auto &ids = boundCRS ? boundCRS->baseCRS()->identifiers()
                                       : crs.first->identifiers();
            if (!ids.empty() && *(ids[0]->codeSpace()) == auth_name) {
                if (!firstGeodCRSForAuth) {
                    sql += ',';
                }
                firstGeodCRSForAuth = false;
                sql += '?';
                params.emplace_back(ids[0]->code());
            }
        }
        sql += "))";
    }
    sql += ')';
    return sql;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::list<crs::ProjectedCRSNNPtr>
AuthorityFactory::createProjectedCRSFromExisting(
    const crs::ProjectedCRSNNPtr &crs) const {
    std::list<crs::ProjectedCRSNNPtr> res;

    const auto &conv = crs->derivingConversionRef();
    const auto &method = conv->method();
    const auto methodEPSGCode = method->getEPSGCode();
    if (methodEPSGCode == 0) {
        return res;
    }

    auto lockedThisFactory(d->getSharedFromThis());
    assert(lockedThisFactory);
    const auto &baseCRS(crs->baseCRS());
    auto candidatesGeodCRS = baseCRS->crs::CRS::identify(lockedThisFactory);
    auto geogCRS = dynamic_cast<const crs::GeographicCRS *>(baseCRS.get());
    if (geogCRS) {
        const auto axisOrder = geogCRS->coordinateSystem()->axisOrder();
        if (axisOrder == cs::EllipsoidalCS::AxisOrder::LONG_EAST_LAT_NORTH ||
            axisOrder == cs::EllipsoidalCS::AxisOrder::LAT_NORTH_LONG_EAST) {
            const auto &unit =
                geogCRS->coordinateSystem()->axisList()[0]->unit();
            auto otherOrderGeogCRS = crs::GeographicCRS::create(
                util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                        geogCRS->nameStr()),
                geogCRS->datum(), geogCRS->datumEnsemble(),
                axisOrder == cs::EllipsoidalCS::AxisOrder::LONG_EAST_LAT_NORTH
                    ? cs::EllipsoidalCS::createLatitudeLongitude(unit)
                    : cs::EllipsoidalCS::createLongitudeLatitude(unit));
            auto otherCandidatesGeodCRS =
                otherOrderGeogCRS->crs::CRS::identify(lockedThisFactory);
            candidatesGeodCRS.insert(candidatesGeodCRS.end(),
                                     otherCandidatesGeodCRS.begin(),
                                     otherCandidatesGeodCRS.end());
        }
    }

    std::string sql("SELECT projected_crs.auth_name, projected_crs.code, "
                    "projected_crs.name FROM projected_crs "
                    "JOIN conversion_table conv ON "
                    "projected_crs.conversion_auth_name = conv.auth_name AND "
                    "projected_crs.conversion_code = conv.code WHERE "
                    "projected_crs.deprecated = 0 AND ");
    ListOfParams params;
    if (!candidatesGeodCRS.empty()) {
        sql += buildSqlLookForAuthNameCode(candidatesGeodCRS, params,
                                           "projected_crs.geodetic_crs_");
        sql += " AND ";
    }
    sql += "conv.method_auth_name = 'EPSG' AND "
           "conv.method_code = ?";
    params.emplace_back(toString(methodEPSGCode));
    if (d->hasAuthorityRestriction()) {
        sql += " AND projected_crs.auth_name = ?";
        params.emplace_back(d->authority());
    }

    int iParam = 0;
    bool hasLat1stStd = false;
    double lat1stStd = 0;
    int iParamLat1stStd = 0;
    bool hasLat2ndStd = false;
    double lat2ndStd = 0;
    int iParamLat2ndStd = 0;
    for (const auto &genOpParamvalue : conv->parameterValues()) {
        iParam++;
        auto opParamvalue =
            dynamic_cast<const operation::OperationParameterValue *>(
                genOpParamvalue.get());
        if (!opParamvalue) {
            break;
        }
        const auto paramEPSGCode = opParamvalue->parameter()->getEPSGCode();
        const auto &parameterValue = opParamvalue->parameterValue();
        if (!(paramEPSGCode > 0 &&
              parameterValue->type() ==
                  operation::ParameterValue::Type::MEASURE)) {
            break;
        }
        const auto &measure = parameterValue->value();
        const auto &unit = measure.unit();
        if (unit == common::UnitOfMeasure::DEGREE &&
            baseCRS->coordinateSystem()->axisList()[0]->unit() == unit) {
            if (methodEPSGCode ==
                EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP) {
                // Special case for standard parallels of LCC_2SP. See below
                if (paramEPSGCode ==
                    EPSG_CODE_PARAMETER_LATITUDE_1ST_STD_PARALLEL) {
                    hasLat1stStd = true;
                    lat1stStd = measure.value();
                    iParamLat1stStd = iParam;
                    continue;
                } else if (paramEPSGCode ==
                           EPSG_CODE_PARAMETER_LATITUDE_2ND_STD_PARALLEL) {
                    hasLat2ndStd = true;
                    lat2ndStd = measure.value();
                    iParamLat2ndStd = iParam;
                    continue;
                }
            }
            const auto iParamAsStr(toString(iParam));
            sql += " AND conv.param";
            sql += iParamAsStr;
            sql += "_code = ? AND conv.param";
            sql += iParamAsStr;
            sql += "_auth_name = 'EPSG' AND conv.param";
            sql += iParamAsStr;
            sql += "_value BETWEEN ? AND ?";
            // As angles might be expressed with the odd unit EPSG:9110
            // "sexagesimal DMS", we have to provide a broad range
            params.emplace_back(toString(paramEPSGCode));
            params.emplace_back(measure.value() - 1);
            params.emplace_back(measure.value() + 1);
        }
    }

    // Special case for standard parallels of LCC_2SP: they can be switched
    if (methodEPSGCode == EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP &&
        hasLat1stStd && hasLat2ndStd) {
        const auto iParam1AsStr(toString(iParamLat1stStd));
        const auto iParam2AsStr(toString(iParamLat2ndStd));
        sql += " AND conv.param";
        sql += iParam1AsStr;
        sql += "_code = ? AND conv.param";
        sql += iParam1AsStr;
        sql += "_auth_name = 'EPSG' AND conv.param";
        sql += iParam2AsStr;
        sql += "_code = ? AND conv.param";
        sql += iParam2AsStr;
        sql += "_auth_name = 'EPSG' AND ((";
        params.emplace_back(
            toString(EPSG_CODE_PARAMETER_LATITUDE_1ST_STD_PARALLEL));
        params.emplace_back(
            toString(EPSG_CODE_PARAMETER_LATITUDE_2ND_STD_PARALLEL));
        double val1 = lat1stStd;
        double val2 = lat2ndStd;
        for (int i = 0; i < 2; i++) {
            if (i == 1) {
                sql += ") OR (";
                std::swap(val1, val2);
            }
            sql += "conv.param";
            sql += iParam1AsStr;
            sql += "_value BETWEEN ? AND ? AND conv.param";
            sql += iParam2AsStr;
            sql += "_value BETWEEN ? AND ?";
            params.emplace_back(val1 - 1);
            params.emplace_back(val1 + 1);
            params.emplace_back(val2 - 1);
            params.emplace_back(val2 + 1);
        }
        sql += "))";
    }
    auto sqlRes = d->run(sql, params);

    for (const auto &row : sqlRes) {
        const auto &name = row[2];
        if (metadata::Identifier::isEquivalentName(crs->nameStr().c_str(),
                                                   name.c_str())) {
            const auto &auth_name = row[0];
            const auto &code = row[1];
            res.emplace_back(
                d->createFactory(auth_name)->createProjectedCRS(code));
        }
    }
    if (!res.empty()) {
        return res;
    }

    params.clear();

    sql = "SELECT auth_name, code FROM projected_crs WHERE "
          "deprecated = 0 AND conversion_auth_name IS NULL AND ";
    if (!candidatesGeodCRS.empty()) {
        sql += buildSqlLookForAuthNameCode(candidatesGeodCRS, params,
                                           "geodetic_crs_");
        sql += " AND ";
    }

    const auto escapeLikeStr = [](const std::string &str) {
        return replaceAll(replaceAll(replaceAll(str, "\\", "\\\\"), "_", "\\_"),
                          "%", "\\%");
    };

    const auto ellpsSemiMajorStr =
        toString(baseCRS->ellipsoid()->semiMajorAxis().getSIValue(), 10);

    sql += "(text_definition LIKE ? ESCAPE '\\'";

    // WKT2 definition
    {
        std::string patternVal("%");

        patternVal += ',';
        patternVal += ellpsSemiMajorStr;
        patternVal += '%';

        patternVal += escapeLikeStr(method->nameStr());
        patternVal += '%';

        params.emplace_back(patternVal);
    }

    const auto *mapping = getMapping(method.get());
    if (mapping && mapping->proj_name_main) {
        sql += " OR (text_definition LIKE ? AND (text_definition LIKE ?";

        std::string patternVal("%");
        patternVal += "proj=";
        patternVal += mapping->proj_name_main;
        patternVal += '%';
        params.emplace_back(patternVal);

        // could be a= or R=
        patternVal = "%=";
        patternVal += ellpsSemiMajorStr;
        patternVal += '%';
        params.emplace_back(patternVal);

        std::string projEllpsName;
        std::string ellpsName;
        if (baseCRS->ellipsoid()->lookForProjWellKnownEllps(projEllpsName,
                                                            ellpsName)) {
            sql += " OR text_definition LIKE ?";
            // Could be ellps= or datum=
            patternVal = "%=";
            patternVal += projEllpsName;
            patternVal += '%';
            params.emplace_back(patternVal);
        }

        sql += "))";
    }

    // WKT1_GDAL definition
    const char *wkt1GDALMethodName = conv->getWKT1GDALMethodName();
    if (wkt1GDALMethodName) {
        sql += " OR text_definition LIKE ? ESCAPE '\\'";
        std::string patternVal("%");

        patternVal += ',';
        patternVal += ellpsSemiMajorStr;
        patternVal += '%';

        patternVal += escapeLikeStr(wkt1GDALMethodName);
        patternVal += '%';

        params.emplace_back(patternVal);
    }

    // WKT1_ESRI definition
    const char *esriMethodName = conv->getESRIMethodName();
    if (esriMethodName) {
        sql += " OR text_definition LIKE ? ESCAPE '\\'";
        std::string patternVal("%");

        patternVal += ',';
        patternVal += ellpsSemiMajorStr;
        patternVal += '%';

        patternVal += escapeLikeStr(esriMethodName);
        patternVal += '%';

        auto fe =
            &conv->parameterValueMeasure(EPSG_CODE_PARAMETER_FALSE_EASTING);
        if (*fe == Measure()) {
            fe = &conv->parameterValueMeasure(
                EPSG_CODE_PARAMETER_EASTING_FALSE_ORIGIN);
        }
        if (!(*fe == Measure())) {
            patternVal += "PARAMETER[\"False\\_Easting\",";
            patternVal +=
                toString(fe->convertToUnit(
                             crs->coordinateSystem()->axisList()[0]->unit()),
                         10);
            patternVal += '%';
        }

        auto lat = &conv->parameterValueMeasure(
            EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN);
        if (*lat == Measure()) {
            lat = &conv->parameterValueMeasure(
                EPSG_NAME_PARAMETER_LATITUDE_FALSE_ORIGIN);
        }
        if (!(*lat == Measure())) {
            patternVal += "PARAMETER[\"Latitude\\_Of\\_Origin\",";
            const auto &angularUnit =
                dynamic_cast<crs::GeographicCRS *>(crs->baseCRS().get())
                    ? crs->baseCRS()->coordinateSystem()->axisList()[0]->unit()
                    : UnitOfMeasure::DEGREE;
            patternVal += toString(lat->convertToUnit(angularUnit), 10);
            patternVal += '%';
        }

        params.emplace_back(patternVal);
    }
    sql += ")";
    if (d->hasAuthorityRestriction()) {
        sql += " AND auth_name = ?";
        params.emplace_back(d->authority());
    }

    auto sqlRes2 = d->run(sql, params);

    if (sqlRes.size() <= 200) {
        for (const auto &row : sqlRes) {
            const auto &auth_name = row[0];
            const auto &code = row[1];
            res.emplace_back(
                d->createFactory(auth_name)->createProjectedCRS(code));
        }
    }
    if (sqlRes2.size() <= 200) {
        for (const auto &row : sqlRes2) {
            const auto &auth_name = row[0];
            const auto &code = row[1];
            res.emplace_back(
                d->createFactory(auth_name)->createProjectedCRS(code));
        }
    }

    return res;
}

// ---------------------------------------------------------------------------

std::list<crs::CompoundCRSNNPtr>
AuthorityFactory::createCompoundCRSFromExisting(
    const crs::CompoundCRSNNPtr &crs) const {
    std::list<crs::CompoundCRSNNPtr> res;

    auto lockedThisFactory(d->getSharedFromThis());
    assert(lockedThisFactory);

    const auto &components = crs->componentReferenceSystems();
    if (components.size() != 2) {
        return res;
    }
    auto candidatesHorizCRS = components[0]->identify(lockedThisFactory);
    auto candidatesVertCRS = components[1]->identify(lockedThisFactory);
    if (candidatesHorizCRS.empty() && candidatesVertCRS.empty()) {
        return res;
    }

    std::string sql("SELECT auth_name, code FROM compound_crs WHERE "
                    "deprecated = 0 AND ");
    ListOfParams params;
    bool addAnd = false;
    if (!candidatesHorizCRS.empty()) {
        sql += buildSqlLookForAuthNameCode(candidatesHorizCRS, params,
                                           "horiz_crs_");
        addAnd = true;
    }
    if (!candidatesVertCRS.empty()) {
        if (addAnd) {
            sql += " AND ";
        }
        sql += buildSqlLookForAuthNameCode(candidatesVertCRS, params,
                                           "vertical_crs_");
        addAnd = true;
    }
    if (d->hasAuthorityRestriction()) {
        if (addAnd) {
            sql += " AND ";
        }
        sql += "auth_name = ?";
        params.emplace_back(d->authority());
    }

    auto sqlRes = d->run(sql, params);
    for (const auto &row : sqlRes) {
        const auto &auth_name = row[0];
        const auto &code = row[1];
        res.emplace_back(d->createFactory(auth_name)->createCompoundCRS(code));
    }
    return res;
}

// ---------------------------------------------------------------------------

std::vector<operation::CoordinateOperationNNPtr>
AuthorityFactory::getTransformationsForGeoid(
    const std::string &geoidName, bool usePROJAlternativeGridNames) const {
    std::vector<operation::CoordinateOperationNNPtr> res;

    const std::string sql("SELECT operation_auth_name, operation_code FROM "
                          "geoid_model WHERE name = ?");
    auto sqlRes = d->run(sql, {geoidName});
    for (const auto &row : sqlRes) {
        const auto &auth_name = row[0];
        const auto &code = row[1];
        res.emplace_back(d->createFactory(auth_name)->createCoordinateOperation(
            code, usePROJAlternativeGridNames));
    }

    return res;
}

// ---------------------------------------------------------------------------

std::vector<operation::PointMotionOperationNNPtr>
AuthorityFactory::getPointMotionOperationsFor(
    const crs::GeodeticCRSNNPtr &crs, bool usePROJAlternativeGridNames) const {
    std::vector<operation::PointMotionOperationNNPtr> res;
    const auto crsList =
        createGeodeticCRSFromDatum(crs->datumNonNull(d->context()),
                                   /* preferredAuthName = */ std::string(),
                                   /* geodetic_crs_type = */ std::string());
    if (crsList.empty())
        return res;
    std::string sql("SELECT auth_name, code FROM coordinate_operation_view "
                    "WHERE source_crs_auth_name = target_crs_auth_name AND "
                    "source_crs_code = target_crs_code AND deprecated = 0 AND "
                    "(");
    bool addOr = false;
    ListOfParams params;
    for (const auto &candidateCrs : crsList) {
        if (addOr)
            sql += " OR ";
        addOr = true;
        sql += "(source_crs_auth_name = ? AND source_crs_code = ?)";
        const auto &ids = candidateCrs->identifiers();
        params.emplace_back(*(ids[0]->codeSpace()));
        params.emplace_back(ids[0]->code());
    }
    sql += ")";
    if (d->hasAuthorityRestriction()) {
        sql += " AND auth_name = ?";
        params.emplace_back(d->authority());
    }

    auto sqlRes = d->run(sql, params);
    for (const auto &row : sqlRes) {
        const auto &auth_name = row[0];
        const auto &code = row[1];
        auto pmo =
            util::nn_dynamic_pointer_cast<operation::PointMotionOperation>(
                d->createFactory(auth_name)->createCoordinateOperation(
                    code, usePROJAlternativeGridNames));
        if (pmo) {
            res.emplace_back(NN_NO_CHECK(pmo));
        }
    }
    return res;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
FactoryException::FactoryException(const char *message) : Exception(message) {}

// ---------------------------------------------------------------------------

FactoryException::FactoryException(const std::string &message)
    : Exception(message) {}

// ---------------------------------------------------------------------------

FactoryException::~FactoryException() = default;

// ---------------------------------------------------------------------------

FactoryException::FactoryException(const FactoryException &) = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

struct NoSuchAuthorityCodeException::Private {
    std::string authority_;
    std::string code_;

    Private(const std::string &authority, const std::string &code)
        : authority_(authority), code_(code) {}
};

// ---------------------------------------------------------------------------

NoSuchAuthorityCodeException::NoSuchAuthorityCodeException(
    const std::string &message, const std::string &authority,
    const std::string &code)
    : FactoryException(message), d(std::make_unique<Private>(authority, code)) {
}

// ---------------------------------------------------------------------------

NoSuchAuthorityCodeException::~NoSuchAuthorityCodeException() = default;

// ---------------------------------------------------------------------------

NoSuchAuthorityCodeException::NoSuchAuthorityCodeException(
    const NoSuchAuthorityCodeException &other)
    : FactoryException(other), d(std::make_unique<Private>(*(other.d))) {}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns authority name. */
const std::string &NoSuchAuthorityCodeException::getAuthority() const {
    return d->authority_;
}

// ---------------------------------------------------------------------------

/** \brief Returns authority code. */
const std::string &NoSuchAuthorityCodeException::getAuthorityCode() const {
    return d->code_;
}

// ---------------------------------------------------------------------------

} // namespace io
NS_PROJ_END

// ---------------------------------------------------------------------------

void pj_clear_sqlite_cache() { NS_PROJ::io::SQLiteHandleCache::get().clear(); }
