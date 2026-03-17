/******************************************************************************
 * Project:  PROJ
 * Purpose:  SQLite3 related utilities
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2019, Even Rouault, <even.rouault at spatialys.com>
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

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Weffc++"
#endif

#include "sqlite3_utils.hpp"

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#ifdef EMBED_RESOURCE_FILES
#error #include "memvfs.h"
#endif

#include <cstdlib>
#include <cstring>
#include <sstream> // std::ostringstream

NS_PROJ_START

// ---------------------------------------------------------------------------

pj_sqlite3_vfs::~pj_sqlite3_vfs() = default;

// ---------------------------------------------------------------------------

SQLite3VFS::SQLite3VFS(pj_sqlite3_vfs *vfs) : vfs_(vfs) {}

// ---------------------------------------------------------------------------

SQLite3VFS::~SQLite3VFS() {
    if (vfs_) {
        sqlite3_vfs_unregister(&(vfs_->base));
        delete vfs_;
    }
}

// ---------------------------------------------------------------------------

const char *SQLite3VFS::name() const { return vfs_->namePtr.c_str(); }

// ---------------------------------------------------------------------------

typedef int (*ClosePtr)(sqlite3_file *);

// ---------------------------------------------------------------------------

static int VFSClose(sqlite3_file *file) {
    sqlite3_vfs *defaultVFS = sqlite3_vfs_find(nullptr);
    assert(defaultVFS);
    ClosePtr defaultClosePtr;
    std::memcpy(&defaultClosePtr,
                reinterpret_cast<char *>(file) + defaultVFS->szOsFile,
                sizeof(ClosePtr));
    void *methods = const_cast<sqlite3_io_methods *>(file->pMethods);
    int ret = defaultClosePtr(file);
    std::free(methods);
    return ret;
}

// ---------------------------------------------------------------------------

static int VSFNoOpLockUnlockSync(sqlite3_file *, int) { return SQLITE_OK; }

// ---------------------------------------------------------------------------

namespace {

struct pj_sqlite3_customvfs_appdata {
    sqlite3_vfs *defaultVFS = nullptr;
    bool fakeSync = false;
    bool fakeLock = false;
};

struct pj_sqlite3_customvfs : public pj_sqlite3_vfs {
    ~pj_sqlite3_customvfs() override {
        delete static_cast<pj_sqlite3_customvfs_appdata *>(base.pAppData);
        base.pAppData = nullptr;
    }
};
} // namespace

static int VFSCustomOpen(sqlite3_vfs *vfs, const char *name, sqlite3_file *file,
                         int flags, int *outFlags) {
    pj_sqlite3_customvfs_appdata *appdata =
        static_cast<pj_sqlite3_customvfs_appdata *>(vfs->pAppData);
    sqlite3_vfs *defaultVFS = appdata->defaultVFS;
    int ret = defaultVFS->xOpen(defaultVFS, name, file, flags, outFlags);
    if (ret == SQLITE_OK) {
        ClosePtr defaultClosePtr = file->pMethods->xClose;
        assert(defaultClosePtr);
        sqlite3_io_methods *methods = static_cast<sqlite3_io_methods *>(
            std::malloc(sizeof(sqlite3_io_methods)));
        if (!methods) {
            file->pMethods->xClose(file);
            return SQLITE_NOMEM;
        }
        memcpy(methods, file->pMethods, sizeof(sqlite3_io_methods));
        methods->xClose = VFSClose;
        if (appdata->fakeSync) {
            // Disable xSync because it can be significantly slow and we don't
            // need
            // that level of data integrity guarantee for the cache.
            methods->xSync = VSFNoOpLockUnlockSync;
        }
        if (appdata->fakeLock) {
            methods->xLock = VSFNoOpLockUnlockSync;
            methods->xUnlock = VSFNoOpLockUnlockSync;
        }
        file->pMethods = methods;
        // Save original xClose pointer at end of file structure
        std::memcpy(reinterpret_cast<char *>(file) + defaultVFS->szOsFile,
                    &defaultClosePtr, sizeof(ClosePtr));
    }
    return ret;
}

// ---------------------------------------------------------------------------

static int VFSCustomAccess(sqlite3_vfs *vfs, const char *zName, int flags,
                           int *pResOut) {
    sqlite3_vfs *defaultVFS = static_cast<sqlite3_vfs *>(vfs->pAppData);
    // Do not bother stat'ing for journal or wal files
    if (std::strstr(zName, "-journal") || std::strstr(zName, "-wal")) {
        *pResOut = false;
        return SQLITE_OK;
    }
    return defaultVFS->xAccess(defaultVFS, zName, flags, pResOut);
}

// ---------------------------------------------------------------------------

// SQLite3 logging infrastructure
static void projSqlite3LogCallback(void *, int iErrCode, const char *zMsg) {
    fprintf(stderr, "SQLite3 message: (code %d) %s\n", iErrCode, zMsg);
}

namespace {
struct InstallSqliteLogger {
    InstallSqliteLogger() {
        if (getenv("PROJ_LOG_SQLITE3") != nullptr) {
            sqlite3_config(SQLITE_CONFIG_LOG, projSqlite3LogCallback, nullptr);
        }
    }

    static InstallSqliteLogger &GetSingleton() {
        static InstallSqliteLogger installSqliteLogger;
        return installSqliteLogger;
    }
};
} // namespace

// ---------------------------------------------------------------------------

std::unique_ptr<SQLite3VFS> SQLite3VFS::create(bool fakeSync, bool fakeLock,
                                               bool skipStatJournalAndWAL) {

    // Install SQLite3 logger if PROJ_LOG_SQLITE3 env var is defined
    InstallSqliteLogger::GetSingleton();

    // Call to sqlite3_initialize() is normally not needed, except for
    // people building SQLite3 with -DSQLITE_OMIT_AUTOINIT
    sqlite3_initialize();
    sqlite3_vfs *defaultVFS = sqlite3_vfs_find(nullptr);
    assert(defaultVFS);

    auto vfs = new pj_sqlite3_customvfs();

    auto vfsUnique = std::unique_ptr<SQLite3VFS>(new SQLite3VFS(vfs));

    std::ostringstream buffer;
    buffer << vfs;
    vfs->namePtr = buffer.str();

    vfs->base.iVersion = 1;
    vfs->base.szOsFile = defaultVFS->szOsFile + sizeof(ClosePtr);
    vfs->base.mxPathname = defaultVFS->mxPathname;
    vfs->base.zName = vfs->namePtr.c_str();
    pj_sqlite3_customvfs_appdata *appdata = new pj_sqlite3_customvfs_appdata;
    appdata->fakeSync = fakeSync;
    appdata->fakeLock = fakeLock;
    appdata->defaultVFS = defaultVFS;
    vfs->base.pAppData = appdata;
    vfs->base.xOpen = VFSCustomOpen;
    vfs->base.xDelete = defaultVFS->xDelete;
    vfs->base.xAccess =
        skipStatJournalAndWAL ? VFSCustomAccess : defaultVFS->xAccess;
    vfs->base.xFullPathname = defaultVFS->xFullPathname;
    vfs->base.xDlOpen = defaultVFS->xDlOpen;
    vfs->base.xDlError = defaultVFS->xDlError;
    vfs->base.xDlSym = defaultVFS->xDlSym;
    vfs->base.xDlClose = defaultVFS->xDlClose;
    vfs->base.xRandomness = defaultVFS->xRandomness;
    vfs->base.xSleep = defaultVFS->xSleep;
    vfs->base.xCurrentTime = defaultVFS->xCurrentTime;
    vfs->base.xGetLastError = defaultVFS->xGetLastError;
    vfs->base.xCurrentTimeInt64 = defaultVFS->xCurrentTimeInt64;
    if (sqlite3_vfs_register(&(vfs->base), false) == SQLITE_OK) {
        return vfsUnique;
    }
    delete vfsUnique->vfs_;
    vfsUnique->vfs_ = nullptr;
    return nullptr;
}

// ---------------------------------------------------------------------------

#ifdef EMBED_RESOURCE_FILES

struct pj_sqlite3_memvfs : public pj_sqlite3_vfs {
    ~pj_sqlite3_memvfs() override;
};

pj_sqlite3_memvfs::~pj_sqlite3_memvfs() {
    pj_sqlite3_memvfs_deallocate_user_data(&base);
}

/* static */
std::unique_ptr<SQLite3VFS> SQLite3VFS::createMem(const void *membuffer,
                                                  size_t bufferSize) {
    // Install SQLite3 logger if PROJ_LOG_SQLITE3 env var is defined
    InstallSqliteLogger::GetSingleton();

    // Call to sqlite3_initialize() is normally not needed, except for
    // people building SQLite3 with -DSQLITE_OMIT_AUTOINIT
    sqlite3_initialize();

    auto vfs = new pj_sqlite3_memvfs();

    auto vfsUnique = std::unique_ptr<SQLite3VFS>(new SQLite3VFS(vfs));

    std::ostringstream buffer;
    buffer << vfs;
    vfs->namePtr = buffer.str();
    if (pj_sqlite3_memvfs_init(&(vfs->base), vfs->namePtr.c_str(), membuffer,
                               bufferSize) == SQLITE_OK) {
        return vfsUnique;
    }
    delete vfsUnique->vfs_;
    vfsUnique->vfs_ = nullptr;
    return nullptr;
}

#endif

// ---------------------------------------------------------------------------

SQLiteStatement::SQLiteStatement(sqlite3_stmt *hStmtIn) : hStmt(hStmtIn) {}

// ---------------------------------------------------------------------------

NS_PROJ_END
