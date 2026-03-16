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

#ifndef SQLITE3_HPP_INCLUDED
#define SQLITE3_HPP_INCLUDED

#include <memory>

#include <sqlite3.h>

#include "proj.h"
#include "proj/util.hpp"

NS_PROJ_START

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

struct pj_sqlite3_vfs {
    sqlite3_vfs base{};
    std::string namePtr{};

    virtual ~pj_sqlite3_vfs();
};

// ---------------------------------------------------------------------------

class SQLite3VFS {
    pj_sqlite3_vfs *vfs_ = nullptr;

    explicit SQLite3VFS(pj_sqlite3_vfs *vfs);

    SQLite3VFS(const SQLite3VFS &) = delete;
    SQLite3VFS &operator=(const SQLite3VFS &) = delete;

  public:
    ~SQLite3VFS();

    static std::unique_ptr<SQLite3VFS> create(bool fakeSync, bool fakeLock,
                                              bool skipStatJournalAndWAL);

#ifdef EMBED_RESOURCE_FILES
    static std::unique_ptr<SQLite3VFS> createMem(const void *membuffer,
                                                 size_t bufferSize);
#endif

    const char *name() const;
    sqlite3_vfs *raw() { return &(vfs_->base); }
};

// ---------------------------------------------------------------------------

class SQLiteStatement {
    sqlite3_stmt *hStmt = nullptr;
    int iBindIdx = 1;
    int iResIdx = 0;
    SQLiteStatement(const SQLiteStatement &) = delete;
    SQLiteStatement &operator=(const SQLiteStatement &) = delete;

  public:
    explicit SQLiteStatement(sqlite3_stmt *hStmtIn);
    ~SQLiteStatement() { sqlite3_finalize(hStmt); }

    int execute() { return sqlite3_step(hStmt); }

    void bindNull() {
        sqlite3_bind_null(hStmt, iBindIdx);
        iBindIdx++;
    }

    void bindText(const char *txt) {
        sqlite3_bind_text(hStmt, iBindIdx, txt, -1, nullptr);
        iBindIdx++;
    }

    void bindInt64(sqlite3_int64 v) {
        sqlite3_bind_int64(hStmt, iBindIdx, v);
        iBindIdx++;
    }

    void bindBlob(const void *blob, size_t blob_size) {
        sqlite3_bind_blob(hStmt, iBindIdx, blob, static_cast<int>(blob_size),
                          nullptr);
        iBindIdx++;
    }

    const char *getText() {
        auto ret = sqlite3_column_text(hStmt, iResIdx);
        iResIdx++;
        return reinterpret_cast<const char *>(ret);
    }

    sqlite3_int64 getInt64() {
        auto ret = sqlite3_column_int64(hStmt, iResIdx);
        iResIdx++;
        return ret;
    }

    const void *getBlob(int &size) {
        size = sqlite3_column_bytes(hStmt, iResIdx);
        auto ret = sqlite3_column_blob(hStmt, iResIdx);
        iResIdx++;
        return ret;
    }

    void reset() {
        sqlite3_reset(hStmt);
        iBindIdx = 1;
        iResIdx = 0;
    }

    void resetResIndex() { iResIdx = 0; }
};

//! @endcond Doxygen_Suppress

NS_PROJ_END

#endif // SQLITE3_HPP_INCLUDED
