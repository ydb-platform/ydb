/******************************************************************************
 * Project:  PROJ
 * Purpose:  File manager
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

#ifndef FILEMANAGER_HPP_INCLUDED
#define FILEMANAGER_HPP_INCLUDED

#include <memory>
#include <string>
#include <vector>

#include "proj.h"
#include "proj/util.hpp"

//! @cond Doxygen_Suppress

NS_PROJ_START

class File;

enum class FileAccess {
    READ_ONLY,   // "rb"
    READ_UPDATE, // "r+b"
    CREATE,      // "w+b"
};

class FileManager {
  private:
    FileManager() = delete;

  public:
    // "Low-level" interface.
    static PROJ_DLL std::unique_ptr<File>
    open(PJ_CONTEXT *ctx, const char *filename, FileAccess access);
    static PROJ_DLL bool exists(PJ_CONTEXT *ctx, const char *filename);
    static bool mkdir(PJ_CONTEXT *ctx, const char *filename);
    static bool unlink(PJ_CONTEXT *ctx, const char *filename);
    static bool rename(PJ_CONTEXT *ctx, const char *oldPath,
                       const char *newPath);
    static std::string getProjDataEnvVar(PJ_CONTEXT *ctx);

    // "High-level" interface, honoring PROJ_DATA and the like.
    static std::unique_ptr<File>
    open_resource_file(PJ_CONTEXT *ctx, const char *name,
                       char *out_full_filename = nullptr,
                       size_t out_full_filename_size = 0);

    static void fillDefaultNetworkInterface(PJ_CONTEXT *ctx);

    static void clearMemoryCache();
};

// ---------------------------------------------------------------------------

class File {
  protected:
    std::string name_;
    std::string readLineBuffer_{};
    bool eofReadLine_ = false;
    explicit File(const std::string &filename);

  public:
    virtual PROJ_DLL ~File();
    virtual size_t read(void *buffer, size_t sizeBytes) = 0;
    virtual size_t write(const void *buffer, size_t sizeBytes) = 0;
    virtual bool seek(unsigned long long offset, int whence = SEEK_SET) = 0;
    virtual unsigned long long tell() = 0;
    virtual void reassign_context(PJ_CONTEXT *ctx) = 0;
    virtual bool hasChanged() const = 0;
    std::string PROJ_DLL read_line(size_t maxLen, bool &maxLenReached,
                                   bool &eofReached);

    const std::string &name() const { return name_; }
};

// ---------------------------------------------------------------------------

std::unique_ptr<File> pj_network_file_open(PJ_CONTEXT *ctx,
                                           const char *filename);
NS_PROJ_END

// Exported for projsync
std::vector<std::string> PROJ_DLL pj_get_default_searchpaths(PJ_CONTEXT *ctx);

//! @endcond Doxygen_Suppress

#endif // FILEMANAGER_HPP_INCLUDED
