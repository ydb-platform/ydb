#pragma once

#include <util/folder/path.h>

#include <functional>
#include <utility>

namespace NYql::NFS {

// This function is called by the storage to transfer user data to the provided temporary file path
// Returns content size and md5
using TDataProvider = std::function<std::pair<ui64, TString> (const TFsPath& dstPath)>;

} // NYql
