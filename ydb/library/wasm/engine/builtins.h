#pragma once

#include "public.h"

#include <ydb/library/wasm/api/bytecode.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

TModuleBytecode GetBuiltinMinimalRuntimeSdk();

TModuleBytecode GetBuiltinYtQlUdfs();

// Returns empty Data when the builtin SDK resource is absent or empty.
TModuleBytecode FindBuiltinSdk();

// Requires a non-empty builtin SDK resource.
TModuleBytecode GetBuiltinSdk();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NWasm
