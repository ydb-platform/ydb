#pragma once

#include "public.h"

#include <ydb/library/wasm/api/bytecode.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

TModuleBytecode GetBuiltinMinimalRuntimeSdk();

TModuleBytecode GetBuiltinYtQlUdfs();

TModuleBytecode GetBuiltinSdk();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
