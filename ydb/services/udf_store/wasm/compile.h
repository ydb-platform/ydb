#pragma once

#include <ydb/library/wasm/api/bytecode.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NWasm {

NYdb::NWasm::EBytecodeFormat DetectBytecodeFormat(TStringBuf extension);

TString CompileModuleObjectCode(TStringBuf wasmBytes, NYdb::NWasm::EBytecodeFormat format);

} // namespace NKikimr::NUdfStore::NWasm
