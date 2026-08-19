#pragma once

#include <ydb/library/wasm/api/bytecode.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NWasm {

NYdb::NWasm::EBytecodeFormat DetectBytecodeFormat(TStringBuf extension);

//! Binary if body starts with WASM magic "\\0asm"; otherwise treat as WAT/WAST
//! (may begin with whitespace or ";;" / "(;" comments).
NYdb::NWasm::EBytecodeFormat DetectBytecodeFormatFromBody(TStringBuf body);

TString CompileModuleObjectCode(TStringBuf wasmBytes, NYdb::NWasm::EBytecodeFormat format);

} // namespace NKikimr::NUdfStore::NWasm
