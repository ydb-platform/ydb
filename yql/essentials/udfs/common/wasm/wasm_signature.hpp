#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NWasm::NYQL {

enum class EWasmValueType {
    Void,
    I32,
    I64,
    F32,
    F64,
    Unsupported,
};

struct TWasmFunctionSignature
{
    TVector<EWasmValueType> Params;
    EWasmValueType Result = EWasmValueType::Void;
    bool Supported = false;
};

struct TWasmExportInfo
{
    TString Name;
    TWasmFunctionSignature Signature;
    ui64 RuntimeTypeEncoding = 0;
};

const char* WasmValueTypeToString(EWasmValueType type);

bool IsSupportedScalarSignature(const TWasmFunctionSignature& signature);

void ValidateSignature(
    const TWasmFunctionSignature& signature,
    const TWasmFunctionSignature& expected,
    const TString& functionName);

} // namespace NWasm::NYQL
