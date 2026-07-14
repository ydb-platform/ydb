#include "wasm_signature.hpp"

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NWasm::NYQL {

const char* WasmValueTypeToString(EWasmValueType type)
{
    switch (type) {
        case EWasmValueType::Void:
            return "void";
        case EWasmValueType::I32:
            return "i32";
        case EWasmValueType::I64:
            return "i64";
        case EWasmValueType::F32:
            return "f32";
        case EWasmValueType::F64:
            return "f64";
        case EWasmValueType::Unsupported:
            return "unsupported";
    }
    return "unsupported";
}

bool IsSupportedScalarSignature(const TWasmFunctionSignature& signature)
{
    for (const auto param : signature.Params) {
        switch (param) {
            case EWasmValueType::I32:
            case EWasmValueType::I64:
            case EWasmValueType::F32:
            case EWasmValueType::F64:
                break;
            default:
                return false;
        }
    }

    switch (signature.Result) {
        case EWasmValueType::Void:
        case EWasmValueType::I32:
        case EWasmValueType::I64:
        case EWasmValueType::F32:
        case EWasmValueType::F64:
            return true;
        default:
            return false;
    }
}

void ValidateSignature(
    const TWasmFunctionSignature& signature,
    const TWasmFunctionSignature& expected,
    const TString& functionName)
{
    if (signature.Params != expected.Params || signature.Result != expected.Result) {
        TStringBuilder expectedSig;
        expectedSig << '(';
        for (size_t i = 0; i < expected.Params.size(); ++i) {
            if (i > 0) {
                expectedSig << ", ";
            }
            expectedSig << WasmValueTypeToString(expected.Params[i]);
        }
        expectedSig << ") -> " << WasmValueTypeToString(expected.Result);

        TStringBuilder actualSig;
        actualSig << '(';
        for (size_t i = 0; i < signature.Params.size(); ++i) {
            if (i > 0) {
                actualSig << ", ";
            }
            actualSig << WasmValueTypeToString(signature.Params[i]);
        }
        actualSig << ") -> " << WasmValueTypeToString(signature.Result);

        ythrow yexception()
            << "Wasm function \"" << functionName << "\" has signature " << actualSig
            << ", expected " << expectedSig;
    }
}

} // namespace NWasm::NYQL
