#include "compile.h"

#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <util/generic/yexception.h>

#include <bit>

namespace NKikimr::NUdfStore::NWasm {

namespace {

WAVM::IR::FeatureSpec MakeFeatureSpec() {
    auto featureSpec = WAVM::IR::FeatureSpec();
    featureSpec.memory64 = true;
    featureSpec.table64 = true;
    featureSpec.exceptionHandling = true;
    return featureSpec;
}

WAVM::IR::Module ParseWasmModule(TStringBuf wasmBytes, NYdb::NWasm::EBytecodeFormat format) {
    auto featureSpec = MakeFeatureSpec();
    switch (format) {
        case NYdb::NWasm::EBytecodeFormat::HumanReadable: {
            WAVM::IR::Module irModule(featureSpec);
            std::vector<WAVM::WAST::Error> errors;
            if (!WAVM::WAST::parseModule(wasmBytes.data(), wasmBytes.size() + 1, irModule, errors)) {
                ythrow yexception() << "Failed to parse WAST module for object-code compilation";
            }
            return irModule;
        }
        case NYdb::NWasm::EBytecodeFormat::Binary: {
            WAVM::IR::Module irModule(featureSpec);
            auto loadError = WAVM::WASM::LoadError();
            const bool parsed = WAVM::WASM::loadBinaryModule(
                std::bit_cast<const WAVM::U8*>(wasmBytes.data()),
                wasmBytes.size(),
                irModule,
                &loadError);
            if (!parsed) {
                ythrow yexception() << "Failed to parse wasm binary for object-code compilation: "
                                    << loadError.message;
            }
            return irModule;
        }
    }
    ythrow yexception() << "Unsupported wasm bytecode format";
}

} // namespace

NYdb::NWasm::EBytecodeFormat DetectBytecodeFormat(TStringBuf extension) {
    if (extension == "wat" || extension == "wast") {
        return NYdb::NWasm::EBytecodeFormat::HumanReadable;
    }
    return NYdb::NWasm::EBytecodeFormat::Binary;
}

NYdb::NWasm::EBytecodeFormat DetectBytecodeFormatFromBody(TStringBuf body) {
    // WASM binary magic: 0x00 0x61 0x73 0x6d ("\0asm").
    if (body.size() >= 4
        && body[0] == '\0'
        && body[1] == 'a'
        && body[2] == 's'
        && body[3] == 'm')
    {
        return NYdb::NWasm::EBytecodeFormat::Binary;
    }
    return NYdb::NWasm::EBytecodeFormat::HumanReadable;
}

TString CompileModuleObjectCode(TStringBuf wasmBytes, NYdb::NWasm::EBytecodeFormat format) {
    auto irModule = ParseWasmModule(wasmBytes, format);
    auto compiled = WAVM::Runtime::compileModule(irModule);
    auto objectCode = WAVM::Runtime::getObjectCode(compiled);
    return TString(
        std::bit_cast<const char*>(objectCode.data()),
        objectCode.size());
}

} // namespace NKikimr::NUdfStore::NWasm
