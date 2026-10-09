#pragma once

#include <ydb/udfs/wasm/sdk/services/rows.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NFq::NWasmServices {

struct TServiceField {
    TString Name;
    NYdb::NWasm::NServices::EValueType Type;
    ui32 MaxBytes = 0;
};

struct TServiceMethod {
    TString Name;
    ui32 Id;
    bool Batch;
    ui32 MaxBatchRows;
    ui32 MaxOutputRowBytes;
    ui32 MinInputRowBytes;
    ui32 MaxInputRowBytes;
    TVector<TServiceField> Input;
    TVector<TServiceField> Output;
};

struct TServiceManifest {
    TString Name;
    THashMap<TString, TServiceMethod> Methods;
};

TServiceManifest ParseServiceManifest(TStringBuf json);

} // namespace NFq::NWasmServices
