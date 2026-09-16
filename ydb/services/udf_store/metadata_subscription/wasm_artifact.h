#pragma once

#include <ydb/services/metadata/manager/object.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore {

enum class EWasmArtifactKind {
    Module,
    Library,
};

//! One row per compiled upload. `id` is the module name and `uid` is the
//! upload it was built from, and both are part of the key: a compile that
//! outlives a re-upload writes its own row instead of overwriting the artifact
//! the current upload is about to be loaded from.
class TUdfWasmArtifact: public NMetadata::NModifications::TObject<TUdfWasmArtifact> {
public:
    static inline const TString IdColName = "id";
    static inline const TString KindColName = "kind";
    static inline const TString UidColName = "uid";
    static inline const TString VersionColName = "version";
    static inline const TString FormatColName = "format";
    static inline const TString WasmDataSizeColName = "wasm_data_size";
    static inline const TString WasmDataChunkCountColName = "wasm_data_chunk_count";
    static inline const TString ObjectCodeSizeColName = "object_code_size";
    static inline const TString ObjectCodeChunkCountColName = "object_code_chunk_count";
    static inline const TString CompiledAtColName = "compiled_at";

    YDB_ACCESSOR_DEF(TString, Id);
    YDB_ACCESSOR_DEF(TString, Kind);
    YDB_ACCESSOR_DEF(TString, Uid);
    YDB_ACCESSOR_DEF(ui64, Version);
    YDB_ACCESSOR_DEF(TString, Format);
    YDB_ACCESSOR_DEF(ui64, WasmDataSize);
    YDB_ACCESSOR_DEF(ui64, WasmDataChunkCount);
    YDB_ACCESSOR_DEF(ui64, ObjectCodeSize);
    YDB_ACCESSOR_DEF(ui64, ObjectCodeChunkCount);
    YDB_ACCESSOR_DEF(ui64, CompiledAt);

public:
    static TVector<NKikimrSchemeOp::TColumnDescription> GetColumnDescription();
    static TVector<TString> GetPk();
};

TString WasmArtifactKindToString(EWasmArtifactKind kind);

} // namespace NKikimr::NUdfStore
