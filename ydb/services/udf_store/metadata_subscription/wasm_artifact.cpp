#include "wasm_artifact.h"

namespace NKikimr::NUdfStore {

TVector<NKikimrSchemeOp::TColumnDescription> TUdfWasmArtifact::GetColumnDescription() {
    auto makeCol = [](const TString& name, const char* type) {
        NKikimrSchemeOp::TColumnDescription col;
        col.SetName(name);
        col.SetType(type);
        return col;
    };
    return {
        makeCol(IdColName, "Utf8"),
        makeCol(KindColName, "Utf8"),
        makeCol(SourceMd5ColName, "Utf8"),
        makeCol(VersionColName, "Uint64"),
        makeCol(FormatColName, "Utf8"),
        makeCol(WasmDataSizeColName, "Uint64"),
        makeCol(WasmDataChunkCountColName, "Uint64"),
        makeCol(ObjectCodeSizeColName, "Uint64"),
        makeCol(ObjectCodeChunkCountColName, "Uint64"),
        makeCol(CompiledAtColName, "Timestamp"),
    };
}

TVector<TString> TUdfWasmArtifact::GetPk() {
    return {IdColName, KindColName};
}

TString WasmArtifactKindToString(EWasmArtifactKind kind) {
    switch (kind) {
        case EWasmArtifactKind::Module:
            return "module";
        case EWasmArtifactKind::Library:
            return "library";
    }
    return "unknown";
}

} // namespace NKikimr::NUdfStore
