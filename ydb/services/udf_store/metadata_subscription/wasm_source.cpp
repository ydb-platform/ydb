#include "wasm_source.h"

namespace NKikimr::NUdfStore {

TVector<NKikimrSchemeOp::TColumnDescription> TUdfWasmSource::GetColumnDescription() {
    auto makeCol = [](const TString& name, const char* type) {
        NKikimrSchemeOp::TColumnDescription col;
        col.SetName(name);
        col.SetType(type);
        return col;
    };
    return {
        makeCol(Md5ColName, "Utf8"),
        makeCol(VersionColName, "Uint64"),
        makeCol(BodyColName, "String"),
    };
}

TVector<TString> TUdfWasmSource::GetPk() {
    return {Md5ColName};
}

} // namespace NKikimr::NUdfStore
