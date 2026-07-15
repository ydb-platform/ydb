#include "udf_meta.h"
#include "udf_behaviour.h"
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NUdfStore {

TUdfMeta::TDecoder::TDecoder(const Ydb::ResultSet& rawData) {
    Md5Idx = GetFieldIndex(rawData, Md5ColName);
    SizeIdx = GetFieldIndex(rawData, SizeColName);
    NameIdx = GetFieldIndex(rawData, NameColName);
    TypeIdx = GetFieldIndex(rawData, TypeColName);
    ManifestIdx = GetFieldIndex(rawData, ManifestColName);
    VersionIdx = GetFieldIndex(rawData, VersionColName);
    CompileStatusIdx = GetFieldIndex(rawData, CompileStatusColName);
    CompileErrorIdx = GetFieldIndex(rawData, CompileErrorColName);
}

bool TUdfMeta::TDecoder::Read(const i32 columnIdx, EUdfType& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    auto& pValue = r.items()[columnIdx];
    if (!pValue.has_text_value()) {
        return false;
    }
    if (pValue.text_value() == "NATIVE_UNSAFE") {
        result = EUdfType::NATIVE_UNSAFE;
    } else if (pValue.text_value() == "WASM") {
        result = EUdfType::WASM;
    } else {
        return false;
    }
    return true;
}

bool TUdfMeta::TDecoder::Read(const i32 columnIdx, ECompileStatus& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    auto& pValue = r.items()[columnIdx];
    if (!pValue.has_text_value()) {
        return false;
    }
    return CompileStatusFromString(pValue.text_value(), result);
}

TString TUdfMeta::CompileStatusToString(ECompileStatus status) {
    switch (status) {
        case ECompileStatus::Pending:
            return "pending";
        case ECompileStatus::Compiling:
            return "compiling";
        case ECompileStatus::Ready:
            return "ready";
        case ECompileStatus::Failed:
            return "failed";
    }
    return "pending";
}

bool TUdfMeta::CompileStatusFromString(TStringBuf value, ECompileStatus& result) {
    if (value == "pending") {
        result = ECompileStatus::Pending;
        return true;
    }
    if (value == "compiling") {
        result = ECompileStatus::Compiling;
        return true;
    }
    if (value == "ready") {
        result = ECompileStatus::Ready;
        return true;
    }
    if (value == "failed") {
        result = ECompileStatus::Failed;
        return true;
    }
    return false;
}

TVector<NKikimrSchemeOp::TColumnDescription> TUdfMeta::GetColumnDescription(){
    auto makeCol = [](const TString& name, const char* type) {
        NKikimrSchemeOp::TColumnDescription col;
        col.SetName(name);
        col.SetType(type);
        return col;
    };
    return {
        makeCol(Md5ColName, "Utf8"),
        makeCol(SizeColName, "Uint64"),
        makeCol(NameColName, "Utf8"),
        makeCol(TypeColName, "Utf8"),
        makeCol(ManifestColName, "Json"),
        makeCol(VersionColName, "Uint64"),
        makeCol(CompileStatusColName, "Utf8"),
        makeCol(CompileErrorColName, "Utf8"),
    };
}

TVector<TString> TUdfMeta::GetPk() {
    return {Md5ColName};
}


bool TUdfMeta::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetMd5Idx(), Md5, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetSizeIdx(), Size, rawValue)) {
        return false;
    }
    if (Size == 0) {
        return false;
    }
    if (!decoder.Read(decoder.GetNameIdx(), Name, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetTypeIdx(), Type, rawValue)) {
        return false;
    }
    if (decoder.GetManifestIdx() >= 0) {
        decoder.Read(decoder.GetManifestIdx(), Manifest, rawValue);
    }
    if (decoder.GetVersionIdx() >= 0) {
        decoder.Read(decoder.GetVersionIdx(), Version, rawValue);
    }
    if (decoder.GetCompileStatusIdx() >= 0) {
        ECompileStatus status = ECompileStatus::Pending;
        if (decoder.Read(decoder.GetCompileStatusIdx(), status, rawValue)) {
            CompileStatus = status;
        }
    } else if (Type == EUdfType::WASM) {
        CompileStatus = ECompileStatus::Pending;
    }
    if (decoder.GetCompileErrorIdx() >= 0) {
        decoder.Read(decoder.GetCompileErrorIdx(), CompileError, rawValue);
    }
    return true;
}

NMetadata::NInternal::TTableRecord TUdfMeta::SerializeToRecord() const {
    return {};
}

TString TUdfMeta::SerializeToString() const {
    return TStringBuilder() << "{" << "Md5: " << Md5 << ", Size: " << Size << ", Name: " << Name
        << ", Type: " << (Type == EUdfType::WASM ? "WASM" : "NATIVE_UNSAFE")
        << ", Manifest: " << Manifest << ", Version: " << Version
        << ", CompileStatus: " << CompileStatusToString(CompileStatus) << "}";
}

NMetadata::IClassBehaviour::TPtr TUdfMeta::GetBehaviour() {
    return TUdfBehaviour::GetInstance();
}

} // namespace NKikimr::NUdfStore
