#include "udf_module.h"
#include "udf_module_behaviour.h"
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NUdfStore {

TUdfModule::TDecoder::TDecoder(const Ydb::ResultSet& rawData) {
    UidIdx = GetFieldIndex(rawData, UidColName);
    Md5Idx = GetFieldIndex(rawData, Md5ColName);
    SizeIdx = GetFieldIndex(rawData, SizeColName);
    NameIdx = GetFieldIndex(rawData, NameColName);
    TypeIdx = GetFieldIndex(rawData, TypeColName);
    ManifestIdx = GetFieldIndex(rawData, ManifestColName);
    VersionIdx = GetFieldIndex(rawData, VersionColName);
    ChunkCountIdx = GetFieldIndex(rawData, ChunkCountColName);
    CompileStatusIdx = GetFieldIndex(rawData, CompileStatusColName);
    CompileErrorIdx = GetFieldIndex(rawData, CompileErrorColName);
    CreatedAtIdx = GetFieldIndex(rawData, CreatedAtColName);
    CompileStartedAtIdx = GetFieldIndex(rawData, CompileStartedAtColName);
    CompileFinishedAtIdx = GetFieldIndex(rawData, CompileFinishedAtColName);
}

bool TUdfModule::TDecoder::Read(const i32 columnIdx, EUdfType& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    auto& pValue = r.items()[columnIdx];
    if (!pValue.has_text_value()) {
        return false;
    }
    return TypeFromString(pValue.text_value(), result);
}

bool TUdfModule::TDecoder::Read(const i32 columnIdx, ECompileStatus& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    auto& pValue = r.items()[columnIdx];
    if (!pValue.has_text_value()) {
        return false;
    }
    return CompileStatusFromString(pValue.text_value(), result);
}

TString TUdfModule::TypeToString(EUdfType type) {
    switch (type) {
        case EUdfType::NATIVE_UNSAFE:
            return "NATIVE_UNSAFE";
        case EUdfType::WASM:
            return "WASM";
        case EUdfType::LIBRARY:
            return "LIBRARY";
    }
    return "NATIVE_UNSAFE";
}

bool TUdfModule::TypeFromString(TStringBuf value, EUdfType& result) {
    if (value == "NATIVE_UNSAFE") {
        result = EUdfType::NATIVE_UNSAFE;
        return true;
    }
    if (value == "WASM") {
        result = EUdfType::WASM;
        return true;
    }
    if (value == "LIBRARY") {
        result = EUdfType::LIBRARY;
        return true;
    }
    return false;
}

TString TUdfModule::CompileStatusToString(ECompileStatus status) {
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

bool TUdfModule::CompileStatusFromString(TStringBuf value, ECompileStatus& result) {
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

TVector<NKikimrSchemeOp::TColumnDescription> TUdfModule::GetColumnDescription() {
    auto makeCol = [](const TString& name, const char* type) {
        NKikimrSchemeOp::TColumnDescription col;
        col.SetName(name);
        col.SetType(type);
        return col;
    };
    return {
        makeCol(NameColName, "Utf8"),
        makeCol(UidColName, "Utf8"),
        makeCol(Md5ColName, "Utf8"),
        makeCol(SizeColName, "Uint64"),
        makeCol(TypeColName, "Utf8"),
        makeCol(ManifestColName, "Json"),
        makeCol(VersionColName, "Uint64"),
        makeCol(ChunkCountColName, "Uint64"),
        makeCol(CompileStatusColName, "Utf8"),
        makeCol(CompileErrorColName, "Utf8"),
        makeCol(CreatedAtColName, "Timestamp"),
        makeCol(CompileStartedAtColName, "Timestamp"),
        makeCol(CompileFinishedAtColName, "Timestamp"),
    };
}

TVector<TString> TUdfModule::GetPk() {
    return {NameColName};
}

bool TUdfModule::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetUidIdx(), Uid, rawValue)) {
        return false;
    }
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
    if (decoder.GetChunkCountIdx() >= 0) {
        decoder.Read(decoder.GetChunkCountIdx(), ChunkCount, rawValue);
    }
    if (decoder.GetCompileStatusIdx() >= 0) {
        ECompileStatus status = ECompileStatus::Pending;
        if (decoder.Read(decoder.GetCompileStatusIdx(), status, rawValue)) {
            CompileStatus = status;
        }
    } else if (Type == EUdfType::WASM || Type == EUdfType::LIBRARY) {
        CompileStatus = ECompileStatus::Pending;
    }
    if (decoder.GetCompileErrorIdx() >= 0) {
        decoder.Read(decoder.GetCompileErrorIdx(), CompileError, rawValue);
    }
    if (decoder.GetCreatedAtIdx() >= 0) {
        decoder.Read(decoder.GetCreatedAtIdx(), CreatedAt, rawValue);
    }
    if (decoder.GetCompileStartedAtIdx() >= 0) {
        decoder.Read(decoder.GetCompileStartedAtIdx(), CompileStartedAt, rawValue);
    }
    if (decoder.GetCompileFinishedAtIdx() >= 0) {
        decoder.Read(decoder.GetCompileFinishedAtIdx(), CompileFinishedAt, rawValue);
    }
    return true;
}

NMetadata::NInternal::TTableRecord TUdfModule::SerializeToRecord() const {
    return {};
}

TString TUdfModule::SerializeToString() const {
    return TStringBuilder() << "{"
        << "Uid: " << Uid
        << ", Md5: " << Md5
        << ", Size: " << Size
        << ", Name: " << Name
        << ", Type: " << TypeToString(Type)
        << ", Manifest: " << Manifest
        << ", Version: " << Version
        << ", ChunkCount: " << ChunkCount
        << ", CompileStatus: " << CompileStatusToString(CompileStatus)
        << "}";
}

NMetadata::IClassBehaviour::TPtr TUdfModule::GetBehaviour() {
    return TUdfModuleBehaviour::GetInstance();
}

} // namespace NKikimr::NUdfStore
