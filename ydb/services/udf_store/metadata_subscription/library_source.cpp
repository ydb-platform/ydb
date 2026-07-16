#include "library_source.h"
#include "library_behaviour.h"

namespace NKikimr::NUdfStore {

TUdfLibrarySource::TDecoder::TDecoder(const Ydb::ResultSet& rawData) {
    NameIdx = GetFieldIndex(rawData, NameColName);
    Md5Idx = GetFieldIndex(rawData, Md5ColName);
    VersionIdx = GetFieldIndex(rawData, VersionColName);
    SizeIdx = GetFieldIndex(rawData, SizeColName);
    ChunkCountIdx = GetFieldIndex(rawData, ChunkCountColName);
    CompileStatusIdx = GetFieldIndex(rawData, CompileStatusColName);
    CompileErrorIdx = GetFieldIndex(rawData, CompileErrorColName);
}

bool TUdfLibrarySource::TDecoder::Read(const i32 columnIdx, ECompileStatus& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    auto& pValue = r.items()[columnIdx];
    if (!pValue.has_text_value()) {
        return false;
    }
    return TUdfMeta::CompileStatusFromString(pValue.text_value(), result);
}

TVector<NKikimrSchemeOp::TColumnDescription> TUdfLibrarySource::GetColumnDescription() {
    auto makeCol = [](const TString& name, const char* type) {
        NKikimrSchemeOp::TColumnDescription col;
        col.SetName(name);
        col.SetType(type);
        return col;
    };
    return {
        makeCol(NameColName, "Utf8"),
        makeCol(Md5ColName, "Utf8"),
        makeCol(VersionColName, "Uint64"),
        makeCol(SizeColName, "Uint64"),
        makeCol(ChunkCountColName, "Uint64"),
        makeCol(CompileStatusColName, "Utf8"),
        makeCol(CompileErrorColName, "Utf8"),
    };
}

TVector<TString> TUdfLibrarySource::GetPk() {
    return {NameColName};
}

bool TUdfLibrarySource::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetNameIdx(), Name, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetMd5Idx(), Md5, rawValue)) {
        return false;
    }
    if (decoder.GetVersionIdx() >= 0) {
        decoder.Read(decoder.GetVersionIdx(), Version, rawValue);
    }
    if (decoder.GetSizeIdx() >= 0) {
        decoder.Read(decoder.GetSizeIdx(), Size, rawValue);
    }
    if (decoder.GetChunkCountIdx() >= 0) {
        decoder.Read(decoder.GetChunkCountIdx(), ChunkCount, rawValue);
    }
    if (decoder.GetCompileStatusIdx() >= 0) {
        ECompileStatus status = ECompileStatus::Pending;
        if (decoder.Read(decoder.GetCompileStatusIdx(), status, rawValue)) {
            CompileStatus = status;
        }
    } else {
        CompileStatus = ECompileStatus::Pending;
    }
    if (decoder.GetCompileErrorIdx() >= 0) {
        decoder.Read(decoder.GetCompileErrorIdx(), CompileError, rawValue);
    }
    return true;
}

NMetadata::NInternal::TTableRecord TUdfLibrarySource::SerializeToRecord() const {
    return {};
}

TString TUdfLibrarySource::SerializeToString() const {
    return TStringBuilder()
        << "{Name: " << Name
        << ", Md5: " << Md5
        << ", Version: " << Version
        << ", Size: " << Size
        << ", ChunkCount: " << ChunkCount
        << ", CompileStatus: " << TUdfMeta::CompileStatusToString(CompileStatus)
        << "}";
}

NMetadata::IClassBehaviour::TPtr TUdfLibrarySource::GetBehaviour() {
    return TLibraryBehaviour::GetInstance();
}

} // namespace NKikimr::NUdfStore
