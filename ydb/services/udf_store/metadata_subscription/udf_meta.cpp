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
}

bool TUdfMeta::TDecoder::Read(const i32 columnIdx, EUdfType& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    auto& pValue = r.items()[columnIdx];
    if (!pValue.has_text_value()) {
        return false;
    }
    // String values are fixed for backward compatibility
    if (pValue.text_value() == "NATIVE_UNSAFE") {
        result = EUdfType::NATIVE_UNSAFE;
    } else if (pValue.text_value() == "WASM") {
        result = EUdfType::WASM;
    } else {
        return false;
    }
    return true;
};

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
    return true;
}

NMetadata::NInternal::TTableRecord TUdfMeta::SerializeToRecord() const {
    return {};
}

TString TUdfMeta::SerializeToString() const {
    return TStringBuilder() << "{" << "Md5: " << Md5 << ", Size: " << Size << ", Name: " << Name << ", Type: " << Type << ", Manifest: " << Manifest << "}";
}

NMetadata::IClassBehaviour::TPtr TUdfMeta::GetBehaviour() {
    return TUdfBehaviour::GetInstance();
}

} // namespace NKikimr::NUdfStore
