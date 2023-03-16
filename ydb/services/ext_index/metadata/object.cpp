#include "object.h"
#include "behaviour.h"
#include <ydb/services/metadata/manager/ydb_value_operator.h>

#include <util/folder/path.h>

namespace NKikimr::NMetadata::NCSIndex {

IClassBehaviour::TPtr TObject::GetBehaviour() {
    return TBehaviour::GetInstance();
}

bool TObject::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetIndexIdIdx(), IndexId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetTablePathIdx(), TablePath, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetActiveIdx(), ActiveFlag, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetDeleteIdx(), DeleteFlag, rawValue)) {
        return false;
    }
    if (!decoder.ReadFromJson(decoder.GetExtractorIdx(), Extractor, rawValue)) {
        return false;
    }
    return true;
}

NKikimr::NMetadata::NInternal::TTableRecord TObject::SerializeToRecord() const {
    NInternal::TTableRecord result;
    result.SetColumn(TDecoder::IndexId, NInternal::TYDBValue::Utf8(IndexId));
    result.SetColumn(TDecoder::TablePath, NInternal::TYDBValue::Utf8(TablePath));
    result.SetColumn(TDecoder::Delete, NInternal::TYDBValue::Bool(DeleteFlag));
    result.SetColumn(TDecoder::Active, NInternal::TYDBValue::Bool(ActiveFlag));
    result.SetColumn(TDecoder::Extractor, NInternal::TYDBValue::Utf8(Extractor.SerializeToJson().GetStringRobust()));
    return result;
}

std::optional<NKikimr::NMetadata::NCSIndex::TObject> TObject::Build(const TString& tablePath, const TString& indexId, IIndexExtractor::TPtr extractor) {
    if (!extractor) {
        return {};
    }
    if (!tablePath || !indexId) {
        return {};
    }
    TObject result;
    result.TablePath = TFsPath(tablePath).Fix().GetPath();
    result.IndexId = indexId;
    result.Extractor = TInterfaceContainer<IIndexExtractor>(extractor);
    return result;
}

TString TObject::GetIndexTablePath() const {
    return GetBehaviour()->GetStorageTableDirectory() + "/" + GetUniqueId();
}

bool TObject::TryProvideTtl(const NKikimrSchemeOp::TColumnTableDescription& csDescription, Ydb::Table::CreateTableRequest* cRequest) {
    if (csDescription.HasTtlSettings() && csDescription.GetTtlSettings().HasEnabled()) {
        auto& ttl = csDescription.GetTtlSettings().GetEnabled();
        if (!ttl.HasExpireAfterSeconds()) {
            return false;
        }
        bool found = false;
        for (auto&& i : csDescription.GetSchema().GetKeyColumnNames()) {
            if (ttl.GetColumnName() == i) {
                found = true;
            }
        }
        if (!found) {
            return false;
        }
        if (cRequest) {
            auto& newTtl = *cRequest->mutable_ttl_settings()->mutable_date_type_column();
            newTtl.set_column_name("pk_" + ttl.GetColumnName());
            newTtl.set_expire_after_seconds(ttl.GetExpireAfterSeconds());
        }
    }
    return true;
}

}
