#include "object.h"
#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NMetadataInitializer {

TString TDBInitialization::GetInternalStorageTablePath() {
    return "initializations";
}

bool TDBInitialization::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetComponentIdIdx(), ComponentId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetModificationIdIdx(), ModificationId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetInstantIdx(), Instant, rawValue)) {
        return false;
    }
    return true;
}

NKikimr::NMetadataManager::TTableRecord TDBInitialization::SerializeToRecord() const {
    NMetadataManager::TTableRecord result;
    result.SetColumn(TDecoder::ComponentId, NMetadataManager::TYDBValue::Bytes(ComponentId));
    result.SetColumn(TDecoder::ModificationId, NMetadataManager::TYDBValue::Bytes(ModificationId));
    result.SetColumn(TDecoder::Instant, NMetadataManager::TYDBValue::UInt32(Instant.Seconds()));
    return result;
}

void TDBInitialization::AlteringPreparation(std::vector<TDBInitialization>&& objects,
    NMetadataManager::IAlterPreparationController<TDBInitialization>::TPtr controller,
    const NMetadata::IOperationsManager::TModificationContext& /*context*/) {
    controller->PreparationFinished(std::move(objects));
}

std::vector<Ydb::Column> TDBInitialization::TDecoder::GetColumns() {
    return {
        NMetadataManager::TYDBColumn::Bytes(ComponentId),
        NMetadataManager::TYDBColumn::Bytes(ModificationId),
        NMetadataManager::TYDBColumn::UInt32(Instant)
    };
}

std::vector<Ydb::Column> TDBInitialization::TDecoder::GetPKColumns() {
    return {
        NMetadataManager::TYDBColumn::Bytes(ComponentId),
        NMetadataManager::TYDBColumn::Bytes(ModificationId)
    };
}

std::vector<TString> TDBInitialization::TDecoder::GetPKColumnIds() {
    return { ComponentId, ModificationId };
}

}
