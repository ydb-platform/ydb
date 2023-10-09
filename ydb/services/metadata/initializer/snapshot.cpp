#include "snapshot.h"

namespace NKikimr::NMetadata::NInitializer {

bool TSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_ABORT_UNLESS(rawDataResult.result_sets().size() == 1);
    {
        auto& rawData = rawDataResult.result_sets()[0];
        TDBInitialization::TDecoder decoder(rawData);
        for (auto&& r : rawData.rows()) {
            TDBInitialization initObject;
            if (!initObject.DeserializeFromRecord(decoder, r)) {
                ALS_ERROR(NKikimrServices::METADATA_INITIALIZER) << "cannot parse initialization info for snapshot";
                continue;
            }
            Objects.emplace(initObject, initObject);
        }
    }
    return true;
}

TString TSnapshot::DoSerializeToString() const {
    TStringBuilder sb;
    for (auto&& i : Objects) {
        sb << i.first.GetComponentId() << ":" << i.first.GetModificationId() << ";";
    }
    return sb;
}

bool TSnapshot::HasComponent(const TString& componentId) const {
    for (auto&& i : Objects) {
        if (i.first.GetComponentId() == componentId) {
            return true;
        }
    }
    return false;
}

}
