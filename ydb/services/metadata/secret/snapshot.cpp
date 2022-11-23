#include "snapshot.h"

namespace NKikimr::NMetadata::NSecret {

bool TSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_VERIFY(rawDataResult.result_sets().size() == 2);
    {
        auto& rawData = rawDataResult.result_sets()[0];
        TSecret::TDecoder decoder(rawData);
        for (auto&& r : rawData.rows()) {
            TSecret object;
            if (!object.DeserializeFromRecord(decoder, r)) {
                ALS_ERROR(NKikimrServices::METADATA_SECRET) << "cannot parse secret info for snapshot";
                continue;
            }
            Secrets.emplace(object, object);
        }
    }
    {
        auto& rawData = rawDataResult.result_sets()[1];
        TAccess::TDecoder decoder(rawData);
        for (auto&& r : rawData.rows()) {
            TAccess object;
            if (!object.DeserializeFromRecord(decoder, r)) {
                ALS_ERROR(NKikimrServices::METADATA_SECRET) << "cannot parse secret info for snapshot";
                continue;
            }
            Access.emplace_back(object);
        }
    }
    return true;
}

TString TSnapshot::DoSerializeToString() const {
    TStringBuilder sb;
    sb << "SECRETS:";
    for (auto&& i : Secrets) {
        sb << i.first.GetOwnerUserId() << ":" << i.first.GetSecretId() << ":" << i.second.GetValue() << ";";
    }
    sb << "ACCESS:";
    for (auto&& i : Access) {
        sb << i.GetOwnerUserId() << ":" << i.GetSecretId() << ":" << i.GetAccessUserId() << ";";
    }
    return sb;
}

}
