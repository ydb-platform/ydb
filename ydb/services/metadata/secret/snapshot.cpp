#include "snapshot.h"

namespace NKikimr::NMetadata::NSecret {

bool TSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_VERIFY(rawDataResult.result_sets().size() == 2);
    ParseSnapshotObjects<TSecret>(rawDataResult.result_sets()[0], [this](TSecret&& s) {Secrets.emplace(s, s); });
    ParseSnapshotObjects<TAccess>(rawDataResult.result_sets()[1], [this](TAccess&& s) {Access.emplace_back(std::move(s)); });
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
        sb << i.GetOwnerUserId() << ":" << i.GetSecretId() << ":" << i.GetAccessSID() << ";";
    }
    return sb;
}

bool TSnapshot::PatchString(TString& stringForPath) const {
    TSecretId sId;
    if (!sId.DeserializeFromString(stringForPath)) {
        return false;
    }
    auto it = Secrets.find(sId);
    if (it == Secrets.end()) {
        return false;
    }
    stringForPath = it->second.GetValue();
    return true;
}

bool TSnapshot::CheckSecretAccess(const TString& secretableString, const std::optional<NACLib::TUserToken>& userToken) const {
    if (!userToken) {
        return true;
    }
    TSecretId sId;
    if (!sId.DeserializeFromString(secretableString)) {
        return true;
    }
    auto it = Secrets.find(sId);
    if (it == Secrets.end()) {
        return false;
    }
    if (it->second.GetOwnerUserId() == userToken->GetUserSID()) {
        return true;
    }
    for (auto&& i : Access) {
        if (i != sId) {
            continue;
        }
        if (userToken->IsExist(i.GetAccessSID())) {
            return true;
        }
    }
    return false;
}

}
