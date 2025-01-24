#include "snapshot.h"

namespace NKikimr::NMetadata::NSecret {

void TSnapshot::BuildIndex() {
    IndexByName.clear();
    for (const auto& [id, secret] : Secrets) {
        IndexByName[id.GetSecretId()].emplace_back(id);
    }
}

bool TSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_ABORT_UNLESS(rawDataResult.result_sets().size() == 2);
    ParseSnapshotObjects<TSecret>(rawDataResult.result_sets()[0], [this](TSecret&& s) {Secrets.emplace(s, s); });
    ParseSnapshotObjects<TAccess>(rawDataResult.result_sets()[1], [this](TAccess&& s) {Access.emplace_back(std::move(s)); });
    BuildIndex();
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
    std::optional<TSecretIdOrValue> sId = TSecretIdOrValue::DeserializeFromString(stringForPath);
    if (!sId) {
        return false;
    }
    if (auto value = GetSecretValue(*sId); value.IsSuccess()) {
        stringForPath = value.DetachResult();
        return true;
    }
    return false;
}

bool TSnapshot::CheckSecretAccess(const TSecretIdOrValue& sIdOrValue, const NACLib::TUserToken& userToken) const {
    if (std::holds_alternative<TString>(sIdOrValue.GetState()) || std::holds_alternative<std::monostate>(sIdOrValue.GetState())) {
        return true;
    }

    auto findId = std::visit(TOverloaded(
        [](std::monostate) -> const TSecretId* {
            Y_ABORT();
        },
        [](const TSecretId& id) -> const TSecretId*{
            return &id;
        },
        [this](const TSecretName& name) -> const TSecretId*{
            const auto findSecrets = IndexByName.FindPtr(name.GetSecretId());
            if (!findSecrets) {
                return nullptr;
            }
            AFL_VERIFY(!findSecrets->empty());
            if (findSecrets->size() > 1) {
                return nullptr;
            }
            return &*findSecrets->begin();
        },
        [](const TString& /* value */) -> const TSecretId*{
            Y_ABORT();
        }
    ),
    sIdOrValue.GetState());

    auto it = Secrets.find(*findId);
    if (it == Secrets.end()) {
        return false;
    }
    if (it->second.GetOwnerUserId() == userToken.GetUserSID()) {
        return true;
    }
    for (auto&& i : Access) {
        if (i != *findId) {
            continue;
        }
        if (userToken.IsExist(i.GetAccessSID())) {
            return true;
        }
    }
    return false;
}

TConclusion<TString> TSnapshot::GetSecretValue(const TSecretIdOrValue& sId) const {
    return std::visit(TOverloaded(
        [](std::monostate) -> TConclusion<TString>{
            return TConclusionStatus::Fail("Empty secret id");
        },
        [this](const TSecretId& id) -> TConclusion<TString>{
            if (const auto findSecret = Secrets.find(id); findSecret != Secrets.end()) {
                return findSecret->second.GetValue();
            }
            return TConclusionStatus::Fail(TStringBuilder() << "No such secret: " << id.SerializeToString());
        },
        [this](const TSecretName& name) -> TConclusion<TString>{
            if (const auto findSecrets = IndexByName.FindPtr(name.GetSecretId())) {
                AFL_VERIFY(!findSecrets->empty());
                if (findSecrets->size() > 1) {
                    return TConclusionStatus::Fail(TStringBuilder() << "Can't identify secret: More than 1 secret found with such name: " << name.GetSecretId());
                }
                auto secret = Secrets.find(*findSecrets->begin());
                AFL_VERIFY(secret != Secrets.end())("secret", findSecrets->begin()->SerializeToString());
                return secret->second.GetValue();
            }
            return TConclusionStatus::Fail(TStringBuilder() << "No such secret: " << name.SerializeToString());
        },
        [](const TString& value) -> TConclusion<TString>{
            return value;
        }
    ),
    sId.GetState());
}

std::vector<TSecretId> TSnapshot::GetSecretIds(const std::optional<NACLib::TUserToken>& userToken, const TString& secretId) const {
    std::vector<TSecretId> secretIds;
    for (const auto& [key, value]: Secrets) {
        if (key.GetSecretId() != secretId) {
            continue;
        }
        if (userToken && !CheckSecretAccess(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(key), *userToken)) {
            continue;
        }
        secretIds.push_back(key);
    }
    return secretIds;
}

}
