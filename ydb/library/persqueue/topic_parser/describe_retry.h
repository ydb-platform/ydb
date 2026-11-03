#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <ydb/core/base/path.h>

namespace NKikimr::NMsgBusProxy::NPqMetaCacheV2 {

// Topics whose primary path was not found are described again as DbRoot/<account>/<modern name>.
// `isUnknown` is indexed the same way as `topics`: the caller has one scheme-cache entry per topic.
// A skipped entry does not shift the index, so the next result stays paired with its own topic.
template <typename TTopics, typename TIsUnknown>
TVector<ui64> SelectSecondTryTopics(
        const TString& dbRoot,
        TTopics& topics,
        const TIsUnknown& isUnknown)
{
    TVector<ui64> secondTry;
    if (dbRoot.empty()) {
        return secondTry;
    }
    for (size_t index = 0; index < topics.size(); ++index) {
        if (!isUnknown(index)) {
            continue;
        }
        const auto& account = topics[index]->GetAccount_();
        if (!account.Defined() || account->empty()) {
            continue;
        }
        topics[index]->SetDatabase(NKikimr::JoinPath({dbRoot, *account}));
        const auto& secondary = topics[index]->GetSecondaryPath("");
        if (!secondary.Defined()) {
            continue;
        }
        secondTry.push_back(index);
    }
    return secondTry;
}

} // NKikimr::NMsgBusProxy::NPqMetaCacheV2
