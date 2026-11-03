#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <ydb/core/base/path.h>

namespace NKikimr::NMsgBusProxy::NPqMetaCacheV2 {

// Topics whose primary path was not found are described again as DbRoot/<account>/<modern name>.
// An entry that is skipped must not shift the index: the next result stays paired with its own topic.
template <typename TTopics, typename TIsUnknown>
TVector<ui64> SelectSecondTryTopics(
        const TString& dbRoot,
        TTopics& topics,
        size_t entryCount,
        const TIsUnknown& isUnknown)
{
    TVector<ui64> secondTry;
    if (dbRoot.empty() || entryCount != topics.size()) {
        return secondTry;
    }
    for (size_t index = 0; index < entryCount; ++index) {
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
