#pragma once

#include <ydb/core/base/statestorage.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/string.h>

namespace NKikimr {

namespace NSchemeBoard {

    struct TProxyInfo {
        TActorId Proxy;
        TActorId Replica;
    };

    struct TProxyGroup {
        bool WriteOnly;
        ERingGroupState State;
        TVector<TProxyInfo> Proxies;
    };

    bool ShouldIgnore(const TProxyGroup& proxyGroup);

}

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TString& path
);

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TString& path,
    const ui64 domainOwnerId
);

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TPathId& pathId,
    const ui64 domainOwnerId
);

// deprecated
enum class ESchemeBoardSubscriberDeletionPolicy {
    First,
    Second,
    Majority,
};

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TString& path,
    const ESchemeBoardSubscriberDeletionPolicy deletionPolicy
);

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TPathId& pathId,
    const ESchemeBoardSubscriberDeletionPolicy deletionPolicy
);

} // NKikimr
