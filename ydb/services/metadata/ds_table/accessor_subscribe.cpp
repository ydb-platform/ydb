#include "accessor_subscribe.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NMetadata::NProvider {

void TDSAccessorNotifier::OnSnapshotModified() {
    auto snapshot = GetCurrentSnapshot();
    if (!snapshot) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot construct snapshot";
        return;
    }

    for (auto&& i : Subscribed) {
        Send(i, new TEvRefreshSubscriberData(snapshot));
    }
}

void TDSAccessorNotifier::Handle(TEvSubscribe::TPtr& ev) {
    Subscribed.emplace(ev->Get()->GetSubscriberId());
    if (TBase::IsReady()) {
        auto snapshot = GetCurrentSnapshot();
        if (!snapshot) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot construct snapshot";
            return;
        }
        Sender<TEvRefreshSubscriberData>(snapshot).SendTo(ev->Get()->GetSubscriberId());
    }
}

void TDSAccessorNotifier::Handle(TEvAsk::TPtr& ev) {
    Asked[Now()].emplace(ev->Get()->GetRequesterId());
    Sender<TEvRefresh>().SendTo(SelfId());
}

void TDSAccessorNotifier::Handle(TEvUnsubscribe::TPtr& ev) {
    Subscribed.erase(ev->Get()->GetSubscriberId());
}

void TDSAccessorNotifier::OnSnapshotRefresh() {
    auto snapshot = GetCurrentSnapshot();
    for (auto it = Asked.begin(); it != Asked.end(); ) {
        if (it->first <= snapshot->GetActuality()) {
            if (!snapshot) {
                ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot construct snapshot on refresh";
                return;
            }
            for (auto&& s : it->second) {
                Sender<TEvRefreshSubscriberData>(snapshot).SendTo(s);
            }
            it = Asked.erase(it);
        } else {
            ++it;
        }
    }
    if (!Asked.empty()) {
        Sender<TEvRefresh>().SendTo(SelfId());
    }
}

void TDSAccessorNotifier::OnBootstrap() {
    TBase::OnBootstrap();
    UnsafeBecome(&TDSAccessorNotifier::StateMain);
}

}
