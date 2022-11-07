#include "accessor_subscribe.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NMetadataProvider {

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

void TDSAccessorNotifier::Handle(TEvUnsubscribe::TPtr& ev) {
    Subscribed.erase(ev->Get()->GetSubscriberId());
}

}
