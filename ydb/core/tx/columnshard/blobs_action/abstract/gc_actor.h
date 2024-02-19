#pragma once
#include "blob_set.h"
#include "common.h"

#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/blobs_action/events/delete_blobs.h>

namespace NKikimr::NOlap::NBlobOperations {

template <class TDerived>
class TSharedBlobsCollectionActor: public TActorBootstrapped<TDerived> {
private:
    using TBase = TActorBootstrapped<TDerived>;
    const TString OperatorId;
    TBlobsByTablet BlobIdsByTablets;
    virtual void DoOnSharedRemovingFinished() = 0;
    void OnSharedRemovingFinished() {
        SharedRemovingFinished = true;
        DoOnSharedRemovingFinished();
    }
    void Handle(NEvents::TEvDeleteSharedBlobsFinished::TPtr& ev) {
        AFL_VERIFY(BlobIdsByTablets.Remove((TTabletId)ev->Cookie));
        if (BlobIdsByTablets.IsEmpty()) {
            AFL_VERIFY(!SharedRemovingFinished);
            OnSharedRemovingFinished();
        }
    }
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        auto* blobIds = BlobIdsByTablets.Find((TTabletId)ev->Cookie);
        AFL_VERIFY(blobIds);
        auto evResend = std::make_unique<NEvents::TEvDeleteSharedBlobs>(TBase::SelfId(), ev->Cookie, OperatorId, *blobIds);
        NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
            new TEvPipeCache::TEvForward(evResend.release(), ev->Cookie, true), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }
protected:
    bool SharedRemovingFinished = false;
public:
    TSharedBlobsCollectionActor(const TString& operatorId, const TBlobsByTablet& blobIds)
        : OperatorId(operatorId)
        , BlobIdsByTablets(blobIds)
    {

    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NEvents::TEvDeleteSharedBlobsFinished, Handle);
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            default:
                AFL_VERIFY(false)("problem", "unexpected event")("event_type", ev->GetTypeName());
        }
    }

    void Bootstrap(const TActorContext& /*ctx*/) {
        if (BlobIdsByTablets.IsEmpty()) {
            OnSharedRemovingFinished();
        } else {
            for (auto&& i : BlobIdsByTablets) {
                auto ev = std::make_unique<NEvents::TEvDeleteSharedBlobs>(TBase::SelfId(), (ui64)i.first, OperatorId, i.second);
                NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
                    new TEvPipeCache::TEvForward(ev.release(), (ui64)i.first, true), IEventHandle::FlagTrackDelivery, (ui64)i.first);
            }
        }
    }

};

}
