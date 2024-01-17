#include "channel_storage.h"

#include "channel_storage_actor.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/generic/set.h>


namespace NYql::NDq {

using namespace NActors;

namespace {

class TDqChannelStorage : public IDqChannelStorage {
public:
    TDqChannelStorage(TTxId txId, ui64 channelId, TWakeUpCallback&& wakeUp, TActorSystem* actorSystem, bool isConcurrent)
    : ActorSystem_(actorSystem)
    {
        if (isConcurrent) {
            SelfActor_ = CreateConcurrentDqChannelStorageActor(txId, channelId, std::move(wakeUp), actorSystem);
        } else {
            SelfActor_ = CreateDqChannelStorageActor(txId, channelId, std::move(wakeUp), actorSystem);
        }
        SelfActorId_ = TlsActivationContext->AsActorContext().RegisterWithSameMailbox(SelfActor_->GetActor());
        SelfId_ = TlsActivationContext->AsActorContext().SelfID;
    }

    ~TDqChannelStorage() {
        if (ActorSystem_) {
            ActorSystem_->Send(
            new IEventHandle(
                SelfActorId_,
                SelfId_,
                new TEvents::TEvPoison,
                /*flags=*/0,
                /*cookie=*/0));
        } else {
            TlsActivationContext->AsActorContext().Send(SelfActorId_, new TEvents::TEvPoison);
        }
    }

    bool IsEmpty() const override {
        return SelfActor_->IsEmpty();
    }

    bool IsFull() const override {
        return SelfActor_->IsFull();
    }

    void Put(ui64 blobId, TRope&& blob, ui64 cookie = 0) override {
        SelfActor_->Put(blobId, std::move(blob), cookie);
    }

    bool Get(ui64 blobId, TBuffer& blob, ui64 cookie = 0) override {
        return SelfActor_->Get(blobId, blob, cookie);
    }

private:
    IDqChannelStorageActor* SelfActor_;
    TActorId SelfActorId_;
    TActorId SelfId_;
    TActorSystem *ActorSystem_;
};

} // anonymous namespace

IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback wakeUp, TActorSystem* actorSystem, bool isConcurrent)
{
    return new TDqChannelStorage(txId, channelId, std::move(wakeUp), actorSystem, isConcurrent);
}

} // namespace NYql::NDq
