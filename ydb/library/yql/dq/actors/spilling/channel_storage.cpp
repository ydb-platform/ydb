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
    TDqChannelStorage(TTxId txId, ui64 channelId, TWakeUpCallback&& wakeUp, TActorSystem* actorSystem) {
        SelfActor_ = CreateDqChannelStorageActor(txId, channelId, std::move(wakeUp), actorSystem);
        TlsActivationContext->AsActorContext().RegisterWithSameMailbox(SelfActor_->GetActor());
    }

    ~TDqChannelStorage() {
        SelfActor_->Terminate();
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
};

} // anonymous namespace

IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback wakeUp, TActorSystem* actorSystem)
{
    return new TDqChannelStorage(txId, channelId, std::move(wakeUp), actorSystem);
}

} // namespace NYql::NDq
