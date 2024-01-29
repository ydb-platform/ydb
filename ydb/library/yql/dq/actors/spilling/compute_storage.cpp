#include "compute_storage.h"

#include "compute_storage_actor.h"

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

class TDqComputeStorage : public NKikimr::NMiniKQL::ISpiller {
public:
    TDqComputeStorage(TTxId txId, const TString& spillerName) {

        SelfActor_ = CreateDqComputeStorageActor(txId, spillerName);
        SelfActorId_ = TlsActivationContext->AsActorContext().Register(SelfActor_->GetActor());
        SelfId_ = TlsActivationContext->AsActorContext().SelfID;
    }

    ~TDqComputeStorage() {
        TlsActivationContext->AsActorContext().Send(SelfActorId_, new TEvents::TEvPoison);
    }

    NThreading::TFuture<TKey> Put(TRope&& blob) override {
        return SelfActor_->Put(std::move(blob));
    }

    std::optional<NThreading::TFuture<TRope>> Get(TKey key) override {
        return SelfActor_->Get(key);
    }

    NThreading::TFuture<void> Delete(TKey key) override {
        return SelfActor_->Delete(key);
    }

    std::optional<NThreading::TFuture<TRope>> Extract(TKey key) override {
        return SelfActor_->Extract(key);
    }

private:
    IDqComputeStorageActor* SelfActor_;
    TActorId SelfActorId_;
    TActorId SelfId_;
};

} // anonymous namespace

NKikimr::NMiniKQL::ISpiller::TPtr MakeSpiller(const TString& spillerName) {
    return std::make_shared<TDqComputeStorage>(TTxId(), spillerName);
}

} // namespace NYql::NDq