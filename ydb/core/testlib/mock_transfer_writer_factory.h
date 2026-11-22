#pragma once

#include <ydb/core/tx/replication/service/transfer_writer_factory.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::Tests {

struct MockTransferWriterFactory : public NKikimr::NReplication::NService::ITransferWriterFactory {
    struct MockActor : public NActors::TActorBootstrapped<MockActor> {
        void Bootstrap() {
            Become(&MockActor::StateWork);
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                sFunc(NActors::TEvents::TEvPoison, PassAway);
            }
        }
    };

    NActors::IActor* Create(const Parameters&) const override {
        return new MockActor();
    }

    NActors::IActor* CreateCompilationService() const override {
        return new MockActor();
    }
};

} // namespace NKikimr::Tests
