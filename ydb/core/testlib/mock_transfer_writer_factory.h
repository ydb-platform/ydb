#pragma once

#include <ydb/core/tx/replication/service/transfer_writer_factory.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::Tests {

struct MockTransferWriterFactory : public NKikimr::NReplication::NService::ITransferWriterFactory {
    struct MockActor : public NActors::TActorBootstrapped<MockActor> {
        void Bootstrap() {}
    };


    NActors::IActor* Create(const Parameters&) const override {
        return new MockActor();
    }
};

} // namespace NKikimr::Tests
