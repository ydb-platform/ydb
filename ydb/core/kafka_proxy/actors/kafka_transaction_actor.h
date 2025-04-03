#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKafka {
    /* 
    This class is responsible for one kafka transaction.

    It accumulates transaction state (partitions in tx, offsets) and on commit submits transaction to KQP
    */
    class TKafkaTransactionActor : public NActors::TActorBootstrapped<TKafkaTransactionActor> {

        using TBase = NActors::TActorBootstrapped<TKafkaTransactionActor>;
        
        public:
            void Bootstrap(const NActors::TActorContext&) {
                TBase::Become(&TKafkaTransactionActor::StateWork);
            }

            TStringBuilder LogPrefix() const {
                return TStringBuilder() << "KafkaTransactionActor";
            }
        
        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    // will be eimplemented in a future PR
                    // ToDo: add poison pill handler
                }
            }
    };
} // namespace NKafka