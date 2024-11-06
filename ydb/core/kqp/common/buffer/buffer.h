#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>

namespace NKikimr {
namespace NKqp {

struct TKqpBufferWriterSettings {
    TActorId SessionActorId;
    IKqpTransactionManagerPtr TxManager;
};

NActors::IActor* CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings);

}
}
