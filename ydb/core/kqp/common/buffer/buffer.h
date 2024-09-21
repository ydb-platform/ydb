#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr {
namespace NKqp {

struct TKqpBufferWriterSettings {
    TActorId SessionActorId;
};

NActors::IActor* CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings);

}
}
