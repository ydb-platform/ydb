#pragma once
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NPQ {

ui32 GetMaxHeaderSize(const NActors::TActorContext& ctx);

NKikimrPQ::TBatchHeader ExtractHeader(const char* buffer, ui32 size);

}// NPQ
}// NKikimr
