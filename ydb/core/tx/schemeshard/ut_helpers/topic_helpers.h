#pragma once

#include <ydb/core/testlib/test_client.h>

namespace NSchemeShardUT_Private {

ui64 DoRequest(NActors::TTestActorRuntime& runtime, ui64& txId, NKikimrSchemeOp::TPersQueueGroupDescription& scheme);

ui64 SplitPartition(NActors::TTestActorRuntime& runtime, ui64& txId, const TString& topic, const ui32 partition, TString boundary);
ui64 MergePartition(NActors::TTestActorRuntime& runtime, ui64& txId, const TString& topic, const ui32 partitionLeft, const ui32 partitionRight);

}
