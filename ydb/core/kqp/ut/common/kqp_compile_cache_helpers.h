#pragma once

#include <ydb/core/kqp/common/compile_cache/types.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NTest {

struct TFakeSnapshot final : TPreCboSnapshot {
    explicit TFakeSnapshot(ui64 bytes = 1024) : Bytes(bytes) {}
    ui64 ByteSize() const override { return Bytes; }
    const ui64 Bytes;
};

inline TKqpTempTablesState::TConstPtr SomeTempTables() {
    auto state = std::make_shared<TKqpTempTablesState>();
    state->TempTables["/Root/tmp"] = {};
    return state;
}

inline TEvKqp::TEvListQueryCacheQueriesResponse::TPtr ListQueryCache(
    NActors::TTestActorRuntimeBase& runtime, ui32 nodeIndex, const TString& tenant)
{
    const auto edge = runtime.AllocateEdgeActor(nodeIndex);
    const auto service = runtime.GetLocalServiceId(MakeKqpCompileServiceID(runtime.GetNodeId(nodeIndex)), nodeIndex);
    auto request = MakeHolder<TEvKqp::TEvListQueryCacheQueriesRequest>();
    request->Record.SetTenantName(tenant);
    request->Record.SetFreeSpace(1024 * 1024);
    runtime.Send(new NActors::IEventHandle(service, edge, request.Release()), nodeIndex);
    auto response = runtime.GrabEdgeEvent<TEvKqp::TEvListQueryCacheQueriesResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT_C(response, "No query cache response from node " << nodeIndex);
    return response;
}

} // namespace NKikimr::NKqp::NTest
