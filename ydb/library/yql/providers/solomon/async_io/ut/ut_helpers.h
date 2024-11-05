#pragma once

#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/solomon/async_io/dq_solomon_write_actor.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <queue>

namespace NYql::NDq {

void InitAsyncOutput(
    TFakeCASetup& caSetup,
    NSo::NProto::TDqSolomonShard&& settings,
    i64 freeSpace = 100000);

void CleanupSolomon(TString cloudId, TString folderId, TString service, bool isCloud);

TString GetSolomonMetrics(TString folderId, TString service);

NSo::NProto::TDqSolomonShard BuildSolomonShardSettings(bool isCloud);

NUdf::TUnboxedValue CreateStruct(
    NKikimr::NMiniKQL::THolderFactory& holderFactory,
    std::initializer_list<NUdf::TUnboxedValuePod> fields);

int GetMetricsCount(TString metrics);

} // namespace NYql::NDq
