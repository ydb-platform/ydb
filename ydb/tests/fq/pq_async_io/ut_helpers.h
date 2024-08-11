#pragma once

#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_rd_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <queue>

namespace NYql::NDq {

NYql::NPq::NProto::TDqPqTopicSource BuildPqTopicSourceSettings(
    TString topic,
    TMaybe<TDuration> watermarksPeriod = Nothing(),
    TDuration lateArrivalDelay = TDuration::Seconds(2),
    bool idlePartitionsEnabled = false);

NYql::NPq::NProto::TDqPqTopicSink BuildPqTopicSinkSettings(TString topic);

TString GetDefaultPqEndpoint();
TString GetDefaultPqDatabase();

struct TPqIoTestFixture : public NUnitTest::TBaseFixture {
    std::unique_ptr<TFakeCASetup> CaSetup = std::make_unique<TFakeCASetup>();
    NYdb::TDriver Driver = NYdb::TDriver(NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr")));
    NActors::TActorId LocalRowDispatcherId;
    NActors::TActorId Coordinator1Id;
    NActors::TActorId Coordinator2Id;
    NActors::TActorId RemoteRowDispatcher;

    TPqIoTestFixture();
    ~TPqIoTestFixture();

    void InitSource(
        NYql::NPq::NProto::TDqPqTopicSource&& settings,
        i64 freeSpace = 1_MB);

    void InitSource(
        const TString& topic,
        i64 freeSpace = 1_MB)
    {
        InitSource(BuildPqTopicSourceSettings(topic), freeSpace);
    }

    void InitRdSource(
        NYql::NPq::NProto::TDqPqTopicSource&& settings,
        i64 freeSpace = 1_MB);

    template<typename T>
    std::vector<std::variant<T, TInstant>> SourceRead(const TReadValueParser<T> parser, i64 freeSpace = 12345) {
        NThreading::TFuture<void> nextDataFuture;
        return CaSetup->AsyncInputRead(parser, nextDataFuture, freeSpace);
    }

    template<typename T>
    std::vector<std::variant<T, TInstant>> SourceReadUntil(
        const TReadValueParser<T> parser,
        ui64 size,
        i64 eachReadFreeSpace = 1000,
        TDuration timeout = TDuration::Seconds(30))
    {
        return CaSetup->AsyncInputReadUntil(parser, size, eachReadFreeSpace, timeout, false);
    }

    template<typename T>
    std::vector<std::variant<T, TInstant>> SourceReadDataUntil(
        const TReadValueParser<T> parser,
        ui64 size,
        i64 eachReadFreeSpace = 1000)
    {
        return CaSetup->AsyncInputReadUntil(parser, size, eachReadFreeSpace, TDuration::Seconds(30), true);
    }


    void SaveSourceState(NDqProto::TCheckpoint checkpoint, TSourceState& state) {
        CaSetup->SaveSourceState(checkpoint, state);
    }

    void LoadSource(const TSourceState& state) {
        return CaSetup->LoadSource(state);
    }


    void InitAsyncOutput(
        NYql::NPq::NProto::TDqPqTopicSink&& settings,
        i64 freeSpace = 1_MB);

    void InitAsyncOutput(
        const TString& topic,
        i64 freeSpace = 1_MB)
    {
        InitAsyncOutput(BuildPqTopicSinkSettings(topic), freeSpace);
    }

    void LoadSink(const TSinkState& state) {
        CaSetup->LoadSink(state);
    }

    void AsyncOutputWrite(std::vector<TString> data, TMaybe<NDqProto::TCheckpoint> checkpoint = Nothing());
};

extern const TString DefaultPqConsumer;
extern const TString DefaultPqCluster;

// Write using YDB driver
void PQWrite(
    const std::vector<TString>& sequence,
    const TString& topic,
    const TString& endpoint = GetDefaultPqEndpoint());

// Read using YDB driver
std::vector<TString> PQReadUntil(
    const TString& topic,
    ui64 size,
    const TString& endpoint = GetDefaultPqEndpoint(),
    TDuration timeout = TDuration::MilliSeconds(10000));

void PQCreateStream(
    const TString& streamName);

void AddReadRule(
    NYdb::TDriver& driver,
    const TString& streamName);

std::vector<TString> UVParser(const NUdf::TUnboxedValue& item);

}
