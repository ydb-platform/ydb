#pragma once

#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sinks.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <queue>

namespace NYql::NDq {

NYql::NPq::NProto::TDqPqTopicSource BuildPqTopicSourceSettings(TString topic);

NYql::NPq::NProto::TDqPqTopicSink BuildPqTopicSinkSettings(TString topic);

struct TPqIoTestFixture : public NUnitTest::TBaseFixture { 
    std::unique_ptr<TFakeCASetup> CaSetup = std::make_unique<TFakeCASetup>(); 
    NYdb::TDriver Driver = NYdb::TDriver(NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr"))); 

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
 
    template<typename T> 
    std::vector<T> SourceRead(const TReadValueParser<T> parser, i64 freeSpace = 12345) { 
        return CaSetup->SourceRead(parser, freeSpace); 
    } 
 
    template<typename T> 
    std::vector<T> SourceReadUntil( 
        const TReadValueParser<T> parser, 
        ui64 size, 
        i64 eachReadFreeSpace = 1000, 
        TDuration timeout = TDuration::Seconds(10)) 
    { 
        return CaSetup->SourceReadUntil(parser, size, eachReadFreeSpace, timeout); 
    } 
 
    void SaveSourceState(NDqProto::TCheckpoint checkpoint, NDqProto::TSourceState& state) { 
        CaSetup->SaveSourceState(checkpoint, state); 
    } 
 
    void LoadSource(const NDqProto::TSourceState& state) { 
        return CaSetup->LoadSource(state); 
    } 
 
 
    void InitSink( 
        NYql::NPq::NProto::TDqPqTopicSink&& settings, 
        i64 freeSpace = 1_MB); 
 
    void InitSink( 
        const TString& topic, 
        i64 freeSpace = 1_MB) 
    { 
        InitSink(BuildPqTopicSinkSettings(topic), freeSpace); 
    } 
 
    void LoadSink(const NDqProto::TSinkState& state) { 
        CaSetup->LoadSink(state); 
    } 
 
    void SinkWrite(std::vector<TString> data, TMaybe<NDqProto::TCheckpoint> checkpoint = Nothing()); 
}; 
 
TString GetDefaultPqEndpoint();

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

std::vector<TString> UVParser(const NUdf::TUnboxedValue& item);

}
