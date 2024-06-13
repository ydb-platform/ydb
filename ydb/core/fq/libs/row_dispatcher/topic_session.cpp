#include "row_dispatcher.h"
#include "coordinator.h"
#include "leader_detector.h"

#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/string/strip.h>

namespace NFq {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTopicSession : public TActorBootstrapped<TTopicSession> {

    const NYql::NPq::NProto::TDqPqTopicSource SourceParams;
    ui32 PartitionId;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;

    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    const i64 BufferSize;
    TInstant StartingMessageTimestamp;


public:
    explicit TTopicSession(
        const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

    void Bootstrap();
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const;
    NYdb::NTopic::TTopicClient& GetTopicClient();
    NYdb::NTopic::TReadSessionSettings GetReadSessionSettings() const;
    NYdb::NTopic::IReadSession& GetReadSession();

    static constexpr char ActorName[] = "YQ_ROW_DISPATCHER";

private:

};

TTopicSession::TTopicSession(
    const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory)
    : SourceParams(sourceParams)
    , PartitionId(partitionId)
    , Driver(driver)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , BufferSize(16_MB)
    , StartingMessageTimestamp(TInstant::MilliSeconds(TInstant::Now().MilliSeconds())) // this field is serialized as milliseconds, so drop microseconds part to be consistent with storage
{
}

void TTopicSession::Bootstrap() {
    //Become(&TRowDispatcher::StateFunc);
    LOG_YQ_ROW_DISPATCHER_DEBUG("TTopicSession, id " << SelfId());
    GetReadSession();

}

 NYdb::NTopic::TTopicClientSettings TTopicSession::GetTopicClientSettings() const {
    NYdb::NTopic::TTopicClientSettings opts;
    opts.Database(SourceParams.GetDatabase())
        .DiscoveryEndpoint(SourceParams.GetEndpoint())
        .SslCredentials(NYdb::TSslCredentials(SourceParams.GetUseSsl()))
        .CredentialsProviderFactory(CredentialsProviderFactory);

    return opts;
}

NYdb::NTopic::TTopicClient& TTopicSession::GetTopicClient() {
    if (!TopicClient) {
        TopicClient = std::make_unique<NYdb::NTopic::TTopicClient>(Driver, GetTopicClientSettings());
    }
    return *TopicClient;
}

NYdb::NTopic::TReadSessionSettings TTopicSession::GetReadSessionSettings() const {
    NYdb::NTopic::TTopicReadSettings topicReadSettings;
    topicReadSettings.Path(SourceParams.GetTopicPath());
    topicReadSettings.AppendPartitionIds(PartitionId);

    return NYdb::NTopic::TReadSessionSettings()
        .AppendTopics(topicReadSettings)
        .ConsumerName(SourceParams.GetConsumerName())
        .MaxMemoryUsageBytes(BufferSize)
        .ReadFromTimestamp(StartingMessageTimestamp);
}

NYdb::NTopic::IReadSession& TTopicSession::GetReadSession() {
    if (!ReadSession) {
        ReadSession = GetTopicClient().CreateReadSession(GetReadSessionSettings());
        LOG_YQ_ROW_DISPATCHER_DEBUG("CreateReadSession");
    }
    return *ReadSession;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////


    
std::unique_ptr<NActors::IActor> NewTopicSession(
    const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    return std::unique_ptr<NActors::IActor>(new TTopicSession(sourceParams, partitionId, driver, credentialsProviderFactory));
}

} // namespace NFq
