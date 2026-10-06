#include "yql_yt_message_stream_source.h"
#include <ydb/library/yql/providers/common/message_stream/async_io/read_actor.h>
#include <ydb/library/yql/providers/yt/gateway/clients/message_stream/yql_yt_client.h>
#include <ydb/library/yql/providers/yt/gateway/clients/message_stream/yql_qyt_message_stream_client.h>
#include <ydb/library/yql/providers/yt/proto/source.pb.h>
#include <util/string/cast.h>
#include <thread>

namespace NYql::NDq {
namespace {
class TYtMessageStreamReadState final : public NFq::NMessageStream::IMessageStreamReadActorState {
public:
    NFq::NMessageStream::TMessageStreamReadState& GetReadState() override { return State; }
    void SaveState(const NDqProto::TCheckpoint&, TSourceState&) override {
        ythrow yexception() << "YT message stream checkpoints are not supported";
    }
    void LoadState(const TSourceState&) override {
        ythrow yexception() << "YT message stream checkpoints are not supported";
    }
private:
    NFq::NMessageStream::TMessageStreamReadState State;
};
}

void RegisterYtMessageStreamReadActorFactory(TDqAsyncIoFactory& factory, IStructuredTokenCredentialsFactory::TPtr credentials) {
    factory.RegisterSource<NQyt::NProto::TSource>("QytSource", [credentials](NQyt::NProto::TSource&& source,
        IDqAsyncIoFactory::TSourceArguments&& args) -> std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> {
        NFq::NMessageStream::TMessageStreamReadActorSettings settings;
        settings.InputIndex = args.InputIndex;
        settings.TaskId = args.TaskId;
        settings.TxId = args.TxId;
        settings.ComputeActorId = args.ComputeActorId;
        settings.Stream = source.GetPath();
        settings.Consumer = source.GetConsumer();
        settings.StopAtCurrentEndOffsets = true;
        settings.StatsLevel = args.StatsLevel;
        settings.MetricsSource = "YtRead";
        settings.Counters = args.TaskCounters;
        settings.HolderFactory = &args.HolderFactory;
        settings.Alloc = std::move(args.Alloc);
        NFq::NMessageStream::TMessageStreamReadCluster cluster;
        for (const auto& range : args.ReadRanges) {
            cluster.Partitions.push_back(FromString<ui64>(range));
        }
        Y_ENSURE(!cluster.Partitions.empty(), "YT source has no assigned partitions");
        cluster.PartitionsCount = cluster.Partitions.size();
        const TString token = args.SecureParams.at(source.GetToken());
        auto client = std::make_shared<std::shared_ptr<NFq::IMessageStreamClient>>();
        cluster.CreateClient = [source, token, credentials, client](const NActors::TActorContext&) {
            if (!*client) {
                const auto auth = credentials->Create(token)->CreateProvider()->GetAuthInfo();
                *client = CreateQytMessageStreamClient(source.GetPath(), {
                    .Client = CreateYtClient(TString(source.GetEndpoint()), TString(auth))});
            }
            return *client;
        };
        cluster.CreateSession = [client](const NActors::TActorContext&, NFq::IMessageStreamClient&,
            const NFq::TMessageStreamReadSessionSettings& read) {
            auto promise = NThreading::NewPromise<std::shared_ptr<NFq::IMessageStreamReadSession>>();
            // Native YT session initialization performs blocking metadata RPCs.
            std::thread([client = *client, read, promise]() mutable {
                try { promise.SetValue(client->CreateReadSession(read)); }
                catch (...) { promise.SetException(std::current_exception()); }
            }).detach();
            return promise.GetFuture();
        };
        settings.Clusters.push_back(std::move(cluster));
        return NFq::NMessageStream::CreateMessageStreamReadActor(std::move(settings), std::make_unique<TYtMessageStreamReadState>());
    });
}
} // namespace NYql::NDq
