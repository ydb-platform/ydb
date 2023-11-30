#include "cli.h"

#include <ydb/core/driver_lib/cli_base/cli_grpc.h>

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>

#include <library/cpp/protobuf/json/proto2json.h>


namespace NKikimr::NDriverClient {

struct TCmdPersQueueDiscoverWriteSessionClustersConfig final : public TCliCmdConfig {
    void Parse(int argc, char** argv) {
        using namespace NLastGetopt;

        TOpts opts = TOpts::Default();

        opts.AddLongOption("topic", "topic").Required().StoreResult(&Topic);
        opts.AddLongOption("source-id", "source id").Required().StoreResult(&SourceId);
        opts.AddLongOption("partition-group", "partition group").Optional().StoreResult(&PartitionGroup);
        opts.AddLongOption("preferred-cluster", "prioritized cluster's name").Optional().StoreResult(&PreferredCluster);

        opts.AddLongOption("verbose", "output detailed information for debugging").Optional().NoArgument().SetFlag(&IsVerbose);

        ConfigureBaseLastGetopt(opts);

        TOptsParseResult res(&opts, argc, argv);
        ConfigureMsgBusLastGetopt(res, argc, argv);
    }

    TString Topic;
    TString SourceId;
    ui32 PartitionGroup = 0;
    TString PreferredCluster;

    bool IsVerbose = false;
};

struct TCmdPersQueueDiscoverReadSessionClustersConfig final : public TCliCmdConfig {
    void Parse(int argc, char** argv) {
        using namespace NLastGetopt;

        TOpts opts = TOpts::Default();

        opts.AddLongOption("topic", "topic").Required().StoreResult(&Topic);
        opts.AddLongOption("mirror-to-cluster", "do not set the option to enable all-original mode").Optional().StoreResult(&MirrorToCluster);

        opts.AddLongOption("verbose", "output detailed information for debugging").Optional().NoArgument().SetFlag(&IsVerbose);

        ConfigureBaseLastGetopt(opts);

        TOptsParseResult res(&opts, argc, argv);
        ConfigureMsgBusLastGetopt(res, argc, argv);
    }

    TString Topic;
    TString MirrorToCluster;

    bool IsVerbose = false;
};


static void PrintGRpcConfigAndRequest(const NYdbGrpc::TGRpcClientConfig& grpcConfig, const Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest& request) {
    Cerr << "Request endpoint: " << grpcConfig.Locator << " "
         << "timeout: " << grpcConfig.Timeout << " "
         << "max message size: " << grpcConfig.MaxMessageSize << Endl;

    Cerr << "Request message: " << NProtobufJson::Proto2Json(request, {}) << Endl;
}

template <class TConfig>
static int MakeRequest(const TConfig& config, const Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest& request) {
    if (config.ClientConfig.Defined()) {
        if (std::holds_alternative<NYdbGrpc::TGRpcClientConfig>(*config.ClientConfig)) {
            const auto& grpcConfig = std::get<NYdbGrpc::TGRpcClientConfig>(*config.ClientConfig);

            if (config.IsVerbose) {
                PrintGRpcConfigAndRequest(grpcConfig, request);
            }

            Ydb::Operations::Operation response;
            const int res = DoGRpcRequest<Ydb::PersQueue::V1::ClusterDiscoveryService,
                Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest,
                Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResponse,
                decltype(&Ydb::PersQueue::V1::ClusterDiscoveryService::Stub::AsyncDiscoverClusters)>(
                    grpcConfig,
                    request,
                    response,
                    &Ydb::PersQueue::V1::ClusterDiscoveryService::Stub::AsyncDiscoverClusters, "");

            if (res == 0) {
                Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult discoveryResult;
                Y_ABORT_UNLESS(response.result().UnpackTo(&discoveryResult));

                Y_ABORT_UNLESS(response.ready()); // there's nothing to wait for

                Cerr << "Code: " << ToString(response.status()) << ". Result message: " << NProtobufJson::Proto2Json(discoveryResult, {}) << Endl;
                return 0;
            }
        }

    }

    return 1;
}

int PersQueueDiscoverClustersRequest(TCommandConfig&, int argc, char** argv) {
    Y_ABORT_UNLESS(argc > 2);

    if (argv[1] == TStringBuf("write-session")) {
        TCmdPersQueueDiscoverWriteSessionClustersConfig config;
        config.Parse(argc - 1, argv + 1);

        Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest request;
        auto* writeSessionParams = request.add_write_sessions();

        writeSessionParams->set_topic(config.Topic);
        writeSessionParams->set_source_id(config.SourceId);
        writeSessionParams->set_partition_group(config.PartitionGroup);

        if (config.PreferredCluster) {
            writeSessionParams->set_preferred_cluster_name(config.PreferredCluster);
        }

        request.set_minimal_version(0);

        return MakeRequest(config, request);
    } else if (argv[1] == TStringBuf("read-session")) {
        TCmdPersQueueDiscoverReadSessionClustersConfig config;
        config.Parse(argc - 1, argv + 1);

        Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest request;
        auto* readSessionParams = request.add_read_sessions();

        readSessionParams->set_topic(config.Topic);

        if (config.MirrorToCluster) {
            readSessionParams->set_mirror_to_cluster(config.MirrorToCluster);
        } else {
            readSessionParams->mutable_all_original(); // set the field
        }

        request.set_minimal_version(0);

        return MakeRequest(config, request);
    }

    Cerr << "Bad commands params" << Endl;
    return 1;
}

} // namespace NKikimr::NDriverClient

