#include <ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>
#include <ydb/library/yql/providers/dq/actors/yt/nodeid_assigner.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/getopt/last_getopt.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/string/builder.h>
#include <util/system/env.h>
#include <util/system/file.h>
#include <util/stream/file.h>
#include <util/generic/guid.h>
#include <util/string/split.h>

using namespace NYdbGrpc;
using namespace Yql::DqsProto;
using namespace NYql;

int SvnRevision(TServiceConnection<DqService>& service, const TVector<TString>& args) {
    if (args.size() != 1) {
        Cerr << "Suspicious extra args" << Endl;
    }

    auto promise = NThreading::NewPromise<int>();
    auto callback = [&](TGrpcStatus&& status, SvnRevisionResponse&& resp) {
        if (status.Ok()) {
            Cout << resp.GetRevision() << Endl;
            promise.SetValue(0);
        } else {
            Cerr << "Error " << status.GRpcStatusCode << " message: " << status.Msg << Endl;
            promise.SetValue(status.GRpcStatusCode);
        }
    };

    service.DoRequest<SvnRevisionRequest, SvnRevisionResponse>(SvnRevisionRequest(), callback,
                                                               &DqService::Stub::AsyncSvnRevision);
    return promise.GetFuture().GetValueSync();
}

ClusterStatusResponse Info(TServiceConnection<DqService>& service) {
    auto promise = NThreading::NewPromise<ClusterStatusResponse>();
    auto callback = [&](TGrpcStatus&& status, ClusterStatusResponse&& resp) {
        if (status.Ok()) {
            promise.SetValue(resp);
        } else {
            Cerr << "Error " << status.GRpcStatusCode << " message: " << status.Msg << Endl;
            promise.SetException("Error");
        }
    };

    service.DoRequest<ClusterStatusRequest, ClusterStatusResponse>(
        ClusterStatusRequest(),
        callback,
        &DqService::Stub::AsyncClusterStatus);
    return promise.GetFuture().GetValueSync();
}

void Stop(TServiceConnection<DqService>& service, const JobStopRequest& request)
{
    auto promise = NThreading::NewPromise<void>();
    auto callback = [&](TGrpcStatus&& status, JobStopResponse&& ) {
        if (status.Ok()) {
            promise.SetValue();
        } else {
            Cerr << "Error " << status.GRpcStatusCode << " message: " << status.Msg << Endl;
            promise.SetValue();
        }
    };

    service.DoRequest<JobStopRequest, JobStopResponse>(
        request,
        callback,
        &DqService::Stub::AsyncJobStop);
    promise.GetFuture().GetValueSync();
    return;
}

void ClusterUpgrade(
    TServiceConnection<DqService>& service,
    const TString& revision,
    const TString& cluster,
    bool force)
{
    Y_UNUSED(force);

    auto status = Info(service);
    int totalWorkers = 0;
    int started = 0;
    for (const auto& worker : status.GetWorker()) {
        if (worker.GetClusterName() == cluster) {
            if (worker.GetRevision() == revision) {
                started ++;
            } else {
                totalWorkers ++;
            }
        }
    }

    Cerr << "Total workers of new revision " << revision << " : " << started << Endl;
    Cerr << "Stopping " << totalWorkers << " workers" << Endl;

    while (true) {
        int started = 0;
        int stopping = 0;
        int pending = 0;
        int ready = 0;
        for (const auto& worker : status.GetWorker()) {
            if (worker.GetDead()) {
                continue;
            }
            if (worker.GetClusterName() == cluster)
            {
                if (worker.GetRevision() == revision) {
                    started ++ ;
                    if (worker.GetDownloadList().size() == 0) {
                        ready ++;
                    }
                } else {
                    pending ++;
                }
                if (worker.GetStopping()) {
                    stopping ++;
                }
            }
        }

        Cerr << "Stopping/Pending/Started/Ready/Total "
             << stopping << "/"
             << pending << "/"
             << started << "/"
             << ready << "/"
             << totalWorkers << Endl;

        if (pending == 0 && stopping== 0 && ready >= 0.8 * totalWorkers) {
            break;
        }

        JobStopRequest request;
        request.SetRevision(revision);
        request.SetNegativeRevision(true);
        request.SetForce(force);
        request.SetClusterName(cluster);

        Stop(service, request);

        Sleep(TDuration::Seconds(10));

        status = Info(service);
    }
    Cerr << "Done" << Endl;
}

void PerClusterUpgrade(TServiceConnection<DqService>& service, const TVector<TString>& args) {
    TString revision;
    bool force = false;

    if (args.size() < 3) {
        Cerr << "Usage: percluster_upgrade revision hahn,arnold [force]" << Endl;
        return;
    }

    revision = args[1];

    if (args.size() > 3) {
        force = args[3] == "force";
    }

    for (const auto& it : StringSplitter(args[2]).Split(',')) {
        Cerr << "Stop " << it.Token() << Endl;

        ClusterUpgrade(service, revision, TString(it.Token()), force);
    }
}

void Stop(TServiceConnection<DqService>& service, const TVector<TString>& args, bool force) {
    if (args.size() != 2) {
        Cerr << "Suspicious extra args" << Endl;
    }

    JobStopRequest request;
    TStringInput inputStream1(args[1]);
    ParseFromTextFormat(inputStream1, request, EParseFromTextFormatOption::AllowUnknownField);
    request.SetForce(force);
    Stop(service, request);
}

void OperationStop(TServiceConnection<DqService>& service, const TVector<TString>& args)
{
    if (args.size() != 2) {
        Cerr << "Suspicious extra args" << Endl;
    }
    TString operationId = args[1];

    auto promise = NThreading::NewPromise<void>();
    auto callback = [&](TGrpcStatus&& status, OperationStopResponse&& ) {
        if (status.Ok()) {
            promise.SetValue();
        } else {
            Cerr << "Error " << status.GRpcStatusCode << " message: " << status.Msg << Endl;
            promise.SetValue();
        }
    };

    OperationStopRequest request;
    request.SetOperationId(operationId);

    service.DoRequest<OperationStopRequest, OperationStopResponse>(
        request,
        callback,
        &DqService::Stub::AsyncOperationStop);
    promise.GetFuture().GetValueSync();
    return;
}

void SmartStop(TServiceConnection<DqService>& service, const TVector<TString>& args, bool negative = false) {
    if (args.size() != 2) {
        Cerr << "Suspicious extra args" << Endl; return;
    }
    if (args.size() < 2) {
        Cerr << "Revision requied" << Endl; return;
    }

    TString revision = args[1];
    auto status = Info(service);
    auto totalWorkers = status.GetWorker().size();
    int maxOperations = -1;

    while (true) {
        THashSet<TString> operations;
        status = Info(service);
        for (const auto& worker : status.GetWorker()) {
            if (negative == (worker.GetRevision() == revision)) {
                continue;
            }
            if (worker.GetDead()) {
                continue;
            }
            // TString OPERATIONID_ATTR = "yql_operation_id"; // TODO
            for (const auto& attr : worker.GetAttribute()) {
                if (attr.GetKey() == NYql::NCommonAttrs::OPERATIONID_ATTR) {
                    operations.emplace(attr.GetValue());
                }
            }
        }
        if (operations.empty()) {
            Cerr << "Done" << Endl;
            break;
        }
        maxOperations = Max<int>(maxOperations, operations.size());
        if (totalWorkers * 0.9 <= status.GetWorker().size()) {
            auto operationId = *operations.begin();
            Cerr << "Stopping operation " << operationId << Endl;
            JobStopRequest request;
            request.SetRevision(revision);
            request.SetNegativeRevision(negative);
            auto* attr = request.AddAttribute();
            attr->SetKey(NYql::NCommonAttrs::OPERATIONID_ATTR);
            attr->SetValue(operationId);
            Stop(service, request);
        }
        Cerr << "Operations: " << (maxOperations - operations.size()) << "/" << maxOperations << Endl;
        Sleep(TDuration::Seconds(10));
    }

    JobStopRequest request;
    request.SetRevision(revision);
    request.SetNegativeRevision(negative);

    auto promise = NThreading::NewPromise<void>();
    auto callback = [&](TGrpcStatus&& status, JobStopResponse&& ) {
        if (status.Ok()) {
            promise.SetValue();
        } else {
            Cerr << "Error " << status.GRpcStatusCode << " message: " << status.Msg << Endl;
            promise.SetValue();
        }
    };

    service.DoRequest<JobStopRequest, JobStopResponse>(
        request,
        callback,
        &DqService::Stub::AsyncJobStop);
    promise.GetFuture().GetValueSync();
    return;
}

void OpenSession(TServiceConnection<DqService>& service, const TString& sessionId, const TString& username) {
    Yql::DqsProto::OpenSessionRequest request;
    request.SetSession(sessionId);
    request.SetUsername(username);

    auto promise = NThreading::NewPromise<void>();
    auto callback = [promise](NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::OpenSessionResponse&& resp) mutable {
        Y_UNUSED(resp);
        if (status.Ok()) {
            promise.SetValue();
        } else {
            promise.SetException(status.Msg);
        }
    };

    service.DoRequest<Yql::DqsProto::OpenSessionRequest, Yql::DqsProto::OpenSessionResponse>(
        request, callback, &Yql::DqsProto::DqService::Stub::AsyncOpenSession);
    promise.GetFuture().GetValueSync();
}

void CloseSession(TServiceConnection<DqService>& service, const TString& sessionId) {
    Yql::DqsProto::CloseSessionRequest request;
    request.SetSession(sessionId);

    auto callback = [](NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::CloseSessionResponse&& resp) {
        Y_UNUSED(resp);
        Y_UNUSED(status);
    };

    service.DoRequest<Yql::DqsProto::CloseSessionRequest, Yql::DqsProto::CloseSessionResponse>(
        request, callback, &Yql::DqsProto::DqService::Stub::AsyncCloseSession);
}

Yql::DqsProto::RoutesResponse Routes(TServiceConnection<DqService>& service, const TString& nodeIdStr) {
    Yql::DqsProto::RoutesRequest request;
    int nodeId = 0;
    TryFromString(nodeIdStr, nodeId);
    request.SetNodeId(nodeId);

    auto promise = NThreading::NewPromise<Yql::DqsProto::RoutesResponse>();
    auto callback = [promise](NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::RoutesResponse&& resp) mutable {
        Y_UNUSED(resp);
        if (status.Ok()) {
            promise.SetValue(resp);
        } else {
            promise.SetException(status.Msg);
        }
    };

    service.DoRequest<Yql::DqsProto::RoutesRequest, Yql::DqsProto::RoutesResponse>(
        request, callback, &Yql::DqsProto::DqService::Stub::AsyncRoutes);
    return promise.GetFuture().GetValueSync();
}

Yql::DqsProto::BenchmarkResponse Bench(TServiceConnection<DqService>& service, const TVector<TString>& args) {
    Yql::DqsProto::BenchmarkRequest request;
    request.SetWorkerCount(10);
    request.SetInflight(10);
    request.SetTotalRequests(10000);

    if (args.size() > 1) {
        TStringInput inputStream1(args[1]);
        ParseFromTextFormat(inputStream1, request, EParseFromTextFormatOption::AllowUnknownField);
    }

    auto promise = NThreading::NewPromise<Yql::DqsProto::BenchmarkResponse>();

    auto callback = [promise](NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::BenchmarkResponse&& resp) mutable
    {
        Y_UNUSED(resp);
        if (status.Ok()) {
            promise.SetValue(resp);
        } else {
            promise.SetException(status.Msg);
        }
    };

    service.DoRequest<Yql::DqsProto::BenchmarkRequest, Yql::DqsProto::BenchmarkResponse>(
        request, callback, &Yql::DqsProto::DqService::Stub::AsyncBenchmark);
    return promise.GetFuture().GetValueSync();
}

int main(int argc, char** argv) {
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();
    opts.AddHelpOption();
    opts.AddLongOption('p', "port", "Grpc port");
    opts.AddLongOption('h', "host", "Grpc host");
    opts.SetFreeArgTitle(0, "command", R"(query or svnrevision)");
    opts.SetFreeArgTitle(1, "<query>", R"(SQL code goes here)");
    opts.SetFreeArgTitle(2, "endpoint", "endpoint of the remote database if the query is remote");
    opts.SetFreeArgTitle(3, "database", "path to the remote database on the endpoint");
    opts.SetFreeArgsMin(1);

    TOptsParseResult res(&opts, argc, argv);

    ui16 port = [&]() {
        if (res.Has("port")) {
            return res.Get<ui16>("port");
        } else {
            ::TFile file("/var/tmp/dq_grpc_port", OpenExisting | RdOnly);
            TString buffer;
            buffer.resize(file.GetLength());
            file.Load(&buffer[0], buffer.size());
            return FromString<ui16>(buffer);
        }
    } ();
    TString host = [&]() {
        if (res.Has("host")) {
            return res.Get<TString>("host");
        } else {
            return TString("localhost");
        }
    } ();
    auto args = res.GetFreeArgs();
    const auto& command = args[0];

    TGRpcClientConfig grpcConf(TStringBuilder() << host << ":" << port);
    TGRpcClientLow grpcClient(2);
    auto conn = grpcClient.CreateGRpcServiceConnection<DqService>(grpcConf);

    if (command == "svnrevision") {
        return SvnRevision(*conn, args);
    }

    if (command == "info") {
        auto status = Info(*conn);
        TString responseStr;
        {
            TStringOutput output1(responseStr);
            SerializeToTextFormat(status, output1);
        }

        Cout << responseStr << Endl;

        return 0;
    }

    if (command == "operation_stop") {
        OperationStop(*conn, args);
        return 0;
    }

    if (command == "stop") {
        Stop(*conn, args, false);
        return 0;
    }

    if (command == "force_stop") {
        Stop(*conn, args, true);
        return 0;
    }

    if (command == "smart_stop") {
        SmartStop(*conn, args, false);
        return 0;
    }

    if (command == "upgrade") {
        SmartStop(*conn, args, true);
        return 0;
    }

    if (command == "percluster_upgrade") {
        PerClusterUpgrade(*conn, args);
        return 0;
    }

    if (command == "open_session") {
        OpenSession(*conn, args[1], args[2]);
        return 0;
    }

    if (command == "close_session") {
        CloseSession(*conn, args[1]);
        return 0;
    }

    if (command == "routes") {
        auto status = Routes(*conn, args[1]);
        TString responseStr;
        {
            TStringOutput output1(responseStr);
            SerializeToTextFormat(status, output1);
        }

        Cout << responseStr << Endl;
        return 0;
    }

    if (command == "bench") {
        auto status = Bench(*conn, args);
        TString responseStr;
        {
            TStringOutput output1(responseStr);
            SerializeToTextFormat(status, output1);
        }

        Cout << responseStr << Endl;
        return 0;
    }

    Cerr << "Unexpected command. Try --help." << Endl;
    return 1;
}
