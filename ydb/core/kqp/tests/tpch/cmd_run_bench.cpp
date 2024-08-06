#include "cmd_run_bench.h"

#include <ydb/core/protos/console_config.pb.h>
#if __has_include("contrib/ydb/core/protos/console_config.deps.pb.h")
    #include <ydb/core/protos/console_config.deps.pb.h> // Y_IGNORE
#endif
#include <ydb/core/protos/grpc.pb.h>
#if __has_include("contrib/ydb/core/protos/grpc.deps.pb.h")
    #include <ydb/core/protos/grpc.deps.pb.h> // Y_IGNORE
#endif
#include <ydb/core/protos/grpc.grpc.pb.h>
#include <ydb/core/kqp/tests/tpch/lib/tpch_runner.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.cpp>

#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/printf.h>

namespace NYdb::NTpch {

namespace {

TMaybe<ui64> ConsumeStream(NTable::TScanQueryPartIterator& it, ui64& maxMemory, ui64& sumMemory, ui64& extraMemory) {
    ui64 resultHash = 0;

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            if (!streamPart.EOS()) {
                Cerr << "[ERROR] " << streamPart.GetIssues().ToString() << Endl;
                return Nothing();
            }
            return resultHash;
        }

        if (streamPart.HasResultSet()) {
            auto resultSet = streamPart.GetResultSet();

            auto parser = TResultSetParser(resultSet);
            while (parser.TryNextRow()) {
                TStringStream out;
                NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);

                writer.OnBeginList();
                for (size_t i = 0; i < resultSet.ColumnsCount(); ++i) {
                    writer.OnListItem();
                    FormatValueYson(parser.GetValue(i), writer);
                }
                writer.OnEndList();

                resultHash = CombineHashes(resultHash, THash<TString>()(out.Str()));
            }
        }

        Y_UNUSED(maxMemory, sumMemory, extraMemory);

//        if (streamPart.HasProfile()) {
//            maxMemory = sumMemory = extraMemory = 0;
//
//            NYql::NDqpProto::TDqExecutionStats stats;
//            Y_ABORT_UNLESS(google::protobuf::TextFormat::ParseFromString(streamPart.GetProfile(), &stats));
//
//            for (auto& stage : stats.GetStages()) {
//                for (auto& ca : stage.GetComputeActors()) {
//                    for (auto& task: ca.GetTasks()) {
//                        maxMemory = std::max(task.GetMkqlMaxMemoryUsage(), maxMemory);
//                        sumMemory += task.GetMkqlMaxMemoryUsage();
//                        extraMemory += task.GetExtraMemoryBytes();
//                    }
//                }
//            }
//        }
    }

    return resultHash;
}

NKikimrClient::TConsoleResponse DoGrpcRequest(const TString& endpoint, NKikimrClient::TConsoleRequest&& request) {
    NYdbGrpc::TGRpcClientLow grpcClient;
    auto grpcContext = grpcClient.CreateContext();

    NYdbGrpc::TGRpcClientConfig grpcConfig{endpoint};
    auto grpc = grpcClient.CreateGRpcServiceConnection<NKikimrClient::TGRpcServer>(grpcConfig);

    TAtomic done = 0;
    NYdbGrpc::TGrpcStatus status;
    NKikimrClient::TConsoleResponse response;
    grpc->DoRequest<NKikimrClient::TConsoleRequest, NKikimrClient::TConsoleResponse>(
        request,
        [&](NYdbGrpc::TGrpcStatus&& _status, NKikimrClient::TConsoleResponse&& _response) {
            status = std::move(_status);
            response = std::move(_response);
            AtomicSet(done, 1);
        },
        &NKikimrClient::TGRpcServer::Stub::AsyncConsoleRequest,
        {},
        grpcContext.get());

    while (AtomicGet(done) == 0) {
        ::Sleep(TDuration::Seconds(1));
    }
    grpcContext.reset();
    grpcClient.Stop(true);

    while (!AtomicGet(done)) {
        ::Sleep(TDuration::MilliSeconds(100));
    }

    if (!status.Ok()) {
        Cerr << "status: {" << status.Msg << ", " << status.InternalError << ", "
             << status.GRpcStatusCode << "}" << Endl;
        Cerr << response.Utf8DebugString() << Endl;

        exit(1);
    }

    return response;
}

TMaybe<int> UpdatePublishTimeout(const TString& endpoint) {
    NKikimrClient::TConsoleRequest request;
    request.MutableGetConfigItemsRequest()->AddItemKinds(NKikimrConsole::TConfigItem::TableServiceConfigItem);

    auto response = DoGrpcRequest(endpoint, std::move(request));
    if (response.GetGetConfigItemsResponse().GetStatus().code() != Ydb::StatusIds::SUCCESS) {
        Cerr << "error: " << response.DebugString() << Endl;
        exit(1);
    }

    // Cerr << "-- initial config: " << response.GetGetConfigItemsResponse().DebugString() << Endl;

    NKikimrConfig::TTableServiceConfig cfg;
    if (!response.GetGetConfigItemsResponse().GetConfigItems().empty()) {
        cfg = response.GetGetConfigItemsResponse().GetConfigItems()[0].GetConfig().GetTableServiceConfig();
    }

    if (cfg.GetResourceManager().GetPublishStatisticsIntervalSec() == 0) {
        // Cerr << "-- publish timeout disabled already" << Endl;
        return Nothing();
    }

    request.Clear();
    auto* action = request.MutableConfigureRequest()->MutableActions()->Add();
    auto* configItem = action->MutableAddConfigItem()->MutableConfigItem();
    configItem->SetKind(NKikimrConsole::TConfigItem::TableServiceConfigItem);
    configItem->MutableConfig()->MutableTableServiceConfig()->MutableResourceManager()->CopyFrom(cfg.GetResourceManager());
    configItem->MutableConfig()->MutableTableServiceConfig()->MutableResourceManager()->SetPublishStatisticsIntervalSec(0);

    // Cerr << "-- publish config: " << request.GetConfigureRequest().DebugString() << Endl;

    response = DoGrpcRequest(endpoint, std::move(request));
    if (response.GetConfigureResponse().GetStatus().code() != Ydb::StatusIds::SUCCESS) {
        Cerr << "error: " << response.DebugString() << Endl;
        exit(1);
    }

    // Cerr << "-- resp: " << response.DebugString() << Endl;

    return response.GetConfigureResponse().GetAddedItemIds()[0];
}

void RestoreResourceManagerConfig(const TString& endpoint, int id) {
    NKikimrClient::TConsoleRequest request;

    auto* action = request.MutableConfigureRequest()->MutableActions()->Add();
    auto* configItem = action->MutableRemoveConfigItem()->MutableConfigItemId();
    configItem->SetId(id);
    configItem->SetGeneration(1);

    // Cerr << "-- remove config id " << id << Endl;

    auto response = DoGrpcRequest(endpoint, std::move(request));
    if (response.GetConfigureResponse().GetStatus().code() != Ydb::StatusIds::SUCCESS) {
        Cerr << "error: " << response.DebugString() << Endl;
        exit(1);
    }
}

} // anonymous namespace

TCommandRunBenchmark::TCommandRunBenchmark()
    : TTpchCommandBase("run-benchmark", {"b"}, "Run benchmark")
{}

void TCommandRunBenchmark::Config(TConfig& config) {
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption('n', "iterations", "Iterations count (without cold-start run) [default: 5]")
        .DefaultValue(5)
        .StoreResult(&IterationsCount);
    config.Opts->AddLongOption('x', "mixed-snapshots", "run with volatile and persistent snapshots [default: false]")
        .StoreTrue(&MixedMode);
    config.Opts->AddLongOption('o', "output", "Output file with benchmark's report [default: report.txt]")
        .DefaultValue("report.txt")
        .StoreResult(&ReportFileName);
    config.Opts->AddLongOption('j', "json", "Json output file name [default: empty]")
        .DefaultValue("")
        .StoreResult(&JsonOutputFileName);
    config.Opts->AddLongOption('m', "mem-profile", "Enable memory profile")
        .StoreTrue(&MemoryProfile);

    auto fillTestCases = [](TStringBuf line, std::function<void(ui32)>&& op) {
        for (const auto& token : StringSplitter(line).Split(',').SkipEmpty()) {
            TStringBuf part = token.Token();
            TStringBuf from, to;
            if (part.TrySplit('-', from, to)) {
                ui32 begin = FromString(from);
                ui32 end = FromString(to);
                while (begin <= end) {
                    op(begin);
                    ++begin;
                }
            } else {
                op(FromString<ui32>(part));
            }
        }
    };

    for (ui32 q = 1; q <= 22; ++q) {
        TestCases.insert(q);
    }
    config.Opts->AddLongOption('i', "include", "Run only specified queries (ex.: 1,2,3,5-10,20)")
        .Optional()
        .Handler1T<TStringBuf>([this, fillTestCases](TStringBuf line) {
            TestCases.clear();
            fillTestCases(line, [this](ui32 q) {
                TestCases.insert(q);
            });
        });
    config.Opts->AddLongOption('e', "exclude", "Run all queries except given onces (ex.: 1,2,3,5-10,20)")
        .Optional()
        .Handler1T<TStringBuf>([this, fillTestCases](TStringBuf line) {
            fillTestCases(line, [this](ui32 q) {
                TestCases.erase(q);
            });
        });
}


NJson::TJsonValue GetQueryLabels(ui32 queryId) {
    NJson::TJsonValue labels(NJson::JSON_MAP);
    labels.InsertValue("query", Sprintf("Query%02u", queryId));
    return labels;
}

NJson::TJsonValue GetSensorValue(TStringBuf sensor, TDuration& value, ui32 queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value.MilliSeconds());
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}

NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, ui32 queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value);
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}


int TCommandRunBenchmark::Run(TConfig& config) {
    auto addedConfigId = UpdatePublishTimeout(config.Address);

    auto driver = CreateDriver(config);
    NTpch::TTpchRunner tpch{driver, Path};

    Cout << "Run benchmark against database `" << Path << "`, iterations = " << IterationsCount << Endl;

    TOFStream report{ReportFileName};
    report << "Results for " << (IterationsCount + 1) << " iterations on database `" << Path << "`" << Endl;
    if (MemoryProfile) {
        report << "+---------+----------+---------+---------+----------+---------+------+---------------------------+" << Endl;
        report << "| Query # | ColdTime |   Min   |   Max   |   Mean   |   Std   |  Ok  |Max/Sum/Extra memory (MB)  |" << Endl;
        report << "+---------+----------+---------+---------+----------+---------+------+---------------------------+" << Endl;
    } else {
        report << "+---------+----------+---------+---------+----------+---------+------+" << Endl;
        report << "| Query # | ColdTime |   Min   |   Max   |   Mean   |   Std   |  Ok  |" << Endl;
        report << "+---------+----------+---------+---------+----------+---------+------+" << Endl;
    }

    NJson::TJsonValue jsonReport(NJson::JSON_ARRAY);
    const bool collectJsonSensors = !JsonOutputFileName.empty();

    for (ui32 queryN : TestCases) {
        Cout << "-- Test query #" << queryN << Endl;
        TVector<TDuration> timings;
        timings.reserve(1 + IterationsCount);
        ui64 maxMemory = 0, sumMemory = 0, extraMemory = 0;
        TMaybe<ui64> queryResultHash;
        bool sameResult = true;
        for (ui32 i = 0; i <= IterationsCount; ++i) {
            bool usePersistentSnapshot = true;
            if (MixedMode) {
                usePersistentSnapshot = (i % 2 == 0);
            }
            Cout << "  + Iteration #" << i << ", snapshot: " << (usePersistentSnapshot ? "persistent" : "volatile") << Endl;
            TInstant before = TInstant::Now();
            auto it = tpch.RunQuery(queryN, MemoryProfile, usePersistentSnapshot);
            ui64 maxMem = 0, sumMem = 0, extraMem = 0;
            auto resultHash = ConsumeStream(it, maxMem, sumMem, extraMem);
            if (resultHash) {
                timings.emplace_back(TInstant::Now() - before);
                if (i == 0) {
                    maxMemory = maxMem;
                    sumMemory = sumMem;
                    extraMemory = extraMem;
                }

                Cout << "    result hash: " << resultHash << Endl;
                if (!queryResultHash) {
                    queryResultHash = std::move(resultHash);
                } else {
                    if (*queryResultHash != *resultHash) {
                        sameResult = false;
                    }
                }
            } else {
                ::Sleep(TDuration::Seconds(15)); // wait node (segfault?)
            }
        }
        auto testInfo = AnalyzeTestRuns(timings);
        if (MemoryProfile) {
            auto mem = Sprintf("%ld / %ld / %ld", maxMemory >> 20, sumMemory >> 20, extraMemory >> 20);
            report << Sprintf("|   %02u    | %8zu | %7zu | %7.zu | %8.2f | %7.2f |  %s  | %27s |", queryN,
                testInfo.ColdTime.MilliSeconds(), testInfo.Min.MilliSeconds(), testInfo.Max.MilliSeconds(),
                testInfo.Mean, testInfo.Std, (sameResult ? "ok" : "no"), mem.c_str()) << Endl;
        } else {
            report << Sprintf("|   %02u    | %8zu | %7zu | %7.zu | %8.2f | %7.2f |  %s  |", queryN,
                testInfo.ColdTime.MilliSeconds(), testInfo.Min.MilliSeconds(),
                testInfo.Max.MilliSeconds(), testInfo.Mean, testInfo.Std, (sameResult ? "ok" : "no")) << Endl;
        }
        if (collectJsonSensors) {
            jsonReport.AppendValue(GetSensorValue("ColdTime", testInfo.ColdTime, queryN));
            jsonReport.AppendValue(GetSensorValue("Min", testInfo.Min, queryN));
            jsonReport.AppendValue(GetSensorValue("Max", testInfo.Max, queryN));
            jsonReport.AppendValue(GetSensorValue("Mean", testInfo.Mean, queryN));
            jsonReport.AppendValue(GetSensorValue("Std", testInfo.Std, queryN));
            if (!sameResult) {
                jsonReport.AppendValue(GetSensorValue("Fail", 1, queryN));
            }
        }
    }

    if (MemoryProfile) {
        report << "+---------+----------+---------+---------+----------+---------+------+-----------------------------+" << Endl;
    } else {
        report << "+---------+----------+---------+---------+----------+---------+------+" << Endl;
    }
    report.Finish();

    if (collectJsonSensors) {
        TOFStream jStream{JsonOutputFileName};
        NJson::WriteJson(&jStream, &jsonReport, /*formatOutput*/ true);
        jStream.Finish();
    }

    {
        Cout << Endl << Endl;
        TFileInput in{ReportFileName};
        in.ReadAll(Cout);
    }

    driver.Stop(true);

    if (addedConfigId) {
        RestoreResourceManagerConfig(config.Address, *addedConfigId);
    }

    return 0;
}

TCommandRunBenchmark::TTestInfo TCommandRunBenchmark::AnalyzeTestRuns(const TVector<TDuration>& timings) {
    TTestInfo info;

    if (timings.empty()) {
        return info;
    }

    info.ColdTime = timings[0];

    if (timings.size() > 1) {
        ui32 sum = 0;
        for (size_t j = 1; j < timings.size(); ++j) {
            if (info.Max < timings[j]) {
                info.Max = timings[j];
            }
            if (!info.Min || info.Min > timings[j]) {
                info.Min = timings[j];
            }
            sum += timings[j].MilliSeconds();
        }
        info.Mean = (double) sum / (double) (timings.size() - 1);
        if (timings.size() > 2) {
            double variance = 0;
            for (size_t j = 1; j < timings.size(); ++j) {
                variance += (info.Mean - timings[j].MilliSeconds()) * (info.Mean - timings[j].MilliSeconds());
            }
            variance = variance / (double) (timings.size() - 2);
            info.Std = sqrt(variance);
        }
    }

    return info;
}

} // namespace NYdb::NTpch
