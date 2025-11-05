#include "rpc_test_shard_base.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/test_tablet/test_shard_impl.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/library/fyamlcpp/fyamlcpp.h>

#include <util/string/join.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

using TEvCreateTestShardRequest =
    TGrpcRequestOperationCall<Ydb::TestShard::CreateTestShardRequest,
        Ydb::TestShard::CreateTestShardResponse>;
using TEvDeleteTestShardRequest =
    TGrpcRequestOperationCall<Ydb::TestShard::DeleteTestShardRequest,
        Ydb::TestShard::DeleteTestShardResponse>;

class TCreateTestShardRequest : public TTestShardRequestBase<TCreateTestShardRequest, TEvCreateTestShardRequest, Ydb::TestShard::CreateTestShardRequest>
{
    using TBase = TTestShardRequestBase<TCreateTestShardRequest, TEvCreateTestShardRequest, Ydb::TestShard::CreateTestShardRequest>;
    using TProtoRequest = Ydb::TestShard::CreateTestShardRequest;

public:
    TCreateTestShardRequest(IRequestOpCtx* request)
        : TBase(request)
    {}

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        const auto* req = GetProtoRequest();
        if (!req) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("Empty request");
            return false;
        }

        if (req->owner_idx() == 0) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("owner_idx must be non-zero");
            return false;
        }

        if (req->count() == 0) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("count must be greater than zero");
            return false;
        }

        if (req->channels_size() == 0) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("at least one channel binding is required");
            return false;
        }

        if (!req->config().empty()) {
            TString errorMsg;
            try {
                auto doc = NFyaml::TDocument::Parse(req->config());

                if (!ValidateYamlConfig(doc.Root(), errorMsg)) {
                    status = Ydb::StatusIds::BAD_REQUEST;
                    issues.AddIssue(TStringBuilder() << "YAML config validation failed: " << errorMsg);
                    return false;
                }

                NKikimrClient::TTestShardControlRequest::TCmdInitialize dummyCmd;
                if (!ParseYamlConfig(doc.Root(), dummyCmd, errorMsg)) {
                    status = Ydb::StatusIds::BAD_REQUEST;
                    issues.AddIssue(errorMsg);
                    return false;
                }
            } catch (const std::exception& e) {
                status = Ydb::StatusIds::BAD_REQUEST;
                issues.AddIssue(TStringBuilder() << "Failed to parse YAML config: " << e.what());
                return false;
            }
        }

        return true;
    }

    void SendRequestToHive() {
        const auto* req = GetProtoRequest();

        DomainUid = req->domain_uid() > 0 ? req->domain_uid() : 1;
        HiveId = req->hive_id();

        std::optional<TSubDomainKey> subdomainKey;
        if (!req->subdomain().empty()) {
            TStringBuf subdomain(req->subdomain());
            TStringBuf schemeShard, pathId;
            if (subdomain.TrySplit(':', schemeShard, pathId)) {
                subdomainKey = TSubDomainKey(FromString<ui64>(schemeShard), FromString<ui64>(pathId));
            }
        }

        SetupHivePipe(HiveId);

        for (ui32 i = 0; i < req->count(); ++i) {
            auto request = MakeHolder<TEvHive::TEvCreateTablet>();
            auto& record = request->Record;

            record.SetOwner(0);
            record.SetOwnerIdx(req->owner_idx() + i);
            record.SetTabletType(TTabletTypes::TestShard);
            record.SetChannelsProfile(0);

            if (subdomainKey) {
                *record.AddAllowedDomains() = *subdomainKey;
            } else {
                auto* domain = record.AddAllowedDomains();
                domain->SetSchemeShard(DomainUid);
                domain->SetPathId(1);
            }

            for (const auto& channel : req->channels()) {
                record.AddBindedChannels()->SetStoragePoolName(channel);
            }

            NTabletPipe::SendData(SelfId(), HivePipeClient, request.Release());
            ++PendingCreationResults;
        }
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHive::TEvCreateTabletReply, Handle);
            hFunc(TEvHive::TEvTabletCreationResult, Handle);
            hFunc(NTestShard::TEvControlResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }

private:
    void Handle(TEvHive::TEvCreateTabletReply::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        if (record.GetStatus() != NKikimrProto::OK && record.GetStatus() != NKikimrProto::ALREADY) {
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Hive returned error: " << NKikimrProto::EReplyStatus_Name(record.GetStatus()));
            return Reply(Ydb::StatusIds::GENERIC_ERROR, issues, Self()->ActorContext());
        }

        CreatedTabletIds.push_back(record.GetTabletID());
    }

    void Handle(TEvHive::TEvTabletCreationResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        if (record.GetStatus() != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Tablet creation failed: " << NKikimrProto::EReplyStatus_Name(record.GetStatus()));
            return Reply(Ydb::StatusIds::GENERIC_ERROR, issues, Self()->ActorContext());
        }

        if (--PendingCreationResults == 0) {
            const auto* req = GetProtoRequest();
            if (!req->config().empty()) {
                return InitializeTablets();
            }
            return ReplySuccess();
        }
    }

    bool ValidateYamlConfig(const NFyaml::TNodeRef& root, TString& errorMsg) {
        try {
            if (root.Type() != NFyaml::ENodeType::Mapping) {
                errorMsg = "Config root must be a YAML map";
                return false;
            }

            auto rootMap = root.Map();

            const THashSet<TString> validTopLevelKeys = {"workload", "limits", "timing", "validation", "tracing"};
            for (const auto& pair : rootMap) {
                TString keyStr{pair.Key().Scalar()};
                if (!validTopLevelKeys.contains(keyStr)) {
                    errorMsg = TStringBuilder() << "Unknown top-level key: '" << keyStr
                        << "'. Valid keys: workload, limits, timing, validation, tracing";
                    return false;
                }
            }

            if (rootMap.Has("workload")) {
                auto workloadNode = rootMap.at("workload");
                if (workloadNode.Type() != NFyaml::ENodeType::Mapping) {
                    errorMsg = "'workload' must be a map";
                    return false;
                }
                auto workload = workloadNode.Map();

                const THashSet<TString> validWorkloadKeys = {"sizes", "write", "restart", "patch_fraction_ppm"};
                for (const auto& pair : workload) {
                    TString keyStr{pair.Key().Scalar()};
                    if (!validWorkloadKeys.contains(keyStr)) {
                        errorMsg = TStringBuilder() << "Unknown key in 'workload': '" << keyStr
                            << "'. Valid keys: sizes, write, restart, patch_fraction_ppm";
                        return false;
                    }
                }

                if (workload.Has("sizes")) {
                    auto sizesNode = workload.at("sizes");
                    if (sizesNode.Type() != NFyaml::ENodeType::Sequence) {
                        errorMsg = "'workload.sizes' must be a list";
                        return false;
                    }
                    size_t idx = 0;
                    for (auto& sizeNode : sizesNode.Sequence()) {
                        if (sizeNode.Type() != NFyaml::ENodeType::Mapping) {
                            errorMsg = TStringBuilder() << "'workload.sizes[" << idx << "]' must be a map";
                            return false;
                        }
                        auto sizeMap = sizeNode.Map();
                        const THashSet<TString> validSizeKeys = {"weight", "min", "max", "inline"};
                        for (const auto& pair : sizeMap) {
                            TString keyStr{pair.Key().Scalar()};
                            if (!validSizeKeys.contains(keyStr)) {
                                errorMsg = TStringBuilder() << "Unknown key in 'workload.sizes[" << idx << "]': '"
                                    << keyStr << "'. Valid keys: weight, min, max, inline";
                                return false;
                            }
                        }
                        ++idx;
                    }
                }

                if (workload.Has("write")) {
                    auto writeNode = workload.at("write");
                    if (writeNode.Type() != NFyaml::ENodeType::Sequence) {
                        errorMsg = "'workload.write' must be a list";
                        return false;
                    }
                    size_t idx = 0;
                    for (auto& wNode : writeNode.Sequence()) {
                        if (wNode.Type() != NFyaml::ENodeType::Mapping) {
                            errorMsg = TStringBuilder() << "'workload.write[" << idx << "]' must be a map";
                            return false;
                        }
                        auto wMap = wNode.Map();
                        const THashSet<TString> validWriteKeys = {"frequency", "max_interval_ms", "weight"};
                        for (const auto& pair : wMap) {
                            TString keyStr{pair.Key().Scalar()};
                            if (!validWriteKeys.contains(keyStr)) {
                                errorMsg = TStringBuilder() << "Unknown key in 'workload.write[" << idx << "]': '"
                                    << keyStr << "'. Valid keys: frequency, max_interval_ms, weight";
                                return false;
                            }
                        }
                        ++idx;
                    }
                }

                if (workload.Has("restart")) {
                    auto restartNode = workload.at("restart");
                    if (restartNode.Type() != NFyaml::ENodeType::Sequence) {
                        errorMsg = "'workload.restart' must be a list";
                        return false;
                    }
                    size_t idx = 0;
                    for (auto& rNode : restartNode.Sequence()) {
                        if (rNode.Type() != NFyaml::ENodeType::Mapping) {
                            errorMsg = TStringBuilder() << "'workload.restart[" << idx << "]' must be a map";
                            return false;
                        }
                        auto rMap = rNode.Map();
                        const THashSet<TString> validRestartKeys = {"frequency", "max_interval_ms", "weight"};
                        for (const auto& pair : rMap) {
                            TString keyStr{pair.Key().Scalar()};
                            if (!validRestartKeys.contains(keyStr)) {
                                errorMsg = TStringBuilder() << "Unknown key in 'workload.restart[" << idx << "]': '"
                                    << keyStr << "'. Valid keys: frequency, max_interval_ms, weight";
                                return false;
                            }
                        }
                        ++idx;
                    }
                }

                if (workload.Has("patch_fraction_ppm")) {
                    auto patchNode = workload.at("patch_fraction_ppm");
                    if (patchNode.Type() != NFyaml::ENodeType::Scalar) {
                        errorMsg = "'workload.patch_fraction_ppm' must be a scalar value";
                        return false;
                    }
                }
            }

            if (rootMap.Has("limits")) {
                auto limitsNode = rootMap.at("limits");
                if (limitsNode.Type() != NFyaml::ENodeType::Mapping) {
                    errorMsg = "'limits' must be a map";
                    return false;
                }
                auto limits = limitsNode.Map();

                const THashSet<TString> validLimitsKeys = {"data", "concurrency"};
                for (const auto& pair : limits) {
                    TString keyStr{pair.Key().Scalar()};
                    if (!validLimitsKeys.contains(keyStr)) {
                        errorMsg = TStringBuilder() << "Unknown key in 'limits': '" << keyStr
                            << "'. Valid keys: data, concurrency";
                        return false;
                    }
                }

                if (limits.Has("data")) {
                    auto dataNode = limits.at("data");
                    if (dataNode.Type() != NFyaml::ENodeType::Mapping) {
                        errorMsg = "'limits.data' must be a map";
                        return false;
                    }
                    auto data = dataNode.Map();
                    const THashSet<TString> validDataKeys = {"min", "max"};
                    for (const auto& pair : data) {
                        TString keyStr{pair.Key().Scalar()};
                        if (!validDataKeys.contains(keyStr)) {
                            errorMsg = TStringBuilder() << "Unknown key in 'limits.data': '" << keyStr
                                << "'. Valid keys: min, max";
                            return false;
                        }
                    }
                }

                if (limits.Has("concurrency")) {
                    auto concNode = limits.at("concurrency");
                    if (concNode.Type() != NFyaml::ENodeType::Mapping) {
                        errorMsg = "'limits.concurrency' must be a map";
                        return false;
                    }
                    auto conc = concNode.Map();
                    const THashSet<TString> validConcKeys = {"writes", "reads"};
                    for (const auto& pair : conc) {
                        TString keyStr{pair.Key().Scalar()};
                        if (!validConcKeys.contains(keyStr)) {
                            errorMsg = TStringBuilder() << "Unknown key in 'limits.concurrency': '" << keyStr
                                << "'. Valid keys: writes, reads";
                            return false;
                        }
                    }
                }
            }

            if (rootMap.Has("timing")) {
                auto timingNode = rootMap.at("timing");
                if (timingNode.Type() != NFyaml::ENodeType::Mapping) {
                    errorMsg = "'timing' must be a map";
                    return false;
                }
                auto timing = timingNode.Map();

                const THashSet<TString> validTimingKeys = {"delay_start", "reset_on_full", "stall_counter"};
                for (const auto& pair : timing) {
                    TString keyStr{pair.Key().Scalar()};
                    if (!validTimingKeys.contains(keyStr)) {
                        errorMsg = TStringBuilder() << "Unknown key in 'timing': '" << keyStr
                            << "'. Valid keys: delay_start, reset_on_full, stall_counter";
                        return false;
                    }
                }
            }

            if (rootMap.Has("validation")) {
                auto validationNode = rootMap.at("validation");
                if (validationNode.Type() != NFyaml::ENodeType::Mapping) {
                    errorMsg = "'validation' must be a map";
                    return false;
                }
                auto validation = validationNode.Map();

                const THashSet<TString> validValidationKeys = {"server", "after_bytes"};
                for (const auto& pair : validation) {
                    TString keyStr{pair.Key().Scalar()};
                    if (!validValidationKeys.contains(keyStr)) {
                        errorMsg = TStringBuilder() << "Unknown key in 'validation': '" << keyStr
                            << "'. Valid keys: server, after_bytes";
                        return false;
                    }
                }
            }

            if (rootMap.Has("tracing")) {
                auto tracingNode = rootMap.at("tracing");
                if (tracingNode.Type() != NFyaml::ENodeType::Mapping) {
                    errorMsg = "'tracing' must be a map";
                    return false;
                }
                auto tracing = tracingNode.Map();

                const THashSet<TString> validTracingKeys = {"put_fraction_ppm", "verbosity"};
                for (const auto& pair : tracing) {
                    TString keyStr{pair.Key().Scalar()};
                    if (!validTracingKeys.contains(keyStr)) {
                        errorMsg = TStringBuilder() << "Unknown key in 'tracing': '" << keyStr
                            << "'. Valid keys: put_fraction_ppm, verbosity";
                        return false;
                    }
                }
            }

            return true;
        } catch (const std::exception& e) {
            errorMsg = TStringBuilder() << "YAML validation error: " << e.what();
            return false;
        }
    }

    bool ParseYamlConfig(const NFyaml::TNodeRef& root, NKikimrClient::TTestShardControlRequest::TCmdInitialize& initCmd, TString& errorMsg) {
        auto rootMap = root.Map();
        try {
            if (rootMap.Has("workload")) {
                auto workload = rootMap.at("workload").Map();

                if (workload.Has("sizes")) {
                    auto sizes = workload.at("sizes");
                    for (auto& sizeNode : sizes.Sequence()) {
                        auto* s = initCmd.AddSizes();
                        auto sizeMap = sizeNode.Map();
                        if (sizeMap.Has("weight")) s->SetWeight(FromString<ui64>(sizeMap.at("weight").Scalar()));
                        if (sizeMap.Has("min")) s->SetMin(FromString<ui32>(sizeMap.at("min").Scalar()));
                        if (sizeMap.Has("max")) s->SetMax(FromString<ui32>(sizeMap.at("max").Scalar()));
                        if (sizeMap.Has("inline")) s->SetInline(FromString<bool>(sizeMap.at("inline").Scalar()));
                    }
                }

                if (workload.Has("write")) {
                    auto write = workload.at("write");
                    for (auto& writeNode : write.Sequence()) {
                        auto* p = initCmd.AddWritePeriods();
                        auto writeMap = writeNode.Map();
                        p->SetWeight(writeMap.Has("weight") ? FromString<ui64>(writeMap.at("weight").Scalar()) : 1);
                        if (writeMap.Has("frequency")) p->SetFrequency(FromString<double>(writeMap.at("frequency").Scalar()));
                        if (writeMap.Has("max_interval_ms")) p->SetMaxIntervalMs(FromString<ui32>(writeMap.at("max_interval_ms").Scalar()));
                    }
                }

                if (workload.Has("restart")) {
                    auto restart = workload.at("restart");
                    for (auto& restartNode : restart.Sequence()) {
                        auto* p = initCmd.AddRestartPeriods();
                        auto restartMap = restartNode.Map();
                        p->SetWeight(restartMap.Has("weight") ? FromString<ui64>(restartMap.at("weight").Scalar()) : 1);
                        if (restartMap.Has("frequency")) p->SetFrequency(FromString<double>(restartMap.at("frequency").Scalar()));
                        if (restartMap.Has("max_interval_ms")) p->SetMaxIntervalMs(FromString<ui32>(restartMap.at("max_interval_ms").Scalar()));
                    }
                }

                if (workload.Has("patch_fraction_ppm")) {
                    initCmd.SetPatchRequestsFractionPPM(FromString<ui32>(workload.at("patch_fraction_ppm").Scalar()));
                }
            }

            if (rootMap.Has("limits")) {
                auto limits = rootMap.at("limits").Map();

                if (limits.Has("data")) {
                    auto data = limits.at("data").Map();
                    if (data.Has("min")) initCmd.SetMinDataBytes(FromString<ui64>(data.at("min").Scalar()));
                    if (data.Has("max")) initCmd.SetMaxDataBytes(FromString<ui64>(data.at("max").Scalar()));
                }

                if (limits.Has("concurrency")) {
                    auto concurrency = limits.at("concurrency").Map();
                    if (concurrency.Has("writes")) initCmd.SetMaxInFlight(FromString<ui32>(concurrency.at("writes").Scalar()));
                    if (concurrency.Has("reads")) initCmd.SetMaxReadsInFlight(FromString<ui32>(concurrency.at("reads").Scalar()));
                }
            }

            if (rootMap.Has("timing")) {
                auto timing = rootMap.at("timing").Map();
                if (timing.Has("delay_start")) initCmd.SetSecondsBeforeLoadStart(FromString<ui32>(timing.at("delay_start").Scalar()));
                if (timing.Has("reset_on_full")) initCmd.SetResetWritePeriodOnFull(FromString<bool>(timing.at("reset_on_full").Scalar()));
                if (timing.Has("stall_counter")) initCmd.SetStallCounter(FromString<ui32>(timing.at("stall_counter").Scalar()));
            }

            if (rootMap.Has("validation")) {
                auto validation = rootMap.at("validation").Map();
                if (validation.Has("server")) {
                    TString serverStr{validation.at("server").Scalar()};
                    TStringBuf server{serverStr};
                    TStringBuf host, port;
                    if (server.TrySplit(':', host, port)) {
                        initCmd.SetStorageServerHost(TString{host});
                        initCmd.SetStorageServerPort(FromString<i32>(port));
                    } else {
                        initCmd.SetStorageServerHost(serverStr);
                    }
                }
                if (validation.Has("after_bytes")) {
                    initCmd.SetValidateAfterBytes(FromString<ui64>(validation.at("after_bytes").Scalar()));
                }
            }

            if (rootMap.Has("tracing")) {
                auto tracing = rootMap.at("tracing").Map();
                if (tracing.Has("put_fraction_ppm")) {
                    initCmd.SetPutTraceFractionPPM(FromString<ui32>(tracing.at("put_fraction_ppm").Scalar()));
                }
                if (tracing.Has("verbosity")) {
                    initCmd.SetPutTraceVerbosity(FromString<ui32>(tracing.at("verbosity").Scalar()));
                }
            }

            return true;
        } catch (const std::exception& e) {
            errorMsg = TStringBuilder() << "Failed to parse YAML values: " << e.what();
            return false;
        }
    }

    void InitializeTablets() {
        const auto* req = GetProtoRequest();

        NKikimrClient::TTestShardControlRequest::TCmdInitialize initCmd;
        TString errorMsg;

        auto doc = NFyaml::TDocument::Parse(req->config());
        if (!ParseYamlConfig(doc.Root(), initCmd, errorMsg)) {
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Unexpected error parsing YAML config: " << errorMsg);
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, Self()->ActorContext());
        }

        for (ui64 tabletId : CreatedTabletIds) {
            NTabletPipe::TClientConfig pipeConfig;
            pipeConfig.RetryPolicy = {.RetryLimitCount = 3u};

            auto pipeId = RegisterWithSameMailbox(
                NTabletPipe::CreateClient(SelfId(), tabletId, pipeConfig));

            TestShardPipes[tabletId] = pipeId;

            auto request = MakeHolder<NTestShard::TEvControlRequest>();
            request->Record.SetTabletId(tabletId);
            *request->Record.MutableInitialize() = initCmd;

            NTabletPipe::SendData(SelfId(), pipeId, request.Release());
            ++PendingInitRequests;
        }
    }

    void Handle(NTestShard::TEvControlResponse::TPtr& /*ev*/) {

        if (--PendingInitRequests == 0) {
            ReplySuccess();
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue("Failed to connect to tablet pipe");
            return Reply(Ydb::StatusIds::UNAVAILABLE, issues, Self()->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        NYql::TIssues issues;
        issues.AddIssue("Tablet pipe destroyed unexpectedly");
        return Reply(Ydb::StatusIds::UNAVAILABLE, issues, Self()->ActorContext());
    }

    void ReplySuccess() {
        Ydb::TestShard::CreateTestShardResult result;
        for (ui64 tabletId : CreatedTabletIds) {
            result.add_tablet_ids(tabletId);
        }

        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, Self()->ActorContext());
    }

    void PassAway() override {
        for (auto& [tabletId, pipeId] : TestShardPipes) {
            if (pipeId) {
                NTabletPipe::CloseClient(Self()->SelfId(), pipeId);
            }
        }
        TestShardPipes.clear();
        TBase::PassAway();
    }

private:
    ui32 PendingCreationResults = 0;
    ui32 PendingInitRequests = 0;
    TVector<ui64> CreatedTabletIds;
    THashMap<ui64, TActorId> TestShardPipes;
};

class TDeleteTestShardRequest : public TTestShardRequestBase<TDeleteTestShardRequest, TEvDeleteTestShardRequest, Ydb::TestShard::DeleteTestShardRequest>
{
    using TBase = TTestShardRequestBase<TDeleteTestShardRequest, TEvDeleteTestShardRequest, Ydb::TestShard::DeleteTestShardRequest>;
    using TProtoRequest = Ydb::TestShard::DeleteTestShardRequest;

public:
    TDeleteTestShardRequest(IRequestOpCtx* request)
        : TBase(request)
    {}

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        const auto* req = GetProtoRequest();
        if (!req) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("Empty request");
            return false;
        }

        if (req->owner_idx() == 0) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("owner_idx must be non-zero");
            return false;
        }

        if (req->count() == 0) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("count must be greater than zero");
            return false;
        }

        return true;
    }

    void SendRequestToHive() {
        const auto* req = GetProtoRequest();

        HiveId = req->hive_id();
        SetupHivePipe(HiveId);

        for (ui32 i = 0; i < req->count(); ++i) {
            auto request = MakeHolder<TEvHive::TEvDeleteTablet>();
            auto& record = request->Record;
            record.SetShardOwnerId(0);
            record.AddShardLocalIdx(req->owner_idx() + i);
            NTabletPipe::SendData(SelfId(), HivePipeClient, request.Release());
            ++PendingRequests;
        }
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHive::TEvDeleteTabletReply, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }

private:
    void Handle(TEvHive::TEvDeleteTabletReply::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        if (record.GetStatus() != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Hive returned error: "
                << NKikimrProto::EReplyStatus_Name(record.GetStatus()));
            return Reply(Ydb::StatusIds::GENERIC_ERROR, issues, Self()->ActorContext());
        }

        if (--PendingRequests == 0) {
            return ReplySuccess();
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (PendingRequests > 0 && ev->Get()->Status != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue("Failed to connect to Hive pipe");
            return Reply(Ydb::StatusIds::UNAVAILABLE, issues, Self()->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        if (PendingRequests > 0) {
            NYql::TIssues issues;
            issues.AddIssue("Hive pipe destroyed unexpectedly");
            return Reply(Ydb::StatusIds::UNAVAILABLE, issues, Self()->ActorContext());
        }
    }

    void ReplySuccess() {
        Ydb::TestShard::DeleteTestShardResult result;

        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, Self()->ActorContext());
    }

private:
    ui32 PendingRequests = 0;
};


void DoCreateTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TCreateTestShardRequest(p.release()));
}

void DoDeleteTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDeleteTestShardRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
