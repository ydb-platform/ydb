#include "rpc_test_shard_base.h"

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/test_shard_control.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/fyamlcpp/fyamlcpp.h>
#include <util/string/join.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

class TYamlParser {
public:
    bool Parse(const TString& yaml, NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd, TString& errorMsg) {
        try {
            auto doc = NFyaml::TDocument::Parse(yaml);
            return ParseYamlConfig(doc.Root(), cmd, errorMsg);
        } catch (const std::exception& e) {
            errorMsg = TStringBuilder() << "Failed to parse YAML config: " << e.what();
            return false;
        }
    }

private:
    bool ValidateYamlKeys(const auto& mapping, const TString& sectionName,
                         const THashSet<TString>& validKeys, TString& errorMsg) {
        for (const auto& pair : mapping) {
            TString keyStr{pair.Key().Scalar()};
            if (!validKeys.contains(keyStr)) {
                TVector<TString> keysList;
                for (const auto& key : validKeys) {
                    keysList.push_back(key);
                }
                Sort(keysList);
                errorMsg = TStringBuilder() << "Unknown key in '" << sectionName << "': '" << keyStr
                    << "'. Valid keys: " << JoinSeq(", ", keysList);
                return false;
            }
        }
        return true;
    }

    bool ParseWorkloadSection(const NFyaml::TNodeRef& workloadNode, NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd, TString& errorMsg) {
        auto workload = workloadNode.Map();
        const THashSet<TString> validWorkloadKeys = {"sizes", "write", "restart", "patch_fraction_ppm"};
        if (!ValidateYamlKeys(workload, "workload", validWorkloadKeys, errorMsg)) {
            return false;
        }

        if (workload.Has("sizes")) {
            auto sizes = workload.at("sizes");
            if (sizes.Type() != NFyaml::ENodeType::Sequence) {
                errorMsg = "'workload.sizes' must be a list";
                return false;
            }
            for (auto& sizeNode : sizes.Sequence()) {
                auto* s = cmd.AddSizes();
                auto sizeMap = sizeNode.Map();
                if (sizeMap.Has("weight")) s->SetWeight(FromString<ui64>(sizeMap.at("weight").Scalar()));
                if (sizeMap.Has("min")) s->SetMin(FromString<ui32>(sizeMap.at("min").Scalar()));
                if (sizeMap.Has("max")) s->SetMax(FromString<ui32>(sizeMap.at("max").Scalar()));
                if (sizeMap.Has("inline")) s->SetInline(FromString<bool>(sizeMap.at("inline").Scalar()));
            }
        }

        if (workload.Has("write")) {
            auto write = workload.at("write");
            if (write.Type() != NFyaml::ENodeType::Sequence) {
                errorMsg = "'workload.write' must be a list";
                return false;
            }
            for (auto& writeNode : write.Sequence()) {
                auto* p = cmd.AddWritePeriods();
                auto writeMap = writeNode.Map();
                p->SetWeight(writeMap.Has("weight") ? FromString<ui64>(writeMap.at("weight").Scalar()) : 1);
                if (writeMap.Has("frequency")) p->SetFrequency(FromString<double>(writeMap.at("frequency").Scalar()));
                if (writeMap.Has("max_interval_ms")) p->SetMaxIntervalMs(FromString<ui32>(writeMap.at("max_interval_ms").Scalar()));
            }
        }

        if (workload.Has("restart")) {
            auto restart = workload.at("restart");
            if (restart.Type() != NFyaml::ENodeType::Sequence) {
                errorMsg = "'workload.restart' must be a list";
                return false;
            }
            for (auto& restartNode : restart.Sequence()) {
                auto* p = cmd.AddRestartPeriods();
                auto restartMap = restartNode.Map();
                p->SetWeight(restartMap.Has("weight") ? FromString<ui64>(restartMap.at("weight").Scalar()) : 1);
                if (restartMap.Has("frequency")) p->SetFrequency(FromString<double>(restartMap.at("frequency").Scalar()));
                if (restartMap.Has("max_interval_ms")) p->SetMaxIntervalMs(FromString<ui32>(restartMap.at("max_interval_ms").Scalar()));
            }
        }

        if (workload.Has("patch_fraction_ppm")) {
            cmd.SetPatchRequestsFractionPPM(FromString<ui32>(workload.at("patch_fraction_ppm").Scalar()));
        }

        return true;
    }

    bool ParseLimitsSection(const NFyaml::TNodeRef& limitsNode, NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd, TString& errorMsg) {
        auto limits = limitsNode.Map();
        const THashSet<TString> validLimitsKeys = {"data", "concurrency"};
        if (!ValidateYamlKeys(limits, "limits", validLimitsKeys, errorMsg)) {
            return false;
        }

        if (limits.Has("data")) {
            auto dataNode = limits.at("data");
            if (dataNode.Type() != NFyaml::ENodeType::Mapping) {
                errorMsg = "'limits.data' must be a map";
                return false;
            }
            auto data = dataNode.Map();
            if (data.Has("min")) cmd.SetMinDataBytes(FromString<ui64>(data.at("min").Scalar()));
            if (data.Has("max")) cmd.SetMaxDataBytes(FromString<ui64>(data.at("max").Scalar()));
        }

        if (limits.Has("concurrency")) {
            auto concNode = limits.at("concurrency");
            if (concNode.Type() != NFyaml::ENodeType::Mapping) {
                errorMsg = "'limits.concurrency' must be a map";
                return false;
            }
            auto concurrency = concNode.Map();
            if (concurrency.Has("writes")) cmd.SetMaxInFlight(FromString<ui32>(concurrency.at("writes").Scalar()));
            if (concurrency.Has("reads")) cmd.SetMaxReadsInFlight(FromString<ui32>(concurrency.at("reads").Scalar()));
        }

        return true;
    }

    bool ParseTimingSection(const NFyaml::TNodeRef& timingNode, NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd, TString& errorMsg) {
        auto timing = timingNode.Map();
        const THashSet<TString> validTimingKeys = {"delay_start", "reset_on_full", "stall_counter"};
        if (!ValidateYamlKeys(timing, "timing", validTimingKeys, errorMsg)) {
            return false;
        }

        if (timing.Has("delay_start")) cmd.SetSecondsBeforeLoadStart(FromString<ui32>(timing.at("delay_start").Scalar()));
        if (timing.Has("reset_on_full")) cmd.SetResetWritePeriodOnFull(FromString<bool>(timing.at("reset_on_full").Scalar()));
        if (timing.Has("stall_counter")) cmd.SetStallCounter(FromString<ui32>(timing.at("stall_counter").Scalar()));

        return true;
    }

    bool ParseValidationSection(const NFyaml::TNodeRef& validationNode, NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd, TString& errorMsg) {
        auto validation = validationNode.Map();
        const THashSet<TString> validValidationKeys = {"server", "after_bytes"};
        if (!ValidateYamlKeys(validation, "validation", validValidationKeys, errorMsg)) {
            return false;
        }

        if (validation.Has("server")) {
            TString serverStr{validation.at("server").Scalar()};
            if (serverStr.find(':') == TString::npos) {
                errorMsg = "'validation.server' must be in 'host:port' format";
                return false;
            }
            TStringBuf server{serverStr};
            TStringBuf host, port;
            if (server.TrySplit(':', host, port)) {
                cmd.SetStorageServerHost(TString{host});
                cmd.SetStorageServerPort(FromString<i32>(port));
            }
        }
        if (validation.Has("after_bytes")) {
            cmd.SetValidateAfterBytes(FromString<ui64>(validation.at("after_bytes").Scalar()));
        }

        return true;
    }

    bool ParseTracingSection(const NFyaml::TNodeRef& tracingNode, NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd, TString& errorMsg) {
        auto tracing = tracingNode.Map();
        const THashSet<TString> validTracingKeys = {"put_fraction_ppm", "verbosity"};
        if (!ValidateYamlKeys(tracing, "tracing", validTracingKeys, errorMsg)) {
            return false;
        }

        if (tracing.Has("put_fraction_ppm")) {
            cmd.SetPutTraceFractionPPM(FromString<ui32>(tracing.at("put_fraction_ppm").Scalar()));
        }
        if (tracing.Has("verbosity")) {
            cmd.SetPutTraceVerbosity(FromString<ui32>(tracing.at("verbosity").Scalar()));
        }

        return true;
    }

    bool ParseYamlConfig(const NFyaml::TNodeRef& root, NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd, TString& errorMsg) {
        try {
            if (root.Type() != NFyaml::ENodeType::Mapping) {
                errorMsg = "Config root must be a YAML map";
                return false;
            }

            auto rootMap = root.Map();

            const THashSet<TString> validTopLevelKeys = {"workload", "limits", "timing", "validation", "tracing"};
            if (!ValidateYamlKeys(rootMap, "config root", validTopLevelKeys, errorMsg)) {
                return false;
            }

            #define PARSE_YAML_SECTION(sectionName, parserMethod) \
                if (rootMap.Has(sectionName)) { \
                    auto node = rootMap.at(sectionName); \
                    if (node.Type() != NFyaml::ENodeType::Mapping) { \
                        errorMsg = TStringBuilder() << "'" << sectionName << "' must be a map"; \
                        return false; \
                    } \
                    if (!parserMethod(node, cmd, errorMsg)) { \
                        return false; \
                    } \
                }

            PARSE_YAML_SECTION("workload", ParseWorkloadSection)
            PARSE_YAML_SECTION("limits", ParseLimitsSection)
            PARSE_YAML_SECTION("timing", ParseTimingSection)
            PARSE_YAML_SECTION("validation", ParseValidationSection)
            PARSE_YAML_SECTION("tracing", ParseTracingSection)

            #undef PARSE_YAML_SECTION

            return true;
        } catch (const std::exception& e) {
            errorMsg = TStringBuilder() << "Failed to parse YAML values: " << e.what();
            return false;
        }
    }
};

using TEvCreateTestShardRequest =
    TGrpcRequestOperationCall<Ydb::TestShard::CreateTestShardRequest,
        Ydb::TestShard::CreateTestShardResponse>;
using TEvDeleteTestShardRequest =
    TGrpcRequestOperationCall<Ydb::TestShard::DeleteTestShardRequest,
        Ydb::TestShard::DeleteTestShardResponse>;

class TCreateTestShardRequest : public TRpcSchemeRequestActor<TCreateTestShardRequest, TEvCreateTestShardRequest> {
public:
    using TBase = TRpcSchemeRequestActor<TCreateTestShardRequest, TEvCreateTestShardRequest>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TCreateTestShardRequest::StateFunc);
        SendProposeRequest(ctx);
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = this->GetProtoRequest();

        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = this->CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTestShardSet);
        auto* op = modifyScheme->MutableCreateTestShardSet();

        op->SetName(name);
        op->SetCount(req->count());

        TYamlParser parser;
        NKikimrClient::TTestShardControlRequest::TCmdInitialize cmd;
        TString errorMsg;
        if (!parser.Parse(req->config(), cmd, errorMsg)) {
            Request_->RaiseIssue(NYql::TIssue(errorMsg));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
        *op->MutableCmdInitialize() = cmd;

        auto* storageConfig = op->MutableStorageConfig();
        for (const auto& channel : req->channels()) {
             auto* settings = storageConfig->AddChannel();
             settings->SetPreferredPoolKind(channel);
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void OnNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) override {
        Y_UNUSED(ev);
        const auto req = this->GetProtoRequest();

        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        SetAuthToken(navigateRequest, *this->Request_);
        SetDatabase(navigateRequest.get(), *this->Request_);
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(req->path());

        ctx.Send(MakeTxProxyID(), navigateRequest.release());
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();

        if (status != NKikimrScheme::StatusSuccess) {
            return this->Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
        }

        const auto& pathDescription = record.GetPathDescription();
        if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeTestShardSet) {
            return this->Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
        }

        Ydb::TestShard::CreateTestShardResult result;
        const auto& testShardSetDesc = pathDescription.GetTestShardSetDescription();
        for (auto tabletId : testShardSetDesc.GetTabletIds()) {
            result.add_tablet_ids(tabletId);
        }

        return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default: TBase::StateWork(ev);
        }
    }
};

class TDeleteTestShardRequest : public TRpcSchemeRequestActor<TDeleteTestShardRequest, TEvDeleteTestShardRequest> {
public:
    using TBase = TRpcSchemeRequestActor<TDeleteTestShardRequest, TEvDeleteTestShardRequest>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TDeleteTestShardRequest::StateFunc);
        SendProposeRequest(ctx);
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = this->GetProtoRequest();

        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = this->CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTestShardSet);
        auto* op = modifyScheme->MutableDrop();
        
        op->SetName(name);

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    STFUNC(StateFunc) {
        return TBase::StateWork(ev);
    }
};

void DoCreateTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TCreateTestShardRequest(p.release()));
}

void DoDeleteTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDeleteTestShardRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
