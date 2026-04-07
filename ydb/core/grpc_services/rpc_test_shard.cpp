#include "rpc_test_shard_base.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/test_tablet/test_shard_impl.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
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
        static constexpr ui32 MAX_TABLETS_PER_REQUEST = 1000;

        const auto* req = GetProtoRequest();
        if (!req) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("Empty request");
            return false;
        }

        if (req->database().empty()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("database path is required");
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

        if (req->count() > MAX_TABLETS_PER_REQUEST) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue(TStringBuilder() << "count must be less than or equal to " << MAX_TABLETS_PER_REQUEST);
            return false;
        }

        if (TBase::IsRootDomain(req->database()) && req->channels_size() == 0) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("channels must be explicitly specified for root domain (/Root)");
            return false;
        }

        if (req->config().empty()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("config must be specified");
            return false;
        }

        TString errorMsg;
        try {
            auto doc = NFyaml::TDocument::Parse(req->config());
            if (!ParseYamlConfig(doc.Root(), errorMsg)) {
                status = Ydb::StatusIds::BAD_REQUEST;
                issues.AddIssue(errorMsg);
                return false;
            }
        } catch (const std::exception& e) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue(TStringBuilder() << "Failed to parse YAML config: " << e.what());
            return false;
        }

        return true;
    }

    void Proceed() {
        const auto* req = GetProtoRequest();
        TBase::ResolveDatabase(req->database());
    }

    void OnDatabaseResolved(bool isRootDomain, const NSchemeCache::TDomainInfo* domainInfo = nullptr,
                            const NSchemeCache::TSchemeCacheNavigate::TDomainDescription* domainDescription = nullptr) {
        if (isRootDomain) {
            const auto* appDomainInfo = AppData()->DomainsInfo.Get();
            SubdomainKey = TPathId(appDomainInfo->Domain->SchemeRoot, 1);
        } else {
            SubdomainKey = domainInfo->DomainKey;

            const auto* req = GetProtoRequest();
            if (req->channels_size() == 0) {
                if (domainDescription && domainDescription->Description.StoragePoolsSize() > 0) {
                    for (const auto& pool : domainDescription->Description.GetStoragePools()) {
                        DomainStoragePools.push_back(pool.GetName());
                    }
                }
            }
        }

        SendRequestToHive();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TBase::HandleNavigateResult(ev);
    }

    void SendRequestToHive() {
        static constexpr ui32 DEFAULT_CHANNEL_COUNT = 3;
        static constexpr ui32 HIVE_TIMEOUT = 15;

        const auto* req = GetProtoRequest();

        std::vector<TString> channels;
        if (req->channels_size() > 0) {
            for (const auto& channel : req->channels()) {
                channels.push_back(channel);
            }
        } else {
            if (DomainStoragePools.empty()) {
                NYql::TIssues issues;
                issues.AddIssue("No channels specified and no storage pools available in domain");
                return Reply(Ydb::StatusIds::BAD_REQUEST, issues, Self()->ActorContext());
            }

            for (ui32 i = 0; i < DEFAULT_CHANNEL_COUNT; ++i) {
                channels.push_back(DomainStoragePools[i % DomainStoragePools.size()]);
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

            auto* domain = record.AddAllowedDomains();
            domain->SetSchemeShard(SubdomainKey.OwnerId);
            domain->SetPathId(SubdomainKey.LocalPathId);

            for (const auto& channel : channels) {
                record.AddBindedChannels()->SetStoragePoolName(channel);
            }

            NTabletPipe::SendData(SelfId(), HivePipeClient, request.Release());
        }

        PendingCreationResults = req->count();

        Schedule(TDuration::Seconds(HIVE_TIMEOUT), new TEvents::TEvWakeup());
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvHive::TEvCreateTabletReply, Handle);
            hFunc(TEvHive::TEvTabletCreationResult, Handle);
            hFunc(NTestShard::TEvControlResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvents::TEvWakeup, HandleTimeout);
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

        if (record.GetStatus() == NKikimrProto::ALREADY) {
            if (--PendingCreationResults == 0) {
                return InitializeTablets();
            }
        }
    }

    void Handle(TEvHive::TEvTabletCreationResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        if (record.GetStatus() != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Tablet creation failed: " << NKikimrProto::EReplyStatus_Name(record.GetStatus()));
            return Reply(Ydb::StatusIds::GENERIC_ERROR, issues, Self()->ActorContext());
        }

        if (--PendingCreationResults == 0) {
            return InitializeTablets();
        }
    }

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

    bool ParseWorkloadSection(const NFyaml::TNodeRef& workloadNode, TString& errorMsg) {
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
                auto* s = ParsedInitCmd.AddSizes();
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
                auto* p = ParsedInitCmd.AddWritePeriods();
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
                auto* p = ParsedInitCmd.AddRestartPeriods();
                auto restartMap = restartNode.Map();
                p->SetWeight(restartMap.Has("weight") ? FromString<ui64>(restartMap.at("weight").Scalar()) : 1);
                if (restartMap.Has("frequency")) p->SetFrequency(FromString<double>(restartMap.at("frequency").Scalar()));
                if (restartMap.Has("max_interval_ms")) p->SetMaxIntervalMs(FromString<ui32>(restartMap.at("max_interval_ms").Scalar()));
            }
        }

        if (workload.Has("patch_fraction_ppm")) {
            ParsedInitCmd.SetPatchRequestsFractionPPM(FromString<ui32>(workload.at("patch_fraction_ppm").Scalar()));
        }

        return true;
    }

    bool ParseLimitsSection(const NFyaml::TNodeRef& limitsNode, TString& errorMsg) {
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
            if (data.Has("min")) ParsedInitCmd.SetMinDataBytes(FromString<ui64>(data.at("min").Scalar()));
            if (data.Has("max")) ParsedInitCmd.SetMaxDataBytes(FromString<ui64>(data.at("max").Scalar()));
        }

        if (limits.Has("concurrency")) {
            auto concNode = limits.at("concurrency");
            if (concNode.Type() != NFyaml::ENodeType::Mapping) {
                errorMsg = "'limits.concurrency' must be a map";
                return false;
            }
            auto concurrency = concNode.Map();
            if (concurrency.Has("writes")) ParsedInitCmd.SetMaxInFlight(FromString<ui32>(concurrency.at("writes").Scalar()));
            if (concurrency.Has("reads")) ParsedInitCmd.SetMaxReadsInFlight(FromString<ui32>(concurrency.at("reads").Scalar()));
        }

        return true;
    }

    bool ParseTimingSection(const NFyaml::TNodeRef& timingNode, TString& errorMsg) {
        auto timing = timingNode.Map();
        const THashSet<TString> validTimingKeys = {"delay_start", "reset_on_full", "stall_counter"};
        if (!ValidateYamlKeys(timing, "timing", validTimingKeys, errorMsg)) {
            return false;
        }

        if (timing.Has("delay_start")) ParsedInitCmd.SetSecondsBeforeLoadStart(FromString<ui32>(timing.at("delay_start").Scalar()));
        if (timing.Has("reset_on_full")) ParsedInitCmd.SetResetWritePeriodOnFull(FromString<bool>(timing.at("reset_on_full").Scalar()));
        if (timing.Has("stall_counter")) ParsedInitCmd.SetStallCounter(FromString<ui32>(timing.at("stall_counter").Scalar()));

        return true;
    }

    bool ParseValidationSection(const NFyaml::TNodeRef& validationNode, TString& errorMsg) {
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
                ParsedInitCmd.SetStorageServerHost(TString{host});
                ParsedInitCmd.SetStorageServerPort(FromString<i32>(port));
            }
        }
        if (validation.Has("after_bytes")) {
            ParsedInitCmd.SetValidateAfterBytes(FromString<ui64>(validation.at("after_bytes").Scalar()));
        }

        return true;
    }

    bool ParseTracingSection(const NFyaml::TNodeRef& tracingNode, TString& errorMsg) {
        auto tracing = tracingNode.Map();
        const THashSet<TString> validTracingKeys = {"put_fraction_ppm", "verbosity"};
        if (!ValidateYamlKeys(tracing, "tracing", validTracingKeys, errorMsg)) {
            return false;
        }

        if (tracing.Has("put_fraction_ppm")) {
            ParsedInitCmd.SetPutTraceFractionPPM(FromString<ui32>(tracing.at("put_fraction_ppm").Scalar()));
        }
        if (tracing.Has("verbosity")) {
            ParsedInitCmd.SetPutTraceVerbosity(FromString<ui32>(tracing.at("verbosity").Scalar()));
        }

        return true;
    }

    bool ParseYamlConfig(const NFyaml::TNodeRef& root, TString& errorMsg) {
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
                    if (!parserMethod(node, errorMsg)) { \
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

    void InitializeTablets() {
        for (ui64 tabletId : CreatedTabletIds) {
            NTabletPipe::TClientConfig pipeConfig;
            pipeConfig.RetryPolicy = {.RetryLimitCount = 3u};

            auto pipeId = RegisterWithSameMailbox(
                NTabletPipe::CreateClient(SelfId(), tabletId, pipeConfig));

            TestShardPipes[tabletId] = pipeId;

            auto request = MakeHolder<NTestShard::TEvControlRequest>();
            request->Record.SetTabletId(tabletId);
            *request->Record.MutableInitialize() = ParsedInitCmd;

            NTabletPipe::SendData(SelfId(), pipeId, request.Release());
        }

        PendingInitResponses = CreatedTabletIds.size();
    }

    void Handle(NTestShard::TEvControlResponse::TPtr& /*ev*/) {
        if (--PendingInitResponses == 0) {
            ReplySuccess();
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            CleanupPipes();
            NYql::TIssues issues;
            issues.AddIssue("Failed to connect to tablet pipe");
            return Reply(Ydb::StatusIds::UNAVAILABLE, issues, Self()->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& /*ev*/) {
        CleanupPipes();
        NYql::TIssues issues;
        issues.AddIssue("Tablet pipe destroyed unexpectedly");
        return Reply(Ydb::StatusIds::UNAVAILABLE, issues, Self()->ActorContext());
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr&) {
        if (PendingCreationResults == 0 && PendingInitResponses == 0) {
            return;
        }

        NYql::TIssues issues;
        if (PendingCreationResults > 0) {
            issues.AddIssue(TStringBuilder()
                << "Timeout waiting for tablet creation from Hive. "
                << "Hive may not be able to allocate storage groups for the specified channels. "
                << "Still waiting for " << PendingCreationResults << " tablet creation results.");
        } else {
            issues.AddIssue(TStringBuilder()
                << "Timeout during tablet initialization. "
                << "Still waiting for " << PendingInitResponses << " initialization responses.");
        }

        return Reply(Ydb::StatusIds::TIMEOUT, issues, Self()->ActorContext());
    }

    void ReplySuccess() {
        Ydb::TestShard::CreateTestShardResult result;
        for (ui64 tabletId : CreatedTabletIds) {
            result.add_tablet_ids(tabletId);
        }

        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, Self()->ActorContext());
    }

    void CleanupPipes() {
        for (auto& [tabletId, pipeId] : TestShardPipes) {
            if (pipeId) {
                NTabletPipe::CloseClient(Self()->SelfId(), pipeId);
            }
        }
        TestShardPipes.clear();
    }

    void PassAway() override {
        CleanupPipes();
        TBase::PassAway();
    }

private:
    TPathId SubdomainKey;
    std::vector<TString> DomainStoragePools;
    ui32 PendingCreationResults = 0;
    ui32 PendingInitResponses = 0;
    std::vector<ui64> CreatedTabletIds;
    THashMap<ui64, TActorId> TestShardPipes;
    NKikimrClient::TTestShardControlRequest::TCmdInitialize ParsedInitCmd;
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

        if (req->database().empty()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("database path is required");
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

    void Proceed() {
        const auto* req = GetProtoRequest();
        TBase::ResolveDatabase(req->database());
    }

    void OnDatabaseResolved(bool isRootDomain, const NSchemeCache::TDomainInfo* domainInfo = nullptr,
                            const NSchemeCache::TSchemeCacheNavigate::TDomainDescription* = nullptr) {
        if (isRootDomain) {
            const auto* appDomainInfo = AppData()->DomainsInfo.Get();
            SubdomainKey = TPathId(appDomainInfo->Domain->SchemeRoot, 1);
        } else {
            SubdomainKey = domainInfo->DomainKey;
        }

        LookupTablets();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TBase::HandleNavigateResult(ev);
    }

    void LookupTablets() {
        const auto* req = GetProtoRequest();

        SetupHivePipe(HiveId);

        TotalTabletsToLookup = req->count();

        for (ui32 i = 0; i < req->count(); ++i) {
            auto request = MakeHolder<TEvHive::TEvLookupTablet>();
            request->Record.SetOwner(0);
            request->Record.SetOwnerIdx(req->owner_idx() + i);
            NTabletPipe::SendData(SelfId(), HivePipeClient, request.Release(), i);
        }
    }

    void Handle(TEvHive::TEvCreateTabletReply::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        if (record.GetStatus() == NKikimrProto::NODATA) {
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder()
                << "Tablet with owner_idx " << record.GetOwnerIdx() << " not found");
            return Reply(Ydb::StatusIds::NOT_FOUND, issues, Self()->ActorContext());
        }

        if (record.GetStatus() != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder()
                << "Lookup failed for owner_idx " << record.GetOwnerIdx()
                << ": " << NKikimrProto::EReplyStatus_Name(record.GetStatus()));
            return Reply(Ydb::StatusIds::GENERIC_ERROR, issues, Self()->ActorContext());
        }

        TabletIdToOwnerIdx[record.GetTabletID()] = record.GetOwnerIdx();

        if (TabletIdToOwnerIdx.size() == TotalTabletsToLookup) {
            RequestTabletsInfo();
        }
    }

    void RequestTabletsInfo() {
        for (const auto& [tabletId, _] : TabletIdToOwnerIdx) {
            auto request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
            request->Record.SetTabletID(tabletId);
            NTabletPipe::SendData(SelfId(), HivePipeClient, request.Release());
        }
    }

    void Handle(TEvHive::TEvResponseHiveInfo::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        if (record.TabletsSize() == 0) {
            NYql::TIssues issues;
            issues.AddIssue("Tablet info not found in Hive response");
            return Reply(Ydb::StatusIds::NOT_FOUND, issues, Self()->ActorContext());
        }

        const auto& tablet = record.GetTablets(0);

        if (tablet.GetTabletType() != TTabletTypes::TestShard) {
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder()
                << "Cannot delete tablet " << tablet.GetTabletID()
                << ": wrong type " << TTabletTypes::TypeToStr(tablet.GetTabletType())
                << ", expected TestShard");
            return Reply(Ydb::StatusIds::PRECONDITION_FAILED, issues, Self()->ActorContext());
        }

        bool domainMatches = false;
        if (tablet.HasObjectDomain()) {
            const auto& objectDomain = tablet.GetObjectDomain();
            if (objectDomain.GetSchemeShard() == SubdomainKey.OwnerId &&
                objectDomain.GetPathId() == SubdomainKey.LocalPathId) {
                domainMatches = true;
            }
        }

        if (!domainMatches) {
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder()
                << "Cannot delete tablet " << tablet.GetTabletID()
                << ": belongs to different database domain");
            return Reply(Ydb::StatusIds::PRECONDITION_FAILED, issues, Self()->ActorContext());
        }

        ++ValidatedCount;

        if (ValidatedCount == TotalTabletsToLookup) {
            DeleteTablets();
        }
    }

    void DeleteTablets() {
        auto request = MakeHolder<TEvHive::TEvDeleteTablet>();
        auto& record = request->Record;
        record.SetShardOwnerId(0);

        for (const auto& [_, ownerIdx] : TabletIdToOwnerIdx) {
            record.AddShardLocalIdx(ownerIdx);
        }

        NTabletPipe::SendData(SelfId(), HivePipeClient, request.Release());
        PendingRequests = 1;
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvHive::TEvCreateTabletReply, Handle);
            hFunc(TEvHive::TEvResponseHiveInfo, Handle);
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
        if (ev->Get()->Status != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue("Failed to connect to Hive pipe");
            return Reply(Ydb::StatusIds::UNAVAILABLE, issues, Self()->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        NYql::TIssues issues;
        issues.AddIssue("Hive pipe destroyed unexpectedly");
        return Reply(Ydb::StatusIds::UNAVAILABLE, issues, Self()->ActorContext());
    }

    void ReplySuccess() {
        Ydb::TestShard::DeleteTestShardResult result;

        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, Self()->ActorContext());
    }

private:
    TPathId SubdomainKey;
    ui32 TotalTabletsToLookup = 0;
    ui32 ValidatedCount = 0;
    ui32 PendingRequests = 0;
    THashMap<ui64, ui64> TabletIdToOwnerIdx;
};


void DoCreateTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TCreateTestShardRequest(p.release()));
}

void DoDeleteTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDeleteTestShardRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
