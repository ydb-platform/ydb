#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/test_tablet/test_shard_impl.h>
#include <ydb/library/fyamlcpp/fyamlcpp.h>
#include <util/string/join.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

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

bool ValidateConfig(const NKikimrSchemeOp::TCreateTestShard& op,
                                       TEvSchemeShard::EStatus& status, TString& errStr)
{
    if (op.GetCount() == 0) {
        errStr = "count must be greater than zero";
        status = TEvSchemeShard::EStatus::StatusInvalidParameter;
        return false;
    }

    if (!op.HasStorageConfig()) {
        errStr = "storage config must be specified";
        status = TEvSchemeShard::EStatus::StatusInvalidParameter;
        return false;
    }

    if (op.GetConfig().empty()) {
        errStr = "config must be specified";
        status = TEvSchemeShard::EStatus::StatusInvalidParameter;
        return false;
    }

    TYamlParser parser;
    NKikimrClient::TTestShardControlRequest::TCmdInitialize cmd;
    if (!parser.Parse(op.GetConfig(), cmd, errStr)) {
        status = TEvSchemeShard::EStatus::StatusInvalidParameter;
        return false;
    }

    return true;
}

TTestShardInfo::TPtr CreateTestShard(const NKikimrSchemeOp::TCreateTestShard& op, TTxState& state, TSchemeShard* ss)
{
    TTestShardInfo::TPtr testShard = new TTestShardInfo(1);

    state.Shards.clear();
    testShard->TestShards.clear();

    ui64 count = op.GetCount();

    state.Shards.reserve(count);
    auto startShardIdx = ss->ReserveShardIdxs(count);
    for (ui64 i = 0; i < count; ++i) {
        const auto idx = ss->NextShardIdx(startShardIdx, i);
        testShard->TestShards[idx] = InvalidTabletId;
        state.Shards.emplace_back(idx, TTabletTypes::TestShard, TTxState::CreateParts);
    }

    return testShard;
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;
    const NKikimrSchemeOp::TCreateTestShard& Op;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateTestShard TConfigureParts"
                << ", operationId: " << OperationId;
    }

public:
    TConfigureParts(TOperationId id, const NKikimrSchemeOp::TCreateTestShard& op)
        : OperationId(id)
        , Op(op)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(NKikimr::NTestShard::TEvControlResponse::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        TTabletId tabletId = TTabletId(ev->Get()->Record.GetTabletId());

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvControlResponse"
                               << ", at schemeshard: " << ssId
                               << ", from tablet: " << tabletId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTestShard);

        auto idx = context.SS->MustGetShardIdx(tabletId);
        txState->ShardsInProgress.erase(idx);

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTestShard);

        auto testShardInfo = context.SS->TestShards[txState->TargetPathId];
        Y_VERIFY_S(testShardInfo, "test shard info is null. PathId: " << txState->TargetPathId);

        TYamlParser parser;
        NKikimrClient::TTestShardControlRequest::TCmdInitialize cmd;
        TString errStr;
        if (!parser.Parse(Op.GetConfig(), cmd, errStr)) {
             // Should have been validated in Propose
             Y_ABORT("Failed to parse config in ProgressState: %s", errStr.c_str());
        }

        txState->ClearShardsInProgress();

        for (const auto& shard: txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos[shardIdx].TabletID;
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::TestShard);

            testShardInfo->TestShards[shardIdx] = tabletId;

            auto event = MakeHolder<NKikimr::NTestShard::TEvControlRequest>();
            event->Record.SetTabletId(ui64(tabletId));
            *event->Record.MutableInitialize() = cmd;

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, shardIdx, event.Release());
            txState->ShardsInProgress.insert(shardIdx);
        }

        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateTestShard TPropose"
                << ", operationId: " << OperationId;
    }
public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if(!txState) {
            return false;
        }

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        IncParentDirAlterVersionWithRepublish(OperationId, TPath::Init(pathId, context.SS), context);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTestShard);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TCreateTestShard: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId, Transaction.GetCreateTestShard());
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const auto& op = Transaction.GetCreateTestShard();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = op.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateTestShard Propose"
                         << ", path: "<< parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));

        TEvSchemeShard::EStatus status = NKikimrScheme::StatusAccepted;
        TString errStr;

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsLikeDirectory()
                .FailOnRestrictedCreateInTempZone();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeTestShard, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .IsValidLeafName(context.UserToken.Get())
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .IsValidACL(acl);
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!ValidateConfig(op, status, errStr)) {
            result->SetError(status, errStr);
            return result;
        }

        TChannelsBindings channelsBinding;
        if (op.GetStorageConfig().ChannelSize() == 0) {
            auto domainInfo = context.SS->ResolveDomainInfo(dstPath.GetPathIdForDomain());
            const auto& pools = domainInfo->GetStoragePools();
            if (pools.empty()) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "No storage pools available in domain");
                return result;
            }

            NKikimrSchemeOp::TKeyValueStorageConfig defaultConfig;
            for (int i = 0; i < 3; ++i) {
                auto* channel = defaultConfig.AddChannel();
                if (static_cast<size_t>(i) < pools.size()) {
                    channel->SetPreferredPoolKind(pools[i].GetKind());
                } else {
                    channel->SetPreferredPoolKind(pools[0].GetKind());
                }
            }

            bool isResolved = context.SS->ResolveSolomonChannels(defaultConfig, dstPath.GetPathIdForDomain(), channelsBinding);
            if (!isResolved) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "Unable to construct default channel binding");
                return result;
            }
        } else {
            bool isResolved = context.SS->ResolveSolomonChannels(op.GetStorageConfig(), dstPath.GetPathIdForDomain(), channelsBinding);
            if (!isResolved) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "Unable to construct channel binding with the storage pools");
                return result;
            }
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TPathElement::TPtr newPath = dstPath.Base();
        newPath->CreateTxId = OperationId.GetTxId();
        newPath->LastTxId = OperationId.GetTxId();
        newPath->PathState = TPathElement::EPathState::EPathStateCreate;
        newPath->PathType = TPathElement::EPathType::EPathTypeTestShard;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateTestShard, newPath->PathId);

        auto testShardInfo = CreateTestShard(op, txState, context.SS);
        if (!testShardInfo.Get()) {
            result->SetError(status, errStr);
            return result;
        }

        context.SS->TestShards[newPath->PathId] = testShardInfo;
        context.SS->TabletCounters->Simple()[COUNTER_TEST_SHARD_COUNT].Add(op.GetCount());
        context.SS->IncrementPathDbRefCount(newPath->PathId);

        TShardInfo shardInfo = TShardInfo::TestShardInfo(OperationId.GetTxId(), newPath->PathId);
        shardInfo.BindedChannels = channelsBinding;

        NIceDb::TNiceDb db(context.GetDB());

        for (const auto& part: testShardInfo->TestShards) {
            TShardIdx shardIdx = part.first;
            context.SS->RegisterShardInfo(shardIdx, shardInfo);

            context.SS->PersistShardMapping(db, shardIdx, InvalidTabletId, newPath->PathId, OperationId.GetTxId(), shardInfo.TabletType);
            context.SS->PersistChannelsBinding(db, shardIdx, channelsBinding);
        }
        context.SS->PersistTestShard(db, newPath->PathId);

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistTxState(db, OperationId);

        if (!acl.empty()) {
            newPath->ApplyACL(acl);
        }
        context.SS->PersistPath(db, newPath->PathId);

        context.SS->PersistUpdateNextPathId(db);
        context.SS->PersistUpdateNextShardIdx(db);

        IncParentDirAlterVersionWithRepublish(OperationId, dstPath, context);

        dstPath.DomainInfo()->IncPathsInside(context.SS);
        dstPath.DomainInfo()->AddInternalShards(txState, context.SS);

        dstPath.Base()->IncShardsInside(op.GetCount());
        IncAliveChildrenDirect(OperationId, parentPath, context);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCreateTestShard");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateTestShard AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateTestShard>;

namespace NOperation {

template <>
std::optional<TString> GetTargetName<TTag>(
    TTag,
    const TTxTransaction& tx)
{
    return tx.GetCreateTestShard().GetName();
}

template <>
bool SetName<TTag>(
    TTag,
    TTxTransaction& tx,
    const TString& name)
{
    tx.MutableCreateTestShard()->SetName(name);
    return true;
}

} // namespace NOperation

ISubOperation::TPtr CreateNewTestShard(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateTestShard>(id, tx);
}

ISubOperation::TPtr CreateNewTestShard(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateTestShard>(id, state);
}

}