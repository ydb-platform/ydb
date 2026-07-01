#include "table_creator.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/retry/retry_policy.h>

#include <util/generic/guid.h>
#include <util/generic/utility.h>
#include <util/random/random.h>

#include <algorithm>

namespace NKikimr {

namespace {

class TTableCreator : public NActors::TActorBootstrapped<TTableCreator> {

using TTableCreatorRetryPolicy = IRetryPolicy<bool>;

public:
    TTableCreator(
        TVector<TString> pathComponents,
        TVector<NKikimrSchemeOp::TColumnDescription> columns,
        TVector<TString> keyColumns,
        NKikimrServices::EServiceKikimr logService,
        TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings = Nothing(),
        const TString& database = {},
        bool isSystemUser = false,
        TMaybe<NKikimrSchemeOp::TPartitioningPolicy> partitioningPolicy = Nothing(),
        TMaybe<NACLib::TDiffACL> tableAclDiff = Nothing(),
        TVector<NKikimrSchemeOp::TIndexDescription> tableIndexes = {},
        TVector<NKikimrSchemeOp::TSequenceDescription> tableSequences = {})
        : PathComponents(std::move(pathComponents))
        , Columns(std::move(columns))
        , KeyColumns(std::move(keyColumns))
        , LogService(logService)
        , TtlSettings(std::move(ttlSettings))
        , Database(database)
        , IsSystemUser(isSystemUser)
        , PartitioningPolicy(std::move(partitioningPolicy))
        , TableAclDiff(std::move(tableAclDiff))
        , TableIndexes(std::move(tableIndexes))
        , TableSequences(std::move(tableSequences))
        , LogPrefix("Table " + TableName() + " updater. ")
    {
        Y_ABORT_UNLESS(!PathComponents.empty());
        Y_ABORT_UNLESS(!Columns.empty());
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TTableCreator>::Registered(sys, owner);
        Owner = owner;
    }

    STRICT_STFUNC(StateFuncCheck,
        hFunc(TEvents::TEvUndelivered, Handle)
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        sFunc(NActors::TEvents::TEvWakeup, CheckTableExistence);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
    )

    STRICT_STFUNC(StateFuncUpgrade,
        hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
        hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        sFunc(NActors::TEvents::TEvWakeup, RunTableRequest);
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
    )

    void Bootstrap() {
        LogPrefix = TStringBuilder() << LogPrefix << " SelfId: " << SelfId() << " Owner: " << Owner << ". ";

        Become(&TTableCreator::StateFuncCheck);
        if (!Database) {
            Database = AppData()->TenantName;
        }
        CheckTableExistence();
    }

    void CheckTableExistence() {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(NTableCreator::BuildSchemeCacheNavigateRequest(
            {PathComponents}, Database
        ).Release()), IEventHandle::FlagTrackDelivery);
    }

    void RunTableRequest() {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetDatabaseName(Database);

        if (IsSystemUser) {
            request->Record.SetUserToken(NACLib::TSystemUsers::Metadata().SerializeAsString());
        }

        auto pathComponents = SplitPath(Database);
        if (!PathComponents.empty()) {
            pathComponents.insert(pathComponents.end(), PathComponents.begin(), PathComponents.end() - 1);
        }

        const auto getModifyScheme = [&](NKikimrSchemeOp::EOperationType operationType) {
            auto* modifyScheme = request->Record.MutableTransaction()->MutableModifyScheme();
            modifyScheme->SetWorkingDir(CanonizePath(pathComponents));
            LOG_DEBUG_S(*TlsActivationContext, LogService, 
                LogPrefix << "Created " << NKikimrSchemeOp::EOperationType_Name(operationType) << " transaction for path: " << modifyScheme->GetWorkingDir() << "/" << TableName());

            modifyScheme->SetOperationType(operationType);
            modifyScheme->SetInternal(true);
            modifyScheme->SetAllowAccessToPrivatePaths(true);

            return modifyScheme;
        };

        switch (OperationType) {
            case NKikimrSchemeOp::ESchemeOpCreateTable: {
                TableCreateAttempted = true;
                const bool useIndexedTable = !TableSequences.empty() || !TableIndexes.empty();
                auto& modifyScheme = *getModifyScheme(useIndexedTable
                    ? NKikimrSchemeOp::ESchemeOpCreateIndexedTable
                    : NKikimrSchemeOp::ESchemeOpCreateTable);

                if (useIndexedTable) {
                    auto& indexedTable = *modifyScheme.MutableCreateIndexedTable();
                    BuildCreateIndexedTable(indexedTable);
                } else {
                    BuildCreateTable(modifyScheme);
                }

                if (TableAclDiff) {
                    BuildModifyACL(modifyScheme);
                }

                break;
            }
            case NKikimrSchemeOp::ESchemeOpAlterTable: {
                BuildAlterTable(*getModifyScheme(NKikimrSchemeOp::ESchemeOpAlterTable));
                break;
            }
            case NKikimrSchemeOp::ESchemeOpModifyACL: {
                BuildModifyACL(*getModifyScheme(NKikimrSchemeOp::ESchemeOpModifyACL));
                break;
            }
            default: {
                LOG_CRIT_S(*TlsActivationContext, LogService, LogPrefix << "Unexpected operation type: " << NKikimrSchemeOp::EOperationType_Name(OperationType));
                Y_ABORT("Unexpected operation type");
            }
        }

        Send(MakeTxProxyID(), std::move(request));
    }

    void RunTableModification(
        const THashMap<ui32, TSysTables::TTableColumnInfo>& existingColumns,
        TIntrusivePtr<TSecurityObject> securityObject,
        const TVector<NKikimrSchemeOp::TIndexDescription>& existingIndexes,
        const TVector<NKikimrSchemeOp::TSequenceDescription>& existingSequences)
    {
        if (!TableCreateAttempted && (!TableIndexes.empty() || !TableSequences.empty())) {
            Fail("Table already exists; index and sequence upgrade is not supported");
            return;
        }

        if (!TableIndexes.empty() || !TableSequences.empty()) {
            if (!HasRequestedIndexedSchema(existingIndexes, existingSequences)) {
                Fail("Existing table schema does not match requested indexes or sequences");
                return;
            }
        }

        ExcludeExistingColumns(existingColumns);
        bool aclChanged = false;

        if (TableAclDiff && securityObject) {
            auto changedObject = *securityObject;
            changedObject.ApplyDiff(*TableAclDiff);
            aclChanged = changedObject != *securityObject || (IsSystemUser && securityObject->GetOwnerSID() != BUILTIN_ACL_METADATA);
        }

        if (Columns.empty() && !aclChanged) {
            Success();
            return;
        }

        if (!Columns.empty()) {
            OperationType = NKikimrSchemeOp::ESchemeOpAlterTable;
            PartialModification = aclChanged;
        } else {
            OperationType = NKikimrSchemeOp::ESchemeOpModifyACL;
            PartialModification = false;
        }

        Become(&TTableCreator::StateFuncUpgrade);
        RunTableRequest();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown) {
            Retry();
            return;
        }
        Fail("Scheme cache is unavailable");
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;
        Y_ABORT_UNLESS(request.ResultSet.size() == 1);
        const NSchemeCache::TSchemeCacheNavigate::TEntry& result  = request.ResultSet[0];
        if (result.Status != EStatus::Ok) {
            LOG_DEBUG_S(*TlsActivationContext, LogService,
                LogPrefix << "Describe result: " << result.Status);
        }

        switch (result.Status) {
            case EStatus::Unknown:
                [[fallthrough]];
            case EStatus::PathNotTable:
                [[fallthrough]];
            case EStatus::PathNotPath:
                [[fallthrough]];
            case EStatus::AccessDenied:
                [[fallthrough]];
            case EStatus::RedirectLookupError:
                Fail(result.Status);
                break;
            case EStatus::RootUnknown:
                [[fallthrough]];
            case EStatus::PathErrorUnknown:
                Become(&TTableCreator::StateFuncUpgrade);
                OperationType = NKikimrSchemeOp::ESchemeOpCreateTable;
                LOG_NOTICE_S(*TlsActivationContext, LogService, LogPrefix << "Creating table");
                RunTableRequest();
                break;
            case EStatus::LookupError:
                [[fallthrough]];
            case EStatus::TableCreationNotComplete:
                Retry();
                break;
            case EStatus::Ok:
                LOG_DEBUG_S(*TlsActivationContext, LogService,
                    LogPrefix << "Table already exists, number of columns: " << result.Columns.size() << ", has SecurityObject: " << (result.SecurityObject ? "true" : "false"));
                RunTableModification(result.Columns, result.SecurityObject, result.Indexes, result.Sequences);
                break;
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        LOG_DEBUG_S(*TlsActivationContext, LogService,
            LogPrefix << "TEvProposeTransactionStatus: " << ev->Get()->Record);
        const auto ssStatus = ev->Get()->Record.GetSchemeShardStatus();
        switch (ev->Get()->Status()) {
            case NTxProxy::TResultStatus::ExecComplete:
                [[fallthrough]];
            case NTxProxy::TResultStatus::ExecAlready:
                if (ssStatus == NKikimrScheme::EStatus::StatusSuccess || ssStatus == NKikimrScheme::EStatus::StatusAlreadyExists) {
                    if ((ssStatus == NKikimrScheme::EStatus::StatusAlreadyExists
                            && (!TableIndexes.empty() || !TableSequences.empty()))
                        || PartialModification)
                    {
                        FallBack();
                    } else {
                        Success(ev);
                    }
                } else {
                    Fail(ev);
                }
                break;
            case NTxProxy::TResultStatus::ProxyShardNotAvailable:
                Retry();
                break;
            case NTxProxy::TResultStatus::ExecError:
                if (ssStatus == NKikimrScheme::EStatus::StatusMultipleModifications) {
                    SubscribeOnTransactionOrFallback(ev);
                // In the process of creating a database, errors of the form may occur -
                // database doesn't have storage pools at all to create tablet
                // channels to storage pool binding by profile id
                // Also, this status is returned when column types mismatch -
                // need to fallback to rebuild column diff
                } else if (ssStatus == NKikimrScheme::EStatus::StatusInvalidParameter) {
                    FallBack(true /* long delay */);
                } else {
                    Fail(ev);
                }
                break;
            case NTxProxy::TResultStatus::ExecInProgress:
                SubscribeOnTransactionOrFallback(ev);
                break;
            default:
                Fail(ev);
        }
    }

    void Retry(bool longDelay = false) {
        auto delay = GetRetryDelay(longDelay);
        if (delay) {
            Schedule(*delay, new NActors::TEvents::TEvWakeup());
        } else {
            Fail("Retry limit exceeded");
        }
    }

    void FallBack(bool longDelay = false) {
        if (SchemePipeActorId){
            PipeClientClosedByUs = true;
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
        }
        Become(&TTableCreator::StateFuncCheck);
        Retry(longDelay);
    }

    void SubscribeOnTransactionOrFallback(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const ui64 txId = ev->Get()->Status() == NTxProxy::TResultStatus::ExecInProgress ? ev->Get()->Record.GetTxId() : ev->Get()->Record.GetPathCreateTxId();
        if (txId == 0) {
            LOG_DEBUG_S(*TlsActivationContext, LogService,
                LogPrefix << "Unable to subscribe to concurrent transaction, falling back");
            FallBack();
            return;
        }
        PipeClientClosedByUs = false;
        NActors::IActor* pipeActor = NTabletPipe::CreateClient(SelfId(), ev->Get()->Record.GetSchemeShardTabletId());
        Y_ABORT_UNLESS(pipeActor);
        SchemePipeActorId = Register(pipeActor);
        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(txId);
        NTabletPipe::SendData(SelfId(), SchemePipeActorId, std::move(request));
        LOG_DEBUG_S(*TlsActivationContext, LogService, LogPrefix << "Subscribe on create table tx: " << txId);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            LOG_ERROR_S(*TlsActivationContext, LogService,
                LogPrefix << "Request: " << GetOperationType() << ". Tablet to pipe not connected: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status) << ", retry");
            PipeClientClosedByUs = true;
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
            SchemePipeActorId = {};
            Retry();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        SchemePipeActorId = {};
        if (!PipeClientClosedByUs) {
            LOG_ERROR_S(*TlsActivationContext, LogService,
                LogPrefix << "Request: " << GetOperationType() << ". Tablet to pipe destroyed, retry");
            Retry();
        }
        PipeClientClosedByUs = false;
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev) {
        LOG_DEBUG_S(*TlsActivationContext, LogService, LogPrefix << "Subscribe on tx: " << ev->Get()->Record.GetTxId() << " registered");
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        LOG_DEBUG_S(*TlsActivationContext, LogService,
            LogPrefix << "Request: " << GetOperationType() << ". Transaction completed: " << ev->Get()->Record.GetTxId() << ". Doublechecking...");
        FallBack();
    }

    void Fail(NSchemeCache::TSchemeCacheNavigate::EStatus status) {
        TString message = TStringBuilder() << "Failed to upgrade table: " << status;
        LOG_ERROR_S(*TlsActivationContext, LogService, LogPrefix << message);
        Reply(false, message);
    }

    void Fail(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        TString message = TStringBuilder() << "Failed " << GetOperationType() << " request: " << ev->Get()->Status() << ". Response: " << ev->Get()->Record;
        LOG_ERROR_S(*TlsActivationContext, LogService, LogPrefix << message);
        Reply(false, message);
    }

    void Fail(const TString& message) {
        LOG_ERROR_S(*TlsActivationContext, LogService, LogPrefix << message);
        Reply(false, message);
    }

    void Success() {
        Reply(true);
    }

    void Success(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        LOG_INFO_S(*TlsActivationContext, LogService,
            LogPrefix << "Successful " << GetOperationType() <<  " request: " << ev->Get()->Status());
        Reply(true);
    }

    void Reply(bool success, const TString& message) {
        Reply(success, {NYql::TIssue(message)});
    }

    void Reply(bool success, NYql::TIssues issues = {}) {
        Send(Owner, new TEvTableCreator::TEvCreateTableResponse(success, std::move(issues)));
        if (SchemePipeActorId) {
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
        }
        PassAway();
    }

    const TString& TableName() const {
        return PathComponents.back();
    }

    bool HasRequestedIndexedSchema(
        const TVector<NKikimrSchemeOp::TIndexDescription>& existingIndexes,
        const TVector<NKikimrSchemeOp::TSequenceDescription>& existingSequences) const
    {
        if (TableIndexes.empty() && TableSequences.empty()) {
            return true;
        }

        for (const auto& requiredIndex : TableIndexes) {
            const auto it = std::find_if(existingIndexes.begin(), existingIndexes.end(),
                [&](const NKikimrSchemeOp::TIndexDescription& index) {
                    return index.GetName() == requiredIndex.GetName();
                });
            if (it == existingIndexes.end() || !IndexDefinitionsMatch(requiredIndex, *it)) {
                return false;
            }
        }

        for (const auto& requiredSequence : TableSequences) {
            const auto it = std::find_if(existingSequences.begin(), existingSequences.end(),
                [&](const NKikimrSchemeOp::TSequenceDescription& sequence) {
                    return sequence.GetName() == requiredSequence.GetName();
                });
            if (it == existingSequences.end() || !SequenceDefinitionsMatch(requiredSequence, *it)) {
                return false;
            }
        }

        return true;
    }

private:
    template <typename TRepeated>
    static bool RepeatedStringFieldsEqual(const TRepeated& left, const TRepeated& right)
    {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); ++i) {
            if (left.Get(i) != right.Get(i)) {
                return false;
            }
        }
        return true;
    }

    static bool IndexDefinitionsMatch(
        const NKikimrSchemeOp::TIndexDescription& required,
        const NKikimrSchemeOp::TIndexDescription& existing)
    {
        if (required.GetType() != existing.GetType()) {
            return false;
        }
        if (required.HasState() && required.GetState() != existing.GetState()) {
            return false;
        }
        if (!RepeatedStringFieldsEqual(required.GetKeyColumnNames(), existing.GetKeyColumnNames())) {
            return false;
        }
        if (!RepeatedStringFieldsEqual(required.GetDataColumnNames(), existing.GetDataColumnNames())) {
            return false;
        }
        if (required.GetSpecializedIndexDescriptionCase() != existing.GetSpecializedIndexDescriptionCase()) {
            if (required.GetSpecializedIndexDescriptionCase()
                != NKikimrSchemeOp::TIndexDescription::SPECIALIZEDINDEXDESCRIPTION_NOT_SET)
            {
                return false;
            }
        } else {
            switch (required.GetSpecializedIndexDescriptionCase()) {
                case NKikimrSchemeOp::TIndexDescription::kVectorIndexKmeansTreeDescription:
                    if (required.GetVectorIndexKmeansTreeDescription().SerializeAsString()
                        != existing.GetVectorIndexKmeansTreeDescription().SerializeAsString())
                    {
                        return false;
                    }
                    break;
                case NKikimrSchemeOp::TIndexDescription::kFulltextIndexDescription:
                    if (required.GetFulltextIndexDescription().SerializeAsString()
                        != existing.GetFulltextIndexDescription().SerializeAsString())
                    {
                        return false;
                    }
                    break;
                case NKikimrSchemeOp::TIndexDescription::kBloomFilterDescription:
                    if (required.GetBloomFilterDescription().SerializeAsString()
                        != existing.GetBloomFilterDescription().SerializeAsString())
                    {
                        return false;
                    }
                    break;
                case NKikimrSchemeOp::TIndexDescription::kBloomNGrammFilterDescription:
                    if (required.GetBloomNGrammFilterDescription().SerializeAsString()
                        != existing.GetBloomNGrammFilterDescription().SerializeAsString())
                    {
                        return false;
                    }
                    break;
                default:
                    break;
            }
        }

        if (!required.GetIndexImplTableDescriptions().empty()) {
            if (required.GetIndexImplTableDescriptions().size()
                != existing.GetIndexImplTableDescriptions().size())
            {
                return false;
            }
            for (int i = 0; i < required.GetIndexImplTableDescriptions().size(); ++i) {
                if (required.GetIndexImplTableDescriptions(i).SerializeAsString()
                    != existing.GetIndexImplTableDescriptions(i).SerializeAsString())
                {
                    return false;
                }
            }
        }

        return true;
    }

    static bool SequenceDefinitionsMatch(
        const NKikimrSchemeOp::TSequenceDescription& required,
        const NKikimrSchemeOp::TSequenceDescription& existing)
    {
#define REQUIRE_MATCHING_SEQUENCE_FIELD(field) \
        if (required.Has##field() && required.Get##field() != existing.Get##field()) { \
            return false; \
        }

        REQUIRE_MATCHING_SEQUENCE_FIELD(MinValue);
        REQUIRE_MATCHING_SEQUENCE_FIELD(MaxValue);
        REQUIRE_MATCHING_SEQUENCE_FIELD(StartValue);
        REQUIRE_MATCHING_SEQUENCE_FIELD(Cache);
        REQUIRE_MATCHING_SEQUENCE_FIELD(Increment);
        REQUIRE_MATCHING_SEQUENCE_FIELD(Cycle);
        REQUIRE_MATCHING_SEQUENCE_FIELD(DataType);
        REQUIRE_MATCHING_SEQUENCE_FIELD(Restart);

#undef REQUIRE_MATCHING_SEQUENCE_FIELD

        return true;
    }

    static NKikimrSchemeOp::TIndexCreationConfig ToIndexCreationConfig(
        const NKikimrSchemeOp::TIndexDescription& index)
    {
        NKikimrSchemeOp::TIndexCreationConfig config;
        config.SetName(index.GetName());
        config.SetType(index.GetType());
        if (index.HasState()) {
            config.SetState(index.GetState());
        }
        config.MutableKeyColumnNames()->Assign(
            index.GetKeyColumnNames().begin(), index.GetKeyColumnNames().end());
        config.MutableDataColumnNames()->Assign(
            index.GetDataColumnNames().begin(), index.GetDataColumnNames().end());
        config.MutableIndexImplTableDescriptions()->Assign(
            index.GetIndexImplTableDescriptions().begin(),
            index.GetIndexImplTableDescriptions().end());

        switch (index.GetSpecializedIndexDescriptionCase()) {
            case NKikimrSchemeOp::TIndexDescription::kVectorIndexKmeansTreeDescription:
                *config.MutableVectorIndexKmeansTreeDescription() =
                    index.GetVectorIndexKmeansTreeDescription();
                break;
            case NKikimrSchemeOp::TIndexDescription::kFulltextIndexDescription:
                *config.MutableFulltextIndexDescription() = index.GetFulltextIndexDescription();
                break;
            case NKikimrSchemeOp::TIndexDescription::kBloomFilterDescription:
                *config.MutableBloomFilterDescription() = index.GetBloomFilterDescription();
                break;
            case NKikimrSchemeOp::TIndexDescription::kBloomNGrammFilterDescription:
                *config.MutableBloomNGrammFilterDescription() =
                    index.GetBloomNGrammFilterDescription();
                break;
            default:
                break;
        }

        return config;
    }

    void BuildCreateIndexedTable(NKikimrSchemeOp::TIndexedTableCreationConfig& indexedTable) const {
        auto& tableDesc = *indexedTable.MutableTableDescription();
        BuildTableOperation(tableDesc, false);
        tableDesc.MutableKeyColumnNames()->Assign(KeyColumns.begin(), KeyColumns.end());

        for (const auto& index : TableIndexes) {
            *indexedTable.AddIndexDescription() = ToIndexCreationConfig(index);
        }

        for (const auto& sequence : TableSequences) {
            *indexedTable.AddSequenceDescription() = sequence;
        }
    }

    void BuildTableOperation(NKikimrSchemeOp::TTableDescription& tableDesc, bool includeTableIndexes = true) const {
        tableDesc.SetName(TableName());
        tableDesc.MutableColumns()->Assign(Columns.begin(), Columns.end());

        if (TtlSettings) {
            *tableDesc.MutableTTLSettings() = *TtlSettings;
        }

        if (PartitioningPolicy) {
            *tableDesc.MutablePartitionConfig()->MutablePartitioningPolicy() = *PartitioningPolicy;
        }

        if (includeTableIndexes && !TableIndexes.empty()) {
            tableDesc.MutableTableIndexes()->Assign(TableIndexes.begin(), TableIndexes.end());
        }
    }

    void BuildCreateTable(NKikimrSchemeOp::TModifyScheme& modifyScheme) const {
        auto& tableDesc = *modifyScheme.MutableCreateTable();
        tableDesc.MutableKeyColumnNames()->Assign(KeyColumns.begin(), KeyColumns.end());

        BuildTableOperation(tableDesc);
    }

    void BuildAlterTable(NKikimrSchemeOp::TModifyScheme& modifyScheme) const {
        BuildTableOperation(*modifyScheme.MutableAlterTable(), false);
    }

    void BuildModifyACL(NKikimrSchemeOp::TModifyScheme& modifyScheme) const {
        Y_ABORT_UNLESS(TableAclDiff);
        auto& acl = *modifyScheme.MutableModifyACL();
        acl.SetName(TableName());
        acl.SetDiffACL(TableAclDiff->SerializeAsString());

        if (IsSystemUser) {
            acl.SetNewOwner(BUILTIN_ACL_METADATA);
        }
    }

    void ExcludeExistingColumns(const THashMap<ui32, TSysTables::TTableColumnInfo>& existingColumns) {
        THashSet<TString> existingNames;
        TStringBuilder columns;
        for (const auto& [_, colInfo] : existingColumns) {
            existingNames.insert(colInfo.Name);
            if (columns) {
                columns << ", ";
            }
            columns << colInfo.Name;
        }

        TVector<NKikimrSchemeOp::TColumnDescription> filteredColumns;
        TStringBuilder filtered;
        for (auto& col : Columns) {
            if (!existingNames.contains(col.GetName())) {
                if (filtered) {
                    filtered << ", ";
                }
                filtered << col.GetName();
                filteredColumns.emplace_back(std::move(col));
            }
        }
        if (filteredColumns.empty()) {
            LOG_DEBUG_S(*TlsActivationContext, LogService,
                LogPrefix << "Column diff is empty, finishing");
        } else {
            LOG_NOTICE_S(*TlsActivationContext, LogService,
                LogPrefix << "Adding columns. New columns: " << filtered << ". Existing columns: " << columns);
        }


        Columns = std::move(filteredColumns);
    }

    TStringBuf GetOperationType() const {
        return OperationType == NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable ? "create" : "alter";
    }

    TMaybe<TDuration> GetRetryDelay(bool longDelay = false) {
        if (!RetryState) {
            RetryState = CreateRetryState();
        }
        return RetryState->GetNextRetryDelay(longDelay);
    }

    static TTableCreatorRetryPolicy::IRetryState::TPtr CreateRetryState() {
        return TTableCreatorRetryPolicy::GetExponentialBackoffPolicy(
                  [](bool longDelay){return longDelay ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;}
                , TDuration::MilliSeconds(100)
                , TDuration::MilliSeconds(300)
                , TDuration::Seconds(1)
                , std::numeric_limits<size_t>::max()
                , TDuration::Seconds(10)
            )->CreateRetryState();
    }

    const TVector<TString> PathComponents;
    TVector<NKikimrSchemeOp::TColumnDescription> Columns;
    const TVector<TString> KeyColumns;
    NKikimrServices::EServiceKikimr LogService;
    const TMaybe<NKikimrSchemeOp::TTTLSettings> TtlSettings;
    TString Database;
    bool IsSystemUser = false;
    const TMaybe<NKikimrSchemeOp::TPartitioningPolicy> PartitioningPolicy;
    const TMaybe<NACLib::TDiffACL> TableAclDiff;
    const TVector<NKikimrSchemeOp::TIndexDescription> TableIndexes;
    const TVector<NKikimrSchemeOp::TSequenceDescription> TableSequences;
    NKikimrSchemeOp::EOperationType OperationType = NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable;
    bool TableCreateAttempted = false;
    bool PartialModification = false;
    NActors::TActorId Owner;
    NActors::TActorId SchemePipeActorId;
    bool PipeClientClosedByUs = false;
    TString LogPrefix;
    TTableCreatorRetryPolicy::IRetryState::TPtr RetryState;
};

} // namespace

namespace NTableCreator {

THolder<NSchemeCache::TSchemeCacheNavigate> BuildSchemeCacheNavigateRequest(const TVector<TVector<TString>>& pathsComponents, const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken) {
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    auto databasePath = SplitPath(database);
    request->DatabaseName = database;
    if (userToken && !userToken->GetSerializedToken().empty()) {
        request->UserToken = userToken;
    }

    for (const auto& pathComponents : pathsComponents) {
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        entry.Path = databasePath;
        entry.Path.insert(entry.Path.end(), pathComponents.begin(), pathComponents.end());
    }

    return request;
}

THolder<NSchemeCache::TSchemeCacheNavigate> BuildSchemeCacheNavigateRequest(
    const TVector<TVector<TString>>& pathsComponents, const TString& database)
{
    return BuildSchemeCacheNavigateRequest(pathsComponents, database ? database : AppData()->TenantName, nullptr);
}

NKikimrSchemeOp::TColumnDescription TMultiTableCreator::Col(const TString& columnName, const char* columnType) {
    NKikimrSchemeOp::TColumnDescription desc;
    desc.SetName(columnName);
    desc.SetType(columnType);
    return desc;
}

NKikimrSchemeOp::TColumnDescription TMultiTableCreator::Col(const TString& columnName, NScheme::TTypeId columnType) {
    return Col(columnName, NScheme::TypeName(columnType));
}

NKikimrSchemeOp::TTTLSettings TMultiTableCreator::TtlCol(const TString& columnName, TDuration expireAfter, TDuration runInterval) {
    NKikimrSchemeOp::TTTLSettings settings;
    settings.MutableEnabled()->SetExpireAfterSeconds(expireAfter.Seconds());
    settings.MutableEnabled()->SetColumnName(columnName);
    settings.MutableEnabled()->MutableSysSettings()->SetRunInterval(runInterval.MicroSeconds());
    return settings;
}

NKikimrSchemeOp::TPartitioningPolicy TMultiTableCreator::AutoPartitioningByLoadPolicy() {
    NKikimrSchemeOp::TPartitioningPolicy policy;
    policy.MutableSplitByLoadSettings()->SetEnabled(true);
    return policy;
}

TMultiTableCreator::TMultiTableCreator(std::vector<NActors::IActor*> tableCreators)
    : TableCreators(std::move(tableCreators))
{}

void TMultiTableCreator::Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) {
    TBase::Registered(sys, owner);
    Owner = owner;
}

void TMultiTableCreator::Bootstrap() {
    Become(&TMultiTableCreator::StateFunc);

    TablesCreating = TableCreators.size();
    for (const auto creator : TableCreators) {
        Register(creator);
    }
}

void TMultiTableCreator::Handle(TEvTableCreator::TEvCreateTableResponse::TPtr& ev) {
    if (!ev->Get()->Success) {
        Success = false;
        Issues.AddIssues(std::move(ev->Get()->Issues));
    }

    Y_ABORT_UNLESS(TablesCreating > 0);
    if (--TablesCreating == 0) {
        OnTablesCreated(Success, std::move(Issues));
        PassAway();
    }
}

STRICT_STFUNC(TMultiTableCreator::StateFunc,
    hFunc(TEvTableCreator::TEvCreateTableResponse, Handle);
);

} // namespace NTableCreator

NActors::IActor* CreateTableCreator(
    TVector<TString> pathComponents,
    TVector<NKikimrSchemeOp::TColumnDescription> columns,
    TVector<TString> keyColumns,
    NKikimrServices::EServiceKikimr logService,
    TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings,
    const TString& database,
    bool isSystemUser,
    TMaybe<NKikimrSchemeOp::TPartitioningPolicy> partitioningPolicy,
    TMaybe<NACLib::TDiffACL> tableAclDiff,
    TVector<NKikimrSchemeOp::TIndexDescription> tableIndexes,
    TVector<NKikimrSchemeOp::TSequenceDescription> tableSequences)
{
    return new TTableCreator(std::move(pathComponents), std::move(columns),
        std::move(keyColumns), logService, std::move(ttlSettings), database,
        isSystemUser, std::move(partitioningPolicy), std::move(tableAclDiff),
        std::move(tableIndexes), std::move(tableSequences));
}

} // namespace NKikimr
