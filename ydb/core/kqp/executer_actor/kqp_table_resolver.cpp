#include "kqp_table_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NKqp {

using namespace NActors;
using namespace NYql;
using namespace NYql::NDq;

namespace {

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)

class TKqpTableResolver : public TActorBootstrapped<TKqpTableResolver> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_TABLE_RESOLVER;
    }

    TKqpTableResolver(const TActorId& owner, ui64 txId,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        TKqpTasksGraph& tasksGraph, bool skipUnresolvedNames)
        : Owner(owner)
        , TxId(txId)
        , UserToken(userToken)
        , SkipUnresolvedNames(skipUnresolvedNames)
        , TasksGraph(tasksGraph) {}

    void Bootstrap() {
        ResolveKeys();
    }

private:

    STATEFN(ResolveNamesState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolveNames);
            hFunc(TEvents::TEvPoison, HandleResolveNames);
            default: {
                LOG_C("ResolveKeysState: unexpected event " << ev->GetTypeRewrite());
                GotUnexpectedEvent = ev->GetTypeRewrite();
            }
        }
    }

    STATEFN(ResolveKeysState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolveKeys);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolveKeys);
            hFunc(TEvents::TEvPoison, HandleResolveKeys);
            default: {
                LOG_C("ResolveKeysState: unexpected event " << ev->GetTypeRewrite());
                GotUnexpectedEvent = ev->GetTypeRewrite();
            }
        }
    }

    void HandleResolveNames(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ShouldTerminate) {
            PassAway();
            return;
        }
        auto& results = ev->Get()->Request->ResultSet;
        if (results.size() != TableRequestPathes.size()) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TIssue(TStringBuilder() << "navigation problems for tables"));
            return;
        }
        LOG_D("Navigated key sets: " << results.size());
        for (auto& entry : results) {
            if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                        << "Failed to resolve table with tableId: " << entry.TableId << " status: " << entry.Status << '.'));
                return;
            }

            auto iterTableRequestPathes = TableRequestPathes.find(CanonizePath(entry.Path));
            if (iterTableRequestPathes == TableRequestPathes.end()) {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                        << "Incorrect table path in reply `" << CanonizePath(entry.Path) << "`."));
                return;
            }

            AFL_ENSURE(entry.RequestType == NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath);
            if (iterTableRequestPathes != TableRequestPathes.end()) {
                TVector<TStageId> stageIds(std::move(iterTableRequestPathes->second));
                const bool isOlap = (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable);

                for (auto stageId : stageIds) {
                    auto& stageMeta = TasksGraph.GetStageInfo(stageId).Meta;
                    stageMeta.TableId = entry.TableId;
                    if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindTable) {
                        stageMeta.TableKind = ETableKind::Datashard;
                    } else {
                        AFL_ENSURE(isOlap);
                        stageMeta.TableKind = ETableKind::Olap;
                    }

                    const auto& stage = stageMeta.GetStage(stageId);
                    AFL_ENSURE(stage.GetSinks().size() == 1);
                    const auto& sink = stage.GetSinks(0);

                    AFL_ENSURE(sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>());
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    AFL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings));
                    AFL_ENSURE(settings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_FILL);
                    settings.MutableTable()->SetOwnerId(entry.TableId.PathId.OwnerId);
                    settings.MutableTable()->SetTableId(entry.TableId.PathId.LocalPathId);
                    settings.MutableTable()->SetSysView(entry.TableId.SysViewInfo);
                    settings.MutableTable()->SetVersion(entry.TableId.SchemaVersion);

                    settings.SetIsOlap(isOlap);

                    auto fillColumnProto = [] (const NKikimr::TSysTables::TTableColumnInfo& columnInfo, NKikimrKqp::TKqpColumnMetadataProto* columnProto ) {
                        columnProto->SetId(columnInfo.Id);
                        columnProto->SetName(columnInfo.Name);
                        columnProto->SetTypeId(columnInfo.PType.GetTypeId());

                        if (NScheme::NTypeIds::IsParametrizedType(columnInfo.PType.GetTypeId())) {
                            ProtoFromTypeInfo(columnInfo.PType, columnInfo.PTypeMod, *columnProto->MutableTypeInfo());
                        }
                    };

                    THashMap<TString, ui32> columnNameToIndex;
                    TMap<ui32, ui32> keyPositionToIndex;
                    TMap<ui32, ui32> columnIdToIndex;
                    TVector<NScheme::TTypeInfo> keyTypes;

                    // CTAS writes all columns
                    AFL_ENSURE(static_cast<size_t>(settings.GetInputColumns().size()) == entry.Columns.size());

                    for (const auto& [index, columnInfo] : entry.Columns) {
                        columnNameToIndex[columnInfo.Name] = index;
                        columnIdToIndex[columnInfo.Id] = index;
                        if (columnInfo.KeyOrder != -1) {
                            AFL_ENSURE(columnInfo.KeyOrder >= 0);
                            keyPositionToIndex[columnInfo.KeyOrder] = index;
                        }
                    }

                    keyTypes.reserve(keyPositionToIndex.size());
                    for (const auto& [_, index] : keyPositionToIndex) {
                        const auto columnInfo = entry.Columns.FindPtr(index);
                        AFL_ENSURE(columnInfo);

                        auto keyColumnProto = settings.AddKeyColumns();
                        fillColumnProto(*columnInfo, keyColumnProto);

                        keyTypes.push_back(columnInfo->PType);
                    }
                    AFL_ENSURE(!keyPositionToIndex.empty());

                    stageMeta.ShardKey = ExtractKey(
                        stageMeta.TableId,
                        keyTypes,
                        TKeyDesc::ERowOperation::Update); // CTAS is Update operation

                    for (const auto& columnName : settings.GetInputColumns()) {
                        const auto index = columnNameToIndex.FindPtr(columnName);
                        AFL_ENSURE(index);
                        const auto columnInfo = entry.Columns.FindPtr(*index);
                        AFL_ENSURE(columnInfo);

                        auto columnProto = settings.AddColumns();
                        fillColumnProto(*columnInfo, columnProto);
                    }

                    {
                        THashMap<TStringBuf, ui32> columnToOrder;
                        ui32 currentIndex = 0;
                        if (!isOlap) {
                            for (const auto& [_, index] : keyPositionToIndex) {
                                const auto columnInfo = entry.Columns.FindPtr(index);
                                AFL_ENSURE(columnInfo);
                                columnToOrder[columnInfo->Name] = currentIndex++;
                            }
                        }
                        for (const auto& [id, index] : columnIdToIndex) {
                            const auto columnInfo = entry.Columns.FindPtr(index);
                            AFL_ENSURE(columnInfo);
                            AFL_ENSURE(columnInfo->Id == id);
                            if (isOlap || columnInfo->KeyOrder == -1) {
                                columnToOrder[columnInfo->Name] = currentIndex++;
                            } else {
                                AFL_ENSURE(columnToOrder.contains(columnInfo->Name));
                            }
                        }

                        for (const auto& columnName : settings.GetInputColumns()) {
                            settings.AddWriteIndexes(columnToOrder.at(columnName));
                        }
                    }

                    AFL_ENSURE(settings.GetColumns().size() == settings.GetWriteIndexes().size());

                    stageMeta.ResolvedSinkSettings = settings;
                }
            }
        }

        ResolvingNamesFinished = true;
        ResolveKeys();
    }

    void HandleResolveKeys(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        AFL_ENSURE(ResolvingNamesFinished);
        if (ShouldTerminate) {
            PassAway();
            return;
        }
        auto& results = ev->Get()->Request->ResultSet;
        if (results.size() != TableRequestIds.size()) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TIssue(TStringBuilder() << "navigation problems for tables"));
            return;
        }
        LOG_D("Navigated key sets: " << results.size());
        for (auto& entry : results) {
            if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                        << "Failed to resolve table with tableId: " << entry.TableId << " status: " << entry.Status << '.'));
                return;
            }

            auto iterTableRequestIds = TableRequestIds.find(entry.TableId);
            if (iterTableRequestIds == TableRequestIds.end()) {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                        << "Incorrect tableId in reply " << entry.TableId << '.'));
                return;
            }

            TVector<TStageId> stageIds(std::move(iterTableRequestIds->second));
            TableRequestIds.erase(entry.TableId);

            for (auto stageId : stageIds) {
                TasksGraph.GetStageInfo(stageId).Meta.ColumnTableInfoPtr = entry.ColumnTableInfo;
            }
        }

        NavigationFinished = true;
        TryFinish();
    }

    void HandleResolveKeys(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr &ev) {
        AFL_ENSURE(ResolvingNamesFinished);
        if (ShouldTerminate) {
            PassAway();
            return;
        }

        if (GotUnexpectedEvent) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TIssue(TStringBuilder()
                << "Unexpected event: " << *GotUnexpectedEvent));
            return;
        }

        auto timer = std::make_unique<NCpuTime::TCpuTimer>(CpuTime);

        auto& results = ev->Get()->Request->ResultSet;
        LOG_D("Resolved key sets: " << results.size());

        for (auto& entry : results) {
            if (entry.Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
                LOG_E("Error resolving keys for entry: " << entry.ToString(*AppData()->TypeRegistry));

                TStringBuilder path;
                if (auto it = TablePathsById.find(entry.KeyDescription->TableId); it != TablePathsById.end()) {
                    path << '`' << it->second << '`';
                } else {
                    path << "with unknown path, tableId: `" << entry.KeyDescription->TableId << '`';
                }

                timer.reset();
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                        << "Failed to resolve table " << path << " status: " << entry.Status << '.'));
                return;
            }

            for (auto& partition : entry.KeyDescription->GetPartitions()) {
                AFL_ENSURE(partition.Range);
            }

            LOG_D("Resolved key: " << entry.ToString(*AppData()->TypeRegistry));

            auto& stageInfo = DecodeStageInfo(entry.UserData);

            if (stageInfo.Meta.TableId == entry.KeyDescription->TableId) {
                stageInfo.Meta.ShardKey = std::move(entry.KeyDescription);
                stageInfo.Meta.ShardKind = std::move(entry.Kind);
            } else {
                for (auto& indexMeta : stageInfo.Meta.IndexMetas) {
                    if (indexMeta.TableId == entry.KeyDescription->TableId) {
                        indexMeta.ShardKey = std::move(entry.KeyDescription);
                        break;
                    }
                }
            }
        }

        timer.reset();
        ResolvingFinished = true;
        TryFinish();
    }

    void HandleResolveKeys(TEvents::TEvPoison::TPtr&) {
        ShouldTerminate = true;
    }

    void HandleResolveNames(TEvents::TEvPoison::TPtr&) {
        ShouldTerminate = true;
    }

private:
    void ResolveKeys() {
        auto requestNavigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        const auto& databaseName = TasksGraph.GetMeta().Database;
        requestNavigate->DatabaseName = databaseName;
        request->DatabaseName = databaseName;
        request->ResultSet.reserve(TasksGraph.GetStagesInfo().size());
        if (UserToken && !UserToken->GetSerializedToken().empty()) {
            request->UserToken = UserToken;
        }

        bool needToResolveNames = false;
        if (!ResolvingNamesFinished) {
            for (const auto& [_, stageInfo] : TasksGraph.GetStagesInfo()) {
                if (!stageInfo.Meta.ShardOperations.empty()) {
                    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
                    if (!tableInfo && !SkipUnresolvedNames) {
                        AFL_ENSURE(!stageInfo.Meta.TableId);
                        AFL_ENSURE(stageInfo.Meta.TablePath);
                        needToResolveNames = true;
                    }
                }
            }
            ResolvingNamesFinished = !needToResolveNames;
        }

        for (auto& pair : TasksGraph.GetStagesInfo()) {
            auto& stageInfo = pair.second;

            if (!stageInfo.Meta.ShardOperations.empty()) {
                for (const auto& operation : stageInfo.Meta.ShardOperations) {
                    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
                    if (tableInfo) {
                        if (ResolvingNamesFinished) {
                            AFL_ENSURE(stageInfo.Meta.TableId);
                            TablePathsById.emplace(stageInfo.Meta.TableId, tableInfo->Path);
                            stageInfo.Meta.TableKind = tableInfo->TableKind;

                            stageInfo.Meta.ShardKey = ExtractKey(stageInfo.Meta.TableId, stageInfo.Meta.TableConstInfo->KeyColumnTypes, operation);

                            if (stageInfo.Meta.TableConstInfo->SysViewInfo && !stageInfo.Meta.TableConstInfo->SysViewInfo->HasSourceObject()) {
                                continue;
                            }

                            if (stageInfo.Meta.TableKind == ETableKind::Olap) {
                                if (TableRequestIds.find(stageInfo.Meta.TableId) == TableRequestIds.end()) {
                                    auto& entry = requestNavigate->ResultSet.emplace_back();
                                    entry.TableId = stageInfo.Meta.TableId;
                                    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
                                    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable;
                                }

                                TableRequestIds[stageInfo.Meta.TableId].emplace_back(pair.first);
                            }

                            auto addRequest = [&](auto&& shardKey) {
                                auto& entry = request->ResultSet.emplace_back(std::move(shardKey));
                                entry.UserData = EncodeStageInfo(stageInfo);
                                switch (operation) {
                                    case TKeyDesc::ERowOperation::Read:
                                        entry.Access = NACLib::EAccessRights::SelectRow;
                                        break;
                                    case TKeyDesc::ERowOperation::Update:
                                        entry.Access = NACLib::EAccessRights::UpdateRow;
                                        break;
                                    case TKeyDesc::ERowOperation::Erase:
                                        entry.Access = NACLib::EAccessRights::EraseRow;
                                        break;
                                    default:
                                        YQL_ENSURE(false, "Unsupported row operation mode: " << (ui32)operation);
                                }
                            };

                            addRequest(stageInfo.Meta.ShardKey);
                            switch (operation) {
                                case TKeyDesc::ERowOperation::Update:
                                case TKeyDesc::ERowOperation::Erase:
                                    for (auto& indexMeta : stageInfo.Meta.IndexMetas) {
                                        indexMeta.ShardKey = ExtractKey(indexMeta.TableId, indexMeta.TableConstInfo->KeyColumnTypes, operation);
                                        addRequest(indexMeta.ShardKey);
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                    } else if (!ResolvingNamesFinished) {
                        // CTAS
                        AFL_ENSURE(!stageInfo.Meta.TableId);
                        AFL_ENSURE(stageInfo.Meta.TablePath);
                        const auto splittedPath = SplitPath(stageInfo.Meta.TablePath);
                        const auto canonizedPath = CanonizePath(splittedPath);
                        if (TableRequestPathes.find(canonizedPath) == TableRequestPathes.end()) {
                            auto& entry = requestNavigate->ResultSet.emplace_back();
                            entry.Path = std::move(splittedPath);
                            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
                            entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable;
                        }

                        TableRequestPathes[canonizedPath].emplace_back(pair.first);
                        if (requestNavigate->DatabaseName.empty()) {
                            requestNavigate->DatabaseName = TasksGraph.GetMeta().Database;
                        }
                    } else if (!SkipUnresolvedNames) {
                        // CTAS
                        AFL_ENSURE(stageInfo.Meta.TableId);
                        AFL_ENSURE(stageInfo.Meta.TablePath);

                        TablePathsById.emplace(stageInfo.Meta.TableId, stageInfo.Meta.TablePath);

                        auto& entry = request->ResultSet.emplace_back(std::move(stageInfo.Meta.ShardKey));
                        entry.UserData = EncodeStageInfo(stageInfo);
                        AFL_ENSURE(operation == TKeyDesc::ERowOperation::Update); // CTAS is Update operation
                        entry.Access = NACLib::EAccessRights::UpdateRow;
                    }
                }
            }
        }

        if (!ResolvingNamesFinished) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(requestNavigate.release()));
            Become(&TKqpTableResolver::ResolveNamesState);
            return;
        }

        if (requestNavigate->ResultSet.size()) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(requestNavigate.release()));
        } else {
            NavigationFinished = true;
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        Become(&TKqpTableResolver::ResolveKeysState);
    }

private:
    THolder<TKeyDesc> ExtractKey(const TTableId& table, const TVector<NScheme::TTypeInfo>& keyTypes, TKeyDesc::ERowOperation operation) {
        auto range = GetFullRange(keyTypes.size());
        return MakeHolder<TKeyDesc>(table, range.ToTableRange(), operation, keyTypes, TVector<TKeyDesc::TColumnOp>{});
    }

    static TSerializedTableRange GetFullRange(ui32 columnsCount) {
        TVector<TCell> fromValues(columnsCount);
        TVector<TCell> toValues;

        return TSerializedTableRange(fromValues, /* inclusiveFrom */ true, toValues, /* inclusiveTo */ false);
    }

    static uintptr_t EncodeStageInfo(TKqpTasksGraph::TStageInfoType& stageInfo) {
        return reinterpret_cast<uintptr_t>(&stageInfo);
    }

    static TKqpTasksGraph::TStageInfoType& DecodeStageInfo(uintptr_t userData) {
        return *reinterpret_cast<TKqpTasksGraph::TStageInfoType*>(userData);
    }

private:
    void UnexpectedEvent(const TString& state, ui32 eventType) {
        LOG_C("TKqpTableResolver, unexpected event: " << eventType << ", at state:" << state << ", self: " << SelfId());
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::UNEXPECTED, "Internal error while executing transaction.");
        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, std::move(issue));
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, TIssue&& issue) {
        auto replyEv = std::make_unique<TEvKqpExecuter::TEvTableResolveStatus>();
        replyEv->Status = status;
        replyEv->Issues.AddIssue(std::move(issue));
        replyEv->CpuTime = CpuTime;
        Send(Owner, replyEv.release());
        PassAway();
    }

    void TryFinish() {
        if (!NavigationFinished || !ResolvingFinished) {
            return;
        }
        auto replyEv = std::make_unique<TEvKqpExecuter::TEvTableResolveStatus>();
        replyEv->CpuTime = CpuTime;

        Send(Owner, replyEv.release());
        PassAway();
    }

private:
    const TActorId Owner;
    const ui64 TxId;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    THashMap<TTableId, TVector<TStageId>> TableRequestIds;
    THashMap<TString, TVector<TStageId>> TableRequestPathes;
    THashMap<TTableId, TString> TablePathsById;
    const bool SkipUnresolvedNames;
    bool ResolvingNamesFinished = false;
    bool NavigationFinished = false;
    bool ResolvingFinished = false;

    // TODO: TableResolver should not populate TasksGraph as it's not related to its job (bad API).
    TKqpTasksGraph& TasksGraph;

    bool ShouldTerminate = false;
    TMaybe<ui32> GotUnexpectedEvent;
    TDuration CpuTime;
};

} // anonymous namespace

NActors::IActor* CreateKqpTableResolver(const TActorId& owner, ui64 txId,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpTasksGraph& tasksGraph, bool skipUnknownNames) {
    return new TKqpTableResolver(owner, txId, userToken, tasksGraph, skipUnknownNames);
}

} // namespace NKikimr::NKqp
