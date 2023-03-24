#include "kqp_metadata_loader.h"
#include "kqp_ic_gateway_actors.h"

#include <ydb/core/base/path.h>

#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>


namespace NKikimr::NKqp {

namespace {

using TNavigate = NSchemeCache::TSchemeCacheNavigate;
using TTableMetadataResult = NYql::IKikimrGateway::TTableMetadataResult;
using TLoadTableMetadataSettings = NYql::IKikimrGateway::TLoadTableMetadataSettings;
using TGenericResult = NYql::IKikimrGateway::TGenericResult;
using namespace NYql::NCommon;
using namespace NThreading;
using TIssuesIds = NYql::TIssuesIds;


std::pair<TNavigate::TEntry, TString> CreateNavigateEntry(const TString& path, const NYql::IKikimrGateway::TLoadTableMetadataSettings& settings) {
    TNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable;
    entry.SyncVersion = true;
    entry.ShowPrivatePath = settings.WithPrivateTables_;
    return {entry, path};
}

std::pair<TNavigate::TEntry, TString> CreateNavigateEntry(const std::pair<TIndexId, TString>& pair, const NYql::IKikimrGateway::TLoadTableMetadataSettings& settings) {
    TNavigate::TEntry entry;

    // TODO: Right now scheme cache use TTableId for index
    // scheme cache api should be changed to use TIndexId to navigate index
    entry.TableId = TTableId(pair.first.PathId.OwnerId, pair.first.PathId.LocalPathId, pair.first.SchemaVersion);

    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpList;
    entry.SyncVersion = true;
    entry.ShowPrivatePath = settings.WithPrivateTables_;
    return {entry, pair.second};
}

ui64 GetExpectedVersion(const std::pair<TIndexId, TString>& pathId) {
    return pathId.first.SchemaVersion;
}

ui64 GetExpectedVersion(const TString&) {
    return 0;
}

template<typename TRequest, typename TResponse, typename TResult>
TFuture<TResult> SendActorRequest(TActorSystem* actorSystem, const TActorId& actorId, TRequest* request,
    typename TActorRequestHandler<TRequest, TResponse, TResult>::TCallbackFunc callback)
{
    auto promise = NewPromise<TResult>();
    IActor* requestHandler = new TActorRequestHandler<TRequest, TResponse, TResult>(actorId, request, promise, callback);
    actorSystem->Register(requestHandler, TMailboxType::HTSwap, actorSystem->AppData<TAppData>()->UserPoolId);
    return promise.GetFuture();
}


template<typename TIndexProto>
void IndexProtoToMetadata(const TIndexProto& indexes, NYql::TKikimrTableMetadataPtr tableMeta) {
    for (const NKikimrSchemeOp::TIndexDescription& index : indexes) {
        const auto indexState = index.GetState();

        YQL_ENSURE(indexState != NKikimrSchemeOp::EIndexState::EIndexStateInvalid,
            "Unexpected index state, probably SchemeShard/SchemeCache bug!");

        // Skip index if the state is NotReady - index just has been created but mark as not
        // ready to use.
        if (indexState == NKikimrSchemeOp::EIndexState::EIndexStateNotReady) {
            continue;
        }

        tableMeta->Indexes.emplace_back(NYql::TIndexDescription(index));
    }
}


TTableMetadataResult GetLoadTableMetadataResult(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry,
        const TString& cluster, const TString& tableName) {
    using TResult = NYql::IKikimrGateway::TTableMetadataResult;
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
    using EKind = NSchemeCache::TSchemeCacheNavigate::EKind;

    auto message = ToString(entry.Status);

    switch (entry.Status) {
        case EStatus::Ok:
            break;
        case EStatus::PathErrorUnknown:
        case EStatus::RootUnknown: {
            TTableMetadataResult result;
            result.SetSuccess();
            result.Metadata = new NYql::TKikimrTableMetadata(cluster, tableName);
            return result;
        }
        case EStatus::PathNotTable:
        case EStatus::TableCreationNotComplete:
            return ResultFromError<TResult>(YqlIssue({}, TIssuesIds::KIKIMR_SCHEME_ERROR, message));
        case EStatus::LookupError:
        case EStatus::RedirectLookupError:
            return ResultFromError<TResult>(YqlIssue({}, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, message));
        default:
            return ResultFromError<TResult>(ToString(entry.Status));
    }

    YQL_ENSURE(entry.Kind == EKind::KindTable || entry.Kind == EKind::KindColumnTable);

    TTableMetadataResult result;
    result.SetSuccess();
    result.Metadata = new NYql::TKikimrTableMetadata(cluster, tableName);
    auto tableMeta = result.Metadata;
    tableMeta->DoesExist = true;
    tableMeta->PathId = NYql::TKikimrPathId(entry.TableId.PathId.OwnerId, entry.TableId.PathId.LocalPathId);
    tableMeta->SysView = entry.TableId.SysViewInfo;
    tableMeta->SchemaVersion = entry.TableId.SchemaVersion;

    if (!tableMeta->SysView.empty()) {
        if (entry.Kind == EKind::KindColumnTable) {
            // NOTE: OLAP sys views for stats are themselves represented by OLAP tables
            tableMeta->Kind = NYql::EKikimrTableKind::Olap;
        } else {
            tableMeta->Kind = NYql::EKikimrTableKind::SysView;
        }
    } else {
        switch (entry.Kind) {
            case EKind::KindTable:
                tableMeta->Kind = NYql::EKikimrTableKind::Datashard;
                break;

            case EKind::KindColumnTable:
                tableMeta->Kind = NYql::EKikimrTableKind::Olap;
                break;

            default:
                YQL_ENSURE(false, "Unexpected entry kind: " << (ui32)entry.Kind);
                break;
        }
    }

    tableMeta->Attributes = entry.Attributes;

    std::map<ui32, TString, std::less<ui32>> keyColumns;
    std::map<ui32, TString, std::less<ui32>> columnOrder;
    for (auto& pair : entry.Columns) {
        const auto& columnDesc = pair.second;
        TString typeName;
        auto notNull = entry.NotNullColumns.contains(columnDesc.Name);
        if (columnDesc.PType.GetTypeId() != NScheme::NTypeIds::Pg) {
            YQL_ENSURE(NScheme::TryGetTypeName(columnDesc.PType.GetTypeId(), typeName));
        } else {
            Y_VERIFY(columnDesc.PType.GetTypeDesc(), "no pg type descriptor");
            Y_VERIFY(!notNull, "pg not null types are not allowed");
            typeName = NPg::PgTypeNameFromTypeDesc(columnDesc.PType.GetTypeDesc(), columnDesc.PTypeMod);
        }
        tableMeta->Columns.emplace(
            columnDesc.Name,
            NYql::TKikimrColumnMetadata(
                columnDesc.Name, columnDesc.Id, typeName, notNull, columnDesc.PType, columnDesc.PTypeMod
            )
        );
        if (columnDesc.KeyOrder >= 0) {
            keyColumns[columnDesc.KeyOrder] = columnDesc.Name;
        }
        columnOrder[columnDesc.Id] = columnDesc.Name;
    }

    tableMeta->KeyColumnNames.reserve(keyColumns.size());
    for (const auto& pair : keyColumns) {
        tableMeta->KeyColumnNames.push_back(pair.second);
    }

    tableMeta->ColumnOrder.reserve(columnOrder.size());
    for (const auto& [_, column] : columnOrder) {
        tableMeta->ColumnOrder.push_back(column);
    }

    IndexProtoToMetadata(entry.Indexes, tableMeta);

    return result;
}

TString GetDebugString(const TString& id) {
    return TStringBuilder() << " Path: " << id;
}

TString GetDebugString(const std::pair<NKikimr::TIndexId, TString>& id) {
    return TStringBuilder() << " Path: " << id.second  << " TableId: " << id.first;
}

void UpdateMetadataIfSuccess(NYql::TKikimrTableMetadataPtr ptr, size_t idx, const TTableMetadataResult& value) {
    if (value.Success()) {
        ptr->SecondaryGlobalIndexMetadata[idx] = value.Metadata;
    }

}

} // anonymous namespace


TVector<TString> TKqpTableMetadataLoader::GetCollectedSchemeData() {
    TVector<TString> result(std::move(CollectedSchemeData));
    CollectedSchemeData = TVector<TString>();
    return result;
}


void TKqpTableMetadataLoader::OnLoadedTableMetadata(TTableMetadataResult& loadTableMetadataResult) {
    if (!NeedCollectSchemeData) return;
    TString data = loadTableMetadataResult.Metadata->SerializeToString();
    with_lock(Lock) {
        CollectedSchemeData.emplace_back(data);
    }
}


NThreading::TFuture<TTableMetadataResult> TKqpTableMetadataLoader::LoadTableMetadata(const TString& cluster, const TString& table,
    const NYql::IKikimrGateway::TLoadTableMetadataSettings& settings, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken)
{
    using TResult = TTableMetadataResult;

    auto ptr = weak_from_base();
    try {
        auto tableMetaFuture = LoadTableMetadataCache(cluster, table, settings, database, userToken);
        return tableMetaFuture.Apply([ptr, database, userToken](const TFuture<TTableMetadataResult>& future) mutable {
            try {
                auto result = future.GetValue();
                if (!result.Success()) {
                    return MakeFuture(result);
                }

                auto locked = ptr.lock();
                if (!locked) {
                    result.SetStatus(TIssuesIds::KIKIMR_INDEX_METADATA_LOAD_FAILED);
                    return MakeFuture(result);
                }

                if (result.Metadata->Indexes.empty()) {
                    locked->OnLoadedTableMetadata(result);
                    return MakeFuture(result);
                } else {
                    return locked->LoadIndexMetadata(result, database, userToken);
                }
            }
            catch (yexception& e) {
                return MakeFuture(ResultFromException<TResult>(e));
            }
        });
    }
    catch (yexception& e) {
        return MakeFuture(ResultFromException<TResult>(e));
    }
}

NThreading::TFuture<TTableMetadataResult> TKqpTableMetadataLoader::LoadIndexMetadata(
    TTableMetadataResult& loadTableMetadataResult, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken)
{
    auto tableMetadata = loadTableMetadataResult.Metadata;
    YQL_ENSURE(tableMetadata);

    const auto& cluster = tableMetadata->Cluster;
    const auto& tableName = tableMetadata->Name;
    const size_t indexesCount = tableMetadata->Indexes.size();

    TVector<NThreading::TFuture<TGenericResult>> children;
    children.reserve(indexesCount);

    tableMetadata->SecondaryGlobalIndexMetadata.resize(indexesCount);
    const ui64 tableOwnerId = tableMetadata->PathId.OwnerId();

    for (size_t i = 0; i < indexesCount; i++) {
        const auto& index = tableMetadata->Indexes[i];
        auto indexTablePath = NYql::IKikimrGateway::CreateIndexTablePath(tableName, index.Name);

        if (!index.SchemaVersion) {
            LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_GATEWAY, "Load index metadata without schema version check index: " << index.Name);
            children.push_back(
                LoadTableMetadata(cluster, indexTablePath,
                    TLoadTableMetadataSettings().WithPrivateTables(true), database, userToken)
                    .Apply([i, tableMetadata](const TFuture<TTableMetadataResult>& result) {
                        auto value = result.GetValue();
                        UpdateMetadataIfSuccess(tableMetadata, i, value);
                        return static_cast<TGenericResult>(value);
                    })
            );

        } else {
            LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_GATEWAY, "Load index metadata with schema version check"
                << "index: " << index.Name
                << "pathId: " << index.LocalPathId
                << "ownerId: " << index.PathOwnerId
                << "schemaVersion: " << index.SchemaVersion
                << "tableOwnerId: " << tableOwnerId);
             auto ownerId = index.PathOwnerId ? index.PathOwnerId : tableOwnerId; //for compat with 20-2
             children.push_back(
                 LoadIndexMetadataByPathId(cluster,
                     NKikimr::TIndexId(ownerId, index.LocalPathId, index.SchemaVersion), indexTablePath, database, userToken)
                     .Apply([i, tableMetadata](const TFuture<TTableMetadataResult>& result) {
                         auto value = result.GetValue();
                         UpdateMetadataIfSuccess(tableMetadata, i, value);
                         return static_cast<TGenericResult>(value);
                     })
             );

        }
    }

    auto ptr = weak_from_base();
    auto loadIndexMetadataChecker =
        [ptr, result{std::move(loadTableMetadataResult)}, children](const NThreading::TFuture<void>) mutable {
            bool loadOk = true;
            for (const auto& child : children) {
                result.AddIssues(child.GetValue().Issues());
                if (!child.GetValue().Success()) {
                    loadOk = false;
                }
            }
            auto locked = ptr.lock();
            if (!loadOk || !locked) {
                result.SetStatus(TIssuesIds::KIKIMR_INDEX_METADATA_LOAD_FAILED);
            } else {
                locked->OnLoadedTableMetadata(result);
            }

            return MakeFuture(result);
    };

    return NThreading::WaitExceptionOrAll(children).Apply(loadIndexMetadataChecker);
}

NThreading::TFuture<TTableMetadataResult> TKqpTableMetadataLoader::LoadIndexMetadataByPathId(
    const TString& cluster, const TIndexId& indexId, const TString& tableName, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken)
{
    using TResult = TTableMetadataResult;

    try {
        auto ptr = weak_from_base();
        const auto settings = TLoadTableMetadataSettings().WithPrivateTables(true);
        auto tableMetaFuture = LoadTableMetadataCache(cluster, std::make_pair(indexId, tableName), settings, database, userToken);
        return tableMetaFuture.Apply([ptr, database, userToken](const TFuture<TTableMetadataResult>& future) mutable {
            try {
                auto result = future.GetValue();
                if (!result.Success()) {
                    return MakeFuture(result);
                }

                auto locked = ptr.lock();
                if (!locked) {
                    result.SetStatus(TIssuesIds::KIKIMR_INDEX_METADATA_LOAD_FAILED);
                    return MakeFuture(result);
                }

                if (result.Metadata->Indexes.empty()) {
                    return MakeFuture(result);
                } else {
                    return locked->LoadIndexMetadata(result, database, userToken);
                }
            }
            catch (yexception& e) {
                return MakeFuture(ResultFromException<TResult>(e));
            }
        });
    }
    catch (yexception& e) {
        return MakeFuture(ResultFromException<TResult>(e));
    }
}

// The type is TString or std::pair<TIndexId, TString>
template<typename TPath>
NThreading::TFuture<TTableMetadataResult> TKqpTableMetadataLoader::LoadTableMetadataCache(
    const TString& cluster, const TPath& id,
    TLoadTableMetadataSettings settings, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken)
{
    using TRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;
    using TResult = TTableMetadataResult;
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
    using EKind = NSchemeCache::TSchemeCacheNavigate::EKind;

    const auto entry = CreateNavigateEntry(id, settings);
    const ui64 expectedSchemaVersion = GetExpectedVersion(id);

    LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_GATEWAY, "Load table metadata from cache by path, request" << GetDebugString(id));

    auto navigate = MakeHolder<TNavigate>();
    navigate->ResultSet.emplace_back(entry.first);
    const TString& table = entry.second;

    navigate->DatabaseName = database;
    if (userToken && !userToken->GetSerializedToken().empty()) {
        navigate->UserToken = userToken;
    }

    auto ev = MakeHolder<TRequest>(navigate.Release());

    const auto schemeCacheId = MakeSchemeCacheID();

    return SendActorRequest<TRequest, TResponse, TResult>(
        ActorSystem,
        schemeCacheId,
        ev.Release(),
        [userToken, database, cluster, table, settings, expectedSchemaVersion, this]
            (TPromise<TResult> promise, TResponse&& response) mutable
        {
            try {
                YQL_ENSURE(response.Request);
                auto& navigate = *response.Request;

                YQL_ENSURE(navigate.ResultSet.size() == 1);
                auto& entry = navigate.ResultSet[0];

                if (entry.Status == EStatus::Ok && expectedSchemaVersion && entry.TableId.SchemaVersion) {
                    if (entry.TableId.SchemaVersion != expectedSchemaVersion) {
                        const auto message = TStringBuilder()
                            << "schema version mismatch during metadata loading for: "
                            << CombinePath(entry.Path.begin(), entry.Path.end())
                            << " expected " << expectedSchemaVersion
                            << " got " << entry.TableId.SchemaVersion;

                        promise.SetValue(ResultFromError<TResult>(YqlIssue({},
                            TIssuesIds::KIKIMR_SCHEME_MISMATCH, message)));
                        return;
                    }
                }

                if (entry.Status == EStatus::Ok && entry.Kind == EKind::KindIndex) {
                    Y_ENSURE(entry.ListNodeEntry, "expected children list");
                    Y_ENSURE(entry.ListNodeEntry->Children.size() == 1, "expected one child");

                    TIndexId pathId = TIndexId(
                        entry.ListNodeEntry->Children[0].PathId,
                        entry.ListNodeEntry->Children[0].SchemaVersion
                    );

                    LoadTableMetadataCache(cluster, std::make_pair(pathId, table), settings, database, userToken)
                        .Apply([promise](const TFuture<TTableMetadataResult>& result) mutable
                    {
                        promise.SetValue(result.GetValue());
                    });
                } else {
                    promise.SetValue(GetLoadTableMetadataResult(entry, cluster, table));
                }
            }
            catch (yexception& e) {
                promise.SetValue(ResultFromException<TResult>(e));
            }
        });
}

}  // namespace NKikimr::NKqp
