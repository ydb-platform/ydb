#include "external_data.h"

#include <ydb/core/base/path.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

bool TConfigsSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_VERIFY(rawDataResult.result_sets().size() == 2);
    {
        auto& rawData = rawDataResult.result_sets()[0];
        TTierConfig::TDecoder decoder(rawData);
        for (auto&& r : rawData.rows()) {
            TTierConfig config;
            if (!config.DeserializeFromRecord(decoder, r)) {
                ALS_ERROR(NKikimrServices::TX_TIERING) << "cannot parse tier config from snapshot";
                continue;
            }
            TierConfigs.emplace(config.GetConfigId(), config);
        }
    }
    {
        auto& rawData = rawDataResult.result_sets()[1];
        TTieringRule::TDecoder decoder(rawData);
        TVector<TTieringRule> rulesLocal;
        rulesLocal.reserve(rawData.rows().size());
        for (auto&& r : rawData.rows()) {
            TTieringRule tr;
            if (!tr.DeserializeFromRecord(decoder, r)) {
                ALS_WARN(NKikimrServices::TX_TIERING) << "cannot parse record for tiering info";
                continue;
            }
            rulesLocal.emplace_back(std::move(tr));
        }
        std::sort(rulesLocal.begin(), rulesLocal.end());
        for (auto&& i : rulesLocal) {
            const TString tablePath = i.GetTablePath();
            TableTierings[tablePath].AddRule(std::move(i));
        }
    }
    return true;
}

std::optional<TTierConfig> TConfigsSnapshot::GetValue(const TString& key) const {
    auto it = TierConfigs.find(key);
    if (it == TierConfigs.end()) {
        return {};
    } else {
        return it->second;
    }
}

void TConfigsSnapshot::RemapTablePathToId(const TString& path, const ui64 pathId) {
    auto it = TableTierings.find(path);
    Y_VERIFY(it != TableTierings.end());
    it->second.SetTablePathId(pathId);
}

const TTableTiering* TConfigsSnapshot::GetTableTiering(const TString& tablePath) const {
    auto it = TableTierings.find(tablePath);
    if (it == TableTierings.end()) {
        return nullptr;
    } else {
        return &it->second;
    }
}

std::vector<NKikimr::NColumnShard::NTiers::TTierConfig> TConfigsSnapshot::GetTiersForPathId(const ui64 pathId) const {
    std::vector<TTierConfig> result;
    std::set<TString> readyIds;
    for (auto&& i : TableTierings) {
        for (auto&& r : i.second.GetRules()) {
            if (r.GetTablePathId() == pathId) {
                auto it = TierConfigs.find(r.GetTierId());
                if (it == TierConfigs.end()) {
                    ALS_ERROR(NKikimrServices::TX_TIERING) << "inconsistency tiering for " << r.GetTierId();
                    continue;
                } else if (readyIds.emplace(r.GetTierId()).second) {
                    result.emplace_back(it->second);
                }
            }
        }
    }
    return result;
}

TVector<NMetadataProvider::ITableModifier::TPtr> TSnapshotConstructor::DoGetTableSchema() const {
    TVector<NMetadataProvider::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(Tables[0]);
        request.add_primary_key("ownerPath");
        request.add_primary_key("tierName");
        {
            auto& column = *request.add_columns();
            column.set_name("tierName");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("ownerPath");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("tierConfig");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        result.emplace_back(new NMetadataProvider::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(request));
    }
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(Tables[1]);
        request.add_primary_key("ownerPath");
        request.add_primary_key("tierName");
        request.add_primary_key("tablePath");
        {
            auto& column = *request.add_columns();
            column.set_name("tierName");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("ownerPath");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("tablePath");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("durationForEvict");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("column");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        result.emplace_back(new NMetadataProvider::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(request));
    }
    return result;
}

NThreading::TFuture<NMetadataProvider::ISnapshot::TPtr> TSnapshotConstructor::EnrichSnapshotData(ISnapshot::TPtr original) const {
    if (!Actor) {
        return NThreading::MakeErrorFuture<NMetadataProvider::ISnapshot::TPtr>(
            std::make_exception_ptr(std::runtime_error("actor finished for enrich process")));
    }
    auto current = std::dynamic_pointer_cast<TConfigsSnapshot>(original);
    Y_VERIFY(current);
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = NKikimr::CanonizePath(AppData()->TenantName);
    for (auto&& i : current->GetTableTierings()) {
        auto it = TablesRemapper.find(i.second.GetTablePath());
        if (it != TablesRemapper.end()) {
            current->RemapTablePathToId(it->first, it->second);
        } else {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = TNavigate::OpTable;
            entry.Path = NKikimr::SplitPath(i.second.GetTablePath());
        }
    }
    if (request->ResultSet.empty()) {
        return NThreading::MakeFuture(original);
    } else {
        WaitPromise = NThreading::NewPromise<ISnapshot::TPtr>();
        WaitSnapshot = current;
        Actor->ProvideEvent(new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), MakeSchemeCacheID());
        return WaitPromise.GetFuture();
    }
}

void TSnapshotConstructorAgent::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    Owner->ResolveInfo(ev->Get());
}

void TSnapshotConstructorAgent::ProvideEvent(IEventBase* event, const TActorId& recipient) {
    Send(recipient, event);
}

TSnapshotConstructorAgent::~TSnapshotConstructorAgent() {
    Owner->ActorStopped();
}

void TSnapshotConstructor::ResolveInfo(const TEvTxProxySchemeCache::TEvNavigateKeySetResult* info) {
    const auto& request = info->Request;

    if (request->ResultSet.empty()) {
        WaitPromise.SetException("cannot resolve table path to ids");
        WaitSnapshot = nullptr;
        return;
    }

    for (auto&& i : request->ResultSet) {
        const TString path = "/" + JoinSeq("/", i.Path);
        switch (i.Status) {
            case TNavigate::EStatus::Ok:
                TablesRemapper.emplace(path, i.TableId.PathId.LocalPathId);
                break;
            case TNavigate::EStatus::RootUnknown:
            case TNavigate::EStatus::PathErrorUnknown:
                WaitPromise.SetException("path not exists " + path);
                break;
            case TNavigate::EStatus::LookupError:
            case TNavigate::EStatus::RedirectLookupError:
                WaitPromise.SetException("RESOLVE_LOOKUP_ERROR" + path);
                break;
            default:
                WaitPromise.SetException("GENERIC_RESOLVE_ERROR" + path);
                break;
        }
    }
    if (WaitPromise.HasException()) {
        WaitSnapshot = nullptr;
    } else {
        for (auto&& i : WaitSnapshot->GetTableTierings()) {
            auto it = TablesRemapper.find(i.second.GetTablePath());
            if (it != TablesRemapper.end()) {
                WaitSnapshot->RemapTablePathToId(it->first, it->second);
            } else {
                WaitPromise.SetException("undecoded path " + i.second.GetTablePath());
                WaitSnapshot = nullptr;
                return;
            }
        }
        WaitPromise.SetValue(WaitSnapshot);
        WaitSnapshot = nullptr;
    }
}

TSnapshotConstructor::TSnapshotConstructor() {
    TablePath = "/" + AppData()->TenantName + "/.external_data";
    Tables.emplace_back(TablePath + "/tiers");
    Tables.emplace_back(TablePath + "/tiering");
}

TString NTiers::TConfigsSnapshot::DoSerializeToString() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonTiers = result.InsertValue("tiers", NJson::JSON_MAP);
    for (auto&& i : TierConfigs) {
        jsonTiers.InsertValue(i.first, i.second.GetDebugJson());
    }
    auto& jsonTiering = result.InsertValue("tiering", NJson::JSON_MAP);
    for (auto&& i : TableTierings) {
        jsonTiering.InsertValue(i.first, i.second.GetDebugJson());
    }
    return result.GetStringRobust();
}

}
