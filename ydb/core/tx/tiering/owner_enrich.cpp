#include "owner_enrich.h"
#include "snapshot.h"

#include <ydb/core/base/path.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

TVector<NMetadataInitializer::ITableModifier::TPtr> TActorOwnerEnrich::BuildModifiers(const NSchemeCache::TSchemeCacheNavigate::TEntry& ownerEntry) const {
    TVector<NMetadataInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TableNames[0]);
        request.add_primary_key("ownerPath");
        request.add_primary_key("tierName");
        {
            auto& column = *request.add_columns();
            column.set_name("ownerPath");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("tierName");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("tierConfig");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        result.emplace_back(new NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(request));
    }
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TableNames[1]);
        request.add_primary_key("ownerPath");
        request.add_primary_key("tablePath");
        request.add_primary_key("tierName");
        {
            auto& column = *request.add_columns();
            column.set_name("ownerPath");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("tierName");
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
        result.emplace_back(new NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(request));
    }
    Y_VERIFY(!!ownerEntry.SecurityObject);
    return result;
}

void TActorOwnerEnrich::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    auto* info = ev->Get();
    const auto& request = info->Request;
    auto g = PassAwayGuard();
    if (request->ResultSet.empty()) {
        Controller->PreparationProblem("cannot resolve owner path to ids");
        return;
    }

    if (request->ResultSet.size() != 1) {
        Controller->PreparationProblem("unexpected result set size: " + ::ToString(request->ResultSet.size()));
        return;
    }

    for (auto&& i : request->ResultSet) {
        const TString path = "/" + JoinSeq("/", i.Path);
        switch (i.Status) {
            case TNavigate::EStatus::Ok:
                Controller->PreparationFinished(BuildModifiers(i));
                return;
            default:
                Controller->PreparationProblem(::ToString(i.Status) + " for '" + path + "'");
                return;
        }
    }
    Controller->PreparationProblem("unexpected");
}

void TActorOwnerEnrich::Bootstrap() {
    Become(&TThis::StateMain);
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = NKikimr::CanonizePath(AppData()->TenantName);
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = TNavigate::OpPath;
    entry.Path = NKikimr::SplitPath(OwnerPath);
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

}
