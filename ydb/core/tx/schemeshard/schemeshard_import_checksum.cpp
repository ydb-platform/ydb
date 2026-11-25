#include "schemeshard_import_checksum.h"

#include "schemeshard_impl.h"

#include <ydb/core/backup/common/table_checksum_factory.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NSchemeShard {

TActorId TImportChecksum::EnsureActor(ui64 operationId, const TActorContext& ctx) {
    if (!Actor) {
        if (const auto* factory = AppData()->TableChecksumFactory) {
            Actor = ctx.Register(factory->CreateImportActor());
        }
    }
    if (Actor) {
        Operations.insert(operationId);
    }
    return Actor;
}

THolder<IEventHandle> TImportChecksum::StopOperation(ui64 operationId, TActorId sender) {
    if (!Operations.erase(operationId)) {
        return {};
    }
    const auto cookie = Operations.empty() ? ++IdleCookie : 0;
    return MakeHolder<IEventHandle>(
        Actor, sender, new NBackup::TEvTableChecksum::TEvStopOperation(operationId), 0, cookie);
}

void TImportChecksum::OnIdle(
    const NBackup::TEvTableChecksum::TEvImportChecksumIdle::TPtr& ev, const TActorContext& ctx) {
    if (ev->Sender == Actor && ev->Cookie == IdleCookie && Operations.empty()) {
        ctx.Send(std::exchange(Actor, {}), new TEvents::TEvPoisonPill());
    }
}

void TImportChecksum::Shutdown(const TActorContext& ctx) {
    if (Owner) {
        Owner->Alive = false;
    }
    if (Actor) {
        ctx.Send(Actor, new TEvents::TEvPoisonPill());
    }
}

NBackup::TEvTableChecksum::TOwnerPtr TImportChecksum::GetOwner() {
    if (!Owner) {
        Owner = std::make_shared<NBackup::TEvTableChecksum::TOwner>();
    }
    return Owner;
}

void TSchemeShard::Handle(NBackup::TEvTableChecksum::TEvResult::TPtr& ev, const TActorContext& ctx) {
    if (ev->Sender == ImportChecksum.GetActor()) {
        Execute(CreateTxProgressImport(ev), ctx);
    }
}

void TSchemeShard::Handle(NBackup::TEvTableChecksum::TEvImportChecksumIdle::TPtr& ev, const TActorContext& ctx) {
    ImportChecksum.OnIdle(ev, ctx);
}

void TSchemeShard::PrepareImportChecksum(
    TTxId restoreTxId, TRowVersion snapshot, TSideEffects& onComplete, const TActorContext& ctx)
{
    const auto route = TxIdToImport.find(restoreTxId);
    if (route == TxIdToImport.end()) {
        return;
    }

    const auto info = Imports.Value(route->second.first, nullptr);
    if (!info || info->State != TImportInfo::EState::Waiting || !info->ShouldValidateTableChecksum()) {
        return;
    }

    const auto idx = route->second.second;
    auto& item = info->Items.at(idx);
    const auto& checksum = item.Metadata.GetTableChecksum();
    if (item.State != TImportInfo::EState::Transferring || item.ParentIdx != Max<ui32>() || !checksum ||
        checksum->Error || !Tables.contains(item.DstPathId)) {
        return;
    }

    item.ChecksumSnapshot = snapshot;
    if (const auto actor = ImportChecksum.EnsureActor(info->Id, ctx)) {
        onComplete.Send(actor, new NBackup::TEvTableChecksum::TEvPrepareChecksum(
                                   ImportChecksum.GetOwner(), MakeImportChecksumItem(*info, idx, snapshot)));
    }
}

NBackup::TEvTableChecksum::TItem TSchemeShard::MakeImportChecksumItem(
    const TImportInfo& info, ui32 itemIdx, TRowVersion snapshot) {
    const auto& item = info.Items.at(itemIdx);
    return {
        .OperationId = info.Id,
        .ItemIdx = itemIdx,
        .Database = TPath::Init(info.DomainPathId, this).PathString(),
        .TablePath = item.DstPathName,
        .Importing = true,
        .TableId = TTableId(item.DstPathId),
        .Snapshot = snapshot,
        .Algorithm = item.Metadata.GetTableChecksum()->Algorithm,
    };
}

void TSchemeShard::PersistImportChecksumStarted(NIceDb::TNiceDb& db, const TImportInfo& info, ui32 itemIdx) {
    NJson::TJsonValue json;
    Y_ENSURE(NJson::ReadJsonTree(info.Items.at(itemIdx).Metadata.Serialize(), &json) && json.IsMap());
    // This marker belongs to the tablet record and is never taken from the archive
    json["checksum_validation_started"] = true;
    db.Table<Schema::ImportItems>()
        .Key(info.Id, itemIdx)
        .Update(NIceDb::TUpdate<Schema::ImportItems::Metadata>(NJson::WriteJson(&json, false)));
}

void TSchemeShard::RestoreImportItemMetadata(TImportInfo& info, ui32 itemIdx, const TString& metadata) {
    auto& item = info.Items.at(itemIdx);
    item.Metadata = NBackup::TMetadata::Deserialize(metadata, info.ShouldValidateTableChecksum());
    if (item.State == TImportInfo::EState::Transferring && item.WaitTxId) {
        NJson::TJsonValue json;
        if (NJson::ReadJsonTree(metadata, &json) && json["checksum_validation_started"].GetBooleanSafe(false)) {
            item.ChecksumState = TImportInfo::TItem::EChecksumState::Interrupted;
        }
    }
}

} // namespace NKikimr::NSchemeShard
