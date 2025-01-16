#include "flat_dbase_scheme.h"

#include <ydb/core/scheme/protos/type_info.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <util/generic/set.h>

namespace NKikimr {
namespace NTable {

TAutoPtr<TSchemeChanges> TScheme::GetSnapshot() const {
    TAlter delta;

    for (const auto& itTable : Tables) {
        const auto table = itTable.first;

        delta.AddTable(itTable.second.Name, table);

        for(const auto& it : itTable.second.Rooms) {
            auto &room = it.second;

            TSet<ui32> blobs;
            for (auto blob : room.Blobs) {
                blobs.insert(blob);
            }
            delta.SetRoom(table, it.first, room.Main, blobs, room.Outer);
        }

        for(const auto& it : itTable.second.Families) {
            auto &family = it.second;

            delta.AddFamily(table, it.first, family.Room);
            delta.SetFamily(table, it.first, family.Cache, family.Codec);
            delta.SetFamilyBlobs(table, it.first, family.Small, family.Large);
        }

        for(const auto& it : itTable.second.Columns) {
            const auto& col = it.second;
            switch (col.PType.GetTypeId()) {
            case NScheme::NTypeIds::Pg: {
                NKikimrProto::TTypeInfo typeInfo;
                NScheme::ProtoFromTypeInfo(col.PType, col.PTypeMod, typeInfo);
                delta.AddColumnWithTypeInfo(table, col.Name, it.first, col.PType.GetTypeId(), typeInfo, col.NotNull, col.Null);
                break;
            }
            case NScheme::NTypeIds::Decimal: {
                NKikimrProto::TTypeInfo typeInfo;
                NScheme::ProtoFromTypeInfo(col.PType, {}, typeInfo);
                delta.AddColumnWithTypeInfo(table, col.Name, it.first, col.PType.GetTypeId(), typeInfo, col.NotNull, col.Null);
                break;
            }
            default: {
                delta.AddColumn(table, col.Name, it.first, col.PType.GetTypeId(), col.NotNull, col.Null);
                break;
            }            
            }

            delta.AddColumnToFamily(table, it.first, col.Family);
        }

        for(ui32 columnId : itTable.second.KeyColumns)
            delta.AddColumnToKey(table, columnId);

        delta.SetCompactionPolicy(table, *itTable.second.CompactionPolicy);

        delta.SetEraseCache(
                table,
                itTable.second.EraseCacheEnabled,
                itTable.second.EraseCacheMinRows,
                itTable.second.EraseCacheMaxBytes);

        // N.B. must be last for compatibility with older versions :(
        delta.SetByKeyFilter(table, itTable.second.ByKeyFilter);
        delta.SetColdBorrow(table, itTable.second.ColdBorrow);
    }

    delta.SetRedo(Redo.Annex);
    delta.SetExecutorCacheSize(Executor.CacheSize);
    delta.SetExecutorAllowLogBatching(Executor.AllowLogBatching);
    delta.SetExecutorLogFlushPeriod(Executor.LogFlushPeriod);
    delta.SetExecutorResourceProfile(Executor.ResourceProfile);
    delta.SetExecutorFastLogPolicy(Executor.LogFastTactic);
    return delta.Flush();
}


TAlter& TAlter::Merge(const TSchemeChanges &log)
{
    Y_ABORT_UNLESS(&Log != &log, "Cannot merge changes onto itself");

    int added = log.DeltaSize();
    if (added > 0) {
        auto* dst = Log.MutableDelta();
        dst->Reserve(Log.DeltaSize() + added);
        for (const auto& delta : log.GetDelta()) {
            *dst->Add() = delta;
            ApplyLastRecord();
        }
    }

    return *this;
}

TAlter& TAlter::AddTable(const TString& name, ui32 id)
{
    TAlterRecord& delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::AddTable);
    delta.SetTableName(name);
    delta.SetTableId(id);

    return ApplyLastRecord();
}

TAlter& TAlter::DropTable(ui32 id)
{
    TAlterRecord& delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::DropTable);
    delta.SetTableId(id);

    return ApplyLastRecord();
}

TAlter& TAlter::AddColumn(ui32 table, const TString& name, ui32 id, ui32 type, bool notNull, TCell null)
{
    Y_ABORT_UNLESS(!NScheme::NTypeIds::IsParametrizedType(type));
    return AddColumnWithTypeInfo(table, name, id, type, {}, notNull, null);
}

TAlter& TAlter::AddColumnWithTypeInfo(ui32 table, const TString& name, ui32 id, ui32 type, const std::optional<NKikimrProto::TTypeInfo>& typeInfoProto, bool notNull, TCell null)
{
    TAlterRecord& delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::AddColumn);
    delta.SetColumnName(name);
    delta.SetTableId(table);
    delta.SetColumnId(id);
    delta.SetColumnType(type);
    delta.SetNotNull(notNull);

    if (!null.IsNull())
        delta.SetDefault(null.Data(), null.Size());

    Y_ABORT_UNLESS((bool)typeInfoProto == NScheme::NTypeIds::IsParametrizedType(type));
    if (typeInfoProto) {
        *delta.MutableColumnTypeInfo() = *typeInfoProto;
    }

    return ApplyLastRecord();
}

TAlter& TAlter::DropColumn(ui32 table, ui32 id)
{
    TAlterRecord& delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::DropColumn);
    delta.SetTableId(table);
    delta.SetColumnId(id);

    return ApplyLastRecord();
}

TAlter& TAlter::AddColumnToFamily(ui32 table, ui32 column, ui32 family)
{
    TAlterRecord& delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::AddColumnToFamily);
    delta.SetTableId(table);
    delta.SetColumnId(column);
    delta.SetFamilyId(family);

    return ApplyLastRecord();
}

TAlter& TAlter::AddFamily(ui32 table, ui32 family, ui32 room)
{
    TAlterRecord& delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::AddFamily);
    delta.SetTableId(table);
    delta.SetFamilyId(family);
    delta.SetRoomId(room);

    return ApplyLastRecord();
}

TAlter& TAlter::AddColumnToKey(ui32 table, ui32 column)
{
    TAlterRecord& delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::AddColumnToKey);
    delta.SetTableId(table);
    delta.SetColumnId(column);

    return ApplyLastRecord();
}

TAlter& TAlter::SetFamily(ui32 table, ui32 family, ECache cache, ECodec codec)
{
    TAlterRecord& delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::SetFamily);
    delta.SetTableId(table);
    delta.SetFamilyId(family);
    delta.SetInMemory(cache == ECache::Ever);
    delta.SetCodec(ui32(codec));
    delta.SetCache(ui32(cache));

    return ApplyLastRecord();
}

TAlter& TAlter::SetFamilyBlobs(ui32 table, ui32 family, ui32 small, ui32 large)
{
    TAlterRecord& delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::SetFamily);
    delta.SetTableId(table);
    delta.SetFamilyId(family);
    delta.SetSmall(small);
    delta.SetLarge(large);

    return ApplyLastRecord();
}

TAlter& TAlter::SetRoom(ui32 table, ui32 room, ui32 main, const TSet<ui32>& blobs, ui32 outer)
{
    Y_ABORT_UNLESS(!blobs.empty(), "Room must have at least one external blob channel");

    auto *delta = Log.AddDelta();

    delta->SetDeltaType(TAlterRecord::SetRoom);
    delta->SetTableId(table);
    delta->SetRoomId(room);
    delta->SetMain(main);
    delta->SetBlobs(*blobs.begin());
    for (auto blob : blobs) {
        delta->AddExternalBlobs(blob);
    }
    delta->SetOuter(outer);

    return ApplyLastRecord();
}

TAlter& TAlter::SetRedo(ui32 annex)
{
    auto *delta = Log.AddDelta();

    delta->SetDeltaType(TAlterRecord::SetRedo);
    delta->SetAnnex(annex);

    return ApplyLastRecord();
}

TAlter& TAlter::SetExecutorCacheSize(ui64 cacheSize)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::UpdateExecutorInfo);
    delta.SetExecutorCacheSize(cacheSize);

    return ApplyLastRecord();
}

TAlter& TAlter::SetExecutorFastLogPolicy(bool allow)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::UpdateExecutorInfo);
    delta.SetExecutorLogFastCommitTactic(allow);

    return ApplyLastRecord();
}

TAlter& TAlter::SetExecutorAllowLogBatching(bool allow)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::UpdateExecutorInfo);
    delta.SetExecutorAllowLogBatching(allow);

    return ApplyLastRecord();
}

TAlter& TAlter::SetExecutorLogFlushPeriod(TDuration flushPeriod)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::UpdateExecutorInfo);
    delta.SetExecutorLogFlushPeriod(flushPeriod.MicroSeconds());

    return ApplyLastRecord();
}

TAlter& TAlter::SetExecutorLimitInFlyTx(ui32 limitTxInFly)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::UpdateExecutorInfo);
    delta.SetExecutorLimitInFlyTx(limitTxInFly);

    return ApplyLastRecord();
}

TAlter& TAlter::SetExecutorResourceProfile(const TString &name)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::UpdateExecutorInfo);
    delta.SetExecutorResourceProfile(name);

    return ApplyLastRecord();
}

TAlter& TAlter::SetCompactionPolicy(ui32 tableId, const TCompactionPolicy& newPolicy)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::SetCompactionPolicy);
    delta.SetTableId(tableId);
    newPolicy.Serialize(*delta.MutableCompactionPolicy());

    return ApplyLastRecord();
}

TAlter& TAlter::SetByKeyFilter(ui32 tableId, bool enabled)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::SetTable);
    delta.SetTableId(tableId);
    delta.SetByKeyFilter(enabled ? 1 : 0);

    return ApplyLastRecord();
}

TAlter& TAlter::SetColdBorrow(ui32 tableId, bool enabled)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::SetTable);
    delta.SetTableId(tableId);
    delta.SetColdBorrow(enabled);

    return ApplyLastRecord();
}

TAlter& TAlter::SetEraseCache(ui32 tableId, bool enabled, ui32 minRows, ui32 maxBytes)
{
    TAlterRecord &delta = *Log.AddDelta();
    delta.SetDeltaType(TAlterRecord::SetTable);
    delta.SetTableId(tableId);
    delta.SetEraseCacheEnabled(enabled ? 1 : 0);
    if (enabled) {
        delta.SetEraseCacheMinRows(minRows);
        delta.SetEraseCacheMaxBytes(maxBytes);
    }

    return ApplyLastRecord();
}

TAlter& TAlter::SetRewrite()
{
    Log.SetRewrite(true);
    return *this;
}

TAlter::operator bool() const noexcept
{
    return Log.DeltaSize() > 0;
}

TAutoPtr<TSchemeChanges> TAlter::Flush()
{
    TAutoPtr<TSchemeChanges> log(new TSchemeChanges);
    log->Swap(&Log);
    return log;
}

TAlter& TAlter::ApplyLastRecord()
{
    if (Sink) {
        int deltasCount = Log.DeltaSize();
        Y_ABORT_UNLESS(deltasCount > 0);

        if (!Sink->ApplyAlterRecord(Log.GetDelta(deltasCount - 1))) {
            Log.MutableDelta()->RemoveLast();
        }
    }

    return *this;
}

}
}
