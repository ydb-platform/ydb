#include "flat_dbase_apply.h"

#include <ydb/core/base/localdb.h>

namespace NKikimr {
namespace NTable {


TSchemeModifier::TSchemeModifier(TScheme &scheme)
    : Scheme(scheme)
{

}

bool TSchemeModifier::Apply(const TAlterRecord &delta)
{
    const auto table = delta.HasTableId() ? delta.GetTableId() : Max<ui32>();
    const auto action = delta.GetDeltaType();
    bool changes = false;

    if (action == TAlterRecord::AddTable) {
        changes = AddTable(delta.GetTableName(), table);
    } else if (action == TAlterRecord::DropTable) {
        changes = DropTable(table);
    } else if (action == TAlterRecord::AddColumn) {
        TCell null;

        if (delta.HasDefault()) {
            auto raw = delta.GetDefault();

            null = TCell(raw.data(), raw.size());
        }

        changes = AddColumn(table, delta.GetColumnName(), delta.GetColumnId(),
                     delta.GetColumnType(), delta.GetNotNull(), null);
    } else if (action == TAlterRecord::DropColumn) {
        changes = DropColumn(table, delta.GetColumnId());
    } else if (action == TAlterRecord::AddColumnToKey) {
        changes = AddColumnToKey(table, delta.GetColumnId());
    } else if (action == TAlterRecord::AddFamily) {
        auto &family = Table(table)->Families[delta.GetFamilyId()];

        const ui32 room = delta.GetRoomId();

        changes = (std::exchange(family.Room, room) != room);

    } else if (action == TAlterRecord::SetFamily) {
        auto &family = Table(table)->Families[delta.GetFamilyId()];

        auto codec = delta.HasCodec() ? ECodec(delta.GetCodec()) :family.Codec;

        Y_VERIFY(ui32(codec) <= 1, "Invalid page encoding code value");

        bool ever = delta.HasInMemory() && delta.GetInMemory();
        auto cache = ever ? ECache::Ever : family.Cache;

        cache = delta.HasCache() ? ECache(delta.GetCache()) : cache;
        ui32 small = delta.HasSmall() ? delta.GetSmall() : family.Small;
        ui32 large = delta.HasLarge() ? delta.GetLarge() : family.Large;

        Y_VERIFY(ui32(cache) <= 2, "Invalid pages cache policy value");

        changes =
            (std::exchange(family.Cache, cache) != cache)
            | (std::exchange(family.Codec, codec) != codec)
            | (std::exchange(family.Small, small) != small)
            | (std::exchange(family.Large, large) != large);

    } else if (action == TAlterRecord::AddColumnToFamily) {
        changes = AddColumnToFamily(table, delta.GetColumnId(), delta.GetFamilyId());
    } else if (action == TAlterRecord::SetRoom) {
        auto &room = Table(table)->Rooms[delta.GetRoomId()];

        ui32 main = delta.HasMain() ? delta.GetMain() : room.Main;
        ui32 blobs = delta.HasBlobs() ? delta.GetBlobs() : room.Blobs;
        ui32 outer = delta.HasOuter() ? delta.GetOuter() : room.Outer;

        changes =
            (std::exchange(room.Main, main) != main)
            | (std::exchange(room.Blobs, blobs) != blobs)
            | (std::exchange(room.Outer, outer) != outer);

    } else if (action == TAlterRecord::SetRedo) {
        const ui32 annex = delta.HasAnnex() ? delta.GetAnnex() : 0;

        changes = (std::exchange(Scheme.Redo.Annex, annex) != annex);

    } else if (action == TAlterRecord::SetTable) {
        if (delta.HasByKeyFilter()) {
            bool enabled = delta.GetByKeyFilter();
            changes |= (std::exchange(Table(table)->ByKeyFilter, enabled) != enabled);
        }

        if (delta.HasEraseCacheEnabled()) {
            bool enabled = delta.GetEraseCacheEnabled();
            changes |= (std::exchange(Table(table)->EraseCacheEnabled, enabled) != enabled);
            if (enabled) {
                ui32 minRows = delta.GetEraseCacheMinRows();
                ui32 maxBytes = delta.GetEraseCacheMaxBytes();
                changes |= (std::exchange(Table(table)->EraseCacheMinRows, minRows) != minRows);
                changes |= (std::exchange(Table(table)->EraseCacheMaxBytes, maxBytes) != maxBytes);
            }
        }

        if (delta.HasColdBorrow()) {
            bool enabled = delta.GetColdBorrow();
            changes |= (std::exchange(Table(table)->ColdBorrow, enabled) != enabled);
        }

    } else if (action == TAlterRecord::UpdateExecutorInfo) {
        if (delta.HasExecutorCacheSize())
            changes |= SetExecutorCacheSize(delta.GetExecutorCacheSize());
        if (delta.HasExecutorAllowLogBatching())
            changes |= SetExecutorAllowLogBatching(delta.GetExecutorAllowLogBatching());
        if (delta.HasExecutorLogFlushPeriod())
            changes |= SetExecutorLogFlushPeriod(TDuration::MicroSeconds(delta.GetExecutorLogFlushPeriod()));
        if (delta.HasExecutorLimitInFlyTx())
            changes |= SetExecutorLimitInFlyTx(delta.GetExecutorLimitInFlyTx());
        if (delta.HasExecutorResourceProfile())
            changes |= SetExecutorResourceProfile(delta.GetExecutorResourceProfile());
        if (delta.HasExecutorLogFastCommitTactic()) 
            changes |= SetExecutorLogFastCommitTactic(delta.GetExecutorLogFastCommitTactic()); 
    } else if (action == TAlterRecord::SetCompactionPolicy) {
        changes = SetCompactionPolicy(table, delta.GetCompactionPolicy());
    } else {
        Y_FAIL("unknown scheme delta record type");
    }

    if (delta.HasTableId() && changes)
        Affects.insert(table);
    return changes;
}

bool TSchemeModifier::AddColumnToFamily(ui32 tid, ui32 cid, ui32 family)
{
    auto* column = Scheme.GetColumnInfo(Table(tid), cid);
    Y_VERIFY(column);

    return std::exchange(column->Family, family) != family;
}

bool TSchemeModifier::AddTable(const TString &name, ui32 id)
{
    auto itName = Scheme.TableNames.find(name);
    auto it = Scheme.Tables.emplace(id, TTable(name, id));
    if (itName != Scheme.TableNames.end()) {
        Y_VERIFY(!it.second && it.first->second.Name == name && itName->second == it.first->first);
        return false;
    } else if (!it.second) {
        // renaming table
        Scheme.TableNames.erase(it.first->second.Name);
        Scheme.TableNames.emplace(name, id);
        it.first->second.Name = name;
        return true;
    } else {
        Y_VERIFY(it.second);
        Scheme.TableNames.emplace(name, id);
        if (id >= 100) {
            auto *table = &it.first->second;
            // HACK: Force user tables to have some reasonable policy with multiple levels
            table->CompactionPolicy = NLocalDb::CreateDefaultUserTablePolicy();
        }
        return true;
    }
}

bool TSchemeModifier::DropTable(ui32 id)
{
    auto it = Scheme.Tables.find(id);
    if (it != Scheme.Tables.end()) {
        Scheme.TableNames.erase(it->second.Name);
        Scheme.Tables.erase(it);
        return true;
    }
    return false;
}

bool TSchemeModifier::AddColumn(ui32 tid, const TString &name, ui32 id, ui32 type, bool notNull, TCell null)
{
    auto *table = Table(tid);
    auto itName = table->ColumnNames.find(name);
    bool haveName = itName != table->ColumnNames.end();
    auto it = table->Columns.emplace(id, TColumn(name, id, type, notNull));

    if (it.second)
        it.first->second.SetDefault(null);

    if (!it.second && !haveName && it.first->second.PType == type) {
        // renaming column
        table->ColumnNames.erase(it.first->second.Name);
        table->ColumnNames.emplace(name, id);
        it.first->second.Name = name;
        return true;
    } else {
        // do we have inserted a new column, OR we already have the same column with the same name?
        bool insertedNew = it.second && !haveName;
        bool replacedExisting = !it.second && it.first->second.Name == name && haveName && itName->second == it.first->first;
        Y_VERIFY_S((insertedNew || replacedExisting),
            "NewName: " << name <<
            " OldName: " << (haveName ? itName->first : it.first->second.Name) <<
            " NewId: " << id <<
            " OldId: " << (haveName ? itName->second : it.first->first));
        if (!haveName) {
            table->ColumnNames.emplace(name, id);
            return true;
        }
    }
    return false;
}

bool TSchemeModifier::DropColumn(ui32 tid, ui32 id)
{
    auto *table = Table(tid);
    auto it = table->Columns.find(id);
    if (it != table->Columns.end()) {
        table->ColumnNames.erase(it->second.Name);
        table->Columns.erase(it);
        return true;
    }
    return false;
}

bool TSchemeModifier::AddColumnToKey(ui32 tid, ui32 columnId)
{
    auto *table = Table(tid);
    auto* column = Scheme.GetColumnInfo(table, columnId);
    Y_VERIFY(column);

    auto keyPos = std::find(table->KeyColumns.begin(), table->KeyColumns.end(), column->Id);
    if (keyPos == table->KeyColumns.end()) {
        column->KeyOrder = table->KeyColumns.size();
        table->KeyColumns.push_back(column->Id);
        return true;
    }
    return false;
}

bool TSchemeModifier::SetExecutorCacheSize(ui64 size)
{
    return std::exchange(Scheme.Executor.CacheSize, size) != size;
}

bool TSchemeModifier::SetExecutorAllowLogBatching(bool allow)
{
    return std::exchange(Scheme.Executor.AllowLogBatching, allow) != allow;
}

bool TSchemeModifier::SetExecutorLogFastCommitTactic(bool allow) 
{ 
    return std::exchange(Scheme.Executor.LogFastTactic, allow) != allow; 
} 
 
bool TSchemeModifier::SetExecutorLogFlushPeriod(TDuration delay)
{
    return std::exchange(Scheme.Executor.LogFlushPeriod, delay) != delay;
}

bool TSchemeModifier::SetExecutorLimitInFlyTx(ui32 limit)
{
    return std::exchange(Scheme.Executor.LimitInFlyTx, limit) != limit;
}

bool TSchemeModifier::SetExecutorResourceProfile(const TString &name)
{
    return std::exchange(Scheme.Executor.ResourceProfile, name) != name;
}

bool TSchemeModifier::SetCompactionPolicy(ui32 tid, const NKikimrSchemeOp::TCompactionPolicy &proto)
{
    auto *table = Table(tid);
    TIntrusiveConstPtr<TCompactionPolicy> policy(new TCompactionPolicy(proto));
    if (table->CompactionPolicy && *(table->CompactionPolicy) == *policy)
        return false;
    table->CompactionPolicy = policy;
    return true;
}

}
}
