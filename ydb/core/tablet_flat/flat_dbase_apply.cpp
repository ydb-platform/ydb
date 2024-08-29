#include "flat_dbase_apply.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr {
namespace NTable {

namespace {
    class TChanges {
    public:
        explicit operator bool() const {
            return Value;
        }

        TChanges& operator|=(bool value) {
            if (value) {
                Value = true;
            }
            return *this;
        }

    private:
        bool Value = false;
    };
}

TSchemeModifier::TSchemeModifier(TScheme &scheme, TSchemeRollbackState *rollbackState)
    : Scheme(scheme)
    , RollbackState(rollbackState)
{

}

bool TSchemeModifier::Apply(const TAlterRecord &delta)
{
    const auto table = delta.HasTableId() ? delta.GetTableId() : Max<ui32>();
    const auto action = delta.GetDeltaType();
    TChanges changes;

    if (action == TAlterRecord::AddTable) {
        changes |= AddTable(delta.GetTableName(), table);
    } else if (action == TAlterRecord::DropTable) {
        changes |= DropTable(table);
    } else if (action == TAlterRecord::AddColumn) {
        TCell null;

        if (delta.HasDefault()) {
            auto raw = delta.GetDefault();

            null = TCell(raw.data(), raw.size());
        }

        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(delta.GetColumnType(),
            delta.HasColumnTypeInfo() ? &delta.GetColumnTypeInfo() : nullptr);
        ui32 pgTypeId = NPg::PgTypeIdFromTypeDesc(typeInfoMod.TypeInfo.GetTypeDesc());
        changes |= AddPgColumn(table, delta.GetColumnName(), delta.GetColumnId(),
            delta.GetColumnType(), pgTypeId, typeInfoMod.TypeMod, delta.GetNotNull(), null);
    } else if (action == TAlterRecord::DropColumn) {
        changes |= DropColumn(table, delta.GetColumnId());
    } else if (action == TAlterRecord::AddColumnToKey) {
        changes |= AddColumnToKey(table, delta.GetColumnId());
    } else if (action == TAlterRecord::AddFamily) {
        auto &tableInfo = *Table(table);
        if (!tableInfo.Families.contains(delta.GetFamilyId())) {
            PreserveTable(table);
            changes |= true;
        }
        auto &family = tableInfo.Families[delta.GetFamilyId()];

        const ui32 room = delta.GetRoomId();

        changes |= ChangeTableSetting(table, family.Room, room);

    } else if (action == TAlterRecord::SetFamily) {
        auto &tableInfo = *Table(table);
        if (!tableInfo.Families.contains(delta.GetFamilyId())) {
            PreserveTable(table);
            changes |= true;
        }
        auto &family = tableInfo.Families[delta.GetFamilyId()];

        auto codec = delta.HasCodec() ? ECodec(delta.GetCodec()) : family.Codec;

        Y_ABORT_UNLESS(ui32(codec) <= 9, "Invalid page encoding code value");

        // FIXME: for now these changes will affect old parts on boot only (see RequestInMemPagesForPartStore)
        bool ever = delta.HasInMemory() && delta.GetInMemory();
        auto cache = ever ? ECache::Ever : family.Cache;

        cache = delta.HasCache() ? ECache(delta.GetCache()) : cache;
        ui32 small = delta.HasSmall() ? delta.GetSmall() : family.Small;
        ui32 large = delta.HasLarge() ? delta.GetLarge() : family.Large;

        Y_ABORT_UNLESS(ui32(cache) <= 2, "Invalid pages cache policy value");
        changes |= ChangeTableSetting(table, family.Cache, cache);
        changes |= ChangeTableSetting(table, family.Codec, codec);
        changes |= ChangeTableSetting(table, family.Small, small);
        changes |= ChangeTableSetting(table, family.Large, large);

    } else if (action == TAlterRecord::AddColumnToFamily) {
        changes |= AddColumnToFamily(table, delta.GetColumnId(), delta.GetFamilyId());
    } else if (action == TAlterRecord::SetRoom) {
        auto &tableInfo = *Table(table);
        if (!tableInfo.Rooms.contains(delta.GetRoomId())) {
            PreserveTable(table);
            changes |= true;
        }
        auto &room = tableInfo.Rooms[delta.GetRoomId()];

        ui8 main = delta.HasMain() ? delta.GetMain() : room.Main;
        ui8 blobs = delta.HasBlobs() ? delta.GetBlobs() : room.Blobs;
        ui8 outer = delta.HasOuter() ? delta.GetOuter() : room.Outer;

        changes |= ChangeTableSetting(table, room.Main, main);
        changes |= ChangeTableSetting(table, room.Blobs, blobs);
        changes |= ChangeTableSetting(table, room.Outer, outer);
    } else if (action == TAlterRecord::SetRedo) {
        const ui32 annex = delta.HasAnnex() ? delta.GetAnnex() : 0;

        changes |= ChangeRedoSetting(Scheme.Redo.Annex, annex);

    } else if (action == TAlterRecord::SetTable) {
        auto &tableInfo = *Table(table);

        if (delta.HasByKeyFilter()) {
            bool enabled = delta.GetByKeyFilter();
            changes |= ChangeTableSetting(table, tableInfo.ByKeyFilter, enabled);
        }

        if (delta.HasEraseCacheEnabled()) {
            bool enabled = delta.GetEraseCacheEnabled();
            changes |= ChangeTableSetting(table, tableInfo.EraseCacheEnabled, enabled);
            if (enabled) {
                ui32 minRows = delta.GetEraseCacheMinRows();
                ui32 maxBytes = delta.GetEraseCacheMaxBytes();
                changes |= ChangeTableSetting(table, tableInfo.EraseCacheMinRows, minRows);
                changes |= ChangeTableSetting(table, tableInfo.EraseCacheMaxBytes, maxBytes);
            }
        }

        if (delta.HasColdBorrow()) {
            bool enabled = delta.GetColdBorrow();
            changes |= ChangeTableSetting(table, tableInfo.ColdBorrow, enabled);
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
        changes |= SetCompactionPolicy(table, delta.GetCompactionPolicy());
    } else {
        Y_ABORT("unknown scheme delta record type");
    }

    if (delta.HasTableId() && changes)
        Affects.insert(table);
    return bool(changes);
}

bool TSchemeModifier::AddColumnToFamily(ui32 tid, ui32 cid, ui32 family)
{
    auto* column = Scheme.GetColumnInfo(Table(tid), cid);
    Y_ABORT_UNLESS(column);

    if (column->Family != family) {
        PreserveTable(tid);
        // FIXME: for now ECache::Ever setting will affect old parts on boot only (see RequestInMemPagesForPartStore)
        column->Family = family;
        return true;
    }

    return false;
}

bool TSchemeModifier::AddTable(const TString &name, ui32 id)
{
    auto it = Scheme.Tables.find(id);
    auto itName = Scheme.TableNames.find(name);

    // We verify id match when a table with this name already exists
    if (itName != Scheme.TableNames.end()) {
        auto describeFailure = [&]() -> TString {
            TStringBuilder out;
            out << "Table " << id << " '" << name << "'"
                << " conflicts with table " << itName->second << " '" << itName->first << "'";
            if (it != Scheme.Tables.end()) {
                out << " and table " << it->first << " '" << it->second.Name << "'";
            }
            return out;
        };
        Y_VERIFY_S(itName->second == id, describeFailure());
        // Sanity check that this table really exists
        Y_ABORT_UNLESS(it != Scheme.Tables.end() && it->second.Name == name);
        return false;
    }

    PreserveTable(id);

    // We assume table is renamed when the same id already exists
    if (it != Scheme.Tables.end()) {
        Scheme.TableNames.erase(it->second.Name);
        it->second.Name = name;
        Scheme.TableNames.emplace(name, id);
        return true;
    }

    // Creating a new table
    auto pr = Scheme.Tables.emplace(id, TTable(name, id));
    Y_ABORT_UNLESS(pr.second);
    it = pr.first;
    Scheme.TableNames.emplace(name, id);

    if (id >= 100) {
        // HACK: Force user tables to have some reasonable policy with multiple levels
        it->second.CompactionPolicy = NLocalDb::CreateDefaultUserTablePolicy();
    }

    return true;
}

bool TSchemeModifier::DropTable(ui32 id)
{
    auto it = Scheme.Tables.find(id);
    if (it != Scheme.Tables.end()) {
        PreserveTable(id);
        Scheme.TableNames.erase(it->second.Name);
        Scheme.Tables.erase(it);
        return true;
    }
    return false;
}

bool TSchemeModifier::AddColumn(ui32 tid, const TString &name, ui32 id, ui32 type, bool notNull, TCell null)
{
    Y_ABORT_UNLESS(type != (ui32)NScheme::NTypeIds::Pg, "No pg type data");
    return AddPgColumn(tid, name, id, type, 0, "", notNull, null);
}

bool TSchemeModifier::AddPgColumn(ui32 tid, const TString &name, ui32 id, ui32 type, ui32 pgType, const TString& pgTypeMod, bool notNull, TCell null)
{
    auto *table = Table(tid);

    auto it = table->Columns.find(id);
    auto itName = table->ColumnNames.find(name);

    NScheme::TTypeInfo typeInfo;
    if (pgType != 0) {
        Y_ABORT_UNLESS((NScheme::TTypeId)type == NScheme::NTypeIds::Pg);
        auto* typeDesc = NPg::TypeDescFromPgTypeId(pgType);
        Y_ABORT_UNLESS(typeDesc);
        typeInfo = NScheme::TTypeInfo(type, typeDesc);
    } else {
        typeInfo = NScheme::TTypeInfo(type);
    }

    // We verify ids and types match when column with the same name already exists
    if (itName != table->ColumnNames.end()) {
        auto describeFailure = [&]() -> TString {
            TStringBuilder out;
            out << "Table " << tid << " '" << table->Name << "'"
                << " adding column " << id << " '" << name << "'"
                << " conflicts with column " << itName->second << " '" << itName->first << "'";
            if (it != table->Columns.end()) {
                out << " and column " << it->first << " '" << it->second.Name << "'";
            }
            return out;
        };
        Y_VERIFY_S(itName->second == id, describeFailure());
        // Sanity check that this column exists and types match
        Y_ABORT_UNLESS(it != table->Columns.end() && it->second.Name == name);
        Y_VERIFY_S(it->second.PType == typeInfo && it->second.PTypeMod == pgTypeMod,
            "Table " << tid << " '" << table->Name << "' column " << id << " '" << name
            << "' expected type " << NScheme::TypeName(typeInfo, pgTypeMod)
            << ", existing type " << NScheme::TypeName(it->second.PType, it->second.PTypeMod));
        return false;
    }

    PreserveTable(tid);

    // We assume column is renamed when the same id already exists
    if (it != table->Columns.end()) {
        Y_VERIFY_S(it->second.PType == typeInfo && it->second.PTypeMod == pgTypeMod,
            "Table " << tid << " '" << table->Name << "' column " << id << " '" << it->second.Name << "' renamed to '" << name << "'"
            << " with type " << NScheme::TypeName(typeInfo, pgTypeMod)
            << ", existing type " << NScheme::TypeName(it->second.PType, it->second.PTypeMod));
        table->ColumnNames.erase(it->second.Name);
        it->second.Name = name;
        table->ColumnNames.emplace(name, id);
        return true;
    }

    auto pr = table->Columns.emplace(id, TColumn(name, id, typeInfo, pgTypeMod, notNull));
    Y_ABORT_UNLESS(pr.second);
    it = pr.first;
    table->ColumnNames.emplace(name, id);

    it->second.SetDefault(null);

    return true;
}

bool TSchemeModifier::DropColumn(ui32 tid, ui32 id)
{
    auto *table = Table(tid);
    auto it = table->Columns.find(id);
    if (it != table->Columns.end()) {
        PreserveTable(tid);
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
    Y_ABORT_UNLESS(column);

    auto keyPos = std::find(table->KeyColumns.begin(), table->KeyColumns.end(), column->Id);
    if (keyPos == table->KeyColumns.end()) {
        PreserveTable(tid);
        column->KeyOrder = table->KeyColumns.size();
        table->KeyColumns.push_back(column->Id);
        return true;
    }
    return false;
}

bool TSchemeModifier::SetExecutorCacheSize(ui64 size)
{
    return ChangeExecutorSetting(Scheme.Executor.CacheSize, size);
}

bool TSchemeModifier::SetExecutorAllowLogBatching(bool allow)
{
    return ChangeExecutorSetting(Scheme.Executor.AllowLogBatching, allow);
}

bool TSchemeModifier::SetExecutorLogFastCommitTactic(bool allow)
{
    return ChangeExecutorSetting(Scheme.Executor.LogFastTactic, allow);
}

bool TSchemeModifier::SetExecutorLogFlushPeriod(TDuration delay)
{
    return ChangeExecutorSetting(Scheme.Executor.LogFlushPeriod, delay);
}

bool TSchemeModifier::SetExecutorLimitInFlyTx(ui32 limit)
{
    return ChangeExecutorSetting(Scheme.Executor.LimitInFlyTx, limit);
}

bool TSchemeModifier::SetExecutorResourceProfile(const TString &name)
{
    return ChangeExecutorSetting(Scheme.Executor.ResourceProfile, name);
}

bool TSchemeModifier::SetCompactionPolicy(ui32 tid, const NKikimrSchemeOp::TCompactionPolicy &proto)
{
    auto *table = Table(tid);
    TIntrusiveConstPtr<TCompactionPolicy> policy(new TCompactionPolicy(proto));
    if (table->CompactionPolicy && *(table->CompactionPolicy) == *policy)
        return false;
    PreserveTable(tid);
    table->CompactionPolicy = policy;
    return true;
}

void TSchemeModifier::PreserveTable(ui32 tid) noexcept
{
    if (RollbackState && !RollbackState->Tables.contains(tid)) {
        auto it = Scheme.Tables.find(tid);
        if (it != Scheme.Tables.end()) {
            RollbackState->Tables[tid] = it->second;
        } else {
            RollbackState->Tables[tid] = std::nullopt;
        }
    }
}

void TSchemeModifier::PreserveExecutor() noexcept
{
    if (RollbackState && !RollbackState->Executor) {
        RollbackState->Executor = Scheme.Executor;
    }
}

void TSchemeModifier::PreserveRedo() noexcept
{
    if (RollbackState && !RollbackState->Redo) {
        RollbackState->Redo = Scheme.Redo;
    }
}

}
}
