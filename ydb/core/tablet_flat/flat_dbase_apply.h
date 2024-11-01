#pragma once

#include "flat_dbase_scheme.h"
#include "flat_page_iface.h"
#include <util/generic/hash_set.h>
#include <ydb/core/protos/scheme_log.pb.h>

namespace NKikimr {
namespace NTable {

    class TSchemeModifier {
    public:
        using TTable = TScheme::TTableInfo;
        using TFamily = TScheme::TFamily;
        using ECodec = NPage::ECodec;
        using ECache = NPage::ECache;

        explicit TSchemeModifier(TScheme &scheme, TSchemeRollbackState *rollbackState = nullptr);

        bool Apply(const TSchemeChanges &delta)
        {
            bool changed = false;

            for (const auto &delta : delta.GetDelta())
                changed = Apply(delta) || changed;

            return changed;
        }

        bool Apply(const TAlterRecord &delta);

    protected:
        bool AddTable(const TString& name, ui32 id);
        bool DropTable(ui32 id);
        bool AddColumn(ui32 table, const TString& name, ui32 id, ui32 type, bool notNull, TCell null = { });
        bool AddColumnWithTypeInfo(ui32 table, const TString& name, ui32 id, ui32 type, const std::optional<NKikimrProto::TTypeInfo>& typeInfoProto, bool notNull, TCell null = { });
        bool DropColumn(ui32 table, ui32 id);
        bool AddColumnToFamily(ui32 table, ui32 column, ui32 family);
        bool AddColumnToKey(ui32 table, ui32 column);

        bool SetExecutorCacheSize(ui64 cacheSize);
        bool SetExecutorAllowLogBatching(bool allow);
        bool SetExecutorLogFastCommitTactic(bool allow);
        bool SetExecutorLogFlushPeriod(TDuration flushPeriod);
        bool SetExecutorLimitInFlyTx(ui32 limitTxInFly);
        bool SetExecutorResourceProfile(const TString &name);
        bool SetCompactionPolicy(ui32 tableId, const NKikimrSchemeOp::TCompactionPolicy& newPolicy);

        TTable* Table(ui32 tid) const noexcept
        {
            auto* table = Scheme.GetTableInfo(tid);
            Y_ABORT_UNLESS(table, "Acccessing table that doesn't exist");
            return table;
        }

        template<class T>
        bool ChangeTableSetting(ui32 tid, T& dst, const T& value) {
            if (dst != value) {
                PreserveTable(tid);
                dst = value;
                return true;
            }
            return false;
        }

        template<class T>
        bool ChangeExecutorSetting(T& dst, const T& value) {
            if (dst != value) {
                PreserveExecutor();
                dst = value;
                return true;
            }
            return false;
        }

        template<class T>
        bool ChangeRedoSetting(T& dst, const T& value) {
            if (dst != value) {
                PreserveRedo();
                dst = value;
                return true;
            }
            return false;
        }

        void PreserveTable(ui32 tid) noexcept;
        void PreserveExecutor() noexcept;
        void PreserveRedo() noexcept;

    public:
        TScheme &Scheme;
        TSchemeRollbackState *RollbackState;
        THashSet<ui32> Affects;
    };

}
}
