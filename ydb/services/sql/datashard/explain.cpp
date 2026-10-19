#include "events.h"

#include <ydb/services/sql/codec/codec.h>

#include <ydb/core/base/path.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_order.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/generic/mapfindptr.h>

#include <algorithm>

namespace NKikimr::NSql {
namespace {

struct TColumn {
    TString Name;
    ui32 Id = 0;
    NScheme::TTypeInfo Type;
    ui8 Precision = 0;
    ui8 Scale = 0;
};

TString TablePath(const TString& database, const TSelectStmt& statement) {
    TString path = database;
    if (path && path.back() == '/') {
        path.pop_back();
    }
    if (statement.Schema) {
        path += "/" + statement.Schema->Name;
    }
    path += "/" + statement.From.Name;
    return path;
}

TString OpText(ECompareOp op) {
    switch (op) {
        case ECompareOp::Eq:
            return "=";
        case ECompareOp::NotEq:
            return "<>";
        case ECompareOp::Lt:
            return "<";
        case ECompareOp::Gt:
            return ">";
        case ECompareOp::Le:
            return "<=";
        case ECompareOp::Ge:
            return ">=";
    }
    return "=";
}

class TExplainActor : public NActors::TActorBootstrapped<TExplainActor> {
public:
    TExplainActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement, ui64 lockTxId)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , LockTxId(lockTxId)
    {
    }

    void Bootstrap() {
        Path = TablePath(Database, Statement);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(Path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TExplainActor::StateNavigate);
    }

private:
    void Fail(const TString& state, const TString& message) {
        if (Replied) {
            return;
        }
        Replied = true;
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = false;
        result->SqlState = state;
        result->Message = message;
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void Reply(const TString& plan) {
        if (Replied) {
            return;
        }
        Replied = true;
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "EXPLAIN";
        result->Columns.push_back({"QUERY PLAN", 25, -1});
        result->Rows.push_back({{false, plan}});
        Send(ReplyTo, result.Release());
        PassAway();
    }

    const TSysTables::TTableColumnInfo* FindColumn(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& name) const {
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            if (column.Name == name) {
                return &column;
            }
        }
        return nullptr;
    }

    bool Prepare(const TSysTables::TTableColumnInfo& info, TColumn& column) const {
        column.Name = info.Name;
        column.Id = info.Id;
        column.Type = info.PType;
        if (info.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
            column.Precision = static_cast<ui8>(info.PType.GetDecimalType().GetPrecision());
            column.Scale = static_cast<ui8>(info.PType.GetDecimalType().GetScale());
        }
        return EncodeCell(column.Type.GetTypeId(), true, {}, EWireFormat::Text, column.Precision, column.Scale).Ok;
    }

    bool Decode(const TColumn& column, const TLiteral& literal, TCell& cell) {
        if (literal.Kind == TLiteral::EKind::Parameter || literal.Kind == TLiteral::EKind::Null) {
            Fail("0A000", "not supported");
            return false;
        }
        const TCodecValue decoded = DecodeCell(column.Type.GetTypeId(), false, literal.Text, EWireFormat::Text, column.Precision, column.Scale);
        if (!decoded.Ok) {
            Fail(decoded.SqlState.empty() ? TString("0A000") : decoded.SqlState, decoded.Message.empty() ? TString("not supported") : decoded.Message);
            return false;
        }
        BoundBytes.push_back(decoded.Bytes);
        cell = TCell(BoundBytes.back().data(), BoundBytes.back().size());
        return true;
    }

    void TightenLower(const TColumn& column, const TCell& cell, bool inclusive) {
        if (!FromSet || CompareTypedCells(cell, FromCells[0], NScheme::TTypeInfoOrder(column.Type)) > 0
            || (CompareTypedCells(cell, FromCells[0], NScheme::TTypeInfoOrder(column.Type)) == 0 && !inclusive && FromInclusive))
        {
            FromCells.assign(1, cell);
            FromInclusive = inclusive;
            FromSet = true;
        }
    }

    void TightenUpper(const TColumn& column, const TCell& cell, bool inclusive) {
        if (!ToSet || CompareTypedCells(cell, ToCells[0], NScheme::TTypeInfoOrder(column.Type)) < 0
            || (CompareTypedCells(cell, ToCells[0], NScheme::TTypeInfoOrder(column.Type)) == 0 && !inclusive && ToInclusive))
        {
            ToCells.assign(1, cell);
            ToInclusive = inclusive;
            ToSet = true;
        }
    }

    TString PlanText(bool point, ui64 shards) const {
        TStringBuilder text;
        text << (point ? "Keys" : "Ranges") << " Shards " << shards;
        if (!Residual.empty()) {
            text << " Residual " << Residual;
        }
        if (Statement.Limit) {
            text << " Limit " << *Statement.Limit;
        }
        return text;
    }

    bool Classify(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        TVector<const TSysTables::TTableColumnInfo*> keys;
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            if (column.KeyOrder >= 0) {
                keys.push_back(&column);
            }
        }
        std::sort(keys.begin(), keys.end(), [](const auto* left, const auto* right) {
            return left->KeyOrder < right->KeyOrder;
        });
        if (keys.empty()) {
            Fail("0A000", "not supported");
            return false;
        }
        BoundBytes.reserve(Statement.Where.size() * 2);

        absl::flat_hash_map<TString, TVector<const TComparison*>> grouped;
        for (const TComparison& comparison : Statement.Where) {
            if (!FindColumn(entry, comparison.Column.Name)) {
                Fail("42703", comparison.Column.Name);
                return false;
            }
            grouped[comparison.Column.Name].push_back(&comparison);
        }

        bool ranged = false;
        size_t equalPrefix = 0;
        for (const auto* info : keys) {
            TColumn prepared;
            if (!Prepare(*info, prepared)) {
                Fail("0A000", "not supported");
                return false;
            }
            KeyTypes.push_back(prepared.Type);
            const auto* group = MapFindPtr(grouped, info->Name);
            if (!group || ranged) {
                break;
            }
            const bool onlyEqual = group->size() == 1 && (*group)[0]->Op == ECompareOp::Eq;
            if (onlyEqual) {
                TCell cell;
                if (!Decode(prepared, (*group)[0]->Value, cell)) {
                    return false;
                }
                FromCells.push_back(cell);
                ToCells.push_back(cell);
                FromSet = true;
                ToSet = true;
                FromInclusive = true;
                ToInclusive = true;
                ++equalPrefix;
                grouped.erase(info->Name);
                continue;
            }
            bool pushed = false;
            TVector<const TComparison*> leftover;
            for (const TComparison* comparison : *group) {
                if (comparison->Op == ECompareOp::NotEq) {
                    leftover.push_back(comparison);
                    continue;
                }
                TCell cell;
                if (!Decode(prepared, comparison->Value, cell)) {
                    return false;
                }
                pushed = true;
                switch (comparison->Op) {
                    case ECompareOp::Eq:
                        TightenLower(prepared, cell, true);
                        TightenUpper(prepared, cell, true);
                        break;
                    case ECompareOp::Gt:
                        TightenLower(prepared, cell, false);
                        break;
                    case ECompareOp::Ge:
                        TightenLower(prepared, cell, true);
                        break;
                    case ECompareOp::Lt:
                        TightenUpper(prepared, cell, false);
                        break;
                    case ECompareOp::Le:
                        TightenUpper(prepared, cell, true);
                        break;
                    case ECompareOp::NotEq:
                        break;
                }
            }
            if (!pushed) {
                break;
            }
            ranged = true;
            if (leftover.empty()) {
                grouped.erase(info->Name);
            } else {
                grouped[info->Name] = std::move(leftover);
            }
            break;
        }
        Point = !ranged && equalPrefix == keys.size();
        KeyPredicate = equalPrefix > 0 || ranged;
        if (KeyPredicate) {
            if (!FromSet) {
                FromCells.assign(KeyTypes.size(), TCell());
                FromInclusive = true;
            }
            if (!ToSet) {
                ToCells.clear();
                ToInclusive = true;
            }
        }

        TVector<TString> parts;
        for (const auto& [name, list] : grouped) {
            Y_UNUSED(name);
            for (const TComparison* comparison : list) {
                parts.push_back(comparison->Column.Name + " " + OpText(comparison->Op) + " " + comparison->Value.Text);
            }
        }
        std::sort(parts.begin(), parts.end());
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i) {
                Residual += " AND ";
            }
            Residual += parts[i];
        }
        return true;
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            Fail("42P01", Path);
            return;
        }
        const auto& entry = request->ResultSet[0];
        const bool column = entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable;
        if (column) {
            if (LockTxId || Statement.Lock != ERowLock::None || Statement.Join || !Statement.Windows.empty()
                || !Statement.Aggs.empty() || !Statement.Group.empty() || !Statement.Having.empty()
                || Statement.Distinct || !Statement.DistinctOn.empty() || Statement.FromQuery
                || !entry.ColumnTableInfo || entry.ColumnTableInfo->Description.GetSharding().ColumnShardsSize() == 0)
            {
                Fail("0A000", "not supported");
                return;
            }
        } else if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            Fail("42P01", Path);
            return;
        }
        if (!Classify(entry)) {
            return;
        }
        if (column) {
            Reply(PlanText(Point, entry.ColumnTableInfo->Description.GetSharding().ColumnShardsSize()));
            return;
        }
        if (!KeyPredicate && entry.Indexes.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        if (!KeyPredicate) {
            Reply("IndexScan");
            return;
        }
        Table = entry.TableId;
        TVector<TKeyDesc::TColumnOp> columns;
        for (const auto& [id, info] : entry.Columns) {
            Y_UNUSED(id);
            if (info.KeyOrder < 0) {
                continue;
            }
            TKeyDesc::TColumnOp operation;
            operation.Column = info.Id;
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = info.PType;
            columns.push_back(operation);
        }
        const TTableRange range = Point
            ? TTableRange(FromCells)
            : TTableRange(FromCells, FromInclusive, ToCells, ToInclusive);
        auto description = MakeHolder<TKeyDesc>(Table, range, TKeyDesc::ERowOperation::Read, KeyTypes, columns);
        auto resolve = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        resolve->DatabaseName = Database;
        resolve->ResultSet.emplace_back(std::move(description));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(resolve.Release()));
        Become(&TExplainActor::StateResolve);
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
            Fail("42P01", Path);
            return;
        }
        const auto& partitions = request->ResultSet[0].KeyDescription->GetPartitions();
        if (partitions.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        Reply(PlanText(Point, partitions.size()));
    }

    STATEFN(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TSelectStmt Statement;
    const ui64 LockTxId;
    TString Path;
    TTableId Table;
    TVector<TString> BoundBytes;
    TVector<TCell> FromCells;
    TVector<TCell> ToCells;
    TVector<NScheme::TTypeInfo> KeyTypes;
    bool FromSet = false;
    bool ToSet = false;
    bool FromInclusive = true;
    bool ToInclusive = true;
    bool Point = false;
    bool KeyPredicate = false;
    TString Residual;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateExplainActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement, ui64 lockTxId) {
    return new TExplainActor(replyTo, std::move(database), std::move(statement), lockTxId);
}

} // namespace NKikimr::NSql
