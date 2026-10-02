#include "key_range_predicate.h"

#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/string/join.h>
#include <util/string/subst.h>
#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/yexception.h>

#include <string>

namespace NKikimr::NStat {

bool CanEncodeKeyBoundType(NScheme::TTypeId typeId) {
    switch (typeId) {
    case NScheme::NTypeIds::Bool:
    case NScheme::NTypeIds::Int8:
    case NScheme::NTypeIds::Uint8:
    case NScheme::NTypeIds::Int16:
    case NScheme::NTypeIds::Uint16:
    case NScheme::NTypeIds::Int32:
    case NScheme::NTypeIds::Uint32:
    case NScheme::NTypeIds::Int64:
    case NScheme::NTypeIds::Uint64:
    case NScheme::NTypeIds::Date:
    case NScheme::NTypeIds::Datetime:
    case NScheme::NTypeIds::Timestamp:
    case NScheme::NTypeIds::Interval:
    case NScheme::NTypeIds::Date32:
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
    case NScheme::NTypeIds::Interval64:
    case NScheme::NTypeIds::String:
    case NScheme::NTypeIds::Utf8:
    case NScheme::NTypeIds::Uuid:
    case NScheme::NTypeIds::DyNumber:
    case NScheme::NTypeIds::Decimal:
        return true;
    default:
        return false;
    }
}

bool CanEncodeKeyRangePredicate(TConstArrayRef<NScheme::TTypeInfo> keyColumnTypes) {
    return !keyColumnTypes.empty() && AllOf(keyColumnTypes, [](const NScheme::TTypeInfo& type) {
        return CanEncodeKeyBoundType(type.GetTypeId());
    });
}

namespace {

TString EscapeYqlId(TStringBuf identifier) {
    auto escaped = EscapeC(identifier);
    SubstGlobal(escaped, "`", "\\`");
    return TStringBuilder() << '`' << escaped << '`';
}

void AddDecimalParam(
    NYdb::TParamsBuilder& params,
    const std::string& paramName,
    const NScheme::TTypeInfo& typeInfo,
    const TCell& cell)
{
    NYdb::TDecimalType decimalType{
        static_cast<ui8>(typeInfo.GetDecimalType().GetPrecision()),
        static_cast<ui8>(typeInfo.GetDecimalType().GetScale())
    };
    const auto loHi = cell.AsValue<std::pair<ui64, i64>>();
    Ydb::Value valueProto;
    valueProto.set_low_128(loHi.first);
    valueProto.set_high_128(loHi.second);
    NYdb::TValueBuilder vb;
    vb.Decimal({valueProto, decimalType});
    params.AddParam(paramName, vb.Build());
}

bool AddCellParam(
    NYdb::TParamsBuilder& params,
    TString& declares,
    const TString& paramName,
    const NScheme::TTypeInfo& typeInfo,
    const TCell& cell,
    TString& error)
{
    Y_ENSURE(!cell.IsNull());
    if (!CanEncodeKeyBoundType(typeInfo.GetTypeId())) {
        error = TStringBuilder() << "unsupported key column type "
            << NScheme::TypeName(typeInfo) << " in a PK range predicate";
        return false;
    }

    const std::string sdkParamName(paramName.data(), paramName.size());
    if (typeInfo.GetTypeId() == NScheme::NTypeIds::Decimal) {
        AddDecimalParam(params, sdkParamName, typeInfo, cell);
    } else if (typeInfo.GetTypeId() == NScheme::NTypeIds::DyNumber) {
        // YQL DyNumber parameters are text; cell bytes are the binary form.
        auto text = NDyNumber::DyNumberToString(cell.AsBuf());
        if (!text) {
            error = "invalid DyNumber key bound";
            return false;
        }
        NYdb::TValueBuilder vb;
        vb.DyNumber(std::string(text->data(), text->size()));
        params.AddParam(sdkParamName, vb.Build());
    } else {
        NYdb::TValueBuilder vb;
        ProtoValueFromCell(vb, typeInfo, cell);
        params.AddParam(sdkParamName, vb.Build());
    }

    declares += TStringBuilder()
        << "DECLARE " << paramName << " AS " << NScheme::TypeName(typeInfo) << ";\n";
    return true;
}

TString JoinAnd(const TVector<TString>& parts) {
    if (parts.empty()) {
        return "TRUE";
    }
    if (parts.size() == 1) {
        return parts[0];
    }
    return TStringBuilder() << '(' << JoinSeq(" AND ", parts) << ')';
}

TString JoinOr(const TVector<TString>& parts) {
    if (parts.empty()) {
        return "FALSE";
    }
    if (parts.size() == 1) {
        return parts[0];
    }
    return TStringBuilder() << '(' << JoinSeq(" OR ", parts) << ')';
}

// DataShard: NULL == NULL and NULL < non-NULL. Empty rhs[i] is a NULL bound cell.
TString BuildLexicographicBoundExpr(
    TConstArrayRef<TString> columns,
    TConstArrayRef<TString> rhs,
    TStringBuf op)
{
    const bool wantLess = op == "<" || op == "<=";
    const bool inclusive = op == "<=" || op == ">=";

    TVector<TString> terms;
    TVector<TString> prefixEq;
    terms.reserve(columns.size() + 1);
    prefixEq.reserve(columns.size());

    for (size_t i = 0; i < columns.size(); ++i) {
        const bool last = (i + 1 == columns.size());
        TString cmp;
        if (wantLess) {
            if (!rhs[i].empty()) {
                const TStringBuf cmpOp = (inclusive && last) ? "<=" : "<";
                cmp = TStringBuilder() << '(' << columns[i] << " IS NULL OR "
                    << columns[i] << " " << cmpOp << " " << rhs[i] << ')';
            }
        } else if (rhs[i].empty()) {
            cmp = TStringBuilder() << columns[i] << " IS NOT NULL";
        } else {
            const TStringBuf cmpOp = (inclusive && last) ? ">=" : ">";
            cmp = TStringBuilder() << columns[i] << " " << cmpOp << " " << rhs[i];
        }

        if (cmp) {
            TVector<TString> term = prefixEq;
            term.push_back(std::move(cmp));
            terms.push_back(JoinAnd(term));
        }

        if (rhs[i].empty()) {
            prefixEq.push_back(TStringBuilder() << columns[i] << " IS NULL");
        } else {
            prefixEq.push_back(TStringBuilder() << columns[i] << " = " << rhs[i]);
        }
    }

    // Inclusive equality is folded into <= / >= on the last non-NULL component.
    if (inclusive && !rhs.empty() && rhs.back().empty()) {
        terms.push_back(JoinAnd(prefixEq));
    }
    return JoinOr(terms);
}

// Trailing NULLs pad DataShard split keys to full PK width.
// k < (P, NULL, ...) <=> k < P; k >= (P, NULL, ...) <=> k >= P.
TConstArrayRef<TCell> StripEquivalentTrailingNulls(TConstArrayRef<TCell> cells, TStringBuf op) {
    if (op != ">=" && op != "<") {
        return cells;
    }
    while (!cells.empty() && cells.back().IsNull()) {
        cells = cells.first(cells.size() - 1);
    }
    return cells;
}

bool BuildBoundExpr(
    TStringBuf paramPrefix,
    TConstArrayRef<TString> keyColumnNames,
    TConstArrayRef<NScheme::TTypeInfo> keyColumnTypes,
    TConstArrayRef<TCell> cells,
    TStringBuf op,
    NYdb::TParamsBuilder& params,
    TString& declares,
    TString& expr,
    TString& error)
{
    if (cells.empty()) {
        expr.clear();
        return true;
    }
    if (cells.size() > keyColumnNames.size()) {
        error = TStringBuilder() << "key bound has " << cells.size()
            << " components, table PK has " << keyColumnNames.size();
        return false;
    }

    const TConstArrayRef<TCell> bound = StripEquivalentTrailingNulls(cells, op);
    if (bound.empty()) {
        // >= min key is unbounded below; < min key matches nothing.
        expr = (op == "<") ? TString("FALSE") : TString();
        return true;
    }

    TVector<TString> columns;
    TVector<TString> rhs;
    columns.reserve(bound.size());
    rhs.reserve(bound.size());
    bool anyNull = false;
    for (size_t i = 0; i < bound.size(); ++i) {
        if (keyColumnNames[i].empty()) {
            error = TStringBuilder() << "missing name for key column at position " << i;
            return false;
        }
        columns.push_back(EscapeYqlId(keyColumnNames[i]));
        if (bound[i].IsNull()) {
            anyNull = true;
            rhs.emplace_back();
            continue;
        }
        const TString paramName = TStringBuilder() << "$" << paramPrefix << "_" << i;
        if (!AddCellParam(params, declares, paramName, keyColumnTypes[i], bound[i], error)) {
            return false;
        }
        rhs.push_back(paramName);
    }

    if (anyNull || op == "<" || op == "<=") {
        expr = BuildLexicographicBoundExpr(columns, rhs, op);
    } else if (bound.size() == 1) {
        expr = TStringBuilder() << columns[0] << " " << op << " " << rhs[0];
    } else {
        expr = TStringBuilder()
            << "AsTuple(" << JoinSeq(",", columns) << ") "
            << op
            << " AsTuple(" << JoinSeq(",", rhs) << ")";
    }
    return true;
}

} // anonymous namespace

TVector<std::pair<ui64, TSerializedTableRange>> MakeShardSubranges(
    const TVector<TKeyDesc::TPartitionInfo>& partitions)
{
    TVector<std::pair<ui64, TSerializedTableRange>> result;
    result.reserve(partitions.size());

    TSerializedCellVec prevEnd;
    bool prevInclusive = false;
    for (const auto& part : partitions) {
        TSerializedTableRange range;
        if (!result.empty()) {
            range.From = prevEnd;
            range.FromInclusive = !prevInclusive;
        }
        if (part.Range && part.Range->EndKeyPrefix) {
            range.To = part.Range->EndKeyPrefix;
            range.ToInclusive = part.Range->IsInclusive || part.Range->IsPoint;
            prevEnd = part.Range->EndKeyPrefix;
            prevInclusive = range.ToInclusive;
        }
        result.emplace_back(part.ShardId, std::move(range));
    }
    return result;
}

TVector<std::pair<ui64, TSerializedTableRange>> MakeBudgetedSubranges(
    const TVector<TKeyDesc::TPartitionInfo>& partitions,
    std::optional<ui64> tableBytesSize,
    ui64 rangeBudgetBytes)
{
    auto shardRanges = MakeShardSubranges(partitions);
    if (shardRanges.empty() || rangeBudgetBytes == 0 || !tableBytesSize || *tableBytesSize == 0) {
        return shardRanges;
    }

    ui64 target = (*tableBytesSize - 1) / rangeBudgetBytes + 1;
    target = Min<ui64>(target, MaxAnalyzeRowTableSubranges);
    const ui64 shardCount = shardRanges.size();
    if (target >= shardCount) {
        return shardRanges;
    }

    TVector<std::pair<ui64, TSerializedTableRange>> result;
    result.reserve(target);
    ui64 offset = 0;
    for (ui64 g = 0; g < target; ++g) {
        const ui64 count = shardCount / target + (g < shardCount % target ? 1 : 0);
        TSerializedTableRange merged;
        merged.From = shardRanges[offset].second.From;
        merged.FromInclusive = shardRanges[offset].second.FromInclusive;
        const ui64 last = offset + count - 1;
        merged.To = shardRanges[last].second.To;
        merged.ToInclusive = shardRanges[last].second.ToInclusive;
        result.emplace_back(shardRanges[offset].first, std::move(merged));
        offset += count;
    }
    return result;
}

bool TryBuildKeyRangePredicate(
    TConstArrayRef<TString> keyColumnNames,
    TConstArrayRef<NScheme::TTypeInfo> keyColumnTypes,
    const TSerializedTableRange& range,
    TString& where,
    TString& declares,
    NYdb::TParamsBuilder& params,
    TString& error)
{
    where.clear();
    declares.clear();
    error.clear();

    if (keyColumnNames.size() != keyColumnTypes.size()) {
        error = TStringBuilder() << "key column names/types size mismatch: "
            << keyColumnNames.size() << " vs " << keyColumnTypes.size();
        return false;
    }
    if (keyColumnNames.empty()) {
        error = "table has no primary key columns";
        return false;
    }

    TString fromExpr;
    TString toExpr;
    const TStringBuf fromOp = range.FromInclusive ? ">=" : ">";
    const TStringBuf toOp = range.ToInclusive ? "<=" : "<";
    if (!BuildBoundExpr("from", keyColumnNames, keyColumnTypes, range.From.GetCells(),
            fromOp, params, declares, fromExpr, error)) {
        return false;
    }
    if (!BuildBoundExpr("to", keyColumnNames, keyColumnTypes, range.To.GetCells(),
            toOp, params, declares, toExpr, error)) {
        return false;
    }

    if (fromExpr && toExpr) {
        where = TStringBuilder() << fromExpr << " AND " << toExpr;
    } else if (fromExpr) {
        where = std::move(fromExpr);
    } else if (toExpr) {
        where = std::move(toExpr);
    }
    return true;
}

} // namespace NKikimr::NStat
