#include "kqp_snapshot_tables.h"
#include "kqp_query_data.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/kqp/common/kqp_resolve.h>

#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>

namespace NKikimr::NKqp {
namespace {

using TSnapshotTable = NKikimrTxUserProxy::TCreateVolatileSnapshotTx::TTable;

bool FillBound(const NKqpProto::TKqpPhyKeyBound& bound, const THashSet<TString>& externalParams,
    TQueryData& queryData, NKikimrMiniKQL::TParams& result, TStringBuf& fullTableReason)
{
    result.MutableType()->SetKind(NKikimrMiniKQL::Tuple);
    result.MutableValue();
    for (const auto& value : bound.GetValues()) {
        const NKikimrMiniKQL::TType* type = nullptr;
        const NKikimrMiniKQL::TValue* item = nullptr;
        if (value.HasLiteralValue()) {
            type = &value.GetLiteralValue().GetType();
            item = &value.GetLiteralValue().GetValue();
        } else {
            TString name;
            if (value.HasParamValue()) {
                name = value.GetParamValue().GetParamName();
            } else if (value.HasParamElementValue()) {
                name = value.GetParamElementValue().GetParamName();
            } else {
                fullTableReason = "unsupported_bound_value";
                return false;
            }
            // Do not materialize bindings to computation results or internal
            // values here: snapshot acquisition precedes query execution.
            if (!externalParams.contains(name)) {
                fullTableReason = "non_external_bound_parameter";
                return false;
            }
            const auto* parameter = queryData.GetParameterMiniKqlValue(name);
            if (!parameter) {
                fullTableReason = "missing_external_bound_parameter";
                return false;
            }
            type = &parameter->GetType();
            item = &parameter->GetValue();
            if (value.HasParamElementValue()) {
                const auto index = value.GetParamElementValue().GetElementIndex();
                if (type->GetKind() != NKikimrMiniKQL::Tuple ||
                    index >= type->GetTuple().ElementSize() || index >= item->TupleSize())
                {
                    fullTableReason = "invalid_bound_parameter_element";
                    return false;
                }
                type = &type->GetTuple().GetElement(index);
                item = &item->GetTuple(index);
            }
        }

        // TxProxy's key tuple format expects optional elements, including for
        // NOT NULL keys. KQP's physical bounds may contain non-optional values.
        auto* elementType = result.MutableType()->MutableTuple()->AddElement();
        auto* element = result.MutableValue()->AddTuple();
        if (type->GetKind() == NKikimrMiniKQL::Data) {
            elementType->SetKind(NKikimrMiniKQL::Optional);
            elementType->MutableOptional()->MutableItem()->CopyFrom(*type);
            element->MutableOptional()->CopyFrom(*item);
        } else if (type->GetKind() == NKikimrMiniKQL::Optional &&
            type->GetOptional().GetItem().GetKind() == NKikimrMiniKQL::Data)
        {
            elementType->CopyFrom(*type);
            element->CopyFrom(*item);
        } else {
            fullTableReason = "unsupported_bound_type";
            return false;
        }
    }
    return true;
}

bool ParseBound(const NKikimrMiniKQL::TParams& bound, const TVector<NScheme::TTypeInfo>& keyTypes,
    bool expandWithNulls, TSerializedCellVec& result)
{
    TVector<TCell> cells;
    TVector<TString> memoryOwner;
    TString error;
    if (!NMiniKQL::CellsFromTuple(&bound.GetType(), bound.GetValue(), keyTypes,
        {}, false, cells, error, memoryOwner))
    {
        return false;
    }
    if (expandWithNulls) {
        cells.resize(keyTypes.size());
    }
    result = TSerializedCellVec(cells);
    return true;
}

struct TSnapshotRange {
    NKikimrTxUserProxy::TKeyRange Proto;
    TSerializedCellVec From;
    TSerializedCellVec To;
};

TMaybe<TSnapshotRange> MakeSnapshotRange(const NKqpProto::TKqpPhyKeyRange& range,
    const TTableConstInfo& table, const THashSet<TString>& externalParams, TQueryData& queryData,
    TStringBuf& fullTableReason)
{
    // The compiler also sets RangeIsPoint for identical inclusive key prefixes.
    // Like ExtractRanges in kqp_partition_helper.cpp, only treat a complete key
    // as a point; a shorter prefix still describes a range of rows.
    const bool isPoint = range.GetRangeIsPoint() &&
        range.GetFrom().ValuesSize() == table.KeyColumnTypes.size();
    if (!range.HasFrom() || (!isPoint && !range.HasTo())) {
        fullTableReason = "missing_range_bound";
        return Nothing();
    }
    TSnapshotRange result;
    if (!FillBound(range.GetFrom(), externalParams, queryData, *result.Proto.MutableFrom(), fullTableReason)) {
        return Nothing();
    }
    if (isPoint) {
        result.Proto.MutableTo()->CopyFrom(result.Proto.GetFrom());
        result.Proto.SetFromInclusive(true);
        result.Proto.SetToInclusive(true);
    } else {
        if (!FillBound(range.GetTo(), externalParams, queryData, *result.Proto.MutableTo(), fullTableReason)) {
            return Nothing();
        }
        result.Proto.SetFromInclusive(range.GetFrom().GetIsInclusive());
        result.Proto.SetToInclusive(range.GetTo().GetIsInclusive());
    }
    if (!ParseBound(result.Proto.GetFrom(), table.KeyColumnTypes, result.Proto.GetFromInclusive(), result.From) ||
        !ParseBound(result.Proto.GetTo(), table.KeyColumnTypes, !result.Proto.GetToInclusive(), result.To))
    {
        fullTableReason = "bound_type_mismatch";
        return Nothing();
    }
    // TxProxy rejects empty ranges. Preserve the existing full-table snapshot
    // for these reads rather than turning a valid empty SELECT into an error.
    if (TTableRange(result.From.GetCells(), result.Proto.GetFromInclusive(),
        result.To.GetCells(), result.Proto.GetToInclusive()).IsEmptyRange(table.KeyColumnTypes))
    {
        fullTableReason = "empty_range";
        return Nothing();
    }
    return result;
}

struct TTableReads {
    const TTableConstInfo* Info = nullptr;
    bool FullTable = false;
    TStringBuf FullTableReason;
    size_t KeyRangeReads = 0;
    size_t ComputedRangeReads = 0;
    TMaybe<TSnapshotRange> Range;

    void RequireFullTable(TStringBuf reason) {
        if (!FullTable) {
            FullTableReason = reason;
        }
        FullTable = true;
        Range.Clear();
    }

    void AddRange(const NKqpProto::TKqpPhyKeyRange& range,
        const THashSet<TString>& externalParams, TQueryData& queryData)
    {
        ++KeyRangeReads;
        if (FullTable) {
            return;
        }
        if (!Info || Info->TableKind != ETableKind::Datashard || Info->KeyColumnTypes.empty()) {
            return RequireFullTable("unsupported_table_metadata");
        }
        TStringBuf reason;
        auto next = MakeSnapshotRange(range, *Info, externalParams, queryData, reason);
        if (!next) {
            return RequireFullTable(reason);
        }
        if (!Range) {
            Range = std::move(next);
            return;
        }

        // A single enclosing range is sufficient: snapshot participation is
        // selected by shards, and it is safe to include additional shards.
        const auto& types = Info->KeyColumnTypes;
        if (CompareBorders<false, false>(next->From.GetCells(), Range->From.GetCells(),
            next->Proto.GetFromInclusive(), Range->Proto.GetFromInclusive(), types) < 0)
        {
            Range->From = std::move(next->From);
            Range->Proto.MutableFrom()->CopyFrom(next->Proto.GetFrom());
            Range->Proto.SetFromInclusive(next->Proto.GetFromInclusive());
        }
        if (CompareBorders<true, true>(next->To.GetCells(), Range->To.GetCells(),
            next->Proto.GetToInclusive(), Range->Proto.GetToInclusive(), types) > 0)
        {
            Range->To = std::move(next->To);
            Range->Proto.MutableTo()->CopyFrom(next->Proto.GetTo());
            Range->Proto.SetToInclusive(next->Proto.GetToInclusive());
        }
    }
};

} // namespace

TVector<TSnapshotTable> BuildSnapshotTables(const TPreparedQueryHolder& preparedQuery, TQueryData& queryData,
    TVector<TSnapshotTableRangeInfo>* rangeInfo)
{
    THashMap<TString, TTableReads> reads;
    for (const auto& path : preparedQuery.GetQueryTables()) {
        reads.emplace(path, TTableReads{});
    }
    for (const auto& [_, info] : preparedQuery.GetTableConstInfo()) {
        if (auto* table = reads.FindPtr(info->Path)) {
            table->Info = info.Get();
        }
    }
    for (const auto& tx : preparedQuery.GetPhysicalQuery().GetTransactions()) {
        THashSet<TString> externalParams;
        for (const auto& binding : tx.GetParamBindings()) {
            if (binding.HasExternalBinding()) {
                externalParams.insert(binding.GetName());
            }
        }
        for (const auto& stage : tx.GetStages()) {
            for (const auto& op : stage.GetTableOps()) {
                auto& table = reads[op.GetTable().GetPath()];
                if (op.HasReadRange()) {
                    table.AddRange(op.GetReadRange().GetKeyRange(), externalParams, queryData);
                } else if (op.HasReadRanges()) {
                    if (op.GetReadRanges().HasKeyRanges()) {
                        ++table.ComputedRangeReads;
                        table.RequireFullTable("computed_read_ranges");
                    } else {
                        table.RequireFullTable("unbounded_read");
                    }
                } else {
                    table.RequireFullTable("unsupported_table_operation");
                }
            }
            for (const auto& source : stage.GetSources()) {
                if (source.HasReadRangesSource()) {
                    const auto& read = source.GetReadRangesSource();
                    auto& table = reads[read.GetTable().GetPath()];
                    if (read.HasKeyRange()) {
                        table.AddRange(read.GetKeyRange(), externalParams, queryData);
                    } else if (read.HasRanges()) {
                        ++table.ComputedRangeReads;
                        table.RequireFullTable("computed_source_ranges");
                    } else {
                        table.RequireFullTable("unbounded_source_read");
                    }
                }
            }
            for (const auto& input : stage.GetInputs()) {
                if (input.HasStreamLookup()) {
                    reads[input.GetStreamLookup().GetTable().GetPath()].RequireFullTable("stream_lookup");
                }
                if (input.HasSequencer()) {
                    reads[input.GetSequencer().GetTable().GetPath()].RequireFullTable("sequencer");
                }
            }
            for (const auto& sink : stage.GetSinks()) {
                if (sink.HasInternalSink() && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    if (sink.GetInternalSink().GetSettings().UnpackTo(&settings)) {
                        reads[settings.GetTable().GetPath()].RequireFullTable("table_sink");
                    }
                }
            }
        }
    }

    TVector<TSnapshotTable> result;
    result.reserve(preparedQuery.GetQueryTables().size());
    if (rangeInfo) {
        rangeInfo->clear();
        rangeInfo->reserve(preparedQuery.GetQueryTables().size());
    }
    for (const auto& path : preparedQuery.GetQueryTables()) {
        auto& table = result.emplace_back();
        table.SetTablePath(path);
        const auto& read = reads.at(path);
        if (!read.FullTable && read.Range) {
            table.MutableKeyRange()->CopyFrom(read.Range->Proto);
        }
        if (rangeInfo) {
            auto& info = rangeInfo->emplace_back();
            info.TablePath = path;
            if (!table.HasKeyRange()) {
                info.FullTableReason = read.FullTable ? read.FullTableReason : TStringBuf("no_key_range");
            }
            info.KeyRangeReads = read.KeyRangeReads;
            info.ComputedRangeReads = read.ComputedRangeReads;
        }
    }
    return result;
}

} // namespace NKikimr::NKqp
