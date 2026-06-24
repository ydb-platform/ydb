#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_log.h>

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/join_defs.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple.h>
#include <ydb/library/yql/dq/comp_nodes/type_utils.h>

namespace NKikimr::NMiniKQL {

inline TString DescribeType(const TType* type) {
    if (!type) {
        return "null";
    }
    if (type->IsOptional()) {
        auto* opt = static_cast<const TOptionalType*>(type);
        return TStringBuilder() << "Optional<" << DescribeType(opt->GetItemType()) << ">";
    }
    if (type->IsData()) {
        auto* data = static_cast<const TDataType*>(type);
        auto slot = data->GetDataSlot();
        if (slot) {
            return TString(NYql::NUdf::GetDataTypeInfo(*slot).Name);
        }
        return TStringBuilder() << "Data(" << data->GetSchemeType() << ")";
    }
    return TString(type->GetKindAsStr());
}

inline TString DescribeJoinKind(EJoinKind kind) {
    switch (kind) {
        case EJoinKind::Inner: return "Inner";
        case EJoinKind::Left: return "Left";
        case EJoinKind::LeftOnly: return "LeftOnly";
        case EJoinKind::LeftSemi: return "LeftSemi";
        case EJoinKind::Right: return "Right";
        case EJoinKind::RightOnly: return "RightOnly";
        case EJoinKind::RightSemi: return "RightSemi";
        case EJoinKind::Full: return "Full";
        case EJoinKind::Exclusion: return "Exclusion";
        case EJoinKind::Cross: return "Cross";
        default: return Sprintf("Unknown(%u)", static_cast<ui32>(kind));
    }
}

inline TString DescribeColumnLayout(const NPackedTuple::TColumnDesc& col) {
    TStringBuilder sb;
    sb << "{idx=" << col.ColumnIndex
       << " orig=" << col.OriginalColumnIndex
       << " role=" << (col.Role == NPackedTuple::EColumnRole::Key ? "Key" : "Payload")
       << " " << (col.SizeType == NPackedTuple::EColumnSizeType::Fixed ? "Fixed" : "Var")
       << " size=" << col.DataSize
       << " off=" << col.Offset << "}";
    return sb;
}

inline TString DescribeTupleLayout(const NPackedTuple::TTupleLayout* layout) {
    if (!layout) {
        return "null";
    }
    TStringBuilder sb;
    sb << "TupleLayout: totalRowSize=" << layout->TotalRowSize
       << " keys=" << layout->KeyColumnsNum << " (size=" << layout->KeyColumnsSize
       << " off=" << layout->KeyColumnsOffset << ".." << layout->KeyColumnsEnd << ")"
       << " bitmask=" << layout->BitmaskSize << " (off=" << layout->BitmaskOffset << ".." << layout->BitmaskEnd << ")"
       << " payload=" << layout->PayloadSize << " (off=" << layout->PayloadOffset << ".." << layout->PayloadEnd << ")"
       << "\n  Columns(" << layout->Columns.size() << "):";
    for (const auto& col : layout->Columns) {
        sb << "\n    " << DescribeColumnLayout(col);
    }
    return sb;
}

inline void LogBlockHashJoinStructure(const NUdf::TLoggerPtr& logger, NUdf::TLogComponentId logComponent,
                                      EJoinKind kind, bool leftIsBuild,
                                      const TSides<TVector<TType*>>& userTypes,
                                      const TSides<TVector<int>>& keyColumns,
                                      const TDqRenames<ESide>& renames,
                                      TSides<const NPackedTuple::TTupleLayout*> layouts) {
    if (!logger || !logger->IsActive(logComponent, NUdf::ELogLevel::Debug)) {
        return;
    }

    TStringBuilder sb;
    sb << "BlockHashJoin structure: joinKind=" << DescribeJoinKind(kind)
       << " buildSide=" << (leftIsBuild ? "Left" : "Right");

    for (ESide side : EachSide) {
        const auto& types = userTypes.SelectSide(side);
        const auto& keys = keyColumns.SelectSide(side);
        sb << "\n  " << AsString(side) << " side: " << types.size() << " columns, "
           << keys.size() << " key columns [";
        for (size_t i = 0; i < keys.size(); ++i) {
            if (i > 0) sb << ", ";
            sb << keys[i];
        }
        sb << "]";
        for (size_t i = 0; i < types.size(); ++i) {
            sb << "\n    col[" << i << "]: " << DescribeType(types[i]);
            bool isKey = std::find(keys.begin(), keys.end(), static_cast<int>(i)) != keys.end();
            if (isKey) sb << " [KEY]";
        }
    }

    sb << "\n  Renames(" << renames.size() << "):";
    for (const auto& r : renames) {
        sb << " {" << AsString(r.Side) << "[" << r.Index << "]}";
    }

    for (ESide side : EachSide) {
        sb << "\n  " << AsString(side) << " " << DescribeTupleLayout(layouts.SelectSide(side));
    }

    logger->Log(logComponent, NUdf::ELogLevel::Debug, sb);
}

} // namespace NKikimr::NMiniKQL
