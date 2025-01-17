#pragma once

#include <ydb/core/scheme_types/scheme_type_info.h>

#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NKikimr {

struct TTableId;

namespace NMiniKQL {

struct TKqpTableColumn {
    ui32 Id;
    TString Name;
    NUdf::TDataTypeId Type;
    bool NotNull;
    NScheme::TTypeInfo TypeInfo;

    TKqpTableColumn(ui32 id, const TStringBuf& name, NUdf::TDataTypeId type, bool notNull, const NScheme::TTypeInfo& typeInfo)
        : Id(id)
        , Name(name)
        , Type(type)
        , NotNull(notNull)
        , TypeInfo(typeInfo) {}
};

using TKqpKeyTuple = TVector<TRuntimeNode>;

struct TKqpKeyRange {
    TKqpKeyTuple FromTuple;
    TKqpKeyTuple ToTuple;
    bool FromInclusive = false;
    bool ToInclusive = false;
    TSmallVec<bool> SkipNullKeys;
    TRuntimeNode ItemsLimit;
    bool Reverse = false;
};

struct TKqpKeyRanges {
    TRuntimeNode Ranges;
    TSmallVec<bool> SkipNullKeys;
    TRuntimeNode ItemsLimit;
    bool Reverse = false;
};

class TKqpProgramBuilder: public TProgramBuilder {
public:
    TKqpProgramBuilder(const TTypeEnvironment& env, const IFunctionRegistry& functionRegistry);

    TRuntimeNode KqpReadTable(const TTableId& tableId, const TKqpKeyRange& range,
        const TArrayRef<TKqpTableColumn>& columns);

    TRuntimeNode KqpWideReadTable(const TTableId& tableId, const TKqpKeyRange& range,
        const TArrayRef<TKqpTableColumn>& columns);

    TRuntimeNode KqpWideReadTableRanges(const TTableId& tableId, const TKqpKeyRanges& range,
        const TArrayRef<TKqpTableColumn>& columns, TType* returnType);

    TRuntimeNode KqpBlockReadTableRanges(const TTableId& tableId, const TKqpKeyRanges& range,
        const TArrayRef<TKqpTableColumn>& columns, TType* returnType);

    TRuntimeNode KqpLookupTable(const TTableId& tableId, const TRuntimeNode& lookupKeys,
        const TArrayRef<TKqpTableColumn>& keyColumns, const TArrayRef<TKqpTableColumn>& columns);

    TRuntimeNode KqpUpsertRows(const TTableId& tableId, const TRuntimeNode& rows,
        const TArrayRef<TKqpTableColumn>& upsertColumns, bool isUpdate);

    TRuntimeNode KqpDeleteRows(const TTableId& tableId, const TRuntimeNode& rows);

    TRuntimeNode KqpEffects(const TArrayRef<const TRuntimeNode>& effects);

    TRuntimeNode KqpEnsure(TRuntimeNode value, TRuntimeNode predicate, TRuntimeNode issueCode, TRuntimeNode message);

    TRuntimeNode KqpIndexLookupJoin(const TRuntimeNode& input, const TString& joinType, const TString& leftLabel, const TString& rightLabel);
};

} // namespace NMiniKQL
} // namespace NKikimr
