#include "kqp_mkql_compiler.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NNodes;
using namespace NKikimr::NMiniKQL;

namespace {

TVector<TKqpTableColumn> GetKqpColumns(const TKikimrTableMetadata& table, const TVector<TStringBuf>& columnNames,
    bool allowSystemColumns)
{
    TVector<TKqpTableColumn> pgmColumns;
    for (const auto& name : columnNames) {
        ui32 columnId = 0;
        ui32 columnType = 0;
        bool notNull = false;
        void* columnTypeDesc = nullptr;

        auto columnData = table.Columns.FindPtr(name);
        if (columnData) {
            columnId = columnData->Id;
            columnType = columnData->TypeInfo.GetTypeId();
            if (columnType == NScheme::NTypeIds::Pg) {
                columnTypeDesc = columnData->TypeInfo.GetTypeDesc();
            }
            notNull = columnData->NotNull;
        } else if (allowSystemColumns) {
            auto systemColumn = GetSystemColumns().find(name);
            YQL_ENSURE(systemColumn != GetSystemColumns().end());
            columnId = systemColumn->second.ColumnId;
            columnType = systemColumn->second.TypeId;
        }

        YQL_ENSURE(columnId, "Unknown column: " << name);
        pgmColumns.emplace_back(columnId, name, columnType, notNull, columnTypeDesc);
    }

    return pgmColumns;
}

TVector<TKqpTableColumn> GetKqpColumns(const TKikimrTableMetadata& table, const TCoAtomList& columns,
    bool allowSystemColumns)
{
    TVector<TStringBuf> columnNames(columns.Size());
    for (ui32 i = 0; i < columnNames.size(); ++i) {
        columnNames[i] = columns.Item(i).Value();
    }

    return GetKqpColumns(table, columnNames, allowSystemColumns);
}

TSmallVec<bool> GetSkipNullKeys(const TKqpReadTableSettings& settings, const TKikimrTableMetadata& tableMeta) {
    TSmallVec<bool> skipNullKeys(tableMeta.KeyColumnNames.size(), false);

    for (const auto& key : settings.SkipNullKeys) {
        size_t keyIndex = FindIndex(tableMeta.KeyColumnNames, key);
        YQL_ENSURE(keyIndex != NPOS);
        skipNullKeys[keyIndex] = true;
    }

    return skipNullKeys;
}

NMiniKQL::TType* CreateColumnType(const NKikimr::NScheme::TTypeInfo& typeInfo, const TKqlCompileContext& ctx) {
    auto typeId = typeInfo.GetTypeId();
    if (typeId == NUdf::TDataType<NUdf::TDecimal>::Id) {
        return ctx.PgmBuilder().NewDecimalType(22, 9);
    } else if (typeId == NKikimr::NScheme::NTypeIds::Pg) {
        return ctx.PgmBuilder().NewPgType(NPg::PgTypeIdFromTypeDesc(typeInfo.GetTypeDesc()));
    } else {
        return ctx.PgmBuilder().NewDataType(typeId);
    }
}

void ValidateColumnType(const TTypeAnnotationNode* type, NKikimr::NScheme::TTypeId columnTypeId) {
    YQL_ENSURE(type);
    bool isOptional;
    if (columnTypeId == NKikimr::NScheme::NTypeIds::Pg) {
        const TPgExprType* pgType = nullptr;
        YQL_ENSURE(IsPg(type, pgType));
    } else {
        const TDataExprType* dataType = nullptr;
        YQL_ENSURE(IsDataOrOptionalOfData(type, isOptional, dataType));
        auto schemeType = NUdf::GetDataTypeInfo(dataType->GetSlot()).TypeId;
        YQL_ENSURE(schemeType == columnTypeId);
    }
}

void ValidateColumnsType(const TStreamExprType* streamType, const TKikimrTableMetadata& tableMeta) {
    YQL_ENSURE(streamType);
    auto rowType = streamType->GetItemType()->Cast<TStructExprType>();

    for (auto* member : rowType->GetItems()) {
        auto columnData = tableMeta.Columns.FindPtr(member->GetName());
        YQL_ENSURE(columnData);
        auto columnDataType = columnData->TypeInfo.GetTypeId();
        YQL_ENSURE(columnDataType != 0);
        ValidateColumnType(member->GetItemType(), columnDataType);
    }
}

void ValidateRangeBoundType(const TTupleExprType* keyTupleType, const TKikimrTableMetadata& tableMeta) {
    YQL_ENSURE(keyTupleType);
    YQL_ENSURE(keyTupleType->GetSize() == tableMeta.KeyColumnNames.size() + 1);

    for (ui32 i = 0; i < tableMeta.KeyColumnNames.size(); ++i) {
        auto columnData = tableMeta.Columns.FindPtr(tableMeta.KeyColumnNames[i]);
        YQL_ENSURE(columnData);
        auto columnDataType = columnData->TypeInfo.GetTypeId();
        YQL_ENSURE(columnDataType != 0);

        ValidateColumnType(keyTupleType->GetItems()[i]->Cast<TOptionalExprType>()->GetItemType(), columnDataType);
    }
}

void ValidateRangesType(const TTypeAnnotationNode* rangesType, const TKikimrTableMetadata& tableMeta) {
    YQL_ENSURE(rangesType);
    if (rangesType->GetKind() == ETypeAnnotationKind::Void) {
        return;
    }

    auto tupleType = rangesType->Cast<TTupleExprType>();
    YQL_ENSURE(tupleType->GetSize() == 1);

    auto rangeType = tupleType->GetItems()[0]->Cast<TListExprType>()->GetItemType()->Cast<TTupleExprType>();
    YQL_ENSURE(rangeType->GetSize() == 2);

    ValidateRangeBoundType(rangeType->GetItems()[0]->Cast<TTupleExprType>(), tableMeta);
    ValidateRangeBoundType(rangeType->GetItems()[1]->Cast<TTupleExprType>(), tableMeta);
}

TKqpKeyRange MakeKeyRange(const TKqlReadTableBase& readTable, const TKqlCompileContext& ctx,
    TMkqlBuildContext& buildCtx)
{
    const auto& fromTuple = readTable.Range().From();
    const auto& toTuple = readTable.Range().To();

    bool fromInclusive = readTable.Range().From().Maybe<TKqlKeyInc>().IsValid();
    bool toInclusive = readTable.Range().To().Maybe<TKqlKeyInc>().IsValid();

    const auto& tableMeta = ctx.GetTableMeta(readTable.Table());

    TVector<TRuntimeNode> fromValues;
    TVector<TRuntimeNode> toValues;
    for (ui32 i = 0; i < tableMeta.KeyColumnNames.size(); ++i) {
        auto keyColumn = tableMeta.KeyColumnNames[i];

        auto columnData = tableMeta.Columns.FindPtr(keyColumn);
        YQL_ENSURE(columnData);
        auto columnDataType = columnData->TypeInfo.GetTypeId();

        YQL_ENSURE(columnDataType != 0);
        auto columnType = CreateColumnType(columnData->TypeInfo, ctx);

        if (fromTuple.ArgCount() > i) {
            ValidateColumnType(fromTuple.Arg(i).Ref().GetTypeAnn(), columnDataType);
            fromValues.push_back(MkqlBuildExpr(fromTuple.Arg(i).Ref(), buildCtx));
        } else if (fromInclusive) {
            fromValues.push_back(ctx.PgmBuilder().NewEmptyOptional(
                ctx.PgmBuilder().NewOptionalType(columnType)));
        }

        if (toTuple.ArgCount() > i) {
            ValidateColumnType(toTuple.Arg(i).Ref().GetTypeAnn(), columnDataType);
            toValues.push_back(MkqlBuildExpr(toTuple.Arg(i).Ref(), buildCtx));
        } else if (!toInclusive) {
            toValues.push_back(ctx.PgmBuilder().NewEmptyOptional(
                ctx.PgmBuilder().NewOptionalType(columnType)));
        }
    }

    auto settings = TKqpReadTableSettings::Parse(readTable);

    TKqpKeyRange keyRange;
    keyRange.FromInclusive = fromInclusive;
    keyRange.ToInclusive = toInclusive;
    keyRange.FromTuple = fromValues;
    keyRange.ToTuple = toValues;
    keyRange.SkipNullKeys = GetSkipNullKeys(settings, tableMeta);
    if (settings.ItemsLimit) {
        keyRange.ItemsLimit = MkqlBuildExpr(*settings.ItemsLimit, buildCtx);
    }
    keyRange.Reverse = settings.Reverse;

    return keyRange;
}

TKqpKeyRanges MakeComputedKeyRanges(const TKqlReadTableRangesBase& readTable, const TKqlCompileContext& ctx,
    TMkqlBuildContext& buildCtx)
{
    auto settings = TKqpReadTableSettings::Parse(readTable);

    TKqpKeyRanges ranges = {
        .Ranges = MkqlBuildExpr(readTable.Ranges().Ref(), buildCtx),
        .ItemsLimit = settings.ItemsLimit ? MkqlBuildExpr(*settings.ItemsLimit, buildCtx) : ctx.PgmBuilder().NewNull(),
        .Reverse = settings.Reverse,
    };

    return ranges;
}

} // namespace

const TKikimrTableMetadata& TKqlCompileContext::GetTableMeta(const TKqpTable& table) const {
    auto& tableData = TablesData_->ExistingTable(Cluster_, table.Path());
    YQL_ENSURE(tableData.Metadata);
    auto& meta = *tableData.Metadata;
    YQL_ENSURE(meta.PathId.ToString() == table.PathId().Value());
    YQL_ENSURE(meta.SysView == table.SysView().Value());
    YQL_ENSURE(meta.SchemaVersion == FromString<ui64>(table.Version()));
    return meta;
}

TIntrusivePtr<IMkqlCallableCompiler> CreateKqlCompiler(const TKqlCompileContext& ctx, TTypeAnnotationContext& typesCtx) {
    auto compiler = MakeIntrusive<NCommon::TMkqlCommonCallableCompiler>();

    compiler->AddCallable({TDqSourceWideWrap::CallableName(), TDqSourceWideBlockWrap::CallableName(), TDqReadWideWrap::CallableName(), TDqReadBlockWideWrap::CallableName()},
        [](const TExprNode& node, NCommon::TMkqlBuildContext&) {
            YQL_ENSURE(false, "Unsupported reader: " << node.Head().Content());
            return TRuntimeNode();
        });

    for (auto* dqIntegration : GetUniqueIntegrations(typesCtx)) {
        dqIntegration->RegisterMkqlCompiler(*compiler);
    }

    compiler->AddCallable(TKqpWideReadTable::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            TKqpWideReadTable readTable(&node);

            const auto& tableMeta = ctx.GetTableMeta(readTable.Table());
            auto keyRange = MakeKeyRange(readTable, ctx, buildCtx);

            auto result = ctx.PgmBuilder().KqpWideReadTable(MakeTableId(readTable.Table()), keyRange,
                GetKqpColumns(tableMeta, readTable.Columns(), true));

            return result;
        });

    compiler->AddCallable(TKqpWideReadTableRanges::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            TKqpWideReadTableRanges readTableRanges(&node);

            const auto& tableMeta = ctx.GetTableMeta(readTableRanges.Table());
            ValidateRangesType(readTableRanges.Ranges().Ref().GetTypeAnn(), tableMeta);

            TKqpKeyRanges ranges = MakeComputedKeyRanges(readTableRanges, ctx, buildCtx);

            return ctx.PgmBuilder().KqpWideReadTableRanges(
                MakeTableId(readTableRanges.Table()),
                ranges,
                GetKqpColumns(tableMeta, readTableRanges.Columns(), true),
                nullptr
            );
        });

    // TODO: Rewrite to DqSource https://st.yandex-team.ru/KIKIMR-17161
    compiler->AddCallable(TKqpWideReadOlapTableRanges::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            TKqpWideReadOlapTableRanges readTable(&node);

            const auto& tableMeta = ctx.GetTableMeta(readTable.Table());
            ValidateRangesType(readTable.Ranges().Ref().GetTypeAnn(), tableMeta);

            TKqpKeyRanges ranges = MakeComputedKeyRanges(readTable, ctx, buildCtx);

            // Return type depends on the process program, so it is built explicitly.
            TStringStream errorStream;
            auto returnType = NCommon::BuildType(*readTable.Ref().GetTypeAnn(), ctx.PgmBuilder(), errorStream);
            YQL_ENSURE(returnType, "Failed to build type: " << errorStream.Str());

            // Process program for OLAP read is not present in MKQL, it is passed in range description
            // in physical plan directly to executer. Read callables in MKQL only used to associate
            // input stream of the graph with the external scans, so it doesn't make much sense to pass
            // the process program through callable.
            // We anyway move to explicit sources as external nodes in KQP program, so all the information
            // about read settings will be passed in a side channel, not the program.
            auto result = ctx.PgmBuilder().KqpWideReadTableRanges(
                MakeTableId(readTable.Table()),
                ranges,
                GetKqpColumns(tableMeta, readTable.Columns(), true),
                returnType
            );

            return result;
        });

    // TODO: Rewrite to DqSource https://st.yandex-team.ru/KIKIMR-17161
    compiler->AddCallable(TKqpBlockReadOlapTableRanges::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            TKqpBlockReadOlapTableRanges readTable(&node);

            const auto& tableMeta = ctx.GetTableMeta(readTable.Table());
            ValidateRangesType(readTable.Ranges().Ref().GetTypeAnn(), tableMeta);

            TKqpKeyRanges ranges = MakeComputedKeyRanges(readTable, ctx, buildCtx);

            // Return type depends on the process program, so it is built explicitly.
            TStringStream errorStream;
            auto returnType = NCommon::BuildType(*readTable.Ref().GetTypeAnn(), ctx.PgmBuilder(), errorStream);
            YQL_ENSURE(returnType, "Failed to build type: " << errorStream.Str());

            // Process program for OLAP read is not present in MKQL, it is passed in range description
            // in physical plan directly to executer. Read callables in MKQL only used to associate
            // input stream of the graph with the external scans, so it doesn't make much sense to pass
            // the process program through callable.
            // We anyway move to explicit sources as external nodes in KQP program, so all the information
            // about read settings will be passed in a side channel, not the program.
            auto result = ctx.PgmBuilder().KqpBlockReadTableRanges(
                MakeTableId(readTable.Table()),
                ranges,
                GetKqpColumns(tableMeta, readTable.Columns(), true),
                returnType
            );

            return result;
        });

    compiler->AddCallable(TKqpLookupTable::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            TKqpLookupTable lookupTable(&node);
            const auto& tableMeta = ctx.GetTableMeta(lookupTable.Table());
            auto lookupKeys = MkqlBuildExpr(lookupTable.LookupKeys().Ref(), buildCtx);

            auto keysType = lookupTable.LookupKeys().Ref().GetTypeAnn()->Cast<TStreamExprType>();
            ValidateColumnsType(keysType, tableMeta);

            TVector<TStringBuf> keyColumns(tableMeta.KeyColumnNames.begin(), tableMeta.KeyColumnNames.end());
            auto result = ctx.PgmBuilder().KqpLookupTable(MakeTableId(lookupTable.Table()), lookupKeys,
                GetKqpColumns(tableMeta, keyColumns, false),
                GetKqpColumns(tableMeta, lookupTable.Columns(), true));

            return result;
        });

    compiler->AddCallable(TKqpUpsertRows::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            TKqpUpsertRows upsertRows(&node);

            auto settings = TKqpUpsertRowsSettings::Parse(upsertRows);

            const auto& tableMeta = ctx.GetTableMeta(upsertRows.Table());

            auto rows = MkqlBuildExpr(upsertRows.Input().Ref(), buildCtx);

            auto rowsType = upsertRows.Input().Ref().GetTypeAnn()->Cast<TStreamExprType>();
            ValidateColumnsType(rowsType, tableMeta);

            auto rowType = rowsType->GetItemType()->Cast<TStructExprType>();
            YQL_ENSURE(rowType->GetItems().size() == upsertRows.Columns().Size());

            THashSet<TStringBuf> keySet(tableMeta.KeyColumnNames.begin(), tableMeta.KeyColumnNames.end());
            THashSet<TStringBuf> upsertSet;
            for (const auto& column : upsertRows.Columns()) {
                if (keySet.contains(column)) {
                    keySet.erase(column);
                } else {
                    upsertSet.insert(column);
                }
            }

            YQL_ENSURE(keySet.empty());
            YQL_ENSURE(tableMeta.KeyColumnNames.size() + upsertSet.size() == upsertRows.Columns().Size());
            TVector<TStringBuf> upsertColumns(upsertSet.begin(), upsertSet.end());

            auto result = ctx.PgmBuilder().KqpUpsertRows(MakeTableId(upsertRows.Table()), rows,
                GetKqpColumns(tableMeta, upsertColumns, false), settings.IsUpdate);

            return result;
        });

    compiler->AddCallable(TKqpDeleteRows::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            TKqpDeleteRows deleteRows(&node);

            const auto& tableMeta = ctx.GetTableMeta(deleteRows.Table());

            auto rowsType = deleteRows.Input().Ref().GetTypeAnn()->Cast<TStreamExprType>();
            ValidateColumnsType(rowsType, tableMeta);

            const auto tableId = MakeTableId(deleteRows.Table());
            const auto rows = MkqlBuildExpr(deleteRows.Input().Ref(), buildCtx);

            return ctx.PgmBuilder().KqpDeleteRows(tableId, rows);
        });

    compiler->AddCallable(TKqpEffects::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            std::vector<TRuntimeNode> args;
            args.reserve(node.ChildrenSize());
            node.ForEachChild([&](const TExprNode& child){
                args.emplace_back(MkqlBuildExpr(child, buildCtx));
            });

            auto result = ctx.PgmBuilder().KqpEffects(args);
            return result;
        });

    compiler->AddCallable(TKqpEnsure::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            TKqpEnsure ensure(&node);

            const auto value = MkqlBuildExpr(ensure.Value().Ref(), buildCtx);
            const auto predicate = MkqlBuildExpr(ensure.Predicate().Ref(), buildCtx);
            const auto issueCode = buildCtx.ProgramBuilder.NewDataLiteral<ui32>(FromString(ensure.IssueCode().Value()));
            const auto message = MkqlBuildExpr(ensure.Message().Ref(), buildCtx);

            return ctx.PgmBuilder().KqpEnsure(value, predicate, issueCode, message);
        });

    compiler->AddCallable(TKqpIndexLookupJoin::CallableName(),
        [&ctx](const TExprNode& node, TMkqlBuildContext& buildCtx) {
            TKqpIndexLookupJoin indexLookupJoin(&node);

            const TString joinType(indexLookupJoin.JoinType().Value());
            const TString leftLabel(indexLookupJoin.LeftLabel().Value());
            const TString rightLabel(indexLookupJoin.RightLabel().Value());

            auto input = MkqlBuildExpr(indexLookupJoin.Input().Ref(), buildCtx);

            return ctx.PgmBuilder().KqpIndexLookupJoin(input, joinType, leftLabel, rightLabel);
        });

    return compiler;
}

} // namespace NKqp
} // namespace NKikimr
