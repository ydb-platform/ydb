#include "yql_expr_minikql.h"

#include "compile_context.h"
#include "db_key_resolver.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>

#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/type_ann/type_ann_impl.h>
#include <ydb/library/yql/core/type_ann/type_ann_list.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>

#include <library/cpp/threading/future/async.h>

#include <util/generic/algorithm.h>
#include <util/generic/bt_exception.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/list.h>
#include <util/generic/mapfindptr.h>
#include <util/generic/stack.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/hex.h>
#include <util/string/builder.h>
#include <utility>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

using namespace NThreading;

using namespace NYql::NCommon;

namespace {

TReadTarget GetReadTarget(TExprNode* node) {
    Y_ENSURE_EX(node->IsAtom(), TNodeException(node) << "Expected atom");
    auto modeStr = node->Content();
    if (modeStr == "head") {
        return TReadTarget::Head();
    }
    else if (modeStr == "online") {
        return TReadTarget::Online();
    }
    else if (modeStr == "follower") {
        return TReadTarget::Follower();
    }
    else {
        ythrow TNodeException(node) << "Unknown read target mode: " << modeStr;
    }
}

EInplaceUpdateMode ParseUpdateMode(const TStringBuf& name) {
    EInplaceUpdateMode mode = EInplaceUpdateMode::Unknown;

    if (name == "Sum") {
        mode = EInplaceUpdateMode::Sum;
    }
    else if (name == "Min") {
        mode = EInplaceUpdateMode::Min;
    }
    else if (name == "Max") {
        mode = EInplaceUpdateMode::Max;
    }
    else if (name == "IfNotExistOrEmpty") {
        mode = EInplaceUpdateMode::IfNotExistOrEmpty;
    }

    return mode;
}

bool CheckVersionedTable(const TExprNode* list) {
    if (list->ChildrenSize() == 3) {
        const auto name = list->Child(0);
        const auto version = list->Child(1);
        const auto pathId = list->Child(2);
        // TODO: KIKIMR-8446 add check for empty version
        if (name->IsAtom() && version->IsAtom() && name->Content() && pathId->IsAtom()) {
            return true;
        }
    }
    return false;
}

TStringBuf GetTableName(const TExprNode* node) {
    if (node->Child(0)->IsList()) {
        const auto list = node->Child(0);
        Y_ENSURE_EX(CheckVersionedTable(list), TNodeException(list)
                    <<  "Expected list of 3 atoms as versioned table.");
        return list->Child(0)->Content();
    } else if (node->Child(0)->IsAtom()) {
        return node->Child(0)->Content();
    } else {
        Y_ENSURE_EX(false, TNodeException(node) <<  "unexpected table node");
    }
    return {};
}

const TTypeAnnotationNode* GetMkqlDataTypeAnnotation(TDataType* dataType, TExprContext& ctx)
{
    return ctx.MakeType<TDataExprType>(*dataType->GetDataSlot());
}

void CollectEraseRowKey(const TExprNode* child, TContext::TPtr ctx) {
    IDbSchemeResolver::TTable request;

    Y_ENSURE_EX(child->ChildrenSize() == 2, TNodeException(child) << child->Content()
                << " takes 2 args.");
    request.TableName = GetTableName(child);

    auto rowTuple = child->Child(1);
    Y_ENSURE_EX(rowTuple->IsList() && rowTuple->ChildrenSize() > 0,
        TNodeException(rowTuple) << child->Content() << "Expected non-empty tuple");

    for (auto& tupleItem : rowTuple->Children()) {
        Y_ENSURE_EX(tupleItem->IsList() && tupleItem->ChildrenSize() == 2,
            TNodeException(*tupleItem) << child->Content() << "Expected pair");

        Y_ENSURE_EX(tupleItem->Child(0)->IsAtom() &&
            !tupleItem->Child(0)->Content().empty(), TNodeException(tupleItem->Child(0))
                    << "Expected column name as non-empty atom.");
        request.ColumnNames.insert(TString(tupleItem->Child(0)->Content()));
    }

    ctx->AddTableLookup(request);
}

void CollectUpdateRowKey(const TExprNode* child, TContext::TPtr ctx) {
    IDbSchemeResolver::TTable request;

    Y_ENSURE_EX(child->ChildrenSize() == 3, TNodeException(child) << child->Content()
                << " takes 3 args.");

    request.TableName = GetTableName(child);
    auto rowTuple = child->Child(1);
    Y_ENSURE_EX(rowTuple->IsList() && rowTuple->ChildrenSize() > 0,
        TNodeException(rowTuple) << child->Content() << "Expected non-empty tuple");

    for (auto& tupleItem : rowTuple->Children()) {
        Y_ENSURE_EX(tupleItem->IsList() && tupleItem->ChildrenSize() == 2,
            TNodeException(*tupleItem) << child->Content() << "Expected pair");

        Y_ENSURE_EX(tupleItem->Child(0)->IsAtom() &&
            !tupleItem->Child(0)->Content().empty(), TNodeException(tupleItem->Child(0))
                << "Expected column name as non-empty atom.");
        request.ColumnNames.insert(TString(tupleItem->Child(0)->Content()));
    }

    auto updateTuple = child->Child(2);
    Y_ENSURE_EX(updateTuple->IsList(), TNodeException(updateTuple) << child->Content() << "Expected tuple");

    for (auto& tupleItem : updateTuple->Children()) {
        Y_ENSURE_EX(tupleItem->IsList() && tupleItem->ChildrenSize() >= 1 && tupleItem->ChildrenSize() <= 3,
            TNodeException(*tupleItem) << child->Content() << "Expected tuple of size 1..3");
        Y_ENSURE_EX(tupleItem->Child(0)->IsAtom() &&
            !tupleItem->Child(0)->Content().empty(), TNodeException(tupleItem->Child(0))
                << "Expected column name as non-empty atom.");
        request.ColumnNames.insert(TString(tupleItem->Child(0)->Content()));
    }

    ctx->AddTableLookup(request);
}

void CollectSelectRowKey(const TExprNode* child, TContext::TPtr ctx) {
    IDbSchemeResolver::TTable request;

    Y_ENSURE_EX(child->ChildrenSize() == 3 || child->ChildrenSize() == 4,
        TNodeException(child) << child->Content() << " takes 3 or 4 args.");

    request.TableName = GetTableName(child);
    auto rowTuple = child->Child(1);
    Y_ENSURE_EX(rowTuple->IsList() && rowTuple->ChildrenSize() > 0,
        TNodeException(rowTuple) << child->Content() << "Expected non-empty tuple");

    for (auto& tupleItem : rowTuple->Children()) {
        Y_ENSURE_EX(tupleItem->IsList() && tupleItem->ChildrenSize() == 2,
            TNodeException(*tupleItem) << child->Content() << "Expected pair");

        Y_ENSURE_EX(tupleItem->Child(0)->IsAtom() &&
            !tupleItem->Child(0)->Content().empty(), TNodeException(tupleItem->Child(0))
                    << "Expected column name as non-empty atom.");
        request.ColumnNames.insert(TString(tupleItem->Child(0)->Content()));
    }

    auto selectTuple = child->Child(2);
    Y_ENSURE_EX(selectTuple->IsList(), TNodeException(selectTuple) << child->Content() << "Expected tuple");

    for (auto& tupleItem : selectTuple->Children()) {
        Y_ENSURE_EX(tupleItem->IsAtom() &&
            !tupleItem->Content().empty(), TNodeException(*tupleItem) << "Expected column name as non-empty atom.");
        request.ColumnNames.insert(TString(tupleItem->Content()));
    }

    ctx->AddTableLookup(request);
}

void CollectSelectRangeKey(const TExprNode* child, TContext::TPtr ctx) {
    IDbSchemeResolver::TTable request;

    Y_ENSURE_EX(child->ChildrenSize() == 4 || child->ChildrenSize() == 5,
        TNodeException(child) << child->Content() << " takes 4 or 5 args.");

    request.TableName = GetTableName(child);
    auto rangeTuple = child->Child(1);
    Y_ENSURE_EX(rangeTuple->IsList() && rangeTuple->ChildrenSize() > 0,
        TNodeException(rangeTuple) << child->Content() << "Expected non-empty tuple");

    for (auto& rangeItem : rangeTuple->Children()) {
        if (rangeItem->IsAtom()) {
            continue;
        }
        else {
            Y_ENSURE_EX(rangeItem->IsList() && rangeItem->ChildrenSize() == 3,
                TNodeException(*rangeItem) << child->Content() << "Expected 3 items in range item - column/from/to");

            Y_ENSURE_EX(rangeItem->Child(0)->IsAtom() &&
                !rangeItem->Child(0)->Content().empty(), TNodeException(rangeItem->Child(0)) << "Expected column name as non-empty atom.");
            request.ColumnNames.insert(TString(rangeItem->Child(0)->Content()));
        }
    }

    auto selectTuple = child->Child(2);
    Y_ENSURE_EX(selectTuple->IsList(), TNodeException(selectTuple) << child->Content() << "Expected tuple");

    for (auto& tupleItem : selectTuple->Children()) {
        Y_ENSURE_EX(tupleItem->IsAtom() &&
            !tupleItem->Content().empty(), TNodeException(*tupleItem) << "Expected column name as non-empty atom.");
        request.ColumnNames.insert(TString(tupleItem->Content()));
    }

    ctx->AddTableLookup(request);
}

class TKikimrCallableTypeAnnotationTransformer : public TSyncTransformerBase {
public:
    TKikimrCallableTypeAnnotationTransformer(
            TContext::TPtr mkqlCtx,
            TAutoPtr<IGraphTransformer> callableTransformer)
        : MkqlCtx(mkqlCtx)
        , CallableTransformer(callableTransformer) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        {
            auto name = input->Content();
            TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                return MakeIntrusive<TIssue>(ctx.GetPosition(input->Pos()), TStringBuilder() << "At function: " << name);
            });

            if (input->IsCallable("SelectRow")) {
                return SelectRowWrapper(*input, ctx);
            }
            if (input->IsCallable("SelectRange")) {
                return SelectRangeWrapper(*input, ctx);
            }
            if (input->IsCallable("UpdateRow")) {
                return UpdateRowWrapper(*input, ctx);
            }
            if (input->IsCallable("EraseRow")) {
                return EraseRowWrapper(*input, ctx);
            }
            if (input->IsCallable("SetResult")) {
                return SetResultWrapper(*input, ctx);
            }
            if (input->IsCallable("Increment")) {
                return IncrementWrapper(*input, ctx);
            }
            if (input->IsCallable("StepTxId")) {
                return StepTxIdWrapper(*input, ctx);
            }
            if (input->IsCallable("Parameter")) {
                return ParameterWrapper(*input, ctx);
            }
            if (input->IsCallable("Parameters")) {
                return ParametersWrapper(*input, ctx);
            }
            if (input->IsCallable("AsParameters")) {
                return AsParametersWrapper(*input, ctx);
            }
            if (input->IsCallable("AddParameter")) {
                return AddParameterWrapper(*input, ctx);
            }
            if (input->IsCallable("MapParameter")) {
                return MapParameterWrapper(*input, ctx);
            }
            if (input->IsCallable("FlatMapParameter")) {
                return FlatMapParameterWrapper(*input, ctx);
            }
            if (input->IsCallable("AcquireLocks")) {
                return AcquireLocksWrapper(*input, ctx);
            }
            if (input->IsCallable("Diagnostics")) {
                return DiagnosticsWrapper(*input, ctx);
            }
            if (input->IsCallable("DataType")) {
                return DataTypeWrapper(*input, ctx);
            }
            if (input->IsCallable("PartialSort")) {
                NTypeAnnImpl::TContext typeAnnCtx(ctx);
                TExprNode::TPtr output;
                return NTypeAnnImpl::SortWrapper(input, output, typeAnnCtx);
            }
            if (input->IsCallable("PartialTake")) {
                NTypeAnnImpl::TContext typeAnnCtx(ctx);
                TExprNode::TPtr output;
                return NTypeAnnImpl::TakeWrapper(input, output, typeAnnCtx);
            }
        }

        return CallableTransformer->Transform(input, output, ctx);
    }

    void Rewind() final {
    }

private:
    static bool CheckKeyColumn(const TStringBuf& columnName, ui32 keyIndex, IDbSchemeResolver::TTableResult* lookup, TExprNode& node, TExprContext& ctx) {
        auto column = lookup->Columns.FindPtr(columnName);
        if (!column) {
            ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                << "Unknown column '" << columnName
                << "' for table [" << lookup->Table.TableName
                << "] at key position #" << keyIndex));
            return false;
        }

        if ((ui32)column->KeyPosition != keyIndex) {
            ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                << "Mismatched key column '" << columnName
                << "' for table [" << lookup->Table.TableName
                << "] at key position #" << keyIndex));
            return false;
        }

        return true;
    }

    static bool CheckRowTuple(IDbSchemeResolver::TTableResult* lookup, TExprNode& node, TExprNode& rowTuple, TExprContext& ctx) {
        if (rowTuple.ChildrenSize() != lookup->KeyColumnCount) {
            ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                << "Mismatch of key columns count for table [" << lookup->Table.TableName
                << "], expected: " << lookup->KeyColumnCount
                << ", but got " << rowTuple.ChildrenSize() << "."));
            return false;
        }

        for (ui32 i = 0; i < rowTuple.ChildrenSize(); ++i) {
            auto columnName = rowTuple.Child(i)->Child(0)->Content();
            if (!CheckKeyColumn(columnName, i, lookup, node, ctx)) {
                return false;
            }
        }

        return true;
    }

    const TTypeAnnotationNode* GetSelectType(IDbSchemeResolver::TTableResult* lookup, TExprNode& selectTuple,
        TExprContext& ctx)
    {
        TVector<const TItemExprType*> resultItems;

        for (auto& tupleItem : selectTuple.Children()) {
            auto columnName = tupleItem->Content();
            const TTypeAnnotationNode *columnDataType;
            auto typeConstraint = EColumnTypeConstraint::Nullable;

            auto systemColumnType = KikimrSystemColumns().find(columnName);
            if (systemColumnType != KikimrSystemColumns().end()) {
                columnDataType = ctx.MakeType<TDataExprType>(systemColumnType->second);
            } else {
                auto column = lookup->Columns.FindPtr(columnName);
                YQL_ENSURE(column);
                typeConstraint = column->TypeConstraint;

                switch (column->Type.GetTypeId()) {
                case NScheme::NTypeIds::Pg: {
                    // TODO: support pg types
                    YQL_ENSURE(false, "pg types are not supported");
                    break;
                }
                case NScheme::NTypeIds::Decimal: {
                    columnDataType = ctx.MakeType<TDataExprParamsType>(
                        EDataSlot::Decimal,
                        ToString(column->Type.GetDecimalType().GetPrecision()),
                        ToString(column->Type.GetDecimalType().GetScale()));
                    break;
                }
                default:{
                    columnDataType = GetMkqlDataTypeAnnotation(
                        TDataType::Create(column->Type.GetTypeId(), *MkqlCtx->TypeEnv), ctx);
                    break;
                }
                }
            }

            if (typeConstraint == EColumnTypeConstraint::Nullable) {
                columnDataType = ctx.MakeType<TOptionalExprType>(columnDataType);
            }

            resultItems.push_back(ctx.MakeType<TItemExprType>(columnName, columnDataType));
        }

        auto selectType = ctx.MakeType<TStructExprType>(resultItems);
        return selectType;
    }

private:
    IGraphTransformer::TStatus SelectRowWrapper(TExprNode& node, TExprContext& ctx) {
        const auto lookup = MkqlCtx->GetTableLookup(node, GetTableName(&node));
        auto rowTuple = node.Child(1);

        if (!CheckRowTuple(lookup, node, *rowTuple, ctx)) {
            return TStatus::Error;
        }

        auto selectTuple = node.Child(2);
        auto selectType = GetSelectType(lookup, *selectTuple, ctx);
        auto optSelectType = ctx.MakeType<TOptionalExprType>(selectType);

        node.SetTypeAnn(optSelectType);
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus SelectRangeWrapper(TExprNode& node, TExprContext& ctx) {
        const auto lookup = MkqlCtx->GetTableLookup(node, GetTableName(&node));
        auto rangeTuple = node.Child(1);

        ui32 keyCount = 0;
        bool finishedFrom = false;
        bool finishedTo = false;
        ui32 fromComponents = 0;

        for (auto rangeItem : rangeTuple->Children()) {
            if (rangeItem->IsAtom()) {
                if (rangeItem->Content() != "IncFrom" &&
                    rangeItem->Content() != "ExcFrom" &&
                    rangeItem->Content() != "IncTo" &&
                    rangeItem->Content() != "ExcTo")
                {
                    ythrow TNodeException(node) << "Unsupported range bound: " << rangeItem->Content();
                }

                continue;
            }

            if (!EnsureComputable(*rangeItem->Child(1), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureComputable(*rangeItem->Child(2), ctx)) {
                return TStatus::Error;
            }

            ++keyCount;
            if (!rangeItem->Child(1)->IsCallable("Void")) {
                Y_ENSURE_EX(!finishedFrom, TNodeException(rangeItem->Child(1))
                    << "From tuple is already set to infinity");
                ++fromComponents;
            } else {
                finishedFrom = true;
            }

            if (!rangeItem->Child(2)->IsCallable("Void")) {
                Y_ENSURE_EX(!finishedTo, TNodeException(rangeItem->Child(2))
                    << "To tuple is already set to infinity");
            }
            else {
                finishedTo = true;
            }
        }

        Y_ENSURE_EX(keyCount <= lookup->KeyColumnCount, TNodeException(node)
            << "Too many key columns specified, table [" << lookup->Table.TableName
            << "] has only: " << lookup->KeyColumnCount
            << ", but got " << keyCount << ".");

        Y_ENSURE_EX(fromComponents > 0, TNodeException(node)
            << "Expected at least one component of key in the 'from' section of the range");

        ui32 keyIndex = 0;
        for (auto rangeItem : rangeTuple->Children()) {
            if (rangeItem->IsAtom()) {
                continue;
            }

            auto columnName = rangeItem->Child(0)->Content();
            if (!CheckKeyColumn(columnName, keyIndex, lookup, node, ctx)) {
                return TStatus::Error;
            }
            ++keyIndex;
        }

        auto selectTuple = node.Child(2);

        // Check that all selected columns are present in table schema
        ui32 selectIndex = 0;
        for (auto selectItem : selectTuple->Children()) {
            auto columnName = selectItem->Content();
            if (!NKikimr::IsSystemColumn(columnName)) {
                auto column = lookup->Columns.FindPtr(columnName);
                Y_ENSURE_EX(column, TNodeException(node)
                    << "Unknown column '" << columnName
                    << "' for table [" << lookup->Table.TableName
                    << "] at select position #" << selectIndex);
            }

            ++selectIndex;
        }

        auto optionsNode = node.Child(3);
        Y_ENSURE_EX(optionsNode->IsList(), TNodeException(optionsNode) << "Expected tuple");
        for (auto optionsItem : optionsNode->Children()) {
            Y_ENSURE_EX(optionsItem->IsList() && optionsItem->ChildrenSize() == 2 && optionsItem->Child(0)->IsAtom(),
                TNodeException(*optionsItem) << "Expected pair of atom and value");

            auto optionName = optionsItem->Child(0)->Content();
            if (optionName != "ItemsLimit" &&
                optionName != "BytesLimit" &&
                optionName != "SkipNullKeys" &&
                optionName != "Reverse" &&
                optionName != "ForbidNullArgsFrom" &&
                optionName != "ForbidNullArgsTo")
            {
                ythrow TNodeException(node) << "Unsupported option: " << optionName;
            }
        }

        auto selectType = GetSelectType(lookup, *selectTuple, ctx);
        auto listSelectType = ctx.MakeType<TListExprType>(selectType);

        TVector<const TItemExprType*> resultItems;
        resultItems.reserve(2);
        resultItems.push_back(ctx.MakeType<TItemExprType>("List", listSelectType));
        auto boolType = ctx.MakeType<TDataExprType>(EDataSlot::Bool);
        resultItems.push_back(ctx.MakeType<TItemExprType>("Truncated", boolType));
        auto resultType = ctx.MakeType<TStructExprType>(resultItems);

        node.SetTypeAnn(resultType);
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus UpdateRowWrapper(TExprNode& node, TExprContext& ctx) {
        const auto lookup = MkqlCtx->GetTableLookup(node, GetTableName(&node));
        auto rowTuple = node.Child(1);

        if (!CheckRowTuple(lookup, node, *rowTuple, ctx)) {
            return TStatus::Error;
        }

        auto updateTuple = node.Child(2);
        for (ui32 i = 0; i < updateTuple->ChildrenSize(); ++i) {
            auto child = updateTuple->Child(i);
            auto columnName = child->Child(0)->Content();
            auto column = lookup->Columns.FindPtr(columnName);
            YQL_ENSURE(column);
            Y_ENSURE_EX(column->KeyPosition < 0, TNodeException(node)
                << "Key column cannot be updated: " << child->Child(0)->Content());

            if (child->ChildrenSize() != 1 && child->ChildrenSize() != 2) {
                Y_ENSURE_EX(child->Child(1)->IsAtom(), TNodeException(child->Child(1))
                    << "Expected atom");
                auto modeStr = child->Child(1)->Content();
                auto mode = ParseUpdateMode(modeStr);

                Y_ENSURE_EX(mode != EInplaceUpdateMode::Unknown, TNodeException(child->Child(1))
                    << "Unknown inplace update mode: " << modeStr);
                Y_ENSURE_EX(column->AllowInplaceMode != (ui32)EInplaceUpdateMode::Unknown, TNodeException(child)
                    << "Inplace update mode is not allowed for column: " << child->Child(0)->Content());
                Y_ENSURE_EX((ui32)mode == column->AllowInplaceMode, TNodeException(child->Child(1))
                    << "Mismatch of column allowed inplace update mode, allowed: " << column->AllowInplaceMode
                    << ", but got: " << (ui32)mode << ", column: " << child->Child(0)->Content());
            }
        }

        node.SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus EraseRowWrapper(TExprNode& node, TExprContext& ctx) {
        const auto lookup = MkqlCtx->GetTableLookup(node, GetTableName(&node));
        auto rowTuple = node.Child(1);

        if (!CheckRowTuple(lookup, node, *rowTuple, ctx)) {
            return TStatus::Error;
        }

        node.SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus SetResultWrapper(TExprNode& node, TExprContext& ctx) {
        Y_ENSURE_EX(node.ChildrenSize() == 2, TNodeException(node) << "SetResult expects 2 args.");
        Y_ENSURE_EX(node.Child(0)->Type() == TExprNode::Atom, TNodeException(node) << "First SetResult argument should be Atom.");

        node.SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus IncrementWrapper(TExprNode& node, TExprContext& ctx) {
        if (!EnsureArgsCount(node, 1, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        node.SetTypeAnn(node.Child(0)->GetTypeAnn());
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus StepTxIdWrapper(TExprNode& node, TExprContext& ctx) {
        if (!EnsureArgsCount(node, 0, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto ui64Type = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
        TVector<const TTypeAnnotationNode*> items;
        items.reserve(2);
        items.push_back(ui64Type);
        items.push_back(ui64Type);

        auto tupleType = ctx.MakeType<TTupleExprType>(items);

        node.SetTypeAnn(tupleType);
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus ParameterWrapper(TExprNode& node, TExprContext& ctx) {
        Y_UNUSED(ctx);

        Y_ENSURE_EX(node.ChildrenSize() == 2, TNodeException(node) << "Parameter expects 2 args.");

        if (!EnsureType(*node.Child(1), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto parameterType = node.Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

        node.SetTypeAnn(parameterType);
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus ParametersWrapper(TExprNode& node, TExprContext& ctx) {
        Y_ENSURE_EX(node.ChildrenSize() == 0, TNodeException(node) << "Parameters expects 0 args.");

        auto structType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>());

        node.SetTypeAnn(structType);
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus AsParametersWrapper(TExprNode& node, TExprContext& ctx) {
        Y_ENSURE_EX(node.ChildrenSize() > 0, TNodeException(node) << "AsParameters expects > 0 args.");

        // Use AsStruct type annotation
        auto tmpNode = ctx.RenameNode(node, "AsStruct");
        auto output = tmpNode;
        YQL_ENSURE(CallableTransformer->Transform(tmpNode, output, ctx) == TStatus::Ok);

        node.SetTypeAnn(tmpNode->GetTypeAnn());
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus AddParameterWrapper(TExprNode& node, TExprContext& ctx) {
        Y_ENSURE_EX(node.ChildrenSize() == 3, TNodeException(node) << "AddParameter expects 3 args.");

        // Use AddMember type annotation
        auto tmpNode = ctx.RenameNode(node, "AddMember");
        auto output = tmpNode;
        YQL_ENSURE(CallableTransformer->Transform(tmpNode, output, ctx) == TStatus::Ok);

        node.SetTypeAnn(tmpNode->GetTypeAnn());
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus MapParameterWrapper(TExprNode& node, TExprContext& ctx) {
        Y_ENSURE_EX(node.ChildrenSize() == 2, TNodeException(node) << "MapParameter expects 2 args.");
        Y_ENSURE_EX(node.Child(0)->IsCallable("Parameter"), TNodeException(node.Child(0)) << "Expected Parameter as 1 arg.");

        if (!EnsureListOrOptionalType(*node.Child(0), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(node.ChildRef(1), ctx, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda = node.ChildRef(1);
        const TTypeAnnotationNode* itemType = node.Child(0)->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputableType(lambda->Pos(), *lambda->GetTypeAnn(), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        node.SetTypeAnn(ctx.MakeType<TListExprType>(lambda->GetTypeAnn()));
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus FlatMapParameterWrapper(TExprNode& node, TExprContext& ctx) {
        Y_ENSURE_EX(node.ChildrenSize() == 2, TNodeException(node) << "MapParameter expects 2 args.");
        Y_ENSURE_EX(node.Child(0)->IsCallable("Parameter"), TNodeException(node.Child(0)) << "Expected Parameter as 1 arg.");

        if (!EnsureListOrOptionalType(*node.Child(0), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(node.ChildRef(1), ctx, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda = node.ChildRef(1);
        const TTypeAnnotationNode* itemType = node.Child(0)->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(lambda->Pos(), *lambda->GetTypeAnn(), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        node.SetTypeAnn(lambda->GetTypeAnn());
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus AcquireLocksWrapper(TExprNode& node, TExprContext& ctx) {
        if (!EnsureArgsCount(node, 1, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureSpecificDataType(*node.Child(0), EDataSlot::Uint64, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        node.SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus DiagnosticsWrapper(TExprNode& node, TExprContext& ctx) {
        if (!EnsureArgsCount(node, 0, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        node.SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

    IGraphTransformer::TStatus DataConstructorWrapper(TExprNode& node, TExprContext& ctx) {
        Y_UNUSED(ctx);

        if (node.Content() == "ByteString") {
            node.SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::String));
        } else if (node.Content() == "Utf8String") {
            node.SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Utf8));
        } else {
            node.SetTypeAnn(ctx.MakeType<TDataExprType>(NKikimr::NUdf::GetDataSlot(node.Content())));
        }

        return TStatus::Ok;
    }

    IGraphTransformer::TStatus DataTypeWrapper(TExprNode& node, TExprContext& ctx)
    {
        switch (node.ChildrenSize()) {
        case 1:
            node.SetTypeAnn(ctx.MakeType<TTypeExprType>(ctx.MakeType<TDataExprType>(NKikimr::NUdf::GetDataSlot(
                AdaptLegacyYqlType(node.Child(0)->Content())))));
            break;
        case 3:
            node.SetTypeAnn(ctx.MakeType<TTypeExprType>(ctx.MakeType<TDataExprParamsType>(
                NKikimr::NUdf::GetDataSlot(AdaptLegacyYqlType(node.Child(0)->Content())), node.Child(1)->Content(), node.Child(2)->Content())));
            break;
        default:
            return TStatus::Error;
        }
        return TStatus::Ok;
    }

private:
    TContext::TPtr MkqlCtx;
    TAutoPtr<IGraphTransformer> CallableTransformer;
};

bool PerformTypeAnnotation(TExprNode::TPtr& exprRoot, TExprContext& ctx, TContext::TPtr mkqlContext) {
    TTypeAnnotationContext types;
    types.DeprecatedSQL = true;

    TAutoPtr<IGraphTransformer> callableTransformer = CreateExtCallableTypeAnnotationTransformer(types);
    types.TimeProvider = CreateDefaultTimeProvider();
    types.RandomProvider = CreateDefaultRandomProvider();

    TAutoPtr<IGraphTransformer> kikimrTransformer = new TKikimrCallableTypeAnnotationTransformer(
        mkqlContext, callableTransformer);

    auto typeTransformer = CreateTypeAnnotationTransformer(kikimrTransformer, types);

    return InstantTransform(*typeTransformer, exprRoot, ctx) == IGraphTransformer::TStatus::Ok;
}

TRuntimeNode GetReadTargetNode(const TExprNode& callable, ui32 index, TMkqlBuildContext& ctx,
    TContext::TPtr mkqlContext)
{
    TReadTarget readTarget;

    if (callable.ChildrenSize() > index) {
        const auto child = callable.Child(index);
        if (child->IsAtom()) {
            readTarget = GetReadTarget(child);
        } else {
            return MkqlBuildExpr(*child, ctx);
        }
    }

    return mkqlContext->PgmBuilder->ReadTarget(readTarget);
}

void FillColumnsToRead(IDbSchemeResolver::TTableResult* lookup, TExprNode* selectTuple, TVector<TSelectColumn>& columnsToRead) {
    for (ui32 i = 0; i < selectTuple->ChildrenSize(); ++i) {
        auto columnName = selectTuple->Child(i)->Content();
        const auto& systemColumn = GetSystemColumns().find(columnName);
        if (systemColumn != GetSystemColumns().end()) {
            columnsToRead.emplace_back(columnName, systemColumn->second.ColumnId,
                NScheme::TTypeInfo(systemColumn->second.TypeId), EColumnTypeConstraint::Nullable);
        } else {
            auto column = lookup->Columns.FindPtr(columnName);
            YQL_ENSURE(column);
            columnsToRead.emplace_back(columnName, column->Column, column->Type, column->TypeConstraint);
        }
    }
}

void ValidateCompiledTable(const TExprNode& node, const TTableId& tableId) {
    auto currentVersion = ToString(tableId.SchemaVersion);
    auto programVersion = node.Child(0)->Child(1)->Content();

    if (programVersion && programVersion != "0" && programVersion != currentVersion) {
        throw TErrorException(TIssuesIds::KIKIMR_SCHEME_MISMATCH)
            << " Schema version missmatch, compiled over: " << programVersion
            << " current: " << currentVersion
            << " for table: " << TString(node.Child(0)->Child(0)->Content());
    }
    auto currentPathId = TKikimrPathId(tableId.PathId.OwnerId, tableId.PathId.LocalPathId).ToString();
    const auto& programPathId = node.Child(0)->Child(2)->Content();
    // TODO: Remove this checks.
    // Check for programPathId just to be able to disable this check
    // by '"" record in ut
    //
    // Check for programPathId != "0:0" to allow legacy yql requests where we
    // can use DDL and DML in one program but no additional metadata
    // update after DDL executed.
    // KIKIMR-9451 for details.
    if (programPathId && programPathId != "0:0" && programPathId != currentPathId) {
        throw TErrorException(TIssuesIds::KIKIMR_SCHEME_MISMATCH)
            << " Path id missmatch, compiled over: " << programPathId
            << " current: " << currentPathId
            << " for table: " << TString(node.Child(0)->Child(0)->Content());
    }
}

bool HasUnversionedTable(const TExprNode& node) {
    return node.Child(0)->IsAtom();
}

IDbSchemeResolver::TTableResult* GetTableLookup(TContext::TPtr ctx, const TExprNode& node) {
    return ctx->GetTableLookup(node, GetTableName(&node));
}

static void FillKeyPosition(TVector<bool>& arr, const TExprNode::TPtr& listNode, const IDbSchemeResolver::TTableResult* lookup) {
    for (auto& valueNode : listNode->Children()) {
        YQL_ENSURE(valueNode->IsAtom());
        auto column = lookup->Columns.FindPtr(valueNode->Content());
        YQL_ENSURE(column);
        YQL_ENSURE(column->KeyPosition >= 0 && (ui32)column->KeyPosition < arr.size());
        arr[column->KeyPosition] = true;
    }
}

TIntrusivePtr<NCommon::IMkqlCallableCompiler> CreateMkqlCompiler(TContext::TPtr mkqlContext) {
    auto compiler = MakeIntrusive<NCommon::TMkqlCommonCallableCompiler>();

    compiler->AddCallable("SetResult",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
            auto label = node.Child(0)->Content();
            auto payload = MkqlBuildExpr(*node.Child(1), ctx);
            TRuntimeNode result = mkqlContext->PgmBuilder->SetResult(label, payload);

            return result;
        });

    compiler->AddCallable("SelectRow",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
            YQL_ENSURE(node.Child(0)->IsList() || HasUnversionedTable(node), "expected list or atom as table");
            const auto lookup = GetTableLookup(mkqlContext, node);
            YQL_ENSURE(lookup->TableId);
            const bool legacy = HasUnversionedTable(node);
            if (!legacy && lookup->TableId->SchemaVersion) {
                ValidateCompiledTable(node, *lookup->TableId);
            }
            if (legacy) {
                lookup->TableId->SchemaVersion = 0;
            }

            auto rowTuple = node.Child(1);
            TVector<TRuntimeNode> row(rowTuple->ChildrenSize());
            TVector<NScheme::TTypeInfo> keyTypes(rowTuple->ChildrenSize());
            for (ui32 i = 0; i < rowTuple->ChildrenSize(); ++i) {
                auto columnName = rowTuple->Child(i)->Child(0)->Content();
                auto column = lookup->Columns.FindPtr(columnName);
                YQL_ENSURE(column);
                auto keyValue = MkqlBuildExpr(*rowTuple->Child(i)->Child(1), ctx);
                row[i] = keyValue;
                keyTypes[i] = column->Type;
            }

            TVector<TSelectColumn> columnsToRead;
            FillColumnsToRead(lookup, node.Child(2), columnsToRead);

            auto readTargetNode = GetReadTargetNode(node, 3, ctx, mkqlContext);

            auto result = mkqlContext->PgmBuilder->SelectRow(*lookup->TableId, keyTypes, columnsToRead, row,
                readTargetNode);
            return result;
        });

    compiler->AddCallable("SelectRange",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
            YQL_ENSURE(node.Child(0)->IsList() || HasUnversionedTable(node), "expected list or atom as table");
            const auto lookup = GetTableLookup(mkqlContext, node);
            YQL_ENSURE(lookup->TableId);
            const bool legacy = HasUnversionedTable(node);
            if (!legacy && lookup->TableId->SchemaVersion) {
                ValidateCompiledTable(node, *lookup->TableId);
            }
            if (legacy) {
                lookup->TableId->SchemaVersion = 0;
            }

            bool includeFrom = true;
            bool includeTo = true;
            auto rangeTuple = node.Child(1);
            ui32 fromComponents = 0;
            ui32 toComponents = 0;
            for (auto rangeItem : rangeTuple->Children()) {
                if (rangeItem->IsAtom()) {
                    if (rangeItem->Content() == "IncFrom") {
                        includeFrom = true;
                    }
                    else if (rangeItem->Content() == "ExcFrom") {
                        includeFrom = false;
                    }
                    else if (rangeItem->Content() == "IncTo") {
                        includeTo = true;
                    }
                    else if (rangeItem->Content() == "ExcTo") {
                        includeTo = false;
                    }
                    else {
                        YQL_ENSURE(false, "Unexpected SelectRange range bound");
                    }

                    continue;
                }

                if (!rangeItem->Child(1)->IsCallable("Void")) {
                    ++fromComponents;
                }

                if (!rangeItem->Child(2)->IsCallable("Void")) {
                    ++toComponents;
                }
            }

            TTableRangeOptions options = mkqlContext->PgmBuilder->GetDefaultTableRangeOptions();
            TVector<NScheme::TTypeInfo> keyTypes(Max(fromComponents, toComponents));
            TVector<TRuntimeNode> from(fromComponents);
            TVector<TRuntimeNode> to(toComponents);
            ui32 keyIndex = 0;
            for (auto rangeItem : rangeTuple->Children()) {
                if (rangeItem->IsAtom()) {
                    continue;
                }

                if (keyIndex >= fromComponents && keyIndex >= toComponents) {
                    break;
                }

                auto columnName = rangeItem->Child(0)->Content();
                auto column = lookup->Columns.FindPtr(columnName);
                YQL_ENSURE(column);

                if (keyIndex < fromComponents) {
                    auto fromKeyValue = MkqlBuildExpr(*rangeItem->Child(1), ctx);
                    from[keyIndex] = fromKeyValue;
                }

                if (keyIndex < toComponents) {
                    auto toKeyValue = MkqlBuildExpr(*rangeItem->Child(2), ctx);
                    to[keyIndex] = toKeyValue;
                }

                keyTypes[keyIndex] = column->Type;
                ++keyIndex;
            }

            const ui32 flags = (includeFrom ? TReadRangeOptions::TFlags::IncludeInitValue : TReadRangeOptions::TFlags::ExcludeInitValue) |
                (includeTo ? TReadRangeOptions::TFlags::IncludeTermValue : TReadRangeOptions::TFlags::ExcludeTermValue);
            options.Flags = ctx.ProgramBuilder.NewDataLiteral(flags);
            options.FromColumns = from;
            options.ToColumns = to;

            TVector<bool> skipNullKeys(lookup->KeyColumnCount, false);
            options.SkipNullKeys = skipNullKeys;
            TVector<bool> forbidNullArgsFrom(lookup->KeyColumnCount, false);
            options.ForbidNullArgsFrom = forbidNullArgsFrom;
            TVector<bool> forbidNullArgsTo(lookup->KeyColumnCount, false);
            options.ForbidNullArgsTo = forbidNullArgsTo;

            TVector<TSelectColumn> columnsToRead;
            FillColumnsToRead(lookup, node.Child(2), columnsToRead);

            auto optionsNode = node.Child(3);
            for (auto optionsItem : optionsNode->Children()) {
                if (optionsItem->Child(0)->Content() == "ItemsLimit") {
                    options.ItemsLimit = MkqlBuildExpr(*optionsItem->Child(1), ctx);
                } else if (optionsItem->Child(0)->Content() == "BytesLimit") {
                    options.BytesLimit = MkqlBuildExpr(*optionsItem->Child(1), ctx);
                } else if (optionsItem->Child(0)->Content() == "SkipNullKeys") {
                    FillKeyPosition(skipNullKeys, optionsItem->Child(1), lookup);
                } else if (optionsItem->Child(0)->Content() == "ForbidNullArgsFrom") {
                    FillKeyPosition(forbidNullArgsFrom, optionsItem->Child(1), lookup);
                } else if (optionsItem->Child(0)->Content() == "ForbidNullArgsTo") {
                    FillKeyPosition(forbidNullArgsTo, optionsItem->Child(1), lookup);
                } else if (optionsItem->Child(0)->Content() == "Reverse") {
                    options.Reverse = MkqlBuildExpr(*optionsItem->Child(1), ctx);
                } else {
                    YQL_ENSURE(false, "Unexpected SelectRange option.");
                }
            }

            auto readTargetNode = GetReadTargetNode(node, 4, ctx, mkqlContext);

            TRuntimeNode result = mkqlContext->PgmBuilder->SelectRange(*lookup->TableId, keyTypes, columnsToRead,
                options, readTargetNode);
            return result;
        });

    compiler->AddCallable("UpdateRow",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
            YQL_ENSURE(node.Child(0)->IsList() || HasUnversionedTable(node), "expected list or atom as table");
            const auto lookup = GetTableLookup(mkqlContext, node);
            YQL_ENSURE(lookup->TableId);
            const bool legacy = HasUnversionedTable(node);
            if (!legacy && lookup->TableId->SchemaVersion) {
                ValidateCompiledTable(node, *lookup->TableId);
            }
            if (legacy) {
                lookup->TableId->SchemaVersion = 0;
            }

            auto rowTuple = node.Child(1);
            TVector<TRuntimeNode> row(rowTuple->ChildrenSize());
            TVector<NScheme::TTypeInfo> keyTypes(rowTuple->ChildrenSize());
            for (ui32 i = 0; i < rowTuple->ChildrenSize(); ++i) {
                auto columnName = rowTuple->Child(i)->Child(0)->Content();
                auto column = lookup->Columns.FindPtr(columnName);
                YQL_ENSURE(column);
                auto keyValue = MkqlBuildExpr(*rowTuple->Child(i)->Child(1), ctx);
                row[i] = keyValue;
                keyTypes[i] = column->Type;
            }

            auto update = mkqlContext->PgmBuilder->GetUpdateRowBuilder();
            auto updateTuple = node.Child(2);
            for (ui32 i = 0; i < updateTuple->ChildrenSize(); ++i) {
                auto child = updateTuple->Child(i);
                auto columnName = child->Child(0)->Content();
                auto column = lookup->Columns.FindPtr(columnName);
                YQL_ENSURE(column);

                if (child->ChildrenSize() == 1) {
                    update.EraseColumn(column->Column);
                }
                else if (child->ChildrenSize() == 2) {
                    auto setValue = MkqlBuildExpr(*updateTuple->Child(i)->Child(1), ctx);
                    update.SetColumn(column->Column, column->Type, setValue);
                } else {
                    auto modeStr = child->Child(1)->Content();
                    EInplaceUpdateMode mode = ParseUpdateMode(modeStr);
                    auto mergeValue = MkqlBuildExpr(*updateTuple->Child(i)->Child(2), ctx);
                    update.InplaceUpdateColumn(column->Column, column->Type, mergeValue, mode);
                }
            }

            TRuntimeNode result = mkqlContext->PgmBuilder->UpdateRow(*lookup->TableId, keyTypes, row, update);
            return result;
        });

    compiler->AddCallable("EraseRow",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
            YQL_ENSURE(node.Child(0)->IsList() || HasUnversionedTable(node), "expected list or atom as table");
            const auto lookup = GetTableLookup(mkqlContext, node);
            YQL_ENSURE(lookup->TableId);
            const bool legacy = HasUnversionedTable(node);
            if (!legacy && lookup->TableId->SchemaVersion) {
                ValidateCompiledTable(node, *lookup->TableId);
            }
            if (legacy) {
                lookup->TableId->SchemaVersion = 0;
            }

            auto rowTuple = node.Child(1);
            TVector<TRuntimeNode> row(rowTuple->ChildrenSize());
            TVector<NScheme::TTypeInfo> keyTypes(rowTuple->ChildrenSize());
            for (ui32 i = 0; i < rowTuple->ChildrenSize(); ++i) {
                auto columnName = rowTuple->Child(i)->Child(0)->Content();
                auto column = lookup->Columns.FindPtr(columnName);
                YQL_ENSURE(column);
                auto keyValue = MkqlBuildExpr(*rowTuple->Child(i)->Child(1), ctx);
                row[i] = keyValue;
                keyTypes[i] = column->Type;
            }

            TRuntimeNode result = mkqlContext->PgmBuilder->EraseRow(*lookup->TableId, keyTypes, row);
            return result;
        });

    compiler->AddCallable("Increment", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto arg = MkqlBuildExpr(*node.Child(0), ctx);

        return ctx.ProgramBuilder.Increment(arg);
    });

    compiler->AddCallable("StepTxId", [mkqlContext](const TExprNode&, TMkqlBuildContext&) {
        return mkqlContext->PgmBuilder->StepTxId();
    });

    compiler->OverrideCallable("Parameter", [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& name = node.Child(0)->Content();
        const auto type = BuildType(*node.Child(1), *node.Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        return mkqlContext->NewParam(name, type);
    });

    compiler->AddCallable("Parameters", [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        Y_UNUSED(ctx);

        if (!mkqlContext->NewParamsBuilder()) {
            throw TNodeException(node) << "You can create only one Parameters object.";
        }

        return TRuntimeNode();
    });

    compiler->AddCallable("AsParameters", [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        if (!mkqlContext->NewParamsBuilder()) {
            throw TNodeException(node) << "You can create only one Parameters object.";
        }
        for (auto child : node.Children()) {
            const auto& name = child->Child(0)->Content();
            auto value = MkqlBuildExpr(*child->Child(1), ctx);
            mkqlContext->ParamsBuilder->Add(name, value);
        }

        return mkqlContext->ParamsBuilder->Build();
    });

    compiler->AddCallable("AddParameter", [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        if (!mkqlContext->NewParamsBuilder()) {
            throw TNodeException(node) << "You can create only one Parameters object.";
        }

        for (auto curNode = &node; curNode->Content() != "Parameters"; curNode = curNode->Child(0)) {
            Y_ENSURE_EX(curNode->Content() == "AddParameter", TNodeException(*curNode) << "Only AddParameter func can be in AddParameter func.");
            const auto& name = curNode->Child(1)->Content();
            auto value = MkqlBuildExpr(*curNode->Child(2), ctx);
            mkqlContext->ParamsBuilder->Add(name, value);
        }

        return mkqlContext->ParamsBuilder->Build();
    });

    compiler->AddCallable("MapParameter",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto list = MkqlBuildExpr(*node.Child(0), ctx);
        return mkqlContext->PgmBuilder->MapParameter(list, [&](TRuntimeNode item) {
            TMkqlBuildContext::TArgumentsMap innerArguments;
            auto arg = node.Child(1)->Child(0)->Child(0);
            innerArguments[arg] = item;
            TMkqlBuildContext innerCtx(ctx, std::move(innerArguments), node.Child(1)->UniqueId());
            auto res = MkqlBuildExpr(*node.Child(1)->Child(1), innerCtx);
            return res;
        });
    });

    compiler->AddCallable("FlatMapParameter",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto list = MkqlBuildExpr(*node.Child(0), ctx);
        return mkqlContext->PgmBuilder->FlatMapParameter(list, [&](TRuntimeNode item) {
            TMkqlBuildContext::TArgumentsMap innerArguments;
            auto arg = node.Child(1)->Child(0)->Child(0);
            innerArguments[arg] = item;
            TMkqlBuildContext innerCtx(ctx, std::move(innerArguments), node.Child(1)->UniqueId());
            auto res = MkqlBuildExpr(*node.Child(1)->Child(1), innerCtx);
            return res;
        });
    });

    compiler->AddCallable("AcquireLocks",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto lockTxId = MkqlBuildExpr(*node.Child(0), ctx);
        return mkqlContext->PgmBuilder->AcquireLocks(lockTxId);
    });

    compiler->AddCallable("Diagnostics",
        [mkqlContext](const TExprNode&, TMkqlBuildContext&) {
        return mkqlContext->PgmBuilder->Diagnostics();
    });

    compiler->AddCallable("PartialSort",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto list = MkqlBuildExpr(*node.Child(0), ctx);
        auto ascending = MkqlBuildExpr(*node.Child(1), ctx);
        return mkqlContext->PgmBuilder->PartialSort(list, ascending, [&](TRuntimeNode item) {
            TMkqlBuildContext::TArgumentsMap innerArguments;
            auto argItem = node.Child(2)->Child(0)->Child(0);
            innerArguments[argItem] = item;
            TMkqlBuildContext innerCtx(ctx, std::move(innerArguments), node.Child(2)->UniqueId());
            auto res = MkqlBuildExpr(*node.Child(2)->Child(1), innerCtx);
            return res;
        });
    });

    compiler->AddCallable("PartialTake",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto list = MkqlBuildExpr(*node.Child(0), ctx);
        auto len = MkqlBuildExpr(*node.Child(1), ctx);
        return mkqlContext->PgmBuilder->PartialTake(list, len);
    });

    compiler->OverrideCallable("CombineByKey",
        [mkqlContext](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto combineByKey = CombineByKeyImpl(node, ctx);
        auto merge = mkqlContext->PgmBuilder->CombineByKeyMerge(combineByKey);
        return merge;
    });

    // TODO: Remove override once we support custom types in YQL type annotaion.
    compiler->OverrideCallable("DataType", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto typeName = node.Child(0)->Content();
        typeName = AdaptLegacyYqlType(typeName);
        auto slot = NKikimr::NUdf::FindDataSlot(typeName);
        if (!slot) {
            throw TNodeException(node) << "Unknown type '" << typeName << "'.";
        }

        NUdf::TDataTypeId typeId = NKikimr::NUdf::GetDataTypeInfo(*slot).TypeId;
        if (typeId == NUdf::TDataType<NUdf::TDecimal>::Id) {
            auto precision = node.Child(1)->Content();
            auto scale = node.Child(2)->Content();
            auto type = ctx.ProgramBuilder.NewDecimalType(FromString<ui8>(precision), FromString<ui8>(scale));
            return TRuntimeNode(type, true);
        } else {
            auto type = ctx.ProgramBuilder.NewDataType(typeId);
            return TRuntimeNode(type, true);
        }
    });

    return compiler;
}

TRuntimeNode CompileNode(const TExprNode& node, TExprContext& exprCtx, TContext::TPtr ctx, const IMkqlCallableCompiler* compiler) {
    const auto guard = ctx->TypeEnv->BindAllocator();
    TMkqlBuildContext mkqlCtx(*compiler, *ctx->PgmBuilder, exprCtx);
    return MkqlBuildExpr(node, mkqlCtx);
}

} // anonymous namespace

void CollectKeys(const TExprNode* root, TContext::TPtr ctx) {
    TStack<const TExprNode*> activeNodes;
    activeNodes.push(root);
    THashSet<const TExprNode*> visited;
    visited.insert(root);
    while (!activeNodes.empty()) {
        auto current = activeNodes.top();
        activeNodes.pop();
        {
            if (current->IsCallable("EraseRow")) {
                CollectEraseRowKey(current, ctx);
            }
            else if (current->IsCallable("UpdateRow")) {
                CollectUpdateRowKey(current, ctx);
            }
            else if (current->IsCallable("SelectRow")) {
                CollectSelectRowKey(current, ctx);
            }
            else if (current->IsCallable("SelectRange")) {
                CollectSelectRangeKey(current, ctx);
            }
        }

        for (auto& child : current->Children()) {
            if (visited.insert(child.Get()).second) {
                activeNodes.push(child.Get());
            }
        }
    }
}

TFuture<TConvertResult>
ConvertToMiniKQL(TExprContainer::TPtr expr,
                 const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
                 const NKikimr::NMiniKQL::TTypeEnvironment* typeEnv,
                 IDbSchemeResolver* dbSchemeResolver)
{
    if (expr->Root == nullptr) {
        return MakeFuture<TConvertResult>(TConvertResult());
    }

    TPromise<TConvertResult> promise = NewPromise<TConvertResult>();

    try {
        TContext::TPtr ctx(new TContext(functionRegistry, typeEnv));
        auto compiler = CreateMkqlCompiler(ctx);
        CollectKeys(expr->Root.Get(), ctx);
        const auto& tablesToResolve = ctx->GetTablesToResolve();
        if (!tablesToResolve.empty()) {
            TVector<IDbSchemeResolver::TTable> requests;
            requests.reserve(tablesToResolve.size());
            for (const auto& x : tablesToResolve) {
                requests.push_back(x.second.Request);
            }
            Y_DEBUG_ABORT_UNLESS(dbSchemeResolver);
            dbSchemeResolver->ResolveTables(requests).Subscribe(
                [ctx, promise, expr, compiler](const TFuture<IDbSchemeResolver::TTableResults>& future) mutable {
                try {
                    const auto& results = future.GetValue();
                    auto& tablesToResolve = ctx->GetTablesToResolve();
                    Y_DEBUG_ABORT_UNLESS(tablesToResolve.size() == results.size(), "tablesToResolve.size() != results.size()");
                    ui32 i = 0;
                    for (auto& x : tablesToResolve) {
                        const auto& response = results[i];
                        Y_ENSURE_EX(response.Status == IDbSchemeResolver::TTableResult::Ok,
                            TNodeException() << "Failed to resolve table " << x.second.Request.TableName
                            << ", error: " << response.Reason);
                        x.second.Response = response;
                        ++i;
                    }

                    if (!PerformTypeAnnotation(expr->Root, expr->Context, ctx)) {
                        TConvertResult convRes;
                        convRes.Errors.AddIssues(expr->Context.IssueManager.GetIssues());
                        promise.SetValue(convRes);
                        return;
                    }

                    TRuntimeNode convertedNode = CompileNode(*expr->Root, expr->Context, ctx, compiler.Get());
                    TConvertResult convRes = ctx->Finish(convertedNode);
                    promise.SetValue(convRes);
                }
                catch (const TNodeException& ex) {
                    // TODO: pass backtrace
                    TConvertResult convRes;
                    convRes.Errors.AddIssue(expr->Context.GetPosition(ex.Pos()), ex.what());
                    promise.SetValue(convRes);
                }
                catch (const yexception& ex) { // Catch TProgramBuilder exceptions.
                                               // TODO: pass backtrace
                    TConvertResult convRes;
                    convRes.Errors.AddIssue(NYql::ExceptionToIssue(ex));
                    promise.SetValue(convRes);
                }
            });
        }
        else {
            if (!PerformTypeAnnotation(expr->Root, expr->Context, ctx)) {
                TConvertResult convRes;
                convRes.Errors.AddIssues(expr->Context.IssueManager.GetIssues());
                promise.SetValue(convRes);
                return promise.GetFuture();
            }

            TRuntimeNode convertedNode = CompileNode(*expr->Root, expr->Context, ctx, compiler.Get());
            TConvertResult convRes = ctx->Finish(convertedNode);
            promise.SetValue(convRes);
        }
    } catch (const TNodeException& ex) {
        // TODO: pass backtrace
        TConvertResult convRes;
        convRes.Errors.AddIssue(expr->Context.GetPosition(ex.Pos()), ex.what());
        promise.SetValue(convRes);
    } catch (const yexception& ex) { // Catch TProgramBuilder exceptions.
        // TODO: pass backtrace
        TConvertResult convRes;
        convRes.Errors.AddIssue(NYql::ExceptionToIssue(ex));
        promise.SetValue(convRes);
    }

    return promise.GetFuture();
}


TMiniKQLCompileActorEvents::TEvCompileResult::TEvCompileResult(const TMiniKQLCompileResult& result, THashMap<TString, ui64> &&resolveCookies)
    : Result(result)
    , CompileResolveCookies(std::move(resolveCookies))
{}

class TMiniKQLCompileActor : public TActorBootstrapped<TMiniKQLCompileActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MINIKQL_COMPILE_ACTOR;
    }

    TMiniKQLCompileActor(const TString& program,
                         const NKikimr::NMiniKQL::TTypeEnvironment* typeEnv,
                         IDbSchemeResolver* dbSchemeResolver,
                         TActorId responseTo,
                         THashMap<TString, ui64> &&resolveRefreshCookies,
                         bool forceCacheRefresh)
        : TypeEnv(typeEnv)
        , Program(program)
        , DbSchemeResolver(dbSchemeResolver)
        , ResponseTo(responseTo)
        , ResolveRefreshCookies(std::move(resolveRefreshCookies))
    {
        Y_UNUSED(forceCacheRefresh);
    }

    void Bootstrap(const TActorContext& ctx) {
        auto* appData = AppData(ctx);
        CompileCtx.Reset(new TContext(appData->FunctionRegistry, TypeEnv));
        try {
            TMiniKQLCompileResult result;
            if (!ParseProgram(result.Errors)) {
                return SendResponseAndDie(result, {}, ctx);
            }

            CollectKeys(Expr->Root.Get(), CompileCtx);

            Compiler = CreateMkqlCompiler(CompileCtx);
            const auto& tablesToResolve = CompileCtx->GetTablesToResolve();
            if (!tablesToResolve.empty()) {
                TVector<IDbSchemeResolver::TTable> requests;
                requests.reserve(tablesToResolve.size());
                for (auto& x : tablesToResolve) {
                    requests.push_back(x.second.Request);
                }

                DbSchemeResolver->ResolveTables(requests, ctx.SelfID);
                Become(&TThis::StateCompileProgram);
            } else {
                if (!PerformTypeAnnotation(Expr->Root, Expr->Context, CompileCtx)) {
                    result.Errors.AddIssues(Expr->Context.IssueManager.GetIssues());
                    return SendResponseAndDie(result, {}, ctx);
                }

                result.CompiledProgram = CompileProgram();
                return SendResponseAndDie(result, {}, ctx);
            }
        } catch (const TNodeException& ex) {
            // TODO: pass backtrace
            TMiniKQLCompileResult res(NYql::TIssue(Expr->Context.GetPosition(ex.Pos()), ex.what()));
            return SendResponseAndDie(res, {}, ctx);
        } catch (const yexception& ex) { // Catch TProgramBuilder exceptions.
            // TODO: pass backtrace
            TMiniKQLCompileResult res(NYql::ExceptionToIssue(ex));
            return SendResponseAndDie(res, {}, ctx);
        }
    }

    STFUNC(StateCompileProgram) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IDbSchemeResolver::TEvents::TEvResolveTablesResult, Handle)
            default:
                Y_ABORT("Unknown event");
        }
    }

private:
    void Handle(IDbSchemeResolver::TEvents::TEvResolveTablesResult::TPtr& ev, const TActorContext& ctx) {
        THashMap<TString, ui64> compileResolveCookies;

        try {
            const auto& results = ev->Get()->Result;
            auto& tablesToResolve = CompileCtx->GetTablesToResolve();
            Y_DEBUG_ABORT_UNLESS(tablesToResolve.size() == results.size(), "tablesToResolve.size() != results.size()");

            TVector<NYql::TIssue> resolveErrors;
            ui32 i = 0;
            for (auto &xpair : tablesToResolve) {
                auto &x = xpair.second;
                auto &response = results[i];

                switch (response.Status) {
                    case IDbSchemeResolver::TTableResult::Ok:
                        break;

                    case IDbSchemeResolver::TTableResult::LookupError:
                        resolveErrors.push_back(
                            NYql::TIssue(TPosition(), TStringBuilder()
                                << "Lookup failed for table: " << x.Request.TableName
                                << ", error: " << response.Reason)
                            .SetCode(NKikimrIssues::TIssuesIds::RESOLVE_LOOKUP_ERROR, TSeverityIds::S_ERROR));
                        break;

                    default:
                        resolveErrors.push_back(
                            NYql::TIssue(TPosition(), TStringBuilder()
                                << "Resolve failed for table: " << x.Request.TableName
                                << ", error: " << response.Reason)
                            .SetCode(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, TSeverityIds::S_ERROR));
                        break;
                }

                compileResolveCookies[x.Request.TableName] = response.CacheGeneration;
                x.Response = response;
                ++i;
            }

            if (!resolveErrors.empty()) {
                TMiniKQLCompileResult result(resolveErrors);
                return SendResponseAndDie(result, std::move(compileResolveCookies), ctx);
            }

            TMiniKQLCompileResult result;

            if (!PerformTypeAnnotation(Expr->Root, Expr->Context, CompileCtx)) {
                result.Errors.AddIssues(Expr->Context.IssueManager.GetIssues());
                return SendResponseAndDie(result, std::move(compileResolveCookies), ctx);
            }

            result.CompiledProgram = CompileProgram();
            return SendResponseAndDie(result, std::move(compileResolveCookies), ctx);
        }
        catch (const TNodeException& ex) {
            // TODO: pass backtrace
            TMiniKQLCompileResult result(NYql::TIssue(Expr->Context.GetPosition(ex.Pos()), ex.what()));
            return SendResponseAndDie(result, std::move(compileResolveCookies), ctx);
        }
        catch (const yexception& ex) { // Catch TProgramBuilder exceptions.
                                       // TODO: pass backtrace
            TMiniKQLCompileResult result(NYql::ExceptionToIssue(ex));
            return SendResponseAndDie(result, std::move(compileResolveCookies), ctx);
        }
    }

    bool ParseProgram(TIssues& errors) {
        Expr.Reset(new NYql::TExprContainer());
        NYql::TAstParseResult astRes = NYql::ParseAst(Program);
        auto root = astRes.Root;
        astRes.Root = nullptr; // don't cleanup library nodes
        if (!root) {
            errors = astRes.Issues;
            return false;
        }
        Expr->Context.IssueManager.AddIssues(std::move(astRes.Issues));
        if (!CompileExpr(*root, Expr->Root, Expr->Context, nullptr, nullptr)) {
            errors = Expr->Context.IssueManager.GetIssues();
            return false;
        }
        IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
        do {
            status = ExpandApply(Expr->Root, Expr->Root, Expr->Context);
        } while (status.Level == IGraphTransformer::TStatus::Repeat);
        Y_DEBUG_ABORT_UNLESS(status.Level == IGraphTransformer::TStatus::Ok ||
                     status.Level == IGraphTransformer::TStatus::Error);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            errors = Expr->Context.IssueManager.GetIssues();
            return false;
        }
        return true;
    }

    TString CompileProgram() {
        TRuntimeNode convertedNode = CompileNode(*Expr->Root, Expr->Context, CompileCtx, Compiler.Get());
        TConvertResult convRes = CompileCtx->Finish(convertedNode);
        return NMiniKQL::SerializeRuntimeNode(convRes.Node, CompileCtx->PgmBuilder->GetTypeEnvironment());
    }

    void SendResponseAndDie(const TMiniKQLCompileResult& result, THashMap<TString, ui64> &&resolveCookies, const TActorContext& ctx) {
        ctx.ExecutorThread.Send(
            new IEventHandle(
                ResponseTo,
                ctx.SelfID,
                new TMiniKQLCompileActorEvents::TEvCompileResult(result, std::move(resolveCookies))
            ));
        Die(ctx);
    }

private:
    void Cleanup() {
        CompileCtx.Drop();
        Expr.Drop();
        Compiler.Drop();
    }

    const NKikimr::NMiniKQL::TTypeEnvironment* TypeEnv;
    TString Program;
    TContext::TPtr CompileCtx;
    IDbSchemeResolver* DbSchemeResolver;
    TActorId ResponseTo;
    TExprContainer::TPtr Expr;
    TIntrusivePtr<NCommon::IMkqlCallableCompiler> Compiler;
    THashMap<TString, ui64> ResolveRefreshCookies;
};

NActors::IActor*
CreateCompileActor(const TString& program,
                   const NKikimr::NMiniKQL::TTypeEnvironment* typeEnv,
                   IDbSchemeResolver* dbSchemeResolver,
                   TActorId responseTo,
                   THashMap<TString, ui64> &&resolveRefreshCookies,
                   bool forceCacheRefresh)
{
    return new TMiniKQLCompileActor(program, typeEnv, dbSchemeResolver, responseTo, std::move(resolveRefreshCookies), forceCacheRefresh);
}

} // namespace NYql
