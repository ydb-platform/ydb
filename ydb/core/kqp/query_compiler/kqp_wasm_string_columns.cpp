#include "kqp_wasm_string_columns.h"

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;
using namespace NYql::NNodes;

bool IsStringLikeDataSlot(NUdf::EDataSlot slot) {
    switch (slot) {
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::JsonDocument:
        case NUdf::EDataSlot::DyNumber:
        case NUdf::EDataSlot::Uuid:
            return true;
        default:
            return false;
    }
}

bool IsStringLikeTypeAnn(const TTypeAnnotationNode* type) {
    bool isOptional = false;
    const TDataExprType* dataType = nullptr;
    if (!IsDataOrOptionalOfData(type, isOptional, dataType) || !dataType) {
        return false;
    }
    return IsStringLikeDataSlot(dataType->GetSlot());
}

//! Sequence operators that keep the row shape and the physical column names.
bool IsRowStreamPassThrough(const TExprNode& node) {
    return node.IsCallable({
        "ToFlow",
        "FromFlow",
        "ToStream",
        "ToList",
        "Iterator",
        "Collect",
        "Take",
        "Skip",
        "Filter",
        "OrderedFilter",
        "TakeWhile",
        "SkipWhile",
        "SkipNullMembers",
        "FilterNullMembers",
        "ExtractMembers",
        "AssumeSorted",
    });
}

//! Wide (multi-column) operators that keep the column order.
bool IsWidePassThrough(const TExprNode& node) {
    return node.IsCallable({
        "ToFlow",
        "FromFlow",
        "WideFilter",
        "WideTake",
        "WideSkip",
        "WideTakeWhile",
        "WideSkipWhile",
    });
}

TMaybeNode<TCoAtomList> TryGetReadColumns(TExprBase node) {
    if (auto read = node.Maybe<TKqpWideReadTable>()) {
        return read.Cast().Columns();
    }
    if (auto read = node.Maybe<TKqpWideReadTableRanges>()) {
        return read.Cast().Columns();
    }
    if (auto read = node.Maybe<TKqpWideReadOlapTableRanges>()) {
        return read.Cast().Columns();
    }
    if (auto read = node.Maybe<TKqpBlockReadOlapTableRanges>()) {
        return read.Cast().Columns();
    }
    return {};
}

//! Only sources whose runtime honors PreferWasm (datashard read actor).
TMaybeNode<TCoAtomList> TryGetSourceColumns(TExprBase settings) {
    if (auto source = settings.Maybe<TKqpReadRangesSourceSettings>()) {
        return source.Cast().Columns();
    }
    return {};
}

//! Wrappers that keep the underlying string buffer intact.
const TExprNode* PeelValueWrappers(const TExprNode* node) {
    while (node) {
        TExprBase base(node);
        if (auto just = base.Maybe<TCoJust>()) {
            node = just.Cast().Input().Raw();
        } else if (auto unwrap = base.Maybe<TCoUnwrap>()) {
            node = unwrap.Cast().Optional().Raw();
        } else if (auto coalesce = base.Maybe<TCoCoalesce>()) {
            node = coalesce.Cast().Predicate().Raw();
        } else {
            break;
        }
    }
    return node;
}

//! Udf / AssumeStrict(Udf) / Apply(Udf, runConfig).
const TExprNode* PeelUdfCallable(const TExprNode* node) {
    while (node) {
        if (node->IsCallable("AssumeStrict") && node->ChildrenSize() >= 1) {
            node = node->Child(0);
            continue;
        }
        if (node->IsCallable({"Apply", "NamedApply"}) && node->ChildrenSize() >= 1) {
            return PeelUdfCallable(node->Child(0));
        }
        break;
    }
    return node;
}

bool IsUdf(const TExprNode* node) {
    return node && node->IsCallable({"Udf", "ScriptUdf"});
}

//! One handler lambda of a fold over a stream: where the lambda sits among the
//! callable's children, and which of its arguments is the stream item.
struct TItemHandler {
    ui32 LambdaIndex;
    ui32 ItemArgIndex;
};

//! Folds keep the row intact and pass it to their handlers, so a Member of a
//! handler item is still the buffer the read produced. A full aggregate without
//! GROUP BY lands here: `SUM(Udf(column))` becomes Condense1 over the source
//! rows with the UDF call inside the init and update handlers.
TVector<TItemHandler> GetFoldItemHandlers(const TExprNode& node) {
    // Condense1(input, λ(item), λ(item, state), λ(item, state))
    if (node.IsCallable({"Condense1", "Squeeze1"})) {
        return {{1, 0}, {2, 0}, {3, 0}};
    }
    // Condense(input, state, λ(item, state), λ(item, state))
    if (node.IsCallable({"Condense", "Squeeze"})) {
        return {{2, 0}, {3, 0}};
    }
    // Fold1(input, λ(item), λ(item, state))
    if (node.IsCallable("Fold1")) {
        return {{1, 0}, {2, 0}};
    }
    // Fold(input, state, λ(item, state))
    if (node.IsCallable("Fold")) {
        return {{2, 0}};
    }
    // CombineCore(input, λ(item), λ(key, item), λ(key, item, state), λ(key, state), memLimit)
    if (node.IsCallable("CombineCore")) {
        return {{1, 0}, {2, 1}, {3, 1}};
    }
    return {};
}

class TCollector {
public:
    explicit TCollector(const TDqPhyStage& stage)
        : Stage_(stage)
    {}

    TWasmUdfStringColumns Collect() {
        CollectPhysicalColumns();

        VisitExpr(Stage_.Program().Ptr(), [this](const TExprNode::TPtr& node) {
            Visit(*node);
            return true;
        });

        if (!Result_.HasTableRead || !Result_.HasUdfCall) {
            Result_.Columns.clear();
        }
        return std::move(Result_);
    }

private:
    void CollectPhysicalColumns() {
        const auto program = Stage_.Program();
        const ui32 argsCount = program.Args().Size();
        const ui32 inputsCount = Stage_.Inputs().Size();
        for (ui32 i = 0; i < Min(inputsCount, argsCount); ++i) {
            auto source = Stage_.Inputs().Item(i).Maybe<TDqSource>();
            if (!source) {
                continue;
            }
            auto columns = TryGetSourceColumns(source.Cast().Settings());
            if (!columns) {
                continue;
            }
            Result_.HasTableRead = true;
            AddPhysicalColumns(columns.Cast());
            RowStreamArgs_.insert(program.Args().Arg(i).Raw());
        }

        VisitExpr(program.Ptr(), [this](const TExprNode::TPtr& node) {
            if (auto columns = TryGetReadColumns(TExprBase(node))) {
                Result_.HasTableRead = true;
                AddPhysicalColumns(columns.Cast());
            }
            return true;
        });
    }

    void AddPhysicalColumns(const TCoAtomList& columns) {
        for (const auto& column : columns) {
            PhysicalColumns_.insert(TString(column.Value()));
        }
    }

    void Visit(const TExprNode& node) {
        if (IsUdf(&node)) {
            Result_.HasUdfCall = true;
            return;
        }
        if (node.IsCallable("ExpandMap")) {
            BindRowArg(node, 0, 1);
            return;
        }
        if (node.IsCallable({"WideMap", "OrderedWideMap", "NarrowMap"})) {
            BindWideLambdaArgs(node);
            return;
        }
        if (node.IsCallable({
                "Map", "OrderedMap", "FlatMap", "OrderedFlatMap",
                "Filter", "OrderedFilter", "TakeWhile", "SkipWhile", "IfPresent"}))
        {
            BindSingleLambdaArg(node, 0, 1);
            return;
        }
        if (const auto handlers = GetFoldItemHandlers(node); !handlers.empty()) {
            BindFoldItemArgs(node, handlers);
            return;
        }
        if (node.IsCallable({"Apply", "NamedApply"})) {
            CollectApply(node);
        }
    }

    static const TExprNode* GetSingleLambdaArg(const TExprNode& node, ui32 lambdaIndex) {
        if (node.ChildrenSize() <= lambdaIndex) {
            return nullptr;
        }
        const TExprNode& lambda = *node.Child(lambdaIndex);
        if (!lambda.IsLambda() || lambda.Child(0)->ChildrenSize() != 1) {
            return nullptr;
        }
        return lambda.Child(0)->Child(0);
    }

    void BindRowArg(const TExprNode& node, ui32 inputIndex, ui32 lambdaIndex) {
        if (!IsPhysicalRowStream(node.Child(inputIndex))) {
            return;
        }
        if (const TExprNode* arg = GetSingleLambdaArg(node, lambdaIndex)) {
            RowArgs_.insert(arg);
        }
    }

    //! Either λ(row) over a stream of physical rows, or λ(x) over a single
    //! column value (the AutoMap rewrite of an optional UDF argument).
    void BindSingleLambdaArg(const TExprNode& node, ui32 inputIndex, ui32 lambdaIndex) {
        const TExprNode* arg = GetSingleLambdaArg(node, lambdaIndex);
        if (!arg) {
            return;
        }
        const TExprNode* input = node.Child(inputIndex);
        if (IsPhysicalRowStream(input)) {
            RowArgs_.insert(arg);
            return;
        }
        if (auto column = ResolveColumn(input)) {
            ColumnValues_[arg] = *column;
        }
    }

    void BindFoldItemArgs(const TExprNode& node, const TVector<TItemHandler>& handlers) {
        if (node.ChildrenSize() == 0 || !IsPhysicalRowStream(node.Child(0))) {
            return;
        }
        for (const auto& handler : handlers) {
            if (node.ChildrenSize() <= handler.LambdaIndex) {
                continue;
            }
            const TExprNode& lambda = *node.Child(handler.LambdaIndex);
            if (!lambda.IsLambda()) {
                continue;
            }
            const TExprNode& args = *lambda.Child(0);
            if (args.ChildrenSize() <= handler.ItemArgIndex) {
                continue;
            }
            RowArgs_.insert(args.Child(handler.ItemArgIndex));
        }
    }

    void BindWideLambdaArgs(const TExprNode& node) {
        if (node.ChildrenSize() < 2 || !node.Child(1)->IsLambda()) {
            return;
        }
        TVector<TString> columns;
        if (!TryGetWideColumns(node.Child(0), columns)) {
            return;
        }
        const TExprNode& args = *node.Child(1)->Child(0);
        for (ui32 i = 0; i < Min<ui32>(args.ChildrenSize(), columns.size()); ++i) {
            if (!columns[i].empty()) {
                ColumnValues_[args.Child(i)] = columns[i];
            }
        }
    }

    bool IsPhysicalRowStream(const TExprNode* node) const {
        while (node) {
            if (RowStreamArgs_.contains(node)) {
                return true;
            }
            if (!IsRowStreamPassThrough(*node) || node->ChildrenSize() == 0) {
                return false;
            }
            node = node->Child(0);
        }
        return false;
    }

    //! Column name per index of a wide stream; empty name means "unknown index".
    bool TryGetWideColumns(const TExprNode* node, TVector<TString>& columns) const {
        if (!node) {
            return false;
        }
        if (auto readColumns = TryGetReadColumns(TExprBase(node))) {
            columns.clear();
            for (const auto& column : readColumns.Cast()) {
                columns.emplace_back(column.Value());
            }
            return !columns.empty();
        }
        if (node->ChildrenSize() > 0 && IsWidePassThrough(*node)) {
            return TryGetWideColumns(node->Child(0), columns);
        }
        if (node->IsCallable("ExpandMap")) {
            return TryGetExpandMapColumns(*node, columns);
        }
        if (node->IsCallable({"WideMap", "OrderedWideMap"})) {
            return TryGetWideMapColumns(*node, columns);
        }
        return false;
    }

    //! ExpandMap(rows, λ(row) → Member(row, c0), Member(row, c1), ...)
    bool TryGetExpandMapColumns(const TExprNode& node, TVector<TString>& columns) const {
        if (node.ChildrenSize() < 2 || !IsPhysicalRowStream(node.Child(0))) {
            return false;
        }
        const TExprNode& lambda = *node.Child(1);
        if (!lambda.IsLambda() || lambda.ChildrenSize() < 2 || lambda.Child(0)->ChildrenSize() != 1) {
            return false;
        }
        const TExprNode* rowArg = lambda.Child(0)->Child(0);
        columns.clear();
        for (ui32 i = 1; i < lambda.ChildrenSize(); ++i) {
            columns.emplace_back(GetMemberName(lambda.Child(i), rowArg));
        }
        return !columns.empty();
    }

    bool TryGetWideMapColumns(const TExprNode& node, TVector<TString>& columns) const {
        TVector<TString> inputColumns;
        if (node.ChildrenSize() < 2 || !node.Child(1)->IsLambda()) {
            return false;
        }
        if (!TryGetWideColumns(node.Child(0), inputColumns)) {
            return false;
        }
        const TExprNode& lambda = *node.Child(1);
        if (lambda.ChildrenSize() < 2) {
            return false;
        }
        const TExprNode& args = *lambda.Child(0);
        THashMap<const TExprNode*, TString> argColumns;
        for (ui32 i = 0; i < Min<ui32>(args.ChildrenSize(), inputColumns.size()); ++i) {
            argColumns[args.Child(i)] = inputColumns[i];
        }
        columns.clear();
        for (ui32 i = 1; i < lambda.ChildrenSize(); ++i) {
            const TExprNode* item = PeelValueWrappers(lambda.Child(i));
            const TString* name = item ? argColumns.FindPtr(item) : nullptr;
            columns.emplace_back(name ? *name : TString());
        }
        return !columns.empty();
    }

    static TString GetMemberName(const TExprNode* node, const TExprNode* rowArg) {
        node = PeelValueWrappers(node);
        if (node && node->IsCallable("Member") && node->ChildrenSize() >= 2 && node->Child(0) == rowArg) {
            return TString(node->Child(1)->Content());
        }
        return {};
    }

    TMaybe<TString> ResolveColumn(const TExprNode* node) const {
        node = PeelValueWrappers(node);
        if (!node) {
            return {};
        }
        if (const TString* name = ColumnValues_.FindPtr(node)) {
            return *name;
        }
        if (node->IsCallable("Member") && node->ChildrenSize() >= 2 && RowArgs_.contains(node->Child(0))) {
            return TString(node->Child(1)->Content());
        }
        return {};
    }

    void CollectApply(const TExprNode& apply) {
        if (apply.ChildrenSize() < 2 || !IsUdf(PeelUdfCallable(apply.Child(0)))) {
            // Apply of a captured lambda argument or of a non-UDF callable.
            return;
        }
        if (apply.IsCallable("NamedApply")) {
            for (const auto& positional : apply.Child(1)->Children()) {
                CollectArg(positional.Get());
            }
            return;
        }
        for (ui32 i = 1; i < apply.ChildrenSize(); ++i) {
            CollectArg(apply.Child(i));
        }
    }

    void CollectArg(const TExprNode* arg) {
        arg = PeelValueWrappers(arg);
        if (!arg) {
            return;
        }
        // Physical plans do not always carry type annotations; require string
        // only when the annotation is there.
        if (arg->GetTypeAnn() && !IsStringLikeTypeAnn(arg->GetTypeAnn())) {
            return;
        }
        auto column = ResolveColumn(arg);
        if (column && PhysicalColumns_.contains(*column)) {
            Result_.Columns.insert(*column);
        }
    }

private:
    const TDqPhyStage& Stage_;

    THashSet<TString> PhysicalColumns_;
    //! Stage program arguments bound to a stream of physical rows.
    THashSet<const TExprNode*> RowStreamArgs_;
    //! Lambda arguments bound to a physical row struct.
    THashSet<const TExprNode*> RowArgs_;
    //! Lambda arguments bound to a single physical column value.
    THashMap<const TExprNode*, TString> ColumnValues_;

    TWasmUdfStringColumns Result_;
};

} // namespace

TWasmUdfStringColumns CollectWasmUdfStringColumns(const NYql::NNodes::TDqPhyStage& stage) {
    return TCollector(stage).Collect();
}

} // namespace NKikimr::NKqp
