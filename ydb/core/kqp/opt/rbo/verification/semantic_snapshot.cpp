#include "semantic_snapshot.h"

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_context.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/writer/json_value.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/utils/utf8.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <string_view>
#include <utility>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;
using namespace NYql::NNodes;

class TUnsupportedSnapshot final : public yexception {
};

[[noreturn]] void Unsupported(const TString& reason) {
    ythrow TUnsupportedSnapshot() << reason;
}

bool IsSupportedType(TStringBuf type) {
    static const THashSet<TString> Types = {
        "Bool",
        "Int8", "Int16", "Int32", "Int64",
        "Uint8", "Uint16", "Uint32", "Uint64",
        "String", "Utf8",
    };
    return Types.contains(type);
}

struct TTableReference {
    TString Identity;
    TString Path;
};

void AppendIdentityField(TStringBuilder& out, TStringBuf name, TStringBuf value) {
    out << name << ":" << value.size() << ":" << value << ";";
}

TTableReference TableReference(const TOpRead& read, TStringBuf cluster) {
    if (!read.TableCallable || !TKqpTable::Match(read.TableCallable.Get())) {
        Unsupported("Read has no KqpTable metadata node");
    }
    const auto table = TKqpTable(read.TableCallable);
    const TString path = table.Path().StringValue();
    if (path.empty()) {
        Unsupported("Read has an empty table path");
    }

    TStringBuilder identity;
    AppendIdentityField(identity, "cluster", cluster);
    AppendIdentityField(identity, "path", path);
    AppendIdentityField(identity, "path_id", table.PathId().StringValue());
    AppendIdentityField(identity, "sys_view", table.SysView().StringValue());
    AppendIdentityField(identity, "version", table.Version().StringValue());
    return {identity, path};
}

void VisitOperators(
    const TIntrusivePtr<IOperator>& op,
    THashSet<const IOperator*>& visited,
    const std::function<void(IOperator&)>& visitor)
{
    if (!op || !visited.insert(op.Get()).second) {
        return;
    }
    for (const auto& child : op->GetChildren()) {
        VisitOperators(child, visited, visitor);
    }
    visitor(*op);
}

void CheckSnapshotProperties(const IOperator& op, bool stageGraphPresent) {
    const auto& props = op.Props;
    const bool joinPhysicalProps = props.JoinAlgo || props.UseBlockHashJoin ||
        props.LeftShuffleBy || props.RightShuffleBy;
    if ((!stageGraphPresent && props.StageId) || props.Algorithm || props.OrderEnforcer ||
        props.EnsureAtMostOne ||
        (joinPhysicalProps && (!stageGraphPresent || op.GetKind() != EOperator::Join)))
    {
        Unsupported(TStringBuilder()
            << op.GetExplainName()
            << " has physical properties that logical snapshot v1 cannot represent");
    }
}

bool HasStageGraphState(TOpRoot& root) {
    const auto& graph = root.PlanProps.StageGraph;
    if (!graph.StageIds.empty() || !graph.SourceStages.empty() ||
        !graph.StageInputs.empty() || !graph.StageOutputs.empty() ||
        !graph.Connections.empty() || !graph.StageOutputIndices.empty() ||
        !graph.StageGUIDs.empty())
    {
        return true;
    }

    bool hasStageId = false;
    THashSet<const IOperator*> visited;
    VisitOperators(root.GetInput(), visited, [&](IOperator& op) {
        hasStageId = hasStageId || op.Props.StageId.has_value();
    });
    return hasStageId;
}

bool HasAggregate(TOpRoot& root) {
    bool result = false;
    THashSet<const IOperator*> visited;
    VisitOperators(root.GetInput(), visited, [&](IOperator& op) {
        result = result || op.GetKind() == EOperator::Aggregate;
    });
    return result;
}

NJson::TJsonValue JsonArray() {
    return NJson::TJsonValue(NJson::JSON_ARRAY);
}

NJson::TJsonValue JsonMap() {
    return NJson::TJsonValue(NJson::JSON_MAP);
}

NJson::TJsonValue ColumnExpr(TStringBuf name) {
    if (name.empty()) {
        Unsupported("Column expression has an empty IU name");
    }
    auto value = JsonMap();
    value["kind"] = "column";
    value["column"] = TString(name);
    return value;
}

TString TypeName(const TTypeAnnotationNode* annotation, bool* nullable = nullptr) {
    if (!annotation) {
        Unsupported("Scalar expression has no type annotation");
    }

    bool optional = false;
    const TDataExprType* data = nullptr;
    if (!IsDataOrOptionalOfData(annotation, optional, data) || !data) {
        Unsupported("Scalar expression is not Data or Optional<Data>");
    }
    const TString type(NUdf::GetDataTypeInfo(data->GetSlot()).Name);
    if (!IsSupportedType(type)) {
        Unsupported(TStringBuilder() << "Unsupported scalar type " << type);
    }
    if (nullable) {
        *nullable = optional;
    }
    return type;
}

template <typename T>
T ParseInteger(TStringBuf text, TStringBuf type) {
    T result = 0;
    if (!TryFromString<T>(text, result)) {
        Unsupported(TStringBuilder() << "Invalid " << type << " literal " << text);
    }
    return result;
}

template <typename T>
void CheckIntegerRange(i64 value, TStringBuf type) {
    if (value < static_cast<i64>(std::numeric_limits<T>::min()) ||
        value > static_cast<i64>(std::numeric_limits<T>::max()))
    {
        Unsupported(TStringBuilder() << type << " literal is out of range: " << value);
    }
}

template <typename T>
void CheckUnsignedRange(ui64 value, TStringBuf type) {
    if (value > static_cast<ui64>(std::numeric_limits<T>::max())) {
        Unsupported(TStringBuilder() << type << " literal is out of range: " << value);
    }
}

NJson::TJsonValue LiteralExpr(const TExprNode& node) {
    const TString type(node.Content());
    if (!IsSupportedType(type) || node.ChildrenSize() != 1 || !node.Child(0)->IsAtom()) {
        Unsupported(TStringBuilder() << "Unsupported literal callable " << node.Content());
    }
    const TString value(node.Child(0)->Content());

    auto result = JsonMap();
    result["kind"] = "literal";
    result["type"] = type;

    if (type == "Bool") {
        if (value == "true") {
            result["value"] = true;
        } else if (value == "false") {
            result["value"] = false;
        } else {
            Unsupported(TStringBuilder() << "Invalid Bool literal " << value);
        }
        return result;
    }

    if (type.StartsWith("Int")) {
        const i64 parsed = ParseInteger<i64>(value, type);
        if (type == "Int8") {
            CheckIntegerRange<i8>(parsed, type);
        } else if (type == "Int16") {
            CheckIntegerRange<i16>(parsed, type);
        } else if (type == "Int32") {
            CheckIntegerRange<i32>(parsed, type);
        }
        result["value"] = parsed;
        return result;
    }

    if (type.StartsWith("Uint")) {
        const ui64 parsed = ParseInteger<ui64>(value, type);
        if (type == "Uint8") {
            CheckUnsignedRange<ui8>(parsed, type);
        } else if (type == "Uint16") {
            CheckUnsignedRange<ui16>(parsed, type);
        } else if (type == "Uint32") {
            CheckUnsignedRange<ui32>(parsed, type);
        }
        result["value"] = parsed;
        return result;
    }

    if (!NYql::IsUtf8(std::string_view(value.data(), value.size()))) {
        Unsupported(TStringBuilder() << type << " literal is not valid UTF-8");
    }
    result["value"] = value;
    return result;
}

NJson::TJsonValue ExportExprNode(
    const TExprNode& node,
    const TExprNode* rowArgument,
    const THashSet<TString>& visibleColumns)
{
    if (node.IsCallable("Member")) {
        if (node.ChildrenSize() != 2 || !node.Child(1)->IsAtom()) {
            Unsupported("Malformed Member expression");
        }
        const TString name(node.Child(1)->Content());
        if (node.Child(0) != rowArgument || !visibleColumns.contains(name)) {
            Unsupported(TStringBuilder() << "Member does not reference the input row column " << name);
        }
        return ColumnExpr(name);
    }

    if (IsSupportedType(node.Content())) {
        return LiteralExpr(node);
    }

    if (node.IsCallable("Nothing")) {
        bool nullable = false;
        const TString type = TypeName(node.GetTypeAnn(), &nullable);
        if (!nullable) {
            Unsupported("Nothing expression is not optional");
        }
        auto result = JsonMap();
        result["kind"] = "null";
        result["type"] = type;
        return result;
    }

    if (node.IsCallable("And") || node.IsCallable("Or")) {
        if (node.ChildrenSize() == 0) {
            Unsupported(TStringBuilder() << node.Content() << " has no arguments");
        }
        auto result = JsonMap();
        result["kind"] = node.IsCallable("And") ? "and" : "or";
        auto args = JsonArray();
        for (const auto& child : node.Children()) {
            args.AppendValue(ExportExprNode(*child, rowArgument, visibleColumns));
        }
        result["args"] = std::move(args);
        return result;
    }

    if (node.IsCallable("Not")) {
        if (node.ChildrenSize() != 1) {
            Unsupported("Not must have exactly one argument");
        }
        auto result = JsonMap();
        result["kind"] = "not";
        result["arg"] = ExportExprNode(*node.Child(0), rowArgument, visibleColumns);
        return result;
    }

    if (node.IsCallable("==") || node.IsCallable("IsNotDistinctFrom")) {
        if (node.ChildrenSize() != 2) {
            Unsupported(TStringBuilder() << node.Content() << " must have exactly two arguments");
        }
        auto result = JsonMap();
        result["kind"] = "eq";
        result["left"] = ExportExprNode(*node.Child(0), rowArgument, visibleColumns);
        result["right"] = ExportExprNode(*node.Child(1), rowArgument, visibleColumns);
        if (node.IsCallable("IsNotDistinctFrom")) {
            result["null_safe"] = true;
        }
        return result;
    }

    Unsupported(TStringBuilder() << "Unsupported scalar callable " << node.Content());
}

NJson::TJsonValue ExportExpr(
    const TExpression& expression,
    const THashSet<TString>& visibleColumns)
{
    if (!expression.Node || !expression.Node->IsLambda() || expression.Node->ChildrenSize() != 2) {
        Unsupported("RBO expression is not a one-body lambda");
    }
    const auto* arguments = expression.Node->Child(0);
    if (!arguments->IsArguments() || arguments->ChildrenSize() != 1 ||
        !arguments->Child(0)->IsArgument())
    {
        Unsupported("RBO expression does not have exactly one row argument");
    }
    return ExportExprNode(
        *expression.GetExpressionBody(),
        arguments->Child(0),
        visibleColumns);
}

NJson::TJsonValue TrueExpr() {
    auto result = JsonMap();
    result["kind"] = "literal";
    result["type"] = "Bool";
    result["value"] = true;
    return result;
}

TString JoinKind(TStringBuf kind) {
    static const THashMap<TString, TString> Kinds = {
        {"Cross", "cross"},
        {"Inner", "inner"},
        {"Left", "left"},
        {"Right", "right"},
        {"Full", "full"},
        {"LeftSemi", "left_semi"},
        {"RightSemi", "right_semi"},
        {"LeftOnly", "left_anti"},
        {"RightOnly", "right_anti"},
        {"Exclusion", "exclusion"},
    };
    const auto* result = Kinds.FindPtr(kind);
    if (!result) {
        Unsupported(TStringBuilder() << "Unsupported join kind " << kind);
    }
    return *result;
}

THashSet<TString> OutputNames(IOperator& op) {
    THashSet<TString> result;
    for (const auto& iu : op.GetOutputIUs()) {
        const TString name = iu.GetFullName();
        if (name.empty() || !result.insert(name).second) {
            Unsupported(TStringBuilder() << op.GetExplainName() << " has duplicate or empty output IU " << name);
        }
    }
    return result;
}

const TStructExprType* OutputStructType(IOperator& op) {
    const auto* annotation = op.GetTypeAnn();
    if (!annotation || annotation->GetKind() != ETypeAnnotationKind::List) {
        Unsupported(TStringBuilder() << op.GetExplainName() << " has no list type annotation");
    }
    const auto* item = annotation->Cast<TListExprType>()->GetItemType();
    if (item->GetKind() != ETypeAnnotationKind::Struct) {
        Unsupported(TStringBuilder() << op.GetExplainName() << " output is not a struct");
    }
    return item->Cast<TStructExprType>();
}

const TTypeAnnotationNode* OutputType(IOperator& op, TStringBuf name) {
    const auto* result = OutputStructType(op)->FindItemType(name);
    if (!result) {
        Unsupported(TStringBuilder() << op.GetExplainName() << " output type omits IU " << name);
    }
    return result;
}

TString Phase(EOpPhase phase) {
    switch (phase) {
        case EOpPhase::Undefined:
            return "undefined";
        case EOpPhase::Intermediate:
            return "intermediate";
        case EOpPhase::Final:
            return "final";
    }
    Unsupported("Unknown operator phase");
}

class TPlanExporter {
public:
    TPlanExporter(
        TOpRoot& root,
        const TSemanticSnapshotCatalogV1& catalog,
        TStringBuf cluster,
        bool stageGraphPresent)
        : Root(root)
        , Cluster(cluster)
        , StageGraphPresent(stageGraphPresent)
    {
        for (const auto& table : catalog.Tables) {
            if (table.Name.empty() || !Catalog.emplace(table.Name, &table).second) {
                Unsupported(TStringBuilder() << "Catalog has duplicate or empty table " << table.Name);
            }
        }
    }

    NJson::TJsonValue Export() {
        if (!Root.PlanProps.Subplans.PlanMap.empty() ||
            !Root.PlanProps.Subplans.OrderedList.empty())
        {
            Unsupported("Logical snapshot v1 cannot represent subplans");
        }
        CheckSnapshotProperties(Root, false);
        if (Root.ColumnOrder.empty()) {
            Unsupported("Root output order must not be empty");
        }

        RootId = ExportNode(Root.GetInput());
        const auto rootNames = OutputNames(*Root.GetInput());
        auto output = JsonArray();
        THashSet<TString> seen;
        for (const auto& name : Root.ColumnOrder) {
            if (name.empty() || !seen.insert(name).second || !rootNames.contains(name)) {
                Unsupported(TStringBuilder() << "Invalid root output IU " << name);
            }
            output.AppendValue(name);
        }

        auto result = JsonMap();
        result["nodes"] = std::move(Nodes);
        result["root"] = RootId;
        result["output"] = std::move(output);
        return result;
    }

    const THashMap<const IOperator*, TString>& GetNodeIds() const {
        return Ids;
    }

    const TVector<IOperator*>& GetNodeOrder() const {
        return NodeOrder;
    }

    const TString& GetRootId() const {
        return RootId;
    }

private:
    TString ExportNode(const TIntrusivePtr<IOperator>& op) {
        if (!op) {
            Unsupported("Plan contains a null operator");
        }
        if (const auto* existing = Ids.FindPtr(op.Get())) {
            return *existing;
        }
        if (!Visiting.insert(op.Get()).second) {
            Unsupported("Plan contains an operator cycle");
        }

        TVector<TString> children;
        children.reserve(op->GetChildren().size());
        for (const auto& child : op->GetChildren()) {
            children.push_back(ExportNode(child));
        }
        Visiting.erase(op.Get());
        CheckSnapshotProperties(*op, StageGraphPresent);

        const TString id = TStringBuilder() << "n" << Ids.size();
        auto node = ExportOperator(*op, id, children);
        Ids.emplace(op.Get(), id);
        NodeOrder.push_back(op.Get());
        Nodes.AppendValue(std::move(node));
        return id;
    }

    NJson::TJsonValue ExportOperator(IOperator& base, const TString& id, const TVector<TString>& children) {
        auto node = JsonMap();
        node["id"] = id;

        switch (base.GetKind()) {
            case EOperator::EmptySource:
                if (!children.empty()) {
                    Unsupported("EmptySource unexpectedly has children");
                }
                node["op"] = "empty_source";
                return node;

            case EOperator::Source: {
                if (!children.empty()) {
                    Unsupported("Read unexpectedly has children");
                }
                auto& read = static_cast<TOpRead&>(base);
                if (read.RangeInfo || read.OlapFilterLambda || read.Limit || read.SortDir != ESortDir::None) {
                    Unsupported("Read has pushdown, limit, or ordering semantics absent from logical snapshot v1");
                }
                if (read.Columns.empty() || read.Columns.size() != read.OutputIUs.size()) {
                    Unsupported("Read has an empty or inconsistent column mapping");
                }
                const auto table = TableReference(read, Cluster);
                const auto* catalogTable = Catalog.FindPtr(table.Identity);
                if (!catalogTable) {
                    Unsupported(TStringBuilder()
                        << "Read table identity " << table.Identity
                        << " is absent from the captured catalog");
                }
                THashSet<TString> catalogColumns;
                for (const auto& column : (*catalogTable)->Columns) {
                    catalogColumns.insert(column.Name);
                }
                THashSet<TString> sources;
                THashSet<TString> outputs;
                auto columns = JsonArray();
                for (size_t index = 0; index < read.Columns.size(); ++index) {
                    const TString output = read.OutputIUs[index].GetFullName();
                    if (!catalogColumns.contains(read.Columns[index]) ||
                        !sources.insert(read.Columns[index]).second || output.empty() ||
                        !outputs.insert(output).second)
                    {
                        Unsupported(TStringBuilder() << "Invalid Read column mapping for " << table.Path);
                    }
                    auto column = JsonMap();
                    column["source"] = read.Columns[index];
                    column["output"] = output;
                    columns.AppendValue(std::move(column));
                }
                node["op"] = "scan";
                node["table"] = table.Identity;
                node["columns"] = std::move(columns);
                return node;
            }

            case EOperator::Map: {
                if (children.size() != 1) {
                    Unsupported("Map must have one input");
                }
                auto& map = static_cast<TOpMap&>(base);
                if (map.IsOrdered()) {
                    Unsupported("Ordered Map is absent from logical snapshot v1");
                }
                const auto inputNames = OutputNames(*map.GetInput());
                THashSet<TString> renameSources;
                for (const auto& element : map.MapElements) {
                    if (element.IsRename()) {
                        const TString source = element.GetRename().GetFullName();
                        if (!inputNames.contains(source) || !renameSources.insert(source).second) {
                            Unsupported(TStringBuilder() << "Invalid Map rename source " << source);
                        }
                    }
                }

                auto columns = JsonArray();
                THashSet<TString> outputs;
                for (const auto& iu : map.GetInput()->GetOutputIUs()) {
                    const TString name = iu.GetFullName();
                    if (renameSources.contains(name)) {
                        continue;
                    }
                    if (!outputs.insert(name).second) {
                        Unsupported(TStringBuilder() << "Duplicate Map output " << name);
                    }
                    auto column = JsonMap();
                    column["output"] = name;
                    column["expression"] = ColumnExpr(name);
                    columns.AppendValue(std::move(column));
                }
                for (const auto& element : map.MapElements) {
                    const TString output = element.GetElementName().GetFullName();
                    if (output.empty() || !outputs.insert(output).second) {
                        Unsupported(TStringBuilder() << "Duplicate or empty Map output " << output);
                    }
                    auto column = JsonMap();
                    column["output"] = output;
                    column["expression"] = element.IsRename()
                        ? ColumnExpr(element.GetRename().GetFullName())
                        : ExportExpr(element.GetExpression(), inputNames);
                    columns.AppendValue(std::move(column));
                }
                if (outputs.empty()) {
                    Unsupported("Project with no columns is absent from logical snapshot v1");
                }
                node["op"] = "project";
                node["input"] = children[0];
                node["columns"] = std::move(columns);
                return node;
            }

            case EOperator::Filter: {
                if (children.size() != 1) {
                    Unsupported("Filter must have one input");
                }
                auto& filter = static_cast<TOpFilter&>(base);
                const auto inputNames = OutputNames(*filter.GetInput());
                node["op"] = "filter";
                node["input"] = children[0];
                node["predicate"] = ExportExpr(filter.FilterExpr, inputNames);
                return node;
            }

            case EOperator::Aggregate: {
                if (children.size() != 1) {
                    Unsupported("Aggregate must have one input");
                }
                auto& aggregate = static_cast<TOpAggregate&>(base);
                const auto inputNames = OutputNames(*aggregate.GetInput());
                const auto outputNames = OutputNames(aggregate);
                const auto traits = aggregate.GetAggregationTraits();
                if (traits.empty()) {
                    Unsupported("Aggregate has no traits");
                }

                auto keys = JsonArray();
                THashSet<TString> expectedOutputs;
                TVector<TString> expectedOutputOrder;
                THashSet<TString> seenKeys;
                for (const auto& key : aggregate.GetKeyColumns()) {
                    const TString name = key.GetFullName();
                    if (name.empty() || !inputNames.contains(name) || !seenKeys.insert(name).second) {
                        Unsupported(TStringBuilder() << "Invalid Aggregate key " << name);
                    }
                    keys.AppendValue(name);
                    if (!aggregate.IsDistinctAll()) {
                        expectedOutputs.insert(name);
                        expectedOutputOrder.push_back(name);
                        bool inputNullable = false;
                        bool outputNullable = false;
                        const TString inputType = TypeName(
                            OutputType(*aggregate.GetInput(), name),
                            &inputNullable);
                        const TString outputType = TypeName(
                            OutputType(aggregate, name),
                            &outputNullable);
                        if (inputType != outputType || inputNullable != outputNullable) {
                            Unsupported(TStringBuilder()
                                << "Aggregate key output type disagrees with input IU " << name);
                        }
                    }
                }

                auto aggregates = JsonArray();
                for (const auto& trait : traits) {
                    const TString input = trait.OriginalColName.GetFullName();
                    const TString output = trait.ResultColName.GetFullName();
                    if (input.empty() || !inputNames.contains(input) || output.empty() ||
                        !expectedOutputs.insert(output).second || trait.AggFunction.empty())
                    {
                        Unsupported(TStringBuilder() << "Invalid Aggregate trait " << output);
                    }
                    expectedOutputOrder.push_back(output);
                    bool nullable = false;
                    const TString type = TypeName(OutputType(aggregate, output), &nullable);
                    auto item = JsonMap();
                    item["input"] = input;
                    item["function"] = trait.AggFunction;
                    item["output"] = output;
                    item["type"] = type;
                    item["nullable"] = nullable;
                    item["distinct"] = trait.Distinct;
                    item["unwrap"] = trait.Unwrap;
                    aggregates.AppendValue(std::move(item));
                }
                const auto& actualOutputIUs = aggregate.GetOutputIUs();
                if (actualOutputIUs.size() != expectedOutputOrder.size()) {
                    Unsupported("Aggregate output IU count does not match keys and traits");
                }
                for (size_t index = 0; index < expectedOutputOrder.size(); ++index) {
                    if (actualOutputIUs[index].GetFullName() != expectedOutputOrder[index]) {
                        Unsupported("Aggregate output IU order does not match keys and traits");
                    }
                }
                const auto* outputStruct = OutputStructType(aggregate);
                if (outputStruct->GetItems().size() != expectedOutputs.size()) {
                    Unsupported("Aggregate output type field count does not match output IUs");
                }
                for (const auto* item : outputStruct->GetItems()) {
                    const TString name(item->GetName());
                    if (!expectedOutputs.contains(name)) {
                        Unsupported(TStringBuilder() << "Unexpected Aggregate output type field " << name);
                    }
                    TypeName(item->GetItemType());
                }
                if (expectedOutputs.size() != outputNames.size()) {
                    Unsupported("Aggregate output IUs do not match keys and traits");
                }

                node["op"] = "aggregate";
                node["input"] = children[0];
                node["keys"] = std::move(keys);
                node["aggregates"] = std::move(aggregates);
                node["phase"] = Phase(aggregate.GetAggregationPhase());
                node["distinct_all"] = aggregate.IsDistinctAll();
                return node;
            }

            case EOperator::Join: {
                if (children.size() != 2) {
                    Unsupported("Join must have two inputs");
                }
                auto& join = static_cast<TOpJoin&>(base);
                const auto leftNames = OutputNames(*join.GetLeftInput());
                const auto rightNames = OutputNames(*join.GetRightInput());
                for (const auto& name : leftNames) {
                    if (rightNames.contains(name)) {
                        Unsupported(TStringBuilder() << "Join inputs share IU " << name);
                    }
                }
                auto visibleNames = leftNames;
                visibleNames.insert(rightNames.begin(), rightNames.end());

                auto conjuncts = JsonArray();
                size_t conjunctCount = 0;
                for (const auto& [left, right] : join.JoinKeys) {
                    const TString leftName = left.GetFullName();
                    const TString rightName = right.GetFullName();
                    if (!leftNames.contains(leftName) || !rightNames.contains(rightName)) {
                        Unsupported(TStringBuilder() << "Join key is absent from its declared input");
                    }
                    auto equality = JsonMap();
                    equality["kind"] = "eq";
                    equality["left"] = ColumnExpr(leftName);
                    equality["right"] = ColumnExpr(rightName);
                    conjuncts.AppendValue(std::move(equality));
                    ++conjunctCount;
                }
                for (const auto& filter : join.JoinFilters) {
                    conjuncts.AppendValue(ExportExpr(filter, visibleNames));
                    ++conjunctCount;
                }

                NJson::TJsonValue predicate;
                if (conjunctCount == 0) {
                    predicate = TrueExpr();
                } else if (conjunctCount == 1) {
                    predicate = std::move(conjuncts[0]);
                } else {
                    predicate = JsonMap();
                    predicate["kind"] = "and";
                    predicate["args"] = std::move(conjuncts);
                }
                node["op"] = "join";
                node["left"] = children[0];
                node["right"] = children[1];
                node["kind"] = JoinKind(join.JoinKind);
                node["predicate"] = std::move(predicate);
                return node;
            }

            case EOperator::UnionAll: {
                if (children.size() != 2) {
                    Unsupported("UnionAll must have two inputs");
                }
                auto& unionAll = static_cast<TOpUnionAll&>(base);
                if (unionAll.Ordered) {
                    Unsupported("Ordered UnionAll is absent from logical snapshot v1");
                }
                if (unionAll.Columns.empty()) {
                    Unsupported("UnionAll has no output columns");
                }
                const auto leftNames = OutputNames(*unionAll.GetLeftInput());
                const auto rightNames = OutputNames(*unionAll.GetRightInput());
                auto inputColumns = JsonArray();
                auto output = JsonArray();
                THashSet<TString> outputNames;
                for (const auto& iu : unionAll.Columns) {
                    const TString name = iu.GetFullName();
                    if (!leftNames.contains(name) || !rightNames.contains(name) ||
                        name.empty() || !outputNames.insert(name).second)
                    {
                        Unsupported(TStringBuilder() << "Invalid UnionAll column " << name);
                    }
                    inputColumns.AppendValue(name);
                    output.AppendValue(name);
                }
                auto inputs = JsonArray();
                for (const auto& child : children) {
                    auto input = JsonMap();
                    input["node"] = child;
                    input["columns"] = inputColumns;
                    inputs.AppendValue(std::move(input));
                }
                node["op"] = "union_all";
                node["inputs"] = std::move(inputs);
                node["output"] = std::move(output);
                return node;
            }

            default:
                Unsupported(TStringBuilder() << "Unsupported operator " << base.GetExplainName());
        }
    }

private:
    TOpRoot& Root;
    TString Cluster;
    bool StageGraphPresent = false;
    THashMap<TString, const TSemanticSnapshotCatalogTableV1*> Catalog;
    THashMap<const IOperator*, TString> Ids;
    THashSet<const IOperator*> Visiting;
    TVector<IOperator*> NodeOrder;
    TString RootId;
    NJson::TJsonValue Nodes = JsonArray();
};

class TStageGraphExporter {
private:
    using TStagePair = std::pair<ui32, ui32>;

    struct TBoundary {
        IOperator* ProducerNode = nullptr;
        TIntrusivePtr<TConnection> Connection;
        ui32 Occurrence = 0;
    };

public:
    TStageGraphExporter(
        TOpRoot& root,
        const THashMap<const IOperator*, TString>& nodeIds,
        const TVector<IOperator*>& nodeOrder,
        TString rootNodeId)
        : Root(root)
        , Graph(root.PlanProps.StageGraph)
        , NodeIds(nodeIds)
        , NodeOrder(nodeOrder)
        , RootNodeId(std::move(rootNodeId))
    {
    }

    NJson::TJsonValue Export() {
        ValidateGraphContainers();
        IndexStageNodes();
        IndexBoundaries();
        BuildEdgesAndInputs();
        ValidateTaskSemantics();
        ValidateAndCompleteOutputs();

        auto stages = JsonArray();
        for (ui32 stageId = 0; stageId < StageCount; ++stageId) {
            auto stage = JsonMap();
            stage["id"] = StableStageId(stageId);

            auto nodes = JsonArray();
            for (const auto* op : StageNodes[stageId]) {
                nodes.AppendValue(NodeId(*op));
            }
            stage["nodes"] = std::move(nodes);

            auto inputs = JsonArray();
            for (const auto& nodeId : StageInputNodes[stageId]) {
                inputs.AppendValue(nodeId);
            }
            stage["inputs"] = std::move(inputs);

            auto outputs = JsonArray();
            for (const auto& [index, nodeId] : StageOutputNodes[stageId]) {
                auto output = JsonMap();
                output["index"] = static_cast<ui64>(index);
                output["node"] = nodeId;
                outputs.AppendValue(std::move(output));
            }
            stage["outputs"] = std::move(outputs);

            const auto source = Graph.SourceStages.find(stageId);
            if (source == Graph.SourceStages.end()) {
                stage["source_storage"] = NJson::TJsonValue(NJson::JSON_NULL);
            } else {
                stage["source_storage"] = SourceStorage(source->second.StorageType);
            }
            stages.AppendValue(std::move(stage));
        }

        auto result = JsonMap();
        result["root_stage"] = StableStageId(RootStageId);
        result["stages"] = std::move(stages);
        result["edges"] = std::move(Edges);
        result["assumptions"] = JsonArray();
        return result;
    }

private:
    static TString StableStageId(ui32 stageId) {
        return TStringBuilder() << "s" << stageId;
    }

    const TString& NodeId(const IOperator& op) const {
        const auto* id = NodeIds.FindPtr(&op);
        if (!id) {
            Unsupported(TStringBuilder()
                << "StageGraph references an operator absent from the logical plan: "
                << op.GetExplainName());
        }
        return *id;
    }

    ui32 OperatorStage(const IOperator& op) const {
        if (!op.Props.StageId || *op.Props.StageId < 0 ||
            static_cast<ui64>(*op.Props.StageId) >= StageCount)
        {
            Unsupported(TStringBuilder()
                << "StageGraph has a missing or invalid stage for " << op.GetExplainName());
        }
        return static_cast<ui32>(*op.Props.StageId);
    }

    static TString SourceStorage(NYql::EStorageType storage) {
        switch (storage) {
            case NYql::EStorageType::RowStorage:
                return "row";
            case NYql::EStorageType::ColumnStorage:
                return "column";
            default:
                Unsupported("StageGraph has unsupported source distribution storage");
        }
    }

    void ValidateGraphContainers() {
        StageCount = Graph.StageIds.size();
        if (StageCount == 0 || StageCount > std::numeric_limits<ui32>::max()) {
            Unsupported("StageGraph must contain a representable non-empty stage set");
        }

        THashSet<ui32> stageIds;
        for (const auto stageId : Graph.StageIds) {
            if (stageId >= StageCount || !stageIds.insert(stageId).second) {
                Unsupported("StageGraph stage IDs must be unique and contiguous from zero");
            }
        }
        for (ui32 stageId = 0; stageId < StageCount; ++stageId) {
            if (!stageIds.contains(stageId)) {
                Unsupported("StageGraph stage IDs must be unique and contiguous from zero");
            }
        }

        if (Graph.StageInputs.size() != StageCount ||
            Graph.StageOutputs.size() != StageCount ||
            Graph.StageGUIDs.size() != StageCount)
        {
            Unsupported("StageGraph stage containers do not cover the exact stage set");
        }

        THashSet<TString> guids;
        for (ui32 stageId = 0; stageId < StageCount; ++stageId) {
            if (!Graph.StageInputs.contains(stageId) ||
                !Graph.StageOutputs.contains(stageId))
            {
                Unsupported("StageGraph input/output containers have an unknown or missing stage");
            }
            const auto guid = Graph.StageGUIDs.find(stageId);
            if (guid == Graph.StageGUIDs.end() || guid->second.empty() ||
                !guids.insert(guid->second).second)
            {
                Unsupported("StageGraph GUIDs must be present and unique");
            }
        }
        for (const auto& [stageId, _] : Graph.StageInputs) {
            if (!stageIds.contains(stageId)) {
                Unsupported("StageGraph inputs contain an unknown stage");
            }
        }
        for (const auto& [stageId, _] : Graph.StageOutputs) {
            if (!stageIds.contains(stageId)) {
                Unsupported("StageGraph outputs contain an unknown stage");
            }
        }
        for (const auto& [stageId, traits] : Graph.SourceStages) {
            if (!stageIds.contains(stageId)) {
                Unsupported("StageGraph source traits contain an unknown stage");
            }
            SourceStorage(traits.StorageType);
        }

        TMap<TStagePair, size_t> inputCounts;
        TMap<TStagePair, size_t> outputCounts;
        for (ui32 consumer = 0; consumer < StageCount; ++consumer) {
            for (const auto producer : Graph.StageInputs.at(consumer)) {
                if (!stageIds.contains(producer) || producer == consumer) {
                    Unsupported("StageGraph inputs contain an unknown or self stage edge");
                }
                ++inputCounts[{producer, consumer}];
            }
        }
        for (ui32 producer = 0; producer < StageCount; ++producer) {
            for (const auto consumer : Graph.StageOutputs.at(producer)) {
                if (!stageIds.contains(consumer) || producer == consumer) {
                    Unsupported("StageGraph outputs contain an unknown or self stage edge");
                }
                ++outputCounts[{producer, consumer}];
            }
        }

        if (inputCounts.size() != Graph.Connections.size() ||
            outputCounts.size() != Graph.Connections.size())
        {
            Unsupported("StageGraph connection keys disagree with stage inputs or outputs");
        }
        for (const auto& [pair, connections] : Graph.Connections) {
            if (!stageIds.contains(pair.first) || !stageIds.contains(pair.second) ||
                pair.first == pair.second || connections.empty())
            {
                Unsupported("StageGraph contains an invalid connection group");
            }
            const auto inputCount = inputCounts.find(pair);
            const auto outputCount = outputCounts.find(pair);
            if (inputCount == inputCounts.end() || outputCount == outputCounts.end() ||
                inputCount->second != connections.size() ||
                outputCount->second != connections.size())
            {
                Unsupported("StageGraph duplicate edge counts are inconsistent");
            }
            for (const auto& connection : connections) {
                if (!connection) {
                    Unsupported("StageGraph contains a null connection");
                }
            }
        }

        // Kahn's algorithm counts duplicate occurrences exactly as the physical graph does.
        TVector<size_t> indegree(StageCount, 0);
        for (ui32 stageId = 0; stageId < StageCount; ++stageId) {
            indegree[stageId] = Graph.StageInputs.at(stageId).size();
        }
        TVector<ui32> ready;
        for (ui32 stageId = 0; stageId < StageCount; ++stageId) {
            if (indegree[stageId] == 0) {
                ready.push_back(stageId);
            }
        }
        size_t processed = 0;
        for (size_t index = 0; index < ready.size(); ++index) {
            const auto stageId = ready[index];
            TopologicalStages.push_back(stageId);
            ++processed;
            for (const auto consumer : Graph.StageOutputs.at(stageId)) {
                if (indegree[consumer] == 0) {
                    Unsupported("StageGraph edge multiplicities underflow during validation");
                }
                if (--indegree[consumer] == 0) {
                    ready.push_back(consumer);
                }
            }
        }
        if (processed != StageCount) {
            Unsupported("StageGraph contains a cycle");
        }

        StageNodes.resize(StageCount);
        StageSinks.resize(StageCount, nullptr);
        StageInputNodes.resize(StageCount);
        LogicalInputNodes.resize(StageCount);
        StageOutputNodes.resize(StageCount);
    }

    void IndexStageNodes() {
        if (NodeOrder.empty() || NodeOrder.size() != NodeIds.size()) {
            Unsupported("StageGraph operator membership cannot be indexed");
        }

        TVector<size_t> sourceCounts(StageCount, 0);
        THashMap<const IOperator*, size_t> sameStageParents;
        for (auto* op : NodeOrder) {
            const auto stageId = OperatorStage(*op);
            StageNodes[stageId].push_back(op);
            sameStageParents.emplace(op, 0);

            if (op->GetKind() == EOperator::Join) {
                const auto& props = op->Props;
                if ((props.LeftShuffleBy && props.LeftShuffleBy->empty()) ||
                    (props.RightShuffleBy && props.RightShuffleBy->empty()))
                {
                    Unsupported(
                        "StageGraph relies on an unsupported source co-partitioning "
                        "assumption from shuffle elimination");
                }
            }

            if (op->GetKind() != EOperator::Source) {
                continue;
            }
            ++sourceCounts[stageId];
            const auto source = Graph.SourceStages.find(stageId);
            const auto& read = static_cast<const TOpRead&>(*op);
            if (source == Graph.SourceStages.end() ||
                source->second.StorageType != read.StorageType)
            {
                Unsupported("StageGraph source operator and source distribution traits disagree");
            }
        }

        for (auto* parent : NodeOrder) {
            const auto parentStage = OperatorStage(*parent);
            for (const auto& child : parent->GetChildren()) {
                if (!child || OperatorStage(*child) != parentStage) {
                    continue;
                }
                auto* parentCount = sameStageParents.FindPtr(child.Get());
                if (!parentCount) {
                    Unsupported("StageGraph same-stage child is absent from plan membership");
                }
                ++*parentCount;
            }
        }

        for (ui32 stageId = 0; stageId < StageCount; ++stageId) {
            if (StageNodes[stageId].empty()) {
                Unsupported("StageGraph contains a stage with no operator node membership");
            }
            const bool sourceStage = Graph.SourceStages.contains(stageId);
            if ((sourceStage && sourceCounts[stageId] != 1) ||
                (!sourceStage && sourceCounts[stageId] != 0))
            {
                Unsupported("StageGraph source stage membership is inconsistent");
            }
            if (sourceStage &&
                Graph.SourceStages.at(stageId).StorageType == NYql::EStorageType::RowStorage &&
                StageNodes[stageId].size() != 1)
            {
                Unsupported("StageGraph row-storage source stage must contain only its Read");
            }
            if (sourceStage && !Graph.StageInputs.at(stageId).empty()) {
                Unsupported("StageGraph source stage unexpectedly has stage inputs");
            }

            for (auto* op : StageNodes[stageId]) {
                const auto* parentCount = sameStageParents.FindPtr(op);
                if (parentCount && *parentCount == 0) {
                    if (StageSinks[stageId]) {
                        Unsupported("StageGraph stage has more than one logical sink");
                    }
                    StageSinks[stageId] = op;
                }
            }
            if (!StageSinks[stageId]) {
                Unsupported("StageGraph stage has no logical sink");
            }
        }

        RootStageId = OperatorStage(*Root.GetInput());
        if (StageSinks[RootStageId] != Root.GetInput().Get() ||
            NodeId(*StageSinks[RootStageId]) != RootNodeId)
        {
            Unsupported("StageGraph root operator does not match plan.root");
        }
        if (!Graph.StageOutputs.at(RootStageId).empty()) {
            Unsupported("StageGraph root stage must be the unique output sink");
        }

        THashSet<ui32> reachesRoot;
        TVector<ui32> pending = {RootStageId};
        for (size_t index = 0; index < pending.size(); ++index) {
            const auto stageId = pending[index];
            if (!reachesRoot.insert(stageId).second) {
                continue;
            }
            for (const auto producer : Graph.StageInputs.at(stageId)) {
                pending.push_back(producer);
            }
        }
        if (reachesRoot.size() != StageCount) {
            Unsupported("StageGraph contains a stage disconnected from the root stage");
        }
    }

    void IndexBoundaries() {
        for (auto* consumerNode : NodeOrder) {
            const auto consumerStage = OperatorStage(*consumerNode);
            for (const auto& child : consumerNode->GetChildren()) {
                if (!child) {
                    Unsupported("StageGraph logical operator has a null child");
                }
                const auto producerStage = OperatorStage(*child);
                if (producerStage == consumerStage) {
                    if (consumerNode->GetKind() == EOperator::Join ||
                        consumerNode->GetKind() == EOperator::UnionAll)
                    {
                        Unsupported(
                            "StageGraph Join/UnionAll input must be a cross-stage consumer input");
                    }
                    continue;
                }

                const TStagePair pair{producerStage, consumerStage};
                auto& boundaries = Boundaries[pair];
                const auto occurrence = boundaries.size();
                if (occurrence > std::numeric_limits<ui32>::max()) {
                    Unsupported("StageGraph has too many duplicate edge occurrences");
                }
                const auto connection = Graph.TryGetConnection(
                    producerStage,
                    consumerStage,
                    static_cast<ui32>(occurrence));
                if (!connection) {
                    Unsupported("StageGraph lacks a connection for a cross-stage logical child");
                }
                boundaries.push_back({child.Get(), connection, static_cast<ui32>(occurrence)});
                LogicalInputNodes[consumerStage].push_back(NodeId(*child));
            }
        }

        if (Boundaries.size() != Graph.Connections.size()) {
            Unsupported("StageGraph connections do not match cross-stage logical children");
        }
        for (const auto& [pair, connections] : Graph.Connections) {
            const auto boundaries = Boundaries.find(pair);
            if (boundaries == Boundaries.end() ||
                boundaries->second.size() != connections.size())
            {
                Unsupported("StageGraph connection occurrences do not match logical child occurrences");
            }
        }
    }

    TString HashFunction(const TShuffleConnection& shuffle) const {
        if (!shuffle.HashFuncType) {
            Unsupported("StageGraph HashShuffle connection has no hash function");
        }
        switch (*shuffle.HashFuncType) {
            case NYql::NDq::EHashShuffleFuncType::HashV1:
                return "HashV1";
            case NYql::NDq::EHashShuffleFuncType::HashV2:
                return "HashV2";
            case NYql::NDq::EHashShuffleFuncType::ColumnShardHashV1:
                Unsupported(
                    "StageGraph ColumnShardHashV1 requires shard mapping absent from "
                    "the version-one snapshot");
        }
        Unsupported("StageGraph HashShuffle connection has an unsupported hash function");
    }

    void CheckConnectionType(const TConnection& connection, TStringBuf expected) const {
        if (connection.Type != expected) {
            Unsupported(TStringBuilder()
                << "StageGraph connection runtime type disagrees with Type="
                << connection.Type);
        }
    }

    NJson::TJsonValue ExportEdge(
        const TStagePair& pair,
        const TBoundary& boundary,
        ui32 consumerInput,
        size_t edgeIndex)
    {
        const auto& connection = *boundary.Connection;
        const ui32 outputIndex = connection.GetOutputIndex();
        const TString& producerNodeId = NodeId(*boundary.ProducerNode);
        if (StageSinks[pair.first] != boundary.ProducerNode) {
            Unsupported("StageGraph producer output does not map the stage's logical sink");
        }
        auto [output, inserted] = StageOutputNodes[pair.first].emplace(
            outputIndex,
            producerNodeId);
        if (!inserted) {
            Unsupported("StageGraph reuses a producer output index for multiple edge occurrences");
        }

        auto edge = JsonMap();
        edge["id"] = TStringBuilder() << "e" << edgeIndex;
        edge["producer"] = StableStageId(pair.first);
        edge["consumer"] = StableStageId(pair.second);
        edge["occurrence"] = static_cast<ui64>(boundary.Occurrence);
        edge["producer_output"] = static_cast<ui64>(outputIndex);
        edge["consumer_input"] = static_cast<ui64>(consumerInput);

        if (dynamic_cast<const TMapConnection*>(&connection)) {
            CheckConnectionType(connection, "Map");
            edge["kind"] = "map";
            return edge;
        }
        if (dynamic_cast<const TBroadcastConnection*>(&connection)) {
            CheckConnectionType(connection, "Broadcast");
            edge["kind"] = "broadcast";
            return edge;
        }
        if (const auto* shuffle = dynamic_cast<const TShuffleConnection*>(&connection)) {
            CheckConnectionType(connection, "HashShuffle");
            if (shuffle->Keys.empty()) {
                Unsupported("StageGraph HashShuffle connection has no keys");
            }
            edge["kind"] = "hash_shuffle";
            auto keys = JsonArray();
            const auto producerOutputs = OutputNames(*boundary.ProducerNode);
            for (const auto& key : shuffle->Keys) {
                const TString name = key.GetFullName();
                if (name.empty() || !producerOutputs.contains(name)) {
                    Unsupported("StageGraph HashShuffle key is absent from its producer output");
                }
                keys.AppendValue(name);
            }
            edge["keys"] = std::move(keys);
            edge["hash_function"] = HashFunction(*shuffle);
            edge["use_spilling"] = shuffle->UseSpilling;
            return edge;
        }
        if (const auto* unionAll = dynamic_cast<const TUnionAllConnection*>(&connection)) {
            CheckConnectionType(connection, "UnionAll");
            edge["kind"] = "union_all";
            edge["parallel"] = unionAll->IsParallel();
            return edge;
        }
        if (const auto* merge = dynamic_cast<const TMergeConnection*>(&connection)) {
            CheckConnectionType(connection, "Merge");
            if (merge->Order.empty()) {
                Unsupported("StageGraph Merge connection has no ordering");
            }
            edge["kind"] = "merge";
            auto order = JsonArray();
            const auto producerOutputs = OutputNames(*boundary.ProducerNode);
            for (const auto& sort : merge->Order) {
                const TString column = sort.SortColumn.GetFullName();
                if (column.empty() || !producerOutputs.contains(column)) {
                    Unsupported("StageGraph Merge column is absent from its producer output");
                }
                auto item = JsonMap();
                item["column"] = column;
                item["ascending"] = sort.Ascending;
                item["nulls_first"] = sort.NullsFirst;
                order.AppendValue(std::move(item));
            }
            edge["order"] = std::move(order);
            return edge;
        }
        if (dynamic_cast<const TSourceConnection*>(&connection)) {
            Unsupported("StageGraph source connections have unsupported distribution semantics");
        }
        Unsupported(TStringBuilder()
            << "StageGraph has unsupported connection type " << connection.Type);
    }

    void BuildEdgesAndInputs() {
        size_t edgeIndex = 0;
        for (ui32 consumer = 0; consumer < StageCount; ++consumer) {
            THashSet<ui32> processedProducers;
            ui32 consumerInput = 0;
            // Keep this identical to TPhysicalQueryBuilder::BuildPhysicalStageGraph:
            // at a producer's first StageInputs occurrence, append every connection
            // for that (producer, consumer) pair in connection insertion order.
            for (const auto producer : Graph.StageInputs.at(consumer)) {
                if (!processedProducers.insert(producer).second) {
                    continue;
                }

                const TStagePair pair{producer, consumer};
                const auto boundaries = Boundaries.find(pair);
                if (boundaries == Boundaries.end()) {
                    Unsupported("StageGraph stage input has no logical boundary");
                }
                for (const auto& boundary : boundaries->second) {
                    if (consumerInput == std::numeric_limits<ui32>::max()) {
                        Unsupported("StageGraph has too many consumer inputs");
                    }
                    const TString& producerNodeId = NodeId(*boundary.ProducerNode);
                    StageInputNodes[consumer].push_back(producerNodeId);
                    Edges.AppendValue(ExportEdge(
                        pair,
                        boundary,
                        consumerInput,
                        edgeIndex++));
                    ++consumerInput;
                }
            }

            if (StageInputNodes[consumer] != LogicalInputNodes[consumer]) {
                Unsupported(
                    "StageGraph effective consumer input order disagrees with "
                    "cross-stage logical child order");
            }
        }
    }

    void ValidateAndCompleteOutputs() {
        for (const auto& [stageId, _] : Graph.StageOutputIndices) {
            if (stageId >= StageCount) {
                Unsupported("StageGraph output-index state contains an unknown stage");
            }
        }

        for (ui32 stageId = 0; stageId < StageCount; ++stageId) {
            auto& outputs = StageOutputNodes[stageId];
            const auto recorded = Graph.StageOutputIndices.find(stageId);
            if (stageId == RootStageId) {
                if (!outputs.empty() || recorded != Graph.StageOutputIndices.end()) {
                    Unsupported("StageGraph root stage unexpectedly has producer outputs");
                }
                // The final gather is implicit at this snapshot boundary.
                outputs.emplace(0, RootNodeId);
                continue;
            }

            if (outputs.empty() || recorded == Graph.StageOutputIndices.end() ||
                recorded->second != outputs.size() - 1)
            {
                Unsupported("StageGraph producer output-index state is incomplete");
            }
            ui32 expected = 0;
            for (const auto& [index, _] : outputs) {
                if (index != expected++) {
                    Unsupported("StageGraph producer output indices must be contiguous from zero");
                }
            }
        }
    }

    void ValidateTaskSemantics() const {
        // Mirror CountComputeTasks with a bounded choice of two tasks for a
        // non-Map HashShuffle stage. Channel-builder constraints are checked
        // against the final count below.
        TVector<ui32> taskCounts(StageCount, 0);
        for (const auto stageId : TopologicalStages) {
            if (Graph.SourceStages.contains(stageId)) {
                taskCounts[stageId] = 2;
                continue;
            }
            if (Graph.StageInputs.at(stageId).empty()) {
                taskCounts[stageId] = 1;
                continue;
            }

            ui32 taskCount = 1;
            bool hasShuffle = false;
            bool forceMapTasks = false;
            ui32 mapConnectionCount = 0;
            THashSet<ui32> processedProducers;
            for (const auto producer : Graph.StageInputs.at(stageId)) {
                if (!processedProducers.insert(producer).second) {
                    continue;
                }
                for (const auto& connection : Graph.GetConnections(producer, stageId)) {
                    if (dynamic_cast<const TMapConnection*>(connection.Get())) {
                        taskCount = taskCounts[producer];
                        forceMapTasks = true;
                        ++mapConnectionCount;
                    } else if (dynamic_cast<const TShuffleConnection*>(connection.Get())) {
                        hasShuffle = true;
                    } else if (const auto* unionAll =
                        dynamic_cast<const TUnionAllConnection*>(connection.Get()))
                    {
                        if (unionAll->IsParallel()) {
                            taskCount = std::max(taskCount, taskCounts[producer]);
                        }
                    }
                }
            }

            if (mapConnectionCount > 1) {
                Unsupported("StageGraph consumer has more than one Map connection");
            }
            if (hasShuffle && !forceMapTasks) {
                taskCount = 2;
            }
            if (taskCount == 0 || taskCount > 2) {
                Unsupported("StageGraph task count exceeds the version-one bound");
            }

            processedProducers.clear();
            for (const auto producer : Graph.StageInputs.at(stageId)) {
                if (!processedProducers.insert(producer).second) {
                    continue;
                }
                for (const auto& connection : Graph.GetConnections(producer, stageId)) {
                    if (dynamic_cast<const TMapConnection*>(connection.Get())) {
                        if (taskCounts[producer] != taskCount) {
                            Unsupported(
                                "StageGraph Map producer and consumer task counts must match");
                        }
                    } else if (const auto* unionAll =
                        dynamic_cast<const TUnionAllConnection*>(connection.Get()))
                    {
                        if (!unionAll->IsParallel() && taskCount != 1) {
                            Unsupported(
                                "StageGraph serial UnionAll requires exactly one consumer task");
                        }
                    } else if (dynamic_cast<const TMergeConnection*>(connection.Get())) {
                        if (taskCount != 1) {
                            Unsupported(
                                "StageGraph Merge requires exactly one consumer task");
                        }
                    } else if (!dynamic_cast<const TShuffleConnection*>(connection.Get()) &&
                        !dynamic_cast<const TBroadcastConnection*>(connection.Get()))
                    {
                        Unsupported("StageGraph connection has unsupported task-count semantics");
                    }
                }
            }
            taskCounts[stageId] = taskCount;
        }
    }

private:
    TOpRoot& Root;
    const TStageGraph& Graph;
    const THashMap<const IOperator*, TString>& NodeIds;
    const TVector<IOperator*>& NodeOrder;
    TString RootNodeId;
    size_t StageCount = 0;
    ui32 RootStageId = 0;
    TVector<TVector<IOperator*>> StageNodes;
    TVector<IOperator*> StageSinks;
    TVector<ui32> TopologicalStages;
    TVector<TVector<TString>> StageInputNodes;
    TVector<TVector<TString>> LogicalInputNodes;
    TVector<TMap<ui32, TString>> StageOutputNodes;
    TMap<TStagePair, TVector<TBoundary>> Boundaries;
    NJson::TJsonValue Edges = JsonArray();
};

NJson::TJsonValue ExportCatalog(const TSemanticSnapshotCatalogV1& catalog) {
    auto tables = JsonArray();
    THashSet<TString> tableNames;
    for (const auto& table : catalog.Tables) {
        if (table.Name.empty() || !tableNames.insert(table.Name).second || table.Columns.empty()) {
            Unsupported(TStringBuilder() << "Invalid catalog table " << table.Name);
        }
        auto tableJson = JsonMap();
        tableJson["name"] = table.Name;
        auto columns = JsonArray();
        THashSet<TString> columnNames;
        for (const auto& column : table.Columns) {
            if (column.Name.empty() || !columnNames.insert(column.Name).second || !IsSupportedType(column.Type)) {
                Unsupported(TStringBuilder() << "Invalid catalog column " << table.Name << "." << column.Name);
            }
            auto columnJson = JsonMap();
            columnJson["name"] = column.Name;
            columnJson["type"] = column.Type;
            columnJson["nullable"] = column.Nullable;
            columns.AppendValue(std::move(columnJson));
        }
        tableJson["columns"] = std::move(columns);

        auto keys = JsonArray();
        for (const auto& key : table.UniqueKeys) {
            if (key.Columns.empty()) {
                Unsupported(TStringBuilder() << "Empty unique key on table " << table.Name);
            }
            auto keyColumns = JsonArray();
            THashSet<TString> seen;
            for (const auto& column : key.Columns) {
                if (!columnNames.contains(column) || !seen.insert(column).second) {
                    Unsupported(TStringBuilder() << "Invalid unique key column " << table.Name << "." << column);
                }
                keyColumns.AppendValue(column);
            }
            auto keyJson = JsonMap();
            keyJson["columns"] = std::move(keyColumns);
            keyJson["nulls_distinct"] = key.NullsDistinct;
            keys.AppendValue(std::move(keyJson));
        }
        tableJson["unique_keys"] = std::move(keys);
        tables.AppendValue(std::move(tableJson));
    }

    auto schema = JsonMap();
    schema["tables"] = std::move(tables);
    return schema;
}

TString SerializeSnapshot(
    TOpRoot& root,
    const TSemanticSnapshotCatalogV1& catalog,
    TStringBuf cluster)
{
    const bool stageGraphPresent = HasStageGraphState(root);
    TPlanExporter planExporter(root, catalog, cluster, stageGraphPresent);

    auto snapshot = JsonMap();
    snapshot["format"] = "ydb-rbo-semantic-snapshot";
    snapshot["version"] = 1;
    snapshot["schema"] = ExportCatalog(catalog);
    snapshot["plan"] = planExporter.Export();
    snapshot["stage_graph"] = stageGraphPresent
        ? TStageGraphExporter(
            root,
            planExporter.GetNodeIds(),
            planExporter.GetNodeOrder(),
            planExporter.GetRootId()).Export()
        : NJson::TJsonValue(NJson::JSON_NULL);

    NJsonWriter::TBuf writer;
    writer.WriteJsonValue(&snapshot, true, PREC_NDIGITS, 17);
    return writer.Str();
}

template <typename TResult, typename TAction>
TResult CatchUnsupported(TAction&& action) {
    TResult result;
    try {
        action(result);
    } catch (const TUnsupportedSnapshot& error) {
        result.UnsupportedReason = error.what();
    } catch (const std::exception& error) {
        result.UnsupportedReason = TStringBuilder() << "Snapshot export failed closed: " << error.what();
    }
    return result;
}

} // anonymous namespace

TSemanticSnapshotCatalogCaptureResult CaptureSemanticSnapshotCatalogV1(
    TOpRoot& initialRoot,
    const TRBOContext& ctx)
{
    return CatchUnsupported<TSemanticSnapshotCatalogCaptureResult>([&](auto& result) {
        if (!initialRoot.PlanProps.Subplans.PlanMap.empty() ||
            !initialRoot.PlanProps.Subplans.OrderedList.empty())
        {
            Unsupported("Logical snapshot v1 cannot capture a catalog for subplans");
        }

        struct TScannedTable {
            TString Path;
            TSet<TString> Columns;
        };
        TMap<TString, TScannedTable> scanned;
        THashSet<const IOperator*> visited;
        VisitOperators(initialRoot.GetInput(), visited, [&](IOperator& op) {
            if (op.GetKind() != EOperator::Source) {
                return;
            }
            auto& read = static_cast<TOpRead&>(op);
            if (read.Columns.empty() || read.Columns.size() != read.OutputIUs.size()) {
                Unsupported("Read has an empty or inconsistent column mapping");
            }
            const auto table = TableReference(read, ctx.KqpCtx.Cluster);
            auto& scannedTable = scanned[table.Identity];
            if (!scannedTable.Path.empty() && scannedTable.Path != table.Path) {
                Unsupported(TStringBuilder() << "Colliding table identity " << table.Identity);
            }
            scannedTable.Path = table.Path;
            scannedTable.Columns.insert(read.Columns.begin(), read.Columns.end());
        });

        for (auto& [identity, scannedTable] : scanned) {
            const auto& path = scannedTable.Path;
            auto& columns = scannedTable.Columns;
            const auto& description = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, path);
            if (!description.Metadata) {
                Unsupported(TStringBuilder() << "Missing metadata for table " << path);
            }
            const auto& metadata = *description.Metadata;
            columns.insert(metadata.KeyColumnNames.begin(), metadata.KeyColumnNames.end());

            TSemanticSnapshotCatalogTableV1 table;
            table.Name = identity;
            THashSet<TString> emitted;
            auto appendColumn = [&](const TString& name) {
                if (!columns.contains(name) || !emitted.insert(name).second) {
                    return;
                }
                const auto it = metadata.Columns.find(name);
                if (it == metadata.Columns.end()) {
                    Unsupported(TStringBuilder() << "Missing metadata for column " << path << "." << name);
                }
                const auto& column = it->second;
                if (column.SetNotNullInProgress || !IsSupportedType(column.Type)) {
                    Unsupported(TStringBuilder() << "Unsupported metadata for column " << path << "." << name);
                }
                table.Columns.push_back({name, column.Type, !column.NotNull});
            };
            for (const auto& name : metadata.ColumnOrder) {
                appendColumn(name);
            }
            for (const auto& name : columns) {
                appendColumn(name);
            }
            if (table.Columns.empty() || emitted.size() != columns.size()) {
                Unsupported(TStringBuilder() << "Could not capture every referenced column for table " << path);
            }
            if (!metadata.KeyColumnNames.empty()) {
                table.UniqueKeys.push_back({metadata.KeyColumnNames, false});
            }
            result.Catalog.Tables.push_back(std::move(table));
        }
    });
}

TSemanticSnapshotExportResult ExportSemanticSnapshotV1(
    TOpRoot& root,
    const TRBOContext& ctx,
    const TSemanticSnapshotCatalogV1& catalog)
{
    return CatchUnsupported<TSemanticSnapshotExportResult>([&](auto& result) {
        result.Json = SerializeSnapshot(root, catalog, ctx.KqpCtx.Cluster);
    });
}

TSemanticSnapshotExportResult ExportSemanticSnapshotV1(TOpRoot& root, const TRBOContext& ctx) {
    const auto catalog = CaptureSemanticSnapshotCatalogV1(root, ctx);
    if (!catalog.IsSupported()) {
        TSemanticSnapshotExportResult result;
        result.UnsupportedReason = catalog.UnsupportedReason;
        return result;
    }
    return ExportSemanticSnapshotV1(root, ctx, catalog.Catalog);
}

TSemanticSnapshotPairCaptureV1::TSemanticSnapshotPairCaptureV1(
    IRBOSemanticSnapshotSink* sink) noexcept
    : Sink(sink)
{
}

void TSemanticSnapshotPairCaptureV1::CaptureInitial(
    TOpRoot& root,
    TRBOContext& ctx) noexcept
{
    if (!Sink) {
        return;
    }

    InitialAttempted = true;
    Catalog.reset();
    CatalogFailure.clear();
    TRBOSemanticSnapshotBoundaryResultV1 result{
        ERBOSemanticSnapshotBoundaryV1::Initial,
        {},
        {},
    };

    try {
        if (HasStageGraphState(root)) {
            CatalogFailure =
                "Initial semantic snapshot boundary requires stage_graph:null";
            result.UnsupportedReason = CatalogFailure;
        } else {
            auto catalog = CaptureSemanticSnapshotCatalogV1(root, ctx);
            if (!catalog.IsSupported()) {
                CatalogFailure = std::move(catalog.UnsupportedReason);
                result.UnsupportedReason = CatalogFailure;
            } else {
                if (HasAggregate(root)) {
                    root.RecomputeOutputIUsSubtree();
                    if (root.ComputeTypes(ctx) != IGraphTransformer::TStatus::Ok) {
                        Unsupported("RBO type annotation failed for the initial semantic snapshot");
                    }
                }
                Catalog.emplace(std::move(catalog.Catalog));
                auto snapshot = ExportSemanticSnapshotV1(root, ctx, *Catalog);
                result.Json = std::move(snapshot.Json);
                result.UnsupportedReason = std::move(snapshot.UnsupportedReason);
            }
        }
    } catch (const std::exception& error) {
        result.UnsupportedReason = TStringBuilder()
            << "Initial semantic snapshot capture failed closed: " << error.what();
        CatalogFailure = result.UnsupportedReason;
        Catalog.reset();
    } catch (...) {
        result.UnsupportedReason = "Initial semantic snapshot capture failed closed with an unknown exception";
        CatalogFailure = result.UnsupportedReason;
        Catalog.reset();
    }

    Deliver(std::move(result));
}

void TSemanticSnapshotPairCaptureV1::CaptureFinal(
    TOpRoot& root,
    const TRBOContext& ctx) noexcept
{
    if (!Sink) {
        return;
    }

    TRBOSemanticSnapshotBoundaryResultV1 result{
        ERBOSemanticSnapshotBoundaryV1::Final,
        {},
        {},
    };

    try {
        if (!InitialAttempted) {
            result.UnsupportedReason = "Initial semantic snapshot capture was not attempted";
        } else if (!Catalog) {
            result.UnsupportedReason = CatalogFailure.empty()
                ? TString("Initial semantic snapshot catalog is unavailable")
                : CatalogFailure;
        } else if (!HasStageGraphState(root)) {
            result.UnsupportedReason =
                "Final semantic snapshot boundary requires a non-null stage_graph";
        } else {
            auto snapshot = ExportSemanticSnapshotV1(root, ctx, *Catalog);
            result.Json = std::move(snapshot.Json);
            result.UnsupportedReason = std::move(snapshot.UnsupportedReason);
        }
    } catch (const std::exception& error) {
        result.UnsupportedReason = TStringBuilder()
            << "Final semantic snapshot capture failed closed: " << error.what();
    } catch (...) {
        result.UnsupportedReason = "Final semantic snapshot capture failed closed with an unknown exception";
    }

    Deliver(std::move(result));
}

void TSemanticSnapshotPairCaptureV1::Deliver(
    TRBOSemanticSnapshotBoundaryResultV1 result) noexcept
{
    try {
        Sink->OnSemanticSnapshot(std::move(result));
    } catch (...) {
        // Snapshot instrumentation must never alter query compilation.
    }
}

} // namespace NKikimr::NKqp
