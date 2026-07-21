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

void CheckLogicalProperties(const IOperator& op) {
    const auto& props = op.Props;
    if (props.StageId || props.Algorithm || props.OrderEnforcer || props.EnsureAtMostOne ||
        props.JoinAlgo || props.UseBlockHashJoin || props.LeftShuffleBy || props.RightShuffleBy)
    {
        Unsupported(TStringBuilder()
            << op.GetExplainName()
            << " has physical properties that logical snapshot v1 cannot represent");
    }
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

class TPlanExporter {
public:
    TPlanExporter(
        TOpRoot& root,
        const TSemanticSnapshotCatalogV1& catalog,
        TStringBuf cluster)
        : Root(root)
        , Cluster(cluster)
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
        const auto& graph = Root.PlanProps.StageGraph;
        if (!graph.StageIds.empty() || !graph.SourceStages.empty() ||
            !graph.StageInputs.empty() || !graph.StageOutputs.empty() ||
            !graph.Connections.empty() || !graph.StageOutputIndices.empty() ||
            !graph.StageGUIDs.empty())
        {
            Unsupported("Logical snapshot v1 cannot represent StageGraph");
        }
        CheckLogicalProperties(Root);
        if (Root.ColumnOrder.empty()) {
            Unsupported("Root output order must not be empty");
        }

        const TString rootId = ExportNode(Root.GetInput());
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
        result["root"] = rootId;
        result["output"] = std::move(output);
        return result;
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
        CheckLogicalProperties(*op);

        const TString id = TStringBuilder() << "n" << Ids.size();
        auto node = ExportOperator(*op, id, children);
        Ids.emplace(op.Get(), id);
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
    THashMap<TString, const TSemanticSnapshotCatalogTableV1*> Catalog;
    THashMap<const IOperator*, TString> Ids;
    THashSet<const IOperator*> Visiting;
    NJson::TJsonValue Nodes = JsonArray();
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
    auto snapshot = JsonMap();
    snapshot["format"] = "ydb-rbo-semantic-snapshot";
    snapshot["version"] = 1;
    snapshot["schema"] = ExportCatalog(catalog);
    snapshot["plan"] = TPlanExporter(root, catalog, cluster).Export();
    snapshot["stage_graph"] = NJson::TJsonValue(NJson::JSON_NULL);

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
    const TRBOContext& ctx) noexcept
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
        auto catalog = CaptureSemanticSnapshotCatalogV1(root, ctx);
        if (!catalog.IsSupported()) {
            CatalogFailure = std::move(catalog.UnsupportedReason);
            result.UnsupportedReason = CatalogFailure;
        } else {
            Catalog.emplace(std::move(catalog.Catalog));
            auto snapshot = ExportSemanticSnapshotV1(root, ctx, *Catalog);
            result.Json = std::move(snapshot.Json);
            result.UnsupportedReason = std::move(snapshot.UnsupportedReason);
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
