#include "semantic_snapshot.h"

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_context.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/scheme_types/scheme_decimal_type.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/writer/json_value.h>

#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/mkql_date_scaler.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
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
#include <optional>
#include <string_view>
#include <utility>
#include <variant>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;
using namespace NYql::NNodes;

class TUnsupportedSnapshot final : public yexception {
};

[[noreturn]] void Unsupported(const TString& reason) {
    ythrow TUnsupportedSnapshot() << reason;
}

struct TDecimalParameters {
    ui8 Precision;
    ui8 Scale;
};

std::optional<TDecimalParameters> ParseCanonicalDecimalParameters(
    std::string_view precisionText,
    std::string_view scaleText)
{
    const auto parse = [](std::string_view digits, ui32& value) {
        if (digits.empty() || (digits.size() > 1 && digits.front() == '0')) {
            return false;
        }
        value = 0;
        for (const char digit : digits) {
            if (digit < '0' || digit > '9') {
                return false;
            }
            value = value * 10 + static_cast<ui32>(digit - '0');
            if (value > NYql::NDecimal::MaxPrecision) {
                return false;
            }
        }
        return true;
    };
    ui32 precision = 0;
    ui32 scale = 0;
    if (!parse(precisionText, precision) ||
        !parse(scaleText, scale) ||
        precision == 0 || scale > precision)
    {
        return std::nullopt;
    }
    return TDecimalParameters{
        static_cast<ui8>(precision),
        static_cast<ui8>(scale),
    };
}

std::optional<TDecimalParameters> ParseCanonicalDecimalType(TStringBuf type) {
    const std::string_view text(type.data(), type.size());
    constexpr std::string_view Prefix = "Decimal(";
    if (!text.starts_with(Prefix) || !text.ends_with(')')) {
        return std::nullopt;
    }
    const auto body = text.substr(Prefix.size(), text.size() - Prefix.size() - 1);
    const auto comma = body.find(',');
    if (comma == std::string_view::npos || body.find(',', comma + 1) != std::string_view::npos) {
        return std::nullopt;
    }
    return ParseCanonicalDecimalParameters(
        body.substr(0, comma),
        body.substr(comma + 1));
}

bool IsCanonicalDecimalType(TStringBuf type) {
    return ParseCanonicalDecimalType(type).has_value();
}

bool IsSupportedType(TStringBuf type) {
    static const THashSet<TString> Types = {
        "Bool",
        "Int8", "Int16", "Int32", "Int64",
        "Uint8", "Uint16", "Uint32", "Uint64",
        "String", "Utf8", "Date",
    };
    if (Types.contains(type)) {
        return true;
    }
    return IsCanonicalDecimalType(type);
}

bool IsSupportedLiteralType(TStringBuf type) {
    static const THashSet<TString> Types = {
        "Bool",
        "Int8", "Int16", "Int32", "Int64",
        "Uint8", "Uint16", "Uint32", "Uint64",
        "String", "Utf8", "Date",
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

void ValidateOperatorArity(IOperator& op, bool allowRoot) {
    const size_t children = op.GetChildren().size();
    switch (op.GetKind()) {
        case EOperator::EmptySource:
            if (children != 0) {
                Unsupported("EmptySource unexpectedly has children");
            }
            return;

        case EOperator::Source:
            if (children != 0) {
                Unsupported("Read unexpectedly has children");
            }
            return;

        case EOperator::Map:
            if (children != 1) {
                Unsupported("Map must have one input");
            }
            return;

        case EOperator::Filter:
            if (children != 1) {
                Unsupported("Filter must have one input");
            }
            return;

        case EOperator::Limit:
            if (children != 1) {
                Unsupported("Limit must have one input");
            }
            return;

        case EOperator::Sort:
            if (children != 1) {
                Unsupported("Sort must have one input");
            }
            return;

        case EOperator::Aggregate:
            if (children != 1) {
                Unsupported("Aggregate must have one input");
            }
            return;

        case EOperator::Join:
            if (children != 2) {
                Unsupported("Join must have two inputs");
            }
            return;

        case EOperator::UnionAll:
            if (children != 2) {
                Unsupported("UnionAll must have two inputs");
            }
            return;

        case EOperator::Root:
            if (!allowRoot) {
                Unsupported("Unsupported operator Root");
            }
            if (children != 1) {
                Unsupported("Root must have one input");
            }
            return;

        case EOperator::AddDependencies:
            if (children != 1) {
                Unsupported("AddDependencies must have one input");
            }
            return;

        case EOperator::CBOTree:
            Unsupported(TStringBuilder()
                << "Unsupported operator " << op.GetExplainName());
    }
    Unsupported("Unsupported operator kind");
}

void ValidateOperatorTopologyImpl(
    IOperator& op,
    THashSet<const IOperator*>& visiting,
    THashSet<const IOperator*>& visited,
    bool allowRoot)
{
    if (visited.contains(&op)) {
        return;
    }
    if (!visiting.insert(&op).second) {
        Unsupported("Plan contains an operator cycle");
    }

    ValidateOperatorArity(op, allowRoot);
    for (const auto& child : op.GetChildren()) {
        if (!child) {
            Unsupported("Plan contains a null operator");
        }
        ValidateOperatorTopologyImpl(*child, visiting, visited, false);
    }

    visiting.erase(&op);
    visited.insert(&op);
}

void ValidateOperatorTopology(IOperator& root, bool allowRoot = false) {
    THashSet<const IOperator*> visiting;
    THashSet<const IOperator*> visited;
    ValidateOperatorTopologyImpl(root, visiting, visited, allowRoot);
}

TVector<TIntrusivePtr<IOperator>> OrderedSubplanRoots(const TSubplans& subplans) {
    if (subplans.OrderedList.size() != subplans.PlanMap.size()) {
        Unsupported("Subplan registry order and map have different sizes");
    }

    TInfoUnitSet seen;
    TVector<TIntrusivePtr<IOperator>> result;
    result.reserve(subplans.OrderedList.size());
    for (const auto& binding : subplans.OrderedList) {
        const TString name = binding.GetFullName();
        if (name.empty() || !seen.insert(binding).second) {
            Unsupported(TStringBuilder()
                << "Subplan registry has an empty or duplicate binding " << name);
        }
        const auto it = subplans.PlanMap.find(binding);
        if (it == subplans.PlanMap.end()) {
            Unsupported(TStringBuilder()
                << "Subplan registry order references missing binding " << name);
        }
        const auto& entry = it->second;
        if (!(entry.IU == binding)) {
            Unsupported(TStringBuilder()
                << "Subplan registry entry disagrees with binding " << name);
        }
        auto* plan = dynamic_cast<IOperator*>(entry.Plan.Get());
        if (!plan) {
            Unsupported(TStringBuilder()
                << "Subplan registry has no operator plan for binding " << name);
        }
        result.emplace_back(plan);
    }
    return result;
}

void ValidateSnapshotTopology(TOpRoot& root) {
    ValidateOperatorTopology(root, true);
    THashSet<const IOperator*> mainNodes;
    VisitOperators(
        root.GetInput(),
        mainNodes,
        [](IOperator& op) {
            if (op.GetKind() == EOperator::AddDependencies) {
                Unsupported(
                    "AddDependencies is only admissible inside a validated "
                    "correlated subplan");
            }
        });
    for (const auto& subplanRoot :
        OrderedSubplanRoots(root.PlanProps.Subplans))
    {
        ValidateOperatorTopology(*subplanRoot);
        THashSet<const IOperator*> subplanNodes;
        VisitOperators(
            subplanRoot,
            subplanNodes,
            [](IOperator& op) {
                if (op.GetKind() != EOperator::AddDependencies) {
                    return;
                }
                const auto& addDependencies =
                    static_cast<const TOpAddDependencies&>(op);
                if (addDependencies.Dependencies.empty() ||
                    addDependencies.Dependencies.size() !=
                        addDependencies.Types.size())
                {
                    Unsupported(
                        "AddDependencies must have equally sized nonempty "
                        "dependency and type vectors");
                }
                TInfoUnitSet seen;
                for (size_t index = 0;
                     index < addDependencies.Dependencies.size();
                     ++index)
                {
                    const auto& dependency =
                        addDependencies.Dependencies[index];
                    if (dependency.GetFullName().empty() ||
                        !seen.insert(dependency).second ||
                        !addDependencies.Types[index])
                    {
                        Unsupported(
                            "AddDependencies has an empty or duplicate "
                            "dependency or a missing type");
                    }
                }
            });
    }
}

void CheckSnapshotProperties(IOperator& op, bool stageGraphPresent) {
    const auto& props = op.Props;
    const bool joinPhysicalProps = props.JoinAlgo || props.UseBlockHashJoin ||
        props.LeftShuffleBy || props.RightShuffleBy;
    if ((!stageGraphPresent && props.StageId) || props.Algorithm || props.OrderEnforcer ||
        (props.EnsureAtMostOne && op.GetKind() != EOperator::Limit) ||
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

bool NeedsInitialSnapshotTypeMaterialization(TOpRoot& root) {
    // Scalar descriptors require an exact type on their selected root output,
    // including Project/Filter/Limit wrappers that otherwise do not trigger
    // the legacy Aggregate/Sort materialization path.
    bool result = !root.PlanProps.Subplans.PlanMap.empty();
    THashSet<const IOperator*> visited;
    const auto inspect = [&](IOperator& op) {
        result = result || op.GetKind() == EOperator::Aggregate ||
            op.GetKind() == EOperator::Sort;
    };
    VisitOperators(root.GetInput(), visited, inspect);
    for (const auto& subplanRoot : OrderedSubplanRoots(root.PlanProps.Subplans)) {
        VisitOperators(subplanRoot, visited, inspect);
    }
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

constexpr size_t MaxExactScalarNodes = 1024;
constexpr size_t MaxExactScalarDepth = 128;
constexpr size_t MaxIfPresentBindingDepth = 64;

class TExactScalarBudget {
public:
    void Charge(size_t depth, size_t count = 1) {
        if (count == 0) {
            return;
        }
        if (depth > MaxExactScalarDepth) {
            Unsupported(TStringBuilder()
                << "Exact scalar expression exceeds the depth audit limit of "
                << MaxExactScalarDepth);
        }
        if (count > MaxExactScalarNodes - Nodes) {
            Unsupported(TStringBuilder()
                << "Exact scalar expression exceeds the node audit limit of "
                << MaxExactScalarNodes);
        }
        Nodes += count;
    }

private:
    size_t Nodes = 0;
};

void AuditExactScalarExpression(const NJson::TJsonValue& root) {
    struct TPending {
        const NJson::TJsonValue* Expression;
        size_t Depth;
    };

    // Count normalized Expr occurrences only: IR metadata is not a node, while
    // repeated JSON children are distinct occurrences even if the source was a DAG.
    TVector<TPending> pending{{&root, 1}};
    size_t nodes = 0;
    while (!pending.empty()) {
        const auto current = pending.back();
        pending.pop_back();

        if (current.Depth > MaxExactScalarDepth) {
            Unsupported(TStringBuilder()
                << "Exact scalar expression exceeds the depth audit limit of "
                << MaxExactScalarDepth);
        }
        if (++nodes > MaxExactScalarNodes) {
            Unsupported(TStringBuilder()
                << "Exact scalar expression exceeds the node audit limit of "
                << MaxExactScalarNodes);
        }

        const auto& expression = *current.Expression;
        if (!expression.IsMap() || !expression["kind"].IsString()) {
            Unsupported("Exact scalar expression has malformed normalized IR");
        }
        const TString kind = expression["kind"].GetStringSafe();
        const auto push = [&](const NJson::TJsonValue& child) {
            pending.push_back({&child, current.Depth + 1});
        };
        const auto pushArray = [&](TStringBuf field) {
            const auto& children = expression[field];
            if (!children.IsArray()) {
                Unsupported("Exact scalar expression has malformed normalized IR");
            }
            const size_t remaining = MaxExactScalarNodes - nodes;
            if (pending.size() > remaining ||
                children.GetArraySafe().size() > remaining - pending.size())
            {
                Unsupported(TStringBuilder()
                    << "Exact scalar expression exceeds the node audit limit of "
                    << MaxExactScalarNodes);
            }
            for (const auto& child : children.GetArraySafe()) {
                push(child);
            }
        };

        if (kind == "column" || kind == "bound" || kind == "void" ||
            kind == "literal" || kind == "null")
        {
            continue;
        }
        if (kind == "and" || kind == "or") {
            pushArray("args");
            continue;
        }
        if (kind == "not" || kind == "exists" || kind == "cast_decimal" ||
            kind == "cast_integral")
        {
            push(expression["arg"]);
            continue;
        }
        if (kind == "in") {
            push(expression["lookup"]);
            pushArray("items");
            continue;
        }
        if (kind == "eq" || kind == "lt" || kind == "lte" || kind == "gt" ||
            kind == "gte" || kind == "add" || kind == "sub" || kind == "mul" ||
            kind == "div")
        {
            push(expression["left"]);
            push(expression["right"]);
            continue;
        }
        if (kind == "if") {
            push(expression["condition"]);
            push(expression["then"]);
            push(expression["else"]);
            continue;
        }
        if (kind == "if_present") {
            push(expression["optional"]);
            push(expression["present"]);
            push(expression["missing"]);
            continue;
        }
        if (kind == "opaque") {
            pushArray("args");
            continue;
        }
        Unsupported(TStringBuilder()
            << "Exact scalar expression has unknown normalized kind " << kind);
    }
}

NJson::TJsonValue BoundExpr(size_t depth) {
    auto value = JsonMap();
    value["kind"] = "bound";
    value["depth"] = static_cast<ui64>(depth);
    return value;
}

NJson::TJsonValue BinaryExpr(
    TStringBuf kind,
    NJson::TJsonValue left,
    NJson::TJsonValue right)
{
    auto result = JsonMap();
    result["kind"] = TString(kind);
    result["left"] = std::move(left);
    result["right"] = std::move(right);
    return result;
}

NJson::TJsonValue NotExpr(NJson::TJsonValue argument) {
    auto result = JsonMap();
    result["kind"] = "not";
    result["arg"] = std::move(argument);
    return result;
}

NJson::TJsonValue ExistsExpr(NJson::TJsonValue argument) {
    auto result = JsonMap();
    result["kind"] = "exists";
    result["arg"] = std::move(argument);
    return result;
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
    TString type(NUdf::GetDataTypeInfo(data->GetSlot()).Name);
    if (data->GetSlot() == NUdf::EDataSlot::Decimal) {
        const auto* decimal = dynamic_cast<const TDataExprParamsType*>(data);
        if (!decimal) {
            Unsupported("Decimal expression type has no precision and scale");
        }
        type = TStringBuilder()
            << "Decimal(" << decimal->GetParamOne() << ","
            << decimal->GetParamTwo() << ")";
    } else if (dynamic_cast<const TDataExprParamsType*>(data)) {
        Unsupported(TStringBuilder() << "Unsupported parameterized scalar type " << type);
    }
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
    if (!IsSupportedLiteralType(type) || node.ChildrenSize() != 1 || !node.Child(0)->IsAtom()) {
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

    if (type == "Date") {
        const ui64 parsed = ParseInteger<ui64>(value, type);
        if (parsed >= NUdf::MAX_DATE) {
            Unsupported(TStringBuilder()
                << "Date literal is out of range [0, "
                << NUdf::MAX_DATE << "): " << value);
        }
        result["value"] = static_cast<ui64>(static_cast<ui16>(parsed));
        return result;
    }

    if (!NYql::IsUtf8(std::string_view(value.data(), value.size()))) {
        Unsupported(TStringBuilder() << type << " literal is not valid UTF-8");
    }
    result["value"] = value;
    return result;
}

NJson::TJsonValue DecimalValueExpr(
    TStringBuf type,
    const TDecimalParameters& parameters,
    NYql::NDecimal::TInt128 value,
    TStringBuf text)
{
    auto encoded = JsonMap();
    if (NYql::NDecimal::IsNan(value)) {
        encoded["kind"] = "nan";
    } else if (value == NYql::NDecimal::Inf()) {
        encoded["kind"] = "pos_inf";
    } else if (value == -NYql::NDecimal::Inf()) {
        encoded["kind"] = "neg_inf";
    } else {
        if (!NYql::NDecimal::IsNormal(value, parameters.Precision)) {
            Unsupported(TStringBuilder() << "Invalid " << type << " literal " << text);
        }
        const char* scaled = NYql::NDecimal::ToString(
            value,
            NYql::NDecimal::MaxPrecision,
            0);
        if (!scaled) {
            Unsupported(TStringBuilder() << "Cannot canonicalize " << type << " literal " << text);
        }
        encoded["kind"] = "finite";
        encoded["scaled"] = TString(scaled);
    }

    auto result = JsonMap();
    result["kind"] = "literal";
    result["type"] = TString(type);
    result["value"] = std::move(encoded);
    return result;
}

NJson::TJsonValue DecimalValueExpr(
    TStringBuf type,
    const TDecimalParameters& parameters,
    TStringBuf text)
{
    const auto value = NYql::NDecimal::FromString(
        text,
        parameters.Precision,
        parameters.Scale);
    if (NYql::NDecimal::IsError(value)) {
        Unsupported(TStringBuilder() << "Invalid " << type << " literal " << text);
    }
    return DecimalValueExpr(type, parameters, value, text);
}

NJson::TJsonValue DecimalLiteralExpr(const TExprNode& node) {
    if (!node.IsCallable("Decimal") || node.ChildrenSize() != 3 ||
        !node.Child(0)->IsAtom() || !node.Child(1)->IsAtom() ||
        !node.Child(2)->IsAtom())
    {
        Unsupported("Unsupported Decimal literal callable");
    }

    const TStringBuf precision = node.Child(1)->Content();
    const TStringBuf scale = node.Child(2)->Content();
    const auto parameters = ParseCanonicalDecimalParameters(
        std::string_view(precision.data(), precision.size()),
        std::string_view(scale.data(), scale.size()));
    if (!parameters) {
        Unsupported("Decimal literal has invalid precision or scale");
    }

    const TString type = TStringBuilder()
        << "Decimal(" << precision << "," << scale << ")";
    bool nullable = false;
    if (TypeName(node.GetTypeAnn(), &nullable) != type || nullable) {
        Unsupported("Decimal literal type annotation does not match its parameters");
    }
    return DecimalValueExpr(type, *parameters, node.Child(0)->Content());
}

NJson::TJsonValue Uint64LiteralExpr(const TExprNode& node, TStringBuf field) {
    if (!node.IsCallable("Uint64") || node.ChildrenSize() != 1 ||
        !node.Child(0)->IsAtom())
    {
        Unsupported(TStringBuilder() << field << " must be a Uint64 literal");
    }
    return LiteralExpr(node);
}

NJson::TJsonValue Uint64LiteralExpr(const TExpression& expression, TStringBuf field) {
    if (!expression.Node || !expression.Node->IsLambda() ||
        expression.Node->ChildrenSize() != 2)
    {
        Unsupported(TStringBuilder() << field << " is not a one-body lambda");
    }
    const auto* arguments = expression.Node->Child(0);
    if (!arguments->IsArguments() || arguments->ChildrenSize() != 1 ||
        !arguments->Child(0)->IsArgument())
    {
        Unsupported(TStringBuilder() << field << " does not have exactly one row argument");
    }
    return Uint64LiteralExpr(*expression.GetExpressionBody(), field);
}

bool IsIntegerType(TStringBuf type) {
    static const THashSet<TString> Types = {
        "Int8", "Int16", "Int32", "Int64",
        "Uint8", "Uint16", "Uint32", "Uint64",
    };
    return Types.contains(type);
}

bool IsStringType(TStringBuf type) {
    return type == "String" || type == "Utf8";
}

bool StringComparisonCompatible(TStringBuf left, TStringBuf right) {
    return IsStringType(left) && IsStringType(right);
}

TString MetadataTypeName(const TKikimrColumnMetadata& column) {
    if (column.TypeInfo.GetTypeId() == NScheme::NTypeIds::Decimal) {
        const auto& decimal = column.TypeInfo.GetDecimalType();
        return TStringBuilder()
            << "Decimal(" << decimal.GetPrecision() << "," << decimal.GetScale() << ")";
    }
    if (column.Type.StartsWith("Decimal")) {
        Unsupported("Decimal metadata has no precision and scale");
    }
    return column.Type;
}

ui32 IntegerTypeWidth(TStringBuf type) {
    if (type.EndsWith("64")) {
        return 64;
    }
    if (type.EndsWith("32")) {
        return 32;
    }
    if (type.EndsWith("16")) {
        return 16;
    }
    if (type.EndsWith("8")) {
        return 8;
    }
    return 0;
}

bool IntegralDataComparisonCompatible(TStringBuf left, TStringBuf right) {
    return IsIntegerType(left) && IsIntegerType(right);
}

bool LosslessIntegerComparisonCompatible(TStringBuf left, TStringBuf right) {
    if (!IsIntegerType(left) || !IsIntegerType(right)) {
        return false;
    }
    const ui32 leftWidth = IntegerTypeWidth(left);
    const ui32 rightWidth = IntegerTypeWidth(right);
    if (!leftWidth || !rightWidth) {
        return false;
    }
    const bool leftSigned = left.StartsWith("Int");
    const bool rightSigned = right.StartsWith("Int");
    if (leftSigned == rightSigned) {
        return true;
    }
    const ui32 signedWidth = leftSigned ? leftWidth : rightWidth;
    const ui32 unsignedWidth = leftSigned ? rightWidth : leftWidth;
    return signedWidth > unsignedWidth;
}

bool StaticSqlInEqualityCompatible(TStringBuf left, TStringBuf right) {
    // Static SQL IN has a separate, deliberately narrow audit surface.  Do
    // not add broader scalar Decimal or cross String/Utf8 compatibility here.
    return (!IsCanonicalDecimalType(left) && !IsCanonicalDecimalType(right)) &&
        (left == right || LosslessIntegerComparisonCompatible(left, right));
}

bool DecimalScaleAlignmentSupported(
    const TDecimalParameters& source,
    ui8 targetPrecision,
    ui8 targetScale)
{
    const ui8 sourceIntegralDigits = source.Precision - source.Scale;
    const ui8 targetIntegralDigits = targetPrecision - targetScale;
    return !(targetIntegralDigits < sourceIntegralDigits &&
        targetScale != source.Scale &&
        targetIntegralDigits + source.Scale == 0);
}

bool DecimalComparisonCompatible(TStringBuf left, TStringBuf right) {
    const auto leftDecimal = ParseCanonicalDecimalType(left);
    const auto rightDecimal = ParseCanonicalDecimalType(right);
    if (leftDecimal && rightDecimal) {
        if (leftDecimal->Scale == rightDecimal->Scale) {
            return true;
        }
        const auto& source = leftDecimal->Scale < rightDecimal->Scale
            ? *leftDecimal
            : *rightDecimal;
        const ui8 targetScale = std::max(leftDecimal->Scale, rightDecimal->Scale);
        const ui8 targetPrecision = std::min<ui8>(
            NYql::NDecimal::MaxPrecision,
            source.Precision + targetScale - source.Scale);
        return DecimalScaleAlignmentSupported(source, targetPrecision, targetScale);
    }

    const auto decimal = leftDecimal ? leftDecimal : rightDecimal;
    const TStringBuf integer = leftDecimal ? right : left;
    if (!decimal || !IsIntegerType(integer)) {
        return false;
    }
    const auto slot = NUdf::FindDataSlot(integer);
    if (!slot) {
        return false;
    }
    const ui8 digits = NUdf::GetDataTypeInfo(*slot).DecimalDigits;
    const TDecimalParameters source{digits, 0};
    const ui8 targetPrecision = std::min<ui8>(
        NYql::NDecimal::MaxPrecision,
        digits + decimal->Scale);
    return DecimalScaleAlignmentSupported(
        source,
        targetPrecision,
        decimal->Scale);
}

bool ScalarEqualityComparisonCompatible(TStringBuf left, TStringBuf right) {
    return left == right ||
        IntegralDataComparisonCompatible(left, right) ||
        StringComparisonCompatible(left, right) ||
        DecimalComparisonCompatible(left, right);
}

bool ScalarOrderingComparisonCompatible(TStringBuf left, TStringBuf right) {
    return IntegralDataComparisonCompatible(left, right) ||
        (left == "Date" && right == "Date") ||
        StringComparisonCompatible(left, right) ||
        DecimalComparisonCompatible(left, right);
}

bool IsModeledOrderingType(TStringBuf type) {
    return IsIntegerType(type) || type == "Date" || IsStringType(type) ||
        IsCanonicalDecimalType(type);
}

TString ScalarTypeName(const TExprNode& node, bool* nullable = nullptr) {
    return TypeName(node.GetTypeAnn(), nullable);
}

void CheckScalarArity(const TExprNode& node, size_t minimum, size_t maximum) {
    if (node.ChildrenSize() < minimum || node.ChildrenSize() > maximum) {
        Unsupported(TStringBuilder()
            << "Opaque scalar callable " << node.Content()
            << " has unsupported arity " << node.ChildrenSize());
    }
}

TString DataTypeDescriptorName(const TExprNode& node, bool* nullable = nullptr) {
    const TExprNode* dataType = &node;
    const bool optional = node.IsCallable("OptionalType");
    if (optional) {
        CheckScalarArity(node, 1, 1);
        dataType = node.Child(0);
    }
    if (!dataType->IsCallable("DataType")) {
        Unsupported("Opaque scalar has a non-data type descriptor");
    }
    if (dataType->ChildrenSize() == 0 || !dataType->Child(0)->IsAtom()) {
        Unsupported("Opaque scalar has an unsupported DataType descriptor");
    }

    const TStringBuf name = dataType->Child(0)->Content();
    TString type;
    if (name == "Decimal") {
        CheckScalarArity(*dataType, 3, 3);
        if (!dataType->Child(1)->IsAtom() ||
            !dataType->Child(2)->IsAtom())
        {
            Unsupported("Opaque scalar has an unsupported DataType descriptor");
        }
        const TStringBuf precision = dataType->Child(1)->Content();
        const TStringBuf scale = dataType->Child(2)->Content();
        if (!ParseCanonicalDecimalParameters(
                std::string_view(precision.data(), precision.size()),
                std::string_view(scale.data(), scale.size())))
        {
            Unsupported("Opaque scalar has an unsupported DataType descriptor");
        }
        type = TStringBuilder() << "Decimal(" << precision << "," << scale << ")";
    } else {
        CheckScalarArity(*dataType, 1, 1);
        if (!IsSupportedType(name) || IsCanonicalDecimalType(name)) {
            Unsupported("Opaque scalar has an unsupported DataType descriptor");
        }
        type = name;
    }
    if (nullable) {
        *nullable = optional;
    }
    return type;
}

TString NothingTypeName(const TExprNode& node) {
    bool nullable = false;
    const TString type = ScalarTypeName(node, &nullable);
    if (!nullable) {
        Unsupported("Nothing expression is not optional");
    }
    CheckScalarArity(node, 1, 1);
    bool descriptorNullable = false;
    if (DataTypeDescriptorName(*node.Child(0), &descriptorNullable) != type ||
        !descriptorNullable)
    {
        Unsupported("Nothing type descriptor does not match its result");
    }
    return type;
}

void CheckComparisonCallable(
    const TExprNode& node,
    bool allowMissingAnnotations = false)
{
    if (node.ChildrenSize() != 2) {
        Unsupported(TStringBuilder()
            << node.Content() << " must have exactly two arguments");
    }

    if (!node.GetTypeAnn() || !node.Child(0)->GetTypeAnn() ||
        !node.Child(1)->GetTypeAnn())
    {
        if (allowMissingAnnotations) {
            return;
        }
        Unsupported(TStringBuilder()
            << node.Content() << " comparison has no type annotation");
    }

    bool resultNullable = false;
    if (ScalarTypeName(node, &resultNullable) != "Bool") {
        Unsupported(TStringBuilder()
            << node.Content() << " comparison result is not Bool");
    }

    bool leftNullable = false;
    bool rightNullable = false;
    const TString leftType = ScalarTypeName(*node.Child(0), &leftNullable);
    const TString rightType = ScalarTypeName(*node.Child(1), &rightNullable);
    const bool equality = node.IsCallable({"==", "!=", "IsNotDistinctFrom"});
    if (!(equality
            ? ScalarEqualityComparisonCompatible(leftType, rightType)
            : ScalarOrderingComparisonCompatible(leftType, rightType)))
    {
        Unsupported(TStringBuilder()
            << node.Content() << " comparison operand types differ: "
            << leftType << " and " << rightType);
    }

    if (node.IsCallable("IsNotDistinctFrom") &&
        (IsCanonicalDecimalType(leftType) || IsCanonicalDecimalType(rightType)) &&
        leftType != rightType)
    {
        Unsupported(
            "IsNotDistinctFrom Decimal operands must have exactly the same type");
    }

    const bool expectedNullable = node.IsCallable("IsNotDistinctFrom")
        ? false
        : leftNullable || rightNullable;
    if (resultNullable != expectedNullable) {
        Unsupported(TStringBuilder()
            << node.Content() << " comparison result has inconsistent nullability");
    }
}

NJson::TJsonValue DecimalConstantCastExpr(const TExprNode& node) {
    CheckScalarArity(node, 2, 2);

    bool resultNullable = false;
    const TString resultType = ScalarTypeName(node, &resultNullable);
    const auto parameters = ParseCanonicalDecimalType(resultType);
    if (!parameters || resultNullable) {
        Unsupported(TStringBuilder()
            << node.Content()
            << " constant Decimal cast must have a non-nullable Decimal result");
    }

    bool targetNullable = false;
    const TString targetType = DataTypeDescriptorName(*node.Child(1), &targetNullable);
    if (targetNullable || targetType != resultType) {
        Unsupported(TStringBuilder()
            << node.Content()
            << " constant Decimal cast target does not match its non-nullable result");
    }

    const auto& source = *node.Child(0);
    bool sourceNullable = false;
    const TString sourceType = ScalarTypeName(source, &sourceNullable);
    if (sourceNullable || !IsIntegerType(sourceType) ||
        !source.IsCallable() || source.Content() != sourceType ||
        source.ChildrenSize() != 1 ||
        !source.Child(0)->IsAtom())
    {
        Unsupported(TStringBuilder()
            << node.Content()
            << " constant Decimal cast source is not a non-nullable integer literal");
    }
    LiteralExpr(source);

    if (CastResult<false>(source.GetTypeAnn(), node.GetTypeAnn()) !=
        NUdf::ECastOptions::Complete)
    {
        Unsupported(TStringBuilder()
            << node.Content()
            << " constant Decimal cast is not complete");
    }
    return DecimalValueExpr(resultType, *parameters, source.Child(0)->Content());
}

bool IsStringDecimalSafeCastCandidate(const TExprNode& node) {
    return node.IsCallable("SafeCast") &&
        node.ChildrenSize() == 2 &&
        node.Child(0)->GetTypeAnn() &&
        IsStringType(ScalarTypeName(*node.Child(0)));
}

NJson::TJsonValue StringLiteralDecimalSafeCastExpr(const TExprNode& node) {
    if (!node.IsCallable("SafeCast")) {
        Unsupported("String literal Decimal cast must use SafeCast");
    }
    CheckScalarArity(node, 2, 2);

    bool resultNullable = false;
    const TString resultType = ScalarTypeName(node, &resultNullable);
    const auto parameters = ParseCanonicalDecimalType(resultType);
    if (!parameters || !resultNullable) {
        Unsupported(
            "String literal Decimal SafeCast must have an optional canonical Decimal result");
    }

    const auto targetAnnotation = node.Child(1)->GetTypeAnn();
    if (!targetAnnotation || targetAnnotation->GetKind() != ETypeAnnotationKind::Type) {
        Unsupported("String literal Decimal SafeCast target annotation is not Type");
    }

    bool targetNullable = false;
    const TString targetType = DataTypeDescriptorName(
        *node.Child(1),
        &targetNullable);
    if (!targetNullable || targetType != resultType) {
        Unsupported(
            "String literal Decimal SafeCast target does not match its optional result");
    }

    const auto& itemDescriptor = *node.Child(1)->Child(0);
    const auto itemAnnotation = itemDescriptor.GetTypeAnn();
    bool itemAnnotationNullable = false;
    if (!itemAnnotation ||
        itemAnnotation->GetKind() != ETypeAnnotationKind::Type ||
        TypeName(
            itemAnnotation->Cast<TTypeExprType>()->GetType(),
            &itemAnnotationNullable) != targetType ||
        itemAnnotationNullable)
    {
        Unsupported(
            "String literal Decimal SafeCast item annotation disagrees with its descriptor");
    }

    bool annotationNullable = false;
    if (TypeName(
            targetAnnotation->Cast<TTypeExprType>()->GetType(),
            &annotationNullable) != targetType ||
        annotationNullable != targetNullable)
    {
        Unsupported(
            "String literal Decimal SafeCast target annotation disagrees with its descriptor");
    }

    const auto& source = *node.Child(0);
    bool sourceNullable = false;
    const TString sourceType = ScalarTypeName(source, &sourceNullable);
    if (sourceNullable || !IsStringType(sourceType) ||
        !source.IsCallable(sourceType) ||
        source.ChildrenSize() != 1 ||
        !source.Child(0)->IsAtom())
    {
        Unsupported(
            "String literal Decimal SafeCast source must be a direct non-null String or Utf8 literal");
    }
    LiteralExpr(source);

    const TStringBuf text = source.Child(0)->Content();
    if (text.empty() || !std::all_of(
            text.begin(),
            text.end(),
            [](char value) {
                return static_cast<unsigned char>(value) <= 0x7f;
            }))
    {
        Unsupported(
            "String literal Decimal SafeCast source must be non-empty 7-bit ASCII");
    }

    constexpr NUdf::TCastResultOptions ExpectedCast =
        static_cast<NUdf::TCastResultOptions>(
            NUdf::ECastOptions::MayFail |
            NUdf::ECastOptions::MayLoseData);
    if (CastResult<false>(
            source.GetTypeAnn(),
            itemAnnotation->Cast<TTypeExprType>()->GetType()) != ExpectedCast)
    {
        Unsupported(
            "String literal Decimal SafeCast is not a reviewed partial conversion");
    }

    const auto value = NYql::NDecimal::FromStringEx(
        text,
        parameters->Precision,
        parameters->Scale);
    if (NYql::NDecimal::IsError(value)) {
        auto result = JsonMap();
        result["kind"] = "null";
        result["type"] = resultType;
        return result;
    }
    return DecimalValueExpr(resultType, *parameters, value, text);
}

TString CheckIntegralDecimalSafeCastCallable(const TExprNode& node) {
    if (!node.IsCallable("SafeCast")) {
        Unsupported("Integral Decimal cast must use SafeCast");
    }
    CheckScalarArity(node, 2, 2);

    bool resultNullable = false;
    const TString resultType = ScalarTypeName(node, &resultNullable);
    const auto parameters = ParseCanonicalDecimalType(resultType);
    if (!parameters || resultNullable) {
        Unsupported(
            "Integral Decimal SafeCast must have a non-nullable canonical Decimal result");
    }
    if (parameters->Precision == parameters->Scale) {
        Unsupported(
            "Integral Decimal SafeCast target must have at least one integral digit");
    }

    const auto targetAnnotation = node.Child(1)->GetTypeAnn();
    if (!targetAnnotation) {
        Unsupported("Integral Decimal SafeCast target is missing its Type annotation");
    }
    if (targetAnnotation->GetKind() != ETypeAnnotationKind::Type) {
        Unsupported("Integral Decimal SafeCast target annotation is not Type");
    }

    bool targetNullable = false;
    const TString targetType = DataTypeDescriptorName(*node.Child(1), &targetNullable);
    bool annotationNullable = false;
    if (TypeName(
            targetAnnotation->Cast<TTypeExprType>()->GetType(),
            &annotationNullable) != targetType ||
        annotationNullable != targetNullable)
    {
        Unsupported(
            "Integral Decimal SafeCast target annotation disagrees with its descriptor");
    }
    if (targetNullable || targetType != resultType) {
        Unsupported(
            "Integral Decimal SafeCast target does not match its non-nullable result");
    }

    bool sourceNullable = false;
    const TString sourceType = ScalarTypeName(*node.Child(0), &sourceNullable);
    if (sourceNullable || !IsIntegerType(sourceType)) {
        Unsupported(
            "Integral Decimal SafeCast source must be a non-nullable exact integer");
    }

    return resultType;
}

bool IsPartialIntegralSafeCast(const TExprNode& node) {
    if (!node.IsCallable("SafeCast") || node.ChildrenSize() != 2) {
        return false;
    }

    const TString sourceType = ScalarTypeName(*node.Child(0));
    const TString resultType = ScalarTypeName(node);
    if (!IsIntegerType(sourceType) || !IsIntegerType(resultType)) {
        return false;
    }

    const auto sourceSlot = NUdf::FindDataSlot(sourceType);
    const auto resultSlot = NUdf::FindDataSlot(resultType);
    const auto options = NUdf::GetCastResult(*sourceSlot, *resultSlot);
    return options && *options == NUdf::ECastOptions::MayFail;
}

TString CheckPartialIntegralSafeCastCallable(const TExprNode& node) {
    if (!node.IsCallable("SafeCast")) {
        Unsupported("Partial integral cast must use SafeCast");
    }
    CheckScalarArity(node, 2, 2);

    bool resultNullable = false;
    const TString resultType = ScalarTypeName(node, &resultNullable);
    if (!resultNullable || !IsIntegerType(resultType)) {
        Unsupported(
            "Partial integral SafeCast must have an optional integer result");
    }

    const auto targetAnnotation = node.Child(1)->GetTypeAnn();
    if (!targetAnnotation || targetAnnotation->GetKind() != ETypeAnnotationKind::Type) {
        Unsupported("Partial integral SafeCast target annotation is not Type");
    }

    bool targetNullable = false;
    const TString targetType = DataTypeDescriptorName(
        *node.Child(1),
        &targetNullable);
    if (!targetNullable || targetType != resultType) {
        Unsupported(
            "Partial integral SafeCast target does not match its optional result");
    }
    const auto& itemDescriptor = *node.Child(1)->Child(0);
    const auto itemAnnotation = itemDescriptor.GetTypeAnn();
    bool itemAnnotationNullable = false;
    if (!itemAnnotation ||
        itemAnnotation->GetKind() != ETypeAnnotationKind::Type ||
        TypeName(
            itemAnnotation->Cast<TTypeExprType>()->GetType(),
            &itemAnnotationNullable) != targetType ||
        itemAnnotationNullable)
    {
        Unsupported(
            "Partial integral SafeCast item annotation disagrees with its descriptor");
    }
    bool annotationNullable = false;
    if (TypeName(
            targetAnnotation->Cast<TTypeExprType>()->GetType(),
            &annotationNullable) != targetType ||
        annotationNullable != targetNullable)
    {
        Unsupported(
            "Partial integral SafeCast target annotation disagrees with its descriptor");
    }
    const TString sourceType = ScalarTypeName(*node.Child(0));
    if (!IsIntegerType(sourceType)) {
        Unsupported("Partial integral SafeCast source must be an exact integer");
    }
    const auto sourceSlot = NUdf::FindDataSlot(sourceType);
    const auto targetSlot = NUdf::FindDataSlot(targetType);
    const auto options = NUdf::GetCastResult(*sourceSlot, *targetSlot);
    if (!options || *options != NUdf::ECastOptions::MayFail) {
        Unsupported("Integral SafeCast is not a reviewed partial conversion");
    }

    return resultType;
}

bool IsCompleteIntegerLiteralDecimalCast(const TExprNode& node) {
    const auto& source = *node.Child(0);
    bool sourceNullable = false;
    const TString sourceType = ScalarTypeName(source, &sourceNullable);
    if (sourceNullable ||
        !IsIntegerType(sourceType) ||
        !source.IsCallable() ||
        source.Content() != sourceType ||
        source.ChildrenSize() != 1 ||
        !source.Child(0)->IsAtom())
    {
        return false;
    }

    LiteralExpr(source);
    return CastResult<false>(source.GetTypeAnn(), node.GetTypeAnn()) ==
        NUdf::ECastOptions::Complete;
}

struct TDecimalArithmeticSignature {
    TString ResultType;
    bool ResultNullable;
};

struct TIfPresentSignature {
    const TExprNode* Optional;
    const TExprNode* Argument;
    const TExprNode* Present;
    const TExprNode* Missing;
    TString ResultType;
    bool ResultNullable;
};

struct TIfSignature {
    const TExprNode* Condition;
    const TExprNode* Then;
    const TExprNode* Else;
    TString ResultType;
    bool ResultNullable;
};

const TExprNode* CheckExistsCallable(const TExprNode& node) {
    if (!node.IsCallable("Exists") || node.ChildrenSize() != 1) {
        Unsupported("Exists must have exactly one scalar argument");
    }

    bool resultNullable = false;
    if (ScalarTypeName(node, &resultNullable) != "Bool" || resultNullable) {
        Unsupported("Exists result must be non-null Bool");
    }
    ScalarTypeName(*node.Child(0));
    return node.Child(0);
}

TIfSignature CheckIfCallable(const TExprNode& node) {
    if (!node.IsCallable("If") || node.ChildrenSize() != 3) {
        Unsupported("If must have one condition and two branches");
    }

    bool conditionNullable = false;
    if (ScalarTypeName(*node.Child(0), &conditionNullable) != "Bool") {
        Unsupported("If condition must be Bool or Optional<Bool>");
    }

    bool resultNullable = false;
    bool thenNullable = false;
    bool elseNullable = false;
    const TString resultType = ScalarTypeName(node, &resultNullable);
    if (ScalarTypeName(*node.Child(1), &thenNullable) != resultType ||
        ScalarTypeName(*node.Child(2), &elseNullable) != resultType)
    {
        Unsupported("If branches must have the result's scalar type");
    }
    if (resultNullable !=
        (conditionNullable || thenNullable || elseNullable))
    {
        Unsupported("If result nullability must equal the OR of its condition and branches");
    }

    return {
        node.Child(0),
        node.Child(1),
        node.Child(2),
        resultType,
        resultNullable,
    };
}

bool IsExactUint32LiteralConversion(const TExprNode& node) {
    if (!node.IsCallable("Convert") || node.ChildrenSize() != 2 ||
        !node.Child(1)->IsCallable("DataType"))
    {
        return false;
    }

    bool resultNullable = false;
    bool targetNullable = false;
    if (ScalarTypeName(node, &resultNullable) != "Uint32" || resultNullable ||
        DataTypeDescriptorName(*node.Child(1), &targetNullable) != "Uint32" ||
        targetNullable)
    {
        return false;
    }

    const auto& source = *node.Child(0);
    bool sourceNullable = false;
    const TString sourceType = ScalarTypeName(source, &sourceNullable);
    if (sourceNullable || !IsIntegerType(sourceType) ||
        !source.IsCallable(sourceType) || source.ChildrenSize() != 1 ||
        !source.Child(0)->IsAtom())
    {
        return false;
    }
    LiteralExpr(source);

    ui64 value = 0;
    return TryFromString<ui64>(source.Child(0)->Content(), value) &&
        value <= std::numeric_limits<ui32>::max();
}

void CheckRestrictedSubstringBound(const TExprNode& node) {
    bool nullable = false;
    if (ScalarTypeName(node, &nullable) != "Uint32" || nullable) {
        Unsupported(
            "Restricted Substring bounds must be non-null Uint32 literals");
    }

    if (node.IsCallable("Uint32")) {
        LiteralExpr(node);
        return;
    }

    if (!IsExactUint32LiteralConversion(node)) {
        Unsupported(
            "Restricted Substring bounds must be direct Uint32 literals or "
            "in-range integer literals converted to Uint32");
    }
}

void CheckRestrictedSubstringCallable(const TExprNode& node) {
    if (!node.IsCallable("Substring") || node.ChildrenSize() != 3) {
        Unsupported(
            "Restricted Substring must have one string and two bound arguments");
    }

    bool inputNullable = false;
    bool resultNullable = false;
    if (ScalarTypeName(*node.Child(0), &inputNullable) != "String" ||
        !inputNullable || ScalarTypeName(node, &resultNullable) != "String" ||
        !resultNullable)
    {
        Unsupported(
            "Restricted Substring requires Optional<String> input and result");
    }

    for (size_t index = 1; index < 3; ++index) {
        CheckRestrictedSubstringBound(*node.Child(index));
    }
}

TIfPresentSignature CheckIfPresentCallable(const TExprNode& node) {
    if (!node.IsCallable("IfPresent") || node.ChildrenSize() != 3) {
        Unsupported("IfPresent must have one optional, one unary handler, and one missing branch");
    }

    bool optionalNullable = false;
    const TString optionalType = ScalarTypeName(*node.Child(0), &optionalNullable);
    if (!optionalNullable) {
        Unsupported("IfPresent input must be exactly Optional<Data>");
    }

    const auto& handler = *node.Child(1);
    if (!handler.IsLambda() || handler.ChildrenSize() != 2) {
        Unsupported("IfPresent handler must be a one-body lambda");
    }
    const auto& arguments = *handler.Child(0);
    if (!arguments.IsArguments() || arguments.ChildrenSize() != 1 ||
        !arguments.Child(0)->IsArgument())
    {
        Unsupported("IfPresent handler must have exactly one argument");
    }

    const auto& argument = *arguments.Child(0);
    bool argumentNullable = false;
    if (ScalarTypeName(argument, &argumentNullable) != optionalType ||
        argumentNullable)
    {
        Unsupported("IfPresent handler argument must be the non-null input value");
    }

    bool resultNullable = false;
    const TString resultType = ScalarTypeName(node, &resultNullable);
    bool presentNullable = false;
    bool missingNullable = false;
    if (ScalarTypeName(*handler.Child(1), &presentNullable) != resultType ||
        ScalarTypeName(*node.Child(2), &missingNullable) != resultType ||
        presentNullable != resultNullable ||
        missingNullable != resultNullable)
    {
        Unsupported("IfPresent branches must exactly match its result type and nullability");
    }

    return {
        node.Child(0),
        &argument,
        handler.Child(1),
        node.Child(2),
        resultType,
        resultNullable,
    };
}

TDecimalArithmeticSignature CheckDecimalArithmeticCallable(const TExprNode& node) {
    CheckScalarArity(node, 2, 2);

    const TStringBuf callable = node.Content();
    bool resultNullable = false;
    bool leftNullable = false;
    bool rightNullable = false;
    const TString resultType = ScalarTypeName(node, &resultNullable);
    const TString leftType = ScalarTypeName(*node.Child(0), &leftNullable);
    const TString rightType = ScalarTypeName(*node.Child(1), &rightNullable);

    if (!ParseCanonicalDecimalType(resultType)) {
        Unsupported(TStringBuilder() << callable << " result is not Decimal");
    }
    if (leftType != resultType) {
        Unsupported(TStringBuilder()
            << callable
            << " left operand must exactly match its Decimal result type");
    }
    const bool acceptsIntegerRight =
        callable == "DecimalMul" || callable == "DecimalDiv";
    if (rightType != resultType &&
        !(acceptsIntegerRight && IsIntegerType(rightType)))
    {
        Unsupported(TStringBuilder()
            << callable << " right operand must be "
            << (acceptsIntegerRight
                ? "the same Decimal type or an integer"
                : "the same Decimal type"));
    }
    if (resultNullable != (leftNullable || rightNullable)) {
        Unsupported(TStringBuilder()
            << callable
            << " result nullability must equal the OR of operand nullability");
    }

    return {resultType, resultNullable};
}

void CheckOpaqueCallable(
    const TExprNode& node,
    bool allowExactUint32LiteralConversion = false)
{
    const TStringBuf name = node.Content();

    if (name == "Decimal") {
        DecimalLiteralExpr(node);
        return;
    }

    if (IsSupportedType(name)) {
        LiteralExpr(node);
        bool nullable = false;
        if (ScalarTypeName(node, &nullable) != name || nullable) {
            Unsupported("Opaque scalar literal type annotation does not match its callable");
        }
        return;
    }

    if (name == "Member") {
        return;
    }

    if (name == "Nothing") {
        NothingTypeName(node);
        return;
    }

    if (name == "DataType") {
        DataTypeDescriptorName(node);
        return;
    }

    if (name == "OptionalType") {
        DataTypeDescriptorName(node);
        return;
    }

    if (node.IsCallable({"DecimalMul", "DecimalDiv"})) {
        CheckDecimalArithmeticCallable(node);
        return;
    }

    // This is deliberately a positive list.  TExprNode exposes side-effect and
    // CSE-safety flags, but YQL has no generic totality contract for a callable.
    // Keep every accepted family small enough to audit and fail closed for UDFs,
    // generic division, strict casts, Unwrap, and every other not-yet-reviewed
    // form. DecimalDiv is an explicitly audited total Decimal operation.
    if (name == "+" || name == "-" || name == "*") {
        if ((name == "+" || name == "-") &&
            ParseCanonicalDecimalType(ScalarTypeName(node)))
        {
            CheckDecimalArithmeticCallable(node);
            return;
        }
        CheckScalarArity(node, 2, 2);
        if (!IsIntegerType(ScalarTypeName(node))) {
            Unsupported(TStringBuilder() << "Opaque arithmetic result is not an integer: " << name);
        }
        for (const auto& child : node.Children()) {
            if (!IsIntegerType(ScalarTypeName(*child))) {
                Unsupported(TStringBuilder() << "Opaque arithmetic operand is not an integer: " << name);
            }
        }
        return;
    }

    if (node.IsCallable({"==", "!=", "<", "<=", ">", ">=", "IsNotDistinctFrom"})) {
        CheckComparisonCallable(
            node,
            node.IsCallable({"==", "IsNotDistinctFrom"}));
        return;
    }

    if (name == "And" || name == "Or") {
        CheckScalarArity(node, 1, std::numeric_limits<size_t>::max());
        return;
    }
    if (name == "Not") {
        CheckScalarArity(node, 1, 1);
        return;
    }
    if (name == "Just") {
        CheckScalarArity(node, 1, 1);
        bool nullable = false;
        const TString resultType = ScalarTypeName(node, &nullable);
        if (!nullable || resultType != ScalarTypeName(*node.Child(0))) {
            Unsupported("Opaque Just has inconsistent types");
        }
        return;
    }
    if (name == "Exists") {
        CheckExistsCallable(node);
        return;
    }
    if (name == "Coalesce") {
        CheckScalarArity(node, 1, std::numeric_limits<size_t>::max());
        bool resultNullable = false;
        const TString resultType = ScalarTypeName(node, &resultNullable);
        bool allNullable = true;
        for (const auto& child : node.Children()) {
            bool childNullable = false;
            if (ScalarTypeName(*child, &childNullable) != resultType) {
                Unsupported("Opaque Coalesce has inconsistent types");
            }
            allNullable = allNullable && childNullable;
        }
        if (resultNullable != allNullable) {
            Unsupported("Opaque Coalesce has inconsistent nullability");
        }
        return;
    }
    if (name == "If") {
        CheckIfCallable(node);
        return;
    }
    if (name == "IfPresent") {
        CheckIfPresentCallable(node);
        return;
    }
    if (name == "Substring") {
        CheckRestrictedSubstringCallable(node);
        return;
    }

    if (name == "SafeCast" || name == "Convert") {
        if (ParseCanonicalDecimalType(ScalarTypeName(node))) {
            if (IsStringDecimalSafeCastCandidate(node)) {
                StringLiteralDecimalSafeCastExpr(node);
                return;
            }
            if (name == "SafeCast") {
                CheckIntegralDecimalSafeCastCallable(node);
                return;
            }
            DecimalConstantCastExpr(node);
            return;
        }
        CheckScalarArity(node, 2, 2);
        const TString resultType = ScalarTypeName(node);
        ScalarTypeName(*node.Child(0));
        if (!node.Child(1)->IsCallable("DataType") &&
            !node.Child(1)->IsCallable("OptionalType"))
        {
            Unsupported(TStringBuilder() << "Opaque " << name << " has an unsupported target type");
        }
        if (DataTypeDescriptorName(*node.Child(1)) != resultType) {
            Unsupported(TStringBuilder() << "Opaque " << name << " target type does not match its result");
        }
        if (name == "Convert") {
            const auto options = CastResult<false>(node.Child(0)->GetTypeAnn(), node.GetTypeAnn());
            if ((options & (NUdf::ECastOptions::MayFail | NUdf::ECastOptions::Impossible)) &&
                !(allowExactUint32LiteralConversion &&
                    IsExactUint32LiteralConversion(node)))
            {
                Unsupported("Opaque Convert may fail");
            }
        }
        return;
    }

    Unsupported(TStringBuilder() << "Unsupported scalar callable " << name);
}

void CheckScalarSafetyMetadata(
    const TExprNode& node,
    bool allowUnorderedChildren = false)
{
    if (node.HasResult()) {
        Unsupported("Scalar expression contains an executed Result node");
    }
    if (node.IsPosAware()) {
        Unsupported("Scalar expression contains a position-aware node");
    }
    if (node.HasSideEffects() || !node.IsCseeSafe()) {
        Unsupported("Scalar expression contains a side-effecting or CSE-unsafe node");
    }
    if (!allowUnorderedChildren &&
        (node.IsCallable() || node.IsList()) &&
        node.UnorderedChildren())
    {
        Unsupported("Scalar expression contains a node with unordered children");
    }
}

enum class EExactCoalesceFalseArgument : ui8 {
    None,
    Comparison,
    BinaryStringMembership,
};

struct TExactCoalesceFalse {
    const TExprNode* Argument = nullptr;
    EExactCoalesceFalseArgument Kind = EExactCoalesceFalseArgument::None;
};

TExactCoalesceFalse ExactCoalesceFalseArgument(const TExprNode& node) {
    if (!node.IsCallable("Coalesce") || node.ChildrenSize() != 2) {
        return {};
    }

    const auto& fallback = *node.Child(1);
    if (!fallback.IsCallable("Bool") || fallback.ChildrenSize() != 1 ||
        !fallback.Child(0)->IsAtom("false"))
    {
        return {};
    }

    const auto& argument = *node.Child(0);
    EExactCoalesceFalseArgument kind = EExactCoalesceFalseArgument::None;
    if (argument.IsCallable({"==", "!=", "<", "<=", ">", ">="}) &&
        argument.ChildrenSize() == 2)
    {
        kind = EExactCoalesceFalseArgument::Comparison;
    } else if (argument.ChildrenSize() == 2 &&
        ((argument.IsCallable("Or") &&
          argument.Child(0)->IsCallable("==") &&
          argument.Child(1)->IsCallable("==")) ||
         (argument.IsCallable("And") &&
          argument.Child(0)->IsCallable("!=") &&
          argument.Child(1)->IsCallable("!="))) &&
        argument.Child(0)->ChildrenSize() == 2 &&
        argument.Child(1)->ChildrenSize() == 2)
    {
        kind = EExactCoalesceFalseArgument::BinaryStringMembership;
    } else {
        return {};
    }

    bool resultNullable = false;
    bool argumentNullable = false;
    bool fallbackNullable = false;
    if (ScalarTypeName(node, &resultNullable) != "Bool" || resultNullable ||
        ScalarTypeName(argument, &argumentNullable) != "Bool" ||
        !argumentNullable ||
        ScalarTypeName(fallback, &fallbackNullable) != "Bool" ||
        fallbackNullable)
    {
        return {};
    }

    CheckScalarSafetyMetadata(node);
    CheckScalarSafetyMetadata(fallback);
    return {&argument, kind};
}

void ValidateExactBinaryStringMembership(
    const TExprNode& argument,
    const TExprNode* rowArgument,
    const THashSet<TString>& visibleColumns)
{
    CheckScalarSafetyMetadata(argument);
    std::optional<TString> commonMemberName;
    for (const auto& comparison : argument.Children()) {
        CheckComparisonCallable(*comparison);
        CheckScalarSafetyMetadata(*comparison, true);

        const TExprNode* member = nullptr;
        const TExprNode* literal = nullptr;
        for (const auto& operand : comparison->Children()) {
            if (operand->IsCallable("Member") && !member) {
                member = operand.Get();
            } else if (operand->IsCallable("String") && !literal) {
                literal = operand.Get();
            } else {
                Unsupported(
                    "Exact binary String membership comparison must contain one Member and one literal");
            }
        }
        if (!member || !literal || member->ChildrenSize() != 2 ||
            member->Child(0) != rowArgument ||
            !member->Child(1)->IsAtom() ||
            !visibleColumns.contains(TString(member->Child(1)->Content())))
        {
            Unsupported(
                "Exact binary String membership Member is malformed or not a direct input value");
        }

        CheckScalarSafetyMetadata(*member);
        CheckScalarSafetyMetadata(*member->Child(1));
        CheckScalarSafetyMetadata(*literal);
        if (literal->ChildrenSize() != 1 || !literal->Child(0)->IsAtom()) {
            Unsupported("Exact binary String membership literal is malformed");
        }
        CheckScalarSafetyMetadata(*literal->Child(0));
        CheckOpaqueCallable(*literal);

        bool memberNullable = false;
        bool literalNullable = false;
        const TString memberType = ScalarTypeName(*member, &memberNullable);
        const TString literalType = ScalarTypeName(*literal, &literalNullable);
        if (!memberNullable || literalNullable ||
            memberType != "String" || literalType != "String")
        {
            Unsupported(
                "Exact binary String membership requires one Optional<String> member and matching literals");
        }

        const TString memberName(member->Child(1)->Content());
        if (!commonMemberName) {
            commonMemberName = memberName;
        } else if (memberName != *commonMemberName) {
            Unsupported(
                "Exact binary String membership comparisons must reference the same member");
        }
    }
}

struct TExactDecimalCoalesceZero {
    const TExprNode* Optional = nullptr;
    const TExprNode* Zero = nullptr;
    TString ResultType;

    explicit operator bool() const {
        return Optional && Zero && !ResultType.empty();
    }
};

bool IsExactDecimalZeroFallback(
    const TExprNode& node,
    TStringBuf expectedType)
{
    bool nullable = false;
    if (ScalarTypeName(node, &nullable) != expectedType || nullable) {
        return false;
    }

    if (node.IsCallable("Decimal")) {
        DecimalLiteralExpr(node);
        const auto parameters = ParseCanonicalDecimalType(expectedType);
        if (!parameters) {
            return false;
        }
        const auto value = NYql::NDecimal::FromString(
            node.Child(0)->Content(),
            parameters->Precision,
            parameters->Scale);
        return !NYql::NDecimal::IsError(value) &&
            value == NYql::NDecimal::TInt128(0);
    }

    if (!node.IsCallable("SafeCast") || node.ChildrenSize() != 2) {
        return false;
    }
    const auto& source = *node.Child(0);
    bool sourceNullable = false;
    if (ScalarTypeName(source, &sourceNullable) != "Int32" ||
        sourceNullable ||
        !source.IsCallable("Int32") ||
        source.ChildrenSize() != 1 ||
        !source.Child(0)->IsAtom("0"))
    {
        return false;
    }

    if (!IsCompleteIntegerLiteralDecimalCast(node)) {
        return false;
    }
    DecimalConstantCastExpr(node);
    return true;
}

TExactDecimalCoalesceZero ExactDecimalCoalesceZeroArgument(
    const TExprNode& node)
{
    if (!node.IsCallable("Coalesce") || node.ChildrenSize() != 2) {
        return {};
    }

    bool resultNullable = false;
    const TString resultType = ScalarTypeName(node, &resultNullable);
    if (resultNullable || !ParseCanonicalDecimalType(resultType)) {
        return {};
    }

    const auto& optional = *node.Child(0);
    bool optionalNullable = false;
    if (!optional.IsCallable("Member") ||
        optional.ChildrenSize() != 2 ||
        ScalarTypeName(optional, &optionalNullable) != resultType ||
        !optionalNullable)
    {
        return {};
    }

    const auto& zero = *node.Child(1);
    if (!IsExactDecimalZeroFallback(zero, resultType)) {
        return {};
    }
    return {&optional, &zero, resultType};
}

void ValidateExactDecimalCoalesceZeroMember(
    const TExactDecimalCoalesceZero& exact,
    const TExprNode* rowArgument,
    const THashSet<TString>& visibleColumns)
{
    if (!exact) {
        Unsupported("Exact Decimal Coalesce zero shape is missing");
    }
    const auto& member = *exact.Optional;
    if (!rowArgument ||
        member.Child(0) != rowArgument ||
        !member.Child(1)->IsAtom() ||
        !visibleColumns.contains(TString(member.Child(1)->Content())))
    {
        Unsupported(
            "Exact Decimal Coalesce zero requires a direct visible input member");
    }
    CheckScalarSafetyMetadata(member);
    CheckScalarSafetyMetadata(*member.Child(1));
}

struct TExactUint64JustCoalesceZero {
    const TExprNode* Optional = nullptr;
    const TExprNode* Zero = nullptr;

    explicit operator bool() const {
        return Optional && Zero;
    }
};

TExactUint64JustCoalesceZero ExactUint64JustCoalesceZero(
    const TExprNode& node)
{
    if (!node.IsCallable("Just") || node.ChildrenSize() != 1) {
        return {};
    }
    bool resultNullable = false;
    if (ScalarTypeName(node, &resultNullable) != "Uint64" ||
        !resultNullable)
    {
        return {};
    }

    const auto& coalesce = *node.Child(0);
    bool coalesceNullable = false;
    if (!coalesce.IsCallable("Coalesce") ||
        coalesce.ChildrenSize() != 2 ||
        ScalarTypeName(coalesce, &coalesceNullable) != "Uint64" ||
        coalesceNullable)
    {
        return {};
    }

    const auto& optional = *coalesce.Child(0);
    bool optionalNullable = false;
    if (!optional.IsCallable("Member") ||
        optional.ChildrenSize() != 2 ||
        ScalarTypeName(optional, &optionalNullable) != "Uint64" ||
        !optionalNullable)
    {
        return {};
    }

    const auto& zero = *coalesce.Child(1);
    bool zeroNullable = false;
    if (!zero.IsCallable("Uint64") ||
        zero.ChildrenSize() != 1 ||
        !zero.Child(0)->IsAtom("0") ||
        ScalarTypeName(zero, &zeroNullable) != "Uint64" ||
        zeroNullable)
    {
        return {};
    }
    return {&optional, &zero};
}

void ValidateExactUint64JustCoalesceZeroMember(
    const TExactUint64JustCoalesceZero& exact,
    const TExprNode* rowArgument,
    const THashSet<TString>& visibleColumns)
{
    if (!exact) {
        Unsupported("Exact Uint64 Just/Coalesce zero shape is missing");
    }
    const auto& member = *exact.Optional;
    if (!rowArgument ||
        member.Child(0) != rowArgument ||
        !member.Child(1)->IsAtom() ||
        !visibleColumns.contains(TString(member.Child(1)->Content())))
    {
        Unsupported(
            "Exact Uint64 Just/Coalesce zero requires a direct visible input member");
    }
    CheckScalarSafetyMetadata(member);
    CheckScalarSafetyMetadata(*member.Child(1));
}

const TExprNode* ExactDecimalJustArgument(const TExprNode& node) {
    if (!node.IsCallable("Just") || node.ChildrenSize() != 1) {
        return nullptr;
    }

    const auto& argument = *node.Child(0);
    bool argumentNullable = false;
    const TString argumentType = ScalarTypeName(argument, &argumentNullable);
    if (argumentNullable || !ParseCanonicalDecimalType(argumentType)) {
        return nullptr;
    }

    const bool directLiteral = argument.IsCallable("Decimal");
    const bool completeLiteralCast =
        argument.IsCallable({"SafeCast", "Convert"}) &&
        argument.ChildrenSize() == 2 &&
        IsCompleteIntegerLiteralDecimalCast(argument);
    const bool directDecimalCoalesceZero =
        static_cast<bool>(ExactDecimalCoalesceZeroArgument(argument));
    if (!directLiteral && !completeLiteralCast && !directDecimalCoalesceZero) {
        return nullptr;
    }

    bool resultNullable = false;
    if (ScalarTypeName(node, &resultNullable) != argumentType ||
        !resultNullable)
    {
        Unsupported(
            "Exact Decimal Just requires a matching Optional<Decimal> result");
    }

    CheckScalarSafetyMetadata(node);
    return &argument;
}

bool IsExactDataAnnotation(
    const TTypeAnnotationNode* annotation,
    NUdf::EDataSlot slot,
    bool nullable)
{
    bool optional = false;
    const TDataExprType* data = nullptr;
    return annotation &&
        IsDataOrOptionalOfData(annotation, optional, data) && data &&
        optional == nullable && data->GetSlot() == slot &&
        !dynamic_cast<const TDataExprParamsType*>(data);
}

constexpr TStringBuf DateTimeTmResource = "DateTime2.TM";

struct TReviewedDateTimeType {
    NUdf::EDataSlot Slot = NUdf::EDataSlot::Date;
    bool Nullable = false;
    bool Resource = false;
};

bool IsExactResourceAnnotation(
    const TTypeAnnotationNode* annotation,
    bool nullable)
{
    if (!annotation) {
        return false;
    }
    if (nullable) {
        if (annotation->GetKind() != ETypeAnnotationKind::Optional) {
            return false;
        }
        annotation = annotation->Cast<TOptionalExprType>()->GetItemType();
    }
    return annotation->GetKind() == ETypeAnnotationKind::Resource &&
        annotation->Cast<TResourceExprType>()->GetTag() == DateTimeTmResource;
}

bool IsExactReviewedDateTimeAnnotation(
    const TTypeAnnotationNode* annotation,
    TReviewedDateTimeType type)
{
    return type.Resource
        ? IsExactResourceAnnotation(annotation, type.Nullable)
        : IsExactDataAnnotation(annotation, type.Slot, type.Nullable);
}

const TTypeAnnotationNode& DescribedType(
    const TExprNode& node,
    TStringBuf label)
{
    const auto annotation = node.GetTypeAnn();
    if (!annotation || annotation->GetKind() != ETypeAnnotationKind::Type ||
        !annotation->Cast<TTypeExprType>()->GetType())
    {
        Unsupported(TStringBuilder() << label << " has no exact Type annotation");
    }
    return *annotation->Cast<TTypeExprType>()->GetType();
}

void CheckReviewedDateTimeTypeDescriptor(
    const TExprNode& node,
    TReviewedDateTimeType type,
    TStringBuf label)
{
    CheckScalarSafetyMetadata(node);
    if (!IsExactReviewedDateTimeAnnotation(&DescribedType(node, label), type)) {
        Unsupported(TStringBuilder() << label << " annotation disagrees");
    }

    const TExprNode* leaf = &node;
    if (type.Nullable) {
        if (!node.IsCallable("OptionalType") || node.ChildrenSize() != 1) {
            Unsupported(TStringBuilder() << label << " is not exact OptionalType");
        }
        leaf = node.Child(0);
        type.Nullable = false;
        CheckScalarSafetyMetadata(*leaf);
        if (!IsExactReviewedDateTimeAnnotation(&DescribedType(*leaf, label), type)) {
            Unsupported(TStringBuilder() << label << " item annotation disagrees");
        }
    }

    const TStringBuf name = type.Resource
        ? DateTimeTmResource
        : NUdf::GetDataTypeInfo(type.Slot).Name;
    const TStringBuf callable = type.Resource ? "ResourceType" : "DataType";
    if (!leaf->IsCallable(callable) || leaf->ChildrenSize() != 1 ||
        !leaf->Child(0)->IsAtom(name))
    {
        Unsupported(TStringBuilder() << label << " does not describe " << name);
    }
}

void CheckDataDescriptor(
    const TExprNode& node,
    NUdf::EDataSlot slot,
    bool nullable,
    TStringBuf label)
{
    CheckReviewedDateTimeTypeDescriptor(node, {slot, nullable, false}, label);
}

void CheckVoidNode(const TExprNode& node, bool typeDescriptor, TStringBuf label) {
    CheckScalarSafetyMetadata(node);
    const bool exact = typeDescriptor
        ? node.IsCallable("VoidType") && node.ChildrenSize() == 0 &&
            DescribedType(node, label).GetKind() == ETypeAnnotationKind::Void
        : node.IsCallable("Void") && node.ChildrenSize() == 0 &&
            node.GetTypeAnn() &&
            node.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Void;
    if (!exact) {
        Unsupported(TStringBuilder() << label << " is not exact Void");
    }
}

enum class EReviewedDateTimeUdf : ui8 {
    IntervalFromDays, Split, ShiftYears, ShiftMonths, MakeDate, Count,
};

struct TReviewedDateTimeArgument {
    TReviewedDateTimeType Type;
    ui64 Flags = 0;
};

struct TReviewedDateTimeUdfSpec {
    TStringBuf Name;
    TReviewedDateTimeType Result;
    TReviewedDateTimeArgument Arguments[2];
    size_t ArgumentCount;
    TReviewedDateTimeType UserArguments[2];
    size_t UserArgumentCount;
    bool Blocks;
};

constexpr ui64 DateTimeAutoMap = NUdf::ICallablePayload::TArgumentFlags::AutoMap;
constexpr TReviewedDateTimeType DateType{NUdf::EDataSlot::Date};
constexpr TReviewedDateTimeType Int32Type{NUdf::EDataSlot::Int32};
constexpr TReviewedDateTimeType OptionalIntervalType{
    NUdf::EDataSlot::Interval, true};
constexpr TReviewedDateTimeType TmType{NUdf::EDataSlot::Date, false, true};
constexpr TReviewedDateTimeType OptionalTmType{
    NUdf::EDataSlot::Date, true, true};

const TReviewedDateTimeUdfSpec ReviewedDateTimeUdfs[] = {
    {"DateTime2.IntervalFromDays", OptionalIntervalType,
        {{Int32Type, DateTimeAutoMap}, {}},
        1, {}, 0, true},
    {"DateTime2.Split", TmType, {{DateType, DateTimeAutoMap}, {}},
        1, {DateType}, 1, true},
    {"DateTime2.ShiftYears", OptionalTmType,
        {{TmType, DateTimeAutoMap}, {Int32Type, 0}},
        2, {DateType, Int32Type}, 2, false},
    {"DateTime2.ShiftMonths", OptionalTmType,
        {{TmType, DateTimeAutoMap}, {Int32Type, 0}},
        2, {DateType, Int32Type}, 2, false},
    {"DateTime2.MakeDate", DateType,
        {{TmType, DateTimeAutoMap}, {}},
        1, {}, 0, true},
};

static_assert(sizeof(ReviewedDateTimeUdfs) / sizeof(ReviewedDateTimeUdfs[0]) ==
    static_cast<size_t>(EReviewedDateTimeUdf::Count));

const TReviewedDateTimeUdfSpec& ReviewedDateTimeUdf(EReviewedDateTimeUdf kind) {
    return ReviewedDateTimeUdfs[static_cast<size_t>(kind)];
}

void CheckTupleDescriptor(
    const TExprNode& node,
    size_t size,
    TStringBuf label)
{
    CheckScalarSafetyMetadata(node);
    const auto& annotation = DescribedType(node, label);
    if (!node.IsCallable("TupleType") || node.ChildrenSize() != size ||
        annotation.GetKind() != ETypeAnnotationKind::Tuple)
    {
        Unsupported(TStringBuilder() << label << " is not an exact TupleType");
    }
    const auto& tuple = *annotation.Cast<TTupleExprType>();
    if (tuple.GetSize() != size) {
        Unsupported(TStringBuilder() << label << " annotation has the wrong arity");
    }
    for (size_t index = 0; index < size; ++index) {
        if (!IsSameAnnotation(
                *tuple.GetItems()[index],
                DescribedType(*node.Child(index), label)))
        {
            Unsupported(TStringBuilder() << label
                << " child annotation disagrees at " << index);
        }
    }
}

void CheckEmptyStructDescriptor(const TExprNode& node, TStringBuf label) {
    CheckScalarSafetyMetadata(node);
    const auto& annotation = DescribedType(node, label);
    if (!node.IsCallable("StructType") || node.ChildrenSize() != 0 ||
        annotation.GetKind() != ETypeAnnotationKind::Struct ||
        annotation.Cast<TStructExprType>()->GetSize() != 0)
    {
        Unsupported(TStringBuilder() << label << " is not an empty StructType");
    }
}


const TCallableExprType& CheckReviewedDateTimeCallable(
    const TTypeAnnotationNode* annotation,
    const TReviewedDateTimeUdfSpec& spec,
    TStringBuf label)
{
    if (!annotation || annotation->GetKind() != ETypeAnnotationKind::Callable) {
        Unsupported(TStringBuilder() << label << " is not Callable");
    }
    const auto& callable = *annotation->Cast<TCallableExprType>();
    const auto& arguments = callable.GetArguments();
    if (!IsExactReviewedDateTimeAnnotation(callable.GetReturnType(), spec.Result) ||
        callable.GetOptionalArgumentsCount() != 0 ||
        !callable.GetPayload().empty() ||
        arguments.size() != spec.ArgumentCount)
    {
        Unsupported(TStringBuilder() << label
            << " is not the reviewed DateTime2 signature");
    }
    for (size_t index = 0; index < spec.ArgumentCount; ++index) {
        if (!IsExactReviewedDateTimeAnnotation(
                arguments[index].Type, spec.Arguments[index].Type) ||
            !arguments[index].Name.empty() ||
            arguments[index].Flags != spec.Arguments[index].Flags)
        {
            Unsupported(TStringBuilder() << label
                << " argument " << index << " is not reviewed");
        }
    }
    return callable;
}

void CheckReviewedDateTimeUserType(
    const TExprNode& node,
    const TReviewedDateTimeUdfSpec& spec)
{
    if (spec.UserArgumentCount == 0) {
        CheckVoidNode(node, true, "DateTime2 Udf user type");
        return;
    }

    CheckTupleDescriptor(node, 3, "DateTime2 Udf user type");
    const auto& arguments = *node.Child(0);
    CheckTupleDescriptor(
        arguments, spec.UserArgumentCount, "DateTime2 Udf arguments");
    for (size_t index = 0; index < spec.UserArgumentCount; ++index) {
        CheckReviewedDateTimeTypeDescriptor(
            *arguments.Child(index),
            spec.UserArguments[index],
            "DateTime2 Udf user argument");
    }
    CheckEmptyStructDescriptor(*node.Child(1), "DateTime2 Udf options");
    CheckTupleDescriptor(*node.Child(2), 0, "DateTime2 Udf type parameters");
}

void CheckReviewedDateTimeCachedType(
    const TExprNode& node,
    const TCallableExprType& udfType,
    const TReviewedDateTimeUdfSpec& spec)
{
    CheckScalarSafetyMetadata(node);
    if (!node.IsCallable("CallableType") ||
        node.ChildrenSize() != spec.ArgumentCount + 2)
    {
        Unsupported("Cached DateTime2 CallableType has the wrong shape");
    }
    const auto& cachedType = CheckReviewedDateTimeCallable(
        &DescribedType(node, "cached DateTime2 type"),
        spec,
        "cached DateTime2 type");
    if (!IsSameAnnotation(cachedType, udfType)) {
        Unsupported("Cached DateTime2 CallableType disagrees with Udf");
    }

    const auto& main = *node.Child(0);
    const auto& result = *node.Child(1);
    CheckScalarSafetyMetadata(main);
    CheckScalarSafetyMetadata(result);
    if (!main.IsList() || main.ChildrenSize() != 0 ||
        !result.IsList() || result.ChildrenSize() != 1)
    {
        Unsupported("Cached DateTime2 CallableType has a noncanonical header");
    }
    CheckReviewedDateTimeTypeDescriptor(
        *result.Child(0), spec.Result, "DateTime2 Udf return type");

    for (size_t index = 0; index < spec.ArgumentCount; ++index) {
        const auto& argument = *node.Child(index + 2);
        const auto flags = spec.Arguments[index].Flags;
        CheckScalarSafetyMetadata(argument);
        const bool exactFlags = flags == DateTimeAutoMap
            ? argument.IsList() && argument.ChildrenSize() == 3 &&
                argument.Child(1)->IsAtom("") &&
                argument.Child(2)->IsAtom("1")
            : flags == 0 && argument.IsList() &&
                argument.ChildrenSize() == 1;
        if (!exactFlags) {
            Unsupported("Cached DateTime2 argument flags are not canonical");
        }
        CheckReviewedDateTimeTypeDescriptor(
            *argument.Child(0),
            spec.Arguments[index].Type,
            "DateTime2 Udf argument type");
    }
}

bool IsExactSetting(const TExprNode& node, TStringBuf name) {
    CheckScalarSafetyMetadata(node);
    return node.IsList() && node.ChildrenSize() == 1 &&
        node.Child(0)->IsAtom(name);
}

const TCallableExprType& CheckReviewedDateTimeUdf(
    const TExprNode& node,
    const TReviewedDateTimeUdfSpec& spec)
{
    CheckScalarSafetyMetadata(node);
    if (!node.IsCallable("Udf") || node.ChildrenSize() != 8 ||
        !node.Child(0)->IsAtom(spec.Name) ||
        !node.Child(3)->IsAtom("") || !node.Child(6)->IsAtom(""))
    {
        Unsupported(TStringBuilder() << spec.Name
            << " requires its normalized eight-child Udf");
    }
    const auto& callable = CheckReviewedDateTimeCallable(
        node.GetTypeAnn(), spec, TStringBuilder() << spec.Name << " Udf");
    CheckVoidNode(*node.Child(1), false, "DateTime2 Udf run config");
    CheckReviewedDateTimeUserType(*node.Child(2), spec);
    CheckReviewedDateTimeCachedType(*node.Child(4), callable, spec);
    CheckVoidNode(*node.Child(5), true, "DateTime2 Udf run-config type");

    const auto& settings = *node.Child(7);
    CheckScalarSafetyMetadata(settings);
    const size_t settingCount = spec.Blocks ? 2 : 1;
    if (!settings.IsList() || settings.ChildrenSize() != settingCount) {
        Unsupported(TStringBuilder() << spec.Name
            << " settings are not canonical");
    }
    if ((spec.Blocks && !IsExactSetting(*settings.Child(0), "blocks")) ||
        !IsExactSetting(*settings.Child(spec.Blocks ? 1 : 0), "strict"))
    {
        Unsupported(TStringBuilder() << spec.Name
            << " settings are not canonical");
    }
    return callable;
}

void CheckReviewedDateTimeApply(
    const TExprNode& node,
    const TReviewedDateTimeUdfSpec& spec,
    TStringBuf label)
{
    CheckScalarSafetyMetadata(node);
    if (!node.IsCallable("Apply") ||
        node.ChildrenSize() != spec.ArgumentCount + 1 ||
        !IsExactReviewedDateTimeAnnotation(node.GetTypeAnn(), spec.Result))
    {
        Unsupported(TStringBuilder() << label << " has the wrong Apply shape");
    }
    const auto& callable = CheckReviewedDateTimeUdf(*node.Child(0), spec);
    if (!IsSameAnnotation(*node.GetTypeAnn(), *callable.GetReturnType())) {
        Unsupported(TStringBuilder() << label << " result disagrees with its Udf");
    }
    for (size_t index = 0; index < spec.ArgumentCount; ++index) {
        const auto annotation = node.Child(index + 1)->GetTypeAnn();
        if (!annotation ||
            !IsSameAnnotation(*annotation, *callable.GetArguments()[index].Type))
        {
            Unsupported(TStringBuilder() << label
                << " argument " << index << " disagrees with its Udf");
        }
    }
}

i32 ParseExactInt32Literal(const TExprNode& node, TStringBuf label) {
    CheckScalarSafetyMetadata(node);
    if (!node.IsCallable("Int32") || node.ChildrenSize() != 1 ||
        !node.Child(0)->IsAtom() ||
        !IsExactDataAnnotation(node.GetTypeAnn(), NUdf::EDataSlot::Int32, false))
    {
        Unsupported(TStringBuilder() << label
            << " is not an exact Int32 literal");
    }
    LiteralExpr(node);
    return ParseInteger<i32>(node.Child(0)->Content(), label);
}

std::optional<ui16> ParseDateSafeCast(const TExprNode& node) {
    CheckScalarSafetyMetadata(node);
    if (!node.IsCallable("SafeCast") || node.ChildrenSize() != 2 ||
        !IsExactDataAnnotation(node.GetTypeAnn(), NUdf::EDataSlot::Date, true))
    {
        Unsupported("Date fold requires SafeCast with Optional<Date> result");
    }
    CheckDataDescriptor(
        *node.Child(1), NUdf::EDataSlot::Date, true, "Date SafeCast target");

    const auto& source = *node.Child(0);
    CheckScalarSafetyMetadata(source);
    bool nullable = false;
    const TString type = ScalarTypeName(source, &nullable);
    if (nullable || !IsStringType(type) || !source.IsCallable(type) ||
        source.ChildrenSize() != 1 || !source.Child(0)->IsAtom())
    {
        Unsupported("Date SafeCast source is not a direct String or Utf8 literal");
    }
    LiteralExpr(source);
    const auto& dateType = DescribedType(
        *node.Child(1)->Child(0), "Date SafeCast item");
    if (CastResult<false>(source.GetTypeAnn(), &dateType) !=
        NUdf::ECastOptions::MayFail)
    {
        Unsupported("Date SafeCast is not the reviewed MayFail conversion");
    }

    const TStringBuf text = source.Child(0)->Content();
    const auto value = NKikimr::NMiniKQL::ValueFromString(
        NUdf::EDataSlot::Date, NUdf::TStringRef(text.data(), text.size()));
    return value.HasValue()
        ? std::optional<ui16>(value.Get<ui16>())
        : std::nullopt;
}

bool IsStringDateSafeCastCandidate(const TExprNode& node) {
    if (!node.IsCallable("SafeCast") || node.ChildrenSize() != 2 ||
        !node.GetTypeAnn() || !node.Child(0)->GetTypeAnn())
    {
        return false;
    }

    const TString resultType = ScalarTypeName(node);
    const TString sourceType = ScalarTypeName(*node.Child(0));
    return resultType == "Date" && IsStringType(sourceType);
}

i32 ParseIntervalFromDays(const TExprNode& node) {
    CheckReviewedDateTimeApply(
        node,
        ReviewedDateTimeUdf(EReviewedDateTimeUdf::IntervalFromDays),
        "IntervalFromDays Apply");
    const auto& days = *node.Child(1);
    const i32 value = ParseExactInt32Literal(days, "IntervalFromDays Int32");
    constexpr i32 MaxDays = static_cast<i32>(NUdf::MAX_DATE - 1);
    if (value < -MaxDays || value > MaxDays) {
        Unsupported(TStringBuilder() << "IntervalFromDays argument is outside ["
            << -MaxDays << ", " << MaxDays << "]");
    }
    return value;
}

NJson::TJsonValue ConstantDateValue(std::optional<ui16> value) {
    auto result = JsonMap();
    result["type"] = "Date";
    if (value) {
        result["kind"] = "literal";
        result["value"] = static_cast<ui64>(*value);
    } else {
        result["kind"] = "null";
    }
    return result;
}

ui16 ParseDirectDateLiteral(const TExprNode& node) {
    CheckScalarSafetyMetadata(node);
    if (!node.IsCallable("Date") || node.ChildrenSize() != 1 ||
        !node.Child(0)->IsAtom() ||
        !IsExactDataAnnotation(node.GetTypeAnn(), NUdf::EDataSlot::Date, false))
    {
        Unsupported("Direct Date/Interval fold requires a non-null Date literal");
    }
    const auto literal = LiteralExpr(node);
    return static_cast<ui16>(literal["value"].GetUIntegerSafe());
}

i64 ParseDirectIntervalLiteral(const TExprNode& node) {
    CheckScalarSafetyMetadata(node);
    if (!node.IsCallable("Interval") || node.ChildrenSize() != 1 ||
        !node.Child(0)->IsAtom() ||
        !IsExactDataAnnotation(
            node.GetTypeAnn(), NUdf::EDataSlot::Interval, false))
    {
        Unsupported("Direct Date/Interval fold requires a non-null Interval literal");
    }
    const i64 value = ParseInteger<i64>(
        node.Child(0)->Content(), "Interval");
    if (!NUdf::IsValidLayoutValue<NUdf::TInterval>(value)) {
        Unsupported(TStringBuilder()
            << "Interval literal is outside ("
            << -static_cast<i64>(NUdf::MAX_TIMESTAMP) << ", "
            << static_cast<i64>(NUdf::MAX_TIMESTAMP) << "): " << value);
    }
    return value;
}

NJson::TJsonValue ConstantDirectDateIntervalExpr(const TExprNode& node) {
    const i64 date = ParseDirectDateLiteral(*node.Child(0));
    const i64 interval = ParseDirectIntervalLiteral(*node.Child(1));

    // MiniKQL converts Date to midnight microseconds, performs the signed
    // arithmetic, validates the scaled result, then truncates back to days in
    // mkql_date_scaler.h and mkql_builtins_{add,sub}.cpp. Valid Date and
    // Interval literals keep either intermediate well inside i64.
    static_assert(2 * NUdf::MAX_TIMESTAMP < std::numeric_limits<i64>::max());
    const i64 scaledDate = date * NMiniKQL::DateScale;
    const i64 scaledResult = node.IsCallable("+")
        ? scaledDate + interval
        : scaledDate - interval;
    if (scaledResult < 0 ||
        scaledResult >= static_cast<i64>(NUdf::MAX_TIMESTAMP))
    {
        return ConstantDateValue(std::nullopt);
    }
    return ConstantDateValue(static_cast<ui16>(
        scaledResult / NMiniKQL::DateScale));
}

// This is a closed normalization, not general Date/Interval support: both
// operands must be the exact constant shapes validated above.
NJson::TJsonValue ConstantDateIntervalExpr(const TExprNode& node) {
    CheckScalarSafetyMetadata(node);
    if (node.ChildrenSize() != 2 ||
        !IsExactDataAnnotation(node.GetTypeAnn(), NUdf::EDataSlot::Date, true))
    {
        Unsupported("Date/Interval fold requires Optional<Date> arithmetic");
    }
    if (node.Child(0)->IsCallable("Date") &&
        node.Child(1)->IsCallable("Interval"))
    {
        return ConstantDirectDateIntervalExpr(node);
    }
    const auto date = ParseDateSafeCast(*node.Child(0));
    const i32 days = ParseIntervalFromDays(*node.Child(1));
    if (date) {
        const i64 value = static_cast<i64>(*date) +
            (node.IsCallable("+") ? static_cast<i64>(days) : -static_cast<i64>(days));
        if (value >= 0 && value < static_cast<i64>(NUdf::MAX_DATE)) {
            return ConstantDateValue(static_cast<ui16>(value));
        }
    }
    return ConstantDateValue(std::nullopt);
}

bool IsDateArithmetic(const TExprNode& node) {
    bool nullable = false;
    const TDataExprType* data = nullptr;
    return node.IsCallable({"+", "-"}) && node.GetTypeAnn() &&
        IsDataOrOptionalOfData(node.GetTypeAnn(), nullable, data) && data &&
        data->GetSlot() == NUdf::EDataSlot::Date;
}

enum class EConstantDateShift {
    Years,
    Months,
};

EConstantDateShift ConstantDateShiftKind(const TExprNode& udf) {
    if (udf.ChildrenSize() > 0) {
        if (udf.Child(0)->IsAtom(
                ReviewedDateTimeUdf(EReviewedDateTimeUdf::ShiftYears).Name))
        {
            return EConstantDateShift::Years;
        }
        if (udf.Child(0)->IsAtom(
                ReviewedDateTimeUdf(EReviewedDateTimeUdf::ShiftMonths).Name))
        {
            return EConstantDateShift::Months;
        }
    }
    Unsupported("Constant DateTime2 fold requires ShiftYears or ShiftMonths");
}

const TReviewedDateTimeUdfSpec& ConstantDateShiftUdf(
    EConstantDateShift kind)
{
    return ReviewedDateTimeUdf(
        kind == EConstantDateShift::Years
            ? EReviewedDateTimeUdf::ShiftYears
            : EReviewedDateTimeUdf::ShiftMonths);
}

ui16 ParseSplitDate(const TExprNode& node) {
    CheckReviewedDateTimeApply(
        node,
        ReviewedDateTimeUdf(EReviewedDateTimeUdf::Split),
        "DateTime2.Split Apply");
    return ParseDirectDateLiteral(*node.Child(1));
}

// Keep this small calendar model separate from the normalized IR-shape gate.
// DateTime2.TM stores Year in an unsigned 12-bit field.
constexpr i64 MaxDateTimeTmYear = (1U << 12) - 1;

std::optional<ui16> ShiftConstantDate(
    ui16 date,
    i32 amount,
    EConstantDateShift kind)
{
    ui32 year = 0;
    ui32 month = 0;
    ui32 day = 0;
    if (!NMiniKQL::SplitDate(date, year, month, day)) {
        Unsupported("DateTime2.Split rejected a validated Date literal");
    }

    i64 shiftedYear = year;
    if (kind == EConstantDateShift::Years) {
        shiftedYear += static_cast<i64>(amount);
    } else {
        i64 shiftedMonth = static_cast<i64>(amount) + month;
        shiftedYear += (shiftedMonth - 1) / 12;
        if (shiftedYear < 0 || shiftedYear > MaxDateTimeTmYear) {
            Unsupported("DateTime2.ShiftMonths would wrap the TM year field");
        }
        shiftedMonth = 1 + (shiftedMonth - 1) % 12;
        if (shiftedMonth <= 0) {
            if (shiftedYear == 0) {
                Unsupported("DateTime2.ShiftMonths would wrap the TM year field");
            }
            --shiftedYear;
            shiftedMonth += 12;
        }
        month = static_cast<ui32>(shiftedMonth);
    }

    // Fail closed instead of modeling modulo-4096 assignment for extreme shifts.
    if (shiftedYear < 0 || shiftedYear > MaxDateTimeTmYear) {
        Unsupported("DateTime2 shift would wrap the TM year field");
    }
    year = static_cast<ui32>(shiftedYear);
    day = std::min(
        day,
        NMiniKQL::GetMonthLength(month, NMiniKQL::IsLeapYear(year)));

    ui16 result = 0;
    return NMiniKQL::MakeDate(year, month, day, result)
        ? std::optional<ui16>(result)
        : std::nullopt;
}

NJson::TJsonValue ConstantShiftedDateExpr(const TExprNode& node) {
    CheckScalarSafetyMetadata(node);
    if (!node.IsCallable("Map") || node.ChildrenSize() != 2 ||
        !IsExactDataAnnotation(node.GetTypeAnn(), NUdf::EDataSlot::Date, true))
    {
        Unsupported("Constant DateTime2 fold requires Optional<Date> Map");
    }

    const auto& shift = *node.Child(0);
    CheckScalarSafetyMetadata(shift);
    if (!shift.IsCallable("Apply") || shift.ChildrenSize() != 3 ||
        !IsExactResourceAnnotation(shift.GetTypeAnn(), true))
    {
        Unsupported("Constant DateTime2 fold requires a two-argument shift Apply");
    }
    const auto& udf = *shift.Child(0);
    const auto kind = ConstantDateShiftKind(udf);
    CheckReviewedDateTimeApply(
        shift, ConstantDateShiftUdf(kind), "DateTime2 shift Apply");
    const ui16 date = ParseSplitDate(*shift.Child(1));

    const auto& amount = *shift.Child(2);
    const i32 value = ParseExactInt32Literal(amount, "DateTime2 shift Int32");

    const auto& lambda = *node.Child(1);
    if (!lambda.IsLambda() || lambda.ChildrenSize() != 2 ||
        !lambda.Child(0)->IsArguments() ||
        lambda.Child(0)->ChildrenSize() != 1 ||
        !lambda.Child(0)->Child(0)->IsArgument() ||
        lambda.Child(0)->Child(0)->ChildrenSize() != 0)
    {
        Unsupported("Constant DateTime2 Map requires a unary lambda");
    }
    const auto& argument = *lambda.Child(0)->Child(0);
    CheckScalarSafetyMetadata(lambda);
    CheckScalarSafetyMetadata(*lambda.Child(0));
    CheckScalarSafetyMetadata(argument);
    if (!IsExactResourceAnnotation(argument.GetTypeAnn(), false)) {
        Unsupported("Constant DateTime2 Map lambda argument is not TM");
    }

    const auto& body = *lambda.Child(1);
    CheckReviewedDateTimeApply(
        body,
        ReviewedDateTimeUdf(EReviewedDateTimeUdf::MakeDate),
        "DateTime2.MakeDate Apply");
    if (body.Child(1) != &argument) {
        Unsupported("Constant DateTime2 Map lambda is not MakeDate identity use");
    }
    return ConstantDateValue(ShiftConstantDate(date, value, kind));
}

bool IsConstantShiftedDate(const TExprNode& node) {
    return node.IsCallable("Map") &&
        IsExactDataAnnotation(node.GetTypeAnn(), NUdf::EDataSlot::Date, true);
}

class TRestrictedConcatAuditor;

class TRestrictedConcatAuditToken {
    friend class TRestrictedConcatAuditor;

private:
    TRestrictedConcatAuditToken() = default;
};

class TOpaqueExpressionEncoder {
public:
    TOpaqueExpressionEncoder(
        const TExprNode* rowArgument,
        const THashSet<TString>& visibleColumns,
        TVector<const TExprNode*> boundArguments = {})
        : RowArgument(rowArgument)
        , VisibleColumns(visibleColumns)
        , BoundArguments(std::move(boundArguments))
    {
    }

    TOpaqueExpressionEncoder(
        TRestrictedConcatAuditToken,
        const TExprNode* rowArgument,
        const THashSet<TString>& visibleColumns)
        : RowArgument(rowArgument)
        , VisibleColumns(visibleColumns)
        , AllowRestrictedConcat(true)
    {
    }

    void Validate(const TExprNode& node) {
        AllowNestedIfPresent = true;
        TStringBuilder fingerprint;
        EncodeRoot(node, fingerprint);
    }

    NJson::TJsonValue Export(
        const TExprNode& node,
        TExactScalarBudget& budget,
        size_t argumentDepth)
    {
        bool nullable = false;
        const TString resultType = ScalarTypeName(node, &nullable);

        TStringBuilder fingerprint;
        EncodeRoot(node, fingerprint);

        budget.Charge(argumentDepth, ExternalArguments.size());
        auto args = JsonArray();
        for (const auto& argument : ExternalArguments) {
            args.AppendValue(argument.IsBound
                ? BoundExpr(argument.Depth)
                : ColumnExpr(argument.Column));
        }

        auto result = JsonMap();
        result["kind"] = "opaque";
        result["fingerprint"] = TString(fingerprint);
        result["type"] = resultType;
        result["nullable"] = nullable;
        result["args"] = std::move(args);
        return result;
    }

private:
    static constexpr size_t MaxNodes = 256;
    static constexpr size_t MaxDepth = 64;
    static constexpr size_t MaxFingerprintBytes = 64 * 1024;

    struct TExternalArgument {
        bool IsBound;
        TString Column;
        size_t Depth;
    };

    void EncodeRoot(const TExprNode& node, TStringBuilder& fingerprint) {
        if (!node.IsCallable()) {
            Unsupported("Opaque scalar root is not a callable");
        }
        AppendIdentityField(fingerprint, "format", "yql-opaque-v1");
        Encode(node, fingerprint, 0);
        if (fingerprint.size() > MaxFingerprintBytes) {
            Unsupported("Opaque scalar fingerprint exceeds the audit limit");
        }
    }

    TString TypeFingerprint(const TExprNode& node) const {
        return node.GetTypeAnn() ? FormatType(node.GetTypeAnn()) : TString("<none>");
    }

    void CheckSafeNode(const TExprNode& node) {
        if (++NodeCount > MaxNodes) {
            Unsupported("Opaque scalar exceeds the node audit limit");
        }
        CheckScalarSafetyMetadata(node);
    }

    void EncodeMember(const TExprNode& node, TStringBuilder& out) {
        if (node.ChildrenSize() != 2 || !node.Child(1)->IsAtom()) {
            Unsupported("Malformed Member expression");
        }
        const TString column(node.Child(1)->Content());
        if (node.Child(0) != RowArgument || !VisibleColumns.contains(column)) {
            Unsupported(TStringBuilder() << "Member does not reference the input row column " << column);
        }
        ScalarTypeName(node);

        const size_t index = ExternalIndex(
            TStringBuilder() << "column:" << column.size() << ":" << column,
            {false, column, 0});

        AppendIdentityField(out, "node", "member");
        AppendIdentityField(out, "type", TypeFingerprint(node));
        AppendIdentityField(out, "argument", ToString(index));
    }

    size_t BoundDepth(const TExprNode& node) const {
        const auto it = std::find(BoundArguments.begin(), BoundArguments.end(), &node);
        if (it == BoundArguments.end()) {
            Unsupported("Opaque scalar contains a free Argument");
        }
        return static_cast<size_t>(it - BoundArguments.begin());
    }

    size_t ExternalIndex(TString key, TExternalArgument argument) {
        const auto [it, inserted] = ExternalIndices.emplace(
            std::move(key),
            ExternalArguments.size());
        if (inserted) {
            ExternalArguments.push_back(std::move(argument));
        }
        return it->second;
    }

    void EncodeBound(const TExprNode& node, TStringBuilder& out) {
        bool nullable = false;
        ScalarTypeName(node, &nullable);
        if (nullable) {
            Unsupported("IfPresent bound argument must be non-nullable");
        }
        const size_t depth = BoundDepth(node);
        const size_t index = ExternalIndex(
            TStringBuilder() << "bound:" << depth,
            {true, {}, depth});
        AppendIdentityField(out, "node", "bound");
        AppendIdentityField(out, "type", TypeFingerprint(node));
        AppendIdentityField(out, "argument", ToString(index));
    }

    void EncodeIfPresent(
        const TExprNode& node,
        TStringBuilder& out,
        size_t depth)
    {
        if (!AllowNestedIfPresent) {
            Unsupported("Opaque scalar cannot hide an IfPresent binder");
        }
        const auto signature = CheckIfPresentCallable(node);
        const auto& handler = *node.Child(1);
        const auto& arguments = *handler.Child(0);
        CheckSafeNode(handler);
        CheckSafeNode(arguments);
        CheckSafeNode(*signature.Argument);

        AppendIdentityField(out, "node", "callable");
        AppendIdentityField(out, "content", "IfPresent");
        AppendIdentityField(out, "type", TypeFingerprint(node));
        AppendIdentityField(out, "children", "3");
        Encode(*signature.Optional, out, depth + 1);

        AppendIdentityField(out, "node", "lambda");
        AppendIdentityField(out, "argument_type", TypeFingerprint(*signature.Argument));
        AppendIdentityField(out, "result_type", TypeFingerprint(*signature.Present));
        AppendIdentityField(out, "children", "1");
        if (BoundArguments.size() >= MaxIfPresentBindingDepth) {
            Unsupported("IfPresent binding depth exceeds the audit limit");
        }
        BoundArguments.insert(BoundArguments.begin(), signature.Argument);
        Encode(*signature.Present, out, depth + 1);
        BoundArguments.erase(BoundArguments.begin());

        Encode(*signature.Missing, out, depth + 1);
    }

    void Encode(
        const TExprNode& node,
        TStringBuilder& out,
        size_t depth,
        bool allowExactUint32LiteralConversion = false)
    {
        if (depth > MaxDepth) {
            Unsupported("Opaque scalar exceeds the nesting audit limit");
        }
        CheckSafeNode(node);

        if (node.IsCallable("Member")) {
            EncodeMember(node, out);
            return;
        }
        if (node.IsArgument()) {
            EncodeBound(node, out);
            return;
        }
        if (node.IsCallable("IfPresent")) {
            EncodeIfPresent(node, out, depth);
            return;
        }

        switch (node.Type()) {
            case TExprNode::Callable:
                if (node.IsCallable("Concat")) {
                    if (!AllowRestrictedConcat) {
                        Unsupported("Unsupported scalar callable Concat");
                    }
                } else {
                    CheckOpaqueCallable(node, allowExactUint32LiteralConversion);
                }
                AppendIdentityField(out, "node", "callable");
                AppendIdentityField(out, "content", node.Content());
                break;
            case TExprNode::Atom:
                AppendIdentityField(out, "node", "atom");
                AppendIdentityField(out, "content", node.Content());
                AppendIdentityField(out, "flags", ToString(node.GetFlagsToCompare()));
                break;
            case TExprNode::List:
                Unsupported("Opaque scalar contains an unsupported List node");
            case TExprNode::Lambda:
                Unsupported("Opaque scalar contains a nested Lambda");
            case TExprNode::Argument:
                Unsupported("Opaque scalar contains a free Argument");
            case TExprNode::Arguments:
                Unsupported("Opaque scalar contains an Arguments node");
            case TExprNode::World:
                Unsupported("Opaque scalar contains World");
        }

        AppendIdentityField(out, "type", TypeFingerprint(node));
        AppendIdentityField(out, "children", ToString(node.ChildrenSize()));
        for (size_t index = 0; index < node.ChildrenSize(); ++index) {
            Encode(
                *node.Child(index),
                out,
                depth + 1,
                node.IsCallable("Substring") && index > 0);
        }
    }

private:
    const TExprNode* RowArgument;
    const THashSet<TString>& VisibleColumns;
    TVector<const TExprNode*> BoundArguments;
    THashMap<TString, size_t> ExternalIndices;
    TVector<TExternalArgument> ExternalArguments;
    size_t NodeCount = 0;
    bool AllowNestedIfPresent = false;
    bool AllowRestrictedConcat = false;
};

// NDataShard::NLimits::MaxWriteValueSize caps an ordinary stored value at
// 16 MiB (ydb/core/tx/datashard/const.h), with write-path enforcement in
// ydb/core/tx/datashard/datashard_write_operation.cpp and
// ydb/core/tx/datashard/datashard_common_upload.cpp.
constexpr ui64 MaxDatashardStoredStringBytes = 16ULL * 1024 * 1024;

// Column-store String values use Arrow BinaryType
// (ydb/core/formats/arrow/switch/switch_type.h) and reach MiniKQL through the
// matching block traits (yql/essentials/public/udf/arrow/dispatch_traits.h).
// BinaryType uses signed 32-bit offsets
// (contrib/libs/apache/arrow/cpp/src/arrow/type.h), and Arrow
// validation requires nonnegative, monotonic offsets within the value buffer
// (contrib/libs/apache/arrow/cpp/src/arrow/array/validate.cc). Thus one logical
// value is at most INT32_MAX bytes regardless of the compressed persistence
// path; blob-size limits are deliberately not used as a cell-size premise.
constexpr ui64 MaxOlapStoredStringBytes =
    static_cast<ui64>(std::numeric_limits<i32>::max());

struct TStoredStringProvenance {
    bool Nullable;
    ui64 MaximumBytes;
};

using TStoredStringColumns = THashMap<TString, TStoredStringProvenance>;

// MiniKQL grows Concat's ui32 allocation capacity by 50 percent in
// yql/essentials/minikql/mkql_string_util.cpp.  This is the largest result for
// which `size + size / 2` cannot wrap that capacity.
constexpr ui64 MaxConcatAllocationBytes =
    2ULL * std::numeric_limits<ui32>::max() / 3;
static_assert(
    MaxConcatAllocationBytes + MaxConcatAllocationBytes / 2 <=
    std::numeric_limits<ui32>::max());
static_assert(
    MaxConcatAllocationBytes + 1 + (MaxConcatAllocationBytes + 1) / 2 >
    std::numeric_limits<ui32>::max());
static_assert(MaxOlapStoredStringBytes < MaxConcatAllocationBytes);
static_assert(2 * MaxOlapStoredStringBytes > MaxConcatAllocationBytes);
static_assert(2 * MaxDatashardStoredStringBytes < MaxConcatAllocationBytes);

class TRestrictedConcatAuditor {
public:
    TRestrictedConcatAuditor(
        const TExprNode* rowArgument,
        const THashSet<TString>& visibleColumns,
        const TStoredStringColumns& storedStringColumns)
        : RowArgument(rowArgument)
        , VisibleColumns(visibleColumns)
        , StoredStringColumns(storedStringColumns)
    {
    }

    NJson::TJsonValue ExportAsOpaque(
        const TExprNode& root,
        TExactScalarBudget& budget,
        size_t argumentDepth)
    {
        Audit(root);
        return TOpaqueExpressionEncoder(
            TRestrictedConcatAuditToken{},
            RowArgument,
            VisibleColumns).Export(root, budget, argumentDepth);
    }

private:
    void Audit(const TExprNode& root) {
        if (!root.IsCallable("Concat")) {
            Fail("root is not Concat");
        }
        Visit(root, 0);
        if (StoredMemberCount == 0) {
            Fail("contains no storage-bounded String member");
        }
    }

    static constexpr size_t MaxNodes = 256;
    static constexpr size_t MaxDepth = 64;
    static constexpr size_t MaxStoredMembers = 2;

    [[noreturn]] void Fail(TStringBuf reason) const {
        Unsupported(TStringBuilder() << "Restricted Concat: " << reason);
    }

    void CheckStringType(const TExprNode& node, bool nullable) const {
        if (!IsExactDataAnnotation(
                node.GetTypeAnn(),
                NUdf::EDataSlot::String,
                nullable))
        {
            Fail(nullable
                ? "node is not exactly Optional<String>"
                : "node is not exactly non-null String");
        }
    }

    void AddMaximumBytes(ui64 bytes) {
        if (bytes > MaxConcatAllocationBytes - MaximumBytes) {
            Fail("maximum byte length exceeds the safe Concat allocation bound");
        }
        MaximumBytes += bytes;
    }

    void AddLiteralBytes(size_t bytes) {
        AddMaximumBytes(bytes);
    }

    void AddStoredMember(ui64 maximumBytes) {
        if (++StoredMemberCount > MaxStoredMembers) {
            Fail("contains more than two stored-member occurrences");
        }
        AddMaximumBytes(maximumBytes);
    }

    void CheckMember(const TExprNode& node, bool nullable) {
        CheckStringType(node, nullable);
        if (node.ChildrenSize() != 2 ||
            node.Child(0) != RowArgument ||
            !node.Child(1)->IsAtom())
        {
            Fail("has a malformed stored member");
        }
        const TString column(node.Child(1)->Content());
        if (!VisibleColumns.contains(column)) {
            Fail(TStringBuilder()
                << "member " << column << " is not visible at the Map input");
        }
        const auto* provenance = StoredStringColumns.FindPtr(column);
        if (!provenance) {
            Fail(TStringBuilder()
                << "member " << column
                << " has no storage-bounded String provenance");
        }
        if (provenance->Nullable != nullable) {
            Fail(TStringBuilder()
                << "member " << column
                << " storage provenance is "
                << (provenance->Nullable ? "nullable" : "non-null")
                << " but the expression is "
                << (nullable ? "nullable" : "non-null"));
        }
        AddStoredMember(provenance->MaximumBytes);
    }

    void CheckLiteral(const TExprNode& node, bool requireEmpty = false) {
        CheckStringType(node, false);
        if (!node.IsCallable("String") ||
            node.ChildrenSize() != 1 ||
            !node.Child(0)->IsAtom())
        {
            Fail("literal is not canonical String data");
        }
        LiteralExpr(node);
        const size_t bytes = node.Child(0)->Content().size();
        if (requireEmpty && bytes != 0) {
            Fail("nullable stored member fallback is not the empty String");
        }
        AddLiteralBytes(bytes);
    }

    void CheckNullableStoredMember(const TExprNode& node) {
        CheckStringType(node, false);
        if (!node.IsCallable("Coalesce") || node.ChildrenSize() != 2) {
            Fail("nullable stored member is not canonical Coalesce");
        }
        CheckMember(*node.Child(0), true);
        CheckLiteral(*node.Child(1), true);
    }

    void Visit(const TExprNode& node, size_t depth) {
        if (++NodeCount > MaxNodes || depth > MaxDepth) {
            Fail("exceeds the scalar audit limit");
        }
        CheckScalarSafetyMetadata(node);

        if (node.IsCallable("Concat")) {
            CheckStringType(node, false);
            if (node.ChildrenSize() != 2) {
                Fail("is not binary");
            }
            Visit(*node.Child(0), depth + 1);
            Visit(*node.Child(1), depth + 1);
            return;
        }
        if (node.IsCallable("String")) {
            CheckLiteral(node);
            return;
        }
        if (node.IsCallable("Member")) {
            CheckMember(node, false);
            return;
        }
        if (node.IsCallable("Coalesce")) {
            CheckNullableStoredMember(node);
            return;
        }
        Fail(TStringBuilder()
            << "contains unsupported callable " << node.Content());
    }

private:
    const TExprNode* RowArgument;
    const THashSet<TString>& VisibleColumns;
    const TStoredStringColumns& StoredStringColumns;
    size_t NodeCount = 0;
    size_t StoredMemberCount = 0;
    ui64 MaximumBytes = 0;
};

NJson::TJsonValue ExportExprNode(
    const TExprNode& node,
    const TExprNode* rowArgument,
    const THashSet<TString>& visibleColumns,
    const TVector<const TExprNode*>& boundArguments,
    TExactScalarBudget& budget,
    size_t normalizedDepth,
    size_t sourceDepth)
{
    if (sourceDepth > MaxExactScalarDepth) {
        Unsupported(TStringBuilder()
            << "Exact scalar expression exceeds the depth audit limit of "
            << MaxExactScalarDepth);
    }
    budget.Charge(normalizedDepth);

    if (node.IsArgument()) {
        const auto it = std::find(boundArguments.begin(), boundArguments.end(), &node);
        if (it == boundArguments.end()) {
            Unsupported("Scalar expression contains a free Argument");
        }
        bool nullable = false;
        ScalarTypeName(node, &nullable);
        if (nullable) {
            Unsupported("IfPresent bound argument must be non-nullable");
        }
        return BoundExpr(static_cast<size_t>(it - boundArguments.begin()));
    }

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

    if (node.IsCallable("Void")) {
        if (node.ChildrenSize() != 0) {
            Unsupported("Void must have no arguments");
        }
        if (!node.GetTypeAnn() ||
            node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Void)
        {
            Unsupported("Void expression is not typed Void");
        }
        auto result = JsonMap();
        result["kind"] = "void";
        return result;
    }

    if (node.IsCallable("Exists")) {
        const auto* argument = CheckExistsCallable(node);
        CheckScalarSafetyMetadata(node);

        return ExistsExpr(ExportExprNode(
            *argument,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1));
    }

    if (node.IsCallable("If")) {
        const auto signature = CheckIfCallable(node);

        // Branch terms are built eagerly by the verifier.  Recursive export
        // keeps that equivalent to lazy YQL If by admitting only audited,
        // deterministic, total scalar subtrees.
        CheckScalarSafetyMetadata(node);

        auto result = JsonMap();
        result["kind"] = "if";
        result["condition"] = ExportExprNode(
            *signature.Condition,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["then"] = ExportExprNode(
            *signature.Then,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["else"] = ExportExprNode(
            *signature.Else,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["type"] = signature.ResultType;
        result["nullable"] = signature.ResultNullable;
        return result;
    }

    if (IsDateArithmetic(node)) {
        return ConstantDateIntervalExpr(node);
    }

    if (IsConstantShiftedDate(node)) {
        return ConstantShiftedDateExpr(node);
    }

    if (node.IsCallable("Decimal")) {
        return DecimalLiteralExpr(node);
    }

    if (IsSupportedType(node.Content())) {
        auto result = LiteralExpr(node);
        if (node.Content() == "Date") {
            bool nullable = false;
            if (ScalarTypeName(node, &nullable) != node.Content() || nullable) {
                Unsupported("Date literal type annotation does not match its callable");
            }
        }
        return result;
    }

    if (const auto exact = ExactUint64JustCoalesceZero(node); exact) {
        // This is the exact optimizer repair for a missing group from an
        // originally keyless correlated COUNT. The inner IfPresent restores
        // COUNT's non-null zero identity; the outer always-taken If preserves
        // the scalar binding's Optional<Uint64> type.
        ValidateExactUint64JustCoalesceZeroMember(
            exact,
            rowArgument,
            visibleColumns);
        TOpaqueExpressionEncoder(
            rowArgument,
            visibleColumns,
            boundArguments).Validate(node);
        if (boundArguments.size() >= MaxIfPresentBindingDepth) {
            Unsupported(
                "Exact Uint64 Just/Coalesce zero binding depth exceeds the audit limit");
        }

        budget.Charge(normalizedDepth + 1, 3);
        budget.Charge(normalizedDepth + 2);

        auto repaired = JsonMap();
        repaired["kind"] = "if_present";
        repaired["optional"] = ExportExprNode(
            *exact.Optional,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 2,
            sourceDepth + 2);
        repaired["present"] = BoundExpr(0);
        repaired["missing"] = ExportExprNode(
            *exact.Zero,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 2,
            sourceDepth + 2);
        repaired["type"] = "Uint64";
        repaired["nullable"] = false;

        auto condition = JsonMap();
        condition["kind"] = "literal";
        condition["type"] = "Bool";
        condition["value"] = true;
        auto unreachable = JsonMap();
        unreachable["kind"] = "null";
        unreachable["type"] = "Uint64";

        auto result = JsonMap();
        result["kind"] = "if";
        result["condition"] = std::move(condition);
        result["then"] = std::move(repaired);
        result["else"] = std::move(unreachable);
        result["type"] = "Uint64";
        result["nullable"] = true;
        return result;
    }

    if (const auto* argument = ExactDecimalJustArgument(node)) {
        // Just never changes the value of its non-null argument.  Keep this
        // normalization deliberately closed over reviewed non-null exact
        // Decimal forms whose semantics are already audited below.  The
        // unreachable NULL branch preserves the source Optional type in the
        // normalized IR and output schema while the true condition makes
        // runtime presence exact.
        TOpaqueExpressionEncoder(
            rowArgument,
            visibleColumns,
            boundArguments).Validate(node);
        const TString resultType = ScalarTypeName(*argument);

        budget.Charge(normalizedDepth + 1, 2); // Synthetic true and typed NULL.
        auto condition = JsonMap();
        condition["kind"] = "literal";
        condition["type"] = "Bool";
        condition["value"] = true;
        auto missing = JsonMap();
        missing["kind"] = "null";
        missing["type"] = resultType;

        auto result = JsonMap();
        result["kind"] = "if";
        result["condition"] = std::move(condition);
        result["then"] = ExportExprNode(
            *argument,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["else"] = std::move(missing);
        result["type"] = resultType;
        result["nullable"] = true;
        return result;
    }

    if (const auto exactCoalesce = ExactCoalesceFalseArgument(node);
        exactCoalesce.Argument)
    {
        const auto* argument = exactCoalesce.Argument;
        // Coalesce(p, false) is the identity handler for a present Boolean and
        // false for NULL.  Reuse the existing exact IfPresent semantics rather
        // than erasing the wrapper, which would be wrong in value-sensitive
        // positions such as Not or projection.  Besides one comparison, admit
        // only the reviewed two-literal String membership/complement trees used
        // by TPCH q12.  Larger Boolean trees retain their shared opaque identity.
        if (exactCoalesce.Kind == EExactCoalesceFalseArgument::Comparison) {
            TOpaqueExpressionEncoder encoder(
                rowArgument,
                visibleColumns,
                boundArguments);
            if (argument->UnorderedChildren()) {
                // YQL marks equality operands unordered because equality is
                // commutative.  The explicit eq/not-eq IR has the same
                // symmetry, so this one marker is semantic rather than an
                // evaluation-order ambiguity. Keep every operand subtree under
                // the ordinary closed-world audit.
                if (!argument->IsCallable({"==", "!="})) {
                    Unsupported(
                        "Exact Coalesce false allows unordered children only on equality");
                }
                CheckScalarSafetyMetadata(*argument, true);
                encoder.Validate(*argument->Child(0));
                encoder.Validate(*argument->Child(1));
            } else {
                encoder.Validate(node);
            }
        } else {
            // Validate the two direct leaves separately so their equality
            // commutativity markers never relax the global opaque encoder.
            ValidateExactBinaryStringMembership(
                *argument,
                rowArgument,
                visibleColumns);
        }
        if (boundArguments.size() >= MaxIfPresentBindingDepth) {
            Unsupported("Exact Coalesce false binding depth exceeds the audit limit");
        }

        budget.Charge(normalizedDepth + 1); // Synthetic identity handler bound value.
        auto result = JsonMap();
        result["kind"] = "if_present";
        result["optional"] = ExportExprNode(
            *argument,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["present"] = BoundExpr(0);
        result["missing"] = ExportExprNode(
            *node.Child(1),
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["type"] = "Bool";
        result["nullable"] = false;
        return result;
    }

    if (const auto exact = ExactDecimalCoalesceZeroArgument(node); exact) {
        // This is the exact SQL value operation `optional ?? 0`, not an opaque
        // syntax identity. Normalize only the reviewed structural shape
        // observed in q77: one direct optional Decimal member and either a
        // canonical Decimal zero or the complete Int32-zero SafeCast spelling
        // that constant folding replaces it with.
        ValidateExactDecimalCoalesceZeroMember(
            exact,
            rowArgument,
            visibleColumns);
        TOpaqueExpressionEncoder(
            rowArgument,
            visibleColumns,
            boundArguments).Validate(node);
        if (boundArguments.size() >= MaxIfPresentBindingDepth) {
            Unsupported(
                "Exact Decimal Coalesce zero binding depth exceeds the audit limit");
        }

        budget.Charge(normalizedDepth + 1); // Synthetic identity handler bound value.
        auto result = JsonMap();
        result["kind"] = "if_present";
        result["optional"] = ExportExprNode(
            *exact.Optional,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["present"] = BoundExpr(0);
        result["missing"] = ExportExprNode(
            *exact.Zero,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["type"] = exact.ResultType;
        result["nullable"] = false;
        return result;
    }

    if (node.IsCallable("IfPresent")) {
        const auto signature = CheckIfPresentCallable(node);

        // The SMT encoder constructs both branch terms eagerly.  That is
        // equivalent to YQL's lazy branch choice only after this closed-world
        // exporter has recursively admitted only deterministic, total branch
        // subtrees.  Audit the binder nodes themselves here; every branch node
        // is checked by its own exact or opaque exporter below.
        CheckScalarSafetyMetadata(node);
        CheckScalarSafetyMetadata(*node.Child(1));
        CheckScalarSafetyMetadata(*node.Child(1)->Child(0));
        CheckScalarSafetyMetadata(*signature.Argument);

        if (boundArguments.size() >= MaxIfPresentBindingDepth) {
            Unsupported("IfPresent binding depth exceeds the audit limit");
        }
        auto presentBindings = boundArguments;
        presentBindings.insert(presentBindings.begin(), signature.Argument);

        auto result = JsonMap();
        result["kind"] = "if_present";
        result["optional"] = ExportExprNode(
            *signature.Optional,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["present"] = ExportExprNode(
            *signature.Present,
            rowArgument,
            visibleColumns,
            presentBindings,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["missing"] = ExportExprNode(
            *signature.Missing,
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["type"] = signature.ResultType;
        result["nullable"] = signature.ResultNullable;
        return result;
    }

    if (node.IsCallable("Contains")) {
        // New RBO lowers a static SQL membership test to a temporary
        // one-value dictionary under IfPresent.  Recognize only that exact,
        // set-like shape and normalize it back to the existing explicit IN
        // semantics.  Generic dictionaries and Contains stay unsupported.
        constexpr size_t MaxItems = 512;
        if (node.ChildrenSize() != 2) {
            Unsupported("Static-set Contains must have exactly two arguments");
        }
        bool resultNullable = false;
        if (ScalarTypeName(node, &resultNullable) != "Bool" || resultNullable) {
            Unsupported("Static-set Contains result must be non-null Bool");
        }

        const auto& dict = *node.Child(0);
        const auto& lookup = *node.Child(1);
        if (!dict.IsCallable("ToDict") || dict.ChildrenSize() != 4) {
            Unsupported("Static-set Contains input must be the exact ToDict shape");
        }
        if (!lookup.IsArgument()) {
            Unsupported("Static-set Contains lookup must be an IfPresent bound value");
        }
        const auto bound = std::find(
            boundArguments.begin(),
            boundArguments.end(),
            &lookup);
        if (bound == boundArguments.end()) {
            Unsupported("Static-set Contains lookup is a free Argument");
        }

        const auto& values = *dict.Child(0);
        if (!values.IsCallable("List") ||
            values.ChildrenSize() < 2 ||
            values.ChildrenSize() > MaxItems + 1)
        {
            Unsupported("Static-set ToDict must contain between 1 and 512 values");
        }
        if (!values.GetTypeAnn() ||
            values.GetTypeAnn()->GetKind() != ETypeAnnotationKind::List)
        {
            Unsupported("Static-set List has no exact List type annotation");
        }
        const auto* itemAnnotation = values.GetTypeAnn()
            ->Cast<TListExprType>()->GetItemType();
        bool itemNullable = false;
        const TString itemType = TypeName(itemAnnotation, &itemNullable);
        if (itemNullable) {
            Unsupported("Static-set items must be non-nullable");
        }
        if (ParseCanonicalDecimalType(itemType)) {
            Unsupported("Static-set Decimal membership is unsupported");
        }

        const auto& listDescriptor = *values.Child(0);
        if (!listDescriptor.IsCallable("ListType") ||
            listDescriptor.ChildrenSize() != 1 ||
            !listDescriptor.GetTypeAnn() ||
            listDescriptor.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Type)
        {
            Unsupported("Static-set List has an invalid ListType descriptor");
        }
        const auto* describedList = listDescriptor.GetTypeAnn()
            ->Cast<TTypeExprType>()->GetType();
        if (!describedList || describedList->GetKind() != ETypeAnnotationKind::List ||
            !IsSameAnnotation(
                *describedList->Cast<TListExprType>()->GetItemType(),
                *itemAnnotation))
        {
            Unsupported("Static-set ListType annotation disagrees with its List");
        }
        bool descriptorNullable = false;
        if (DataTypeDescriptorName(
                *listDescriptor.Child(0),
                &descriptorNullable) != itemType ||
            descriptorNullable)
        {
            Unsupported("Static-set ListType descriptor disagrees with its item type");
        }

        bool lookupNullable = false;
        if (ScalarTypeName(lookup, &lookupNullable) != itemType || lookupNullable) {
            Unsupported("Static-set Contains lookup must exactly match its item type");
        }

        const auto checkUnaryLambda = [&](const TExprNode& lambda, TStringBuf label) {
            if (!lambda.IsLambda() || lambda.ChildrenSize() != 2 ||
                !lambda.Child(0)->IsArguments() ||
                lambda.Child(0)->ChildrenSize() != 1 ||
                !lambda.Child(0)->Child(0)->IsArgument())
            {
                Unsupported(TStringBuilder()
                    << "Static-set ToDict " << label << " must be a unary lambda");
            }
            const auto& argument = *lambda.Child(0)->Child(0);
            bool nullable = false;
            if (ScalarTypeName(argument, &nullable) != itemType || nullable) {
                Unsupported(TStringBuilder()
                    << "Static-set ToDict " << label << " argument has the wrong type");
            }
            CheckScalarSafetyMetadata(lambda);
            CheckScalarSafetyMetadata(*lambda.Child(0));
            CheckScalarSafetyMetadata(argument);
            return &argument;
        };

        const auto& keyLambda = *dict.Child(1);
        const auto* keyArgument = checkUnaryLambda(keyLambda, "key selector");
        if (keyLambda.Child(1) != keyArgument) {
            Unsupported("Static-set ToDict key selector must be identity");
        }

        const auto& payloadLambda = *dict.Child(2);
        checkUnaryLambda(payloadLambda, "payload selector");
        const auto& payload = *payloadLambda.Child(1);
        if (!payload.IsCallable("Void") || payload.ChildrenSize() != 0 ||
            !payload.GetTypeAnn() ||
            payload.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Void)
        {
            Unsupported("Static-set ToDict payload selector must return Void");
        }

        const auto& settings = *dict.Child(3);
        if (!settings.IsList() || settings.ChildrenSize() != 2 ||
            !settings.Child(0)->IsAtom("One") ||
            !settings.Child(1)->IsAtom("Auto"))
        {
            Unsupported("Static-set ToDict settings must be exactly (One, Auto)");
        }

        if (!dict.GetTypeAnn() ||
            dict.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Dict)
        {
            Unsupported("Static-set ToDict has no exact Dict type annotation");
        }
        const auto* dictType = dict.GetTypeAnn()->Cast<TDictExprType>();
        if (!IsSameAnnotation(*dictType->GetKeyType(), *itemAnnotation) ||
            dictType->GetPayloadType()->GetKind() != ETypeAnnotationKind::Void)
        {
            Unsupported("Static-set ToDict annotation disagrees with its selectors");
        }

        CheckScalarSafetyMetadata(node);
        CheckScalarSafetyMetadata(dict);
        CheckScalarSafetyMetadata(values);
        CheckScalarSafetyMetadata(listDescriptor);
        CheckScalarSafetyMetadata(payload);
        CheckScalarSafetyMetadata(settings);

        budget.Charge(normalizedDepth + 1); // Emitted bound lookup.
        auto items = JsonArray();
        for (size_t index = 1; index < values.ChildrenSize(); ++index) {
            const auto& item = *values.Child(index);
            bool nullable = false;
            if (ScalarTypeName(item, &nullable) != itemType || nullable) {
                Unsupported("Static-set List items must have one non-null type");
            }
            items.AppendValue(ExportExprNode(
                item,
                rowArgument,
                visibleColumns,
                boundArguments,
                budget,
                normalizedDepth + 1,
                sourceDepth + 1));
        }

        auto result = JsonMap();
        result["kind"] = "in";
        result["lookup"] = BoundExpr(static_cast<size_t>(
            bound - boundArguments.begin()));
        result["items"] = std::move(items);
        return result;
    }

    if (IsStringDateSafeCastCandidate(node)) {
        auto result = ConstantDateValue(ParseDateSafeCast(node));

        // Preserve the closed-world metadata and totality audit used for
        // opaque expressions, while giving this fixed conversion its exact
        // runtime value. Dynamic String-to-Date casts deliberately fail the
        // direct-literal gate in ParseDateSafeCast.
        TOpaqueExpressionEncoder(
            rowArgument,
            visibleColumns,
            boundArguments).Validate(node);
        return result;
    }

    if (IsPartialIntegralSafeCast(node)) {
        const TString resultType = CheckPartialIntegralSafeCastCallable(node);

        // Keep the same closed-world totality and metadata audit as opaque
        // expressions while assigning this checked conversion an exact value.
        TOpaqueExpressionEncoder(
            rowArgument,
            visibleColumns,
            boundArguments).Validate(node);

        auto result = JsonMap();
        result["kind"] = "cast_integral";
        result["arg"] = ExportExprNode(
            *node.Child(0),
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["type"] = resultType;
        result["nullable"] = true;
        return result;
    }

    if (node.IsCallable("SafeCast") &&
        ParseCanonicalDecimalType(ScalarTypeName(node)))
    {
        if (IsStringDecimalSafeCastCandidate(node)) {
            auto result = StringLiteralDecimalSafeCastExpr(node);

            // Keep the closed-world safety and metadata audit even though this
            // fixed conversion is normalized to a literal or typed NULL.
            TOpaqueExpressionEncoder(
                rowArgument,
                visibleColumns,
                boundArguments).Validate(node);
            return result;
        }

        const TString resultType = CheckIntegralDecimalSafeCastCallable(node);
        if (IsCompleteIntegerLiteralDecimalCast(node)) {
            return DecimalConstantCastExpr(node);
        }

        // Retain the closed-world node checks used by opaque expressions while
        // assigning this reviewed cast shape an exact verifier meaning.
        TOpaqueExpressionEncoder(
            rowArgument,
            visibleColumns,
            boundArguments).Validate(node);

        auto result = JsonMap();
        result["kind"] = "cast_decimal";
        result["arg"] = ExportExprNode(
            *node.Child(0),
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["type"] = resultType;
        result["nullable"] = false;
        return result;
    }

    if (node.IsCallable("Convert") &&
        ParseCanonicalDecimalType(ScalarTypeName(node)))
    {
        return DecimalConstantCastExpr(node);
    }

    if (node.IsCallable("Nothing")) {
        const TString type = NothingTypeName(node);
        auto result = JsonMap();
        result["kind"] = "null";
        result["type"] = type;
        return result;
    }

    if (node.IsCallable("SqlIn")) {
        constexpr size_t MaxItems = 512;
        if (node.ChildrenSize() != 3) {
            Unsupported("SqlIn must have exactly three arguments");
        }

        bool lookupNullable = false;
        const TString lookupType = ScalarTypeName(*node.Child(1), &lookupNullable);
        bool resultNullable = false;
        if (ScalarTypeName(node, &resultNullable) != "Bool") {
            Unsupported("SqlIn result is not Bool");
        }
        if (resultNullable != lookupNullable) {
            Unsupported("SqlIn result nullability does not match its lookup");
        }

        const auto& collection = *node.Child(0);
        if (!collection.IsList() && !collection.IsCallable("AsList")) {
            Unsupported("SqlIn collection is not a direct static tuple or AsList");
        }
        if (collection.ChildrenSize() == 0 || collection.ChildrenSize() > MaxItems) {
            Unsupported(TStringBuilder()
                << "SqlIn static collection size must be in [1, " << MaxItems << "]");
        }

        if (!collection.GetTypeAnn()) {
            Unsupported("SqlIn static collection has no type annotation");
        }
        TVector<TString> annotatedItemTypes;
        annotatedItemTypes.reserve(collection.ChildrenSize());
        if (collection.IsList()) {
            if (collection.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Tuple) {
                Unsupported("SqlIn raw static collection is not typed as a tuple");
            }
            const auto* tupleType = collection.GetTypeAnn()->Cast<TTupleExprType>();
            if (tupleType->GetSize() != collection.ChildrenSize()) {
                Unsupported("SqlIn static tuple annotation has the wrong size");
            }
            for (const auto* itemType : tupleType->GetItems()) {
                bool nullable = false;
                const TString type = TypeName(itemType, &nullable);
                if (nullable) {
                    Unsupported("SqlIn static tuple item annotation is nullable");
                }
                if (!StaticSqlInEqualityCompatible(lookupType, type)) {
                    Unsupported("SqlIn static tuple item is not equality-compatible with its lookup");
                }
                annotatedItemTypes.push_back(type);
            }
            if (std::any_of(
                annotatedItemTypes.begin(),
                annotatedItemTypes.end(),
                [&](const TString& type) { return type != annotatedItemTypes.front(); }))
            {
                Unsupported("SqlIn static tuple must have one item type");
            }
        } else {
            if (collection.GetTypeAnn()->GetKind() != ETypeAnnotationKind::List) {
                Unsupported("SqlIn AsList collection is not typed as a list");
            }
            bool nullable = false;
            const auto* itemType = collection.GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            const TString type = TypeName(itemType, &nullable);
            if (nullable) {
                Unsupported("SqlIn AsList item annotation is nullable");
            }
            if (!StaticSqlInEqualityCompatible(lookupType, type)) {
                Unsupported("SqlIn AsList item is not equality-compatible with its lookup");
            }
            annotatedItemTypes.assign(collection.ChildrenSize(), type);
        }

        auto items = JsonArray();
        for (size_t index = 0; index < collection.ChildrenSize(); ++index) {
            const auto& item = *collection.Child(index);
            bool nullable = false;
            const TString type = ScalarTypeName(item, &nullable);
            if (nullable) {
                Unsupported("SqlIn item is nullable");
            }
            if (type != annotatedItemTypes[index]) {
                Unsupported("SqlIn item type does not match its collection annotation");
            }
            items.AppendValue(ExportExprNode(
                item,
                rowArgument,
                visibleColumns,
                boundArguments,
                budget,
                normalizedDepth + 1,
                sourceDepth + 1));
        }

        const auto& options = *node.Child(2);
        if (!options.IsList()) {
            Unsupported("SqlIn options are not a tuple");
        }
        THashSet<TString> optionNames;
        for (const auto& option : options.Children()) {
            if (!option->IsList() || option->ChildrenSize() != 1 ||
                !option->Child(0)->IsAtom())
            {
                Unsupported("SqlIn option must be a one-atom tuple");
            }
            const TString name(option->Child(0)->Content());
            if (name == "tableSource") {
                Unsupported("SqlIn tableSource collections are unsupported");
            }
            // For a nonempty, non-null collection with one losslessly comparable
            // item type, ANSI and legacy IN have the same 3VL result. The other
            // accepted flags control warnings, representation, or optimizer
            // bookkeeping only. tableSource is rejected above because it changes
            // how collection items are extracted.
            if (name != "ansi" && name != "warnNoAnsi" &&
                name != "isCompact" && name != "nullsProcessed")
            {
                Unsupported(TStringBuilder() << "Unsupported SqlIn option " << name);
            }
            if (!optionNames.insert(name).second) {
                Unsupported(TStringBuilder() << "Duplicate SqlIn option " << name);
            }
        }

        auto result = JsonMap();
        result["kind"] = "in";
        result["lookup"] = ExportExprNode(
            *node.Child(1),
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        result["items"] = std::move(items);
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
            args.AppendValue(ExportExprNode(
                *child,
                rowArgument,
                visibleColumns,
                boundArguments,
                budget,
                normalizedDepth + 1,
                sourceDepth + 1));
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
        result["arg"] = ExportExprNode(
            *node.Child(0),
            rowArgument,
            visibleColumns,
            boundArguments,
            budget,
            normalizedDepth + 1,
            sourceDepth + 1);
        return result;
    }

    if (node.IsCallable({"==", "!=", "<", "<=", ">", ">=", "IsNotDistinctFrom"})) {
        CheckComparisonCallable(node, true);
        const bool equality = node.IsCallable({"==", "!=", "IsNotDistinctFrom"});
        const bool negated = node.IsCallable("!=");
        if (negated) {
            budget.Charge(normalizedDepth + 1); // Inner equality below Not.
        }
        const size_t childDepth = normalizedDepth + (negated ? 2 : 1);

        static const THashMap<TString, TString> Kinds = {
            {"<", "lt"},
            {"<=", "lte"},
            {">", "gt"},
            {">=", "gte"},
        };
        auto result = BinaryExpr(
            equality ? TStringBuf("eq") : TStringBuf(Kinds.at(TString(node.Content()))),
            ExportExprNode(
                *node.Child(0), rowArgument, visibleColumns, boundArguments,
                budget, childDepth, sourceDepth + 1),
            ExportExprNode(
                *node.Child(1), rowArgument, visibleColumns, boundArguments,
                budget, childDepth, sourceDepth + 1));
        if (node.IsCallable("IsNotDistinctFrom")) {
            result["null_safe"] = true;
        }
        return negated ? NotExpr(std::move(result)) : std::move(result);
    }

    if (node.IsCallable({"+", "-", "*"}) && node.ChildrenSize() == 2) {
        bool resultNullable = false;
        bool leftNullable = false;
        bool rightNullable = false;
        const TString resultType = ScalarTypeName(node, &resultNullable);
        const TString leftType = ScalarTypeName(*node.Child(0), &leftNullable);
        const TString rightType = ScalarTypeName(*node.Child(1), &rightNullable);
        const bool exactInteger =
            IsIntegerType(resultType) &&
            leftType == resultType &&
            rightType == resultType;
        const bool exactDecimalAddOrSub =
            node.IsCallable({"+", "-"}) &&
            ParseCanonicalDecimalType(resultType) &&
            leftType == resultType &&
            rightType == resultType;
        if ((exactInteger || exactDecimalAddOrSub) &&
            resultNullable == (leftNullable || rightNullable))
        {
            // Keep the old closed-world and safety checks even though the result
            // now has a concrete verifier meaning instead of an opaque identity.
            TOpaqueExpressionEncoder(
                rowArgument,
                visibleColumns,
                boundArguments).Validate(node);

            TStringBuf kind;
            if (node.IsCallable("+")) {
                kind = "add";
            } else if (node.IsCallable("-")) {
                kind = "sub";
            } else {
                kind = "mul";
            }
            auto result = BinaryExpr(
                kind,
                ExportExprNode(
                    *node.Child(0), rowArgument, visibleColumns, boundArguments,
                    budget, normalizedDepth + 1, sourceDepth + 1),
                ExportExprNode(
                    *node.Child(1), rowArgument, visibleColumns, boundArguments,
                    budget, normalizedDepth + 1, sourceDepth + 1));
            result["type"] = resultType;
            result["nullable"] = resultNullable;
            return result;
        }
    }

    if (node.IsCallable({"DecimalMul", "DecimalDiv"})) {
        const auto signature = CheckDecimalArithmeticCallable(node);

        // Retain the same closed-world node checks as opaque expressions while
        // giving the admitted Decimal arithmetic an exact verifier meaning.
        TOpaqueExpressionEncoder(
            rowArgument,
            visibleColumns,
            boundArguments).Validate(node);

        const TStringBuf kind = node.IsCallable("DecimalMul") ? "mul" : "div";
        auto result = BinaryExpr(
            kind,
            ExportExprNode(
                *node.Child(0), rowArgument, visibleColumns, boundArguments,
                budget, normalizedDepth + 1, sourceDepth + 1),
            ExportExprNode(
                *node.Child(1), rowArgument, visibleColumns, boundArguments,
                budget, normalizedDepth + 1, sourceDepth + 1));
        result["type"] = signature.ResultType;
        result["nullable"] = signature.ResultNullable;
        return result;
    }

    return TOpaqueExpressionEncoder(
        rowArgument,
        visibleColumns,
        boundArguments).Export(node, budget, normalizedDepth + 1);
}

NJson::TJsonValue ExportExprWithBudget(
    const TExpression& expression,
    const THashSet<TString>& visibleColumns,
    TExactScalarBudget& budget,
    size_t normalizedDepth)
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
        visibleColumns,
        {},
        budget,
        normalizedDepth,
        1);
}

NJson::TJsonValue ExportExpr(
    const TExpression& expression,
    const THashSet<TString>& visibleColumns)
{
    TExactScalarBudget budget;
    auto result = ExportExprWithBudget(expression, visibleColumns, budget, 1);
    AuditExactScalarExpression(result);
    return result;
}

NJson::TJsonValue ExportExpr(
    const TExpression& expression,
    const THashSet<TString>& visibleColumns,
    const TStoredStringColumns& storedStringColumns)
{
    if (!expression.Node ||
        !expression.Node->IsLambda() ||
        expression.Node->ChildrenSize() != 2)
    {
        Unsupported("RBO expression is not a one-body lambda");
    }
    const auto* arguments = expression.Node->Child(0);
    if (!arguments->IsArguments() ||
        arguments->ChildrenSize() != 1 ||
        !arguments->Child(0)->IsArgument())
    {
        Unsupported("RBO expression does not have exactly one row argument");
    }
    const auto body = expression.GetExpressionBody();
    if (!body->IsCallable("Concat")) {
        return ExportExpr(expression, visibleColumns);
    }

    TExactScalarBudget budget;
    budget.Charge(1);
    auto result = TRestrictedConcatAuditor(
        arguments->Child(0),
        visibleColumns,
        storedStringColumns).ExportAsOpaque(*body, budget, 2);
    AuditExactScalarExpression(result);
    return result;
}

using TOlapColumnMap = THashMap<TString, TString>;

NJson::TJsonValue ExportOlapScalar(
    const TExprNode::TPtr& node,
    const TOlapColumnMap& columns,
    bool positiveFilterContext,
    TExactScalarBudget& budget,
    size_t normalizedDepth,
    size_t sourceDepth);

NJson::TJsonValue OlapColumnExpr(
    TStringBuf physicalName,
    const TOlapColumnMap& columns)
{
    const auto* output = columns.FindPtr(TString(physicalName));
    if (!output) {
        Unsupported(TStringBuilder()
            << "OLAP predicate references unavailable physical column "
            << physicalName);
    }
    return ColumnExpr(*output);
}

void CheckOlapBoolOpType(const TExprNode& node) {
    if (node.ChildrenSize() != 4) {
        return;
    }
    const auto* descriptor = node.Child(3);
    bool descriptorNullable = false;
    if (DataTypeDescriptorName(*descriptor, &descriptorNullable) != "Bool") {
        Unsupported("OLAP Boolean operation has an invalid result type descriptor");
    }
    if (const auto annotation = descriptor->GetTypeAnn()) {
        if (annotation->GetKind() != ETypeAnnotationKind::Type) {
            Unsupported("OLAP Boolean operation type annotation is not Type");
        }
        bool annotationNullable = false;
        if (TypeName(
                annotation->Cast<TTypeExprType>()->GetType(),
                &annotationNullable) != "Bool" ||
            annotationNullable != descriptorNullable)
        {
            Unsupported("OLAP Boolean operation type annotation disagrees with its descriptor");
        }
    }
}

NJson::TJsonValue ExportOlapBinary(
    const TKqpOlapFilterBinaryOp& operation,
    const TOlapColumnMap& columns,
    bool positiveFilterContext,
    TExactScalarBudget& budget,
    size_t normalizedDepth,
    size_t sourceDepth)
{
    const auto& node = operation.Ref();
    if (node.ChildrenSize() != 3 && node.ChildrenSize() != 4) {
        Unsupported("Malformed OLAP binary operation");
    }
    const TString op(operation.Operator().StringValue());
    CheckOlapBoolOpType(node);

    if (op == "??") {
        const auto& fallback = operation.Right().Ref();
        if (!fallback.IsCallable("Bool") || fallback.ChildrenSize() != 1 ||
            !fallback.Child(0)->IsAtom("false"))
        {
            Unsupported("OLAP filter coalesce is supported only with false fallback");
        }
        if (!positiveFilterContext) {
            Unsupported(
                "OLAP filter coalesce requires a positive filter context");
        }
        // IsTrue(Coalesce(predicate, false)) is exactly IsTrue(predicate), and
        // that equivalence remains a congruence through AND/OR.  It is not
        // valid in value-sensitive positions such as NOT or exists/empty, so
        // recursive callers carry the positive-filter context explicitly.
        return ExportOlapScalar(
            operation.Left().Ptr(), columns, true,
            budget, normalizedDepth, sourceDepth + 1);
    }

    TStringBuf kind;
    if (op == "eq" || op == "neq") {
        kind = "eq";
    } else if (op == "lt" || op == "lte" || op == "gt" || op == "gte") {
        kind = op;
    } else {
        Unsupported(TStringBuilder() << "Unsupported OLAP binary operation " << op);
    }

    const bool negated = op == "neq";
    budget.Charge(normalizedDepth);
    if (negated) {
        budget.Charge(normalizedDepth + 1); // Inner equality below Not.
    }
    const size_t childDepth = normalizedDepth + (negated ? 2 : 1);
    auto result = BinaryExpr(
        kind,
        ExportOlapScalar(
            operation.Left().Ptr(), columns, false,
            budget, childDepth, sourceDepth + 1),
        ExportOlapScalar(
            operation.Right().Ptr(), columns, false,
            budget, childDepth, sourceDepth + 1));
    return negated ? NotExpr(std::move(result)) : std::move(result);
}

NJson::TJsonValue ExportOlapUnary(
    const TKqpOlapFilterUnaryOp& operation,
    const TOlapColumnMap& columns,
    TExactScalarBudget& budget,
    size_t normalizedDepth,
    size_t sourceDepth)
{
    const auto& node = operation.Ref();
    if (node.ChildrenSize() != 2) {
        Unsupported("Malformed OLAP unary operation");
    }
    // The generated tuple wrapper matches every two-child expression list;
    // this explicit tag check is therefore part of the fail-closed boundary.
    if (!node.Child(0)->IsAtom()) {
        Unsupported("OLAP unary operation has a non-Atom operator");
    }

    const TString op(operation.Operator().StringValue());
    if (op == "just") {
        const auto& argument = operation.Arg().Ref();
        CheckScalarSafetyMetadata(node);
        CheckScalarSafetyMetadata(argument);
        if (!argument.IsCallable("Date") ||
            !IsExactDataAnnotation(
                argument.GetTypeAnn(), NUdf::EDataSlot::Date, false))
        {
            Unsupported(
                "OLAP just may erase only a direct non-null Date literal");
        }
        budget.Charge(normalizedDepth);
        return LiteralExpr(argument);
    }
    if (op != "exists" && op != "empty") {
        Unsupported(TStringBuilder() << "Unsupported OLAP unary operation " << op);
    }

    const bool negated = op == "empty";
    budget.Charge(normalizedDepth);
    if (negated) {
        budget.Charge(normalizedDepth + 1); // Exists below Not.
    }
    auto result = ExistsExpr(ExportOlapScalar(
        operation.Arg().Ptr(), columns, false,
        budget, normalizedDepth + (negated ? 2 : 1), sourceDepth + 1));
    return negated ? NotExpr(std::move(result)) : std::move(result);
}

NJson::TJsonValue ExportOlapScalar(
    const TExprNode::TPtr& node,
    const TOlapColumnMap& columns,
    bool positiveFilterContext,
    TExactScalarBudget& budget,
    size_t normalizedDepth,
    size_t sourceDepth)
{
    if (sourceDepth > MaxExactScalarDepth) {
        Unsupported(TStringBuilder()
            << "Exact scalar expression exceeds the depth audit limit of "
            << MaxExactScalarDepth);
    }
    if (!node) {
        Unsupported("OLAP predicate contains a null expression node");
    }

    if (node->IsAtom()) {
        budget.Charge(normalizedDepth);
        return OlapColumnExpr(node->Content(), columns);
    }

    if (node->IsCallable("Decimal")) {
        budget.Charge(normalizedDepth);
        return DecimalLiteralExpr(*node);
    }

    if (IsStringDateSafeCastCandidate(*node)) {
        budget.Charge(normalizedDepth);
        return ConstantDateValue(ParseDateSafeCast(*node));
    }

    if (node->IsCallable({"SafeCast", "Convert"}) &&
        ParseCanonicalDecimalType(ScalarTypeName(*node)))
    {
        budget.Charge(normalizedDepth);
        if (IsStringDecimalSafeCastCandidate(*node)) {
            return StringLiteralDecimalSafeCastExpr(*node);
        }
        return DecimalConstantCastExpr(*node);
    }

    if (node->IsCallable() && IsSupportedType(node->Content())) {
        budget.Charge(normalizedDepth);
        return LiteralExpr(*node);
    }

    if (const auto maybeUnary = TMaybeNode<TKqpOlapFilterUnaryOp>(node)) {
        return ExportOlapUnary(
            maybeUnary.Cast(), columns, budget, normalizedDepth, sourceDepth);
    }

    if (const auto maybeBinary = TMaybeNode<TKqpOlapFilterBinaryOp>(node)) {
        return ExportOlapBinary(
            maybeBinary.Cast(),
            columns,
            positiveFilterContext,
            budget,
            normalizedDepth,
            sourceDepth);
    }

    if (const auto maybeAnd = TMaybeNode<TKqpOlapAnd>(node)) {
        if (maybeAnd.Ref().ChildrenSize() == 0) {
            Unsupported("KqpOlapAnd has no arguments");
        }
        budget.Charge(normalizedDepth);
        auto result = JsonMap();
        result["kind"] = "and";
        auto args = JsonArray();
        for (const auto& child : maybeAnd.Ref().Children()) {
            args.AppendValue(ExportOlapScalar(
                child,
                columns,
                positiveFilterContext,
                budget,
                normalizedDepth + 1,
                sourceDepth + 1));
        }
        result["args"] = std::move(args);
        return result;
    }

    if (const auto maybeOr = TMaybeNode<TKqpOlapOr>(node)) {
        if (maybeOr.Ref().ChildrenSize() == 0) {
            Unsupported("KqpOlapOr has no arguments");
        }
        budget.Charge(normalizedDepth);
        auto result = JsonMap();
        result["kind"] = "or";
        auto args = JsonArray();
        for (const auto& child : maybeOr.Ref().Children()) {
            args.AppendValue(ExportOlapScalar(
                child,
                columns,
                positiveFilterContext,
                budget,
                normalizedDepth + 1,
                sourceDepth + 1));
        }
        result["args"] = std::move(args);
        return result;
    }

    if (const auto maybeNot = TMaybeNode<TKqpOlapNot>(node)) {
        if (maybeNot.Ref().ChildrenSize() != 1) {
            Unsupported("Malformed KqpOlapNot");
        }
        budget.Charge(normalizedDepth);
        return NotExpr(ExportOlapScalar(
            maybeNot.Cast().Value().Ptr(), columns, false,
            budget, normalizedDepth + 1, sourceDepth + 1));
    }

    Unsupported(TStringBuilder()
        << "Unsupported OLAP predicate node "
        << (node->IsCallable() ? node->Content() : TStringBuf("<non-callable>")));
}

NJson::TJsonValue ExportOlapPredicate(
    const TExprNode::TPtr& lambda,
    const TOlapColumnMap& columns)
{
    if (!lambda || !lambda->IsLambda() || lambda->ChildrenSize() != 2) {
        Unsupported("OLAP process is not a one-body lambda");
    }
    const auto* arguments = lambda->Child(0);
    if (!arguments->IsArguments() || arguments->ChildrenSize() != 1 ||
        !arguments->Child(0)->IsArgument())
    {
        Unsupported("OLAP process does not have exactly one flow argument");
    }
    const auto* rowArgument = arguments->Child(0);
    TVector<TExprNode::TPtr> conditions;

    auto node = lambda->ChildPtr(1);
    while (node.Get() != rowArgument) {
        if (!node) {
            Unsupported("OLAP process contains a null operation");
        }
        if (const auto maybeFilter = TMaybeNode<TKqpOlapFilter>(node)) {
            const auto filter = maybeFilter.Cast();
            // Two or more pushed filters add one synthetic AND, so 1,023
            // one-node conditions are the largest possibly admissible chain.
            if (conditions.size() >= MaxExactScalarNodes - 1) {
                Unsupported(TStringBuilder()
                    << "Exact scalar expression exceeds the node audit limit of "
                    << MaxExactScalarNodes);
            }
            conditions.push_back(filter.Condition().Ptr());
            node = filter.Input().Ptr();
            continue;
        }
        Unsupported(TStringBuilder()
            << "Unsupported OLAP process operation "
            << (node->IsCallable() ? node->Content() : TStringBuf("<non-callable>")));
    }

    std::reverse(conditions.begin(), conditions.end());

    if (conditions.empty()) {
        Unsupported("OLAP process contains no filter operation");
    }
    const bool combined = conditions.size() > 1;
    TExactScalarBudget budget;
    if (combined) {
        budget.Charge(1); // Synthetic conjunction of pushed filters.
    }
    const size_t conditionDepth = combined ? 2 : 1;
    TVector<NJson::TJsonValue> predicates;
    predicates.reserve(conditions.size());
    for (const auto& condition : conditions) {
        predicates.push_back(ExportOlapScalar(
            condition,
            columns,
            true,
            budget,
            conditionDepth,
            1));
    }

    if (predicates.size() == 1) {
        auto result = std::move(predicates.front());
        AuditExactScalarExpression(result);
        return result;
    }

    auto result = JsonMap();
    result["kind"] = "and";
    auto args = JsonArray();
    for (auto& predicate : predicates) {
        args.AppendValue(std::move(predicate));
    }
    result["args"] = std::move(args);
    AuditExactScalarExpression(result);
    return result;
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
private:
    enum class ESubplanKind {
        Scalar,
        Exists,
    };

    struct TScalarSubplanDetails {
        struct TCorrelation {
            TString Dependency;
            const TTypeAnnotationNode* DependencyType = nullptr;
        };

        TString OutputColumn;
        TString Type;
        bool OutputNullable = false;
        std::optional<TCorrelation> Correlation;
    };

    struct TExistsCorrelation {
        NJson::TJsonValue Predicate;
        TString Dependency;
        const TTypeAnnotationNode* DependencyType = nullptr;
    };

    struct TExistsSubplanDetails {
        std::optional<TExistsCorrelation> Correlation;
    };

    struct TSubplanDescriptor {
        TString Binding;
        // RegistryRoot retains optimizer topology for nesting audits.
        // ExportedRoot is the closed relation actually serialized.
        TIntrusivePtr<IOperator> RegistryRoot;
        TIntrusivePtr<IOperator> ExportedRoot;
        std::variant<TScalarSubplanDetails, TExistsSubplanDetails> Details;
        TVector<IOperator*> Consumers;
    };

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
        ValidateSnapshotTopology(Root);
        PrepareSubplans();
        CheckSnapshotProperties(Root, false);
        if (Root.ColumnOrder.empty()) {
            Unsupported("Root output order must not be empty");
        }

        // Subplan roots precede the main root so descriptors and consumer IDs
        // share one deterministic post-order node namespace.
        for (const auto& subplan : Subplans) {
            ExportNode(subplan.ExportedRoot);
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
        result["subplans"] = ExportSubplans();
        return result;
    }

    const THashMap<const IOperator*, TString>& GetNodeIds() const {
        return Ids;
    }

    const TVector<IOperator*>& GetNodeOrder() const {
        return NodeOrder;
    }

    void ValidateStageProperties() const {
        if (!StageGraphPresent) {
            Unsupported("Stage properties require a StageGraph");
        }
        for (auto* op : NodeOrder) {
            CheckSnapshotProperties(*op, true);
        }
    }

    const TString& GetRootId() const {
        return RootId;
    }

private:
    struct TExactType {
        TString Name;
        bool Nullable = false;
    };

    static ESubplanKind SubplanKind(const TSubplanDescriptor& subplan) {
        if (std::get_if<TScalarSubplanDetails>(&subplan.Details)) {
            return ESubplanKind::Scalar;
        }
        if (std::get_if<TExistsSubplanDetails>(&subplan.Details)) {
            return ESubplanKind::Exists;
        }
        Unsupported("Subplan descriptor has invalid details");
    }

    static TString KindName(const TSubplanDescriptor& subplan) {
        return SubplanKind(subplan) == ESubplanKind::Scalar
            ? TString("Scalar")
            : TString("EXISTS");
    }

    static const TScalarSubplanDetails& ScalarDetails(
        const TSubplanDescriptor& subplan)
    {
        const auto* result =
            std::get_if<TScalarSubplanDetails>(&subplan.Details);
        Y_ENSURE(result, "Scalar subplan descriptor has invalid details");
        return *result;
    }

    static const TExistsSubplanDetails& ExistsDetails(
        const TSubplanDescriptor& subplan)
    {
        const auto* result =
            std::get_if<TExistsSubplanDetails>(&subplan.Details);
        Y_ENSURE(result, "EXISTS subplan descriptor has invalid details");
        return *result;
    }

    static const TString& SubplanType(const TSubplanDescriptor& subplan) {
        if (const auto* scalar =
                std::get_if<TScalarSubplanDetails>(&subplan.Details))
        {
            return scalar->Type;
        }
        if (std::get_if<TExistsSubplanDetails>(&subplan.Details)) {
            static const TString BoolType = "Bool";
            return BoolType;
        }
        Unsupported("Subplan descriptor has invalid details");
    }

    static TExactType ExactType(const TTypeAnnotationNode* type) {
        TExactType result;
        result.Name = TypeName(type, &result.Nullable);
        return result;
    }

    static bool SameType(const TExactType& left, const TExactType& right) {
        return left.Name == right.Name && left.Nullable == right.Nullable;
    }

    static THashSet<TString> ExpressionColumns(const TExpression& expression) {
        if (!expression.Node) {
            Unsupported("Subplan predicate is empty");
        }
        THashSet<TString> result;
        THashSet<const TExprNode*> visited;
        std::function<void(const TExprNode&)> visit =
            [&](const TExprNode& node) {
                if (!visited.insert(&node).second) {
                    return;
                }
                if (node.IsCallable("Member")) {
                    if (node.ChildrenSize() != 2 || !node.Child(1)->IsAtom()) {
                        Unsupported("Subplan predicate contains a malformed Member");
                    }
                    const TString name(node.Child(1)->Content());
                    if (name.empty()) {
                        Unsupported("Subplan predicate contains an empty column name");
                    }
                    result.insert(name);
                }
                for (const auto& child : node.Children()) {
                    visit(*child);
                }
            };
        visit(*expression.Node);
        return result;
    }

    static std::optional<TString> DirectMemberName(const TExprNode& node) {
        if (!node.IsCallable("Member") ||
            node.ChildrenSize() != 2 ||
            !node.Child(1)->IsAtom())
        {
            return std::nullopt;
        }
        const TString name(node.Child(1)->Content());
        return name.empty() ? std::nullopt : std::optional<TString>(name);
    }

    struct TDirectCorrelation {
        TExpression Expression;
        TString InnerColumn;
    };

    static TDirectCorrelation ExtractDirectCorrelation(
        TStringBuf kind,
        TStringBuf binding,
        TStringBuf dependency,
        const TExpression& predicate,
        const THashSet<TString>& innerNames)
    {
        std::optional<TExpression> correlation;
        TString innerColumn;
        for (const auto& conjunct : predicate.SplitConjunct()) {
            const auto columns = ExpressionColumns(conjunct);
            if (!columns.contains(dependency)) {
                for (const auto& column : columns) {
                    if (!innerNames.contains(column)) {
                        Unsupported(TStringBuilder()
                            << kind << " subplan binding " << binding
                            << " residual predicate references unavailable column "
                            << column);
                    }
                }
                continue;
            }
            if (correlation) {
                Unsupported(TStringBuilder()
                    << kind << " subplan binding " << binding
                    << " references its outer dependency in multiple conjuncts");
            }
            const auto body = conjunct.GetExpressionBody();
            if (!body->IsCallable("==") || body->ChildrenSize() != 2) {
                Unsupported(TStringBuilder()
                    << kind << " subplan binding " << binding
                    << " correlation must be one strict column equality");
            }
            const auto left = DirectMemberName(*body->Child(0));
            const auto right = DirectMemberName(*body->Child(1));
            if (!left || !right ||
                ((*left == dependency) == (*right == dependency)))
            {
                Unsupported(TStringBuilder()
                    << kind << " subplan binding " << binding
                    << " correlation must compare its dependency to one inner column");
            }
            innerColumn = *left == dependency ? *right : *left;
            if (!innerNames.contains(innerColumn) || columns.size() != 2) {
                Unsupported(TStringBuilder()
                    << kind << " subplan binding " << binding
                    << " correlation does not reference exactly one inner column");
            }
            correlation = conjunct;
        }
        if (!correlation) {
            Unsupported(TStringBuilder()
                << kind << " subplan binding " << binding
                << " has no equality for its outer dependency");
        }
        return {
            .Expression = std::move(*correlation),
            .InnerColumn = std::move(innerColumn),
        };
    }

    void ValidateCorrelationTypes(
        TStringBuf kind,
        TStringBuf binding,
        TStringBuf dependency,
        const TTypeAnnotationNode* dependencyType,
        IOperator& innerRoot,
        const TExpression& correlation,
        TStringBuf innerColumn)
    {
        const auto body = correlation.GetExpressionBody();
        const auto leftName = DirectMemberName(*body->Child(0));
        const auto rightName = DirectMemberName(*body->Child(1));
        Y_ENSURE(leftName && rightName);

        const TExprNode* dependencyMember =
            *leftName == dependency
                ? body->Child(0)
                : body->Child(1);
        const TExprNode* innerMember =
            *leftName == dependency
                ? body->Child(1)
                : body->Child(0);

        const auto declaredDependency = ExactType(dependencyType);
        bool memberNullable = false;
        const TExactType dependencyMemberType{
            ScalarTypeName(*dependencyMember, &memberNullable),
            memberNullable,
        };
        if (!SameType(declaredDependency, dependencyMemberType)) {
            Unsupported(TStringBuilder()
                << kind << " subplan binding " << binding
                << " dependency Member type disagrees with AddDependencies");
        }

        const auto innerOutput = ExactType(
            OutputType(innerRoot, innerColumn));
        memberNullable = false;
        const TExactType innerMemberType{
            ScalarTypeName(*innerMember, &memberNullable),
            memberNullable,
        };
        if (!SameType(innerOutput, innerMemberType)) {
            Unsupported(TStringBuilder()
                << kind << " subplan binding " << binding
                << " inner equality Member type disagrees with its input");
        }
        if (declaredDependency.Name != innerOutput.Name) {
            Unsupported(TStringBuilder()
                << kind << " subplan binding " << binding
                << " equality column types do not match");
        }

        bool predicateNullable = false;
        const TString predicateType =
            ScalarTypeName(*body, &predicateNullable);
        if (predicateType != "Bool" ||
            predicateNullable !=
                (declaredDependency.Nullable || innerOutput.Nullable))
        {
            Unsupported(TStringBuilder()
                << kind << " subplan binding " << binding
                << " has an invalid equality result type or nullability");
        }
    }

    TScalarSubplanDetails::TCorrelation PrepareScalarCorrelation(
        const TString& binding,
        const TSubplanEntry& entry,
        const TIntrusivePtr<IOperator>& plan)
    {
        if (entry.DependentIUs.size() != 1) {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << binding
                << " must have exactly one outer dependency");
        }
        const auto& dependencyIU = entry.DependentIUs.front();
        const TString dependency = dependencyIU.GetFullName();
        if (dependency.empty()) {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << binding
                << " has an empty outer dependency");
        }

        THashSet<const IOperator*> nodes;
        THashMap<const IOperator*, TVector<IOperator*>> parents;
        TVector<TOpAddDependencies*> outerBinds;
        VisitOperators(plan, nodes, [&](IOperator& op) {
            if (op.Props.EnsureAtMostOne ||
                op.GetKind() == EOperator::Limit ||
                op.GetKind() == EOperator::Sort ||
                (op.GetKind() == EOperator::UnionAll &&
                 static_cast<const TOpUnionAll&>(op).Ordered))
            {
                Unsupported(TStringBuilder()
                    << "Correlated scalar subplan binding " << binding
                    << " has nondeterministic or error-bearing "
                       "per-invocation semantics");
            }
            CheckSnapshotProperties(op, false);
            for (const auto& child : op.GetChildren()) {
                parents[child.Get()].push_back(&op);
            }
            if (op.GetKind() == EOperator::AddDependencies) {
                outerBinds.push_back(
                    &static_cast<TOpAddDependencies&>(op));
            }
        });
        if (outerBinds.size() != 1) {
            Unsupported(TStringBuilder()
                << "Correlated scalar subplan binding " << binding
                << " must contain exactly one AddDependencies");
        }

        auto* outerBind = outerBinds.front();
        if (outerBind->Dependencies.size() != 1 ||
            outerBind->Types.size() != 1 ||
            !outerBind->Types.front() ||
            outerBind->Dependencies.front() != dependencyIU)
        {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << binding
                << " dependency registry disagrees with AddDependencies");
        }

        auto shape = plan;
        size_t aggregateCount = 0;
        while (shape->GetKind() == EOperator::Map ||
               shape->GetKind() == EOperator::Aggregate)
        {
            if (shape->GetChildren().size() != 1) {
                Unsupported(TStringBuilder()
                    << "Correlated scalar subplan binding " << binding
                    << " root path must be unary");
            }
            if (shape.Get() != plan.Get()) {
                const auto* shapeParents = parents.FindPtr(shape.Get());
                if (!shapeParents || shapeParents->size() != 1) {
                    Unsupported(TStringBuilder()
                        << "Correlated scalar subplan binding " << binding
                        << " root path must not fan out");
                }
            }
            if (shape->GetKind() == EOperator::Aggregate) {
                ++aggregateCount;
                const auto& aggregate =
                    static_cast<const TOpAggregate&>(*shape);
                if (aggregate.IsDistinctAll() ||
                    !aggregate.GetKeyColumns().empty() ||
                    aggregate.GetAggregationPhase() != EOpPhase::Undefined)
                {
                    Unsupported(TStringBuilder()
                        << "Correlated scalar subplan binding " << binding
                        << " Aggregate must be ungrouped, undefined, "
                           "and non-DistinctAll");
                }
            }
            shape = shape->GetChildren().front();
        }
        if (aggregateCount != 1) {
            Unsupported(TStringBuilder()
                << "Correlated scalar subplan binding " << binding
                << " root path must contain exactly one Aggregate "
                   "among Map wrappers");
        }

        const auto* bindParents = parents.FindPtr(outerBind);
        if (!bindParents || bindParents->size() != 1 ||
            bindParents->front()->GetKind() != EOperator::Filter)
        {
            Unsupported(TStringBuilder()
                << "Correlated scalar subplan binding " << binding
                << " must have one Filter directly above AddDependencies");
        }
        auto* filter = static_cast<TOpFilter*>(bindParents->front());
        const auto* filterParents = parents.FindPtr(filter);
        if (shape.Get() != filter ||
            !filterParents ||
            filterParents->size() != 1 ||
            filter->GetInput().Get() != outerBind)
        {
            Unsupported(TStringBuilder()
                << "Correlated scalar subplan binding " << binding
                << " unary path must end in one Filter directly above "
                   "AddDependencies without fanout");
        }

        auto innerPlan = outerBind->GetInput();
        const auto innerNames = OutputNames(*innerPlan);
        if (innerNames.contains(dependency)) {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << binding
                << " outer dependency collides with an inner column");
        }

        const auto correlation = ExtractDirectCorrelation(
            "Scalar",
            binding,
            dependency,
            filter->FilterExpr,
            innerNames);

        THashSet<const IOperator*> expressionNodes;
        VisitOperators(plan, expressionNodes, [&](IOperator& op) {
            if (&op == filter) {
                return;
            }
            for (const auto& expression : op.GetExpressions()) {
                if (ExpressionColumns(expression.get()).contains(dependency)) {
                    Unsupported(TStringBuilder()
                        << "Scalar subplan binding " << binding
                        << " uses its outer dependency outside the "
                           "correlation Filter");
                }
            }
            if (op.GetKind() == EOperator::Aggregate) {
                const auto& candidate =
                    static_cast<const TOpAggregate&>(op);
                for (const auto& key : candidate.KeyColumns) {
                    if (key == dependencyIU) {
                        Unsupported(TStringBuilder()
                            << "Scalar subplan binding " << binding
                            << " aggregates by its outer dependency");
                    }
                }
                for (const auto& trait : candidate.AggregationTraitsList) {
                    if (trait.OriginalColName == dependencyIU) {
                        Unsupported(TStringBuilder()
                            << "Scalar subplan binding " << binding
                            << " aggregates its outer dependency");
                    }
                }
            }
        });

        const auto declaredDependency =
            ExactType(outerBind->Types.front());
        const auto bindOutput =
            ExactType(OutputType(*outerBind, dependency));
        if (!SameType(declaredDependency, bindOutput)) {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << binding
                << " outer_bind output type disagrees with AddDependencies");
        }
        ValidateCorrelationTypes(
            "Scalar",
            binding,
            dependency,
            outerBind->Types.front(),
            *innerPlan,
            correlation.Expression,
            correlation.InnerColumn);

        if (!AuthorizedOuterBinds.insert(outerBind).second) {
            Unsupported(TStringBuilder()
                << "Correlated scalar subplan binding " << binding
                << " shares AddDependencies with another subplan");
        }
        return {
            .Dependency = dependency,
            .DependencyType = outerBind->Types.front(),
        };
    }

    TVector<TString> ReferencedSubplanBindings(IOperator& op) {
        THashSet<TString> referenced;
        for (const auto& iu : op.GetSubplanIUs(Root.PlanProps)) {
            const TString name = iu.GetFullName();
            if (!SubplanIndices.contains(name)) {
                Unsupported(TStringBuilder()
                    << op.GetExplainName()
                    << " references an undeclared subplan binding "
                    << name);
            }
            referenced.insert(name);
        }

        TVector<TString> result;
        result.reserve(referenced.size());
        for (const auto& subplan : Subplans) {
            if (referenced.contains(subplan.Binding)) {
                result.push_back(subplan.Binding);
            }
        }
        return result;
    }

    TSubplanDescriptor PrepareScalarSubplan(
        const TString& binding,
        const TSubplanEntry& entry,
        const TIntrusivePtr<IOperator>& plan)
    {
        if (!entry.Tuple.empty()) {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << binding
                << " has tuple inputs");
        }
        std::optional<TScalarSubplanDetails::TCorrelation> correlation;
        if (entry.DependentIUs.empty()) {
            THashSet<const IOperator*> nodes;
            VisitOperators(plan, nodes, [&](IOperator& op) {
                if (op.GetKind() == EOperator::AddDependencies) {
                    Unsupported(TStringBuilder()
                        << "Scalar subplan binding " << binding
                        << " has residual AddDependencies");
                }
            });
        } else {
            correlation =
                PrepareScalarCorrelation(binding, entry, plan);
        }
        const auto resultIUs = GetSubplanResultIUs(plan);
        if (resultIUs.size() != 1) {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << binding
                << " must have exactly one result column");
        }
        const TString output = resultIUs.front().GetFullName();
        const auto outputNames = OutputNames(*plan);
        if (output.empty() || !outputNames.contains(output)) {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << binding
                << " has an invalid result column " << output);
        }

        bool outputNullable = false;
        const TString type = TypeName(
            OutputType(*plan, output),
            &outputNullable);
        return {
            .Binding = binding,
            .RegistryRoot = plan,
            .ExportedRoot = plan,
            .Details = TScalarSubplanDetails{
                .OutputColumn = output,
                .Type = type,
                .OutputNullable = outputNullable,
                .Correlation = std::move(correlation),
            },
        };
    }

    void ValidatePeeledExistsMap(TOpMap& map, const TString& binding) {
        CheckSnapshotProperties(map, false);
        const auto inputNames = OutputNames(*map.GetInput());
        const auto outputNames = OutputNames(map);
        if (outputNames.empty()) {
            Unsupported(TStringBuilder()
                << "EXISTS subplan binding " << binding
                << " has a peeled Map with no output");
        }
        THashSet<TString> produced;
        for (const auto& element : map.MapElements) {
            if (!element.IsColumnAccess()) {
                Unsupported(TStringBuilder()
                    << "EXISTS subplan binding " << binding
                    << " has a Map wrapper that is not a plain column projection");
            }
            const TString source = element.GetColumnAccess().GetFullName();
            const TString output = element.GetElementName().GetFullName();
            if (source.empty() || !inputNames.contains(source) ||
                output.empty() || !produced.insert(output).second)
            {
                Unsupported(TStringBuilder()
                    << "EXISTS subplan binding " << binding
                    << " has an invalid Map wrapper");
            }
            bool expressionNullable = false;
            const TExactType expressionType{
                ScalarTypeName(
                    *element.GetExpression().GetExpressionBody(),
                    &expressionNullable),
                expressionNullable,
            };
            if (!SameType(
                    ExactType(OutputType(*map.GetInput(), source)),
                    expressionType) ||
                !SameType(
                    ExactType(OutputType(map, output)),
                    expressionType))
            {
                Unsupported(TStringBuilder()
                    << "EXISTS subplan binding " << binding
                    << " has a Map wrapper with inconsistent column types");
            }
            Y_UNUSED(ExportExpr(element.GetExpression(), inputNames));
        }
    }

    void ValidateClosedExistsRoot(
        const TIntrusivePtr<IOperator>& plan,
        const TString& binding,
        bool correlated)
    {
        THashSet<const IOperator*> nodes;
        VisitOperators(plan, nodes, [&](IOperator& op) {
            if (op.GetKind() == EOperator::AddDependencies) {
                Unsupported(TStringBuilder()
                    << "EXISTS subplan binding " << binding
                    << " has residual AddDependencies below its exported root");
            }
            if (op.Props.EnsureAtMostOne) {
                Unsupported(TStringBuilder()
                    << "EXISTS subplan binding " << binding
                    << " has an error-bearing cardinality check");
            }
            if (correlated &&
                (op.GetKind() == EOperator::Limit ||
                 (op.GetKind() == EOperator::Sort &&
                  static_cast<TOpSort&>(op).IsTopSort())))
            {
                Unsupported(TStringBuilder()
                    << "Correlated EXISTS subplan binding " << binding
                    << " has per-invocation row-selection semantics");
            }
        });
    }

    TSubplanDescriptor PrepareExistsSubplan(
        const TString& binding,
        const TSubplanEntry& entry,
        const TIntrusivePtr<IOperator>& originalPlan)
    {
        if (!entry.Tuple.empty()) {
            Unsupported(TStringBuilder()
                << "EXISTS subplan binding " << binding
                << " has tuple inputs");
        }
        if (entry.DependentIUs.empty()) {
            TSubplanDescriptor result{
                .Binding = binding,
                .RegistryRoot = originalPlan,
                .ExportedRoot = originalPlan,
                .Details = TExistsSubplanDetails{},
            };
            ValidateClosedExistsRoot(result.ExportedRoot, binding, false);
            return result;
        }
        if (entry.DependentIUs.size() != 1) {
            Unsupported(TStringBuilder()
                << "EXISTS subplan binding " << binding
                << " must have exactly one outer dependency");
        }
        const TString dependency =
            entry.DependentIUs.front().GetFullName();
        if (dependency.empty()) {
            Unsupported(TStringBuilder()
                << "EXISTS subplan binding " << binding
                << " has an empty outer dependency");
        }

        auto shape = originalPlan;
        while (shape->GetKind() == EOperator::Map) {
            auto map = CastOperator<TOpMap>(shape);
            ValidatePeeledExistsMap(*map, binding);
            shape = map->GetInput();
        }
        if (shape->GetKind() != EOperator::Filter) {
            Unsupported(TStringBuilder()
                << "Correlated EXISTS subplan binding " << binding
                << " must contain one Filter directly above AddDependencies");
        }
        auto filter = CastOperator<TOpFilter>(shape);
        CheckSnapshotProperties(*filter, false);
        if (filter->GetInput()->GetKind() != EOperator::AddDependencies) {
            Unsupported(TStringBuilder()
                << "Correlated EXISTS subplan binding " << binding
                << " Filter is not directly above AddDependencies");
        }
        auto addDependencies =
            CastOperator<TOpAddDependencies>(filter->GetInput());
        CheckSnapshotProperties(*addDependencies, false);
        if (addDependencies->Dependencies.size() != 1 ||
            addDependencies->Types.size() != 1 ||
            !addDependencies->Types.front() ||
            addDependencies->Dependencies.front() !=
                entry.DependentIUs.front())
        {
            Unsupported(TStringBuilder()
                << "EXISTS subplan binding " << binding
                << " dependency registry disagrees with AddDependencies");
        }

        auto innerPlan = addDependencies->GetInput();
        const auto innerNames = OutputNames(*innerPlan);
        if (innerNames.contains(dependency)) {
            Unsupported(TStringBuilder()
                << "EXISTS subplan binding " << binding
                << " outer dependency collides with an inner column");
        }
        auto predicateJson = ExportExpr(
            filter->FilterExpr,
            [&]() {
                auto visible = innerNames;
                visible.insert(dependency);
                return visible;
            }());
        const auto correlation = ExtractDirectCorrelation(
            "EXISTS",
            binding,
            dependency,
            filter->FilterExpr,
            innerNames);

        TSubplanDescriptor result{
            .Binding = binding,
            .RegistryRoot = originalPlan,
            .ExportedRoot = innerPlan,
            .Details = TExistsSubplanDetails{
                .Correlation = TExistsCorrelation{
                    .Predicate = std::move(predicateJson),
                    .Dependency = dependency,
                    .DependencyType = addDependencies->Types.front(),
                },
            },
        };
        ValidateClosedExistsRoot(result.ExportedRoot, binding, true);
        const auto& details = ExistsDetails(result);
        Y_ENSURE(
            details.Correlation,
            "Correlated EXISTS subplan descriptor has no correlation details");
        ValidateCorrelationTypes(
            "EXISTS",
            result.Binding,
            details.Correlation->Dependency,
            details.Correlation->DependencyType,
            *result.ExportedRoot,
            correlation.Expression,
            correlation.InnerColumn);
        return result;
    }

    void ValidateExistsConsumer(const TSubplanDescriptor& subplan) {
        const auto& details = ExistsDetails(subplan);
        if (!details.Correlation) {
            return;
        }
        Y_ENSURE(subplan.Consumers.size() == 1);
        auto* consumer = subplan.Consumers.front();
        const TString& dependency = details.Correlation->Dependency;
        const auto inputNames = OutputNames(*consumer->GetChildren().front());
        if (!inputNames.contains(dependency)) {
            Unsupported(TStringBuilder()
                << "EXISTS subplan binding " << subplan.Binding
                << " dependency is absent from its Filter input");
        }
        const auto outerType = ExactType(
            OutputType(*consumer->GetChildren().front(), dependency));
        const auto declaredType =
            ExactType(details.Correlation->DependencyType);
        if (!SameType(outerType, declaredType)) {
            Unsupported(TStringBuilder()
                << "EXISTS subplan binding " << subplan.Binding
                << " dependency type or nullability disagrees with its consumer input");
        }
    }

    void ValidateScalarConsumer(const TSubplanDescriptor& subplan) {
        const auto& details = ScalarDetails(subplan);
        if (!details.Correlation) {
            return;
        }
        Y_ENSURE(subplan.Consumers.size() == 1);
        auto* consumer = subplan.Consumers.front();
        const TString& dependency = details.Correlation->Dependency;
        auto& input = *consumer->GetChildren().front();
        const auto inputNames = OutputNames(input);
        if (!inputNames.contains(dependency)) {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << subplan.Binding
                << " dependency is absent from its consumer input");
        }
        const auto outerType =
            ExactType(OutputType(input, dependency));
        const auto declaredType =
            ExactType(details.Correlation->DependencyType);
        if (!SameType(outerType, declaredType)) {
            Unsupported(TStringBuilder()
                << "Scalar subplan binding " << subplan.Binding
                << " dependency type or nullability disagrees with its "
                   "consumer input");
        }
    }

    void RegisterSubplans(
        const TVector<TIntrusivePtr<IOperator>>& roots)
    {
        Subplans.reserve(roots.size());
        for (size_t index = 0; index < roots.size(); ++index) {
            const auto& bindingIU = Root.PlanProps.Subplans.OrderedList[index];
            const TString binding = bindingIU.GetFullName();
            const auto entryIt = Root.PlanProps.Subplans.PlanMap.find(bindingIU);
            if (entryIt == Root.PlanProps.Subplans.PlanMap.end()) {
                Unsupported(TStringBuilder()
                    << "Subplan registry order references missing binding "
                    << binding);
            }
            if (!SubplanIndices.emplace(binding, Subplans.size()).second) {
                Unsupported(TStringBuilder()
                    << "Duplicate subplan binding " << binding);
            }

            const auto& entry = entryIt->second;
            switch (entry.Type) {
                case ESubplanType::EXPR:
                    Subplans.push_back(
                        PrepareScalarSubplan(binding, entry, roots[index]));
                    break;
                case ESubplanType::EXISTS:
                    Subplans.push_back(
                        PrepareExistsSubplan(binding, entry, roots[index]));
                    break;
                case ESubplanType::IN_SUBPLAN:
                    Unsupported(TStringBuilder()
                        << "Subplan binding " << binding
                        << " has unsupported IN_SUBPLAN semantics");
                default:
                    Unsupported(TStringBuilder()
                        << "Subplan binding " << binding
                        << " has an unknown subplan type");
            }
        }
    }

    void ValidateSubplanRootTopology() {
        THashSet<const IOperator*> mainReachable;
        VisitOperators(
            Root.GetInput(),
            mainReachable,
            [](IOperator&) {});
        for (const auto& subplan : Subplans) {
            if (mainReachable.contains(subplan.RegistryRoot.Get()) ||
                mainReachable.contains(subplan.ExportedRoot.Get()))
            {
                Unsupported(TStringBuilder()
                    << KindName(subplan)
                    << " subplan root for binding "
                    << subplan.Binding
                    << " is reachable from the main plan");
            }
        }

        TVector<THashSet<const IOperator*>> subplanReachable(Subplans.size());
        for (size_t index = 0; index < Subplans.size(); ++index) {
            VisitOperators(
                Subplans[index].RegistryRoot,
                subplanReachable[index],
                [](IOperator&) {});
        }
        for (size_t outer = 0; outer < Subplans.size(); ++outer) {
            for (size_t inner = 0; inner < Subplans.size(); ++inner) {
                if (outer == inner ||
                    Subplans[outer].RegistryRoot.Get() ==
                        Subplans[inner].RegistryRoot.Get())
                {
                    continue;
                }
                if (subplanReachable[outer].contains(
                        Subplans[inner].RegistryRoot.Get()))
                {
                    Unsupported(TStringBuilder()
                        << KindName(Subplans[inner])
                        << " subplan root for binding "
                        << Subplans[inner].Binding
                        << " is reachable below distinct subplan binding "
                        << Subplans[outer].Binding);
                }
            }
        }

        TVector<THashSet<const IOperator*>> exportedSubplanReachable(
            Subplans.size());
        for (size_t index = 0; index < Subplans.size(); ++index) {
            VisitOperators(
                Subplans[index].ExportedRoot,
                exportedSubplanReachable[index],
                [](IOperator&) {});
        }
        for (size_t outer = 0; outer < Subplans.size(); ++outer) {
            for (size_t inner = 0; inner < Subplans.size(); ++inner) {
                if (outer == inner ||
                    Subplans[outer].ExportedRoot.Get() ==
                        Subplans[inner].ExportedRoot.Get())
                {
                    continue;
                }
                if (exportedSubplanReachable[outer].contains(
                        Subplans[inner].ExportedRoot.Get()))
                {
                    Unsupported(TStringBuilder()
                        << KindName(Subplans[inner])
                        << " exported subplan root for binding "
                        << Subplans[inner].Binding
                        << " is reachable below distinct subplan binding "
                        << Subplans[outer].Binding);
                }
            }
        }
    }

    void ValidateNoNestedSubplanReferences() {
        // Registry roots may contain the one explicit outer dependency, but
        // never another virtual subplan binding.
        for (const auto& subplan : Subplans) {
            THashSet<const IOperator*> subplanNodes;
            VisitOperators(
                subplan.RegistryRoot,
                subplanNodes,
                [&](IOperator& op) {
                    if (!ReferencedSubplanBindings(op).empty()) {
                        Unsupported(TStringBuilder()
                            << KindName(subplan)
                            << " subplan binding " << subplan.Binding
                            << " contains a nested subplan reference");
                    }
                });
        }
    }

    void IndexSubplanConsumers() {
        THashSet<const IOperator*> mainNodes;
        VisitOperators(
            Root.GetInput(),
            mainNodes,
            [&](IOperator& op) {
                const auto outputNames = OutputNames(op);
                for (const auto& subplan : Subplans) {
                    if (outputNames.contains(subplan.Binding)) {
                        const auto kind = SubplanKind(subplan);
                        Unsupported(TStringBuilder()
                            << op.GetExplainName()
                            << " output collides with "
                            << (kind == ESubplanKind::Scalar
                                ? "scalar"
                                : "EXISTS")
                            << " subplan binding " << subplan.Binding);
                    }
                }
                auto bindings = ReferencedSubplanBindings(op);
                if (bindings.empty()) {
                    return;
                }
                if (op.GetChildren().size() != 1) {
                    Unsupported(TStringBuilder()
                        << op.GetExplainName()
                        << " subplan consumer is not unary");
                }
                const auto inputNames = OutputNames(*op.GetChildren().front());
                for (const auto& binding : bindings) {
                    auto& subplan = Subplans[SubplanIndices.at(binding)];
                    const auto kind = SubplanKind(subplan);
                    const bool allowed =
                        kind == ESubplanKind::Scalar
                            ? op.GetKind() == EOperator::Map ||
                                op.GetKind() == EOperator::Filter
                            : op.GetKind() == EOperator::Filter;
                    if (!allowed) {
                        Unsupported(TStringBuilder()
                            << op.GetExplainName() << " cannot consume "
                            << (kind == ESubplanKind::Scalar
                                ? "a scalar"
                                : "an EXISTS")
                            << " subplan binding");
                    }
                    if (inputNames.contains(binding)) {
                        Unsupported(TStringBuilder()
                            << op.GetExplainName()
                            << " subplan binding collides with input column "
                            << binding);
                    }
                    subplan.Consumers.push_back(&op);
                }
                if (!ConsumerBindings.emplace(&op, std::move(bindings)).second) {
                    Unsupported("Subplan consumer was indexed twice");
                }
            });
    }

    void ValidateSubplanConsumerContracts() {
        for (const auto& subplan : Subplans) {
            if (subplan.Consumers.empty()) {
                Unsupported(TStringBuilder()
                    << KindName(subplan)
                    << " subplan binding " << subplan.Binding
                    << " has no consumer");
            }
            if (SubplanKind(subplan) == ESubplanKind::Scalar) {
                const auto& scalar = ScalarDetails(subplan);
                if (scalar.Correlation) {
                    if (subplan.Consumers.size() != 1) {
                        Unsupported(TStringBuilder()
                            << "Correlated scalar subplan binding "
                            << subplan.Binding
                            << " must have exactly one Project or Filter "
                               "consumer");
                    }
                    ValidateScalarConsumer(subplan);
                }
            } else {
                if (subplan.Consumers.size() != 1) {
                    Unsupported(TStringBuilder()
                        << "EXISTS subplan binding " << subplan.Binding
                        << " must have exactly one Filter consumer");
                }
                ValidateExistsConsumer(subplan);
            }
        }
    }

    void PrepareSubplans() {
        const auto roots = OrderedSubplanRoots(Root.PlanProps.Subplans);
        if (!roots.empty() && StageGraphPresent) {
            Unsupported(
                "A staged logical snapshot cannot contain residual subplans");
        }
        if (roots.empty()) {
            return;
        }

        RegisterSubplans(roots);
        ValidateSubplanRootTopology();
        ValidateNoNestedSubplanReferences();
        IndexSubplanConsumers();
        ValidateSubplanConsumerContracts();
    }

    const TVector<TString>& VirtualBindings(const IOperator& op) const {
        static const TVector<TString> Empty;
        const auto* result = ConsumerBindings.FindPtr(&op);
        return result ? *result : Empty;
    }

    THashSet<TString> VisibleInputNames(
        IOperator& consumer,
        IOperator& input) const
    {
        auto result = OutputNames(input);
        for (const auto& binding : VirtualBindings(consumer)) {
            if (!result.insert(binding).second) {
                Unsupported(TStringBuilder()
                    << consumer.GetExplainName()
                    << " subplan binding collides with input column "
                    << binding);
            }
        }
        return result;
    }

    void AuditVirtualBindingMemberTypes(
        const TExpression& expression,
        const IOperator& consumer) const
    {
        const auto& bindings = VirtualBindings(consumer);
        if (bindings.empty()) {
            return;
        }
        THashSet<TString> expected(bindings.begin(), bindings.end());
        THashSet<const TExprNode*> visited;
        std::function<void(const TExprNode&)> visit =
            [&](const TExprNode& node) {
                if (!visited.insert(&node).second) {
                    return;
                }
                if (node.IsCallable("Member") &&
                    node.ChildrenSize() == 2 &&
                    node.Child(1)->IsAtom())
                {
                    const TString name(node.Child(1)->Content());
                    if (expected.contains(name)) {
                        bool nullable = false;
                        const TString type = ScalarTypeName(node, &nullable);
                        const auto& subplan =
                            Subplans[SubplanIndices.at(name)];
                        const auto kind = SubplanKind(subplan);
                        const bool expectedNullable =
                            kind == ESubplanKind::Scalar;
                        if (type != SubplanType(subplan) ||
                            nullable != expectedNullable)
                        {
                            Unsupported(TStringBuilder()
                                << consumer.GetExplainName()
                                << " subplan Member " << name
                                << " must be "
                                << (expectedNullable ? "Optional<" : "")
                                << SubplanType(subplan)
                                << (expectedNullable ? ">" : ""));
                        }
                    }
                }
                for (const auto& child : node.Children()) {
                    visit(*child);
                }
            };
        if (!expression.Node) {
            Unsupported(TStringBuilder()
                << consumer.GetExplainName()
                << " has an empty subplan expression");
        }
        visit(*expression.Node);
    }

    NJson::TJsonValue ExportSubplans() const {
        auto result = JsonArray();
        for (const auto& subplan : Subplans) {
            const auto* rootId = Ids.FindPtr(subplan.ExportedRoot.Get());
            if (!rootId) {
                Unsupported(TStringBuilder()
                    << KindName(subplan)
                    << " subplan root was not exported for binding "
                    << subplan.Binding);
            }

            auto dependencies = JsonArray();
            const auto kind = SubplanKind(subplan);
            if (kind == ESubplanKind::Scalar) {
                const auto& scalar = ScalarDetails(subplan);
                if (scalar.Correlation) {
                    dependencies.AppendValue(
                        scalar.Correlation->Dependency);
                }
            } else {
                const auto& exists = ExistsDetails(subplan);
                if (exists.Correlation) {
                    dependencies.AppendValue(
                        exists.Correlation->Dependency);
                }
            }
            auto consumers = JsonArray();
            for (const auto* consumer : subplan.Consumers) {
                const auto* consumerId = Ids.FindPtr(consumer);
                if (!consumerId) {
                    Unsupported(TStringBuilder()
                        << KindName(subplan)
                        << " subplan consumer was not exported for binding "
                        << subplan.Binding);
                }
                consumers.AppendValue(*consumerId);
            }

            auto descriptor = JsonMap();
            descriptor["binding"] = subplan.Binding;
            descriptor["root"] = *rootId;
            descriptor["type"] = SubplanType(subplan);
            descriptor["dependencies"] = std::move(dependencies);
            descriptor["consumers"] = std::move(consumers);
            if (kind == ESubplanKind::Scalar) {
                const auto& scalar = ScalarDetails(subplan);
                auto output = JsonMap();
                output["column"] = scalar.OutputColumn;
                output["type"] = scalar.Type;
                output["nullable"] = scalar.OutputNullable;
                descriptor["kind"] = "scalar";
                descriptor["output"] = std::move(output);
                descriptor["nullable"] = true;
            } else {
                const auto& exists = ExistsDetails(subplan);
                descriptor["kind"] = "exists";
                descriptor["predicate"] = exists.Correlation
                    ? exists.Correlation->Predicate
                    : NJson::TJsonValue(NJson::JSON_NULL);
                descriptor["nullable"] = false;
            }
            result.AppendValue(std::move(descriptor));
        }
        return result;
    }

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
        if (!StageGraphPresent) {
            CheckSnapshotProperties(*op, false);
        }

        const TString id = TStringBuilder() << "n" << Ids.size();
        auto node = ExportOperator(*op, id, children);
        TrackStoredStringOutputs(*op);
        Ids.emplace(op.Get(), id);
        NodeOrder.push_back(op.Get());
        Nodes.AppendValue(std::move(node));
        return id;
    }

    const TStoredStringColumns& StoredStringOutputs(const IOperator& op) const {
        const auto* result = StoredStringOutputMap.FindPtr(&op);
        if (!result) {
            Unsupported("Stored String provenance is unavailable for an operator input");
        }
        return *result;
    }

    void TrackStoredStringOutputs(IOperator& base) {
        TStoredStringColumns result;

        switch (base.GetKind()) {
            case EOperator::EmptySource:
                break;

            case EOperator::Source: {
                auto& read = static_cast<TOpRead&>(base);
                const auto table = TableReference(read, Cluster);
                const auto* catalogTable = Catalog.FindPtr(table.Identity);
                if (!catalogTable) {
                    Unsupported("Stored String provenance has no catalog table");
                }
                if ((*catalogTable)->MaximumStoredStringCellBytes == 0 ||
                    !TKqpTable(read.TableCallable).SysView().StringValue().empty())
                {
                    break;
                }
                TStoredStringColumns stringColumns;
                for (const auto& column : (*catalogTable)->Columns) {
                    if (column.Type == "String") {
                        stringColumns.emplace(
                            column.Name,
                            TStoredStringProvenance{
                                column.Nullable,
                                (*catalogTable)->MaximumStoredStringCellBytes});
                    }
                }
                // A scan's serialized semantics come from this captured catalog
                // and its source/output mapping.  The initial RBO boundary can
                // legitimately precede operator type annotation; the Concat
                // auditor still requires each Member annotation to match the
                // catalog nullability carried here.
                for (size_t index = 0; index < read.Columns.size(); ++index) {
                    const auto* provenance = stringColumns.FindPtr(read.Columns[index]);
                    const TString output = read.OutputIUs[index].GetFullName();
                    if (provenance) {
                        result.emplace(output, *provenance);
                    }
                }
                break;
            }

            case EOperator::Map: {
                auto& map = static_cast<TOpMap&>(base);
                const auto& input = StoredStringOutputs(*map.GetInput());
                THashSet<TString> renameSources;
                for (const auto& element : map.MapElements) {
                    if (element.IsRename()) {
                        renameSources.insert(element.GetRename().GetFullName());
                    }
                }
                for (const auto& iu : map.GetInput()->GetOutputIUs()) {
                    const TString name = iu.GetFullName();
                    if (!renameSources.contains(name) &&
                        input.contains(name))
                    {
                        result.emplace(name, *input.FindPtr(name));
                    }
                }
                for (const auto& element : map.MapElements) {
                    if (element.IsRename() &&
                        input.contains(element.GetRename().GetFullName()))
                    {
                        result.emplace(
                            element.GetElementName().GetFullName(),
                            *input.FindPtr(element.GetRename().GetFullName()));
                    }
                }
                break;
            }

            case EOperator::Filter:
            case EOperator::Limit:
            case EOperator::Sort:
            case EOperator::AddDependencies: {
                const auto& input = base.GetChildren().front();
                const auto outputNames = OutputNames(base);
                for (const auto& [name, provenance] : StoredStringOutputs(*input)) {
                    if (outputNames.contains(name)) {
                        result.emplace(name, provenance);
                    }
                }
                break;
            }

            case EOperator::Aggregate: {
                auto& aggregate = static_cast<TOpAggregate&>(base);
                if (!aggregate.IsDistinctAll()) {
                    const auto& input = StoredStringOutputs(*aggregate.GetInput());
                    for (const auto& key : aggregate.GetKeyColumns()) {
                        const TString name = key.GetFullName();
                        if (input.contains(name)) {
                            result.emplace(name, *input.FindPtr(name));
                        }
                    }
                }
                break;
            }

            case EOperator::Join: {
                auto& join = static_cast<TOpJoin&>(base);
                const auto& outputNames = OutputNames(base);
                const TString kind = JoinKind(join.JoinKind);
                const bool keepLeft =
                    kind != "right_semi" && kind != "right_anti";
                const bool keepRight =
                    kind != "left_semi" && kind != "left_anti";
                const bool nullableLeft =
                    kind == "right" || kind == "full" || kind == "exclusion";
                const bool nullableRight =
                    kind == "left" || kind == "full" || kind == "exclusion";
                const auto propagate = [&](IOperator& child, bool keep, bool forceNullable) {
                    if (!keep) {
                        return;
                    }
                    for (const auto& [name, provenance] : StoredStringOutputs(child)) {
                        if (outputNames.contains(name)) {
                            result.emplace(
                                name,
                                TStoredStringProvenance{
                                    provenance.Nullable || forceNullable,
                                    provenance.MaximumBytes});
                        }
                    }
                };
                propagate(*join.GetLeftInput(), keepLeft, nullableLeft);
                propagate(*join.GetRightInput(), keepRight, nullableRight);
                break;
            }

            case EOperator::UnionAll: {
                auto& unionAll = static_cast<TOpUnionAll&>(base);
                const auto& left = StoredStringOutputs(*unionAll.GetLeftInput());
                const auto& right = StoredStringOutputs(*unionAll.GetRightInput());
                for (const auto& iu : unionAll.Columns) {
                    const TString name = iu.GetFullName();
                    const auto* leftProvenance = left.FindPtr(name);
                    const auto* rightProvenance = right.FindPtr(name);
                    if (leftProvenance && rightProvenance) {
                        result.emplace(
                            name,
                            TStoredStringProvenance{
                                leftProvenance->Nullable || rightProvenance->Nullable,
                                std::max(
                                    leftProvenance->MaximumBytes,
                                    rightProvenance->MaximumBytes)});
                    }
                }
                break;
            }

            default:
                Unsupported("Stored String provenance reached an unsupported operator");
        }

        if (!StoredStringOutputMap.emplace(&base, std::move(result)).second) {
            Unsupported("Stored String provenance was recorded twice");
        }
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
                if (read.RangeInfo || read.SortDir != ESortDir::None) {
                    Unsupported("Read has range or ordering semantics absent from logical snapshot v1");
                }
                if (read.OlapFilterLambda &&
                    read.StorageType != NYql::EStorageType::ColumnStorage)
                {
                    Unsupported("Read pushed predicate is supported only for column storage");
                }
                if (read.OlapFilterLambda && !StageGraphPresent) {
                    Unsupported("Read pushed predicate requires a StageGraph source boundary");
                }
                if (read.Limit && read.StorageType != NYql::EStorageType::ColumnStorage) {
                    Unsupported("Read pushed limit is supported only for column storage");
                }
                if (read.Limit && !StageGraphPresent) {
                    Unsupported("Read pushed limit requires a StageGraph source boundary");
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
                TOlapColumnMap olapColumns;
                auto columns = JsonArray();
                for (size_t index = 0; index < read.Columns.size(); ++index) {
                    const TString output = read.OutputIUs[index].GetFullName();
                    if (!catalogColumns.contains(read.Columns[index]) ||
                        !sources.insert(read.Columns[index]).second || output.empty() ||
                        !outputs.insert(output).second)
                    {
                        Unsupported(TStringBuilder() << "Invalid Read column mapping for " << table.Path);
                    }
                    if (!olapColumns.emplace(read.Columns[index], output).second) {
                        Unsupported(TStringBuilder()
                            << "Ambiguous OLAP physical column " << read.Columns[index]);
                    }
                    auto column = JsonMap();
                    column["source"] = read.Columns[index];
                    column["output"] = output;
                    columns.AppendValue(std::move(column));
                }
                node["op"] = "scan";
                node["table"] = table.Identity;
                node["columns"] = std::move(columns);
                node["predicate"] = read.OlapFilterLambda
                    ? ExportOlapPredicate(read.OlapFilterLambda, olapColumns)
                    : NJson::TJsonValue(NJson::JSON_NULL);
                node["pushed_limit"] = read.Limit
                    ? Uint64LiteralExpr(*read.Limit, "Read pushed limit")
                    : NJson::TJsonValue(NJson::JSON_NULL);
                return node;
            }

            case EOperator::AddDependencies: {
                if (children.size() != 1) {
                    Unsupported("AddDependencies must have one input");
                }
                auto& outerBind =
                    static_cast<TOpAddDependencies&>(base);
                if (!AuthorizedOuterBinds.contains(&outerBind)) {
                    Unsupported(
                        "AddDependencies is not authorized as a correlated "
                        "scalar outer_bind");
                }
                if (outerBind.Dependencies.size() != 1 ||
                    outerBind.Types.size() != 1 ||
                    !outerBind.Types.front())
                {
                    Unsupported(
                        "A correlated scalar outer_bind must have exactly "
                        "one typed dependency");
                }

                const TString dependency =
                    outerBind.Dependencies.front().GetFullName();
                const auto& inputIUs =
                    outerBind.GetInput()->GetOutputIUs();
                const auto& outputIUs = outerBind.GetOutputIUs();
                if (dependency.empty() ||
                    OutputNames(*outerBind.GetInput()).contains(dependency) ||
                    outputIUs.size() != inputIUs.size() + 1 ||
                    OutputStructType(outerBind)->GetItems().size() !=
                        outputIUs.size())
                {
                    Unsupported(
                        "A correlated scalar outer_bind has an invalid "
                        "dependency or output shape");
                }
                for (size_t index = 0; index < inputIUs.size(); ++index) {
                    const TString input = inputIUs[index].GetFullName();
                    const TString output = outputIUs[index].GetFullName();
                    if (input.empty() || input != output ||
                        !SameType(
                            ExactType(OutputType(
                                *outerBind.GetInput(), input)),
                            ExactType(OutputType(outerBind, output))))
                    {
                        Unsupported(
                            "A correlated scalar outer_bind must preserve "
                            "its input schema exactly and in order");
                    }
                }
                if (outputIUs.back().GetFullName() != dependency ||
                    !SameType(
                        ExactType(outerBind.Types.front()),
                        ExactType(OutputType(outerBind, dependency))))
                {
                    Unsupported(
                        "A correlated scalar outer_bind dependency type or "
                        "output position is inconsistent");
                }

                const auto dependencyType =
                    ExactType(outerBind.Types.front());
                node["op"] = "outer_bind";
                node["input"] = children[0];
                node["dependency"] = dependency;
                node["type"] = dependencyType.Name;
                node["nullable"] = dependencyType.Nullable;
                return node;
            }

            case EOperator::Map: {
                if (children.size() != 1) {
                    Unsupported("Map must have one input");
                }
                auto& map = static_cast<TOpMap&>(base);
                const auto inputNames =
                    VisibleInputNames(map, *map.GetInput());
                THashSet<TString> renameSources;
                for (const auto& element : map.MapElements) {
                    AuditVirtualBindingMemberTypes(
                        element.GetExpression(),
                        map);
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
                        : ExportExpr(
                            element.GetExpression(),
                            inputNames,
                            StoredStringOutputs(*map.GetInput()));
                    columns.AppendValue(std::move(column));
                }
                if (outputs.empty()) {
                    Unsupported("Project with no columns is absent from logical snapshot v1");
                }
                node["op"] = "project";
                node["input"] = children[0];
                node["columns"] = std::move(columns);
                node["ordered"] = map.IsOrdered();
                return node;
            }

            case EOperator::Filter: {
                if (children.size() != 1) {
                    Unsupported("Filter must have one input");
                }
                auto& filter = static_cast<TOpFilter&>(base);
                const auto inputNames =
                    VisibleInputNames(filter, *filter.GetInput());
                AuditVirtualBindingMemberTypes(filter.FilterExpr, filter);
                node["op"] = "filter";
                node["input"] = children[0];
                node["predicate"] = ExportExpr(filter.FilterExpr, inputNames);
                return node;
            }

            case EOperator::Limit: {
                if (children.size() != 1) {
                    Unsupported("Limit must have one input");
                }
                auto& limit = static_cast<TOpLimit&>(base);
                const auto& inputIUs = limit.GetInput()->GetOutputIUs();
                const auto& outputIUs = limit.GetOutputIUs();
                if (inputIUs.size() != outputIUs.size()) {
                    Unsupported("Limit output IUs do not match its input");
                }
                for (size_t index = 0; index < inputIUs.size(); ++index) {
                    if (inputIUs[index].GetFullName() != outputIUs[index].GetFullName()) {
                        Unsupported("Limit output IUs do not match its input");
                    }
                }

                node["op"] = "limit";
                node["input"] = children[0];
                node["count"] = Uint64LiteralExpr(limit.GetLimitCond(), "Limit count");
                const auto offset = limit.GetOffsetCond();
                node["offset"] = offset
                    ? Uint64LiteralExpr(*offset, "Limit offset")
                    : NJson::TJsonValue(NJson::JSON_NULL);
                node["phase"] = Phase(limit.GetLimitPhase());
                node["ensure_at_most_one"] = limit.Props.EnsureAtMostOne;
                return node;
            }

            case EOperator::Sort: {
                if (children.size() != 1) {
                    Unsupported("Sort must have one input");
                }
                auto& sort = static_cast<TOpSort&>(base);
                const auto& inputIUs = sort.GetInput()->GetOutputIUs();
                const auto& outputIUs = sort.GetOutputIUs();
                if (inputIUs.size() != outputIUs.size()) {
                    Unsupported("Sort output IUs do not match its input");
                }
                for (size_t index = 0; index < inputIUs.size(); ++index) {
                    const TString inputName = inputIUs[index].GetFullName();
                    const TString outputName = outputIUs[index].GetFullName();
                    if (inputName != outputName) {
                        Unsupported("Sort output IUs do not match its input");
                    }

                    bool inputNullable = false;
                    bool outputNullable = false;
                    const TString inputType = TypeName(
                        OutputType(*sort.GetInput(), inputName),
                        &inputNullable);
                    const TString outputType = TypeName(
                        OutputType(sort, outputName),
                        &outputNullable);
                    if (inputType != outputType || inputNullable != outputNullable) {
                        Unsupported(TStringBuilder()
                            << "Sort output type disagrees with input IU " << inputName);
                    }
                }

                const auto inputNames = OutputNames(*sort.GetInput());
                const auto outputNames = OutputNames(sort);
                const auto& sortElements = sort.GetSortElements();
                if (sortElements.empty()) {
                    Unsupported("Sort order must not be empty");
                }

                auto order = JsonArray();
                for (const auto& element : sortElements) {
                    const TString column = element.SortColumn.GetFullName();
                    if (column.empty() || !inputNames.contains(column) ||
                        !outputNames.contains(column))
                    {
                        Unsupported(TStringBuilder() << "Invalid Sort key " << column);
                    }
                    const TString type = TypeName(
                        OutputType(*sort.GetInput(), column));
                    if (!IsModeledOrderingType(type)) {
                        Unsupported(TStringBuilder()
                            << "Sort ordering column " << column
                            << " has unsupported type " << type
                            << "; modeled types are integers, Date, String, Utf8, and Decimal");
                    }

                    auto item = JsonMap();
                    item["column"] = column;
                    item["ascending"] = element.Ascending;
                    item["nulls_first"] = element.NullsFirst;
                    order.AppendValue(std::move(item));
                }

                node["op"] = "sort";
                node["input"] = children[0];
                node["order"] = std::move(order);
                node["limit"] = sort.LimitCond
                    ? Uint64LiteralExpr(*sort.LimitCond, "Sort limit")
                    : NJson::TJsonValue(NJson::JSON_NULL);
                node["phase"] = Phase(sort.GetSortPhase());
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
                const auto& keyColumns = aggregate.GetKeyColumns();
                if (traits.empty()) {
                    Unsupported("Aggregate has no traits");
                }
                if (aggregate.IsDistinctAll() &&
                    (keyColumns.empty() || traits.size() != keyColumns.size()))
                {
                    Unsupported(
                        "DistinctAll requires one distinct trait for each ordered key");
                }

                auto keys = JsonArray();
                THashSet<TString> expectedOutputs;
                TVector<TString> expectedOutputOrder;
                THashSet<TString> seenKeys;
                for (const auto& key : keyColumns) {
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
                for (size_t index = 0; index < traits.size(); ++index) {
                    const auto& trait = traits[index];
                    const TString input = trait.OriginalColName.GetFullName();
                    const TString output = trait.ResultColName.GetFullName();
                    if (input.empty() || !inputNames.contains(input) || output.empty() ||
                        !expectedOutputs.insert(output).second || trait.AggFunction.empty())
                    {
                        Unsupported(TStringBuilder() << "Invalid Aggregate trait " << output);
                    }
                    expectedOutputOrder.push_back(output);
                    bool outputNullable = false;
                    const TString outputType = TypeName(
                        OutputType(aggregate, output),
                        &outputNullable);
                    if (aggregate.IsDistinctAll()) {
                        const TString key = keyColumns[index].GetFullName();
                        if (trait.AggFunction != "distinct" ||
                            input != key ||
                            trait.Distinct ||
                            trait.Unwrap)
                        {
                            Unsupported(
                                "DistinctAll traits must be plain distinct "
                                "aliases of their corresponding ordered keys");
                        }
                        bool inputNullable = false;
                        const TString inputType = TypeName(
                            OutputType(*aggregate.GetInput(), input),
                            &inputNullable);
                        if (inputType != outputType ||
                            inputNullable != outputNullable)
                        {
                            Unsupported(
                                "DistinctAll output type and nullability must "
                                "match its input key");
                        }
                    } else if (trait.AggFunction == "distinct") {
                        Unsupported(
                            "The distinct aggregate requires DistinctAll");
                    }
                    auto item = JsonMap();
                    item["input"] = input;
                    item["function"] = trait.AggFunction;
                    item["output"] = output;
                    item["type"] = outputType;
                    item["nullable"] = outputNullable;
                    item["distinct"] = trait.Distinct;
                    item["unwrap"] = trait.Unwrap;
                    if (trait.AggFunction == "avg") {
                        bool inputNullable = false;
                        const TString inputType = TypeName(
                            OutputType(*aggregate.GetInput(), input),
                            &inputNullable);
                        const auto inputDecimal =
                            ParseCanonicalDecimalType(inputType);
                        const auto outputDecimal =
                            ParseCanonicalDecimalType(outputType);
                        if (!inputDecimal || !outputDecimal ||
                            inputType != outputType)
                        {
                            Unsupported(TStringBuilder()
                                << "Aggregate avg requires identical canonical "
                                << "Decimal input and output types, got "
                                << inputType << " and " << outputType);
                        }

                        auto state = JsonMap();
                        state["sum_type"] = TStringBuilder()
                            << "Decimal("
                            << static_cast<ui32>(NYql::NDecimal::MaxPrecision)
                            << "," << static_cast<ui32>(inputDecimal->Scale)
                            << ")";
                        state["count_type"] = "Uint64";
                        state["nullable"] = inputNullable;
                        item["state"] = std::move(state);
                    }
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

                const size_t conjunctCount =
                    join.JoinKeys.size() + join.JoinFilters.size();
                TExactScalarBudget budget;
                const bool combined = conjunctCount > 1;
                if (combined) {
                    budget.Charge(1); // Synthetic conjunction of join conditions.
                }
                const size_t conjunctDepth = combined ? 2 : 1;
                auto conjuncts = JsonArray();
                for (const auto& [left, right] : join.JoinKeys) {
                    const TString leftName = left.GetFullName();
                    const TString rightName = right.GetFullName();
                    if (!leftNames.contains(leftName) || !rightNames.contains(rightName)) {
                        Unsupported(TStringBuilder() << "Join key is absent from its declared input");
                    }
                    budget.Charge(conjunctDepth); // Synthetic equality.
                    budget.Charge(conjunctDepth + 1, 2); // Key columns.
                    auto equality = JsonMap();
                    equality["kind"] = "eq";
                    equality["left"] = ColumnExpr(leftName);
                    equality["right"] = ColumnExpr(rightName);
                    conjuncts.AppendValue(std::move(equality));
                }
                for (const auto& filter : join.JoinFilters) {
                    conjuncts.AppendValue(ExportExprWithBudget(
                        filter,
                        visibleNames,
                        budget,
                        conjunctDepth));
                }

                NJson::TJsonValue predicate;
                if (conjunctCount == 0) {
                    budget.Charge(1);
                    predicate = TrueExpr();
                } else if (conjunctCount == 1) {
                    predicate = std::move(conjuncts[0]);
                } else {
                    predicate = JsonMap();
                    predicate["kind"] = "and";
                    predicate["args"] = std::move(conjuncts);
                }
                AuditExactScalarExpression(predicate);
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
                node["ordered"] = unionAll.Ordered;
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
    THashMap<const IOperator*, TStoredStringColumns> StoredStringOutputMap;
    THashMap<const IOperator*, TString> Ids;
    THashSet<const IOperator*> Visiting;
    THashSet<const TOpAddDependencies*> AuthorizedOuterBinds;
    TVector<IOperator*> NodeOrder;
    TVector<TSubplanDescriptor> Subplans;
    THashMap<TString, size_t> SubplanIndices;
    THashMap<const IOperator*, TVector<TString>> ConsumerBindings;
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
                const TString type = TypeName(
                    OutputType(*boundary.ProducerNode, column));
                if (!IsModeledOrderingType(type)) {
                    Unsupported(TStringBuilder()
                        << "StageGraph Merge ordering column " << column
                        << " has unsupported type " << type
                        << "; modeled types are integers, Date, String, Utf8, and Decimal");
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

    void ValidateTaskSemantics() {
        // Mirror CountComputeTasks with a bounded choice of two tasks for a
        // non-Map HashShuffle stage. Channel-builder constraints are checked
        // against the final count below.
        TaskCounts.assign(StageCount, 0);
        for (const auto stageId : TopologicalStages) {
            if (Graph.SourceStages.contains(stageId)) {
                TaskCounts[stageId] = 2;
                continue;
            }
            if (Graph.StageInputs.at(stageId).empty()) {
                TaskCounts[stageId] = 1;
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
                        taskCount = TaskCounts[producer];
                        forceMapTasks = true;
                        ++mapConnectionCount;
                    } else if (dynamic_cast<const TShuffleConnection*>(connection.Get())) {
                        hasShuffle = true;
                    } else if (const auto* unionAll =
                        dynamic_cast<const TUnionAllConnection*>(connection.Get()))
                    {
                        if (unionAll->IsParallel()) {
                            taskCount = std::max(taskCount, TaskCounts[producer]);
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
                        if (TaskCounts[producer] != taskCount) {
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
            TaskCounts[stageId] = taskCount;
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
    TVector<ui32> TaskCounts;
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
    ValidateSnapshotTopology(root);
    const bool stageGraphPresent = HasStageGraphState(root);
    TPlanExporter planExporter(root, catalog, cluster, stageGraphPresent);

    auto snapshot = JsonMap();
    snapshot["format"] = "ydb-rbo-semantic-snapshot";
    snapshot["version"] = 1;
    snapshot["schema"] = ExportCatalog(catalog);
    snapshot["plan"] = planExporter.Export();
    if (stageGraphPresent) {
        TStageGraphExporter stageGraphExporter(
            root,
            planExporter.GetNodeIds(),
            planExporter.GetNodeOrder(),
            planExporter.GetRootId());
        snapshot["stage_graph"] = stageGraphExporter.Export();
        planExporter.ValidateStageProperties();
    } else {
        snapshot["stage_graph"] = NJson::TJsonValue(NJson::JSON_NULL);
    }

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
        ValidateSnapshotTopology(initialRoot);
        const auto subplanRoots =
            OrderedSubplanRoots(initialRoot.PlanProps.Subplans);

        struct TScannedTable {
            TString Path;
            TSet<TString> Columns;
        };
        TMap<TString, TScannedTable> scanned;
        THashSet<const IOperator*> visited;
        const auto recordRead = [&](IOperator& op) {
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
        };
        VisitOperators(initialRoot.GetInput(), visited, recordRead);
        for (const auto& subplanRoot : subplanRoots) {
            VisitOperators(subplanRoot, visited, recordRead);
        }

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
            if (metadata.Kind == EKikimrTableKind::Datashard) {
                table.MaximumStoredStringCellBytes =
                    MaxDatashardStoredStringBytes;
            } else if (metadata.Kind == EKikimrTableKind::Olap) {
                table.MaximumStoredStringCellBytes =
                    MaxOlapStoredStringBytes;
            }
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
                const TString type = MetadataTypeName(column);
                if (column.SetNotNullInProgress || !IsSupportedType(type)) {
                    Unsupported(TStringBuilder() << "Unsupported metadata for column " << path << "." << name);
                }
                table.Columns.push_back({name, type, !column.NotNull});
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
        {},
    };

    try {
        ValidateSnapshotTopology(root);
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
                if (NeedsInitialSnapshotTypeMaterialization(root)) {
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
        ctx.TransformationDebug.Events,
    };

    try {
        ValidateSnapshotTopology(root);
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

void TSemanticSnapshotPairCaptureV1::CaptureTransformationPrefix(
    TOpRoot& root,
    TRBOContext& ctx,
    const TVector<TRBOTransformationEventV1>& events) noexcept
{
    if (!Sink) {
        return;
    }

    TRBOSemanticSnapshotBoundaryResultV1 result{
        ERBOSemanticSnapshotBoundaryV1::TransformationPrefix,
        {},
        {},
        events,
    };

    try {
        ValidateSnapshotTopology(root);
        bool validEvents = !result.TransformationEvents.empty();
        for (ui64 index = 0; index < result.TransformationEvents.size(); ++index) {
            const auto& event = result.TransformationEvents[index];
            validEvents = validEvents &&
                event.Ordinal == index + 1 &&
                (event.Kind == ERBOTransformationEventKindV1::RuleApplication ||
                    event.Kind == ERBOTransformationEventKindV1::AtomicStageCommit) &&
                !event.Stage.empty() &&
                !event.Name.empty();
        }
        if (!validEvents) {
            result.UnsupportedReason =
                "Transformation-prefix metadata must be non-empty, contiguous, and have a valid kind, stage, and name";
        } else if (!InitialAttempted) {
            result.UnsupportedReason = "Initial semantic snapshot capture was not attempted";
        } else if (!Catalog) {
            result.UnsupportedReason = CatalogFailure.empty()
                ? TString("Initial semantic snapshot catalog is unavailable")
                : CatalogFailure;
        } else {
            // A committed transformation invalidates derived properties. Rebuild the
            // minimum semantic caches only after the optimizer has stopped;
            // this diagnostic path must never perturb later rule matching.
            root.RecomputeOutputIUsSubtree();
            if (root.ComputeTypes(ctx) != IGraphTransformer::TStatus::Ok) {
                Unsupported("RBO type annotation failed for a transformation-prefix snapshot");
            }
            auto snapshot = ExportSemanticSnapshotV1(root, ctx, *Catalog);
            result.Json = std::move(snapshot.Json);
            result.UnsupportedReason = std::move(snapshot.UnsupportedReason);
        }
    } catch (const std::exception& error) {
        result.UnsupportedReason = TStringBuilder()
            << "Transformation-prefix semantic snapshot capture failed closed: "
            << error.what();
    } catch (...) {
        result.UnsupportedReason =
            "Transformation-prefix semantic snapshot capture failed closed with an unknown exception";
    }

    Deliver(std::move(result));
}

std::optional<ui64> TSemanticSnapshotPairCaptureV1::GetTransformationPrefixTarget() const noexcept {
    if (!Sink) {
        return std::nullopt;
    }
    try {
        const auto target = Sink->GetTransformationPrefixTarget();
        return target && *target > 0 ? target : std::nullopt;
    } catch (...) {
        // Instrumentation configuration must not alter normal compilation.
        return std::nullopt;
    }
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
