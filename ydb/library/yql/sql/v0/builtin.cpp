#include "node.h"
#include "context.h"

#include "list_builtin.h"

#include <ydb/library/yql/ast/yql_type_string.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/public/issue/yql_issue_id.h>

#include <library/cpp/charset/ci_string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/util.h>
#include <util/string/join.h>

#include <unordered_map>

using namespace NYql;

namespace NSQLTranslationV0 {

class TGroupingNode final: public TAstListNode {
public:
    TGroupingNode(TPosition pos, const TVector<TNodePtr>& args)
        : TAstListNode(pos)
        , Args(args)
    {}

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!src) {
            ctx.Error(Pos) << "Grouping function should have source";
            return false;
        }
        TVector<TString> columns;
        columns.reserve(Args.size());
        for (const auto& node: Args) {
            auto namePtr = node->GetColumnName();
            if (!namePtr || !*namePtr) {
                ctx.Error(Pos) << "Grouping function should use columns as arguments";
                return false;
            }
            const auto column = *namePtr;
            ISource* composite = src->GetCompositeSource();
            if (!src->IsGroupByColumn(column) && !src->IsAlias(EExprSeat::GroupBy, column) && (!composite || !composite->IsGroupByColumn(column))) {
                ctx.Error(node->GetPos()) << "Column '" << column << "' not used as grouping column";
                return false;
            }
            columns.emplace_back(column);
        }
        ui64 hint;
        if (!src->CalculateGroupingHint(ctx, columns, hint)) {
            return false;
        }
        Nodes.push_back(BuildAtom(Pos, "Uint64"));
        Nodes.push_back(BuildQuotedAtom(Pos, IntToString<10>(hint)));
        return TAstListNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TGroupingNode(Pos, Args);
    }

private:
    const TVector<TNodePtr> Args;
};

class TBasicAggrFunc final: public TAstListNode {
public:
    TBasicAggrFunc(TPosition pos, const TString& name, TAggregationPtr aggr, const TVector<TNodePtr>& args)
        : TAstListNode(pos)
        , Name(name)
        , Aggr(aggr)
        , Args(args)
    {}

    TCiString GetName() const {
        return Name;
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!src) {
            ctx.Error(Pos) << "Unable to use aggregation function '" << Name << "' without data source";
            return false;
        }
        if (!DoInitAggregation(ctx, src)) {
            return false;
        }

        return TAstListNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        TAggregationPtr aggrClone = static_cast<IAggregation*>(Aggr->Clone().Release());
        return new TBasicAggrFunc(Pos, Name, aggrClone, CloneContainer(Args));
    }

    TAggregationPtr GetAggregation() const override {
        return Aggr;
    }

private:
    bool DoInitAggregation(TContext& ctx, ISource* src) {
        if (!Aggr->InitAggr(ctx, false, src, *this, Args)) {
            return false;
        }
        return src->AddAggregation(ctx, Aggr);
    }

    void DoUpdateState() const final {
        State.Set(ENodeState::Const, Args.front()->IsConstant());
        State.Set(ENodeState::Aggregated);
    }

protected:
    const TString Name;
    TAggregationPtr Aggr;
    TVector<TNodePtr> Args;
};

class TBasicAggrFactory final : public TAstListNode {
public:
    TBasicAggrFactory(TPosition pos, const TString& name, TAggregationPtr aggr, const TVector<TNodePtr>& args)
        : TAstListNode(pos)
        , Name(name)
        , Aggr(aggr)
        , Args(args)
    {}

    TCiString GetName() const {
        return Name;
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!DoInitAggregation(ctx)) {
            return false;
        }

        auto factory = Aggr->AggregationTraitsFactory();
        auto apply = Y("Apply", factory, Y("ListType", "type"));

        auto columnIndices = Aggr->GetFactoryColumnIndices();
        if (columnIndices.size() == 1) {
            apply = L(apply, "extractor");
        } else {
            // make several extractors from main that returns a tuple
            for (ui32 arg = 0; arg < columnIndices.size(); ++arg) {
                auto partial = BuildLambda(Pos, Y("row"), Y("Nth", Y("Apply", "extractor", "row"), Q(ToString(columnIndices[arg]))));
                apply = L(apply, partial);
            }
        }

        Aggr->AddFactoryArguments(apply);
        Lambda = BuildLambda(Pos, Y("type", "extractor"), apply);
        return TAstListNode::DoInit(ctx, src);
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Lambda->Translate(ctx);
    }

    TNodePtr DoClone() const final {
        TAggregationPtr aggrClone = static_cast<IAggregation*>(Aggr->Clone().Release());
        return new TBasicAggrFactory(Pos, Name, aggrClone, CloneContainer(Args));
    }

    TAggregationPtr GetAggregation() const override {
        return Aggr;
    }

private:
    bool DoInitAggregation(TContext& ctx) {
        return Aggr->InitAggr(ctx, true, nullptr, *this, Args);
    }

protected:
    const TString Name;
    TAggregationPtr Aggr;
    TVector<TNodePtr> Args;
    TNodePtr Lambda;
};

typedef THolder<TBasicAggrFunc> TAggrFuncPtr;

class TLiteralStringAtom: public INode {
public:
    TLiteralStringAtom(TPosition pos, TNodePtr node, const TString& info)
        : INode(pos)
        , Node(node)
        , Info(info)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        if (!Node) {
            ctx.Error(Pos) << Info;
            return false;
        }

        if (!Node->Init(ctx, src)) {
            return false;
        }

        Atom = MakeAtomFromExpression(ctx, Node).Build();
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Atom->Translate(ctx);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TNodePtr Node;
    TNodePtr Atom;
    TString Info;
};

class TYqlAsAtom: public TLiteralStringAtom {
public:
    TYqlAsAtom(TPosition pos, const TVector<TNodePtr>& args)
        : TLiteralStringAtom(pos, args.size() == 1 ? args[0] : nullptr, "Literal string is required as argument")
    {
    }
};

class TYqlData: public TCallNode {
public:
    TYqlData(TPosition pos, const TString& type, const TVector<TNodePtr>& args)
        : TCallNode(pos, type, 1, 1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto slot = NUdf::FindDataSlot(GetOpName());
        if (!slot) {
            ctx.Error(Pos) << "Unexpected type " << GetOpName();
            return false;
        }

        if (*slot == NUdf::EDataSlot::Decimal) {
            MinArgs = MaxArgs = 3;
        }

        if (!ValidateArguments(ctx)) {
            return false;
        }

        auto stringNode = Args[0];
        auto atom = stringNode->GetLiteral("String");
        if (!atom) {
            ctx.Error(Pos) << "Expected literal string as argument in " << GetOpName() << " function";
            return false;
        }

        TString value;
        if (*slot == NUdf::EDataSlot::Decimal) {
            const auto precision = Args[1]->GetLiteral("Int32");
            const auto scale = Args[2]->GetLiteral("Int32");

            if (!NKikimr::NMiniKQL::IsValidDecimal(*atom)) {
                ctx.Error(Pos) << "Invalid value " << atom->Quote() << " for type " << GetOpName();
                return false;
            }

            ui8 stub;
            if (!(precision && TryFromString<ui8>(*precision, stub))) {
                ctx.Error(Pos) << "Invalid precision " << (precision ? precision->Quote() : "") << " for type " << GetOpName();
                return false;
            }

            if (!(scale && TryFromString<ui8>(*scale, stub))) {
                ctx.Error(Pos) << "Invalid scale " << (scale ? scale->Quote() : "") << " for type " << GetOpName();
                return false;
            }

            Args[0] = BuildQuotedAtom(GetPos(), *atom);
            Args[1] = BuildQuotedAtom(GetPos(), *precision);
            Args[2] = BuildQuotedAtom(GetPos(), *scale);
            return TCallNode::DoInit(ctx, src);
        } else if (NUdf::GetDataTypeInfo(*slot).Features & (NUdf::DateType | NUdf::TzDateType | NUdf::TimeIntervalType)) {
            const auto out = NKikimr::NMiniKQL::ValueFromString(*slot, *atom);
            if (!out) {
                ctx.Error(Pos) << "Invalid value " << atom->Quote() << " for type " << GetOpName();
                return false;
            }

            switch (*slot) {
            case NUdf::EDataSlot::Date:
            case NUdf::EDataSlot::TzDate:
                value = ToString(out.Get<ui16>());
                break;
            case NUdf::EDataSlot::Datetime:
            case NUdf::EDataSlot::TzDatetime:
                value = ToString(out.Get<ui32>());
                break;
            case NUdf::EDataSlot::Timestamp:
            case NUdf::EDataSlot::TzTimestamp:
                value = ToString(out.Get<ui64>());
                break;
            case NUdf::EDataSlot::Interval:
                value = ToString(out.Get<i64>());
                if ('T' == atom->back()) {
                    ctx.Warning(Pos, TIssuesIds::YQL_DEPRECATED_INTERVAL_CONSTANT) << "Time prefix 'T' at end of interval constant";
                }
                break;
            default:
                Y_ABORT("Unexpected data slot");
            }

            if (NUdf::GetDataTypeInfo(*slot).Features & NUdf::TzDateType) {
                value += ",";
                value += NKikimr::NMiniKQL::GetTimezoneIANAName(out.GetTimezoneId());
            }
        } else if (NUdf::EDataSlot::Uuid == *slot) {
            char out[0x10];
            if (!NKikimr::NMiniKQL::ParseUuid(*atom, out)) {
                ctx.Error(Pos) << "Invalid value " << atom->Quote() << " for type " << GetOpName();
                return false;
            }

            value.assign(out, sizeof(out));
        } else {
            if (!NKikimr::NMiniKQL::IsValidStringValue(*slot, *atom)) {
                ctx.Error(Pos) << "Invalid value " << atom->Quote() << " for type " << GetOpName();
                return false;
            }

            value = *atom;
        }

        Args[0] = BuildQuotedAtom(GetPos(), value);
        return TCallNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TYqlData(GetPos(), OpName, CloneContainer(Args));
    }
};

class TTableName : public TCallNode {
public:
    TTableName(TPosition pos, const TVector<TNodePtr>& args, const TString& cluster)
        : TCallNode(pos, "TableName", 0, 2, args)
        , Cluster(cluster)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (Args.empty()) {
            Args.push_back(Y("TablePath", Y("DependsOn", "row")));
        }

        if (Args.size() == 2) {
            auto literal = Args[1]->GetLiteral("String");
            if (!literal) {
                ctx.Error(Args[1]->GetPos()) << "Expected literal string as second argument in TableName function";
                return false;
            }

            Args[1] = BuildQuotedAtom(Args[1]->GetPos(), to_lower(*literal));
        } else {
            if (Cluster.empty()) {
                ctx.Error(GetPos()) << GetOpName() << " requires either one of \"yt\"/\"kikimr\"/\"rtmr\" as second argument or current cluster name";
                return false;
            }

            auto service = ctx.GetClusterProvider(Cluster);
            if (!service) {
                ctx.Error() << "Unknown cluster name: " << Cluster;
                return false;
            }

            Args.push_back(BuildQuotedAtom(GetPos(), to_lower(*service)));
        }

        return TCallNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TTableName(GetPos(), CloneContainer(Args), Cluster);
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, false);
    }

private:
    TString Cluster;
};

class TYqlParseType final : public INode {
public:
    TYqlParseType(TPosition pos, const TVector<TNodePtr>& args)
        : INode(pos)
        , Args(args)
    {}

    TAstNode* Translate(TContext& ctx) const override {
        if (Args.size() != 1) {
            ctx.Error(Pos) << "Expected 1 argument in ParseType function";
            return nullptr;
        }

        auto literal = Args[0]->GetLiteral("String");
        if (!literal) {
            ctx.Error(Args[0]->GetPos()) << "Expected literal string as argument in ParseType function";
            return nullptr;
        }

        auto parsed = ParseType(*literal, *ctx.Pool, ctx.Issues, Args[0]->GetPos());
        if (!parsed) {
            ctx.Error(Args[0]->GetPos()) << "Failed to parse type";
            return nullptr;
        }

        return parsed;
    }

    TNodePtr DoClone() const final {
        return new TYqlParseType(Pos, Args);
    }

private:
    TVector<TNodePtr> Args;
};

class TYqlAddTimezone: public TCallNode {
public:
    TYqlAddTimezone(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "AddTimezone", 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        Args[1] = Y("TimezoneId", Args[1]);
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlAddTimezone(Pos, CloneContainer(Args));
    }
};

template <bool Strict>
class TYqlTypeAssert : public TCallNode {
public:
    TYqlTypeAssert(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, Strict ? "EnsureType" : "EnsureConvertibleTo", 2, 3, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        auto literal = Args[1]->GetLiteral("String");
        INode::TPtr type;
        if (literal) {
            auto parsed = ParseType(*literal, *ctx.Pool, ctx.Issues, Args[0]->GetPos());
            if (!parsed) {
                ctx.Error(Args[1]->GetPos()) << "Failed to parse type";
                return false;
            }

            type = AstNode(parsed);
        } else {
            type = Args[1];
        }

        if (!type->Init(ctx, src)) {
            return false;
        }

        Args[1] = type;
        if (Args.size() == 3) {
            if (!Args[2]->Init(ctx, src)) {
                return false;
            }

            auto message = MakeAtomFromExpression(ctx, Args[2]).Build();
            Args[2] = message;
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTypeAssert<Strict>(Pos, Args);
    }
};

class TFromBytes final : public TCallNode {
public:
    TFromBytes(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "FromBytes", 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[1]->Init(ctx, src)) {
            return false;
        }

        Args[1] = MakeAtomFromExpression(ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TFromBytes(Pos, Args);
    }
};

class TYqlTaggedBase : public TCallNode {
public:
    TYqlTaggedBase(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[1]->Init(ctx, src)) {
            return false;
        }

        Args[1] = MakeAtomFromExpression(ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }
};

class TYqlAsTagged final : public TYqlTaggedBase {
public:
    TYqlAsTagged(TPosition pos, const TVector<TNodePtr>& args)
        : TYqlTaggedBase(pos, "AsTagged", args)
    {}

    TNodePtr DoClone() const final {
        return new TYqlAsTagged(Pos, Args);
    }
};

class TYqlUntag final : public TYqlTaggedBase {
public:
    TYqlUntag(TPosition pos, const TVector<TNodePtr>& args)
        : TYqlTaggedBase(pos, "Untag", args)
    {}

    TNodePtr DoClone() const final {
        return new TYqlUntag(Pos, Args);
    }
};

class TYqlVariant final : public TCallNode {
public:
    TYqlVariant(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "Variant", 3, 3, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[1]->Init(ctx, src)) {
            return false;
        }

        Args[1] = MakeAtomFromExpression(ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlVariant(Pos, Args);
    }
};

TNodePtr BuildFileNameArgument(TPosition pos, const TNodePtr& argument) {
    return new TLiteralStringAtom(pos, argument, "FilePath requires string literal as parameter");
}

class TYqlAtom final: public TCallNode {
public:
    TYqlAtom(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 1, 1, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Args.empty()) {
            Args[0] = BuildFileNameArgument(ctx.Pos(), Args[0]);
        }
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlAtom(Pos, OpName, Args);
    }
};

class TAddMember final: public TCallNode {
public:
    TAddMember(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 3, 3, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args.size() != 3) {
            ctx.Error(Pos) << OpName << " requires exactly three arguments";
            return false;
        }
        for (const auto& arg : Args) {
            if (!arg->Init(ctx, src)) {
                return false;
            }
        }
        Args[1] = MakeAtomFromExpression(ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TAddMember(Pos, OpName, Args);
    }
};
class TRemoveMember final: public TCallNode {
public:
    TRemoveMember(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args.size() != 2) {
            ctx.Error(Pos) << OpName << " requires exactly two arguments";
            return false;
        }
        for (const auto& arg : Args) {
            if (!arg->Init(ctx, src)) {
                return false;
            }
        }
        Args[1] = MakeAtomFromExpression(ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TRemoveMember(Pos, OpName, Args);
    }
};

class TCombineMembers final: public TCallNode {
public:
    TCombineMembers(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 1, -1, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args.empty()) {
            ctx.Error(Pos) << "CombineMembers requires at least one argument";
            return false;
        }
        for (size_t i = 0; i < Args.size(); ++i) {
            Args[i] = Q(Y(Q(""), Args[i])); // flatten without prefix
        }
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TCombineMembers(Pos, OpName, Args);
    }
};

class TFlattenMembers final: public TCallNode {
public:
    TFlattenMembers(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 1, -1, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args.empty()) {
            ctx.Error(Pos) << OpName << " requires at least one argument";
            return false;
        }
        for (size_t i = 0; i < Args.size(); ++i) {
            if (!Args[i]->Init(ctx, src)) {
                return false;
            }
            if (Args[i]->GetTupleSize() == 2) {
                // flatten with prefix
                Args[i] = Q(Y(
                    MakeAtomFromExpression(ctx, Args[i]->GetTupleElement(0)).Build(),
                    Args[i]->GetTupleElement(1)
                ));
            } else {
                ctx.Error(Pos) << OpName << " requires arguments to be tuples of size 2: prefix and struct";
                return false;
            }
        }
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TFlattenMembers(Pos, OpName, Args);
    }
};

TString NormalizeTypeString(const TString& str) {
    auto ret = to_title(str);
    if (ret.StartsWith("Tz")) {
        ret = "Tz" + to_title(ret.substr(2));
    }

    return ret;
}

static const TSet<TString> AvailableDataTypes = {"Bool", "String", "Uint32", "Uint64", "Int32", "Int64", "Float", "Double", "Utf8", "Yson", "Json",
    "Date", "Datetime", "Timestamp", "Interval", "Uint8", "Int8", "Uint16", "Int16", "TzDate", "TzDatetime", "TzTimestamp", "Uuid", "Decimal"};
TNodePtr GetDataTypeStringNode(TContext& ctx, TCallNode& node, unsigned argNum, TString* outTypeStrPtr = nullptr) {
    auto errMsgFunc = [&node, argNum]() {
        static std::array<TString, 2> numToName = {{"first", "second"}};
        TStringBuilder sb;
        sb << "At " << numToName.at(argNum) << " argument of " << node.GetOpName() << " expected type string, available one of: "
            << JoinRange(", ", AvailableDataTypes.begin(), AvailableDataTypes.end()) << ";";
        return TString(sb);
    };
    auto typeStringNode = node.GetArgs().at(argNum);
    auto typeStringPtr = typeStringNode->GetLiteral("String");
    TNodePtr dataTypeNode;
    if (typeStringPtr) {
        TString typeString = NormalizeTypeString(*typeStringPtr);
        if (!AvailableDataTypes.contains(typeString)) {
            ctx.Error(typeStringNode->GetPos()) << "Bad type string: '" << typeString << "'. " << errMsgFunc();
            return {};
        }
        if (outTypeStrPtr) {
            *outTypeStrPtr = typeString;
        }
        dataTypeNode = typeStringNode->Q(typeString);
    } else {
        ctx.Error(typeStringNode->GetPos()) << errMsgFunc();
        return {};
    }
    return dataTypeNode;
}

class TYqlParseFileOp final: public TCallNode {
public:
    TYqlParseFileOp(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "ParseFile", 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        auto dataTypeStringNode = GetDataTypeStringNode(ctx, *this, 0);
        if (!dataTypeStringNode) {
            return false;
        }
        auto aliasNode = BuildFileNameArgument(Args[1]->GetPos(), Args[1]);
        OpName = "Apply";
        Args[0] = Y("Udf", Q("File.ByLines"), Y("Void"),
            Y("TupleType",
                Y("TupleType", Y("DataType", dataTypeStringNode)),
                Y("StructType"),
                Y("TupleType")));

        Args[1] = Y("FilePath", aliasNode);
        return TCallNode::DoInit(ctx, src);
    }

    TString GetOpName() const override {
        return "ParseFile";
    }

    TNodePtr DoClone() const final {
        return new TYqlParseFileOp(Pos, Args);
    }
};

class TYqlDataType final : public TCallNode {
public:
    TYqlDataType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "DataType", 1, 1, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        auto dataTypeStringNode = GetDataTypeStringNode(ctx, *this, 0);
        if (!dataTypeStringNode) {
            return false;
        }
        Args[0] = dataTypeStringNode;
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlDataType(Pos, Args);
    }
};

TNodePtr TryBuildDataType(TPosition pos, const TString& stringType) {
    auto normStringType = NormalizeTypeString(stringType);
    if (!AvailableDataTypes.contains(normStringType)) {
        return {};
    }
    return new TYqlDataType(pos, {BuildLiteralRawString(pos, normStringType)});
}

class TYqlResourceType final : public TCallNode {
public:
    TYqlResourceType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "ResourceType", 1, 1, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[0]->Init(ctx, src)) {
            return false;
        }

        Args[0] = MakeAtomFromExpression(ctx, Args[0]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlResourceType(Pos, Args);
    }
};

class TYqlTaggedType final : public TCallNode {
public:
    TYqlTaggedType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "TaggedType", 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[1]->Init(ctx, src)) {
            return false;
        }

        Args[1] = MakeAtomFromExpression(ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTaggedType(Pos, Args);
    }
};

class TYqlCallableType final : public TCallNode {
public:
    TYqlCallableType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "CallableType", 2, -1, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!dynamic_cast<TTupleNode*>(Args[0].Get())) {
            ui32 numOptArgs;
            if (!Parseui32(Args[0], numOptArgs)) {
                ctx.Error(Args[0]->GetPos()) << "Expected either tuple or number of optional arguments";
                return false;
            }

            Args[0] = Q(Y(BuildQuotedAtom(Args[0]->GetPos(), ToString(numOptArgs))));
        }

        if (!dynamic_cast<TTupleNode*>(Args[1].Get())) {
            Args[1] = Q(Y(Args[1]));
        }

        for (ui32 index = 2; index < Args.size(); ++index) {
            if (!dynamic_cast<TTupleNode*>(Args[index].Get())) {
                Args[index] = Q(Y(Args[index]));
            }
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlCallableType(Pos, Args);
    }
};

class TYqlTupleElementType final : public TCallNode {
public:
    TYqlTupleElementType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "TupleElementType", 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[1]->Init(ctx, src)) {
            return false;
        }

        Args[1] = MakeAtomFromExpression(ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTupleElementType(Pos, Args);
    }
};

class TYqlStructMemberType final : public TCallNode {
public:
    TYqlStructMemberType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "StructMemberType", 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[1]->Init(ctx, src)) {
            return false;
        }

        Args[1] = MakeAtomFromExpression(ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlStructMemberType(Pos, Args);
    }
};

class TYqlCallableArgumentType final : public TCallNode {
public:
    TYqlCallableArgumentType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "CallableArgumentType", 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        ui32 index;
        if (!Parseui32(Args[1], index)) {
            ctx.Error(Args[1]->GetPos()) << "Expected index of the callable argument";
            return false;
        }

        Args[1] = BuildQuotedAtom(Args[1]->GetPos(), ToString(index));
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlCallableArgumentType(Pos, Args);
    }
};

class TStructTypeNode : public TAstListNode {
public:
    TStructTypeNode(TPosition pos, const TVector<TNodePtr>& exprs)
        : TAstListNode(pos)
        , Exprs(exprs)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        Nodes.push_back(BuildAtom(Pos, "StructType", TNodeFlags::Default));
        for (const auto& expr : Exprs) {
            const auto& label = expr->GetLabel();
            if (!label) {
                ctx.Error(expr->GetPos()) << "Structure does not allow anonymous members";
                return false;
            }
            Nodes.push_back(Q(Y(Q(label), expr)));
        }
        return TAstListNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TStructTypeNode(Pos, CloneContainer(Exprs));
    }

private:
    const TVector<TNodePtr> Exprs;
};

template <bool IsStrict>
class TYqlIf final: public TCallNode {
public:
    TYqlIf(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, IsStrict ? "IfStrict" : "If", 2, 3, args)
    {}

private:
    TCallNode::TPtr DoClone() const override {
       return new TYqlIf(GetPos(), CloneContainer(Args));
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        Args[0] = Y("Coalesce", Args[0], Y("Bool", Q("false")));
        if (Args.size() == 2) {
            Args.push_back(Y("Null"));
        }
        return TCallNode::DoInit(ctx, src);
    }
};

class TYqlSubstring final: public TCallNode {
public:
    TYqlSubstring(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "Substring", 2, 3, args)
    {}

private:
    TCallNode::TPtr DoClone() const override {
       return new TYqlSubstring(GetPos(), CloneContainer(Args));
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args.size() == 2) {
            Args.push_back(Y("Uint32", Q(ToString(Max<ui32>()))));
        }
        return TCallNode::DoInit(ctx, src);
    }
};

class TYqlIn final: public TCallNode {
public:
    TYqlIn(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "IN", 3, 3, args)
    {}

private:
    TNodePtr DoClone() const final {
        return new TYqlIn(Pos, CloneContainer(Args));
    }
    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        auto key = Args[0];
        auto inNode = Args[1];
        auto hints = Args[2];

        if (!key->Init(ctx, src)) {
            return false;
        }

        if (!inNode->Init(ctx, inNode->GetSource() ? nullptr : src)) {
            return false;
        }

        if (inNode->GetLiteral("String")) {
            ctx.Error(inNode->GetPos()) << "Unable to use IN predicate with string argument, it won't search substring - "
                                           "expecting tuple, list, dict or single column table source";
            return false;
        }

        if (inNode->GetTupleSize() == 1) {
            auto singleElement = inNode->GetTupleElement(0);
            // TODO: 'IN ((select ...))' is parsed exactly like 'IN (select ...)' instead of a single element tuple
            if (singleElement->GetSource() || singleElement->IsSelect()) {
                TStringBuf parenKind = singleElement->GetSource() ? "" : "external ";
                ctx.Warning(inNode->GetPos(),
                            TIssuesIds::YQL_CONST_SUBREQUEST_IN_LIST) << "Using subrequest in scalar context after IN, "
                                                                      << "perhaps you should remove "
                                                                      << parenKind << "parenthesis here";
            }
        }

        if (inNode->GetSource() || inNode->IsSelect()) {
            TVector<TNodePtr> hintElements;
            for (size_t i = 0; i < hints->GetTupleSize(); ++i) {
                hintElements.push_back(hints->GetTupleElement(i));
            }
            auto pos = inNode->GetPos();
            auto tableSourceHint = BuildTuple(pos, { BuildQuotedAtom(pos, "tableSource", NYql::TNodeFlags::Default) });
            hintElements.push_back(tableSourceHint);
            hints = BuildTuple(pos, hintElements);
        }

        OpName = "SqlIn";
        MinArgs = MaxArgs = 3;
        Args = {
            inNode->GetSource() ? inNode->GetSource() : inNode,
            key,
            hints
        };

        return TCallNode::DoInit(ctx, src);
    }

    TString GetOpName() const override {
        return "IN predicate";
    }
};

class TYqlUdfBase : public TCallNode {
public:
    TYqlUdfBase(TPosition pos, const TString& name)
        : TCallNode(pos, "Udf", 1, 1, UdfArgs(pos, name))
    {}

    TYqlUdfBase(TPosition pos, const TString& name, const TVector<TNodePtr>& args, ui32 argsCount = 2)
        : TCallNode(pos, "Udf", argsCount, argsCount, UdfArgs(pos, name, &args))
    {}

protected:
    TYqlUdfBase(TPosition pos, const TString& opName, ui32 minArgs, ui32 maxArgs, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, minArgs, maxArgs, args)
    {}

private:
    static TVector<TNodePtr> UdfArgs(TPosition pos, const TString& name, const TVector<TNodePtr>* args = nullptr) {
        TVector<TNodePtr> res = { BuildQuotedAtom(pos, name) };
        if (args) {
            res.insert(res.end(), args->begin(), args->end());
        }
        return res;
    }

    void DoUpdateState() const override {
        TCallNode::DoUpdateState();
        State.Set(ENodeState::Aggregated, false/*!RunConfig || RunConfig->IsAggregated()*/);
        State.Set(ENodeState::Const, true /* FIXME: To avoid CheckAggregationLevel issue for non-const TypeOf. */);
    }

private:
    TNodePtr RunConfig;
};

class TYqlUdf final : public TYqlUdfBase {
public:
    TYqlUdf(TPosition pos, const TString& name)
        : TYqlUdfBase(pos, name)
    {}

    TYqlUdf(TPosition pos, const TString& name, const TVector<TNodePtr>& args, ui32 argsCount = 2)
        : TYqlUdfBase(pos, name, args, argsCount)
    {}

private:
    TYqlUdf(const TYqlUdf& other)
        : TYqlUdfBase(other.GetPos(), "Udf", other.MinArgs, other.MaxArgs, CloneContainer(other.Args))
    {}

    TNodePtr DoClone() const final {
        return new TYqlUdf(*this);
    }
};

class TYqlTypeConfigUdf final : public TYqlUdfBase {
public:
    TYqlTypeConfigUdf(TPosition pos, const TString& name)
        : TYqlUdfBase(pos, name)
    {}

    TYqlTypeConfigUdf(TPosition pos, const TString& name, const TVector<TNodePtr>& args, ui32 argsCount = 2)
        : TYqlUdfBase(pos, name, args, argsCount)
    {}

private:
    TYqlTypeConfigUdf(const TYqlTypeConfigUdf& other)
        : TYqlUdfBase(other.GetPos(), "Udf", other.MinArgs, other.MaxArgs, CloneContainer(other.Args))
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[3]->Init(ctx, src)) {
            return false;
        }

        Args[3] = MakeAtomFromExpression(ctx, Args[3]).Build();
        return TYqlUdfBase::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTypeConfigUdf(*this);
    }
};

class TWeakFieldOp final: public TCallNode {
public:
    TWeakFieldOp(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "WeakField", 2, 3, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!src) {
            ctx.Error(Pos) << GetCallExplain() << " unable use without source";
            return false;
        }

        src->AllColumns();

        if (!ValidateArguments(ctx)) {
            return false;
        }
        PrecacheState();

        const auto memberPos = Args[0]->GetPos();
        TVector<TNodePtr> repackArgs = {BuildAtom(memberPos, "row", NYql::TNodeFlags::Default)};
        if (auto literal = Args[1]->GetLiteral("String")) {
            TString targetType;
            if (!GetDataTypeStringNode(ctx, *this, 1, &targetType)) {
                return false;
            }

            repackArgs.push_back(Args[1]->Q(targetType));
        } else {
            repackArgs.push_back(Args[1]);
        }

        TVector<TNodePtr> column;
        auto namePtr = Args[0]->GetColumnName();
        if (!namePtr || !*namePtr) {
            ctx.Error(Pos) << GetCallExplain() << " expect as first argument column name";
            return false;
        }
        auto memberName = *namePtr;
        column.push_back(Args[0]->Q(*namePtr));

        if (src->GetJoin() && !src->IsJoinKeysInitializing()) {
            const auto sourcePtr = Args[0]->GetSourceName();
            if (!sourcePtr || !*sourcePtr) {
                ctx.Error(Pos) << GetOpName() << " required to have correlation name in case of JOIN for column at first parameter";
                return false;
            }
            column.push_back(Args[0]->Q(*sourcePtr));
            memberName = DotJoin(*sourcePtr, memberName);
        }
        if (!GetLabel()) {
            SetLabel(memberName);
        }
        repackArgs.push_back(BuildTuple(memberPos, column));
        if (Args.size() == 3) {
            repackArgs.push_back(Args[2]);
        }
        ++MinArgs;
        ++MaxArgs;
        Args.swap(repackArgs);

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TWeakFieldOp(Pos, Args);
    }
};

class TTableRow final : public TAstAtomNode {
public:
    TTableRow(TPosition pos, const TVector<TNodePtr>& args)
        : TTableRow(pos, args.size())
    {}

    TTableRow(TPosition pos, ui32 argsCount)
        : TAstAtomNode(pos, "row", 0)
        , ArgsCount(argsCount)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!src || src->IsFake()) {
            ctx.Error(Pos) << "TableRow requires FROM section";
            return false;
        }

        if (ArgsCount > 0) {
            ctx.Error(Pos) << "TableRow requires exactly 0 arguments";
            return false;
        }

        src->AllColumns();
        return true;
    }

    void DoUpdateState() const final {
        State.Set(ENodeState::Const, false);
    }

    TNodePtr DoClone() const final {
        return MakeIntrusive<TTableRow>(Pos, ArgsCount);
    }

private:
    ui32 ArgsCount;
};

TNodePtr BuildUdfUserTypeArg(TPosition pos, const TVector<TNodePtr>& args, TNodePtr customUserType) {
    TVector<TNodePtr> argsTypeItems;
    for (auto& arg : args) {
        argsTypeItems.push_back(new TCallNodeImpl(pos, "TypeOf", TVector<TNodePtr>(1, arg)));
    }

    TVector<TNodePtr> userTypeItems;
    userTypeItems.push_back(new TCallNodeImpl(pos, "TupleType", argsTypeItems));
    userTypeItems.push_back(new TCallNodeImpl(pos, "StructType", {}));
    if (customUserType) {
        userTypeItems.push_back(customUserType);
    } else {
        userTypeItems.push_back(new TCallNodeImpl(pos, "TupleType", {}));
    }

    return new TCallNodeImpl(pos, "TupleType", userTypeItems);
}

TNodePtr BuildUdfUserTypeArg(TPosition pos, TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr customUserType) {
    TVector<TNodePtr> userTypeItems;
    userTypeItems.reserve(3);
    userTypeItems.push_back(positionalArgs->Y("TypeOf", positionalArgs));
    userTypeItems.push_back(positionalArgs->Y("TypeOf", namedArgs));
    if (customUserType) {
        userTypeItems.push_back(customUserType);
    } else {
        userTypeItems.push_back(new TCallNodeImpl(pos, "TupleType", {}));
    }

    return new TCallNodeImpl(pos, "TupleType", userTypeItems);
}

TVector<TNodePtr> BuildUdfArgs(const TContext& ctx, TPosition pos, const TVector<TNodePtr>& args,
        TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr customUserType) {
    if (!ctx.Settings.EnableGenericUdfs) {
        return {};
    }
    TVector<TNodePtr> udfArgs;
    udfArgs.push_back(new TAstListNodeImpl(pos));
    udfArgs[0]->Add(new TAstAtomNodeImpl(pos, "Void", 0));
    if (namedArgs) {
        udfArgs.push_back(BuildUdfUserTypeArg(pos, positionalArgs, namedArgs, customUserType));
    } else {
        udfArgs.push_back(BuildUdfUserTypeArg(pos, args, customUserType));
    }
    return udfArgs;
}

class TCallableNode final: public INode {
public:
    TCallableNode(TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args)
        : INode(pos)
        , Module(module)
        , Name(name)
        , Args(args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Module == "yql") {
            ui32 flags;
            TString nameParseError;
            TPosition pos = Pos;
            TString parsedName;
            if (!TryStringContent(Name, parsedName, flags, nameParseError, pos)) {
                ctx.Error(pos) << "Failed to parse YQL: " << nameParseError;
                return false;
            }

            const TString yql("(" + parsedName + ")");
            TAstParseResult ast = ParseAst(yql, ctx.Pool.get());
            /// TODO: do not drop warnings
            if (ast.IsOk()) {
                Node = AstNode(ast.Root->GetChild(0));
            } else {
                ctx.Error(Pos) << "Failed to parse YQL: " << ast.Issues.ToString();
                return false;
            }

            if (src) {
                src->AllColumns();
            }
        } else if (ctx.Settings.ModuleMapping.contains(Module)) {
            Node = Y("bind", Module + "_module", Q(Name));
            if (src) {
                src->AllColumns();
            }
        } else {
            TNodePtr customUserType = nullptr;
            if (Module == "Tensorflow" && Name == "RunBatch") {
                if (Args.size() > 2) {
                    auto passThroughAtom = Q("PassThrough");
                    auto passThroughType = Y("StructMemberType", Y("ListItemType", Y("TypeOf", Args[1])), passThroughAtom);
                    customUserType = Y("AddMemberType", Args[2], passThroughAtom, passThroughType);
                    Args.erase(Args.begin() + 2);
                }
            }
            auto udfArgs = BuildUdfArgs(ctx, Pos, Args, nullptr, nullptr, customUserType);
            Node = BuildUdf(ctx, Pos, Module, Name, udfArgs);
        }
        return Node->Init(ctx, src);
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node);
        return Node->Translate(ctx);
    }

    void DoUpdateState() const override {
        YQL_ENSURE(Node);
        State.Set(ENodeState::Const, Node->IsConstant());
        State.Set(ENodeState::Aggregated, Node->IsAggregated());
    }

    TNodePtr DoClone() const override {
        return new TCallableNode(Pos, Module, Name, Args);
    }

private:
    TCiString Module;
    TString Name;
    TVector<TNodePtr> Args;
    TNodePtr Node;
};

TNodePtr BuildCallable(TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args) {
    return new TCallableNode(pos, module, name, args);
}

TNodePtr BuildUdf(TContext& ctx, TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args) {
    auto fullName = module + "." + name;
    if (!args.empty()) {
        return new TYqlUdf(pos, fullName, args, args.size() + 1);

    } else {
        auto varName = ctx.AddSimpleUdf(fullName);
        return new TAstAtomNodeImpl(pos, varName, TNodeFlags::ArbitraryContent);
    }
}

class TScriptUdf final: public INode {
public:
    TScriptUdf(TPosition pos, const TString& moduleName, const TString& funcName, const TVector<TNodePtr>& args)
        : INode(pos)
        , ModuleName(moduleName)
        , FuncName(funcName)
        , Args(args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        const bool isPython = ModuleName.find(TStringBuf("Python")) != TString::npos;
        if (!isPython) {
            if (Args.size() != 2) {
                ctx.Error(Pos) << ModuleName << " script declaration requires exactly two parameters";
                return false;
            }
        } else {
            if (Args.size() < 1 || Args.size() > 2) {
                ctx.Error(Pos) << ModuleName << " script declaration requires one or two parameters";
                return false;
            }
        }

        auto nameAtom = BuildQuotedAtom(Pos, FuncName);
        auto scriptNode = Args.back();
        if (!scriptNode->Init(ctx, src)) {
            return false;
        }
        auto scriptStrPtr = Args.back()->GetLiteral("String");
        if (scriptStrPtr && scriptStrPtr->size() > SQL_MAX_INLINE_SCRIPT_LEN) {
            scriptNode = ctx.UniversalAlias("scriptudf", std::move(scriptNode));
        }

        INode::TPtr type;
        if (Args.size() == 2) {
            auto literal = Args[0]->GetLiteral("String");
            if (literal) {
                auto parsed = ParseType(*literal, *ctx.Pool, ctx.Issues, Args[0]->GetPos());
                if (!parsed) {
                    ctx.Error(Args[0]->GetPos()) << "Failed to parse script signature";
                    return false;
                }

                type = AstNode(parsed);
            } else {
                type = Args[0];
            }
        } else {
            // Python supports getting functions signatures right from docstrings
            type = Y("EvaluateType", Y("ParseTypeHandle", Y("Apply",
                Y("bind", "core_module", Q("PythonFuncSignature")),
                Q(ModuleName),
                scriptNode,
                Y("String", nameAtom)
                )));
        }

        if (!type->Init(ctx, src)) {
            return false;
        }

        Node = Y("ScriptUdf", Q(ModuleName), nameAtom, type, scriptNode);
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_UNUSED(ctx);
        Y_DEBUG_ABORT_UNLESS(Node);
        return Node->Translate(ctx);
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, true);
    }

    TNodePtr DoClone() const final {
        return new TScriptUdf(GetPos(), ModuleName, FuncName, CloneContainer(Args));
    }
private:
    TString ModuleName;
    TString FuncName;
    TVector<TNodePtr> Args;
    TNodePtr Node;
};

template <bool Sorted>
class TYqlToDict final: public TCallNode {
public:
    TYqlToDict(TPosition pos, const TString& mode, const TVector<TNodePtr>& args)
        : TCallNode(pos, "ToDict", 4, 4, args)
        , Mode(mode)
    {}

private:
    TCallNode::TPtr DoClone() const override {
       return new TYqlToDict<Sorted>(GetPos(), Mode, CloneContainer(Args));
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args.size() != 1) {
            ctx.Error(Pos) << "ToDict required exactly one argument";
            return false;
        }
        Args.push_back(BuildLambda(Pos, Y("val"), Y("Nth", "val", Q("0"))));
        Args.push_back(BuildLambda(Pos, Y("val"), Y("Nth", "val", Q("1"))));
        Args.push_back(Q(Y(Q(Sorted ? "Sorted" : "Hashed"), Q(Mode))));
        return TCallNode::DoInit(ctx, src);
    }
private:
    TString Mode;
};

template <bool IsStart>
class THoppingTime final: public TAstListNode {
public:
    THoppingTime(TPosition pos, const TVector<TNodePtr>& args = {})
        : TAstListNode(pos)
    {
        Y_UNUSED(args);
    }

private:
    TNodePtr DoClone() const override {
        return new THoppingTime(GetPos());
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(ctx);

        auto window = src->GetHoppingWindowSpec();
        if (!window) {
            ctx.Error(Pos) << "No hopping window parameters in aggregation";
            return false;
        }

        Nodes.clear();

        if (!IsStart) {
            Add("Member", "row", Q("_yql_time"));
            return true;
        }

        Add("Sub",
            Y("Member", "row", Q("_yql_time")),
            window->Interval);
        return true;
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Aggregated, true);
    }
};

class TInvalidBuiltin final: public INode {
public:
    TInvalidBuiltin(TPosition pos, const TString& info)
        : INode(pos)
        , Info(info)
    {
    }

    bool DoInit(TContext& ctx, ISource*) override {
        ctx.Error(Pos) << Info;
        return false;
    }

    TAstNode* Translate(TContext&) const override {
        return nullptr;
    }

    TPtr DoClone() const override {
        return {};
    }
private:
    TString Info;
};

enum EAggrFuncTypeCallback {
    NORMAL,
    WINDOW_AUTOARGS,
    KEY_PAYLOAD,
    PAYLOAD_PREDICATE,
    TWO_ARGS,
    COUNT,
    HISTOGRAM,
    LINEAR_HISTOGRAM,
    PERCENTILE,
    TOPFREQ,
    TOP,
    TOP_BY,
    COUNT_DISTINCT_ESTIMATE,
    LIST,
    UDAF
};

struct TCoreFuncInfo {
    TString Name;
    ui32 MinArgs;
    ui32 MaxArgs;
};

using TAggrFuncFactoryCallback = std::function<INode::TPtr(TPosition pos, const TVector<TNodePtr>& args, EAggregateMode aggMode, bool isFactory)>;
using TAggrFuncFactoryCallbackMap = std::unordered_map<TString, TAggrFuncFactoryCallback, THash<TString>>;
using TBuiltinFactoryCallback = std::function<TNodePtr(TPosition pos, const TVector<TNodePtr>& args)>;
using TBuiltinFactoryCallbackMap = std::unordered_map<TString, TBuiltinFactoryCallback, THash<TString>>;
using TCoreFuncMap = std::unordered_map<TString, TCoreFuncInfo, THash<TString>>;

TAggrFuncFactoryCallback BuildAggrFuncFactoryCallback(
        const TString& functionName,
        const TString& factoryName,
        EAggrFuncTypeCallback type = NORMAL,
        const TString& functionNameOverride = TString(),
        const TVector<EAggregateMode>& validModes = {}) {

    const TString realFunctionName = functionNameOverride.empty() ? functionName : functionNameOverride;
    return [functionName, realFunctionName, factoryName, type, validModes] (TPosition pos, const TVector<TNodePtr>& args, EAggregateMode aggMode, bool isFactory) -> INode::TPtr {
        if (!validModes.empty()) {
            if (!IsIn(validModes, aggMode)) {
                const TString errorText = TStringBuilder()
                     << "Can't use " << functionName << " in " << ToString(aggMode) << " aggregation mode";
                return INode::TPtr(new TInvalidBuiltin(pos, errorText));
            }
        }
        TAggregationPtr factory = nullptr;
        switch (type) {
        case NORMAL:
            factory = BuildFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case WINDOW_AUTOARGS:
            factory = BuildFactoryAggregationWinAutoarg(pos, realFunctionName, factoryName, aggMode);
            break;
        case KEY_PAYLOAD:
            factory = BuildKeyPayloadFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case PAYLOAD_PREDICATE:
            factory = BuildPayloadPredicateFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case TWO_ARGS:
            factory = BuildTwoArgsFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case COUNT:
            factory = BuildCountAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case HISTOGRAM:
            factory = BuildHistogramFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case LINEAR_HISTOGRAM:
            factory = BuildLinearHistogramFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case PERCENTILE:
            factory = BuildPercentileFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case TOPFREQ:
            factory = BuildTopFreqFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case TOP:
            factory = BuildTopFactoryAggregation<false>(pos, realFunctionName, factoryName, aggMode);
            break;
        case TOP_BY:
            factory = BuildTopFactoryAggregation<true>(pos, realFunctionName, factoryName, aggMode);
            break;
        case COUNT_DISTINCT_ESTIMATE:
            factory = BuildCountDistinctEstimateFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case LIST:
            factory = BuildListFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        case UDAF:
            factory = BuildUserDefinedFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
            break;
        }
        if (isFactory) {
            auto realArgs = args;
            realArgs.erase(realArgs.begin()); // skip function name
            return new TBasicAggrFactory(pos, functionName, factory, realArgs);
        } else {
            return new TBasicAggrFunc(pos, functionName, factory, args);
        }
    };
}

TAggrFuncFactoryCallback BuildAggrFuncFactoryCallback(
        const TString& functionName,
        const TString& factoryName,
        const TVector<EAggregateMode>& validModes,
        EAggrFuncTypeCallback type = NORMAL,
        const TString& functionNameOverride = TString()) {
    return BuildAggrFuncFactoryCallback(functionName, factoryName, type, functionNameOverride, validModes);
}

template<typename TType>
TBuiltinFactoryCallback BuildSimpleBuiltinFactoryCallback() {
    return [] (TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, args);
    };
}

template<typename TType>
TBuiltinFactoryCallback BuildNamedBuiltinFactoryCallback(const TString& name) {
    return [name] (TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, name, args);
    };
}

template<typename TType>
TBuiltinFactoryCallback BuildArgcBuiltinFactoryCallback(i32 minArgs, i32 maxArgs) {
    return [minArgs, maxArgs] (TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, minArgs, maxArgs, args);
    };
}

template<typename TType>
TBuiltinFactoryCallback BuildNamedArgcBuiltinFactoryCallback(const TString& name, i32 minArgs, i32 maxArgs) {
    return [name, minArgs, maxArgs] (TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, name, minArgs, maxArgs, args);
    };
}

template<typename TType>
TBuiltinFactoryCallback BuildNamedDepsArgcBuiltinFactoryCallback(ui32 reqArgsCount, const TString& name, i32 minArgs, i32 maxArgs) {
    return [reqArgsCount, name, minArgs, maxArgs](TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(reqArgsCount, pos, name, minArgs, maxArgs, args);
    };
}

template<typename TType>
TBuiltinFactoryCallback BuildBoolBuiltinFactoryCallback(bool arg) {
    return [arg] (TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, args, arg);
    };
}

template<typename TType>
TBuiltinFactoryCallback BuildFoldBuiltinFactoryCallback(const TString& name, const TString& defaultValue) {
    return [name, defaultValue] (TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, name, "Bool", defaultValue, 1, args);
    };
}

TNodePtr MakePair(TPosition pos, const TVector<TNodePtr>& args) {
    TNodePtr list = new TAstListNodeImpl(pos, {
        args[0],
        args.size() > 1 ? args[1] : new TAstListNodeImpl(pos,{ new TAstAtomNodeImpl(pos, "Null", TNodeFlags::Default) })
    });

    return new TAstListNodeImpl(pos, {
        new TAstAtomNodeImpl(pos, "quote", TNodeFlags::Default),
        list
    });
}

struct TBuiltinFuncData {
    const TBuiltinFactoryCallbackMap BuiltinFuncs;
    const TAggrFuncFactoryCallbackMap AggrFuncs;
    const TCoreFuncMap CoreFuncs;

    TBuiltinFuncData():
        BuiltinFuncs(MakeBuiltinFuncs()),
        AggrFuncs(MakeAggrFuncs()),
        CoreFuncs(MakeCoreFuncs())
    {
    }

    TBuiltinFactoryCallbackMap MakeBuiltinFuncs() {
        TBuiltinFactoryCallbackMap builtinFuncs = {
            // Branching
            {"if", BuildSimpleBuiltinFactoryCallback<TYqlIf<false>>()},
            {"ifstrict", BuildSimpleBuiltinFactoryCallback<TYqlIf<true>>() },

            // String builtins
            {"len", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Size", 1, 1)},
            {"length", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Size", 1, 1)},
            {"charlength", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Size", 1, 1)},
            {"characterlength", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Size", 1, 1)},
            {"substring", BuildSimpleBuiltinFactoryCallback<TYqlSubstring>()},
            {"byteat", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ByteAt", 2, 2) },

            // Numeric builtins
            {"abs", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Abs", 1, 1) },
            {"tobytes", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ToBytes", 1, 1) },
            {"frombytes", BuildSimpleBuiltinFactoryCallback<TFromBytes>() },

            // Compare builtins
            {"minof", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Min", 1, -1)},
            {"maxof", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Max", 1, -1)},
            {"greatest", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Max", 1, -1)},
            {"least", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Min", 1, -1)},
            {"in", BuildSimpleBuiltinFactoryCallback<TYqlIn>()},

            // List builtins
            {"aslist", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsList", 1, -1)},
            {"asliststrict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsListStrict", 1, -1) },
            {"listlength", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Length", 1, 1)},
            {"listhasitems", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("HasItems", 1, 1)},
            {"listcount", BuildSimpleBuiltinFactoryCallback<TListCountBuiltin>()},
            {"listextend", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Extend", 1, -1)},
            {"listunionall", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("UnionAll", 1, -1) },
            {"listzip", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Zip", -1, -1)},
            {"listzipall", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ZipAll", -1, -1)},
            {"listenumerate", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Enumerate", 1, 3)},
            {"listreverse", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Reverse", 1, 1)},
            {"listskip", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Skip", 2, 2)},
            {"listtake", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Take", 2, 2)},
            {"listsort", BuildBoolBuiltinFactoryCallback<TListSortBuiltin>(true)},
            {"listsortasc", BuildBoolBuiltinFactoryCallback<TListSortBuiltin>(true)},
            {"listsortdesc", BuildBoolBuiltinFactoryCallback<TListSortBuiltin>(false)},
            {"listmap", BuildBoolBuiltinFactoryCallback<TListMapBuiltin>(false)},
            {"listflatmap", BuildBoolBuiltinFactoryCallback<TListMapBuiltin>(true)},
            {"listfilter", BuildSimpleBuiltinFactoryCallback<TListFilterBuiltin>()},
            {"listany", BuildFoldBuiltinFactoryCallback<TListFoldBuiltinImpl>("Or", "false")},
            {"listall", BuildFoldBuiltinFactoryCallback<TListFoldBuiltinImpl>("And", "true")},
            {"listhas", BuildSimpleBuiltinFactoryCallback<TListHasBuiltin>()},
            {"listmax", BuildNamedBuiltinFactoryCallback<TListFold1Builtin>("AggrMax")},
            {"listmin", BuildNamedBuiltinFactoryCallback<TListFold1Builtin>("AggrMin")},
            {"listsum", BuildNamedBuiltinFactoryCallback<TListFold1Builtin>("AggrAdd")},
            {"listavg", BuildSimpleBuiltinFactoryCallback<TListAvgBuiltin>()},
            {"listconcat", BuildNamedBuiltinFactoryCallback<TListFold1Builtin>("Concat")},
            {"listextract", BuildSimpleBuiltinFactoryCallback<TListExtractBuiltin>()},
            {"listuniq", BuildSimpleBuiltinFactoryCallback<TListUniqBuiltin>()},
            {"listcreate", BuildSimpleBuiltinFactoryCallback<TListCreateBuiltin>()},
            {"listfromrange", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFromRange", 2, 3) },
            {"listreplicate", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Replicate", 2, 2) },
            {"listtakewhile", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TakeWhile", 2, 2) },
            {"listskipwhile", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SkipWhile", 2, 2) },
            {"listcollect", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Collect", 1, 1) },

            // Dict builtins
            {"dictcreate", BuildSimpleBuiltinFactoryCallback<TDictCreateBuiltin>()},
            {"asdict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsDict", 1, -1)},
            {"asdictstrict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsDictStrict", 1, -1)},
            {"todict", BuildNamedBuiltinFactoryCallback<TYqlToDict<false>>("One")},
            {"tomultidict", BuildNamedBuiltinFactoryCallback<TYqlToDict<false>>("Many")},
            {"tosorteddict", BuildNamedBuiltinFactoryCallback<TYqlToDict<true>>("One")},
            {"tosortedmultidict", BuildNamedBuiltinFactoryCallback<TYqlToDict<true>>("Many")},
            {"dictkeys", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictKeys", 1, 1) },
            {"dictpayloads", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictPayloads", 1, 1) },
            {"dictitems", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictItems", 1, 1) },
            {"dictlookup", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Lookup", 2, 2) },
            {"dictcontains", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Contains", 2, 2) },

            // Atom builtins
            {"asatom", BuildSimpleBuiltinFactoryCallback<TYqlAsAtom>()},
            {"secureparam", BuildNamedBuiltinFactoryCallback<TYqlAtom>("SecureParam")},

            {"void", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Void", 0, 0)},
            {"callable", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Callable", 2, 2)},
            {"way", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Way", 1, 1) },
            {"variant", BuildSimpleBuiltinFactoryCallback<TYqlVariant>() },
            {"astagged", BuildSimpleBuiltinFactoryCallback<TYqlAsTagged>() },
            {"untag", BuildSimpleBuiltinFactoryCallback<TYqlUntag>() },
            {"parsetype", BuildSimpleBuiltinFactoryCallback<TYqlParseType>() },
            {"ensuretype", BuildSimpleBuiltinFactoryCallback<TYqlTypeAssert<true>>() },
            {"ensureconvertibleto", BuildSimpleBuiltinFactoryCallback<TYqlTypeAssert<false>>() },
            {"ensure", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Ensure", 2, 3) },
            {"evaluateexpr", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EvaluateExpr", 1, 1) },
            {"evaluateatom", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EvaluateAtom", 1, 1) },
            {"evaluatetype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EvaluateType", 1, 1) },
            {"unwrap", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Unwrap", 1, 2) },
            {"just", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Just", 1, 1) },
            {"nothing", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Nothing", 1, 1) },
            {"formattype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FormatType", 1, 1) },
            {"typeof", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TypeOf", 1, 1) },
            {"instanceof", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("InstanceOf", 1, 1) },
            {"datatype", BuildSimpleBuiltinFactoryCallback<TYqlDataType>() },
            {"optionaltype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("OptionalType", 1, 1) },
            {"listtype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListType", 1, 1) },
            {"streamtype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StreamType", 1, 1) },
            {"dicttype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictType", 2, 2) },
            {"tupletype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TupleType", 0, -1) },
            {"generictype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("GenericType", 0, 0) },
            {"unittype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("UnitType", 0, 0) },
            {"voidtype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VoidType", 0, 0) },
            {"resourcetype", BuildSimpleBuiltinFactoryCallback<TYqlResourceType>() },
            {"taggedtype", BuildSimpleBuiltinFactoryCallback<TYqlTaggedType>() },
            {"varianttype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VariantType", 1, 1) },
            {"callabletype", BuildSimpleBuiltinFactoryCallback<TYqlCallableType>() },
            {"optionalitemtype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("OptionalItemType", 1, 1) },
            {"listitemtype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListItemType", 1, 1) },
            {"streamitemtype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StreamItemType", 1, 1) },
            {"dictkeytype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictKeyType", 1, 1) },
            {"dictpayloadtype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictPayloadType", 1, 1) },
            {"tupleelementtype", BuildSimpleBuiltinFactoryCallback<TYqlTupleElementType>() },
            {"structmembertype", BuildSimpleBuiltinFactoryCallback<TYqlStructMemberType>() },
            {"callableresulttype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableResultType", 1, 1) },
            {"callableargumenttype", BuildSimpleBuiltinFactoryCallback<TYqlCallableArgumentType>() },
            {"variantunderlyingtype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VariantUnderlyingType", 1, 1) },
            {"fromysonsimpletype", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FromYsonSimpleType", 2, 2) },
            {"currentutcdate", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "CurrentUtcDate", 0, -1) },
            {"currentutcdatetime", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "CurrentUtcDatetime", 0, -1) },
            {"currentutctimestamp", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "CurrentUtcTimestamp", 0, -1) },
            {"currentoperationid", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentOperationId", 0, 0) },
            {"currentoperationsharedid", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentOperationSharedId", 0, 0) },
            {"currentauthenticateduser", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentAuthenticatedUser", 0, 0) },
            {"addtimezone", BuildSimpleBuiltinFactoryCallback<TYqlAddTimezone>() },
            {"removetimezone", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("RemoveTimezone", 1, 1) },
            {"typehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TypeHandle", 1, 1) },
            {"parsetypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ParseTypeHandle", 1, 1) },
            {"typekind", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TypeKind", 1, 1) },
            {"datatypecomponents", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DataTypeComponents", 1, 1) },
            {"datatypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DataTypeHandle", 1, 1) },
            {"optionaltypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("OptionalTypeHandle", 1, 1) },
            {"listtypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTypeHandle", 1, 1) },
            {"streamtypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StreamTypeHandle", 1, 1) },
            {"tupletypecomponents", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TupleTypeComponents", 1, 1) },
            {"tupletypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TupleTypeHandle", 1, 1) },
            {"structtypecomponents", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructTypeComponents", 1, 1) },
            {"structtypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructTypeHandle", 1, 1) },
            {"dicttypecomponents", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictTypeComponents", 1, 1) },
            {"dicttypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictTypeHandle", 2, 2) },
            {"resourcetypetag", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ResourceTypeTag", 1, 1) },
            {"resourcetypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ResourceTypeHandle", 1, 1) },
            {"taggedtypecomponents", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TaggedTypeComponents", 1, 1) },
            {"taggedtypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TaggedTypeHandle", 2, 2) },
            {"varianttypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VariantTypeHandle", 1, 1) },
            {"voidtypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VoidTypeHandle", 0, 0) },
            {"nulltypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("NullTypeHandle", 0, 0) },
            {"callabletypecomponents", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableTypeComponents", 1, 1) },
            {"callableargument", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableArgument", 1, 3) },
            {"callabletypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableTypeHandle", 2, 4) },
            {"formatcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FormatCode", 1, 1) },
            {"worldcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("WorldCode", 0, 0) },
            {"atomcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AtomCode", 1, 1) },
            {"listcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListCode", 0, -1) },
            {"funccode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FuncCode", 1, -1) },
            {"lambdacode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LambdaCode", 1, 2) },
            {"evaluatecode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EvaluateCode", 1, 1) },
            {"reprcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ReprCode", 1, 1) },
            {"quotecode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("QuoteCode", 1, 1) },

            // Tuple builtins
            {"astuple", BuildSimpleBuiltinFactoryCallback<TTupleNode>()},

            // Struct builtins
            {"addmember", BuildNamedBuiltinFactoryCallback<TAddMember>("AddMember")},
            {"removemember", BuildNamedBuiltinFactoryCallback<TRemoveMember>("RemoveMember")},
            {"forceremovemember", BuildNamedBuiltinFactoryCallback<TRemoveMember>("ForceRemoveMember")},
            {"combinemembers", BuildNamedBuiltinFactoryCallback<TCombineMembers>("FlattenMembers")},
            {"flattenmembers", BuildNamedBuiltinFactoryCallback<TFlattenMembers>("FlattenMembers")},

            // File builtins
            {"filepath", BuildNamedBuiltinFactoryCallback<TYqlAtom>("FilePath")},
            {"filecontent", BuildNamedBuiltinFactoryCallback<TYqlAtom>("FileContent")},
            {"folderpath", BuildNamedBuiltinFactoryCallback<TYqlAtom>("FolderPath") },
            {"files", BuildNamedBuiltinFactoryCallback<TYqlAtom>("Files")},
            {"parsefile", BuildSimpleBuiltinFactoryCallback<TYqlParseFileOp>()},

            // Misc builtins
            {"coalesce", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Coalesce", 1, -1)},
            {"nvl", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Coalesce", 1, -1) },
            {"nanvl", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Nanvl", 2, 2) },
            {"likely", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Likely", 1, -1)},
            {"random", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "Random", 1, -1)},
            {"randomnumber", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "RandomNumber", 1, -1)},
            {"randomuuid", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "RandomUuid", 1, -1) },
            {"tablepath", BuildNamedBuiltinFactoryCallback<TCallDirectRow>("TablePath") },
            {"tablerecord", BuildNamedBuiltinFactoryCallback<TCallDirectRow>("TableRecord") },
            {"tablerecordindex", BuildNamedBuiltinFactoryCallback<TCallDirectRow>("TableRecord") },
            {"weakfield", BuildSimpleBuiltinFactoryCallback<TWeakFieldOp>()},
            {"tablerow", BuildSimpleBuiltinFactoryCallback<TTableRow>() },

            // Hint builtins
            {"grouping", BuildSimpleBuiltinFactoryCallback<TGroupingNode>()},

            // Window funcitons
            {"rownumber", BuildNamedArgcBuiltinFactoryCallback<TWinRowNumber>("RowNumber", 0, 0)},
            /// by SQL2011 should be with sort
            {"lead", BuildNamedArgcBuiltinFactoryCallback<TWinLeadLag>("Lead", 1, 2)},
            {"lag", BuildNamedArgcBuiltinFactoryCallback<TWinLeadLag>("Lag", 1, 2)},

            // Hopping intervals time functions
            {"hopstart", BuildSimpleBuiltinFactoryCallback<THoppingTime<true>>()},
            {"hopend", BuildSimpleBuiltinFactoryCallback<THoppingTime<false>>()},
        };
        return builtinFuncs;
    }

    TAggrFuncFactoryCallbackMap MakeAggrFuncs() {
        constexpr auto OverWindow = EAggregateMode::OverWindow;

        TAggrFuncFactoryCallbackMap aggrFuncs = {
            {"min", BuildAggrFuncFactoryCallback("Min", "min_traits_factory")},
            {"max", BuildAggrFuncFactoryCallback("Max", "max_traits_factory")},

            {"minby", BuildAggrFuncFactoryCallback("MinBy", "min_by_traits_factory", KEY_PAYLOAD)},
            {"maxby", BuildAggrFuncFactoryCallback("MaxBy", "max_by_traits_factory", KEY_PAYLOAD)},

            {"sum", BuildAggrFuncFactoryCallback("Sum", "sum_traits_factory")},
            {"sumif", BuildAggrFuncFactoryCallback("SumIf", "sum_if_traits_factory", PAYLOAD_PREDICATE) },

            {"some", BuildAggrFuncFactoryCallback("Some", "some_traits_factory")},
            {"somevalue", BuildAggrFuncFactoryCallback("SomeValue", "some_traits_factory")},

            {"count", BuildAggrFuncFactoryCallback("Count", "count_traits_factory", COUNT)},
            {"countif", BuildAggrFuncFactoryCallback("CountIf", "count_if_traits_factory")},

            {"every", BuildAggrFuncFactoryCallback("Every", "and_traits_factory")},
            {"booland", BuildAggrFuncFactoryCallback("BoolAnd", "and_traits_factory")},
            {"boolor", BuildAggrFuncFactoryCallback("BoolOr", "or_traits_factory")},

            {"bitand", BuildAggrFuncFactoryCallback("BitAnd", "bit_and_traits_factory")},
            {"bitor", BuildAggrFuncFactoryCallback("BitOr", "bit_or_traits_factory")},
            {"bitxor", BuildAggrFuncFactoryCallback("BitXor", "bit_xor_traits_factory")},

            {"avg", BuildAggrFuncFactoryCallback("Avg", "avg_traits_factory")},
            {"avgif", BuildAggrFuncFactoryCallback("AvgIf", "avg_if_traits_factory", PAYLOAD_PREDICATE) },

            {"list", BuildAggrFuncFactoryCallback("List", "list_traits_factory", LIST)},
            {"agglist", BuildAggrFuncFactoryCallback("AggregateList", "list2_traits_factory", LIST)},
            {"aggrlist", BuildAggrFuncFactoryCallback("AggregateList", "list2_traits_factory", LIST)},
            {"aggregatelist", BuildAggrFuncFactoryCallback("AggregateList", "list2_traits_factory", LIST)},
            {"listdistinct", BuildAggrFuncFactoryCallback("ListDistinct", "set_traits_factory", LIST)},
            {"agglistdistinct", BuildAggrFuncFactoryCallback("AggregateListDistinct", "set_traits_factory", LIST)},
            {"aggrlistdistinct", BuildAggrFuncFactoryCallback("AggregateListDistinct", "set_traits_factory", LIST)},
            {"aggregatelistdistinct", BuildAggrFuncFactoryCallback("AggregateListDistinct", "set_traits_factory", LIST)},

            {"median", BuildAggrFuncFactoryCallback("Median", "percentile_traits_factory", PERCENTILE)},
            {"percentile", BuildAggrFuncFactoryCallback("Percentile", "percentile_traits_factory", PERCENTILE)},

            {"mode", BuildAggrFuncFactoryCallback("Mode", "topfreq_traits_factory", TOPFREQ) },
            {"topfreq", BuildAggrFuncFactoryCallback("TopFreq", "topfreq_traits_factory", TOPFREQ) },

            {"top", BuildAggrFuncFactoryCallback("Top", "top_traits_factory", TOP)},
            {"bottom", BuildAggrFuncFactoryCallback("Bottom", "bottom_traits_factory", TOP)},
            {"topby", BuildAggrFuncFactoryCallback("TopBy", "top_by_traits_factory", TOP_BY)},
            {"bottomby", BuildAggrFuncFactoryCallback("BottomBy", "bottom_by_traits_factory", TOP_BY)},

            {"histogram", BuildAggrFuncFactoryCallback("AdaptiveWardHistogram", "histogram_adaptive_ward_traits_factory", HISTOGRAM, "Histogram")},
            {"adaptivewardhistogram", BuildAggrFuncFactoryCallback("AdaptiveWardHistogram", "histogram_adaptive_ward_traits_factory", HISTOGRAM)},
            {"adaptiveweighthistogram", BuildAggrFuncFactoryCallback("AdaptiveWeightHistogram", "histogram_adaptive_weight_traits_factory", HISTOGRAM)},
            {"adaptivedistancehistogram", BuildAggrFuncFactoryCallback("AdaptiveDistanceHistogram", "histogram_adaptive_distance_traits_factory", HISTOGRAM)},
            {"blockwardhistogram", BuildAggrFuncFactoryCallback("BlockWardHistogram", "histogram_block_ward_traits_factory", HISTOGRAM)},
            {"blockweighthistogram", BuildAggrFuncFactoryCallback("BlockWeightHistogram", "histogram_block_weight_traits_factory", HISTOGRAM)},
            {"linearhistogram", BuildAggrFuncFactoryCallback("LinearHistogram", "histogram_linear_traits_factory", LINEAR_HISTOGRAM)},
            {"logarithmichistogram", BuildAggrFuncFactoryCallback("LogarithmicHistogram", "histogram_logarithmic_traits_factory", LINEAR_HISTOGRAM)},
            {"loghistogram", BuildAggrFuncFactoryCallback("LogarithmicHistogram", "histogram_logarithmic_traits_factory", LINEAR_HISTOGRAM, "LogHistogram")},

            {"hyperloglog", BuildAggrFuncFactoryCallback("HyperLogLog", "hyperloglog_traits_factory", COUNT_DISTINCT_ESTIMATE)},
            {"hll", BuildAggrFuncFactoryCallback("HyperLogLog", "hyperloglog_traits_factory", COUNT_DISTINCT_ESTIMATE, "HLL")},
            {"countdistinctestimate", BuildAggrFuncFactoryCallback("HyperLogLog", "hyperloglog_traits_factory", COUNT_DISTINCT_ESTIMATE, "CountDistinctEstimate")},

            {"variance", BuildAggrFuncFactoryCallback("Variance", "variance_0_1_traits_factory")},
            {"stddev", BuildAggrFuncFactoryCallback("StdDev", "variance_1_1_traits_factory")},
            {"populationvariance", BuildAggrFuncFactoryCallback("VariancePopulation", "variance_0_0_traits_factory")},
            {"variancepopulation", BuildAggrFuncFactoryCallback("VariancePopulation", "variance_0_0_traits_factory")},
            {"populationstddev", BuildAggrFuncFactoryCallback("StdDevPopulation", "variance_1_0_traits_factory")},
            {"stddevpopulation", BuildAggrFuncFactoryCallback("StdDevPopulation", "variance_1_0_traits_factory")},
            {"varpop", BuildAggrFuncFactoryCallback("VariancePopulation", "variance_0_0_traits_factory")},
            {"stddevpop", BuildAggrFuncFactoryCallback("StdDevPopulation", "variance_1_0_traits_factory")},
            {"varp", BuildAggrFuncFactoryCallback("VariancePopulation", "variance_0_0_traits_factory")},
            {"stddevp", BuildAggrFuncFactoryCallback("StdDevPopulation", "variance_1_0_traits_factory")},
            {"variancesample", BuildAggrFuncFactoryCallback("VarianceSample", "variance_0_1_traits_factory")},
            {"stddevsample", BuildAggrFuncFactoryCallback("StdDevSample", "variance_1_1_traits_factory")},
            {"varsamp", BuildAggrFuncFactoryCallback("VarianceSample", "variance_0_1_traits_factory")},
            {"stddevsamp", BuildAggrFuncFactoryCallback("StdDevSample", "variance_1_1_traits_factory")},
            {"vars", BuildAggrFuncFactoryCallback("VarianceSample", "variance_0_1_traits_factory")},
            {"stddevs", BuildAggrFuncFactoryCallback("StdDevSample", "variance_1_1_traits_factory")},

            {"correlation", BuildAggrFuncFactoryCallback("Correlation", "correlation_traits_factory", TWO_ARGS)},
            {"corr", BuildAggrFuncFactoryCallback("Correlation", "correlation_traits_factory", TWO_ARGS, "Corr")},
            {"covariance", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS, "Covariance")},
            {"covariancesample", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS)},
            {"covarsamp", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS, "CovarSamp")},
            {"covar", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS, "Covar")},
            {"covars", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS, "CovarS")},
            {"covariancepopulation", BuildAggrFuncFactoryCallback("CovariancePopulation", "covariance_population_traits_factory", TWO_ARGS)},
            {"covarpop", BuildAggrFuncFactoryCallback("CovariancePopulation", "covariance_population_traits_factory", TWO_ARGS, "CovarPop")},
            {"covarp", BuildAggrFuncFactoryCallback("CovariancePopulation", "covariance_population_traits_factory", TWO_ARGS, "CovarP")},

            {"udaf", BuildAggrFuncFactoryCallback("UDAF", "udaf_traits_factory", UDAF)},

            // Window functions
            /// by SQL2011 should be with sort
            {"rank", BuildAggrFuncFactoryCallback("Rank", "rank_traits_factory", WINDOW_AUTOARGS)},
            {"denserank", BuildAggrFuncFactoryCallback("DenseRank", "dense_rank_traits_factory", WINDOW_AUTOARGS)},
            // \todo unsupported now, required count element in window
            //{"ntile", BuildAggrFuncFactoryCallback("Ntile", "ntile_traits_factory")},
            //{"percentrank", BuildAggrFuncFactoryCallback("PercentRank", "percent_rank_traits_factory")},
            //{"cumedist", BuildAggrFuncFactoryCallback("CumeDist", "cume_dist_traits_factory")},

            {"firstvalue", BuildAggrFuncFactoryCallback("FirstValue", "first_value_traits_factory", {OverWindow})},
            {"lastvalue", BuildAggrFuncFactoryCallback("LastValue", "last_value_traits_factory", {OverWindow})},
            {"firstvalueignorenulls", BuildAggrFuncFactoryCallback("FirstValueIgnoreNulls", "first_value_ignore_nulls_traits_factory", {OverWindow})},
            {"lastvalueignorenulls", BuildAggrFuncFactoryCallback("LastValueIgnoreNulls", "last_value_ignore_nulls_traits_factory", {OverWindow})},
        };
        return aggrFuncs;
    }

    TCoreFuncMap MakeCoreFuncs() {
        TCoreFuncMap coreFuncs = {
            {"listindexof", { "IndexOf", 2, 2}},
            {"testbit", { "TestBit", 2, 2}},
            {"setbit", { "SetBit", 2, 2}},
            {"clearbit", { "ClearBit", 2, 2}},
            {"flipbit", { "FlipBit", 2, 2 }},
            {"toset", { "ToSet", 1, 1 }},
            {"setisdisjoint", { "SetIsDisjoint", 2, 2}},
            {"setintersection", { "SetIntersection", 2, 3}},
            {"setincludes", { "SetIncludes", 2, 2}},
            {"setunion", { "SetUnion", 2, 3}},
            {"setdifference", { "SetDifference", 2, 2}},
            {"setsymmetricdifference", { "SetSymmetricDifference", 2, 3}},
            {"listaggregate", { "ListAggregate", 2, 2}},
            {"dictaggregate", { "DictAggregate", 2, 2}},
        };
        return coreFuncs;
    }
};

TNodePtr BuildBuiltinFunc(TContext& ctx, TPosition pos, TString name, const TVector<TNodePtr>& args,
    const TString& nameSpace, EAggregateMode aggMode, bool* mustUseNamed, TFuncPrepareNameNode funcPrepareNameNode) {

    const TBuiltinFuncData* funcData = Singleton<TBuiltinFuncData>();
    const TBuiltinFactoryCallbackMap& builtinFuncs = funcData->BuiltinFuncs;
    const TAggrFuncFactoryCallbackMap& aggrFuncs = funcData->AggrFuncs;
    const TCoreFuncMap& coreFuncs = funcData->CoreFuncs;

    for (auto& arg: args) {
        if (!arg) {
            return nullptr;
        }
    }

    TString normalizedName(name);
    TString ns = to_lower(nameSpace);
    if (ns.empty()) {
        TMaybe<TIssue> error = NormalizeName(pos, normalizedName);
        if (!error.Empty()) {
            return new TInvalidBuiltin(pos, error->GetMessage());
        }

        auto coreFunc = coreFuncs.find(normalizedName);
        if (coreFunc != coreFuncs.end()) {
            ns = "core";
            name = coreFunc->second.Name;
            if (args.size() < coreFunc->second.MinArgs || args.size() > coreFunc->second.MaxArgs) {
                return new TInvalidBuiltin(pos, TStringBuilder() << name << " expected from "
                    << coreFunc->second.MinArgs << " to " << coreFunc->second.MaxArgs << " arguments, but got: " << args.size());
            }

            if (coreFunc->second.MinArgs != coreFunc->second.MaxArgs) {
                name += ToString(args.size());
            }
        }
    }

    TString moduleResource;
    if (ctx.Settings.ModuleMapping.contains(ns)) {
        moduleResource = ctx.Settings.ModuleMapping.at(ns);
    }

    if (ns == "js") {
        ns = "javascript";
    }

    auto scriptType = NKikimr::NMiniKQL::ScriptTypeFromStr(ns);
    if (scriptType == NKikimr::NMiniKQL::EScriptType::SystemPython2) {
        scriptType = NKikimr::NMiniKQL::EScriptType::Python2;
    }

    if (ns == "yql") {
        return new TCallNodeImpl(pos, name, -1, -1, args);
    } else if (ns == "string" && name == "SplitToList") {
        TNodePtr positionalArgs;
        TNodePtr namedArgs;
        if (mustUseNamed && *mustUseNamed) {
            YQL_ENSURE(args.size() == 2);
            positionalArgs = args[0];
            namedArgs = args[1];
            *mustUseNamed = false;
        }

        TVector<TNodePtr> reuseArgs;
        if (!namedArgs && args && funcPrepareNameNode) {
            TString reusedBaseName = TStringBuilder() << "Arg" << to_title(nameSpace) << to_title(name);
            reuseArgs.reserve(args.size());
            for (const auto& arg: args) {
                reuseArgs.push_back(funcPrepareNameNode(reusedBaseName, arg));
            }
        }

        auto usedArgs = reuseArgs ? reuseArgs : args;

        TVector<TNodePtr> positionalArgsElements;

        if (namedArgs) {
            auto positionalArgsTuple = dynamic_cast<TTupleNode*>(positionalArgs.Get());
            Y_DEBUG_ABORT_UNLESS(positionalArgsTuple, "unexpected value at String::SplitToList positional args");
            positionalArgsElements = positionalArgsTuple->Elements();
        } else {
            positionalArgsElements = usedArgs;
        }

        auto positionalArgsTupleSize = positionalArgsElements.size();
        auto argsSize = positionalArgsTupleSize;

        TNodePtr trueLiteral = BuildLiteralBool(pos, "true");
        TNodePtr falseLiteral = BuildLiteralBool(pos, "false");
        TNodePtr namedDelimeterStringArg;
        TNodePtr namedSkipEmptyArg;

        bool hasDelimeterString = false;
        if (auto namedArgsStruct = dynamic_cast<TStructNode*>(namedArgs.Get())) {
            auto exprs = namedArgsStruct->GetExprs();
            for (auto& expr : exprs) {
                if (expr->GetLabel() == "DelimeterString") {
                    hasDelimeterString = true;
                    break;
                }
            }
            argsSize += namedArgsStruct->GetExprs().size();
        }

        if (argsSize < 3) {
            positionalArgsElements.push_back(falseLiteral);
        }
        if (argsSize < 4 && !hasDelimeterString) {
            positionalArgsElements.push_back(trueLiteral);
        }

        if (namedArgs) {
            positionalArgs = BuildTuple(pos, positionalArgsElements);
        } else {
            usedArgs = positionalArgsElements;
        }

        TNodePtr customUserType = nullptr;
        const auto& udfArgs = BuildUdfArgs(ctx, pos, usedArgs, positionalArgs, namedArgs, customUserType);
        TNodePtr udfNode = BuildUdf(ctx, pos, nameSpace, name, udfArgs);
        TVector<TNodePtr> applyArgs = { udfNode };
        applyArgs.insert(applyArgs.end(), usedArgs.begin(), usedArgs.end());
        return new TCallNodeImpl(pos, namedArgs ? "NamedApply" : "Apply", applyArgs);
    } else if (moduleResource) {
        auto exportName = ns == "core" ? name : "$" + name;
        TVector<TNodePtr> applyArgs = {
           new TCallNodeImpl(pos, "bind", {
               BuildAtom(pos, ns + "_module", 0), BuildQuotedAtom(pos, exportName)
           })
        };
        applyArgs.insert(applyArgs.end(), args.begin(), args.end());
        return new TCallNodeImpl(pos, "Apply", applyArgs);
    } else if (ns == "hyperscan" || ns == "pcre" || ns == "pire" || ns.StartsWith("re2")) {
        TString moduleName(nameSpace);
        moduleName.to_title();
        if ((args.size() == 1 || args.size() == 2) && (name.StartsWith("Multi") || (ns.StartsWith("re2") && name == "Capture"))) {
            TVector<TNodePtr> multiArgs{
                ns.StartsWith("re2") && name == "Capture" ? MakePair(pos, args) : args[0],
                new TCallNodeImpl(pos, "Void", 0, 0, {}),
                args[0]
            };
            auto fullName = moduleName + "." + name;
            return new TYqlTypeConfigUdf(pos, fullName, multiArgs, multiArgs.size() + 1);
        } else if (!(ns.StartsWith("re2") && name == "Options")) {
            auto newArgs = args;
            if (ns.StartsWith("re2")) {
                // convert run config is tuple of string and optional options
                if (args.size() == 1 || args.size() == 2) {
                    newArgs[0] = MakePair(pos, args);
                    if (args.size() == 2) {
                        newArgs.pop_back();
                    }
                } else {
                    return new TInvalidBuiltin(pos, TStringBuilder() << ns << "." << name << " expected one or two arguments.");
                }
            }

            return BuildUdf(ctx, pos, moduleName, name, newArgs);
        }
    } else if (ns == "datetime2" && (name == "Format" || name == "Parse")) {
        return BuildUdf(ctx, pos, nameSpace, name, args);

    } else if (scriptType != NKikimr::NMiniKQL::EScriptType::Unknown) {
        auto scriptName = NKikimr::NMiniKQL::ScriptTypeAsStr(scriptType);
        return new TScriptUdf(pos, TString(scriptName), name, args);
    } else if (ns.empty()) {
        auto type = NormalizeTypeString(normalizedName);
        if (AvailableDataTypes.contains(type)) {
            return new TYqlData(pos, type, args);
        }

        if (normalizedName == "tablename") {
            return new TTableName(pos, args, ctx.CurrCluster);
        }

        if (normalizedName == "aggregationfactory") {
            if (args.size() < 1 || !args[0]->GetLiteral("String")) {
                return new TInvalidBuiltin(pos, "AGGREGATION_FACTORY requries a function name");
            }

            auto aggNormalizedName = *args[0]->GetLiteral("String");
            auto error = NormalizeName(pos, aggNormalizedName);
            if (!error.Empty()) {
                return new TInvalidBuiltin(pos, error->GetMessage());
            }

            if (aggNormalizedName == "aggregateby") {
                return new TInvalidBuiltin(pos, "AGGREGATE_BY is not allowed to use with AGGREGATION_FACTORY");
            }

            if (aggNormalizedName == "multiaggregateby") {
                return new TInvalidBuiltin(pos, "MULTI_AGGREGATE_BY is not allowed to use with AGGREGATION_FACTORY");
            }

            auto aggrCallback = aggrFuncs.find(aggNormalizedName);
            if (aggrCallback == aggrFuncs.end()) {
                return new TInvalidBuiltin(pos, TStringBuilder() << "Unknown aggregation function: " << *args[0]->GetLiteral("String"));
            }

            if (aggMode == EAggregateMode::Distinct) {
                return new TInvalidBuiltin(pos, "Only aggregation functions allow DISTINCT set specification");
            }

            return (*aggrCallback).second(pos, args, aggMode, true).Release();
        }

        if (normalizedName == "aggregateby" || normalizedName == "multiaggregateby") {
            const bool multi = (normalizedName == "multiaggregateby");
            if (args.size() != 2) {
                return new TInvalidBuiltin(pos, TStringBuilder() << (multi ? "MULTI_AGGREGATE_BY" : "AGGREGATE_BY") << " requries two arguments");
            }

            auto name = multi ? "MultiAggregateBy" : "AggregateBy";
            auto aggr = BuildFactoryAggregation(pos, name, "", aggMode, multi);
            return new TBasicAggrFunc(pos, name, aggr, args);
        }

        auto aggrCallback = aggrFuncs.find(normalizedName);
        if (aggrCallback != aggrFuncs.end()) {
            return (*aggrCallback).second(pos, args, aggMode, false).Release();
        }
        if (aggMode == EAggregateMode::Distinct) {
            return new TInvalidBuiltin(pos, "Only aggregation functions allow DISTINCT set specification");
        }

        auto builtinCallback = builtinFuncs.find(normalizedName);
        if (builtinCallback != builtinFuncs.end()) {
            return (*builtinCallback).second(pos, args);
        } else if (normalizedName == "asstruct" || normalizedName == "structtype") {
            if (args.empty()) {
                return new TCallNodeImpl(pos, normalizedName == "asstruct" ? "AsStruct" : "StructType", 0, 0, args);
            }

            if (mustUseNamed && *mustUseNamed) {
                *mustUseNamed = false;
                YQL_ENSURE(args.size() == 2);
                Y_DEBUG_ABORT_UNLESS(dynamic_cast<TTupleNode*>(args[0].Get()));
                auto posArgs = static_cast<TTupleNode*>(args[0].Get());
                if (posArgs->IsEmpty()) {
                    if (normalizedName == "asstruct") {
                        return args[1];
                    } else {
                        Y_DEBUG_ABORT_UNLESS(dynamic_cast<TStructNode*>(args[1].Get()));
                        auto namedArgs = static_cast<TStructNode*>(args[1].Get());
                        return new TStructTypeNode(pos, namedArgs->GetExprs());
                    }
                }
            }
            return new TInvalidBuiltin(pos, TStringBuilder() <<
                (normalizedName == "asstruct" ? "AsStruct" : "StructType") <<
                " requires all argument to be named");
        } else if (normalizedName == "expandstruct") {
            if (mustUseNamed) {
                if (!*mustUseNamed) {
                    return new TInvalidBuiltin(pos, TStringBuilder() << "ExpandStruct requires at least one named argument");
                }
                *mustUseNamed = false;
            }
            YQL_ENSURE(args.size() == 2);
            auto posArgs = static_cast<TTupleNode*>(args[0].Get());
            Y_DEBUG_ABORT_UNLESS(dynamic_cast<TTupleNode*>(args[0].Get()));
            Y_DEBUG_ABORT_UNLESS(dynamic_cast<TStructNode*>(args[1].Get()));
            if (posArgs->GetTupleSize() != 1) {
                return new TInvalidBuiltin(pos, TStringBuilder() << "ExpandStruct requires all arguments except first to be named");
            }

            TVector<TNodePtr> flattenMembersArgs = {
                BuildTuple(pos, {BuildQuotedAtom(pos, ""), posArgs->GetTupleElement(0)}),
                BuildTuple(pos, {BuildQuotedAtom(pos, ""), args[1]}),
            };
            return new TCallNodeImpl(pos, "FlattenMembers", 2, 2, flattenMembersArgs);
        } else {
            return new TInvalidBuiltin(pos, TStringBuilder() << "Unknown builtin: " << name << ", to use YQL functions, try YQL::" << name);
        }
    }

    TNodePtr positionalArgs;
    TNodePtr namedArgs;
    if (mustUseNamed && *mustUseNamed) {
        YQL_ENSURE(args.size() == 2);
        positionalArgs = args[0];
        namedArgs = args[1];
        *mustUseNamed = false;
    }

    TVector<TNodePtr> reuseArgs;
    if (!namedArgs && args && funcPrepareNameNode) {
        TString reusedBaseName = TStringBuilder() << "Arg" << to_title(nameSpace) << to_title(name);
        reuseArgs.reserve(args.size());
        for (const auto& arg: args) {
            reuseArgs.push_back(funcPrepareNameNode(reusedBaseName, arg));
        }
    }

    auto usedArgs = reuseArgs ? reuseArgs : args;

    TNodePtr customUserType = nullptr;
    if (ns == "yson") {
        if (name == "ConvertTo" && usedArgs.size() > 1) {
            customUserType = usedArgs[1];
            usedArgs.erase(usedArgs.begin() + 1);
        }
        ui32 optionsIndex = name.Contains("Lookup") ? 2 : 1;
        if (usedArgs.size() <= optionsIndex && (ctx.PragmaYsonAutoConvert || ctx.PragmaYsonStrict)) {
            usedArgs.push_back(BuildYsonOptionsNode(pos, ctx.PragmaYsonAutoConvert, ctx.PragmaYsonStrict));
        }
    } else if (ns == "json") {
        ctx.Warning(pos, TIssuesIds::YQL_DEPRECATED_JSON_UDF)
            << "Json UDF is deprecated and is going to be removed, please switch to Yson UDF that also supports Json input: https://yql.yandex-team.ru/docs/yt/udf/list/yson/";
    }

    const auto& udfArgs = BuildUdfArgs(ctx, pos, usedArgs, positionalArgs, namedArgs, customUserType);
    TNodePtr udfNode = BuildUdf(ctx, pos, nameSpace, name, udfArgs);
    TVector<TNodePtr> applyArgs = { udfNode };
    applyArgs.insert(applyArgs.end(), usedArgs.begin(), usedArgs.end());
    return new TCallNodeImpl(pos, namedArgs ? "NamedApply" : "Apply", applyArgs);
}

} // namespace NSQLTranslationV0
