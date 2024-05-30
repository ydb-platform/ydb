#include "node.h"
#include "context.h"

#include "list_builtin.h"
#include "match_recognize.h"

#include <ydb/library/yql/ast/yql_type_string.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/core/sql_types/simple_types.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/public/issue/yql_issue_id.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <library/cpp/charset/ci_string.h>
#include <library/cpp/yson/node/node_io.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/util.h>
#include <util/string/join.h>
#include <util/system/env.h>

#include <unordered_map>

using namespace NYql;

namespace NSQLTranslationV1 {

extern const char SubqueryExtendFor[] = "SubqueryExtendFor";
extern const char SubqueryUnionAllFor[] = "SubqueryUnionAllFor";
extern const char SubqueryMergeFor[] = "SubqueryMergeFor";
extern const char SubqueryUnionMergeFor[] = "SubqueryUnionMergeFor";
extern const char SubqueryOrderBy[] = "SubqueryOrderBy";
extern const char SubqueryAssumeOrderBy[] = "SubqueryAssumeOrderBy";

TNodePtr MakeTypeConfig(const TPosition& pos, const TString& ns, const TVector<TNodePtr>& udfArgs) {
    if (ns == "clickhouse") {
        auto settings = NYT::TNode::CreateMap();
        auto args = NYT::TNode::CreateMap();
        for (ui32 i = 0; i < udfArgs.size(); ++i) {
            if (!udfArgs[i]->IsNull() && udfArgs[i]->IsLiteral()) {
                args[ToString(i)] = NYT::TNode()
                    ("type", udfArgs[i]->GetLiteralType())
                    ("value", udfArgs[i]->GetLiteralValue());
            }
        }

        settings["args"] = args;
        return (TDeferredAtom(pos, NYT::NodeToYsonString(settings))).Build();
    }

    return nullptr;
}

void AdjustCheckedAggFuncName(TString& aggNormalizedName, TContext& ctx) {
    if (!ctx.Scoped->PragmaCheckedOps) {
        return;
    }

    if (aggNormalizedName == "sum") {
        aggNormalizedName = "checked_sum";
    } else if (aggNormalizedName == "sumif") {
        aggNormalizedName = "checked_sumif";
    }
}

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
        const bool isJoin = src->GetJoin();
        ISource* composite = src->GetCompositeSource();
        for (const auto& node: Args) {
            auto namePtr = node->GetColumnName();
            if (!namePtr || !*namePtr) {
                ctx.Error(Pos) << "GROUPING function should use columns as arguments";
                return false;
            }
            TString column = *namePtr;
            if (isJoin) {
                auto sourceNamePtr = node->GetSourceName();
                if (sourceNamePtr && !sourceNamePtr->empty()) {
                    column = DotJoin(*sourceNamePtr, column);
                }
            }

            if (!src->IsGroupByColumn(column) && !src->IsAlias(EExprSeat::GroupBy, *namePtr) && (!composite || !composite->IsGroupByColumn(column))) {
                ctx.Error(node->GetPos()) << "Column '" << column << "' is not a grouping column";
                return false;
            }
            columns.emplace_back(column);
        }
        TString groupingColumn;
        if (!src->AddGrouping(ctx, columns, groupingColumn)) {
            return false;
        }
        Nodes.push_back(BuildAtom(Pos, "Member"));
        Nodes.push_back(BuildAtom(Pos, "row"));
        Nodes.push_back(BuildQuotedAtom(Pos, groupingColumn));
        return TAstListNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TGroupingNode(Pos, CloneContainer(Args));
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

    void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override {
        if (Args.empty() || Aggr->GetAggregationMode() != EAggregateMode::Distinct) {
            return;
        }

        auto& expr = Args.front();

        // need to initialize expr before checking whether it is a column
        auto clone = expr->Clone();
        if (!clone->Init(ctx, &src)) {
            return;
        }

        const auto column = clone->GetColumnName();
        if (column) {
            return;
        }

        auto tmpColumn = src.MakeLocalName("_yql_preagg_" + Name);
        YQL_ENSURE(!expr->GetLabel());
        expr->SetLabel(tmpColumn);

        PreaggregateExpr = expr;
        exprs.push_back(PreaggregateExpr);
        expr = BuildColumn(expr->GetPos(), tmpColumn);

        Aggr->MarkKeyColumnAsGenerated();
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
        if (PreaggregateExpr) {
            YQL_ENSURE(PreaggregateExpr->HasState(ENodeState::Initialized));
            if (PreaggregateExpr->IsAggregated() && !PreaggregateExpr->IsAggregationKey() && !Aggr->IsOverWindow()) {
                ctx.Error(Aggr->GetPos()) << "Aggregation of aggregated values is forbidden";
                return false;
            }
        }

        if (!Aggr->InitAggr(ctx, false, src, *this, Args)) {
            return false;
        }
        return src->AddAggregation(ctx, Aggr);
    }

    void DoUpdateState() const final {
        State.Set(ENodeState::Const, !Args.empty() && AllOf(Args, [](const auto& arg){ return arg->IsConstant(); }));
        State.Set(ENodeState::Aggregated);
    }

    TNodePtr PreaggregateExpr;
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
    TLiteralStringAtom(TPosition pos, TNodePtr node, const TString& info, const TString& prefix = {})
        : INode(pos)
        , Node(node)
        , Info(info)
        , Prefix(prefix)
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

        Atom = MakeAtomFromExpression(Pos, ctx, Node, Prefix).Build();
        return true;
    }

    bool IsLiteral() const override {
        return Atom ? Atom->IsLiteral() : false;
    }

    TString GetLiteralType() const override {
        return Atom ? Atom->GetLiteralType() : "";
    }

    TString GetLiteralValue() const override {
        return Atom ? Atom->GetLiteralValue() : "";
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Atom->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TLiteralStringAtom(GetPos(), SafeClone(Node), Info, Prefix);
    }

    void DoUpdateState() const override {
        YQL_ENSURE(Atom);
        State.Set(ENodeState::Const, Atom->IsConstant());
        State.Set(ENodeState::Aggregated, Atom->IsAggregated());
        State.Set(ENodeState::OverWindow, Atom->IsOverWindow());
    }
private:
    TNodePtr Node;
    TNodePtr Atom;
    TString Info;
    TString Prefix;
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
            case NUdf::EDataSlot::Date32:
                value = ToString(out.Get<i32>());
                break;
            case NUdf::EDataSlot::Datetime:
            case NUdf::EDataSlot::TzDatetime:
                value = ToString(out.Get<ui32>());
                break;
            case NUdf::EDataSlot::Timestamp:
            case NUdf::EDataSlot::TzTimestamp:
                value = ToString(out.Get<ui64>());
                break;
            case NUdf::EDataSlot::Datetime64:
            case NUdf::EDataSlot::Timestamp64:
                value = ToString(out.Get<i64>());
                break;
            case NUdf::EDataSlot::Interval:
            case NUdf::EDataSlot::Interval64:
                value = ToString(out.Get<i64>());
                if ('T' == atom->back()) {
                    ctx.Error(Pos) << "Time prefix 'T' at end of interval constant. The designator 'T' shall be absent if all of the time components are absent.";
                    return false;
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
    TTableName(TPosition pos, const TVector<TNodePtr>& args, const TString& service)
        : TCallNode(pos, "TableName", 0, 2, args)
        , Service(service)
        , EmptyArgs(args.empty())
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (Args.empty()) {
            if (!src) {
                ctx.Error(Pos) << "Unable to use TableName() without source";
                return false;
            }

            // TODO: TablePath() and TableRecordIndex() have more strict limitations
            if (src->GetJoin()) {
                ctx.Warning(Pos,
                    TIssuesIds::YQL_EMPTY_TABLENAME_RESULT) << "TableName() may produce empty result when used in ambiguous context (with JOIN)";
            }

            if (src->HasAggregations()) {
                ctx.Warning(Pos,
                    TIssuesIds::YQL_EMPTY_TABLENAME_RESULT) << "TableName() will produce empty result when used with aggregation.\n"
                                                               "Please consult documentation for possible workaround";
            }

            Args.push_back(Y("TablePath", Y("DependsOn", "row")));
        }

        if (Args.size() == 2) {
            auto literal = Args[1]->GetLiteral("String");
            if (!literal) {
                ctx.Error(Args[1]->GetPos()) << "Expected literal string as second argument in TableName function";
                return false;
            }

            Args[1] = BuildQuotedAtom(Args[1]->GetPos(), *literal);
        } else {
            if (Service.empty()) {
                ctx.Error(GetPos()) << GetOpName() << " requires either service name as second argument or current cluster name";
                return false;
            }

            Args.push_back(BuildQuotedAtom(GetPos(), Service));
        }

        return TCallNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TTableName(GetPos(), CloneContainer(Args), Service);
    }

    void DoUpdateState() const override {
        if (EmptyArgs) {
            State.Set(ENodeState::Const, false);
        } else {
            TCallNode::DoUpdateState();
        }
    }

private:
    TString Service;
    const bool EmptyArgs;
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
        return new TYqlParseType(Pos, CloneContainer(Args));
    }

    void DoUpdateState() const final {
        State.Set(ENodeState::Const);
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

class TYqlPgType: public TCallNode {
public:
    TYqlPgType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "PgType", 1, 1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        ui32 oid;
        if (Args[0]->IsIntegerLiteral() && TryFromString<ui32>(Args[0]->GetLiteralValue(), oid)) {
            if (!NPg::HasType(oid)) {
                ctx.Error(Args[0]->GetPos()) << "Unknown pg type oid: " << oid;
                return false;
            } else {
                Args[0] = BuildQuotedAtom(Args[0]->GetPos(), NPg::LookupType(oid).Name);
            }
        } else if (Args[0]->IsLiteral() && Args[0]->GetLiteralType() == "String") {
            if (!NPg::HasType(Args[0]->GetLiteralValue())) {
                ctx.Error(Args[0]->GetPos()) << "Unknown pg type: " << Args[0]->GetLiteralValue();
                return false;
            } else {
                Args[0] = BuildQuotedAtom(Args[0]->GetPos(), Args[0]->GetLiteralValue());
            }
        } else {
            ctx.Error(Args[0]->GetPos()) << "Expecting string literal with pg type name or integer literal with pg type oid";
            return false;
        }

        return TCallNode::DoInit(ctx, src);
    }


    TNodePtr DoClone() const final {
        return new TYqlPgType(Pos, CloneContainer(Args));
    }
};

class TYqlPgConst : public TCallNode {
public:
    TYqlPgConst(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "PgConst", 2, -1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[0]->Init(ctx, src)) {
            return false;
        }

        if (Args[0]->IsLiteral()) {
            Args[0] = BuildQuotedAtom(Args[0]->GetPos(), Args[0]->GetLiteralValue());
        } else {
            auto value = MakeAtomFromExpression(Pos, ctx, Args[0]).Build();
            Args[0] = value;
        }

        if (Args.size() > 2) {
            TVector<TNodePtr> typeModArgs;
            typeModArgs.push_back(Args[1]);
            for (ui32 i = 2; i < Args.size(); ++i) {
                if (!Args[i]->IsLiteral()) {
                    ctx.Error(Args[i]->GetPos()) << "Expecting literal";
                    return false;
                }

                typeModArgs.push_back(BuildQuotedAtom(Args[i]->GetPos(), Args[i]->GetLiteralValue()));
            }

            Args.erase(Args.begin() + 2, Args.end());
            Args.push_back(new TCallNodeImpl(Pos, "PgTypeMod", typeModArgs));
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlPgConst(Pos, CloneContainer(Args));
    }
};

class TYqlPgCast : public TCallNode {
public:
    TYqlPgCast(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "PgCast", 2, -1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (Args.size() > 2) {
            TVector<TNodePtr> typeModArgs;
            typeModArgs.push_back(Args[1]);
            for (ui32 i = 2; i < Args.size(); ++i) {
                if (!Args[i]->IsLiteral()) {
                    ctx.Error(Args[i]->GetPos()) << "Expecting literal";
                    return false;
                }

                typeModArgs.push_back(BuildQuotedAtom(Args[i]->GetPos(), Args[i]->GetLiteralValue()));
            }

            Args.erase(Args.begin() + 2, Args.end());
            Args.push_back(new TCallNodeImpl(Pos, "PgTypeMod", typeModArgs));
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlPgCast(Pos, CloneContainer(Args));
    }
};

class TYqlPgOp : public TCallNode {
public:
    TYqlPgOp(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "PgOp", 2, 3, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[0]->Init(ctx, src)) {
            return false;
        }

        if (!Args[0]->IsLiteral() || Args[0]->GetLiteralType() != "String") {
            ctx.Error(Args[0]->GetPos()) << "Expecting string literal as first argument";
            return false;
        }

        Args[0] = BuildQuotedAtom(Args[0]->GetPos(), Args[0]->GetLiteralValue());
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlPgOp(Pos, CloneContainer(Args));
    }
};

template <bool RangeFunction>
class TYqlPgCall : public TCallNode {
public:
    TYqlPgCall(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "PgCall", 1, -1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[0]->Init(ctx, src)) {
            return false;
        }

        if (!Args[0]->IsLiteral() || Args[0]->GetLiteralType() != "String") {
            ctx.Error(Args[0]->GetPos()) << "Expecting string literal as first argument";
            return false;
        }

        Args[0] = BuildQuotedAtom(Args[0]->GetPos(), Args[0]->GetLiteralValue());
        Args.insert(Args.begin() + 1, RangeFunction ? Q(Y(Q(Y(Q("range"))))) : Q(Y()));
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlPgCall<RangeFunction>(Pos, CloneContainer(Args));
    }
};

template <const char* Name>
class TYqlSubqueryFor : public TCallNode {
public:
    TYqlSubqueryFor(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, Name, 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        Args[0] = Y("EvaluateExpr", Args[0]);
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlSubqueryFor<Name>(Pos, CloneContainer(Args));
    }
};

template <const char* Name>
class TYqlSubqueryOrderBy : public TCallNode {
public:
    TYqlSubqueryOrderBy(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, Name, 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        Args[1] = Y("EvaluateExpr", Args[1]);
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlSubqueryOrderBy<Name>(Pos, CloneContainer(Args));
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

        if (!Args[1]->Init(ctx, src)) {
            return false;
        }
        if (Args.size() == 3) {
            if (!Args[2]->Init(ctx, src)) {
                return false;
            }

            auto message = MakeAtomFromExpression(Pos, ctx, Args[2]).Build();
            Args[2] = message;
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTypeAssert<Strict>(Pos, CloneContainer(Args));
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
        Args[1] = MakeAtomFromExpression(Pos, ctx, Y("FormatType", Args[1])).Build();

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TFromBytes(Pos, CloneContainer(Args));
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

        Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }
};

class TYqlAsTagged final : public TYqlTaggedBase {
public:
    TYqlAsTagged(TPosition pos, const TVector<TNodePtr>& args)
        : TYqlTaggedBase(pos, "AsTagged", args)
    {}

    TNodePtr DoClone() const final {
        return new TYqlAsTagged(Pos, CloneContainer(Args));
    }
};

class TYqlUntag final : public TYqlTaggedBase {
public:
    TYqlUntag(TPosition pos, const TVector<TNodePtr>& args)
        : TYqlTaggedBase(pos, "Untag", args)
    {}

    TNodePtr DoClone() const final {
        return new TYqlUntag(Pos, CloneContainer(Args));
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

        Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlVariant(Pos, CloneContainer(Args));
    }
};

class TYqlEnum final : public TCallNode {
public:
    TYqlEnum(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "Enum", 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[0]->Init(ctx, src)) {
            return false;
        }

        Args[0] = MakeAtomFromExpression(Pos, ctx, Args[0]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlEnum(Pos, CloneContainer(Args));
    }
};

class TYqlAsVariant final : public TCallNode {
public:
    TYqlAsVariant(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "AsVariant", 2, 2, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[1]->Init(ctx, src)) {
            return false;
        }

        Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlAsVariant(Pos, CloneContainer(Args));
    }
};

class TYqlAsEnum final : public TCallNode {
public:
    TYqlAsEnum(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "AsEnum", 1, 1, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[0]->Init(ctx, src)) {
            return false;
        }

        Args[0] = MakeAtomFromExpression(Pos, ctx, Args[0]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlAsEnum(Pos, CloneContainer(Args));
    }
};

TNodePtr BuildFileNameArgument(TPosition pos, const TNodePtr& argument, const TString& prefix) {
    return new TLiteralStringAtom(pos, argument, "FilePath requires string literal as parameter", prefix);
}

template <typename TDerived, bool IsFile>
class TYqlAtomBase: public TCallNode {
public:
    TYqlAtomBase(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 1, 1, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Args.empty()) {
            Args[0] = BuildFileNameArgument(Pos, Args[0], IsFile ? ctx.Settings.FileAliasPrefix : TString());
        }
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TDerived(Pos, OpName, CloneContainer(Args));
    }

    bool IsLiteral() const override {
        return !Args.empty() ? Args[0]->IsLiteral() : false;
    }

    TString GetLiteralType() const override {
        return !Args.empty() ? Args[0]->GetLiteralType() : "";
    }

    TString GetLiteralValue() const override {
        return !Args.empty() ? Args[0]->GetLiteralValue() : "";
    }
};

class TYqlAtom final : public TYqlAtomBase<TYqlAtom, false>
{
    using TBase = TYqlAtomBase<TYqlAtom, false>;
    using TBase::TBase;
};

class TFileYqlAtom final : public TYqlAtomBase<TFileYqlAtom, true>
{
    using TBase = TYqlAtomBase<TFileYqlAtom, true>;
    using TBase::TBase;
};

class TTryMember final: public TCallNode {
public:
    TTryMember(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
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
        Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TTryMember(Pos, OpName, CloneContainer(Args));
    }
};

template<bool Pretty>
class TFormatTypeDiff final: public TCallNode {
public:
    TFormatTypeDiff(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 3, 3, args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args.size() != 2) {
            ctx.Error(Pos) << OpName << " requires exactly 2 arguments";
            return false;
        }
        for (const auto& arg : Args) {
            if (!arg->Init(ctx, src)) {
                return false;
            }
        }
        Args.push_back(Q(Pretty ? "true" : "false"));
        OpName = "FormatTypeDiff";
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TFormatTypeDiff<Pretty>(GetPos(), OpName, CloneContainer(Args));
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
        Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TAddMember(Pos, OpName, CloneContainer(Args));
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
        Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TRemoveMember(Pos, OpName, CloneContainer(Args));
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
        return new TCombineMembers(Pos, OpName, CloneContainer(Args));
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
                    MakeAtomFromExpression(Pos, ctx, Args[i]->GetTupleElement(0)).Build(),
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
        return new TFlattenMembers(Pos, OpName, CloneContainer(Args));
    }
};

TString NormalizeTypeString(const TString& str) {
    auto ret = to_title(str);
    if (ret.StartsWith("Tz")) {
        ret = "Tz" + to_title(ret.substr(2));
    }
    if (ret.StartsWith("Json")) {
        ret = "Json" + to_title(ret.substr(4));
    }
    if (ret.StartsWith("Dy")) {
        ret = "Dy" + to_title(ret.substr(2));
    }

    return ret;
}

static const TSet<TString> AvailableDataTypes = {"Bool", "String", "Uint32", "Uint64", "Int32", "Int64", "Float", "Double", "Utf8", "Yson", "Json", "JsonDocument",
    "Date", "Datetime", "Timestamp", "Interval", "Uint8", "Int8", "Uint16", "Int16", "TzDate", "TzDatetime", "TzTimestamp", "Uuid", "Decimal", "DyNumber",
    "Date32", "Datetime64", "Timestamp64", "Interval64", };
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
        auto aliasNode = BuildFileNameArgument(Args[1]->GetPos(), Args[1], ctx.Settings.FileAliasPrefix);
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
        return new TYqlParseFileOp(Pos, CloneContainer(Args));
    }
};

class TYqlDataType final : public TCallNode {
public:
    TYqlDataType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "DataType", 1, 3, args)
    {
        FakeSource = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        for (ui32 i = 0; i < Args.size(); ++i) {
            if (!Args[i]->Init(ctx, FakeSource.Get())) {
               return false;
            }

            Args[i] = MakeAtomFromExpression(Pos, ctx, Args[i]).Build();
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlDataType(Pos, CloneContainer(Args));
    }

private:
    TSourcePtr FakeSource;
};

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

        Args[0] = MakeAtomFromExpression(Pos, ctx, Args[0]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlResourceType(Pos, CloneContainer(Args));
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

        Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTaggedType(Pos, CloneContainer(Args));
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
        return new TYqlCallableType(Pos, CloneContainer(Args));
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

        Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTupleElementType(Pos, CloneContainer(Args));
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

        Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlStructMemberType(Pos, CloneContainer(Args));
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
        return new TYqlCallableArgumentType(Pos, CloneContainer(Args));
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
    TYqlSubstring(TPosition pos, const TString& name, const TVector<TNodePtr>& args)
        : TCallNode(pos, name, 2, 3, args)
    {}

private:
    TCallNode::TPtr DoClone() const override {
       return new TYqlSubstring(GetPos(), OpName, CloneContainer(Args));
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args.size() == 2) {
            Args.push_back(Y("Null"));
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

        const auto pos = inNode->GetPos();

        if (!key->Init(ctx, src)) {
            return false;
        }

        if (!inNode->Init(ctx, inNode->GetSource() ? nullptr : src)) {
            return false;
        }

        if (inNode->GetLiteral("String")) {
            ctx.Error(pos) << "Unable to use IN predicate with string argument, it won't search substring - "
                              "expecting tuple, list, dict or single column table source";
            return false;
        }

        if (inNode->GetTupleSize() == 1) {
            auto singleElement = inNode->GetTupleElement(0);
            // TODO: 'IN ((select ...))' is parsed exactly like 'IN (select ...)' instead of a single element tuple
            if (singleElement->GetSource() || singleElement->IsSelect()) {
                TStringBuf parenKind = singleElement->GetSource() ? "" : "external ";
                ctx.Warning(pos,
                            TIssuesIds::YQL_CONST_SUBREQUEST_IN_LIST) << "Using subrequest in scalar context after IN, "
                                                                      << "perhaps you should remove "
                                                                      << parenKind << "parenthesis here";
            }
        }

        TVector<TNodePtr> hintElements;
        for (size_t i = 0; i < hints->GetTupleSize(); ++i) {
            hintElements.push_back(hints->GetTupleElement(i));
        }

        if (inNode->GetSource() || inNode->IsSelect()) {
            hintElements.push_back(BuildHint(pos, "tableSource"));
        }

        if (!ctx.AnsiInForEmptyOrNullableItemsCollections.Defined()) {
            hintElements.push_back(BuildHint(pos, "warnNoAnsi"));
        } else if (*ctx.AnsiInForEmptyOrNullableItemsCollections) {
            hintElements.push_back(BuildHint(pos, "ansi"));
        }

        OpName = "SqlIn";
        MinArgs = MaxArgs = 3;
        Args = {
            inNode->GetSource() ? inNode->GetSource() : inNode,
            key,
            BuildTuple(pos, hintElements)
        };

        return TCallNode::DoInit(ctx, src);
    }

    static TNodePtr BuildHint(TPosition pos, const TString& name) {
        return BuildTuple(pos, { BuildQuotedAtom(pos, name, NYql::TNodeFlags::Default) });
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

        Args[3] = MakeAtomFromExpression(Pos, ctx, Args[3]).Build();
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

        bool hasError = false;
        for (auto& arg: Args) {
            if (!arg->Init(ctx, src)) {
                hasError = true;
                continue;
            }
        }

        if (hasError) {
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
            ctx.Error(Pos) << GetCallExplain() << " expects column name as first argument";
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
        return new TWeakFieldOp(Pos, CloneContainer(Args));
    }
};

template <bool Join>
class TTableRow final : public INode {
public:
    TTableRow(TPosition pos, const TVector<TNodePtr>& args)
        : TTableRow(pos, args.size())
    {}

    TTableRow(TPosition pos, ui32 argsCount)
        : INode(pos)
        , ArgsCount(argsCount)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!src || src->IsFake()) {
            ctx.Error(Pos) << "TableRow requires data source";
            return false;
        }

        if (ArgsCount > 0) {
            ctx.Error(Pos) << "TableRow requires exactly 0 arguments";
            return false;
        }

        src->AllColumns();
        const bool isJoin = src->GetJoin();
        if (!Join && ctx.SimpleColumns && isJoin) {
            TNodePtr block = Y();
            const auto& sameKeyMap = src->GetJoin()->GetSameKeysMap();
            if (sameKeyMap) {
                block = L(block, Y("let", "flatSameKeys", "row"));
                for (const auto& sameKeysPair: sameKeyMap) {
                    const auto& column = sameKeysPair.first;
                    auto keys = Y("Coalesce");
                    auto sameSourceIter = sameKeysPair.second.begin();
                    for (auto end = sameKeysPair.second.end(); sameSourceIter != end; ++sameSourceIter) {
                        auto addKeyNode = Q(DotJoin(*sameSourceIter, column));
                        keys = L(keys, Y("TryMember", "row", addKeyNode, Y("Null")));
                    }

                    block = L(block, Y("let", "flatSameKeys", Y("AddMember", "flatSameKeys", Q(column), keys)));
                    sameSourceIter = sameKeysPair.second.begin();
                    for (auto end = sameKeysPair.second.end(); sameSourceIter != end; ++sameSourceIter) {
                        auto removeKeyNode = Q(DotJoin(*sameSourceIter, column));
                        block = L(block, Y("let", "flatSameKeys", Y("ForceRemoveMember", "flatSameKeys", removeKeyNode)));
                    }
                }
                block = L(block, Y("let", "row", "flatSameKeys"));
            }

            auto members = Y();
            for (auto& joinLabel: src->GetJoin()->GetJoinLabels()) {
                members = L(members, BuildQuotedAtom(Pos, joinLabel + "."));
            }
            block = L(block, Y("let", "res", Y("DivePrefixMembers", "row", Q(members))));

            for (const auto& sameKeysPair: src->GetJoin()->GetSameKeysMap()) {
                const auto& column = sameKeysPair.first;
                auto addMemberKeyNode = Y("Member", "row", Q(column));
                block = L(block, Y("let", "res", Y("AddMember", "res", Q(column), addMemberKeyNode)));
            }

            Node = Y("block", Q(L(block, Y("return", "res"))));
        } else {
            Node = ctx.EnableSystemColumns ? Y("RemoveSystemMembers", "row") : BuildAtom(Pos, "row", 0);
        }
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node);
        return Node->Translate(ctx);
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, false);
    }

    TNodePtr DoClone() const final {
        return new TTableRow<Join>(Pos, ArgsCount);
    }

private:
    const size_t ArgsCount;
    TNodePtr Node;
};

TTableRows::TTableRows(TPosition pos, const TVector<TNodePtr>& args)
    : TTableRows(pos, args.size())
{}

TTableRows::TTableRows(TPosition pos, ui32 argsCount)
    : INode(pos)
    , ArgsCount(argsCount)
{}

bool TTableRows::DoInit(TContext& ctx, ISource* /*src*/) {
    if (ArgsCount > 0) {
        ctx.Error(Pos) << "TableRows requires exactly 0 arguments";
        return false;
    }
    Node = ctx.EnableSystemColumns ? Y("RemoveSystemMembers", "inputRowsList") : BuildAtom(Pos, "inputRowsList", 0);
    return true;
}

TAstNode* TTableRows::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(Node);
    return Node->Translate(ctx);
}

void TTableRows::DoUpdateState() const {
    State.Set(ENodeState::Const, false);
}

TNodePtr TTableRows::DoClone() const {
    return MakeIntrusive<TTableRows>(Pos, ArgsCount);
}

TSessionWindow::TSessionWindow(TPosition pos, const TVector<TNodePtr>& args)
    : INode(pos)
    , Args(args)
    , FakeSource(BuildFakeSource(pos))
    , Valid(false)
{}

void TSessionWindow::MarkValid() {
    YQL_ENSURE(!HasState(ENodeState::Initialized));
    Valid = true;
}

TNodePtr TSessionWindow::BuildTraits(const TString& label) const {
    YQL_ENSURE(HasState(ENodeState::Initialized));

    auto trueNode = Y("Bool", Q("true"));

    if (Args.size() == 2) {
        auto timeExpr = Args[0];
        auto timeoutExpr = Args[1];

        auto coalesceLess = [&](auto first, auto second) {
            // first < second ?? true
            return Y("Coalesce", Y("<", first, second), trueNode);
        };

        auto absDelta = Y("If",
            coalesceLess("prev", "curr"),
            Y("-", "curr", "prev"),
            Y("-", "prev", "curr"));

        auto newSessionPred = Y("And", Y("AggrNotEquals", "curr", "prev"), coalesceLess(timeoutExpr, absDelta));
        auto timeoutLambda = BuildLambda(timeoutExpr->GetPos(), Y("prev", "curr"), newSessionPred);
        auto sortSpec = Y("SortTraits", Y("TypeOf", label), trueNode, BuildLambda(Pos, Y("row"), Y("PersistableRepr", timeExpr)));

        return Y("SessionWindowTraits",
            Y("TypeOf", label),
            sortSpec,
            BuildLambda(Pos, Y("row"), timeExpr),
            timeoutLambda);
    }

    auto orderExpr = Args[0];
    auto initLambda = Args[1];
    auto updateLambda = Args[2];
    auto calculateLambda = Args[3];

    auto sortSpec = Y("SortTraits", Y("TypeOf", label), trueNode, BuildLambda(Pos, Y("row"), Y("PersistableRepr", orderExpr)));

    return Y("SessionWindowTraits",
        Y("TypeOf", label),
        sortSpec,
        initLambda,
        updateLambda,
        calculateLambda);
}

bool TSessionWindow::DoInit(TContext& ctx, ISource* src) {
    if (!src || src->IsFake()) {
        ctx.Error(Pos) << "SessionWindow requires data source";
        return false;
    }

    if (!(Args.size() == 2 || Args.size() == 4)) {
        ctx.Error(Pos) << "SessionWindow requires either two or four arguments";
        return false;
    }

    if (!Valid) {
        ctx.Error(Pos) << "SessionWindow can only be used as a top-level GROUP BY / PARTITION BY expression";
        return false;
    }

    if (Args.size() == 2) {
        auto timeExpr = Args[0];
        auto timeoutExpr = Args[1];
        return timeExpr->Init(ctx, src) && timeoutExpr->Init(ctx, FakeSource.Get());
    }

    auto orderExpr = Args[0];
    auto initLambda = Args[1];
    auto updateLambda = Args[2];
    auto calculateLambda = Args[3];
    src->AllColumns();

    return orderExpr->Init(ctx, src) && initLambda->Init(ctx, FakeSource.Get()) &&
        updateLambda->Init(ctx, FakeSource.Get()) && calculateLambda->Init(ctx, FakeSource.Get());
}

TAstNode* TSessionWindow::Translate(TContext&) const {
    YQL_ENSURE(false, "Translate is called for SessionWindow");
    return nullptr;
}

void TSessionWindow::DoUpdateState() const {
    State.Set(ENodeState::Const, false);
}

TNodePtr TSessionWindow::DoClone() const {
    return new TSessionWindow(Pos, CloneContainer(Args));
}

TString TSessionWindow::GetOpName() const {
    return "SessionWindow";
}

template<bool IsStart>
class TSessionStart final : public INode {
public:
    TSessionStart(TPosition pos, const TVector<TNodePtr>& args)
        : INode(pos)
        , ArgsCount(args.size())
    {
    }
private:
    TSessionStart(TPosition pos, size_t argsCount)
        : INode(pos)
        , ArgsCount(argsCount)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!src || src->IsFake()) {
            ctx.Error(Pos) << GetOpName() << " requires data source";
            return false;
        }

        if (ArgsCount > 0) {
            ctx.Error(Pos) << GetOpName() << " requires exactly 0 arguments";
            return false;
        }

        auto windowName = src->GetWindowName();
        OverWindow = windowName != nullptr;
        TNodePtr sessionWindow;
        if (windowName) {
            auto spec = src->FindWindowSpecification(ctx, *windowName);
            if (!spec) {
                return false;
            }
            sessionWindow = spec->Session;
            if (!sessionWindow) {
                ctx.Error(Pos) << GetOpName() << " can not be used with window " << *windowName << ": SessionWindow specification is missing in PARTITION BY";
                return false;
            }
        } else {
            sessionWindow = src->GetSessionWindowSpec();
            if (!sessionWindow) {
                TString extra;
                if (src->IsOverWindowSource()) {
                    extra = ". Maybe you forgot to add OVER `window_name`?";
                }
                if (src->HasAggregations()) {
                    ctx.Error(Pos) << GetOpName() << " can not be used here: SessionWindow specification is missing in GROUP BY" << extra;
                } else {
                    ctx.Error(Pos) << GetOpName() << " can not be used without aggregation by SessionWindow" << extra;
                }
                return false;
            }

            if (!IsStart) {
                ctx.Error(Pos) << GetOpName() << " with GROUP BY is not supported yet";
                return false;
            }
        }

        if (sessionWindow->HasState(ENodeState::Failed)) {
            return false;
        }

        YQL_ENSURE(sessionWindow->HasState(ENodeState::Initialized));
        YQL_ENSURE(sessionWindow->GetLabel());
        Node = Y("Member", "row", BuildQuotedAtom(Pos, sessionWindow->GetLabel()));
        if (OverWindow) {
            Node = Y("Member", Node, BuildQuotedAtom(Pos, IsStart ? "start" : "state"));
        }
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node);
        return Node->Translate(ctx);
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, false);
        if (OverWindow) {
            State.Set(ENodeState::OverWindow, true);
        } else if (IsStart) {
            State.Set(ENodeState::Aggregated, true);
        }
    }

    TNodePtr DoClone() const override {
        return new TSessionStart<IsStart>(Pos, ArgsCount);
    }

    TString GetOpName() const override {
        return IsStart ? "SessionStart" : "SessionState";
    }

    const size_t ArgsCount;
    bool OverWindow = false;
    TNodePtr Node;
};

THoppingWindow::THoppingWindow(TPosition pos, const TVector<TNodePtr>& args)
    : INode(pos)
    , Args(args)
    , FakeSource(BuildFakeSource(pos))
    , Valid(false)
{}

void THoppingWindow::MarkValid() {
    YQL_ENSURE(!HasState(ENodeState::Initialized));
    Valid = true;
}

TNodePtr THoppingWindow::BuildTraits(const TString& label) const {
    YQL_ENSURE(HasState(ENodeState::Initialized));

    return Y(
        "HoppingTraits",
        Y("ListItemType", Y("TypeOf", label)),
        BuildLambda(Pos, Y("row"), Y("Just", Y("SystemMetadata", Y("String", Q("write_time")), Y("DependsOn", "row")))),
        Hop,
        Interval,
        Interval,
        Q("true"),
        Q("v2"));
}

bool THoppingWindow::DoInit(TContext& ctx, ISource* src) {
    if (!src || src->IsFake()) {
        ctx.Error(Pos) << "HoppingWindow requires data source";
        return false;
    }

    if (!(Args.size() == 2)) {
        ctx.Error(Pos) << "HoppingWindow requires two arguments";
        return false;
    }

    if (!Valid) {
        ctx.Error(Pos) << "HoppingWindow can only be used as a top-level GROUP BY expression";
        return false;
    }

    auto hopExpr = Args[0];
    auto intervalExpr = Args[1];
    if (!(hopExpr->Init(ctx, FakeSource.Get()) && intervalExpr->Init(ctx, FakeSource.Get()))) {
        return false;
    }

    Hop = ProcessIntervalParam(hopExpr);
    Interval = ProcessIntervalParam(intervalExpr);

    return true;
}

TAstNode* THoppingWindow::Translate(TContext&) const {
    YQL_ENSURE(false, "Translate is called for HoppingWindow");
    return nullptr;
}

void THoppingWindow::DoUpdateState() const {
    State.Set(ENodeState::Const, false);
}

TNodePtr THoppingWindow::DoClone() const {
    return new THoppingWindow(Pos, CloneContainer(Args));
}

TString THoppingWindow::GetOpName() const {
    return "HoppingWindow";
}

TNodePtr THoppingWindow::ProcessIntervalParam(const TNodePtr& node) const {
    auto literal = node->GetLiteral("String");
    if (!literal) {
        return Y("EvaluateExpr", node);
    }

    return new TYqlData(node->GetPos(), "Interval", {node});
}

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
        TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr customUserType, TNodePtr typeConfig) {
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

    if (typeConfig) {
        udfArgs.push_back(typeConfig);
    }

    return udfArgs;
}

TNodePtr BuildSqlCall(TContext& ctx, TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args,
    TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr customUserType, const TDeferredAtom& typeConfig, TNodePtr runConfig)
{
    const TString fullName = module + "." + name;
    TNodePtr callable;
    if (to_lower(module) == "@yql") {
        callable = BuildCallable(pos, module, name, {});
    } else if (!ctx.Settings.EnableGenericUdfs) {
        auto varName = ctx.AddSimpleUdf(fullName);
        callable = new TAstAtomNodeImpl(pos, varName, TNodeFlags::ArbitraryContent);
    }

    if (callable) {
        TVector<TNodePtr> applyArgs = { callable };
        applyArgs.insert(applyArgs.end(), args.begin(), args.end());
        return new TCallNodeImpl(pos, namedArgs ? "NamedApply" : "Apply", applyArgs);
    }

    TVector<TNodePtr> sqlCallArgs;
    sqlCallArgs.push_back(BuildQuotedAtom(pos, fullName));
    if (namedArgs) {
        auto tupleNodePtr = dynamic_cast<const TTupleNode*>(positionalArgs.Get());
        YQL_ENSURE(tupleNodePtr);
        TNodePtr positionalArgsNode = new TCallNodeImpl(pos, "PositionalArgs", tupleNodePtr->Elements());
        sqlCallArgs.push_back(BuildTuple(pos, { positionalArgsNode, namedArgs }));
    } else {
        TNodePtr positionalArgsNode = new TCallNodeImpl(pos, "PositionalArgs", args);
        sqlCallArgs.push_back(BuildTuple(pos, { positionalArgsNode }));
    }

    // optional arguments
    if (customUserType) {
        sqlCallArgs.push_back(customUserType);
    } else if (!typeConfig.Empty()) {
        sqlCallArgs.push_back(new TCallNodeImpl(pos, "TupleType", {}));
    }

    if (!typeConfig.Empty()) {
        sqlCallArgs.push_back(typeConfig.Build());
    } else if (runConfig) {
        sqlCallArgs.push_back(BuildQuotedAtom(pos, ""));
    }

    if (runConfig) {
        sqlCallArgs.push_back(runConfig);
    }

    return new TCallNodeImpl(pos, "SqlCall", sqlCallArgs);
}

class TCallableNode final: public INode {
public:
    TCallableNode(TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args, bool forReduce)
        : INode(pos)
        , Module(module)
        , Name(name)
        , Args(args)
        , ForReduce(forReduce)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Module == "yql") {
            Node = new TFuncNodeImpl(Pos, Name);
        } else if (Module == "@yql") {
            auto parsedName = StringContent(ctx, Pos, Name);
            if (!parsedName) {
                return false;
            }

            const TString yql("(" + parsedName->Content + ")");
            TAstParseResult ast = ParseAst(yql, ctx.Pool.get());
            /// TODO: do not drop warnings
            if (ast.IsOk()) {
                const auto rootCount = ast.Root->GetChildrenCount();
                if (rootCount != 1) {
                    ctx.Error(Pos) << "Failed to parse YQL: expecting AST root node with single child, but got " << rootCount;
                    return false;
                }
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

            if ("Datetime" == Module || ("Yson" == Module && ctx.PragmaYsonFast))
                Module.append('2');

            TNodePtr typeConfig = MakeTypeConfig(Pos, to_lower(Module), Args);
            if (ForReduce) {
                TVector<TNodePtr> udfArgs;
                udfArgs.push_back(BuildQuotedAtom(Pos, TString(Module) + "." + Name));
                udfArgs.push_back(customUserType ? customUserType : new TCallNodeImpl(Pos, "TupleType", {}));
                if (typeConfig) {
                    udfArgs.push_back(typeConfig);
                }
                Node = new TCallNodeImpl(Pos, "SqlReduceUdf", udfArgs);
            } else {
                auto udfArgs = BuildUdfArgs(ctx, Pos, Args, nullptr, nullptr, customUserType, typeConfig);
                Node = BuildUdf(ctx, Pos, Module, Name, udfArgs);
            }
        }
        return Node->Init(ctx, src);
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node);
        return Node->Translate(ctx);
    }

    const TString* FuncName() const override {
        return &Name;
    }

    const TString* ModuleName() const override {
        return &Module;
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, Node->IsConstant());
        State.Set(ENodeState::Aggregated, Node->IsAggregated());
    }

    TNodePtr DoClone() const override {
        return new TCallableNode(Pos, Module, Name, CloneContainer(Args), ForReduce);
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node);
        Node->VisitTree(func, visited);
    }
private:
    TCiString Module;
    TString Name;
    TVector<TNodePtr> Args;
    TNodePtr Node;
    const bool ForReduce;
};

TNodePtr BuildCallable(TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args, bool forReduce) {
    return new TCallableNode(pos, module, name, args, forReduce);
}

TNodePtr BuildUdf(TContext& ctx, TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args) {
    if (to_lower(module) == "@yql") {
        return BuildCallable(pos, module, name, args);
    }

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
            type = Args[0];
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

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node);
        Node->VisitTree(func, visited);
    }
private:
    TString ModuleName;
    TString FuncName;
    TVector<TNodePtr> Args;
    TNodePtr Node;
};

template <bool Sorted, bool Hashed>
class TYqlToDict final: public TCallNode {
public:
    TYqlToDict(TPosition pos, const TString& mode, const TVector<TNodePtr>& args)
        : TCallNode(pos, "ToDict", 4, 4, args)
        , Mode(mode)
    {}

private:
    TCallNode::TPtr DoClone() const override {
       return new TYqlToDict<Sorted, Hashed>(GetPos(), Mode, CloneContainer(Args));
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args.size() != 1) {
            ctx.Error(Pos) << "ToDict required exactly one argument";
            return false;
        }
        Args.push_back(BuildLambda(Pos, Y("val"), Y("Nth", "val", Q("0"))));
        Args.push_back(BuildLambda(Pos, Y("val"), Y("Nth", "val", Q("1"))));
        Args.push_back(Q(Y(Q(Sorted ? "Sorted" : Hashed ? "Hashed" : "Auto"), Q(Mode))));
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

        auto legacySpec = src->GetLegacyHoppingWindowSpec();
        auto spec = src->GetHoppingWindowSpec();
        if (!legacySpec && !spec) {
            ctx.Error(Pos) << "No hopping window parameters in aggregation";
            return false;
        }

        Nodes.clear();

        const auto fieldName = legacySpec
            ? "_yql_time"
            : spec->GetLabel();

        const auto interval = legacySpec
            ? legacySpec->Interval
            : dynamic_cast<THoppingWindow*>(spec.Get())->Interval;

        if (!IsStart) {
            Add("Member", "row", Q(fieldName));
            return true;
        }

        Add("Sub",
            Y("Member", "row", Q(fieldName)),
            interval);
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
        return new TInvalidBuiltin(GetPos(), Info);
    }
private:
    TString Info;
};

enum EAggrFuncTypeCallback {
    NORMAL,
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
    UDAF,
    PG
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
                TString errorText;
                if (TVector{EAggregateMode::OverWindow} == validModes) {
                    errorText = TStringBuilder()
                        << "Can't use window function " << functionName << " without window specification (OVER keyword is missing)";
                } else {
                    errorText = TStringBuilder()
                        << "Can't use " << functionName << " in " << ToString(aggMode) << " aggregation mode";
                }
                return INode::TPtr(new TInvalidBuiltin(pos, errorText));
            }
        }
        TAggregationPtr factory = nullptr;
        switch (type) {
        case NORMAL:
            factory = BuildFactoryAggregation(pos, realFunctionName, factoryName, aggMode);
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
        case PG:
            factory = BuildPGFactoryAggregation(pos, realFunctionName, aggMode);
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
            {"substring", BuildNamedBuiltinFactoryCallback<TYqlSubstring>("Substring")},
            {"find", BuildNamedBuiltinFactoryCallback<TYqlSubstring>("Find")},
            {"rfind", BuildNamedBuiltinFactoryCallback<TYqlSubstring>("RFind")},
            {"byteat", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ByteAt", 2, 2) },
            {"startswith", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StartsWith", 2, 2)},
            {"endswith", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EndsWith", 2, 2)},

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
            {"aslist", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsListMayWarn", 0, -1)},
            {"asliststrict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsListStrict", 0, -1) },
            {"listlength", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Length", 1, 1)},
            {"listhasitems", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("HasItems", 1, 1)},
            {"listextend", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListExtend", 0, -1)},
            {"listextendstrict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListExtendStrict", 0, -1)},
            {"listunionall", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListUnionAll", 0, -1) },
            {"listzip", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListZip", -1, -1)},
            {"listzipall", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListZipAll", -1, -1)},
            {"listenumerate", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListEnumerate", 1, 3)},
            {"listreverse", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListReverse", 1, 1)},
            {"listskip", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListSkip", 2, 2)},
            {"listtake", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTake", 2, 2)},
            {"listhead", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListHead", 1, 1)},
            {"listlast", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListLast", 1, 1)},
            {"listsort", BuildBoolBuiltinFactoryCallback<TListSortBuiltin>(true)},
            {"listsortasc", BuildBoolBuiltinFactoryCallback<TListSortBuiltin>(true)},
            {"listsortdesc", BuildBoolBuiltinFactoryCallback<TListSortBuiltin>(false)},
            {"listmap", BuildBoolBuiltinFactoryCallback<TListMapBuiltin>(false)},
            {"listflatmap", BuildBoolBuiltinFactoryCallback<TListMapBuiltin>(true)},
            {"listfilter", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListFilter")},
            {"listany", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListAny", 1, 1)},
            {"listall", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListAll", 1, 1)},
            {"listhas", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListHas", 2, 2)},
            {"listmax", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListMax", 1, 1)},
            {"listmin", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListMin", 1, 1)},
            {"listsum", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListSum", 1, 1)},
            {"listfold", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFold", 3, 3)},
            {"listfold1", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFold1", 3, 3)},
            {"listfoldmap", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFoldMap", 3, 3)},
            {"listfold1map", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFold1Map", 3, 3)},
            {"listavg", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListAvg", 1, 1)},
            {"listconcat", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListConcat", 1, 2)},
            {"listextract", BuildSimpleBuiltinFactoryCallback<TListExtractBuiltin>()},
            {"listuniq", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListUniq", 1, 1)},
            {"listuniqstable", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListUniqStable", 1, 1)},
            {"listcreate", BuildSimpleBuiltinFactoryCallback<TListCreateBuiltin>()},
            {"listfromrange", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFromRange", 2, 3) },
            {"listreplicate", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Replicate", 2, 2) },
            {"listtakewhile", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListTakeWhile") },
            {"listskipwhile", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListSkipWhile") },
            {"listtakewhileinclusive", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListTakeWhileInclusive") },
            {"listskipwhileinclusive", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListSkipWhileInclusive") },
            {"listcollect", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListCollect", 1, 1) },
            {"listnotnull", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListNotNull", 1, 1)},
            {"listflatten", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFlatten", 1, 1)},
            {"listtop", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTop", 2, 3)},
            {"listtopasc", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopAsc", 2, 3)},
            {"listtopdesc", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopDesc", 2, 3)},
            {"listtopsort", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopSort", 2, 3)},
            {"listtopsortasc", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopSortAsc", 2, 3)},
            {"listtopsortdesc", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopSortDesc", 2, 3)},

            // Dict builtins
            {"dictlength", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Length", 1, 1)},
            {"dicthasitems", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("HasItems", 1, 1)},
            {"dictcreate", BuildSimpleBuiltinFactoryCallback<TDictCreateBuiltin>()},
            {"setcreate", BuildSimpleBuiltinFactoryCallback<TSetCreateBuiltin>()},
            {"asdict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsDictMayWarn", 0, -1)},
            {"asdictstrict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsDictStrict", 0, -1)},
            {"asset", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsSetMayWarn", 0, -1)},
            {"assetstrict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsSetStrict", 0, -1)},
            {"todict", BuildNamedBuiltinFactoryCallback<TYqlToDict<false, false>>("One")},
            {"tomultidict", BuildNamedBuiltinFactoryCallback<TYqlToDict<false, false>>("Many")},
            {"tosorteddict", BuildNamedBuiltinFactoryCallback<TYqlToDict<true, false>>("One")},
            {"tosortedmultidict", BuildNamedBuiltinFactoryCallback<TYqlToDict<true, false>>("Many")},
            {"tohasheddict", BuildNamedBuiltinFactoryCallback<TYqlToDict<false, true>>("One")},
            {"tohashedmultidict", BuildNamedBuiltinFactoryCallback<TYqlToDict<false, true>>("Many")},
            {"dictkeys", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictKeys", 1, 1) },
            {"dictpayloads", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictPayloads", 1, 1) },
            {"dictitems", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictItems", 1, 1) },
            {"dictlookup", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Lookup", 2, 2) },
            {"dictcontains", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Contains", 2, 2) },

            // Atom builtins
            {"asatom", BuildSimpleBuiltinFactoryCallback<TYqlAsAtom>()},
            {"secureparam", BuildNamedBuiltinFactoryCallback<TYqlAtom>("SecureParam")},

            {"void", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Void", 0, 0)},
            {"emptylist", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyList", 0, 0)},
            {"emptydict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyDict", 0, 0)},
            {"callable", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Callable", 2, 2)},
            {"way", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Way", 1, 1) },
            {"variant", BuildSimpleBuiltinFactoryCallback<TYqlVariant>() },
            {"enum", BuildSimpleBuiltinFactoryCallback<TYqlEnum>() },
            {"asvariant", BuildSimpleBuiltinFactoryCallback<TYqlAsVariant>() },
            {"asenum", BuildSimpleBuiltinFactoryCallback<TYqlAsEnum>() },
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
            {"formattypediff", BuildNamedBuiltinFactoryCallback<TFormatTypeDiff<false>>("FormatTypeDiff") },
            {"formattypediffpretty", BuildNamedBuiltinFactoryCallback<TFormatTypeDiff<true>>("FormatTypeDiffPretty") },
            {"pgtype", BuildSimpleBuiltinFactoryCallback<TYqlPgType>() },
            {"pgconst", BuildSimpleBuiltinFactoryCallback<TYqlPgConst>() },
            {"pgop", BuildSimpleBuiltinFactoryCallback<TYqlPgOp>() },
            {"pgcall", BuildSimpleBuiltinFactoryCallback<TYqlPgCall<false>>() },
            {"pgrangecall", BuildSimpleBuiltinFactoryCallback<TYqlPgCall<true>>() },
            {"pgcast", BuildSimpleBuiltinFactoryCallback<TYqlPgCast>() },
            {"frompg", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FromPg", 1, 1) },
            {"topg", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ToPg", 1, 1) },
            {"pgor", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgOr", 2, 2) },
            {"pgand", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgAnd", 2, 2) },
            {"pgnot", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgNot", 1, 1) },
            {"pgarray", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgArray", 1, -1) },
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
            { "currenttzdate", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(1, "CurrentTzDate", 1, -1) },
            { "currenttzdatetime", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(1, "CurrentTzDatetime", 1, -1) },
            { "currenttztimestamp", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(1, "CurrentTzTimestamp", 1, -1) },
            {"currentoperationid", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentOperationId", 0, 0) },
            {"currentoperationsharedid", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentOperationSharedId", 0, 0) },
            {"currentauthenticateduser", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentAuthenticatedUser", 0, 0) },
            {"addtimezone", BuildSimpleBuiltinFactoryCallback<TYqlAddTimezone>() },
            {"removetimezone", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("RemoveTimezone", 1, 1) },
            {"pickle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Pickle", 1, 1) },
            {"stablepickle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StablePickle", 1, 1) },
            {"unpickle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Unpickle", 2, 2) },

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
            {"emptylisttypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyListTypeHandle", 0, 0) },
            {"emptydicttypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyDictTypeHandle", 0, 0) },
            {"callabletypecomponents", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableTypeComponents", 1, 1) },
            {"callableargument", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableArgument", 1, 3) },
            {"callabletypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableTypeHandle", 2, 4) },
            {"pgtypename", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgTypeName", 1, 1) },
            {"pgtypehandle", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgTypeHandle", 1, 1) },
            {"formatcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FormatCode", 1, 1) },
            {"worldcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("WorldCode", 0, 0) },
            {"atomcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AtomCode", 1, 1) },
            {"listcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListCode", 0, -1) },
            {"funccode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FuncCode", 1, -1) },
            {"lambdacode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LambdaCode", 1, 2) },
            {"evaluatecode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EvaluateCode", 1, 1) },
            {"reprcode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ReprCode", 1, 1) },
            {"quotecode", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("QuoteCode", 1, 1) },
            {"lambdaargumentscount", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LambdaArgumentsCount", 1, 1) },
            {"lambdaoptionalargumentscount", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LambdaOptionalArgumentsCount", 1, 1) },
            {"subqueryextend", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SubqueryExtend", 1, -1) },
            {"subqueryunionall", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SubqueryUnionAll", 1, -1) },
            {"subquerymerge", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SubqueryMerge", 1, -1) },
            {"subqueryunionmerge", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SubqueryUnionMerge", 1, -1) },
            {"subqueryextendfor", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryFor<SubqueryExtendFor>>() },
            {"subqueryunionallfor", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryFor<SubqueryUnionAllFor>>() },
            {"subquerymergefor", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryFor<SubqueryMergeFor>>() },
            {"subqueryunionmergefor", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryFor<SubqueryUnionMergeFor>>() },
            {"subqueryorderby", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryOrderBy<SubqueryOrderBy>>() },
            {"subqueryassumeorderby", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryOrderBy<SubqueryAssumeOrderBy>>() },

            // Tuple builtins
            {"astuple", BuildSimpleBuiltinFactoryCallback<TTupleNode>()},

            // Struct builtins
            {"trymember", BuildNamedBuiltinFactoryCallback<TTryMember>("TryMember")},
            {"addmember", BuildNamedBuiltinFactoryCallback<TAddMember>("AddMember")},
            {"replacemember", BuildNamedBuiltinFactoryCallback<TAddMember>("ReplaceMember")},
            {"removemember", BuildNamedBuiltinFactoryCallback<TRemoveMember>("RemoveMember")},
            {"forceremovemember", BuildNamedBuiltinFactoryCallback<TRemoveMember>("ForceRemoveMember")},
            {"combinemembers", BuildNamedBuiltinFactoryCallback<TCombineMembers>("FlattenMembers")},
            {"flattenmembers", BuildNamedBuiltinFactoryCallback<TFlattenMembers>("FlattenMembers")},
            {"staticmap", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StaticMap", 2, 2) },
            {"staticzip", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StaticZip", 1, -1) },
            {"structunion", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructUnion", 2, 3)},
            {"structintersection", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructIntersection", 2, 3)},
            {"structdifference", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructDifference", 2, 2)},
            {"structsymmetricdifference", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructSymmetricDifference", 2, 2)},
            {"staticfold", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StaticFold", 3, 3)},
            {"staticfold1", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StaticFold1", 3, 3)},

            // File builtins
            {"filepath", BuildNamedBuiltinFactoryCallback<TFileYqlAtom>("FilePath")},
            {"filecontent", BuildNamedBuiltinFactoryCallback<TFileYqlAtom>("FileContent")},
            {"folderpath", BuildNamedBuiltinFactoryCallback<TFileYqlAtom>("FolderPath") },
            {"files", BuildNamedBuiltinFactoryCallback<TFileYqlAtom>("Files")},
            {"parsefile", BuildSimpleBuiltinFactoryCallback<TYqlParseFileOp>()},

            // Misc builtins
            {"coalesce", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Coalesce", 1, -1)},
            {"nvl", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Coalesce", 1, -1) },
            {"nanvl", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Nanvl", 2, 2) },
            {"likely", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Likely", 1, -1)},
            {"assumestrict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AssumeStrict", 1, 1)},
            {"assumenonstrict", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AssumeNonStrict", 1, 1)},
            {"random", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "Random", 1, -1)},
            {"randomnumber", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "RandomNumber", 1, -1)},
            {"randomuuid", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "RandomUuid", 1, -1) },
            {"tablepath", BuildNamedBuiltinFactoryCallback<TCallDirectRow>("TablePath") },
            {"tablerecordindex", BuildNamedBuiltinFactoryCallback<TCallDirectRow>("TableRecord") },
            {"tablerow", BuildSimpleBuiltinFactoryCallback<TTableRow<false>>() },
            {"jointablerow", BuildSimpleBuiltinFactoryCallback<TTableRow<true>>() },
            {"tablerows", BuildSimpleBuiltinFactoryCallback<TTableRows>() },
            {"weakfield", BuildSimpleBuiltinFactoryCallback<TWeakFieldOp>()},

            {"systemmetadata", BuildNamedArgcBuiltinFactoryCallback<TCallDirectRow>("SystemMetadata", 1, -1)},

            // Hint builtins
            {"grouping", BuildSimpleBuiltinFactoryCallback<TGroupingNode>()},

            // Window funcitons
            {"rownumber", BuildNamedArgcBuiltinFactoryCallback<TWinRowNumber>("RowNumber", 0, 0)},
            {"rank", BuildNamedArgcBuiltinFactoryCallback<TWinRank>("Rank", 0, 1)},
            {"denserank", BuildNamedArgcBuiltinFactoryCallback<TWinRank>("DenseRank", 0, 1)},
            {"lead", BuildNamedArgcBuiltinFactoryCallback<TWinLeadLag>("Lead", 1, 2)},
            {"lag", BuildNamedArgcBuiltinFactoryCallback<TWinLeadLag>("Lag", 1, 2)},

            // Session window
            {"sessionwindow", BuildSimpleBuiltinFactoryCallback<TSessionWindow>()},
            {"sessionstart", BuildSimpleBuiltinFactoryCallback<TSessionStart<true>>()},
            {"sessionstate", BuildSimpleBuiltinFactoryCallback<TSessionStart<false>>()},

            // New hopping
            {"hoppingwindow", BuildSimpleBuiltinFactoryCallback<THoppingWindow>()},

            // Hopping intervals time functions
            {"hopstart", BuildSimpleBuiltinFactoryCallback<THoppingTime<true>>()},
            {"hopend", BuildSimpleBuiltinFactoryCallback<THoppingTime<false>>()},

            //MatchRecognize navigation functions
            {"first", BuildNamedBuiltinFactoryCallback<TMatchRecognizeNavigate>("FIRST")},
            {"last", BuildNamedBuiltinFactoryCallback<TMatchRecognizeNavigate>("LAST")},
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

            {"checked_sum", BuildAggrFuncFactoryCallback("CheckedSum", "checked_sum_traits_factory")},
            {"checked_sumif", BuildAggrFuncFactoryCallback("CheckedSumIf", "checked_sum_if_traits_factory", PAYLOAD_PREDICATE) },

            {"some", BuildAggrFuncFactoryCallback("Some", "some_traits_factory")},
            {"somevalue", BuildAggrFuncFactoryCallback("SomeValue", "some_traits_factory")},

            {"count", BuildAggrFuncFactoryCallback("Count", "count_traits_factory", COUNT)},
            {"countif", BuildAggrFuncFactoryCallback("CountIf", "count_if_traits_factory")},

            {"every", BuildAggrFuncFactoryCallback("Every", "and_traits_factory")},
            {"booland", BuildAggrFuncFactoryCallback("BoolAnd", "and_traits_factory")},
            {"boolor", BuildAggrFuncFactoryCallback("BoolOr", "or_traits_factory")},
            {"boolxor", BuildAggrFuncFactoryCallback("BoolXor", "xor_traits_factory")},

            {"bitand", BuildAggrFuncFactoryCallback("BitAnd", "bit_and_traits_factory")},
            {"bitor", BuildAggrFuncFactoryCallback("BitOr", "bit_or_traits_factory")},
            {"bitxor", BuildAggrFuncFactoryCallback("BitXor", "bit_xor_traits_factory")},

            {"avg", BuildAggrFuncFactoryCallback("Avg", "avg_traits_factory")},
            {"avgif", BuildAggrFuncFactoryCallback("AvgIf", "avg_if_traits_factory", PAYLOAD_PREDICATE) },

            {"agglist", BuildAggrFuncFactoryCallback("AggregateList", "list2_traits_factory", LIST)},
            {"aggrlist", BuildAggrFuncFactoryCallback("AggregateList", "list2_traits_factory", LIST)},
            {"aggregatelist", BuildAggrFuncFactoryCallback("AggregateList", "list2_traits_factory", LIST)},
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
            {"histogramcdf", BuildAggrFuncFactoryCallback("AdaptiveWardHistogramCDF", "histogram_cdf_adaptive_ward_traits_factory", HISTOGRAM, "HistogramCDF")},
            {"adaptivewardhistogram", BuildAggrFuncFactoryCallback("AdaptiveWardHistogram", "histogram_adaptive_ward_traits_factory", HISTOGRAM)},
            {"adaptivewardhistogramcdf", BuildAggrFuncFactoryCallback("AdaptiveWardHistogramCDF", "histogram_cdf_adaptive_ward_traits_factory", HISTOGRAM)},
            {"adaptiveweighthistogram", BuildAggrFuncFactoryCallback("AdaptiveWeightHistogram", "histogram_adaptive_weight_traits_factory", HISTOGRAM)},
            {"adaptiveweighthistogramcdf", BuildAggrFuncFactoryCallback("AdaptiveWeightHistogramCDF", "histogram_cdf_adaptive_weight_traits_factory", HISTOGRAM)},
            {"adaptivedistancehistogram", BuildAggrFuncFactoryCallback("AdaptiveDistanceHistogram", "histogram_adaptive_distance_traits_factory", HISTOGRAM)},
            {"adaptivedistancehistogramcdf", BuildAggrFuncFactoryCallback("AdaptiveDistanceHistogramCDF", "histogram_cdf_adaptive_distance_traits_factory", HISTOGRAM)},
            {"blockwardhistogram", BuildAggrFuncFactoryCallback("BlockWardHistogram", "histogram_block_ward_traits_factory", HISTOGRAM)},
            {"blockwardhistogramcdf", BuildAggrFuncFactoryCallback("BlockWardHistogramCDF", "histogram_cdf_block_ward_traits_factory", HISTOGRAM)},
            {"blockweighthistogram", BuildAggrFuncFactoryCallback("BlockWeightHistogram", "histogram_block_weight_traits_factory", HISTOGRAM)},
            {"blockweighthistogramcdf", BuildAggrFuncFactoryCallback("BlockWeightHistogramCDF", "histogram_cdf_block_weight_traits_factory", HISTOGRAM)},
            {"linearhistogram", BuildAggrFuncFactoryCallback("LinearHistogram", "histogram_linear_traits_factory", LINEAR_HISTOGRAM)},
            {"linearhistogramcdf", BuildAggrFuncFactoryCallback("LinearHistogramCDF", "histogram_cdf_linear_traits_factory", LINEAR_HISTOGRAM)},
            {"logarithmichistogram", BuildAggrFuncFactoryCallback("LogarithmicHistogram", "histogram_logarithmic_traits_factory", LINEAR_HISTOGRAM)},
            {"logarithmichistogramcdf", BuildAggrFuncFactoryCallback("LogarithmicHistogramCDF", "histogram_cdf_logarithmic_traits_factory", LINEAR_HISTOGRAM)},
            {"loghistogram", BuildAggrFuncFactoryCallback("LogarithmicHistogram", "histogram_logarithmic_traits_factory", LINEAR_HISTOGRAM, "LogHistogram")},
            {"loghistogramcdf", BuildAggrFuncFactoryCallback("LogarithmicHistogramCDF", "histogram_cdf_logarithmic_traits_factory", LINEAR_HISTOGRAM, "LogHistogramCDF")},

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
            {"aggregatetransforminput", { "AggregateTransformInput", 2, 2}},
            {"aggregatetransformoutput", { "AggregateTransformOutput", 2, 2}},
            {"aggregateflatten", { "AggregateFlatten", 1, 1}},
            {"choosemembers", { "ChooseMembers", 2, 2}},
            {"removemembers", { "RemoveMembers", 2, 2}},
            {"forceremovemembers", { "ForceRemoveMembers", 2, 2}},
            {"structmembers", { "StructMembers", 1, 1}},
            {"gathermembers", { "GatherMembers", 1, 1}},
            {"renamemembers", { "RenameMembers", 2, 2}},
            {"forcerenamemembers", { "ForceRenameMembers", 2, 2}},
            {"spreadmembers", { "SpreadMembers", 2, 2}},
            {"forcespreadmembers", { "ForceSpreadMembers", 2, 2}},
            {"listfromtuple", { "ListFromTuple", 1, 1}},
            {"listtotuple", { "ListToTuple", 2, 2}},
        };
        return coreFuncs;
    }
};

TNodePtr BuildBuiltinFunc(TContext& ctx, TPosition pos, TString name, const TVector<TNodePtr>& args,
    const TString& originalNameSpace, EAggregateMode aggMode, bool* mustUseNamed, bool warnOnYqlNameSpace) {

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
    TString nameSpace(originalNameSpace);
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
        nameSpace = "JavaScript";
    }

    if (ns == "datetime2") {
        ctx.Warning(pos, TIssuesIds::YQL_DEPRECATED_DATETIME2) << "DateTime2:: is a temporary alias for DateTime:: which will be removed in the future, use DateTime:: instead";
    }

    if (ns == "datetime") {
        ns = "datetime2";
        nameSpace = "DateTime2";
    }

    auto scriptType = NKikimr::NMiniKQL::ScriptTypeFromStr(ns);
    switch (scriptType) {
        case NKikimr::NMiniKQL::EScriptType::Python:
        case NKikimr::NMiniKQL::EScriptType::Python3:
        case NKikimr::NMiniKQL::EScriptType::ArcPython3:
            scriptType = NKikimr::NMiniKQL::EScriptType::Python3;
            break;
        case NKikimr::NMiniKQL::EScriptType::Python2:
            scriptType = NKikimr::NMiniKQL::EScriptType::ArcPython2;
            break;
        case NKikimr::NMiniKQL::EScriptType::SystemPython2:
            scriptType = NKikimr::NMiniKQL::EScriptType::Python2;
            break;
        default:
            break;
    }

    if (ns == "yql" || ns == "@yql") {
        if (warnOnYqlNameSpace && GetEnv("YQL_DETERMINISTIC_MODE").empty()) {
            ctx.Warning(pos, TIssuesIds::YQL_S_EXPRESSIONS_CALL)
                << "It is not recommended to directly access s-expressions functions via YQL::" << Endl
                << "This mechanism is mostly intended for temporary workarounds or internal testing purposes";
        }

        if (ns == "yql") {
            return new TCallNodeImpl(pos, name, -1, -1, args);
        }
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
    } else if (ns == "pg") {
        const bool isAggregateFunc = NYql::NPg::HasAggregation(name, NYql::NPg::EAggKind::Normal);
        if (isAggregateFunc) {
            if (aggMode == EAggregateMode::Distinct) {
                return new TInvalidBuiltin(pos, "Distinct is not supported yet for PG aggregation ");
            }

            return BuildAggrFuncFactoryCallback(name, "", EAggrFuncTypeCallback::PG)(pos, args, aggMode, false);
        } else {
            TVector<TNodePtr> pgCallArgs;
            pgCallArgs.push_back(BuildLiteralRawString(pos, name));
            pgCallArgs.insert(pgCallArgs.end(), args.begin(), args.end());
            return new TYqlPgCall<false>(pos, pgCallArgs);
        }
    } else if (name == "MakeLibraPreprocessor") {
        if (args.size() != 1) {
            return new TInvalidBuiltin(pos, TStringBuilder() << name << " requires exactly one argument");
        }

        auto settings = NYT::TNode::CreateMap();

        auto makeUdfArgs = [&args, &pos, &settings]() {
            return TVector<TNodePtr> {
                args[0],
                new TCallNodeImpl(pos, "Void", {}),
                BuildQuotedAtom(pos, NYT::NodeToYsonString(settings))
            };
        };

        auto structNode = dynamic_cast<TStructNode*>(args[0].Get());
        if (!structNode) {
            if (auto callNode = dynamic_cast<TCallNode*>(args[0].Get())) {
                if (callNode->GetOpName() == "AsStruct") {
                    return BuildUdf(ctx, pos, nameSpace, name, makeUdfArgs());
                }
            }

            return new TInvalidBuiltin(pos, TStringBuilder() << name << " requires struct as argument");
        }

        for (const auto& item : structNode->GetExprs()) {
            const auto& label = item->GetLabel();
            if (label == "Entities") {
                auto callNode = dynamic_cast<TCallNode*>(item.Get());
                if (!callNode || callNode->GetOpName() != "AsListMayWarn") {
                    return new TInvalidBuiltin(pos, TStringBuilder() << name << " entities must be list of strings");
                }

                auto entities = NYT::TNode::CreateList();
                for (const auto& entity : callNode->GetArgs()) {
                    if (!entity->IsLiteral() || entity->GetLiteralType() != "String") {
                        return new TInvalidBuiltin(pos, TStringBuilder() << name << " entity must be string literal");
                    }
                    entities.Add(entity->GetLiteralValue());
                }

                settings(label, std::move(entities));
            } else if (label == "EntitiesStrategy") {
                if (!item->IsLiteral() || item->GetLiteralType() != "String") {
                    return new TInvalidBuiltin(
                        pos, TStringBuilder() << name << " entities strategy must be string literal"
                    );
                }

                if (!EqualToOneOf(item->GetLiteralValue(), "whitelist", "blacklist")) {
                    return new TInvalidBuiltin(
                        pos,
                        TStringBuilder() << name << " got invalid entities strategy: expected 'whitelist' or 'blacklist'"
                    );
                }

                settings(label, item->GetLiteralValue());
            } else if (label == "Mode") {
                if (!item->IsLiteral() || item->GetLiteralType() != "String") {
                    return new TInvalidBuiltin(
                        pos, TStringBuilder() << name << " mode must be string literal"
                    );
                }

                settings(label, item->GetLiteralValue());
            } else if (EqualToOneOf(label, "BlockstatDict", "ParseWithFat")) {
                continue;
            } else {
                return new TInvalidBuiltin(
                    pos,
                    TStringBuilder()
                        << name << " got unsupported setting: " << label
                        << "; supported: Entities, EntitiesStrategy, BlockstatDict, ParseWithFat" );
            }
        }

        return BuildUdf(ctx, pos, nameSpace, name, makeUdfArgs());
    } else if (scriptType != NKikimr::NMiniKQL::EScriptType::Unknown) {
        auto scriptName = NKikimr::NMiniKQL::IsCustomPython(scriptType) ? nameSpace : TString(NKikimr::NMiniKQL::ScriptTypeAsStr(scriptType));
        return new TScriptUdf(pos, scriptName, name, args);
    } else if (ns.empty()) {
        if (auto simpleType = LookupSimpleType(normalizedName, ctx.FlexibleTypes, /* isPgType = */ false)) {
            const auto type = *simpleType;
            if (NUdf::FindDataSlot(type)) {
                YQL_ENSURE(type != "Decimal");
                return new TYqlData(pos, type, args);
            }

            if (type.StartsWith("pg") || type.StartsWith("_pg")) {
                TVector<TNodePtr> pgConstArgs;
                if (!args.empty()) {
                    pgConstArgs.push_back(args.front());
                    pgConstArgs.push_back(new TCallNodeImpl(pos, "PgType", { BuildQuotedAtom(pos,
                        TString(type.StartsWith("pg") ? "" : "_") + type.substr(type.StartsWith("pg") ? 2 : 3), TNodeFlags::Default) }));
                    pgConstArgs.insert(pgConstArgs.end(), args.begin() + 1, args.end());
                }
                return new TYqlPgConst(pos, pgConstArgs);
            } else if (type == "Void" || type == "EmptyList" || type == "EmptyDict") {
                return new TCallNodeImpl(pos, type, 0, 0, args);
            } else {
                return new TInvalidBuiltin(pos, TStringBuilder() << "Can not create objects of type " << type);
            }
        }

        if (normalizedName == "decimal") {
            if (args.size() == 2) {
                TVector<TNodePtr> dataTypeArgs = { BuildQuotedAtom(pos, "Decimal", TNodeFlags::Default) };
                for (auto& arg : args) {
                    if (auto literal = arg->GetLiteral("Int32")) {
                        dataTypeArgs.push_back(BuildQuotedAtom(pos, *literal, TNodeFlags::Default));
                    } else {
                        dataTypeArgs.push_back(MakeAtomFromExpression(ctx.Pos(), ctx, arg).Build());
                    }
                }
                return new TCallNodeImpl(pos, "DataType", dataTypeArgs);
            }
            return new TYqlData(pos, "Decimal", args);
        }

        if (normalizedName == "tablename") {
            return new TTableName(pos, args, ctx.Scoped->CurrService);
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

            if (aggMode == EAggregateMode::Distinct) {
                return new TInvalidBuiltin(pos, "DISTINCT can only be used in aggregation functions");
            }

            if (to_lower(*args[0]->GetLiteral("String")).StartsWith("pg::")) {
                auto name = args[0]->GetLiteral("String")->substr(4);
                const bool isAggregateFunc = NYql::NPg::HasAggregation(name, NYql::NPg::EAggKind::Normal);
                if (!isAggregateFunc) {
                    return new TInvalidBuiltin(pos, TStringBuilder() << "Unknown aggregation function: " << *args[0]->GetLiteral("String"));
                }

                return BuildAggrFuncFactoryCallback(name, "", EAggrFuncTypeCallback::PG)(pos, args, aggMode, true);
            }

            AdjustCheckedAggFuncName(aggNormalizedName, ctx);

            auto aggrCallback = aggrFuncs.find(aggNormalizedName);
            if (aggrCallback == aggrFuncs.end()) {
                return new TInvalidBuiltin(pos, TStringBuilder() << "Unknown aggregation function: " << *args[0]->GetLiteral("String"));
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

        AdjustCheckedAggFuncName(normalizedName, ctx);

        auto aggrCallback = aggrFuncs.find(normalizedName);
        if (aggrCallback != aggrFuncs.end()) {
            return (*aggrCallback).second(pos, args, aggMode, false).Release();
        }
        if (aggMode == EAggregateMode::Distinct) {
            return new TInvalidBuiltin(pos, "DISTINCT can only be used in aggregation functions");
        }

        auto builtinCallback = builtinFuncs.find(normalizedName);
        if (builtinCallback != builtinFuncs.end()) {
            return (*builtinCallback).second(pos, args);
        } else if (normalizedName == "udf") {
            if (mustUseNamed && *mustUseNamed) {
                *mustUseNamed = false;
            }
            return new TUdfNode(pos, args);
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
        } else if (normalizedName == "sqlexternalfunction") {
            return new TCallNodeImpl(pos, "SqlExternalFunction", args);
        } else {
            return new TInvalidBuiltin(pos, TStringBuilder() << "Unknown builtin: " << name);
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

    TVector<TNodePtr> usedArgs = args;

    TNodePtr customUserType = nullptr;
    if (ns == "json") {
        ctx.Warning(pos, TIssuesIds::YQL_DEPRECATED_JSON_UDF) << "Json UDF is deprecated. Please use JSON API instead";

        ns = "yson";
        nameSpace = "Yson";
        if (name == "Serialize") {
            name = "SerializeJson";
        }
        else if (name == "Parse") {
            name = "ParseJson";
        }
    }

    if (ctx.PragmaYsonFast && ns == "yson") {
        ns.append('2');
        nameSpace.append('2');
    }

    if (ns.StartsWith("yson")) {
        if (name == "ConvertTo" && usedArgs.size() > 1) {
            customUserType = usedArgs[1];
            usedArgs.erase(usedArgs.begin() + 1);
        }

        if (name == "Serialize") {
            if (usedArgs) {
                usedArgs.resize(1U);
            }
        } else if (ctx.PragmaYsonFast && name == "SerializeJsonEncodeUtf8") {
            name = "SerializeJson";
            if (usedArgs.size() < 2U) {
                usedArgs.emplace_back(BuildYsonOptionsNode(pos, ctx.PragmaYsonAutoConvert, ctx.PragmaYsonStrict, ctx.PragmaYsonFast));
            }
            positionalArgs = BuildTuple(pos, usedArgs);
            auto encodeUtf8 = BuildLiteralBool(pos, true);
            encodeUtf8->SetLabel("EncodeUtf8");
            namedArgs = BuildStructure(pos, {encodeUtf8});
            usedArgs = {positionalArgs, namedArgs};
        } else if (name.StartsWith("From")) {
            if (usedArgs) {
                usedArgs.resize(1U);
            }
            name = "From";
        } else if (name == "GetLength" || name.StartsWith("ConvertTo") || name.StartsWith("Parse") || name.StartsWith("SerializeJson")) {
            if (usedArgs.size() < 2U) {
                usedArgs.emplace_back(BuildYsonOptionsNode(pos, ctx.PragmaYsonAutoConvert, ctx.PragmaYsonStrict, ctx.PragmaYsonFast));
            }
        } else if (name == "Contains" || name.StartsWith("Lookup") || name.StartsWith("YPath")) {
            if (usedArgs.size() < 3U) {
                usedArgs.push_back(BuildYsonOptionsNode(pos, ctx.PragmaYsonAutoConvert, ctx.PragmaYsonStrict, ctx.PragmaYsonFast));
            }
        }
    }

    if (ns == "datetime2" && name == "Update") {
        if (namedArgs) {
            TStructNode* castedNamedArgs = dynamic_cast<TStructNode*>(namedArgs.Get());
            Y_DEBUG_ABORT_UNLESS(castedNamedArgs);
            auto exprs = castedNamedArgs->GetExprs();
            for (auto& arg : exprs) {
                if (arg->GetLabel() == "Timezone") {
                    arg = new TCallNodeImpl(pos, "TimezoneId", 1, 1, { arg });
                    arg->SetLabel("TimezoneId");
                }
            }

            namedArgs = BuildStructure(pos, exprs);
            usedArgs.pop_back();
            usedArgs.push_back(namedArgs);
        };
    }

    TNodePtr typeConfig = MakeTypeConfig(pos, ns, usedArgs);
    return BuildSqlCall(ctx, pos, nameSpace, name, usedArgs, positionalArgs, namedArgs, customUserType, TDeferredAtom(typeConfig, ctx), nullptr);
}

} // namespace NSQLTranslationV1
