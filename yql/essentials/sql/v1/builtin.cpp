#include "node.h"

#include "aggregation.h"
#include "context.h"
#include "list_builtin.h"
#include "match_recognize.h"
#include "select_yql_aggregation.h"

#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/core/sql_types/simple_types.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/public/issue/yql_issue_id.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

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
                args[ToString(i)] = NYT::TNode()("type", udfArgs[i]->GetLiteralType())("value", udfArgs[i]->GetLiteralValue());
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
        , Args_(args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!src) {
            ctx.Error(Pos_) << "Grouping function should have source";
            return false;
        }
        TVector<TString> columns;
        columns.reserve(Args_.size());
        const bool isJoin = src->GetJoin();
        ISource* composite = src->GetCompositeSource();
        for (const auto& node : Args_) {
            auto namePtr = node->GetColumnName();
            if (!namePtr || !*namePtr) {
                ctx.Error(Pos_) << "GROUPING function should use columns as arguments";
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
        Nodes_.push_back(BuildAtom(Pos_, "Member"));
        Nodes_.push_back(BuildAtom(Pos_, "row"));
        Nodes_.push_back(BuildQuotedAtom(Pos_, groupingColumn));
        return TAstListNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TGroupingNode(Pos_, CloneContainer(Args_));
    }

private:
    const TVector<TNodePtr> Args_;
};

class TBasicAggrFunc final: public TAstListNode {
public:
    TBasicAggrFunc(TPosition pos, const TString& name, TAggregationPtr aggr, const TVector<TNodePtr>& args)
        : TAstListNode(pos)
        , Name_(name)
        , Aggr_(aggr)
        , Args_(args)
    {
    }

    TCiString GetName() const {
        return Name_;
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!src) {
            ctx.Error(Pos_) << "Unable to use aggregation function '" << Name_ << "' without data source";
            return false;
        }
        if (!DoInitAggregation(ctx, src)) {
            return false;
        }

        return TAstListNode::DoInit(ctx, src);
    }

    void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override {
        if (Args_.empty() || (Aggr_->GetAggregationMode() != EAggregateMode::Distinct && Aggr_->GetAggregationMode() != EAggregateMode::OverWindowDistinct)) {
            return;
        }

        auto& expr = Args_.front();

        // need to initialize expr before checking whether it is a column
        auto clone = expr->Clone();
        if (!clone->Init(ctx, &src)) {
            return;
        }

        const auto column = clone->GetColumnName();
        if (column) {
            return;
        }

        auto tmpColumn = src.MakeLocalName("_yql_preagg_" + Name_);
        YQL_ENSURE(!expr->GetLabel());
        expr->SetLabel(tmpColumn);

        PreaggregateExpr_ = expr;
        exprs.push_back(PreaggregateExpr_);
        expr = BuildColumn(expr->GetPos(), tmpColumn);

        Aggr_->MarkKeyColumnAsGenerated();
    }

    TNodePtr DoClone() const final {
        TAggregationPtr aggrClone = static_cast<IAggregation*>(Aggr_->Clone().Release());
        return new TBasicAggrFunc(Pos_, Name_, aggrClone, CloneContainer(Args_));
    }

    TAggregationPtr GetAggregation() const override {
        return Aggr_;
    }

private:
    bool DoInitAggregation(TContext& ctx, ISource* src) {
        if (PreaggregateExpr_) {
            YQL_ENSURE(PreaggregateExpr_->HasState(ENodeState::Initialized));
            if (PreaggregateExpr_->IsAggregated() && !PreaggregateExpr_->IsAggregationKey() && !Aggr_->IsOverWindow()) {
                ctx.Error(Aggr_->GetPos()) << "Aggregation of aggregated values is forbidden";
                return false;
            }
        }

        if (!Aggr_->InitAggr(ctx, false, src, *this, Args_)) {
            return false;
        }
        return src->AddAggregation(ctx, Aggr_);
    }

    void DoUpdateState() const final {
        State_.Set(ENodeState::Const, !Args_.empty() && AllOf(Args_, [](const auto& arg) { return arg->IsConstant(); }));
        State_.Set(ENodeState::Aggregated);
    }

    TNodePtr PreaggregateExpr_;

protected:
    const TString Name_;
    TAggregationPtr Aggr_;
    TVector<TNodePtr> Args_;
};

class TBasicAggrFactory final: public TAstListNode {
public:
    TBasicAggrFactory(TPosition pos, const TString& name, TAggregationPtr aggr, const TVector<TNodePtr>& args)
        : TAstListNode(pos)
        , Name_(name)
        , Aggr_(aggr)
        , Args_(args)
    {
    }

    TCiString GetName() const {
        return Name_;
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!DoInitAggregation(ctx)) {
            return false;
        }

        auto factory = Aggr_->AggregationTraitsFactory();
        auto apply = Y("Apply", factory, Y("ListType", "type"));

        auto columnIndices = Aggr_->GetFactoryColumnIndices();
        if (columnIndices.size() == 1) {
            apply = L(apply, "extractor");
        } else {
            // make several extractors from main that returns a tuple
            for (ui32 arg = 0; arg < columnIndices.size(); ++arg) {
                auto partial = BuildLambda(Pos_, Y("row"), Y("Nth", Y("Apply", "extractor", "row"), Q(ToString(columnIndices[arg]))));
                apply = L(apply, partial);
            }
        }

        Aggr_->AddFactoryArguments(apply);
        Lambda_ = BuildLambda(Pos_, Y("type", "extractor"), apply);
        return TAstListNode::DoInit(ctx, src);
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Lambda_->Translate(ctx);
    }

    TNodePtr DoClone() const final {
        TAggregationPtr aggrClone = static_cast<IAggregation*>(Aggr_->Clone().Release());
        return new TBasicAggrFactory(Pos_, Name_, aggrClone, CloneContainer(Args_));
    }

    TAggregationPtr GetAggregation() const override {
        return Aggr_;
    }

private:
    bool DoInitAggregation(TContext& ctx) {
        return Aggr_->InitAggr(ctx, true, nullptr, *this, Args_);
    }

protected:
    const TString Name_;
    TAggregationPtr Aggr_;
    TVector<TNodePtr> Args_;
    TNodePtr Lambda_;
};

typedef THolder<TBasicAggrFunc> TAggrFuncPtr;

class TLiteralStringAtom: public INode {
public:
    TLiteralStringAtom(TPosition pos, TNodePtr node, const TString& info, const TString& prefix = {})
        : INode(pos)
        , Node_(node)
        , Info_(info)
        , Prefix_(prefix)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        if (!Node_) {
            ctx.Error(Pos_) << Info_;
            return false;
        }

        if (!Node_->Init(ctx, src)) {
            return false;
        }

        Atom_ = MakeAtomFromExpression(Pos_, ctx, Node_, Prefix_).Build();
        return true;
    }

    bool IsLiteral() const override {
        return Atom_ ? Atom_->IsLiteral() : false;
    }

    TString GetLiteralType() const override {
        return Atom_ ? Atom_->GetLiteralType() : "";
    }

    TString GetLiteralValue() const override {
        return Atom_ ? Atom_->GetLiteralValue() : "";
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Atom_->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TLiteralStringAtom(GetPos(), SafeClone(Node_), Info_, Prefix_);
    }

    void DoUpdateState() const override {
        YQL_ENSURE(Atom_);
        State_.Set(ENodeState::Const, Atom_->IsConstant());
        State_.Set(ENodeState::Aggregated, Atom_->IsAggregated());
        State_.Set(ENodeState::OverWindow, Atom_->IsOverWindow());
    }

private:
    TNodePtr Node_;
    TNodePtr Atom_;
    TString Info_;
    TString Prefix_;
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
            ctx.Error(Pos_) << "Unexpected type " << GetOpName();
            return false;
        }

        if (*slot == NUdf::EDataSlot::Decimal) {
            MinArgs_ = MaxArgs_ = 3;
        }

        if (!ValidateArguments(ctx)) {
            return false;
        }

        auto stringNode = Args_[0];
        auto atom = stringNode->GetLiteral("String");
        if (!atom) {
            ctx.Error(Pos_) << "Expected literal string as argument in " << GetOpName() << " function";
            return false;
        }

        TString value;
        if (*slot == NUdf::EDataSlot::Decimal) {
            const auto precision = Args_[1]->GetLiteral("Int32");
            const auto scale = Args_[2]->GetLiteral("Int32");

            if (!NKikimr::NMiniKQL::IsValidDecimal(*atom)) {
                ctx.Error(Pos_) << "Invalid value " << atom->Quote() << " for type " << GetOpName();
                return false;
            }

            ui8 stub;
            if (!(precision && TryFromString<ui8>(*precision, stub))) {
                ctx.Error(Pos_) << "Invalid precision " << (precision ? precision->Quote() : "") << " for type " << GetOpName();
                return false;
            }

            if (!(scale && TryFromString<ui8>(*scale, stub))) {
                ctx.Error(Pos_) << "Invalid scale " << (scale ? scale->Quote() : "") << " for type " << GetOpName();
                return false;
            }

            Args_[0] = BuildQuotedAtom(GetPos(), *atom);
            Args_[1] = BuildQuotedAtom(GetPos(), *precision);
            Args_[2] = BuildQuotedAtom(GetPos(), *scale);
            return TCallNode::DoInit(ctx, src);
        } else if (NUdf::GetDataTypeInfo(*slot).Features & (NUdf::DateType | NUdf::TzDateType | NUdf::TimeIntervalType)) {
            const auto out = NKikimr::NMiniKQL::ValueFromString(*slot, *atom);
            if (!out) {
                ctx.Error(Pos_) << "Invalid value " << atom->Quote() << " for type " << GetOpName();
                return false;
            }

            switch (*slot) {
                case NUdf::EDataSlot::Date:
                case NUdf::EDataSlot::TzDate:
                    value = ToString(out.Get<ui16>());
                    break;
                case NUdf::EDataSlot::Date32:
                case NUdf::EDataSlot::TzDate32:
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
                case NUdf::EDataSlot::TzDatetime64:
                case NUdf::EDataSlot::TzTimestamp64:
                    value = ToString(out.Get<i64>());
                    break;
                case NUdf::EDataSlot::Interval:
                case NUdf::EDataSlot::Interval64:
                    value = ToString(out.Get<i64>());
                    if ('T' == atom->back()) {
                        ctx.Error(Pos_) << "Time prefix 'T' at end of interval constant. The designator 'T' shall be absent if all of the time components are absent.";
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
            std::array<char, 0x10> out;
            if (!NKikimr::NMiniKQL::ParseUuid(*atom, out.data())) {
                ctx.Error(Pos_) << "Invalid value " << atom->Quote() << " for type " << GetOpName();
                return false;
            }

            value.assign(out.data(), sizeof(out));
        } else {
            if (!NKikimr::NMiniKQL::IsValidStringValue(*slot, *atom)) {
                ctx.Error(Pos_) << "Invalid value " << atom->Quote() << " for type " << GetOpName();
                return false;
            }

            value = *atom;
        }

        Args_[0] = BuildQuotedAtom(GetPos(), value);
        return TCallNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TYqlData(GetPos(), OpName_, CloneContainer(Args_));
    }
};

template <bool HasMode>
class TSideEffects: public TCallNode {
public:
    TSideEffects(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "WithSideEffectsMode", 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        const size_t expectedArgs = HasMode ? 2 : 1;
        if (Args_.size() != expectedArgs) {
            ctx.Error(Pos_) << OpName_ << " requires exactly " << expectedArgs << " arguments";
            return false;
        }

        for (const auto& arg : Args_) {
            if (!arg->Init(ctx, src)) {
                return false;
            }
        }

        if (HasMode) {
            Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        } else {
            Args_.push_back(Q("General"));
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TSideEffects<HasMode>(Pos_, CloneContainer(Args_));
    }
};

class TTableName: public TCallNode {
public:
    TTableName(TPosition pos, const TVector<TNodePtr>& args, const TString& service)
        : TCallNode(pos, "TableName", 0, 2, args)
        , Service_(service)
        , EmptyArgs_(args.empty())
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (Args_.empty()) {
            if (!src) {
                ctx.Error(Pos_) << "Unable to use TableName() without source";
                return false;
            }

            // TODO: TablePath() and TableRecordIndex() have more strict limitations
            if (src->GetJoin()) {
                if (!ctx.Warning(Pos_, TIssuesIds::YQL_EMPTY_TABLENAME_RESULT, [](auto& out) {
                        out << "TableName() may produce empty result when used in ambiguous context (with JOIN)";
                    })) {
                    return false;
                }
            }

            if (src->HasAggregations()) {
                if (!ctx.Warning(Pos_, TIssuesIds::YQL_EMPTY_TABLENAME_RESULT, [](auto& out) {
                        out << "TableName() will produce empty result when used with aggregation.\n"
                               "Please consult documentation for possible workaround";
                    })) {
                    return false;
                }
            }

            if (ctx.DirectRowDependsOn.GetOrElse(true)) {
                Args_.push_back(Y("TablePath", Y("DependsOn", "row")));
            } else {
                Args_.push_back(Y("TablePath", "row"));
            }
        }

        if (Args_.size() == 2) {
            auto literal = Args_[1]->GetLiteral("String");
            if (!literal) {
                ctx.Error(Args_[1]->GetPos()) << "Expected literal string as second argument in TableName function";
                return false;
            }

            Args_[1] = BuildQuotedAtom(Args_[1]->GetPos(), *literal);
        } else {
            if (Service_.empty()) {
                ctx.Error(GetPos()) << GetOpName() << " requires either service name as second argument or current cluster name";
                return false;
            }

            Args_.push_back(BuildQuotedAtom(GetPos(), Service_));
        }

        return TCallNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TTableName(GetPos(), CloneContainer(Args_), Service_);
    }

    void DoUpdateState() const override {
        if (EmptyArgs_) {
            State_.Set(ENodeState::Const, false);
        } else {
            TCallNode::DoUpdateState();
        }
    }

private:
    TString Service_;
    const bool EmptyArgs_;
};

class TYqlParseType final: public INode {
public:
    TYqlParseType(TPosition pos, const TVector<TNodePtr>& args)
        : INode(pos)
        , Args_(args)
    {
    }

    TAstNode* Translate(TContext& ctx) const override {
        if (Args_.size() != 1) {
            ctx.Error(Pos_) << "Expected 1 argument in ParseType function";
            return nullptr;
        }

        auto literal = Args_[0]->GetLiteral("String");
        if (!literal) {
            ctx.Error(Args_[0]->GetPos()) << "Expected literal string as argument in ParseType function";
            return nullptr;
        }

        auto parsed = ParseType(*literal, *ctx.Pool, ctx.Issues, Args_[0]->GetPos());
        if (!parsed) {
            ctx.Error(Args_[0]->GetPos()) << "Failed to parse type";
            return nullptr;
        }

        return parsed;
    }

    TNodePtr DoClone() const final {
        return new TYqlParseType(Pos_, CloneContainer(Args_));
    }

    void DoUpdateState() const final {
        State_.Set(ENodeState::Const);
    }

private:
    TVector<TNodePtr> Args_;
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

        Args_[1] = Y("TimezoneId", Args_[1]);
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlAddTimezone(Pos_, CloneContainer(Args_));
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
        if (Args_[0]->IsIntegerLiteral() && TryFromString<ui32>(Args_[0]->GetLiteralValue(), oid)) {
            if (!NPg::HasType(oid)) {
                ctx.Error(Args_[0]->GetPos()) << "Unknown pg type oid: " << oid;
                return false;
            } else {
                Args_[0] = BuildQuotedAtom(Args_[0]->GetPos(), NPg::LookupType(oid).Name);
            }
        } else if (Args_[0]->IsLiteral() && Args_[0]->GetLiteralType() == "String") {
            if (!NPg::HasType(Args_[0]->GetLiteralValue())) {
                ctx.Error(Args_[0]->GetPos()) << "Unknown pg type: " << Args_[0]->GetLiteralValue();
                return false;
            } else {
                Args_[0] = BuildQuotedAtom(Args_[0]->GetPos(), Args_[0]->GetLiteralValue());
            }
        } else {
            ctx.Error(Args_[0]->GetPos()) << "Expecting string literal with pg type name or integer literal with pg type oid";
            return false;
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlPgType(Pos_, CloneContainer(Args_));
    }
};

class TYqlPgConst: public TCallNode {
public:
    TYqlPgConst(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "PgConst", 2, -1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[0]->Init(ctx, src)) {
            return false;
        }

        if (Args_[0]->IsLiteral()) {
            Args_[0] = BuildQuotedAtom(Args_[0]->GetPos(), Args_[0]->GetLiteralValue());
        } else {
            auto value = MakeAtomFromExpression(Pos_, ctx, Args_[0]).Build();
            Args_[0] = value;
        }

        if (Args_.size() > 2) {
            TVector<TNodePtr> typeModArgs;
            typeModArgs.push_back(Args_[1]);
            for (ui32 i = 2; i < Args_.size(); ++i) {
                if (!Args_[i]->IsLiteral()) {
                    ctx.Error(Args_[i]->GetPos()) << "Expecting literal";
                    return false;
                }

                typeModArgs.push_back(BuildQuotedAtom(Args_[i]->GetPos(), Args_[i]->GetLiteralValue()));
            }

            Args_.erase(Args_.begin() + 2, Args_.end());
            Args_.push_back(new TCallNodeImpl(Pos_, "PgTypeMod", typeModArgs));
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlPgConst(Pos_, CloneContainer(Args_));
    }
};

class TYqlPgCast: public TCallNode {
public:
    TYqlPgCast(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "PgCast", 2, -1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (Args_.size() > 2) {
            TVector<TNodePtr> typeModArgs;
            typeModArgs.push_back(Args_[1]);
            for (ui32 i = 2; i < Args_.size(); ++i) {
                if (!Args_[i]->IsLiteral()) {
                    ctx.Error(Args_[i]->GetPos()) << "Expecting literal";
                    return false;
                }

                typeModArgs.push_back(BuildQuotedAtom(Args_[i]->GetPos(), Args_[i]->GetLiteralValue()));
            }

            Args_.erase(Args_.begin() + 2, Args_.end());
            Args_.push_back(new TCallNodeImpl(Pos_, "PgTypeMod", typeModArgs));
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlPgCast(Pos_, CloneContainer(Args_));
    }
};

class TYqlPgOp: public TCallNode {
public:
    TYqlPgOp(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "PgOp", 2, 3, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[0]->Init(ctx, src)) {
            return false;
        }

        if (!Args_[0]->IsLiteral() || Args_[0]->GetLiteralType() != "String") {
            ctx.Error(Args_[0]->GetPos()) << "Expecting string literal as first argument";
            return false;
        }

        Args_[0] = BuildQuotedAtom(Args_[0]->GetPos(), Args_[0]->GetLiteralValue());
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlPgOp(Pos_, CloneContainer(Args_));
    }
};

template <bool RangeFunction>
class TYqlPgCall: public TCallNode {
public:
    TYqlPgCall(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "PgCall", 1, -1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[0]->Init(ctx, src)) {
            return false;
        }

        if (!Args_[0]->IsLiteral() || Args_[0]->GetLiteralType() != "String") {
            ctx.Error(Args_[0]->GetPos()) << "Expecting string literal as first argument";
            return false;
        }

        Args_[0] = BuildQuotedAtom(Args_[0]->GetPos(), Args_[0]->GetLiteralValue());
        Args_.insert(Args_.begin() + 1, RangeFunction ? Q(Y(Q(Y(Q("range"))))) : Q(Y()));
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlPgCall<RangeFunction>(Pos_, CloneContainer(Args_));
    }
};

template <const char* Name>
class TYqlSubqueryFor: public TCallNode {
public:
    TYqlSubqueryFor(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, Name, 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        Args_[0] = Y("EvaluateExpr", Args_[0]);
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlSubqueryFor<Name>(Pos_, CloneContainer(Args_));
    }
};

template <const char* Name>
class TYqlSubqueryOrderBy: public TCallNode {
public:
    TYqlSubqueryOrderBy(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, Name, 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        Args_[1] = Y("EvaluateExpr", Args_[1]);
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlSubqueryOrderBy<Name>(Pos_, CloneContainer(Args_));
    }
};

template <bool Strict>
class TYqlTypeAssert: public TCallNode {
public:
    TYqlTypeAssert(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, Strict ? "EnsureType" : "EnsureConvertibleTo", 2, 3, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[1]->Init(ctx, src)) {
            return false;
        }
        if (Args_.size() == 3) {
            if (!Args_[2]->Init(ctx, src)) {
                return false;
            }

            auto message = MakeAtomFromExpression(Pos_, ctx, Args_[2]).Build();
            Args_[2] = message;
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTypeAssert<Strict>(Pos_, CloneContainer(Args_));
    }
};

class TFromBytes final: public TCallNode {
public:
    TFromBytes(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "FromBytes", 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[1]->Init(ctx, src)) {
            return false;
        }
        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Y("FormatType", Args_[1])).Build();

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TFromBytes(Pos_, CloneContainer(Args_));
    }
};

class TYqlTaggedBase: public TCallNode {
public:
    TYqlTaggedBase(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[1]->Init(ctx, src)) {
            return false;
        }

        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }
};

class TYqlAsTagged final: public TYqlTaggedBase {
public:
    TYqlAsTagged(TPosition pos, const TVector<TNodePtr>& args)
        : TYqlTaggedBase(pos, "AsTagged", args)
    {
    }

    TNodePtr DoClone() const final {
        return new TYqlAsTagged(Pos_, CloneContainer(Args_));
    }
};

class TYqlUntag final: public TYqlTaggedBase {
public:
    TYqlUntag(TPosition pos, const TVector<TNodePtr>& args)
        : TYqlTaggedBase(pos, "Untag", args)
    {
    }

    TNodePtr DoClone() const final {
        return new TYqlUntag(Pos_, CloneContainer(Args_));
    }
};

class TYqlVariant final: public TCallNode {
public:
    TYqlVariant(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "Variant", 3, 3, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[1]->Init(ctx, src)) {
            return false;
        }

        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlVariant(Pos_, CloneContainer(Args_));
    }
};

class TYqlEnum final: public TCallNode {
public:
    TYqlEnum(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "Enum", 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[0]->Init(ctx, src)) {
            return false;
        }

        Args_[0] = MakeAtomFromExpression(Pos_, ctx, Args_[0]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlEnum(Pos_, CloneContainer(Args_));
    }
};

class TYqlAsVariant final: public TCallNode {
public:
    TYqlAsVariant(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "AsVariant", 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[1]->Init(ctx, src)) {
            return false;
        }

        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlAsVariant(Pos_, CloneContainer(Args_));
    }
};

class TYqlAsEnum final: public TCallNode {
public:
    TYqlAsEnum(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "AsEnum", 1, 1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[0]->Init(ctx, src)) {
            return false;
        }

        Args_[0] = MakeAtomFromExpression(Pos_, ctx, Args_[0]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlAsEnum(Pos_, CloneContainer(Args_));
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
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Args_.empty()) {
            Args_[0] = BuildFileNameArgument(Pos_, Args_[0], IsFile ? ctx.Settings.FileAliasPrefix : TString());
        }
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TDerived(Pos_, OpName_, CloneContainer(Args_));
    }

    bool IsLiteral() const override {
        return !Args_.empty() ? Args_[0]->IsLiteral() : false;
    }

    TString GetLiteralType() const override {
        return !Args_.empty() ? Args_[0]->GetLiteralType() : "";
    }

    TString GetLiteralValue() const override {
        return !Args_.empty() ? Args_[0]->GetLiteralValue() : "";
    }
};

class TYqlAtom final: public TYqlAtomBase<TYqlAtom, false> {
    using TBase = TYqlAtomBase<TYqlAtom, false>;
    using TBase::TBase;
};

class TFileYqlAtom final: public TYqlAtomBase<TFileYqlAtom, true> {
    using TBase = TYqlAtomBase<TFileYqlAtom, true>;
    using TBase::TBase;
};

class TTryMember final: public TCallNode {
public:
    TTryMember(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 3, 3, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args_.size() != 3) {
            ctx.Error(Pos_) << OpName_ << " requires exactly three arguments";
            return false;
        }
        for (const auto& arg : Args_) {
            if (!arg->Init(ctx, src)) {
                return false;
            }
        }
        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TTryMember(Pos_, OpName_, CloneContainer(Args_));
    }
};

template <bool Pretty>
class TFormatTypeDiff final: public TCallNode {
public:
    TFormatTypeDiff(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 3, 3, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args_.size() != 2) {
            ctx.Error(Pos_) << OpName_ << " requires exactly 2 arguments";
            return false;
        }
        for (const auto& arg : Args_) {
            if (!arg->Init(ctx, src)) {
                return false;
            }
        }
        Args_.push_back(Q(Pretty ? "true" : "false"));
        OpName_ = "FormatTypeDiff";
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TFormatTypeDiff<Pretty>(GetPos(), OpName_, CloneContainer(Args_));
    }
};

class TAddMember final: public TCallNode {
public:
    TAddMember(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 3, 3, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args_.size() != 3) {
            ctx.Error(Pos_) << OpName_ << " requires exactly three arguments";
            return false;
        }
        for (const auto& arg : Args_) {
            if (!arg->Init(ctx, src)) {
                return false;
            }
        }
        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TAddMember(Pos_, OpName_, CloneContainer(Args_));
    }
};

class TRemoveMember final: public TCallNode {
public:
    TRemoveMember(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args_.size() != 2) {
            ctx.Error(Pos_) << OpName_ << " requires exactly two arguments";
            return false;
        }
        for (const auto& arg : Args_) {
            if (!arg->Init(ctx, src)) {
                return false;
            }
        }
        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TRemoveMember(Pos_, OpName_, CloneContainer(Args_));
    }
};

class TCombineMembers final: public TCallNode {
public:
    TCombineMembers(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 1, -1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args_.empty()) {
            ctx.Error(Pos_) << "CombineMembers requires at least one argument";
            return false;
        }
        for (size_t i = 0; i < Args_.size(); ++i) {
            Args_[i] = Q(Y(Q(""), Args_[i])); // flatten without prefix
        }
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TCombineMembers(Pos_, OpName_, CloneContainer(Args_));
    }
};

class TFlattenMembers final: public TCallNode {
public:
    TFlattenMembers(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, 1, -1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args_.empty()) {
            ctx.Error(Pos_) << OpName_ << " requires at least one argument";
            return false;
        }
        for (size_t i = 0; i < Args_.size(); ++i) {
            if (!Args_[i]->Init(ctx, src)) {
                return false;
            }
            if (Args_[i]->GetTupleSize() == 2) {
                // flatten with prefix
                Args_[i] = Q(Y(
                    MakeAtomFromExpression(Pos_, ctx, Args_[i]->GetTupleElement(0)).Build(),
                    Args_[i]->GetTupleElement(1)));
            } else {
                auto tuple = Y("EnsureTupleSize", Args_[i], Q("2"));
                Args_[i] = Q(Y(
                    MakeAtomFromExpression(Pos_, ctx, Y("Nth", tuple, Q("0"))).Build(),
                    Y("Nth", tuple, Q("1"))));
            }
        }
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TFlattenMembers(Pos_, OpName_, CloneContainer(Args_));
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
                                                 "Date32", "Datetime64", "Timestamp64", "Interval64", "TzDate32", "TzDatetime64", "TzTimestamp64"};
TNodePtr GetDataTypeStringNode(TContext& ctx, TCallNode& node, unsigned argNum, TString* outTypeStrPtr = nullptr) {
    auto errMsgFunc = [&node, argNum]() {
        static std::array<TString, 2> NumToName = {{"first", "second"}};
        TStringBuilder sb;
        sb << "At " << NumToName.at(argNum) << " argument of " << node.GetOpName() << " expected type string, available one of: "
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
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        auto dataTypeStringNode = GetDataTypeStringNode(ctx, *this, 0);
        if (!dataTypeStringNode) {
            return false;
        }
        auto aliasNode = BuildFileNameArgument(Args_[1]->GetPos(), Args_[1], ctx.Settings.FileAliasPrefix);
        OpName_ = "Apply";
        Args_[0] = Y("Udf", Q("File.ByLines"), Y("Void"),
                     Y("TupleType",
                       Y("TupleType", Y("DataType", dataTypeStringNode)),
                       Y("StructType"),
                       Y("TupleType")));

        Args_[1] = Y("FilePath", aliasNode);
        return TCallNode::DoInit(ctx, src);
    }

    TString GetOpName() const override {
        return "ParseFile";
    }

    TNodePtr DoClone() const final {
        return new TYqlParseFileOp(Pos_, CloneContainer(Args_));
    }
};

class TYqlDataType final: public TCallNode {
public:
    TYqlDataType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "DataType", 1, 3, args)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        for (ui32 i = 0; i < Args_.size(); ++i) {
            if (!Args_[i]->Init(ctx, FakeSource_.Get())) {
                return false;
            }

            Args_[i] = MakeAtomFromExpression(Pos_, ctx, Args_[i]).Build();
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlDataType(Pos_, CloneContainer(Args_));
    }

private:
    TSourcePtr FakeSource_;
};

class TYqlResourceType final: public TCallNode {
public:
    TYqlResourceType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "ResourceType", 1, 1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[0]->Init(ctx, src)) {
            return false;
        }

        Args_[0] = MakeAtomFromExpression(Pos_, ctx, Args_[0]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlResourceType(Pos_, CloneContainer(Args_));
    }
};

class TYqlTaggedType final: public TCallNode {
public:
    TYqlTaggedType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "TaggedType", 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[1]->Init(ctx, src)) {
            return false;
        }

        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTaggedType(Pos_, CloneContainer(Args_));
    }
};

class TYqlCallableType final: public TCallNode {
public:
    TYqlCallableType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "CallableType", 2, -1, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[0]->GetTupleNode()) {
            ui32 numOptArgs;
            if (!Parseui32(Args_[0], numOptArgs)) {
                ctx.Error(Args_[0]->GetPos()) << "Expected either tuple or number of optional arguments";
                return false;
            }

            Args_[0] = Q(Y(BuildQuotedAtom(Args_[0]->GetPos(), ToString(numOptArgs))));
        }

        if (!Args_[1]->GetTupleNode()) {
            Args_[1] = Q(Y(Args_[1]));
        }

        for (ui32 index = 2; index < Args_.size(); ++index) {
            if (!Args_[index]->GetTupleNode()) {
                Args_[index] = Q(Y(Args_[index]));
            }
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlCallableType(Pos_, CloneContainer(Args_));
    }
};

class TYqlTupleElementType final: public TCallNode {
public:
    TYqlTupleElementType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "TupleElementType", 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[1]->Init(ctx, src)) {
            return false;
        }

        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlTupleElementType(Pos_, CloneContainer(Args_));
    }
};

class TYqlStructMemberType final: public TCallNode {
public:
    TYqlStructMemberType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "StructMemberType", 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[1]->Init(ctx, src)) {
            return false;
        }

        Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlStructMemberType(Pos_, CloneContainer(Args_));
    }
};

class TYqlCallableArgumentType final: public TCallNode {
public:
    TYqlCallableArgumentType(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "CallableArgumentType", 2, 2, args)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        ui32 index;
        if (!Parseui32(Args_[1], index)) {
            ctx.Error(Args_[1]->GetPos()) << "Expected index of the callable argument";
            return false;
        }

        Args_[1] = BuildQuotedAtom(Args_[1]->GetPos(), ToString(index));
        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlCallableArgumentType(Pos_, CloneContainer(Args_));
    }
};

class TStructTypeNode: public TAstListNode {
public:
    TStructTypeNode(TPosition pos, const TVector<TNodePtr>& exprs)
        : TAstListNode(pos)
        , Exprs_(exprs)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Nodes_.push_back(BuildAtom(Pos_, "StructType", TNodeFlags::Default));
        for (const auto& expr : Exprs_) {
            const auto& label = expr->GetLabel();
            if (!label) {
                ctx.Error(expr->GetPos()) << "Structure does not allow anonymous members";
                return false;
            }
            Nodes_.push_back(Q(Y(Q(label), expr)));
        }
        return TAstListNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TStructTypeNode(Pos_, CloneContainer(Exprs_));
    }

private:
    const TVector<TNodePtr> Exprs_;
};

template <bool IsStrict>
class TYqlIf final: public TCallNode {
public:
    TYqlIf(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, IsStrict ? "IfStrict" : "If", 2, 3, args)
    {
    }

private:
    TCallNode::TPtr DoClone() const override {
        return new TYqlIf(GetPos(), CloneContainer(Args_));
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        Args_[0] = Y("Coalesce", Args_[0], Y("Bool", Q("false")));
        if (Args_.size() == 2) {
            Args_.push_back(Y("Null"));
        }
        return TCallNode::DoInit(ctx, src);
    }
};

class TYqlSubstring final: public TCallNode {
public:
    TYqlSubstring(TPosition pos, const TString& name, const TVector<TNodePtr>& args)
        : TCallNode(pos, name, 2, 3, args)
    {
    }

private:
    TCallNode::TPtr DoClone() const override {
        return new TYqlSubstring(GetPos(), OpName_, CloneContainer(Args_));
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args_.size() == 2) {
            Args_.push_back(Y("Null"));
        }
        return TCallNode::DoInit(ctx, src);
    }
};

class TYqlIn final: public TCallNode {
public:
    TYqlIn(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "IN", 3, 3, args)
    {
    }

private:
    TNodePtr DoClone() const final {
        return new TYqlIn(Pos_, CloneContainer(Args_));
    }
    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        auto key = Args_[0];
        auto inNode = Args_[1];
        auto hints = Args_[2];

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
                if (!ctx.Warning(pos, TIssuesIds::YQL_CONST_SUBREQUEST_IN_LIST, [&](auto& out) {
                        out << "Using subrequest in scalar context after IN, "
                            << "perhaps you should remove "
                            << parenKind << "parenthesis here";
                    })) {
                    return false;
                }
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

        OpName_ = "SqlIn";
        MinArgs_ = MaxArgs_ = 3;
        Args_ = {
            inNode->GetSource() ? inNode->GetSource() : inNode,
            key,
            BuildTuple(pos, hintElements)};

        return TCallNode::DoInit(ctx, src);
    }

    static TNodePtr BuildHint(TPosition pos, const TString& name) {
        return BuildTuple(pos, {BuildQuotedAtom(pos, name, NYql::TNodeFlags::Default)});
    }

    TString GetOpName() const override {
        return "IN predicate";
    }
};

class TYqlUdfBase: public TCallNode {
public:
    TYqlUdfBase(TPosition pos, const TString& name)
        : TCallNode(pos, "Udf", 1, 1, UdfArgs(pos, name))
    {
    }

    TYqlUdfBase(TPosition pos, const TString& name, const TVector<TNodePtr>& args, ui32 argsCount = 2)
        : TCallNode(pos, "Udf", argsCount, argsCount, UdfArgs(pos, name, &args))
    {
    }

protected:
    TYqlUdfBase(TPosition pos, const TString& opName, ui32 minArgs, ui32 maxArgs, const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, minArgs, maxArgs, args)
    {
    }

private:
    static TVector<TNodePtr> UdfArgs(TPosition pos, const TString& name, const TVector<TNodePtr>* args = nullptr) {
        TVector<TNodePtr> res = {BuildQuotedAtom(pos, name)};
        if (args) {
            res.insert(res.end(), args->begin(), args->end());
        }
        return res;
    }

    void DoUpdateState() const override {
        TCallNode::DoUpdateState();
        State_.Set(ENodeState::Aggregated, false /*!RunConfig || RunConfig->IsAggregated()*/);
        State_.Set(ENodeState::Const, true /* FIXME: To avoid CheckAggregationLevel issue for non-const TypeOf. */);
    }

private:
    TNodePtr RunConfig_;
};

class TYqlUdf final: public TYqlUdfBase {
public:
    TYqlUdf(TPosition pos, const TString& name)
        : TYqlUdfBase(pos, name)
    {
    }

    TYqlUdf(TPosition pos, const TString& name, const TVector<TNodePtr>& args, ui32 argsCount = 2)
        : TYqlUdfBase(pos, name, args, argsCount)
    {
    }

private:
    TYqlUdf(const TYqlUdf& other)
        : TYqlUdfBase(other.GetPos(), "Udf", other.MinArgs_, other.MaxArgs_, CloneContainer(other.Args_))
    {
    }

    TNodePtr DoClone() const final {
        return new TYqlUdf(*this);
    }
};

class TYqlTypeConfigUdf final: public TYqlUdfBase {
public:
    TYqlTypeConfigUdf(TPosition pos, const TString& name)
        : TYqlUdfBase(pos, name)
    {
    }

    TYqlTypeConfigUdf(TPosition pos, const TString& name, const TVector<TNodePtr>& args, ui32 argsCount = 2)
        : TYqlUdfBase(pos, name, args, argsCount)
    {
    }

private:
    TYqlTypeConfigUdf(const TYqlTypeConfigUdf& other)
        : TYqlUdfBase(other.GetPos(), "Udf", other.MinArgs_, other.MaxArgs_, CloneContainer(other.Args_))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args_[3]->Init(ctx, src)) {
            return false;
        }

        Args_[3] = MakeAtomFromExpression(Pos_, ctx, Args_[3]).Build();
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
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!src) {
            ctx.Error(Pos_) << GetCallExplain() << " unable use without source";
            return false;
        }

        src->AllColumns();

        if (!ValidateArguments(ctx)) {
            return false;
        }

        bool hasError = false;
        for (auto& arg : Args_) {
            if (!arg->Init(ctx, src)) {
                hasError = true;
                continue;
            }
        }

        if (hasError) {
            return false;
        }

        PrecacheState();

        const auto memberPos = Args_[0]->GetPos();
        TVector<TNodePtr> repackArgs = {BuildAtom(memberPos, "row", NYql::TNodeFlags::Default)};
        if (/* auto literal = */ Args_[1]->GetLiteral("String")) {
            TString targetType;
            if (!GetDataTypeStringNode(ctx, *this, 1, &targetType)) {
                return false;
            }

            repackArgs.push_back(Args_[1]->Q(targetType));
        } else {
            repackArgs.push_back(Args_[1]);
        }

        TVector<TNodePtr> column;
        auto namePtr = Args_[0]->GetColumnName();
        if (!namePtr || !*namePtr) {
            ctx.Error(Pos_) << GetCallExplain() << " expects column name as first argument";
            return false;
        }
        auto memberName = *namePtr;
        column.push_back(Args_[0]->Q(*namePtr));

        if (src->GetJoin() && !src->IsJoinKeysInitializing()) {
            const auto sourcePtr = Args_[0]->GetSourceName();
            if (!sourcePtr || !*sourcePtr) {
                ctx.Error(Pos_) << GetOpName() << " required to have correlation name in case of JOIN for column at first parameter";
                return false;
            }
            column.push_back(Args_[0]->Q(*sourcePtr));
            memberName = DotJoin(*sourcePtr, memberName);
        }
        if (!GetLabel()) {
            SetLabel(memberName);
        }
        repackArgs.push_back(BuildTuple(memberPos, column));
        if (Args_.size() == 3) {
            repackArgs.push_back(Args_[2]);
        }
        ++MinArgs_;
        ++MaxArgs_;
        Args_.swap(repackArgs);

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TWeakFieldOp(Pos_, CloneContainer(Args_));
    }
};

template <bool Join>
class TTableRow final: public INode {
public:
    TTableRow(TPosition pos, const TVector<TNodePtr>& args)
        : TTableRow(pos, args.size())
    {
    }

    TTableRow(TPosition pos, ui32 argsCount)
        : INode(pos)
        , ArgsCount_(argsCount)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!src || src->IsFake()) {
            ctx.Error(Pos_) << TStringBuilder() << (Join ? "Join" : "") << "TableRow requires data source";
            return false;
        }

        if (ArgsCount_ > 0) {
            ctx.Error(Pos_) << "TableRow requires exactly 0 arguments";
            return false;
        }

        src->AllColumns();
        const bool isJoin = src->GetJoin();
        if (!Join && ctx.SimpleColumns && isJoin) {
            TNodePtr block = Y();
            const auto& sameKeyMap = src->GetJoin()->GetSameKeysMap();
            if (sameKeyMap) {
                block = L(block, Y("let", "flatSameKeys", "row"));
                for (const auto& sameKeysPair : sameKeyMap) {
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
            for (auto& joinLabel : src->GetJoin()->GetJoinLabels()) {
                members = L(members, BuildQuotedAtom(Pos_, joinLabel + "."));
            }
            block = L(block, Y("let", "res", Y("DivePrefixMembers", "row", Q(members))));

            for (const auto& sameKeysPair : src->GetJoin()->GetSameKeysMap()) {
                const auto& column = sameKeysPair.first;
                auto addMemberKeyNode = Y("Member", "row", Q(column));
                block = L(block, Y("let", "res", Y("AddMember", "res", Q(column), addMemberKeyNode)));
            }

            Node_ = Y("block", Q(L(block, Y("return", "res"))));
        } else {
            Node_ = ctx.EnableSystemColumns ? Y("RemoveSystemMembers", "row") : BuildAtom(Pos_, "row", 0);
        }
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node_);
        return Node_->Translate(ctx);
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const, false);
    }

    TNodePtr DoClone() const final {
        return new TTableRow<Join>(Pos_, ArgsCount_);
    }

    bool IsTableRow() const final {
        return true;
    }

private:
    const size_t ArgsCount_;
    TNodePtr Node_;
};

TTableRows::TTableRows(TPosition pos, const TVector<TNodePtr>& args)
    : TTableRows(pos, args.size())
{
}

TTableRows::TTableRows(TPosition pos, ui32 argsCount)
    : INode(pos)
    , ArgsCount_(argsCount)
{
}

bool TTableRows::DoInit(TContext& ctx, ISource* /*src*/) {
    if (ArgsCount_ > 0) {
        ctx.Error(Pos_) << "TableRows requires exactly 0 arguments";
        return false;
    }
    Node_ = ctx.EnableSystemColumns ? Y("RemoveSystemMembers", "inputRowsList") : BuildAtom(Pos_, "inputRowsList", 0);
    return true;
}

TAstNode* TTableRows::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(Node_);
    return Node_->Translate(ctx);
}

void TTableRows::DoUpdateState() const {
    State_.Set(ENodeState::Const, false);
}

TNodePtr TTableRows::DoClone() const {
    return MakeIntrusive<TTableRows>(Pos_, ArgsCount_);
}

TSessionWindow::TSessionWindow(TPosition pos, const TVector<TNodePtr>& args)
    : INode(pos)
    , Args_(args)
    , FakeSource_(BuildFakeSource(pos))
    , Valid_(false)
{
}

void TSessionWindow::MarkValid() {
    YQL_ENSURE(!HasState(ENodeState::Initialized));
    Valid_ = true;
}

TNodePtr TSessionWindow::BuildTraits(const TString& label) const {
    YQL_ENSURE(HasState(ENodeState::Initialized));

    auto trueNode = Y("Bool", Q("true"));

    if (Args_.size() == 2) {
        auto timeExpr = Args_[0];
        auto timeoutExpr = Args_[1];

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
        auto sortSpec = Y("SortTraits", Y("TypeOf", label), trueNode, BuildLambda(Pos_, Y("row"), Y("PersistableRepr", timeExpr)));

        return Y("SessionWindowTraits",
                 Y("TypeOf", label),
                 sortSpec,
                 BuildLambda(Pos_, Y("row"), timeExpr),
                 timeoutLambda);
    }

    auto orderExpr = Args_[0];
    auto initLambda = Args_[1];
    auto updateLambda = Args_[2];
    auto calculateLambda = Args_[3];

    auto sortSpec = Y("SortTraits", Y("TypeOf", label), trueNode, BuildLambda(Pos_, Y("row"), Y("PersistableRepr", orderExpr)));

    return Y("SessionWindowTraits",
             Y("TypeOf", label),
             sortSpec,
             initLambda,
             updateLambda,
             calculateLambda);
}

bool TSessionWindow::DoInit(TContext& ctx, ISource* src) {
    if (!src || src->IsFake()) {
        ctx.Error(Pos_) << "SessionWindow requires data source";
        return false;
    }

    if (!(Args_.size() == 2 || Args_.size() == 4)) {
        ctx.Error(Pos_) << "SessionWindow requires either two or four arguments";
        return false;
    }

    if (!Valid_) {
        ctx.Error(Pos_) << "SessionWindow can only be used as a top-level GROUP BY / PARTITION BY expression";
        return false;
    }

    if (Args_.size() == 2) {
        auto timeExpr = Args_[0];
        auto timeoutExpr = Args_[1];
        return timeExpr->Init(ctx, src) && timeoutExpr->Init(ctx, FakeSource_.Get());
    }

    auto orderExpr = Args_[0];
    auto initLambda = Args_[1];
    auto updateLambda = Args_[2];
    auto calculateLambda = Args_[3];
    src->AllColumns();

    return orderExpr->Init(ctx, src) && initLambda->Init(ctx, FakeSource_.Get()) &&
           updateLambda->Init(ctx, FakeSource_.Get()) && calculateLambda->Init(ctx, FakeSource_.Get());
}

TAstNode* TSessionWindow::Translate(TContext&) const {
    YQL_ENSURE(false, "Translate is called for SessionWindow");
    return nullptr;
}

void TSessionWindow::DoUpdateState() const {
    State_.Set(ENodeState::Const, false);
}

TNodePtr TSessionWindow::DoClone() const {
    return new TSessionWindow(Pos_, CloneContainer(Args_));
}

TString TSessionWindow::GetOpName() const {
    return "SessionWindow";
}

template <bool IsStart>
class TSessionStart final: public INode {
public:
    TSessionStart(TPosition pos, const TVector<TNodePtr>& args)
        : INode(pos)
        , ArgsCount_(args.size())
    {
    }

private:
    TSessionStart(TPosition pos, size_t argsCount)
        : INode(pos)
        , ArgsCount_(argsCount)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!src || src->IsFake()) {
            ctx.Error(Pos_) << GetOpName() << " requires data source";
            return false;
        }

        if (ArgsCount_ > 0) {
            ctx.Error(Pos_) << GetOpName() << " requires exactly 0 arguments";
            return false;
        }

        auto windowName = src->GetWindowName();
        OverWindow_ = windowName != nullptr;
        TNodePtr sessionWindow;
        if (windowName) {
            auto spec = src->FindWindowSpecification(ctx, *windowName);
            if (!spec) {
                return false;
            }
            sessionWindow = spec->Session;
            if (!sessionWindow) {
                ctx.Error(Pos_) << GetOpName() << " can not be used with window " << *windowName << ": SessionWindow specification is missing in PARTITION BY";
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
                    ctx.Error(Pos_) << GetOpName() << " can not be used here: SessionWindow specification is missing in GROUP BY" << extra;
                } else {
                    ctx.Error(Pos_) << GetOpName() << " can not be used without aggregation by SessionWindow" << extra;
                }
                return false;
            }

            if (!IsStart) {
                ctx.Error(Pos_) << GetOpName() << " with GROUP BY is not supported yet";
                return false;
            }
        }

        if (sessionWindow->HasState(ENodeState::Failed)) {
            return false;
        }

        YQL_ENSURE(sessionWindow->HasState(ENodeState::Initialized));
        YQL_ENSURE(sessionWindow->GetLabel());
        Node_ = Y("Member", "row", BuildQuotedAtom(Pos_, sessionWindow->GetLabel()));
        if (OverWindow_) {
            Node_ = Y("Member", Node_, BuildQuotedAtom(Pos_, IsStart ? "start" : "state"));
        }
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node_);
        return Node_->Translate(ctx);
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const, false);
        if (OverWindow_) {
            State_.Set(ENodeState::OverWindow, true);
        } else if (IsStart) {
            State_.Set(ENodeState::Aggregated, true);
        }
    }

    TNodePtr DoClone() const override {
        return new TSessionStart<IsStart>(Pos_, ArgsCount_);
    }

    TString GetOpName() const override {
        return IsStart ? "SessionStart" : "SessionState";
    }

    const size_t ArgsCount_;
    bool OverWindow_ = false;
    TNodePtr Node_;
};

THoppingWindow::THoppingWindow(TPosition pos, TVector<TNodePtr> args, bool useNamed)
    : INode(pos)
    , Args_(std::move(args))
    , FakeSource_(BuildFakeSource(pos))
    , UseNamed_(useNamed)
    , Valid_(false)
{
}

TNodePtr THoppingWindow::BuildTraits(const TString& label) const {
    YQL_ENSURE(HasState(ENodeState::Initialized));

    auto result = Y(
        "HoppingTraits",
        Y("ListItemType", Y("TypeOf", label)),
        BuildLambda(Pos_, Y("row"), TimeExtractor_),
        Hop_,
        Interval_,
        Delay_,
        Q(DataWatermarks_),
        Q("v2"));
    if (SizeLimit_ || TimeLimit_ || EarlyPolicy_ || LatePolicy_) {
        result->Add(
            SizeLimit_ ? SizeLimit_ : Y("Void"),
            TimeLimit_ ? TimeLimit_ : Y("Void"),
            EarlyPolicy_ ? EarlyPolicy_ : Y("Void"),
            LatePolicy_ ? LatePolicy_ : Y("Void"));
    }
    return result;
}

TNodePtr THoppingWindow::GetInterval() const {
    return Interval_;
}

void THoppingWindow::MarkValid() {
    YQL_ENSURE(!HasState(ENodeState::Initialized));
    Valid_ = true;
}

bool THoppingWindow::DoInit(TContext& ctx, ISource* src) {
    if (!src || src->IsFake()) {
        ctx.Error(Pos_) << "HoppingWindow requires data source";
        return false;
    }

    if (!Valid_) {
        ctx.Error(Pos_) << "HoppingWindow can only be used as a top-level GROUP BY expression";
        return false;
    }

    TNodePtr timeExtractor;
    TNodePtr hopExpr;
    TNodePtr intervalExpr;
    if (UseNamed_) {
        YQL_ENSURE(Args_.size() == 2);
        auto posArgs = Args_[0]->GetTupleNode();
        Y_DEBUG_ABORT_UNLESS(posArgs);
        if (posArgs->GetTupleSize() != 3) {
            ctx.Error(Pos_) << "HoppingWindow requires three positional arguments";
            return false;
        }
        timeExtractor = posArgs->GetTupleElement(0);
        hopExpr = posArgs->GetTupleElement(1);
        intervalExpr = posArgs->GetTupleElement(2);
        auto namedArgs = Args_[1]->GetStructNode();
        Y_DEBUG_ABORT_UNLESS(namedArgs);
        for (auto arg : namedArgs->GetExprs()) {
            if (!arg->Init(ctx, FakeSource_.Get())) {
                return false;
            }

            const auto& label = arg->GetLabel();
            if (label == "SizeLimit") {
                SizeLimit_ = std::move(arg);
            } else if (label == "TimeLimit") {
                TimeLimit_ = ProcessIntervalParam(arg);
            } else if (label == "EarlyPolicy") {
                EarlyPolicy_ = std::move(arg);
            } else if (label == "LatePolicy") {
                LatePolicy_ = std::move(arg);
            } else {
                ctx.Error(arg->GetPos()) << "HoppingWindow: unsupported parameter: " << label
                                         << "; expected: SizeLimit, TimeLimit, EarlyPolicy, LatePolicy";
                return false;
            }
        }
    } else {
        if (Args_.size() != 3) {
            ctx.Error(Pos_) << "HoppingWindow requires three positional arguments";
            return false;
        }

        timeExtractor = Args_[0];
        hopExpr = Args_[1];
        intervalExpr = Args_[2];
    }

    if (!timeExtractor->Init(ctx, src) ||
        !hopExpr->Init(ctx, FakeSource_.Get()) ||
        !intervalExpr->Init(ctx, FakeSource_.Get())) {
        return false;
    }

    TimeExtractor_ = timeExtractor;
    Hop_ = ProcessIntervalParam(hopExpr);
    Interval_ = ProcessIntervalParam(intervalExpr);

    return true;
}

TAstNode* THoppingWindow::Translate(TContext&) const {
    YQL_ENSURE(false, "Translate is called for HoppingWindow");
    return nullptr;
}

void THoppingWindow::DoUpdateState() const {
    State_.Set(ENodeState::Const, false);
}

TNodePtr THoppingWindow::DoClone() const {
    return new THoppingWindow(Pos_, CloneContainer(Args_), UseNamed_);
}

TString THoppingWindow::GetOpName() const {
    return "HoppingWindow";
}

TNodePtr THoppingWindow::ProcessIntervalParam(const TNodePtr& node) const {
    auto literal = node->GetLiteral("String");
    if (!literal) {
        return Y("EvaluateExpr", node);
    }
    if (*literal == "max") {
        return node;
    }

    return new TYqlData(node->GetPos(), "Interval", {node});
}

TNodePtr BuildUdfUserTypeArg(TPosition pos, const TVector<TNodePtr>& args, TNodePtr externalTypes) {
    TVector<TNodePtr> argsTypeItems;
    for (auto& arg : args) {
        argsTypeItems.push_back(new TCallNodeImpl(pos, "TypeOf", TVector<TNodePtr>(1, arg)));
    }

    TVector<TNodePtr> userTypeItems;
    userTypeItems.push_back(new TCallNodeImpl(pos, "TupleType", argsTypeItems));
    userTypeItems.push_back(new TCallNodeImpl(pos, "StructType", {}));
    if (externalTypes) {
        userTypeItems.push_back(externalTypes);
    } else {
        userTypeItems.push_back(new TCallNodeImpl(pos, "TupleType", {}));
    }

    return new TCallNodeImpl(pos, "TupleType", userTypeItems);
}

TNodePtr BuildUdfUserTypeArg(TPosition pos, TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr externalTypes) {
    TVector<TNodePtr> userTypeItems;
    userTypeItems.reserve(3);
    userTypeItems.push_back(positionalArgs->Y("TypeOf", positionalArgs));
    userTypeItems.push_back(positionalArgs->Y("TypeOf", namedArgs));
    if (externalTypes) {
        userTypeItems.push_back(externalTypes);
    } else {
        userTypeItems.push_back(new TCallNodeImpl(pos, "TupleType", {}));
    }

    return new TCallNodeImpl(pos, "TupleType", userTypeItems);
}

TVector<TNodePtr> BuildUdfArgs(const TContext& ctx, TPosition pos, const TVector<TNodePtr>& args,
                               TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr externalTypes, TNodePtr typeConfig) {
    if (!ctx.Settings.EnableGenericUdfs) {
        return {};
    }
    TVector<TNodePtr> udfArgs;
    udfArgs.push_back(new TAstListNodeImpl(pos));
    udfArgs[0]->Add(new TAstAtomNodeImpl(pos, "Void", 0));
    if (namedArgs) {
        udfArgs.push_back(BuildUdfUserTypeArg(pos, positionalArgs, namedArgs, externalTypes));
    } else {
        udfArgs.push_back(BuildUdfUserTypeArg(pos, args, externalTypes));
    }

    if (typeConfig) {
        udfArgs.push_back(typeConfig);
    }

    return udfArgs;
}

TNodePtr BuildSqlCall(TContext& ctx, TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args,
                      TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr externalTypes, const TDeferredAtom& typeConfig, TNodePtr runConfig,
                      TNodePtr options, const TVector<TNodePtr>& depends)
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
        TVector<TNodePtr> applyArgs = {callable};
        applyArgs.insert(applyArgs.end(), args.begin(), args.end());
        return new TCallNodeImpl(pos, namedArgs ? "NamedApply" : "Apply", applyArgs);
    }

    TVector<TNodePtr> sqlCallArgs;
    sqlCallArgs.push_back(BuildQuotedAtom(pos, fullName));
    if (namedArgs) {
        auto tupleNodePtr = positionalArgs->GetTupleNode();
        YQL_ENSURE(tupleNodePtr);
        TNodePtr positionalArgsNode = new TCallNodeImpl(pos, "PositionalArgs", tupleNodePtr->Elements());
        sqlCallArgs.push_back(BuildTuple(pos, {positionalArgsNode, namedArgs}));
    } else {
        TNodePtr positionalArgsNode = new TCallNodeImpl(pos, "PositionalArgs", args);
        sqlCallArgs.push_back(BuildTuple(pos, {positionalArgsNode}));
    }

    // optional arguments
    if (externalTypes) {
        sqlCallArgs.push_back(externalTypes);
    } else if (!typeConfig.Empty() || runConfig || options || !depends.empty()) {
        sqlCallArgs.push_back(new TCallNodeImpl(pos, "TupleType", {}));
    }

    if (!typeConfig.Empty()) {
        sqlCallArgs.push_back(typeConfig.Build());
    } else if (runConfig || options || !depends.empty()) {
        sqlCallArgs.push_back(BuildQuotedAtom(pos, ""));
    }

    if (runConfig) {
        sqlCallArgs.push_back(runConfig);
    } else if (options || !depends.empty()) {
        sqlCallArgs.push_back(new TCallNodeImpl(pos, "Void", {}));
    }

    if (options) {
        sqlCallArgs.push_back(options);
    } else if (!depends.empty()) {
        sqlCallArgs.push_back(BuildQuote(pos, BuildList(pos)));
    }

    for (const auto& d : depends) {
        sqlCallArgs.push_back(new TCallNodeImpl(pos, "DependsOn", {d}));
    }

    return new TCallNodeImpl(pos, "SqlCall", sqlCallArgs);
}

class TCallableNode final: public INode {
public:
    TCallableNode(TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args, bool forReduce)
        : INode(pos)
        , Module_(module)
        , Name_(name)
        , Args_(args)
        , ForReduce_(forReduce)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Module_ == "yql") {
            Node_ = new TFuncNodeImpl(Pos_, Name_);
        } else if (Module_ == "@yql") {
            auto parsedName = StringContent(ctx, Pos_, Name_);
            if (!parsedName) {
                return false;
            }

            const TString yql("(" + parsedName->Content + ")");
            TAstParseResult ast = ParseAst(yql, ctx.Pool.get());
            /// TODO: do not drop warnings
            if (ast.IsOk()) {
                const auto rootCount = ast.Root->GetChildrenCount();
                if (rootCount != 1) {
                    ctx.Error(Pos_) << "Failed to parse YQL: expecting AST root node with single child, but got " << rootCount;
                    return false;
                }
                Node_ = AstNode(ast.Root->GetChild(0));
            } else {
                ctx.Error(Pos_) << "Failed to parse YQL: " << ast.Issues.ToString();
                return false;
            }

            if (src) {
                src->AllColumns();
            }
        } else if (ctx.Settings.ModuleMapping.contains(Module_)) {
            Node_ = Y("bind", Module_ + "_module", Q(Name_));
            if (src) {
                src->AllColumns();
            }
        } else {
            TNodePtr externalTypes = nullptr;
            if (Module_ == "Tensorflow" && Name_ == "RunBatch") {
                if (Args_.size() > 2) {
                    auto passThroughAtom = Q("PassThrough");
                    auto passThroughType = Y("StructMemberType", Y("ListItemType", Y("TypeOf", Args_[1])), passThroughAtom);
                    externalTypes = Y("AddMemberType", Args_[2], passThroughAtom, passThroughType);
                    Args_.erase(Args_.begin() + 2);
                }
            }

            if ("Datetime" == Module_ || ("Yson" == Module_ && ctx.PragmaYsonFast)) {
                Module_.append('2');
            }

            TNodePtr typeConfig = MakeTypeConfig(Pos_, to_lower(Module_), Args_);
            if (ForReduce_) {
                TVector<TNodePtr> udfArgs;
                udfArgs.push_back(BuildQuotedAtom(Pos_, TString(Module_) + "." + Name_));
                udfArgs.push_back(externalTypes ? externalTypes : new TCallNodeImpl(Pos_, "TupleType", {}));
                if (typeConfig) {
                    udfArgs.push_back(typeConfig);
                }
                Node_ = new TCallNodeImpl(Pos_, "SqlReduceUdf", udfArgs);
            } else {
                auto udfArgs = BuildUdfArgs(ctx, Pos_, Args_, nullptr, nullptr, externalTypes, typeConfig);
                Node_ = BuildUdf(ctx, Pos_, Module_, Name_, udfArgs);
            }
        }
        return Node_->Init(ctx, src);
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node_);
        return Node_->Translate(ctx);
    }

    const TString* FuncName() const override {
        return &Name_;
    }

    const TString* ModuleName() const override {
        return &Module_;
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const, Node_->IsConstant());
        State_.Set(ENodeState::Aggregated, Node_->IsAggregated());
    }

    TNodePtr DoClone() const override {
        return new TCallableNode(Pos_, Module_, Name_, CloneContainer(Args_), ForReduce_);
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node_);
        Node_->VisitTree(func, visited);
    }

private:
    TCiString Module_;
    TString Name_;
    TVector<TNodePtr> Args_;
    TNodePtr Node_;
    const bool ForReduce_;
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
    TScriptUdf(TPosition pos, const TString& moduleName, const TString& funcName, const TVector<TNodePtr>& args,
               TNodePtr options)
        : INode(pos)
        , ModuleName_(moduleName)
        , FuncName_(funcName)
        , Args_(args)
        , Options_(options)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        const bool isPython = ModuleName_.find(TStringBuf("Python")) != TString::npos;
        if (!isPython) {
            if (Args_.size() != 2) {
                ctx.Error(Pos_) << ModuleName_ << " script declaration requires exactly two parameters";
                return false;
            }
        } else {
            if (Args_.size() < 1 || Args_.size() > 2) {
                ctx.Error(Pos_) << ModuleName_ << " script declaration requires one or two parameters";
                return false;
            }
        }

        auto nameAtom = BuildQuotedAtom(Pos_, FuncName_);
        auto scriptNode = Args_.back();
        if (!scriptNode->Init(ctx, src)) {
            return false;
        }
        auto scriptStrPtr = Args_.back()->GetLiteral("String");
        if (!ctx.CompactNamedExprs && scriptStrPtr && scriptStrPtr->size() > SQL_MAX_INLINE_SCRIPT_LEN) {
            scriptNode = ctx.UniversalAlias("scriptudf", std::move(scriptNode));
        }

        INode::TPtr type;
        if (Args_.size() == 2) {
            type = Args_[0];
        } else {
            // Python supports getting functions signatures right from docstrings
            type = Y("EvaluateType", Y("ParseTypeHandle", Y("Apply",
                                                            Y("bind", "core_module", Q("PythonFuncSignature")),
                                                            Q(ModuleName_),
                                                            scriptNode,
                                                            Y("String", nameAtom))));
        }

        if (!type->Init(ctx, src)) {
            return false;
        }

        Node_ = Y("ScriptUdf", Q(ModuleName_), nameAtom, type, scriptNode);
        if (Options_) {
            Node_ = L(Node_, Options_);
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_UNUSED(ctx);
        Y_DEBUG_ABORT_UNLESS(Node_);
        return Node_->Translate(ctx);
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const, true);
    }

    TNodePtr DoClone() const final {
        return new TScriptUdf(GetPos(), ModuleName_, FuncName_, CloneContainer(Args_), Options_);
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node_);
        Node_->VisitTree(func, visited);
    }

    const TString* FuncName() const final {
        return &FuncName_;
    }

    const TString* ModuleName() const final {
        return &ModuleName_;
    }

    bool IsScript() const final {
        return true;
    }

    size_t GetTupleSize() const final {
        return Args_.size();
    }

    TPtr GetTupleElement(size_t index) const final {
        return Args_[index];
    }

private:
    TString ModuleName_;
    TString FuncName_;
    TVector<TNodePtr> Args_;
    TNodePtr Node_;
    TNodePtr Options_;
};

TNodePtr BuildScriptUdf(TPosition pos, const TString& moduleName, const TString& funcName, const TVector<TNodePtr>& args,
                        TNodePtr options) {
    return new TScriptUdf(pos, moduleName, funcName, args, options);
}

template <bool Sorted, bool Hashed>
class TYqlToDict final: public TCallNode {
public:
    TYqlToDict(TPosition pos, const TString& mode, const TVector<TNodePtr>& args)
        : TCallNode(pos, "ToDict", 4, 4, args)
        , Mode_(mode)
    {
    }

private:
    TCallNode::TPtr DoClone() const override {
        return new TYqlToDict<Sorted, Hashed>(GetPos(), Mode_, CloneContainer(Args_));
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Args_.size() != 1) {
            ctx.Error(Pos_) << "ToDict required exactly one argument";
            return false;
        }
        Args_.push_back(BuildLambda(Pos_, Y("val"), Y("Nth", "val", Q("0"))));
        Args_.push_back(BuildLambda(Pos_, Y("val"), Y("Nth", "val", Q("1"))));
        Args_.push_back(Q(Y(Q(Sorted ? "Sorted" : Hashed ? "Hashed"
                                                         : "Auto"), Q(Mode_))));
        return TCallNode::DoInit(ctx, src);
    }

private:
    TString Mode_;
};

template <bool IsStart>
class THoppingTime final: public TAstListNode {
public:
    THoppingTime(TPosition pos, TVector<TNodePtr> args)
        : TAstListNode(pos)
        , Args_(std::move(args))
    {
    }

private:
    bool DoInit(TContext& ctx, ISource* src) override {
        if (!src || src->IsFake()) {
            ctx.Error(Pos_) << GetOpName() << " requires data source";
            return false;
        }

        if (Args_.size() > 0) {
            ctx.Error(Pos_) << GetOpName() << " requires exactly 0 arguments";
            return false;
        }

        auto legacySpec = src->GetLegacyHoppingWindowSpec();
        auto spec = src->GetHoppingWindowSpec();
        if (!legacySpec && !spec) {
            if (src->HasAggregations()) {
                ctx.Error(Pos_) << GetOpName() << " can not be used here: HoppingWindow specification is missing in GROUP BY";
            } else {
                ctx.Error(Pos_) << GetOpName() << " can not be used without aggregation by HoppingWindow";
            }
            return false;
        }

        const auto fieldName = legacySpec
                                   ? "_yql_time"
                                   : spec->GetLabel();

        if constexpr (IsStart) {
            const auto interval = legacySpec
                                      ? legacySpec->Interval
                                      : dynamic_cast<THoppingWindow*>(spec.Get())->GetInterval();

            Add("Sub",
                Y("Member", "row", Q(fieldName)),
                interval);
        } else {
            Add("Member", "row", Q(fieldName));
        }

        return true;
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Aggregated, true);
    }

    TNodePtr DoClone() const override {
        return new THoppingTime<IsStart>(Pos_, CloneContainer(Args_));
    }

    TString GetOpName() const override {
        return IsStart ? "HopStart" : "HopEnd";
    }

private:
    TVector<TNodePtr> Args_;
};

class TInvalidBuiltin final: public INode {
public:
    TInvalidBuiltin(TPosition pos, const TString& info)
        : INode(pos)
        , Info_(info)
    {
    }

    bool DoInit(TContext& ctx, ISource*) override {
        ctx.Error(Pos_) << Info_;
        return false;
    }

    TAstNode* Translate(TContext&) const override {
        return nullptr;
    }

    TPtr DoClone() const override {
        return new TInvalidBuiltin(GetPos(), Info_);
    }

private:
    TString Info_;
};

struct TCoreFuncInfo {
    std::string_view Name;
    ui32 MinArgs;
    ui32 MaxArgs;
};

using TAggrFuncFactoryCallback = std::function<TNodeResult(
    TPosition pos,
    const TVector<TNodePtr>& args,
    EAggregateMode aggMode,
    bool isFactory,
    bool isYqlSelect)>;

struct TAggrFuncFactoryInfo {
    std::string_view CanonicalSqlName;
    std::string_view Kind;
    TAggrFuncFactoryCallback Callback;
};

using TAggrFuncFactoryCallbackMap = std::unordered_map<TString, TAggrFuncFactoryInfo, THash<TString>>;
using TBuiltinFactoryCallback = std::function<TNodePtr(TPosition pos, const TVector<TNodePtr>& args)>;

struct TBuiltinFuncInfo {
    std::string_view CanonicalSqlName;
    std::string_view Kind;
    TBuiltinFactoryCallback Callback;
};

struct TSimplePgFuncInfo {
    std::string_view NativeFuncName;
};

struct TMissingFuncInfo {
    std::string_view Suggestion;
};

using TBuiltinFactoryCallbackMap = std::unordered_map<TString, TBuiltinFuncInfo, THash<TString>>;
using TCoreFuncMap = std::unordered_map<TString, TCoreFuncInfo, THash<TString>>;
using TSimplePgFuncMap = std::unordered_map<TString, TSimplePgFuncInfo, THash<TString>>;
using TMissingFuncMap = std::unordered_map<TString, TMissingFuncInfo, THash<TString>>;

TAggrFuncFactoryCallback BuildAggrFuncFactoryCallback(
    const TString& functionName,
    const TString& factoryName,
    EAggregationType type = NORMAL,
    const TString& functionNameOverride = TString(),
    const TVector<EAggregateMode>& validModes = {})
{
    const TString realFunctionName = functionNameOverride.empty() ? functionName : functionNameOverride;
    // TODO(YQL-20095): Explore real problem to fix this.
    // NOLINTNEXTLINE(bugprone-exception-escape)
    return [functionName, realFunctionName, factoryName, type, validModes](
               TPosition pos,
               const TVector<TNodePtr>& args,
               EAggregateMode aggMode,
               bool isFactory,
               bool isYqlSelect) -> TNodeResult {
        if (!validModes.empty() && !IsIn(validModes, aggMode)) {
            TString errorText;
            if (TVector{EAggregateMode::OverWindow} == validModes) {
                errorText = TStringBuilder()
                            << "Can't use window function " << functionName << " without window specification (OVER keyword is missing)";
            } else {
                errorText = TStringBuilder()
                            << "Can't use " << functionName << " in " << ToString(aggMode) << " aggregation mode";
            }
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, errorText)));
        }

        if (isYqlSelect) {
            TYqlAggregationArgs aggregation = {
                .FunctionName = std::move(realFunctionName),
                .FactoryName = std::move(factoryName),
                .Type = type,
                .Mode = aggMode,
                .Args = args,
            };
            return BuildYqlAggregation(std::move(pos), std::move(aggregation));
        }

        TAggregationPtr factory = BuildAggregationByType(type, pos, realFunctionName, factoryName, aggMode);
        if (isFactory) {
            auto realArgs = args;
            realArgs.erase(realArgs.begin()); // skip function name
            return TNonNull(TNodePtr(new TBasicAggrFactory(pos, functionName, factory, realArgs)));
        } else {
            return TNonNull(TNodePtr(new TBasicAggrFunc(pos, functionName, factory, args)));
        }
    };
}

TAggrFuncFactoryCallback BuildAggrFuncFactoryCallback(
    const TString& functionName,
    const TString& factoryName,
    const TVector<EAggregateMode>& validModes,
    EAggregationType type = NORMAL,
    const TString& functionNameOverride = TString()) {
    return BuildAggrFuncFactoryCallback(functionName, factoryName, type, functionNameOverride, validModes);
}

template <typename TType>
TBuiltinFactoryCallback BuildSimpleBuiltinFactoryCallback() {
    return [](TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, args);
    };
}

template <typename TType>
TBuiltinFactoryCallback BuildNamedBuiltinFactoryCallback(const TString& name) {
    return [name](TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, name, args);
    };
}

template <typename TType>
TBuiltinFactoryCallback BuildArgcBuiltinFactoryCallback(i32 minArgs, i32 maxArgs) {
    return [minArgs, maxArgs](TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, minArgs, maxArgs, args);
    };
}

template <typename TType>
TBuiltinFactoryCallback BuildNamedArgcBuiltinFactoryCallback(const TString& name, i32 minArgs, i32 maxArgs) {
    return [name, minArgs, maxArgs](TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, name, minArgs, maxArgs, args);
    };
}

template <typename TType>
TBuiltinFactoryCallback BuildNamedDepsArgcBuiltinFactoryCallback(ui32 reqArgsCount, const TString& name, i32 minArgs, i32 maxArgs) {
    return [reqArgsCount, name, minArgs, maxArgs](TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(reqArgsCount, pos, name, minArgs, maxArgs, args);
    };
}

template <typename TType>
TBuiltinFactoryCallback BuildBoolBuiltinFactoryCallback(bool arg) {
    return [arg](TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, args, arg);
    };
}

template <typename TType>
TBuiltinFactoryCallback BuildFoldBuiltinFactoryCallback(const TString& name, const TString& defaultValue) {
    return [name, defaultValue](TPosition pos, const TVector<TNodePtr>& args) -> TNodePtr {
        return new TType(pos, name, "Bool", defaultValue, 1, args);
    };
}

TNodePtr MakePair(TPosition pos, const TVector<TNodePtr>& args) {
    TNodePtr list = new TAstListNodeImpl(pos, {args[0],
                                               args.size() > 1 ? args[1] : new TAstListNodeImpl(pos, {new TAstAtomNodeImpl(pos, "Null", TNodeFlags::Default)})});

    return new TAstListNodeImpl(pos, {new TAstAtomNodeImpl(pos, "quote", TNodeFlags::Default),
                                      list});
}

struct TBuiltinFuncData {
    const TBuiltinFactoryCallbackMap BuiltinFuncs;
    const TAggrFuncFactoryCallbackMap AggrFuncs;
    const TCoreFuncMap CoreFuncs;
    const TSimplePgFuncMap SimplePgFuncs;
    const TMissingFuncMap MissingFuncs;

    TBuiltinFuncData()
        : BuiltinFuncs(MakeBuiltinFuncs())
        ,
        AggrFuncs(MakeAggrFuncs())
        ,
        CoreFuncs(MakeCoreFuncs())
        ,
        SimplePgFuncs(MakeSimplePgFuncs())
        ,
        MissingFuncs(MakeMissingFuncs())
    {
    }

    TBuiltinFactoryCallbackMap MakeBuiltinFuncs() {
        TBuiltinFactoryCallbackMap builtinFuncs = {
            // Branching
            {"if", {"If", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlIf<false>>()}},
            {"ifstrict", {"IfStrict", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlIf<true>>()}},

            // String builtins
            {"len", {"Length", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Size", 1, 1)}},
            {"length", {"Length", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Size", 1, 1)}},
            {"charlength", {"Length", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Size", 1, 1)}},
            {"characterlength", {"Length", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Size", 1, 1)}},
            {"substring", {"Substring", "Normal", BuildNamedBuiltinFactoryCallback<TYqlSubstring>("Substring")}},
            {"find", {"Find", "Normal", BuildNamedBuiltinFactoryCallback<TYqlSubstring>("Find")}},
            {"rfind", {"RFind", "Normal", BuildNamedBuiltinFactoryCallback<TYqlSubstring>("RFind")}},
            {"byteat", {"ByteAt", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ByteAt", 2, 2)}},
            {"startswith", {"StartsWith", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StartsWith", 2, 2)}},
            {"endswith", {"EndsWith", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EndsWith", 2, 2)}},
            {"concat", {"Concat", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SqlConcat", 1, -1)}},

            // Numeric builtins
            {"abs", {"Abs", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Abs", 1, 1)}},
            {"tobytes", {"ToBytes", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ToBytes", 1, 1)}},
            {"frombytes", {"FromBytes", "Normal", BuildSimpleBuiltinFactoryCallback<TFromBytes>()}},

            // Compare builtins
            {"minof", {"MinOf", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Min", 1, -1)}},
            {"maxof", {"MaxOf", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Max", 1, -1)}},
            {"greatest", {"MaxOf", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Max", 1, -1)}},
            {"least", {"MinOf", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Min", 1, -1)}},
            {"in", {"", "", BuildSimpleBuiltinFactoryCallback<TYqlIn>()}},

            // List builtins
            {"aslist", {"AsList", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsListMayWarn", 0, -1)}},
            {"asliststrict", {"AsListStrict", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsListStrict", 0, -1)}},
            {"listlength", {"ListLength", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Length", 1, 1)}},
            {"listhasitems", {"ListHasItems", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("HasItems", 1, 1)}},
            {"listextend", {"ListExtend", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListExtend", 0, -1)}},
            {"listextendstrict", {"ListExtendStrict", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListExtendStrict", 0, -1)}},
            {"listunionall", {"ListUnionAll", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListUnionAll", 0, -1)}},
            {"listzip", {"ListZip", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListZip", -1, -1)}},
            {"listzipall", {"ListZipAll", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListZipAll", -1, -1)}},
            {"listenumerate", {"ListEnumerate", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListEnumerate", 1, 3)}},
            {"listreverse", {"ListReverse", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListReverse", 1, 1)}},
            {"listskip", {"ListSkip", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListSkip", 2, 2)}},
            {"listtake", {"ListTake", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTake", 2, 2)}},
            {"listhead", {"ListHead", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListHead", 1, 1)}},
            {"listlast", {"ListLast", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListLast", 1, 1)}},
            {"listsort", {"ListSort", "Normal", BuildBoolBuiltinFactoryCallback<TListSortBuiltin>(true)}},
            {"listsortasc", {"ListSortAsc", "Normal", BuildBoolBuiltinFactoryCallback<TListSortBuiltin>(true)}},
            {"listsortdesc", {"ListSortDesc", "Normal", BuildBoolBuiltinFactoryCallback<TListSortBuiltin>(false)}},
            {"listmap", {"ListMap", "Normal", BuildBoolBuiltinFactoryCallback<TListMapBuiltin>(false)}},
            {"listflatmap", {"ListFlatMap", "Normal", BuildBoolBuiltinFactoryCallback<TListMapBuiltin>(true)}},
            {"listfilter", {"ListFilter", "Normal", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListFilter")}},
            {"listany", {"ListAny", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListAny", 1, 1)}},
            {"listall", {"ListAll", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListAll", 1, 1)}},
            {"listhas", {"ListHas", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListHas", 2, 2)}},
            {"listmax", {"ListMax", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListMax", 1, 1)}},
            {"listmin", {"ListMin", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListMin", 1, 1)}},
            {"listsum", {"ListSum", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListSum", 1, 1)}},
            {"listfold", {"ListFold", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFold", 3, 3)}},
            {"listfold1", {"ListFold1", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFold1", 3, 3)}},
            {"listfoldmap", {"ListFoldMap", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFoldMap", 3, 3)}},
            {"listfold1map", {"ListFold1Map", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFold1Map", 3, 3)}},
            {"listavg", {"ListAvg", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListAvg", 1, 1)}},
            {"listconcat", {"ListConcat", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListConcat", 1, 2)}},
            {"listextract", {"ListExtract", "Normal", BuildSimpleBuiltinFactoryCallback<TListExtractBuiltin>()}},
            {"listuniq", {"ListUniq", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListUniq", 1, 1)}},
            {"listuniqstable", {"ListUniqStable", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListUniqStable", 1, 1)}},
            {"listcreate", {"ListCreate", "Normal", BuildSimpleBuiltinFactoryCallback<TListCreateBuiltin>()}},
            {"listfromrange", {"ListFromRange", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFromRange", 2, 3)}},
            {"listreplicate", {"ListReplicate", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Replicate", 2, 2)}},
            {"listtakewhile", {"ListTakeWhile", "Normal", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListTakeWhile")}},
            {"listskipwhile", {"ListSkipWhile", "Normal", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListSkipWhile")}},
            {"listtakewhileinclusive", {"ListTakeWhileInclusive", "Normal", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListTakeWhileInclusive")}},
            {"listskipwhileinclusive", {"ListSkipWhileInclusive", "Normal", BuildNamedBuiltinFactoryCallback<TListFilterBuiltin>("ListSkipWhileInclusive")}},
            {"listcollect", {"ListCollect", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListCollect", 1, 1)}},
            {"listnotnull", {"ListNotNull", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListNotNull", 1, 1)}},
            {"listflatten", {"ListFlatten", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListFlatten", 1, 1)}},
            {"listtop", {"ListTop", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTop", 2, 3)}},
            {"listtopasc", {"ListTopAsc", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopAsc", 2, 3)}},
            {"listtopdesc", {"ListTopDesc", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopDesc", 2, 3)}},
            {"listtopsort", {"ListTopSort", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopSort", 2, 3)}},
            {"listtopsortasc", {"ListTopSortAsc", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopSortAsc", 2, 3)}},
            {"listtopsortdesc", {"ListTopSortDesc", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTopSortDesc", 2, 3)}},
            {"listsample", {"ListSample", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListSample", 2, 3)}},
            {"listsamplen", {"ListSampleN", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListSampleN", 2, 3)}},
            {"listshuffle", {"ListShuffle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListShuffle", 1, 2)}},

            // Dict builtins
            {"dictlength", {"DictLength", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Length", 1, 1)}},
            {"dicthasitems", {"DictHasItems", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("HasItems", 1, 1)}},
            {"dictcreate", {"DictCreate", "Normal", BuildSimpleBuiltinFactoryCallback<TDictCreateBuiltin>()}},
            {"setcreate", {"SetCreate", "Normal", BuildSimpleBuiltinFactoryCallback<TSetCreateBuiltin>()}},
            {"asdict", {"AsDict", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsDictMayWarn", 0, -1)}},
            {"asdictstrict", {"AsDictStrict", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsDictStrict", 0, -1)}},
            {"asset", {"AsSet", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsSetMayWarn", 0, -1)}},
            {"assetstrict", {"AsSetStrict", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AsSetStrict", 0, -1)}},
            {"todict", {"ToDict", "Normal", BuildNamedBuiltinFactoryCallback<TYqlToDict<false, false>>("One")}},
            {"tomultidict", {"ToMultiDict", "Normal", BuildNamedBuiltinFactoryCallback<TYqlToDict<false, false>>("Many")}},
            {"tosorteddict", {"ToSortedDict", "Normal", BuildNamedBuiltinFactoryCallback<TYqlToDict<true, false>>("One")}},
            {"tosortedmultidict", {"ToSortedMultiDict", "Normal", BuildNamedBuiltinFactoryCallback<TYqlToDict<true, false>>("Many")}},
            {"tohasheddict", {"ToHashedDict", "Normal", BuildNamedBuiltinFactoryCallback<TYqlToDict<false, true>>("One")}},
            {"tohashedmultidict", {"ToHashedMultiDict", "Normal", BuildNamedBuiltinFactoryCallback<TYqlToDict<false, true>>("Many")}},
            {"dictkeys", {"DictKeys", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictKeys", 1, 1)}},
            {"dictpayloads", {"DictPayloads", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictPayloads", 1, 1)}},
            {"dictitems", {"DictItems", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictItems", 1, 1)}},
            {"dictlookup", {"DictLookup", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Lookup", 2, 2)}},
            {"dictcontains", {"DictContains", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Contains", 2, 2)}},

            {"todynamiclinear", {"ToDynamicLinear", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ToDynamicLinear", 1, 1)}},
            {"fromdynamiclinear", {"FromDynamicLinear", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FromDynamicLinear", 1, 1)}},
            {"lineardestroy", {"LinearDestroy", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LinearDestroy", 1, -1)}},
            // MutDict builtins
            {"mutdictcreate", {"MutDictCreate", "Normal", BuildSimpleBuiltinFactoryCallback<TMutDictCreateBuiltin>()}},
            {"tomutdict", {"ToMutDict", "Normal", BuildSimpleBuiltinFactoryCallback<TToMutDictBuiltin>()}},
            {"frommutdict", {"FromMutDict", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FromMutDict", 1, 1)}},
            {"mutdictinsert", {"MutDictInsert", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictInsert", 3, 3)}},
            {"mutdictupsert", {"MutDictUpsert", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictUpsert", 3, 3)}},
            {"mutdictupdate", {"MutDictUpdate", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictUpdate", 3, 3)}},
            {"mutdictremove", {"MutDictRemove", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictRemove", 2, 2)}},
            {"mutdictpop", {"MutDictPop", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictPop", 2, 2)}},
            {"mutdictcontains", {"MutDictContains", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictContains", 2, 2)}},
            {"mutdictlookup", {"MutDictLookup", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictLookup", 2, 2)}},
            {"mutdicthasitems", {"MutDictHasItems", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictHasItems", 1, 1)}},
            {"mutdictlength", {"MutDictLength", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictLength", 1, 1)}},
            {"mutdictitems", {"MutDictItems", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictItems", 1, 1)}},
            {"mutdictkeys", {"MutDictKeys", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictKeys", 1, 1)}},
            {"mutdictpayloads", {"MutDictPayloads", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("MutDictPayloads", 1, 1)}},

            {"dictinsert", {"DictInsert", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictInsert", 3, 3)}},
            {"dictupsert", {"DictUpsert", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictUpsert", 3, 3)}},
            {"dictupdate", {"DictUpdate", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictUpdate", 3, 3)}},
            {"dictremove", {"DictRemove", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictRemove", 2, 2)}},

            // Atom builtins
            {"asatom", {"AsAtom", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlAsAtom>()}},
            {"secureparam", {"SecureParam", "Normal", BuildNamedBuiltinFactoryCallback<TYqlAtom>("SecureParam")}},

            {"void", {"Void", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Void", 0, 0)}},
            {"emptylist", {"EmptyList", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyList", 0, 0)}},
            {"emptydict", {"EmptyDict", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyDict", 0, 0)}},
            {"callable", {"Callable", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Callable", 2, 2)}},
            {"way", {"Way", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Way", 1, 1)}},
            {"dynamicvariant", {"DynamicVariant", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DynamicVariant", 3, 3)}},
            {"variant", {"Variant", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlVariant>()}},
            {"enum", {"Enum", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlEnum>()}},
            {"asvariant", {"AsVariant", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlAsVariant>()}},
            {"asenum", {"AsEnum", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlAsEnum>()}},
            {"astagged", {"AsTagged", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlAsTagged>()}},
            {"untag", {"Untag", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlUntag>()}},
            {"parsetype", {"ParseType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlParseType>()}},
            {"ensuretype", {"EnsureType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlTypeAssert<true>>()}},
            {"ensureconvertibleto", {"EnsureConvertibleTo", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlTypeAssert<false>>()}},
            {"ensure", {"Ensure", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EnsureWarn", 2, 3)}},
            {"withsideeffects", {"WithSideEffects", "Normal", BuildSimpleBuiltinFactoryCallback<TSideEffects<false>>()}},
            {"withsideeffectsmode", {"WithSideEffectsMode", "Normal", BuildSimpleBuiltinFactoryCallback<TSideEffects<true>>()}},
            {"evaluateexpr", {"EvaluateExpr", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EvaluateExpr", 1, 1)}},
            {"evaluateatom", {"EvaluateAtom", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EvaluateAtom", 1, 1)}},
            {"evaluatetype", {"EvaluateType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EvaluateType", 1, 1)}},
            {"block", {"Block", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Block", 1, 1)}},
            {"unwrap", {"Unwrap", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Unwrap", 1, 2)}},
            {"just", {"Just", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Just", 1, 1)}},
            {"nothing", {"Nothing", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Nothing", 1, 1)}},
            {"formattype", {"FormatType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FormatType", 1, 1)}},
            {"formattypediff", {"FormatTypeDiff", "Normal", BuildNamedBuiltinFactoryCallback<TFormatTypeDiff<false>>("FormatTypeDiff")}},
            {"formattypediffpretty", {"FormatTypeDiffPretty", "Normal", BuildNamedBuiltinFactoryCallback<TFormatTypeDiff<true>>("FormatTypeDiffPretty")}},
            {"pgtype", {"PgType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlPgType>()}},
            {"pgconst", {"PgConst", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlPgConst>()}},
            {"pgop", {"PgOp", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlPgOp>()}},
            {"pgcall", {"PgCall", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlPgCall<false>>()}},
            {"pgrangecall", {"PgRangeCall", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlPgCall<true>>()}},
            {"pgcast", {"PgCast", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlPgCast>()}},
            {"frompg", {"FromPg", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FromPg", 1, 1)}},
            {"topg", {"ToPg", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ToPg", 1, 1)}},
            {"pgor", {"PgOr", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgOr", 2, 2)}},
            {"pgand", {"PgAnd", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgAnd", 2, 2)}},
            {"pgnot", {"PgNot", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgNot", 1, 1)}},
            {"pgarray", {"PgArray", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgArray", 1, -1)}},
            {"typeof", {"TypeOf", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TypeOf", 1, 1)}},
            {"instanceof", {"InstanceOf", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("InstanceOf", 1, 1)}},
            {"datatype", {"DataType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlDataType>()}},
            {"optionaltype", {"OptionalType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("OptionalType", 1, 1)}},
            {"listtype", {"ListType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListType", 1, 1)}},
            {"streamtype", {"StreamType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StreamType", 1, 1)}},
            {"dicttype", {"DictType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictType", 2, 2)}},
            {"tupletype", {"TupleType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TupleType", 0, -1)}},
            {"generictype", {"GenericType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("GenericType", 0, 0)}},
            {"unittype", {"UnitType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("UnitType", 0, 0)}},
            {"voidtype", {"VoidType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VoidType", 0, 0)}},
            {"nulltype", {"NullType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("NullType", 0, 0)}},
            {"emptylisttype", {"EmptyListType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyListType", 0, 0)}},
            {"emptydicttype", {"EmptyDictType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyDictType", 0, 0)}},
            {"resourcetype", {"ResourceType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlResourceType>()}},
            {"taggedtype", {"TaggedType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlTaggedType>()}},
            {"varianttype", {"VariantType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VariantType", 1, 1)}},
            {"callabletype", {"CallableType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlCallableType>()}},
            {"lineartype", {"LinearType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LinearType", 1, 1)}},
            {"dynamiclineartype", {"LinearType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DynamicLinearType", 1, 1)}},
            {"linearitemtype", {"LinearItemType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LinearItemType", 1, 1)}},
            {"optionalitemtype", {"OptionalItemType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("OptionalItemType", 1, 1)}},
            {"listitemtype", {"ListItemType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListItemType", 1, 1)}},
            {"streamitemtype", {"ListItemType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StreamItemType", 1, 1)}},
            {"dictkeytype", {"DictKeyType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictKeyType", 1, 1)}},
            {"dictpayloadtype", {"DictPayloadType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictPayloadType", 1, 1)}},
            {"tupleelementtype", {"TupleElementType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlTupleElementType>()}},
            {"structmembertype", {"StructMemberType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlStructMemberType>()}},
            {"callableresulttype", {"CallableResultType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableResultType", 1, 1)}},
            {"callableargumenttype", {"CallableArgumentType", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlCallableArgumentType>()}},
            {"variantunderlyingtype", {"VariantUnderlyingType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VariantUnderlyingType", 1, 1)}},
            {"variantitem", {"VariantItem", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SqlVariantItem", 1, 1)}},
            {"fromysonsimpletype", {"FromYsonSimpleType", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FromYsonSimpleType", 2, 2)}},
            {"currentutcdate", {"CurrentUtcDate", "Normal", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "CurrentUtcDate", 0, -1)}},
            {"currentutcdatetime", {"CurrentUtcDatetime", "Normal", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "CurrentUtcDatetime", 0, -1)}},
            {"currentutctimestamp", {"CurrentUtcTimestamp", "Normal", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "CurrentUtcTimestamp", 0, -1)}},
            {"currenttzdate", {"CurrentTzDate", "Normal", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(1, "CurrentTzDate", 1, -1)}},
            {"currenttzdatetime", {"CurrentTzDatetime", "Normal", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(1, "CurrentTzDatetime", 1, -1)}},
            {"currenttztimestamp", {"CurrentTzTimestamp", "Normal", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(1, "CurrentTzTimestamp", 1, -1)}},
            {"currentoperationid", {"CurrentOperationId", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentOperationId", 0, 0)}},
            {"currentoperationsharedid", {"CurrentOperationSharedId", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentOperationSharedId", 0, 0)}},
            {"currentauthenticateduser", {"CurrentAuthenticatedUser", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentAuthenticatedUser", 0, 0)}},
            {"currentlanguageversion", {"CurrentLanguageVersion", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CurrentLanguageVersion", 0, 0)}},
            {"addtimezone", {"AddTimezone", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlAddTimezone>()}},
            {"removetimezone", {"RemoveTimezone", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("RemoveTimezone", 1, 1)}},
            {"pickle", {"Pickle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Pickle", 1, 1)}},
            {"stablepickle", {"StablePickle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StablePickle", 1, 1)}},
            {"unpickle", {"Unpickle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Unpickle", 2, 2)}},

            {"typehandle", {"TypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TypeHandle", 1, 1)}},
            {"parsetypehandle", {"ParseTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ParseTypeHandle", 1, 1)}},
            {"typekind", {"TypeKind", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TypeKind", 1, 1)}},
            {"datatypecomponents", {"DataTypeComponents", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DataTypeComponents", 1, 1)}},
            {"datatypehandle", {"DataTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DataTypeHandle", 1, 1)}},
            {"optionaltypehandle", {"OptionalTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("OptionalTypeHandle", 1, 1)}},
            {"lineartypehandle", {"LinearTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LinearTypeHandle", 1, 1)}},
            {"dynamiclineartypehandle", {"DynamicLinearTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DynamicLinearTypeHandle", 1, 1)}},
            {"listtypehandle", {"ListTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListTypeHandle", 1, 1)}},
            {"streamtypehandle", {"StreamTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StreamTypeHandle", 1, 1)}},
            {"tupletypecomponents", {"TupleTypeComponents", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TupleTypeComponents", 1, 1)}},
            {"tupletypehandle", {"TupleTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TupleTypeHandle", 1, 1)}},
            {"structtypecomponents", {"StructTypeComponents", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructTypeComponents", 1, 1)}},
            {"structtypehandle", {"StructTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructTypeHandle", 1, 1)}},
            {"dicttypecomponents", {"DictTypeComponents", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictTypeComponents", 1, 1)}},
            {"dicttypehandle", {"DictTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("DictTypeHandle", 2, 2)}},
            {"resourcetypetag", {"ResourceTypeTag", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ResourceTypeTag", 1, 1)}},
            {"resourcetypehandle", {"ResourceTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ResourceTypeHandle", 1, 1)}},
            {"taggedtypecomponents", {"TaggedTypeComponents", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TaggedTypeComponents", 1, 1)}},
            {"taggedtypehandle", {"TaggedTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("TaggedTypeHandle", 2, 2)}},
            {"varianttypehandle", {"VariantTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VariantTypeHandle", 1, 1)}},
            {"voidtypehandle", {"VoidTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("VoidTypeHandle", 0, 0)}},
            {"nulltypehandle", {"NullTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("NullTypeHandle", 0, 0)}},
            {"emptylisttypehandle", {"EmptyListTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyListTypeHandle", 0, 0)}},
            {"emptydicttypehandle", {"EmptyDictTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EmptyDictTypeHandle", 0, 0)}},
            {"callabletypecomponents", {"CallableTypeComponents", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableTypeComponents", 1, 1)}},
            {"callableargument", {"CallableArgument", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableArgument", 1, 3)}},
            {"callabletypehandle", {"CallableTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("CallableTypeHandle", 2, 4)}},
            {"pgtypename", {"PgTypeName", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgTypeName", 1, 1)}},
            {"pgtypehandle", {"PgTypeHandle", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("PgTypeHandle", 1, 1)}},
            {"formatcode", {"FormatCode", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FormatCode", 1, 1)}},
            {"worldcode", {"WorldCode", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("WorldCode", 0, 0)}},
            {"atomcode", {"AtomCode", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AtomCode", 1, 1)}},
            {"listcode", {"ListCode", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ListCode", 0, -1)}},
            {"funccode", {"FuncCode", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("FuncCode", 1, -1)}},
            {"lambdacode", {"LambdaCode", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LambdaCode", 1, 2)}},
            {"evaluatecode", {"EvaluateCode", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("EvaluateCode", 1, 1)}},
            {"reprcode", {"ReprCode", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("ReprCode", 1, 1)}},
            {"quotecode", {"QuoteCode", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("QuoteCode", 1, 1)}},
            {"lambdaargumentscount", {"LambdaArgumentsCount", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LambdaArgumentsCount", 1, 1)}},
            {"lambdaoptionalargumentscount", {"LambdaOptionalArgumentsCount", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("LambdaOptionalArgumentsCount", 1, 1)}},
            {"subqueryextend", {"SubqueryExtend", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SubqueryExtend", 1, -1)}},
            {"subqueryunionall", {"SubqueryUnionAll", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SubqueryUnionAll", 1, -1)}},
            {"subquerymerge", {"SubqueryMerge", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SubqueryMerge", 1, -1)}},
            {"subqueryunionmerge", {"SubqueryUnionMerge", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("SubqueryUnionMerge", 1, -1)}},
            {"subqueryextendfor", {"SubqueryExtendFor", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryFor<SubqueryExtendFor>>()}},
            {"subqueryunionallfor", {"SubqueryUnionAllFor", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryFor<SubqueryUnionAllFor>>()}},
            {"subquerymergefor", {"SubqueryMergeFor", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryFor<SubqueryMergeFor>>()}},
            {"subqueryunionmergefor", {"SubqueryUnionMergeFor", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryFor<SubqueryUnionMergeFor>>()}},
            {"subqueryorderby", {"SubqueryOrderBy", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryOrderBy<SubqueryOrderBy>>()}},
            {"subqueryassumeorderby", {"SubqueryAssumeOrderBy", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlSubqueryOrderBy<SubqueryAssumeOrderBy>>()}},

            // Tuple builtins
            {"astuple", {"AsTuple", "Normal", BuildSimpleBuiltinFactoryCallback<TTupleNode>()}},

            // Struct builtins
            {"trymember", {"TryMember", "Normal", BuildNamedBuiltinFactoryCallback<TTryMember>("TryMember")}},
            {"addmember", {"AddMember", "Normal", BuildNamedBuiltinFactoryCallback<TAddMember>("AddMember")}},
            {"replacemember", {"ReplaceMember", "Normal", BuildNamedBuiltinFactoryCallback<TAddMember>("ReplaceMember")}},
            {"removemember", {"RemoveMember", "Normal", BuildNamedBuiltinFactoryCallback<TRemoveMember>("RemoveMember")}},
            {"forceremovemember", {"ForceRemoveMember", "Normal", BuildNamedBuiltinFactoryCallback<TRemoveMember>("ForceRemoveMember")}},
            {"combinemembers", {"CombineMembers", "Normal", BuildNamedBuiltinFactoryCallback<TCombineMembers>("FlattenMembers")}},
            {"flattenmembers", {"FlattenMembers", "Normal", BuildNamedBuiltinFactoryCallback<TFlattenMembers>("FlattenMembers")}},
            {"staticmap", {"StaticMap", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StaticMap", 2, 2)}},
            {"staticzip", {"StaticZip", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StaticZip", 1, -1)}},
            {"structunion", {"StructUnion", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructUnion", 2, 3)}},
            {"structintersection", {"StructIntersection", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructIntersection", 2, 3)}},
            {"structdifference", {"StructDifference", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructDifference", 2, 2)}},
            {"structsymmetricdifference", {"StructSymmetricDifference", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StructSymmetricDifference", 2, 2)}},
            {"staticfold", {"StaticFold", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StaticFold", 3, 3)}},
            {"staticfold1", {"StaticFold1", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("StaticFold1", 3, 3)}},

            // File builtins
            {"filepath", {"FilePath", "Normal", BuildNamedBuiltinFactoryCallback<TFileYqlAtom>("FilePath")}},
            {"filecontent", {"FileContent", "Normal", BuildNamedBuiltinFactoryCallback<TFileYqlAtom>("FileContent")}},
            {"folderpath", {"FolderPath", "Normal", BuildNamedBuiltinFactoryCallback<TFileYqlAtom>("FolderPath")}},
            {"files", {"Files", "Normal", BuildNamedBuiltinFactoryCallback<TFileYqlAtom>("Files")}},
            {"parsefile", {"ParseFile", "Normal", BuildSimpleBuiltinFactoryCallback<TYqlParseFileOp>()}},

            // Misc builtins
            {"coalesce", {"Coalesce", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Coalesce", 1, -1)}},
            {"nvl", {"Nvl", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Coalesce", 1, -1)}},
            {"nanvl", {"Nanvl", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Nanvl", 2, 2)}},
            {"nullif", {"NullIf", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("NullIf", 2, 2)}},
            {"likely", {"Likely", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Likely", 1, -1)}},
            {"assumestrict", {"AssumeStrict", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AssumeStrict", 1, 1)}},
            {"assumenonstrict", {"AssumeNonStrict", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("AssumeNonStrict", 1, 1)}},
            {"random", {"Random", "Normal", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "Random", 1, -1)}},
            {"randomnumber", {"RandomNumber", "Normal", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "RandomNumber", 1, -1)}},
            {"randomuuid", {"RandomUuid", "Normal", BuildNamedDepsArgcBuiltinFactoryCallback<TCallNodeDepArgs>(0, "RandomUuid", 1, -1)}},
            {"tablepath", {"TablePath", "Normal", BuildNamedBuiltinFactoryCallback<TCallDirectRow>("TablePath")}},
            {"tablerecordindex", {"TableRecordIndex", "Normal", BuildNamedBuiltinFactoryCallback<TCallDirectRow>("TableRecord")}},
            {"tablerow", {"TableRow", "Normal", BuildSimpleBuiltinFactoryCallback<TTableRow<false>>()}},
            {"jointablerow", {"JoinTableRow", "Normal", BuildSimpleBuiltinFactoryCallback<TTableRow<true>>()}},
            {"tablerows", {"TableRows", "Produce", BuildSimpleBuiltinFactoryCallback<TTableRows>()}},
            {"weakfield", {"WeakField", "Normal", BuildSimpleBuiltinFactoryCallback<TWeakFieldOp>()}},
            {"version", {"Version", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallNodeImpl>("Version", 0, 0)}},

            {"systemmetadata", {"SystemMetadata", "Normal", BuildNamedArgcBuiltinFactoryCallback<TCallDirectRow>("SystemMetadata", 1, -1)}},

            // Hint builtins
            {"grouping", {"Grouping", "AggKey", BuildSimpleBuiltinFactoryCallback<TGroupingNode>()}},

            // Window funcitons
            {"rownumber", {"RowNumber", "Window", BuildNamedArgcBuiltinFactoryCallback<TWinRowNumber>("RowNumber", 0, 0)}},
            {"rank", {"Rank", "Window", BuildNamedArgcBuiltinFactoryCallback<TWinRank>("Rank", 0, 1)}},
            {"denserank", {"DenseRank", "Window", BuildNamedArgcBuiltinFactoryCallback<TWinRank>("DenseRank", 0, 1)}},
            {"lead", {"Lead", "Window", BuildNamedArgcBuiltinFactoryCallback<TWinLeadLag>("Lead", 1, 2)}},
            {"lag", {"Lag", "Window", BuildNamedArgcBuiltinFactoryCallback<TWinLeadLag>("Lag", 1, 2)}},
            {"percentrank", {"PercentRank", "Window", BuildNamedArgcBuiltinFactoryCallback<TWinRank>("PercentRank", 0, 1)}},
            {"cumedist", {"CumeDist", "Window", BuildNamedArgcBuiltinFactoryCallback<TWinCumeDist>("CumeDist", 0, 0)}},
            {"ntile", {"NTile", "Window", BuildNamedArgcBuiltinFactoryCallback<TWinNTile>("NTile", 1, 1)}},

            // Session window
            {"sessionwindow", {"SessionWindow", "Partition", BuildSimpleBuiltinFactoryCallback<TSessionWindow>()}},
            {"sessionstart", {"SessionStart", "Agg", BuildSimpleBuiltinFactoryCallback<TSessionStart<true>>()}},
            {"sessionstate", {"SessionState", "Agg", BuildSimpleBuiltinFactoryCallback<TSessionStart<false>>()}},

            // Hopping intervals time functions
            {"hopstart", {"HopStart", "Agg", BuildSimpleBuiltinFactoryCallback<THoppingTime<true>>()}},
            {"hopend", {"HopEnd", "Agg", BuildSimpleBuiltinFactoryCallback<THoppingTime<false>>()}}};
        return builtinFuncs;
    }

    TAggrFuncFactoryCallbackMap MakeAggrFuncs() {
        constexpr auto OverWindow = EAggregateMode::OverWindow;

        TAggrFuncFactoryCallbackMap aggrFuncs = {
            {"min", {"Min", "Agg", BuildAggrFuncFactoryCallback("Min", "min_traits_factory")}},
            {"max", {"Max", "Agg", BuildAggrFuncFactoryCallback("Max", "max_traits_factory")}},

            {"minby", {"MinBy", "Agg", BuildAggrFuncFactoryCallback("MinBy", "min_by_traits_factory", KEY_PAYLOAD)}},
            {"maxby", {"MaxBy", "Agg", BuildAggrFuncFactoryCallback("MaxBy", "max_by_traits_factory", KEY_PAYLOAD)}},

            {"sum", {"Sum", "Agg", BuildAggrFuncFactoryCallback("Sum", "sum_traits_factory")}},
            {"sumif", {"SumIf", "Agg", BuildAggrFuncFactoryCallback("SumIf", "sum_if_traits_factory", PAYLOAD_PREDICATE)}},

            {"checked_sum", {"", "", BuildAggrFuncFactoryCallback("CheckedSum", "checked_sum_traits_factory")}},
            {"checked_sumif", {"", "", BuildAggrFuncFactoryCallback("CheckedSumIf", "checked_sum_if_traits_factory", PAYLOAD_PREDICATE)}},

            {"some", {"Some", "Agg", BuildAggrFuncFactoryCallback("Some", "some_traits_factory")}},
            {"somevalue", {"", "", BuildAggrFuncFactoryCallback("SomeValue", "some_traits_factory")}},

            {"count", {"Count", "Agg", BuildAggrFuncFactoryCallback("Count", "count_traits_factory", COUNT)}},
            {"countif", {"CountIf", "Agg", BuildAggrFuncFactoryCallback("CountIf", "count_if_traits_factory")}},

            {"every", {"", "", BuildAggrFuncFactoryCallback("Every", "and_traits_factory")}},
            {"booland", {"BoolAnd", "Agg", BuildAggrFuncFactoryCallback("BoolAnd", "and_traits_factory")}},
            {"boolor", {"BoolOr", "Agg", BuildAggrFuncFactoryCallback("BoolOr", "or_traits_factory")}},
            {"boolxor", {"BoolXor", "Agg", BuildAggrFuncFactoryCallback("BoolXor", "xor_traits_factory")}},

            {"bitand", {"BitAnd", "Agg", BuildAggrFuncFactoryCallback("BitAnd", "bit_and_traits_factory")}},
            {"bitor", {"BitOr", "Agg", BuildAggrFuncFactoryCallback("BitOr", "bit_or_traits_factory")}},
            {"bitxor", {"BitXor", "Agg", BuildAggrFuncFactoryCallback("BitXor", "bit_xor_traits_factory")}},

            {"avg", {"Avg", "Agg", BuildAggrFuncFactoryCallback("Avg", "avg_traits_factory")}},
            {"avgif", {"AvgIf", "Agg", BuildAggrFuncFactoryCallback("AvgIf", "avg_if_traits_factory", PAYLOAD_PREDICATE)}},

            {"agglist", {"AggList", "Agg", BuildAggrFuncFactoryCallback("AggregateList", "list2_traits_factory", LIST)}},
            {"aggrlist", {"AggList", "Agg", BuildAggrFuncFactoryCallback("AggregateList", "list2_traits_factory", LIST)}},
            {"aggregatelist", {"AggList", "Agg", BuildAggrFuncFactoryCallback("AggregateList", "list2_traits_factory", LIST)}},
            {"agglistdistinct", {"AggListDistinct", "Agg", BuildAggrFuncFactoryCallback("AggregateListDistinct", "set_traits_factory", LIST)}},
            {"aggrlistdistinct", {"AggListDistinct", "Agg", BuildAggrFuncFactoryCallback("AggregateListDistinct", "set_traits_factory", LIST)}},
            {"aggregatelistdistinct", {"AggListDistinct", "Agg", BuildAggrFuncFactoryCallback("AggregateListDistinct", "set_traits_factory", LIST)}},

            {"median", {"Median", "Agg", BuildAggrFuncFactoryCallback("Median", "percentile_traits_factory", PERCENTILE)}},
            {"percentile", {"Percentile", "Agg", BuildAggrFuncFactoryCallback("Percentile", "percentile_traits_factory", PERCENTILE)}},

            {"mode", {"Mode", "Agg", BuildAggrFuncFactoryCallback("Mode", "topfreq_traits_factory", TOPFREQ)}},
            {"topfreq", {"TopFreq", "Agg", BuildAggrFuncFactoryCallback("TopFreq", "topfreq_traits_factory", TOPFREQ)}},

            {"top", {"Top", "Agg", BuildAggrFuncFactoryCallback("Top", "top_traits_factory", TOP)}},
            {"bottom", {"Bottom", "Agg", BuildAggrFuncFactoryCallback("Bottom", "bottom_traits_factory", TOP)}},
            {"topby", {"TopBy", "Agg", BuildAggrFuncFactoryCallback("TopBy", "top_by_traits_factory", TOP_BY)}},
            {"bottomby", {"BottomBy", "Agg", BuildAggrFuncFactoryCallback("BottomBy", "bottom_by_traits_factory", TOP_BY)}},

            {"histogram", {"Histogram", "Agg", BuildAggrFuncFactoryCallback("AdaptiveWardHistogram", "histogram_adaptive_ward_traits_factory", HISTOGRAM, "Histogram")}},
            {"histogramcdf", {"HistogramCDF", "Agg", BuildAggrFuncFactoryCallback("AdaptiveWardHistogramCDF", "histogram_cdf_adaptive_ward_traits_factory", HISTOGRAM, "HistogramCDF")}},
            {"adaptivewardhistogram", {"AdaptiveWardHistogram", "Agg", BuildAggrFuncFactoryCallback("AdaptiveWardHistogram", "histogram_adaptive_ward_traits_factory", HISTOGRAM)}},
            {"adaptivewardhistogramcdf", {"AdaptiveWardHistogramCDF", "Agg", BuildAggrFuncFactoryCallback("AdaptiveWardHistogramCDF", "histogram_cdf_adaptive_ward_traits_factory", HISTOGRAM)}},
            {"adaptiveweighthistogram", {"AdaptiveWeightHistogram", "Agg", BuildAggrFuncFactoryCallback("AdaptiveWeightHistogram", "histogram_adaptive_weight_traits_factory", HISTOGRAM)}},
            {"adaptiveweighthistogramcdf", {"AdaptiveWeightHistogramCDF", "Agg", BuildAggrFuncFactoryCallback("AdaptiveWeightHistogramCDF", "histogram_cdf_adaptive_weight_traits_factory", HISTOGRAM)}},
            {"adaptivedistancehistogram", {"AdaptiveDistanceHistogram", "Agg", BuildAggrFuncFactoryCallback("AdaptiveDistanceHistogram", "histogram_adaptive_distance_traits_factory", HISTOGRAM)}},
            {"adaptivedistancehistogramcdf", {"AdaptiveDistanceHistogramCDF", "Agg", BuildAggrFuncFactoryCallback("AdaptiveDistanceHistogramCDF", "histogram_cdf_adaptive_distance_traits_factory", HISTOGRAM)}},
            {"blockwardhistogram", {"BlockWardHistogram", "Agg", BuildAggrFuncFactoryCallback("BlockWardHistogram", "histogram_block_ward_traits_factory", HISTOGRAM)}},
            {"blockwardhistogramcdf", {"BlockWardHistogramCDF", "Agg", BuildAggrFuncFactoryCallback("BlockWardHistogramCDF", "histogram_cdf_block_ward_traits_factory", HISTOGRAM)}},
            {"blockweighthistogram", {"BlockWeightHistogram", "Agg", BuildAggrFuncFactoryCallback("BlockWeightHistogram", "histogram_block_weight_traits_factory", HISTOGRAM)}},
            {"blockweighthistogramcdf", {"BlockWeightHistogramCDF", "Agg", BuildAggrFuncFactoryCallback("BlockWeightHistogramCDF", "histogram_cdf_block_weight_traits_factory", HISTOGRAM)}},
            {"linearhistogram", {"LinearHistogram", "Agg", BuildAggrFuncFactoryCallback("LinearHistogram", "histogram_linear_traits_factory", LINEAR_HISTOGRAM)}},
            {"linearhistogramcdf", {"LinearHistogramCDF", "Agg", BuildAggrFuncFactoryCallback("LinearHistogramCDF", "histogram_cdf_linear_traits_factory", LINEAR_HISTOGRAM)}},
            {"logarithmichistogram", {"LogarithmicHistogram", "Agg", BuildAggrFuncFactoryCallback("LogarithmicHistogram", "histogram_logarithmic_traits_factory", LINEAR_HISTOGRAM)}},
            {"logarithmichistogramcdf", {"LogarithmicHistogramCDF", "Agg", BuildAggrFuncFactoryCallback("LogarithmicHistogramCDF", "histogram_cdf_logarithmic_traits_factory", LINEAR_HISTOGRAM)}},
            {"loghistogram", {"LogHistogram", "Agg", BuildAggrFuncFactoryCallback("LogarithmicHistogram", "histogram_logarithmic_traits_factory", LINEAR_HISTOGRAM, "LogHistogram")}},
            {"loghistogramcdf", {"LogHistogramCDF", "Agg", BuildAggrFuncFactoryCallback("LogarithmicHistogramCDF", "histogram_cdf_logarithmic_traits_factory", LINEAR_HISTOGRAM, "LogHistogramCDF")}},

            {"hyperloglog", {"HyperLogLog", "Agg", BuildAggrFuncFactoryCallback("HyperLogLog", "hyperloglog_traits_factory", COUNT_DISTINCT_ESTIMATE)}},
            {"hll", {"HLL", "Agg", BuildAggrFuncFactoryCallback("HyperLogLog", "hyperloglog_traits_factory", COUNT_DISTINCT_ESTIMATE, "HLL")}},
            {"countdistinctestimate", {"CountDistinctEstimate", "Agg", BuildAggrFuncFactoryCallback("HyperLogLog", "hyperloglog_traits_factory", COUNT_DISTINCT_ESTIMATE, "CountDistinctEstimate")}},

            {"variance", {"Variance", "Agg", BuildAggrFuncFactoryCallback("Variance", "variance_0_1_traits_factory")}},
            {"stddev", {"StdDev", "Agg", BuildAggrFuncFactoryCallback("StdDev", "variance_1_1_traits_factory")}},
            {"populationvariance", {"PopulationVariance", "Agg", BuildAggrFuncFactoryCallback("VariancePopulation", "variance_0_0_traits_factory")}},
            {"variancepopulation", {"VariancePopulation", "Agg", BuildAggrFuncFactoryCallback("VariancePopulation", "variance_0_0_traits_factory")}},
            {"populationstddev", {"PopulationStdDev", "Agg", BuildAggrFuncFactoryCallback("StdDevPopulation", "variance_1_0_traits_factory")}},
            {"stddevpopulation", {"StdDevPopulation", "Agg", BuildAggrFuncFactoryCallback("StdDevPopulation", "variance_1_0_traits_factory")}},
            {"varpop", {"VarPop", "Agg", BuildAggrFuncFactoryCallback("VariancePopulation", "variance_0_0_traits_factory")}},
            {"stddevpop", {"StdDevPop", "Agg", BuildAggrFuncFactoryCallback("StdDevPopulation", "variance_1_0_traits_factory")}},
            {"varp", {"VarP", "Agg", BuildAggrFuncFactoryCallback("VariancePopulation", "variance_0_0_traits_factory")}},
            {"stddevp", {"StdDevP", "Agg", BuildAggrFuncFactoryCallback("StdDevPopulation", "variance_1_0_traits_factory")}},
            {"variancesample", {"VarianceSample", "Agg", BuildAggrFuncFactoryCallback("VarianceSample", "variance_0_1_traits_factory")}},
            {"stddevsample", {"StdDevSample", "Agg", BuildAggrFuncFactoryCallback("StdDevSample", "variance_1_1_traits_factory")}},
            {"varsamp", {"VarSamp", "Agg", BuildAggrFuncFactoryCallback("VarianceSample", "variance_0_1_traits_factory")}},
            {"stddevsamp", {"StdDevSamp", "Agg", BuildAggrFuncFactoryCallback("StdDevSample", "variance_1_1_traits_factory")}},
            {"vars", {"VarS", "Agg", BuildAggrFuncFactoryCallback("VarianceSample", "variance_0_1_traits_factory")}},
            {"stddevs", {"StdDevS", "Agg", BuildAggrFuncFactoryCallback("StdDevSample", "variance_1_1_traits_factory")}},

            {"correlation", {"Correlation", "Agg", BuildAggrFuncFactoryCallback("Correlation", "correlation_traits_factory", TWO_ARGS)}},
            {"corr", {"Corr", "Agg", BuildAggrFuncFactoryCallback("Correlation", "correlation_traits_factory", TWO_ARGS, "Corr")}},
            {"covariance", {"Covariance", "Agg", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS, "Covariance")}},
            {"covariancesample", {"CovarianceSample", "Agg", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS)}},
            {"covarsamp", {"CovarSamp", "Agg", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS, "CovarSamp")}},
            {"covar", {"Covar", "Agg", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS, "Covar")}},
            {"covars", {"CovarS", "Agg", BuildAggrFuncFactoryCallback("CovarianceSample", "covariance_sample_traits_factory", TWO_ARGS, "CovarS")}},
            {"covariancepopulation", {"CovariancePopulation", "Agg", BuildAggrFuncFactoryCallback("CovariancePopulation", "covariance_population_traits_factory", TWO_ARGS)}},
            {"covarpop", {"CovarPop", "Agg", BuildAggrFuncFactoryCallback("CovariancePopulation", "covariance_population_traits_factory", TWO_ARGS, "CovarPop")}},
            {"covarp", {"CovarP", "Agg", BuildAggrFuncFactoryCallback("CovariancePopulation", "covariance_population_traits_factory", TWO_ARGS, "CovarP")}},

            {"udaf", {"UDAF", "Agg", BuildAggrFuncFactoryCallback("UDAF", "udaf_traits_factory", UDAF)}},

            // Window functions
            {"firstvalue", {"FirstValue", "Window", BuildAggrFuncFactoryCallback("FirstValue", "first_value_traits_factory", {OverWindow})}},
            {"lastvalue", {"LastValue", "Window", BuildAggrFuncFactoryCallback("LastValue", "last_value_traits_factory", {OverWindow})}},
            {"nthvalue", {"NthValue", "Window", BuildAggrFuncFactoryCallback("NthValue", "nth_value_traits_factory", {OverWindow}, NTH_VALUE)}},
            {"firstvalueignorenulls", {"", "", BuildAggrFuncFactoryCallback("FirstValueIgnoreNulls", "first_value_ignore_nulls_traits_factory", {OverWindow})}},
            {"lastvalueignorenulls", {"", "", BuildAggrFuncFactoryCallback("LastValueIgnoreNulls", "last_value_ignore_nulls_traits_factory", {OverWindow})}},
            {"nthvalueignorenulls", {"", "", BuildAggrFuncFactoryCallback("NthValueIgnoreNulls", "nth_value_ignore_nulls_traits_factory", {OverWindow}, NTH_VALUE)}},

            // MatchRecognize navigation functions
            {"first", {"First", "MatchRec", BuildAggrFuncFactoryCallback("First", "first_traits_factory")}},
            {"last", {"Last", "MatchRec", BuildAggrFuncFactoryCallback("Last", "last_traits_factory")}},

            {"randomsample", {"RandomSample", "Agg", BuildAggrFuncFactoryCallback("RandomSample", "random_sample_factory", RANDOM_SAMPLE)}},
            {"randomvalue", {"RandomValue", "Agg", BuildAggrFuncFactoryCallback("RandomValue", "random_value_factory", RANDOM_VALUE)}}};
        return aggrFuncs;
    }

    TCoreFuncMap MakeCoreFuncs() {
        TCoreFuncMap coreFuncs = {
            {"listindexof", {"IndexOf", 2, 2}},
            {"testbit", {"TestBit", 2, 2}},
            {"setbit", {"SetBit", 2, 2}},
            {"clearbit", {"ClearBit", 2, 2}},
            {"flipbit", {"FlipBit", 2, 2}},
            {"toset", {"ToSet", 1, 1}},
            {"setisdisjoint", {"SetIsDisjoint", 2, 2}},
            {"setintersection", {"SetIntersection", 2, 3}},
            {"setincludes", {"SetIncludes", 2, 2}},
            {"setunion", {"SetUnion", 2, 3}},
            {"setdifference", {"SetDifference", 2, 2}},
            {"setsymmetricdifference", {"SetSymmetricDifference", 2, 3}},
            {"listaggregate", {"ListAggregate", 2, 2}},
            {"dictaggregate", {"DictAggregate", 2, 2}},
            {"aggregatetransforminput", {"AggregateTransformInput", 2, 2}},
            {"aggregatetransformoutput", {"AggregateTransformOutput", 2, 2}},
            {"aggregateflatten", {"AggregateFlatten", 1, 1}},
            {"choosemembers", {"ChooseMembers", 2, 2}},
            {"removemembers", {"RemoveMembers", 2, 2}},
            {"forceremovemembers", {"ForceRemoveMembers", 2, 2}},
            {"structmembers", {"StructMembers", 1, 1}},
            {"gathermembers", {"GatherMembers", 1, 1}},
            {"renamemembers", {"RenameMembers", 2, 2}},
            {"forcerenamemembers", {"ForceRenameMembers", 2, 2}},
            {"spreadmembers", {"SpreadMembers", 2, 2}},
            {"forcespreadmembers", {"ForceSpreadMembers", 2, 2}},
            {"listfromtuple", {"ListFromTuple", 1, 1}},
            {"listtotuple", {"ListToTuple", 2, 2}},
            {"opaque", {"Opaque", 1, 1}},
        };
        return coreFuncs;
    }

    TSimplePgFuncMap MakeSimplePgFuncs() {
        TSimplePgFuncMap simplePgFuncs = {
            {"now", {"CurrentUtcTimestamp"}},
            {"to_date", {"DateTime::Parse"}},
            {"round", {"Math::Round"}},
            {"floor", {"Math::Floor"}},
            {"ceil", {"Math::Ceil"}},
            {"date_trunc", {"DateTime::StartOf"}},
            {"date_part", {"DateTime::Get*"}},
            {"to_char", {"DateTime::Format/String::LeftPad/String::RightPad/String::Prec"}},
        };
        return simplePgFuncs;
    }

    TMissingFuncMap MakeMissingFuncs() {
        TMissingFuncMap missingFuncs = {
            {"lower", {"String::AsciiToLower or Unicode::ToLower"}},
            {"tolower", {"String::AsciiToLower or Unicode::ToLower"}},
            {"upper", {"String::AsciiToUpper or Unicode::ToUpper"}},
            {"toupper", {"String::AsciiToUpper or Unicode::ToUpper"}},
            {"replace", {"String::ReplaceAll"}},
            {"todate", {"CAST(_ as Date)"}},
            {"todatetime", {"CAST(_ as DateTime)"}},
            {"today", {"CurrentUtcDate()"}},
            {"curdate", {"CurrentUtcDate()"}},
            {"tolist", {"AsList or DictKeys/DictItems/DictPayloads"}},
            {"tostring", {"CAST(_ as String)"}},
            {"listdistinct", {"ListUniq or ListUniqStable"}},
            {"ypathstring", {"Yson::YPathString"}},
            {"ypathint64", {"Yson::YPathInt64"}},
            {"ypathuint64", {"Yson::YPathUint64"}},
            {"substr", {"Substring or Unicode::Substring"}},
            {"type", {"FormatType(TypeOf(_))"}},
            {"splittolist", {"String::SplitToList or Unicode::SplitToList"}},
            {"listlenght", {"ListLength"}},
            {"listsize", {"ListLength"}},
            {"converttostring", {"Yson::ConvertToString"}},
            {"lookupstring", {"Yson::LookupString"}},
            {"uniq", {"HLL"}},
            {"cnt", {"Count"}},
            {"as_table", {"FROM AS_TABLE(_)"}},
            {"astable", {"FROM AS_TABLE(_)"}},
            {"range", {"FROM RANGE(_)"}},
            {"rand", {"Random(_)"}},
            {"regexp", {"regexp operator or Re2::Match"}},
            {"left", {"Substring or Unicode::Substring"}},
            {"str", {"CAST(_ as String)"}},
            {"values", {"FROM (VALUES _)"}},
            {"has", {"ListHas/DictContains"}},
            {"hasitems", {"ListHasItems/DictHasItems or ListHas/DictContains"}},
            {"mean", {"Avg"}},
            {"average", {"Avg"}},
            {"currentdate", {"CurrentUtcDate"}},
            {"currenttimestamp", {"CurrentUtcTimestamp"}},
            {"regexp_extract", {"Re2::Capture"}},
            {"lenght", {"Length"}},
            {"convert", {"CAST"}},
            {"indexof", {"Find"}},
            {"convertyson", {"Yson::Serialize/Yson::SerializeText/Yson::SerializePretty"}},
            {"each", {"FROM EACH(_)"}},
            {"listcontains", {"ListHas"}},
            {"ifnull", {"operator '\?\?' or Coalesce/NVL"}},
            {"date_format", {"DateTime::Format"}},
            {"str_to_date", {"DateTime::Format"}},
            {"asstring", {"CAST(_ as String)"}},
            {"flatten", {"FROM FLATTEN LIST BY or ListFlatten"}},
            {"jsonextractstring", {"Yson::ParseJson + Yson::LookupString"}},
            {"ysonextractstring", {"Yson::Parse + Yson::LookupString"}},
            {"dateadd", {"DateTime::Update"}},
            {"date_add", {"DateTime::Update"}},
            {"like", {"FROM LIKE(_)"}},
            {"isnull", {"_ IS NULL"}},
            {"is_null", {"_ IS NULL"}},
            {"from_unixtime", {"DateTime::FromSeconds"}},
            {"position", {"Find or Unicode::Find"}},
            {"strpos", {"Find or Unicode::Find"}},
            {"regexp_replace", {"Re2::Replace"}},
            {"toint64", {"CAST(_ as Int64)"}},
            {"touint64", {"CAST(_ as Uint64)"}},
            {"toint32", {"CAST(_ as Int32)"}},
            {"touint32", {"CAST(_ as Uint32)"}},
            {"tofloat32", {"CAST(_ as Float)"}},
            {"tofloat64", {"CAST(_ as Double)"}},
            {"datediff", {"operator '-' + DateTime::To*"}},
            {"date_diff", {"operator '-' + DateTime::To*"}},
            {"timestampdiff", {"operator '-' + DateTime::To*"}},
            {"todate", {"CAST(_ as Date)"}},
            {"todate32", {"CAST(_ as Date32)"}},
            {"trim", {"String::Strip or Unicode::Strip"}},
            {"converttolist", {"Yson::ConvertToList"}},
            {"converttodict", {"Yson::ConvertToDict"}},
            {"multiif", {"CASE WHEN"}},
            {"decode", {"CASE WHEN"}},
            {"hash", {"Digest::*"}},
            {"getlength", {"Length"}},
            {"group_concat", {"AGG_LIST + String::JoinFromList or Unicode::JoinFromList"}},
            {"intervalfromdays", {"DateTime::IntervalFromDays"}},
            {"argmax", {"MaxBy"}},
            {"argmin", {"MinBy"}},
            {"tostartofmonth", {"DateTime::StartOfMonth"}},
            {"startofmonth", {"DateTime::StartOfMonth"}},
            {"tostartofhour", {"DateTime::StartOf"}},
            {"tounixtimestamp", {"DateTime::ToSeconds"}},
            {"unixtimestamp", {"DateTime::ToSeconds"}},
            {"toseconds", {"DateTime::ToSeconds"}},
            {"split", {"String::SplitToList or Unicode::SplitToList"}},
            {"match", {"Re2::Match"}},
            {"regexp_like", {"Re2::Match"}},
            {"contains", {"Find(_) IS NOT NULL or ListHas/DictContains"}},
            {"quantile", {"PERCENTILE"}},
            {"grouparray", {"AGG_LIST"}},
            {"groupuniqarray", {"AGG_LIST_DISTINCT"}},
            {"listagg", {"AGG_LIST"}},
            {"list_agg", {"AGG_LIST"}},
            {"folder", {"FROM FOLDER(_)"}},
            {"tablerecord", {"TableRecordIndex"}},
            {"substring_index", {"Find"}},
            {"month", {"DateTime::GetMonth"}},
            {"year", {"DateTime::GetYear"}},
            {"day", {"DateTime::GetDayOfMonth"}},
            {"listrange", {"ListFromRange"}},
            {"array_agg", {"AGG_LIST"}},
            {"listunique", {"ListUniq or ListUniqStable"}},
            {"charindex", {"Find"}},
            {"size", {"Length/Unicode::GetLength or ListLength/DictLength"}},
            {"string_agg", {"AGG_LIST + String::JoinFromList or Unicode::JoinFromList"}},
            {"listcount", {"ListLength"}},
            {"tablerowindex", {"TableRecordIndex"}},
            {"unnest", {"FROM FLATTEN LIST BY"}},
            {"ypathextract", {"Yson::YPath"}},
            {"arrayjoin", {"FROM FLATTEN LIST BY"}},
            {"yson_value", {"Yson::YPath"}},
            {"countd", {"COUNT (DISTINCT _)"}},
            {"listlen", {"ListLength"}},
            {"unix_timestamp", {"DateTime::ToSeconds + CurrentUtcDatetime"}},
            {"trunc", {"DateTime::StartOf"}},
            {"farm_hash", {"Digest::FarmHash"}},
            {"makedate", {"DateTime::MakeDate"}},
            {"makedatetime", {"DateTime::MakeDatetime"}},
            {"any_value", {"Some"}},
            {"substing", {"Substring"}},
            {"listfirst", {"ListHead"}},
            {"listtail", {"ListLast"}},
            {"filter", {"FROM FILTER(_)"}},
            {"json_extract", {"JSON_QUERY"}},
            {"to_timestamp", {"CAST(_ AS Timestamp)"}},
            {"right", {"Substring/Unicode::Substring + Length/Unicode::GetLength"}},
            {"dictvalues", {"DictPayloads"}},
            {"isnotnull", {"_ IS NOT NULL"}},
            {"instr", {"Find"}},
            {"currentdatetime", {"CurrentUtcDatetime"}},
            {"parsejson", {"Yson::ParseJson"}},
            {"splitbychar", {"String::SplitToList or Unicode::SplitToList"}},
            {"splitbystring", {"String::SplitToList or Unicode::SplitToList"}},
            {"converttostringlist", {"Yson::ConvertToStringList"}},
            {"converttoint64", {"Yson::ConvertToInt64"}},
            {"asoptional", {"Just"}},
            {"getdate", {"CurrentUtcDate"}},
            {"datetrunc", {"DateTime::StartOf"}},
            {"regexp_substr", {"Re2::Capture"}},
            {"sqrt", {"Math::Sqrt"}},
            {"pow", {"Math::Pow"}},
            {"power", {"Math::Pow"}},
            {"path", {"TablePath"}},
            {"log", {"Math::Log"}},
            {"regiontoname", {"Geo::RegionById(_).Name"}},
            {"regiontocountry", {"Geo::RoundRegionById(_,'country').name"}},
            {"formatdatetime", {"DateTime::Format"}},
            {"yesterday", {"CurrentUtcDate() - Interval('P1D')"}},
            {"date_format", {"DateTime::Format"}},
            {"string_split", {"String::SplitToList or Unicode::SplitToList"}},
            {"joinfromlist", {"String::JoinFromList or Unicode::JoinFromList"}},
            {"listjoin", {"String::JoinFromList or Unicode::JoinFromList"}},
            {"array_length", {"ListLength"}},
            {"split_part", {"String::SplitToList or Unicode::SplitToList + operator '[]'"}},
            {"createlist", {"ListCreate"}},
            {"setlength", {"DictLength"}},
            {"lookup", {"DictLookup"}},
            {"arrayelement", {"operator '[]'"}},
            {"hist", {"HISTOGRAM"}},
            {"min_if", {"MIN(IF(_))"}},
            {"minif", {"MIN(IF(_))"}},
            {"max_if", {"MAX(IF(_))"}},
            {"maxif", {"MAX(IF(_))"}},
            {"totuple", {"ListToTuple(_,N)"}},
            {"map", {"ListMap"}},
            {"startofweek", {"DateTime::StartOfWeek"}},
            {"dayofweek", {"DateTime::GetDayOfWeek"}},
            {"nth", {"operator '.'"}},
            {"member", {"operator '.'"}},
        };

        return missingFuncs;
    }
};

TNodeResult BuildBuiltinFunc(
    TContext& ctx,
    TPosition pos,
    TString name,
    const TVector<TNodePtr>& args,
    bool isYqlSelect,
    const TString& originalNameSpace,
    EAggregateMode aggMode,
    bool* mustUseNamed,
    bool warnOnYqlNameSpace)
{
    const TBuiltinFuncData* funcData = Singleton<TBuiltinFuncData>();
    const TBuiltinFactoryCallbackMap& builtinFuncs = funcData->BuiltinFuncs;
    const TAggrFuncFactoryCallbackMap& aggrFuncs = funcData->AggrFuncs;
    const TCoreFuncMap& coreFuncs = funcData->CoreFuncs;
    const TSimplePgFuncMap& simplePgFuncs = funcData->SimplePgFuncs;
    const TMissingFuncMap& missingFuncs = funcData->MissingFuncs;

    for (auto& arg : args) {
        if (!arg) {
            return std::unexpected(ESQLError::Basic);
        }
    }

    TString normalizedName(name);
    TString nameSpace(originalNameSpace);
    TString ns = to_lower(nameSpace);
    if (ns.empty()) {
        TMaybe<TIssue> error = NormalizeName(pos, normalizedName);
        if (!error.Empty()) {
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, error->GetMessage())));
        }

        auto coreFunc = coreFuncs.find(normalizedName);
        if (coreFunc != coreFuncs.end()) {
            ns = "core";
            name = coreFunc->second.Name;
            if (args.size() < coreFunc->second.MinArgs || args.size() > coreFunc->second.MaxArgs) {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << name << " expected from "
                                                                                   << coreFunc->second.MinArgs << " to " << coreFunc->second.MaxArgs << " arguments, but got: " << args.size())));
            }

            if (coreFunc->second.MinArgs != coreFunc->second.MaxArgs) {
                name += ToString(args.size());
            }
        }
    }

    TString lowerName = to_lower(name);

    TString moduleResource;
    if (ctx.Settings.ModuleMapping.contains(ns)) {
        moduleResource = ctx.Settings.ModuleMapping.at(ns);
    }

    if (ns == "js") {
        ns = "javascript";
        nameSpace = "JavaScript";
    }

    if (ns == "datetime2") {
        if (!ctx.Warning(pos, TIssuesIds::YQL_DEPRECATED_DATETIME2, [](auto& out) {
                out << "DateTime2:: is a temporary alias for DateTime:: which will be "
                    << "removed in the future, use DateTime:: instead";
            })) {
            return std::unexpected(ESQLError::Basic);
        }
    }

    if (ns == "datetime") {
        ns = "datetime2";
        nameSpace = "DateTime2";
    }

    // SimplePg shadows builtins
    if (ctx.Scoped->SimplePgByDefault) {
        if (simplePgFuncs.contains(lowerName)) {
            ns = "simplepg";
        }
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

    bool checkFilter = true;
    if (ns == "yql" || ns == "@yql") {
        if (warnOnYqlNameSpace && GetEnv("YQL_DETERMINISTIC_MODE").empty()) {
            if (!ctx.Warning(pos, TIssuesIds::YQL_S_EXPRESSIONS_CALL, [](auto& out) {
                    out << "It is not recommended to directly access s-expressions functions via YQL::" << Endl
                        << "This mechanism is mostly intended for temporary workarounds or internal testing purposes";
                })) {
                return std::unexpected(ESQLError::Basic);
            }
        }

        if (ns == "yql") {
            return TNonNull(TNodePtr(new TCallNodeImpl(pos, name, -1, -1, args)));
        }
    } else if (moduleResource) {
        auto exportName = ns == "core" ? name : "$" + name;
        TVector<TNodePtr> applyArgs = {
            new TCallNodeImpl(pos, "bind", {BuildAtom(pos, ns + "_module", 0), BuildQuotedAtom(pos, exportName)})};
        applyArgs.insert(applyArgs.end(), args.begin(), args.end());
        return TNonNull(TNodePtr(new TCallNodeImpl(pos, "Apply", applyArgs)));
    } else if (ns == "hyperscan" || ns == "pcre" || ns == "pire" || ns.StartsWith("re2")) {
        TString moduleName(nameSpace);
        moduleName.to_title();
        if ((args.size() == 1 || args.size() == 2) && (lowerName.StartsWith("multi") || (ns.StartsWith("re2") && lowerName == "capture"))) {
            TVector<TNodePtr> multiArgs{
                ns.StartsWith("re2") && lowerName == "capture" ? MakePair(pos, args) : args[0],
                new TCallNodeImpl(pos, "Void", 0, 0, {}),
                args[0]};
            auto fullName = moduleName + "." + name;
            return TNonNull(TNodePtr(new TYqlTypeConfigUdf(pos, fullName, multiArgs, multiArgs.size() + 1)));
        } else if (!(ns.StartsWith("re2") && (lowerName == "options" || lowerName == "isvalidregexp"))) {
            auto newArgs = args;
            if (ns.StartsWith("re2")) {
                // convert run config is tuple of string and optional options
                if (args.size() == 1 || args.size() == 2) {
                    newArgs[0] = MakePair(pos, args);
                    if (args.size() == 2) {
                        newArgs.pop_back();
                    }
                } else {
                    return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << ns << "." << name << " expected one or two arguments.")));
                }
            }

            return Wrap(BuildUdf(ctx, pos, moduleName, name, newArgs));
        }
    } else if (ns == "pg" || ns == "pgagg" || ns == "pgproc") {
        bool isAggregateFunc = NYql::NPg::HasAggregation(name, NYql::NPg::EAggKind::Normal);
        bool isNormalFunc = NYql::NPg::HasProc(name, NYql::NPg::EProcKind::Function);
        if (!isAggregateFunc && !isNormalFunc) {
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "Unknown function: " << name)));
        }

        if (isAggregateFunc && isNormalFunc) {
            if (ns == "pg") {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "Ambigious function: " << name << ", use either PgAgg:: or PgProc:: namespace")));
            } else if (ns == "pgagg") {
                isNormalFunc = false;
            } else {
                isAggregateFunc = false;
            }
        }

        if (isAggregateFunc && ns == "pgproc") {
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "Invalid namespace for aggregation function: " << name << ", use either Pg:: or PgAgg:: namespace")));
        }

        if (isNormalFunc && ns == "pgagg") {
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "Invalid namespace for normal function: " << name << ", use either Pg:: or PgProc:: namespace")));
        }

        if (isAggregateFunc) {
            if (aggMode == EAggregateMode::Distinct) {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, "Distinct is not supported yet for PG aggregation ")));
            }

            return BuildAggrFuncFactoryCallback(name, "", EAggregationType::PG)(pos, args, aggMode, false, /*isYqlSelect=*/isYqlSelect);
        } else {
            YQL_ENSURE(isNormalFunc);
            TVector<TNodePtr> pgCallArgs;
            pgCallArgs.push_back(BuildLiteralRawString(pos, name));
            pgCallArgs.insert(pgCallArgs.end(), args.begin(), args.end());
            return TNonNull(TNodePtr(new TYqlPgCall<false>(pos, pgCallArgs)));
        }
    } else if (name == "MakeLibraPreprocessor") {
        if (args.size() != 1) {
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << name << " requires exactly one argument")));
        }

        auto settings = NYT::TNode::CreateMap();

        auto makeUdfArgs = [&args, &pos, &settings]() {
            return TVector<TNodePtr>{
                args[0],
                new TCallNodeImpl(pos, "Void", {}),
                BuildQuotedAtom(pos, NYT::NodeToYsonString(settings))};
        };

        auto structNode = args[0]->GetStructNode();
        if (!structNode) {
            if (auto callNode = args[0]->GetCallNode()) {
                if (callNode->GetOpName() == "AsStruct") {
                    return Wrap(BuildUdf(ctx, pos, nameSpace, name, makeUdfArgs()));
                }
            }

            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << name << " requires struct as argument")));
        }

        for (const auto& item : structNode->GetExprs()) {
            const auto& label = item->GetLabel();
            if (label == "Entities") {
                auto callNode = item->GetCallNode();
                if (!callNode || callNode->GetOpName() != "AsListMayWarn") {
                    return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << name << " entities must be list of strings")));
                }

                auto entities = NYT::TNode::CreateList();
                for (const auto& entity : callNode->GetArgs()) {
                    if (!entity->IsLiteral() || entity->GetLiteralType() != "String") {
                        return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << name << " entity must be string literal")));
                    }
                    entities.Add(entity->GetLiteralValue());
                }

                settings(label, std::move(entities));
            } else if (label == "EntitiesStrategy") {
                if (!item->IsLiteral() || item->GetLiteralType() != "String") {
                    return TNonNull(TNodePtr(new TInvalidBuiltin(
                        pos, TStringBuilder() << name << " entities strategy must be string literal")));
                }

                if (!EqualToOneOf(item->GetLiteralValue(), "whitelist", "blacklist")) {
                    return TNonNull(TNodePtr(new TInvalidBuiltin(
                        pos,
                        TStringBuilder() << name << " got invalid entities strategy: expected 'whitelist' or 'blacklist'")));
                }

                settings(label, item->GetLiteralValue());
            } else if (label == "Mode") {
                if (!item->IsLiteral() || item->GetLiteralType() != "String") {
                    return TNonNull(TNodePtr(new TInvalidBuiltin(
                        pos, TStringBuilder() << name << " mode must be string literal")));
                }

                settings(label, item->GetLiteralValue());
            } else if (EqualToOneOf(label, "BlockstatDict", "ParseWithFat")) {
                continue;
            } else {
                return TNonNull(TNodePtr(new TInvalidBuiltin(
                    pos,
                    TStringBuilder()
                        << name << " got unsupported setting: " << label
                        << "; supported: Entities, EntitiesStrategy, BlockstatDict, ParseWithFat")));
            }
        }

        return Wrap(BuildUdf(ctx, pos, nameSpace, name, makeUdfArgs()));
    } else if (scriptType != NKikimr::NMiniKQL::EScriptType::Unknown) {
        auto scriptName = NKikimr::NMiniKQL::IsCustomPython(scriptType) ? nameSpace : TString(NKikimr::NMiniKQL::ScriptTypeAsStr(scriptType));
        return Wrap(BuildScriptUdf(pos, scriptName, name, args, nullptr));
    } else if (ns.empty()) {
        if (auto simpleType = LookupSimpleType(normalizedName, ctx.FlexibleTypes, /* isPgType = */ false)) {
            const auto type = *simpleType;
            if (NUdf::FindDataSlot(type)) {
                YQL_ENSURE(type != "Decimal");
                return TNonNull(TNodePtr(new TYqlData(pos, type, args)));
            }

            if (type.StartsWith("pg") || type.StartsWith("_pg")) {
                TVector<TNodePtr> pgConstArgs;
                if (!args.empty()) {
                    pgConstArgs.push_back(args.front());
                    pgConstArgs.push_back(new TCallNodeImpl(pos, "PgType", {BuildQuotedAtom(pos,
                                                                                            TString(type.StartsWith("pg") ? "" : "_") + type.substr(type.StartsWith("pg") ? 2 : 3), TNodeFlags::Default)}));
                    pgConstArgs.insert(pgConstArgs.end(), args.begin() + 1, args.end());
                }
                return TNonNull(TNodePtr(new TYqlPgConst(pos, pgConstArgs)));
            } else if (type == "Void" || type == "EmptyList" || type == "EmptyDict") {
                return TNonNull(TNodePtr(new TCallNodeImpl(pos, type, 0, 0, args)));
            } else {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "Can not create objects of type " << type)));
            }
        }

        if (normalizedName == "decimal") {
            if (args.size() == 2) {
                TVector<TNodePtr> dataTypeArgs = {BuildQuotedAtom(pos, "Decimal", TNodeFlags::Default)};
                for (auto& arg : args) {
                    if (auto literal = arg->GetLiteral("Int32")) {
                        dataTypeArgs.push_back(BuildQuotedAtom(pos, *literal, TNodeFlags::Default));
                    } else {
                        dataTypeArgs.push_back(MakeAtomFromExpression(ctx.Pos(), ctx, arg).Build());
                    }
                }
                return TNonNull(TNodePtr(new TCallNodeImpl(pos, "DataType", dataTypeArgs)));
            }
            return TNonNull(TNodePtr(new TYqlData(pos, "Decimal", args)));
        }

        if (normalizedName == "tablename") {
            return TNonNull(TNodePtr(new TTableName(pos, args, ctx.Scoped->CurrService)));
        }

        if (normalizedName == "aggregationfactory") {
            if (args.size() < 1 || !args[0]->GetLiteral("String")) {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, "AGGREGATION_FACTORY requries a function name")));
            }

            auto aggNormalizedName = *args[0]->GetLiteral("String");
            auto error = NormalizeName(pos, aggNormalizedName);
            if (!error.Empty()) {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, error->GetMessage())));
            }

            if (aggNormalizedName == "aggregateby") {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, "AGGREGATE_BY is not allowed to use with AGGREGATION_FACTORY")));
            }

            if (aggNormalizedName == "multiaggregateby") {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, "MULTI_AGGREGATE_BY is not allowed to use with AGGREGATION_FACTORY")));
            }

            if (aggMode == EAggregateMode::Distinct || aggMode == EAggregateMode::OverWindowDistinct) {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, "DISTINCT can only be used in aggregation functions")));
            }

            if (to_lower(*args[0]->GetLiteral("String")).StartsWith("pg::")) {
                auto name = args[0]->GetLiteral("String")->substr(4);
                const bool isAggregateFunc = NYql::NPg::HasAggregation(name, NYql::NPg::EAggKind::Normal);
                if (!isAggregateFunc) {
                    return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "Unknown aggregation function: " << *args[0]->GetLiteral("String"))));
                }

                return BuildAggrFuncFactoryCallback(name, "", EAggregationType::PG)(pos, args, aggMode, true, /*isYqlSelect=*/isYqlSelect);
            }

            AdjustCheckedAggFuncName(aggNormalizedName, ctx);

            auto aggrCallback = aggrFuncs.find(aggNormalizedName);
            if (aggrCallback == aggrFuncs.end()) {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "Unknown aggregation function: " << *args[0]->GetLiteral("String"))));
            }

            switch (ctx.GetColumnReferenceState()) {
                case EColumnRefState::MatchRecognizeMeasures:
                    [[fallthrough]];
                case EColumnRefState::MatchRecognizeDefine:
                    return TNonNull(TNodePtr(new TInvalidBuiltin(pos, "Cannot use aggregation factory inside the MATCH_RECOGNIZE context")));
                default:
                    if ("first" == aggNormalizedName || "last" == aggNormalizedName) {
                        return TNonNull(TNodePtr(new TInvalidBuiltin(pos, "Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context")));
                    }
                    return (*aggrCallback).second.Callback(pos, args, aggMode, true, /*isYqlSelect=*/isYqlSelect);
            }
        }

        if (normalizedName == "aggregateby" || normalizedName == "multiaggregateby") {
            const bool multi = (normalizedName == "multiaggregateby");
            if (args.size() != 2) {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << (multi ? "MULTI_AGGREGATE_BY" : "AGGREGATE_BY") << " requries two arguments")));
            }

            auto name = multi ? "MultiAggregateBy" : "AggregateBy";
            auto aggr = BuildFactoryAggregation(pos, name, "", aggMode, multi);
            return TNonNull(TNodePtr(new TBasicAggrFunc(pos, name, aggr, args)));
        }

        AdjustCheckedAggFuncName(normalizedName, ctx);

        auto aggrCallback = aggrFuncs.find(normalizedName);
        if (aggrCallback != aggrFuncs.end()) {
            TNodeResult result = (*aggrCallback).second.Callback(pos, args, aggMode, false, /*isYqlSelect=*/isYqlSelect);
            if (!result && result.error() == ESQLError::UnsupportedYqlSelect) {
                return UnsupportedYqlSelect(
                    ctx, TStringBuilder() << "Aggregation '"
                                          << (originalNameSpace.empty() ? "" : originalNameSpace)
                                          << (originalNameSpace.empty() ? "" : "::")
                                          << name << "'");
            }
            if (!result) {
                return std::unexpected(result.error());
            }

            switch (ctx.GetColumnReferenceState()) {
                case EColumnRefState::MatchRecognizeMeasures:
                    return Wrap(BuildMatchRecognizeVarAccess(pos, std::move(*result)));
                case EColumnRefState::MatchRecognizeDefine:
                    return Wrap(BuildMatchRecognizeDefineAggregate(ctx.Pos(), normalizedName, args));
                default:
                    if ("first" == normalizedName || "last" == normalizedName) {
                        return TNonNull(TNodePtr(new TInvalidBuiltin(pos, "Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context")));
                    }
                    return result;
            }
        }
        if (aggMode == EAggregateMode::Distinct || aggMode == EAggregateMode::OverWindowDistinct) {
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, "DISTINCT can only be used in aggregation functions")));
        }

        auto builtinCallback = builtinFuncs.find(normalizedName);
        if (builtinCallback != builtinFuncs.end()) {
            return Wrap((*builtinCallback).second.Callback(pos, args));
        } else if (normalizedName == "udf") {
            if (mustUseNamed && *mustUseNamed) {
                *mustUseNamed = false;
            }
            return TNonNull(TNodePtr(new TUdfNode(pos, args)));
        } else if (normalizedName == "fulltextmatch" || normalizedName == "fulltextscore") {
            if (mustUseNamed && *mustUseNamed) {
                *mustUseNamed = false;
            }
            auto fulltextBuiltinName = normalizedName == "fulltextmatch" ? "FulltextMatch" : "FulltextScore";
            return TNonNull(TNodePtr(new TCallNodeImpl(pos, fulltextBuiltinName, args)));
        } else if (normalizedName == "asstruct" || normalizedName == "structtype") {
            if (args.empty()) {
                return TNonNull(TNodePtr(new TCallNodeImpl(pos, normalizedName == "asstruct" ? "AsStruct" : "StructType", 0, 0, args)));
            }

            if (mustUseNamed && *mustUseNamed) {
                *mustUseNamed = false;
                YQL_ENSURE(args.size() == 2);
                Y_DEBUG_ABORT_UNLESS(args[0]->GetTupleNode());
                auto posArgs = args[0]->GetTupleNode();
                if (posArgs->IsEmpty()) {
                    if (normalizedName == "asstruct") {
                        return Wrap(args[1]);
                    } else {
                        Y_DEBUG_ABORT_UNLESS(args[1]->GetStructNode());
                        auto namedArgs = args[1]->GetStructNode();
                        return TNonNull(TNodePtr(new TStructTypeNode(pos, namedArgs->GetExprs())));
                    }
                }
            }
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << (normalizedName == "asstruct" ? "AsStruct" : "StructType") << " requires all argument to be named")));
        } else if (normalizedName == "expandstruct") {
            if (mustUseNamed) {
                if (!*mustUseNamed) {
                    return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "ExpandStruct requires at least one named argument")));
                }
                *mustUseNamed = false;
            }
            YQL_ENSURE(args.size() == 2);
            Y_DEBUG_ABORT_UNLESS(args[0]->GetTupleNode());
            Y_DEBUG_ABORT_UNLESS(args[1]->GetStructNode());
            auto posArgs = args[0]->GetTupleNode();
            if (posArgs->GetTupleSize() != 1) {
                return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "ExpandStruct requires all arguments except first to be named")));
            }

            TVector<TNodePtr> flattenMembersArgs = {
                BuildTuple(pos, {BuildQuotedAtom(pos, ""), posArgs->GetTupleElement(0)}),
                BuildTuple(pos, {BuildQuotedAtom(pos, ""), args[1]}),
            };
            return TNonNull(TNodePtr(new TCallNodeImpl(pos, "FlattenMembers", 2, 2, flattenMembersArgs)));
        } else if (normalizedName == "visit" || normalizedName == "visitordefault") {
            bool withDefault = normalizedName == "visitordefault";
            TNodePtr variant;
            TVector<TNodePtr> labels, handlers;
            TMaybe<TNodePtr> dflt;
            if (mustUseNamed && *mustUseNamed) {
                *mustUseNamed = false;
                auto& positional = *args[0]->GetTupleNode();
                if (positional.GetTupleSize() != (withDefault ? 2 : 1)) {
                    return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << name
                                                                                       << " requires exactly " << (withDefault ? 2 : 1) << " positional arguments when named args are used")));
                }
                auto& named = *args[1]->GetStructNode();
                variant = positional.GetTupleElement(0);
                auto& namedExprs = named.GetExprs();
                labels.reserve(namedExprs.size());
                handlers.reserve(namedExprs.size());
                for (size_t idx = 0; idx < namedExprs.size(); idx++) {
                    labels.push_back(BuildQuotedAtom(pos, namedExprs[idx]->GetLabel()));
                    handlers.push_back(namedExprs[idx]);
                }
                if (withDefault) {
                    dflt = positional.GetTupleElement(positional.GetTupleSize() - 1);
                }
            } else {
                size_t minArgs = withDefault ? 2 : 1;
                if (args.size() < minArgs) {
                    return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << name
                                                                                       << " requires at least " << minArgs << " positional arguments")));
                }
                variant = args[0];
                labels.reserve(args.size() - minArgs);
                handlers.reserve(args.size() - minArgs);
                for (size_t idx = 0; idx < args.size() - minArgs; idx++) {
                    labels.push_back(BuildQuotedAtom(pos, ToString(idx)));
                    handlers.push_back(args[minArgs + idx]);
                }
                if (withDefault) {
                    dflt = args[1];
                }
            }
            TVector<TNodePtr> resultArgs;
            resultArgs.reserve(1 + labels.size() + handlers.size());
            resultArgs.emplace_back(std::move(variant));
            for (size_t idx = 0; idx < labels.size(); idx++) {
                resultArgs.emplace_back(std::move(labels[idx]));
                resultArgs.emplace_back(std::move(handlers[idx]));
            }
            if (dflt.Defined()) {
                resultArgs.emplace_back(std::move(dflt->Get()));
            }
            return TNonNull(TNodePtr(new TCallNodeImpl(pos, "SqlVisit", 1, -1, resultArgs)));
        } else if (normalizedName == "sqlexternalfunction") {
            return TNonNull(TNodePtr(new TCallNodeImpl(pos, "SqlExternalFunction", args)));
        } else if (normalizedName == "hoppingwindow") {
            bool useNamed = mustUseNamed && *mustUseNamed;
            if (useNamed) {
                *mustUseNamed = false;
            }
            return TNonNull(TNodePtr(new THoppingWindow(pos, args, useNamed)));
        } else {
            TStringBuilder b;
            b << "Unknown builtin: " << name;
            auto simplePgFunc = simplePgFuncs.find(lowerName);
            if (simplePgFunc != simplePgFuncs.end()) {
                b << ", consider using " << simplePgFunc->second.NativeFuncName << " function instead.";
                b << " It's possible to use SimplePg::" << lowerName << " function as well but with some performance overhead.";
            } else if (auto it = missingFuncs.find(lowerName); it != missingFuncs.end()) {
                b << ", consider using " << it->second.Suggestion << " function(s) instead.";
            } else {
                bool isAggregateFunc = NYql::NPg::HasAggregation(name, NYql::NPg::EAggKind::Normal);
                bool isNormalFunc = NYql::NPg::HasProc(name, NYql::NPg::EProcKind::Function);
                if (isAggregateFunc) {
                    b << ", consider using PgAgg::" << name;
                } else if (isNormalFunc) {
                    b << ", consider using Pg::" << name;
                }
            }

            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, b)));
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

    TNodePtr externalTypes = nullptr;
    if (ns == "json") {
        if (!ctx.Warning(pos, TIssuesIds::YQL_DEPRECATED_JSON_UDF, [](auto& out) {
                out << "Json UDF is deprecated. Please use JSON API instead";
            })) {
            return std::unexpected(ESQLError::Basic);
        }

        ns = "yson";
        nameSpace = "Yson";
        if (lowerName == "serialize") {
            name = "SerializeJson";
            lowerName = to_lower(name);
        } else if (lowerName == "parse") {
            name = "ParseJson";
            lowerName = to_lower(name);
        }
    }

    if (ctx.PragmaYsonFast && ns == "yson") {
        ns.append('2');
        nameSpace.append('2');
    }

    if (ns.StartsWith("yson")) {
        if (lowerName == "convertto" && usedArgs.size() > 1) {
            externalTypes = usedArgs[1];
            usedArgs.erase(usedArgs.begin() + 1);
        }

        if (lowerName == "serialize") {
            if (usedArgs) {
                usedArgs.resize(1U);
            }
        } else if (ctx.PragmaYsonFast && lowerName == "serializejsonencodeutf8") {
            name = "SerializeJson";
            lowerName = to_lower(name);
            if (usedArgs.size() < 2U) {
                usedArgs.emplace_back(BuildYsonOptionsNode(pos, ctx.PragmaYsonAutoConvert, ctx.PragmaYsonStrict, ctx.PragmaYsonFast));
            }
            positionalArgs = BuildTuple(pos, usedArgs);
            auto encodeUtf8 = BuildLiteralBool(pos, true);
            encodeUtf8->SetLabel("EncodeUtf8");
            namedArgs = BuildStructure(pos, {encodeUtf8});
            usedArgs = {positionalArgs, namedArgs};
        } else if (lowerName.StartsWith("from")) {
            name = "From";
            lowerName = to_lower(name);
        } else if (lowerName == "getlength" || lowerName.StartsWith("convertto") || lowerName.StartsWith("parse") || lowerName.StartsWith("serializejson")) {
            if (usedArgs.size() < 2U) {
                usedArgs.emplace_back(BuildYsonOptionsNode(pos, ctx.PragmaYsonAutoConvert, ctx.PragmaYsonStrict, ctx.PragmaYsonFast));
            }
        } else if (lowerName == "contains" || lowerName.StartsWith("lookup") || lowerName.StartsWith("ypath")) {
            if (usedArgs.size() < 3U) {
                usedArgs.push_back(BuildYsonOptionsNode(pos, ctx.PragmaYsonAutoConvert, ctx.PragmaYsonStrict, ctx.PragmaYsonFast));
            }
        }
    }

    if (ns == "datetime2" && lowerName == "update") {
        if (namedArgs) {
            TStructNode* castedNamedArgs = namedArgs->GetStructNode();
            Y_DEBUG_ABORT_UNLESS(castedNamedArgs);
            auto exprs = castedNamedArgs->GetExprs();
            for (auto& arg : exprs) {
                if (arg->GetLabel() == "Timezone") {
                    arg = new TCallNodeImpl(pos, "TimezoneId", 1, 1, {arg});
                    arg->SetLabel("TimezoneId");
                }
            }

            namedArgs = BuildStructure(pos, exprs);
            usedArgs.pop_back();
            usedArgs.push_back(namedArgs);
        };
    }

    if (ns == "simplepg") {
        checkFilter = false;
        auto simplePgFunc = simplePgFuncs.find(lowerName);
        if (simplePgFunc == simplePgFuncs.end()) {
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "Unknown function: SimplePg::" << name)));
        }

        nameSpace = "SimplePg";
        name = lowerName;
        if (!ctx.Warning(pos, TIssuesIds::CORE_SIMPLE_PG, [&](auto& out) {
                out << "Consider using function " << simplePgFunc->second.NativeFuncName << " instead to avoid performance overhead";
            })) {
            return std::unexpected(ESQLError::Basic);
        }
    }

    if (checkFilter && ctx.Settings.UdfFilter) {
        if (ns == "yson2") {
            ns = "yson";
        } else if (ns == "datetime2") {
            ns = "datetime";
        }

        auto ptr = ctx.Settings.UdfFilter->FindPtr(ns);
        if (ptr && !ptr->contains(lowerName)) {
            return TNonNull(TNodePtr(new TInvalidBuiltin(pos, TStringBuilder() << "Unknown function: " << originalNameSpace << "::" << name)));
        }
    }

    TNodePtr typeConfig = MakeTypeConfig(pos, ns, usedArgs);
    return Wrap(BuildSqlCall(ctx, pos, nameSpace, name, usedArgs, positionalArgs, namedArgs, externalTypes,
                             TDeferredAtom(typeConfig, ctx), nullptr, nullptr, {}));
}

void EnumerateBuiltins(const std::function<void(std::string_view name, std::string_view kind)>& callback) {
    const TBuiltinFuncData* funcData = Singleton<TBuiltinFuncData>();
    const TBuiltinFactoryCallbackMap& builtinFuncs = funcData->BuiltinFuncs;
    const TAggrFuncFactoryCallbackMap& aggrFuncs = funcData->AggrFuncs;
    const TCoreFuncMap& coreFuncs = funcData->CoreFuncs;
    const TSimplePgFuncMap& simplePgFuncs = funcData->SimplePgFuncs;

    std::map<TString, TString> map;
    for (const auto& x : builtinFuncs) {
        if (!x.second.CanonicalSqlName.empty()) {
            map.emplace(x.second.CanonicalSqlName, x.second.Kind);
        }
    }

    for (const auto& x : aggrFuncs) {
        if (!x.second.CanonicalSqlName.empty()) {
            map.emplace(x.second.CanonicalSqlName, x.second.Kind);
        }
    }

    for (const auto& x : coreFuncs) {
        map.emplace(x.second.Name, "Normal");
    }

    for (const auto& x : simplePgFuncs) {
        map.emplace(TString("SimplePg::") + x.first, "Normal");
    }

    for (const auto& x : map) {
        callback(x.first, x.second);
    }
}

} // namespace NSQLTranslationV1
