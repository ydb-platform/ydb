#include "select_yql.h"

#include "context.h"

#include <util/generic/overloaded.h>
#include <util/generic/scope.h>

namespace NSQLTranslationV1 {

class TYqlTableRefNode final: public INode, private TYqlTableRefArgs {
public:
    TYqlTableRefNode(TPosition position, TYqlTableRefArgs&& args)
        : INode(std::move(position))
        , TYqlTableRefArgs(std::move(args))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);

        const TString ref = ctx.MakeName("yql_read");
        TNodePtr read = Y("Read!", "world", BuildDataSource(), BuildKey(), Y("Void"), Q(Y()));

        TBlocks& blocks = ctx.GetCurrentBlocks();
        blocks.emplace_back(Y("let", ref, std::move(read)));
        blocks.emplace_back(Y("let", "world", Y("Left!", ref)));
        Node_ = Y("Right!", ref);

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

    TNodePtr DoClone() const override {
        return new TYqlTableRefNode(*this);
    }

private:
    TNodePtr BuildDataSource() const {
        return Y("DataSource", Q(Service), Q(Cluster));
    }

    TNodePtr BuildKey() const {
        if (IsAnonymous) {
            return Y("TempTable", Q(Key));
        }

        return Y("Key", Q(Y(Q("table"), Y("String", Q(Key)))));
    }

    TNodePtr Node_;
};

class TYqlValuesNode final: public INode, private TYqlValuesArgs {
public:
    TYqlValuesNode(TPosition position, TYqlValuesArgs&& args)
        : INode(std::move(position))
        , TYqlValuesArgs(std::move(args))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        for (const auto& row : Rows) {
            if (!::NSQLTranslationV1::Init(ctx, src, row)) {
                return false;
            }
        }

        if (TMaybe<size_t> width = Width(Rows, ctx)) {
            Width_ = *width;
        } else {
            return false;
        }

        Values_ = BuildValueList(Rows);
        if (!Values_) {
            return false;
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        TNodePtr node =
            Y("YqlSelect",
              Q(Y(Q(Y(Q("set_items"),
                      Q(Y(Y("YqlSetItem",
                            Q(Y(Q(Y(Q("values"),
                                    Q(BuildColumnList()),
                                    Values_))))))))),
                  Q(Y(Q("set_ops"), Q(Y(Q("push"))))))));
        return node->Translate(ctx);
    }

    TNodePtr DoClone() const override {
        return new TYqlValuesNode(*this);
    }

    bool SetColumns(TVector<TString> columns, TContext& ctx) {
        if (columns.empty()) {
            return true;
        }

        if (Width_ < columns.size()) {
            ctx.Error() << "Derived column list size "
                        << "exceeds column count in VALUES";
            return false;
        }

        Columns_ = std::move(columns);
        return true;
    }

private:
    TNodePtr BuildColumnList() const {
        TNodePtr columns = Y();
        for (size_t i = 0; i < Width_; ++i) {
            TString name;
            if (!Columns_ || Columns_->size() <= i) {
                name = TStringBuilder() << "column" << i;
            } else {
                name = Columns_->at(i);
            }

            columns->Add(Q(std::move(name)));
        }
        return columns;
    }

    TNodePtr BuildValueList(const TVector<TVector<TNodePtr>>& rows) const {
        TNodePtr values = Y("YqlValuesList");
        for (const auto& row : rows) {
            TNodePtr value = Y();
            for (const TNodePtr& column : row) {
                value->Add(column);
            }

            values->Add(Q(std::move(value)));
        }
        return values;
    }

    TMaybe<size_t> Width(const TVector<TVector<TNodePtr>>& rows, TContext& ctx) const {
        size_t width = std::numeric_limits<size_t>::max();
        for (const auto& row : rows) {
            if (width == std::numeric_limits<size_t>::max()) {
                width = row.size();
            } else if (width != row.size()) {
                ctx.Error() << "VALUES lists must all be the same length. "
                            << "Expected width is " << width << ", "
                            << "but got " << row.size();
                return Nothing();
            }
        }
        return width;
    }

    TNodePtr Values_;
    size_t Width_ = 0;
    TMaybe<TVector<TString>> Columns_;
};

class TYqlSelectLikeNode: public INode {
public:
    explicit TYqlSelectLikeNode(TPosition position)
        : INode(std::move(position))
    {
    }

protected:
    bool Init(TContext& ctx, ISource* src, const TMaybe<TOrderBy>& orderBy) const {
        if (!orderBy) {
            return true;
        }

        for (const auto& key : orderBy->Keys) {
            if (!key->OrderExpr->Init(ctx, src)) {
                return false;
            }
        }

        return true;
    }

    TNodePtr BuildSortSpecification(const TVector<TSortSpecificationPtr>& keys) const {
        TNodePtr specification = Y();
        for (const TSortSpecificationPtr& key : keys) {
            specification->Add(BuildSortSpecification(key));
        }
        return specification;
    }

    TNodePtr BuildSortSpecification(const TSortSpecificationPtr& key) const {
        TString modifier = key->Ascending ? "asc" : "desc";
        return Y("YqlSort", Y("Void"), Y("lambda", Q(Y()), key->OrderExpr), Q(modifier), Q("first"));
    }
};

class TYqlSetItemNode final: public TYqlSelectLikeNode, private TYqlSetItemArgs {
public:
    explicit TYqlSetItemNode(TYqlSetItemArgs&& args)
        : TYqlSelectLikeNode(args.Position)
        , TYqlSetItemArgs(std::move(args))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!InitProjection(ctx, src) ||
            !InitSource(ctx, src) ||
            (Where && !Where->GetRef().Init(ctx, src)) ||
            (GroupBy && !NSQLTranslationV1::Init(ctx, src, GroupBy->Keys)) ||
            (Having && !Having->GetRef().Init(ctx, src) ||
             !Init(ctx, src, OrderBy) ||
             (Limit && !Limit->GetRef().Init(ctx, src)) ||
             (Offset && !Offset->GetRef().Init(ctx, src)))) {
            return false;
        }

        TNodePtr item = Y();
        {
            TNodePtr items = BuildYqlResultItems(Projection);
            if (!items) {
                return false;
            }

            item->Add(Q(Y(Q("result"), Q(std::move(items)))));
        }

        if (Source) {
            YQL_ENSURE(
                !Source->Sources.empty(),
                "Expected non-empty sources, got " << Source->Sources.size());
            YQL_ENSURE(
                Source->Sources.size() == Source->Constraints.size() + 1,
                "Expected len(join_sources) == len(join_constraints) + 1, got "
                    << Source->Sources.size() << " != " << Source->Constraints.size());

            TNodePtr from = Y();
            for (const auto& source : Source->Sources) {
                if (auto element = BuildFromElement(ctx, source)) {
                    from->Add(std::move(*element));
                } else {
                    return false;
                }
            }

            TNodePtr joinOps = Y();
            joinOps->Add(Q(Y(Q("push"))));
            for (const auto& constraint : Source->Constraints) {
                joinOps->Add(Q(Y(Q("push"))));
                joinOps->Add(BuildJoinConstraint(constraint));
            }

            item->Add(Q(Y(Q("from"), Q(std::move(from)))));
            item->Add(Q(Y(Q("join_ops"), Q(Y(Q(std::move(joinOps)))))));
        }

        if (Where) {
            if (!Source) {
                ctx.Error() << "Filtering is not allowed without FROM";
                return false;
            }

            item->Add(Q(Y(Q("where"), BuildYqlWhere(*Where))));
        }

        if (GroupBy) {
            item->Add(Q(Y(Q("group_by"), Q(BuildGroupBy(*GroupBy)))));
        }

        if (Having) {
            item->Add(Q(Y(Q("having"), BuildYqlWhere(*Having))));
        }

        if (OrderBy) {
            item->Add(Q(Y(Q("sort"), Q(BuildSortSpecification(OrderBy->Keys)))));
        }

        if (Limit) {
            item->Add(Q(Y(Q("limit"), *Limit)));
        }

        if (Offset) {
            item->Add(Q(Y(Q("offset"), *Offset)));
        }

        Node_ = Y("YqlSetItem", Q(std::move(item)));

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

    TNodePtr DoClone() const override {
        return new TYqlSetItemNode(*this);
    }

    bool IsOrdered() const {
        return OrderBy.Defined();
    }

    TMaybe<TVector<TString>> Columns() const {
        return std::visit(
            TOverloaded{
                [&](const TVector<TNodePtr>& terms) -> TMaybe<TVector<TString>> {
                    TVector<TString> columns(Reserve(terms.size()));
                    for (const auto& term : terms) {
                        columns.emplace_back(term->GetLabel());
                    }
                    return columns;
                },
                [](const TPlainAsterisk&) -> TMaybe<TVector<TString>> {
                    return Nothing();
                },
            }, Projection);
    }

private:
    bool InitProjection(TContext& ctx, ISource* src) const {
        return std::visit(
            TOverloaded{
                [&](const TVector<TNodePtr>& terms) {
                    return InitTerms(ctx, src, terms);
                },
                [](const TPlainAsterisk&) {
                    return true;
                },
            }, Projection);
    }

    bool InitTerms(TContext& ctx, ISource* src, const TVector<TNodePtr>& terms) const {
        THashSet<TString> used = UsedLables(terms);

        for (size_t i = 0; i < terms.size(); ++i) {
            const TNodePtr& term = terms[i];

            TString label = TermAlias(term, i, used);
            used.emplace(label);

            term->SetLabel(std::move(label));
        }

        return ::NSQLTranslationV1::Init(ctx, src, terms);
    }

    bool InitSource(TContext& ctx, ISource* src) const {
        if (!Source) {
            return true;
        }

        for (const auto& source : Source->Sources) {
            if (!source.Node->Init(ctx, src)) {
                return false;
            }
        }

        for (const auto& constraint : Source->Constraints) {
            if (constraint.Condition && !constraint.Condition->Init(ctx, src)) {
                return false;
            }
        }

        return true;
    }

    THashSet<TString> UsedLables(const TVector<TNodePtr>& terms) const {
        THashSet<TString> used(terms.size());
        for (const TNodePtr& term : terms) {
            used.emplace(term->GetLabel());
        }
        return used;
    }

    TString TermAlias(const TNodePtr& term, size_t i, const THashSet<TString>& used) const {
        if (const TString& label = term->GetLabel(); !label.empty()) {
            return label;
        }

        if (TMaybe<TString> alias = ColumnAlias(term)) {
            return std::move(*alias);
        }

        for (;; ++i) {
            TString alias = TStringBuilder() << "column" << i;
            if (!used.contains(alias)) {
                return alias;
            }
        }
    }

    TNodePtr BuildYqlResultItems(const TProjection& projection) const {
        return std::visit(
            TOverloaded{
                [&](const TVector<TNodePtr>& terms) { return BuildYqlResultItems(terms); },
                [&](const TPlainAsterisk& terms) { return BuildYqlResultItems(terms); },
            }, projection);
    }

    TNodePtr BuildYqlResultItems(const TVector<TNodePtr>& terms) const {
        TNodePtr items = Y();
        for (const TNodePtr& term : terms) {
            items->Add(BuildYqlResultItem(term->GetLabel(), term));
        }
        return items;
    }

    TNodePtr BuildYqlResultItems(const TPlainAsterisk&) const {
        return Y(BuildYqlResultItem("", Y("YqlStar")));
    }

    TNodePtr BuildYqlResultItem(TString name, TNodePtr term) const {
        return Y("YqlResultItem", Q(std::move(name)), Y("Void"), Y("lambda", Q(Y()), std::move(term)));
    }

    TMaybe<TString> ColumnAlias(const TNodePtr& term) const {
        if (!term->GetColumnName()) {
            return Nothing();
        }

        TString name = *term->GetColumnName();

        if (const auto* source = term->GetSourceName();
            source && !source->empty() &&
            Source && 1 < Source->Sources.size()) {
            name.prepend(".").prepend(*source);
        }

        return name;
    }

    TMaybe<TNodePtr> BuildFromElement(TContext& ctx, const TYqlSource& source) const {
        const auto build = [this](TNodePtr node, TString name) {
            YQL_ENSURE(!name.empty(), "An empty source name is unsupported");
            return Q(Y(
                std::move(node),
                Q(std::move(name)),
                Q(Y(/* Columns are passed through SetColumns */))));
        };

        if (!source.Alias) {
            return build(source.Node, ctx.MakeName("_yql_source_"));
        }

        if (auto& columns = source.Alias->Columns) {
            if (auto* values = dynamic_cast<TYqlValuesNode*>(source.Node.Get())) {
                if (!values->SetColumns(std::move(columns), ctx)) {
                    return Nothing();
                }
            } else {
                ctx.Error() << "Qualified by column names source alias "
                            << "is viable only for VALUES statement";
                return Nothing();
            }
        }

        return build(source.Node, source.Alias->Name);
    }

    TNodePtr BuildJoinConstraint(const TYqlJoinConstraint& constraint) const {
        // YQL has no IMPLICIT CROSS JOIN (COMMA) over JOIN precedence.
        // Consider the following query:
        // FROM       (VALUES (01)      ) AS a(a)
        // CROSS JOIN (VALUES (10)      ) AS b(b) -- 'CROSS JOIN' <-> ','
        // RIGHT JOIN (VALUES (10), (00)) AS c(c) ON b.b = c.c;

        EYqlJoinKind kind = constraint.Kind;
        if (constraint.Kind == EYqlJoinKind::Cross) {
            kind = EYqlJoinKind::Inner;
        }

        TNodePtr condition = constraint.Condition;
        if (constraint.Kind == EYqlJoinKind::Cross) {
            condition = Y("Bool", Q("true"));
        }

        return Q(Y(
            Q(ToString(kind)),
            Y("YqlWhere", Y("Void"), Y("lambda", Q(Y()), std::move(condition)))));
    }

    TNodePtr BuildGroupBy(const TGroupBy& groupBy) const {
        TNodePtr clause = Y();
        for (TNodePtr key : groupBy.Keys) {
            clause = L(std::move(clause), BuildYqlGroup(std::move(key)));
        }
        return clause;
    }

    TNodePtr BuildYqlGroup(TNodePtr node) const {
        return Y("YqlGroup", Y("Void"), Y("lambda", Q(Y()), std::move(node)));
    }

    TNodePtr BuildYqlWhere(TNodePtr expr) const {
        return Y("YqlWhere", Y("Void"), Y("lambda", Q(Y()), std::move(expr)));
    }

    static TString ToString(EYqlJoinKind kind) {
        switch (kind) {
            case EYqlJoinKind::Cross:
                return "cross";
            case EYqlJoinKind::Inner:
                return "inner";
            case EYqlJoinKind::Left:
                return "left";
            case EYqlJoinKind::Right:
                return "right";
        }
    }

    TNodePtr Node_;
};

class TYqlSelectNode final: public TYqlSelectLikeNode, private TYqlSelectArgs {
public:
    TYqlSelectNode(TPosition position, TYqlSelectArgs&& args)
        : TYqlSelectLikeNode(std::move(position))
        , TYqlSelectArgs(std::move(args))
        , SetItems_(Reserve(SetItems.size()))
    {
        for (const TYqlSetItemArgs& args : SetItems) {
            SetItems_.emplace_back(new TYqlSetItemNode(TYqlSetItemArgs(args)));
        }
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!NSQLTranslationV1::Init(ctx, src, SetItems_) ||
            !Init(ctx, src, OrderBy) ||
            (Limit && !Limit->GetRef().Init(ctx, src)) ||
            (Offset && !Offset->GetRef().Init(ctx, src))) {
            return false;
        }

        TNodePtr setItems = Y();
        for (TNodePtr setItem : SetItems_) {
            setItems->Add(std::move(setItem));
        }

        TNodePtr setOps = Y();
        for (EYqlSetOp op : SetOps) {
            setOps->Add(Q(ToString(op)));
        }

        TNodePtr body = Y();
        body->Add(Q(Y(Q("set_items"), Q(std::move(setItems)))));
        body->Add(Q(Y(Q("set_ops"), Q(std::move(setOps)))));

        if (OrderBy) {
            body->Add(Q(Y(Q("sort"), Q(BuildSortSpecification(OrderBy->Keys)))));
        }

        if (Limit) {
            body->Add(Q(Y(Q("limit"), *Limit)));
        }

        if (Offset) {
            body->Add(Q(Y(Q("offset"), *Offset)));
        }

        Node_ = Y("YqlSelect", Q(std::move(body)));
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

    TNodePtr DoClone() const override {
        return new TYqlSelectNode(*this);
    }

    bool IsOrdered() const {
        YQL_ENSURE(!SetItems.empty());
        if (1 < SetItems.size()) {
            return OrderBy.Defined();
        }
        return GetSingleSetItem().IsOrdered();
    }

    TMaybe<TVector<TString>> Columns() const {
        YQL_ENSURE(!SetItems.empty());
        if (1 < SetItems.size()) {
            return Nothing();
        }
        return GetSingleSetItem().Columns();
    }

    const TYqlSelectArgs& Args() const {
        return *this;
    }

private:
    const TYqlSetItemNode& GetSingleSetItem() const {
        YQL_ENSURE(SetItems_.size() == 1);
        const INode* item = SetItems_.at(0).Get();
        const auto* node = dynamic_cast<const TYqlSetItemNode*>(item);
        YQL_ENSURE(node);
        return *node;
    }

    TNodePtr Node_;
    TVector<TNodePtr> SetItems_;
};

bool IsYqlSource(const TNodePtr& node) {
    return IsYqlSubqueryRef(node) ||
           dynamic_cast<TYqlSelectNode*>(node.Get()) ||
           dynamic_cast<TYqlValuesNode*>(node.Get());
}

class TYqlStatementNode final: public INode {
public:
    explicit TYqlStatementNode(TNodePtr source)
        : INode(source->GetPos())
        , Source_(std::move(source))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        TBlocks dependencies;
        {
            ctx.PushCurrentBlocks(&dependencies);
            Y_DEFER {
                ctx.PopCurrentBlocks();
            };

            if (!Source_->Init(ctx, src)) {
                return false;
            }
        }

        TNodePtr block = BuildList(GetPos(), std::move(dependencies));
        {
            block->Add(Y("let", "output", Source_));

            if (!IsOrdered()) {
                block->Add(Y("let", "output", Y("Unordered", "output")));
            }

            block->Add(Y("let", "result_sink",
                         Y("DataSink", Q("result"))));

            TNodePtr options = Y();
            options->Add(Q(Y(Q("type"))));
            options->Add(Q(Y(Q("autoref"))));

            if (!IsOrdered()) {
                options->Add(Q(Y(Q("unordered"))));
            }

            if (auto columns = Columns()) {
                TNodePtr list = Y();
                for (auto& column : *columns) {
                    list = L(std::move(list), Q(std::move(column)));
                }

                options->Add(Q(Y(Q("columns"), Q(std::move(list)))));
            }

            block->Add(Y("let", "world",
                         Y("Write!", "world", "result_sink", Y("Key"), "output", Q(options))));

            block->Add(Y("return", Y("Commit!", "world", "result_sink")));
        }

        Node_ = Y("block", Q(std::move(block)));

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

    TNodePtr DoClone() const override {
        return new TYqlStatementNode(*this);
    }

private:
    bool IsOrdered() const {
        if (const auto* select = dynamic_cast<const TYqlSelectNode*>(Source_.Get())) {
            return select->IsOrdered();
        }

        return false;
    }

    TMaybe<TVector<TString>> Columns() const {
        if (const auto* select = dynamic_cast<const TYqlSelectNode*>(Source_.Get())) {
            return select->Columns();
        }

        return Nothing();
    }

    TNodePtr Source_;
    TNodePtr Node_;
};

class TYqlSubLinkNode final: public INode {
public:
    struct TScalar {
    };

    struct TExists {
    };

    struct TIn {
        TNodePtr Expression;
    };

    using TVariant = std::variant<
        TScalar,
        TExists,
        TIn>;

    TYqlSubLinkNode(TNodePtr source, TVariant variant)
        : INode(source->GetPos())
        , Source_(Unbox(std::move(source)))
        , Variant_(std::move(variant))
    {
        YQL_ENSURE(IsYqlSource(Source_));
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Source_->Init(ctx, src) ||
            !Init(ctx, src, Variant_)) {
            return false;
        }

        Node_ = ToSubLink(Source_, Variant_);
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

    TNodePtr DoClone() const override {
        return new TYqlSubLinkNode(*this);
    }

    TNodePtr Source() const {
        YQL_ENSURE(
            std::holds_alternative<TScalar>(Variant_),
            "Only a scalar subquery can be a box for a select");
        return Source_;
    }

private:
    bool Init(TContext& ctx, ISource* src, const TVariant& variant) {
        return std::visit(
            TOverloaded{
                [&](const TScalar&) { return true; },
                [&](const TExists&) { return true; },
                [&](const TIn& x) { return Init(ctx, src, x); },
            }, variant);
    }

    bool Init(TContext& ctx, ISource* src, const TIn& in) {
        return in.Expression->Init(ctx, src);
    }

    TNodePtr ToSubLink(TNodePtr source, const TVariant& variant) {
        source = Y("lambda", Q(Y()), std::move(source));
        return std::visit(
            TOverloaded{
                [&](const TScalar& x) { return ToSubLink(std::move(source), x); },
                [&](const TExists& x) { return ToSubLink(std::move(source), x); },
                [&](const TIn& x) { return ToSubLink(std::move(source), x); },
            }, variant);
    }

    TNodePtr ToSubLink(TNodePtr lambda, const TScalar&) {
        return Y("YqlSubLink", Q("expr"), Y("Void"), Y("Void"), Y("Void"), std::move(lambda));
    }

    TNodePtr ToSubLink(TNodePtr lambda, const TExists&) {
        return Y("YqlSubLink", Q("exists"), Y("Void"), Y("Void"), Y("Void"), std::move(lambda));
    }

    TNodePtr ToSubLink(TNodePtr lambda, const TIn& in) {
        TNodePtr compare = Y("lambda", Q(Y("value")), Y("==", in.Expression, "value"));
        return Y("YqlSubLink", Q("any"), Y("Void"), Y("Void"), std::move(compare), std::move(lambda));
    }

    static TNodePtr Unbox(TNodePtr node) {
        if (const auto* sub = dynamic_cast<const TYqlSubLinkNode*>(node.Get())) {
            node = sub->Source();
        }
        return node;
    }

    TNodePtr Source_;
    TVariant Variant_;
    TNodePtr Node_;
};

EYqlSetOp AllQualified(EYqlSetOp op) {
    switch (op) {
        case EYqlSetOp::Push:
        case EYqlSetOp::UnionAll:
        case EYqlSetOp::ExceptAll:
        case EYqlSetOp::IntersectAll:
            Y_UNREACHABLE();
        case EYqlSetOp::Union:
            return EYqlSetOp::UnionAll;
        case EYqlSetOp::Except:
            return EYqlSetOp::ExceptAll;
        case EYqlSetOp::Intersect:
            return EYqlSetOp::IntersectAll;
    }
}

TNodePtr GetYqlSource(const TNodePtr& node) {
    if (IsYqlSource(node)) {
        return node;
    }

    if (auto* link = dynamic_cast<TYqlSubLinkNode*>(node.Get())) {
        return link->Source();
    }

    return nullptr;
}

TNodePtr ToTableExpression(TNodePtr source) {
    TPosition position = source->GetPos();

    TYqlSelectArgs select = {
        .SetItems = {{
            .Position = position,
            .Projection = TPlainAsterisk{},
            .Source = TYqlJoin{
                .Sources = {
                    TYqlSource{
                        .Node = std::move(source),
                    },
                },
            },
        }},
        .SetOps = {EYqlSetOp::Push},
    };

    return BuildYqlSelect(std::move(position), std::move(select));
}

TYqlSelectArgs DestructYqlSelect(TNodePtr node) {
    const auto* select = dynamic_cast<const TYqlSelectNode*>(node.Get());
    YQL_ENSURE(select);

    return select->Args();
}

TNodePtr BuildYqlTableRef(TPosition position, TYqlTableRefArgs&& args) {
    return new TYqlTableRefNode(std::move(position), std::move(args));
}

TNodePtr BuildYqlValues(TPosition position, TYqlValuesArgs&& args) {
    return new TYqlValuesNode(std::move(position), std::move(args));
}

TNodePtr BuildYqlSelect(TPosition position, TYqlSelectArgs&& args) {
    return new TYqlSelectNode(std::move(position), std::move(args));
}

TNodePtr WrapYqlSelectSubExpr(TNodePtr node) {
    if (IsYqlSource(node)) {
        node = BuildYqlScalarSubquery(std::move(node));
    }
    return node;
}

TNodePtr BuildYqlScalarSubquery(TNodePtr node) {
    TYqlSubLinkNode::TScalar variant = {};
    return new TYqlSubLinkNode(std::move(node), std::move(variant));
}

TNodePtr BuildYqlExistsSubquery(TNodePtr node) {
    TYqlSubLinkNode::TExists variant = {};
    return new TYqlSubLinkNode(std::move(node), std::move(variant));
}

TNodePtr BuildYqlInSubquery(TNodePtr node, TNodePtr expression) {
    TYqlSubLinkNode::TIn variant = {
        .Expression = std::move(expression),
    };
    return new TYqlSubLinkNode(std::move(node), std::move(variant));
}

TNodePtr BuildYqlStatement(TNodePtr node) {
    return new TYqlStatementNode(std::move(node));
}

} // namespace NSQLTranslationV1

template <>
void Out<NSQLTranslationV1::EYqlSetOp>(
    IOutputStream& out,
    NSQLTranslationV1::EYqlSetOp value)
{
    switch (value) {
        case NSQLTranslationV1::EYqlSetOp::Push:
            out << "push";
            break;
        case NSQLTranslationV1::EYqlSetOp::Union:
            out << "union";
            break;
        case NSQLTranslationV1::EYqlSetOp::UnionAll:
            out << "union_all";
            break;
        case NSQLTranslationV1::EYqlSetOp::Except:
            out << "except";
            break;
        case NSQLTranslationV1::EYqlSetOp::ExceptAll:
            out << "except_all";
            break;
        case NSQLTranslationV1::EYqlSetOp::Intersect:
            out << "intersect";
            break;
        case NSQLTranslationV1::EYqlSetOp::IntersectAll:
            out << "intersect_all";
            break;
    }
}
