#include "select_yql.h"

#include "context.h"

#include <util/generic/overloaded.h>
#include <util/generic/scope.h>

namespace NSQLTranslationV1 {

bool Init(TContext& ctx, ISource* src, const TVector<TNodePtr>& nodes) {
    for (const TNodePtr& node : nodes) {
        if (!node->Init(ctx, src)) {
            return false;
        }
    }
    return true;
}

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

        if (columns.size() != Width_) {
            ctx.Error() << "VALUES statement width is " << Width_
                        << ", but got " << columns.size()
                        << " column aliases";
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
            if (!Columns_) {
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

class TYqlSelectNode final: public INode, private TYqlSelectArgs {
public:
    TYqlSelectNode(TPosition position, TYqlSelectArgs&& args)
        : INode(std::move(position))
        , TYqlSelectArgs(std::move(args))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!InitProjection(ctx, src) ||
            !InitSource(ctx, src) ||
            (Where && !Where->GetRef().Init(ctx, src)) ||
            !InitOrderBy(ctx, src)) {
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

            item->Add(Q(Y(Q("where"), Y("YqlWhere", Y("Void"), Y("lambda", Q(Y()), *Where)))));
        }

        if (OrderBy) {
            item->Add(Q(Y(Q("sort"), Q(BuildSortSpecification(OrderBy->Keys)))));
        }

        TNodePtr body = Y();
        {
            body->Add(Q(Y(Q("set_items"), Q(Y(Y("YqlSetItem", Q(std::move(item))))))));
            body->Add(Q(Y(Q("set_ops"), Q(Y(Q("push"))))));
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
        for (size_t i = 0; i < terms.size(); ++i) {
            const TNodePtr& term = terms[i];
            term->SetLabel(TermAlias(term, i));
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
            if (!constraint.Condition->Init(ctx, src)) {
                return false;
            }
        }

        return true;
    }

    bool InitOrderBy(TContext& ctx, ISource* src) const {
        if (!OrderBy) {
            return true;
        }

        for (const auto& key : OrderBy->Keys) {
            if (!key->OrderExpr->Init(ctx, src)) {
                return false;
            }
        }

        return true;
    }

    TString TermAlias(const TNodePtr& term, size_t i) const {
        const TString& label = term->GetLabel();
        if (!label.empty()) {
            return label;
        }
        if (const TString* column = term->GetColumnName()) {
            return *column;
        }
        return TStringBuilder() << "column" << i;
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

    TNodePtr BuildYqlResultItem(TString name, const TNodePtr& term) const {
        name = DisambiguatedResultItemName(std::move(name), term);
        return Y("YqlResultItem", Q(name), Y("Void"), Y("lambda", Q(Y()), term));
    }

    TString DisambiguatedResultItemName(TString name, const TNodePtr& term) const {
        if (const auto* source = term->GetSourceName(); source && 1 < Source->Sources.size()) {
            name.prepend(".").prepend(*source);
        }

        return name;
    }

    TMaybe<TNodePtr> BuildFromElement(TContext& ctx, const TYqlSource& source) const {
        const auto build = [this](TNodePtr node, TString name = "") {
            return Q(Y(
                std::move(node),
                Q(std::move(name)),
                Q(Y(/* Columns are passed through SetColumns */))));
        };

        if (!source.Alias) {
            return build(source.Node);
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
        return Q(Y(
            Q(ToString(constraint.Kind)),
            Y("YqlWhere", Y("Void"), Y("lambda", Q(Y()), constraint.Condition))));
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

    static TString ToString(EYqlJoinKind kind) {
        switch (kind) {
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

            block->Add(Y("let", "result_sink",
                         Y("DataSink", Q("result"))));

            block->Add(Y("let", "world",
                         Y("Write!", "world", "result_sink", Y("Key"), "output",
                           Q(Y(Q(Y(Q("type"))), Q(Y(Q("autoref"))))))));

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
    TNodePtr Source_;
    TNodePtr Node_;
};

TNodePtr BuildYqlTableRef(TPosition position, TYqlTableRefArgs&& args) {
    return new TYqlTableRefNode(std::move(position), std::move(args));
}

TNodePtr BuildYqlValues(TPosition position, TYqlValuesArgs&& args) {
    return new TYqlValuesNode(std::move(position), std::move(args));
}

TNodePtr BuildYqlSelect(TPosition position, TYqlSelectArgs&& args) {
    return new TYqlSelectNode(std::move(position), std::move(args));
}

TNodePtr BuildYqlStatement(TNodePtr node) {
    return new TYqlStatementNode(std::move(node));
}

} // namespace NSQLTranslationV1
