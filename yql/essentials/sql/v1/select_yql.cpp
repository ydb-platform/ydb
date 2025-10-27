#include "select_yql.h"

#include "context.h"

namespace NSQLTranslationV1 {

bool Init(TContext& ctx, ISource* src, const TVector<TNodePtr>& nodes) {
    for (const TNodePtr& node : nodes) {
        if (!node->Init(ctx, src)) {
            return false;
        }
    }
    return true;
}

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
        if (!InitTerms(ctx, src) || (Source && !Source->Node->Init(ctx, src))) {
            return false;
        }

        TNodePtr item = Y();
        {
            TNodePtr items = BuildYqlResultItems(Terms);
            if (!items) {
                return false;
            }

            item->Add(Q(Y(Q("result"), Q(std::move(items)))));
        }

        if (Source) {
            TNodePtr& node = Source->Node;

            TString sourceName;
            if (auto& alias = Source->Alias) {
                sourceName = std::move(alias->Name);

                if (auto& columns = alias->Columns) {
                    if (auto* values = dynamic_cast<TYqlValuesNode*>(node.Get())) {
                        if (!values->SetColumns(std::move(columns), ctx)) {
                            return false;
                        }
                    } else {
                        ctx.Error() << "Qualified by column names source alias "
                                    << "is viable only for VALUES statement";
                        return false;
                    }
                }
            }

            item->Add(Q(Y(Q("from"),
                          Q(Y(Q(Y(
                              std::move(node),
                              Q(std::move(sourceName)),
                              Q(Y(/* Columns are passed through SetColumns */)))))))));

            item->Add(Q(Y(Q("join_ops"), Q(Y(Q(Y(Q(Y(Q("push"))))))))));
        }

        TNodePtr output =
            Y("YqlSelect",
              Q(Y(Q(Y(Q("set_items"),
                      Q(Y(Y("YqlSetItem", Q(std::move(item))))))),
                  Q(Y(Q("set_ops"), Q(Y(Q("push"))))))));

        TNodePtr block = Y();
        {
            block->Add(Y("let", "output", std::move(output)));

            block->Add(Y("let", "result_sink",
                         Y("DataSink", Q("result"))));

            block->Add(Y("let", "world",
                         Y("Write!", "world", "result_sink", Y("Key"), "output",
                           Q(Y(Q(Y(Q("type"))), Q(Y(Q("autoref"))), Q(Y(Q("unordered"))))))));

            block->Add(Y("return", Y("Commit!", "world", "result_sink")));
        }

        Node_ = Y("block", Q(std::move(block)));
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

    TNodePtr DoClone() const override {
        return new TYqlSelectNode(*this);
    }

private:
    bool InitTerms(TContext& ctx, ISource* src) {
        for (size_t i = 0; i < Terms.size(); ++i) {
            const TNodePtr& term = Terms[i];
            term->SetLabel(TermAlias(term, i));
        }

        return ::NSQLTranslationV1::Init(ctx, src, Terms);
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

    TNodePtr BuildYqlResultItems(const TVector<TNodePtr>& terms) const {
        TNodePtr items = Y();
        for (const TNodePtr& term : terms) {
            items->Add(BuildYqlResultItem(term->GetLabel(), term));
        }
        return items;
    }

    TNodePtr BuildYqlResultItem(const TString& name, const TNodePtr& term) const {
        return Y("YqlResultItem", Q(name), Y("Void"), Y("lambda", Q(Y()), term));
    }

    TNodePtr Node_;
};

TNodePtr BuildYqlValues(TPosition position, TYqlValuesArgs&& args) {
    return new TYqlValuesNode(std::move(position), std::move(args));
}

TNodePtr BuildYqlSelect(TPosition position, TYqlSelectArgs&& args) {
    return new TYqlSelectNode(std::move(position), std::move(args));
}

} // namespace NSQLTranslationV1
