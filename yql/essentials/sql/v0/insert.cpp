#include "node.h"
#include "context.h"

#include <yql/essentials/utils/yql_panic.h>

using namespace NYql;

namespace NSQLTranslationV0 {

static const TMap<ESQLWriteColumnMode, EWriteColumnMode> sqlIntoMode2WriteColumn = {
    {ESQLWriteColumnMode::InsertInto, EWriteColumnMode::Insert},
    {ESQLWriteColumnMode::InsertOrAbortInto, EWriteColumnMode::InsertOrAbort},
    {ESQLWriteColumnMode::InsertOrIgnoreInto, EWriteColumnMode::InsertOrIgnore},
    {ESQLWriteColumnMode::InsertOrRevertInto, EWriteColumnMode::InsertOrRevert},
    {ESQLWriteColumnMode::UpsertInto, EWriteColumnMode::Upsert},
    {ESQLWriteColumnMode::ReplaceInto, EWriteColumnMode::Replace},
    {ESQLWriteColumnMode::InsertIntoWithTruncate, EWriteColumnMode::Renew},
    {ESQLWriteColumnMode::Update, EWriteColumnMode::Update},
    {ESQLWriteColumnMode::Delete, EWriteColumnMode::Delete},
};

class TModifySourceBase: public ISource {
public:
    TModifySourceBase(TPosition pos, const TVector<TString>& columnsHint)
        : ISource(pos)
        , ColumnsHint_(columnsHint)
    {
    }

    bool AddFilter(TContext& ctx, TNodePtr filter) override {
        Y_UNUSED(filter);
        ctx.Error(Pos_) << "Source does not allow filtering";
        return false;
    }

    bool AddGroupKey(TContext& ctx, const TString& column) override {
        Y_UNUSED(column);
        ctx.Error(Pos_) << "Source does not allow grouping";
        return false;
    }

    bool AddAggregation(TContext& ctx, TAggregationPtr aggr) override {
        Y_UNUSED(aggr);
        ctx.Error(Pos_) << "Source does not allow aggregation";
        return false;
    }

    TNodePtr BuildFilter(TContext& ctx, const TString& label, const TNodePtr& groundNode) override {
        Y_UNUSED(ctx);
        Y_UNUSED(label);
        Y_UNUSED(groundNode);
        return nullptr;
    }

    TNodePtr BuildAggregation(const TString& label) override {
        Y_UNUSED(label);
        return nullptr;
    }

protected:
    TVector<TString> ColumnsHint_;
    TString OperationHumanName_;
};

class TUpdateByValues: public TModifySourceBase {
public:
    TUpdateByValues(TPosition pos, const TString& operationHumanName, const TVector<TString>& columnsHint, const TVector<TNodePtr>& values)
        : TModifySourceBase(pos, columnsHint)
        , OperationHumanName_(operationHumanName)
        , Values_(values)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (ColumnsHint_.size() != Values_.size()) {
            ctx.Error(Pos_) << "VALUES have " << Values_.size() << " columns, " << OperationHumanName_ << " expects: " << ColumnsHint_.size();
            return false;
        }
        for (auto& value: Values_) {
            if (!value->Init(ctx, src)) {
                return false;
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        YQL_ENSURE(Values_.size() == ColumnsHint_.size());

        auto structObj = Y("AsStruct");
        for (size_t i = 0; i < Values_.size(); ++i) {
            TString column = ColumnsHint_[i];
            TNodePtr value = Values_[i];

            structObj = L(structObj, Q(Y(Q(column), value)));
        }

        auto updateRow = BuildLambda(Pos_, Y("row"), structObj);
        return updateRow;
    }

    TNodePtr DoClone() const final {
        return new TUpdateByValues(Pos_, OperationHumanName_, ColumnsHint_, CloneContainer(Values_));
    }
private:
    TString OperationHumanName_;

protected:
    TVector<TNodePtr> Values_;
};

class TModifyByValues: public TModifySourceBase {
public:
    TModifyByValues(TPosition pos, const TString& operationHumanName, const TVector<TString>& columnsHint, const TVector<TVector<TNodePtr>>& values)
        : TModifySourceBase(pos, columnsHint)
        , OperationHumanName_(operationHumanName)
        , Values_(values)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        bool hasError = false;
        for (const auto& row: Values_) {
            if (ColumnsHint_.empty()) {
                ctx.Error(Pos_) << OperationHumanName_ << " ... VALUES requires specification of table columns";
                hasError = true;
                continue;
            }
            if (ColumnsHint_.size() != row.size()) {
                ctx.Error(Pos_) << "VALUES have " << row.size() << " columns, " << OperationHumanName_ << " expects: " << ColumnsHint_.size();
                hasError = true;
                continue;
            }
            for (auto& value: row) {
                if (!value->Init(ctx, FakeSource_.Get())) {
                    hasError = true;
                    continue;
                }
            }
        }
        return !hasError;
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        auto tuple = Y();
        for (const auto& row: Values_) {
            auto rowValues = Y("AsStruct");
            auto column = ColumnsHint_.begin();
            for (auto value: row) {
                rowValues = L(rowValues, Q(Y(BuildQuotedAtom(Pos_, *column), value)));
                ++column;
            }
            tuple = L(tuple, rowValues);
        }
        return Y("EnsurePersistable", Q(tuple));
    }

    TNodePtr DoClone() const final {
        TVector<TVector<TNodePtr>> clonedValues;
        clonedValues.reserve(Values_.size());
        for (auto cur: Values_) {
            clonedValues.push_back(CloneContainer(cur));
        }
        return new TModifyByValues(Pos_, OperationHumanName_, ColumnsHint_, clonedValues);
    }

private:
    TString OperationHumanName_;
    TVector<TVector<TNodePtr>> Values_;
    TSourcePtr FakeSource_;
};

class TModifyBySource: public TModifySourceBase {
public:
    TModifyBySource(TPosition pos, const TString& operationHumanName, const TVector<TString>& columnsHint, TSourcePtr source)
        : TModifySourceBase(pos, columnsHint)
        , OperationHumanName_(operationHumanName)
        , Source_(std::move(source))
    {}

    void GetInputTables(TTableList& tableList) const override {
        if (Source_) {
            return Source_->GetInputTables(tableList);
        }
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Source_->Init(ctx, src)) {
            return false;
        }
        const auto& sourceColumns = Source_->GetColumns();
        const auto numColumns = !ColumnsHint_.empty() && sourceColumns ? sourceColumns->List.size() : 0;
        if (ColumnsHint_.size() != numColumns) {
            ctx.Error(Pos_) << "SELECT has " << numColumns << " columns, " << OperationHumanName_ << " expects: " << ColumnsHint_.size();
            return false;
        }
        if (numColumns) {
            TStringStream str;
            bool mismatchFound = false;
            for (size_t i = 0; i < numColumns; ++i) {
                bool hasName = sourceColumns->NamedColumns[i];
                if (hasName) {
                    const auto& hintColumn = ColumnsHint_[i];
                    const auto& sourceColumn = sourceColumns->List[i];
                    if (hintColumn != sourceColumn) {
                        if (!mismatchFound) {
                            str << "Column names in SELECT don't match column specification in parenthesis";
                            mismatchFound = true;
                        }
                        str << ". \"" << hintColumn << "\" doesn't match \"" << sourceColumn << "\"";
                    }
                }
            }
            if (mismatchFound) {
                ctx.Warning(Pos_, TIssuesIds::YQL_SOURCE_SELECT_COLUMN_MISMATCH) << str.Str();
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source_->Build(ctx);
        if (ColumnsHint_.empty() || !Source_->GetColumns()) {
            return input;
        }
        auto srcColumn = Source_->GetColumns()->List.begin();
        auto structObj = Y("AsStruct");
        for (auto column: ColumnsHint_) {
            structObj = L(structObj, Q(Y(BuildQuotedAtom(Pos_, column),
                Y("Member", "row", BuildQuotedAtom(Pos_, *srcColumn))
            )));
            ++srcColumn;
        }
        return Y("OrderedMap", input, BuildLambda(Pos_, Y("row"), structObj));
    }

    TNodePtr DoClone() const final {
        return new TModifyBySource(Pos_, OperationHumanName_, ColumnsHint_, Source_->CloneSource());
    }

    bool IsOrdered() const final {
        return Source_->IsOrdered();
    }

private:
    TString OperationHumanName_;
    TSourcePtr Source_;
};

TSourcePtr BuildWriteValues(TPosition pos, const TString& operationHumanName, const TVector<TString>& columnsHint, const TVector<TVector<TNodePtr>>& values) {
    return new TModifyByValues(pos, operationHumanName, columnsHint, values);
}

TSourcePtr BuildWriteValues(TPosition pos, const TString& operationHumanName, const TVector<TString>& columnsHint, const TVector<TNodePtr>& values) {
    return new TModifyByValues(pos, operationHumanName, columnsHint, {values});
}

TSourcePtr BuildWriteValues(TPosition pos, const TString& operationHumanName, const TVector<TString>& columnsHint, TSourcePtr source) {
    return new TModifyBySource(pos, operationHumanName, columnsHint, std::move(source));
}

TSourcePtr BuildUpdateValues(TPosition pos, const TVector<TString>& columnsHint, const TVector<TNodePtr>& values) {
    return new TUpdateByValues(pos, "UPDATE", columnsHint, values);
}

class TWriteColumnsNode: public TAstListNode {
public:
    TWriteColumnsNode(TPosition pos, const TTableRef& table, EWriteColumnMode mode, TSourcePtr values = nullptr, TNodePtr options = nullptr)
        : TAstListNode(pos)
        , Table_(table)
        , Mode_(mode)
        , Values_(std::move(values))
        , Options_(std::move(options))
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    void ResetSource(TSourcePtr source) {
        TableSource_ = std::move(source);
    }

    void ResetUpdate(TSourcePtr update) {
        Update_ = std::move(update);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Table_.Check(ctx)) {
            return false;
        }

        TTableList tableList;
        TNodePtr values;
        auto options = Y();
        if (Options_) {
            if (!Options_->Init(ctx, src)) {
                return false;
            }
            options = L(Options_);
        }

        ISource* underlyingSrc = src;

        if (TableSource_) {
            ctx.PushBlockShortcuts();
            if (!TableSource_->Init(ctx, src) || !TableSource_->InitFilters(ctx)) {
                return false;
            }
            options = L(options, Q(Y(Q("filter"), TableSource_->BuildFilterLambda(ctx.GroundBlockShortcuts(Pos_)))));
        }

        bool unordered = false;
        ctx.PushBlockShortcuts();
        if (Values_) {
            if (!Values_->Init(ctx, TableSource_.Get())) {
                return false;
            }

            Values_->GetInputTables(tableList);
            underlyingSrc = Values_.Get();
            values = Values_->Build(ctx);
            if (!values) {
                return false;
            }
            unordered = !Values_->IsOrdered();
        }

        TNodePtr node(BuildInputTables(Pos_, tableList, false));
        if (!node->Init(ctx, underlyingSrc)) {
            return false;
        }

        if (Update_) {
            if (!Update_->Init(ctx, TableSource_.Get()) || !Update_->InitFilters(ctx)) {
                return false;
            }
            options = L(options, Q(Y(Q("update"), Update_->Build(ctx))));
        }

        auto write = BuildWriteTable(Pos_, "values", Table_, Mode_, std::move(options));
        if (!write->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        node = ctx.GroundBlockShortcuts(Pos_, node);
        if (values) {
            node = L(node, Y("let", "values", values));
            if (unordered && ctx.UseUnordered(Table_)) {
                node = L(node, Y("let", "values", Y("Unordered", "values")));
            }
        } else {
            node = L(node, Y("let", "values", Y("Void")));
        }
        node = L(node, Y("let", "world", write));
        node = L(node, Y("return", "world"));

        Add("block", Q(node));
        return true;
    }

    TNodePtr DoClone() const final {
        return {};
    }

protected:
    TTableRef Table_;
    TSourcePtr TableSource_;
    EWriteColumnMode Mode_;
    TSourcePtr Values_;
    TSourcePtr Update_;
    TSourcePtr FakeSource_;
    TNodePtr Options_;
};

EWriteColumnMode ToWriteColumnsMode(ESQLWriteColumnMode sqlWriteColumnMode) {
    return sqlIntoMode2WriteColumn.at(sqlWriteColumnMode);
}

TNodePtr BuildWriteColumns(TPosition pos, const TTableRef& table, EWriteColumnMode mode, TSourcePtr values, TNodePtr options) {
    YQL_ENSURE(values, "Invalid values node");
    return new TWriteColumnsNode(pos, table, mode, std::move(values), std::move(options));
}

TNodePtr BuildUpdateColumns(TPosition pos, const TTableRef& table, TSourcePtr values, TSourcePtr source) {
    YQL_ENSURE(values, "Invalid values node");
    TIntrusivePtr<TWriteColumnsNode> writeNode = new TWriteColumnsNode(pos, table, EWriteColumnMode::Update);
    writeNode->ResetSource(std::move(source));
    writeNode->ResetUpdate(std::move(values));
    return writeNode;
}

TNodePtr BuildDelete(TPosition pos, const TTableRef& table, TSourcePtr source) {
    TIntrusivePtr<TWriteColumnsNode> writeNode = new TWriteColumnsNode(pos, table, EWriteColumnMode::Delete);
    writeNode->ResetSource(std::move(source));
    return writeNode;
}


class TEraseColumnsNode: public TAstListNode {
public:
    TEraseColumnsNode(TPosition pos, const TVector<TString>& columns)
        : TAstListNode(pos)
        , Columns_(columns)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(ctx);
        Y_UNUSED(src);

        TNodePtr columnList = Y();
        for (const auto& column: Columns_) {
            columnList->Add(Q(column));
        }

        Add(Q(Y(Q("erase_columns"), Q(columnList))));

        return true;
    }

    TNodePtr DoClone() const final {
        return {};
    }

private:
    TVector<TString> Columns_;
};


TNodePtr BuildEraseColumns(TPosition pos, const TVector<TString>& columns) {
    return new TEraseColumnsNode(pos, columns);
}

} // namespace NSQLTranslationV0
