#include "node.h"
#include "context.h"

#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/core/sql_types/yql_callable_names.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <library/cpp/charset/ci_string.h>

#include <util/digest/fnv.h>

using namespace NYql;

namespace NSQLTranslationV0 {

class TUniqueTableKey: public ITableKeys {
public:
    TUniqueTableKey(TPosition pos, const TString& cluster, const TDeferredAtom& name, const TString& view)
        : ITableKeys(pos)
        , Cluster_(cluster)
        , Name_(name)
        , View_(view)
        , Full_(name.GetRepr())
    {
        if (!View_.empty()) {
            Full_ += ":" + View_;
        }
    }

    const TString* GetTableName() const override {
        return Name_.GetLiteral() ? &Full_ : nullptr;
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode mode) override {
        if (View_ == "@") {
            auto key = Y("TempTable", Name_.Build());
            return key;
        }

        bool tableScheme = mode == ITableKeys::EBuildKeysMode::CREATE;
        if (tableScheme && !View_.empty()) {
            ctx.Error(Pos_) << "Table view can not be created with CREATE TABLE clause";
            return nullptr;
        }
        auto path = ctx.GetPrefixedPath(Cluster_, Name_);
        if (!path) {
            return nullptr;
        }
        auto key = Y("Key", Q(Y(Q(tableScheme ? "tablescheme" : "table"), Y("String", path))));
        if (!View_.empty()) {
            key = L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos_, View_)))));
        }
        if (mode == ITableKeys::EBuildKeysMode::INPUT &&
            IsQueryMode(ctx.Settings.Mode) &&
            ctx.GetClusterProvider(Cluster_).GetRef() != "kikimr" &&
            ctx.GetClusterProvider(Cluster_).GetRef() != "rtmr") {

            key = Y("MrTableConcat", key);
        }
        return key;
    }

private:
    TString Cluster_;
    TDeferredAtom Name_;
    TString View_;
    TString Full_;
};

TNodePtr BuildTableKey(TPosition pos, const TString& cluster, const TDeferredAtom& name, const TString& view) {
    return new TUniqueTableKey(pos, cluster, name, view);
}

class TPrepTableKeys: public ITableKeys {
public:
    TPrepTableKeys(TPosition pos, const TString& cluster, const TString& func, const TVector<TTableArg>& args)
        : ITableKeys(pos)
        , Cluster_(cluster)
        , Func_(func)
        , Args_(args)
    {
    }

    void ExtractTableName(TContext&ctx, TTableArg& arg) {
        MakeTableFromExpression(ctx, arg.Expr, arg.Id);
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode mode) override {
        if (mode == ITableKeys::EBuildKeysMode::CREATE) {
            // TODO: allow creation of multiple tables
            ctx.Error(Pos_) << "Mutiple table creation is not implemented yet";
            return nullptr;
        }

        TCiString func(Func_);
        if (func == "concat_strict") {
            auto tuple = Y();
            for (auto& arg: Args_) {
                ExtractTableName(ctx, arg);
                TNodePtr key;
                if (arg.HasAt) {
                    key = Y("TempTable", arg.Id.Build());
                } else {
                    auto path = ctx.GetPrefixedPath(Cluster_, arg.Id);
                    if (!path) {
                        return nullptr;
                    }

                    key = Y("Key", Q(Y(Q("table"), Y("String", path))));
                    if (!arg.View.empty()) {
                        key = L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos_, arg.View)))));
                    }
                }

                tuple = L(tuple, key);
            }
            return Q(tuple);
        }
        else if (func == "concat") {
            auto concat = Y("MrTableConcat");
            for (auto& arg : Args_) {
                ExtractTableName(ctx, arg);
                TNodePtr key;
                if (arg.HasAt) {
                    key = Y("TempTable", arg.Id.Build());
                } else {
                    auto path = ctx.GetPrefixedPath(Cluster_, arg.Id);
                    if (!path) {
                        return nullptr;
                    }

                    key = Y("Key", Q(Y(Q("table"), Y("String", path))));
                    if (!arg.View.empty()) {
                        key = L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos_, arg.View)))));
                    }
                }

                concat = L(concat, key);
            }

            return concat;
        }

        else if (func == "range" || func == "range_strict" || func == "like" || func == "like_strict" ||
            func == "regexp" || func == "regexp_strict" || func == "filter" || func == "filter_strict") {
            bool isRange = func.StartsWith("range");
            bool isFilter = func.StartsWith("filter");
            size_t minArgs = isRange ? 1 : 2;
            size_t maxArgs = isRange ? 5 : 4;
            if (Args_.size() < minArgs || Args_.size() > maxArgs) {
                ctx.Error(Pos_) << Func_ << " requires from " << minArgs << " to " << maxArgs << " arguments, but got: " << Args_.size();
                return nullptr;
            }

            for (ui32 index=0; index < Args_.size(); ++index) {
                auto& arg = Args_[index];
                if (arg.HasAt) {
                    ctx.Error(Pos_) << "Temporary tables are not supported here";
                    return nullptr;
                }

                if (!arg.View.empty()) {
                    TStringBuilder sb;
                    sb << "Use the last argument of " << Func_ << " to specify a VIEW." << Endl;
                    if (isRange) {
                        sb << "Possible arguments are: prefix, from, to, suffix, view." << Endl;
                    } else if (isFilter) {
                        sb << "Possible arguments are: prefix, filtering callable, suffix, view." << Endl;
                    } else {
                        sb << "Possible arguments are: prefix, pattern, suffix, view." << Endl;
                    }
                    sb << "Pass [] to arguments you want to skip.";

                    ctx.Error(Pos_) << sb;
                    return nullptr;
                }

                if (!func.StartsWith("filter") || index != 1) {
                    ExtractTableName(ctx, arg);
                }
            }

            auto path = ctx.GetPrefixedPath(Cluster_, Args_[0].Id);
            if (!path) {
                return nullptr;
            }
            auto range = Y(func.EndsWith("_strict") ? "MrTableRangeStrict" : "MrTableRange", path);
            TNodePtr predicate;
            TDeferredAtom suffix;
            if (func.StartsWith("range")) {
                TDeferredAtom min;
                TDeferredAtom max;
                if (Args_.size() > 1) {
                    min = Args_[1].Id;
                }

                if (Args_.size() > 2) {
                    max = Args_[2].Id;
                }

                if (Args_.size() > 3) {
                    suffix = Args_[3].Id;
                }

                if (min.Empty() && max.Empty()) {
                    predicate = BuildLambda(Pos_, Y("item"), Y("Bool", Q("true")));
                }
                else {
                    auto minPred = !min.Empty() ? Y(">=", "item", Y("String", min.Build())) : nullptr;
                    auto maxPred = !max.Empty() ? Y("<=", "item", Y("String", max.Build())) : nullptr;
                    if (!minPred) {
                        predicate = BuildLambda(Pos_, Y("item"), maxPred);
                    } else if (!maxPred) {
                        predicate = BuildLambda(Pos_, Y("item"), minPred);
                    } else {
                        predicate = BuildLambda(Pos_, Y("item"), Y("And", minPred, maxPred));
                    }
                }
            } else {
                if (Args_.size() > 2) {
                    suffix = Args_[2].Id;
                }

                if (func.StartsWith("regexp")) {
                    auto pattern = Args_[1].Id;
                    auto udf = Y("Udf", Q("Pcre.BacktrackingGrep"), Y("String", pattern.Build()));
                    predicate = BuildLambda(Pos_, Y("item"), Y("Apply", udf, "item"));
                } else if (func.StartsWith("like")) {
                    auto pattern = Args_[1].Id;
                    auto convertedPattern = Y("Apply", Y("Udf", Q("Re2.PatternFromLike")),
                        Y("String", pattern.Build()));
                    auto udf = Y("Udf", Q("Re2.Match"), Q(Y(convertedPattern, Y("Null"))));
                    predicate = BuildLambda(Pos_, Y("item"), Y("Apply", udf, "item"));
                } else {
                    predicate = BuildLambda(Pos_, Y("item"), Y("Apply", Args_[1].Expr, "item"));
                }
            }

            range = L(range, predicate);
            range = L(range, suffix.Build() ? suffix.Build() : BuildQuotedAtom(Pos_, ""));
            auto key = Y("Key", Q(Y(Q("table"), range)));
            if (Args_.size() == maxArgs) {
                const auto& lastArg = Args_.back();
                if (!lastArg.View.empty()) {
                    ctx.Error(Pos_) << Func_ << " requires that view should be set as last argument";
                    return nullptr;
                }

                if (!lastArg.Id.Empty()) {
                    key = L(key, Q(Y(Q("view"), Y("String", lastArg.Id.Build()))));
                }
            }

            return key;
        } else if (func == "each" || func == "each_strict") {
            auto each = Y(func == "each" ? "MrTableEach" : "MrTableEachStrict");
            for (auto& arg : Args_) {
                if (arg.HasAt) {
                    ctx.Error(Pos_) << "Temporary tables are not supported here";
                    return nullptr;
                }

                auto type = Y("ListType", Y("DataType", Q("String")));
                auto key = Y("Key", Q(Y(Q("table"), Y("EvaluateExpr",
                    Y("EnsureType", Y("Coalesce", arg.Expr, Y("List", type)), type)))));
                if (!arg.View.empty()) {
                    key = L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos_, arg.View)))));
                }
                each = L(each, key);
            }

            return each;
        }
        else if (func == "folder") {
            size_t minArgs = 1;
            size_t maxArgs = 2;
            if (Args_.size() < minArgs || Args_.size() > maxArgs) {
                ctx.Error(Pos_) << Func_ << " requires from " << minArgs << " to " << maxArgs << " arguments, but found: " << Args_.size();
                return nullptr;
            }

            for (ui32 index = 0; index < Args_.size(); ++index) {
                auto& arg = Args_[index];
                if (arg.HasAt) {
                    ctx.Error(Pos_) << "Temporary tables are not supported here";
                    return nullptr;
                }

                if (!arg.View.empty()) {
                    ctx.Error(Pos_) << Func_ << " doesn't supports views";
                    return nullptr;
                }

                ExtractTableName(ctx, arg);
            }

            auto folder = Y("MrFolder");
            folder = L(folder, Args_[0].Id.Build());
            folder = L(folder, Args_.size() > 1 ? Args_[1].Id.Build() : BuildQuotedAtom(Pos_, ""));
            return folder;
        }

        ctx.Error(Pos_) << "Unknown table name preprocessor: " << Func_;
        return nullptr;
    }

private:
    TString Cluster_;
    TString Func_;
    TVector<TTableArg> Args_;
};

TNodePtr BuildTableKeys(TPosition pos, const TString& cluster, const TString& func, const TVector<TTableArg>& args) {
    return new TPrepTableKeys(pos, cluster, func, args);
}

class TInputOptions final: public TAstListNode {
public:
    TInputOptions(TPosition pos, const TVector<TString>& hints)
        : TAstListNode(pos)
        , Hints_(hints)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        TSet<TString> used;
        for (auto& hint: Hints_) {
            TMaybe<TIssue> normalizeError = NormalizeName(Pos_, hint);
            if (!normalizeError.Empty()) {
                ctx.Error() << normalizeError->GetMessage();
                ctx.IncrementMonCounter("sql_errors", "NormalizeHintError");
                return false;
            }
            TNodePtr option;
            if (used.insert(hint).second) {
                option = Y(BuildQuotedAtom(Pos_, hint));
            }
            if (option) {
                Nodes_.push_back(Q(option));
            }
        }
        return true;
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TVector<TString> Hints_;
};

TNodePtr BuildInputOptions(TPosition pos, const TVector<TString>& hints) {
    if (hints.empty()) {
        return nullptr;
    }

    return new TInputOptions(pos, hints);
}

class TInputTablesNode final: public TAstListNode {
public:
    TInputTablesNode(TPosition pos, const TTableList& tables, bool inSubquery)
        : TAstListNode(pos)
        , Tables_(tables)
        , InSubquery_(inSubquery)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        THashSet<TString> tables;
        for (auto& tr: Tables_) {
            if (!tables.insert(tr.RefName).second) {
                continue;
            }

            if (!tr.Check(ctx)) {
                return false;
            }
            auto tableKeys = tr.Keys->GetTableKeys();
            auto keys = tableKeys->BuildKeys(ctx, ITableKeys::EBuildKeysMode::INPUT);
            ctx.PushBlockShortcuts();
            if (!keys || !keys->Init(ctx, src)) {
                return false;
            }
            keys = ctx.GroundBlockShortcutsForExpr(keys);
            auto service = tr.ServiceName(ctx);
            auto fields = Y("Void");
            auto source = Y("DataSource", BuildQuotedAtom(Pos_, service), BuildQuotedAtom(Pos_, tr.Cluster));
            auto options = tr.Options ? Q(tr.Options) : Q(Y());
            Add(Y("let", "x", keys->Y(TString(ReadName), "world", source, keys, fields, options)));
            if (service != YtProviderName) {
                if (InSubquery_) {
                    ctx.Error() << "Using of system '" << service << "' is not allowed in SUBQUERY";
                    return false;
                }

                Add(Y("let", "world", Y(TString(LeftName), "x")));
            }

            Add(Y("let", tr.RefName, Y(TString(RightName), "x")));
            ctx.UsedClusters.insert(tr.Cluster);
        }
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TTableList Tables_;
    const bool InSubquery_;
};

TNodePtr BuildInputTables(TPosition pos, const TTableList& tables, bool inSubquery) {
    return new TInputTablesNode(pos, tables, inSubquery);
}

class TCreateTableNode final: public TAstListNode {
public:
    TCreateTableNode(TPosition pos, const TTableRef& tr, const TVector<TColumnSchema>& columns,
        const TVector<TIdentifier>& pkColumns, const TVector<TIdentifier>& partitionByColumns,
        const TVector<std::pair<TIdentifier, bool>>& orderByColumns)
        : TAstListNode(pos)
        , Table_(tr)
        , Columns_(columns)
        , PkColumns_(pkColumns)
        , PartitionByColumns_(partitionByColumns)
        , OrderByColumns_(orderByColumns)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Table_.Check(ctx)) {
            return false;
        }
        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        ctx.PushBlockShortcuts();
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }
        keys = ctx.GroundBlockShortcutsForExpr(keys);

        if (!PkColumns_.empty() || !PartitionByColumns_.empty() || !OrderByColumns_.empty()) {
            THashSet<TString> columnsSet;
            for (auto& col : Columns_) {
                columnsSet.insert(col.Name);
            }

            for (auto& keyColumn : PkColumns_) {
                if (!columnsSet.contains(keyColumn.Name)) {
                    ctx.Error(keyColumn.Pos) << "Undefined column: " << keyColumn.Name;
                    return false;
                }
            }
            for (auto& keyColumn : PartitionByColumns_) {
                if (!columnsSet.contains(keyColumn.Name)) {
                    ctx.Error(keyColumn.Pos) << "Undefined column: " << keyColumn.Name;
                    return false;
                }
            }
            for (auto& keyColumn : OrderByColumns_) {
                if (!columnsSet.contains(keyColumn.first.Name)) {
                    ctx.Error(keyColumn.first.Pos) << "Undefined column: " << keyColumn.first.Name;
                    return false;
                }
            }
        }

        auto columns = Y();
        for (auto& col: Columns_) {
            auto type = ParseType(TypeByAlias(col.Type, !col.IsTypeString), *ctx.Pool, ctx.Issues, col.Pos);
            if (!type) {
                return false;
            }
            Y_ASSERT(type->IsList());
            Y_ASSERT(type->GetChildrenCount() > 1);
            auto typeName = type->GetChild(0);
            Y_ASSERT(typeName->IsAtom());
            if (typeName->GetContent() == "OptionalType") {
                ctx.Error(col.Pos) << "CREATE TABLE clause requires non-optional column types in scheme";
                return false;
            }
            if (col.Nullable) {
                type = TAstNode::NewList(
                    col.Pos,
                    *ctx.Pool,
                    TAstNode::NewLiteralAtom(
                        col.Pos,
                        "OptionalType",
                        *ctx.Pool
                    ),
                    type
                );
            }
            columns = L(columns, Q(Y(BuildQuotedAtom(Pos_, col.Name), AstNode(type))));
        }

        auto opts = Y();
        if (Table_.Options) {
            if (!Table_.Options->Init(ctx, src)) {
                return false;
            }
            opts = Table_.Options;
        }
        opts = L(opts, Q(Y(Q("mode"), Q("create"))));
        opts = L(opts, Q(Y(Q("columns"), Q(columns))));

        const auto serviceName = to_lower(Table_.ServiceName(ctx));
        if (serviceName == RtmrProviderName) {
            if (!PkColumns_.empty() && !PartitionByColumns_.empty()) {
                ctx.Error() << "Only one of PRIMARY KEY or PARTITION BY constraints may be specified";
                return false;
            }
        } else {
            if (!PartitionByColumns_.empty() || !OrderByColumns_.empty()) {
                ctx.Error() << "PARTITION BY and ORDER BY are supported only for " << RtmrProviderName << " provider";
                return false;
            }
        }

        if (!PkColumns_.empty()) {
            auto primaryKey = Y();
            for (auto& col : PkColumns_) {
                primaryKey = L(primaryKey, BuildQuotedAtom(col.Pos, col.Name));
            }
            opts = L(opts, Q(Y(Q("primarykey"), Q(primaryKey))));
            if (!OrderByColumns_.empty()) {
                ctx.Error() << "PRIMARY KEY cannot be used with ORDER BY, use PARTITION BY instead";
                return false;
            }
        } else {
            if (!PartitionByColumns_.empty()) {
                auto partitionBy = Y();
                for (auto& col : PartitionByColumns_) {
                    partitionBy = L(partitionBy, BuildQuotedAtom(col.Pos, col.Name));
                }
                opts = L(opts, Q(Y(Q("partitionby"), Q(partitionBy))));
            }
            if (!OrderByColumns_.empty()) {
                auto orderBy = Y();
                for (auto& col : OrderByColumns_) {
                    orderBy = L(orderBy, Q(Y(BuildQuotedAtom(col.first.Pos, col.first.Name), col.second ? Q("1") : Q("0"))));
                }
                opts = L(opts, Q(Y(Q("orderby"), Q(orderBy))));
            }
        }

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Table_.ServiceName(ctx)), BuildQuotedAtom(Pos_, Table_.Cluster))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        ctx.UsedClusters.insert(Table_.Cluster);
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TTableRef Table_;
    TVector<TColumnSchema> Columns_;
    TVector<TIdentifier> PkColumns_;
    TVector<TIdentifier> PartitionByColumns_;
    TVector<std::pair<TIdentifier, bool>> OrderByColumns_; // column, is desc?
};

TNodePtr BuildCreateTable(TPosition pos, const TTableRef& tr, const TVector<TColumnSchema>& columns,
    const TVector<TIdentifier>& pkColumns, const TVector<TIdentifier>& partitionByColumns,
    const TVector<std::pair<TIdentifier, bool>>& orderByColumns)
{
    return new TCreateTableNode(pos, tr, columns, pkColumns, partitionByColumns, orderByColumns);
}

class TAlterTableNode final: public TAstListNode {
public:
    TAlterTableNode(TPosition pos, const TTableRef& tr, const TVector<TColumnSchema>& columns, EAlterTableIntentnt intent)
        : TAstListNode(pos)
        , Table_(tr)
        , Columns_(columns)
        , Intent_(intent)
    {}
    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Table_.Check(ctx)) {
            return false;
        }

        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        ctx.PushBlockShortcuts();
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }
        keys = ctx.GroundBlockShortcutsForExpr(keys);

        auto actions = Y();

        if (Intent_ == EAlterTableIntentnt::DropColumn) {
            auto columns = Y();
            for (auto& col : Columns_) {
                columns = L(columns, BuildQuotedAtom(Pos_, col.Name));
            }
            actions = L(actions, Q(Y(Q("dropColumns"), Q(columns))));
        } else {
            auto columns = Y();
            for (auto& col: Columns_) {
                auto type = ParseType(TypeByAlias(col.Type, !col.IsTypeString), *ctx.Pool, ctx.Issues, col.Pos);
                if (!type) {
                    return false;
                }
                Y_ASSERT(type->IsList());
                Y_ASSERT(type->GetChildrenCount() > 1);
                auto typeName = type->GetChild(0);
                Y_ASSERT(typeName->IsAtom());
                if (typeName->GetContent() == "OptionalType") {
                    ctx.Error(col.Pos) << "ALTER TABLE clause requires non-optional column types in scheme";
                    return false;
                }
                if (col.Nullable) {
                    type = TAstNode::NewList(
                        col.Pos,
                        *ctx.Pool,
                        TAstNode::NewLiteralAtom(
                            col.Pos,
                            "OptionalType",
                            *ctx.Pool
                        ),
                        type
                    );
                }
                columns = L(columns, Q(Y(BuildQuotedAtom(Pos_, col.Name), AstNode(type))));
            }
            actions = L(actions, Q(Y(Q("addColumns"), Q(columns))));
        }

        auto opts = Y();

        opts = L(opts, Q(Y(Q("mode"), Q("alter"))));
        opts = L(opts, Q(Y(Q("actions"), Q(actions))));

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Table_.ServiceName(ctx)), BuildQuotedAtom(Pos_, Table_.Cluster))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        ctx.UsedClusters.insert(Table_.Cluster);
        return TAstListNode::DoInit(ctx, src);
    }
    TPtr DoClone() const final {
        return {};
    }
private:
    TTableRef Table_;
    TVector<TColumnSchema> Columns_;
    EAlterTableIntentnt Intent_;
};

TNodePtr BuildAlterTable(TPosition pos, const TTableRef& tr, const TVector<TColumnSchema>& columns, EAlterTableIntentnt intent)
{
    return new TAlterTableNode(pos, tr, columns, intent);
}

class TDropTableNode final: public TAstListNode {
public:
    TDropTableNode(TPosition pos, const TTableRef& tr)
        : TAstListNode(pos)
        , Table_(tr)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        if (!Table_.Check(ctx)) {
            return false;
        }
        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::DROP);
        ctx.PushBlockShortcuts();
        if (!keys || !keys->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        keys = ctx.GroundBlockShortcutsForExpr(keys);

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Table_.ServiceName(ctx)), BuildQuotedAtom(Pos_, Table_.Cluster))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(Y(Q(Y(Q("mode"), Q("drop"))))))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        ctx.UsedClusters.insert(Table_.Cluster);
        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TTableRef Table_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildDropTable(TPosition pos, const TTableRef& tr) {
    return new TDropTableNode(pos, tr);
}

static const TMap<EWriteColumnMode, TString> columnModeToStrMapMR {
    {EWriteColumnMode::Default, ""},
    {EWriteColumnMode::Insert, "append"},
    {EWriteColumnMode::Renew, "renew"}
};

static const TMap<EWriteColumnMode, TString> columnModeToStrMapStat {
    {EWriteColumnMode::Upsert, "upsert"}
};

static const TMap<EWriteColumnMode, TString> columnModeToStrMapKikimr {
    {EWriteColumnMode::Default, ""},
    {EWriteColumnMode::Insert, "insert_abort"},
    {EWriteColumnMode::InsertOrAbort, "insert_abort"},
    {EWriteColumnMode::InsertOrIgnore, "insert_ignore"},
    {EWriteColumnMode::InsertOrRevert, "insert_revert"},
    {EWriteColumnMode::Upsert, "upsert"},
    {EWriteColumnMode::Replace, "replace"},
    {EWriteColumnMode::Update, "update"},
    {EWriteColumnMode::UpdateOn, "update_on"},
    {EWriteColumnMode::Delete, "delete"},
    {EWriteColumnMode::DeleteOn, "delete_on"},
};

class TWriteTableNode final: public TAstListNode {
public:
    TWriteTableNode(TPosition pos, const TString& label, const TTableRef& table, EWriteColumnMode mode,
        TNodePtr options)
        : TAstListNode(pos)
        , Label_(label)
        , Table_(table)
        , Mode_(mode)
        , Options_(options)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Table_.Check(ctx)) {
            return false;
        }
        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::WRITE);
        ctx.PushBlockShortcuts();
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }
        keys = ctx.GroundBlockShortcutsForExpr(keys);

        const auto serviceName = to_lower(Table_.ServiceName(ctx));
        auto getModesMap = [] (const TString& serviceName) -> const TMap<EWriteColumnMode, TString>& {
            if (serviceName == KikimrProviderName) {
                return columnModeToStrMapKikimr;
            } else if (serviceName == StatProviderName) {
                return columnModeToStrMapStat;
            } else {
                return columnModeToStrMapMR;
            }
        };

        auto options = Y();
        if (Options_) {
            if (!Options_->Init(ctx, src)) {
                return false;
            }

            options = L(Options_);
        }

        if (Mode_ != EWriteColumnMode::Default) {
            auto modeStr = getModesMap(serviceName).FindPtr(Mode_);

            options->Add(Q(Y(Q("mode"), Q(modeStr ? *modeStr : "unsupported"))));
        }

        Add("block", Q((Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Table_.ServiceName(ctx)), BuildQuotedAtom(Pos_, Table_.Cluster))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Label_, Q(options))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        ))));

        ctx.UsedClusters.insert(Table_.Cluster);
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TString Label_;
    TTableRef Table_;
    EWriteColumnMode Mode_;
    TNodePtr Options_;
};

TNodePtr BuildWriteTable(TPosition pos, const TString& label, const TTableRef& table, EWriteColumnMode mode, TNodePtr options)
{
    return new TWriteTableNode(pos, label, table, mode, std::move(options));
}

class TClustersSinkOperationBase: public TAstListNode {
protected:
    TClustersSinkOperationBase(TPosition pos, const TSet<TString>& clusters)
        : TAstListNode(pos)
        , Clusters_(clusters) {}

    virtual TPtr ProduceOperation(TContext& ctx, const TString& sinkName, const TString& service) = 0;

    bool DoInit(TContext& ctx, ISource* src) override {
        auto block(Y());
        auto clusters = &Clusters_;
        if (Clusters_.empty()) {
            clusters = &ctx.UsedClusters;
        }
        if (clusters->empty() && !ctx.CurrCluster.empty()) {
            clusters->insert(ctx.CurrCluster);
        }

        for (auto& cluster: *clusters) {
            TString normalizedClusterName;
            auto service = ctx.GetClusterProvider(cluster, normalizedClusterName);
            if (!service) {
                ctx.Error(ctx.Pos()) << "Unknown cluster: " << cluster;
                return false;
            }

            auto sinkName = normalizedClusterName + "_sink";

            auto op = ProduceOperation(ctx, sinkName, *service);
            if (!op) {
                return false;
            }

            block = L(block, Y("let", sinkName, Y("DataSink", BuildQuotedAtom(Pos_, *service), BuildQuotedAtom(Pos_, normalizedClusterName))));
            block = L(block, op);
        }

        clusters->clear();
        block = L(block, Y("return", "world"));
        Add("block", Q(block));

        return TAstListNode::DoInit(ctx, src);
     }

    TPtr DoClone() const final {
        return {};
    }
private:
    TSet<TString> Clusters_;
};

class TCommitClustersNode: public TClustersSinkOperationBase {
public:
    TCommitClustersNode(TPosition pos, const TSet<TString>& clusters)
        : TClustersSinkOperationBase(pos, clusters) {}

    TPtr ProduceOperation(TContext& ctx, const TString& sinkName, const TString& service) override {
        Y_UNUSED(ctx);
        Y_UNUSED(service);
        return Y("let", "world", Y(TString(CommitName), "world", sinkName));
    }
};

TNodePtr BuildCommitClusters(TPosition pos, const TSet<TString>& clusters) {
    return new TCommitClustersNode(pos, clusters);
}

class TRollbackClustersNode: public TClustersSinkOperationBase {
public:
    TRollbackClustersNode(TPosition pos, const TSet<TString>& clusters)
        : TClustersSinkOperationBase(pos, clusters) {}

    TPtr ProduceOperation(TContext& ctx, const TString& sinkName, const TString& service) override {
        if (service != KikimrProviderName) {
            ctx.Error(ctx.Pos()) << "ROLLBACK isn't supported for provider: " << TStringBuf(service);
            return nullptr;
        }

        return Y("let", "world", Y(TString(CommitName), "world", sinkName, Q(Y(Q(Y(Q("mode"), Q("rollback")))))));
    }
};

TNodePtr BuildRollbackClusters(TPosition pos, const TSet<TString>& clusters) {
    return new TRollbackClustersNode(pos, clusters);
}

class TWriteResultNode final: public TAstListNode {
public:
    TWriteResultNode(TPosition pos, const TString& label, TNodePtr settings, const TSet<TString>& clusters)
        : TAstListNode(pos)
        , Label_(label)
        , Settings_(settings)
        , CommitClusters_(BuildCommitClusters(Pos_, clusters))
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        auto block(Y(
            Y("let", "result_sink", Y("DataSink", Q(TString(ResultProviderName)))),
            Y("let", "world", Y(TString(WriteName), "world", "result_sink", Y("Key"), Label_, Q(Settings_)))
        ));
        if (ctx.PragmaAutoCommit) {
            block = L(block, Y("let", "world", CommitClusters_));
        }

        block = L(block, Y("return", Y(TString(CommitName), "world", "result_sink")));
        Add("block", Q(block));
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TString Label_;
    TNodePtr Settings_;
    TNodePtr CommitClusters_;
};

TNodePtr BuildWriteResult(TPosition pos, const TString& label, TNodePtr settings, const TSet<TString>& clusters) {
    return new TWriteResultNode(pos, label, settings, clusters);
}

class TYqlProgramNode: public TAstListNode {
public:
    TYqlProgramNode(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel)
        : TAstListNode(pos)
        , Blocks_(blocks)
        , TopLevel_(topLevel)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        bool hasError = false;
        if (TopLevel_) {
            for (auto& var: ctx.Variables) {
                if (!var.second->Init(ctx, src)) {
                    hasError = true;
                    continue;
                }
                Add(Y("declare", var.first, var.second));
            }

            for (const auto& lib : ctx.Libraries) {
                Add(Y("library",
                    new TAstAtomNodeImpl(Pos_, lib, TNodeFlags::ArbitraryContent)));
            }

            Add(Y("import", "aggregate_module", BuildQuotedAtom(Pos_, "/lib/yql/aggregate.yqls")));
            Add(Y("import", "window_module", BuildQuotedAtom(Pos_, "/lib/yql/window.yqls")));
            for (const auto& module : ctx.Settings.ModuleMapping) {
                TString moduleName(module.first + "_module");
                moduleName.to_lower();
                Add(Y("import", moduleName, BuildQuotedAtom(Pos_, module.second)));
            }
            for (const auto& moduleAlias : ctx.ImportModuleAliases) {
                Add(Y("import", moduleAlias.second, BuildQuotedAtom(Pos_, moduleAlias.first)));
            }

            for (const auto& x : ctx.SimpleUdfs) {
                Add(Y("let", x.second, Y("Udf", BuildQuotedAtom(Pos_, x.first))));
            }

            for (auto& nodes: ctx.NamedNodes) {
                if (src || ctx.Exports.contains(nodes.first)) {
                    auto& node = nodes.second.top();
                    ctx.PushBlockShortcuts();
                    if (!node->Init(ctx, src)) {
                        hasError = true;
                        continue;
                    }

                    node = ctx.GroundBlockShortcutsForExpr(node);
                    // Some constants may be used directly by YQL code and need to be translated without reference from SQL AST
                    if (node->IsConstant() || ctx.Exports.contains(nodes.first)) {
                        Add(Y("let", BuildAtom(node->GetPos(), nodes.first), node));
                    }
                }
            }

            if (ctx.Settings.Mode != NSQLTranslation::ESqlMode::LIBRARY) {
                auto configSource = Y("DataSource", BuildQuotedAtom(Pos_, TString(ConfigProviderName)));
                auto resultSink = Y("DataSink", BuildQuotedAtom(Pos_, TString(ResultProviderName)));

                for (const auto& warningPragma : ctx.WarningPolicy.GetRules()) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos_, "Warning"), BuildQuotedAtom(Pos_, warningPragma.GetPattern()),
                            BuildQuotedAtom(Pos_, to_lower(ToString(warningPragma.GetAction()))))));
                }

                if (ctx.ResultSizeLimit > 0) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", resultSink,
                        BuildQuotedAtom(Pos_, "SizeLimit"), BuildQuotedAtom(Pos_, ToString(ctx.ResultSizeLimit)))));
                }

                if (!ctx.PragmaPullUpFlatMapOverJoin) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos_, "DisablePullUpFlatMapOverJoin"))));
                }
            }
        }

        for (auto& block: Blocks_) {
            if (block->SubqueryAlias()) {
                continue;
            }
            if (!block->Init(ctx, nullptr)) {
                hasError = true;
                continue;
            }
        }

        for (auto& block: Blocks_) {
            const auto subqueryAliasPtr = block->SubqueryAlias();
            if (subqueryAliasPtr) {
                if (block->UsedSubquery()) {
                    const auto& ref = block->GetLabel();
                    YQL_ENSURE(!ref.empty());
                    Add(block);
                    Add(Y("let", "world", Y("Nth", *subqueryAliasPtr, Q("0"))));
                    Add(Y("let", ref, Y("Nth", *subqueryAliasPtr, Q("1"))));
                }
            } else {
                const auto& ref = block->GetLabel();
                Add(Y("let", ref ? ref : "world", block));
            }
        }

        if (TopLevel_) {
            if (ctx.UniversalAliases) {
                decltype(Nodes_) preparedNodes;
                preparedNodes.swap(Nodes_);
                for (auto aliasPair : ctx.UniversalAliases) {
                    Add(Y("let", aliasPair.first, aliasPair.second));
                }
                Nodes_.insert(Nodes_.end(), preparedNodes.begin(), preparedNodes.end());
            }

            for (const auto& symbol: ctx.Exports) {
                Add(Y("export", symbol));
            }
        }

        if (!TopLevel_ || ctx.Settings.Mode != NSQLTranslation::ESqlMode::LIBRARY) {
            Add(Y("return", "world"));
        }

        return !hasError;
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TVector<TNodePtr> Blocks_;
    const bool TopLevel_;
};

TNodePtr BuildQuery(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel) {
    return new TYqlProgramNode(pos, blocks, topLevel);
}

class TPragmaNode final: public INode {
public:
    TPragmaNode(TPosition pos, const TString& prefix, const TString& name, const TVector<TDeferredAtom>& values, bool valueDefault)
        : INode(pos)
        , Prefix_(prefix)
        , Name_(name)
        , Values_(values)
        , ValueDefault_(valueDefault)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        TString serviceName;
        TString cluster;
        if (std::find(Providers.cbegin(), Providers.cend(), Prefix_) != Providers.cend()) {
            cluster = "$all";
            serviceName = Prefix_;
        } else {
            serviceName = *ctx.GetClusterProvider(Prefix_, cluster);
        }

        auto datasource = Y("DataSource", BuildQuotedAtom(Pos_, serviceName));
        if (Prefix_ != ConfigProviderName) {
            datasource = L(datasource, BuildQuotedAtom(Pos_, cluster));
        }

        Node_ = Y();
        Node_ = L(Node_, AstNode(TString(ConfigureName)));
        Node_ = L(Node_, AstNode(TString(TStringBuf("world"))));
        Node_ = L(Node_, datasource);

        if (Name_ == TStringBuf("flags")) {
            for (ui32 i = 0; i < Values_.size(); ++i) {
                Node_ = L(Node_, Values_[i].Build());
            }
        }
        else if (Name_ == TStringBuf("AddFileByUrl") || Name_ == TStringBuf("AddFolderByUrl") || Name_ == TStringBuf("ImportUdfs")) {
            Node_ = L(Node_, BuildQuotedAtom(Pos_, Name_));
            for (ui32 i = 0; i < Values_.size(); ++i) {
                Node_ = L(Node_, Values_[i].Build());
            }
        }
        else if (Name_ == TStringBuf("auth")) {
            Node_ = L(Node_, BuildQuotedAtom(Pos_, "Auth"));
            Node_ = L(Node_, Values_.empty() ? BuildQuotedAtom(Pos_, TString()) : Values_.front().Build());
        }
        else {
            Node_ = L(Node_, BuildQuotedAtom(Pos_, "Attr"));
            Node_ = L(Node_, BuildQuotedAtom(Pos_, Name_));
            if (!ValueDefault_) {
                Node_ = L(Node_, Values_.empty() ? BuildQuotedAtom(Pos_, TString()) : Values_.front().Build());
            }
        }

        ctx.PushBlockShortcuts();
        if (!Node_->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        Node_ = ctx.GroundBlockShortcutsForExpr(Node_);
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TString Prefix_;
    TString Name_;
    TVector<TDeferredAtom> Values_;
    bool ValueDefault_;
    TNodePtr Node_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildPragma(TPosition pos, const TString& prefix, const TString& name, const TVector<TDeferredAtom>& values, bool valueDefault) {
    return new TPragmaNode(pos, prefix, name, values, valueDefault);
}

class TSqlLambda final: public TAstListNode {
public:
    TSqlLambda(TPosition pos, TVector<TString>&& args, TVector<TNodePtr>&& exprSeq)
        : TAstListNode(pos)
        , Args_(args)
        , ExprSeq_(exprSeq)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        for (auto& exprPtr: ExprSeq_) {
            ctx.PushBlockShortcuts();
            if (!exprPtr->Init(ctx, FakeSource_.Get())) {
                return {};
            }
            const auto label = exprPtr->GetLabel();
            exprPtr = ctx.GroundBlockShortcutsForExpr(exprPtr);
            exprPtr->SetLabel(label);
        }
        YQL_ENSURE(!ExprSeq_.empty());
        auto body = Y();
        auto end = ExprSeq_.end() - 1;
        for (auto iter = ExprSeq_.begin(); iter != end; ++iter) {
            auto exprPtr = *iter;
            const auto& label = exprPtr->GetLabel();
            YQL_ENSURE(label);
            body = L(body, Y("let", label, exprPtr));
        }
        body = Y("block", Q(L(body, Y("return", *end))));
        auto args = Y();
        for (const auto& arg: Args_) {
            args = L(args, BuildAtom(GetPos(), arg, NYql::TNodeFlags::Default));
        }
        Add("lambda", Q(args), body);
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const);
    }

private:
    TVector<TString> Args_;
    TVector<TNodePtr> ExprSeq_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildSqlLambda(TPosition pos, TVector<TString>&& args, TVector<TNodePtr>&& exprSeq) {
    return new TSqlLambda(pos, std::move(args), std::move(exprSeq));
}

class TEvaluateIf final : public TAstListNode {
public:
    TEvaluateIf(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode)
        : TAstListNode(pos)
        , Predicate_(predicate)
        , ThenNode_(thenNode)
        , ElseNode_(elseNode)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        ctx.PushBlockShortcuts();
        if (!Predicate_->Init(ctx, FakeSource_.Get())) {
            return{};
        }
        auto predicate = ctx.GroundBlockShortcutsForExpr(Predicate_);
        Add("EvaluateIf!");
        Add("world");
        Add(Y("EvaluateExpr", Y("EnsureType", Y("Coalesce", predicate, Y("Bool", Q("false"))), Y("DataType", Q("Bool")))));

        ctx.PushBlockShortcuts();
        if (!ThenNode_->Init(ctx, FakeSource_.Get())) {
            return{};
        }

        auto thenNode = ctx.GroundBlockShortcutsForExpr(ThenNode_);
        Add(thenNode);
        if (ElseNode_) {
            ctx.PushBlockShortcuts();
            if (!ElseNode_->Init(ctx, FakeSource_.Get())) {
                return{};
            }

            auto elseNode = ctx.GroundBlockShortcutsForExpr(ElseNode_);
            Add(elseNode);
        }

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TNodePtr Predicate_;
    TNodePtr ThenNode_;
    TNodePtr ElseNode_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildEvaluateIfNode(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode) {
    return new TEvaluateIf(pos, predicate, thenNode, elseNode);
}

class TEvaluateFor final : public TAstListNode {
public:
    TEvaluateFor(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode)
        : TAstListNode(pos)
        , List_(list)
        , BodyNode_(bodyNode)
        , ElseNode_(elseNode)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        ctx.PushBlockShortcuts();
        if (!List_->Init(ctx, FakeSource_.Get())) {
            return{};
        }
        auto list = ctx.GroundBlockShortcutsForExpr(List_);
        Add("EvaluateFor!");
        Add("world");
        Add(Y("EvaluateExpr", list));
        ctx.PushBlockShortcuts();
        if (!BodyNode_->Init(ctx, FakeSource_.Get())) {
            return{};
        }

        auto bodyNode = ctx.GroundBlockShortcutsForExpr(BodyNode_);
        Add(bodyNode);
        if (ElseNode_) {
            ctx.PushBlockShortcuts();
            if (!ElseNode_->Init(ctx, FakeSource_.Get())) {
                return{};
            }

            auto elseNode = ctx.GroundBlockShortcutsForExpr(ElseNode_);
            Add(elseNode);
        }

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return{};
    }

private:
    TNodePtr List_;
    TNodePtr BodyNode_;
    TNodePtr ElseNode_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildEvaluateForNode(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode) {
    return new TEvaluateFor(pos, list, bodyNode, elseNode);
}
} // namespace NSQLTranslationV0
