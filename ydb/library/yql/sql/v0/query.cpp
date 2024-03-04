#include "node.h"
#include "context.h"

#include <ydb/library/yql/ast/yql_type_string.h>
#include <ydb/library/yql/core/yql_callable_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <library/cpp/charset/ci_string.h>

#include <util/digest/fnv.h>

using namespace NYql;

namespace NSQLTranslationV0 {

class TUniqueTableKey: public ITableKeys {
public:
    TUniqueTableKey(TPosition pos, const TString& cluster, const TDeferredAtom& name, const TString& view)
        : ITableKeys(pos)
        , Cluster(cluster)
        , Name(name)
        , View(view)
        , Full(name.GetRepr())
    {
        if (!View.empty()) {
            Full += ":" + View;
        }
    }

    const TString* GetTableName() const override {
        return Name.GetLiteral() ? &Full : nullptr;
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode mode) override {
        if (View == "@") {
            auto key = Y("TempTable", Name.Build());
            return key;
        }

        bool tableScheme = mode == ITableKeys::EBuildKeysMode::CREATE;
        if (tableScheme && !View.empty()) {
            ctx.Error(Pos) << "Table view can not be created with CREATE TABLE clause";
            return nullptr;
        }
        auto path = ctx.GetPrefixedPath(Cluster, Name);
        if (!path) {
            return nullptr;
        }
        auto key = Y("Key", Q(Y(Q(tableScheme ? "tablescheme" : "table"), Y("String", path))));
        if (!View.empty()) {
            key = L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos, View)))));
        }
        if (mode == ITableKeys::EBuildKeysMode::INPUT &&
            IsQueryMode(ctx.Settings.Mode) &&
            ctx.GetClusterProvider(Cluster).GetRef() != "kikimr" &&
            ctx.GetClusterProvider(Cluster).GetRef() != "rtmr") {

            key = Y("MrTableConcat", key);
        }
        return key;
    }

private:
    TString Cluster;
    TDeferredAtom Name;
    TString View;
    TString Full;
};

TNodePtr BuildTableKey(TPosition pos, const TString& cluster, const TDeferredAtom& name, const TString& view) {
    return new TUniqueTableKey(pos, cluster, name, view);
}

class TPrepTableKeys: public ITableKeys {
public:
    TPrepTableKeys(TPosition pos, const TString& cluster, const TString& func, const TVector<TTableArg>& args)
        : ITableKeys(pos)
        , Cluster(cluster)
        , Func(func)
        , Args(args)
    {
    }

    void ExtractTableName(TContext&ctx, TTableArg& arg) {
        MakeTableFromExpression(ctx, arg.Expr, arg.Id);
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode mode) override {
        if (mode == ITableKeys::EBuildKeysMode::CREATE) {
            // TODO: allow creation of multiple tables
            ctx.Error(Pos) << "Mutiple table creation is not implemented yet";
            return nullptr;
        }

        TCiString func(Func);
        if (func == "concat_strict") {
            auto tuple = Y();
            for (auto& arg: Args) {
                ExtractTableName(ctx, arg);
                TNodePtr key;
                if (arg.HasAt) {
                    key = Y("TempTable", arg.Id.Build());
                } else {
                    auto path = ctx.GetPrefixedPath(Cluster, arg.Id);
                    if (!path) {
                        return nullptr;
                    }

                    key = Y("Key", Q(Y(Q("table"), Y("String", path))));
                    if (!arg.View.empty()) {
                        key = L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos, arg.View)))));
                    }
                }

                tuple = L(tuple, key);
            }
            return Q(tuple);
        }
        else if (func == "concat") {
            auto concat = Y("MrTableConcat");
            for (auto& arg : Args) {
                ExtractTableName(ctx, arg);
                TNodePtr key;
                if (arg.HasAt) {
                    key = Y("TempTable", arg.Id.Build());
                } else {
                    auto path = ctx.GetPrefixedPath(Cluster, arg.Id);
                    if (!path) {
                        return nullptr;
                    }

                    key = Y("Key", Q(Y(Q("table"), Y("String", path))));
                    if (!arg.View.empty()) {
                        key = L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos, arg.View)))));
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
            if (Args.size() < minArgs || Args.size() > maxArgs) {
                ctx.Error(Pos) << Func << " requires from " << minArgs << " to " << maxArgs << " arguments, but got: " << Args.size();
                return nullptr;
            }

            for (ui32 index=0; index < Args.size(); ++index) {
                auto& arg = Args[index];
                if (arg.HasAt) {
                    ctx.Error(Pos) << "Temporary tables are not supported here";
                    return nullptr;
                }

                if (!arg.View.empty()) {
                    TStringBuilder sb;
                    sb << "Use the last argument of " << Func << " to specify a VIEW." << Endl;
                    if (isRange) {
                        sb << "Possible arguments are: prefix, from, to, suffix, view." << Endl;
                    } else if (isFilter) {
                        sb << "Possible arguments are: prefix, filtering callable, suffix, view." << Endl;
                    } else {
                        sb << "Possible arguments are: prefix, pattern, suffix, view." << Endl;
                    }
                    sb << "Pass [] to arguments you want to skip.";

                    ctx.Error(Pos) << sb;
                    return nullptr;
                }

                if (!func.StartsWith("filter") || index != 1) {
                    ExtractTableName(ctx, arg);
                }
            }

            auto path = ctx.GetPrefixedPath(Cluster, Args[0].Id);
            if (!path) {
                return nullptr;
            }
            auto range = Y(func.EndsWith("_strict") ? "MrTableRangeStrict" : "MrTableRange", path);
            TNodePtr predicate;
            TDeferredAtom suffix;
            if (func.StartsWith("range")) {
                TDeferredAtom min;
                TDeferredAtom max;
                if (Args.size() > 1) {
                    min = Args[1].Id;
                }

                if (Args.size() > 2) {
                    max = Args[2].Id;
                }

                if (Args.size() > 3) {
                    suffix = Args[3].Id;
                }

                if (min.Empty() && max.Empty()) {
                    predicate = BuildLambda(Pos, Y("item"), Y("Bool", Q("true")));
                }
                else {
                    auto minPred = !min.Empty() ? Y(">=", "item", Y("String", min.Build())) : nullptr;
                    auto maxPred = !max.Empty() ? Y("<=", "item", Y("String", max.Build())) : nullptr;
                    if (!minPred) {
                        predicate = BuildLambda(Pos, Y("item"), maxPred);
                    } else if (!maxPred) {
                        predicate = BuildLambda(Pos, Y("item"), minPred);
                    } else {
                        predicate = BuildLambda(Pos, Y("item"), Y("And", minPred, maxPred));
                    }
                }
            } else {
                if (Args.size() > 2) {
                    suffix = Args[2].Id;
                }

                if (func.StartsWith("regexp")) {
                    auto pattern = Args[1].Id;
                    auto udf = Y("Udf", Q("Pcre.BacktrackingGrep"), Y("String", pattern.Build()));
                    predicate = BuildLambda(Pos, Y("item"), Y("Apply", udf, "item"));
                } else if (func.StartsWith("like")) {
                    auto pattern = Args[1].Id;
                    auto convertedPattern = Y("Apply", Y("Udf", Q("Re2.PatternFromLike")),
                        Y("String", pattern.Build()));
                    auto udf = Y("Udf", Q("Re2.Match"), Q(Y(convertedPattern, Y("Null"))));
                    predicate = BuildLambda(Pos, Y("item"), Y("Apply", udf, "item"));
                } else {
                    predicate = BuildLambda(Pos, Y("item"), Y("Apply", Args[1].Expr, "item"));
                }
            }

            range = L(range, predicate);
            range = L(range, suffix.Build() ? suffix.Build() : BuildQuotedAtom(Pos, ""));
            auto key = Y("Key", Q(Y(Q("table"), range)));
            if (Args.size() == maxArgs) {
                const auto& lastArg = Args.back();
                if (!lastArg.View.empty()) {
                    ctx.Error(Pos) << Func << " requires that view should be set as last argument";
                    return nullptr;
                }

                if (!lastArg.Id.Empty()) {
                    key = L(key, Q(Y(Q("view"), Y("String", lastArg.Id.Build()))));
                }
            }

            return key;
        } else if (func == "each" || func == "each_strict") {
            auto each = Y(func == "each" ? "MrTableEach" : "MrTableEachStrict");
            for (auto& arg : Args) {
                if (arg.HasAt) {
                    ctx.Error(Pos) << "Temporary tables are not supported here";
                    return nullptr;
                }

                auto type = Y("ListType", Y("DataType", Q("String")));
                auto key = Y("Key", Q(Y(Q("table"), Y("EvaluateExpr",
                    Y("EnsureType", Y("Coalesce", arg.Expr, Y("List", type)), type)))));
                if (!arg.View.empty()) {
                    key = L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos, arg.View)))));
                }
                each = L(each, key);
            }

            return each;
        }
        else if (func == "folder") {
            size_t minArgs = 1;
            size_t maxArgs = 2;
            if (Args.size() < minArgs || Args.size() > maxArgs) {
                ctx.Error(Pos) << Func << " requires from " << minArgs << " to " << maxArgs << " arguments, but found: " << Args.size();
                return nullptr;
            }

            for (ui32 index = 0; index < Args.size(); ++index) {
                auto& arg = Args[index];
                if (arg.HasAt) {
                    ctx.Error(Pos) << "Temporary tables are not supported here";
                    return nullptr;
                }

                if (!arg.View.empty()) {
                    ctx.Error(Pos) << Func << " doesn't supports views";
                    return nullptr;
                }

                ExtractTableName(ctx, arg);
            }

            auto folder = Y("MrFolder");
            folder = L(folder, Args[0].Id.Build());
            folder = L(folder, Args.size() > 1 ? Args[1].Id.Build() : BuildQuotedAtom(Pos, ""));
            return folder;
        }

        ctx.Error(Pos) << "Unknown table name preprocessor: " << Func;
        return nullptr;
    }

private:
    TString Cluster;
    TString Func;
    TVector<TTableArg> Args;
};

TNodePtr BuildTableKeys(TPosition pos, const TString& cluster, const TString& func, const TVector<TTableArg>& args) {
    return new TPrepTableKeys(pos, cluster, func, args);
}

class TInputOptions final: public TAstListNode {
public:
    TInputOptions(TPosition pos, const TVector<TString>& hints)
        : TAstListNode(pos)
        , Hints(hints)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        TSet<TString> used;
        for (auto& hint: Hints) {
            TMaybe<TIssue> normalizeError = NormalizeName(Pos, hint);
            if (!normalizeError.Empty()) {
                ctx.Error() << normalizeError->GetMessage();
                ctx.IncrementMonCounter("sql_errors", "NormalizeHintError");
                return false;
            }
            TNodePtr option;
            if (used.insert(hint).second) {
                option = Y(BuildQuotedAtom(Pos, hint));
            }
            if (option) {
                Nodes.push_back(Q(option));
            }
        }
        return true;
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TVector<TString> Hints;
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
        , Tables(tables)
        , InSubquery(inSubquery)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        THashSet<TString> tables;
        for (auto& tr: Tables) {
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
            auto source = Y("DataSource", BuildQuotedAtom(Pos, service), BuildQuotedAtom(Pos, tr.Cluster));
            auto options = tr.Options ? Q(tr.Options) : Q(Y());
            Add(Y("let", "x", keys->Y(TString(ReadName), "world", source, keys, fields, options)));
            if (service != YtProviderName) {
                if (InSubquery) {
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
    TTableList Tables;
    const bool InSubquery;
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
        , Table(tr)
        , Columns(columns)
        , PkColumns(pkColumns)
        , PartitionByColumns(partitionByColumns)
        , OrderByColumns(orderByColumns)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Table.Check(ctx)) {
            return false;
        }
        auto keys = Table.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        ctx.PushBlockShortcuts();
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }
        keys = ctx.GroundBlockShortcutsForExpr(keys);

        if (!PkColumns.empty() || !PartitionByColumns.empty() || !OrderByColumns.empty()) {
            THashSet<TString> columnsSet;
            for (auto& col : Columns) {
                columnsSet.insert(col.Name);
            }

            for (auto& keyColumn : PkColumns) {
                if (!columnsSet.contains(keyColumn.Name)) {
                    ctx.Error(keyColumn.Pos) << "Undefined column: " << keyColumn.Name;
                    return false;
                }
            }
            for (auto& keyColumn : PartitionByColumns) {
                if (!columnsSet.contains(keyColumn.Name)) {
                    ctx.Error(keyColumn.Pos) << "Undefined column: " << keyColumn.Name;
                    return false;
                }
            }
            for (auto& keyColumn : OrderByColumns) {
                if (!columnsSet.contains(keyColumn.first.Name)) {
                    ctx.Error(keyColumn.first.Pos) << "Undefined column: " << keyColumn.first.Name;
                    return false;
                }
            }
        }

        auto columns = Y();
        for (auto& col: Columns) {
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
            columns = L(columns, Q(Y(BuildQuotedAtom(Pos, col.Name), AstNode(type))));
        }

        auto opts = Y();
        if (Table.Options) {
            if (!Table.Options->Init(ctx, src)) {
                return false;
            }
            opts = Table.Options;
        }
        opts = L(opts, Q(Y(Q("mode"), Q("create"))));
        opts = L(opts, Q(Y(Q("columns"), Q(columns))));

        const auto serviceName = to_lower(Table.ServiceName(ctx));
        if (serviceName == RtmrProviderName) {
            if (!PkColumns.empty() && !PartitionByColumns.empty()) {
                ctx.Error() << "Only one of PRIMARY KEY or PARTITION BY constraints may be specified";
                return false;
            }
        } else {
            if (!PartitionByColumns.empty() || !OrderByColumns.empty()) {
                ctx.Error() << "PARTITION BY and ORDER BY are supported only for " << RtmrProviderName << " provider";
                return false;
            }
        }

        if (!PkColumns.empty()) {
            auto primaryKey = Y();
            for (auto& col : PkColumns) {
                primaryKey = L(primaryKey, BuildQuotedAtom(col.Pos, col.Name));
            }
            opts = L(opts, Q(Y(Q("primarykey"), Q(primaryKey))));
            if (!OrderByColumns.empty()) {
                ctx.Error() << "PRIMARY KEY cannot be used with ORDER BY, use PARTITION BY instead";
                return false;
            }
        } else {
            if (!PartitionByColumns.empty()) {
                auto partitionBy = Y();
                for (auto& col : PartitionByColumns) {
                    partitionBy = L(partitionBy, BuildQuotedAtom(col.Pos, col.Name));
                }
                opts = L(opts, Q(Y(Q("partitionby"), Q(partitionBy))));
            }
            if (!OrderByColumns.empty()) {
                auto orderBy = Y();
                for (auto& col : OrderByColumns) {
                    orderBy = L(orderBy, Q(Y(BuildQuotedAtom(col.first.Pos, col.first.Name), col.second ? Q("1") : Q("0"))));
                }
                opts = L(opts, Q(Y(Q("orderby"), Q(orderBy))));
            }
        }

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Table.ServiceName(ctx)), BuildQuotedAtom(Pos, Table.Cluster))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        ctx.UsedClusters.insert(Table.Cluster);
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TTableRef Table;
    TVector<TColumnSchema> Columns;
    TVector<TIdentifier> PkColumns;
    TVector<TIdentifier> PartitionByColumns;
    TVector<std::pair<TIdentifier, bool>> OrderByColumns; // column, is desc?
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
        , Table(tr)
        , Columns(columns)
        , Intent(intent)
    {}
    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Table.Check(ctx)) {
            return false;
        }

        auto keys = Table.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        ctx.PushBlockShortcuts();
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }
        keys = ctx.GroundBlockShortcutsForExpr(keys);

        auto actions = Y();

        if (Intent == EAlterTableIntentnt::DropColumn) {
            auto columns = Y();
            for (auto& col : Columns) {
                columns = L(columns, BuildQuotedAtom(Pos, col.Name));
            }
            actions = L(actions, Q(Y(Q("dropColumns"), Q(columns))));
        } else {
            auto columns = Y();
            for (auto& col: Columns) {
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
                columns = L(columns, Q(Y(BuildQuotedAtom(Pos, col.Name), AstNode(type))));
            }
            actions = L(actions, Q(Y(Q("addColumns"), Q(columns))));
        }

        auto opts = Y();

        opts = L(opts, Q(Y(Q("mode"), Q("alter"))));
        opts = L(opts, Q(Y(Q("actions"), Q(actions))));

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Table.ServiceName(ctx)), BuildQuotedAtom(Pos, Table.Cluster))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        ctx.UsedClusters.insert(Table.Cluster);
        return TAstListNode::DoInit(ctx, src);
    }
    TPtr DoClone() const final {
        return {};
    }
private:
    TTableRef Table;
    TVector<TColumnSchema> Columns;
    EAlterTableIntentnt Intent;
};

TNodePtr BuildAlterTable(TPosition pos, const TTableRef& tr, const TVector<TColumnSchema>& columns, EAlterTableIntentnt intent)
{
    return new TAlterTableNode(pos, tr, columns, intent);
}

class TDropTableNode final: public TAstListNode {
public:
    TDropTableNode(TPosition pos, const TTableRef& tr)
        : TAstListNode(pos)
        , Table(tr)
    {
        FakeSource = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        if (!Table.Check(ctx)) {
            return false;
        }
        auto keys = Table.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::DROP);
        ctx.PushBlockShortcuts();
        if (!keys || !keys->Init(ctx, FakeSource.Get())) {
            return false;
        }
        keys = ctx.GroundBlockShortcutsForExpr(keys);

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Table.ServiceName(ctx)), BuildQuotedAtom(Pos, Table.Cluster))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(Y(Q(Y(Q("mode"), Q("drop"))))))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        ctx.UsedClusters.insert(Table.Cluster);
        return TAstListNode::DoInit(ctx, FakeSource.Get());
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TTableRef Table;
    TSourcePtr FakeSource;
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
        , Label(label)
        , Table(table)
        , Mode(mode)
        , Options(options)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Table.Check(ctx)) {
            return false;
        }
        auto keys = Table.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::WRITE);
        ctx.PushBlockShortcuts();
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }
        keys = ctx.GroundBlockShortcutsForExpr(keys);

        const auto serviceName = to_lower(Table.ServiceName(ctx));
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
        if (Options) {
            if (!Options->Init(ctx, src)) {
                return false;
            }

            options = L(Options);
        }

        if (Mode != EWriteColumnMode::Default) {
            auto modeStr = getModesMap(serviceName).FindPtr(Mode);

            options->Add(Q(Y(Q("mode"), Q(modeStr ? *modeStr : "unsupported"))));
        }

        Add("block", Q((Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Table.ServiceName(ctx)), BuildQuotedAtom(Pos, Table.Cluster))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Label, Q(options))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        ))));

        ctx.UsedClusters.insert(Table.Cluster);
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TString Label;
    TTableRef Table;
    EWriteColumnMode Mode;
    TNodePtr Options;
};

TNodePtr BuildWriteTable(TPosition pos, const TString& label, const TTableRef& table, EWriteColumnMode mode, TNodePtr options)
{
    return new TWriteTableNode(pos, label, table, mode, std::move(options));
}

class TClustersSinkOperationBase: public TAstListNode {
protected:
    TClustersSinkOperationBase(TPosition pos, const TSet<TString>& clusters)
        : TAstListNode(pos)
        , Clusters(clusters) {}

    virtual TPtr ProduceOperation(TContext& ctx, const TString& sinkName, const TString& service) = 0;

    bool DoInit(TContext& ctx, ISource* src) override {
        auto block(Y());
        auto clusters = &Clusters;
        if (Clusters.empty()) {
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

            block = L(block, Y("let", sinkName, Y("DataSink", BuildQuotedAtom(Pos, *service), BuildQuotedAtom(Pos, normalizedClusterName))));
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
    TSet<TString> Clusters;
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
        , Label(label)
        , Settings(settings)
        , CommitClusters(BuildCommitClusters(Pos, clusters))
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        auto block(Y(
            Y("let", "result_sink", Y("DataSink", Q(TString(ResultProviderName)))),
            Y("let", "world", Y(TString(WriteName), "world", "result_sink", Y("Key"), Label, Q(Settings)))
        ));
        if (ctx.PragmaAutoCommit) {
            block = L(block, Y("let", "world", CommitClusters));
        }

        block = L(block, Y("return", Y(TString(CommitName), "world", "result_sink")));
        Add("block", Q(block));
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TString Label;
    TNodePtr Settings;
    TNodePtr CommitClusters;
};

TNodePtr BuildWriteResult(TPosition pos, const TString& label, TNodePtr settings, const TSet<TString>& clusters) {
    return new TWriteResultNode(pos, label, settings, clusters);
}

class TYqlProgramNode: public TAstListNode {
public:
    TYqlProgramNode(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel)
        : TAstListNode(pos)
        , Blocks(blocks)
        , TopLevel(topLevel)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        bool hasError = false;
        if (TopLevel) {
            for (auto& var: ctx.Variables) {
                if (!var.second->Init(ctx, src)) {
                    hasError = true;
                    continue;
                }
                Add(Y("declare", var.first, var.second));
            }

            for (const auto& lib : ctx.Libraries) {
                Add(Y("library",
                    new TAstAtomNodeImpl(Pos, lib, TNodeFlags::ArbitraryContent)));
            }

            Add(Y("import", "aggregate_module", BuildQuotedAtom(Pos, "/lib/yql/aggregate.yql")));
            Add(Y("import", "window_module", BuildQuotedAtom(Pos, "/lib/yql/window.yql")));
            for (const auto& module : ctx.Settings.ModuleMapping) {
                TString moduleName(module.first + "_module");
                moduleName.to_lower();
                Add(Y("import", moduleName, BuildQuotedAtom(Pos, module.second)));
            }
            for (const auto& moduleAlias : ctx.ImportModuleAliases) {
                Add(Y("import", moduleAlias.second, BuildQuotedAtom(Pos, moduleAlias.first)));
            }

            for (const auto& x : ctx.SimpleUdfs) {
                Add(Y("let", x.second, Y("Udf", BuildQuotedAtom(Pos, x.first))));
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
                auto configSource = Y("DataSource", BuildQuotedAtom(Pos, TString(ConfigProviderName)));
                auto resultSink = Y("DataSink", BuildQuotedAtom(Pos, TString(ResultProviderName)));

                for (const auto& warningPragma : ctx.WarningPolicy.GetRules()) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos, "Warning"), BuildQuotedAtom(Pos, warningPragma.GetPattern()),
                            BuildQuotedAtom(Pos, to_lower(ToString(warningPragma.GetAction()))))));
                }

                if (ctx.ResultSizeLimit > 0) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", resultSink,
                        BuildQuotedAtom(Pos, "SizeLimit"), BuildQuotedAtom(Pos, ToString(ctx.ResultSizeLimit)))));
                }

                if (!ctx.PragmaPullUpFlatMapOverJoin) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos, "DisablePullUpFlatMapOverJoin"))));
                }
            }
        }

        for (auto& block: Blocks) {
            if (block->SubqueryAlias()) {
                continue;
            }
            if (!block->Init(ctx, nullptr)) {
                hasError = true;
                continue;
            }
        }

        for (auto& block: Blocks) {
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

        if (TopLevel) {
            if (ctx.UniversalAliases) {
                decltype(Nodes) preparedNodes;
                preparedNodes.swap(Nodes);
                for (auto aliasPair : ctx.UniversalAliases) {
                    Add(Y("let", aliasPair.first, aliasPair.second));
                }
                Nodes.insert(Nodes.end(), preparedNodes.begin(), preparedNodes.end());
            }

            for (const auto& symbol: ctx.Exports) {
                Add(Y("export", symbol));
            }
        }

        if (!TopLevel || ctx.Settings.Mode != NSQLTranslation::ESqlMode::LIBRARY) {
            Add(Y("return", "world"));
        }

        return !hasError;
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TVector<TNodePtr> Blocks;
    const bool TopLevel;
};

TNodePtr BuildQuery(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel) {
    return new TYqlProgramNode(pos, blocks, topLevel);
}

class TPragmaNode final: public INode {
public:
    TPragmaNode(TPosition pos, const TString& prefix, const TString& name, const TVector<TDeferredAtom>& values, bool valueDefault)
        : INode(pos)
        , Prefix(prefix)
        , Name(name)
        , Values(values)
        , ValueDefault(valueDefault)
    {
        FakeSource = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        TString serviceName;
        TString cluster;
        if (std::find(Providers.cbegin(), Providers.cend(), Prefix) != Providers.cend()) {
            cluster = "$all";
            serviceName = Prefix;
        } else {
            serviceName = *ctx.GetClusterProvider(Prefix, cluster);
        }

        auto datasource = Y("DataSource", BuildQuotedAtom(Pos, serviceName));
        if (Prefix != ConfigProviderName) {
            datasource = L(datasource, BuildQuotedAtom(Pos, cluster));
        }

        Node = Y();
        Node = L(Node, AstNode(TString(ConfigureName)));
        Node = L(Node, AstNode(TString(TStringBuf("world"))));
        Node = L(Node, datasource);

        if (Name == TStringBuf("flags")) {
            for (ui32 i = 0; i < Values.size(); ++i) {
                Node = L(Node, Values[i].Build());
            }
        }
        else if (Name == TStringBuf("AddFileByUrl") || Name == TStringBuf("AddFolderByUrl") || Name == TStringBuf("ImportUdfs")) {
            Node = L(Node, BuildQuotedAtom(Pos, Name));
            for (ui32 i = 0; i < Values.size(); ++i) {
                Node = L(Node, Values[i].Build());
            }
        }
        else if (Name == TStringBuf("auth")) {
            Node = L(Node, BuildQuotedAtom(Pos, "Auth"));
            Node = L(Node, Values.empty() ? BuildQuotedAtom(Pos, TString()) : Values.front().Build());
        }
        else {
            Node = L(Node, BuildQuotedAtom(Pos, "Attr"));
            Node = L(Node, BuildQuotedAtom(Pos, Name));
            if (!ValueDefault) {
                Node = L(Node, Values.empty() ? BuildQuotedAtom(Pos, TString()) : Values.front().Build());
            }
        }

        ctx.PushBlockShortcuts();
        if (!Node->Init(ctx, FakeSource.Get())) {
            return false;
        }

        Node = ctx.GroundBlockShortcutsForExpr(Node);
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node->Translate(ctx);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TString Prefix;
    TString Name;
    TVector<TDeferredAtom> Values;
    bool ValueDefault;
    TNodePtr Node;
    TSourcePtr FakeSource;
};

TNodePtr BuildPragma(TPosition pos, const TString& prefix, const TString& name, const TVector<TDeferredAtom>& values, bool valueDefault) {
    return new TPragmaNode(pos, prefix, name, values, valueDefault);
}

class TSqlLambda final: public TAstListNode {
public:
    TSqlLambda(TPosition pos, TVector<TString>&& args, TVector<TNodePtr>&& exprSeq)
        : TAstListNode(pos)
        , Args(args)
        , ExprSeq(exprSeq)
    {
        FakeSource = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        for (auto& exprPtr: ExprSeq) {
            ctx.PushBlockShortcuts();
            if (!exprPtr->Init(ctx, FakeSource.Get())) {
                return {};
            }
            const auto label = exprPtr->GetLabel();
            exprPtr = ctx.GroundBlockShortcutsForExpr(exprPtr);
            exprPtr->SetLabel(label);
        }
        YQL_ENSURE(!ExprSeq.empty());
        auto body = Y();
        auto end = ExprSeq.end() - 1;
        for (auto iter = ExprSeq.begin(); iter != end; ++iter) {
            auto exprPtr = *iter;
            const auto& label = exprPtr->GetLabel();
            YQL_ENSURE(label);
            body = L(body, Y("let", label, exprPtr));
        }
        body = Y("block", Q(L(body, Y("return", *end))));
        auto args = Y();
        for (const auto& arg: Args) {
            args = L(args, BuildAtom(GetPos(), arg, NYql::TNodeFlags::Default));
        }
        Add("lambda", Q(args), body);
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const);
    }

private:
    TVector<TString> Args;
    TVector<TNodePtr> ExprSeq;
    TSourcePtr FakeSource;
};

TNodePtr BuildSqlLambda(TPosition pos, TVector<TString>&& args, TVector<TNodePtr>&& exprSeq) {
    return new TSqlLambda(pos, std::move(args), std::move(exprSeq));
}

class TEvaluateIf final : public TAstListNode {
public:
    TEvaluateIf(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode)
        : TAstListNode(pos)
        , Predicate(predicate)
        , ThenNode(thenNode)
        , ElseNode(elseNode)
    {
        FakeSource = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        ctx.PushBlockShortcuts();
        if (!Predicate->Init(ctx, FakeSource.Get())) {
            return{};
        }
        auto predicate = ctx.GroundBlockShortcutsForExpr(Predicate);
        Add("EvaluateIf!");
        Add("world");
        Add(Y("EvaluateExpr", Y("EnsureType", Y("Coalesce", predicate, Y("Bool", Q("false"))), Y("DataType", Q("Bool")))));

        ctx.PushBlockShortcuts();
        if (!ThenNode->Init(ctx, FakeSource.Get())) {
            return{};
        }

        auto thenNode = ctx.GroundBlockShortcutsForExpr(ThenNode);
        Add(thenNode);
        if (ElseNode) {
            ctx.PushBlockShortcuts();
            if (!ElseNode->Init(ctx, FakeSource.Get())) {
                return{};
            }

            auto elseNode = ctx.GroundBlockShortcutsForExpr(ElseNode);
            Add(elseNode);
        }

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TNodePtr Predicate;
    TNodePtr ThenNode;
    TNodePtr ElseNode;
    TSourcePtr FakeSource;
};

TNodePtr BuildEvaluateIfNode(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode) {
    return new TEvaluateIf(pos, predicate, thenNode, elseNode);
}

class TEvaluateFor final : public TAstListNode {
public:
    TEvaluateFor(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode)
        : TAstListNode(pos)
        , List(list)
        , BodyNode(bodyNode)
        , ElseNode(elseNode)
    {
        FakeSource = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        ctx.PushBlockShortcuts();
        if (!List->Init(ctx, FakeSource.Get())) {
            return{};
        }
        auto list = ctx.GroundBlockShortcutsForExpr(List);
        Add("EvaluateFor!");
        Add("world");
        Add(Y("EvaluateExpr", list));
        ctx.PushBlockShortcuts();
        if (!BodyNode->Init(ctx, FakeSource.Get())) {
            return{};
        }

        auto bodyNode = ctx.GroundBlockShortcutsForExpr(BodyNode);
        Add(bodyNode);
        if (ElseNode) {
            ctx.PushBlockShortcuts();
            if (!ElseNode->Init(ctx, FakeSource.Get())) {
                return{};
            }

            auto elseNode = ctx.GroundBlockShortcutsForExpr(ElseNode);
            Add(elseNode);
        }

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return{};
    }

private:
    TNodePtr List;
    TNodePtr BodyNode;
    TNodePtr ElseNode;
    TSourcePtr FakeSource;
};

TNodePtr BuildEvaluateForNode(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode) {
    return new TEvaluateFor(pos, list, bodyNode, elseNode);
}
} // namespace NSQLTranslationV0
