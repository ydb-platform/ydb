#include "node.h"
#include "context.h"
#include "object_processing.h"

#include <ydb/library/yql/ast/yql_type_string.h>
#include <ydb/library/yql/core/yql_callable_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <library/cpp/charset/ci_string.h>

#include <util/digest/fnv.h>

using namespace NYql;

namespace NSQLTranslationV1 {

bool ValidateView(TPosition pos, TContext& ctx, TStringBuf service, TViewDescription& view) {
    if (view.PrimaryFlag && !(service == KikimrProviderName || service == YdbProviderName)) {
        ctx.Error(pos) << "primary view is not supported for " << service << " tables";
        return false;
    }
    return true;
}

class TUniqueTableKey: public ITableKeys {
public:
    TUniqueTableKey(TPosition pos, const TString& service, const TDeferredAtom& cluster,
        const TDeferredAtom& name, const TViewDescription& view)
        : ITableKeys(pos)
        , Service(service)
        , Cluster(cluster)
        , Name(name)
        , View(view)
        , Full(name.GetRepr())
    {
        if (!View.ViewName.empty()) {
            Full += ":" + View.ViewName;
        }
    }

    bool SetPrimaryView(TContext& ctx, TPosition pos) override {
        Y_UNUSED(ctx);
        Y_UNUSED(pos);
        View = {"", true};
        return true;
    }

    bool SetViewName(TContext& ctx, TPosition pos, const TString& view) override {
        Y_UNUSED(ctx);
        Y_UNUSED(pos);
        Full = Name.GetRepr();
        View = {view};
        if (!View.empty()) {
            Full = ":" + View.ViewName;
        }

        return true;
    }

    const TString* GetTableName() const override {
        return Name.GetLiteral() ? &Full : nullptr;
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode mode) override {
        if (View == TViewDescription{"@"}) {
            auto key = Y("TempTable", Name.Build());
            return key;
        }

        bool tableScheme = mode == ITableKeys::EBuildKeysMode::CREATE;
        if (tableScheme && !View.empty()) {
            ctx.Error(Pos) << "Table view can not be created with CREATE TABLE clause";
            return nullptr;
        }
        auto path = ctx.GetPrefixedPath(Service, Cluster, Name);
        if (!path) {
            return nullptr;
        }
        auto key = Y("Key", Q(Y(Q(tableScheme ? "tablescheme" : "table"), Y("String", path))));
        key = AddView(key, View);
        if (!ValidateView(GetPos(), ctx, Service, View)) {
            return nullptr;
        }
        if (mode == ITableKeys::EBuildKeysMode::INPUT &&
            IsQueryMode(ctx.Settings.Mode) &&
            Service != KikimrProviderName &&
            Service != RtmrProviderName &&
            Service != YdbProviderName) {

            key = Y("MrTableConcat", key);
        }
        return key;
    }

private:
    TString Service;
    TDeferredAtom Cluster;
    TDeferredAtom Name;
    TViewDescription View;
    TString Full;
};

TNodePtr BuildTableKey(TPosition pos, const TString& service, const TDeferredAtom& cluster,
    const TDeferredAtom& name, const TViewDescription& view) {
    return new TUniqueTableKey(pos, service, cluster, name, view);
}

class TTopicKey: public ITableKeys {
public:
    TTopicKey(TPosition pos, const TDeferredAtom& cluster, const TDeferredAtom& name)
        : ITableKeys(pos)
        , Cluster(cluster)
        , Name(name)
        , Full(name.GetRepr())
    {
    }

    const TString* GetTableName() const override {
        return Name.GetLiteral() ? &Full : nullptr;
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode) override {
        const auto path = ctx.GetPrefixedPath(Service, Cluster, Name);
        if (!path) {
            return nullptr;
        }
        auto key = Y("Key", Q(Y(Q("topic"), Y("String", path))));
        return key;
    }

private:
    TString Service;
    TDeferredAtom Cluster;
    TDeferredAtom Name;
    TString View;
    TString Full;
};

TNodePtr BuildTopicKey(TPosition pos, const TDeferredAtom& cluster, const TDeferredAtom& name) {
    return new TTopicKey(pos, cluster, name);
}

static INode::TPtr CreateIndexType(TIndexDescription::EType type, const INode& node) {
    switch (type) {
        case TIndexDescription::EType::GlobalSync:
            return node.Q("syncGlobal");
        case TIndexDescription::EType::GlobalAsync:
            return node.Q("asyncGlobal");
        case TIndexDescription::EType::GlobalSyncUnique:
            return node.Q("syncGlobalUnique");
    }
}

enum class ETableSettingsParsingMode {
    Create,
    Alter
};

static INode::TPtr CreateTableSettings(const TTableSettings& tableSettings, ETableSettingsParsingMode parsingMode, const INode& node) {
    // short aliases for member function calls
    auto Y = [&node](auto&&... args) { return node.Y(std::forward<decltype(args)>(args)...); };
    auto Q = [&node](auto&&... args) { return node.Q(std::forward<decltype(args)>(args)...); };
    auto L = [&node](auto&&... args) { return node.L(std::forward<decltype(args)>(args)...); };

    auto settings = Y();

    if (tableSettings.DataSourcePath) {
        settings = L(settings, Q(Y(Q("data_source_path"), tableSettings.DataSourcePath)));
    }
    if (tableSettings.Location) {
        if (tableSettings.Location.IsSet()) {
            settings = L(settings, Q(Y(Q("location"), tableSettings.Location.GetValueSet())));
        } else {
            Y_ENSURE(parsingMode != ETableSettingsParsingMode::Create, "Can't reset LOCATION in create mode");
            settings = L(settings, Q(Y(Q("location"))));
        }
    }
    for (const auto& resetableParam : tableSettings.ExternalSourceParameters) {
        Y_ENSURE(resetableParam, "Empty parameter");
        if (resetableParam.IsSet()) {
            const auto& [id, value] = resetableParam.GetValueSet();
            settings = L(settings, Q(Y(Q(id.Name), value)));
        } else {
            Y_ENSURE(parsingMode != ETableSettingsParsingMode::Create,
                     "Can't reset " << resetableParam.GetValueReset().Name << " in create mode"
            );
            settings = L(settings, Q(Y(Q(resetableParam.GetValueReset().Name))));
        }
    }
    if (tableSettings.CompactionPolicy) {
        settings = L(settings, Q(Y(Q("compactionPolicy"), tableSettings.CompactionPolicy)));
    }
    if (tableSettings.AutoPartitioningBySize) {
        const auto& ref = tableSettings.AutoPartitioningBySize.GetRef();
        settings = L(settings, Q(Y(Q("autoPartitioningBySize"), BuildQuotedAtom(ref.Pos, ref.Name))));
    }
    if (tableSettings.UniformPartitions && parsingMode == ETableSettingsParsingMode::Create) {
        settings = L(settings, Q(Y(Q("uniformPartitions"), tableSettings.UniformPartitions)));
    }
    if (tableSettings.PartitionAtKeys && parsingMode == ETableSettingsParsingMode::Create) {
        auto keysDesc = Y();
        for (const auto& key : tableSettings.PartitionAtKeys) {
            auto columnsDesc = Y();
            for (auto column : key) {
                columnsDesc = L(columnsDesc, column);
            }
            keysDesc = L(keysDesc, Q(columnsDesc));
        }
        settings = L(settings, Q(Y(Q("partitionAtKeys"), Q(keysDesc))));
    }
    if (tableSettings.PartitionSizeMb) {
        settings = L(settings, Q(Y(Q("partitionSizeMb"), tableSettings.PartitionSizeMb)));
    }
    if (tableSettings.AutoPartitioningByLoad) {
        const auto& ref = tableSettings.AutoPartitioningByLoad.GetRef();
        settings = L(settings, Q(Y(Q("autoPartitioningByLoad"), BuildQuotedAtom(ref.Pos, ref.Name))));
    }
    if (tableSettings.MinPartitions) {
        settings = L(settings, Q(Y(Q("minPartitions"), tableSettings.MinPartitions)));
    }
    if (tableSettings.MaxPartitions) {
        settings = L(settings, Q(Y(Q("maxPartitions"), tableSettings.MaxPartitions)));
    }
    if (tableSettings.KeyBloomFilter) {
        const auto& ref = tableSettings.KeyBloomFilter.GetRef();
        settings = L(settings, Q(Y(Q("keyBloomFilter"), BuildQuotedAtom(ref.Pos, ref.Name))));
    }
    if (tableSettings.ReadReplicasSettings) {
        settings = L(settings, Q(Y(Q("readReplicasSettings"), tableSettings.ReadReplicasSettings)));
    }
    if (const auto& ttl = tableSettings.TtlSettings) {
        if (ttl.IsSet()) {
            const auto& ttlSettings = ttl.GetValueSet();
            auto opts = Y();

            opts = L(opts, Q(Y(Q("columnName"), BuildQuotedAtom(ttlSettings.ColumnName.Pos, ttlSettings.ColumnName.Name))));
            opts = L(opts, Q(Y(Q("expireAfter"), ttlSettings.Expr)));

            if (ttlSettings.ColumnUnit) {
                opts = L(opts, Q(Y(Q("columnUnit"), Q(ToString(*ttlSettings.ColumnUnit)))));
            }

            settings = L(settings, Q(Y(Q("setTtlSettings"), Q(opts))));
        } else {
            YQL_ENSURE(parsingMode != ETableSettingsParsingMode::Create, "Can't reset TTL settings in create mode");
            settings = L(settings, Q(Y(Q("resetTtlSettings"), Q(Y()))));
        }
    }
    if (const auto& tiering = tableSettings.Tiering) {
        if (tiering.IsSet()) {
            settings = L(settings, Q(Y(Q("setTiering"), tiering.GetValueSet())));
        } else {
            YQL_ENSURE(parsingMode != ETableSettingsParsingMode::Create, "Can't reset TIERING in create mode");
            settings = L(settings, Q(Y(Q("resetTiering"), Q(Y()))));
        }
    }
    if (tableSettings.StoreExternalBlobs) {
        const auto& ref = tableSettings.StoreExternalBlobs.GetRef();
        settings = L(settings, Q(Y(Q("storeExternalBlobs"), BuildQuotedAtom(ref.Pos, ref.Name))));
    }
    if (tableSettings.StoreType && parsingMode == ETableSettingsParsingMode::Create) {
        const auto& ref = tableSettings.StoreType.GetRef();
        settings = L(settings, Q(Y(Q("storeType"), BuildQuotedAtom(ref.Pos, ref.Name))));
    }
    if (tableSettings.PartitionByHashFunction && parsingMode == ETableSettingsParsingMode::Create) {
        settings = L(settings, Q(Y(Q("partitionByHashFunction"), tableSettings.PartitionByHashFunction)));
    }

    return settings;
}

static INode::TPtr CreateIndexDesc(const TIndexDescription& index, ETableSettingsParsingMode parsingMode, const INode& node) {
    auto indexColumns = node.Y();
    for (const auto& col : index.IndexColumns) {
        indexColumns = node.L(indexColumns, BuildQuotedAtom(col.Pos, col.Name));
    }
    auto dataColumns = node.Y();
    for (const auto& col : index.DataColumns) {
        dataColumns = node.L(dataColumns, BuildQuotedAtom(col.Pos, col.Name));
    }
    const auto& indexType = node.Y(node.Q("indexType"), CreateIndexType(index.Type, node));
    const auto& indexName = node.Y(node.Q("indexName"), BuildQuotedAtom(index.Name.Pos, index.Name.Name));
    auto indexNode = node.Y(
        node.Q(indexName),
        node.Q(indexType),
        node.Q(node.Y(node.Q("indexColumns"), node.Q(indexColumns))),
        node.Q(node.Y(node.Q("dataColumns"), node.Q(dataColumns)))
    );
    if (index.TableSettings.IsSet()) {
        const auto& tableSettings = node.Y(
            node.Q("tableSettings"),
            node.Q(CreateTableSettings(index.TableSettings, parsingMode, node))
        );
        indexNode = node.L(indexNode, tableSettings);
    }
    return indexNode;
}

static INode::TPtr CreateAlterIndex(const TIndexDescription& index, const INode& node) {
    const auto& indexName = node.Y(node.Q("indexName"), BuildQuotedAtom(index.Name.Pos, index.Name.Name));
    const auto& tableSettings = node.Y(
        node.Q("tableSettings"),
        node.Q(CreateTableSettings(index.TableSettings, ETableSettingsParsingMode::Alter, node))
    );
    return node.Y(
        node.Q(indexName),
        node.Q(tableSettings)
    );
}

static INode::TPtr CreateChangefeedDesc(const TChangefeedDescription& desc, const INode& node) {
    auto settings = node.Y();
    if (desc.Settings.Mode) {
        settings = node.L(settings, node.Q(node.Y(node.Q("mode"), desc.Settings.Mode)));
    }
    if (desc.Settings.Format) {
        settings = node.L(settings, node.Q(node.Y(node.Q("format"), desc.Settings.Format)));
    }
    if (desc.Settings.InitialScan) {
        settings = node.L(settings, node.Q(node.Y(node.Q("initial_scan"), desc.Settings.InitialScan)));
    }
    if (desc.Settings.VirtualTimestamps) {
        settings = node.L(settings, node.Q(node.Y(node.Q("virtual_timestamps"), desc.Settings.VirtualTimestamps)));
    }
    if (desc.Settings.ResolvedTimestamps) {
        settings = node.L(settings, node.Q(node.Y(node.Q("resolved_timestamps"), desc.Settings.ResolvedTimestamps)));
    }
    if (desc.Settings.RetentionPeriod) {
        settings = node.L(settings, node.Q(node.Y(node.Q("retention_period"), desc.Settings.RetentionPeriod)));
    }
    if (desc.Settings.TopicPartitions) {
        settings = node.L(settings, node.Q(node.Y(node.Q("topic_min_active_partitions"), desc.Settings.TopicPartitions)));
    }
    if (desc.Settings.AwsRegion) {
        settings = node.L(settings, node.Q(node.Y(node.Q("aws_region"), desc.Settings.AwsRegion)));
    }
    if (const auto& sink = desc.Settings.SinkSettings) {
        switch (sink->index()) {
            case 0: // local
                settings = node.L(settings, node.Q(node.Y(node.Q("local"), node.Q(node.Y()))));
                break;
            default:
                YQL_ENSURE(false, "Unexpected sink settings");
        }
    }

    auto state = node.Y();
    if (desc.Disable) {
        state = node.Q("disable");
    }

    return node.Y(
        node.Q(node.Y(node.Q("name"), BuildQuotedAtom(desc.Name.Pos, desc.Name.Name))),
        node.Q(node.Y(node.Q("settings"), node.Q(settings))),
        node.Q(node.Y(node.Q("state"), node.Q(state)))
    );
}

class TPrepTableKeys: public ITableKeys {
public:
    TPrepTableKeys(TPosition pos, const TString& service, const TDeferredAtom& cluster,
        const TString& func, const TVector<TTableArg>& args)
        : ITableKeys(pos)
        , Service(service)
        , Cluster(cluster)
        , Func(func)
        , Args(args)
    {
    }

    void ExtractTableName(TContext&ctx, TTableArg& arg) {
        MakeTableFromExpression(Pos, ctx, arg.Expr, arg.Id);
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode mode) override {
        if (mode == ITableKeys::EBuildKeysMode::CREATE) {
            // TODO: allow creation of multiple tables
            ctx.Error(Pos) << "Mutiple table creation is not implemented yet";
            return nullptr;
        }

        TCiString func(Func);
        if (func != "object" && func != "walkfolders") {
            for (auto& arg: Args) {
                if (arg.Expr->GetLabel()) {
                    ctx.Error(Pos) << "Named arguments are not supported for table function " << to_upper(Func);
                    return nullptr;
                }
            }
        }
        if (func == "concat_strict") {
            auto tuple = Y();
            for (auto& arg: Args) {
                ExtractTableName(ctx, arg);
                TNodePtr key;
                if (arg.HasAt) {
                    key = Y("TempTable", arg.Id.Build());
                } else {
                    auto path = ctx.GetPrefixedPath(Service, Cluster, arg.Id);
                    if (!path) {
                        return nullptr;
                    }

                    key = Y("Key", Q(Y(Q("table"), Y("String", path))));
                    key = AddView(key, arg.View);
                    if (!ValidateView(GetPos(), ctx, Service, arg.View)) {
                        return nullptr;
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
                    auto path = ctx.GetPrefixedPath(Service, Cluster, arg.Id);
                    if (!path) {
                        return nullptr;
                    }

                    key = Y("Key", Q(Y(Q("table"), Y("String", path))));
                    key = AddView(key, arg.View);
                    if (!ValidateView(GetPos(), ctx, Service, arg.View)) {
                        return nullptr;
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
            if (ctx.DiscoveryMode) {
                ctx.Error(Pos, TIssuesIds::YQL_NOT_ALLOWED_IN_DISCOVERY) << Func << " is not allowed in Discovery mode";
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

            auto path = ctx.GetPrefixedPath(Service, Cluster, Args[0].Id);
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
                    if (!ctx.PragmaRegexUseRe2) {
                        ctx.Warning(Pos, TIssuesIds::CORE_LEGACY_REGEX_ENGINE) << "Legacy regex engine works incorrectly with unicode. Use PRAGMA RegexUseRe2='true';";
                    }

                    auto pattern = Args[1].Id;
                    auto udf = ctx.PragmaRegexUseRe2 ?
                        Y("Udf", Q("Re2.Grep"), Q(Y(Y("String", pattern.Build()), Y("Null")))):
                        Y("Udf", Q("Pcre.BacktrackingGrep"), Y("String", pattern.Build()));
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
                    Y("EnsureType", Y("Coalesce", arg.Expr,
                    Y("List", type)), type)))));

                key = AddView(key, arg.View);
                if (!ValidateView(GetPos(), ctx, Service, arg.View)) {
                    return nullptr;
                }
                each = L(each, key);
            }
            if (ctx.PragmaUseTablePrefixForEach) {
                TStringBuf prefixPath = ctx.GetPrefixPath(Service, Cluster);
                if (prefixPath) {
                    each = L(each, BuildQuotedAtom(Pos, TString(prefixPath)));
                }
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

            if (ctx.DiscoveryMode) {
                ctx.Error(Pos, TIssuesIds::YQL_NOT_ALLOWED_IN_DISCOVERY) << Func << " is not allowed in Discovery mode";
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
        else if (func == "walkfolders") {
            const size_t minPositionalArgs = 1;
            const size_t maxPositionalArgs = 2;

            size_t positionalArgsCnt = 0;
            for (const auto& arg : Args) {
                if (!arg.Expr->GetLabel()) {
                    positionalArgsCnt++;
                } else {
                    break;
                }
            }
            if (positionalArgsCnt < minPositionalArgs || positionalArgsCnt > maxPositionalArgs) {
                ctx.Error(Pos) << Func << " requires from " << minPositionalArgs
                << " to " << maxPositionalArgs
                << " positional arguments, but got: " << positionalArgsCnt;
                return nullptr;
            }

            constexpr auto walkFoldersModuleName = "walk_folders_module";
            ctx.RequiredModules.emplace(walkFoldersModuleName, "/lib/yql/walk_folders.yql");

            auto& rootFolderArg = Args[0];
            if (rootFolderArg.HasAt) {
                ctx.Error(Pos) << "Temporary tables are not supported here";
                return nullptr;
            }
            if (!rootFolderArg.View.empty()) {
                ctx.Error(Pos) << Func << " doesn't supports views";
                return nullptr;
            }
            ExtractTableName(ctx, rootFolderArg);

            const auto initState =
                positionalArgsCnt > 1
                    ? Args[1].Expr
                    : Y("List", Y("ListType", Y("DataType", Q("String"))));

            TNodePtr rootAttributes;
            TNodePtr preHandler;
            TNodePtr resolveHandler;
            TNodePtr diveHandler;
            TNodePtr postHandler;
            for (auto it = Args.begin() + positionalArgsCnt; it != Args.end(); ++it) {
                auto& arg = *it;
                const auto label = arg.Expr->GetLabel();
                if (label == "RootAttributes") {
                    ExtractTableName(ctx, arg);
                    rootAttributes = arg.Id.Build();
                }
                else if (label == "PreHandler") {
                    preHandler = arg.Expr;
                }
                else if (label == "ResolveHandler") {
                    resolveHandler = arg.Expr;
                }
                else if (label == "DiveHandler") {
                    diveHandler = arg.Expr;
                }
                else if (label == "PostHandler") {
                    postHandler = arg.Expr;
                }
                else {
                    ctx.Warning(Pos, DEFAULT_ERROR) << "Unsupported named argument: "
                        << label << " in " << Func;
                }
            }
            if (rootAttributes == nullptr) {
                rootAttributes = BuildQuotedAtom(Pos, "");
            }

            if (preHandler != nullptr || postHandler != nullptr) {
                const auto makePrePostHandlerType = BuildBind(Pos, walkFoldersModuleName, "MakePrePostHandlersType");
                const auto prePostHandlerType = Y("EvaluateType", Y("TypeHandle", Y("Apply", makePrePostHandlerType, Y("TypeOf", initState))));

                if (preHandler != nullptr) {
                    preHandler = Y("Callable", prePostHandlerType, preHandler);
                }
                if (postHandler != nullptr) {
                    postHandler = Y("Callable", prePostHandlerType, postHandler);
                }
            }
            if (preHandler == nullptr) {
                preHandler = Y("Void");
            }
            if (postHandler == nullptr) {
                postHandler = Y("Void");
            }

            const auto makeResolveDiveHandlerType = BuildBind(Pos, walkFoldersModuleName, "MakeResolveDiveHandlersType");
            const auto resolveDiveHandlerType = Y("EvaluateType", Y("TypeHandle", Y("Apply", makeResolveDiveHandlerType, Y("TypeOf", initState))));
            if (resolveHandler == nullptr) {
                resolveHandler = BuildBind(Pos, walkFoldersModuleName, "AnyNodeDiveHandler");
            }
            if (diveHandler == nullptr) {
                diveHandler = BuildBind(Pos, walkFoldersModuleName, "AnyNodeDiveHandler");
            }

            resolveHandler = Y("Callable", resolveDiveHandlerType, resolveHandler);
            diveHandler = Y("Callable", resolveDiveHandlerType, diveHandler);

            const auto initStateType = Y("EvaluateType", Y("TypeHandle", Y("TypeOf", initState)));
            const auto pickledInitState = Y("Pickle", initState);

            const auto initPath = rootFolderArg.Id.Build();

            return Y("MrWalkFolders", initPath, rootAttributes, pickledInitState, initStateType,
                preHandler, resolveHandler, diveHandler, postHandler);
        }
        else if (func == "tables") {
            if (!Args.empty()) {
                ctx.Error(Pos) << Func << " doesn't accept arguments";
                return nullptr;
            }

            return L(Y("DataTables"));
        }
        else if (func == "object") {
            const size_t positionalArgs = 2;
            auto result = Y("MrObject");
            auto settings = Y();
            //TVector<TNodePtr> settings;
            size_t argc = 0;
            for (ui32 index = 0; index < Args.size(); ++index) {
                auto& arg = Args[index];
                if (arg.HasAt) {
                    ctx.Error(arg.Expr->GetPos()) << "Temporary tables are not supported here";
                    return nullptr;
                }

                if (!arg.View.empty()) {
                    ctx.Error(Pos) << to_upper(Func) << " doesn't supports views";
                    return nullptr;
                }

                if (!arg.Expr->GetLabel()) {
                    ExtractTableName(ctx, arg);
                    result = L(result, arg.Id.Build());
                    ++argc;
                } else {
                    settings = L(settings, Q(Y(BuildQuotedAtom(arg.Expr->GetPos(), arg.Expr->GetLabel()), arg.Expr)));
                }
            }

            if (argc != positionalArgs) {
                ctx.Error(Pos) << to_upper(Func) << " requires exacty " << positionalArgs << " positional args, but got " << argc;
                return nullptr;
            }

            result = L(result, Q(settings));
            return result;
        }

        ctx.Error(Pos) << "Unknown table name preprocessor: " << Func;
        return nullptr;
    }

private:
    TString Service;
    TDeferredAtom Cluster;
    TString Func;
    TVector<TTableArg> Args;
};

TNodePtr BuildTableKeys(TPosition pos, const TString& service, const TDeferredAtom& cluster,
    const TString& func, const TVector<TTableArg>& args) {
    return new TPrepTableKeys(pos, service, cluster, func, args);
}

class TInputOptions final: public TAstListNode {
public:
    TInputOptions(TPosition pos, const TTableHints& hints)
        : TAstListNode(pos)
        , Hints(hints)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        for (auto& hint: Hints) {
            TString hintName = hint.first;
            TMaybe<TIssue> normalizeError = NormalizeName(Pos, hintName);
            if (!normalizeError.Empty()) {
                ctx.Error() << normalizeError->GetMessage();
                ctx.IncrementMonCounter("sql_errors", "NormalizeHintError");
                return false;
            }
            TNodePtr option = Y(BuildQuotedAtom(Pos, hintName));
            for (auto& x : hint.second) {
                if (!x->Init(ctx, src)) {
                    return false;
                }

                option = L(option, x);
            }

            Nodes.push_back(Q(option));
        }
        return true;
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TTableHints Hints;
};

TNodePtr BuildInputOptions(TPosition pos, const TTableHints& hints) {
    if (hints.empty()) {
        return nullptr;
    }

    return new TInputOptions(pos, hints);
}

class TIntoTableOptions: public TAstListNode {
public:
    TIntoTableOptions(TPosition pos, const TVector<TString>& columns, const TTableHints& hints)
        : TAstListNode(pos)
        , Columns(columns)
        , Hints(hints)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(ctx);
        Y_UNUSED(src);

        TNodePtr options = Y();
        for (const auto& column: Columns) {
            options->Add(Q(column));
        }
        if (Columns) {
            Add(Q(Y(Q("erase_columns"), Q(options))));
        }

        for (const auto& hint : Hints) {
            TString hintName = hint.first;
            TMaybe<TIssue> normalizeError = NormalizeName(Pos, hintName);
            if (!normalizeError.Empty()) {
                ctx.Error() << normalizeError->GetMessage();
                ctx.IncrementMonCounter("sql_errors", "NormalizeHintError");
                return false;
            }
            TNodePtr option = Y(BuildQuotedAtom(Pos, hintName));
            for (auto& x : hint.second) {
                if (!x->Init(ctx, src)) {
                    return false;
                }
                option = L(option, x);
            }
            Add(Q(option));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TIntoTableOptions(GetPos(), Columns, CloneContainer(Hints));
    }

private:
    TVector<TString> Columns;
    TTableHints Hints;
};

TNodePtr BuildIntoTableOptions(TPosition pos, const TVector<TString>& eraseColumns, const TTableHints& hints) {
    return new TIntoTableOptions(pos, eraseColumns, hints);
}

class TInputTablesNode final: public TAstListNode {
public:
    TInputTablesNode(TPosition pos, const TTableList& tables, bool inSubquery, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Tables(tables)
        , InSubquery(inSubquery)
        , Scoped(scoped)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        THashSet<TString> processedTables;
        for (auto& tr: Tables) {
            if (!processedTables.insert(tr.RefName).second) {
                continue;
            }

            Scoped->UseCluster(tr.Service, tr.Cluster);
            auto tableKeys = tr.Keys->GetTableKeys();
            auto keys = tableKeys->BuildKeys(ctx, ITableKeys::EBuildKeysMode::INPUT);
            if (!keys || !keys->Init(ctx, src)) {
                return false;
            }
            auto fields = Y("Void");
            auto source = Y("DataSource", BuildQuotedAtom(Pos, tr.Service), Scoped->WrapCluster(tr.Cluster, ctx));
            auto options = tr.Options ? Q(tr.Options) : Q(Y());
            Add(Y("let", "x", keys->Y(TString(ReadName), "world", source, keys, fields, options)));

            if (tr.Service != YtProviderName && InSubquery) {
                ctx.Error() << "Using of system '" << tr.Service << "' is not allowed in SUBQUERY";
                return false;
            }

            if (tr.Service != YtProviderName || ctx.Settings.SaveWorldDependencies) {
                Add(Y("let", "world", Y(TString(LeftName), "x")));
            }

            Add(Y("let", tr.RefName, Y(TString(RightName), "x")));
        }
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TTableList Tables;
    const bool InSubquery;
    TScopedStatePtr Scoped;
};

TNodePtr BuildInputTables(TPosition pos, const TTableList& tables, bool inSubquery, TScopedStatePtr scoped) {
    return new TInputTablesNode(pos, tables, inSubquery, scoped);
}

class TCreateTableNode final: public TAstListNode {
public:
    TCreateTableNode(TPosition pos, const TTableRef& tr, bool existingOk, bool replaceIfExists, const TCreateTableParameters& params, TSourcePtr values, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Table(tr)
        , Params(params)
        , ExistingOk(existingOk)
        , ReplaceIfExists(replaceIfExists)
        , Values(std::move(values))
        , Scoped(scoped)
    {
        scoped->UseCluster(Table.Service, Table.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Table.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        if (!Params.PkColumns.empty()
            || !Params.PartitionByColumns.empty()
            || !Params.OrderByColumns.empty()
            || !Params.Indexes.empty()
            || !Params.Changefeeds.empty())
        {
            THashSet<TString> columnsSet;
            for (auto& col : Params.Columns) {
                columnsSet.insert(col.Name);
            }

            const bool allowUndefinedColumns = (Values != nullptr) && columnsSet.empty();

            THashSet<TString> pkColumns;
            for (auto& keyColumn : Params.PkColumns) {
                if (!allowUndefinedColumns && !columnsSet.contains(keyColumn.Name)) {
                    ctx.Error(keyColumn.Pos) << "Undefined column: " << keyColumn.Name;
                    return false;
                }
                if (!pkColumns.insert(keyColumn.Name).second) {
                    ctx.Error(keyColumn.Pos) << "Duplicated column in PK: " << keyColumn.Name;
                    return false;
                }
            }
            for (auto& keyColumn : Params.PartitionByColumns) {
                if (!allowUndefinedColumns && !columnsSet.contains(keyColumn.Name)) {
                    ctx.Error(keyColumn.Pos) << "Undefined column: " << keyColumn.Name;
                    return false;
                }
            }
            for (auto& keyColumn : Params.OrderByColumns) {
                if (!allowUndefinedColumns && !columnsSet.contains(keyColumn.first.Name)) {
                    ctx.Error(keyColumn.first.Pos) << "Undefined column: " << keyColumn.first.Name;
                    return false;
                }
            }

            THashSet<TString> indexNames;
            for (const auto& index : Params.Indexes) {
                if (!indexNames.insert(index.Name.Name).second) {
                    ctx.Error(index.Name.Pos) << "Index " << index.Name.Name << " must be defined once";
                    return false;
                }

                for (const auto& indexColumn : index.IndexColumns) {
                    if (!allowUndefinedColumns && !columnsSet.contains(indexColumn.Name)) {
                        ctx.Error(indexColumn.Pos) << "Undefined column: " << indexColumn.Name;
                        return false;
                    }
                }

                for (const auto& dataColumn : index.DataColumns) {
                    if (!allowUndefinedColumns && !columnsSet.contains(dataColumn.Name)) {
                        ctx.Error(dataColumn.Pos) << "Undefined column: " << dataColumn.Name;
                        return false;
                    }
                }
            }

            THashSet<TString> cfNames;
            for (const auto& cf : Params.Changefeeds) {
                if (!cfNames.insert(cf.Name.Name).second) {
                    ctx.Error(cf.Name.Pos) << "Changefeed " << cf.Name.Name << " must be defined once";
                    return false;
                }
            }
        }

        auto opts = Y();
        if (Table.Options) {
            if (!Table.Options->Init(ctx, src)) {
                return false;
            }
            opts = Table.Options;
        }

        if (ExistingOk) {
          opts = L(opts, Q(Y(Q("mode"), Q("create_if_not_exists"))));
        } else if (ReplaceIfExists) {
          opts = L(opts, Q(Y(Q("mode"), Q("create_or_replace"))));
        } else {
          opts = L(opts, Q(Y(Q("mode"), Q("create"))));
        }

        THashSet<TString> columnFamilyNames;

        if (Params.ColumnFamilies) {
            auto columnFamilies = Y();
            for (const auto& family : Params.ColumnFamilies) {
                if (!columnFamilyNames.insert(family.Name.Name).second) {
                    ctx.Error(family.Name.Pos) << "Family " << family.Name.Name << " specified more than once";
                    return false;
                }
                auto familyDesc = Y();
                familyDesc = L(familyDesc, Q(Y(Q("name"), BuildQuotedAtom(family.Name.Pos, family.Name.Name))));
                if (family.Data) {
                    familyDesc = L(familyDesc, Q(Y(Q("data"), family.Data)));
                }
                if (family.Compression) {
                    familyDesc = L(familyDesc, Q(Y(Q("compression"), family.Compression)));
                }
                columnFamilies = L(columnFamilies, Q(familyDesc));
            }
            opts = L(opts, Q(Y(Q("columnFamilies"), Q(columnFamilies))));
        }

        auto columns = Y();
        THashSet<TString> columnsWithDefaultValue;
        auto columnsDefaultValueSettings = Y();

        for (auto& col : Params.Columns) {
            auto columnDesc = Y();
            columnDesc = L(columnDesc, BuildQuotedAtom(Pos, col.Name));
            auto type = col.Type;

            if (type) {
                if (col.Nullable) {
                    type = Y("AsOptionalType", type);
                }

                columnDesc = L(columnDesc, type);

                auto columnConstraints = Y();

                if (!col.Nullable) {
                    columnConstraints = L(columnConstraints, Q(Y(Q("not_null"))));
                }

                if (col.Serial) {
                    columnConstraints = L(columnConstraints, Q(Y(Q("serial"))));
                }

                if (col.DefaultExpr) {
                    if (!col.DefaultExpr->Init(ctx, src)) {
                        return false;
                    }

                    columnConstraints = L(columnConstraints, Q(Y(Q("default"), col.DefaultExpr)));
                }

                columnDesc = L(columnDesc, Q(Y(Q("columnConstrains"), Q(columnConstraints))));

                auto familiesDesc = Y();

                if (col.Families) {
                    for (const auto& family : col.Families) {
                        if (columnFamilyNames.find(family.Name) == columnFamilyNames.end()) {
                            ctx.Error(family.Pos) << "Unknown family " << family.Name;
                            return false;
                        }
                        familiesDesc = L(familiesDesc, BuildQuotedAtom(family.Pos, family.Name));
                    }
                }

                columnDesc = L(columnDesc, Q(familiesDesc));
            }

            columns = L(columns, Q(columnDesc));
        }
        opts = L(opts, Q(Y(Q("columns"), Q(columns))));

        if (!columnsWithDefaultValue.empty()) {
            opts = L(opts, Q(Y(Q("columnsDefaultValues"), Q(columnsDefaultValueSettings))));
        }

        if (Table.Service == RtmrProviderName) {
            if (!Params.PkColumns.empty() && !Params.PartitionByColumns.empty()) {
                ctx.Error() << "Only one of PRIMARY KEY or PARTITION BY constraints may be specified";
                return false;
            }
        } else {
            if (!Params.OrderByColumns.empty()) {
                ctx.Error() << "ORDER BY is supported only for " << RtmrProviderName << " provider";
                return false;
            }
        }

        if (!Params.PkColumns.empty()) {
            auto primaryKey = Y();
            for (auto& col : Params.PkColumns) {
                primaryKey = L(primaryKey, BuildQuotedAtom(col.Pos, col.Name));
            }
            opts = L(opts, Q(Y(Q("primarykey"), Q(primaryKey))));
            if (!Params.OrderByColumns.empty()) {
                ctx.Error() << "PRIMARY KEY cannot be used with ORDER BY, use PARTITION BY instead";
                return false;
            }
        }

        if (!Params.PartitionByColumns.empty()) {
            auto partitionBy = Y();
            for (auto& col : Params.PartitionByColumns) {
                partitionBy = L(partitionBy, BuildQuotedAtom(col.Pos, col.Name));
            }
            opts = L(opts, Q(Y(Q("partitionby"), Q(partitionBy))));
        }

        if (!Params.OrderByColumns.empty()) {
            auto orderBy = Y();
            for (auto& col : Params.OrderByColumns) {
                orderBy = L(orderBy, Q(Y(BuildQuotedAtom(col.first.Pos, col.first.Name), col.second ? Q("1") : Q("0"))));
            }
            opts = L(opts, Q(Y(Q("orderby"), Q(orderBy))));
        }

        for (const auto& index : Params.Indexes) {
            const auto& desc = CreateIndexDesc(index, ETableSettingsParsingMode::Create, *this);
            opts = L(opts, Q(Y(Q("index"), Q(desc))));
        }

        for (const auto& cf : Params.Changefeeds) {
            const auto& desc = CreateChangefeedDesc(cf, *this);
            opts = L(opts, Q(Y(Q("changefeed"), Q(desc))));
        }

        if (Params.TableSettings.IsSet()) {
            opts = L(opts, Q(Y(Q("tableSettings"), Q(
                CreateTableSettings(Params.TableSettings, ETableSettingsParsingMode::Create, *this)
            ))));
        }

        switch (Params.TableType) {
            case ETableType::TableStore:
                opts = L(opts, Q(Y(Q("tableType"), Q("tableStore"))));
                break;
            case ETableType::ExternalTable:
                opts = L(opts, Q(Y(Q("tableType"), Q("externalTable"))));
                break;
            case ETableType::Table:
                break;
        }

        if (Params.Temporary) {
            opts = L(opts, Q(Y(Q("temporary"))));
        }

        TNodePtr node = nullptr;
        if (Values) {
            if (!Values->Init(ctx, nullptr)) {
                return false;
            }
            TTableList tableList;
            Values->GetInputTables(tableList);
            auto valuesSource = Values.Get();
            auto values = Values->Build(ctx);
            if (!Values) {
                return false;
            }

            TNodePtr inputTables(BuildInputTables(Pos, tableList, false, Scoped));
            if (!inputTables->Init(ctx, valuesSource)) {
                return false;
            }

            node = inputTables;
            node = L(node, Y("let", "values", values));
        } else {
            node = Y(Y("let", "values", Y("Void")));
        }

        auto write = Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Table.Service), Scoped->WrapCluster(Table.Cluster, ctx))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, "values", Q(opts))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        );

        node = L(node, Y("let", "world", Y("block", Q(write))));
        node = L(node, Y("return", "world"));

        Add("block", Q(node));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    const TTableRef Table;
    const TCreateTableParameters Params;
    const bool ExistingOk;
    const bool ReplaceIfExists;
    const TSourcePtr Values;
    TScopedStatePtr Scoped;
};

TNodePtr BuildCreateTable(TPosition pos, const TTableRef& tr, bool existingOk, bool replaceIfExists, const TCreateTableParameters& params, TSourcePtr values, TScopedStatePtr scoped)
{
    return new TCreateTableNode(pos, tr, existingOk, replaceIfExists, params, std::move(values), scoped);
}

class TAlterTableNode final: public TAstListNode {
public:
    TAlterTableNode(TPosition pos, const TTableRef& tr, const TAlterTableParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Table(tr)
        , Params(params)
        , Scoped(scoped)
    {
        scoped->UseCluster(Table.Service, Table.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Table.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        auto actions = Y();

        if (Params.AddColumns) {
            auto columns = Y();
            for (auto& col : Params.AddColumns) {
                auto columnDesc = Y();
                columnDesc = L(columnDesc, BuildQuotedAtom(Pos, col.Name));
                auto type = col.Type;
                if (col.Nullable) {
                    type = Y("AsOptionalType", type);
                }

                columnDesc = L(columnDesc, type);
                auto columnConstraints = Y();
                if (!col.Nullable) {
                    columnConstraints = L(columnConstraints, Q(Y(Q("not_null"))));
                }

                if (col.Serial) {
                    columnConstraints = L(columnConstraints, Q(Y(Q("serial"))));
                }

                if (col.DefaultExpr) {
                    if (!col.DefaultExpr->Init(ctx, src)) {
                        return false;
                    }

                    columnConstraints = L(columnConstraints, Q(Y(Q("default"), col.DefaultExpr)));
                }

                columnDesc = L(columnDesc, Q(Y(Q("columnConstrains"), Q(columnConstraints))));

                auto familiesDesc = Y();
                for (const auto& family : col.Families) {
                    familiesDesc = L(familiesDesc, BuildQuotedAtom(family.Pos, family.Name));
                }
                columnDesc = L(columnDesc, Q(familiesDesc));

                columns = L(columns, Q(columnDesc));
            }
            actions = L(actions, Q(Y(Q("addColumns"), Q(columns))));
        }

        if (Params.DropColumns) {
            auto columns = Y();
            for (auto& colName : Params.DropColumns) {
                columns = L(columns, BuildQuotedAtom(Pos, colName));
            }
            actions = L(actions, Q(Y(Q("dropColumns"), Q(columns))));
        }

        if (Params.AlterColumns) {
            auto columns = Y();
            for (auto& col : Params.AlterColumns) {
                auto columnDesc = Y();
                columnDesc = L(columnDesc, BuildQuotedAtom(Pos, col.Name));
                auto familiesDesc = Y();
                for (const auto& family : col.Families) {
                    familiesDesc = L(familiesDesc, BuildQuotedAtom(family.Pos, family.Name));
                }

                columnDesc = L(columnDesc, Q(Y(Q("setFamily"), Q(familiesDesc))));
                columns = L(columns, Q(columnDesc));
            }
            actions = L(actions, Q(Y(Q("alterColumns"), Q(columns))));
        }

        if (Params.AddColumnFamilies) {
            auto columnFamilies = Y();
            for (const auto& family : Params.AddColumnFamilies) {
                auto familyDesc = Y();
                familyDesc = L(familyDesc, Q(Y(Q("name"), BuildQuotedAtom(family.Name.Pos, family.Name.Name))));
                if (family.Data) {
                    familyDesc = L(familyDesc, Q(Y(Q("data"), family.Data)));
                }
                if (family.Compression) {
                    familyDesc = L(familyDesc, Q(Y(Q("compression"), family.Compression)));
                }
                columnFamilies = L(columnFamilies, Q(familyDesc));
            }
            actions = L(actions, Q(Y(Q("addColumnFamilies"), Q(columnFamilies))));
        }

        if (Params.AlterColumnFamilies) {
            auto columnFamilies = Y();
            for (const auto& family : Params.AlterColumnFamilies) {
                auto familyDesc = Y();
                familyDesc = L(familyDesc, Q(Y(Q("name"), BuildQuotedAtom(family.Name.Pos, family.Name.Name))));
                if (family.Data) {
                    familyDesc = L(familyDesc, Q(Y(Q("data"), family.Data)));
                }
                if (family.Compression) {
                    familyDesc = L(familyDesc, Q(Y(Q("compression"), family.Compression)));
                }
                columnFamilies = L(columnFamilies, Q(familyDesc));
            }
            actions = L(actions, Q(Y(Q("alterColumnFamilies"), Q(columnFamilies))));
        }

        if (Params.TableSettings.IsSet()) {
            actions = L(actions, Q(Y(Q("setTableSettings"), Q(
                CreateTableSettings(Params.TableSettings, ETableSettingsParsingMode::Alter, *this)
            ))));
        }

        for (const auto& index : Params.AddIndexes) {
            const auto& desc = CreateIndexDesc(index, ETableSettingsParsingMode::Alter, *this);
            actions = L(actions, Q(Y(Q("addIndex"), Q(desc))));
        }

        for (const auto& index : Params.AlterIndexes) {
            const auto& desc = CreateAlterIndex(index, *this);
            actions = L(actions, Q(Y(Q("alterIndex"), Q(desc))));
        }

        for (const auto& id : Params.DropIndexes) {
            auto indexName = BuildQuotedAtom(id.Pos, id.Name);
            actions = L(actions, Q(Y(Q("dropIndex"), indexName)));
        }

        if (Params.RenameIndexTo) {
            auto src = BuildQuotedAtom(Params.RenameIndexTo->first.Pos, Params.RenameIndexTo->first.Name);
            auto dst = BuildQuotedAtom(Params.RenameIndexTo->second.Pos, Params.RenameIndexTo->second.Name);

            auto desc = Y();

            desc = L(desc, Q(Y(Q("src"), src)));
            desc = L(desc, Q(Y(Q("dst"), dst)));

            actions = L(actions, Q(Y(Q("renameIndexTo"), Q(desc))));
        }

        if (Params.RenameTo) {
            auto destination = ctx.GetPrefixedPath(Scoped->CurrService, Scoped->CurrCluster,
                                                   TDeferredAtom(Params.RenameTo->Pos, Params.RenameTo->Name));
            actions = L(actions, Q(Y(Q("renameTo"), destination)));
        }

        for (const auto& cf : Params.AddChangefeeds) {
            const auto& desc = CreateChangefeedDesc(cf, *this);
            actions = L(actions, Q(Y(Q("addChangefeed"), Q(desc))));
        }

        for (const auto& cf : Params.AlterChangefeeds) {
            const auto& desc = CreateChangefeedDesc(cf, *this);
            actions = L(actions, Q(Y(Q("alterChangefeed"), Q(desc))));
        }

        for (const auto& id : Params.DropChangefeeds) {
            const auto name = BuildQuotedAtom(id.Pos, id.Name);
            actions = L(actions, Q(Y(Q("dropChangefeed"), name)));
        }

        auto opts = Y();

        opts = L(opts, Q(Y(Q("mode"), Q("alter"))));
        opts = L(opts, Q(Y(Q("actions"), Q(actions))));

        switch (Params.TableType) {
            case ETableType::TableStore:
                opts = L(opts, Q(Y(Q("tableType"), Q("tableStore"))));
                break;
            case ETableType::ExternalTable:
                opts = L(opts, Q(Y(Q("tableType"), Q("externalTable"))));
                break;
            case ETableType::Table:
                break;
        }

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Table.Service), Scoped->WrapCluster(Table.Cluster, ctx))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        return TAstListNode::DoInit(ctx, src);
    }
    TPtr DoClone() const final {
        return {};
    }
private:
    TTableRef Table;
    const TAlterTableParameters Params;
    TScopedStatePtr Scoped;
};

TNodePtr BuildAlterTable(TPosition pos, const TTableRef& tr, const TAlterTableParameters& params, TScopedStatePtr scoped)
{
    return new TAlterTableNode(pos, tr, params, scoped);
}

class TDropTableNode final: public TAstListNode {
public:
    TDropTableNode(TPosition pos, const TTableRef& tr, bool missingOk, ETableType tableType, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Table(tr)
        , TableType(tableType)
        , Scoped(scoped)
        , MissingOk(missingOk)
    {
        FakeSource = BuildFakeSource(pos);
        scoped->UseCluster(Table.Service, Table.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto keys = Table.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::DROP);
        if (!keys || !keys->Init(ctx, FakeSource.Get())) {
            return false;
        }

        auto opts = Y();

        opts = L(opts, Q(Y(Q("mode"), Q(MissingOk ? "drop_if_exists" : "drop"))));

        switch (TableType) {
            case ETableType::TableStore:
                opts = L(opts, Q(Y(Q("tableType"), Q("tableStore"))));
                break;
            case ETableType::ExternalTable:
                opts = L(opts, Q(Y(Q("tableType"), Q("externalTable"))));
                break;
            case ETableType::Table:
                break;
        }

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Table.Service), Scoped->WrapCluster(Table.Cluster, ctx))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        return TAstListNode::DoInit(ctx, FakeSource.Get());
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TTableRef Table;
    ETableType TableType;
    TScopedStatePtr Scoped;
    TSourcePtr FakeSource;
    const bool MissingOk;
};

TNodePtr BuildDropTable(TPosition pos, const TTableRef& tr, bool missingOk, ETableType tableType, TScopedStatePtr scoped) {
    return new TDropTableNode(pos, tr, missingOk, tableType, scoped);
}


static INode::TPtr CreateConsumerDesc(const TTopicConsumerDescription& desc, const INode& node, bool alter) {
    auto settings = node.Y();
    if (desc.Settings.Important) {
        settings = node.L(settings, node.Q(node.Y(node.Q("important"), desc.Settings.Important)));
    }
    if (const auto& readFromTs = desc.Settings.ReadFromTs) {
        if (readFromTs.IsSet()) {
            settings = node.L(settings, node.Q(node.Y(node.Q("setReadFromTs"), readFromTs.GetValueSet())));
        } else if (alter) {
            settings = node.L(settings, node.Q(node.Y(node.Q("resetReadFromTs"), node.Q(node.Y()))));
        } else {
            YQL_ENSURE(false, "Cannot reset on create");
        }
    }
    if (const auto& readFromTs = desc.Settings.SupportedCodecs) {
        if (readFromTs.IsSet()) {
            settings = node.L(settings, node.Q(node.Y(node.Q("setSupportedCodecs"), readFromTs.GetValueSet())));
        } else if (alter) {
            settings = node.L(settings, node.Q(node.Y(node.Q("resetSupportedCodecs"), node.Q(node.Y()))));
        } else {
            YQL_ENSURE(false, "Cannot reset on create");
        }
    }
    return node.Y(
            node.Q(node.Y(node.Q("name"), BuildQuotedAtom(desc.Name.Pos, desc.Name.Name))),
            node.Q(node.Y(node.Q("settings"), node.Q(settings)))
    );
}

class TCreateTopicNode final: public TAstListNode {
public:
    TCreateTopicNode(TPosition pos, const TTopicRef& tr, const TCreateTopicParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Topic(tr)
        , Params(params)
        , Scoped(scoped)
    {
        scoped->UseCluster(TString(KikimrProviderName), Topic.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Topic.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        if (!Params.Consumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& consumer : Params.Consumers) {
                if (!consumerNames.insert(consumer.Name.Name).second) {
                    ctx.Error(consumer.Name.Pos) << "Consumer " << consumer.Name.Name << " defined more than once";
                    return false;
                }
            }
        }

        auto opts = Y();
        TString mode = Params.ExistingOk ? "create_if_not_exists" : "create";
        opts = L(opts, Q(Y(Q("mode"), Q(mode))));

        for (const auto& consumer : Params.Consumers) {
            const auto& desc = CreateConsumerDesc(consumer, *this, false);
            opts = L(opts, Q(Y(Q("consumer"), Q(desc))));
        }

        if (Params.TopicSettings.IsSet()) {
            auto settings = Y();

#define INSERT_TOPIC_SETTING(NAME)                                                                      \
    if (const auto& NAME##Val = Params.TopicSettings.NAME) {                                            \
        if (NAME##Val.IsSet()) {                                                                        \
            settings = L(settings, Q(Y(Q(Y_STRINGIZE(set##NAME)), NAME##Val.GetValueSet())));           \
        } else {                                                                                        \
            YQL_ENSURE(false, "Can't reset on create");                                                 \
        }                                                                                               \
    }

            INSERT_TOPIC_SETTING(PartitionsLimit)
            INSERT_TOPIC_SETTING(MinPartitions)
            INSERT_TOPIC_SETTING(RetentionPeriod)
            INSERT_TOPIC_SETTING(SupportedCodecs)
            INSERT_TOPIC_SETTING(PartitionWriteSpeed)
            INSERT_TOPIC_SETTING(PartitionWriteBurstSpeed)
            INSERT_TOPIC_SETTING(MeteringMode)

#undef INSERT_TOPIC_SETTING

            opts = L(opts, Q(Y(Q("topicSettings"), Q(settings))));
        }


        Add("block", Q(Y(
                Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, TString(KikimrProviderName)),
                                   Scoped->WrapCluster(Topic.Cluster, ctx))),
                Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    const TTopicRef Topic;
    const TCreateTopicParameters Params;
    TScopedStatePtr Scoped;
};

TNodePtr BuildCreateTopic(
        TPosition pos, const TTopicRef& tr, const TCreateTopicParameters& params, TScopedStatePtr scoped
){
    return new TCreateTopicNode(pos, tr, params, scoped);
}

class TAlterTopicNode final: public TAstListNode {
public:
    TAlterTopicNode(TPosition pos, const TTopicRef& tr, const TAlterTopicParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Topic(tr)
        , Params(params)
        , Scoped(scoped)
    {
        scoped->UseCluster(TString(KikimrProviderName), Topic.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Topic.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        if (!Params.AddConsumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& consumer : Params.AddConsumers) {
                if (!consumerNames.insert(consumer.Name.Name).second) {
                    ctx.Error(consumer.Name.Pos) << "Consumer " << consumer.Name.Name << " defined more than once";
                    return false;
                }
            }
        }
        if (!Params.AlterConsumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& [_, consumer] : Params.AlterConsumers) {
                if (!consumerNames.insert(consumer.Name.Name).second) {
                    ctx.Error(consumer.Name.Pos) << "Consumer " << consumer.Name.Name << " altered more than once";
                    return false;
                }
            }
        }
        if (!Params.DropConsumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& consumer : Params.DropConsumers) {
                if (!consumerNames.insert(consumer.Name).second) {
                    ctx.Error(consumer.Pos) << "Consumer " << consumer.Name << " dropped more than once";
                    return false;
                }
            }
        }

        auto opts = Y();
        TString mode = Params.MissingOk ? "alter_if_exists" : "alter";
        opts = L(opts, Q(Y(Q("mode"), Q(mode))));

        for (const auto& consumer : Params.AddConsumers) {
            const auto& desc = CreateConsumerDesc(consumer, *this, false);
            opts = L(opts, Q(Y(Q("addConsumer"), Q(desc))));
        }

        for (const auto& [_, consumer] : Params.AlterConsumers) {
            const auto& desc = CreateConsumerDesc(consumer, *this, true);
            opts = L(opts, Q(Y(Q("alterConsumer"), Q(desc))));
        }

        for (const auto& consumer : Params.DropConsumers) {
            const auto name = BuildQuotedAtom(consumer.Pos, consumer.Name);
            opts = L(opts, Q(Y(Q("dropConsumer"), name)));
        }

        if (Params.TopicSettings.IsSet()) {
            auto settings = Y();

#define INSERT_TOPIC_SETTING(NAME)                                                                      \
    if (const auto& NAME##Val = Params.TopicSettings.NAME) {                                            \
        if (NAME##Val.IsSet()) {                                                                        \
            settings = L(settings, Q(Y(Q(Y_STRINGIZE(set##NAME)), NAME##Val.GetValueSet())));           \
        } else {                                                                                        \
            settings = L(settings, Q(Y(Q(Y_STRINGIZE(reset##NAME)), Y())));           \
        }                                                                                               \
    }

            INSERT_TOPIC_SETTING(PartitionsLimit)
            INSERT_TOPIC_SETTING(MinPartitions)
            INSERT_TOPIC_SETTING(RetentionPeriod)
            INSERT_TOPIC_SETTING(SupportedCodecs)
            INSERT_TOPIC_SETTING(PartitionWriteSpeed)
            INSERT_TOPIC_SETTING(PartitionWriteBurstSpeed)
            INSERT_TOPIC_SETTING(MeteringMode)

#undef INSERT_TOPIC_SETTING

            opts = L(opts, Q(Y(Q("topicSettings"), Q(settings))));
        }


        Add("block", Q(Y(
                Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, TString(KikimrProviderName)),
                                   Scoped->WrapCluster(Topic.Cluster, ctx))),
                Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    const TTopicRef Topic;
    const TAlterTopicParameters Params;
    TScopedStatePtr Scoped;
};

TNodePtr BuildAlterTopic(
        TPosition pos, const TTopicRef& tr, const TAlterTopicParameters& params, TScopedStatePtr scoped
){
    return new TAlterTopicNode(pos, tr, params, scoped);
}

class TDropTopicNode final: public TAstListNode {
public:
    TDropTopicNode(TPosition pos, const TTopicRef& tr, const TDropTopicParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Topic(tr)
        , Params(params)
        , Scoped(scoped)
    {
        scoped->UseCluster(TString(KikimrProviderName), Topic.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto keys = Topic.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::DROP);
        if (!keys || !keys->Init(ctx, FakeSource.Get())) {
            return false;
        }

        auto opts = Y();

        TString mode = Params.MissingOk ? "drop_if_exists" : "drop";
        opts = L(opts, Q(Y(Q("mode"), Q(mode))));


        Add("block", Q(Y(
                Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, TString(KikimrProviderName)),
                                   Scoped->WrapCluster(Topic.Cluster, ctx))),
                Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        return TAstListNode::DoInit(ctx, FakeSource.Get());
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    TTopicRef Topic;
    TDropTopicParameters Params;
    TScopedStatePtr Scoped;
    TSourcePtr FakeSource;
};

TNodePtr BuildDropTopic(TPosition pos, const TTopicRef& tr, const TDropTopicParameters& params, TScopedStatePtr scoped) {
    return new TDropTopicNode(pos, tr, params, scoped);
}

class TCreateRole final: public TAstListNode {
public:
    TCreateRole(TPosition pos, bool isUser, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TRoleParameters>& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , IsUser(isUser)
        , Service(service)
        , Cluster(cluster)
        , Name(name)
        , Params(params)
        , Scoped(scoped)
    {
        FakeSource = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto name = Name.Build();
        TNodePtr password;
        if (Params && Params->Password) {
            password = Params->Password->Build();
        }
        TNodePtr cluster = Scoped->WrapCluster(Cluster, ctx);

        if (!name->Init(ctx, FakeSource.Get()) || !cluster->Init(ctx, FakeSource.Get())) {
            return false;
        }
        if (password && !password->Init(ctx, FakeSource.Get())) {
            return false;
        }

        TVector<TNodePtr> roles;
        if (Params && !Params->Roles.empty()) {
            for (auto& item : Params->Roles) {
                roles.push_back(item.Build());
                if (!roles.back()->Init(ctx, FakeSource.Get())) {
                    return false;
                }
            }
        }


        auto options = Y(Q(Y(Q("mode"), Q(IsUser ? "createUser" : "createGroup"))));
        if (Params) {
            if (Params->IsPasswordEncrypted) {
                options = L(options, Q(Y(Q("passwordEncrypted"))));
            }
            if (Params->Password) {
                options = L(options, Q(Y(Q("password"), password)));
            } else {
                options = L(options, Q(Y(Q("nullPassword"))));
            }
            if (!Params->Roles.empty()) {
                options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos, std::move(roles))))));
            }
        }

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Service), cluster)),
            Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
            )));

        return TAstListNode::DoInit(ctx, FakeSource.Get());
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    const bool IsUser;
    const TString Service;
    TDeferredAtom Cluster;
    TDeferredAtom Name;
    const TMaybe<TRoleParameters> Params;
    TScopedStatePtr Scoped;
    TSourcePtr FakeSource;
};

TNodePtr BuildCreateUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TRoleParameters>& params, TScopedStatePtr scoped) {
    bool isUser = true;
    return new TCreateRole(pos, isUser, service, cluster, name, params, scoped);
}

TNodePtr BuildCreateGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TRoleParameters>& params, TScopedStatePtr scoped) {
    bool isUser = false;
    return new TCreateRole(pos, isUser, service, cluster, name, params, scoped);
}

class TAlterUser final: public TAstListNode {
public:
    TAlterUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TRoleParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service(service)
        , Cluster(cluster)
        , Name(name)
        , Params(params)
        , Scoped(scoped)
    {
        FakeSource = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto name = Name.Build();
        TNodePtr password;
        if (Params.Password) {
            password = Params.Password->Build();
        }
        TNodePtr cluster = Scoped->WrapCluster(Cluster, ctx);

        if (!name->Init(ctx, FakeSource.Get()) || !cluster->Init(ctx, FakeSource.Get())) {
            return false;
        }
        if (password && !password->Init(ctx, FakeSource.Get())) {
            return false;
        }

        auto options = Y(Q(Y(Q("mode"), Q("alterUser"))));
        if (Params.IsPasswordEncrypted) {
            options = L(options, Q(Y(Q("passwordEncrypted"))));
        }
        if (Params.Password) {
            options = L(options, Q(Y(Q("password"), password)));
        } else {
            options = L(options, Q(Y(Q("nullPassword"))));
        }

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Service), cluster)),
            Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
            )));

        return TAstListNode::DoInit(ctx, FakeSource.Get());
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    const TString Service;
    TDeferredAtom Cluster;
    TDeferredAtom Name;
    const TRoleParameters Params;
    TScopedStatePtr Scoped;
    TSourcePtr FakeSource;
};

TNodePtr BuildAlterUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TRoleParameters& params, TScopedStatePtr scoped) {
    return new TAlterUser(pos, service, cluster, name, params, scoped);
}

class TRenameRole final: public TAstListNode {
public:
    TRenameRole(TPosition pos, bool isUser, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , IsUser(isUser)
        , Service(service)
        , Cluster(cluster)
        , Name(name)
        , NewName(newName)
        , Scoped(scoped)
    {
        FakeSource = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto name = Name.Build();
        auto newName = NewName.Build();
        TNodePtr cluster = Scoped->WrapCluster(Cluster, ctx);

        if (!name->Init(ctx, FakeSource.Get()) ||
            !newName->Init(ctx, FakeSource.Get()) ||
            !cluster->Init(ctx, FakeSource.Get()))
        {
            return false;
        }

        auto options = Y(Q(Y(Q("mode"), Q(IsUser ? "renameUser" : "renameGroup"))));
        options = L(options, Q(Y(Q("newName"), newName)));

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Service), cluster)),
            Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
            )));

        return TAstListNode::DoInit(ctx, FakeSource.Get());
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    const bool IsUser;
    const TString Service;
    TDeferredAtom Cluster;
    TDeferredAtom Name;
    TDeferredAtom NewName;
    TScopedStatePtr Scoped;
    TSourcePtr FakeSource;
};

TNodePtr BuildRenameUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped) {
    const bool isUser = true;
    return new TRenameRole(pos, isUser, service, cluster, name, newName, scoped);
}

TNodePtr BuildRenameGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped) {
    const bool isUser = false;
    return new TRenameRole(pos, isUser, service, cluster, name, newName, scoped);
}

class TAlterGroup final: public TAstListNode {
public:
    TAlterGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TVector<TDeferredAtom>& toChange, bool isDrop, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service(service)
        , Cluster(cluster)
        , Name(name)
        , ToChange(toChange)
        , IsDrop(isDrop)
        , Scoped(scoped)
    {
        FakeSource = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto name = Name.Build();
        TNodePtr cluster = Scoped->WrapCluster(Cluster, ctx);

        if (!name->Init(ctx, FakeSource.Get()) || !cluster->Init(ctx, FakeSource.Get())) {
            return false;
        }

        TVector<TNodePtr> toChange;
        for (auto& item : ToChange) {
            toChange.push_back(item.Build());
            if (!toChange.back()->Init(ctx, FakeSource.Get())) {
                return false;
            }
        }

        auto options = Y(Q(Y(Q("mode"), Q(IsDrop ? "dropUsersFromGroup" : "addUsersToGroup"))));
        options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos, std::move(toChange))))));

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Service), cluster)),
            Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
            )));

        return TAstListNode::DoInit(ctx, FakeSource.Get());
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    const TString Service;
    TDeferredAtom Cluster;
    TDeferredAtom Name;
    TVector<TDeferredAtom> ToChange;
    const bool IsDrop;
    TScopedStatePtr Scoped;
    TSourcePtr FakeSource;
};

TNodePtr BuildAlterGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TVector<TDeferredAtom>& toChange, bool isDrop,
    TScopedStatePtr scoped)
{
    return new TAlterGroup(pos, service, cluster, name, toChange, isDrop, scoped);
}

class TDropRoles final: public TAstListNode {
public:
    TDropRoles(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& toDrop, bool isUser, bool missingOk, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service(service)
        , Cluster(cluster)
        , ToDrop(toDrop)
        , IsUser(isUser)
        , MissingOk(missingOk)
        , Scoped(scoped)
    {
        FakeSource = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        TNodePtr cluster = Scoped->WrapCluster(Cluster, ctx);

        if (!cluster->Init(ctx, FakeSource.Get())) {
            return false;
        }

        const char* mode = IsUser ?
            (MissingOk ? "dropUserIfExists" : "dropUser") :
            (MissingOk ? "dropGroupIfExists" : "dropGroup");

        auto options = Y(Q(Y(Q("mode"), Q(mode))));

        auto block = Y(Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Service), cluster)));
        for (auto& item : ToDrop) {
            auto name = item.Build();
            if (!name->Init(ctx, FakeSource.Get())) {
                return false;
            }

            block = L(block, Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))));
        }
        block = L(block, Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")));
        Add("block", Q(block));

        return TAstListNode::DoInit(ctx, FakeSource.Get());
    }

    TPtr DoClone() const final {
        return {};
    }
private:
    const TString Service;
    TDeferredAtom Cluster;
    TVector<TDeferredAtom> ToDrop;
    const bool IsUser;
    const bool MissingOk;
    TScopedStatePtr Scoped;
    TSourcePtr FakeSource;
};

TNodePtr BuildUpsertObjectOperation(TPosition pos, const TString& objectId, const TString& typeId,
    std::map<TString, TDeferredAtom>&& features, const TObjectOperatorContext& context) {
    return new TUpsertObject(pos, objectId, typeId, false, false, std::move(features), std::set<TString>(), context);
}
TNodePtr BuildCreateObjectOperation(TPosition pos, const TString& objectId, const TString& typeId,
    bool existingOk, bool replaceIfExists, std::map<TString, TDeferredAtom>&& features, const TObjectOperatorContext& context) {
    return new TCreateObject(pos, objectId, typeId, existingOk, replaceIfExists, std::move(features), std::set<TString>(), context);
}
TNodePtr BuildAlterObjectOperation(TPosition pos, const TString& secretId, const TString& typeId,
    std::map<TString, TDeferredAtom>&& features, std::set<TString>&& featuresToReset, const TObjectOperatorContext& context)
{
    return new TAlterObject(pos, secretId, typeId, false, false, std::move(features), std::move(featuresToReset), context);
}
TNodePtr BuildDropObjectOperation(TPosition pos, const TString& secretId, const TString& typeId,
    bool missingOk, std::map<TString, TDeferredAtom>&& options, const TObjectOperatorContext& context)
{
    return new TDropObject(pos, secretId, typeId, missingOk, false, std::move(options), std::set<TString>(), context);
}

TNodePtr BuildDropRoles(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& toDrop, bool isUser, bool missingOk, TScopedStatePtr scoped) {
    return new TDropRoles(pos, service, cluster, toDrop, isUser, missingOk, scoped);
}

class TPermissionsAction final : public TAstListNode {
public:
    struct TPermissionParameters {
        TString PermissionAction;
        TVector<TDeferredAtom> Permissions;
        TVector<TDeferredAtom> SchemaPaths;
        TVector<TDeferredAtom> RoleNames;
    };

    TPermissionsAction(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TPermissionParameters& parameters, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service(service)
        , Cluster(cluster)
        , Parameters(parameters)
        , Scoped(scoped)
    {
        FakeSource = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);

        TNodePtr cluster = Scoped->WrapCluster(Cluster, ctx);
        TNodePtr permissionAction = TDeferredAtom(Pos, Parameters.PermissionAction).Build();

        if (!permissionAction->Init(ctx, FakeSource.Get()) ||
            !cluster->Init(ctx, FakeSource.Get())) {
            return false;
        }

        TVector<TNodePtr> paths;
        paths.reserve(Parameters.SchemaPaths.size());
        for (auto& item : Parameters.SchemaPaths) {
            paths.push_back(item.Build());
            if (!paths.back()->Init(ctx, FakeSource.Get())) {
                return false;
            }
        }
        auto options = Y(Q(Y(Q("paths"), Q(new TAstListNodeImpl(Pos, std::move(paths))))));

        TVector<TNodePtr> permissions;
        permissions.reserve(Parameters.Permissions.size());
        for (auto& item : Parameters.Permissions) {
            permissions.push_back(item.Build());
            if (!permissions.back()->Init(ctx, FakeSource.Get())) {
                return false;
            }
        }
        options = L(options, Q(Y(Q("permissions"), Q(new TAstListNodeImpl(Pos, std::move(permissions))))));

        TVector<TNodePtr> roles;
        roles.reserve(Parameters.RoleNames.size());
        for (auto& item : Parameters.RoleNames) {
            roles.push_back(item.Build());
            if (!roles.back()->Init(ctx, FakeSource.Get())) {
                return false;
            }
        }
        options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos, std::move(roles))))));

        auto block = Y(Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Service), cluster)));
        block = L(block, Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("permission"), Y("String", permissionAction)))), Y("Void"), Q(options))));
        block = L(block, Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")));
        Add("block", Q(block));

        return TAstListNode::DoInit(ctx, FakeSource.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service;
    TDeferredAtom Cluster;
    TPermissionParameters Parameters;
    TScopedStatePtr Scoped;
    TSourcePtr FakeSource;
};

TNodePtr BuildGrantPermissions(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& permissions, const TVector<TDeferredAtom>& schemaPaths, const TVector<TDeferredAtom>& roleNames, TScopedStatePtr scoped) {
    return new TPermissionsAction(pos,
                                  service,
                                  cluster,
                                  {.PermissionAction = "grant",
                                               .Permissions = permissions,
                                               .SchemaPaths = schemaPaths,
                                               .RoleNames = roleNames},
                                  scoped);
}

TNodePtr BuildRevokePermissions(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& permissions, const TVector<TDeferredAtom>& schemaPaths, const TVector<TDeferredAtom>& roleNames, TScopedStatePtr scoped) {
    return new TPermissionsAction(pos,
                                  service,
                                  cluster,
                                  {.PermissionAction = "revoke",
                                               .Permissions = permissions,
                                               .SchemaPaths = schemaPaths,
                                               .RoleNames = roleNames},
                                  scoped);
}

class TAsyncReplication
    : public TAstListNode
    , protected TObjectOperatorContext
{
protected:
    virtual INode::TPtr FillOptions(INode::TPtr options) const = 0;

public:
    explicit TAsyncReplication(TPosition pos, const TString& id, const TString& mode, const TObjectOperatorContext& context)
        : TAstListNode(pos)
        , TObjectOperatorContext(context)
        , Id(id)
        , Mode(mode)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Scoped->UseCluster(ServiceId, Cluster);

        auto keys = Y("Key", Q(Y(Q("replication"), Y("String", BuildQuotedAtom(Pos, Id)))));
        auto options = FillOptions(Y(Q(Y(Q("mode"), Q(Mode)))));

        Add("block", Q(Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, ServiceId), Scoped->WrapCluster(Cluster, ctx))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        )));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Id;
    const TString Mode;

}; // TAsyncReplication

class TCreateAsyncReplication final: public TAsyncReplication {
public:
    explicit TCreateAsyncReplication(TPosition pos, const TString& id,
            std::vector<std::pair<TString, TString>>&& targets,
            std::map<TString, TNodePtr>&& settings,
            const TObjectOperatorContext& context)
        : TAsyncReplication(pos, id, "create", context)
        , Targets(std::move(targets))
        , Settings(std::move(settings))
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        if (!Targets.empty()) {
            auto targets = Y();
            for (auto&& [remote, local] : Targets) {
                auto target = Y();
                target = L(target, Q(Y(Q("remote"), Q(remote))));
                target = L(target, Q(Y(Q("local"), Q(local))));
                targets = L(targets, Q(target));
            }
            options = L(options, Q(Y(Q("targets"), Q(targets))));
        }

        if (!Settings.empty()) {
            auto settings = Y();
            for (auto&& [k, v] : Settings) {
                if (v) {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos, k), v)));
                } else {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos, k))));
                }
            }
            options = L(options, Q(Y(Q("settings"), Q(settings))));
        }

        return options;
    }

private:
    std::vector<std::pair<TString, TString>> Targets; // (remote, local)
    std::map<TString, TNodePtr> Settings;

}; // TCreateAsyncReplication

TNodePtr BuildCreateAsyncReplication(TPosition pos, const TString& id,
        std::vector<std::pair<TString, TString>>&& targets,
        std::map<TString, TNodePtr>&& settings,
        const TObjectOperatorContext& context)
{
    return new TCreateAsyncReplication(pos, id, std::move(targets), std::move(settings), context);
}

class TDropAsyncReplication final: public TAsyncReplication {
public:
    explicit TDropAsyncReplication(TPosition pos, const TString& id, bool cascade, const TObjectOperatorContext& context)
        : TAsyncReplication(pos, id, cascade ? "dropCascade" : "drop", context)
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        return options;
    }

}; // TDropAsyncReplication

TNodePtr BuildDropAsyncReplication(TPosition pos, const TString& id, bool cascade, const TObjectOperatorContext& context) {
    return new TDropAsyncReplication(pos, id, cascade, context);
}

class TAlterAsyncReplication final: public TAsyncReplication {
public:
    explicit TAlterAsyncReplication(TPosition pos, const TString& id,
            std::map<TString, TNodePtr>&& settings,
            const TObjectOperatorContext& context)
        : TAsyncReplication(pos, id, "alter", context)
        , Settings(std::move(settings))
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        if (!Settings.empty()) {
            auto settings = Y();
            for (auto&& [k, v] : Settings) {
                if (v) {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos, k), v)));
                } else {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos, k))));
                }
            }
            options = L(options, Q(Y(Q("settings"), Q(settings))));
        }

        return options;
    }

private:
    std::map<TString, TNodePtr> Settings;

}; // TAlterAsyncReplication

TNodePtr BuildAlterAsyncReplication(TPosition pos, const TString& id,
        std::map<TString, TNodePtr>&& settings,
        const TObjectOperatorContext& context)
{
    return new TAlterAsyncReplication(pos, id, std::move(settings), context);
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
        TNodePtr options, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Label(label)
        , Table(table)
        , Mode(mode)
        , Options(options)
        , Scoped(scoped)
    {
        scoped->UseCluster(Table.Service, Table.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Table.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::WRITE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        auto getModesMap = [] (const TString& serviceName) -> const TMap<EWriteColumnMode, TString>& {
            if (serviceName == KikimrProviderName || serviceName == YdbProviderName) {
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
            auto modeStr = getModesMap(Table.Service).FindPtr(Mode);

            options->Add(Q(Y(Q("mode"), Q(modeStr ? *modeStr : "unsupported"))));
        }

        Add("block", Q((Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, Table.Service), Scoped->WrapCluster(Table.Cluster, ctx))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Label, Q(options))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
        ))));

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
    TScopedStatePtr Scoped;
};

TNodePtr BuildWriteTable(TPosition pos, const TString& label, const TTableRef& table, EWriteColumnMode mode, TNodePtr options,
    TScopedStatePtr scoped)
{
    return new TWriteTableNode(pos, label, table, mode, std::move(options), scoped);
}

class TClustersSinkOperationBase: public TAstListNode {
protected:
    TClustersSinkOperationBase(TPosition pos)
        : TAstListNode(pos)
    {}

    virtual TPtr ProduceOperation() = 0;

    bool DoInit(TContext& ctx, ISource* src) override {
        auto block(Y());

        auto op = ProduceOperation();
        if (!op) {
            return false;
        }

        block = L(block, op);
        block = L(block, Y("return", "world"));
        Add("block", Q(block));

        return TAstListNode::DoInit(ctx, src);
     }

    TPtr DoClone() const final {
        return {};
    }
};

class TCommitClustersNode: public TClustersSinkOperationBase {
public:
    TCommitClustersNode(TPosition pos)
        : TClustersSinkOperationBase(pos)
    {
    }

    TPtr ProduceOperation() override {
        return Y("let", "world", Y("CommitAll!", "world"));
    }
};

TNodePtr BuildCommitClusters(TPosition pos) {
    return new TCommitClustersNode(pos);
}

class TRollbackClustersNode: public TClustersSinkOperationBase {
public:
    TRollbackClustersNode(TPosition pos)
        : TClustersSinkOperationBase(pos)
    {
    }

    TPtr ProduceOperation() override {
        return Y("let", "world", Y("CommitAll!", "world", Q(Y(Q(Y(Q("mode"), Q("rollback")))))));
    }
};

TNodePtr BuildRollbackClusters(TPosition pos) {
    return new TRollbackClustersNode(pos);
}

class TWriteResultNode final: public TAstListNode {
public:
    TWriteResultNode(TPosition pos, const TString& label, TNodePtr settings)
        : TAstListNode(pos)
        , Label(label)
        , Settings(settings)
        , CommitClusters(BuildCommitClusters(Pos))
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

TNodePtr BuildWriteResult(TPosition pos, const TString& label, TNodePtr settings) {
    return new TWriteResultNode(pos, label, settings);
}

class TYqlProgramNode: public TAstListNode {
public:
    TYqlProgramNode(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Blocks(blocks)
        , TopLevel(topLevel)
        , Scoped(scoped)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        bool hasError = false;
        if (TopLevel) {
            for (auto& var: ctx.Variables) {
                if (!var.second.second->Init(ctx, src)) {
                    hasError = true;
                    continue;
                }
                Add(Y(
                    "declare",
                    new TAstAtomNodeImpl(var.second.first, var.first, TNodeFlags::ArbitraryContent),
                    var.second.second));
            }

            for (const auto& overrideLibrary: ctx.OverrideLibraries) {
                auto node = Y(
                    "override_library",
                    new TAstAtomNodeImpl(
                        std::get<TPosition>(overrideLibrary.second),
                        overrideLibrary.first, TNodeFlags::ArbitraryContent
                    ));

                Add(node);
            }

            for (const auto& package: ctx.Packages) {
                const auto& [url, urlPosition] = std::get<1U>(package.second);

                auto node = Y(
                    "package",
                    new TAstAtomNodeImpl(
                        std::get<TPosition>(package.second), package.first,
                        TNodeFlags::ArbitraryContent
                    ),
                    new TAstAtomNodeImpl(urlPosition, url, TNodeFlags::ArbitraryContent));

                if (const auto& tokenWithPosition = std::get<2U>(package.second)) {
                    const auto& [token, tokenPosition] = *tokenWithPosition;

                    node = L(node, new TAstAtomNodeImpl(tokenPosition, token, TNodeFlags::ArbitraryContent));
                }

                Add(node);
            }

            for (const auto& lib : ctx.Libraries) {
                auto node = Y("library", new TAstAtomNodeImpl(std::get<TPosition>(lib.second), lib.first, TNodeFlags::ArbitraryContent));
                if (const auto& first = std::get<1U>(lib.second)) {
                    node = L(node, new TAstAtomNodeImpl(first->second, first->first, TNodeFlags::ArbitraryContent));
                    if (const auto& second = std::get<2U>(lib.second)) {
                        node = L(node, new TAstAtomNodeImpl(second->second, second->first, TNodeFlags::ArbitraryContent));
                    }
                }

                Add(node);
            }

            for (const auto& p : ctx.PackageVersions) {
                Add(Y("set_package_version", BuildQuotedAtom(Pos, p.first), BuildQuotedAtom(Pos, ToString(p.second))));
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

            if (!ctx.CompactNamedExprs) {
                for (auto& nodes: Scoped->NamedNodes) {
                    if (src || ctx.Exports.contains(nodes.first)) {
                        auto& item = nodes.second.front();
                        if (!item->Node->Init(ctx, src)) {
                            hasError = true;
                            continue;
                        }

                        // Some constants may be used directly by YQL code and need to be translated without reference from SQL AST
                        if (item->Node->IsConstant() || ctx.Exports.contains(nodes.first)) {
                            Add(Y("let", BuildAtom(item->Node->GetPos(), nodes.first), item->Node));
                        }
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

                if (ctx.FilterPushdownOverJoinOptionalSide) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos, "FilterPushdownOverJoinOptionalSide"))));
                }

                if (!ctx.RotateJoinTree) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos, "RotateJoinTree"), BuildQuotedAtom(Pos, "false"))));
                }

                if (ctx.DiscoveryMode) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos, "DiscoveryMode"))));
                }

                if (ctx.DqEngineEnable) {
                    TString mode = "auto";
                    if (ctx.PqReadByRtmrCluster && ctx.PqReadByRtmrCluster != "dq") {
                        mode = "disable";
                    } else if (ctx.DqEngineForce) {
                        mode = "force";
                    }
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos, "DqEngine"), BuildQuotedAtom(Pos, mode))));
                }

                if (ctx.CostBasedOptimizer) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos, "CostBasedOptimizer"), BuildQuotedAtom(Pos, ctx.CostBasedOptimizer))));
                }

                if (ctx.JsonQueryReturnsJsonDocument.Defined()) {
                    TString pragmaName = "DisableJsonQueryReturnsJsonDocument";
                    if (*ctx.JsonQueryReturnsJsonDocument) {
                        pragmaName = "JsonQueryReturnsJsonDocument";
                    }

                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource, BuildQuotedAtom(Pos, pragmaName))));
                }

                if (ctx.OrderedColumns) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos, "OrderedColumns"))));
                }

                if (ctx.PqReadByRtmrCluster) {
                    auto pqSourceAll = Y("DataSource", BuildQuotedAtom(Pos, TString(PqProviderName)), BuildQuotedAtom(Pos, "$all"));
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", pqSourceAll,
                        BuildQuotedAtom(Pos, "Attr"), BuildQuotedAtom(Pos, "PqReadByRtmrCluster_"), BuildQuotedAtom(Pos, ctx.PqReadByRtmrCluster))));

                    auto rtmrSourceAll = Y("DataSource", BuildQuotedAtom(Pos, TString(RtmrProviderName)), BuildQuotedAtom(Pos, "$all"));
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", rtmrSourceAll,
                        BuildQuotedAtom(Pos, "Attr"), BuildQuotedAtom(Pos, "PqReadByRtmrCluster_"), BuildQuotedAtom(Pos, ctx.PqReadByRtmrCluster))));

                    if (ctx.PqReadByRtmrCluster != "dq") {
                        // set any dynamic settings for particular RTMR cluster for CommitAll!
                        auto rtmrSource = Y("DataSource", BuildQuotedAtom(Pos, TString(RtmrProviderName)), BuildQuotedAtom(Pos, ctx.PqReadByRtmrCluster));
                        Add(Y("let", "world", Y(TString(ConfigureName), "world", rtmrSource,
                            BuildQuotedAtom(Pos, "Attr"), BuildQuotedAtom(Pos, "Dummy_"), BuildQuotedAtom(Pos, "1"))));
                    }
                }

                if (ctx.YsonCastToString.Defined()) {
                    const TString pragmaName = *ctx.YsonCastToString ? "YsonCastToString" : "DisableYsonCastToString";
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource, BuildQuotedAtom(Pos, pragmaName))));
                }

                if (ctx.UseBlocks) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource, BuildQuotedAtom(Pos, "UseBlocks"))));
                }

                if (ctx.BlockEngineEnable) {
                    TString mode = ctx.BlockEngineForce ? "force" : "auto";
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                        BuildQuotedAtom(Pos, "BlockEngine"), BuildQuotedAtom(Pos, mode))));
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

        for (const auto& x : Scoped->Local.ExprClusters) {
            auto& data = Scoped->Local.ExprClustersMap[x.Get()];
            auto& node = data.second;

            if (!node->Init(ctx, nullptr)) {
                hasError = true;
                continue;
            }

            Add(Y("let", data.first, node));
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
                for (const auto& [name, node] : ctx.UniversalAliases) {
                    Add(Y("let", name, node));
                }
                Nodes.insert(Nodes.end(), preparedNodes.begin(), preparedNodes.end());
            }

            decltype(Nodes) imports;
            for (const auto& [alias, path]: ctx.RequiredModules) {
                imports.push_back(Y("import", alias, BuildQuotedAtom(Pos, path)));
            }
            Nodes.insert(Nodes.begin(), std::make_move_iterator(imports.begin()), std::make_move_iterator(imports.end()));

            for (const auto& symbol: ctx.Exports) {
                if (ctx.CompactNamedExprs) {
                    auto node = Scoped->LookupNode(symbol);
                    YQL_ENSURE(node);
                    if (!node->Init(ctx, src)) {
                        hasError = true;
                        continue;
                    }
                    Add(Y("let", BuildAtom(node->GetPos(), symbol), node));
                }
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
    TScopedStatePtr Scoped;
};

TNodePtr BuildQuery(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel, TScopedStatePtr scoped) {
    return new TYqlProgramNode(pos, blocks, topLevel, scoped);
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
        else if (Name == TStringBuf("AddFileByUrl") || Name == TStringBuf("SetFileOption") || Name == TStringBuf("AddFolderByUrl") || Name == TStringBuf("ImportUdfs") || Name == TStringBuf("SetPackageVersion")) {
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

        if (!Node->Init(ctx, FakeSource.Get())) {
            return false;
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const final {
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
            if (!exprPtr->Init(ctx, FakeSource.Get())) {
                return {};
            }
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
            args = L(args, BuildAtom(GetPos(), arg));
        }
        Add("lambda", Q(args), body);
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TSqlLambda(Pos, TVector<TString>(Args), CloneContainer(ExprSeq));
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

class TWorldIf final : public TAstListNode {
public:
    TWorldIf(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode, bool isEvaluate)
        : TAstListNode(pos)
        , Predicate(predicate)
        , ThenNode(thenNode)
        , ElseNode(elseNode)
        , IsEvaluate(isEvaluate)
    {
        FakeSource = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Predicate->Init(ctx, FakeSource.Get())) {
            return{};
        }
        Add(IsEvaluate ? "EvaluateIf!" : "If!");
        Add("world");
        auto coalesced = Y("Coalesce", Predicate, Y("Bool", Q("false")));
        Add(IsEvaluate ? Y("EvaluateExpr", Y("EnsureType", coalesced, Y("DataType", Q("Bool")))) : coalesced);

        if (!ThenNode->Init(ctx, FakeSource.Get())) {
            return{};
        }

        Add(ThenNode);
        if (ElseNode) {
            if (!ElseNode->Init(ctx, FakeSource.Get())) {
                return{};
            }

            Add(ElseNode);
        }

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TWorldIf(GetPos(), SafeClone(Predicate), SafeClone(ThenNode), SafeClone(ElseNode), IsEvaluate);
    }

private:
    TNodePtr Predicate;
    TNodePtr ThenNode;
    TNodePtr ElseNode;
    bool IsEvaluate;
    TSourcePtr FakeSource;
};

TNodePtr BuildWorldIfNode(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode, bool isEvaluate) {
    return new TWorldIf(pos, predicate, thenNode, elseNode, isEvaluate);
}

class TWorldFor final : public TAstListNode {
public:
    TWorldFor(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode, bool isEvaluate, bool isParallel)
        : TAstListNode(pos)
        , List(list)
        , BodyNode(bodyNode)
        , ElseNode(elseNode)
        , IsEvaluate(isEvaluate)
        , IsParallel(isParallel)
    {
        FakeSource = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!List->Init(ctx, FakeSource.Get())) {
            return{};
        }
        Add(TStringBuilder() << (IsEvaluate ? "Evaluate": "") << (IsParallel ? "Parallel" : "") << "For!");
        Add("world");
        Add(IsEvaluate ? Y("EvaluateExpr", List) : List);

        if (!BodyNode->Init(ctx, FakeSource.Get())) {
            return{};
        }
        Add(BodyNode);

        if (ElseNode) {
            if (!ElseNode->Init(ctx, FakeSource.Get())) {
                return{};
            }
            Add(ElseNode);
        }

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TWorldFor(GetPos(), SafeClone(List), SafeClone(BodyNode), SafeClone(ElseNode), IsEvaluate, IsParallel);
    }

private:
    TNodePtr List;
    TNodePtr BodyNode;
    TNodePtr ElseNode;
    bool IsEvaluate;
    bool IsParallel;
    TSourcePtr FakeSource;
};

TNodePtr BuildWorldForNode(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode, bool isEvaluate, bool isParallel) {
    return new TWorldFor(pos, list, bodyNode, elseNode, isEvaluate, isParallel);
}
} // namespace NSQLTranslationV1
