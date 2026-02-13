#include "node.h"
#include "context.h"
#include "object_processing.h"

#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/core/sql_types/yql_callable_names.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <library/cpp/charset/ci_string.h>

#include <util/digest/fnv.h>
#include <yql/essentials/public/issue/yql_issue.h>

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
        , Service_(service)
        , Cluster_(cluster)
        , Name_(name)
        , View_(view)
        , Full_(name.GetRepr())
    {
        if (!View_.ViewName.empty()) {
            Full_ += ":" + View_.ViewName;
        }
    }

    bool SetPrimaryView(TContext& ctx, TPosition pos) override {
        Y_UNUSED(ctx);
        Y_UNUSED(pos);
        View_ = {"", true};
        return true;
    }

    bool SetViewName(TContext& ctx, TPosition pos, const TString& view) override {
        Y_UNUSED(ctx);
        Y_UNUSED(pos);
        Full_ = Name_.GetRepr();
        View_ = {view};
        if (!View_.empty()) {
            Full_ = ":" + View_.ViewName;
        }

        return true;
    }

    const TString* GetTableName() const override {
        return Name_.GetLiteral() ? &Full_ : nullptr;
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode mode) override {
        if (View_ == TViewDescription{"@"}) {
            auto key = Y("TempTable", Name_.Build());
            return key;
        }

        bool tableScheme = mode == ITableKeys::EBuildKeysMode::CREATE;
        if (tableScheme && !View_.empty()) {
            ctx.Error(Pos_) << "Table view can not be created with CREATE TABLE clause";
            return nullptr;
        }
        auto path = ctx.GetPrefixedPath(Service_, Cluster_, Name_);
        if (!path) {
            return nullptr;
        }
        auto key = Y("Key", Q(Y(Q(tableScheme ? "tablescheme" : "table"), Y("String", path))));
        key = AddView(key, View_);
        if (!ValidateView(GetPos(), ctx, Service_, View_)) {
            return nullptr;
        }
        if (mode == ITableKeys::EBuildKeysMode::INPUT &&
            IsQueryMode(ctx.Settings.Mode) &&
            Service_ != KikimrProviderName &&
            Service_ != RtmrProviderName &&
            Service_ != YdbProviderName) {
            key = Y("MrTableConcat", key);
        }
        return key;
    }

private:
    TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    TViewDescription View_;
    TString Full_;
};

TNodePtr BuildTableKey(TPosition pos, const TString& service, const TDeferredAtom& cluster,
                       const TDeferredAtom& name, const TViewDescription& view) {
    return new TUniqueTableKey(pos, service, cluster, name, view);
}

class TTopicKey: public ITableKeys {
public:
    TTopicKey(TPosition pos, const TDeferredAtom& cluster, const TDeferredAtom& name)
        : ITableKeys(pos)
        , Cluster_(cluster)
        , Name_(name)
        , Full_(name.GetRepr())
    {
    }

    const TString* GetTableName() const override {
        return Name_.GetLiteral() ? &Full_ : nullptr;
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode) override {
        const auto path = ctx.GetPrefixedPath(Service_, Cluster_, Name_);
        if (!path) {
            return nullptr;
        }
        auto key = Y("Key", Q(Y(Q("topic"), Y("String", path))));
        return key;
    }

private:
    TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    TString View_;
    TString Full_;
};

TNodePtr BuildTopicKey(TPosition pos, const TDeferredAtom& cluster, const TDeferredAtom& name) {
    return new TTopicKey(pos, cluster, name);
}

namespace {

INode::TPtr CreateIndexType(TIndexDescription::EType type, const INode& node) {
    switch (type) {
        case TIndexDescription::EType::GlobalSync:
            return node.Q("syncGlobal");
        case TIndexDescription::EType::GlobalAsync:
            return node.Q("asyncGlobal");
        case TIndexDescription::EType::GlobalSyncUnique:
            return node.Q("syncGlobalUnique");
        case TIndexDescription::EType::GlobalVectorKmeansTree:
            return node.Q("globalVectorKmeansTree");
        case TIndexDescription::EType::GlobalFulltextPlain:
            return node.Q("globalFulltextPlain");
        case TIndexDescription::EType::GlobalFulltextRelevance:
            return node.Q("globalFulltextRelevance");
    }
}

} // namespace

enum class ETableSettingsParsingMode {
    Create,
    Alter
};

namespace {

INode::TPtr CreateTableSettings(const TTableSettings& tableSettings, ETableSettingsParsingMode parsingMode, const INode& node) {
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
                     "Can't reset " << resetableParam.GetValueReset().Name << " in create mode");
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
    if (tableSettings.PartitionCount) {
        settings = L(settings, Q(Y(Q("maxPartitions"), tableSettings.PartitionCount)));
        settings = L(settings, Q(Y(Q("minPartitions"), tableSettings.PartitionCount)));
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

            auto tiersDesc = Y();
            for (const auto& tier : ttlSettings.Tiers) {
                auto tierDesc = Y();
                tierDesc = L(tierDesc, Q(Y(Q("evictionDelay"), tier.EvictionDelay)));
                if (tier.StorageName) {
                    tierDesc = L(tierDesc, Q(Y(Q("storageName"), BuildQuotedAtom(tier.StorageName->Pos, tier.StorageName->Name))));
                }
                tiersDesc = L(tiersDesc, Q(tierDesc));
            }
            opts = L(opts, Q(Y(Q("tiers"), Q(tiersDesc))));

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
    if (tableSettings.ExternalDataChannelsCount) {
        settings = L(settings, Q(Y(Q("externalDataChannelsCount"), tableSettings.ExternalDataChannelsCount)));
    }

    return settings;
}

INode::TPtr CreateIndexSettings(const TIndexDescription::TIndexSettings& indexSettings, const INode& node) {
    // short aliases for member function calls
    auto Y = [&node](auto&&... args) { return node.Y(std::forward<decltype(args)>(args)...); };
    auto Q = [&node](auto&&... args) { return node.Q(std::forward<decltype(args)>(args)...); };
    auto L = [&node](auto&&... args) { return node.L(std::forward<decltype(args)>(args)...); };

    auto settings = Y();

    for (const auto& [_, indexSetting] : indexSettings) {
        settings = L(settings, Q(Y(
                                   BuildQuotedAtom(indexSetting.NamePosition, indexSetting.Name),
                                   BuildQuotedAtom(indexSetting.ValuePosition, indexSetting.Value))));
    }

    return settings;
}

INode::TPtr CreateIndexDesc(const TIndexDescription& index, ETableSettingsParsingMode parsingMode, const INode& node) {
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
        node.Q(node.Y(node.Q("dataColumns"), node.Q(dataColumns))));
    if (index.TableSettings.IsSet()) {
        const auto& tableSettings = node.Q(node.Y(
            node.Q("tableSettings"),
            node.Q(CreateTableSettings(index.TableSettings, parsingMode, node))));
        indexNode = node.L(indexNode, tableSettings);
    }
    if (index.IndexSettings) {
        const auto& indexSettings = node.Q(node.Y(
            node.Q("indexSettings"),
            node.Q(CreateIndexSettings(index.IndexSettings, node))));
        indexNode = node.L(indexNode, indexSettings);
    }
    return indexNode;
}

INode::TPtr CreateAlterIndex(const TIndexDescription& index, const INode& node) {
    const auto& indexName = node.Y(node.Q("indexName"), BuildQuotedAtom(index.Name.Pos, index.Name.Name));
    const auto& tableSettings = node.Y(
        node.Q("tableSettings"),
        node.Q(CreateTableSettings(index.TableSettings, ETableSettingsParsingMode::Alter, node)));
    return node.Y(
        node.Q(indexName),
        node.Q(tableSettings));
}

INode::TPtr CreateChangefeedDesc(const TChangefeedDescription& desc, const INode& node) {
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
    if (desc.Settings.BarriersInterval) {
        settings = node.L(settings, node.Q(node.Y(node.Q("barriers_interval"), desc.Settings.BarriersInterval)));
    }
    if (desc.Settings.SchemaChanges) {
        settings = node.L(settings, node.Q(node.Y(node.Q("schema_changes"), desc.Settings.SchemaChanges)));
    }
    if (desc.Settings.RetentionPeriod) {
        settings = node.L(settings, node.Q(node.Y(node.Q("retention_period"), desc.Settings.RetentionPeriod)));
    }
    if (desc.Settings.TopicAutoPartitioning) {
        settings = node.L(settings, node.Q(node.Y(node.Q("topic_auto_partitioning"), desc.Settings.TopicAutoPartitioning)));
    }
    if (desc.Settings.TopicMaxActivePartitions) {
        settings = node.L(settings, node.Q(node.Y(node.Q("topic_max_active_partitions"), desc.Settings.TopicMaxActivePartitions)));
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
        node.Q(node.Y(node.Q("state"), node.Q(state))));
}

} // namespace

class TPrepTableKeys: public ITableKeys {
public:
    TPrepTableKeys(TPosition pos, const TString& service, const TDeferredAtom& cluster,
                   const TString& func, const TVector<TTableArg>& args)
        : ITableKeys(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Func_(func)
        , Args_(args)
    {
    }

    void ExtractTableName(TContext& ctx, TTableArg& arg) {
        MakeTableFromExpression(Pos_, ctx, arg.Expr, arg.Id);
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode mode) override {
        if (mode == ITableKeys::EBuildKeysMode::CREATE) {
            // TODO: allow creation of multiple tables
            ctx.Error(Pos_) << "Mutiple table creation is not implemented yet";
            return nullptr;
        }

        TString func = Func_;
        if (auto issue = NormalizeName(Pos_, func)) {
            ctx.Error(Pos_) << issue->GetMessage();
            ctx.IncrementMonCounter("sql_errors", "NormalizeTableFunctionError");
            return nullptr;
        }

        if (func != "object" && func != "walkfolders") {
            for (auto& arg : Args_) {
                if (arg.Expr->GetLabel()) {
                    ctx.Error(Pos_) << "Named arguments are not supported for table function " << to_upper(Func_);
                    return nullptr;
                }
            }
        }
        if (func == "concatstrict") {
            auto tuple = Y();
            for (auto& arg : Args_) {
                ExtractTableName(ctx, arg);
                TNodePtr key;
                if (arg.HasAt) {
                    key = Y("TempTable", arg.Id.Build());
                } else {
                    auto path = ctx.GetPrefixedPath(Service_, Cluster_, arg.Id);
                    if (!path) {
                        return nullptr;
                    }

                    key = Y("Key", Q(Y(Q("table"), Y("String", path))));
                    key = AddView(key, arg.View);
                    if (!ValidateView(GetPos(), ctx, Service_, arg.View)) {
                        return nullptr;
                    }
                }

                tuple = L(tuple, key);
            }
            return Q(tuple);
        } else if (func == "concat") {
            auto concat = Y("MrTableConcat");
            for (auto& arg : Args_) {
                ExtractTableName(ctx, arg);
                TNodePtr key;
                if (arg.HasAt) {
                    key = Y("TempTable", arg.Id.Build());
                } else {
                    auto path = ctx.GetPrefixedPath(Service_, Cluster_, arg.Id);
                    if (!path) {
                        return nullptr;
                    }

                    key = Y("Key", Q(Y(Q("table"), Y("String", path))));
                    key = AddView(key, arg.View);
                    if (!ValidateView(GetPos(), ctx, Service_, arg.View)) {
                        return nullptr;
                    }
                }

                concat = L(concat, key);
            }

            return concat;
        }

        else if (func == "range" || func == "rangestrict" || func == "like" || func == "likestrict" ||
                 func == "regexp" || func == "regexpstrict" || func == "filter" || func == "filterstrict") {
            bool isRange = func.StartsWith("range");
            bool isFilter = func.StartsWith("filter");
            size_t minArgs = isRange ? 1 : 2;
            size_t maxArgs = isRange ? 5 : 4;
            if (Args_.size() < minArgs || Args_.size() > maxArgs) {
                ctx.Error(Pos_) << Func_ << " requires from " << minArgs << " to " << maxArgs << " arguments, but got: " << Args_.size();
                return nullptr;
            }
            if (ctx.DiscoveryMode) {
                ctx.Error(Pos_, TIssuesIds::YQL_NOT_ALLOWED_IN_DISCOVERY) << Func_ << " is not allowed in Discovery mode";
                return nullptr;
            }

            for (ui32 index = 0; index < Args_.size(); ++index) {
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
                    sb << "Pass empty string in arguments if you want to skip.";

                    ctx.Error(Pos_) << sb;
                    return nullptr;
                }

                if (!func.StartsWith("filter") || index != 1) {
                    ExtractTableName(ctx, arg);
                }
            }

            auto path = ctx.GetPrefixedPath(Service_, Cluster_, Args_[0].Id);
            if (!path) {
                return nullptr;
            }
            auto range = Y(func.EndsWith("strict") ? "MrTableRangeStrict" : "MrTableRange", path);
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
                } else {
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
                    if (!ctx.PragmaRegexUseRe2) {
                        if (!ctx.Warning(Pos_, TIssuesIds::CORE_LEGACY_REGEX_ENGINE, [&](auto& out) {
                                out << "Legacy regex engine works incorrectly with unicode. "
                                    << "Use PRAGMA RegexUseRe2='true';";
                            })) {
                            return nullptr;
                        }
                    }

                    auto pattern = Args_[1].Id;
                    auto udf = ctx.PragmaRegexUseRe2 ? Y("Udf", Q("Re2.Grep"), Q(Y(Y("String", pattern.Build()), Y("Null")))) : Y("Udf", Q("Pcre.BacktrackingGrep"), Y("String", pattern.Build()));
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
        } else if (func == "each" || func == "eachstrict") {
            auto each = Y(func == "each" ? "MrTableEach" : "MrTableEachStrict");
            for (auto& arg : Args_) {
                if (arg.HasAt) {
                    ctx.Error(Pos_) << "Temporary tables are not supported here";
                    return nullptr;
                }

                auto type = Y("ListType", Y("DataType", Q("String")));
                auto key = Y("Key", Q(Y(Q("table"), Y("EvaluateExpr",
                                                      Y("EnsureType", Y("Coalesce", arg.Expr,
                                                                        Y("List", type)), type)))));

                key = AddView(key, arg.View);
                if (!ValidateView(GetPos(), ctx, Service_, arg.View)) {
                    return nullptr;
                }
                each = L(each, key);
            }
            if (ctx.PragmaUseTablePrefixForEach) {
                TStringBuf prefixPath = ctx.GetPrefixPath(Service_, Cluster_);
                if (prefixPath) {
                    each = L(each, BuildQuotedAtom(Pos_, TString(prefixPath)));
                }
            }
            return each;
        } else if (func == "folder") {
            size_t minArgs = 1;
            size_t maxArgs = 2;
            if (Args_.size() < minArgs || Args_.size() > maxArgs) {
                ctx.Error(Pos_) << Func_ << " requires from " << minArgs << " to " << maxArgs << " arguments, but found: " << Args_.size();
                return nullptr;
            }

            if (ctx.DiscoveryMode) {
                ctx.Error(Pos_, TIssuesIds::YQL_NOT_ALLOWED_IN_DISCOVERY) << Func_ << " is not allowed in Discovery mode";
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
        } else if (func == "walkfolders") {
            const size_t minPositionalArgs = 1;
            const size_t maxPositionalArgs = 2;

            size_t positionalArgsCnt = 0;
            for (const auto& arg : Args_) {
                if (!arg.Expr->GetLabel()) {
                    positionalArgsCnt++;
                } else {
                    break;
                }
            }
            if (positionalArgsCnt < minPositionalArgs || positionalArgsCnt > maxPositionalArgs) {
                ctx.Error(Pos_) << Func_ << " requires from " << minPositionalArgs
                                << " to " << maxPositionalArgs
                                << " positional arguments, but got: " << positionalArgsCnt;
                return nullptr;
            }

            constexpr auto walkFoldersModuleName = "walk_folders_module";
            ctx.RequiredModules.emplace(walkFoldersModuleName, "/lib/yql/walk_folders.yql");

            auto& rootFolderArg = Args_[0];
            if (rootFolderArg.HasAt) {
                ctx.Error(Pos_) << "Temporary tables are not supported here";
                return nullptr;
            }
            if (!rootFolderArg.View.empty()) {
                ctx.Error(Pos_) << Func_ << " doesn't supports views";
                return nullptr;
            }
            ExtractTableName(ctx, rootFolderArg);

            const auto initState =
                positionalArgsCnt > 1
                    ? Args_[1].Expr
                    : Y("List", Y("ListType", Y("DataType", Q("String"))));

            TNodePtr rootAttributes;
            TNodePtr preHandler;
            TNodePtr resolveHandler;
            TNodePtr diveHandler;
            TNodePtr postHandler;
            for (auto it = Args_.begin() + positionalArgsCnt; it != Args_.end(); ++it) {
                auto& arg = *it;
                const auto label = arg.Expr->GetLabel();
                if (label == "RootAttributes") {
                    ExtractTableName(ctx, arg);
                    rootAttributes = arg.Id.Build();
                } else if (label == "PreHandler") {
                    preHandler = arg.Expr;
                } else if (label == "ResolveHandler") {
                    resolveHandler = arg.Expr;
                } else if (label == "DiveHandler") {
                    diveHandler = arg.Expr;
                } else if (label == "PostHandler") {
                    postHandler = arg.Expr;
                } else {
                    if (!ctx.Warning(Pos_, DEFAULT_ERROR, [&](auto& out) {
                            out << "Unsupported named argument: "
                                << label << " in " << Func_;
                        })) {
                        return nullptr;
                    }
                }
            }
            if (rootAttributes == nullptr) {
                rootAttributes = BuildQuotedAtom(Pos_, "");
            }

            if (preHandler != nullptr || postHandler != nullptr) {
                const auto makePrePostHandlerType = BuildBind(Pos_, walkFoldersModuleName, "MakePrePostHandlersType");
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

            const auto makeResolveDiveHandlerType = BuildBind(Pos_, walkFoldersModuleName, "MakeResolveDiveHandlersType");
            const auto resolveDiveHandlerType = Y("EvaluateType", Y("TypeHandle", Y("Apply", makeResolveDiveHandlerType, Y("TypeOf", initState))));
            if (resolveHandler == nullptr) {
                resolveHandler = BuildBind(Pos_, walkFoldersModuleName, "AnyNodeDiveHandler");
            }
            if (diveHandler == nullptr) {
                diveHandler = BuildBind(Pos_, walkFoldersModuleName, "AnyNodeDiveHandler");
            }

            resolveHandler = Y("Callable", resolveDiveHandlerType, resolveHandler);
            diveHandler = Y("Callable", resolveDiveHandlerType, diveHandler);

            const auto initStateType = Y("EvaluateType", Y("TypeHandle", Y("TypeOf", initState)));
            const auto pickledInitState = Y("Pickle", initState);

            const auto initPath = rootFolderArg.Id.Build();

            return Y("MrWalkFolders", initPath, rootAttributes, pickledInitState, initStateType,
                     preHandler, resolveHandler, diveHandler, postHandler);
        } else if (func == "tables") {
            if (!Args_.empty()) {
                ctx.Error(Pos_) << Func_ << " doesn't accept arguments";
                return nullptr;
            }

            return L(Y("DataTables"));
        } else if (func == "object") {
            const size_t positionalArgs = 2;
            auto result = Y("MrObject");
            auto settings = Y();
            // TVector<TNodePtr> settings;
            size_t argc = 0;
            for (ui32 index = 0; index < Args_.size(); ++index) {
                auto& arg = Args_[index];
                if (arg.HasAt) {
                    ctx.Error(arg.Expr->GetPos()) << "Temporary tables are not supported here";
                    return nullptr;
                }

                if (!arg.View.empty()) {
                    ctx.Error(Pos_) << to_upper(Func_) << " doesn't supports views";
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
                ctx.Error(Pos_) << to_upper(Func_) << " requires exacty " << positionalArgs << " positional args, but got " << argc;
                return nullptr;
            }

            result = L(result, Q(settings));
            return result;
        } else if (func == "partitionlist" || func == "partitionliststrict") {
            if (!ctx.EnsureBackwardCompatibleFeatureAvailable(
                    Pos_,
                    "PARTITION_LIST table function",
                    MakeLangVersion(2025, 4)))
            {
                return nullptr;
            }

            if (Args_.size() != 1) {
                ctx.Error(Pos_) << "Single argument required, but got " << Args_.size() << " arguments";
                return nullptr;
            }
            const auto& arg = Args_.front();
            if (arg.HasAt) {
                ctx.Error(Pos_) << "Temporary tables are not supported here, expecting expression";
                return nullptr;
            }
            if (!arg.View.empty()) {
                ctx.Error(Pos_) << "Views are not supported here, expecting expression";
                return nullptr;
            }
            if (!arg.Id.Empty()) {
                ctx.Error(Pos_) << "Expecting expression as argument, but got identifier";
                return nullptr;
            }
            if (!arg.Expr) {
                ctx.Error(Pos_) << "Expecting expression as argument";
                return nullptr;
            }

            auto partitionList = Y(func.EndsWith("strict") ? "MrPartitionListStrict" : "MrPartitionList", Y("EvaluateExpr", arg.Expr));
            if (ctx.PragmaUseTablePrefixForEach) {
                TStringBuf prefixPath = ctx.GetPrefixPath(Service_, Cluster_);
                if (prefixPath) {
                    partitionList = L(partitionList, BuildQuotedAtom(Pos_, TString(prefixPath)));
                }
            }
            return partitionList;
        } else if (func == "partitions" || func == "partitionsstrict") {
            if (!ctx.EnsureBackwardCompatibleFeatureAvailable(
                    Pos_,
                    "PARTITIONS table function",
                    MakeLangVersion(2025, 4)))
            {
                return nullptr;
            }

            const size_t minArgs = 2;
            const size_t maxArgs = 3;
            if (Args_.size() < minArgs || Args_.size() > maxArgs) {
                ctx.Error(Pos_) << Func_ << " requires from " << minArgs << " to " << maxArgs << " arguments, but got: " << Args_.size();
                return nullptr;
            }

            if (ctx.DiscoveryMode) {
                ctx.Error(Pos_, TIssuesIds::YQL_NOT_ALLOWED_IN_DISCOVERY) << "PARTITIONS is not allowed in Discovery mode";
                return nullptr;
            }

            for (auto& arg : Args_) {
                if (arg.HasAt) {
                    ctx.Error(Pos_) << "Temporary tables are not supported here";
                    return nullptr;
                }

                if (!arg.View.empty()) {
                    ctx.Error(Pos_) << "Use the last argument of " << Func_ << " to specify a VIEW." << Endl
                                    << "Possible arguments are: prefix, pattern, view.";
                    return nullptr;
                }

                ExtractTableName(ctx, arg);
            }

            auto path = ctx.GetPrefixedPath(Service_, Cluster_, Args_[0].Id);
            if (!path) {
                return nullptr;
            }
            TNodePtr key = Y("Key", Q(Y(Q("table"), Y("String", path))));
            if (Args_.size() == maxArgs) {
                auto& lastArg = Args_.back();
                if (!lastArg.Id.Empty()) {
                    key = L(key, Q(Y(Q("view"), Y("String", lastArg.Id.Build()))));
                }
            }

            TDeferredAtom pattern = Args_[1].Id;
            return Y(func.EndsWith("strict") ? "MrPartitionsStrict" : "MrPartitions", key, pattern.Build());
        }

        ctx.Error(Pos_) << "Unknown table name preprocessor: " << Func_;
        return nullptr;
    }

private:
    TString Service_;
    TDeferredAtom Cluster_;
    TString Func_;
    TVector<TTableArg> Args_;
};

TNodePtr BuildTableKeys(TPosition pos, const TString& service, const TDeferredAtom& cluster,
                        const TString& func, const TVector<TTableArg>& args) {
    return new TPrepTableKeys(pos, service, cluster, func, args);
}

class TInputOptions final: public TAstListNode {
public:
    TInputOptions(TPosition pos, const TTableHints& hints)
        : TAstListNode(pos)
        , Hints_(hints)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        for (auto& hint : Hints_) {
            TString hintName = hint.first;
            TMaybe<TIssue> normalizeError = NormalizeName(Pos_, hintName);
            if (!normalizeError.Empty()) {
                ctx.Error() << normalizeError->GetMessage();
                ctx.IncrementMonCounter("sql_errors", "NormalizeHintError");
                return false;
            }

            if ("watermark" == hintName) {
                TNodePtr option = Y(BuildQuotedAtom(Pos_, hintName));
                auto anyColumnSrc = BuildAnyColumnSource(Pos_);
                for (auto& x : hint.second) {
                    if (!x->Init(ctx, anyColumnSrc.Get())) {
                        return false;
                    }
                    option = L(option, x);
                }
                Nodes_.push_back(Q(option));
                continue;
            }

            TNodePtr option = Y(BuildQuotedAtom(Pos_, hintName));
            for (auto& x : hint.second) {
                if (!x->Init(ctx, src)) {
                    return false;
                }

                option = L(option, x);
            }

            Nodes_.push_back(Q(option));
        }
        return true;
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TTableHints Hints_;
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
        , Columns_(columns)
        , Hints_(hints)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(ctx);
        Y_UNUSED(src);

        TNodePtr options = Y();
        for (const auto& column : Columns_) {
            options->Add(Q(column));
        }
        if (Columns_) {
            Add(Q(Y(Q("erase_columns"), Q(options))));
        }

        for (const auto& hint : Hints_) {
            TString hintName = hint.first;
            TMaybe<TIssue> normalizeError = NormalizeName(Pos_, hintName);
            if (!normalizeError.Empty()) {
                ctx.Error() << normalizeError->GetMessage();
                ctx.IncrementMonCounter("sql_errors", "NormalizeHintError");
                return false;
            }
            TNodePtr option = Y(BuildQuotedAtom(Pos_, hintName));
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
        return new TIntoTableOptions(GetPos(), Columns_, CloneContainer(Hints_));
    }

private:
    TVector<TString> Columns_;
    TTableHints Hints_;
};

TNodePtr BuildIntoTableOptions(TPosition pos, const TVector<TString>& eraseColumns, const TTableHints& hints) {
    return new TIntoTableOptions(pos, eraseColumns, hints);
}

class TInputTablesNode final: public TAstListNode {
public:
    TInputTablesNode(TPosition pos, const TTableList& tables, bool inSubquery, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Tables_(tables)
        , InSubquery_(inSubquery)
        , Scoped_(scoped)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        THashSet<TString> processedTables;
        for (auto& tr : Tables_) {
            if (!processedTables.insert(tr.RefName).second) {
                continue;
            }

            Scoped_->UseCluster(tr.Service, tr.Cluster);
            auto tableKeys = tr.Keys->GetTableKeys();
            auto keys = tableKeys->BuildKeys(ctx, ITableKeys::EBuildKeysMode::INPUT);
            if (!keys || !keys->Init(ctx, src)) {
                return false;
            }
            auto fields = Y("Void");
            auto source = Y("DataSource", BuildQuotedAtom(Pos_, tr.Service), Scoped_->WrapCluster(tr.Cluster, ctx));
            auto options = tr.Options ? Q(tr.Options) : Q(Y());
            Add(Y("let", "x", keys->Y(TString(ReadName), "world", source, keys, fields, options)));

            if (IsIn({KikimrProviderName, YdbProviderName}, tr.Service) && InSubquery_) {
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
    TTableList Tables_;
    const bool InSubquery_;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildInputTables(TPosition pos, const TTableList& tables, bool inSubquery, TScopedStatePtr scoped) {
    return new TInputTablesNode(pos, tables, inSubquery, scoped);
}

class TCreateTableNode final: public TAstListNode {
public:
    TCreateTableNode(TPosition pos, const TTableRef& tr, bool existingOk, bool replaceIfExists, const TCreateTableParameters& params, TSourcePtr values, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Table_(tr)
        , Params_(params)
        , ExistingOk_(existingOk)
        , ReplaceIfExists_(replaceIfExists)
        , Values_(std::move(values))
        , Scoped_(scoped)
    {
        scoped->UseCluster(Table_.Service, Table_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        if (!Params_.PkColumns.empty() || !Params_.PartitionByColumns.empty() || !Params_.OrderByColumns.empty() || !Params_.Indexes.empty() || !Params_.Changefeeds.empty())
        {
            THashSet<TString> columnsSet;
            for (auto& col : Params_.Columns) {
                columnsSet.insert(col.Name);
            }

            const bool allowUndefinedColumns = (Values_ != nullptr) && columnsSet.empty();

            THashSet<TString> pkColumns;
            for (auto& keyColumn : Params_.PkColumns) {
                if (!allowUndefinedColumns && !columnsSet.contains(keyColumn.Name)) {
                    ctx.Error(keyColumn.Pos) << "Undefined column: " << keyColumn.Name;
                    return false;
                }
                if (!pkColumns.insert(keyColumn.Name).second) {
                    ctx.Error(keyColumn.Pos) << "Duplicated column in PK: " << keyColumn.Name;
                    return false;
                }
            }
            for (auto& keyColumn : Params_.PartitionByColumns) {
                if (!allowUndefinedColumns && !columnsSet.contains(keyColumn.Name)) {
                    ctx.Error(keyColumn.Pos) << "Undefined column: " << keyColumn.Name;
                    return false;
                }
            }
            for (auto& keyColumn : Params_.OrderByColumns) {
                if (!allowUndefinedColumns && !columnsSet.contains(keyColumn.first.Name)) {
                    ctx.Error(keyColumn.first.Pos) << "Undefined column: " << keyColumn.first.Name;
                    return false;
                }
            }

            THashSet<TString> indexNames;
            for (const auto& index : Params_.Indexes) {
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
            for (const auto& cf : Params_.Changefeeds) {
                if (!cfNames.insert(cf.Name.Name).second) {
                    ctx.Error(cf.Name.Pos) << "Changefeed " << cf.Name.Name << " must be defined once";
                    return false;
                }
            }
        }

        auto opts = Y();
        if (Table_.Options) {
            if (!Table_.Options->Init(ctx, src)) {
                return false;
            }
            opts = Table_.Options;
        }

        if (ExistingOk_) {
            opts = L(opts, Q(Y(Q("mode"), Q("create_if_not_exists"))));
        } else if (ReplaceIfExists_) {
            opts = L(opts, Q(Y(Q("mode"), Q("create_or_replace"))));
        } else {
            opts = L(opts, Q(Y(Q("mode"), Q("create"))));
        }

        THashSet<TString> columnFamilyNames;

        if (Params_.ColumnFamilies) {
            auto columnFamilies = Y();
            for (const auto& family : Params_.ColumnFamilies) {
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
                if (family.CompressionLevel) {
                    familyDesc = L(familyDesc, Q(Y(Q("compression_level"), family.CompressionLevel)));
                }
                if (family.CacheMode) {
                    familyDesc = L(familyDesc, Q(Y(Q("cache_mode"), family.CacheMode)));
                }
                columnFamilies = L(columnFamilies, Q(familyDesc));
            }
            opts = L(opts, Q(Y(Q("columnFamilies"), Q(columnFamilies))));
        }

        auto columns = Y();
        THashSet<TString> columnsWithDefaultValue;
        auto columnsDefaultValueSettings = Y();

        for (auto& col : Params_.Columns) {
            auto columnDesc = Y();
            columnDesc = L(columnDesc, BuildQuotedAtom(Pos_, col.Name));
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

                if (col.Compression) {
                    auto columnCompression = Y();
                    for (const auto& [key, value] : col.Compression->Entries) {
                        columnCompression = L(columnCompression, Q(Y(Q(key), value)));
                    }
                    columnDesc = L(columnDesc, Q(Y(Q("columnCompression"), Q(columnCompression))));
                }

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

        if (Table_.Service == YtProviderName || Table_.Service == RtmrProviderName) {
            if (!Params_.PkColumns.empty() && !Params_.PartitionByColumns.empty()) {
                ctx.Error() << "Only one of PRIMARY KEY or PARTITION BY constraints may be specified";
                return false;
            }
        } else {
            if (!Params_.OrderByColumns.empty()) {
                ctx.Error() << "ORDER BY is supported only for " << YtProviderName << " or " << RtmrProviderName << " provider.";
                return false;
            }
        }

        if (!Params_.PkColumns.empty()) {
            if (Table_.Service == YtProviderName) {
                ctx.Error() << "PRIMARY KEY is not supported by " << YtProviderName << " provider.";
                return false;
            }

            auto primaryKey = Y();
            for (auto& col : Params_.PkColumns) {
                primaryKey = L(primaryKey, BuildQuotedAtom(col.Pos, col.Name));
            }
            opts = L(opts, Q(Y(Q("primarykey"), Q(primaryKey))));
            if (!Params_.OrderByColumns.empty()) {
                ctx.Error() << "PRIMARY KEY cannot be used with ORDER BY, use PARTITION BY instead";
                return false;
            }
        }

        if (!Params_.PartitionByColumns.empty()) {
            if (Table_.Service == YtProviderName) {
                ctx.Error() << "PARTITION BY is not supported by " << YtProviderName << " provider.";
                return false;
            }

            auto partitionBy = Y();
            for (auto& col : Params_.PartitionByColumns) {
                partitionBy = L(partitionBy, BuildQuotedAtom(col.Pos, col.Name));
            }
            opts = L(opts, Q(Y(Q("partitionby"), Q(partitionBy))));
        }

        if (!Params_.OrderByColumns.empty()) {
            auto orderBy = Y();
            for (auto& col : Params_.OrderByColumns) {
                orderBy = L(orderBy, Q(Y(BuildQuotedAtom(col.first.Pos, col.first.Name), col.second ? Q("1") : Q("0"))));
            }
            opts = L(opts, Q(Y(Q("orderby"), Q(orderBy))));
        }

        for (const auto& index : Params_.Indexes) {
            const auto& desc = CreateIndexDesc(index, ETableSettingsParsingMode::Create, *this);
            opts = L(opts, Q(Y(Q("index"), Q(desc))));
        }

        for (const auto& cf : Params_.Changefeeds) {
            const auto& desc = CreateChangefeedDesc(cf, *this);
            opts = L(opts, Q(Y(Q("changefeed"), Q(desc))));
        }

        if (Params_.TableSettings.IsSet()) {
            opts = L(opts, Q(Y(Q("tableSettings"), Q(
                                                       CreateTableSettings(Params_.TableSettings, ETableSettingsParsingMode::Create, *this)))));
        }

        switch (Params_.TableType) {
            case ETableType::TableStore:
                opts = L(opts, Q(Y(Q("tableType"), Q("tableStore"))));
                break;
            case ETableType::ExternalTable:
                opts = L(opts, Q(Y(Q("tableType"), Q("externalTable"))));
                break;
            case ETableType::Table:
                break;
        }

        if (Params_.Temporary) {
            opts = L(opts, Q(Y(Q("temporary"))));
        }

        TNodePtr node = nullptr;
        if (Values_) {
            if (!Values_->Init(ctx, nullptr)) {
                return false;
            }
            TTableList tableList;
            Values_->GetInputTables(tableList);
            auto valuesSource = Values_.Get();
            auto values = Values_->Build(ctx);
            if (!Values_) {
                return false;
            }

            TNodePtr inputTables(BuildInputTables(Pos_, tableList, false, Scoped_));
            if (!inputTables->Init(ctx, valuesSource)) {
                return false;
            }

            node = inputTables;
            node = L(node, Y("let", "values", values));
        } else {
            node = Y(Y("let", "values", Y("Void")));
        }

        auto write = Y(
            Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Table_.Service), Scoped_->WrapCluster(Table_.Cluster, ctx))),
            Y("let", "world", Y(TString(WriteName), "world", "sink", keys, "values", Q(opts))),
            Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")));

        node = L(node, Y("let", "world", Y("block", Q(write))));
        node = L(node, Y("return", "world"));

        Add("block", Q(node));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TTableRef Table_;
    const TCreateTableParameters Params_;
    const bool ExistingOk_;
    const bool ReplaceIfExists_;
    const TSourcePtr Values_;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildCreateTable(TPosition pos, const TTableRef& tr, bool existingOk, bool replaceIfExists, const TCreateTableParameters& params, TSourcePtr values, TScopedStatePtr scoped)
{
    return new TCreateTableNode(pos, tr, existingOk, replaceIfExists, params, std::move(values), scoped);
}

namespace {

bool InitDatabaseSettings(TContext& ctx, ISource* src, const THashMap<TString, TNodePtr>& settings) {
    for (const auto& [setting, value] : settings) {
        if (!value || !value->Init(ctx, src)) {
            return false;
        }
    }
    return true;
}

} // namespace

class TAlterDatabaseNode final: public TAstListNode {
public:
    TAlterDatabaseNode(
        TPosition pos,
        const TString& service,
        const TDeferredAtom& cluster,
        const TAlterDatabaseParameters& params,
        TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Params_(params)
        , Scoped_(scoped)
        , Cluster_(cluster)
        , Service_(service)
    {
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        auto options = Y(Q(Y(Q("mode"), Q("alterDatabase"))));

        if (Params_.Owner.has_value()) {
            options = L(options, Q(Y(Q("owner"), Params_.Owner.value().Build())));
        }
        if (!InitDatabaseSettings(ctx, src, Params_.DatabaseSettings)) {
            return false;
        }
        AddDatabaseSettings(options, Params_.DatabaseSettings);

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("databasePath"), Y("String", Params_.DbPath.Build())))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TAlterDatabaseParameters Params_;
    TScopedStatePtr Scoped_;
    TDeferredAtom Cluster_;
    TString Service_;

    void AddDatabaseSettings(TNodePtr& options, const THashMap<TString, TNodePtr>& settings) {
        for (const auto& [setting, value] : settings) {
            options = L(options, Q(Y(BuildQuotedAtom(Pos_, setting), value)));
        }
    }
};

TNodePtr BuildAlterDatabase(
    TPosition pos,
    const TString& service,
    const TDeferredAtom& cluster,
    const TAlterDatabaseParameters& params,
    TScopedStatePtr scoped) {
    return new TAlterDatabaseNode(
        pos,
        service,
        cluster,
        params,
        scoped);
}

class TTruncateTableNode final: public TAstListNode {
public:
    TTruncateTableNode(TPosition pos, const TTableRef& tr, const TTruncateTableParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Params_(params)
        , Table_(tr)
        , Scoped_(scoped)
    {
        scoped->UseCluster(Table_.Service, Table_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(Params_);
        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        TNodePtr cluster = Scoped_->WrapCluster(Table_.Cluster, ctx);

        auto options = Y(Q(Y(Q("mode"), Q("truncateTable"))));

        Add("block", Q(Y(Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Table_.Service), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const override {
        return new TTruncateTableNode(GetPos(), Table_, Params_, Scoped_);
    }

private:
    const TTruncateTableParameters Params_;
    const TTableRef Table_;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildTruncateTable(
    TPosition pos,
    const TTableRef& tr,
    const TTruncateTableParameters& params,
    TScopedStatePtr scoped)
{
    return new TTruncateTableNode(
        pos,
        tr,
        params,
        scoped);
}

class TAlterTableNode final: public TAstListNode {
public:
    TAlterTableNode(TPosition pos, const TTableRef& tr, const TAlterTableParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Table_(tr)
        , Params_(params)
        , Scoped_(scoped)
    {
        scoped->UseCluster(Table_.Service, Table_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        auto actions = Y();

        if (Params_.AddColumns) {
            auto columns = Y();
            for (auto& col : Params_.AddColumns) {
                auto columnDesc = Y();
                columnDesc = L(columnDesc, BuildQuotedAtom(Pos_, col.Name));
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

        if (Params_.DropColumns) {
            auto columns = Y();
            for (auto& colName : Params_.DropColumns) {
                columns = L(columns, BuildQuotedAtom(Pos_, colName));
            }
            actions = L(actions, Q(Y(Q("dropColumns"), Q(columns))));
        }

        if (Params_.AlterColumns) {
            auto columns = Y();
            for (auto& col : Params_.AlterColumns) {
                switch (col.TypeOfChange) {
                    case TColumnSchema::ETypeOfChange::SetNotNullConstraint:
                    case TColumnSchema::ETypeOfChange::DropNotNullConstraint: {
                        auto columnDesc = Y();
                        columnDesc = L(columnDesc, BuildQuotedAtom(Pos_, col.Name));

                        auto columnConstraints = Y();
                        if (col.TypeOfChange == TColumnSchema::ETypeOfChange::DropNotNullConstraint) {
                            columnConstraints = L(columnConstraints, Q(Y(Q("drop_not_null"))));
                        } else {
                            columnConstraints = L(columnConstraints, Q(Y(Q("set_not_null"))));
                        }
                        columnDesc = L(columnDesc, Q(Y(Q("changeColumnConstraints"), Q(columnConstraints))));
                        columns = L(columns, Q(columnDesc));

                        break;
                    }
                    case TColumnSchema::ETypeOfChange::SetFamily: {
                        auto columnDesc = Y();
                        columnDesc = L(columnDesc, BuildQuotedAtom(Pos_, col.Name));
                        auto familiesDesc = Y();
                        for (const auto& family : col.Families) {
                            familiesDesc = L(familiesDesc, BuildQuotedAtom(family.Pos, family.Name));
                        }

                        columnDesc = L(columnDesc, Q(Y(Q("setFamily"), Q(familiesDesc))));
                        columns = L(columns, Q(columnDesc));

                        break;
                    }
                    case TColumnSchema::ETypeOfChange::SetCompression: {
                        auto columnDesc = Y();
                        columnDesc = L(columnDesc, BuildQuotedAtom(Pos_, col.Name));

                        auto columnCompression = Y();

                        if (col.Compression) {
                            for (const auto& [key, value] : col.Compression->Entries) {
                                columnCompression = L(columnCompression, Q(Y(Q(key), value)));
                            }
                        }

                        columnDesc = L(columnDesc, Q(Y(Q("changeCompression"), Q(columnCompression))));
                        columns = L(columns, Q(columnDesc));

                        break;
                    }
                    case TColumnSchema::ETypeOfChange::Nothing: {
                        // do nothing

                        break;
                    }
                }
            }

            actions = L(actions, Q(Y(Q("alterColumns"), Q(columns))));
        }

        if (Params_.AddColumnFamilies) {
            auto columnFamilies = Y();
            for (const auto& family : Params_.AddColumnFamilies) {
                auto familyDesc = Y();
                familyDesc = L(familyDesc, Q(Y(Q("name"), BuildQuotedAtom(family.Name.Pos, family.Name.Name))));
                if (family.Data) {
                    familyDesc = L(familyDesc, Q(Y(Q("data"), family.Data)));
                }
                if (family.Compression) {
                    familyDesc = L(familyDesc, Q(Y(Q("compression"), family.Compression)));
                }
                if (family.CompressionLevel) {
                    familyDesc = L(familyDesc, Q(Y(Q("compression_level"), family.CompressionLevel)));
                }
                if (family.CacheMode) {
                    familyDesc = L(familyDesc, Q(Y(Q("cache_mode"), family.CacheMode)));
                }
                columnFamilies = L(columnFamilies, Q(familyDesc));
            }
            actions = L(actions, Q(Y(Q("addColumnFamilies"), Q(columnFamilies))));
        }

        if (Params_.AlterColumnFamilies) {
            auto columnFamilies = Y();
            for (const auto& family : Params_.AlterColumnFamilies) {
                auto familyDesc = Y();
                familyDesc = L(familyDesc, Q(Y(Q("name"), BuildQuotedAtom(family.Name.Pos, family.Name.Name))));
                if (family.Data) {
                    familyDesc = L(familyDesc, Q(Y(Q("data"), family.Data)));
                }
                if (family.Compression) {
                    familyDesc = L(familyDesc, Q(Y(Q("compression"), family.Compression)));
                }
                if (family.CompressionLevel) {
                    familyDesc = L(familyDesc, Q(Y(Q("compression_level"), family.CompressionLevel)));
                }
                if (family.CacheMode) {
                    familyDesc = L(familyDesc, Q(Y(Q("cache_mode"), family.CacheMode)));
                }
                columnFamilies = L(columnFamilies, Q(familyDesc));
            }
            actions = L(actions, Q(Y(Q("alterColumnFamilies"), Q(columnFamilies))));
        }

        if (Params_.TableSettings.IsSet()) {
            actions = L(actions, Q(Y(Q("setTableSettings"), Q(
                                                                CreateTableSettings(Params_.TableSettings, ETableSettingsParsingMode::Alter, *this)))));
        }

        for (const auto& index : Params_.AddIndexes) {
            const auto& desc = CreateIndexDesc(index, ETableSettingsParsingMode::Alter, *this);
            actions = L(actions, Q(Y(Q("addIndex"), Q(desc))));
        }

        for (const auto& index : Params_.AlterIndexes) {
            const auto& desc = CreateAlterIndex(index, *this);
            actions = L(actions, Q(Y(Q("alterIndex"), Q(desc))));
        }

        for (const auto& id : Params_.DropIndexes) {
            auto indexName = BuildQuotedAtom(id.Pos, id.Name);
            actions = L(actions, Q(Y(Q("dropIndex"), indexName)));
        }

        if (Params_.RenameIndexTo) {
            auto src = BuildQuotedAtom(Params_.RenameIndexTo->first.Pos, Params_.RenameIndexTo->first.Name);
            auto dst = BuildQuotedAtom(Params_.RenameIndexTo->second.Pos, Params_.RenameIndexTo->second.Name);

            auto desc = Y();

            desc = L(desc, Q(Y(Q("src"), src)));
            desc = L(desc, Q(Y(Q("dst"), dst)));

            actions = L(actions, Q(Y(Q("renameIndexTo"), Q(desc))));
        }

        if (Params_.RenameTo) {
            auto destination = ctx.GetPrefixedPath(Scoped_->CurrService, Scoped_->CurrCluster,
                                                   TDeferredAtom(Params_.RenameTo->Pos, Params_.RenameTo->Name));
            actions = L(actions, Q(Y(Q("renameTo"), destination)));
        }

        for (const auto& cf : Params_.AddChangefeeds) {
            const auto& desc = CreateChangefeedDesc(cf, *this);
            actions = L(actions, Q(Y(Q("addChangefeed"), Q(desc))));
        }

        for (const auto& cf : Params_.AlterChangefeeds) {
            const auto& desc = CreateChangefeedDesc(cf, *this);
            actions = L(actions, Q(Y(Q("alterChangefeed"), Q(desc))));
        }

        for (const auto& id : Params_.DropChangefeeds) {
            const auto name = BuildQuotedAtom(id.Pos, id.Name);
            actions = L(actions, Q(Y(Q("dropChangefeed"), name)));
        }

        if (Params_.Compact) {
            auto settings = Y();
            if (Params_.Compact->Cascade) {
                settings = L(settings, Q(Y(Q("cascade"), Params_.Compact->Cascade)));
            }
            if (Params_.Compact->MaxShardsInFlight) {
                settings = L(settings, Q(Y(Q("maxShardsInFlight"), Params_.Compact->MaxShardsInFlight)));
            }
            actions = L(actions, Q(Y(Q("compact"), Q(Y(Q(Y(Q("settings"), Q(settings))))))));
        }

        auto opts = Y();

        opts = L(opts, Q(Y(Q("mode"), Q("alter"))));
        opts = L(opts, Q(Y(Q("actions"), Q(actions))));

        switch (Params_.TableType) {
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
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Table_.Service), Scoped_->WrapCluster(Table_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }
    TPtr DoClone() const final {
        return {};
    }

private:
    TTableRef Table_;
    const TAlterTableParameters Params_;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildAlterTable(TPosition pos, const TTableRef& tr, const TAlterTableParameters& params, TScopedStatePtr scoped)
{
    return new TAlterTableNode(pos, tr, params, scoped);
}

class TDropTableNode final: public TAstListNode {
public:
    TDropTableNode(TPosition pos, const TTableRef& tr, bool missingOk, ETableType tableType, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Table_(tr)
        , TableType_(tableType)
        , Scoped_(scoped)
        , MissingOk_(missingOk)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(Table_.Service, Table_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::DROP);
        if (!keys || !keys->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        auto opts = Y();

        opts = L(opts, Q(Y(Q("mode"), Q(MissingOk_ ? "drop_if_exists" : "drop"))));

        switch (TableType_) {
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
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Table_.Service), Scoped_->WrapCluster(Table_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TTableRef Table_;
    ETableType TableType_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
    const bool MissingOk_;
};

TNodePtr BuildDropTable(TPosition pos, const TTableRef& tr, bool missingOk, ETableType tableType, TScopedStatePtr scoped) {
    return new TDropTableNode(pos, tr, missingOk, tableType, scoped);
}

namespace {

TNullable<TNodePtr> CreateConsumerDesc(TContext& ctx, const TTopicConsumerDescription& desc, const INode& node, bool alter) {
    auto setValue = [&](const TNodePtr& settings, const TNodePtr& value, const auto& setter) {
        if (value) {
            return node.L(settings, node.Q(node.Y(node.Q(setter), value)));
        }
        return settings;
    };

    auto setValueWithReset = [&](const TNodePtr& settings, const NYql::TResetableSetting<TNodePtr, void>& value, const auto& setter, const auto& resetter) {
        if (!value) {
            return settings;
        }
        if (value.IsSet()) {
            return node.L(settings, node.Q(node.Y(node.Q(setter), value.GetValueSet())));
        } else {
            YQL_ENSURE(alter, "Cannot reset on create");
            return node.L(settings, node.Q(node.Y(node.Q(resetter), node.Q(node.Y()))));
        }
    };

    if (alter) {
        if (desc.Settings.Type) {
            ctx.Error() << "type alter is not supported";
            return {nullptr};
        }
        if (desc.Settings.KeepMessagesOrder) {
            ctx.Error() << "keep_messages_order alter is not supported";
            return {nullptr};
        }
    }

    auto settings = node.Y();
    settings = setValue(settings, desc.Settings.Important, "important");
    settings = setValueWithReset(settings, desc.Settings.AvailabilityPeriod, "setAvailabilityPeriod", "resetAvailabilityPeriod");
    settings = setValueWithReset(settings, desc.Settings.ReadFromTs, "setReadFromTs", "resetReadFromTs");
    settings = setValueWithReset(settings, desc.Settings.SupportedCodecs, "setSupportedCodecs", "resetSupportedCodecs");
    settings = setValue(settings, desc.Settings.Type, "type");
    settings = setValue(settings, desc.Settings.KeepMessagesOrder, "keep_messages_order");
    settings = setValue(settings, desc.Settings.DefaultProcessingTimeout, "default_processing_timeout");
    settings = setValue(settings, desc.Settings.MaxProcessingAttempts, "max_processing_attempts");
    settings = setValue(settings, desc.Settings.DeadLetterPolicy, "dead_letter_policy");
    settings = setValue(settings, desc.Settings.DeadLetterQueue, "dead_letter_queue");

    return node.Y(
        node.Q(node.Y(node.Q("name"), BuildQuotedAtom(desc.Name.Pos, desc.Name.Name))),
        node.Q(node.Y(node.Q("settings"), node.Q(settings))));
}

} // namespace

class TCreateTopicNode final: public TAstListNode {
public:
    TCreateTopicNode(TPosition pos, const TTopicRef& tr, const TCreateTopicParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Topic_(tr)
        , Params_(params)
        , Scoped_(scoped)
    {
        scoped->UseCluster(TString(KikimrProviderName), Topic_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Topic_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        if (!Params_.Consumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& consumer : Params_.Consumers) {
                if (!consumerNames.insert(consumer.Name.Name).second) {
                    ctx.Error(consumer.Name.Pos) << "Consumer " << consumer.Name.Name << " defined more than once";
                    return false;
                }
            }
        }

        auto opts = Y();
        TString mode = Params_.ExistingOk ? "create_if_not_exists" : "create";
        opts = L(opts, Q(Y(Q("mode"), Q(mode))));

        for (const auto& consumer : Params_.Consumers) {
            const auto desc = CreateConsumerDesc(ctx, consumer, *this, false);
            if (!desc) {
                return false;
            }
            opts = L(opts, Q(Y(Q("consumer"), Q(desc))));
        }

        if (Params_.TopicSettings.IsSet()) {
            auto settings = Y();

#define INSERT_TOPIC_SETTING(NAME)                                                            \
    if (const auto& NAME##Val = Params_.TopicSettings.NAME) {                                 \
        if (NAME##Val.IsSet()) {                                                              \
            settings = L(settings, Q(Y(Q(Y_STRINGIZE(set##NAME)), NAME##Val.GetValueSet()))); \
        } else {                                                                              \
            YQL_ENSURE(false, "Can't reset on create");                                       \
        }                                                                                     \
    }

            INSERT_TOPIC_SETTING(MaxPartitions)
            INSERT_TOPIC_SETTING(MinPartitions)
            INSERT_TOPIC_SETTING(RetentionPeriod)
            INSERT_TOPIC_SETTING(SupportedCodecs)
            INSERT_TOPIC_SETTING(PartitionWriteSpeed)
            INSERT_TOPIC_SETTING(PartitionWriteBurstSpeed)
            INSERT_TOPIC_SETTING(MeteringMode)
            INSERT_TOPIC_SETTING(AutoPartitioningStabilizationWindow)
            INSERT_TOPIC_SETTING(AutoPartitioningUpUtilizationPercent)
            INSERT_TOPIC_SETTING(AutoPartitioningDownUtilizationPercent)
            INSERT_TOPIC_SETTING(AutoPartitioningStrategy)
            INSERT_TOPIC_SETTING(MetricsLevel)

#undef INSERT_TOPIC_SETTING

            opts = L(opts, Q(Y(Q("topicSettings"), Q(settings))));
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, TString(KikimrProviderName)),
                                            Scoped_->WrapCluster(Topic_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TTopicRef Topic_;
    const TCreateTopicParameters Params_;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildCreateTopic(
    TPosition pos, const TTopicRef& tr, const TCreateTopicParameters& params, TScopedStatePtr scoped) {
    return new TCreateTopicNode(pos, tr, params, scoped);
}

class TAlterTopicNode final: public TAstListNode {
public:
    TAlterTopicNode(TPosition pos, const TTopicRef& tr, const TAlterTopicParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Topic_(tr)
        , Params_(params)
        , Scoped_(scoped)
    {
        scoped->UseCluster(TString(KikimrProviderName), Topic_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Topic_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        if (!Params_.AddConsumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& consumer : Params_.AddConsumers) {
                if (!consumerNames.insert(consumer.Name.Name).second) {
                    ctx.Error(consumer.Name.Pos) << "Consumer " << consumer.Name.Name << " defined more than once";
                    return false;
                }
            }
        }
        if (!Params_.AlterConsumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& [_, consumer] : Params_.AlterConsumers) {
                if (!consumerNames.insert(consumer.Name.Name).second) {
                    ctx.Error(consumer.Name.Pos) << "Consumer " << consumer.Name.Name << " altered more than once";
                    return false;
                }
            }
        }
        if (!Params_.DropConsumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& consumer : Params_.DropConsumers) {
                if (!consumerNames.insert(consumer.Name).second) {
                    ctx.Error(consumer.Pos) << "Consumer " << consumer.Name << " dropped more than once";
                    return false;
                }
            }
        }

        auto opts = Y();
        TString mode = Params_.MissingOk ? "alter_if_exists" : "alter";
        opts = L(opts, Q(Y(Q("mode"), Q(mode))));

        for (const auto& consumer : Params_.AddConsumers) {
            const auto desc = CreateConsumerDesc(ctx, consumer, *this, false);
            if (!desc) {
                return false;
            }
            opts = L(opts, Q(Y(Q("addConsumer"), Q(desc))));
        }

        for (const auto& [_, consumer] : Params_.AlterConsumers) {
            const auto desc = CreateConsumerDesc(ctx, consumer, *this, true);
            if (!desc) {
                return false;
            }
            opts = L(opts, Q(Y(Q("alterConsumer"), Q(desc))));
        }

        for (const auto& consumer : Params_.DropConsumers) {
            const auto name = BuildQuotedAtom(consumer.Pos, consumer.Name);
            opts = L(opts, Q(Y(Q("dropConsumer"), name)));
        }

        if (Params_.TopicSettings.IsSet()) {
            auto settings = Y();

#define INSERT_TOPIC_SETTING(NAME)                                                            \
    if (const auto& NAME##Val = Params_.TopicSettings.NAME) {                                 \
        if (NAME##Val.IsSet()) {                                                              \
            settings = L(settings, Q(Y(Q(Y_STRINGIZE(set##NAME)), NAME##Val.GetValueSet()))); \
        } else {                                                                              \
            settings = L(settings, Q(Y(Q(Y_STRINGIZE(reset##NAME)), Q(Y()))));                \
        }                                                                                     \
    }

            INSERT_TOPIC_SETTING(MaxPartitions)
            INSERT_TOPIC_SETTING(MinPartitions)
            INSERT_TOPIC_SETTING(RetentionPeriod)
            INSERT_TOPIC_SETTING(SupportedCodecs)
            INSERT_TOPIC_SETTING(PartitionWriteSpeed)
            INSERT_TOPIC_SETTING(PartitionWriteBurstSpeed)
            INSERT_TOPIC_SETTING(MeteringMode)
            INSERT_TOPIC_SETTING(AutoPartitioningStabilizationWindow)
            INSERT_TOPIC_SETTING(AutoPartitioningUpUtilizationPercent)
            INSERT_TOPIC_SETTING(AutoPartitioningDownUtilizationPercent)
            INSERT_TOPIC_SETTING(AutoPartitioningStrategy)
            INSERT_TOPIC_SETTING(MetricsLevel)

#undef INSERT_TOPIC_SETTING

            opts = L(opts, Q(Y(Q("topicSettings"), Q(settings))));
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, TString(KikimrProviderName)),
                                            Scoped_->WrapCluster(Topic_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TTopicRef Topic_;
    const TAlterTopicParameters Params_;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildAlterTopic(
    TPosition pos, const TTopicRef& tr, const TAlterTopicParameters& params, TScopedStatePtr scoped) {
    return new TAlterTopicNode(pos, tr, params, scoped);
}

class TDropTopicNode final: public TAstListNode {
public:
    TDropTopicNode(TPosition pos, const TTopicRef& tr, const TDropTopicParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Topic_(tr)
        , Params_(params)
        , Scoped_(scoped)
    {
        scoped->UseCluster(TString(KikimrProviderName), Topic_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto keys = Topic_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::DROP);
        if (!keys || !keys->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        auto opts = Y();

        TString mode = Params_.MissingOk ? "drop_if_exists" : "drop";
        opts = L(opts, Q(Y(Q("mode"), Q(mode))));

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, TString(KikimrProviderName)),
                                            Scoped_->WrapCluster(Topic_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TTopicRef Topic_;
    TDropTopicParameters Params_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildDropTopic(TPosition pos, const TTopicRef& tr, const TDropTopicParameters& params, TScopedStatePtr scoped) {
    return new TDropTopicNode(pos, tr, params, scoped);
}

class TControlUser final: public TAstListNode {
public:
    TControlUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TUserParameters>& params, TScopedStatePtr scoped, bool IsCreateUser)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Name_(name)
        , Params_(params)
        , Scoped_(scoped)
        , IsCreateUser_(IsCreateUser)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource*) override {
        auto name = Name_.Build();
        TNodePtr password;
        TNodePtr hash;

        if (Params_) {
            if (Params_->Password) {
                password = Params_->Password->Build();
            } else if (Params_->Hash) {
                hash = Params_->Hash->Build();
            }
        }

        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        if (!name->Init(ctx, FakeSource_.Get()) ||
            !cluster->Init(ctx, FakeSource_.Get()) ||
            (password && !password->Init(ctx, FakeSource_.Get())) ||
            (hash && !hash->Init(ctx, FakeSource_.Get())))
        {
            return false;
        }

        auto options = Y(Q(Y(Q("mode"), Q(IsCreateUser_ ? "createUser" : "alterUser"))));

        TVector<TNodePtr> roles;
        if (Params_ && !Params_->Roles.empty()) {
            for (auto& item : Params_->Roles) {
                roles.push_back(item.Build());
                if (!roles.back()->Init(ctx, FakeSource_.Get())) {
                    return false;
                }
            }

            options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos_, std::move(roles))))));
        }

        if (Params_) {
            if (Params_->IsPasswordEncrypted) {
                options = L(options, Q(Y(Q("passwordEncrypted"))));
            }

            if (Params_->Password) {
                options = L(options, Q(Y(Q("password"), password)));
            } else if (Params_->Hash) {
                options = L(options, Q(Y(Q("hash"), hash)));
            } else if (Params_->IsPasswordNull) {
                options = L(options, Q(Y(Q("nullPassword"))));
            }

            if (Params_->CanLogin.has_value()) {
                options = L(options, Q(Y(Q(Params_->CanLogin.value() ? "login" : "noLogin"))));
            }
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    const TMaybe<TUserParameters> Params_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
    bool IsCreateUser_;
};

TNodePtr BuildControlUser(TPosition pos,
                          const TString& service,
                          const TDeferredAtom& cluster,
                          const TDeferredAtom& name,
                          const TMaybe<TUserParameters>& params,
                          TScopedStatePtr scoped,
                          bool isCreateUser)
{
    return new TControlUser(pos, service, cluster, name, params, scoped, isCreateUser);
}

class TCreateGroup final: public TAstListNode {
public:
    TCreateGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TCreateGroupParameters>& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Name_(name)
        , Params_(params)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource*) override {
        auto options = Y(Q(Y(Q("mode"), Q("createGroup"))));

        TVector<TNodePtr> roles;
        if (Params_ && !Params_->Roles.empty()) {
            for (auto& item : Params_->Roles) {
                roles.push_back(item.Build());
                if (!roles.back()->Init(ctx, FakeSource_.Get())) {
                    return false;
                }
            }

            options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos_, std::move(roles))))));
        }

        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", Name_.Build())))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    const TMaybe<TCreateGroupParameters> Params_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildCreateGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TCreateGroupParameters>& params, TScopedStatePtr scoped) {
    return new TCreateGroup(pos, service, cluster, name, params, scoped);
}

class TAlterSequence final: public TAstListNode {
public:
    TAlterSequence(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TString& id, const TSequenceParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Id_(id)
        , Params_(params)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);

        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        if (!cluster->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        auto options = Y();
        TString mode = Params_.MissingOk ? "alter_if_exists" : "alter";
        options = L(options, Q(Y(Q("mode"), Q(mode))));

        if (Params_.IsRestart) {
            if (Params_.RestartValue) {
                TString strValue = Params_.RestartValue->Build()->GetLiteralValue();
                ui64 value = FromString<ui64>(strValue);
                ui64 maxValue = ui64(std::numeric_limits<i64>::max());
                ui64 minValue = 1;
                if (value > maxValue) {
                    ctx.Error(Pos_) << "Restart value: " << value << " cannot be greater than max value: " << maxValue;
                    return false;
                }
                if (value < minValue) {
                    ctx.Error(Pos_) << "Restart value: " << value << " cannot be less than min value: " << minValue;
                    return false;
                }
                options = L(options, Q(Y(Q("restart"), Q(ToString(value)))));
            } else {
                options = L(options, Q(Y(Q("restart"), Q(TString()))));
            }
        }
        if (Params_.StartValue) {
            TString strValue = Params_.StartValue->Build()->GetLiteralValue();
            ui64 value = FromString<ui64>(strValue);
            ui64 maxValue = ui64(std::numeric_limits<i64>::max());
            ui64 minValue = 1;
            if (value > maxValue) {
                ctx.Error(Pos_) << "Start value: " << value << " cannot be greater than max value: " << maxValue;
                return false;
            }
            if (value < minValue) {
                ctx.Error(Pos_) << "Start value: " << value << " cannot be less than min value: " << minValue;
                return false;
            }
            options = L(options, Q(Y(Q("start"), Q(ToString(value)))));
        }

        if (Params_.Increment) {
            TString strValue = Params_.Increment->Build()->GetLiteralValue();
            ui64 value = FromString<ui64>(strValue);
            ui64 maxValue = ui64(std::numeric_limits<i64>::max());
            if (value > maxValue) {
                ctx.Error(Pos_) << "Increment: " << value << " cannot be greater than max value: " << maxValue;
                return false;
            }
            if (value == 0) {
                ctx.Error(Pos_) << "Increment must not be zero";
                return false;
            }
            options = L(options, Q(Y(Q("increment"), Q(ToString(value)))));
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, TString(KikimrProviderName)),
                                            Scoped_->WrapCluster(Cluster_, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("sequence"), Y("String", BuildQuotedAtom(Pos_, Id_))))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TString Id_;
    const TSequenceParameters Params_;

    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildAlterSequence(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TString& id, const TSequenceParameters& params, TScopedStatePtr scoped) {
    return new TAlterSequence(pos, service, cluster, id, params, scoped);
}

class TRenameRole final: public TAstListNode {
public:
    TRenameRole(TPosition pos, bool isUser, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , IsUser_(isUser)
        , Service_(service)
        , Cluster_(cluster)
        , Name_(name)
        , NewName_(newName)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto name = Name_.Build();
        auto newName = NewName_.Build();
        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        if (!name->Init(ctx, FakeSource_.Get()) ||
            !newName->Init(ctx, FakeSource_.Get()) ||
            !cluster->Init(ctx, FakeSource_.Get()))
        {
            return false;
        }

        auto options = Y(Q(Y(Q("mode"), Q(IsUser_ ? "renameUser" : "renameGroup"))));
        options = L(options, Q(Y(Q("newName"), newName)));

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const bool IsUser_;
    const TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    TDeferredAtom NewName_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
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
        , Service_(service)
        , Cluster_(cluster)
        , Name_(name)
        , ToChange_(toChange)
        , IsDrop_(isDrop)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto name = Name_.Build();
        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        if (!name->Init(ctx, FakeSource_.Get()) || !cluster->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        TVector<TNodePtr> toChange;
        for (auto& item : ToChange_) {
            toChange.push_back(item.Build());
            if (!toChange.back()->Init(ctx, FakeSource_.Get())) {
                return false;
            }
        }

        auto options = Y(Q(Y(Q("mode"), Q(IsDrop_ ? "dropUsersFromGroup" : "addUsersToGroup"))));
        options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos_, std::move(toChange))))));

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    TVector<TDeferredAtom> ToChange_;
    const bool IsDrop_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
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
        , Service_(service)
        , Cluster_(cluster)
        , ToDrop_(toDrop)
        , IsUser_(isUser)
        , MissingOk_(missingOk)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        if (!cluster->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        const char* mode = IsUser_
                               ? (MissingOk_ ? "dropUserIfExists" : "dropUser")
                               : (MissingOk_ ? "dropGroupIfExists" : "dropGroup");

        auto options = Y(Q(Y(Q("mode"), Q(mode))));

        auto block = Y(Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)));
        for (auto& item : ToDrop_) {
            auto name = item.Build();
            if (!name->Init(ctx, FakeSource_.Get())) {
                return false;
            }

            block = L(block, Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))));
        }
        block = L(block, Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")));
        Add("block", Q(block));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TVector<TDeferredAtom> ToDrop_;
    const bool IsUser_;
    const bool MissingOk_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildUpsertObjectOperation(TPosition pos, const TString& objectId, const TString& typeId,
                                    TObjectFeatureNodePtr features, const TObjectOperatorContext& context) {
    return new TUpsertObject(pos, objectId, typeId, context, TObjectFeatureNode::SkipEmpty(features));
}

TNodePtr BuildCreateObjectOperation(TPosition pos, const TString& objectId, const TString& typeId,
                                    bool existingOk, bool replaceIfExists, TObjectFeatureNodePtr features, const TObjectOperatorContext& context) {
    return new TCreateObject(pos, objectId, typeId, context, TObjectFeatureNode::SkipEmpty(features), existingOk, replaceIfExists);
}

TNodePtr BuildAlterObjectOperation(TPosition pos, const TString& secretId, const TString& typeId,
                                   bool missingOk, TObjectFeatureNodePtr features, std::set<TString>&& featuresToReset, const TObjectOperatorContext& context) {
    return new TAlterObject(pos, secretId, typeId, context, TObjectFeatureNode::SkipEmpty(features), std::move(featuresToReset), missingOk);
}

TNodePtr BuildDropObjectOperation(TPosition pos, const TString& secretId, const TString& typeId,
                                  bool missingOk, TObjectFeatureNodePtr features, const TObjectOperatorContext& context) {
    return new TDropObject(pos, secretId, typeId, context, TObjectFeatureNode::SkipEmpty(features), missingOk);
}

TNodePtr BuildDropRoles(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& toDrop, bool isUser, bool missingOk, TScopedStatePtr scoped) {
    return new TDropRoles(pos, service, cluster, toDrop, isUser, missingOk, scoped);
}

class TPermissionsAction final: public TAstListNode {
public:
    struct TPermissionParameters {
        TString PermissionAction;
        TVector<TDeferredAtom> Permissions;
        TVector<TDeferredAtom> SchemaPaths;
        TVector<TDeferredAtom> RoleNames;
    };

    TPermissionsAction(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TPermissionParameters& parameters, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Parameters_(parameters)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);

        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);
        TNodePtr permissionAction = TDeferredAtom(Pos_, Parameters_.PermissionAction).Build();

        if (!permissionAction->Init(ctx, FakeSource_.Get()) ||
            !cluster->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        TVector<TNodePtr> paths;
        paths.reserve(Parameters_.SchemaPaths.size());
        for (auto& item : Parameters_.SchemaPaths) {
            paths.push_back(item.Build());
            if (!paths.back()->Init(ctx, FakeSource_.Get())) {
                return false;
            }
        }
        auto options = Y(Q(Y(Q("paths"), Q(new TAstListNodeImpl(Pos_, std::move(paths))))));

        TVector<TNodePtr> permissions;
        permissions.reserve(Parameters_.Permissions.size());
        for (auto& item : Parameters_.Permissions) {
            permissions.push_back(item.Build());
            if (!permissions.back()->Init(ctx, FakeSource_.Get())) {
                return false;
            }
        }
        options = L(options, Q(Y(Q("permissions"), Q(new TAstListNodeImpl(Pos_, std::move(permissions))))));

        TVector<TNodePtr> roles;
        roles.reserve(Parameters_.RoleNames.size());
        for (auto& item : Parameters_.RoleNames) {
            roles.push_back(item.Build());
            if (!roles.back()->Init(ctx, FakeSource_.Get())) {
                return false;
            }
        }
        options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos_, std::move(roles))))));

        auto block = Y(Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)));
        block = L(block, Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("permission"), Y("String", permissionAction)))), Y("Void"), Q(options))));
        block = L(block, Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")));
        Add("block", Q(block));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TPermissionParameters Parameters_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
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
    : public TAstListNode,
      protected TObjectOperatorContext {
protected:
    virtual INode::TPtr FillOptions(INode::TPtr options) const = 0;

public:
    explicit TAsyncReplication(TPosition pos, const TString& id, const TString& mode, const TObjectOperatorContext& context)
        : TAstListNode(pos)
        , TObjectOperatorContext(context)
        , Id_(id)
        , Mode_(mode)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Scoped_->UseCluster(ServiceId, Cluster);

        auto keys = Y("Key", Q(Y(Q("replication"), Y("String", BuildQuotedAtom(Pos_, Id_)))));
        auto options = FillOptions(Y(Q(Y(Q("mode"), Q(Mode_)))));

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Id_;
    const TString Mode_;

}; // TAsyncReplication

class TCreateAsyncReplication final: public TAsyncReplication {
public:
    explicit TCreateAsyncReplication(TPosition pos, const TString& id,
                                     std::vector<std::pair<TString, TString>>&& targets,
                                     std::map<TString, TNodePtr>&& settings,
                                     const TObjectOperatorContext& context)
        : TAsyncReplication(pos, id, "create", context)
        , Targets_(std::move(targets))
        , Settings_(std::move(settings))
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        if (!Targets_.empty()) {
            auto targets = Y();
            for (auto&& [remote, local] : Targets_) {
                auto target = Y();
                target = L(target, Q(Y(Q("remote"), Q(remote))));
                target = L(target, Q(Y(Q("local"), Q(local))));
                targets = L(targets, Q(target));
            }
            options = L(options, Q(Y(Q("targets"), Q(targets))));
        }

        if (!Settings_.empty()) {
            auto settings = Y();
            for (auto&& [k, v] : Settings_) {
                if (v) {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k), v)));
                } else {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k))));
                }
            }
            options = L(options, Q(Y(Q("settings"), Q(settings))));
        }

        return options;
    }

private:
    std::vector<std::pair<TString, TString>> Targets_; // (remote, local)
    std::map<TString, TNodePtr> Settings_;

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
        , Settings_(std::move(settings))
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        if (!Settings_.empty()) {
            auto settings = Y();
            for (auto&& [k, v] : Settings_) {
                if (v) {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k), v)));
                } else {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k))));
                }
            }
            options = L(options, Q(Y(Q("settings"), Q(settings))));
        }

        return options;
    }

private:
    std::map<TString, TNodePtr> Settings_;

}; // TAlterAsyncReplication

TNodePtr BuildAlterAsyncReplication(TPosition pos, const TString& id,
                                    std::map<TString, TNodePtr>&& settings,
                                    const TObjectOperatorContext& context)
{
    return new TAlterAsyncReplication(pos, id, std::move(settings), context);
}

class TTransfer
    : public TAstListNode,
      protected TObjectOperatorContext {
protected:
    virtual INode::TPtr FillOptions(INode::TPtr options) const = 0;

public:
    explicit TTransfer(TPosition pos, const TString& id, const TString& mode, const TObjectOperatorContext& context)
        : TAstListNode(pos)
        , TObjectOperatorContext(context)
        , Id_(id)
        , Mode_(mode)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Scoped_->UseCluster(ServiceId, Cluster);

        auto keys = Y("Key", Q(Y(Q("transfer"), Y("String", BuildQuotedAtom(Pos_, Id_)))));
        auto options = FillOptions(Y(Q(Y(Q("mode"), Q(Mode_)))));

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Id_;
    const TString Mode_;

}; // TTransfer

class TCreateTransfer final: public TTransfer {
public:
    explicit TCreateTransfer(TPosition pos, const TString& id, const TString&& source, const TString&& target,
                             const TString&& transformLambda,
                             std::map<TString, TNodePtr>&& settings,
                             const TObjectOperatorContext& context)
        : TTransfer(pos, id, "create", context)
        , Source_(std::move(source))
        , Target_(std::move(target))
        , TransformLambda_(std::move(transformLambda))
        , Settings_(std::move(settings))
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        options = L(options, Q(Y(Q("source"), Q(Source_))));
        options = L(options, Q(Y(Q("target"), Q(Target_))));
        options = L(options, Q(Y(Q("transformLambda"), Q(TransformLambda_))));

        if (!Settings_.empty()) {
            auto settings = Y();
            for (auto&& [k, v] : Settings_) {
                if (v) {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k), v)));
                } else {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k))));
                }
            }
            options = L(options, Q(Y(Q("settings"), Q(settings))));
        }

        return options;
    }

private:
    const TString Source_;
    const TString Target_;
    const TString TransformLambda_;
    std::map<TString, TNodePtr> Settings_;

}; // TCreateTransfer

TNodePtr BuildCreateTransfer(TPosition pos, const TString& id, const TString&& source, const TString&& target,
                             const TString&& transformLambda,
                             std::map<TString, TNodePtr>&& settings,
                             const TObjectOperatorContext& context)
{
    return new TCreateTransfer(pos, id, std::move(source), std::move(target), std::move(transformLambda), std::move(settings), context);
}

class TDropTransfer final: public TTransfer {
public:
    explicit TDropTransfer(TPosition pos, const TString& id, bool cascade, const TObjectOperatorContext& context)
        : TTransfer(pos, id, cascade ? "dropCascade" : "drop", context)
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        return options;
    }

}; // TDropTransfer

TNodePtr BuildDropTransfer(TPosition pos, const TString& id, bool cascade, const TObjectOperatorContext& context) {
    return new TDropTransfer(pos, id, cascade, context);
}

class TAlterTransfer final: public TTransfer {
public:
    explicit TAlterTransfer(TPosition pos, const TString& id, std::optional<TString>&& transformLambda,
                            std::map<TString, TNodePtr>&& settings,
                            const TObjectOperatorContext& context)
        : TTransfer(pos, id, "alter", context)
        , TransformLambda_(std::move(transformLambda))
        , Settings_(std::move(settings))
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        options = L(options, Q(Y(Q("transformLambda"), Q(TransformLambda_ ? TransformLambda_.value() : ""))));

        if (!Settings_.empty()) {
            auto settings = Y();
            for (auto&& [k, v] : Settings_) {
                if (v) {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k), v)));
                } else {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k))));
                }
            }
            options = L(options, Q(Y(Q("settings"), Q(settings))));
        }

        return options;
    }

private:
    const std::optional<TString> TransformLambda_;
    std::map<TString, TNodePtr> Settings_;

}; // TAlterTransfer

TNodePtr BuildAlterTransfer(TPosition pos, const TString& id, std::optional<TString>&& transformLambda,
                            std::map<TString, TNodePtr>&& settings,
                            const TObjectOperatorContext& context)
{
    return new TAlterTransfer(pos, id, std::move(transformLambda), std::move(settings), context);
}

static const TMap<EWriteColumnMode, TString> columnModeToStrMapMR{
    {EWriteColumnMode::Default, ""},
    {EWriteColumnMode::Insert, "append"},
    {EWriteColumnMode::Renew, "renew"}, // insert with truncat
    {EWriteColumnMode::Replace, "replace"}};

static const TMap<EWriteColumnMode, TString> columnModeToStrMapStat{
    {EWriteColumnMode::Upsert, "upsert"}};

static const TMap<EWriteColumnMode, TString> columnModeToStrMapKikimr{
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
        , Label_(label)
        , Table_(table)
        , Mode_(mode)
        , Options_(options)
        , Scoped_(scoped)
    {
        scoped->UseCluster(Table_.Service, Table_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::WRITE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        auto getModesMap = [](const TString& serviceName) -> const TMap<EWriteColumnMode, TString>& {
            if (serviceName == KikimrProviderName || serviceName == YdbProviderName) {
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
            auto modeStr = getModesMap(Table_.Service).FindPtr(Mode_);

            options->Add(Q(Y(Q("mode"), Q(modeStr ? *modeStr : "unsupported"))));
        }

        Add("block", Q((Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Table_.Service), Scoped_->WrapCluster(Table_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Label_, Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))))));

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
    TScopedStatePtr Scoped_;
};

TNodePtr BuildWriteTable(TPosition pos, const TString& label, const TTableRef& table, EWriteColumnMode mode, TNodePtr options,
                         TScopedStatePtr scoped)
{
    return new TWriteTableNode(pos, label, table, mode, std::move(options), scoped);
}

class TClustersSinkOperationBase: public TAstListNode {
protected:
    explicit TClustersSinkOperationBase(TPosition pos)
        : TAstListNode(pos)
    {
    }

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
    explicit TCommitClustersNode(TPosition pos)
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
    explicit TRollbackClustersNode(TPosition pos)
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
        , Label_(label)
        , Settings_(settings)
        , CommitClusters_(BuildCommitClusters(Pos_))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto block(Y(
            Y("let", "result_sink", Y("DataSink", Q(TString(ResultProviderName)))),
            Y("let", "world", Y(TString(WriteName), "world", "result_sink", Y("Key"), Label_, Q(Settings_)))));
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

TNodePtr BuildWriteResult(TPosition pos, const TString& label, TNodePtr settings) {
    return new TWriteResultNode(pos, label, settings);
}

class TYqlProgramNode: public TAstListNode {
public:
    TYqlProgramNode(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel, TScopedStatePtr scoped, bool useSeq)
        : TAstListNode(pos)
        , Blocks_(blocks)
        , TopLevel_(topLevel)
        , Scoped_(scoped)
        , UseSeq_(useSeq)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        bool hasError = false;
        INode::TPtr currentWorldsHolder;
        INode::TPtr seqNode;
        if (UseSeq_) {
            currentWorldsHolder = new TAstListNodeImpl(GetPos());
            seqNode = new TAstListNodeImpl(GetPos());
            seqNode->Add("Seq!", "world");
        }

        INode* currentWorlds = UseSeq_ ? currentWorldsHolder.Get() : this;
        auto flushCurrentWorlds = [&](bool changeSeq, bool finish) {
            currentWorldsHolder->Add(Y("return", "world"));
            auto lambda = BuildLambda(GetPos(), Y("world"), Y("block", Q(currentWorldsHolder)));
            seqNode->Add(lambda);

            if (finish) {
                Add(Y("let", "world", seqNode));
            } else {
                currentWorldsHolder = new TAstListNodeImpl(GetPos());
                currentWorlds = currentWorldsHolder.Get();
            }

            if (changeSeq) {
                Add(Y("let", "world", seqNode));
                seqNode = new TAstListNodeImpl(GetPos());
                seqNode->Add("Seq!", "world");
            }
        };

        if (TopLevel_) {
            for (auto& var : ctx.Variables) {
                if (!var.second.second->Init(ctx, src)) {
                    hasError = true;
                    continue;
                }
                Add(Y(
                    "declare",
                    new TAstAtomNodeImpl(var.second.first, var.first, TNodeFlags::ArbitraryContent),
                    var.second.second));
            }

            for (const auto& overrideLibrary : ctx.OverrideLibraries) {
                auto node = Y(
                    "override_library",
                    new TAstAtomNodeImpl(
                        std::get<TPosition>(overrideLibrary.second),
                        overrideLibrary.first, TNodeFlags::ArbitraryContent));

                Add(node);
            }

            for (const auto& package : ctx.Packages) {
                const auto& [url, urlPosition] = std::get<1U>(package.second);

                auto node = Y(
                    "package",
                    new TAstAtomNodeImpl(
                        std::get<TPosition>(package.second), package.first,
                        TNodeFlags::ArbitraryContent),
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
                Add(Y("set_package_version", BuildQuotedAtom(Pos_, p.first), BuildQuotedAtom(Pos_, ToString(p.second))));
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

            if (!ctx.CompactNamedExprs) {
                for (auto& nodes : Scoped_->NamedNodes) {
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
                auto configSource = Y("DataSource", BuildQuotedAtom(Pos_, TString(ConfigProviderName)));
                auto resultSink = Y("DataSink", BuildQuotedAtom(Pos_, TString(ResultProviderName)));

                for (const auto& warningPragma : ctx.WarningPolicy.GetRules()) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "Warning"), BuildQuotedAtom(Pos_, warningPragma.GetPattern()),
                                                           BuildQuotedAtom(Pos_, to_lower(ToString(warningPragma.GetAction()))))));
                }

                if (ctx.RuntimeLogLevel) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "RuntimeLogLevel"), BuildQuotedAtom(Pos_, ctx.RuntimeLogLevel))));
                }

                if (ctx.ResultSizeLimit > 0) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", resultSink,
                                                           BuildQuotedAtom(Pos_, "SizeLimit"), BuildQuotedAtom(Pos_, ToString(ctx.ResultSizeLimit)))));
                }

                if (!ctx.PragmaPullUpFlatMapOverJoin) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "DisablePullUpFlatMapOverJoin"))));
                }

                if (ctx.FilterPushdownOverJoinOptionalSide) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "FilterPushdownOverJoinOptionalSide"))));
                }

                if (!ctx.RotateJoinTree) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "RotateJoinTree"), BuildQuotedAtom(Pos_, "false"))));
                }

                if (ctx.DiscoveryMode) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "DiscoveryMode"))));
                }

                if (ctx.DqEngineEnable) {
                    TString mode = "auto";
                    if (ctx.PqReadByRtmrCluster && ctx.PqReadByRtmrCluster != "dq") {
                        mode = "disable";
                    } else if (ctx.DqEngineForce) {
                        mode = "force";
                    }
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "DqEngine"), BuildQuotedAtom(Pos_, mode))));
                }

                if (ctx.CostBasedOptimizer) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "CostBasedOptimizer"), BuildQuotedAtom(Pos_, ctx.CostBasedOptimizer))));
                }

                if (ctx.CostBasedOptimizerVersion) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "CostBasedOptimizerVersion"), BuildQuotedAtom(Pos_, ToString(*ctx.CostBasedOptimizerVersion)))));
                }

                if (ctx.JsonQueryReturnsJsonDocument.Defined()) {
                    TString pragmaName = "DisableJsonQueryReturnsJsonDocument";
                    if (*ctx.JsonQueryReturnsJsonDocument) {
                        pragmaName = "JsonQueryReturnsJsonDocument";
                    }

                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource, BuildQuotedAtom(Pos_, pragmaName))));
                }

                if (ctx.OrderedColumns) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "OrderedColumns"))));
                }

                if (ctx.DeriveColumnOrder) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "DeriveColumnOrder"))));
                }

                if (ctx.PqReadByRtmrCluster) {
                    auto pqSourceAll = Y("DataSource", BuildQuotedAtom(Pos_, TString(PqProviderName)), BuildQuotedAtom(Pos_, "$all"));
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", pqSourceAll,
                                                           BuildQuotedAtom(Pos_, "Attr"), BuildQuotedAtom(Pos_, "PqReadByRtmrCluster_"), BuildQuotedAtom(Pos_, ctx.PqReadByRtmrCluster))));

                    auto rtmrSourceAll = Y("DataSource", BuildQuotedAtom(Pos_, TString(RtmrProviderName)), BuildQuotedAtom(Pos_, "$all"));
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", rtmrSourceAll,
                                                           BuildQuotedAtom(Pos_, "Attr"), BuildQuotedAtom(Pos_, "PqReadByRtmrCluster_"), BuildQuotedAtom(Pos_, ctx.PqReadByRtmrCluster))));

                    if (ctx.PqReadByRtmrCluster != "dq") {
                        // set any dynamic settings for particular RTMR cluster for CommitAll!
                        auto rtmrSource = Y("DataSource", BuildQuotedAtom(Pos_, TString(RtmrProviderName)), BuildQuotedAtom(Pos_, ctx.PqReadByRtmrCluster));
                        currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", rtmrSource,
                                                               BuildQuotedAtom(Pos_, "Attr"), BuildQuotedAtom(Pos_, "Dummy_"), BuildQuotedAtom(Pos_, "1"))));
                    }
                }

                if (ctx.YsonCastToString.Defined()) {
                    const TString pragmaName = *ctx.YsonCastToString ? "YsonCastToString" : "DisableYsonCastToString";
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource, BuildQuotedAtom(Pos_, pragmaName))));
                }

                if (ctx.UseBlocks) {
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource, BuildQuotedAtom(Pos_, "UseBlocks"))));
                }

                if (ctx.BlockEngineEnable) {
                    TString mode = ctx.BlockEngineForce ? "force" : "auto";
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                                           BuildQuotedAtom(Pos_, "BlockEngine"), BuildQuotedAtom(Pos_, mode))));
                }

                if (ctx.Engine) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                            BuildQuotedAtom(Pos_, "Engine"), BuildQuotedAtom(Pos_, *ctx.Engine))));
                }

                if (ctx.DebugPositions) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                            BuildQuotedAtom(Pos_, "DebugPositions"))));
                }

                if (ctx.WindowNewPipeline) {
                    Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource,
                                            BuildQuotedAtom(Pos_, "WindowNewPipeline"))));
                }

                if (ctx.DirectRowDependsOn.Defined()) {
                    const TString pragmaName = *ctx.DirectRowDependsOn ? "DirectRowDependsOn" : "DisableDirectRowDependsOn";
                    currentWorlds->Add(Y("let", "world", Y(TString(ConfigureName), "world", configSource, BuildQuotedAtom(Pos_, pragmaName))));
                }
            }
        }

        for (auto& block : Blocks_) {
            if (block->SubqueryAlias()) {
                continue;
            }
            if (!block->Init(ctx, nullptr)) {
                hasError = true;
                continue;
            }
        }

        for (const auto& x : Scoped_->Local.ExprClusters) {
            auto& data = Scoped_->Local.ExprClustersMap[x.Get()];
            auto& node = data.second;

            if (!node->Init(ctx, nullptr)) {
                hasError = true;
                continue;
            }

            Add(Y("let", data.first, node));
        }

        if (UseSeq_) {
            flushCurrentWorlds(false, false);
        }

        for (auto& block : Blocks_) {
            const auto subqueryAliasPtr = block->SubqueryAlias();
            if (subqueryAliasPtr) {
                if (block->UsedSubquery()) {
                    if (UseSeq_) {
                        flushCurrentWorlds(true, false);
                    }

                    const auto& ref = block->GetLabel();
                    YQL_ENSURE(!ref.empty());
                    Add(block);
                    currentWorlds->Add(Y("let", "world", Y("Nth", *subqueryAliasPtr, Q("0"))));
                    Add(Y("let", ref, Y("Nth", *subqueryAliasPtr, Q("1"))));
                }
            } else {
                const auto& ref = block->GetLabel();
                if (ref) {
                    Add(Y("let", ref, block));
                } else {
                    currentWorlds->Add(Y("let", "world", block));
                    if (UseSeq_) {
                        flushCurrentWorlds(false, false);
                    }
                }
            }
        }

        if (UseSeq_) {
            flushCurrentWorlds(false, true);
        }

        if (TopLevel_) {
            if (ctx.UniversalAliases) {
                decltype(Nodes_) preparedNodes;
                preparedNodes.swap(Nodes_);
                for (const auto& [name, node] : ctx.UniversalAliases) {
                    Add(Y("let", name, node));
                }
                Nodes_.insert(Nodes_.end(), preparedNodes.begin(), preparedNodes.end());
            }

            decltype(Nodes_) imports;
            for (const auto& [alias, path] : ctx.RequiredModules) {
                imports.push_back(Y("import", alias, BuildQuotedAtom(Pos_, path)));
            }
            Nodes_.insert(Nodes_.begin(), std::make_move_iterator(imports.begin()), std::make_move_iterator(imports.end()));

            for (const auto& symbol : ctx.Exports) {
                if (ctx.CompactNamedExprs) {
                    auto node = Scoped_->LookupNode(symbol);
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
    TScopedStatePtr Scoped_;
    const bool UseSeq_;
};

TNodePtr BuildQuery(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel, TScopedStatePtr scoped, bool useSeq) {
    return new TYqlProgramNode(pos, blocks, topLevel, scoped, useSeq);
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
        } else if (Name_ == TStringBuf("AddFileByUrl") || Name_ == TStringBuf("SetFileOption") || Name_ == TStringBuf("AddFolderByUrl") || Name_ == TStringBuf("ImportUdfs") || Name_ == TStringBuf("SetPackageVersion") || Name_ == TStringBuf("Layer")) {
            Node_ = L(Node_, BuildQuotedAtom(Pos_, Name_));
            for (ui32 i = 0; i < Values_.size(); ++i) {
                Node_ = L(Node_, Values_[i].Build());
            }
        } else if (Name_ == TStringBuf("auth")) {
            Node_ = L(Node_, BuildQuotedAtom(Pos_, "Auth"));
            Node_ = L(Node_, Values_.empty() ? BuildQuotedAtom(Pos_, TString()) : Values_.front().Build());
        } else {
            Node_ = L(Node_, BuildQuotedAtom(Pos_, "Attr"));
            Node_ = L(Node_, BuildQuotedAtom(Pos_, Name_));
            if (!ValueDefault_) {
                Node_ = L(Node_, Values_.empty() ? BuildQuotedAtom(Pos_, TString()) : Values_.front().Build());
            }
        }

        if (!Node_->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const final {
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
        for (auto& exprPtr : ExprSeq_) {
            if (!exprPtr->Init(ctx, FakeSource_.Get())) {
                return {};
            }
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
        for (const auto& arg : Args_) {
            args = L(args, BuildAtom(GetPos(), arg));
        }
        Add("lambda", Q(args), body);
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TSqlLambda(Pos_, TVector<TString>(Args_), CloneContainer(ExprSeq_));
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

class TWorldIf final: public TAstListNode {
public:
    TWorldIf(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode, bool isEvaluate)
        : TAstListNode(pos)
        , Predicate_(predicate)
        , ThenNode_(thenNode)
        , ElseNode_(elseNode)
        , IsEvaluate_(isEvaluate)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Predicate_->Init(ctx, FakeSource_.Get())) {
            return {};
        }
        Add(IsEvaluate_ ? "EvaluateIf!" : "If!");
        Add("world");
        auto coalesced = Y("Coalesce", Predicate_, Y("Bool", Q("false")));
        Add(IsEvaluate_ ? Y("EvaluateExpr", Y("EnsureType", coalesced, Y("DataType", Q("Bool")))) : coalesced);

        if (!ThenNode_->Init(ctx, FakeSource_.Get())) {
            return {};
        }

        Add(ThenNode_);
        if (ElseNode_) {
            if (!ElseNode_->Init(ctx, FakeSource_.Get())) {
                return {};
            }

            Add(ElseNode_);
        }

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TWorldIf(GetPos(), SafeClone(Predicate_), SafeClone(ThenNode_), SafeClone(ElseNode_), IsEvaluate_);
    }

private:
    TNodePtr Predicate_;
    TNodePtr ThenNode_;
    TNodePtr ElseNode_;
    bool IsEvaluate_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildWorldIfNode(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode, bool isEvaluate) {
    return new TWorldIf(pos, predicate, thenNode, elseNode, isEvaluate);
}

class TWorldFor final: public TAstListNode {
public:
    TWorldFor(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode, bool isEvaluate, bool isParallel)
        : TAstListNode(pos)
        , List_(list)
        , BodyNode_(bodyNode)
        , ElseNode_(elseNode)
        , IsEvaluate_(isEvaluate)
        , IsParallel_(isParallel)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!List_->Init(ctx, FakeSource_.Get())) {
            return {};
        }
        Add(TStringBuilder() << (IsEvaluate_ ? "Evaluate" : "") << (IsParallel_ ? "Parallel" : "") << "For!");
        Add("world");
        Add(IsEvaluate_ ? Y("EvaluateExpr", List_) : List_);

        if (!BodyNode_->Init(ctx, FakeSource_.Get())) {
            return {};
        }
        Add(BodyNode_);

        if (ElseNode_) {
            if (!ElseNode_->Init(ctx, FakeSource_.Get())) {
                return {};
            }
            Add(ElseNode_);
        }

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TWorldFor(GetPos(), SafeClone(List_), SafeClone(BodyNode_), SafeClone(ElseNode_), IsEvaluate_, IsParallel_);
    }

private:
    TNodePtr List_;
    TNodePtr BodyNode_;
    TNodePtr ElseNode_;
    bool IsEvaluate_;
    bool IsParallel_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildWorldForNode(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode, bool isEvaluate, bool isParallel) {
    return new TWorldFor(pos, list, bodyNode, elseNode, isEvaluate, isParallel);
}

class TAnalyzeNode final: public TAstListNode {
public:
    TAnalyzeNode(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TAnalyzeParams& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Params_(params)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(Service_, Cluster_);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto keys = Params_.Table->Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::DROP);
        if (!keys || !keys->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        auto opts = Y();

        auto columns = Y();
        for (const auto& column : Params_.Columns) {
            columns->Add(Q(column));
        }
        opts->Add(Q(Y(Q("columns"), Q(columns))));

        opts->Add(Q(Y(Q("mode"), Q("analyze"))));
        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), Scoped_->WrapCluster(Cluster_, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TString Service_;
    TDeferredAtom Cluster_;
    TAnalyzeParams Params_;

    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildAnalyze(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TAnalyzeParams& params, TScopedStatePtr scoped) {
    return new TAnalyzeNode(pos, service, cluster, params, scoped);
}

class TShowCreateNode final: public TAstListNode {
public:
    TShowCreateNode(TPosition pos, const TTableRef& tr, const TString& type, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Table_(tr)
        , Type_(type)
        , Scoped_(scoped)
        , FakeSource_(BuildFakeSource(pos))
    {
        Scoped_->UseCluster(Table_.Service, Table_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Table_.Options) {
            if (!Table_.Options->Init(ctx, src)) {
                return false;
            }
            Table_.Options = L(Table_.Options, Q(Y(Q(Type_))));
        } else {
            Table_.Options = Y(Q(Y(Q(Type_))));
        }

        bool asRef = ctx.PragmaRefSelect;
        bool asAutoRef = true;
        if (ctx.PragmaSampleSelect) {
            asRef = false;
            asAutoRef = false;
        }

        auto settings = Y(Q(Y(Q("type"))));
        if (asRef) {
            settings = L(settings, Q(Y(Q("ref"))));
        } else if (asAutoRef) {
            settings = L(settings, Q(Y(Q("autoref"))));
        }

        TNodePtr node(BuildInputTables(Pos_, {Table_}, false, Scoped_));
        if (!node->Init(ctx, src)) {
            return false;
        }

        auto source = BuildTableSource(TPosition(ctx.Pos()), Table_);
        if (!source) {
            return false;
        }
        auto output = source->Build(ctx);
        if (!output) {
            return false;
        }
        node = L(node, Y("let", "output", output));

        auto writeResult(BuildWriteResult(Pos_, "output", settings));
        if (!writeResult->Init(ctx, src)) {
            return false;
        }
        node = L(node, Y("let", "world", writeResult));
        node = L(node, Y("return", "world"));
        Add("block", Q(node));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TTableRef Table_;
    // showCreateTable, showCreateView, ...
    TString Type_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildShowCreate(TPosition pos, const TTableRef& tr, const TString& type, TScopedStatePtr scoped) {
    return new TShowCreateNode(pos, tr, type, scoped);
}

class TBaseBackupCollectionNode
    : public TAstListNode,
      public TObjectOperatorContext {
    using TBase = TAstListNode;

public:
    TBaseBackupCollectionNode(
        TPosition pos,
        const TString& prefix,
        const TString& objectId,
        const TObjectOperatorContext& context)
        : TBase(pos)
        , TObjectOperatorContext(context)
        , Prefix_(prefix)
        , Id_(objectId)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        auto keys = Y("Key");
        keys = L(keys, Q(Y(Q("backupCollection"), Y("String", BuildQuotedAtom(Pos_, Id_)), Y("String", BuildQuotedAtom(Pos_, Prefix_)))));
        auto options = this->FillOptions(ctx, Y());

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    virtual INode::TPtr FillOptions(TContext& ctx, INode::TPtr options) const = 0;

protected:
    TString Prefix_;
    TString Id_;
};

class TCreateBackupCollectionNode
    : public TBaseBackupCollectionNode {
    using TBase = TBaseBackupCollectionNode;

public:
    TCreateBackupCollectionNode(
        TPosition pos,
        const TString& prefix,
        const TString& objectId,
        const TCreateBackupCollectionParameters& params,
        const TObjectOperatorContext& context)
        : TBase(pos, prefix, objectId, context)
        , Params_(params)
    {
    }

    INode::TPtr FillOptions(TContext& ctx, INode::TPtr options) const final {
        options->Add(Q(Y(Q("mode"), Q("create"))));

        auto settings = Y();
        for (auto& [key, value] : Params_.Settings) {
            settings->Add(Q(Y(BuildQuotedAtom(Pos_, key), Y("String", value.Build()))));
        }
        options->Add(Q(Y(Q("settings"), Q(settings))));

        auto entries = Y();
        if (Params_.Database) {
            entries->Add(Q(Y(Q(Y(Q("type"), Q("database"))))));
        }
        for (auto& table : Params_.Tables) {
            auto path = ctx.GetPrefixedPath(ServiceId, Cluster, table);
            entries->Add(Q(Y(Q(Y(Q("type"), Q("table"))), Q(Y(Q("path"), path)))));
        }
        options->Add(Q(Y(Q("entries"), Q(entries))));

        return options;
    }

    TPtr DoClone() const final {
        return new TCreateBackupCollectionNode(GetPos(), Prefix_, Id_, Params_, *this);
    }

private:
    TCreateBackupCollectionParameters Params_;
};

class TAlterBackupCollectionNode
    : public TBaseBackupCollectionNode {
    using TBase = TBaseBackupCollectionNode;

public:
    TAlterBackupCollectionNode(
        TPosition pos,
        const TString& prefix,
        const TString& objectId,
        const TAlterBackupCollectionParameters& params,
        const TObjectOperatorContext& context)
        : TBase(pos, prefix, objectId, context)
        , Params_(params)
    {
    }

    INode::TPtr FillOptions(TContext& ctx, INode::TPtr options) const final {
        options->Add(Q(Y(Q("mode"), Q("alter"))));

        auto settings = Y();
        for (auto& [key, value] : Params_.Settings) {
            settings->Add(Q(Y(BuildQuotedAtom(Pos_, key), Y("String", value.Build()))));
        }
        options->Add(Q(Y(Q("settings"), Q(settings))));

        auto resetSettings = Y();
        for (auto& key : Params_.SettingsToReset) {
            resetSettings->Add(BuildQuotedAtom(Pos_, key));
        }
        options->Add(Q(Y(Q("resetSettings"), Q(resetSettings))));

        auto entries = Y();
        if (Params_.Database != TAlterBackupCollectionParameters::EDatabase::Unchanged) {
            entries->Add(Q(Y(Q(Y(Q("type"), Q("database"))), Q(Y(Q("action"), Q(Params_.Database == TAlterBackupCollectionParameters::EDatabase::Add ? "add" : "drop"))))));
        }
        for (auto& table : Params_.TablesToAdd) {
            auto path = ctx.GetPrefixedPath(ServiceId, Cluster, table);
            entries->Add(Q(Y(Q(Y(Q("type"), Q("table"))), Q(Y(Q("path"), path)), Q(Y(Q("action"), Q("add"))))));
        }
        for (auto& table : Params_.TablesToDrop) {
            auto path = ctx.GetPrefixedPath(ServiceId, Cluster, table);
            entries->Add(Q(Y(Q(Y(Q("type"), Q("table"))), Q(Y(Q("path"), path)), Q(Y(Q("action"), Q("drop"))))));
        }
        options->Add(Q(Y(Q("alterEntries"), Q(entries))));

        return options;
    }

    TPtr DoClone() const final {
        return new TAlterBackupCollectionNode(GetPos(), Prefix_, Id_, Params_, *this);
    }

private:
    TAlterBackupCollectionParameters Params_;
};

class TDropBackupCollectionNode
    : public TBaseBackupCollectionNode {
    using TBase = TBaseBackupCollectionNode;

public:
    TDropBackupCollectionNode(
        TPosition pos,
        const TString& prefix,
        const TString& objectId,
        const TDropBackupCollectionParameters&,
        const TObjectOperatorContext& context)
        : TBase(pos, prefix, objectId, context)
    {
    }

    INode::TPtr FillOptions(TContext&, INode::TPtr options) const final {
        options->Add(Q(Y(Q("mode"), Q("drop"))));

        return options;
    }

    TPtr DoClone() const final {
        TDropBackupCollectionParameters params;
        return new TDropBackupCollectionNode(GetPos(), Prefix_, Id_, params, *this);
    }
};

TNodePtr BuildCreateBackupCollection(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TCreateBackupCollectionParameters& params,
    const TObjectOperatorContext& context)
{
    return new TCreateBackupCollectionNode(pos, prefix, id, params, context);
}

TNodePtr BuildAlterBackupCollection(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TAlterBackupCollectionParameters& params,
    const TObjectOperatorContext& context)
{
    return new TAlterBackupCollectionNode(pos, prefix, id, params, context);
}

TNodePtr BuildDropBackupCollection(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TDropBackupCollectionParameters& params,
    const TObjectOperatorContext& context)
{
    return new TDropBackupCollectionNode(pos, prefix, id, params, context);
}

class TBackupNode final
    : public TAstListNode,
      public TObjectOperatorContext {
    using TBase = TAstListNode;

public:
    TBackupNode(
        TPosition pos,
        const TString& prefix,
        const TString& id,
        const TBackupParameters& params,
        const TObjectOperatorContext& context)
        : TBase(pos)
        , TObjectOperatorContext(context)
        , Prefix_(prefix)
        , Id_(id)
        , Params_(params)
    {
        Y_UNUSED(Params_);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Y("Key");
        keys = L(keys, Q(Y(Q("backup"), Y("String", BuildQuotedAtom(Pos_, Id_)), Y("String", BuildQuotedAtom(Pos_, Prefix_)))));

        auto opts = Y();

        if (Params_.Incremental) {
            opts->Add(Q(Y(Q("mode"), Q("backupIncremental"))));
        } else {
            opts->Add(Q(Y(Q("mode"), Q("backup"))));
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TBackupNode(GetPos(), Prefix_, Id_, Params_, *this);
    }

private:
    TString Prefix_;
    TString Id_;
    TBackupParameters Params_;
};

TNodePtr BuildBackup(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TBackupParameters& params,
    const TObjectOperatorContext& context)
{
    return new TBackupNode(pos, prefix, id, params, context);
}

class TRestoreNode final
    : public TAstListNode,
      public TObjectOperatorContext {
    using TBase = TAstListNode;

public:
    TRestoreNode(
        TPosition pos,
        const TString& prefix,
        const TString& id,
        const TRestoreParameters& params,
        const TObjectOperatorContext& context)
        : TBase(pos)
        , TObjectOperatorContext(context)
        , Prefix_(prefix)
        , Id_(id)
        , Params_(params)
    {
        Y_UNUSED(Params_);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Y("Key");
        keys = L(keys, Q(Y(Q("restore"), Y("String", BuildQuotedAtom(Pos_, Id_)), Y("String", BuildQuotedAtom(Pos_, Prefix_)))));

        auto opts = Y();
        opts->Add(Q(Y(Q("mode"), Q("restore"))));

        if (Params_.At) {
            opts->Add(Q(Y(Q("at"), BuildQuotedAtom(Pos_, Params_.At))));
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TRestoreNode(GetPos(), Prefix_, Id_, Params_, *this);
    }

private:
    TString Prefix_;
    TString Id_;
    TRestoreParameters Params_;
};

TNodePtr BuildRestore(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TRestoreParameters& params,
    const TObjectOperatorContext& context)
{
    return new TRestoreNode(pos, prefix, id, params, context);
}

class TSecretNode: public TAstListNode {
    using TBase = TAstListNode;

public:
    TSecretNode(
        TPosition pos,
        const TString& objectId,
        const TSecretParameters& params,
        const TObjectOperatorContext& context,
        TScopedStatePtr scoped)
        : TBase(pos)
        , Pos_(pos)
        , ObjectId_(objectId)
        , Params_(params)
        , Context_(context)
        , Scoped_(scoped)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        Scoped_->UseCluster(Context_.ServiceId, Context_.Cluster);
        const auto keys = Y("Key", Q(Y(Q("secret"), Y("String", BuildQuotedAtom(Pos_, ObjectId_)))));
        const auto options = BuildOptions();

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Context_.ServiceId), Scoped_->WrapCluster(Context_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

protected:
    virtual TString GetMode() = 0;

private:
    TPtr BuildOptions() {
        auto options = Y();
        options = L(options, Q(Y(Q("mode"), Q(GetMode()))));
        if (Params_.Value) {
            if (Params_.Value->HasNode()) {
                options = L(options, Q(Y(BuildQuotedAtom(Pos_, "value"), Params_.Value->Build())));
            } else {
                options = L(options, Q(Y(BuildQuotedAtom(Pos_, "value"))));
            }
        }
        if (Params_.InheritPermissions) {
            if (Params_.InheritPermissions->HasNode()) {
                options = L(options, Q(Y(BuildQuotedAtom(Pos_, "inherit_permissions"), Params_.InheritPermissions->Build())));
            } else {
                options = L(options, Q(Y(BuildQuotedAtom(Pos_, "inherit_permissions"))));
            }
        }
        return options;
    }

protected:
    TPosition Pos_;
    const TString ObjectId_;
    const TSecretParameters Params_;
    const TObjectOperatorContext Context_;
    TScopedStatePtr Scoped_;
};

class TCreateSecretNode: public TSecretNode {
    using TBase = TSecretNode;

public:
    TCreateSecretNode(
        TPosition pos,
        const TString& objectId,
        const TSecretParameters& params,
        const TObjectOperatorContext& context,
        TScopedStatePtr scoped)
        : TBase(pos, objectId, params, context, scoped)
    {
    }

protected:
    TString GetMode() override {
        return "create";
    }

    TPtr DoClone() const final {
        return new TCreateSecretNode(Pos_, ObjectId_, Params_, Context_, Scoped_);
    }
};

TNodePtr BuildCreateSecret(
    TPosition pos,
    const TString& objectId,
    const TSecretParameters& secretParams,
    const TObjectOperatorContext& context,
    TScopedStatePtr scoped) {
    return new TCreateSecretNode(pos, objectId, secretParams, context, scoped);
}

class TAlterSecretNode: public TSecretNode {
    using TBase = TSecretNode;

public:
    TAlterSecretNode(
        TPosition pos,
        const TString& objectId,
        const TSecretParameters& params,
        const TObjectOperatorContext& context,
        TScopedStatePtr scoped)
        : TBase(pos, objectId, params, context, scoped)
    {
    }

protected:
    TString GetMode() override {
        return "alter";
    }

    TPtr DoClone() const final {
        return new TAlterSecretNode(Pos_, ObjectId_, Params_, Context_, Scoped_);
    }
};

TNodePtr BuildAlterSecret(
    TPosition pos,
    const TString& objectId,
    const TSecretParameters& secretParams,
    const TObjectOperatorContext& context,
    TScopedStatePtr scoped) {
    return new TAlterSecretNode(pos, objectId, secretParams, context, scoped);
}

class TDropSecretNode: public TSecretNode {
    using TBase = TSecretNode;

public:
    TDropSecretNode(
        TPosition pos,
        const TString& objectId,
        const TObjectOperatorContext& context,
        TScopedStatePtr scoped)
        : TBase(pos, objectId, TSecretParameters{}, context, scoped)
    {
    }

protected:
    TPtr DoClone() const final {
        return new TDropSecretNode(Pos_, ObjectId_, Context_, Scoped_);
    }

    TString GetMode() override {
        return "drop";
    }
};

TNodePtr BuildDropSecret(
    TPosition pos,
    const TString& objectId,
    const TObjectOperatorContext& context,
    TScopedStatePtr scoped) {
    return new TDropSecretNode(pos, objectId, context, scoped);
}

} // namespace NSQLTranslationV1
