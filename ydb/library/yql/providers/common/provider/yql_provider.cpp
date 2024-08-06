#include "yql_provider.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <util/folder/path.h>
#include <util/generic/is_in.h>
#include <util/generic/utility.h>
#include <util/string/join.h>


namespace NYql {
namespace NCommon {

using namespace NNodes;

namespace {
    constexpr std::array<std::string_view, 8> FormatsForInput = {
        "csv_with_names"sv,
        "tsv_with_names"sv,
        "json_list"sv,
        "json"sv,
        "raw"sv,
        "json_as_string"sv,
        "json_each_row"sv,
        "parquet"sv
    };
    constexpr std::array<std::string_view, 7> FormatsForOutput = {
        "csv_with_names"sv,
        "tsv_with_names"sv,
        "json_list"sv,
        "json"sv,
        "raw"sv,
        "json_each_row"sv,
        "parquet"sv
    };
    constexpr std::array<std::string_view, 6> Compressions = {
        "gzip"sv,
        "zstd"sv,
        "lz4"sv,
        "brotli"sv,
        "bzip2"sv,
        "xz"sv
    };
    constexpr std::array<std::string_view, 7> IntervalUnits = {
        "MICROSECONDS"sv,
        "MILLISECONDS"sv,
        "SECONDS"sv,
        "MINUTES"sv,
        "HOURS"sv,
        "DAYS"sv,
        "WEEKS"sv
    };
    constexpr std::array<std::string_view, 2> DateTimeFormatNames = {
        "POSIX"sv,
        "ISO"sv
    };

    constexpr std::array<std::string_view, 5> TimestampFormatNames = {
        "POSIX"sv,
        "ISO"sv,
        "UNIX_TIME_MILLISECONDS"sv,
        "UNIX_TIME_SECONDS"sv,
        "UNIX_TIME_MICROSECONDS"sv
    };

    TCoAtom InferIndexName(TCoAtomList key, TExprContext& ctx) {
        static const TString end = "_idx";
        static const TString delimiter = "_";

        size_t sz = end.Size();
        for (const auto& n: key)
            sz += n.Value().Size() + delimiter.Size();

        TString name(Reserve(sz));
        for (const auto& n: key) {
            name += n.Value() + delimiter;
        }
        name += end;

        return Build<TCoAtom>(ctx, key.Pos())
            .Value(name)
            .Done();
    }
} // namespace

bool TCommitSettings::EnsureModeEmpty(TExprContext& ctx) {
    if (Mode) {
        ctx.AddError(TIssue(ctx.GetPosition(Pos), TStringBuilder()
            << "Unsupported mode:" << Mode.Cast().Value()));
        return false;
    }

    return true;
}

bool TCommitSettings::EnsureEpochEmpty(TExprContext& ctx) {
    if (Epoch) {
        ctx.AddError(TIssue(ctx.GetPosition(Pos), TStringBuilder()
            << "Epochs are unsupported."));
        return false;
    }

    return true;
}

bool TCommitSettings::EnsureOtherEmpty(TExprContext& ctx) {
    if (!Other.Empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(Pos), TStringBuilder()
            << "Unsupported setting:" << Other.Item(0).Name().Value()));
        return false;
    }

    return true;
}

TCoNameValueTupleList TCommitSettings::BuildNode(TExprContext& ctx) const {
    TVector<TExprBase> settings;

    auto addSettings = [this, &settings, &ctx] (const TString& name, TMaybeNode<TExprBase> value) {
        if (value) {

            auto node = Build<TCoNameValueTuple>(ctx, Pos)
                .Name().Build(name)
                .Value(value.Cast())
                .Done();

            settings.push_back(node);

        }
    };

    addSettings("mode", Mode);
    addSettings("epoch", Epoch);

    for (auto setting : Other) {
        settings.push_back(setting);
    }

    auto ret = Build<TCoNameValueTupleList>(ctx, Pos)
        .Add(settings)
        .Done();

    return ret;
}

const TStructExprType* BuildCommonTableListType(TExprContext& ctx) {
    TVector<const TItemExprType*> items;
    auto stringType = ctx.MakeType<TDataExprType>(EDataSlot::String);
    auto listOfString = ctx.MakeType<TListExprType>(stringType);

    items.push_back(ctx.MakeType<TItemExprType>("Prefix", stringType));
    items.push_back(ctx.MakeType<TItemExprType>("Folders", listOfString));
    items.push_back(ctx.MakeType<TItemExprType>("Tables", listOfString));

    return ctx.MakeType<TStructExprType>(items);
}

TExprNode::TPtr BuildTypeExpr(TPositionHandle pos, const TTypeAnnotationNode& ann, TExprContext& ctx) {
    return ExpandType(pos, ann, ctx);
}

bool HasResOrPullOption(const TExprNode& node, const TStringBuf& option) {
    if (node.Content() == "Result" || node.Content() == "Pull") {
        auto options = node.Child(4);
        for (auto setting : options->Children()) {
            if (setting->Head().Content() == option) {
                return true;
            }
        }
    }
    return false;
}

TVector<TString> GetResOrPullColumnHints(const TExprNode& node) {
    TVector<TString> columns;
    auto setting = GetSetting(*node.Child(4), "columns");
    if (setting) {
        auto type = node.Head().GetTypeAnn();
        if (type->GetKind() != ETypeAnnotationKind::EmptyList) {
            auto structType = type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            for (ui32 i = 0; i < structType->GetSize(); ++i) {
               auto field = setting->Child(1)->Child(i);
               columns.push_back(TString(field->Content()));
            }
        }
    }
    return columns;
}

TString FullTableName(const TStringBuf& cluster, const TStringBuf& table) {
    return TStringBuilder() << cluster << ".[" << table << "]";
}

IDataProvider::TFillSettings GetFillSettings(const TExprNode& node) {
    IDataProvider::TFillSettings fillSettings;
    fillSettings.AllResultsBytesLimit = node.Child(1)->Content().empty()
        ? Nothing()
        : TMaybe<ui64>(FromString<ui64>(node.Child(1)->Content()));

    fillSettings.RowsLimitPerWrite = node.Child(2)->Content().empty()
        ? Nothing()
        : TMaybe<ui64>(FromString<ui64>(node.Child(2)->Content()));

    fillSettings.Format = (IDataProvider::EResultFormat)FromString<ui32>(node.Child(5)->Content());
    fillSettings.FormatDetails = node.Child(3)->Content();
    fillSettings.Discard = FromString<bool>(node.Child(7)->Content());

    return fillSettings;
}

NYson::EYsonFormat GetYsonFormat(const IDataProvider::TFillSettings& fillSettings) {
    YQL_ENSURE(fillSettings.Format == IDataProvider::EResultFormat::Yson);
    return (NYson::EYsonFormat)FromString<ui32>(fillSettings.FormatDetails);
}

TWriteTableSettings ParseWriteTableSettings(TExprList node, TExprContext& ctx) {
    TMaybeNode<TCoAtom> mode;
    TMaybeNode<TCoAtom> temporary;
    TMaybeNode<TExprList> columns;
    TMaybeNode<TExprList> returningList;
    TMaybeNode<TCoAtomList> primaryKey;
    TMaybeNode<TCoAtomList> partitionBy;
    TMaybeNode<TCoNameValueTupleList> orderBy;
    TMaybeNode<TCoLambda> filter;
    TMaybeNode<TCoLambda> update;
    TVector<TCoNameValueTuple> other;
    TVector<TCoIndex> indexes;
    TVector<TCoChangefeed> changefeeds;
    TMaybeNode<TExprList> columnFamilies;
    TVector<TCoNameValueTuple> tableSettings;
    TVector<TCoNameValueTuple> alterActions;
    TMaybeNode<TCoAtom> tableType;
    TMaybeNode<TCallable> pgFilter;
    for (auto child : node) {
        if (auto maybeTuple = child.Maybe<TCoNameValueTuple>()) {
            auto tuple = maybeTuple.Cast();
            auto name = tuple.Name().Value();

            if (name == "mode") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                mode = tuple.Value().Cast<TCoAtom>();
            } else if (name == "columns")  {
                YQL_ENSURE(tuple.Value().Maybe<TExprList>());
                columns = tuple.Value().Cast<TExprList>();
            } else if (name == "primarykey")  {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtomList>());
                primaryKey = tuple.Value().Cast<TCoAtomList>();
            } else if (name == "partitionby")  {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtomList>());
                partitionBy = tuple.Value().Cast<TCoAtomList>();
            } else if (name == "orderby")  {
                YQL_ENSURE(tuple.Value().Maybe<TCoNameValueTupleList>());
                orderBy = tuple.Value().Cast<TCoNameValueTupleList>();
            } else if (name == "filter") {
                YQL_ENSURE(tuple.Value().Maybe<TCoLambda>());
                filter = tuple.Value().Cast<TCoLambda>();
            } else if (name == "update") {
                YQL_ENSURE(tuple.Value().Maybe<TCoLambda>());
                update = tuple.Value().Cast<TCoLambda>();
            } else if (name == "index") {
                YQL_ENSURE(tuple.Value().Maybe<TCoNameValueTupleList>());
                auto index = Build<TCoIndex>(ctx, node.Pos());
                bool inferName = false;
                TCoNameValueTupleList tableSettings = Build<TCoNameValueTupleList>(ctx, node.Pos()).Done();
                TCoNameValueTupleList indexSettings = Build<TCoNameValueTupleList>(ctx, node.Pos()).Done();
                TMaybe<TCoAtomList> columnList;
                for (const auto& item : tuple.Value().Cast<TCoNameValueTupleList>()) {
                    const auto& indexItemName = item.Name().Value();
                    if (indexItemName == "indexName") {
                        if (auto atom = item.Value().Maybe<TCoAtom>()) {
                            index.Name(atom.Cast());
                        } else {
                            // No index name given - infer name from column set
                            inferName = true;
                        }
                    } else if (indexItemName == "indexType") {
                        index.Type(item.Value().Cast<TCoAtom>());
                    } else if (indexItemName == "indexColumns") {
                        columnList = item.Value().Cast<TCoAtomList>();
                        index.Columns(item.Value().Cast<TCoAtomList>());
                    } else if (indexItemName == "dataColumns") {
                        index.DataColumns(item.Value().Cast<TCoAtomList>());
                    } else if (indexItemName == "tableSettings") {
                        tableSettings = item.Value().Cast<TCoNameValueTupleList>();
                    } else if (indexItemName == "indexSettings") {
                        indexSettings = item.Value().Cast<TCoNameValueTupleList>();
                    } else {
                        YQL_ENSURE(false, "unknown index item");
                    }
                }

                index.TableSettings(tableSettings);
                index.IndexSettings(indexSettings);

                if (inferName) {
                    YQL_ENSURE(columnList);
                    index.Name(InferIndexName(*columnList, ctx));
                }
                indexes.push_back(index.Done());
            } else if (name == "changefeed") {
                YQL_ENSURE(tuple.Value().Maybe<TCoNameValueTupleList>());
                auto cf = Build<TCoChangefeed>(ctx, node.Pos());
                for (const auto& item : tuple.Value().Cast<TCoNameValueTupleList>()) {
                    const auto& itemName = item.Name().Value();
                    if (itemName == "name") {
                        cf.Name(item.Value().Cast<TCoAtom>());
                    } else if (itemName == "settings") {
                        YQL_ENSURE(item.Value().Maybe<TCoNameValueTupleList>());
                        cf.Settings(item.Value().Cast<TCoNameValueTupleList>());
                    } else if (itemName == "state") {
                        cf.State(item.Value().Cast<TCoAtom>());
                    } else {
                        YQL_ENSURE(false, "unknown changefeed item");
                    }
                }
                changefeeds.push_back(cf.Done());
            } else if (name == "columnFamilies") {
                YQL_ENSURE(tuple.Value().Maybe<TExprList>());
                columnFamilies = tuple.Value().Cast<TExprList>();
            } else if (name == "tableSettings") {
                YQL_ENSURE(tuple.Value().Maybe<TCoNameValueTupleList>());
                for (const auto& item : tuple.Value().Cast<TCoNameValueTupleList>()) {
                    tableSettings.push_back(item);
                }
            } else if (name == "actions") {
                YQL_ENSURE(tuple.Value().Maybe<TCoNameValueTupleList>());
                for (const auto& item : tuple.Value().Cast<TCoNameValueTupleList>()) {
                    alterActions.push_back(item);
                }
            } else if (name == "tableType") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                tableType = tuple.Value().Cast<TCoAtom>();
            } else if (name == "pg_delete" || name == "pg_update") {
                YQL_ENSURE(tuple.Value().Maybe<TCallable>());
                pgFilter = tuple.Value().Cast<TCallable>();
            } else if (name == "temporary") {
                temporary = Build<TCoAtom>(ctx, node.Pos()).Value("true").Done();
            } else if (name == "returning") {
                YQL_ENSURE(tuple.Value().Maybe<TExprList>());
                returningList = tuple.Value().Cast<TExprList>();
            } else {
                other.push_back(tuple);
            }
        }
    }

    const auto& otherSettings = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(other)
        .Done();

    const auto& idx = Build<TCoIndexList>(ctx, node.Pos())
        .Add(indexes)
        .Done();

    const auto& cfs = Build<TCoChangefeedList>(ctx, node.Pos())
        .Add(changefeeds)
        .Done();

    const auto& tableProfileSettings = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(tableSettings)
        .Done();

    const auto& alterTableActions = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(alterActions)
        .Done();

    if (!columnFamilies.IsValid()) {
        columnFamilies = Build<TExprList>(ctx, node.Pos()).Done();
    }

    TWriteTableSettings ret(otherSettings);
    ret.Mode = mode;
    ret.Temporary = temporary;
    ret.Columns = columns;
    ret.ReturningList = returningList;
    ret.PrimaryKey = primaryKey;
    ret.PartitionBy = partitionBy;
    ret.OrderBy = orderBy;
    ret.Filter = filter;
    ret.Update = update;
    ret.Indexes = idx;
    ret.Changefeeds = cfs;
    ret.ColumnFamilies = columnFamilies;
    ret.TableSettings = tableProfileSettings;
    ret.AlterActions = alterTableActions;
    ret.TableType = tableType;
    ret.PgFilter = pgFilter;
    return ret;
}

TWriteSequenceSettings ParseSequenceSettings(NNodes::TExprList node, TExprContext& ctx) {
    TMaybeNode<TCoAtom> mode;
    TMaybeNode<TCoAtom> valueType;
    TMaybeNode<TCoAtom> temporary;
    TMaybeNode<TCoAtom> ownedBy;

    TVector<TCoNameValueTuple> sequenceSettings;

    TVector<TCoNameValueTuple> other;

    const static std::unordered_set<TString> sequenceSettingNames =
        {"start", "increment", "cache", "minvalue", "maxvalue", "cycle"};

    for (auto child : node) {
        if (auto maybeTuple = child.Maybe<TCoNameValueTuple>()) {
            auto tuple = maybeTuple.Cast();
            auto name = tuple.Name().Value();

            if (name == "mode") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                mode = tuple.Value().Cast<TCoAtom>();
            } else if (name == "as") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                valueType = tuple.Value().Cast<TCoAtom>();
            } else if (name == "temporary")  {
                temporary = Build<TCoAtom>(ctx, node.Pos()).Value("true").Done();
            } else if (sequenceSettingNames.contains(TString(name))) {
                sequenceSettings.push_back(tuple);
            } else {
                other.push_back(tuple);
            }
        }
    }

    const auto& sequenceSettingsList = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(sequenceSettings)
        .Done();

    const auto& otherSettings = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(other)
        .Done();

    TWriteSequenceSettings ret(otherSettings);
    ret.Mode = mode;
    ret.ValueType = valueType;
    ret.Temporary = temporary;
    ret.SequenceSettings = sequenceSettingsList;
    return ret;
}

TWriteTopicSettings ParseWriteTopicSettings(TExprList node, TExprContext& ctx) {
    Y_UNUSED(ctx);
    TMaybeNode<TCoAtom> mode;
    TVector<TCoTopicConsumer> consumers;
    TVector<TCoTopicConsumer> addConsumers;
    TVector<TCoTopicConsumer> alterConsumers;
    TVector<TCoAtom> dropConsumers;
    TVector<TCoNameValueTuple> topicSettings;

    auto parseNewConsumer = [&](const auto& node, const auto& tuple, auto& consumersList) {
        YQL_ENSURE(tuple.Value().template Maybe<TCoNameValueTupleList>());
        auto consumer = Build<TCoTopicConsumer>(ctx, node.Pos());
        for (const auto& item : tuple.Value().template Cast<TCoNameValueTupleList>()) {
            const auto& itemName = item.Name().Value();
            if (itemName == "name") {
                consumer.Name(item.Value().template Cast<TCoAtom>());
            } else if (itemName == "settings") {
                YQL_ENSURE(item.Value().template Maybe<TCoNameValueTupleList>());
                consumer.Settings(item.Value().template Cast<TCoNameValueTupleList>());
            } else {
                YQL_ENSURE(false, "unknown consumer item");
            }
        }
        consumersList.push_back(consumer.Done());
    };

    for (auto child : node) {
        if (auto maybeTuple = child.Maybe<TCoNameValueTuple>()) {
            auto tuple = maybeTuple.Cast();
            auto name = tuple.Name().Value();

            if (name == "mode") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                mode = tuple.Value().Cast<TCoAtom>();
            }  else if (name == "consumer") {
                parseNewConsumer(node, tuple, consumers);
            } else if (name == "addConsumer") {
                parseNewConsumer(node, tuple, addConsumers);
            } else if (name == "alterConsumer") {
                parseNewConsumer(node, tuple, alterConsumers);
            } else if (name == "dropConsumer") {
                auto name = tuple.Value().Cast<TCoAtom>();
                dropConsumers.push_back(name);
            } else if (name == "topicSettings") {
                YQL_ENSURE(tuple.Value().Maybe<TCoNameValueTupleList>());
                for (const auto& item : tuple.Value().Cast<TCoNameValueTupleList>()) {
                    topicSettings.push_back(item);
                }
            }
        }
    }

    const auto& builtCons = Build<TCoTopicConsumerList>(ctx, node.Pos())
            .Add(consumers)
            .Done();

    const auto& builtAddCons = Build<TCoTopicConsumerList>(ctx, node.Pos())
            .Add(addConsumers)
            .Done();

    const auto& builtAlterCons = Build<TCoTopicConsumerList>(ctx, node.Pos())
            .Add(alterConsumers)
            .Done();

    const auto& builtDropCons = Build<TCoAtomList>(ctx, node.Pos())
            .Add(dropConsumers)
            .Done();


    const auto& builtSettings = Build<TCoNameValueTupleList>(ctx, node.Pos())
            .Add(topicSettings)
            .Done();

    TVector<TCoNameValueTuple> other;
    const auto& otherSettings = Build<TCoNameValueTupleList>(ctx, node.Pos())
            .Add(other)
            .Done();

    TWriteTopicSettings ret(otherSettings);
    ret.Mode = mode;
    ret.Consumers = builtCons;
    ret.AddConsumers = builtAddCons;
    ret.TopicSettings = builtSettings;
    ret.AlterConsumers = builtAlterCons;
    ret.DropConsumers = builtDropCons;

    return ret;
}

TWriteReplicationSettings ParseWriteReplicationSettings(TExprList node, TExprContext& ctx) {
    TMaybeNode<TCoAtom> mode;
    TVector<TCoReplicationTarget> targets;
    TVector<TCoNameValueTuple> settings;
    TVector<TCoNameValueTuple> other;

    for (auto child : node) {
        if (auto maybeTuple = child.Maybe<TCoNameValueTuple>()) {
            auto tuple = maybeTuple.Cast();
            auto name = tuple.Name().Value();

            if (name == "mode") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                mode = tuple.Value().Cast<TCoAtom>();
            } else if (name == "targets") {
                YQL_ENSURE(tuple.Value().Maybe<TExprList>());
                for (const auto& target : tuple.Value().Cast<TExprList>()) {
                    auto builtTarget = Build<TCoReplicationTarget>(ctx, node.Pos());

                    YQL_ENSURE(target.Maybe<TCoNameValueTupleList>());
                    for (const auto& item : target.Cast<TCoNameValueTupleList>()) {
                        auto itemName = item.Name().Value();
                        if (itemName == "remote") {
                            builtTarget.RemotePath(item.Value().Cast<TCoAtom>());
                        } else if (itemName == "local") {
                            builtTarget.LocalPath(item.Value().Cast<TCoAtom>());
                        } else {
                            YQL_ENSURE(false, "unknown target item");
                        }
                    }

                    targets.push_back(builtTarget.Done());
                }
            } else if (name == "settings") {
                YQL_ENSURE(tuple.Value().Maybe<TCoNameValueTupleList>());
                for (const auto& item : tuple.Value().Cast<TCoNameValueTupleList>()) {
                    settings.push_back(item);
                }
            } else {
                other.push_back(tuple);
            }
        }
    }

    const auto& builtTargets = Build<TCoReplicationTargetList>(ctx, node.Pos())
        .Add(targets)
        .Done();

    const auto& builtSettings = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(settings)
        .Done();

    const auto& builtOther = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(other)
        .Done();

    TWriteReplicationSettings ret(builtOther);
    ret.Mode = mode;
    ret.Targets = builtTargets;
    ret.ReplicationSettings = builtSettings;

    return ret;
}

TWriteRoleSettings ParseWriteRoleSettings(TExprList node, TExprContext& ctx) {
    TMaybeNode<TCoAtom> mode;
    TVector<TCoAtom> roles;
    TMaybeNode<TCoAtom> newName;
    TVector<TCoNameValueTuple> other;
    for (auto child : node) {
        if (auto maybeTuple = child.Maybe<TCoNameValueTuple>()) {
            auto tuple = maybeTuple.Cast();
            auto name = tuple.Name().Value();

            if (name == "mode") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                mode = tuple.Value().Cast<TCoAtom>();
            } else if (name == "roles") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtomList>());
                for (const auto& item : tuple.Value().Cast<TCoAtomList>()) {
                    roles.push_back(item);
                }
            } else if (name == "newName") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                newName = tuple.Value().Cast<TCoAtom>();
            } else {
                other.push_back(tuple);
            }
        }
    }

    const auto& builtRoles = Build<TCoAtomList>(ctx, node.Pos())
        .Add(roles)
        .Done();

    const auto& otherSettings = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(other)
        .Done();

    TWriteRoleSettings ret(otherSettings);
    ret.Roles = builtRoles;;
    ret.NewName = newName;
    ret.Mode = mode;

    return ret;
}

TWritePermissionSettings ParseWritePermissionsSettings(TExprList node, TExprContext&) {
    TMaybeNode<TCoAtomList> permissions;
    TMaybeNode<TCoAtomList> paths;
    TMaybeNode<TCoAtomList> roleNames;
    for (auto child : node) {
        if (auto maybeTuple = child.Maybe<TCoNameValueTuple>()) {
            auto tuple = maybeTuple.Cast();
            auto name = tuple.Name().Value();

            if (name == "permissions") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtomList>());
                permissions = tuple.Value().Cast<TCoAtomList>();;
            } else if (name == "roles") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtomList>());
                roleNames = tuple.Value().Cast<TCoAtomList>();
            } else if (name == "paths") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtomList>());
                paths = tuple.Value().Cast<TCoAtomList>();
            }
        }
    }

    TWritePermissionSettings ret(std::move(permissions), std::move(paths), std::move(roleNames));
    return ret;
}

TWriteObjectSettings ParseWriteObjectSettings(TExprList node, TExprContext& ctx) {
    TMaybeNode<TCoAtom> mode;
    TMaybe<TCoNameValueTupleList> kvFeatures;
    TMaybe<TCoAtomList> resetFeatures;
    for (auto child : node) {
        if (auto maybeTuple = child.Maybe<TCoNameValueTuple>()) {
            auto tuple = maybeTuple.Cast();
            auto name = tuple.Name().Value();

            if (name == "mode") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                mode = tuple.Value().Cast<TCoAtom>();
            } else if (name == "features") {
                auto maybeFeatures = tuple.Value().Maybe<TCoNameValueTupleList>();
                Y_ABORT_UNLESS(maybeFeatures);
                kvFeatures = maybeFeatures.Cast();
            } else if (name == "resetFeatures") {
                auto maybeFeatures = tuple.Value().Maybe<TCoAtomList>();
                Y_ABORT_UNLESS(maybeFeatures);
                resetFeatures = maybeFeatures.Cast();
            }
        }
    }
    if (!kvFeatures) {
        kvFeatures = Build<TCoNameValueTupleList>(ctx, node.Pos()).Done();
    }
    if (!resetFeatures) {
        resetFeatures = Build<TCoAtomList>(ctx, node.Pos()).Done();
    }
    TWriteObjectSettings ret(std::move(mode), std::move(*kvFeatures), std::move(*resetFeatures));
    return ret;
}

TCommitSettings ParseCommitSettings(TCoCommit node, TExprContext& ctx) {
    if (!node.Settings()) {
        return TCommitSettings(Build<TCoNameValueTupleList>(ctx, node.Pos()).Done());
    }

    TMaybeNode<TCoAtom> mode;
    TMaybeNode<TCoAtom> epoch;
    TVector<TExprBase> other;

    if (node.Settings()) {
        auto settings = node.Settings().Cast();
        for (auto setting : settings) {
            if (setting.Name() == "mode") {
                YQL_ENSURE(setting.Value().Maybe<TCoAtom>());
                mode = setting.Value().Cast<TCoAtom>();
            } else if (setting.Name() == "epoch") {
                YQL_ENSURE(setting.Value().Maybe<TCoAtom>());
                epoch = setting.Value().Cast<TCoAtom>();
            } else {
                other.push_back(setting);
            }
        }
    }

    auto otherSettings = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(other)
        .Done();

    TCommitSettings ret(otherSettings);
    ret.Pos = node.Pos();
    ret.Mode = mode;
    ret.Epoch = epoch;

    return ret;
}

TPgObjectSettings ParsePgObjectSettings(TExprList node, TExprContext&) {
    TMaybeNode<TCoAtom> mode;
    TMaybeNode<TCoAtom> ifExists;
    for (auto child : node) {
        if (auto maybeTuple = child.Maybe<TCoNameValueTuple>()) {
            auto tuple = maybeTuple.Cast();
            auto name = tuple.Name().Value();

            if (name == "mode") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                mode = tuple.Value().Cast<TCoAtom>();
            } else if (name == "ifExists") {
                YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
                ifExists = tuple.Value().Cast<TCoAtom>();
            }
        }
    }

    TPgObjectSettings ret(std::move(mode), std::move(ifExists));
    return ret;
}


TVector<TString> GetStructFields(const TTypeAnnotationNode* type) {
    TVector<TString> fields;
    if (type->GetKind() == ETypeAnnotationKind::List) {
        type = type->Cast<TListExprType>()->GetItemType();
    }
    if (type->GetKind() == ETypeAnnotationKind::Struct) {
        auto structType = type->Cast<TStructExprType>();
        for (auto& member : structType->GetItems()) {
            fields.push_back(TString(member->GetName()));
        }
    }
    return fields;
}


void TransformerStatsToYson(const TString& name, const IGraphTransformer::TStatistics& stats,
    NYson::TYsonWriter& writer)
{
    writer.OnBeginMap();

    if (!name.empty()) {
        writer.OnKeyedItem("Name");
        writer.OnStringScalar(name);
    }

    if (stats.TransformDuration.MicroSeconds() > 0) {
        writer.OnKeyedItem("TransformDurationUs");
        writer.OnUint64Scalar(stats.TransformDuration.MicroSeconds());
    }
    if (stats.WaitDuration.MicroSeconds() > 0) {
        writer.OnKeyedItem("WaitDurationUs");
        writer.OnUint64Scalar(stats.WaitDuration.MicroSeconds());
    }
    if (stats.NewExprNodes > 0) {
        writer.OnKeyedItem("NewExprNodes");
        writer.OnInt64Scalar(stats.NewExprNodes);
    }
    if (stats.NewTypeNodes > 0) {
        writer.OnKeyedItem("NewTypeNodes");
        writer.OnInt64Scalar(stats.NewTypeNodes);
    }
    if (stats.NewConstraintNodes > 0) {
        writer.OnKeyedItem("NewConstraintNodes");
        writer.OnInt64Scalar(stats.NewConstraintNodes);
    }
    if (stats.Repeats > 0) {
        writer.OnKeyedItem("Repeats");
        writer.OnUint64Scalar(stats.Repeats);
    }
    if (stats.Restarts > 0) {
        writer.OnKeyedItem("Restarts");
        writer.OnUint64Scalar(stats.Restarts);
    }

    if (!stats.Stages.empty()) {
        writer.OnKeyedItem("Stages");
        writer.OnBeginList();
        for (auto& stage : stats.Stages) {
            writer.OnListItem();
            TransformerStatsToYson(stage.first, stage.second, writer);
        }
        writer.OnEndList();
    }

    writer.OnEndMap();
}

TString TransformerStatsToYson(const IGraphTransformer::TStatistics& stats, NYson::EYsonFormat format) {
    TStringStream out;
    NYson::TYsonWriter writer(&out, format);

    TransformerStatsToYson("", stats, writer);

    return out.Str();
}

bool FillUsedFilesImpl(
    const TExprNode& node,
    TUserDataTable& files,
    const TTypeAnnotationContext& types,
    TExprContext& ctx,
    const TUserDataTable& crutches,
    TNodeSet& visited,
    ui64& usedPgExtensions,
    bool needFullPgCatalog)
{
    if (!visited.insert(&node).second) {
        return true;
    }

    usedPgExtensions |= node.GetTypeAnn()->GetUsedPgExtensions();
    if (node.IsCallable("PgResolvedCall")) {
        auto procId = FromString<ui32>(node.Child(1)->Content());
        const auto& proc = NPg::LookupProc(procId);
        usedPgExtensions |= MakePgExtensionMask(proc.ExtensionIndex);
    }

    if (node.IsCallable("PgResolvedOp")) {
        auto operId = FromString<ui32>(node.Child(1)->Content());
        const auto& oper = NPg::LookupOper(operId);
        const auto& proc = NPg::LookupProc(oper.ProcId);
        usedPgExtensions |= MakePgExtensionMask(proc.ExtensionIndex);
    }

    if (node.IsCallable({"PgAnyResolvedOp", "PgAllResolvedOp"})) {
        auto operId = FromString<ui32>(node.Child(1)->Content());
        const auto& oper = NPg::LookupOper(operId);
        const auto& proc = NPg::LookupProc(oper.ProcId);
        usedPgExtensions |= MakePgExtensionMask(proc.ExtensionIndex);
    }

    if (node.IsCallable("PgTableContent")) {
        needFullPgCatalog = true;
    }

    if (node.IsCallable("FilePath") || node.IsCallable("FileContent")) {
        const auto& name = node.Head().Content();
        const auto block = types.UserDataStorage->FindUserDataBlock(name);
        if (!block) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "File not found: " << name));
            return false;
        }
        else {
            auto iter = files.insert({ TUserDataStorage::ComposeUserDataKey(name), *block }).first;
            iter->second.Usage.Set(node.IsCallable("FilePath") ? EUserDataBlockUsage::Path : EUserDataBlockUsage::Content);
        }
    }

    if (node.IsCallable("FolderPath")) {
        const auto& name = node.Head().Content();
        auto blocks = types.UserDataStorage->FindUserDataFolder(name);
        if (!blocks) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Folder not found: " << name));
            return false;
        } else {
            for (const auto& x : *blocks) {
                auto iter = files.insert({ x.first, *x.second }).first;
                iter->second.Usage.Set(EUserDataBlockUsage::Path);
            }
        }
    }

    if (node.IsCallable("Udf") || node.IsCallable("ScriptUdf")) {
        TStringBuf moduleName = node.Head().Content();
        if (node.IsCallable("Udf")) {
            TStringBuf funcName;
            YQL_ENSURE(SplitUdfName(node.Head().Content(), moduleName, funcName));
        }

        auto scriptType = NKikimr::NMiniKQL::ScriptTypeFromStr(moduleName);
        if (node.IsCallable("ScriptUdf") && !NKikimr::NMiniKQL::IsCustomPython(scriptType)) {
            moduleName = NKikimr::NMiniKQL::ScriptTypeAsStr(NKikimr::NMiniKQL::CanonizeScriptType(scriptType));
        }

        bool addSysModule = true;
        TString fileAlias;
        if (node.IsCallable("Udf")) {
            fileAlias = node.Child(6)->Content();
        } else {
            auto iterator = types.UdfModules.find(moduleName);
            // we have external UdfModule (not in preinstalled udfs)
            if (iterator != types.UdfModules.end()) {
                fileAlias = iterator->second.FileAlias;
            }
        }

        if (!fileAlias.empty()) {
            addSysModule = false;
            const auto block = types.UserDataStorage->FindUserDataBlock(fileAlias);
            if (!block) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "File not found: " << fileAlias));
                return false;
            } else {
                files.emplace(TUserDataStorage::ComposeUserDataKey(fileAlias), *block).first->second.Usage.Set(EUserDataBlockUsage::Udf);
            }
        }

        if (moduleName == TStringBuf("Geo")) {
            const auto geobase = TUserDataKey::File(TStringBuf("/home/geodata6.bin"));
            if (const auto block = types.UserDataStorage->FindUserDataBlock(geobase)) {
                files.emplace(geobase, *block).first->second.Usage.Set(EUserDataBlockUsage::Path);
            } else {
                const auto it = crutches.find(geobase);
                if (crutches.cend() != it) {
                    auto pragma = it->second;
                    types.UserDataStorage->AddUserDataBlock(geobase, pragma);
                    files.emplace(geobase, pragma).first->second.Usage.Set(EUserDataBlockUsage::Path);
                }
            }
        }

        if (addSysModule) {
            auto pathWithMd5 = types.UdfResolver->GetSystemModulePath(moduleName);
            YQL_ENSURE(pathWithMd5);
            TUserDataBlock sysBlock;
            sysBlock.Type = EUserDataType::PATH;
            sysBlock.Data = pathWithMd5->Path;
            sysBlock.Usage.Set(EUserDataBlockUsage::Udf);

            auto alias = TFsPath(sysBlock.Data).GetName();
            auto key = TUserDataKey::Udf(alias);
            if (const auto block = types.UserDataStorage->FindUserDataBlock(key)) {
                files[key] = *block;
                YQL_ENSURE(block->FrozenFile);
            } else {
                // Check alias clash with user files
                if (files.contains(TUserDataStorage::ComposeUserDataKey(alias))) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "System module name " << alias << " clashes with one of the user's files"));
                    return false;
                }

                if (!alias.StartsWith(NKikimr::NMiniKQL::StaticModulePrefix) && !files.contains(key)) {
                    // CreateFakeFileLink calculates md5 for file, let's do it once
                    sysBlock.FrozenFile = CreateFakeFileLink(sysBlock.Data, pathWithMd5->Md5);
                    files[key] = sysBlock;
                    types.UserDataStorage->AddUserDataBlock(key, sysBlock);
                }
            }
        }
    }

    bool childrenOk = true;
    for (auto& child : node.Children()) {
        childrenOk = FillUsedFilesImpl(*child, files, types, ctx, crutches, visited,
            usedPgExtensions, needFullPgCatalog) && childrenOk;
    }

    return childrenOk;
}

static void GetToken(const TString& string, TString& out, const TTypeAnnotationContext& type) {
    auto separator = string.find(":");
    const auto p0 = string.substr(0, separator);
    if (p0 == "api") {
        const auto p1 = string.substr(separator + 1);
        if (p1 == "oauth") {
            out = type.Credentials->GetUserCredentials().OauthToken;
        } else if (p1 == "cookie") {
            out = type.Credentials->GetUserCredentials().BlackboxSessionIdCookie;
        } else {
            YQL_ENSURE(false, "unexpected token id");
        }
    } else if (p0 == "token" || p0 == "cluster") {
        const auto p1 = string.substr(separator + 1);
        auto cred = type.Credentials->FindCredential(p1);
        if (cred == nullptr) {
            if (p0 == "cluster") {
                TStringBuf clusterName = p1;
                if (clusterName.SkipPrefix("default_")) {
                    for (auto& x : type.DataSources) {
                        auto tokens = x->GetClusterTokens();
                        if (tokens) {
                            auto token = tokens->FindPtr(clusterName);
                            if (token) {
                                out = *token;
                                return;
                            }
                        }
                    }
                    for (auto& x : type.DataSinks) {
                        auto tokens = x->GetClusterTokens();
                        if (tokens) {
                            auto token = tokens->FindPtr(clusterName);
                            if (token) {
                                out = *token;
                                return;
                            }
                        }
                    }
                }
            }

            YQL_ENSURE(false, "unexpected token id");
        }

        out = cred->Content;
    } else {
        YQL_ENSURE(false, "unexpected token prefix");
    }
}

void FillSecureParams(
    const TExprNode::TPtr& root,
    const TTypeAnnotationContext& types,
    THashMap<TString, TString>& secureParams) {

    NYql::VisitExpr(root, [&secureParams](const TExprNode::TPtr& node) {
        if (auto maybeSecureParam = TMaybeNode<TCoSecureParam>(node)) {
            const auto& secureParamName = TString(maybeSecureParam.Cast().Name().Value());
            secureParams.insert({secureParamName, TString()});
        }
        return true;
    });

    for (auto& it : secureParams) {
        GetToken(it.first, it.second, types);
    }
}

bool AddPgFile(bool isPath, const TString& pathOrContent, const TString& md5, const TString& alias, TUserDataTable& files, 
    const TTypeAnnotationContext& types, TPositionHandle pos, TExprContext& ctx) {

    TUserDataBlock block;
    block.Data = pathOrContent;
    if (isPath) {
        block.Type = EUserDataType::PATH;
        block.Usage.Set(EUserDataBlockUsage::Path);
        block.Usage.Set(EUserDataBlockUsage::PgExt);
    } else {
        block.Type = EUserDataType::RAW_INLINE_DATA;
        block.Usage.Set(EUserDataBlockUsage::Content);
    }

    auto key = TUserDataKey::File(alias);
    if (const auto foundBlock = types.UserDataStorage->FindUserDataBlock(key)) {
        files[key] = *foundBlock;
        YQL_ENSURE(!isPath || foundBlock->FrozenFile);
    } else {
        // Check alias clash with user files
        if (files.contains(TUserDataStorage::ComposeUserDataKey(alias))) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "File " << alias << " clashes with one of the user's files"));
            return false;
        }

        // CreateFakeFileLink calculates md5 for file, let's do it once if needed
        if (isPath) {
            block.FrozenFile = CreateFakeFileLink(block.Data, md5);
        }

        files[key] = block;
        types.UserDataStorage->AddUserDataBlock(key, block);
    }

    return true;
}

bool FillUsedFiles(
    const TExprNode& node,
    TUserDataTable& files,
    const TTypeAnnotationContext& types,
    TExprContext& ctx,
    const TUserDataTable& crutches) {
    TNodeSet visited;
    ui64 usedPgExtensions = 0;
    bool needFullPgCatalog = false;
    auto ret = FillUsedFilesImpl(node, files, types, ctx, crutches, visited, usedPgExtensions, needFullPgCatalog);
    if (!ret) {
        return false;
    }

    auto remainingPgExtensions = usedPgExtensions;
    TSet<ui32> filter;
    for (ui32 extensionIndex = 1; remainingPgExtensions && (extensionIndex <= 64); ++extensionIndex) {
        auto mask = MakePgExtensionMask(extensionIndex);
        if (!(mask & usedPgExtensions)) {
            continue;
        }

        filter.insert(extensionIndex);
        remainingPgExtensions &= ~mask;
        const auto& e = NPg::LookupExtension(extensionIndex);
        needFullPgCatalog = true;
        auto alias = TFsPath(e.LibraryPath).GetName();
        if (!AddPgFile(true, e.LibraryPath, e.LibraryMD5, alias, files, types, node.Pos(), ctx)) {
            return false;
        }
    }
    
    Y_ENSURE(remainingPgExtensions == 0);
    if (!needFullPgCatalog) {
        return true;
    }

    TString content = NPg::ExportExtensions(filter);
    if (!AddPgFile(false, content, "", TString(PgCatalogFileName), files, types, node.Pos(), ctx)) {
        return false;
    }

    return true;
}

std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> FreezeUsedFiles(const TExprNode& node, TUserDataTable& files, const TTypeAnnotationContext& types, TExprContext& ctx, const std::function<bool(const TString&)>& urlDownloadFilter, const TUserDataTable& crutches) {
    if (!FillUsedFiles(node, files, types, ctx, crutches)) {
        return SyncError();
    }

    auto future = FreezeUserDataTableIfNeeded(types.UserDataStorage, files, urlDownloadFilter);
    if (future.Wait(TDuration::Zero())) {
        files = future.GetValue()();
        return SyncOk();
    }
    else {
        return std::make_pair(IGraphTransformer::TStatus::Async, future.Apply(
            [](const NThreading::TFuture<std::function<TUserDataTable()>>& completedFuture) {
                return TAsyncTransformCallback([completedFuture](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                    output = input;
                    try {
                        completedFuture.GetValue()();
                    }
                    catch (const std::exception& e) {
                        auto inputPos = ctx.GetPosition(input->Pos());
                        TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                            return MakeIntrusive<TIssue>(YqlIssue(inputPos, TIssuesIds::UNEXPECTED));
                        });
                        ctx.AddError(ExceptionToIssue(e, inputPos));
                        input->SetState(TExprNode::EState::Error);
                        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error);
                    }
                    catch (...) {
                        auto inputPos = ctx.GetPosition(input->Pos());
                        TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                            return MakeIntrusive<TIssue>(YqlIssue(inputPos, TIssuesIds::UNEXPECTED));
                        });
                        ctx.AddError(YqlIssue(inputPos, TIssuesIds::UNEXPECTED, CurrentExceptionMessage()));
                        input->SetState(TExprNode::EState::Error);
                        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error);
                    }

                    input->SetState(TExprNode::EState::ExecutionRequired);
                    return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat);
                });
            }));
    }
}

bool FreezeUsedFilesSync(const TExprNode& node, TUserDataTable& files, const TTypeAnnotationContext& types, TExprContext& ctx, const std::function<bool(const TString&)>& urlDownloadFilter) {
    if (!FillUsedFiles(node, files, types, ctx)) {
        return false;
    }

    auto future = FreezeUserDataTableIfNeeded(types.UserDataStorage, files, urlDownloadFilter);
    files = future.GetValueSync()();
    return true;
}

void WriteColumns(NYson::TYsonWriter& writer, const TExprBase& columns) {
    if (auto maybeList = columns.Maybe<TExprList>()) {
        writer.OnBeginList();
        for (const auto& column : maybeList.Cast()) {
            writer.OnListItem();
            if (column.Maybe<TCoAtom>()) {
                writer.OnStringScalar(column.Cast<TCoAtom>().Value());
            } else {
                writer.OnStringScalar(column.Cast<TCoAtomList>().Item(0).Value());
            }
        }
        writer.OnEndList();
    } else if (columns.Maybe<TCoVoid>()) {
        writer.OnStringScalar("*");
    } else {
        writer.OnStringScalar("?");
    }
}

TString SerializeExpr(TExprContext& ctx, const TExprNode& expr, bool withTypes) {
    ui32 typeFlags = TExprAnnotationFlags::None;
    if (withTypes) {
        typeFlags |= TExprAnnotationFlags::Types;
    }

    auto ast = ConvertToAst(expr, ctx, typeFlags, true);
    YQL_ENSURE(ast.Root);
    return ast.Root->ToString();
}

TString ExprToPrettyString(TExprContext& ctx, const TExprNode& expr) {
    auto ast = ConvertToAst(expr, ctx, TExprAnnotationFlags::None, true);
    TStringStream exprStream;
    YQL_ENSURE(ast.Root);
    ast.Root->PrettyPrintTo(exprStream, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
    TString exprText = exprStream.Str();

    return exprText;
}

bool IsFlowOrStream(const TExprNode* node) {
    auto kind = node->GetTypeAnn()->GetKind();
    return kind == ETypeAnnotationKind::Stream || kind == ETypeAnnotationKind::Flow;
}

void WriteStream(NYson::TYsonWriter& writer, const TExprNode* node, const TExprNode* source) {
    if (node == source) {
        return;
    }

    if (!node->IsCallable()) {
        return;
    }

    if (!node->GetTypeAnn()) {
        return;
    }

    TVector<const TExprNode*> applyStreamChildren;
    if (TCoApply::Match(node)) {
        switch (node->GetTypeAnn()->GetKind()) {
        case ETypeAnnotationKind::Stream:
        case ETypeAnnotationKind::Flow:
        case ETypeAnnotationKind::List:
            break;
        default:
            return;
        }

        for (size_t i = 1; i < node->ChildrenSize(); ++i) {
            if (IsFlowOrStream(*node->Child(i))) {
                applyStreamChildren.push_back(node->Child(i));
            } else if (node->Child(i)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
                if (node->Child(i)->IsCallable("ForwardList")) {
                    applyStreamChildren.push_back(node->Child(i)->Child(0));
                } else if (node->Child(i)->IsCallable("Collect") && IsFlowOrStream(node->Child(i)->Head())) {
                    applyStreamChildren.push_back(node->Child(i)->Child(0));
                }
            }
        }
        if (applyStreamChildren.size() == 1) {
            WriteStream(writer, applyStreamChildren.front(), source);
        }
    }
    else if (!TCoExtendBase::Match(node) && node->ChildrenSize() > 0) {
        WriteStream(writer, node->Child(0), source);
    }

    writer.OnListItem();
    writer.OnBeginMap();
    writer.OnKeyedItem("Name");
    writer.OnStringScalar(node->Content());
    if (TCoFlatMapBase::Match(node) && IsFlowOrStream(*node->Child(1))) {
        writer.OnKeyedItem("Children");
        writer.OnBeginList();
        writer.OnListItem();
        writer.OnBeginList();
        WriteStream(writer, node->Child(1)->Child(1), node->Child(1)->Head().Child(0));
        writer.OnEndList();
        writer.OnEndList();
    }

    if (TCoChopper::Match(node) || node->IsCallable("WideChopper")) {
        writer.OnKeyedItem("Children");
        writer.OnBeginList();
        writer.OnListItem();
        writer.OnBeginList();
        WriteStream(writer, &node->Tail().Tail(), &node->Tail().Head().Head());
        writer.OnEndList();
        writer.OnEndList();
    }

    if (TCoSwitch::Match(node)) {
        writer.OnKeyedItem("Children");
        writer.OnBeginList();
        for (size_t i = 3; i < node->ChildrenSize(); i += 2) {
            writer.OnListItem();
            writer.OnBeginList();
            WriteStream(writer, node->Child(i)->Child(1), node->Child(i)->Head().Child(0));
            writer.OnEndList();
        }

        writer.OnEndList();
    }

    if (TCoExtendBase::Match(node) && node->ChildrenSize() > 0) {
        writer.OnKeyedItem("Children");
        writer.OnBeginList();
        for (size_t i = 0; i < node->ChildrenSize(); ++i) {
            writer.OnListItem();
            writer.OnBeginList();
            WriteStream(writer, node->Child(i), source);
            writer.OnEndList();
        }

        writer.OnEndList();
    }

    if (TCoApply::Match(node) && applyStreamChildren.size() > 1) {
        writer.OnKeyedItem("Children");
        writer.OnBeginList();
        for (auto child: applyStreamChildren) {
            writer.OnListItem();
            writer.OnBeginList();
            WriteStream(writer, child, source);
            writer.OnEndList();
        }
        writer.OnEndList();
    }

    writer.OnEndMap();
}

void WriteStreams(NYson::TYsonWriter& writer, TStringBuf name, const TCoLambda& lambda) {
    writer.OnKeyedItem(name);
    writer.OnBeginList();
    WriteStream(writer, lambda.Body().Raw(), lambda.Args().Size() > 0 ? lambda.Args().Arg(0).Raw() : nullptr);
    writer.OnEndList();
}

double GetDataReplicationFactor(double factor, const TExprNode* node, const TExprNode* stream, TExprContext& ctx) {
    if (node == stream) {
        return factor;
    }

    if (!node->IsCallable()) {
        return factor;
    }

    if (TCoApply::Match(node)) {
        switch (node->GetTypeAnn()->GetKind()) {
        case ETypeAnnotationKind::Stream:
        case ETypeAnnotationKind::Flow:
        case ETypeAnnotationKind::List: {
            double applyFactor = 0.0;
            for (size_t i = 1; i < node->ChildrenSize(); ++i) {
                if (IsFlowOrStream(*node->Child(i))) {
                    applyFactor += GetDataReplicationFactor(factor, node->Child(i), stream, ctx);
                } else if (node->Child(i)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
                    if (node->Child(i)->IsCallable("ForwardList")) {
                        applyFactor += GetDataReplicationFactor(factor, node->Child(i)->Child(0), stream, ctx);
                    } else if (node->Child(i)->IsCallable("Collect") && IsFlowOrStream(node->Child(i)->Head())) {
                        applyFactor += GetDataReplicationFactor(factor, node->Child(i)->Child(0), stream, ctx);
                    }
                }
            }
            factor = 2.0 * Max(1.0, applyFactor);
            break;
        }
        default:
            break;
        }
        return factor;
    }

    if (!TCoExtendBase::Match(node) && node->ChildrenSize() > 0) {
        factor = GetDataReplicationFactor(factor, node->Child(0), stream, ctx);
    }

    if (TCoFlatMapBase::Match(node)) {
        // TODO: check MapJoinCore input unique using constraints
        if (const auto& lambda = node->Tail(); node->Head().IsCallable("SqueezeToDict") && lambda.Tail().IsCallable("MapJoinCore") && lambda.Tail().Child(1U) == &lambda.Head().Head()) {
            TMaybe<bool> isMany;
            TMaybe<EDictType> type;
            bool isCompact = false;
            TMaybe<ui64> itemsCount;
            ParseToDictSettings(node->Head(), ctx, type, isMany, itemsCount, isCompact);
            if (isMany.GetOrElse(true)) {
                factor *= 5.0;
            }
        } else {
            switch (lambda.GetTypeAnn()->GetKind()) {
            case ETypeAnnotationKind::Stream:
            case ETypeAnnotationKind::Flow:
                factor = GetDataReplicationFactor(factor, &lambda.Tail(), &lambda.Head().Head(), ctx);
                break;
            case ETypeAnnotationKind::List:
                factor *= 2.0;
                break;
            default:
                break;
            }
        }
    }
    else if (node->IsCallable("CommonJoinCore")) {
        factor *= 5.0;
    }
    else if (node->IsCallable("MapJoinCore")) {
        // TODO: check MapJoinCore input unique using constraints
        if (node->Child(1)->IsCallable("ToDict")) {
            TMaybe<bool> isMany;
            TMaybe<EDictType> type;
            bool isCompact = false;
            TMaybe<ui64> itemsCount;
            ParseToDictSettings(*node->Child(1), ctx, type, isMany, itemsCount, isCompact);
            if (isMany.GetOrElse(true)) {
                factor *= 5.0;
            }
        }
    }
    else if (TCoSwitch::Match(node)) {
        double switchFactor = 0.0;
        for (size_t i = 3; i < node->ChildrenSize(); i += 2) {
            switchFactor += GetDataReplicationFactor(factor, node->Child(i)->Child(1), node->Child(i)->Head().Child(0), ctx);
        }
        factor = Max(1.0, switchFactor);
    }
    else if (TCoExtendBase::Match(node) && node->ChildrenSize() > 0) {
        double extendFactor = 0.0;
        for (size_t i = 0; i < node->ChildrenSize(); ++i) {
            extendFactor += GetDataReplicationFactor(factor, node->Child(i), stream, ctx);
        }
        factor = Max(1.0, extendFactor);
    }
    else if (TCoChopper::Match(node) || node->IsCallable("WideChopper")) {
        factor = GetDataReplicationFactor(factor, &node->Child(TCoChopper::idx_Handler)->Tail(), &node->Child(TCoChopper::idx_Handler)->Head().Tail(), ctx);
    }

    return factor;
}

double GetDataReplicationFactor(const TExprNode& lambda, TExprContext& ctx) {
    return GetDataReplicationFactor(1.0, lambda.Child(1), lambda.Head().ChildrenSize() > 0 ? lambda.Head().Child(0) : nullptr, ctx);
}

void WriteStatistics(NYson::TYsonWriter& writer, const TOperationStatistics& statistics)
{
    writer.OnBeginMap();
    for (auto& el : statistics.Entries) {
        writer.OnKeyedItem(el.Name);
        if (el.Value) {
            writer.OnStringScalar(*el.Value);
            continue;
        }

        writer.OnBeginMap();
        if (auto val = el.Sum) {
            writer.OnKeyedItem("sum");
            writer.OnInt64Scalar(*val);
        }
        if (auto val = el.Count) {
            writer.OnKeyedItem("count");
            writer.OnInt64Scalar(*val);
        }
        if (auto val = el.Avg) {
            writer.OnKeyedItem("avg");
            writer.OnInt64Scalar(*val);
        }
        if (auto val = el.Max) {
            writer.OnKeyedItem("max");
            writer.OnInt64Scalar(*val);
        }
        if (auto val = el.Min) {
            writer.OnKeyedItem("min");
            writer.OnInt64Scalar(*val);
        }
        writer.OnEndMap();
    }
    writer.OnEndMap();
}

void WriteStatistics(NYson::TYsonWriter& writer, bool totalOnly, const THashMap<ui32, TOperationStatistics>& statistics, bool addTotalKey, bool addExternalMap) {
    if (statistics.empty()) {
        return;
    }

    THashMap<TString, std::tuple<i64, i64, i64, TMaybe<i64>>> total; // sum, count, max, min

    if (addExternalMap) {
        writer.OnBeginMap();
    }

    for (const auto& opStatistics : statistics) {
        for (auto& el : opStatistics.second.Entries) {
            if (el.Value) {
                continue;
            }

            auto& totalEntry = total[el.Name];
            if (auto val = el.Sum) {
                std::get<0>(totalEntry) += *val;
            }
            if (auto val = el.Count) {
                std::get<1>(totalEntry) += *val;
            }
            if (auto val = el.Max) {
                std::get<2>(totalEntry) = Max<i64>(*val, std::get<2>(totalEntry));
            }
            if (auto val = el.Min) {
                std::get<3>(totalEntry) = Min<i64>(*val, std::get<3>(totalEntry).GetOrElse(Max<i64>()));
            }
        }
    }

    if (totalOnly == false) {
        for (const auto& [key, value] : statistics) {
            writer.OnKeyedItem(ToString(key));
            WriteStatistics(writer, value);
        }
    }

    TVector<TString> statKeys;
    std::transform(total.cbegin(), total.cend(), std::back_inserter(statKeys), [](const decltype(total)::value_type& v) { return v.first; });
    std::sort(statKeys.begin(), statKeys.end());

    if (addTotalKey) {
        writer.OnKeyedItem("total");
        writer.OnBeginMap();
    }
    for (auto& key: statKeys) {
        auto& totalEntry = total[key];
        writer.OnKeyedItem(key);
        writer.OnBeginMap();

        writer.OnKeyedItem("sum");
        writer.OnInt64Scalar(std::get<0>(totalEntry));

        writer.OnKeyedItem("count");
        writer.OnInt64Scalar(std::get<1>(totalEntry));

        writer.OnKeyedItem("avg");
        writer.OnInt64Scalar(std::get<1>(totalEntry) ? (std::get<0>(totalEntry) / std::get<1>(totalEntry)) : 0l);

        writer.OnKeyedItem("max");
        writer.OnInt64Scalar(std::get<2>(totalEntry));

        writer.OnKeyedItem("min");
        writer.OnInt64Scalar(std::get<3>(totalEntry).GetOrElse(0));

        writer.OnEndMap();
    }
    if (addTotalKey) {
        writer.OnEndMap(); // total
    }
    if (addExternalMap) {
        writer.OnEndMap();
    }
}

bool ValidateCompressionForInput(std::string_view format, std::string_view compression, TExprContext& ctx) {
    if (compression.empty()) {
        return true;
    }
    if (format == "parquet"sv) {
        ctx.AddError(TIssue(TStringBuilder() << "External compression for parquet is not supported"));
        return false;
    }
    if (IsIn(Compressions, compression)) {
        return true;
    }
    ctx.AddError(TIssue(TStringBuilder() << "Unknown compression: " << compression
        << ". Use one of: " << JoinSeq(", ", Compressions)));
    return false;
}

bool ValidateCompressionForOutput(std::string_view format, std::string_view compression, TExprContext& ctx) {
    if (compression.empty()) {
        return true;
    }
    if (format == "parquet"sv) {
        ctx.AddError(TIssue(TStringBuilder() << "External compression for parquet is not supported"));
        return false;
    }
    if (IsIn(Compressions, compression)) {
        return true;
    }
    ctx.AddError(TIssue(TStringBuilder() << "Unknown compression: " << compression
        << ". Use one of: " << JoinSeq(", ", Compressions)));
    return false;
}

bool ValidateFormatForInput(
    std::string_view format,
    const TStructExprType* schemaStructRowType,
    const std::function<bool(TStringBuf)>& excludeFields,
    TExprContext& ctx) {
    if (format.empty()) {
        return true;
    }

    if (!IsIn(FormatsForInput, format)) {
        ctx.AddError(TIssue(TStringBuilder() << "Unknown format: " << format
            << ". Use one of: " << JoinSeq(", ", FormatsForInput)));
        return false;
    }

    if (schemaStructRowType && format == TStringBuf("raw")) {
        ui64 realSchemaColumnsCount = 0;

        for (const TItemExprType* item : schemaStructRowType->GetItems()) {
            if (excludeFields && excludeFields(item->GetName())) {
                continue;
            }
            const TTypeAnnotationNode* rowType = item->GetItemType();
            if (rowType->GetKind() == ETypeAnnotationKind::Optional) {
                rowType = rowType->Cast<TOptionalExprType>()->GetItemType();
            }

            if (rowType->GetKind() != ETypeAnnotationKind::Data
                || !IsDataTypeString(rowType->Cast<TDataExprType>()->GetSlot())) {
                ctx.AddError(TIssue(TStringBuilder() << "Only string type column in schema supported in raw format (you have '"
                    << item->GetName() << " " << FormatType(rowType) << "' field)"));
                return false;
            }
            ++realSchemaColumnsCount;
        }

        if (realSchemaColumnsCount != 1) {
            ctx.AddError(TIssue(TStringBuilder() << "Only one column in schema supported in raw format (you have "
                << realSchemaColumnsCount << " fields)"));
            return false;
        }
    }
    return true;
}

bool ValidateFormatForOutput(std::string_view format, TExprContext& ctx) {
    if (format.empty() || IsIn(FormatsForOutput, format)) {
        return true;
    }
    ctx.AddError(TIssue(TStringBuilder() << "Unknown format: " << format
        << ". Use one of: " << JoinSeq(", ", FormatsForOutput)));
    return false;
}

template<typename T>
bool ValidateValueInDictionary(std::string_view value, TExprContext& ctx, const T& dictionary) {
    if (value.empty() || IsIn(dictionary, value)) {
        return true;
    }
    ctx.AddError(TIssue(TStringBuilder() << "Unknown format: " << value
        << ". Use one of: " << JoinSeq(", ", dictionary)));
    return false;
}

bool ValidateIntervalUnit(std::string_view unit, TExprContext& ctx) {
    return ValidateValueInDictionary(unit, ctx, IntervalUnits);
}

bool ValidateDateTimeFormatName(std::string_view formatName, TExprContext& ctx) {
    return ValidateValueInDictionary(formatName, ctx, DateTimeFormatNames);
}

bool ValidateTimestampFormatName(std::string_view formatName, TExprContext& ctx) {
    return ValidateValueInDictionary(formatName, ctx, TimestampFormatNames);
}

namespace {
    bool MatchesSetItemOption(const TExprBase& setItemOption, TStringBuf name) {
        if (setItemOption.Ref().IsList() && setItemOption.Ref().ChildrenSize() > 0) {
            if (setItemOption.Ref().ChildPtr(0)->Content() == name) {
                return true;
            }
        }
        return false;
    }
} //namespace

bool TransformPgSetItemOption(
    const TCoPgSelect& pgSelect,
    TStringBuf optionName,
    std::function<void(const TExprBase&)> lambda
) {
    bool applied = false;
    for (const auto& option : pgSelect.SelectOptions()) {
        if (option.Name() == "set_items") {
            auto pgSetItems = option.Value().Cast<TExprList>();
            for (const auto& setItem : pgSetItems) {
                auto setItemNode = setItem.Cast<TCoPgSetItem>();
                for (const auto& setItemOption : setItemNode.SetItemOptions()) {
                    if (MatchesSetItemOption(setItemOption, optionName)) {
                        applied = true;
                        lambda(setItemOption);
                    }
                }
            }
        }
    }
    return applied;
}

TExprNode::TPtr GetSetItemOption(const TCoPgSelect& pgSelect, TStringBuf optionName) {
    TExprNode::TPtr nodePtr = nullptr;
    TransformPgSetItemOption(pgSelect, optionName, [&nodePtr](const TExprBase& option) {
        nodePtr = option.Ptr();
    });
    return nodePtr;
}

TExprNode::TPtr GetSetItemOptionValue(const TExprBase& setItemOption) {
    if (setItemOption.Ref().IsList() && setItemOption.Ref().ChildrenSize() > 1) {
        return setItemOption.Ref().ChildPtr(1);
    }
    return nullptr;
}

bool NeedToRenamePgSelectColumns(const TCoPgSelect& pgSelect) {
    auto fill = NCommon::GetSetItemOption(pgSelect, "fill_target_columns");
    return fill && !NCommon::GetSetItemOptionValue(TExprBase(fill));
}

bool RenamePgSelectColumns(
    const TCoPgSelect& node,
    TExprNode::TPtr& output,
    const TMaybe<TColumnOrder>& tableColumnOrder,
    TExprContext& ctx,
    TTypeAnnotationContext& types) {

    bool hasValues = (bool)GetSetItemOption(node, "values");
    bool hasProjectionOrder = (bool)GetSetItemOption(node, "projection_order");
    Y_ENSURE(hasValues ^ hasProjectionOrder, "Only one of values and projection_order should be present");
    TString optionName = (hasValues) ? "values" : "projection_order";

    auto selectorColumnOrder = types.LookupColumnOrder(node.Ref());
    TColumnOrder insertColumnOrder;
    if (auto targetColumnsOption = GetSetItemOption(node, "target_columns")) {
        auto targetColumns = GetSetItemOptionValue(TExprBase(targetColumnsOption));
        for (const auto& child : targetColumns->ChildrenList()) {
            insertColumnOrder.AddColumn(TString(child->Content()));
        }
    } else {
        YQL_ENSURE(tableColumnOrder);
        insertColumnOrder = *tableColumnOrder;
    }
    YQL_ENSURE(selectorColumnOrder);
    if (selectorColumnOrder->Size() > insertColumnOrder.Size()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << Sprintf(
            "%s have %zu columns, INSERT INTO expects: %zu",
            optionName.Data(),
            selectorColumnOrder->Size(),
            insertColumnOrder.Size()
        )));
        return false;
    }

    if (*selectorColumnOrder == insertColumnOrder) {
        output = node.Ptr();
        return true;
    }

    TVector<const TItemExprType*> rowTypeItems;
    rowTypeItems.reserve(selectorColumnOrder->Size());
    const TTypeAnnotationNode* inputType;
    switch (node.Ref().GetTypeAnn()->GetKind()) {
        case ETypeAnnotationKind::List:
            inputType = node.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            break;
        default:
            inputType = node.Ref().GetTypeAnn();
            break;
    }
    YQL_ENSURE(inputType->GetKind() == ETypeAnnotationKind::Struct);

    const auto rowArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("row")
        .Done();
    auto structBuilder = Build<TCoAsStruct>(ctx, node.Pos());

    for (size_t i = 0; i < selectorColumnOrder->Size(); i++) {
        const auto& columnName = selectorColumnOrder->at(i);
        structBuilder.Add<TCoNameValueTuple>()
            .Name().Build(insertColumnOrder.at(i).PhysicalName)
            .Value<TCoMember>()
                .Struct(rowArg)
                .Name().Build(columnName.PhysicalName)
            .Build()
        .Build();
    }

    auto fill = GetSetItemOption(node, "fill_target_columns");

    output = Build<TCoMap>(ctx, node.Pos())
        .Input(node)
        .Lambda<TCoLambda>()
            .Args({rowArg})
            .Body(structBuilder.Done().Ptr())
        .Build()
    .Done().Ptr();

    fill->ChangeChildrenInplace({
        fill->Child(0),
        Build<TCoAtom>(ctx, node.Pos())
            .Value("done")
        .Done().Ptr()
    });
    fill->ChildPtr(1)->SetTypeAnn(ctx.MakeType<TUnitExprType>());

    return true;
}

} // namespace NCommon
} // namespace NYql
