#include "yql_pq_dq_integration.h"
#include "yql_pq_helpers.h"
#include "yql_pq_mkql_compiler.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/string/builder.h>

namespace NYql {

using namespace NNodes;

namespace {

class TPqDqIntegration: public TDqIntegrationBase {
public:
    explicit TPqDqIntegration(const TPqState::TPtr& state)
        : State_(state.Get())
    {
    }

    ui64 PartitionTopicRead(const TPqTopic& topic, size_t maxPartitions, TVector<TString>& partitions) {
        size_t topicPartitionsCount = 0;
        for (auto kv : topic.Props()) {
            auto key = kv.Name().Value();
            if (key == PartitionsCountProp) {
                topicPartitionsCount = FromString(kv.Value().Ref().Content());
            }
        }
        YQL_ENSURE(topicPartitionsCount > 0);

        const size_t tasks = Min(maxPartitions, topicPartitionsCount);
        partitions.reserve(tasks);
        for (size_t i = 0; i < tasks; ++i) {
            NPq::NProto::TDqReadTaskParams params;
            auto* partitioninigParams = params.MutablePartitioningParams();
            partitioninigParams->SetTopicPartitionsCount(topicPartitionsCount);
            partitioninigParams->SetEachTopicPartitionGroupId(i);
            partitioninigParams->SetDqPartitionsCount(tasks);
            YQL_CLOG(DEBUG, ProviderPq) << "Create DQ reading partition " << params;

            TString serializedParams;
            YQL_ENSURE(params.SerializeToString(&serializedParams));
            partitions.emplace_back(std::move(serializedParams));
        }
        return 0;
    }

    ui64 Partition(const TDqSettings&, size_t maxPartitions, const TExprNode& node, TVector<TString>& partitions, TString*, TExprContext&, bool) override {
        if (auto maybePqRead = TMaybeNode<TPqReadTopic>(&node)) {
            return PartitionTopicRead(maybePqRead.Cast().Topic(), maxPartitions, partitions);
        }
        if (auto maybeDqSource = TMaybeNode<TDqSource>(&node)) {
            auto settings = maybeDqSource.Cast().Settings();
            if (auto topicSource = TMaybeNode<TDqPqTopicSource>(settings.Raw())) {
                return PartitionTopicRead(topicSource.Cast().Topic(), maxPartitions, partitions);
            }
        }
        return 0;
    }

    TExprNode::TPtr WrapRead(const TDqSettings& dqSettings, const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (const auto& maybePqReadTopic = TMaybeNode<TPqReadTopic>(read)) {
            const auto& pqReadTopic = maybePqReadTopic.Cast();
            YQL_ENSURE(pqReadTopic.Ref().GetTypeAnn(), "No type annotation for node " << pqReadTopic.Ref().Content());

            const auto rowType = pqReadTopic.Ref().GetTypeAnn()
                ->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()
                ->GetItemType()->Cast<TStructExprType>();
            const auto& clusterName = pqReadTopic.DataSource().Cluster().StringValue();

            TVector<TCoNameValueTuple> settings;
            settings.push_back(Build<TCoNameValueTuple>(ctx, pqReadTopic.Pos())
                .Name().Build("format")
                .Value(pqReadTopic.Format())
                .Done());

            auto format = pqReadTopic.Format().Ref().Content();

            TVector<TCoNameValueTuple> innerSettings;
            if (pqReadTopic.Compression() != "") {
                innerSettings.push_back(Build<TCoNameValueTuple>(ctx, pqReadTopic.Pos())
                        .Name().Build("compression")
                        .Value(pqReadTopic.Compression())
                    .Done());
            }

            if (!innerSettings.empty()) {
                settings.push_back(Build<TCoNameValueTuple>(ctx, pqReadTopic.Pos())
                    .Name().Build("settings")
                    .Value<TCoNameValueTupleList>()
                        .Add(innerSettings)
                        .Build()
                    .Done());
            }

            TExprNode::TListType metadataFieldsList;
            for (auto sysColumn : AllowedPqMetaSysColumns()) {
                metadataFieldsList.push_back(ctx.NewAtom(pqReadTopic.Pos(), sysColumn));
            }

            settings.push_back(Build<TCoNameValueTuple>(ctx, pqReadTopic.Pos())
                .Name().Build("metadataColumns")
                .Value(ctx.NewList(pqReadTopic.Pos(), std::move(metadataFieldsList)))
                .Done());


            settings.push_back(Build<TCoNameValueTuple>(ctx, pqReadTopic.Pos())
                .Name().Build("formatSettings")
                .Value(std::move(pqReadTopic.Settings()))
                .Done());

            const auto token = "cluster:default_" + clusterName;
            // auto columns = pqReadTopic.Columns().Ptr();
            // if (!columns->IsList()) {
            //     const auto pos = columns->Pos();
            //     const auto& items = rowType->GetItems();
            //     TExprNode::TListType cols;
            //     cols.reserve(items.size());
            //     std::transform(items.cbegin(), items.cend(), std::back_inserter(cols),
            //         [&](const TItemExprType* item) {
            //             const TTypeAnnotationNode* type =  item->GetItemType();
            //             YQL_CLOG(DEBUG, ProviderPq) << "type type " << FormatType(type);
            //             return ctx.NewAtom(pos, item->GetName());
            //         });
            //     columns = ctx.NewList(pos, std::move(cols));
            // }

            // const auto& typeItems = rowType->GetItems();
            // YQL_CLOG(DEBUG, ProviderPq) << "size " << items.size();
            // for (const auto item : items) {
            //     const TTypeAnnotationNode* type =  item->GetItemType();
            //     YQL_CLOG(DEBUG, ProviderPq) << item->GetName() << ": " << "type type2 " << FormatType(type);
            // }

            auto rowSchema = pqReadTopic.Topic().RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
            TExprNode::TListType colTypes;
            const auto& typeItems = rowSchema->GetItems();
            colTypes.reserve(typeItems.size());
            const auto pos = read->Pos(); // TODO
            std::transform(typeItems.cbegin(), typeItems.cend(), std::back_inserter(colTypes),
                [&](const TItemExprType* item) {
                    return ctx.NewAtom(pos, FormatType(item->GetItemType()));
                });
            auto columnTypes = ctx.NewList(pos, std::move(colTypes));
            
            TExprNode::TListType colNames;
            colNames.reserve(typeItems.size());
            std::transform(typeItems.cbegin(), typeItems.cend(), std::back_inserter(colNames),
                [&](const TItemExprType* item) {
                    return ctx.NewAtom(pos, item->GetName());
                });
            auto columnNames = ctx.NewList(pos, std::move(colNames));
    
            auto row = Build<TCoArgument>(ctx, read->Pos())
                .Name("row")
                .Done();
            auto emptyPredicate = Build<TCoLambda>(ctx, read->Pos())
                .Args({row})
                .Body<TCoBool>()
                    .Literal().Build("true")
                    .Build()
                .Done().Ptr();


            return Build<TDqSourceWrap>(ctx, read->Pos())
                .Input<TDqPqTopicSource>()
                    .Topic(pqReadTopic.Topic())
                    .Columns(std::move(columnNames))    // TODO
                    .Settings(BuildTopicReadSettings(clusterName, dqSettings, read->Pos(), format, ctx))
                    .Token<TCoSecureParam>()
                        .Name().Build(token)
                        .Build()
                    .FilterPredicate(emptyPredicate)
                    .ColumnTypes(std::move(columnTypes))
                    .Build()
                .RowType(ExpandType(pqReadTopic.Pos(), *rowType, ctx))
                .DataSource(pqReadTopic.DataSource().Cast<TCoDataSource>())
                .Settings(Build<TCoNameValueTupleList>(ctx, read->Pos()).Add(settings).Done())
                .Done().Ptr();
        }
        return read;
    }

    TMaybe<bool> CanWrite(const TExprNode&, TExprContext&) override {
        YQL_ENSURE(false, "Unimplemented");
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        RegisterDqPqMkqlCompilers(compiler);
    }

    static TStringBuf Name(const TCoNameValueTuple& nameValue) {
        return nameValue.Name().Value();
    }

    static TStringBuf Value(const TCoNameValueTuple& nameValue) {
        if (TMaybeNode<TExprBase> maybeValue = nameValue.Value()) {
            const TExprNode& value = maybeValue.Cast().Ref();
            YQL_ENSURE(value.IsAtom());
            return value.Content();
        }

        return {};
    }

    static NPq::NProto::EClusterType ToClusterType(NYql::TPqClusterConfig::EClusterType t) {
        switch (t) {
        case NYql::TPqClusterConfig::CT_UNSPECIFIED:
            return NPq::NProto::Unspecified;
        case NYql::TPqClusterConfig::CT_PERS_QUEUE:
            return NPq::NProto::PersQueue;
        case NYql::TPqClusterConfig::CT_DATA_STREAMS:
            return NPq::NProto::DataStreams;
        }
    }

    bool SerializeMember(const TCoMember& member, NPq::NProto::TExpression* proto, const TCoArgument& arg, TStringBuilder& err) {
        if (member.Struct().Raw() != arg.Raw()) { // member callable called not for lambda argument
            err << "member callable called not for lambda argument";
            return false;
        }
        proto->set_column(member.Name().StringValue());
        return true;
    }

    template <class T>
    T Cast(const TStringBuf& from) {
        return FromString<T>(from);
    }

    // Special convertation from TStringBuf to TString
    template <>
    TString Cast<TString>(const TStringBuf& from) {
        return TString(from);
    }

#define MATCH_ATOM(AtomType, ATOM_ENUM, proto_name, cpp_type)                             \
    if (auto atom = expression.Maybe<Y_CAT(TCo, AtomType)>()) {                           \
        auto* value = proto->mutable_typed_value();                                       \
        auto* t = value->mutable_type();                                                  \
        t->set_type_id(Ydb::Type::ATOM_ENUM);                                             \
        auto* v = value->mutable_value();                                                 \
        v->Y_CAT(Y_CAT(set_, proto_name), _value)(Cast<cpp_type>(atom.Cast().Literal())); \
        return true;                                                                      \
    }

#define MATCH_ARITHMETICAL(OpType, OP_ENUM)                                                                                                                                  \
    if (auto maybeExpr = expression.Maybe<Y_CAT(TCo, OpType)>()) {                                                                                                           \
        auto expr = maybeExpr.Cast();                                                                                                                                        \
        auto* exprProto = proto->mutable_arithmetical_expression();                                                                                                          \
        exprProto->set_operation(NPq::NProto::TExpression::TArithmeticalExpression::OP_ENUM);                                                                                             \
        return SerializeExpression(expr.Left(), exprProto->mutable_left_value(), arg, err) && SerializeExpression(expr.Right(), exprProto->mutable_right_value(), arg, err); \
    }

    bool SerializeExpression(const TExprBase& expression, NPq::NProto::TExpression* proto, const TCoArgument& arg, TStringBuilder& err) {
        if (auto member = expression.Maybe<TCoMember>()) {
            return SerializeMember(member.Cast(), proto, arg, err);
        }

        // data
        MATCH_ATOM(Int8, INT8, int32, i8);
        MATCH_ATOM(Uint8, UINT8, uint32, ui8);
        MATCH_ATOM(Int16, INT16, int32, i16);
        MATCH_ATOM(Uint16, UINT16, uint32, ui16);
        MATCH_ATOM(Int32, INT32, int32, i32);
        MATCH_ATOM(Uint32, UINT32, uint32, ui32);
        MATCH_ATOM(Int64, INT64, int64, i64);
        MATCH_ATOM(Uint64, UINT64, uint64, ui64);
        MATCH_ATOM(Float, FLOAT, float, float);
        MATCH_ATOM(Double, DOUBLE, double, double);
        MATCH_ATOM(String, STRING, bytes, TString);
        MATCH_ATOM(Utf8, UTF8, text, TString);
        MATCH_ARITHMETICAL(Sub, SUB);
        MATCH_ARITHMETICAL(Add, ADD);
        MATCH_ARITHMETICAL(Mul, MUL);

        if (auto maybeNull = expression.Maybe<TCoNull>()) {
            proto->mutable_null();
            return true;
        }

        err << "unknown expression: " << expression.Raw()->Content();
        return false;
    }

#undef MATCH_ATOM

#define EXPR_NODE_TO_COMPARE_TYPE(TExprNodeType, COMPARE_TYPE)       \
    if (!opMatched && compare.Maybe<TExprNodeType>()) {              \
        opMatched = true;                                            \
        proto->set_operation(NPq::NProto::TPredicate::TComparison::COMPARE_TYPE); \
    }

    bool SerializeCompare(const TCoCompare& compare, NPq::NProto::TPredicate* predicateProto, const TCoArgument& arg, TStringBuilder& err) {
        NPq::NProto::TPredicate::TComparison* proto = predicateProto->mutable_comparison();
        bool opMatched = false;

        EXPR_NODE_TO_COMPARE_TYPE(TCoCmpEqual, EQ);
        EXPR_NODE_TO_COMPARE_TYPE(TCoCmpNotEqual, NE);
        EXPR_NODE_TO_COMPARE_TYPE(TCoCmpLess, L);
        EXPR_NODE_TO_COMPARE_TYPE(TCoCmpLessOrEqual, LE);
        EXPR_NODE_TO_COMPARE_TYPE(TCoCmpGreater, G);
        EXPR_NODE_TO_COMPARE_TYPE(TCoCmpGreaterOrEqual, GE);

        if (proto->operation() == NPq::NProto::TPredicate::TComparison::COMPARISON_OPERATION_UNSPECIFIED) {
            err << "unknown operation: " << compare.Raw()->Content();
            return false;
        }
        return SerializeExpression(compare.Left(), proto->mutable_left_value(), arg, err) && SerializeExpression(compare.Right(), proto->mutable_right_value(), arg, err);
    }

#undef EXPR_NODE_TO_COMPARE_TYPE

    bool SerializeCoalesce(const TCoCoalesce& coalesce, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
        auto predicate = coalesce.Predicate();
        if (auto compare = predicate.Maybe<TCoCompare>()) {
            return SerializeCompare(compare.Cast(), proto, arg, err);
        }

        err << "unknown coalesce predicate: " << predicate.Raw()->Content();
        return false;
    }


    bool SerializeExists(const TCoExists& exists, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, bool withNot = false) {
        auto* expressionProto = withNot ? proto->mutable_is_null()->mutable_value() : proto->mutable_is_not_null()->mutable_value();
        return SerializeExpression(exists.Optional(), expressionProto, arg, err);
    }

    bool SerializeAnd(const TCoAnd& andExpr, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
        auto* dstProto = proto->mutable_conjunction();
        for (const auto& child : andExpr.Ptr()->Children()) {
            if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), arg, err)) {
                return false;
            }
        }
        return true;
    }

    bool SerializeOr(const TCoOr& orExpr, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
        auto* dstProto = proto->mutable_disjunction();
        for (const auto& child : orExpr.Ptr()->Children()) {
            if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), arg, err)) {
                return false;
            }
        }
        return true;
    }

    bool SerializeNot(const TCoNot& notExpr, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
        // Special case: (Not (Exists ...))
        if (auto exists = notExpr.Value().Maybe<TCoExists>()) {
            return SerializeExists(exists.Cast(), proto, arg, err, true);
        }
        auto* dstProto = proto->mutable_negation();
        return SerializePredicate(notExpr.Value(), dstProto->mutable_operand(), arg, err);
    }

    bool SerializeMember(const TCoMember& member, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
        return SerializeMember(member, proto->mutable_bool_expression()->mutable_value(), arg, err);
    }

    bool SerializePredicate(const TExprBase& predicate, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
        if (auto compare = predicate.Maybe<TCoCompare>()) {
            return SerializeCompare(compare.Cast(), proto, arg, err);
        }
        if (auto coalesce = predicate.Maybe<TCoCoalesce>()) {
            return SerializeCoalesce(coalesce.Cast(), proto, arg, err);
        }
        if (auto andExpr = predicate.Maybe<TCoAnd>()) {
            return SerializeAnd(andExpr.Cast(), proto, arg, err);
        }
        if (auto orExpr = predicate.Maybe<TCoOr>()) {
            return SerializeOr(orExpr.Cast(), proto, arg, err);
        }
        if (auto notExpr = predicate.Maybe<TCoNot>()) {
            return SerializeNot(notExpr.Cast(), proto, arg, err);
        }
        if (auto member = predicate.Maybe<TCoMember>()) {
            return SerializeMember(member.Cast(), proto, arg, err);
        }
        if (auto exists = predicate.Maybe<TCoExists>()) {
            return SerializeExists(exists.Cast(), proto, arg, err);
        }

        err << "unknown predicate: " << predicate.Raw()->Content();
        return false;
    }

    bool IsEmptyFilterPredicate(const TCoLambda& lambda) {
        auto maybeBool = lambda.Body().Maybe<TCoBool>();
        if (!maybeBool) {
            return false;
        }
        return TStringBuf(maybeBool.Cast().Literal()) == "true"sv;
    }

    bool SerializeFilterPredicate(const TCoLambda& predicate, NPq::NProto::TPredicate* proto, TStringBuilder& err) {
        return SerializePredicate(predicate.Body(), proto, predicate.Args().Arg(0), err);
    }

    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType, size_t) override {
        if (auto maybeDqSource = TMaybeNode<TDqSource>(&node)) {
            auto settings = maybeDqSource.Cast().Settings();
            if (auto maybeTopicSource = TMaybeNode<TDqPqTopicSource>(settings.Raw())) {
                NPq::NProto::TDqPqTopicSource srcDesc;
                TDqPqTopicSource topicSource = maybeTopicSource.Cast();

                TPqTopic topic = topicSource.Topic();
                srcDesc.SetTopicPath(TString(topic.Path().Value()));
                srcDesc.SetDatabase(TString(topic.Database().Value()));
                const TStringBuf cluster = topic.Cluster().Value();
                const auto* clusterDesc = State_->Configuration->ClustersConfigurationSettings.FindPtr(cluster);
                YQL_ENSURE(clusterDesc, "Unknown cluster " << cluster);
                srcDesc.SetClusterType(ToClusterType(clusterDesc->ClusterType));
                srcDesc.SetDatabaseId(clusterDesc->DatabaseId);

                bool useRowDispatcher = false;
                TString format;
                size_t const settingsCount = topicSource.Settings().Size();
                for (size_t i = 0; i < settingsCount; ++i) {
                    TCoNameValueTuple setting = topicSource.Settings().Item(i);
                    const TStringBuf name = Name(setting);
                    if (name == ConsumerSetting) {
                        srcDesc.SetConsumerName(TString(Value(setting)));
                    } else if (name == EndpointSetting) {
                        srcDesc.SetEndpoint(TString(Value(setting)));
                    } else if (name == UseRowDispatcher) {
                        useRowDispatcher = FromString<bool>(Value(setting));
                    } else if (name == Format) {
                        format = TString(Value(setting));
                    } else if (name == UseSslSetting) {
                        srcDesc.SetUseSsl(FromString<bool>(Value(setting)));
                    } else if (name == AddBearerToTokenSetting) {
                        srcDesc.SetAddBearerToToken(FromString<bool>(Value(setting)));
                    } else if (name == WatermarksEnableSetting) {
                        srcDesc.MutableWatermarks()->SetEnabled(true);
                    } else if (name == WatermarksGranularityUsSetting) {
                        srcDesc.MutableWatermarks()->SetGranularityUs(FromString<ui64>(Value(setting)));
                    } else if (name == WatermarksLateArrivalDelayUsSetting) {
                        srcDesc.MutableWatermarks()->SetLateArrivalDelayUs(FromString<ui64>(Value(setting)));
                    } else if (name == WatermarksIdlePartitionsSetting) {
                        srcDesc.MutableWatermarks()->SetIdlePartitionsEnabled(true);
                    }
                }

                if (auto maybeToken = TMaybeNode<TCoSecureParam>(topicSource.Token().Raw())) {
                    srcDesc.MutableToken()->SetName(TString(maybeToken.Cast().Name().Value()));
                }

                if (clusterDesc->ClusterType == NYql::TPqClusterConfig::CT_PERS_QUEUE) {
                    YQL_ENSURE(srcDesc.GetConsumerName(), "No consumer specified for PersQueue cluster");
                }

                for (const auto metadata : topic.Metadata()) {
                    srcDesc.AddMetadataFields(metadata.Value().Maybe<TCoAtom>().Cast().StringValue());
                }

                for (const auto& column : topicSource.Columns().Cast<TCoAtomList>()) {
                    srcDesc.AddColumns(column.StringValue());
                }

                for (const auto& columnTypes : topicSource.ColumnTypes().Cast<TCoAtomList>()) {
                    srcDesc.AddColumnTypes(columnTypes.StringValue());
                }
            
                if (auto predicate = topicSource.FilterPredicate(); !IsEmptyFilterPredicate(predicate)) {
                    TStringBuilder err;
                    if (!SerializeFilterPredicate(predicate, srcDesc.MutablePredicate(), err)) {
                        ythrow yexception() << "Failed to serialize filter predicate for source: " << err;
                    }
                }

                YQL_CLOG(INFO, Core) << "UseRowDispatcher " << useRowDispatcher;
                YQL_CLOG(INFO, Core) << "UseRowDispatcher format" << format;

                //useRowDispatcher = true; // TODO
                protoSettings.PackFrom(srcDesc);
                useRowDispatcher = useRowDispatcher && (format == "json_each_row");
                sourceType = !useRowDispatcher ? "PqSource" : "PqRdSource";
            }
        }
    }

    void FillSinkSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sinkType) override {
        if (auto maybeDqSink = TMaybeNode<TDqSink>(&node)) {
            auto settings = maybeDqSink.Cast().Settings();
            if (auto maybeTopicSink = TMaybeNode<TDqPqTopicSink>(settings.Raw())) {
                NPq::NProto::TDqPqTopicSink sinkDesc;
                TDqPqTopicSink topicSink = maybeTopicSink.Cast();

                TPqTopic topic = topicSink.Topic();
                const TStringBuf cluster = topic.Cluster().Value();
                const auto* clusterDesc = State_->Configuration->ClustersConfigurationSettings.FindPtr(cluster);
                YQL_ENSURE(clusterDesc, "Unknown cluster " << cluster);
                sinkDesc.SetClusterType(ToClusterType(clusterDesc->ClusterType));
                sinkDesc.SetTopicPath(TString(topic.Path().Value()));
                sinkDesc.SetDatabase(TString(topic.Database().Value()));

                size_t const settingsCount = topicSink.Settings().Size();
                for (size_t i = 0; i < settingsCount; ++i) {
                    TCoNameValueTuple setting = topicSink.Settings().Item(i);
                    const TStringBuf name = Name(setting);
                    if (name == EndpointSetting) {
                        sinkDesc.SetEndpoint(TString(Value(setting)));
                    } else if (name == UseSslSetting) {
                        sinkDesc.SetUseSsl(FromString<bool>(Value(setting)));
                    } else if (name == AddBearerToTokenSetting) {
                        sinkDesc.SetAddBearerToToken(FromString<bool>(Value(setting)));
                    }
                }

                if (auto maybeToken = TMaybeNode<TCoSecureParam>(topicSink.Token().Raw())) {
                    sinkDesc.MutableToken()->SetName(TString(maybeToken.Cast().Name().Value()));
                }

                protoSettings.PackFrom(sinkDesc);
                sinkType = "PqSink";
            }
        }
    }

    NNodes::TCoNameValueTupleList BuildTopicReadSettings(
        const TString& cluster,
        const TDqSettings& dqSettings,
        TPositionHandle pos,
        std::string_view format,
        TExprContext& ctx) const
    {
        TVector<TCoNameValueTuple> props;

        {
            TMaybe<TString> consumer = State_->Configuration->Consumer.Get();
            if (consumer) {
                Add(props, ConsumerSetting, *consumer, pos, ctx);
            }
        }

        auto clusterConfiguration = State_->Configuration->ClustersConfigurationSettings.FindPtr(cluster);
        if (!clusterConfiguration) {
            ythrow yexception() << "Unknown pq cluster \"" << cluster << "\"";
        }

        Add(props, EndpointSetting, clusterConfiguration->Endpoint, pos, ctx);
        Add(props, UseRowDispatcher, ToString(clusterConfiguration->UseRowDispatcher), pos, ctx);
        Add(props, Format, format, pos, ctx);

        
        if (clusterConfiguration->UseSsl) {
            Add(props, UseSslSetting, "1", pos, ctx);
        }

        if (clusterConfiguration->AddBearerToToken) {
            Add(props, AddBearerToTokenSetting, "1", pos, ctx);
        }

        if (dqSettings.WatermarksMode.Get().GetOrElse("") == "default") {
            Add(props, WatermarksEnableSetting, ToString(true), pos, ctx);

            const auto granularity = TDuration::MilliSeconds(dqSettings
                .WatermarksGranularityMs
                .Get()
                .GetOrElse(TDqSettings::TDefault::WatermarksGranularityMs));
            Add(props, WatermarksGranularityUsSetting, ToString(granularity.MicroSeconds()), pos, ctx);

            const auto lateArrivalDelay = TDuration::MilliSeconds(dqSettings
                .WatermarksLateArrivalDelayMs
                .Get()
                .GetOrElse(TDqSettings::TDefault::WatermarksLateArrivalDelayMs));
            Add(props, WatermarksLateArrivalDelayUsSetting, ToString(lateArrivalDelay.MicroSeconds()), pos, ctx);
        }

        if (dqSettings.WatermarksEnableIdlePartitions.Get().GetOrElse(false)) {
            Add(props, WatermarksIdlePartitionsSetting, ToString(true), pos, ctx);
        }

        return Build<TCoNameValueTupleList>(ctx, pos)
            .Add(props)
            .Done();
    }

private:
    TPqState* State_; // State owns dq integration, so back reference must be not smart.
};

}

THolder<IDqIntegration> CreatePqDqIntegration(const TPqState::TPtr& state) {
    return MakeHolder<TPqDqIntegration>(state);
}

}
