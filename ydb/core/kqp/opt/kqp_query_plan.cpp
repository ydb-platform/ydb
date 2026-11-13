#include "kqp_query_plan.h"

#include <ydb/core/base/fulltext.h>
#include <ydb/core/base/fulltext_query.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/common/kqp_expr_helpers.h>
#include <ydb/library/json_index/json_index.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/rbo/kqp_olap_expr_inspection.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/library/formats/arrow/protos/ssa.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/utils/plan/plan_utils.h>
#include <ydb/public/lib/value/value.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/utf8.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/queue.h>
#include <util/string/hex.h>
#include <util/string/strip.h>
#include <util/string/vector.h>

#include <unordered_map>
#include <regex>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;
using namespace NClient;

namespace {

TString GetNameByReadType(EPlanTableReadType readType) {
    switch (readType) {
        case EPlanTableReadType::Unspecified:
            return "Unspecified";
        case EPlanTableReadType::FullScan:
            return "TableFullScan";
        case EPlanTableReadType::Scan:
            return "TableRangeScan";
        case EPlanTableReadType::Lookup:
            return "TablePointLookup";
        case EPlanTableReadType::MultiLookup:
            return "TableMultiLookup";
    }
}

/* remove chars, that break json plan */
std::string RemoveForbiddenChars(std::string s) {
    return NYql::IsUtf8(s)? s: "Non-UTF8 string";
}

struct TTableRead: public NYql::TSortingOperator<NYql::ERequestSorting::ASC> {
    EPlanTableReadType Type = EPlanTableReadType::Unspecified;
    TVector<TString> LookupBy;
    TVector<TString> ScanBy;
    TVector<TString> Columns;
    TMaybe<TString> Limit;
};

struct TTableWrite {
    EPlanTableWriteType Type = EPlanTableWriteType::Unspecified;
    TVector<TString> Keys;
    TVector<TString> Columns;
};

struct TTableInfo {
    TVector<TTableRead> Reads;
    TVector<TTableWrite> Writes;
};

struct TExprScope {
    NYql::NNodes::TCallable Callable;
    NYql::NNodes::TCoLambda Lambda;
    ui32 Depth;

    TExprScope(NYql::NNodes::TCallable callable, NYql::NNodes::TCoLambda Lambda, ui32 depth)
        : Callable(callable)
        , Lambda(Lambda)
        , Depth(depth) {}
};

struct TSerializerCtx {
    TSerializerCtx(TExprContext& exprCtx, const TString& database,
        const TString& cluster,
        const TIntrusivePtr<NYql::TKikimrTablesData> tablesData,
        const TKikimrConfiguration::TPtr config, ui32 txCount,
        TVector<TVector<NKikimrMiniKQL::TResult>> pureTxResults,
        TTypeAnnotationContext& typeCtx,
        TIntrusivePtr<NOpt::TKqpOptimizeContext> optCtx)
        : ExprCtx(exprCtx)
        , Database(database)
        , Cluster(cluster)
        , TablesData(tablesData)
        , Config(config)
        , TxCount(txCount)
        , PureTxResults(std::move(pureTxResults))
        , TypeCtx(typeCtx)
        , OptimizeCtx(optCtx)
    {}

    TMap<TString, TTableInfo> Tables;
    TMap<TString, TExprNode::TPtr> BindNameToStage;
    TMap<TString, ui32> StageGuidToId;
    THashMap<ui32, TVector<std::pair<ui32, ui32>>> ParamBindings;
    THashSet<ui32> PrecomputePhases;
    TMap<TString, const TExprNode*> OptimizedStages;
    ui32 PlanNodeId = 0;

    TExprContext& ExprCtx;
    const TString Database;
    const TString& Cluster;
    const TIntrusivePtr<NYql::TKikimrTablesData> TablesData;
    const TKikimrConfiguration::TPtr Config;
    const ui32 TxCount;
    TVector<TVector<NKikimrMiniKQL::TResult>> PureTxResults;
    TTypeAnnotationContext& TypeCtx;
    TIntrusivePtr<NOpt::TKqpOptimizeContext> OptimizeCtx;
};

TString GetExprStr(const TExprBase& scalar, bool quoteStr = true) {
    TMaybe<TString> literal = Nothing();
    if (auto maybeData = scalar.Maybe<TCoDataCtor>()) {
        literal = TString(maybeData.Cast().Literal());
    } else if (auto maybeData = scalar.Maybe<TCoPgConst>()) {
        literal = TString(maybeData.Cast().Value());
    }

    if (literal) {
        if (literal->size() > 100) {
            Utf8TruncateInplace(*literal, 96);
            *literal += " ...";
        }
        if (quoteStr) {
            return TStringBuilder() << '"' << *literal << '"';
        } else {
            return *literal;
        }
    }

    if (auto maybeParam = scalar.Maybe<TCoParameter>()) {
        return TString(maybeParam.Cast().Name());
    }

    return "expr";
}

const NKqpProto::TKqpPhyStage* FindStageProtoByGuid(const NKqpProto::TKqpPhyTx& txProto, const std::string& stageGuid) {
    for (auto& stage : txProto.GetStages()) {
        if (stage.GetStageGuid() == stageGuid) {
            return &stage;
        }
    }
    return nullptr;
}

const NKqpProto::TKqpPhyTableOperation* GetTableOpByTable(const TString& tablePath, const NKqpProto::TKqpPhyStage* stageProto) {
    for (auto& op : stageProto->GetTableOps()) {
        if (op.GetTable().GetPath() == tablePath) {
            return &op;
        }
    }
    return nullptr;
}

NJson::TJsonValue GetSsaProgramInJsonByTable(const TString& tablePath, const NKqpProto::TKqpPhyStage* stageProto) {
    const auto op = GetTableOpByTable(tablePath, stageProto);
    YQL_ENSURE(op, "Could not find table `" << tablePath << "` in stage with id " << stageProto->GetStageGuid());
    if (op->GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
        NKikimrSSA::TProgram program;
        if (!program.ParseFromString(op->GetReadOlapRange().GetOlapProgram())) {
            return NJson::TJsonValue();
        }
        NJson::TJsonValue jsonData;
        NProtobufJson::Proto2Json(program, jsonData);
        jsonData.EraseValue("Kernels");
        return jsonData;
    }
    return NJson::TJsonValue();
}

// Uniform view over stats that may come from either TKqpStatsStore or TTypeAnnotationContext
struct TStatsView {
    double Nrows = 0;
    double Cost = 0;
    double ByteSize = 0;
    bool Valid = false;

    TStatsView() = default;

    template <typename T>
    explicit TStatsView(const std::shared_ptr<T>& stats) {
        if (stats) {
            Nrows = stats->Nrows;
            Cost = stats->Cost;
            ByteSize = stats->ByteSize;
            Valid = true;
        }
    }

    explicit operator bool() const { return Valid; }
};

class TxPlanSerializer {
public:
    TxPlanSerializer(TSerializerCtx& serializerCtx, ui32 txId, const TKqpPhysicalTx& tx, const NKqpProto::TKqpPhyTx& txProto)
        : SerializerCtx(serializerCtx)
        , TxId(txId)
        , Tx(tx)
        , TxProto(txProto)
    {}

    void Serialize() {
        auto& phaseNode = QueryPlanNodes[++SerializerCtx.PlanNodeId];
        phaseNode.TypeName = "Phase";

        for (ui32 resId = 0; resId < Tx.Results().Size(); ++resId) {
            auto res = Tx.Results().Item(resId);
            auto& planNode = AddPlanNode(phaseNode);

            TStringBuilder typeName;
            if (SerializerCtx.PrecomputePhases.find(TxId) != SerializerCtx.PrecomputePhases.end()) {
                typeName << "Precompute";
                planNode.CteName = TStringBuilder() << "precompute_" << TxId << "_" << resId;
                planNode.Type = EPlanNodeType::Materialize;
            } else {
                typeName << "ResultSet";
                planNode.Type = EPlanNodeType::ResultSet;
            }

            if (SerializerCtx.TxCount > 1) {
                typeName << "_" << TxId;
            }

            if (Tx.Results().Size() > 1) {
                typeName << "_" << resId;
            }

            planNode.TypeName = typeName;

            auto visitResultStage = [&](const TDqStageBase& resultStage) {
                // Write with RETURNING produces the result straight from the sink stage.
                // Keep the ResultSet node clean and attach the sink stage below it so the plan
                // looks like a regular write with the ResultSet on top.
                if (resultStage.Outputs()) {
                    auto& sinkPlanNode = AddPlanNode(planNode);
                    Visit(resultStage, sinkPlanNode);
                } else {
                    Visit(resultStage, planNode);
                }
            };

            if (res.Maybe<TDqCnResult>()) {
                visitResultStage(res.Cast<TDqCnResult>().Output().Stage());
            } else if (res.Maybe<TDqCnValue>()) {
                visitResultStage(res.Cast<TDqCnValue>().Output().Stage());
            } else {
                Y_ENSURE(false, res.Ref().Content());
            }
        }

        for (const auto& stage: Tx.Stages()) {
            TDqStageBase stageBase = stage.Cast<TDqStageBase>();
            if (stageBase.Outputs()) { // Sink
                if (VisitedStages.contains(stage.Raw())) {
                    // Already rendered as a result output stage (e.g. write with RETURNING).
                    continue;
                }

                auto& planNode = AddPlanNode(phaseNode);
                Visit(TExprBase(stage), planNode);
            }
        }

        /* set NodeId using reverse order */
        ui64 firstPlanNodeId = SerializerCtx.PlanNodeId - QueryPlanNodes.size() + 1;
        for (auto& [id, planNode] : QueryPlanNodes) {
            planNode.NodeId = firstPlanNodeId + SerializerCtx.PlanNodeId - id;
        }
    }

    void WriteToJson(NJsonWriter::TBuf& writer) const {
        if (!QueryPlanNodes.empty()) {
            auto phasePlanNode = QueryPlanNodes.begin();
            WritePlanNodeToJson(phasePlanNode->second, writer);
        }
    }

private:
    struct TArgContext {
        TVector<TExprNode*> stack;

        TArgContext(){}

        TArgContext AddArg(TExprNode* a) {
            TArgContext res;
            res.stack = stack;
            res.stack.push_back(a);
            return res;
        }

        bool operator==(const TArgContext& other) const
        {
            return stack == other.stack;
        }

        struct HashFunction
        {
            size_t operator()(const TArgContext& e) const
            {
                size_t res = 0;
                for (auto el : e.stack) {
                    res += std::hash<TExprNode*>{}(el);
                }
                return res;
            }
        };
    };

    struct TOperator {
        TMap<TString, NJson::TJsonValue> Properties;
        TVector<std::variant<ui32, TArgContext>> Inputs;
    };

    enum class EPlanNodeType {
        Stage,
        Connection,
        Materialize,
        ResultSet
    };

    struct TQueryPlanNode {
        ui32 NodeId;
        TString Guid;
        TString TypeName;
        EPlanNodeType Type = EPlanNodeType::Stage;
        TMaybe<TString> CteName;
        TMaybe<TString> CteRefName;
        TMap<TString, NJson::TJsonValue> NodeInfo;
        TVector<TOperator> Operators;
        TSet<ui32> Plans;
        const NKqpProto::TKqpPhyStage* StageProto;
        TMap<TString, TString> OptEstimates;
    };

    void WritePlanNodeToJson(const TQueryPlanNode& planNode, NJsonWriter::TBuf& writer) const {
        writer.BeginObject();

        writer.WriteKey("PlanNodeId").WriteInt(planNode.NodeId);
        writer.WriteKey("Node Type").WriteString(planNode.TypeName);
        writer.WriteKey("StageGuid").WriteString(planNode.Guid);

        for (auto [k, v] : planNode.OptEstimates) {
            writer.WriteKey(k).WriteString(v);
        }

        if (auto type = GetPlanNodeType(planNode)) {
            writer.WriteKey("PlanNodeType").WriteString(type);
        }

        if (planNode.CteName) {
            writer.WriteKey("Parent Relationship").WriteString("InitPlan");
            writer.WriteKey("Subplan Name").WriteString("CTE " + *planNode.CteName);
        }

        if (planNode.CteRefName) {
            writer.WriteKey("CTE Name").WriteString(*planNode.CteRefName);
        }

        for (const auto& [key, value] : planNode.NodeInfo) {
            writer.WriteKey(key);
            writer.WriteJsonValue(&value, true, PREC_NDIGITS, 17);
        }

        if (!planNode.Operators.empty()) {
            writer.WriteKey("Operators");
            writer.BeginList();

            for (auto& op : planNode.Operators) {
                writer.BeginObject();
                for (const auto& [key, value] : op.Properties) {
                    writer.WriteKey(key);
                    writer.WriteJsonValue(&value, true, PREC_NDIGITS, 17);
                }

                writer.WriteKey("Inputs");
                writer.BeginList();

                for (const auto& input : op.Inputs) {

                    if (std::holds_alternative<ui32>(input)) {
                        writer.BeginObject();
                        writer.WriteKey("InternalOperatorId");
                        writer.WriteInt(std::get<ui32>(input));
                        writer.EndObject();
                    }
                    else {
                        TArgContext c = std::get<TArgContext>(input);
                        writer.BeginObject();

                        auto input = LambdaInputs.find(c);
                        if (input != LambdaInputs.end()){
                            if (std::holds_alternative<ui32>(input->second)) {
                                writer.WriteKey("InternalOperatorId");
                                writer.WriteInt(std::get<ui32>(input->second));
                            } else {
                                writer.WriteKey("ExternalPlanNodeId");
                                writer.WriteInt(std::get<TQueryPlanNode*>(input->second)->NodeId);
                            }
                        } else {
                            writer.WriteKey("Other");
                            writer.WriteString("ConstantExpression");
                        }
                        writer.EndObject();
                    }
                }

                writer.EndList();

                writer.EndObject();
            }

            writer.EndList();
        }

        if (!planNode.Plans.empty()) {
            writer.WriteKey("Plans");
            writer.BeginList();

            for (auto planId : planNode.Plans) {
                Y_ENSURE(QueryPlanNodes.contains(planId));
                WritePlanNodeToJson(QueryPlanNodes.at(planId), writer);
            }

            writer.EndList();
        }

        writer.EndObject();
    }

    TString GetPlanNodeType(const TQueryPlanNode& planNode) const {
        switch (planNode.Type) {
            case EPlanNodeType::Connection:
                return "Connection";
            case EPlanNodeType::Materialize:
                return "Materialize";
            case EPlanNodeType::ResultSet:
                return "ResultSet";
            default:
                return {};
        }
    }

    TMaybe<std::pair<ui32, ui32>> ContainResultBinding(const TString& str) {
        static const std::regex regex("tx_result_binding_(\\d+)_(\\d+)");
        std::smatch match;
        if (std::regex_search(str.c_str(), match, regex)) {
            return TMaybe<std::pair<ui32, ui32>>(
                {FromString<ui32>(match[1].str()), FromString<ui32>(match[2].str())});
        }

        return {};
    }

    TMaybe<TValue> GetResult(ui32 txId, ui32 resId) const {
        if (txId < SerializerCtx.PureTxResults.size() && resId < SerializerCtx.PureTxResults[txId].size()) {
            auto result = TValue::Create(SerializerCtx.PureTxResults[txId][resId]);
            Y_ENSURE(result.HaveValue());
            return TMaybe<TValue>(result);
        }

        return {};
    }

    ui32 AddOperator(TQueryPlanNode& planNode, const TString& name, TOperator op) {
        if (!planNode.TypeName.empty()) {
            planNode.TypeName += "-" + name;
        } else {
            planNode.TypeName = name;
        }

        planNode.Operators.push_back(std::move(op));
        return planNode.Operators.size() - 1;
    }

    TQueryPlanNode& GetParent(ui32 nodeId) {
        auto it = std::find_if(QueryPlanNodes.begin(), QueryPlanNodes.end(), [nodeId](const auto& node) {
            return node.second.Plans.contains(nodeId);
        });

        Y_ENSURE(it != QueryPlanNodes.end());
        return it->second;
    }

    TQueryPlanNode& AddPlanNode(TQueryPlanNode& planNode) {
        planNode.Plans.insert(++SerializerCtx.PlanNodeId);
        return QueryPlanNodes[SerializerCtx.PlanNodeId];
    }

    void FillConnectionPlanNode(const TDqConnection& connection, TQueryPlanNode& planNode) {
        TDqStageSettings settings = TDqStageSettings::Parse(connection.Output().Stage());
        auto GetNarrowColumnName = [&](const TString& wideColumnName) {
            ui32 idx;
            if (!TryFromString(wideColumnName, idx)) {
                return wideColumnName;
            }

            YQL_ENSURE(idx < settings.OutputNarrowType->GetSize(),
                "Failed to lookup column name for index " << idx << " in type " << settings.OutputNarrowType->ToString());

            return TString(settings.OutputNarrowType->GetItems()[idx]->GetName());
        };

        planNode.Type = EPlanNodeType::Connection;

        auto isBlockConnection = IsWideSequenceBlockType(*connection.Output().Stage().Program().Body().Ref().GetTypeAnn());
        if (!isBlockConnection) {
            if (auto it = SerializerCtx.OptimizedStages.find(settings.Id); it != SerializerCtx.OptimizedStages.end()) {
                isBlockConnection = IsWideSequenceBlockType(*TMaybeNode<TDqPhyStage>(it->second).Cast().Program().Body().Ref().GetTypeAnn());
            }
        }
        if (isBlockConnection) {
            planNode.NodeInfo["Blocks"] = "True";
        }

        if (connection.Maybe<TDqCnUnionAll>()) {
            planNode.TypeName = "UnionAll";
        } else if (connection.Maybe<TDqCnParallelUnionAll>()) {
            planNode.TypeName = "UnionAll";
            planNode.NodeInfo["Parallel"] = "True";
        } else if (connection.Maybe<TDqCnBroadcast>()) {
            planNode.TypeName = "Broadcast";
        } else if (connection.Maybe<TDqCnMap>()) {
            planNode.TypeName = "Map";
        } else if (auto hashShuffle = connection.Maybe<TDqCnHashShuffle>()) {
            planNode.TypeName = "HashShuffle";
            auto& keyColumns = planNode.NodeInfo["KeyColumns"];
            for (const auto& column : hashShuffle.Cast().KeyColumns()) {
                if (settings.WideChannels) {
                    keyColumns.AppendValue(GetNarrowColumnName(TString(column.Value())));
                } else {
                    keyColumns.AppendValue(TString(column.Value()));
                }
            }

            auto& hashFunc = planNode.NodeInfo["HashFunc"];
            if (hashShuffle.HashFunc().IsValid()) {
                hashFunc = hashShuffle.HashFunc().Cast().StringValue();
            } else {
                hashFunc = ToString(SerializerCtx.Config->GetDqDefaultHashShuffleFuncType());
            }
        } else if (auto merge = connection.Maybe<TDqCnMerge>()) {
            planNode.TypeName = "Merge";
            auto& sortColumns = planNode.NodeInfo["SortColumns"];
            for (const auto& sortColumn : merge.Cast().SortColumns()) {
                TStringBuilder sortColumnDesc;
                if (settings.WideChannels) {
                    sortColumnDesc << GetNarrowColumnName(TString(sortColumn.Column().Value()));
                } else {
                    sortColumnDesc << sortColumn.Column().Value();
                }
                sortColumnDesc << " (" << sortColumn.SortDirection().Value() << ")";

                sortColumns.AppendValue(sortColumnDesc);
            }
        } else if (auto maybeTableLookup = connection.Maybe<TKqpCnStreamLookup>()) {
            auto tableLookup = maybeTableLookup.Cast();

            TTableRead readInfo;
            readInfo.Type = EPlanTableReadType::Lookup;
            TString tablePath(tableLookup.Table().Path().Value());
            auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, tablePath);
            planNode.NodeInfo["Table"] = tableData.RelativePath ? *tableData.RelativePath : tablePath;
            planNode.NodeInfo["Path"] = tablePath;

            readInfo.Columns.reserve(tableLookup.Columns().Size());
            auto& columns = planNode.NodeInfo["Columns"];
            for (const auto& column : tableLookup.Columns()) {
                columns.AppendValue(column.Value());
                readInfo.Columns.push_back(TString(column.Value()));
            }

            const auto inputType = tableLookup.InputType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            YQL_ENSURE(inputType);
            YQL_ENSURE(inputType->GetKind() == ETypeAnnotationKind::List);
            const auto inputItemType = inputType->Cast<TListExprType>()->GetItemType();

            const TStructExprType* lookupKeyColumnsStruct = nullptr;
            if (inputItemType->GetKind() == ETypeAnnotationKind::Struct) {
                planNode.TypeName = "TableLookup";
                lookupKeyColumnsStruct = inputItemType->Cast<TStructExprType>();
            } else if (inputItemType->GetKind() == ETypeAnnotationKind::Tuple) {
                planNode.TypeName = "TableLookupJoin";
                const auto inputTupleType = inputItemType->Cast<TTupleExprType>();
                YQL_ENSURE(inputTupleType->GetItems()[1]->GetKind() == ETypeAnnotationKind::Optional);
                const auto joinKeyType = inputTupleType->GetItems()[1]->Cast<TOptionalExprType>()->GetItemType();
                YQL_ENSURE(joinKeyType->GetKind() == ETypeAnnotationKind::Struct);
                lookupKeyColumnsStruct = joinKeyType->Cast<TStructExprType>();
            }

            YQL_ENSURE(lookupKeyColumnsStruct);
            readInfo.LookupBy.reserve(lookupKeyColumnsStruct->GetItems().size());
            auto& lookupKeyColumns = planNode.NodeInfo["LookupKeyColumns"];
            for (const auto keyColumn : lookupKeyColumnsStruct->GetItems()) {
                lookupKeyColumns.AppendValue(keyColumn->GetName());
                readInfo.LookupBy.push_back(TString(keyColumn->GetName()));
            }

            if (SerializerCtx.Config->CostBasedOptimizationLevel.Get().GetOrElse(SerializerCtx.Config->GetDefaultCostBasedOptimizationLevel())!=0) {

                TStatsView stats = SerializerCtx.OptimizeCtx
                    ? TStatsView(SerializerCtx.OptimizeCtx->KqpStats.GetStats(tableLookup.Raw()))
                    : TStatsView(SerializerCtx.TypeCtx.GetStats(tableLookup.Raw()));
                if (stats) {
                    planNode.OptEstimates["E-Rows"] = TStringBuilder() << stats.Nrows;
                    planNode.OptEstimates["E-Cost"] = TStringBuilder() << stats.Cost;
                    planNode.OptEstimates["E-Size"] = TStringBuilder() << stats.ByteSize;
                }
                else {
                    planNode.OptEstimates["E-Rows"] = "No estimate";
                    planNode.OptEstimates["E-Cost"] = "No estimate";
                    planNode.OptEstimates["E-Size"] = "No estimate";
                }
            }

            SerializerCtx.Tables[tablePath].Reads.push_back(std::move(readInfo));
        } else if (auto maybeVectorSearch = connection.Maybe<TKqpCnVectorSearch>()) {
            auto vectorSearch = maybeVectorSearch.Cast();

            planNode.TypeName = "VectorSearch";
            const TString indexName(vectorSearch.Index().Value());
            planNode.NodeInfo["Index"] = indexName;

            // The actor always reads the kmeans-tree level and posting index tables; whether it also
            // reads the main table depends on the index being covering for the requested columns. When
            // it is covering, every output column is served from the posting table and the main table
            // is not touched — reflect that so covered-index plans show no main-table access.
            // The posting table holds the main table's key columns plus the index data columns; the
            // index key columns (the embedding, and the prefix of a prefixed index) are not in it, so
            // ask its metadata rather than deriving the set from the index description.
            TString tablePath(vectorSearch.Table().Path().Value());
            auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, tablePath);

            TKikimrTableMetadataPtr postingMeta;
            {
                const TString postingPath = TStringBuilder()
                    << tablePath << "/" << indexName << "/" << NTableIndex::NKMeans::PostingTable;
                const auto& tables = SerializerCtx.TablesData->GetTables();
                if (auto* desc = tables.FindPtr(std::make_pair(SerializerCtx.Cluster, postingPath))) {
                    postingMeta = desc->Metadata;
                }
            }

            // Without the posting metadata, fall back to reporting the main table read.
            bool covered = bool(postingMeta);
            auto& columns = planNode.NodeInfo["Columns"];
            TVector<TString> readColumns;
            readColumns.reserve(vectorSearch.Columns().Size());
            for (const auto& column : vectorSearch.Columns()) {
                columns.AppendValue(column.Value());
                readColumns.push_back(TString(column.Value()));
                if (postingMeta && !postingMeta->Columns.contains(readColumns.back())) {
                    covered = false;
                }
            }

            if (!covered) {
                planNode.NodeInfo["Table"] = tableData.RelativePath ? *tableData.RelativePath : tablePath;
                planNode.NodeInfo["Path"] = tablePath;

                TTableRead readInfo;
                readInfo.Type = EPlanTableReadType::Lookup;
                readInfo.Columns = readColumns;
                SerializerCtx.Tables[tablePath].Reads.push_back(std::move(readInfo));
            }

            // TopK (LIMIT) is either a literal or a query parameter resolved at runtime.
            TExprBase topK = vectorSearch.TopK();
            if (auto literal = topK.Maybe<TCoUint64>()) {
                planNode.NodeInfo["TopK"] = TString(literal.Cast().Literal().Value());
            } else if (auto param = topK.Maybe<TCoParameter>()) {
                planNode.NodeInfo["TopK"] = TString(param.Cast().Name().Value());
            }
        } else {
            planNode.TypeName = connection.Ref().Content();
        }
    }

    TString DescribeValue(const NKikimr::NClient::TValue& value) {
        if (value.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::Data) {
            auto str = value.GetDataText();
            switch (value.GetType().GetData().GetScheme()) {
            case NScheme::NTypeIds::Utf8:
            case NScheme::NTypeIds::Json:
            case NScheme::NTypeIds::String:
            case NScheme::NTypeIds::String4k:
            case NScheme::NTypeIds::String2m:
                return "«" + str + "»";
            default:
                return str;
            }
        }
        if (value.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::Pg) {
            return value.GetPgText();
        }
        Y_ENSURE(false, TStringBuilder() << "unexpected NKikimrMiniKQL::ETypeKind: " << ETypeKind_Name(value.GetType().GetKind()));
    }

    struct TReadRangesParams {
        TString TablePath;
        TKqpReadTableExplainPrompt ExplainPrompt;
        TString RangesDesc;

        bool IncludePointPrefixLen = false;
        THashMap<TString, TString> IndexSelectionInfo;
    };

    template<typename TColumnsIterator>
    void ProcessReadRangesCommon(const TReadRangesParams& params, TTableRead& readInfo, TOperator& op, TQueryPlanNode& planNode, TColumnsIterator columnsIter) {
        auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, params.TablePath);
        op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : params.TablePath;
        op.Properties["Path"] = params.TablePath;
        planNode.NodeInfo["Tables"].AppendValue(op.Properties["Table"]);

        // Collect columns with ranges info
        auto& columns = op.Properties["ReadColumns"];
        THashSet<TString> addedColumns;

        if (params.RangesDesc == "Void" || params.ExplainPrompt.UsedKeyColumns.empty()) {
            readInfo.Type = EPlanTableReadType::FullScan;

            for (const auto& col : tableData.Metadata->KeyColumnNames) {
                TStringBuilder rangeDesc;
                rangeDesc << col << " (-∞, +∞)";
                readInfo.ScanBy.push_back(rangeDesc);
                columns.AppendValue(rangeDesc);
                addedColumns.insert(col);
            }
        } else if (auto maybeResultBinding = ContainResultBinding(params.RangesDesc)) {
            readInfo.Type = EPlanTableReadType::Scan;

            auto [txId, resId] = *maybeResultBinding;
            if (auto result = GetResult(txId, resId)) {
                auto ranges = (*result)[0];
                const auto& keyColumns = tableData.Metadata->KeyColumnNames;
                for (size_t rangeId = 0; rangeId < ranges.Size(); ++rangeId) {
                    Y_ENSURE(ranges[rangeId].HaveValue() && ranges[rangeId].Size() == 2);
                    auto from = ranges[rangeId][0];
                    auto to = ranges[rangeId][1];

                    for (size_t colId = 0; colId < keyColumns.size(); ++colId) {
                        if (!from[colId].HaveValue() && !to[colId].HaveValue()) {
                            continue;
                        }

                        TStringBuilder rangeDesc;
                        rangeDesc << keyColumns[colId] << " "
                            << (from[keyColumns.size()].GetDataText() == "1" ? "[" : "(")
                            << (from[colId].HaveValue() ? RemoveForbiddenChars(from[colId].GetSimpleValueText()) : "-∞") << ", "
                            << (to[colId].HaveValue() ? RemoveForbiddenChars(to[colId].GetSimpleValueText()) : "+∞")
                            << (to[keyColumns.size()].GetDataText() == "1" ? "]" : ")");

                        readInfo.ScanBy.push_back(rangeDesc);
                        columns.AppendValue(rangeDesc);
                        addedColumns.insert(keyColumns[colId]);
                    }
                }
            } else {
                columns.AppendValue(params.RangesDesc);
            }
        } else {
            Y_ENSURE(false, params.RangesDesc);
        }

        if (!params.ExplainPrompt.UsedKeyColumns.empty()) {
            auto& usedColumns = op.Properties["ReadRangesKeys"];
            for (const auto& col : params.ExplainPrompt.UsedKeyColumns) {
                usedColumns.AppendValue(col);
            }
        }

        if (params.ExplainPrompt.ExpectedMaxRanges) {
            op.Properties["ReadRangesExpectedSize"] = ToString(*params.ExplainPrompt.ExpectedMaxRanges);
        }

        if (params.IncludePointPrefixLen) {
            op.Properties["ReadRangesPointPrefixLen"] = ToString(params.ExplainPrompt.PointPrefixLen);
        }

        if (!params.ExplainPrompt.IndexSelectionInfo.empty()) {
            TStringBuilder fullInfo;
            bool isFirst = true;
            for(const auto& [name, info] : params.IndexSelectionInfo) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    fullInfo << ", ";
                }

                fullInfo << name << ": " << info;
            }

            op.Properties["IndexSelectionInfo"] = TString(fullInfo);
        }

        // Add remaining columns from columnsIter (avoiding duplicates)
        for (const auto& col : columnsIter) {
            TString colName = TString(col.Value());
            if (!addedColumns.contains(colName)) {
                columns.AppendValue(colName);
                addedColumns.insert(colName);
            }
            readInfo.Columns.emplace_back(colName);
        }
    }

    void Visit(const TKqpReadTableFullTextIndexSourceSettings& sourceSettings, TQueryPlanNode& planNode) {
        const auto& tablePath = sourceSettings.Table().Path();
        const auto& index = sourceSettings.Index();
        const auto& columns = sourceSettings.Columns();
        const auto& settings = TKqpReadTableFullTextIndexSettings::Parse(sourceSettings.Settings());

        auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, TString(tablePath));
        auto [implTable, indexDesc] = tableData.Metadata->GetIndex(index);

        TOperator op;

        TVector<TString> readColumns;
        TTableRead readInfo;
        for(const auto& column: columns) {
            readColumns.push_back(TString(column.Value()));
            if (readColumns.back() == NTableIndex::NFulltext::FullTextRelevanceColumn) {
                op.Properties["RelevanceQuery"] = "True";
            } else {
                readInfo.Columns.push_back(readColumns.back());
            }
        }

        if (sourceSettings.MainTableCovered().Value() != "true") {
            readInfo.Type = EPlanTableReadType::Lookup;
            readInfo.LookupBy = tableData.Metadata->KeyColumnNames;
            SerializerCtx.Tables[TString(tablePath)].Reads.push_back(std::move(readInfo));
        }

        op.Properties["Table"] = TString(tablePath);
        op.Properties["Index"] = TString(index);
        op.Properties["Columns"] = JoinSeq(", ", readColumns);
        op.Properties["Name"] = "ReadFullTextIndex";

        std::vector<TString> subparts;
        TVector<TString> searchTerms;
        for(const auto& query: sourceSettings.Query().Cast<TExprList>()) {
            auto pattern = query;
            std::optional<char> escape;
            TString mode;
            if (const auto patternQuery = NOpt::TryParseFulltextPatternQuery(query.Ptr())) {
                pattern = TExprBase(patternQuery->Pattern);
                escape = patternQuery->Escape;
                mode = patternQuery->Mode;
            }

            TString expr = GetExprStr(pattern, true);
            if (expr != "expr") {
                if (escape) {
                    expr += TStringBuilder() << " ESCAPE \"" << *escape << '"';
                }
                if (!mode.empty()) {
                    expr = TStringBuilder() << mode << '(' << expr << ')';
                }
                subparts.emplace_back(std::move(expr));
            }

            if (pattern.Maybe<TCoDataCtor>()) {
                auto literal = TString(pattern.Cast<TCoDataCtor>().Literal());
                YQL_ENSURE(indexDesc);
                YQL_ENSURE(indexDesc->Type == TIndexDescription::EType::GlobalFulltextPlain
                    || indexDesc->Type == TIndexDescription::EType::GlobalFulltextRelevance
                    || indexDesc->Type == TIndexDescription::EType::GlobalFulltextCompact
                    || indexDesc->Type == TIndexDescription::EType::GlobalFulltextCompactRelevance);

                auto& desc = std::get<NKikimrSchemeOp::TFulltextIndexDescription>(indexDesc->SpecializedIndexDescription);
                for(const auto& column: readColumns) {
                    for(const auto& analyzer: desc.settings().columns()) {
                        if (analyzer.column() == column) {
                            const auto terms = mode.empty()
                                ? NFulltext::BuildSearchTerms(literal, analyzer.analyzers())
                                : NFulltext::BuildStringMatchTerms(literal, mode, escape, analyzer.analyzers());
                            for(const auto& term: terms) {
                                searchTerms.push_back(term);
                            }
                        }
                    }
                }
            }
        }

        if (!subparts.empty()) {
            op.Properties["Query"] = JoinSeq(" ", subparts);
        }

        if (!searchTerms.empty())
            op.Properties["SearchTerms"] = JoinSeq(", ", searchTerms);

        TVector<TString> queryColumns;
        for(const auto& column: sourceSettings.QueryColumns()) {
            queryColumns.push_back(TString(column.Value()));
        }

        if (settings.ItemsLimit) {
            op.Properties["ItemsLimit"] = GetExprStr(TExprBase(settings.ItemsLimit), true);
        }

        if (settings.SkipLimit) {
            op.Properties["SkipLimit"] = GetExprStr(TExprBase(settings.SkipLimit), true);
        }

        if (settings.BFactor) {
            op.Properties["BFactor"] = GetExprStr(TExprBase(settings.BFactor), true);
        }

        if (settings.K1Factor) {
            op.Properties["K1Factor"] = GetExprStr(TExprBase(settings.K1Factor), true);
        }

        std::vector<TString> tokens;
        if (settings.Tokens) {
            YQL_ENSURE(
                indexDesc->Type == TIndexDescription::EType::GlobalJson || indexDesc->Type == TIndexDescription::EType::GlobalJsonCompact);

            for (const auto& token : TExprBase(settings.Tokens).Cast<TExprList>()) {
                auto pair = token.Cast<TExprList>();
                YQL_ENSURE(pair.Size() == 2, "Expected 2 items in token pair, got: " << pair.Size());

                auto pathToken = TString(pair.Item(0).Cast<TCoString>().Literal().Value());
                auto paramName = TString(pair.Item(1).Cast<TCoString>().Literal().Value());

                tokens.push_back(NJsonIndex::FormatJsonIndexToken(pathToken, paramName));
            }

            auto& tokensJson = op.Properties["Tokens"];
            tokensJson.SetType(NJson::JSON_ARRAY);
            for (const auto& t : tokens) {
                tokensJson.AppendValue(t);
            }
        }

        NTableIndex::NFulltext::EDefaultOperator defaultOperator = NTableIndex::NFulltext::EDefaultOperator::Invalid;
        if (settings.DefaultOperator) {
            op.Properties["DefaultOperator"] = GetExprStr(TExprBase(settings.DefaultOperator), true);
            if (TExprBase(settings.DefaultOperator).Maybe<TCoDataCtor>()) {
                TString error;
                defaultOperator = NTableIndex::NFulltext::DefaultOperatorFromString(TString(TExprBase(settings.DefaultOperator).Cast<TCoDataCtor>().Literal()), error);
            }
        }

        if (settings.MinimumShouldMatch) {
            op.Properties["MinimumShouldMatch"] = GetExprStr(TExprBase(settings.MinimumShouldMatch), true);
            TString minimumShouldMatchError;
            if (searchTerms.size() > 0 && defaultOperator != NTableIndex::NFulltext::EDefaultOperator::Invalid && TExprBase(settings.MinimumShouldMatch).Maybe<TCoDataCtor>()) {
                ui32 minimumShouldMatch = NTableIndex::NFulltext::MinimumShouldMatchFromString(
                    searchTerms.size(), defaultOperator, TString(TExprBase(settings.MinimumShouldMatch).Cast<TCoDataCtor>().Literal()), minimumShouldMatchError);
                op.Properties["MinimumShouldMatchExplained"] = std::to_string(minimumShouldMatch);
            }
        }

        op.Properties["QueryColumns"] = JoinSeq(", ", queryColumns);

        AddOperator(planNode, "ReadFullTextIndex", std::move(op));
    }

    void Visit(const TKqpReadRangesSourceSettings& sourceSettings, TQueryPlanNode& planNode) {
        if (sourceSettings.RangesExpr().Maybe<TKqlKeyRange>()) {
            auto tablePath = TString(sourceSettings.Table().Path());
            auto range = sourceSettings.RangesExpr().Cast<TKqlKeyRange>();
            Visit(tablePath, range, sourceSettings, planNode);
            return;
        }

        const auto tablePath = TString(sourceSettings.Table().Path());
        const auto explainPrompt = TKqpReadTableExplainPrompt::Parse(sourceSettings.ExplainPrompt().Cast());
        const auto rangesDesc = NPlanUtils::PrettyExprStr(sourceSettings.RangesExpr());

        TTableRead readInfo;
        TOperator op;

        TReadRangesParams params {
            .TablePath = tablePath,
            .ExplainPrompt = explainPrompt,
            .RangesDesc = rangesDesc,
            .IncludePointPrefixLen = true,
            .IndexSelectionInfo = TKqpReadTableSettings::Parse(sourceSettings.Settings()).IndexSelectionInfo
        };

        ProcessReadRangesCommon(params, readInfo, op, planNode, sourceSettings.Columns());

        AddReadTableSettings(op, sourceSettings.Settings(), readInfo);
        AddOptimizerEstimates(op, sourceSettings);

        auto readName = GetNameByReadType(readInfo.Type);
        op.Properties["Name"] = readName;
        AddOperator(planNode, readName, std::move(op));

        SerializerCtx.Tables[tablePath].Reads.push_back(std::move(readInfo));
    }

    // Try get cluster from data surce or data sink node
    TMaybe<TString> TryGetCluster(const TExprBase& d) {
        if (d.Raw()->ChildrenSize() >= 2) {
            TExprBase child(d.Raw()->Child(1));
            auto cluster = child.Maybe<TCoAtom>();
            if (cluster) {
                return cluster.Cast().StringValue();
            }
        }
        return Nothing();
    }

    TString RemovePathPrefix(TString path) {
        const auto& prefix = SerializerCtx.Config->_KqpTablePathPrefix.Get();
        if (prefix && path.StartsWith(*prefix)) {
            size_t count = prefix->size();
            if (path.size() > count && path[count] == '/') {
                ++count;
            }
            path.erase(0, count);
        }
        return path;
    }

    TStatsView FindWrapStats(TExprNode::TPtr node, const TExprNode* dataSourceNode) {
        if (auto maybeWrapBase = TMaybeNode<TDqSourceWrapBase>(node)) {
            if (maybeWrapBase.Cast().DataSource().Raw() == dataSourceNode) {
                if (SerializerCtx.OptimizeCtx) {
                    return TStatsView(SerializerCtx.OptimizeCtx->KqpStats.GetStats(node.Get()));
                } else {
                    return TStatsView(SerializerCtx.TypeCtx.GetStats(node.Get()));
                }
            }
        }
        for (const auto& child : node->Children()) {
            if (child->IsLambda()) {
                // support wide lambda as well
                for (size_t bodyIndex = 1; bodyIndex < child->ChildrenSize(); ++bodyIndex) {
                    if (auto result = FindWrapStats(child->ChildPtr(bodyIndex), dataSourceNode)) {
                        return result;
                    }
                }
            } else {
                if (auto result = FindWrapStats(child, dataSourceNode)) {
                    return result;
                }
            }
        }
        return TStatsView{};
    }

    void Visit(const TDqSource& source, TQueryPlanNode& stagePlanNode, const TCoLambda& Lambda) {
        // YDB sources
        if (auto settings = source.Settings().Maybe<TKqpReadRangesSourceSettings>(); settings.IsValid()) {
            Visit(settings.Cast(), stagePlanNode);
            return;
        }

        if (auto settings = source.Settings().Maybe<TKqpReadTableFullTextIndexSourceSettings>(); settings.IsValid()) {
            Visit(settings.Cast(), stagePlanNode);
            return;
        }

        // Federated providers
        TOperator op;
        TCoDataSource dataSource = source.DataSource().Cast<TCoDataSource>();
        const TString dataSourceCategory = dataSource.Category().StringValue();
        IDqIntegration* dqIntegration = nullptr;

        {
            auto providerIt = SerializerCtx.TypeCtx.DataSourceMap.find(dataSourceCategory);
            if (providerIt != SerializerCtx.TypeCtx.DataSourceMap.end()) {
                dqIntegration = providerIt->second->GetDqIntegration();
            }
        }

        // Common settings that can be overwritten by provider
        op.Properties["SourceType"] = dataSourceCategory;
        if (auto cluster = TryGetCluster(dataSource)) {
            TString dataSource = RemovePathPrefix(std::move(*cluster));
            op.Properties["ExternalDataSource"] = dataSource;
            op.Properties["Name"] = TStringBuilder() << "Read " << dataSource;
        } else {
            op.Properties["Name"] = "Read from external data source";
        }

        // Actual stats must be binded with TDqSourceWrapBase
        TStatsView stats = FindWrapStats(Lambda.Body().Ptr(), dataSource.Raw());

        if (!stats) {
            // Fallback to TCoDataSource
            stats = SerializerCtx.OptimizeCtx
                ? TStatsView(SerializerCtx.OptimizeCtx->KqpStats.GetStats(dataSource.Raw()))
                : TStatsView(SerializerCtx.TypeCtx.GetStats(dataSource.Raw()));
        }

        if (stats) {
            op.Properties["E-Rows"] = TStringBuilder() << stats.Nrows;
            op.Properties["E-Cost"] = TStringBuilder() << stats.Cost;
            op.Properties["E-Size"] = TStringBuilder() << stats.ByteSize;
        }

        if (dqIntegration) {
            dqIntegration->FillSourcePlanProperties(source, op.Properties);
        }

        AddOperator(stagePlanNode, "Source", op);
    }

    template<typename TSinkLike>
    void VisitTableSink(const TSinkLike& sinkLike, const TDqStageBase& stage, TQueryPlanNode& planNode, TQueryPlanNode& stagePlanNode, TString planNodeSuffix = "Sink") {
        // Federated providers
        TOperator op;
        TCoDataSink dataSink = sinkLike.DataSink().template Cast<TCoDataSink>();
        const TString dataSinkCategory = dataSink.Category().StringValue();

        // Common settings that can be overwritten by provider
        op.Properties["SinkType"] = dataSinkCategory;
        if (dataSinkCategory == NYql::KqpTableSinkName) {
            auto settings = sinkLike.Settings().template Cast<TKqpTableSinkSettings>();

            NKikimrKqp::TKqpTableSinkSettings tableSinkSettings;
            {
                AFL_ENSURE(stagePlanNode.StageProto);
                bool found = false;
                for (const auto& protoSink : stagePlanNode.StageProto->GetSinks()) {
                    if (FromString<ui32>(TStringBuf(sinkLike.Index())) == protoSink.GetOutputIndex()
                            && protoSink.HasInternalSink()
                            && protoSink.GetInternalSink().GetSettings().UnpackTo(&tableSinkSettings)) {
                                found = true;
                                break;
                    }
                }
                for (const auto& protoTransform : stagePlanNode.StageProto->GetOutputTransforms()) {
                    if (FromString<ui32>(TStringBuf(sinkLike.Index())) == protoTransform.GetOutputIndex()
                            && protoTransform.HasInternalSink()
                            && protoTransform.GetInternalSink().GetSettings().UnpackTo(&tableSinkSettings)) {
                                found = true;
                                break;
                    }
                }
                AFL_ENSURE(found);
            }

            TString tablePath;
            TTableWrite writeInfo;
            if (tableSinkSettings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE) {
                op.Properties["Name"] = "Replace";
                writeInfo.Type = EPlanTableWriteType::MultiReplace;
            } else if (tableSinkSettings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT) {
                op.Properties["Name"] = "Upsert";
                writeInfo.Type = EPlanTableWriteType::MultiUpsert;
            } else if (tableSinkSettings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT) {
                op.Properties["Name"] = "Insert";
                writeInfo.Type = EPlanTableWriteType::MultiInsert;
            } else if (tableSinkSettings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE) {
                op.Properties["Name"] = "Delete";
                writeInfo.Type = EPlanTableWriteType::MultiErase;
            } else if (tableSinkSettings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE) {
                op.Properties["Name"] = "Update";
                writeInfo.Type = EPlanTableWriteType::MultiUpdate;
            } else if (tableSinkSettings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_FILL) {
                op.Properties["Name"] = "FillTable";
                writeInfo.Type = EPlanTableWriteType::MultiReplace;
            } else if (tableSinkSettings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT_INCREMENT) {
                op.Properties["Name"] = "UpsertIncrement";
                writeInfo.Type = EPlanTableWriteType::MultiUpsertIncrement;
            } else {
                YQL_ENSURE(false, "Unsupported sink mode");
            }

            if (settings.Mode().StringValue() != "fill_table") {
                tablePath = settings.Table().Path().StringValue();
                const auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, tablePath);
                op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : tablePath;
                op.Properties["Path"] = tablePath;
            } else {
                const auto originalPathNode = GetSetting(settings.Settings().Ref(), "OriginalPath");
                AFL_ENSURE(originalPathNode);
                tablePath = TCoNameValueTuple(originalPathNode).Value().Cast<TCoAtom>().StringValue();
                op.Properties["Path"] = tablePath;

                TString error;
                std::pair<TString, TString> pathPair;
                if (NKikimr::TrySplitPathByDb(tablePath, SerializerCtx.Database, pathPair, error)) {
                    op.Properties["Table"]= pathPair.second;
                } else {
                    op.Properties["Table"] = tablePath;
                }
            }

            if (writeInfo.Type != EPlanTableWriteType::MultiErase) {
                const auto& tupleType = stage.Ref().GetTypeAnn()->Cast<TTupleExprType>();
                YQL_ENSURE(tupleType);
                YQL_ENSURE(tupleType->GetSize() == 1);
                const auto& listType = tupleType->GetItems()[0]->Cast<TListExprType>();
                YQL_ENSURE(listType);
                const auto& structType = listType->GetItemType()->Cast<TStructExprType>();
                YQL_ENSURE(structType);
                for (const auto& item : structType->GetItems()) {
                    writeInfo.Columns.push_back(TString(item->GetName()));
                }
            }

            SerializerCtx.Tables[tablePath].Writes.push_back(writeInfo);

            if (tableSinkSettings.GetNeedLookup()) {
                TTableRead readInfo;
                readInfo.Type = EPlanTableReadType::Lookup;

                NJson::TJsonValue lookupColumns;
                lookupColumns.SetType(NJson::EJsonValueType::JSON_ARRAY);
                for (const auto& col : tableSinkSettings.GetKeyColumns()) {
                    readInfo.Columns.push_back(col.GetName());
                }
                for (const auto& col : tableSinkSettings.GetLookupColumns()) {
                    lookupColumns.AppendValue(col.GetName());
                    readInfo.Columns.push_back(col.GetName());
                }

                op.Properties["Lookup"] = lookupColumns;
                SerializerCtx.Tables[tablePath].Reads.push_back(readInfo);
            }

            if (!tableSinkSettings.GetIndexes().empty()) {
                NJson::TJsonValue updateIndexes;
                updateIndexes.SetType(NJson::EJsonValueType::JSON_ARRAY);
                for (const auto& index : tableSinkSettings.GetIndexes()) {
                    NJson::TJsonValue indexInfo;
                    indexInfo["Name"] = index.GetIndexName();
                    indexInfo["Path"] = index.GetTable().GetPath();

                    TTableWrite indexWriteInfo;
                    if (index.GetOperationType() == NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE) {
                        indexWriteInfo.Type = EPlanTableWriteType::MultiUpdate;
                    } else if (index.GetOperationType() == NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE) {
                        indexWriteInfo.Type = EPlanTableWriteType::MultiErase;
                    } else if (index.GetOperationType() == NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT) {
                        indexWriteInfo.Type = EPlanTableWriteType::MultiUpsert;
                    } else {
                        YQL_ENSURE(false, "Unexpected operation type for index");
                    }

                    NJson::TJsonValue columns;
                    columns.SetType(NJson::EJsonValueType::JSON_ARRAY);
                    for (const auto& col : index.GetColumns()) {
                        columns.AppendValue(col.GetName());
                        indexWriteInfo.Columns.push_back(col.GetName());
                    }
                    indexInfo["Columns"] = columns;
                    SerializerCtx.Tables[index.GetTable().GetPath()].Writes.push_back(indexWriteInfo);

                    if (index.GetIsUniq()) {
                        indexInfo["CheckUnique"] = true;

                        TTableRead readInfo;
                        readInfo.Type = EPlanTableReadType::Lookup;
                        for (const auto& col : index.GetKeyColumns()) {
                            readInfo.Columns.push_back(col.GetName());
                        }
                        SerializerCtx.Tables[index.GetTable().GetPath()].Reads.push_back(readInfo);
                    }

                    if (index.GetNeedDeleteOldRows()) {
                        TTableWrite indexDeleteInfo;
                        indexDeleteInfo.Type = EPlanTableWriteType::MultiErase;
                        for (const auto& col : index.GetKeyColumns()) {
                            indexDeleteInfo.Columns.push_back(col.GetName());
                        }
                        SerializerCtx.Tables[index.GetTable().GetPath()].Writes.push_back(indexDeleteInfo);
                    }

                    updateIndexes.AppendValue(indexInfo);
                }
                op.Properties["UpdateIndexes"] = updateIndexes;
            }

            planNode.NodeInfo["Tables"].AppendValue(op.Properties["Table"]);
        } else if (auto cluster = TryGetCluster(dataSink)) {
            TString dataSource = RemovePathPrefix(std::move(*cluster));
            op.Properties["ExternalDataSource"] = dataSource;
            op.Properties["Name"] = TStringBuilder() << "Write " << dataSource;
        } else {
            op.Properties["Name"] = "Write to external data source";
        }

        auto inputContext = CurrentArgContext.AddArg(sinkLike.Ptr().Get());
        LambdaInputs[inputContext] = &stagePlanNode;
        op.Inputs.push_back(inputContext);

        AddOperator(planNode, planNodeSuffix, op);
    }

    void Visit(const TDqSink& sink, const TDqStageBase& stage, TQueryPlanNode& planNode, TQueryPlanNode& stagePlanNode) {
        IDqIntegration* dqIntegration = nullptr;
        auto dataSink = sink.DataSink().Cast<TCoDataSink>();
        auto providerIt = SerializerCtx.TypeCtx.DataSinkMap.find(dataSink.Category().StringValue());
        if (providerIt != SerializerCtx.TypeCtx.DataSinkMap.end()) {
            dqIntegration = providerIt->second->GetDqIntegration();
        }

        VisitTableSink(sink, stage, planNode, stagePlanNode);

        if (dqIntegration) {
            dqIntegration->FillSinkPlanProperties(sink, planNode.Operators.back().Properties);
        }
    }

    void Visit(const TDqTransform& transform, const TDqStageBase& stage, TQueryPlanNode& planNode, TQueryPlanNode& stagePlanNode) {
        IDqIntegration* dqIntegration = nullptr;
        auto dataSink = transform.DataSink().Cast<TCoDataSink>();
        auto providerIt = SerializerCtx.TypeCtx.DataSinkMap.find(dataSink.Category().StringValue());
        if (providerIt != SerializerCtx.TypeCtx.DataSinkMap.end()) {
            dqIntegration = providerIt->second->GetDqIntegration();
        }

        VisitTableSink(transform, stage, planNode, stagePlanNode, TString(transform.Type().Value()));

        if (dqIntegration) {
            dqIntegration->FillSinkPlanProperties(transform, planNode.Operators.back().Properties);
        }
    }

    void Visit(const TExprBase& expr, TQueryPlanNode& planNode) {
        if (expr.Maybe<TDqPhyStage>()) {
            auto stageGuid = NDq::TDqStageSettings::Parse(expr.Cast<TDqPhyStage>()).Id;

            if (VisitedStages.find(expr.Raw()) != VisitedStages.end()) {
                auto stageId = SerializerCtx.StageGuidToId[stageGuid];
                Y_ENSURE(QueryPlanNodes.contains(stageId));
                auto& commonNode = QueryPlanNodes[stageId];

                if (!commonNode.CteName) {
                    commonNode.CteName = TStringBuilder() << commonNode.TypeName << "_" << stageId;
                }

                planNode.CteRefName = *commonNode.CteName;

                return;
            }

            auto& stagePlanNode = AddPlanNode(planNode);
            stagePlanNode.Guid = stageGuid;
            stagePlanNode.StageProto = FindStageProtoByGuid(TxProto, stageGuid);
            YQL_ENSURE(stagePlanNode.StageProto, "Could not find a stage with id " << stageGuid);
            SerializerCtx.StageGuidToId[stageGuid] = SerializerCtx.PlanNodeId;
            VisitedStages.insert(expr.Raw());

            TVector<TQueryPlanNode*> inputIds;

            for (const auto& input : expr.Cast<TDqStageBase>().Inputs()) {
                if (auto source = input.Maybe<TDqSource>()) {
                    auto& inputSourceNode = AddPlanNode(stagePlanNode);
                    Visit(source.Cast(), inputSourceNode, expr.Cast<TDqStageBase>().Program());
                    inputIds.emplace_back(&inputSourceNode);
                } else {
                    auto inputCn = input.Cast<TDqConnection>();

                    auto& inputPlanNode = AddPlanNode(stagePlanNode);
                    FillConnectionPlanNode(inputCn, inputPlanNode);

                    Visit(inputCn.Output().Stage(), inputPlanNode);
                    inputIds.emplace_back(&inputPlanNode);
                }
            }

            CurrentArgContext.stack.push_back(expr.Ptr().Get());

            if (inputIds.size() == expr.Cast<TDqStageBase>().Program().Args().Size()) {
                for (size_t i = 0; i < expr.Cast<TDqStageBase>().Program().Args().Size(); i++ ) {
                    const auto& arg = expr.Cast<TDqStageBase>().Program().Args().Arg(i);
                    LambdaInputs[CurrentArgContext.AddArg(arg.Ptr().Get())] = inputIds[0];
                    inputIds.erase(inputIds.begin());
                }
            }

            auto node = expr.Cast<TDqStageBase>().Program().Body().Ptr();
            Visit(node, stagePlanNode);

            CurrentArgContext.stack.pop_back();

            /* is that collect stage? */
            if (stagePlanNode.TypeName.empty()) {
                if (expr.Cast<TDqStageBase>().Program().Body().Maybe<TCoArgument>()) {
                    stagePlanNode.TypeName = "Collect";
                } else {
                    stagePlanNode.TypeName = "Stage";
                }
            }

            if (auto outputs = expr.Cast<TDqStageBase>().Outputs()) {
                for (auto output : outputs.Cast()) {
                    if (auto sink = output.Maybe<TDqSink>()) {
                        Visit(sink.Cast(), expr.Cast<TDqStageBase>(), planNode, stagePlanNode);
                    } else if (auto transform = output.Maybe<TDqTransform>()) {
                        Visit(transform.Cast(), expr.Cast<TDqStageBase>(), planNode, stagePlanNode);
                    }
                }
            }
        } else {
            Visit(expr.Ptr(),  planNode);

            if (planNode.TypeName.empty()) {
                planNode.TypeName = "Stage";
            }
        }
    }

    TVector<std::variant<ui32, TArgContext>> Visit(TExprNode::TPtr node, TQueryPlanNode& planNode) {
        TMaybe<std::variant<ui32, TArgContext>> operatorId;

        auto maybeCallable = TMaybeNode<TCallable>(node);

        if (auto maybeRead = TMaybeNode<TKqlReadTableBase>(node)) {
            auto read = maybeRead.Cast();
            TString table = TString(read.Table().Path()); TKqlKeyRange range = read.Range();
            operatorId = Visit(table, range, read, planNode);
        } else if (TMaybeNode<TKqlReadTableRangesBase>(node) && !TMaybeNode<TKqpReadOlapTableRangesBase>(node)) {
            auto maybeReadRanges = TMaybeNode<TKqlReadTableRangesBase>(node);
            operatorId = Visit(maybeReadRanges.Cast(), planNode);
        } else if (auto maybeLookup = TMaybeNode<TKqlLookupTableBase>(node)) {
            operatorId = Visit(maybeLookup.Cast(), planNode);
        } else if (node->IsCallable({"Map", "FlatMap", "OrderedMap", "OrderedFlatMap"}) && node->ChildrenSize() >= 2
            && FindNode(node->ChildPtr(1), [](const TExprNode::TPtr& n) {
                   auto udf = TMaybeNode<TCoUdf>(n);
                   return udf && udf.Cast().MethodName().Value().StartsWith("HybridSearch.");
               })) {
            // The Reciprocal Rank Fusion step uniquely identifies a hybrid-search (HybridRank) query.
            TOperator op;
            op.Properties["Name"] = "HybridSearch";
            operatorId = AddOperator(planNode, "HybridSearch", std::move(op));
        } else if (auto maybeFilter = TMaybeNode<TCoFilterBase>(node)) {
            operatorId = Visit(maybeFilter.Cast(), planNode);
        } else if (auto maybeMapJoin = TMaybeNode<TCoMapJoinCore>(node)) {
            operatorId = Visit(maybeMapJoin.Cast(), planNode);
        } else if (TMaybeNode<TCoFlatMapBase>(node).Lambda().Body().Maybe<TCoMapJoinCore>() ||
                   TMaybeNode<TCoFlatMapBase>(node).Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoMapJoinCore>()) {
            auto flatMap = TMaybeNode<TCoFlatMapBase>(node).Cast();
            auto join = TExprBase(FindNode(node, [](const TExprNode::TPtr& node) {
                Y_ENSURE(!TMaybeNode<TDqConnection>(node).IsValid());
                return TMaybeNode<TCoMapJoinCore>(node).IsValid();
            })).Cast<TCoMapJoinCore>();
            operatorId = Visit(flatMap, join, planNode);
            node = join.Ptr();
        } else if (auto maybeJoinDict = TMaybeNode<TCoJoinDict>(node)) {
            operatorId = Visit(maybeJoinDict.Cast(), planNode);
        } else if (TMaybeNode<TCoFlatMapBase>(node).Lambda().Body().Maybe<TCoJoinDict>() ||
                   TMaybeNode<TCoFlatMapBase>(node).Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoJoinDict>()) {
            auto flatMap = TMaybeNode<TCoFlatMapBase>(node).Cast();
            auto join = TExprBase(FindNode(node, [](const TExprNode::TPtr& node) {
                Y_ENSURE(!TMaybeNode<TDqConnection>(node).IsValid());
                return TMaybeNode<TCoJoinDict>(node).IsValid();
            })).Cast<TCoJoinDict>();
            operatorId = Visit(flatMap, join, planNode);
            node = join.Ptr();
        } else if (auto maybeJoinDict = TMaybeNode<TCoGraceJoinCore>(node)) {
            operatorId = Visit(maybeJoinDict.Cast(), planNode);
        } else if (TMaybeNode<TCoFlatMapBase>(node).Lambda().Body().Maybe<TCoGraceJoinCore>() ||
                   TMaybeNode<TCoFlatMapBase>(node).Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoGraceJoinCore>()) {
            auto flatMap = TMaybeNode<TCoFlatMapBase>(node).Cast();
            auto join = TExprBase(FindNode(node, [](const TExprNode::TPtr& node) {
                Y_ENSURE(!TMaybeNode<TDqConnection>(node).IsValid());
                return TMaybeNode<TCoGraceJoinCore>(node).IsValid();
            })).Cast<TCoGraceJoinCore>();
            operatorId = Visit(flatMap, join, planNode);
            node = join.Ptr();
        } else if (auto maybeCondense1 = TMaybeNode<TCoCondense1>(node)) {
            operatorId = Visit(maybeCondense1.Cast(), planNode);
        } else if (auto maybeCondense = TMaybeNode<TCoCondense>(node)) {
            operatorId = Visit(maybeCondense.Cast(), planNode);
        } else if (auto maybeCombiner = TMaybeNode<TCoCombineCore>(node)) {
            operatorId = Visit(maybeCombiner.Cast(), planNode);
        } else if (auto maybeBlockCombine = TMaybeNode<TCoBlockCombineHashed>(node)) {
            operatorId = Visit(maybeBlockCombine.Cast(), planNode);
        } else if (maybeCallable && (maybeCallable.Cast().CallableName() == "BlockMergeFinalizeHashed" || maybeCallable.Cast().CallableName() == "BlockMergeManyFinalizeHashed")) {
            TOperator op;
            op.Properties["Name"] = "Aggregate";
            op.Properties["Blocks"] = "True";
            op.Properties["Phase"] = "Final";
            AddOptimizerEstimates(op, TExprBase(node));
            operatorId = AddOperator(planNode, "Aggregate", std::move(op));
        } else if (auto maybeCombiner = TMaybeNode<TCoWideCombiner>(node)) {
            operatorId = Visit(maybeCombiner.Cast(), planNode);
        } else if (auto maybeSort = TMaybeNode<TCoWideSort>(node)) {
            operatorId = Visit(maybeSort.Cast(), planNode);
        } else if (auto maybeSort = TMaybeNode<TCoSort>(node)) {
            operatorId = Visit(maybeSort.Cast(), planNode);
        } else if (auto maybeTop = TMaybeNode<TCoTop>(node)) {
            operatorId = Visit(maybeTop.Cast(), planNode);
        } else if (auto maybeTopSort = TMaybeNode<TCoTopSort>(node)) {
            operatorId = Visit(maybeTopSort.Cast(), planNode);
        } else if (auto maybeTake = TMaybeNode<TCoTake>(node)) {
            operatorId = Visit(maybeTake.Cast(), planNode);
        } else if (auto maybeSkip = TMaybeNode<TCoSkip>(node)) {
            operatorId = Visit(maybeSkip.Cast(), planNode);
        } else if (auto maybeExtend = TMaybeNode<TCoExtendBase>(node)) {
            operatorId = Visit(maybeExtend.Cast(), planNode);
        } else if (auto maybeToFlow = TMaybeNode<TCoToFlow>(node)) {
            operatorId = Visit(maybeToFlow.Cast(), planNode);
        } else if (auto maybeAssumeSorted = TMaybeNode<TCoAssumeSorted>(node)) {
            operatorId = Visit(maybeAssumeSorted.Cast(), planNode);
        } else if (auto maybeMember = TMaybeNode<TCoMember>(node)) {
            operatorId = Visit(maybeMember.Cast(), planNode);
        } else if (auto maybeIter = TMaybeNode<TCoIterator>(node)) {
            operatorId = Visit(maybeIter.Cast(), planNode);
        } else if (auto maybePartitionByKey = TMaybeNode<TCoPartitionByKey>(node)) {
            operatorId = Visit(maybePartitionByKey.Cast(), planNode);
        } else if (auto maybeArg = TMaybeNode<TCoArgument>(node)) {
            return {CurrentArgContext.AddArg(node.Get())};
        } else if (auto maybeCrossJoin = TMaybeNode<TDqPhyCrossJoin>(node)) {
            operatorId = Visit(maybeCrossJoin.Cast(), planNode);
        } else if (auto maybeBlockHashJoin = TMaybeNode<TDqBlockHashJoinCore>(node)) {
            operatorId = Visit(maybeBlockHashJoin.Cast(), planNode);
        } else if (auto maybeScalarHashJoin = TMaybeNode<TDqScalarHashJoinCore>(node)) {
            operatorId = Visit(maybeScalarHashJoin.Cast(), planNode);
        } else if (auto lookupJoin = TMaybeNode<TKqpIndexLookupJoin>(node)) {
            operatorId = Visit(lookupJoin.Cast(), planNode);
        } else if (auto maybeCombineByKey = TMaybeNode<TCoCombineByKey>(node)) {
            operatorId = Visit(maybeCombineByKey.Cast(), planNode);
        }

        TVector<std::variant<ui32, TArgContext>> inputIds;
        {
            if (TMaybeNode<TCoFlatMapBase>(node)) {
                auto flatMap = TExprBase(node).Cast<TCoFlatMapBase>();
                auto flatMapInputs = Visit(flatMap, planNode);

                auto flatMapLambdaInputs = Visit(flatMap.Lambda().Body().Ptr(), planNode);
                inputIds.insert(inputIds.end(), flatMapLambdaInputs.begin(), flatMapLambdaInputs.end());
            } else if (TMaybeNode<TCoMap>(node)) {
                auto map = TExprBase(node).Cast<TCoMap>();
                auto mapInputs = Visit(map, planNode);

                auto mapLambdaInputs = Visit(map.Lambda().Body().Ptr(), planNode);
                inputIds.insert(inputIds.end(), mapLambdaInputs.begin(), mapLambdaInputs.end());
            } else if (TMaybeNode<TKqpReadOlapTableRangesBase>(node)) {
                auto olapTable = TExprBase(node).Cast<TKqpReadOlapTableRangesBase>();

                ui32 currentOperatorId = 0;

                auto aggr = [](const TExprNode::TPtr& n) -> bool {
                    if (auto maybeAggregation = TMaybeNode<TKqpOlapAgg>(n)) { return true; } return false;
                };

                if (auto maybeKqpOlapAggregation = FindNode(olapTable.Process().Body().Ptr(), aggr)) {
                    auto kqpOlapAggregation = TExprBase(maybeKqpOlapAggregation).Cast<TKqpOlapAgg>();

                    TOperator op;
                    op.Properties["Name"] = "Aggregate";
                    op.Properties["Phase"] = "Intermediate";
                    op.Properties["Pushdown"] = "True";
                    op.Properties["Blocks"] = "True";

                    AddOptimizerEstimates(op, kqpOlapAggregation);
                    currentOperatorId = AddOperator(planNode, "Aggregate", std::move(op));
                    operatorId = currentOperatorId;
                }

                auto distinctPred = [](const TExprNode::TPtr& n) -> bool {
                    if (auto maybeDistinct = TMaybeNode<TKqpOlapDistinct>(n)) { return true; } return false;
                };

                if (auto maybeKqpOlapDistinct = FindNode(olapTable.Process().Body().Ptr(), distinctPred)) {
                    auto kqpOlapDistinct = TExprBase(maybeKqpOlapDistinct).Cast<TKqpOlapDistinct>();

                    TOperator op;
                    op.Properties["Name"] = "Distinct";
                    op.Properties["Pushdown"] = "True";
                    op.Properties["Blocks"] = "True";

                    AddOptimizerEstimates(op, kqpOlapDistinct);
                    currentOperatorId = AddOperator(planNode, "Distinct", std::move(op));
                    operatorId = currentOperatorId;
                }

                auto pred = [](const TExprNode::TPtr& n) -> bool {
                    if (auto maybeFilter = TMaybeNode<TKqpOlapFilter>(n)) { return true; } return false;
                };

                if (auto maybeKqpOlapFilter = FindNode(olapTable.Process().Body().Ptr(), pred)) {
                    auto kqpOlapFilter = TExprBase(maybeKqpOlapFilter).Cast<TKqpOlapFilter>();

                    TOperator op;
                    op.Properties["Name"] = "Filter";
                    op.Properties["Predicate"] = NOpt::FormatOlapFilter(kqpOlapFilter);
                    op.Properties["Pushdown"] = "True";
                    op.Properties["Blocks"] = "True";

                    AddOptimizerEstimates(op, kqpOlapFilter);
                    auto filterOperatorId = AddOperator(planNode, "Filter", std::move(op));

                    if (operatorId) {
                        planNode.Operators[currentOperatorId].Inputs.push_back(filterOperatorId);
                    } else {
                        operatorId = filterOperatorId;
                    }
                    currentOperatorId = filterOperatorId;
                }

                auto tableOperatorId = Visit(olapTable, planNode);

                if (operatorId) {
                    planNode.Operators[currentOperatorId].Inputs.push_back(tableOperatorId);
                } else {
                    operatorId = tableOperatorId;
                }
            } else if (TMaybeNode<TCoToFlow>(node)) {
                // do nothing
            } else {
                for (const auto& child : node->Children()) {
                    if(!child->IsLambda()) {
                        auto ids = Visit(child, planNode);
                        inputIds.insert(inputIds.end(), ids.begin(), ids.end());
                    }
                }
            }
        }

        if (operatorId) {
            if (std::holds_alternative<ui32>(*operatorId)) {
                TVector<std::variant<ui32, TArgContext>>& opInputs = planNode.Operators[std::get<ui32>(*operatorId)].Inputs;
                opInputs.insert(opInputs.begin(), inputIds.begin(), inputIds.end());
                return {std::get<ui32>(*operatorId)};
            }
            else {
                return {std::get<TArgContext>(*operatorId)};
            }
        }

        return inputIds;
    }

    TVector<std::variant<ui32, TArgContext>> Visit(const TCoMap& map, TQueryPlanNode& planNode) {
        auto mapInputs = Visit(map.Input().Ptr(), planNode);

        if (!mapInputs.empty() && map.Lambda().Args().Size() != 0) {
            auto input = mapInputs[0];
            auto newContext = CurrentArgContext.AddArg(map.Lambda().Args().Arg(0).Ptr().Get());

            if (std::holds_alternative<ui32>(input)) {
                LambdaInputs[newContext] = std::get<ui32>(input);
            } else {
                auto context = std::get<TArgContext>(input);
                if (LambdaInputs.contains(context)){
                    LambdaInputs[newContext] = LambdaInputs.at(context);
                }
            }
        }

        return mapInputs;
    }

    TVector<std::variant<ui32, TArgContext>> Visit(const TCoFlatMapBase& flatMap, TQueryPlanNode& planNode) {
        auto flatMapInputs = Visit(flatMap.Input().Ptr(), planNode);

        if (!flatMapInputs.empty() && flatMap.Lambda().Args().Size() != 0) {
            auto input = flatMapInputs[0];
            auto newContext = CurrentArgContext.AddArg(flatMap.Lambda().Args().Arg(0).Ptr().Get());

            if (std::holds_alternative<ui32>(input)) {
                LambdaInputs[newContext] = std::get<ui32>(input);
            } else {
                auto context = std::get<TArgContext>(input);
                if (LambdaInputs.contains(context)){
                    LambdaInputs[newContext] = LambdaInputs.at(context);
                }
            }
        }

        return flatMapInputs;
    }

    std::variant<ui32, TArgContext> Visit(const TCoCondense1& condense, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Aggregate";
        op.Properties["Phase"] = "Intermediate";
        AddOptimizerEstimates(op, condense);

        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoCondense& condense, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Aggregate";
        op.Properties["Phase"] = "Final";
        AddOptimizerEstimates(op, condense);

        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoCombineCore& combiner, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Aggregate";
        op.Properties["GroupBy"] = NPlanUtils::PrettyExprStr(combiner.KeyExtractor());
        op.Properties["Aggregation"] = NPlanUtils::PrettyExprStr(combiner.UpdateHandler());
        op.Properties["Phase"] = "Intermediate";
        AddOptimizerEstimates(op, combiner);

        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoBlockCombineHashed& blockCombine, TQueryPlanNode& planNode) {

        static const THashMap<TString, TString> aggregations = {
            {"count", "COUNT"},
            {"count_all", "COUNT"},
            {"min", "MIN"},
            {"max", "MAX"},
            {"avg", "AVG"},
            {"sum", "SUM"}
        };

        TOperator op;
        op.Properties["Name"] = "Aggregate";
        op.Properties["Blocks"] = "True";
        op.Properties["GroupBy"] = NPlanUtils::PrettyExprStr(blockCombine.Keys());

        if (blockCombine.Aggregations().Ref().IsList()) {
            TVector<TString> aggrs;
            for (ui32 index = 0; index < blockCombine.Aggregations().Ref().ChildrenSize(); index++) {
                auto child = blockCombine.Aggregations().Ref().Child(index);
                if (child && child->IsList() && child->ChildrenSize() >= 2) {
                    auto callable = child->Child(0);
                    if (callable->ChildrenSize() >= 1) {
                        if (auto aggrName = TMaybeNode<TCoAtom>(callable->Child(0))) {
                            aggrs.push_back(aggregations.Value(aggrName.Cast().StringValue(), "??"));
                        }
                    }
                }
            }
            op.Properties["Aggregation"] = JoinStrings(std::move(aggrs), ",");
        }
        op.Properties["Phase"] = "Intermediate";

        AddOptimizerEstimates(op, blockCombine);
        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoWideCombiner& combiner, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Aggregate";
        // op.Properties["GroupBy"] = NPlanUtils::PrettyExprStr(combiner.KeyExtractor());
        // op.Properties["Aggregation"] = NPlanUtils::PrettyExprStr(combiner.UpdateHandler());
        // op.Properties["Finish"] = NPlanUtils::PrettyExprStr(combiner.FinishHandler());
        op.Properties["Phase"] = "Final";
        AddOptimizerEstimates(op, combiner);
        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoSort& sort, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Sort";
        op.Properties["SortBy"] = NPlanUtils::PrettyExprStr(sort.KeySelectorLambda());

        return AddOperator(planNode, "Sort", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoWideSort& sort, TQueryPlanNode& planNode) {
        const auto& input = sort.Input().Ref();
        YQL_ENSURE(input.IsCallable("ExpandMap") && input.ChildrenSize() == 2);

        const auto& expandLambda = input.Tail();
        YQL_ENSURE(expandLambda.IsLambda());

        TStringBuilder sortBy;
        for (const auto& key : sort.Keys()) {
            const auto index = FromString<ui32>(key.Index().Value());
            YQL_ENSURE(index + 1 < expandLambda.ChildrenSize());

            const TExprBase item(expandLambda.ChildPtr(index + 1));
            const auto ascending = FromString<bool>(key.Direction().Cast<TCoBool>().Literal().Value());

            if (sortBy.size()) {
                sortBy << ", ";
            }
            if (const auto member = item.Maybe<TCoMember>()) {
                sortBy << member.Cast().Name().Value();
            } else {
                sortBy << NPlanUtils::PrettyExprStr(item);
            }
            sortBy << (ascending ? " asc" : " desc");
        }

        TOperator op;
        op.Properties["Name"] = "WideSort";
        op.Properties["SortBy"] = TString(sortBy);

        return AddOperator(planNode, "WideSort", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoTop& top, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Top";
        op.Properties["TopBy"] = NPlanUtils::PrettyExprStr(top.KeySelectorLambda());
        op.Properties["Limit"] = NPlanUtils::PrettyExprStr(top.Count());

        return AddOperator(planNode, "Top", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoTopSort& topSort, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "TopSort";
        op.Properties["TopSortBy"] = NPlanUtils::PrettyExprStr(topSort.KeySelectorLambda());
        op.Properties["Limit"] = NPlanUtils::PrettyExprStr(topSort.Count());

        return AddOperator(planNode, "TopSort", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoTake& take, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Limit";
        op.Properties["Limit"] = NPlanUtils::PrettyExprStr(take.Count());

        return AddOperator(planNode, "Limit", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoSkip& skip, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Offset";
        op.Properties["Offset"] = NPlanUtils::PrettyExprStr(skip.Count());

        return AddOperator(planNode, "Offset", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoExtendBase& /*extend*/, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Union";

        return AddOperator(planNode, "Union", std::move(op));
    }

    TMaybe<std::variant<ui32, TArgContext>> Visit(const TCoToFlow& toflow, TQueryPlanNode& planNode) {
        const auto toflowValue = NPlanUtils::PrettyExprStr(toflow.Input());

        TOperator op;
        op.Properties["Name"] = "ToFlow";

        if (auto maybeResultBinding = ContainResultBinding(toflowValue)) {
            auto [txId, resId] = *maybeResultBinding;
            planNode.CteRefName = TStringBuilder() << "precompute_" << txId << "_" << resId;
            op.Properties["ToFlow"] = *planNode.CteRefName;
        } else {
            auto inputs = Visit(toflow.Input().Ptr(), planNode);
            if (inputs.size() == 1) {
                return inputs[0];
            } else {
                return TMaybe<std::variant<ui32, TArgContext>> ();
            }
        }

        return AddOperator(planNode, "ConstantExpr", std::move(op));
    }

    TMaybe<std::variant<ui32, TArgContext>> Visit(const TCoCombineByKey& combineByKey, TQueryPlanNode& planNode) {
        const auto combineByKeyValue = NPlanUtils::PrettyExprStr(combineByKey.Input());

        TOperator op;
        op.Properties["Name"] = "CombineByKey";

        if (auto maybeResultBinding = ContainResultBinding(combineByKeyValue)) {
            auto [txId, resId] = *maybeResultBinding;
            planNode.CteRefName = TStringBuilder() << "precompute_" << txId << "_" << resId;
            op.Properties["CombineByKey"] = *planNode.CteRefName;
        } else {
            auto inputs = Visit(combineByKey.Input().Ptr(), planNode);
            if (inputs.size() == 1) {
                return inputs[0];
            } else {
                return TMaybe<std::variant<ui32, TArgContext>> ();
            }
        }

        return AddOperator(planNode, "ConstantExpr", std::move(op));
    }

    TMaybe<std::variant<ui32, TArgContext>> Visit(const TCoAssumeSorted& assumeSorted, TQueryPlanNode& planNode) {
        const auto precomputeValue = NPlanUtils::PrettyExprStr(assumeSorted.Input());

        TOperator op;
        op.Properties["Name"] = "AssumeSorted";

        if (auto maybeResultBinding = ContainResultBinding(precomputeValue)) {
            auto [txId, resId] = *maybeResultBinding;
            planNode.CteRefName = TStringBuilder() << "precompute_" << txId << "_" << resId;
            op.Properties["AssumeSorted"] = *planNode.CteRefName;
        } else {
            auto inputs = Visit(assumeSorted.Input().Ptr(), planNode);
            if (inputs.size() == 1) {
                return inputs[0];
            } else {
                return TMaybe<std::variant<ui32, TArgContext>> ();
            }
        }

        return AddOperator(planNode, "ConstantExpr", std::move(op));
    }

    TMaybe<std::variant<ui32, TArgContext>> Visit(const TCoMember& member, TQueryPlanNode& planNode) {
        const auto memberValue = NPlanUtils::PrettyExprStr(member.Struct());

        TOperator op;
        op.Properties["Name"] = "Member";

        if (auto maybeResultBinding = ContainResultBinding(memberValue)) {
            auto [txId, resId] = *maybeResultBinding;
            planNode.CteRefName = TStringBuilder() << "precompute_" << txId << "_" << resId;
            op.Properties["Member"] = *planNode.CteRefName;
        } else {
            auto inputs = Visit(member.Struct().Ptr(), planNode);
            if (inputs.size() == 1) {
                return inputs[0];
            } else {
                return TMaybe<std::variant<ui32, TArgContext>> ();
            }
        }

        if (std::find_if(planNode.Operators.begin(), planNode.Operators.end(), [op](const auto & x) {
            return x.Properties.at("Name")=="Member" && x.Properties.at("Member")==op.Properties.at("Member");
        }) != planNode.Operators.end()) {
            return TMaybe<std::variant<ui32, TArgContext>> ();
        }

        return AddOperator(planNode, "ConstantExpr", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoIterator& iter, TQueryPlanNode& planNode) {
        const auto iterValue = NPlanUtils::PrettyExprStr(iter.List());

        TOperator op;
        op.Properties["Name"] = "Iterator";

        if (auto maybeResultBinding = ContainResultBinding(iterValue)) {
            auto [txId, resId] = *maybeResultBinding;
            planNode.CteRefName = TStringBuilder() << "precompute_" << txId << "_" << resId;
            op.Properties["Iterator"] = *planNode.CteRefName;
        } else {
            op.Properties["Iterator"] = iterValue;
        }

        return AddOperator(planNode, "ConstantExpr", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoPartitionByKey& partitionByKey, TQueryPlanNode& planNode) {
        const auto inputValue = NPlanUtils::PrettyExprStr(partitionByKey.Input());
        TOperator op;
        op.Properties["Name"] = "PartitionByKey";

        if (auto maybeResultBinding = ContainResultBinding(inputValue)) {
            auto [txId, resId] = *maybeResultBinding;
            planNode.CteRefName = TStringBuilder() << "precompute_" << txId << "_" << resId;
            op.Properties["Input"] = *planNode.CteRefName;
        } else {
            op.Properties["Input"] = inputValue;
        }

        AddOptimizerEstimates(op, partitionByKey);
        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    TString MakeJoinConditionString(const TCoAtomList& leftKeys, const TCoAtomList& rightKeys) {
        TString result = "";

        for (size_t i = 0; i < leftKeys.Size(); i++) {
            result += leftKeys.Item(i).StringValue();
            if (i != leftKeys.Size() - 1) {
                result += ",";
            }
        }

        result += " = ";

        for (size_t i = 0; i < rightKeys.Size(); i++) {
            result += rightKeys.Item(i).StringValue();
            if (i != rightKeys.Size() - 1) {
                result += ",";
            }
        }

        return result;

    }

    std::variant<ui32, TArgContext> Visit(const TCoFlatMapBase& flatMap, const TCoMapJoinCore& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (Map)";

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumnNames(), join.RightKeysColumnNames());

        AddOptimizerEstimates(op, join);

        auto operatorId = AddOperator(planNode, name, std::move(op));

        Visit(flatMap, planNode);

        return operatorId;
    }

    std::variant<ui32, TArgContext> Visit(const TDqPhyCrossJoin&, TQueryPlanNode& planNode) {
        const auto name = "CrossJoin";

        TOperator op;
        op.Properties["Name"] = name;

        return AddOperator(planNode, name, std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TDqBlockHashJoinCore& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (BlockHash)";

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumnNames(), join.RightKeysColumnNames());

        AddOptimizerEstimates(op, join);

        return AddOperator(planNode, name, std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TDqScalarHashJoinCore& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (ScalarHash)";

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumnNames(), join.RightKeysColumnNames());

        AddOptimizerEstimates(op, join);

        return AddOperator(planNode, name, std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TKqpIndexLookupJoin& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinType().Value() << "Join (Lookup)";

        TOperator op;
        op.Properties["Name"] = name;
        // op["LookupKeyColumns"] = plan.GetMapSafe().at("LookupKeyColumns");

        return AddOperator(planNode, name, std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoMapJoinCore& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (Map)";

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumnNames(), join.RightKeysColumnNames());


        AddOptimizerEstimates(op, join);

        return AddOperator(planNode, name, std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoFlatMapBase& flatMap, const TCoJoinDict& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (Dict)";

        TOperator op;
        op.Properties["Name"] = name;

        AddOptimizerEstimates(op, join);

        auto operatorId = AddOperator(planNode, name, std::move(op));

        Visit(flatMap, planNode);

        return operatorId;
    }

    std::variant<ui32, TArgContext> Visit(const TCoJoinDict& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (Dict)";

        TOperator op;
        op.Properties["Name"] = name;

        AddOptimizerEstimates(op, join);

        return AddOperator(planNode, name, std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoFlatMapBase& flatMap, const TCoGraceJoinCore& join, TQueryPlanNode& planNode) {
        auto joinAlgo = "(Grace)";
        for (size_t i=0; i<join.Flags().Size(); i++) {
            if (join.Flags().Item(i).StringValue() == "Broadcast") {
                joinAlgo = "(Map)";
            }
        }
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join " << joinAlgo;

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumnNames(), join.RightKeysColumnNames());

        AddOptimizerEstimates(op, join);

        auto operatorId = AddOperator(planNode, name, std::move(op));

        Visit(flatMap, planNode);

        return operatorId;
    }

    std::variant<ui32, TArgContext> Visit(const TCoGraceJoinCore& join, TQueryPlanNode& planNode) {
        auto joinAlgo = "(Grace)";
        for (size_t i=0; i<join.Flags().Size(); i++) {
            if (join.Flags().Item(i).StringValue() == "Broadcast") {
                joinAlgo = "(Map)";
            }
        }
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join " << joinAlgo;

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumnNames(), join.RightKeysColumnNames());

        AddOptimizerEstimates(op, join);

        return AddOperator(planNode, name, std::move(op));
    }

    void AddOptimizerEstimates(TOperator& op, const TExprBase& expr) {
        if (SerializerCtx.Config->CostBasedOptimizationLevel.Get().GetOrElse(SerializerCtx.Config->GetDefaultCostBasedOptimizationLevel())==0) {
            return;
        }

        TStatsView stats = SerializerCtx.OptimizeCtx
            ? TStatsView(SerializerCtx.OptimizeCtx->KqpStats.GetStats(expr.Raw()))
            : TStatsView(SerializerCtx.TypeCtx.GetStats(expr.Raw()));
        if (stats) {
            op.Properties["E-Rows"] = TStringBuilder() << stats.Nrows;
            op.Properties["E-Cost"] = TStringBuilder() << stats.Cost;
            op.Properties["E-Size"] = TStringBuilder() << stats.ByteSize;
        }
        else {
            op.Properties["E-Rows"] = "No estimate";
            op.Properties["E-Cost"] = "No estimate";
            op.Properties["E-Size"] = "No estimate";

        }
    }

    static bool IsIdentityLambda(const TCoLambda& lambda) {
        return lambda.Args().Size() == 1 && lambda.Body().Raw() == lambda.Args().Arg(0).Raw();
    }

    std::variant<ui32, TArgContext> Visit(const TCoFilterBase& filter, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Filter";
        try {
            auto pred = NPlanUtils::ExtractPredicate(filter.Lambda());
            op.Properties["Predicate"] = pred.Body;
        } catch (...) {
            op.Properties["Predicate"] = "";
        }

        AddOptimizerEstimates(op, filter);

        if (filter.Limit()) {
            op.Properties["Limit"] = NPlanUtils::PrettyExprStr(filter.Limit().Cast());
        }

        return AddOperator(planNode, "Filter", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TKqlLookupTableBase& lookup, TQueryPlanNode& planNode) {
        auto tablePath = TString(lookup.Table().Path().Value());
        auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, tablePath);

        auto lookupKeysType = lookup.LookupKeys().Ref().GetTypeAnn();
        const TTypeAnnotationNode* lookupKeysItemType = nullptr;
        if (lookupKeysType->GetKind() == ETypeAnnotationKind::List) {
            lookupKeysItemType = lookupKeysType->Cast<TListExprType>()->GetItemType();
        } else if (lookupKeysType->GetKind() == ETypeAnnotationKind::Stream) {
            lookupKeysItemType = lookupKeysType->Cast<TStreamExprType>()->GetItemType();
        } else {
            Y_ENSURE(false, "Unexpected lookup keys type");
        }

        Y_ENSURE(lookupKeysItemType);
        Y_ENSURE(lookupKeysItemType->GetKind() == ETypeAnnotationKind::Struct);
        auto lookupKeyColumnsCount = lookupKeysItemType->Cast<TStructExprType>()->GetSize();

        TTableRead readInfo;
        readInfo.Type = lookupKeyColumnsCount == tableData.Metadata->KeyColumnNames.size()
            ? EPlanTableReadType::Lookup : EPlanTableReadType::Scan;
        auto readName = GetNameByReadType(readInfo.Type);

        TOperator op;
        op.Properties["Name"] = readName;
        op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : tablePath;
        op.Properties["Path"] = tablePath;
        auto& columns = op.Properties["ReadColumns"];
        for (auto const& col : lookup.Columns()) {
            readInfo.Columns.push_back(TString(col.Value()));
            columns.AppendValue(col.Value());
        }

        AddOptimizerEstimates(op, lookup);

        SerializerCtx.Tables[tablePath].Reads.push_back(readInfo);
        planNode.NodeInfo["Tables"].AppendValue(op.Properties["Table"]);
        return AddOperator(planNode, readName, std::move(op));
    }

    ui32 Visit(const TKqlReadTableRangesBase& read, TQueryPlanNode& planNode) {
        const auto tablePath = TString(read.Table().Path());
        const auto explainPrompt = TKqpReadTableExplainPrompt::Parse(read);
        const auto rangesDesc = NPlanUtils::PrettyExprStr(read.Ranges());

        TTableRead readInfo;
        TOperator op;

        TReadRangesParams params {
            .TablePath = tablePath,
            .ExplainPrompt = explainPrompt,
            .RangesDesc = rangesDesc,
            .IncludePointPrefixLen = false,
            .IndexSelectionInfo = TKqpReadTableSettings::Parse(read.Settings()).IndexSelectionInfo,
        };

        ProcessReadRangesCommon(params, readInfo, op, planNode, read.Columns());

        AddReadTableSettings(op, read, readInfo);

        if (auto maybeRead = read.Maybe<TKqpReadOlapTableRangesBase>()) {
            const auto olapRead = maybeRead.Cast();
            op.Properties["SsaProgram"] = GetSsaProgramInJsonByTable(read.Table().Path().StringValue(), planNode.StageProto);
            if (IsIdentityLambda(olapRead.Process())) {
                AddOptimizerEstimates(op, olapRead);
            } else {
                AddOptimizerEstimates(op, olapRead.Process());
            }
        } else {
            AddOptimizerEstimates(op, read);
        }

        auto readName = GetNameByReadType(readInfo.Type);
        op.Properties["Name"] = readName;
        ui32 operatorId = AddOperator(planNode, readName, std::move(op));

        SerializerCtx.Tables[tablePath].Reads.push_back(std::move(readInfo));
        return operatorId;
    }

    template <typename TReadTableNode>
    std::variant<ui32, TArgContext> Visit(const TString& tablePath, const TKqlKeyRange& range, const TReadTableNode& read, TQueryPlanNode& planNode) {
        TOperator op;
        TTableRead readInfo;

        auto describeBoundary = [this](const TExprBase& key) {
            TString res("n/a");

            if (auto param = key.Maybe<TCoParameter>()) {
                res = param.Cast().Name().StringValue();
            } else if (auto param = key.Maybe<TCoNth>().Tuple().Maybe<TCoParameter>()) {
                if (auto maybeResultBinding = ContainResultBinding(param.Cast().Name().StringValue())) {
                    auto [txId, resId] = *maybeResultBinding;
                    if (auto result = GetResult(txId, resId)) {
                        auto index = FromString<ui32>(key.Cast<TCoNth>().Index());
                        Y_ENSURE(index < result->Size());
                        res = DescribeValue((*result)[index]);
                    }
                }
            } else if (auto literal = key.Maybe<TCoUuid>()) {
                res = NUuid::UuidBytesToString(literal.Cast().Literal().StringValue());
            } else if (auto literal = key.Maybe<TCoDataCtor>()) {
                res = literal.Cast().Literal().StringValue();
            } else if (auto literal = key.Maybe<TCoPgConst>()) {
                res = literal.Cast().Value().StringValue();
            } else if (auto literal = key.Maybe<TCoNothing>()) {
                res = TString("null");
            }

            return res.empty()? "«»" : res;
        };

        /* Collect info about scan range */
        struct TKeyPartRange {
            std::optional<std::string> From{};
            std::optional<std::string> To{};
            std::string ColumnName{};
        };
        auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, tablePath);
        op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : tablePath;
        op.Properties["Path"] = tablePath;
        planNode.NodeInfo["Tables"].AppendValue(op.Properties["Table"]);
        TVector<TKeyPartRange> scanRangeDescr(tableData.Metadata->KeyColumnNames.size());

        auto maybeFromKey = range.From().Maybe<TKqlKeyTuple>();
        auto maybeToKey = range.To().Maybe<TKqlKeyTuple>();
        if (maybeFromKey && maybeToKey) {
            auto fromKey = maybeFromKey.Cast();
            auto toKey = maybeToKey.Cast();

            for (ui32 i = 0; i < fromKey.ArgCount(); ++i) {
                scanRangeDescr[i].From = describeBoundary(fromKey.Arg(i));
            }
            for (ui32 i = 0; i < toKey.ArgCount(); ++i) {
                scanRangeDescr[i].To = describeBoundary(toKey.Arg(i));
            }
            for (ui32 i = 0; i < scanRangeDescr.size(); ++i) {
                scanRangeDescr[i].ColumnName = tableData.Metadata->KeyColumnNames[i];
            }

            TString leftParen = range.From().Maybe<TKqlKeyInc>().IsValid() ? "[" : "(";
            TString rightParen = range.To().Maybe<TKqlKeyInc>().IsValid() ? "]" : ")";
            bool hasRangeScans = false;
            auto& ranges = op.Properties["ReadRange"];
            for (const auto& keyPartRange : scanRangeDescr) {
                TStringBuilder rangeDescr;

                if (keyPartRange.From == keyPartRange.To) {
                    if (!keyPartRange.From.has_value()) {
                        rangeDescr << keyPartRange.ColumnName << " (-∞, +∞)";
                        readInfo.ScanBy.push_back(rangeDescr);
                    } else {
                        rangeDescr << keyPartRange.ColumnName
                                   << " (" << RemoveForbiddenChars(*keyPartRange.From) << ")";
                        readInfo.LookupBy.push_back(rangeDescr);
                    }
                } else {
                    rangeDescr << keyPartRange.ColumnName << " "
                               << (!keyPartRange.From.has_value() ? "(" : leftParen)
                               << (!keyPartRange.From.has_value() ? "-∞" : RemoveForbiddenChars(*keyPartRange.From)) << ", "
                               << (!keyPartRange.To.has_value() ? "+∞" : RemoveForbiddenChars(*keyPartRange.To))
                               << (!keyPartRange.To.has_value() ? ")" : rightParen);
                    readInfo.ScanBy.push_back(rangeDescr);
                    hasRangeScans = true;
                }

                ranges.AppendValue(rangeDescr);
            }

            if (readInfo.LookupBy.size() > 0) {
                bool isFullPk = readInfo.LookupBy.size() == tableData.Metadata->KeyColumnNames.size();
                readInfo.Type = isFullPk ? EPlanTableReadType::Lookup : EPlanTableReadType::Scan;
            } else {
                readInfo.Type = hasRangeScans ? EPlanTableReadType::Scan : EPlanTableReadType::FullScan;
            }
        }

        auto& columns = op.Properties["ReadColumns"];
        for (auto const& col : read.Columns()) {
            readInfo.Columns.emplace_back(TString(col.Value()));
            columns.AppendValue(col.Value());
        }

        if constexpr (std::is_same_v<TKqpReadRangesSourceSettings, TReadTableNode>) {
            AddReadTableSettings(op, read.Settings(), readInfo);
        } else {
            AddReadTableSettings(op, read, readInfo);
        }

        SerializerCtx.Tables[tablePath].Reads.push_back(readInfo);

        AddOptimizerEstimates(op, read);

        auto readName = GetNameByReadType(readInfo.Type);
        op.Properties["Name"] = readName;
        ui32 operatorId = AddOperator(planNode, readName, std::move(op));

        return operatorId;
    }

    template <typename TReadTableSettings>
    void AddReadTableSettings(
        TOperator& op,
        const TReadTableSettings& readTableSettings,
        TTableRead& readInfo
    ) {
        auto settings = NYql::TKqpReadTableSettings::Parse(readTableSettings);
        if (settings.ItemsLimit && !readInfo.Limit) {
            auto limit = GetExprStr(TExprBase(settings.ItemsLimit), false);
            if (auto maybeResultBinding = ContainResultBinding(limit)) {
                const auto [txId, resId] = *maybeResultBinding;
                if (auto result = GetResult(txId, resId)) {
                    limit = result->GetDataText();
                }
            }

            readInfo.Limit = limit;
            op.Properties["ReadLimit"] = limit;
        }
        readInfo.SetSorting(settings.GetSorting());
        if (settings.RequireSortedRead) {
            op.Properties["RequireSortedRead"] = true;
        }
        if (settings.GetSorting() == ERequestSorting::DESC) {
            op.Properties["Reverse"] = true;
        } else if (settings.GetSorting() == ERequestSorting::ASC) {
            op.Properties["Reverse"] = false;
        }

        if (settings.SequentialInFlight) {
            op.Properties["Scan"] = "Sequential";
        } else {
            op.Properties["Scan"] = "Parallel";
        }
    }


private:
    TSerializerCtx& SerializerCtx;
    const ui32 TxId;
    const TKqpPhysicalTx& Tx;
    const NKqpProto::TKqpPhyTx& TxProto;

    TMap<ui32, TQueryPlanNode> QueryPlanNodes;
    TNodeSet VisitedStages;
    std::unordered_map<TArgContext, std::variant<ui32, TQueryPlanNode*>, TArgContext::HashFunction> LambdaInputs;
    TArgContext CurrentArgContext;
};

void WriteCommonTablesInfo(NJsonWriter::TBuf& writer, TMap<TString, TTableInfo>& tables) {

    for (auto& pair : tables) {
        auto& info = pair.second;

        std::sort(info.Reads.begin(), info.Reads.end(), [](const auto& l, const auto& r) {
            return std::tie(l.Type, l.LookupBy, l.ScanBy, l.Limit, l.Columns) <
                   std::tie(r.Type, r.LookupBy, r.ScanBy, r.Limit, r.Columns);
        });

        std::sort(info.Writes.begin(), info.Writes.end(), [](const auto& l, const auto& r) {
            return std::tie(l.Type, l.Keys, l.Columns) <
                   std::tie(r.Type, r.Keys, r.Columns);
        });
    }

    writer.BeginList();

    for (auto& pair : tables) {
        auto& name = pair.first;
        auto& table = pair.second;

        writer.BeginObject();
        writer.WriteKey("name").WriteString(name);

        if (!table.Reads.empty()) {
            writer.WriteKey("reads");
            writer.BeginList();

            for (auto& read : table.Reads) {
                writer.BeginObject();
                writer.WriteKey("type").WriteString(ToString(read.Type));

                if (!read.LookupBy.empty()) {
                    writer.WriteKey("lookup_by");
                    writer.BeginList();
                    for (auto& column : read.LookupBy) {
                        writer.WriteString(column);
                    }
                    writer.EndList();
                }

                if (!read.ScanBy.empty()) {
                    writer.WriteKey("scan_by");
                    writer.BeginList();
                    for (auto& column : read.ScanBy) {
                        writer.WriteString(column);
                    }
                    writer.EndList();
                }

                if (read.Limit) {
                    writer.WriteKey("limit").WriteString(*read.Limit);
                }
                if (read.IsReverse()) {
                    writer.WriteKey("reverse").WriteBool(true);
                }

                if (!read.Columns.empty()) {
                    writer.WriteKey("columns");
                    writer.BeginList();
                    for (auto& column : read.Columns) {
                        writer.WriteString(column);
                    }
                    writer.EndList();
                }

                writer.EndObject();
            }

            writer.EndList();
        }

        if (!table.Writes.empty()) {
            writer.WriteKey("writes");
            writer.BeginList();

            for (auto& write : table.Writes) {
                writer.BeginObject();
                writer.WriteKey("type").WriteString(ToString(write.Type));

                if (!write.Keys.empty()) {
                    writer.WriteKey("key");
                    writer.BeginList();
                    for (auto& column : write.Keys) {
                        writer.WriteString(column);
                    }
                    writer.EndList();
                }

                if (!write.Columns.empty()) {
                    writer.WriteKey("columns");
                    writer.BeginList();
                    for (auto& column : write.Columns) {
                        writer.WriteString(column);
                    }
                    writer.EndList();
                }

                writer.EndObject();
            }

            writer.EndList();
        }

        writer.EndObject();
    }

    writer.EndList();

}

NJson::TJsonValue MakeOptimizerStats(const TIntrusivePtr<NOpt::TKqpOptimizeContext>& optCtx) {
    const auto& cboStats = optCtx->CBOStats;

    NJson::TJsonValue optimizerStats(NJson::EJsonValueType::JSON_MAP);
    optimizerStats["JoinsCount"] = static_cast<ui64>(optCtx->JoinsCount);
    optimizerStats["EquiJoinsCount"] = static_cast<ui64>(optCtx->EquiJoinsCount);
    optimizerStats["CBOTreesTotal"] = cboStats.TreesTotal;
    optimizerStats["CBOTreesOptimized"] = cboStats.TreesOptimized;
    return optimizerStats;
}

} // anonymous namespace

// TODO(sk): check prepared statements params in read ranges
// TODO(sk): check params from correlated subqueries // lookup join
void PhyQuerySetTxPlans(NKqpProto::TKqpPhyQuery& queryProto, const TKqpPhysicalQuery& query,
    const NYql::NNodes::TKqpPhysicalQuery& peepHoleOptimizedQuery,
    TVector<TVector<NKikimrMiniKQL::TResult>> pureTxResults, TExprContext& ctx, const TString& database,
    const TString& cluster, const TIntrusivePtr<NYql::TKikimrTablesData> tablesData, TKikimrConfiguration::TPtr config,
    TTypeAnnotationContext& typeCtx, TIntrusivePtr<NOpt::TKqpOptimizeContext> optCtx)
{

    YQL_CVLOG(NYql::NLog::ELevel::TRACE, NYql::NLog::EComponent::Core) << "JSON_PLAN: PhyQuerySetTxPlans";
    TSerializerCtx serializerCtx(ctx, database, cluster, tablesData, config, query.Transactions().Size(), std::move(pureTxResults), typeCtx, optCtx);

    /* bindingName -> stage */
    auto collectBindings = [&serializerCtx, &query] (auto id, const auto& phase) {
        for (const auto& param: phase.ParamBindings()) {
            if (auto maybeResultBinding = param.Binding().template Maybe<TKqpTxResultBinding>()) {
                auto resultBinding = maybeResultBinding.Cast();
                auto txId = FromString<ui32>(resultBinding.TxIndex());
                auto resultId = FromString<ui32>(resultBinding.ResultIndex());
                auto result = query.Transactions().Item(txId).Results().Item(resultId);
                if (auto maybeConnection = result.template Maybe<TDqConnection>()) {
                    auto stage = maybeConnection.Cast().Output().Stage();
                    serializerCtx.BindNameToStage[param.Name().StringValue()] = stage.Ptr();
                    serializerCtx.PrecomputePhases.insert(txId);
                    serializerCtx.ParamBindings[id].push_back({txId, resultId});
                }
            }
        }
    };

    ui32 id = 0;
    for (const auto& tx: query.Transactions()) {
        collectBindings(id++, tx);
    }

    auto setPlan = [&serializerCtx](auto txId, const auto& tx, auto& txProto) {
        // Unsafe truncate runs in a separate executer and has no DQ execution plan.
        if (GetTableTruncateSettings(tx)) {
            return;
        }

        NJsonWriter::TBuf txWriter;
        txWriter.SetIndentSpaces(2);
        TxPlanSerializer txPlanSerializer(serializerCtx, txId, tx, txProto);
        if (serializerCtx.PureTxResults.at(txId).empty()) {
            txPlanSerializer.Serialize();
            txPlanSerializer.WriteToJson(txWriter);
            txProto.SetPlan(txWriter.Str());
        }
    };

    id = 0;
    for (ui32 txId = 0; txId < query.Transactions().Size(); ++txId) {
        if (txId < peepHoleOptimizedQuery.Transactions().Size()) {
            VisitExpr(peepHoleOptimizedQuery.Transactions().Item(txId).Ref(),
                [&serializerCtx](const TExprNode& node) {
                    if (auto maybeStage = TMaybeNode<TDqPhyStage>(&node)) {
                        auto stageGuid = NDq::TDqStageSettings::Parse(maybeStage.Cast()).Id;
                        serializerCtx.OptimizedStages[stageGuid] = maybeStage.Raw();
                    }
                    return true;
                }
            );
        }
        setPlan(id++, query.Transactions().Item(txId), (*queryProto.MutableTransactions())[txId]);
        serializerCtx.OptimizedStages.clear();
    }

    TVector<const TString> txPlans;
    txPlans.reserve(queryProto.GetTransactions().size());
    for (const auto& phyTx: queryProto.GetTransactions()) {
        txPlans.emplace_back(phyTx.GetPlan());
    }

    TString queryStats = "";
    if (optCtx && optCtx->UserRequestContext && optCtx->UserRequestContext->PoolId) {
        NJsonWriter::TBuf writer;
        writer.BeginObject();
        writer.WriteKey("ResourcePoolId").WriteString(optCtx->UserRequestContext->PoolId);
        writer.EndObject();

        queryStats = writer.Str();
    }

    NJsonWriter::TBuf writer;
    writer.SetIndentSpaces(2);
    WriteCommonTablesInfo(writer, serializerCtx.Tables);

    queryProto.SetQueryPlan(SerializeTxPlans(txPlans, writer.Str(), queryStats, optCtx ? MakeOptimizerStats(optCtx) : NJson::TJsonValue()));
}

} // namespace NKikimr::NKqp
