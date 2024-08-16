#include "kqp_query_plan.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/protos/ssa.pb.h>
#include <ydb/public/lib/value/value.h>

#include <ydb/library/yql/ast/yql_ast_escaping.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>
#include <ydb/library/yql/utils/plan/plan_utils.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/queue.h>
#include <util/string/strip.h>
#include <util/string/vector.h>

#include <unordered_map>
#include <regex>

namespace NKikimr {
namespace NKqp {

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

struct TTableRead {
    EPlanTableReadType Type = EPlanTableReadType::Unspecified;
    TVector<TString> LookupBy;
    TVector<TString> ScanBy;
    TVector<TString> Columns;
    TMaybe<TString> Limit;
    bool Reverse = false;
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
    TSerializerCtx(TExprContext& exprCtx, const TString& cluster,
        const TIntrusivePtr<NYql::TKikimrTablesData> tablesData,
        const TKikimrConfiguration::TPtr config, ui32 txCount,
        TVector<TVector<NKikimrMiniKQL::TResult>> pureTxResults,
        TTypeAnnotationContext& typeCtx)
        : ExprCtx(exprCtx)
        , Cluster(cluster)
        , TablesData(tablesData)
        , Config(config)
        , TxCount(txCount)
        , PureTxResults(std::move(pureTxResults))
        , TypeCtx(typeCtx)
    {}

    TMap<TString, TTableInfo> Tables;
    TMap<TString, TExprNode::TPtr> BindNameToStage;
    TMap<TString, ui32> StageGuidToId;
    THashMap<ui32, TVector<std::pair<ui32, ui32>>> ParamBindings;
    THashSet<ui32> PrecomputePhases;
    ui32 PlanNodeId = 0;

    const TExprContext& ExprCtx;
    const TString& Cluster;
    const TIntrusivePtr<NYql::TKikimrTablesData> TablesData;
    const TKikimrConfiguration::TPtr Config;
    const ui32 TxCount;
    TVector<TVector<NKikimrMiniKQL::TResult>> PureTxResults;
    TTypeAnnotationContext& TypeCtx;
};

TString GetExprStr(const TExprBase& scalar, bool quoteStr = true) {
    if (auto maybeData = scalar.Maybe<TCoDataCtor>()) {
        auto literal = TString(maybeData.Cast().Literal());
        CollapseText(literal, 32);

        if (quoteStr) {
            return TStringBuilder() << '"' << literal << '"';
        } else {
            return literal;
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

            if (res.Maybe<TDqCnResult>()) {
                Visit(res.Cast<TDqCnResult>().Output().Stage(), planNode);
            } else if (res.Maybe<TDqCnValue>()) {
                Visit(res.Cast<TDqCnValue>().Output().Stage(), planNode);
            } else {
                Y_ENSURE(false, res.Ref().Content());
            }
        }

        for (const auto& stage: Tx.Stages()) {
            TDqStageBase stageBase = stage.Cast<TDqStageBase>();
            if (stageBase.Program().Body().Maybe<TKqpEffects>()) {
                auto& planNode = AddPlanNode(phaseNode);
                planNode.TypeName = "Effect";
                Visit(TExprBase(stage), planNode);
            } else if (stageBase.Outputs()) { // Sink
                auto& planNode = AddPlanNode(phaseNode);
                planNode.TypeName = "Sink";
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
        THashSet<ui32> Plans;
        const NKqpProto::TKqpPhyStage* StageProto;
    };

    void WritePlanNodeToJson(const TQueryPlanNode& planNode, NJsonWriter::TBuf& writer) const {
        writer.BeginObject();

        writer.WriteKey("PlanNodeId").WriteInt(planNode.NodeId);
        writer.WriteKey("Node Type").WriteString(planNode.TypeName);
        writer.WriteKey("StageGuid").WriteString(planNode.Guid);

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
            writer.WriteJsonValue(&value, true);
        }

        if (!planNode.Operators.empty()) {
            writer.WriteKey("Operators");
            writer.BeginList();

            for (auto& op : planNode.Operators) {
                writer.BeginObject();
                for (const auto& [key, value] : op.Properties) {
                    writer.WriteKey(key);
                    writer.WriteJsonValue(&value, true);
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
        if (!planNode.TypeName.Empty()) {
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
        planNode.Type = EPlanNodeType::Connection;

        if (connection.Maybe<TDqCnUnionAll>()) {
            planNode.TypeName = "UnionAll";
        } else if (connection.Maybe<TDqCnBroadcast>()) {
            planNode.TypeName = "Broadcast";
        } else if (connection.Maybe<TDqCnMap>()) {
            planNode.TypeName = "Map";
        } else if (auto hashShuffle = connection.Maybe<TDqCnHashShuffle>()) {
            planNode.TypeName = "HashShuffle";
            auto& keyColumns = planNode.NodeInfo["KeyColumns"];
            for (const auto& column : hashShuffle.Cast().KeyColumns()) {
                keyColumns.AppendValue(TString(column.Value()));
            }
        } else if (auto merge = connection.Maybe<TDqCnMerge>()) {
            planNode.TypeName = "Merge";
            auto& sortColumns = planNode.NodeInfo["SortColumns"];
            for (const auto& sortColumn : merge.Cast().SortColumns()) {
                TStringBuilder sortColumnDesc;
                sortColumnDesc << sortColumn.Column().Value() << " ("
                    << sortColumn.SortDirection().Value() << ")";

                sortColumns.AppendValue(sortColumnDesc);
            }
        } else if (auto maybeTableLookup = connection.Maybe<TKqpCnStreamLookup>()) {
            auto tableLookup = maybeTableLookup.Cast();

            TTableRead readInfo;
            readInfo.Type = EPlanTableReadType::Lookup;
            TString table(tableLookup.Table().Path().Value());
            auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, table);
            planNode.NodeInfo["Table"] = tableData.RelativePath ? *tableData.RelativePath : table;

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
                lookupKeyColumnsStruct = inputTupleType->GetItems()[0]->Cast<TStructExprType>();
            }

            YQL_ENSURE(lookupKeyColumnsStruct);
            readInfo.LookupBy.reserve(lookupKeyColumnsStruct->GetItems().size());
            auto& lookupKeyColumns = planNode.NodeInfo["LookupKeyColumns"];
            for (const auto keyColumn : lookupKeyColumnsStruct->GetItems()) {
                lookupKeyColumns.AppendValue(keyColumn->GetName());
                readInfo.LookupBy.push_back(TString(keyColumn->GetName()));
            }

            SerializerCtx.Tables[table].Reads.push_back(std::move(readInfo));
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

    void Visit(const TKqpReadRangesSourceSettings& sourceSettings, TQueryPlanNode& planNode) {
        if (sourceSettings.RangesExpr().Maybe<TKqlKeyRange>()) {
            auto table = TString(sourceSettings.Table().Path());
            auto range = sourceSettings.RangesExpr().Cast<TKqlKeyRange>();

            TOperator op;
            TTableRead readInfo;

            auto describeBoundary = [this](const TExprBase& key) {
                if (auto param = key.Maybe<TCoParameter>()) {
                   return param.Cast().Name().StringValue();
                }

                if (auto param = key.Maybe<TCoNth>().Tuple().Maybe<TCoParameter>()) {
                    if (auto maybeResultBinding = ContainResultBinding(param.Cast().Name().StringValue())) {
                        auto [txId, resId] = *maybeResultBinding;
                        if (auto result = GetResult(txId, resId)) {
                            auto index = FromString<ui32>(key.Cast<TCoNth>().Index());
                            Y_ENSURE(index < result->Size());
                            return DescribeValue((*result)[index]);
                        }
                    }
                }

                if (auto literal = key.Maybe<TCoUuid>()) {
                    return NUuid::UuidBytesToString(literal.Cast().Literal().StringValue());
                }

                if (auto literal = key.Maybe<TCoDataCtor>()) {
                    return literal.Cast().Literal().StringValue();
                }

                if (auto literal = key.Maybe<TCoNothing>()) {
                    return TString("null");
                }

                return TString("n/a");
            };

            /* Collect info about scan range */
            struct TKeyPartRange {
                TString From;
                TString To;
                TString ColumnName;
            };
            auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, table);
            op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : table;
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
                        if (keyPartRange.From.Empty()) {
                            rangeDescr << keyPartRange.ColumnName << " (-∞, +∞)";
                            readInfo.ScanBy.push_back(rangeDescr);
                        } else {
                            rangeDescr << keyPartRange.ColumnName
                                       << " (" << keyPartRange.From << ")";
                            readInfo.LookupBy.push_back(rangeDescr);
                        }
                    } else {
                        rangeDescr << keyPartRange.ColumnName << " "
                                   << (keyPartRange.From.Empty() ? "(" : leftParen)
                                   << (keyPartRange.From.Empty() ? "-∞" : keyPartRange.From) << ", "
                                   << (keyPartRange.To.Empty() ? "+∞" : keyPartRange.To)
                                   << (keyPartRange.To.Empty() ? ")" : rightParen);
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
            for (auto const& col : sourceSettings.Columns()) {
                readInfo.Columns.emplace_back(TString(col.Value()));
                columns.AppendValue(col.Value());
            }

            auto settings = NYql::TKqpReadTableSettings::Parse(sourceSettings.Settings());
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
            if (settings.Reverse) {
                readInfo.Reverse = true;
                op.Properties["Reverse"] = true;
            }

            AddOptimizerEstimates(op, sourceSettings);

            SerializerCtx.Tables[table].Reads.push_back(readInfo);

            auto readName = GetNameByReadType(readInfo.Type);
            op.Properties["Name"] = readName;
            AddOperator(planNode, readName, std::move(op));
        } else {
            const auto table = TString(sourceSettings.Table().Path());
            const auto explainPrompt = TKqpReadTableExplainPrompt::Parse(sourceSettings.ExplainPrompt().Cast());

            TTableRead readInfo;
            TOperator op;

            auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, table);
            op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : table;
            planNode.NodeInfo["Tables"].AppendValue(op.Properties["Table"]);

            auto rangesDesc = NPlanUtils::PrettyExprStr(sourceSettings.RangesExpr());
            if (rangesDesc == "Void" || explainPrompt.UsedKeyColumns.empty()) {
                readInfo.Type = EPlanTableReadType::FullScan;

                auto& ranges = op.Properties["ReadRanges"];
                for (const auto& col : tableData.Metadata->KeyColumnNames) {
                    TStringBuilder rangeDesc;
                    rangeDesc << col << " (-∞, +∞)";
                    readInfo.ScanBy.push_back(rangeDesc);
                    ranges.AppendValue(rangeDesc);
                }
            } else if (auto maybeResultBinding = ContainResultBinding(rangesDesc)) {
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
                                << (from[colId].HaveValue() ? from[colId].GetSimpleValueText() : "-∞") << ", "
                                << (to[colId].HaveValue() ? to[colId].GetSimpleValueText() : "+∞")
                                << (to[keyColumns.size()].GetDataText() == "1" ? "]" : ")");

                            readInfo.ScanBy.push_back(rangeDesc);
                            op.Properties["ReadRanges"].AppendValue(rangeDesc);
                        }
                    }
                } else {
                    op.Properties["ReadRanges"] = rangesDesc;
                }
            } else {
                Y_ENSURE(false, rangesDesc);
            }

            if (!explainPrompt.UsedKeyColumns.empty()) {
                auto& usedColumns = op.Properties["ReadRangesKeys"];
                for (const auto& col : explainPrompt.UsedKeyColumns) {
                    usedColumns.AppendValue(col);
                }
            }

            if (explainPrompt.ExpectedMaxRanges) {
                op.Properties["ReadRangesExpectedSize"] = ToString(*explainPrompt.ExpectedMaxRanges);
            }

            op.Properties["ReadRangesPointPrefixLen"] = ToString(explainPrompt.PointPrefixLen);

            auto& columns = op.Properties["ReadColumns"];
            for (const auto& col : sourceSettings.Columns()) {
                readInfo.Columns.emplace_back(TString(col.Value()));
                columns.AppendValue(col.Value());
            }

            auto settings = NYql::TKqpReadTableSettings::Parse(sourceSettings.Settings());
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
            if (settings.Reverse) {
                readInfo.Reverse = true;
                op.Properties["Reverse"] = true;
            }

            AddOptimizerEstimates(op, sourceSettings);

            auto readName = GetNameByReadType(readInfo.Type);
            op.Properties["Name"] = readName;
            AddOperator(planNode, readName, std::move(op));

            SerializerCtx.Tables[table].Reads.push_back(std::move(readInfo));
        }
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

    void Visit(const TDqSource& source, TQueryPlanNode& stagePlanNode) {
        // YDB sources
        if (auto settings = source.Settings().Maybe<TKqpReadRangesSourceSettings>(); settings.IsValid()) {
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

        if (dqIntegration) {
            dqIntegration->FillSourcePlanProperties(source, op.Properties);
        }

        AddOperator(stagePlanNode, "Source", op);
    }

    void Visit(const TDqSink& sink, TQueryPlanNode& stagePlanNode) {
        // Federated providers
        TOperator op;
        TCoDataSink dataSink = sink.DataSink().Cast<TCoDataSink>();
        const TString dataSinkCategory = dataSink.Category().StringValue();
        IDqIntegration* dqIntegration = nullptr;

        {
            auto providerIt = SerializerCtx.TypeCtx.DataSinkMap.find(dataSinkCategory);
            if (providerIt != SerializerCtx.TypeCtx.DataSinkMap.end()) {
                dqIntegration = providerIt->second->GetDqIntegration();
            }
        }

        // Common settings that can be overwritten by provider
        op.Properties["SinkType"] = dataSinkCategory;
        if (auto cluster = TryGetCluster(dataSink)) {
            TString dataSource = RemovePathPrefix(std::move(*cluster));
            op.Properties["ExternalDataSource"] = dataSource;
            op.Properties["Name"] = TStringBuilder() << "Write " << dataSource;
        } else {
            op.Properties["Name"] = "Write to external data source";
        }

        if (dqIntegration) {
            dqIntegration->FillSinkPlanProperties(sink, op.Properties);
        }

        AddOperator(stagePlanNode, "Sink", op);
    }

    void Visit(const TExprBase& expr, TQueryPlanNode& planNode) {
        if (expr.Maybe<TDqPhyStage>()) {
            auto stageGuid = NDq::TDqStageSettings::Parse(expr.Cast<TDqPhyStage>()).Id;

            if (VisitedStages.find(expr.Raw()) != VisitedStages.end()) {
                auto stageId = SerializerCtx.StageGuidToId[stageGuid];
                Y_ENSURE(QueryPlanNodes.contains(stageId));
                auto& commonNode = QueryPlanNodes[stageId];

                auto& parentNode = GetParent(stageId);
                parentNode.Plans.erase(stageId);

                auto& cteNode = AddPlanNode(QueryPlanNodes.begin()->second);
                cteNode.Plans.insert(stageId);
                cteNode.TypeName = TStringBuilder() << commonNode.TypeName;
                cteNode.CteName = cteNode.TypeName;

                parentNode.CteRefName = *cteNode.CteName;
                planNode.CteRefName = *cteNode.CteName;

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
                    Visit(source.Cast(), inputSourceNode);
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
                for (const auto& arg : expr.Cast<TDqStageBase>().Program().Args()) {
                    LambdaInputs[CurrentArgContext.AddArg(arg.Ptr().Get())] = inputIds[0];
                    inputIds.erase(inputIds.begin());
                }
            }

            auto node = expr.Cast<TDqStageBase>().Program().Body().Ptr();
            Visit(node, stagePlanNode);

            CurrentArgContext.stack.pop_back();

            /* is that collect stage? */
            if (stagePlanNode.TypeName.Empty()) {
                if (expr.Cast<TDqStageBase>().Program().Body().Maybe<TCoArgument>()) {
                    stagePlanNode.TypeName = "Collect";
                } else {
                    stagePlanNode.TypeName = "Stage";
                }
            }

            if (auto outputs = expr.Cast<TDqStageBase>().Outputs()) {
                for (auto output : outputs.Cast()) {
                    if (auto sink = output.Maybe<TDqSink>()) {
                        Visit(sink.Cast(), stagePlanNode);
                    }
                }
            }
        } else {
            Visit(expr.Ptr(),  planNode);

            if (planNode.TypeName.Empty()) {
                planNode.TypeName = "Stage";
            }
        }
    }

    TVector<std::variant<ui32, TArgContext>> Visit(TExprNode::TPtr node, TQueryPlanNode& planNode) {
        TMaybe<std::variant<ui32, TArgContext>> operatorId;

        if (auto maybeRead = TMaybeNode<TKqlReadTableBase>(node)) {
            operatorId = Visit(maybeRead.Cast(), planNode);
        } else if (auto maybeReadRanges = TMaybeNode<TKqlReadTableRangesBase>(node)) {
            operatorId = Visit(maybeReadRanges.Cast(), planNode);
        } else if (auto maybeLookup = TMaybeNode<TKqlLookupTableBase>(node)) {
            operatorId = Visit(maybeLookup.Cast(), planNode);
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
        } else if (auto maybeIter = TMaybeNode<TCoIterator>(node)) {
            operatorId = Visit(maybeIter.Cast(), planNode);
        } else if (auto maybePartitionByKey = TMaybeNode<TCoPartitionByKey>(node)) {
            operatorId = Visit(maybePartitionByKey.Cast(), planNode);
        } else if (auto maybeUpsert = TMaybeNode<TKqpUpsertRows>(node)) {
            operatorId = Visit(maybeUpsert.Cast(), planNode);
        } else if (auto maybeDelete = TMaybeNode<TKqpDeleteRows>(node)) {
            operatorId = Visit(maybeDelete.Cast(), planNode);
        } else if (auto maybeArg = TMaybeNode<TCoArgument>(node)) {
            return {CurrentArgContext.AddArg(node.Get())};
        }

        TVector<std::variant<ui32, TArgContext>> inputIds;
        if (auto maybeEffects = TMaybeNode<TKqpEffects>(node)) {
            for (const auto& effect : maybeEffects.Cast().Args()) {
                auto ids = Visit(effect, planNode);
                inputIds.insert(inputIds.end(), ids.begin(), ids.end());
            }
        } else {
            if (TMaybeNode<TCoFlatMapBase>(node)) {

                auto flatMap = TExprBase(node).Cast<TCoFlatMapBase>();
                auto flatMapInputs = Visit(flatMap, planNode);

                inputIds.insert(inputIds.end(), flatMapInputs.begin(), flatMapInputs.end());

                auto flatMapLambdaInputs = Visit(flatMap.Lambda().Body().Ptr(), planNode);
                inputIds.insert(inputIds.end(), flatMapLambdaInputs.begin(), flatMapLambdaInputs.end());

            }
            else {
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

    TVector<std::variant<ui32, TArgContext>> Visit(const TCoFlatMapBase& flatMap, TQueryPlanNode& planNode) {
        auto flatMapInputs = Visit(flatMap.Input().Ptr(), planNode);

        if (flatMapInputs.size() >= 1) {
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

    std::variant<ui32, TArgContext> Visit(const TCoCondense1& /*condense*/, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Aggregate";

        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoCondense& /*condense*/, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Aggregate";

        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoCombineCore& combiner, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Aggregate";
        op.Properties["GroupBy"] = NPlanUtils::PrettyExprStr(combiner.KeyExtractor());
        op.Properties["Aggregation"] = NPlanUtils::PrettyExprStr(combiner.UpdateHandler());

        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoSort& sort, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Sort";
        op.Properties["SortBy"] = NPlanUtils::PrettyExprStr(sort.KeySelectorLambda());

        return AddOperator(planNode, "Sort", std::move(op));
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

        return AddOperator(planNode, "Aggregate", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TKqpUpsertRows& upsert, TQueryPlanNode& planNode) {
        const auto table = upsert.Table().Path().StringValue();

        TOperator op;
        op.Properties["Name"] = "Upsert";
        auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, table);
        op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : table;

        TTableWrite writeInfo;
        writeInfo.Type = EPlanTableWriteType::MultiUpsert;
        for (const auto& column : upsert.Columns()) {
            writeInfo.Columns.push_back(TString(column.Value()));
        }

        SerializerCtx.Tables[table].Writes.push_back(writeInfo);
        planNode.NodeInfo["Tables"].AppendValue(op.Properties["Table"]);
        return AddOperator(planNode, "Upsert", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TKqpDeleteRows& del, TQueryPlanNode& planNode) {
        const auto table = del.Table().Path().StringValue();

        TOperator op;
        op.Properties["Name"] = "Delete";
        auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, table);
        op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : table;

        TTableWrite writeInfo;
        writeInfo.Type = EPlanTableWriteType::MultiErase;

        SerializerCtx.Tables[table].Writes.push_back(writeInfo);
        planNode.NodeInfo["Tables"].AppendValue(op.Properties["Table"]);
        return AddOperator(planNode, "Delete", std::move(op));
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
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (MapJoin)";

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumns(), join.RightKeysColumns());

        AddOptimizerEstimates(op, join);

        auto operatorId = AddOperator(planNode, name, std::move(op));

        Visit(flatMap, planNode);

        return operatorId;
    }

    std::variant<ui32, TArgContext> Visit(const TCoMapJoinCore& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (MapJoin)";

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumns(), join.RightKeysColumns());


        AddOptimizerEstimates(op, join);

        return AddOperator(planNode, name, std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoFlatMapBase& flatMap, const TCoJoinDict& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (JoinDict)";

        TOperator op;
        op.Properties["Name"] = name;

        AddOptimizerEstimates(op, join);

        auto operatorId = AddOperator(planNode, name, std::move(op));

        Visit(flatMap, planNode);

        return operatorId;
    }

    std::variant<ui32, TArgContext> Visit(const TCoJoinDict& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (JoinDict)";

        TOperator op;
        op.Properties["Name"] = name;

        AddOptimizerEstimates(op, join);

        return AddOperator(planNode, name, std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TCoFlatMapBase& flatMap, const TCoGraceJoinCore& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (Grace)";

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumns(), join.RightKeysColumns());

        auto operatorId = AddOperator(planNode, name, std::move(op));

        AddOptimizerEstimates(op, join);

        Visit(flatMap, planNode);

        return operatorId;
    }

    std::variant<ui32, TArgContext> Visit(const TCoGraceJoinCore& join, TQueryPlanNode& planNode) {
        const auto name = TStringBuilder() << join.JoinKind().Value() << "Join (Grace)";

        TOperator op;
        op.Properties["Name"] = name;
        op.Properties["Condition"] = MakeJoinConditionString(join.LeftKeysColumns(), join.RightKeysColumns());


        AddOptimizerEstimates(op, join);

        return AddOperator(planNode, name, std::move(op));
    }

    void AddOptimizerEstimates(TOperator& op, const TExprBase& expr) {
        if (SerializerCtx.Config->CostBasedOptimizationLevel.Get().GetOrElse(TDqSettings::TDefault::CostBasedOptimizationLevel)==0) {
            return;
        }

        if (auto stats = SerializerCtx.TypeCtx.GetStats(expr.Raw())) {
            op.Properties["E-Rows"] = stats->Nrows;
            op.Properties["E-Cost"] = stats->Cost;
        }
        else {
            op.Properties["E-Rows"] = "No estimate";
            op.Properties["E-Cost"] = "No estimate";

        }
    }

    std::variant<ui32, TArgContext> Visit(const TCoFilterBase& filter, TQueryPlanNode& planNode) {
        TOperator op;
        op.Properties["Name"] = "Filter";

        auto pred = NPlanUtils::ExtractPredicate(filter.Lambda());
        op.Properties["Predicate"] = pred.Body;

        AddOptimizerEstimates(op, filter);

        if (filter.Limit()) {
            op.Properties["Limit"] = NPlanUtils::PrettyExprStr(filter.Limit().Cast());
        }

        return AddOperator(planNode, "Filter", std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TKqlLookupTableBase& lookup, TQueryPlanNode& planNode) {
        auto table = TString(lookup.Table().Path().Value());
        auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, table);

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
        op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : table;
        auto& columns = op.Properties["ReadColumns"];
        for (auto const& col : lookup.Columns()) {
            readInfo.Columns.push_back(TString(col.Value()));
            columns.AppendValue(col.Value());
        }

        AddOptimizerEstimates(op, lookup);

        SerializerCtx.Tables[table].Reads.push_back(readInfo);
        planNode.NodeInfo["Tables"].AppendValue(op.Properties["Table"]);
        return AddOperator(planNode, readName, std::move(op));
    }

    std::variant<ui32, TArgContext> Visit(const TKqlReadTableRangesBase& read, TQueryPlanNode& planNode) {
        const auto table = TString(read.Table().Path());
        const auto explainPrompt = TKqpReadTableExplainPrompt::Parse(read);

        TTableRead readInfo;
        TOperator op;

        auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, table);
        op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : table;
        planNode.NodeInfo["Tables"].AppendValue(op.Properties["Table"]);

        auto rangesDesc = NPlanUtils::PrettyExprStr(read.Ranges());
        if (rangesDesc == "Void" || explainPrompt.UsedKeyColumns.empty()) {
            readInfo.Type = EPlanTableReadType::FullScan;

            auto& ranges = op.Properties["ReadRanges"];
            for (const auto& col : tableData.Metadata->KeyColumnNames) {
                TStringBuilder rangeDesc;
                rangeDesc << col << " (-∞, +∞)";
                readInfo.ScanBy.push_back(rangeDesc);
                ranges.AppendValue(rangeDesc);
            }
        } else if (auto maybeResultBinding = ContainResultBinding(rangesDesc)) {
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
                            << (from[colId].HaveValue() ? from[colId].GetSimpleValueText() : "-∞") << ", "
                            << (to[colId].HaveValue() ? to[colId].GetSimpleValueText() : "+∞")
                            << (to[keyColumns.size()].GetDataText() == "1" ? "]" : ")");

                        readInfo.ScanBy.push_back(rangeDesc);
                        op.Properties["ReadRanges"].AppendValue(rangeDesc);
                    }
                }
            } else {
                op.Properties["ReadRanges"] = rangesDesc;
            }
        } else {
            Y_ENSURE(false, rangesDesc);
        }

        if (!explainPrompt.UsedKeyColumns.empty()) {
            auto& usedColumns = op.Properties["ReadRangesKeys"];
            for (const auto& col : explainPrompt.UsedKeyColumns) {
                usedColumns.AppendValue(col);
            }
        }

        if (explainPrompt.ExpectedMaxRanges) {
            op.Properties["ReadRangesExpectedSize"] = *explainPrompt.ExpectedMaxRanges;
        }

        auto& columns = op.Properties["ReadColumns"];
        for (const auto& col : read.Columns()) {
            readInfo.Columns.emplace_back(TString(col.Value()));
            columns.AppendValue(col.Value());
        }

        auto settings = NYql::TKqpReadTableSettings::Parse(read);
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
        if (settings.Reverse) {
            readInfo.Reverse = true;
            op.Properties["Reverse"] = true;
        }

        if (read.Maybe<TKqpReadOlapTableRangesBase>()) {
            op.Properties["SsaProgram"] = GetSsaProgramInJsonByTable(table, planNode.StageProto);
        }

        AddOptimizerEstimates(op, read);

        auto readName = GetNameByReadType(readInfo.Type);
        op.Properties["Name"] = readName;
        ui32 operatorId = AddOperator(planNode, readName, std::move(op));

        SerializerCtx.Tables[table].Reads.push_back(std::move(readInfo));
        return operatorId;
    }

    std::variant<ui32, TArgContext> Visit(const TKqlReadTableBase& read, TQueryPlanNode& planNode) {
        auto table = TString(read.Table().Path());
        auto range = read.Range();

        TOperator op;
        TTableRead readInfo;

        auto describeBoundary = [this](const TExprBase& key) {
            if (auto param = key.Maybe<TCoParameter>()) {
               return param.Cast().Name().StringValue();
            }

            if (auto param = key.Maybe<TCoNth>().Tuple().Maybe<TCoParameter>()) {
                if (auto maybeResultBinding = ContainResultBinding(param.Cast().Name().StringValue())) {
                    auto [txId, resId] = *maybeResultBinding;
                    if (auto result = GetResult(txId, resId)) {
                        auto index = FromString<ui32>(key.Cast<TCoNth>().Index());
                        Y_ENSURE(index < result->Size());
                        return DescribeValue((*result)[index]);
                    }
                }
            }

            if (auto literal = key.Maybe<TCoDataCtor>()) {
                return literal.Cast().Literal().StringValue();
            }

            if (auto literal = key.Maybe<TCoNothing>()) {
                return TString("null");
            }

            return TString("n/a");
        };

        /* Collect info about scan range */
        struct TKeyPartRange {
            TString From;
            TString To;
            TString ColumnName;
        };
        auto& tableData = SerializerCtx.TablesData->GetTable(SerializerCtx.Cluster, table);
        op.Properties["Table"] = tableData.RelativePath ? *tableData.RelativePath : table;
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
                    if (keyPartRange.From.Empty()) {
                        rangeDescr << keyPartRange.ColumnName << " (-∞, +∞)";
                        readInfo.ScanBy.push_back(rangeDescr);
                    } else {
                        rangeDescr << keyPartRange.ColumnName
                                   << " (" << keyPartRange.From << ")";
                        readInfo.LookupBy.push_back(rangeDescr);
                    }
                } else {
                    rangeDescr << keyPartRange.ColumnName << " "
                               << (keyPartRange.From.Empty() ? "(" : leftParen)
                               << (keyPartRange.From.Empty() ? "-∞" : keyPartRange.From) << ", "
                               << (keyPartRange.To.Empty() ? "+∞" : keyPartRange.To)
                               << (keyPartRange.To.Empty() ? ")" : rightParen);
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

        auto settings = NYql::TKqpReadTableSettings::Parse(read);
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
        if (settings.Reverse) {
            readInfo.Reverse = true;
            op.Properties["Reverse"] = true;
        }

        SerializerCtx.Tables[table].Reads.push_back(readInfo);

        AddOptimizerEstimates(op, read);

        auto readName = GetNameByReadType(readInfo.Type);
        op.Properties["Name"] = readName;
        ui32 operatorId = AddOperator(planNode, readName, std::move(op));

        return operatorId;
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

using ModifyFunction = std::function<void (NJson::TJsonValue& node)>;

void ModifyPlan(NJson::TJsonValue& plan, const ModifyFunction& modify) {
    modify(plan);

    auto& map = plan.GetMapSafe();
    if (map.contains("Plans")) {
        for (auto& subplan : map.at("Plans").GetArraySafe()) {
            ModifyPlan(subplan, modify);
        }
    }
}

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
                if (read.Reverse) {
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

template<typename T>
void SetNonZero(NJson::TJsonValue& node, const TStringBuf& name, T value) {
    if (value) {
        node[name] = value;
    }
}

TString SerializeTxPlans(const TVector<const TString>& txPlans, const TString commonPlanInfo = "") {
    NJsonWriter::TBuf writer;
    writer.SetIndentSpaces(2);

    writer.BeginObject();
    writer.WriteKey("meta");
    writer.BeginObject();
    writer.WriteKey("version").WriteString("0.2");
    writer.WriteKey("type").WriteString("query");
    writer.EndObject();

    if (!commonPlanInfo.Empty()) {
        NJson::TJsonValue commonPlanJson;
        NJson::ReadJsonTree(commonPlanInfo, &commonPlanJson, true);

        writer.WriteKey("tables");
        writer.WriteJsonValue(&commonPlanJson);
    }

    writer.WriteKey("Plan");
    writer.BeginObject();
    writer.WriteKey("Node Type").WriteString("Query");
    writer.WriteKey("PlanNodeType").WriteString("Query");
    writer.WriteKey("Plans");
    writer.BeginList();

    auto removeStageGuid = [](NJson::TJsonValue& node) {
        auto& map = node.GetMapSafe();
        if (map.contains("StageGuid")) {
            map.erase("StageGuid");
        }
    };

    for (auto txPlanIt = txPlans.rbegin(); txPlanIt != txPlans.rend(); ++txPlanIt) {
        if (txPlanIt->empty()) {
            continue;
        }

        NJson::TJsonValue txPlanJson;
        NJson::ReadJsonTree(*txPlanIt, &txPlanJson, true);

        for (auto& subplan : txPlanJson.GetMapSafe().at("Plans").GetArraySafe()) {
            ModifyPlan(subplan, removeStageGuid);
            writer.WriteJsonValue(&subplan, true);
        }
    }

    writer.EndList();
    writer.EndObject();
    writer.EndObject();

    return writer.Str();
}

} // namespace

// TODO(sk): check prepared statements params in read ranges
// TODO(sk): check params from correlated subqueries // lookup join
void PhyQuerySetTxPlans(NKqpProto::TKqpPhyQuery& queryProto, const TKqpPhysicalQuery& query,
    TVector<TVector<NKikimrMiniKQL::TResult>> pureTxResults, TExprContext& ctx, const TString& cluster,
    const TIntrusivePtr<NYql::TKikimrTablesData> tablesData, TKikimrConfiguration::TPtr config,
    TTypeAnnotationContext& typeCtx)
{
    TSerializerCtx serializerCtx(ctx, cluster, tablesData, config, query.Transactions().Size(), std::move(pureTxResults), typeCtx);

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
        setPlan(id++, query.Transactions().Item(txId), (*queryProto.MutableTransactions())[txId]);
    }

    TVector<const TString> txPlans;
    txPlans.reserve(queryProto.GetTransactions().size());
    for (const auto& phyTx: queryProto.GetTransactions()) {
        txPlans.emplace_back(phyTx.GetPlan());
    }

    NJsonWriter::TBuf writer;
    writer.SetIndentSpaces(2);
    WriteCommonTablesInfo(writer, serializerCtx.Tables);

    queryProto.SetQueryPlan(SerializeTxPlans(txPlans, writer.Str()));
}

void FillAggrStat(NJson::TJsonValue& node, const NYql::NDqProto::TDqStatsAggr& aggr, const TString& name) {
    auto min = aggr.GetMin();
    auto max = aggr.GetMax();
    auto sum = aggr.GetSum();
    if (min || max || sum) { // do not fill empty metrics
        auto& aggrStat = node.InsertValue(name, NJson::JSON_MAP);
        aggrStat["Min"] = min;
        aggrStat["Max"] = max;
        aggrStat["Sum"] = sum;
        aggrStat["Count"] = aggr.GetCnt();
    }
}

void FillAsyncAggrStat(NJson::TJsonValue& node, const NYql::NDqProto::TDqAsyncStatsAggr& asyncAggr) {
    if (asyncAggr.HasBytes()) {
        FillAggrStat(node, asyncAggr.GetBytes(), "Bytes");
    }
    if (asyncAggr.HasRows()) {
        FillAggrStat(node, asyncAggr.GetRows(), "Rows");
    }
    if (asyncAggr.HasChunks()) {
        FillAggrStat(node, asyncAggr.GetChunks(), "Chunks");
    }
    if (asyncAggr.HasSplits()) {
        FillAggrStat(node, asyncAggr.GetSplits(), "Splits");
    }

    if (asyncAggr.HasFirstMessageMs()) {
        FillAggrStat(node, asyncAggr.GetFirstMessageMs(), "FirstMessageMs");
    }
    if (asyncAggr.HasPauseMessageMs()) {
        FillAggrStat(node, asyncAggr.GetPauseMessageMs(), "PauseMessageMs");
    }
    if (asyncAggr.HasResumeMessageMs()) {
        FillAggrStat(node, asyncAggr.GetResumeMessageMs(), "ResumeMessageMs");
    }
    if (asyncAggr.HasLastMessageMs()) {
        FillAggrStat(node, asyncAggr.GetLastMessageMs(), "LastMessageMs");
    }

    if (asyncAggr.HasWaitTimeUs()) {
        FillAggrStat(node, asyncAggr.GetWaitTimeUs(), "WaitTimeUs");
    }
    if (asyncAggr.HasWaitPeriods()) {
        FillAggrStat(node, asyncAggr.GetWaitPeriods(), "WaitPeriods");
    }
    if (asyncAggr.HasActiveTimeUs()) {
        FillAggrStat(node, asyncAggr.GetActiveTimeUs(), "ActiveTimeUs");
    }
    if (asyncAggr.HasFirstMessageMs() && asyncAggr.HasLastMessageMs()) {
        auto firstMessageMs = asyncAggr.GetFirstMessageMs().GetMin();
        auto lastMessageMs = asyncAggr.GetLastMessageMs().GetMax();
        if (firstMessageMs && lastMessageMs > firstMessageMs) {
            auto& aggrStat = node.InsertValue("ActiveMessageMs", NJson::JSON_MAP);
            aggrStat["Min"] = firstMessageMs;
            aggrStat["Max"] = lastMessageMs;
            aggrStat["Count"] = asyncAggr.GetFirstMessageMs().GetCnt();
        }
    }
    if (asyncAggr.HasPauseMessageMs() && asyncAggr.HasResumeMessageMs()) {
        auto firstMessageMs = asyncAggr.GetPauseMessageMs().GetMin();
        auto lastMessageMs = asyncAggr.GetResumeMessageMs().GetMax();
        if (firstMessageMs && lastMessageMs > firstMessageMs) {
            auto& aggrStat = node.InsertValue("WaitMessageMs", NJson::JSON_MAP);
            aggrStat["Min"] = firstMessageMs;
            aggrStat["Max"] = lastMessageMs;
            aggrStat["Count"] = asyncAggr.GetPauseMessageMs().GetCnt();
        }
    }
}

TString AddExecStatsToTxPlan(const TString& txPlanJson, const NYql::NDqProto::TDqExecutionStats& stats) {
    if (txPlanJson.empty()) {
        return {};
    }

    THashMap<TProtoStringType, const NYql::NDqProto::TDqStageStats*> stages;
    THashMap<ui32, TString> stageIdToGuid;
    for (const auto& stage : stats.GetStages()) {
        stages[stage.GetStageGuid()] = &stage;
        stageIdToGuid[stage.GetStageId()] = stage.GetStageGuid();
    }

    NJson::TJsonValue root;
    NJson::ReadJsonTree(txPlanJson, &root, true);

    auto fillInputStats = [](NJson::TJsonValue& node, const NYql::NDqProto::TDqInputChannelStats& inputStats) {
        node["ChannelId"] = inputStats.GetChannelId();
        node["SrcStageId"] = inputStats.GetSrcStageId();

        SetNonZero(node, "Bytes", inputStats.GetPush().GetBytes());
        SetNonZero(node, "Rows", inputStats.GetPush().GetRows());

        SetNonZero(node, "WaitTimeUs", inputStats.GetPush().GetWaitTimeUs());
    };

    auto fillOutputStats = [](NJson::TJsonValue& node, const NYql::NDqProto::TDqOutputChannelStats& outputStats) {
        node["ChannelId"] = outputStats.GetChannelId();
        node["DstStageId"] = outputStats.GetDstStageId();

        SetNonZero(node, "Bytes", outputStats.GetPop().GetBytes());
        SetNonZero(node, "Rows", outputStats.GetPop().GetRows());

        SetNonZero(node, "WaitTimeUs", outputStats.GetPop().GetWaitTimeUs());
        SetNonZero(node, "SpilledBytes", outputStats.GetSpilledBytes());
    };

    auto fillTaskStats = [&](NJson::TJsonValue& node, const NYql::NDqProto::TDqTaskStats& taskStats) {
        node["TaskId"] = taskStats.GetTaskId();

        SetNonZero(node, "Host", taskStats.GetHostName());
        SetNonZero(node, "NodeId", taskStats.GetNodeId());
        SetNonZero(node, "InputRows", taskStats.GetInputRows());
        SetNonZero(node, "InputBytes", taskStats.GetInputBytes());
        SetNonZero(node, "OutputRows", taskStats.GetOutputRows());
        SetNonZero(node, "OutputBytes", taskStats.GetOutputBytes());
        SetNonZero(node, "ResultRows", taskStats.GetResultRows());
        SetNonZero(node, "ResultBytes", taskStats.GetResultBytes());
        SetNonZero(node, "IngressRows", taskStats.GetIngressRows());
        SetNonZero(node, "IngressBytes", taskStats.GetIngressBytes());
        SetNonZero(node, "EgressRows", taskStats.GetEgressRows());
        SetNonZero(node, "EgressBytes", taskStats.GetEgressBytes());

        SetNonZero(node, "StartTimeMs", taskStats.GetStartTimeMs());   // need to be reviewed
        SetNonZero(node, "FinishTimeMs", taskStats.GetFinishTimeMs()); // need to be reviewed

        SetNonZero(node, "ComputeTimeUs", taskStats.GetComputeCpuTimeUs());

        SetNonZero(node, "WaitInputTimeUs", taskStats.GetWaitInputTimeUs());
        SetNonZero(node, "WaitOutputTimeUs", taskStats.GetWaitOutputTimeUs());

        NKqpProto::TKqpTaskExtraStats taskExtraStats;
        if (taskStats.GetExtra().UnpackTo(&taskExtraStats)) {
            SetNonZero(node, "ScanRetries", taskExtraStats.GetScanTaskExtraStats().GetRetriesCount());
        }

        for (auto& inputStats : taskStats.GetInputChannels()) {
            auto& inputNode = node["InputChannels"].AppendValue(NJson::TJsonValue());
            fillInputStats(inputNode, inputStats);
        }

        for (auto& outputStats : taskStats.GetOutputChannels()) {
            auto& outputNode = node["OutputChannels"].AppendValue(NJson::TJsonValue());
            fillOutputStats(outputNode, outputStats);
        }
    };

    auto fillCaStats = [&](NJson::TJsonValue& node, const NYql::NDqProto::TDqComputeActorStats& caStats) {
        SetNonZero(node, "CpuTimeUs", caStats.GetCpuTimeUs());
        SetNonZero(node, "DurationUs", caStats.GetDurationUs());

        SetNonZero(node, "PeakMemoryUsageBytes", caStats.GetMkqlMaxMemoryUsage());

        for (auto& taskStats : caStats.GetTasks()) {
            auto& taskNode = node["Tasks"].AppendValue(NJson::TJsonValue());
            fillTaskStats(taskNode, taskStats);
        }
    };

    THashMap<TString, ui32> guidToPlaneId;

    auto collectPlanNodeId = [&](NJson::TJsonValue& node) {
        if (auto stageGuid = node.GetMapSafe().FindPtr("StageGuid")) {
            if (auto planNodeId = node.GetMapSafe().FindPtr("PlanNodeId")) {
                guidToPlaneId[stageGuid->GetStringSafe()] = planNodeId->GetIntegerSafe();
            }
        }
    };

    auto addStatsToPlanNode = [&](NJson::TJsonValue& node) {
        if (auto stageGuid = node.GetMapSafe().FindPtr("StageGuid")) {
            if (auto stat = stages.FindPtr(stageGuid->GetStringSafe())) {
                auto& stats = node["Stats"];
                if ((*stat)->HasUseLlvm()) {
                    stats["UseLlvm"] = (*stat)->GetUseLlvm();
                } else {
                    stats["UseLlvm"] = "undefined";
                }

                stats["Tasks"] = (*stat)->GetTotalTasksCount();

                stats["StageDurationUs"] = (*stat)->GetStageDurationUs();

                if ((*stat)->HasDurationUs()) {
                    FillAggrStat(stats, (*stat)->GetDurationUs(), "DurationUs");
                }
                if ((*stat)->HasWaitInputTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetWaitInputTimeUs(), "WaitInputTimeUs");
                }
                if ((*stat)->HasWaitOutputTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetWaitOutputTimeUs(), "WaitOutputTimeUs");
                }
                if ((*stat)->HasInputRows()) {
                    FillAggrStat(stats, (*stat)->GetInputRows(), "InputRows");
                }
                if ((*stat)->HasInputBytes()) {
                    FillAggrStat(stats, (*stat)->GetInputBytes(), "InputBytes");
                }
                if ((*stat)->HasOutputRows()) {
                    FillAggrStat(stats, (*stat)->GetOutputRows(), "OutputRows");
                }
                if ((*stat)->HasOutputBytes()) {
                    FillAggrStat(stats, (*stat)->GetOutputBytes(), "OutputBytes");
                }
                if ((*stat)->HasResultRows()) {
                    FillAggrStat(stats, (*stat)->GetResultRows(), "ResultRows");
                }
                if ((*stat)->HasResultBytes()) {
                    FillAggrStat(stats, (*stat)->GetResultBytes(), "ResultBytes");
                }
                if ((*stat)->HasIngressRows()) {
                    FillAggrStat(stats, (*stat)->GetIngressRows(), "IngressRows");
                }
                if ((*stat)->HasIngressBytes()) {
                    FillAggrStat(stats, (*stat)->GetIngressBytes(), "IngressBytes");
                }
                if ((*stat)->HasEgressRows()) {
                    FillAggrStat(stats, (*stat)->GetEgressRows(), "EgressRows");
                }
                if ((*stat)->HasEgressBytes()) {
                    FillAggrStat(stats, (*stat)->GetEgressBytes(), "EgressBytes");
                }
                if ((*stat)->HasCpuTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetCpuTimeUs(), "CpuTimeUs");
                }
                if ((*stat)->HasSourceCpuTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetSourceCpuTimeUs(), "SourceCpuTimeUs");
                }
                if ((*stat)->HasMaxMemoryUsage()) {
                    FillAggrStat(stats, (*stat)->GetMaxMemoryUsage(), "MaxMemoryUsage");
                }

                if (!(*stat)->GetIngress().empty()) {
                    auto& ingressStats = stats.InsertValue("Ingress", NJson::JSON_ARRAY);
                    for (auto ingress : (*stat)->GetIngress()) {
                        auto& ingressInfo = ingressStats.AppendValue(NJson::JSON_MAP);
                        ingressInfo["Name"] = ingress.first;
                        if (ingress.second.HasIngress()) {
                            FillAsyncAggrStat(ingressInfo.InsertValue("Ingress", NJson::JSON_MAP), ingress.second.GetIngress());
                        }
                        if (ingress.second.HasPush()) {
                            FillAsyncAggrStat(ingressInfo.InsertValue("Push", NJson::JSON_MAP), ingress.second.GetPush());
                        }
                        if (ingress.second.HasPop()) {
                            FillAsyncAggrStat(ingressInfo.InsertValue("Pop", NJson::JSON_MAP), ingress.second.GetPop());
                        }
                    }
                }
                if (!(*stat)->GetInput().empty()) {
                    auto& inputStats = stats.InsertValue("Input", NJson::JSON_ARRAY);
                    for (auto input : (*stat)->GetInput()) {
                        auto& inputInfo = inputStats.AppendValue(NJson::JSON_MAP);
                        auto stageGuid = stageIdToGuid.at(input.first);
                        auto planNodeId = guidToPlaneId.at(stageGuid);
                        inputInfo["Name"] = ToString(planNodeId);
                        if (input.second.HasPush()) {
                            FillAsyncAggrStat(inputInfo.InsertValue("Push", NJson::JSON_MAP), input.second.GetPush());
                        }
                        if (input.second.HasPop()) {
                            FillAsyncAggrStat(inputInfo.InsertValue("Pop", NJson::JSON_MAP), input.second.GetPop());
                        }
                    }
                }
                if (!(*stat)->GetOutput().empty()) {
                    auto& outputStats = stats.InsertValue("Output", NJson::JSON_ARRAY);
                    for (auto output : (*stat)->GetOutput()) {
                        auto& outputInfo = outputStats.AppendValue(NJson::JSON_MAP);
                        if (output.first == 0) {
                            outputInfo["Name"] = "RESULT";
                        } else {
                            auto stageGuid = stageIdToGuid.at(output.first);
                            auto planNodeId = guidToPlaneId.at(stageGuid);
                            outputInfo["Name"] = ToString(planNodeId);
                        }
                        if (output.second.HasPush()) {
                            FillAsyncAggrStat(outputInfo.InsertValue("Push", NJson::JSON_MAP), output.second.GetPush());
                        }
                        if (output.second.HasPop()) {
                            FillAsyncAggrStat(outputInfo.InsertValue("Pop", NJson::JSON_MAP), output.second.GetPop());
                        }
                    }
                }
                if (!(*stat)->GetEgress().empty()) {
                    auto& egressStats = stats.InsertValue("Egress", NJson::JSON_ARRAY);
                    for (auto egress : (*stat)->GetEgress()) {
                        auto& egressInfo = egressStats.AppendValue(NJson::JSON_MAP);
                        egressInfo["Name"] = egress.first;
                        if (egress.second.HasPush()) {
                            FillAsyncAggrStat(egressInfo.InsertValue("Push", NJson::JSON_MAP), egress.second.GetPush());
                        }
                        if (egress.second.HasPop()) {
                            FillAsyncAggrStat(egressInfo.InsertValue("Pop", NJson::JSON_MAP), egress.second.GetPop());
                        }
                        if (egress.second.HasEgress()) {
                            FillAsyncAggrStat(egressInfo.InsertValue("Egress", NJson::JSON_MAP), egress.second.GetEgress());
                        }
                    }
                }

                NKqpProto::TKqpStageExtraStats kqpStageStats;
                if ((*stat)->GetExtra().UnpackTo(&kqpStageStats)) {
                    auto& nodesStats = stats.InsertValue("NodesScanShards", NJson::JSON_ARRAY);
                    for (auto&& i : kqpStageStats.GetNodeStats()) {
                        auto& nodeInfo = nodesStats.AppendValue(NJson::JSON_MAP);
                        nodeInfo.InsertValue("shards_count", i.GetShardsCount());
                        nodeInfo.InsertValue("node_id", i.GetNodeId());
                    }
                }

                for (auto& caStats : (*stat)->GetComputeActors()) {
                    auto& caNode = stats["ComputeNodes"].AppendValue(NJson::TJsonValue());
                    fillCaStats(caNode, caStats);
                }
            }
        }
    };

    ModifyPlan(root, collectPlanNodeId);
    ModifyPlan(root, addStatsToPlanNode);

    NJsonWriter::TBuf txWriter;
    txWriter.WriteJsonValue(&root, true);
    return txWriter.Str();
}

TString SerializeAnalyzePlan(const NKqpProto::TKqpStatsQuery& queryStats) {
    TVector<const TString> txPlans;
    for (const auto& execStats: queryStats.GetExecutions()) {
        for (const auto& txPlan: execStats.GetTxPlansWithStats()) {
            txPlans.push_back(txPlan);
        }
    }
    return SerializeTxPlans(txPlans);
}

TString SerializeScriptPlan(const TVector<const TString>& queryPlans) {
    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE);
    writer.SetIndentSpaces(2);

    writer.BeginObject();
    writer.WriteKey("meta");
    writer.BeginObject();
    writer.WriteKey("version").WriteString("0.2");
    writer.WriteKey("type").WriteString("script");
    writer.EndObject();

    writer.WriteKey("queries");
    writer.BeginList();

    for (auto planIt = queryPlans.rbegin(); planIt != queryPlans.rend(); ++planIt) {
        if (planIt->empty()) {
            continue;
        }

        NJson::TJsonValue root;
        NJson::ReadJsonTree(*planIt, &root, true);
        auto planMap = root.GetMapSafe();

        writer.BeginObject();
        if (auto tableAccesses = planMap.FindPtr("tables")) {
            writer.WriteKey("tables");
            writer.WriteJsonValue(tableAccesses);
        }
        if (auto dqPlan = planMap.FindPtr("Plan")) {
            writer.WriteKey("Plan");
            writer.WriteJsonValue(dqPlan);
        }
        writer.EndObject();
    }

    writer.EndList();
    writer.EndObject();

    return writer.Str();
}

} // namespace NKqp
} // namespace NKikimr
