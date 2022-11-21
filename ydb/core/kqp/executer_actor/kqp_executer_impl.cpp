#include "kqp_executer_impl.h"

#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>

#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>

#include <library/cpp/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

void BuildKqpExecuterResult(const NKqpProto::TKqpPhyResult& txResult, TKqpExecuterTxResult& result) {
    result.IsStream = txResult.GetIsStream();
    result.ItemType.CopyFrom(txResult.GetItemType());

    if (txResult.ColumnHintsSize() > 0) {
        TVector<TString> columnHints;
        columnHints.reserve(txResult.GetColumnHints().size());
        std::copy(txResult.GetColumnHints().begin(), txResult.GetColumnHints().end(), std::back_inserter(columnHints));

        result.ResultItemType.ConstructInPlace(TKqpProtoBuilder::ApplyColumnHints(result.ItemType, columnHints));
    }
}

void BuildKqpExecuterResults(const NKqpProto::TKqpPhyTx& tx, TVector<TKqpExecuterTxResult>& results) {
    ui32 i = results.size();
    results.resize(results.size() + tx.GetResults().size());

    for (auto& resultProto : tx.GetResults()) {
        BuildKqpExecuterResult(resultProto, results[i++]);
    }
}

void PrepareKqpTaskParameters(const NKqpProto::TKqpPhyStage& stage, const TStageInfo& stageInfo, const TTask& task,
    NDqProto::TDqTask& dqTask, const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory)
{
    auto g = typeEnv.BindAllocator();
    for (auto& paramName : stage.GetProgramParameters()) {
        auto& dqParams = *dqTask.MutableParameters();
        if (auto* taskParam = task.Meta.Params.FindPtr(paramName)) {
            dqParams[paramName] = *taskParam;
        } else {
            auto* paramValue = stageInfo.Meta.Tx.Params.Values.FindPtr(paramName);
            YQL_ENSURE(paramValue);
            dqParams[paramName] = NDq::TDqDataSerializer::SerializeParam(*paramValue, typeEnv, holderFactory);
        }
    }
}

std::pair<TString, TString> SerializeKqpTasksParametersForOlap(const NKqpProto::TKqpPhyStage& stage,
    const TStageInfo& stageInfo, const TTask& task, const NMiniKQL::THolderFactory& holderFactory,
    const NMiniKQL::TTypeEnvironment& typeEnv)
{
    std::vector<std::shared_ptr<arrow::Field>> columns;
    std::vector<std::shared_ptr<arrow::Array>> data;
    auto& parameterNames = task.Meta.ReadInfo.OlapProgram.ParameterNames;

    columns.reserve(parameterNames.size());
    data.reserve(parameterNames.size());

    for (auto& name : stage.GetProgramParameters()) {
        if (!parameterNames.contains(name)) {
            continue;
        }

        if (auto* taskParam = task.Meta.Params.FindPtr(name)) {
            // This parameter is the list, holding type from task.Meta.ParamTypes
            // Those parameters can't be used in Olap programs now
            YQL_ENSURE(false, "OLAP program contains task parameter, not supported yet.");
            continue;
        }

        const NYql::NDq::TMkqlValueRef* mkqlValue = stageInfo.Meta.Tx.Params.Values.FindPtr(name);

        auto [type, value] = ImportValueFromProto(mkqlValue->GetType(), mkqlValue->GetValue(), typeEnv, holderFactory);

        YQL_ENSURE(NYql::NArrow::IsArrowCompatible(type), "Incompatible parameter type. Can't convert to arrow");

        std::unique_ptr<arrow::ArrayBuilder> builder = NYql::NArrow::MakeArrowBuilder(type);
        NYql::NArrow::AppendElement(value, builder.get(), type);

        std::shared_ptr<arrow::Array> array;
        auto status = builder->Finish(&array);

        YQL_ENSURE(status.ok(), "Failed to build arrow array of variables.");

        auto field = std::make_shared<arrow::Field>(name, array->type());

        columns.emplace_back(std::move(field));
        data.emplace_back(std::move(array));
    }

    auto schema = std::make_shared<arrow::Schema>(columns);
    auto recordBatch = arrow::RecordBatch::Make(schema, 1, data);

    return std::make_pair<TString, TString>(
        NArrow::SerializeSchema(*schema),
        NArrow::SerializeBatchNoCompression(recordBatch)
    );
}

TActorId ReportToRl(ui64 ru, const TString& database, const TString& userToken,
    const NKikimrKqp::TRlPath& path)
{
    Ydb::RateLimiter::AcquireResourceRequest req;
    req.set_coordination_node_path(path.GetCoordinationNode());
    req.set_resource_path(path.GetResourcePath());
    req.set_used(ru);

    // No need to handle result of rate limiter response on the response hook
    // just report ru usage
    auto noop = [](Ydb::RateLimiter::AcquireResourceResponse) {};
    return NKikimr::NRpcService::RateLimiterAcquireUseSameMailbox(
        std::move(req),
        database,
        userToken,
        std::move(noop),
        TActivationContext::AsActorContext());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IActor* CreateKqpExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TMaybe<TString>& userToken, TKqpRequestCounters::TPtr counters)
{
    if (request.Transactions.empty()) {
        // commit-only or rollback-only data transaction
        YQL_ENSURE(request.EraseLocks);
        return CreateKqpDataExecuter(std::move(request), database, userToken, counters);
    }

    if (request.Locks.empty()) {
        bool literal = true;
        for (const auto& tx : request.Transactions) {
            if (tx.Body->GetType() != NKqpProto::TKqpPhyTx::TYPE_COMPUTE) {
                literal = false;
                break;
            }
            for (const auto& stage : tx.Body->GetStages()) {
                if (stage.InputsSize() != 0) {
                    literal = false;  // allow only independent stages
                    break;
                }
            }
        }

        if (literal) {
            return CreateKqpLiteralExecuter(std::move(request), counters);
        }
    }

    bool data = true; // `false` stands for Scan

    TMaybe<NKqpProto::TKqpPhyTx::EType> txsType;
    for (auto& tx : request.Transactions) {
        if (txsType) {
            YQL_ENSURE(*txsType == tx.Body->GetType(), "Mixed physical tx types in executer.");
            YQL_ENSURE(*txsType == NKqpProto::TKqpPhyTx::TYPE_DATA, "Cannot execute multiple non-data physical txs.");
        } else {
            txsType = tx.Body->GetType();

            switch (tx.Body->GetType()) {
                case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
                case NKqpProto::TKqpPhyTx::TYPE_DATA:
                    data = true;
                    break;

                case NKqpProto::TKqpPhyTx::TYPE_SCAN:
                    data = false;
                    break;

                default:
                    YQL_ENSURE(false, "Unsupported physical tx type: " << (ui32)tx.Body->GetType());
            }
        }
    }

    return data
        ? CreateKqpDataExecuter(std::move(request), database, userToken, counters)
        : CreateKqpScanExecuter(std::move(request), database, userToken, counters);
}

} // namespace NKqp
} // namespace NKikimr
