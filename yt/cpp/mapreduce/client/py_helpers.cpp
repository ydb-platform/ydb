#include "py_helpers.h"

#include "client.h"
#include "operation.h"
#include "transaction.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/fluent.h>

#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/hash_set.h>

namespace NYT {

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

IStructuredJobPtr ConstructJob(const TString& jobName, const TString& state)
{
    auto node = TNode();
    if (!state.empty()) {
        node = NodeFromYsonString(state);
    }
    return TJobFactory::Get()->GetConstructingFunction(jobName.data())(node);
}

TString GetJobStateString(const IStructuredJob& job)
{
    TString result;
    {
        TStringOutput output(result);
        job.Save(output);
        output.Finish();
    }
    return result;
}

TStructuredJobTableList NodeToStructuredTablePaths(const TNode& node, const TOperationPreparer& preparer)
{
    int intermediateTableCount = 0;
    TVector<TRichYPath> paths;
    for (const auto& inputNode : node.AsList()) {
        if (inputNode.IsNull()) {
            ++intermediateTableCount;
        } else {
            paths.emplace_back(inputNode.AsString());
        }
    }
    paths = NRawClient::CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetContext(), paths);
    TStructuredJobTableList result(intermediateTableCount, TStructuredJobTable::Intermediate(TUnspecifiedTableStructure()));
    for (const auto& path : paths) {
        result.emplace_back(TStructuredJobTable{TUnspecifiedTableStructure(), path});
    }
    return result;
}

TString GetIOInfo(
    const IStructuredJob& job,
    const TCreateClientOptions& options,
    const TString& cluster,
    const TString& transactionId,
    const TString& inputPaths,
    const TString& outputPaths,
    const TString& neededColumns)
{
    auto client = NDetail::CreateClientImpl(cluster, options);
    TOperationPreparer preparer(client, GetGuid(transactionId));

    auto structuredInputs = NodeToStructuredTablePaths(NodeFromYsonString(inputPaths), preparer);
    auto structuredOutputs = NodeToStructuredTablePaths(NodeFromYsonString(outputPaths), preparer);

    auto neededColumnsNode = NodeFromYsonString(neededColumns);
    THashSet<TString> columnsUsedInOperations;
    for (const auto& columnNode : neededColumnsNode.AsList()) {
        columnsUsedInOperations.insert(columnNode.AsString());
    }

    auto operationIo = CreateSimpleOperationIoHelper(
        job,
        preparer,
        TOperationOptions(),
        std::move(structuredInputs),
        std::move(structuredOutputs),
        TUserJobFormatHints(),
        ENodeReaderFormat::Yson,
        columnsUsedInOperations);

    return BuildYsonStringFluently().BeginMap()
        .Item("input_format").Value(operationIo.InputFormat.Config)
        .Item("output_format").Value(operationIo.OutputFormat.Config)
        .Item("input_table_paths").List(operationIo.Inputs)
        .Item("output_table_paths").List(operationIo.Outputs)
        .Item("small_files").DoListFor(
            operationIo.JobFiles.begin(),
            operationIo.JobFiles.end(),
            [] (TFluentList fluent, auto fileIt) {
                fluent.Item().BeginMap()
                    .Item("file_name").Value(fileIt->FileName)
                    .Item("data").Value(fileIt->Data)
                .EndMap();
            })
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
