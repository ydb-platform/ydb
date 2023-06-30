#include "yql_job_infer_schema.h"

#include <ydb/library/yql/providers/yt/lib/infer_schema/infer_schema.h>

#include <yt/cpp/mapreduce/io/node_table_reader.h>
#include <yt/cpp/mapreduce/io/node_table_writer.h>
#include <library/cpp/yson/node/node_io.h>

namespace NYql {

void TYqlInferSchemaJob::DoImpl(const TFile& inHandle, const TVector<TFile>& outHandles) {
    NYT::TTableReader<NYT::TNode> reader(MakeIntrusive<NYT::TNodeTableReader>(MakeIntrusive<NYT::TJobReader>(inHandle)));
    NYT::TTableWriter<NYT::TNode> writer(MakeIntrusive<NYT::TNodeTableWriter>(MakeHolder<NYT::TJobWriter>(outHandles)));

    Init();

    THashMap<ui32, TStreamSchemaInferer> infererByTableIndex;

    for (; reader.IsValid(); reader.Next()) {
        ui32 idx = reader.GetTableIndex();

        YQL_ENSURE(idx < TableNames.size());
        TString tableName = TableNames[idx];

        auto it = infererByTableIndex.find(idx);

        if (it == infererByTableIndex.end()) {
            it = infererByTableIndex.insert({ idx, TStreamSchemaInferer(tableName) }).first;
        }

        it->second.AddRow(reader.GetRow());
    }

    for (const auto &i : infererByTableIndex) {
        ui32 idx = i.first;

        NYT::TNode schema;
        try {
            schema = i.second.GetSchema();
        } catch (const std::exception& e) {
            schema = NYT::TNode(e.what());
        }

        NYT::TNode output = NYT::TNode()("index", idx)("schema", NYT::NodeToYsonString(schema));
        writer.AddRow(output);
    }
}

} // NYql
