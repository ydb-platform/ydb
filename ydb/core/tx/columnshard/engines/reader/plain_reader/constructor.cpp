#include "constructor.h"
#include "filter_assembler.h"
#include "column_assembler.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>

namespace NKikimr::NOlap::NPlainReader {

TPortionInfo::TPreparedBatchData TAssembleColumnsTaskConstructor::BuildBatchAssembler(IDataReader& reader) {
    auto blobSchema = reader.GetReadMetadata()->GetLoadSchema(PortionInfo->GetMinSnapshot());
    auto readSchema = reader.GetReadMetadata()->GetLoadSchema(reader.GetReadMetadata()->GetSnapshot());
    ISnapshotSchema::TPtr resultSchema;
    if (ColumnIds.size()) {
        resultSchema = std::make_shared<TFilteredSnapshotSchema>(readSchema, ColumnIds);
    } else {
        resultSchema = readSchema;
    }

    return PortionInfo->PrepareForAssemble(*blobSchema, *resultSchema, Data);
}

void TEFTaskConstructor::DoOnDataReady(IDataReader& reader) {
    reader.GetContext().MutableProcessor().Add(reader, std::make_shared<TAssembleFilter>(BuildBatchAssembler(reader),
        reader.GetReadMetadata(), SourceIdx, ColumnIds, reader.GetContext().GetProcessor().GetObject(), UseEarlyFilter));
}

void TFFColumnsTaskConstructor::DoOnDataReady(IDataReader& reader) {
    reader.GetContext().MutableProcessor().Add(reader, std::make_shared<TAssembleFFBatch>(BuildBatchAssembler(reader),
        SourceIdx, AppliedFilter, reader.GetContext().GetProcessor().GetObject()));
}

}
