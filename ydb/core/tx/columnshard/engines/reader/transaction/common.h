#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap::NReader {

// Tells the scan initiator that the scan could not be started. `problem` names the step that failed,
// `details` explains why.
void SendScanError(const ui64 tabletId, const TActorId& scanComputeActor, const ui32 scanGen, const TString& table, const TString& problem,
    const TString& details, const TActorContext& ctx);

// Builds the read metadata for a prepared read description, accounting how long that took.
TConclusion<TReadMetadataBase::TConstPtr> MakeReadMetadata(NColumnShard::TColumnShard* self, const NColumnShard::TScanCounters& scanCounters,
    const IScannerConstructor& scannerConstructor, const TReadDescription& read);

}   // namespace NKikimr::NOlap::NReader
