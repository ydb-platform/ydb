#include "common.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>

#include <ydb/library/actors/struct_log/log_stack.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader {

void SendScanError(const ui64 tabletId, const TActorId& scanComputeActor, const ui32 scanGen, const TString& table, const TString& problem,
    const TString& details, const TActorContext& ctx) {
    YDB_LOG_WARN("",
        {"event", "TTxScan failed"},
        {"problem", problem},
        {"details", details});

    auto ev = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>(scanGen, tabletId);
    ev->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
    auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
        TStringBuilder() << "Table " << table << " (shard " << tabletId << ") scan failed, reason: " << problem << "/" << details);
    NYql::IssueToMessage(issue, ev->Record.MutableIssues()->Add());

    ctx.Send(scanComputeActor, ev.Release());
}

TConclusion<TReadMetadataBase::TConstPtr> MakeReadMetadata(NColumnShard::TColumnShard* self, const NColumnShard::TScanCounters& scanCounters,
    const IScannerConstructor& scannerConstructor, const TReadDescription& read) {
    const TInstant startInstant = TAppData::TimeProvider->Now();
    auto newRange = scannerConstructor.BuildReadMetadata(self, read);
    if (newRange.IsFail()) {
        return TConclusionStatus::Fail(newRange.GetErrorMessage());
    }
    scanCounters.OnReadMetadata(TAppData::TimeProvider->Now() - startInstant);
    return TReadMetadataBase::TConstPtr(TValidator::CheckNotNull(newRange.DetachResult()));
}

}   // namespace NKikimr::NOlap::NReader
