#pragma once
#include "kqp_compute_events.h"
#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/library/accessor/accessor.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <yql/essentials/core/issue/protos/issue_id.pb.h>
#include <yql/essentials/core/issue/yql_issue.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NKqp::NScanPrivate {

struct TEvScanExchange {

    enum EEvents {
        EvSendData = EventSpaceBegin(TKikimrEvents::ES_KQP_SCAN_EXCHANGE),
        EvAckData,
        EvTerminateFromFetcher,
        EvTerminateFromCompute,
        EvRegisterFetcher,
        EvFetcherFinished,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_KQP_SCAN_EXCHANGE), "expected EvEnd < EventSpaceEnd");

    class TEvRegisterFetcher: public NActors::TEventLocal<TEvRegisterFetcher, EvRegisterFetcher> {
    public:
    };

    class TEvFetcherFinished: public NActors::TEventLocal<TEvFetcherFinished, EvFetcherFinished> {
    public:
    };

    class TEvSendData: public NActors::TEventLocal<TEvSendData, EvSendData> {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::Table>, ArrowBatch);
        YDB_ACCESSOR_DEF(TVector<TOwnedCellVec>, Rows);
        YDB_READONLY(ui64, TabletId, 0);
        YDB_ACCESSOR_DEF(std::vector<ui32>, DataIndexes);
        YDB_READONLY_DEF(TLocksInfo, LocksInfo);
        YDB_ACCESSOR_DEF(bool, Finished);
        YDB_ACCESSOR_DEF(ui64, CpuTimeUs);
        YDB_ACCESSOR_DEF(ui64, WaitTimeUs);
        YDB_ACCESSOR_DEF(ui64, WaitOutputTimeUs);
    public:
        ui32 GetRowsCount() const {
            return ArrowBatch ? ArrowBatch->num_rows() : Rows.size();
        }

        TEvSendData(const ui64 tabletId, const TEvKqpCompute::TEvScanData& data, const std::shared_ptr<arrow::Table>& arrowBatch)
            : ArrowBatch(arrowBatch)
            , TabletId(tabletId)
            , LocksInfo(data.LocksInfo)
            , Finished(data.Finished)
            , CpuTimeUs(data.CpuTime.MicroSeconds())
            , WaitTimeUs(data.WaitTime.MicroSeconds())
        {
            Y_ABORT_UNLESS(ArrowBatch);
            Y_ABORT_UNLESS(ArrowBatch->num_rows());
        }

        TEvSendData(const ui64 tabletId, const TEvKqpCompute::TEvScanData& data, const std::shared_ptr<arrow::Table>& arrowBatch, std::vector<ui32>&& dataIndexes)
            : ArrowBatch(arrowBatch)
            , TabletId(tabletId)
            , DataIndexes(std::move(dataIndexes))
            , LocksInfo(data.LocksInfo)
            , Finished(data.Finished)
            , CpuTimeUs(data.CpuTime.MicroSeconds())
            , WaitTimeUs(data.WaitTime.MicroSeconds())
        {
            Y_ABORT_UNLESS(ArrowBatch);
            Y_ABORT_UNLESS(ArrowBatch->num_rows());
        }

        TEvSendData(const ui64 tabletId, const TEvKqpCompute::TEvScanData& data, TVector<TOwnedCellVec>&& rows)
            : Rows(std::move(rows))
            , TabletId(tabletId)
            , LocksInfo(data.LocksInfo)
            , Finished(data.Finished)
            , CpuTimeUs(data.CpuTime.MicroSeconds())
            , WaitTimeUs(data.WaitTime.MicroSeconds())
        {
            Y_ABORT_UNLESS(Rows.size());
        }

        TEvSendData(const ui64 tabletId, const TEvKqpCompute::TEvScanData& data)
            : TabletId(tabletId)
            , LocksInfo(data.LocksInfo)
            , Finished(data.Finished)
            , CpuTimeUs(data.CpuTime.MicroSeconds())
            , WaitTimeUs(data.WaitTime.MicroSeconds())
        {
        }
    };

    class TEvAckData: public NActors::TEventLocal<TEvAckData, EvAckData> {
    private:
        YDB_READONLY(ui32, FreeSpace, 0)
    public:
        TEvAckData(const ui32 freeSpace)
            : FreeSpace(freeSpace) {

        }
    };

    class TEvTerminateFromFetcher: public NActors::TEventLocal<TEvTerminateFromFetcher, EvTerminateFromFetcher> {
    private:
        YDB_READONLY(NYql::NDqProto::EComputeState, State, NYql::NDqProto::COMPUTE_STATE_FAILURE);
        YDB_READONLY(NYql::NDqProto::StatusIds::StatusCode, StatusCode, NYql::NDqProto::StatusIds::UNSPECIFIED);
        YDB_READONLY_DEF(NYql::TIssues, Issues);
    public:
        TEvTerminateFromFetcher(NYql::TIssuesIds::EIssueCode issueCode, const TString& message) {
            NYql::TIssue issue(message);
            NYql::SetIssueCode(issueCode, issue);
            Issues = { issue };
            StatusCode = NYql::NDqProto::StatusIds::PRECONDITION_FAILED;
        }

        TEvTerminateFromFetcher(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssuesIds::EIssueCode issueCode, const TString& message) {
            NYql::TIssue issue(message);
            NYql::SetIssueCode(issueCode, issue);
            Issues = { issue };
            StatusCode = statusCode;
        }

        TEvTerminateFromFetcher(const NYql::NDqProto::EComputeState state, const NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& issues)
            : State(state)
            , StatusCode(statusCode)
            , Issues(issues) {

        }
    };

    class TEvTerminateFromCompute: public NActors::TEventLocal<TEvTerminateFromCompute, EvTerminateFromCompute> {
    private:
        YDB_READONLY_FLAG(Success, false);
        YDB_READONLY_DEF(NYql::TIssues, Issues);
    public:
        TEvTerminateFromCompute(const bool success, const NYql::TIssues& issues)
            : SuccessFlag(success)
            , Issues(issues) {

        }
    };
};

}
