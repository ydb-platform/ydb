#pragma once
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NEvWrite::NErrorCodes {

class TOperator {
public:
    class TYdbStatusInfo {
    private:
        YDB_READONLY(Ydb::StatusIds::StatusCode, YdbStatusCode, Ydb::StatusIds::UNSPECIFIED);
        YDB_READONLY(NYql::TIssuesIds::EIssueCode, IssueCode, NYql::TIssuesIds::UNEXPECTED);
        YDB_READONLY_DEF(TString, IssueGeneralText);
    public:
        TYdbStatusInfo(const Ydb::StatusIds::StatusCode code, const NYql::TIssuesIds::EIssueCode issueCode, const TString& issueMessage)
            : YdbStatusCode(code)
            , IssueCode(issueCode)
            , IssueGeneralText(issueMessage) {

        }
    };

    static TConclusion<TYdbStatusInfo> GetStatusInfo(const NKikimrDataEvents::TEvWriteResult::EStatus value) {
        switch (result.GetStatus()) {
            case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED:
            case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED:
            case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED:
                return TConclusionStatus::Fail("Incorrect status for interpretation to YdbStatus");
            case NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED:
                return TYdbStatusInfo(Ydb::StatusIds::ABORTED, NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED, "Request aborted");
            case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_SPACE_EXHAUSTED:
                return TYdbStatusInfo(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssuesIds::KIKIMR_DISK_SPACE_EXHAUSTED, "Disk space exhausted");
            case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR:
                return TYdbStatusInfo(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Request aborted");
            case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED:
                return TYdbStatusInfo(Ydb::StatusIds::OVERLOADED, NYql::TIssuesIds::KIKIMR_OVERLOADED, "System overloaded");
            case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED:
                return TYdbStatusInfo(Ydb::StatusIds::CANCELLED, NYql::TIssuesIds::KIKIMR_OPERATION_CANCELLED, "Request cancelled");
            case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST:
                return TYdbStatusInfo(Ydb::StatusIds::BAD_REQUEST, NYql::TIssuesIds::KIKIMR_BAD_REQUEST, "Incorrect request");
            case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED:
                return TYdbStatusInfo(Ydb::StatusIds::SCHEME_ERROR, NYql::TIssuesIds::KIKIMR_SCHEMA_CHANGED, "Schema changed");
            case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
                return TYdbStatusInfo(Ydb::StatusIds::ABORTED, NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED, "Transaction locks invalidated.");
            }
        }
    }
};

}
