#include "error_codes.h"

namespace NKikimr::NEvWrite::NErrorCodes {

TConclusion<NErrorCodes::TOperator::TYdbStatusInfo> TOperator::GetStatusInfo(
    const NKikimrDataEvents::TEvWriteResult::EStatus value) {
    switch (value) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED:
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED:
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED:
            return TConclusionStatus::Fail("Incorrect status for interpretation to YdbStatus");
        case NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED:
            return TYdbStatusInfo(Ydb::StatusIds::ABORTED, NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED, "Request aborted");
        case NKikimrDataEvents::TEvWriteResult::STATUS_DATABASE_DISK_SPACE_QUOTA_EXCEEDED:
            return TYdbStatusInfo(Ydb::StatusIds::UNAVAILABLE, NYql::TIssuesIds::KIKIMR_DATABASE_DISK_SPACE_QUOTA_EXCEEDED, "Database disk space quota exceeded");
        case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_GROUP_OUT_OF_SPACE:
            return TYdbStatusInfo(Ydb::StatusIds::OVERLOADED, NYql::TIssuesIds::KIKIMR_OVERLOADED, "Out of space");
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
        case NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE:
            return TYdbStatusInfo(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, "Wrong shard state");
        case NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION:
            return TYdbStatusInfo(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, "Constraint violated");
    }
}

}
