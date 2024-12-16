#pragma once
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>
#include <yql/essentials/core/issue/protos/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NEvWrite::NErrorCodes {

class TOperator {
public:
    class TYdbStatusInfo {
    private:
        YDB_READONLY(Ydb::StatusIds::StatusCode, YdbStatusCode, Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);
        YDB_READONLY(NYql::TIssuesIds::EIssueCode, IssueCode, NYql::TIssuesIds::UNEXPECTED);
        YDB_READONLY_DEF(TString, IssueGeneralText);

    public:
        TYdbStatusInfo(const Ydb::StatusIds::StatusCode code, const NYql::TIssuesIds::EIssueCode issueCode, const TString& issueMessage)
            : YdbStatusCode(code)
            , IssueCode(issueCode)
            , IssueGeneralText(issueMessage) {
        }
    };

    static TConclusion<TYdbStatusInfo> GetStatusInfo(const NKikimrDataEvents::TEvWriteResult::EStatus value);
};

}   // namespace NKikimr::NEvWrite::NErrorCodes
