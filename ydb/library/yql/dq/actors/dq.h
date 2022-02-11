#pragma once

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/dq_events_ids.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NYql::NDq {

struct TEvDq {

    struct TEvAbortExecution : public NActors::TEventPB<TEvAbortExecution, NDqProto::TEvAbortExecution, TDqEvents::EvAbortExecution> {
        static THolder<TEvAbortExecution> Unavailable(const TString& s, const TIssues& subIssues = {}) {
            return MakeHolder<TEvAbortExecution>(Ydb::StatusIds::UNAVAILABLE, s, subIssues);
        }

        static THolder<TEvAbortExecution> InternalError(const TString& s, const TIssues& subIssues = {}) {
            return MakeHolder<TEvAbortExecution>(Ydb::StatusIds::INTERNAL_ERROR, s, subIssues);
        }

        static THolder<TEvAbortExecution> Aborted(const TString& s, const TIssues& subIssues = {}) {
            return MakeHolder<TEvAbortExecution>(Ydb::StatusIds::ABORTED, s, subIssues);
        }

        TEvAbortExecution() = default;

        TEvAbortExecution(TEvAbortExecution&&) = default;

        TEvAbortExecution(const TEvAbortExecution&) = default;

        TEvAbortExecution(Ydb::StatusIds::StatusCode code, const TIssues& issues) {
            Record.SetStatusCode(code);
            IssuesToMessage(issues, Record.MutableIssues());
        }

        TEvAbortExecution(Ydb::StatusIds::StatusCode code, const TString& message, const TIssues& subIssues = {}) {
            Record.SetStatusCode(code);
            Record.SetLegacyMessage(message);
            TIssue issue(message);
            for (const TIssue& i : subIssues) {
                issue.AddSubIssue(MakeIntrusive<TIssue>(i));
            }
            TIssues issues;
            issues.AddIssue(std::move(issue));
            IssuesToMessage(issues, Record.MutableIssues());
        }

        TIssues GetIssues() const {
            TIssues issues;
            if (Record.IssuesSize()) {
                IssuesFromMessage(Record.GetIssues(), issues);
            } else if (const TString& msg = Record.GetLegacyMessage()) {
                issues.AddIssue(msg);
            }
            return issues;
        }
    };

};

} // namespace NYql::NDq
