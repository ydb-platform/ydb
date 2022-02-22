#pragma once

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/dq_events_ids.h>

namespace NYql {
namespace NDq {

struct TEvDq {

    struct TEvAbortExecution : public NActors::TEventPB<TEvAbortExecution, NDqProto::TEvAbortExecution, TDqEvents::EvAbortExecution> {
        static THolder <TEvAbortExecution> Unavailable(const TString& s) {
            return MakeHolder<TEvAbortExecution>(Ydb::StatusIds::UNAVAILABLE, s);
        }

        static THolder <TEvAbortExecution> InternalError(const TString& s) {
            return MakeHolder<TEvAbortExecution>(Ydb::StatusIds::INTERNAL_ERROR, s);
        }

        static THolder <TEvAbortExecution> Aborted(const TString& s) {
            return MakeHolder<TEvAbortExecution>(Ydb::StatusIds::ABORTED, s);
        }

        TEvAbortExecution() = default;

        TEvAbortExecution(TEvAbortExecution&&) = default;

        TEvAbortExecution(const TEvAbortExecution&) = default;

        TEvAbortExecution(Ydb::StatusIds::StatusCode code, const TString& message) {
            Record.SetStatusCode(code);
            Record.SetMessage(message);
        }
    };

};

} // namespace NDq
} // namespace NYql
