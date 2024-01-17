#include <ydb/core/base/events.h>

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/load_test.pb.h>

#include <library/cpp/monlib/dynamic_counters/percentile/percentile_lg.h>
#include <library/cpp/json/writer/json_value.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {
struct TEvLoad {
    enum EEv {
        EvLoadTestRequest = EventSpaceBegin(TKikimrEvents::ES_TEST_LOAD),
        EvLoadTestFinished,
        EvLoadTestResponse,
        EvNodeFinishResponse,
        EvYqlSingleQueryResponse,
    };

    struct TEvLoadTestRequest : public TEventPB<TEvLoadTestRequest,
        NKikimr::TEvLoadTestRequest, EvLoadTestRequest>
    {};

    struct TEvLoadTestResponse : public TEventPB<TEvLoadTestResponse,
        NKikimr::TEvLoadTestResponse, EvLoadTestResponse>
    {};

    struct TLoadReport : public TThrRefBase {
        enum ELoadType {
            LOAD_READ,
            LOAD_WRITE,
            LOAD_LOG_WRITE,
        };

        TDuration Duration;
        ui64 Size;
        ui32 InFlight;
        TVector<ui64> RwSpeedBps;
        ELoadType LoadType;
        NMonitoring::TPercentileTrackerLg<10, 4, 1> LatencyUs; // Upper threshold of this tracker is ~134 seconds, size is 256kB

        double GetAverageSpeed() const {
            if (RwSpeedBps.size() < 1) {
                return 0;
            }
            double avg = 0;
            for (const ui64& speed : RwSpeedBps) {
                avg += speed;
            }
            avg /= RwSpeedBps.size();
            return avg;
        }

        double GetSpeedDeviation() const {
            if (RwSpeedBps.size() <= 1) {
                return 0;
            }
            i64 avg = (i64)GetAverageSpeed();
            double sd = 0;
            for (const ui64& speed : RwSpeedBps) {
                sd += ((i64)speed - avg) * ((i64)speed - avg);
            }
            sd /= RwSpeedBps.size();
            return std::sqrt(sd);
        }

        TString LoadTypeName() const {
            switch (LoadType) {
            case LOAD_READ:
                return "read";
            case LOAD_WRITE:
                return "write";
            case LOAD_LOG_WRITE:
                return "log_write";
            }
        }
    };

    struct TEvLoadTestFinished : public TEventLocal<TEvLoadTestFinished, TEvLoad::EvLoadTestFinished> {
        ui64 Tag;
        TIntrusivePtr<TLoadReport> Report; // nullptr indicates an error or an early stop
        TString ErrorReason; // human readable status, might be nonempty even in the case of success
        TString LastHtmlPage;
        NJson::TJsonValue JsonResult;

        TEvLoadTestFinished(ui64 tag, TIntrusivePtr<TLoadReport> report, TString errorReason = {})
            : Tag(tag)
            , Report(report)
            , ErrorReason(errorReason)
        {}
    };

    struct TEvNodeFinishResponse : public TEventPB<TEvNodeFinishResponse,
        NKikimr::TEvNodeFinishResponse, EvNodeFinishResponse>
    {
        TString ToString() const {
            TString str;
            google::protobuf::TextFormat::PrintToString(Record, &str);
            return str;
        }
    };

    struct TEvYqlSingleQueryResponse : public TEventLocal<TEvYqlSingleQueryResponse, TEvLoad::EvYqlSingleQueryResponse> {
        TString Result;  // empty in case if there is an error
        TMaybe<TString> ErrorMessage;
        TMaybe<NKikimrKqp::TQueryResponse> Response;

        TEvYqlSingleQueryResponse(TString result, TMaybe<TString> errorMessage, TMaybe<NKikimrKqp::TQueryResponse> response)
            : Result(std::move(result))
            , ErrorMessage(std::move(errorMessage))
            , Response(std::move(response))
        {}
    };

};

}
