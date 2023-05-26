#include "probes.h"

#include <library/cpp/lwtrace/protos/lwtrace.pb.h>

#include <util/generic/string.h>

namespace NLWTrace {

LWTRACE_USING(LWTRACE_INTERNAL_PROVIDER);

TProbeMap TManager::GetProbesMap() {
    class TProbeReader
    {
    private:
        TProbeMap& Result;

    public:
        TProbeReader(TProbeMap& result)
            : Result(result)
        {}

        void Push(NLWTrace::TProbe* probe)
        {
            Result[std::make_pair(probe->Event.Name, probe->Event.GetProvider())] = probe;
        }
    };

    TProbeMap result;

    auto reader = TProbeReader(result);
    ReadProbes(reader);
    return result;
}

void TManager::CreateTraceRequest(TTraceRequest& msg, TOrbit& orbit)
{
    msg.SetIsTraced(orbit.HasShuttles());
}

bool TManager::HandleTraceRequest(
    const TTraceRequest& msg,
    TOrbit& orbit)
{
    if (!msg.GetIsTraced()) {
        return false;
    }
    TParams params;
    SerializingExecutor->Execute(orbit, params);
    return true;
}

TTraceDeserializeStatus TManager::HandleTraceResponse(
    const TTraceResponse& msg,
    const TProbeMap& probesMap,
    TOrbit& orbit,
    i64 timeOffset,
    double timeScale)
{
    TTraceDeserializeStatus result;
    if (!msg.GetTrace().GetEvents().size()) {
        return result;
    }

    ui64 prev = EpochNanosecondsToCycles(
        msg.GetTrace().GetEvents()[0].GetTimestampNanosec());

    for (auto& v : msg.GetTrace().GetEvents()) {
        auto it = probesMap.find(std::make_pair(v.GetName(), v.GetProvider()));
        if (it != probesMap.end()) {
            TProbe* probe = it->second;
            TParams params;
            if(!probe->Event.Signature.DeserializeFromPb(params, v.GetParams())) {
                LWTRACK(DeserializationError, orbit, probe->Event.Name, probe->Event.GetProvider());
                result.AddFailedEventName(v.GetName());
            } else {
                // in case of fork join probes would be like "start 0 fork 1 ....... join 10 forked 5 forked 6"
                ui64 timestamp = EpochNanosecondsToCycles(v.GetTimestampNanosec());
                if (timestamp > prev) {
                    timestamp = prev + (timestamp-prev)*timeScale + timeOffset;
                } else {
                    timestamp += timeOffset;
                }

                orbit.AddProbe(
                    probe,
                    params,
                    timestamp);
                probe->Event.Signature.DestroyParams(params);
                prev = timestamp;
            }
        } else {
            result.AddFailedEventName(v.GetName());
        }
    }
    return result;
}

void TManager::CreateTraceResponse(TTraceResponse& msg, TOrbit& orbit)
{
    orbit.Serialize(0, *msg.MutableTrace());
}

}

