#pragma once

#include <ydb/core/control/common_controls/tracing_control.h>

namespace NKikimr {
namespace NKeyValue {

class TKeyValueTracingControl {
public:
    enum class ERequestType : std::size_t {
        READ,
        READ_RANGE,
        EXECUTE_TRANSACTION,
        GET_STORAGE_CHANNEL_STATUS,
        ACQUIRE_LOCK,
    };
    
    TKeyValueTracingControl(TIntrusivePtr<TControlBoard>& icb, TIntrusivePtr<ITimeProvider> timeProvider,
            TIntrusivePtr<IRandomProvider>& randomProvider) {
        constexpr std::array<std::pair<ERequestType, TStringBuf>, RequestTypes> domainMapping = {{
            {ERequestType::READ, "Read"},
            {ERequestType::READ_RANGE, "ReadRange"},
            {ERequestType::EXECUTE_TRANSACTION, "ExecuteTransaction"},
            {ERequestType::GET_STORAGE_CHANNEL_STATUS, "GetStorageChannelStatus"},
            {ERequestType::ACQUIRE_LOCK, "AcquireLock"},
        }};
        const TString keyValueControlDomain = "TracingControls.KeyValue";

        for (auto [requestType, domain] : domainMapping) {
            Y_ABORT_UNLESS(static_cast<std::size_t>(requestType) == Controls.size(), "Incorrect domain mapping");
            Controls.emplace_back(icb, timeProvider, randomProvider, keyValueControlDomain + "." + domain);
        }
    }

    void HandleTracing(ERequestType requestType, NWilson::TTraceId& traceId) {
        Controls[static_cast<std::size_t>(requestType)].SampleThrottle(traceId);
    }

private:
    static constexpr std::size_t RequestTypes = 5;

    TVector<TTracingControl> Controls;
};

} // namespace NKeyValue
} // namespace NKikimr
