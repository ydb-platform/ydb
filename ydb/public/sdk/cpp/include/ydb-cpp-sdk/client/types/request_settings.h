#pragma once

#include "fwd.h"

#include "fluent_settings_helpers.h"

#include <util/datetime/base.h>

#include <vector>
#include <utility>
#include <string>

namespace NYdb::inline Dev {

template<typename TDerived>
struct TRequestSettings {
    using TSelf = TDerived;
    using THeader = std::vector<std::pair<std::string, std::string>>;

    FLUENT_SETTING(std::string, TraceId);
    FLUENT_SETTING(std::string, RequestType);
    FLUENT_SETTING(THeader, Header);
    FLUENT_SETTING(TDuration, ClientTimeout);
    FLUENT_SETTING(std::string, TraceParent);

    TRequestSettings() = default;

    template <typename T>
    explicit TRequestSettings(const TRequestSettings<T>& other)
        : TraceId_(other.TraceId_)
        , RequestType_(other.RequestType_)
        , Header_(other.Header_)
        , ClientTimeout_(other.ClientTimeout_)
        , TraceParent_(other.TraceParent_)
    {}
};

template<typename TDerived>
struct TSimpleRequestSettings : public TRequestSettings<TDerived> {
    using TSelf = TDerived;

    TSimpleRequestSettings() = default;

    template <typename T>
    explicit TSimpleRequestSettings(const TSimpleRequestSettings<T>& other)
        : TRequestSettings<TDerived>(other)
    {}
};

template<typename TDerived>
struct TOperationRequestSettings : public TSimpleRequestSettings<TDerived> {
    using TSelf = TDerived;

    /* Cancel/timeout operation settings available from 18-8 YDB server version */
    FLUENT_SETTING(TDuration, OperationTimeout);
    FLUENT_SETTING(TDuration, CancelAfter);
    FLUENT_SETTING(TDuration, ForgetAfter);
    FLUENT_SETTING_DEFAULT(bool, UseClientTimeoutForOperation, true);
    FLUENT_SETTING_DEFAULT(bool, ReportCostInfo, false);

    TOperationRequestSettings() = default;

    template <typename T>
    explicit TOperationRequestSettings(const TOperationRequestSettings<T>& other)
        : TSimpleRequestSettings<TDerived>(other)
        , OperationTimeout_(other.OperationTimeout_)
        , CancelAfter_(other.CancelAfter_)
        , ForgetAfter_(other.ForgetAfter_)
        , UseClientTimeoutForOperation_(other.UseClientTimeoutForOperation_)
        , ReportCostInfo_(other.ReportCostInfo_)
    {}

    TSelf& CancelAfterWithTimeout(const TDuration& cancelAfter, const TDuration& operationTimeout) {
        CancelAfter_ = cancelAfter;
        OperationTimeout_ = operationTimeout;
        return static_cast<TSelf&>(*this);
    }
};

} // namespace NYdb
