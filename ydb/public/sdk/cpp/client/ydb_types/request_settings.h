#pragma once

#include "fluent_settings_helpers.h"

#include <util/datetime/base.h>

#include <vector>
#include <utility>

namespace NYdb {

template<typename TDerived>
struct TRequestSettings {
    using TSelf = TDerived;
    using THeader = std::vector<std::pair<TString, TString>>;

    FLUENT_SETTING(TString, TraceId);
    FLUENT_SETTING(TString, RequestType);
    FLUENT_SETTING(THeader, Header);
    FLUENT_SETTING(TDuration, ClientTimeout);

    TRequestSettings() = default;

    template <typename T>
    explicit TRequestSettings(const TRequestSettings<T>& other)
        : TraceId_(other.TraceId_)
        , RequestType_(other.RequestType_)
        , Header_(other.Header_)
        , ClientTimeout_(other.ClientTimeout_)
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
