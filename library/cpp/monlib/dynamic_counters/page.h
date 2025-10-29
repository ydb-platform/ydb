#pragma once

#include "counters.h"

#include <library/cpp/monlib/encode/format.h>
#include <library/cpp/monlib/service/pages/pre_mon_page.h>

#include <util/generic/ptr.h>

#include <functional>

namespace NMonitoring {
    enum class EUnknownGroupPolicy {
        Error,  // send 404
        Ignore, // send 204
    };

    struct TDynamicCountersPage: public TPreMonPage {
    public:
        using TOutputCallback = std::function<void()>;
        using TFilterCallback = std::function<THolder<ICountableConsumer>(const TString& setName, THolder<ICountableConsumer>&& nextConsumer)>;

    private:
        const TIntrusivePtr<TDynamicCounters> Counters;
        TOutputCallback OutputCallback;
        TFilterCallback FilterCallback;
        EUnknownGroupPolicy UnknownGroupPolicy {EUnknownGroupPolicy::Error};

    private:
        void HandleAbsentSubgroup(IMonHttpRequest& request);

    public:
        TDynamicCountersPage(const TString& path,
                             const TString& title,
                             TIntrusivePtr<TDynamicCounters> counters,
                             TOutputCallback outputCallback = nullptr,
                             TFilterCallback filterCallback = nullptr)
            : TPreMonPage(path, title)
            , Counters(counters)
            , OutputCallback(outputCallback)
            , FilterCallback(filterCallback)
        {
        }

        void Output(NMonitoring::IMonHttpRequest& request) override;

        void BeforePre(NMonitoring::IMonHttpRequest& request) override;

        void OutputText(IOutputStream& out, NMonitoring::IMonHttpRequest&) override;

        /// If set to Error, responds with 404 if the requested subgroup is not found. This is the default.
        /// If set to Ignore, responds with 204 if the requested subgroup is not found
        void SetUnknownGroupPolicy(EUnknownGroupPolicy value);

    protected:
        virtual THolder<ICountableConsumer> CreateEncoder(IOutputStream* out, EFormat format, TStringBuf nameLabel, TCountableBase::EVisibility visibility) const;
    };
}
