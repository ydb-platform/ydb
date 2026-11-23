#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/conclusion/status.h>

#include <util/string/cast.h>

namespace NKikimr::NKqp {

// Live-object counters of the column shard read path. Both are released asynchronously: the scan actor drops
// the data sources and the memory scope guard after the last result has been sent, and the grouped memory
// limiter actor removes the scope when it handles the resulting event. A client may therefore see the query
// result while the counters are still non-zero.
struct TBackgroundProcessesState {
    ui64 ProcessScopeCount = 0;
    ui64 DataSourceCount = 0;
    // How long WaitBackgroundProcesses polled before returning, zero if the counters were clean at the first check.
    TDuration Waited;

    bool IsClean() const {
        return ProcessScopeCount == 0 && DataSourceCount == 0;
    }

    static TBackgroundProcessesState Current();
};

// Polls the counters until both are zero or the timeout expires and returns the last observed state.
TBackgroundProcessesState WaitBackgroundProcesses(const TDuration timeout);

class TWaitBackgroundProcessesCommand: public ICommand {
private:
    ui32 TimeoutSeconds = 30;

    virtual std::set<TString> DoGetCommandProperties() const override {
        return { "TIMEOUT" };
    }

    virtual TConclusionStatus DoDeserializeProperties(const TPropertiesCollection& props) override {
        if (auto timeout = props.GetOptional("TIMEOUT")) {
            if (!TryFromString(*timeout, TimeoutSeconds)) {
                return TConclusionStatus::Fail("Invalid timeout value: " + *timeout);
            }
        }
        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override;

public:
    TWaitBackgroundProcessesCommand() = default;
};

}   // namespace NKikimr::NKqp
