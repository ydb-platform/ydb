#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/conclusion/status.h>

#include <util/string/cast.h>

namespace NKikimr::NKqp {

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
