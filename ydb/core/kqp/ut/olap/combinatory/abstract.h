#pragma once
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

class ICommand {
private:
    virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) = 0;

public:
    virtual ~ICommand() = default;

    TConclusionStatus Execute(TKikimrRunner& kikimr) {
        return DoExecute(kikimr);
    }
};

}   // namespace NKikimr::NKqp
