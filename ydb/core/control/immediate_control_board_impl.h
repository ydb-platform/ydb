#pragma once

#include "defs.h"
#include "immediate_control_board_wrapper.h"

#include <ydb/core/util/concurrent_rw_hash.h>

namespace NKikimr {

class TControlBoard : public TThrRefBase {
private:
    TConcurrentRWHashMap<TString, TIntrusivePtr<TControl>, 16> Board;

public:
    bool RegisterLocalControl(TControlWrapper control, TString name);

    bool RegisterSharedControl(TControlWrapper& control, TString name);

    void RestoreDefaults();

    void RestoreDefault(TString name);

    bool SetValue(TString name, TAtomic value, TAtomic &outPrevValue);

    // Only for tests
    void GetValue(TString name, TAtomic &outValue, bool &outIsControlExists) const;

    TString RenderAsHtml() const;
};

}
