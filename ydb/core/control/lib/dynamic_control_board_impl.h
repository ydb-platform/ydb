#pragma once

#include "immediate_control_board_wrapper.h"

#include <ydb/core/util/concurrent_rw_hash.h>

namespace NKikimr {

class TControlBoardTableHtmlRenderer;

class TDynamicControlBoard : public TThrRefBase {
private:
    TConcurrentRWHashMap<TString, TIntrusivePtr<TControl>, 16> Board;
public:
    bool RegisterLocalControl(TControlWrapper control, TString name);

    bool RegisterSharedControl(TControlWrapper& control, TString name);

    void RestoreDefaults();

    void RestoreDefault(TString name);

    bool SetValue(TString name, TAtomic value, TAtomic &outPrevValue);
    bool SetValue(TString name, TAtomic value, TControlMutation& outMutation);
    bool ClearOverride(TString name, TControlMutation& outMutation);

    ui64 GetOverriddenCount() const;

    // Only for tests
    void GetValue(TString name, TAtomic &outValue, bool &outIsControlExists) const;

    void RenderAsHtml(TControlBoardTableHtmlRenderer& renderer) const;
};

}
