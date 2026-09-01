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

    // Restore the default; return false for an unknown name without changing output.
    bool RestoreDefault(TString name, TControlMutation& outMutation);

    bool SetValue(TString name, TAtomic value, TAtomic &outPrevValue);

    // Apply an HTML-style value; return false for an unknown name without changing output.
    bool SetValue(TString name, TAtomic value, TControlMutation& outMutation);

    // Only for tests
    void GetValue(TString name, TAtomic &outValue, bool &outIsControlExists) const;

    void RenderAsHtml(TControlBoardTableHtmlRenderer& renderer) const;
};

}
