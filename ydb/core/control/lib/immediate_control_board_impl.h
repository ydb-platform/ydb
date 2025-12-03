#pragma once

#include "immediate_control_board_wrapper.h"

#include <ydb/core/control/lib/generated/control_board_proto.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

namespace NKikimr {

class TImmediateControlActor;
class TControlBoardTableHtmlRenderer;

class TControlBoard: public TControlBoardBase {
    friend class TImmediateControlActor;
private:
    TIntrusivePtr<TControl> GetControlByName(const TString& name) const;
public:
    void RestoreDefaults();
    void RenderAsHtml(TControlBoardTableHtmlRenderer& renderer) const;

    static void RegisterSharedControl(TControlWrapper& control, THotSwap<TControl>& icbControl);
    static void RegisterLocalControl(TControlWrapper control, THotSwap<TControl>& icbControl);
    static void SetValue(TAtomicBase value, THotSwap<TControl>& icbControl);
};

}
