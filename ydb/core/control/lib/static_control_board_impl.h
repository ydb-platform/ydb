#pragma once

#include "defs.h"
#include "immediate_control_board_wrapper.h"
#include "immediate_control_board_html_renderer.h"

#include <ydb/core/control/lib/defs.h_serialized.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/generic/serialized_enum.h>


namespace NKikimr {

class TStaticControlBoard: public TThrRefBase {
private:
    using TPtr = THotSwap<TControl>;
    std::array<TPtr,  GetEnumItemsCount<EStaticControlType>()> StaticControls;
private:
    size_t GetIndex(EStaticControlType type) const;
public:
    bool RegisterLocalControl(TControlWrapper control, EStaticControlType type);

    bool RegisterSharedControl(TControlWrapper& control, EStaticControlType type);

    void RestoreDefault(EStaticControlType tpe);

    void RestoreDefaults();

    bool SetValue(EStaticControlType name, TAtomic value, TAtomic &outPrevValue);

    // Only for tests
    void GetValue(EStaticControlType name, TAtomic &outValue, bool &outIsControlExists) const;

    void RenderAsHtml(TControlBoardTableHtmlRenderer& renderer) const;

    TMaybe<EStaticControlType> GetStaticControlId(const TStringBuf name) const;
};

}
