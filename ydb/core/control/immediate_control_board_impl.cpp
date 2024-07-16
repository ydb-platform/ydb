#include "immediate_control_board_impl.h"
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

void TControlBoard::RenderAsHtmlTableRows(TStringStream& str) const {
    HTML(str) {
        ICB_RENDER_HTML_TABLE_ROWS(*this, str);
    }
}

void TControlBoard::Assign(TIntrusivePtr<TControl>& to, TControlWrapper& from) {
    to = from.Control;
}

void TControlBoard::Assign(TControlWrapper& to, TIntrusivePtr<TControl>& from) {
    to.Control = from;
}

} // namespace NKikimr