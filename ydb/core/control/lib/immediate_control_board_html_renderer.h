#pragma once

#include "immediate_control_board_control.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>
#include <util/generic/maybe.h>

namespace NKikimr {

// ICB registry that owns a control rendered on the HTML page.
enum class EControlBoardType {
    Static,
    Dynamic,
};

class TControlBoardTableHtmlRenderer : TNonCopyable {
public:
    TControlBoardTableHtmlRenderer();
    void AddNewTable(const TString& caption, EControlBoardType controlBoardType);
    void AddTableItem(const TString& name, TIntrusivePtr<TControl> control);
    TString GetHtml();

private:
    TStringStream HtmlStrm;
    TMaybe<NMonitoring::TOutputStreamRef> Html;
    TMaybe<NMonitoring::TTable> Table;
    TMaybe<NMonitoring::TTableBody> TableBody;

    // Registry used to route per-control actions back to the owning board.
    EControlBoardType ControlBoardType = EControlBoardType::Static;
};

}
