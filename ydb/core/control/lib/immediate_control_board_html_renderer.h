#pragma once

#include "immediate_control_board_control.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>
#include <util/generic/maybe.h>

namespace NKikimr {

class TControlBoardTableHtmlRenderer : TNonCopyable {
private:
    TStringStream HtmlStrm;
    TMaybe<NMonitoring::TOutputStreamRef> Html;
    TMaybe<NMonitoring::TTable> Table;
    TMaybe<NMonitoring::TTableBody> TableBody;

    // Controls with an active override in the snapshots rendered across all tables.
    ui64 OverriddenCount = 0;
public:
    TControlBoardTableHtmlRenderer();
    void AddNewTable(const TString& caption);
    void AddTableItem(const TString& name, TIntrusivePtr<TControl> control);
    TString GetHtml();

    // Return the number of rendered controls with an active override.
    ui64 GetOverriddenCount() const;
};

}

