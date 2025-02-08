#pragma once

#include "immediate_control_board_control.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>

namespace NKikimr {

class TControlBoardTableHtmlRenderer : TNonCopyable {
private:
    TStringStream HtmlStrm;
    TMaybe<NMonitoring::TOutputStreamRef> Html;
    TMaybe<NMonitoring::TTable> Table;
    TMaybe<NMonitoring::TTableBody> TableBody;
public:
    TControlBoardTableHtmlRenderer();
    void AddTableItem(const TString& name, TIntrusivePtr<TControl> control);
    TString GetHtml();
};

}

