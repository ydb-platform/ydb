#include "immediate_control_board_html_renderer.h"

namespace NKikimr {
TControlBoardTableHtmlRenderer::TControlBoardTableHtmlRenderer()
    : Html(NMonitoring::TOutputStreamRef(HtmlStrm))
{
    Table.ConstructInPlace(*Html, "table table-sortable");
}

void TControlBoardTableHtmlRenderer::AddNewTable(const TString& caption) {
    if (TableBody) {
        TableBody.Clear(); //Closing existing table
        Table.Clear();
        Table.ConstructInPlace(*Html, "table table-sortable");
    }

    auto& __stream = *Html;
    CAPTION() {
        __stream << caption;
    }
    TABLEHEAD() {
        TABLER() {
            TABLEH() { HtmlStrm << "Parameter"; }
            TABLEH() { HtmlStrm << "Acceptable range"; }
            TABLEH() { HtmlStrm << "Current"; }
            TABLEH() { HtmlStrm << "Default"; }
            TABLEH() { HtmlStrm << "Send new value"; }
            TABLEH() { HtmlStrm << "Changed"; }
        }
    }
    TableBody.ConstructInPlace(__stream);
}

void TControlBoardTableHtmlRenderer::AddTableItem(const TString& name, TIntrusivePtr<TControl> control) {
    Y_ENSURE(!!TableBody);
    const TControlState state = control->GetState();
    OverriddenCount += state.Overridden;
    auto& __stream = *Html;
    TABLER() {
        TABLED() { HtmlStrm << name; }
        TABLED() { HtmlStrm << control->RangeAsString(); }
        TABLED() {
            if (!state.Overridden) {
                HtmlStrm << "<p>" << state.Value << "</p>";
            } else {
                HtmlStrm << "<p style='color:red;'><b>" << state.Value << "</b>";
                if (state.Value == state.Default) {
                    HtmlStrm << "<br/><span>(overridden)</span>";
                }
                HtmlStrm << "</p>";
            }
        }
        TABLED() {
            if (!state.Overridden) {
                HtmlStrm << "<p>" << state.Default << "</p>";
            } else {
                HtmlStrm << "<p style='color:red;'><b>" << state.Default << " </b></p>";
            }
        }
        TABLED() {
            HtmlStrm << "<form class='form_horizontal' method='post' style='margin:0;'>";
            HtmlStrm << "<input name='" << name << "' type='text' value='" << state.Value << "'/>";
            HtmlStrm << "<button type='submit' style='color:red;'>" << "<b>Change</b></button>";
            if (state.Overridden) {
                HtmlStrm << "<button type='submit' name='restoreDefault' value='" << name << "'"
                    << " style='color:green; margin-left:4px; white-space:nowrap;'>"
                    << "<b>Restore Default</b></button>";
            }
            HtmlStrm << "</form>";
        }
        TABLED() { HtmlStrm << state.Overridden; }
    }
}

TString TControlBoardTableHtmlRenderer::GetHtml() {
    TableBody.Clear();
    Table.Clear();
    HtmlStrm << "<form class='form_horizontal' method='post'>";
    HtmlStrm << "<button type='submit' name='restoreDefaults' style='color:green;'><b>Restore Defaults</b></button>";
    HtmlStrm << "</form>";
    Html.Clear();
    return HtmlStrm.Str();
}

ui64 TControlBoardTableHtmlRenderer::GetOverriddenCount() const {
    return OverriddenCount;
}

} // NKikimr
