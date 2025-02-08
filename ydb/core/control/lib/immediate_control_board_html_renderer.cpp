#include "immediate_control_board_html_renderer.h"

namespace NKikimr {
TControlBoardTableHtmlRenderer::TControlBoardTableHtmlRenderer()
    : Html(NMonitoring::TOutputStreamRef(HtmlStrm))
    , Table(NMonitoring::TTable(*Html, "table table-sortable")) {
    auto& __stream = *Html;
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
    auto& __stream = *Html;
    TABLER() {
        TABLED() { HtmlStrm << name; }
        TABLED() { HtmlStrm << control->RangeAsString(); }
        TABLED() {
            if (control->IsDefault()) {
                HtmlStrm << "<p>" << control->Get() << "</p>";
            } else {
                HtmlStrm << "<p style='color:red;'><b>" << control->Get() << " </b></p>";
            }
        }
        TABLED() {
            if (control->IsDefault()) {
                HtmlStrm << "<p>" << control->GetDefault() << "</p>";
            } else {
                HtmlStrm << "<p style='color:red;'><b>" << control->GetDefault() << " </b></p>";
            }
        }
        TABLED() {
            HtmlStrm << "<form class='form_horizontal' method='post'>";
            HtmlStrm << "<input name='" << name << "' type='text' value='"
                << control->Get() << "'/>";
            HtmlStrm  << "<button type='submit' style='color:red;'><b>Change</b></button>";
            HtmlStrm  << "</form>";
        }
        TABLED() { HtmlStrm << !control->IsDefault(); }
    }
}

TString TControlBoardTableHtmlRenderer::GetHtml() {
    TableBody.Clear();
    Table.Clear();
    HtmlStrm << "<form class='form_horizontal' method='post'>";
    HtmlStrm << "<button type='submit' name='restoreDefaults' style='color:green;'><b>Restore Default</b></button>";
    HtmlStrm << "</form>";
    Html.Clear();
    return HtmlStrm.Str();
}

} // NKikimr
