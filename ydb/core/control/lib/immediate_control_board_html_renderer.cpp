#include "immediate_control_board_html_renderer.h"

namespace NKikimr {
TControlBoardTableHtmlRenderer::TControlBoardTableHtmlRenderer()
    : Html(NMonitoring::TOutputStreamRef(HtmlStrm))
{
    Table.ConstructInPlace(*Html, "table table-sortable");
}

void TControlBoardTableHtmlRenderer::AddNewTable(
    const TString& caption,
    EControlBoardType controlBoardType)
{
    ControlBoardType = controlBoardType;
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
    auto& __stream = *Html;
    TABLER() {
        TABLED() { HtmlStrm << name; }
        TABLED() { HtmlStrm << control->RangeAsString(); }
        TABLED() {
            if (!state.Overridden) {
                HtmlStrm << "<p>" << state.Value << "</p>";
            } else {
                HtmlStrm << "<p style='color:red;'><b>" << state.Value
                    << " </b><span>override</span></p>";
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
            HtmlStrm << "<form class='form_horizontal' method='post'>";
            HtmlStrm << "<input name='" << name << "' type='text' value='"
                << state.Value << "'/>";
            HtmlStrm  << "<button type='submit' style='color:red;'><b>Change</b></button>";
            HtmlStrm  << "</form>";
            if (state.Overridden) {
                HtmlStrm << "<form class='form_horizontal' method='post'>";
                HtmlStrm << "<input type='hidden' name='__icb_action' value='resetOverride'/>";
                HtmlStrm << "<input type='hidden' name='__icb_board' value='"
                    << (ControlBoardType == EControlBoardType::Static ? "static" : "dynamic")
                    << "'/>";
                HtmlStrm << "<input type='hidden' name='__icb_control' value='"
                    << name << "'/>";
                HtmlStrm << "<button type='submit' style='color:green;'>"
                    << "<b>Reset override</b></button>";
                HtmlStrm << "</form>";
            }
        }
        TABLED() { HtmlStrm << state.Overridden; }
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
