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
    const bool isDefault = control->IsDefault();
    ChangedCount += !isDefault;
    auto& __stream = *Html;
    TABLER() {
        TABLED() { HtmlStrm << name; }
        TABLED() { HtmlStrm << control->RangeAsString(); }
        TABLED() {
            if (isDefault) {
                HtmlStrm << "<p>" << control->Get() << "</p>";
            } else {
                HtmlStrm << "<p style='color:red;'><b>" << control->Get() << " </b></p>";
            }
        }
        TABLED() {
            if (isDefault) {
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
            if (!isDefault) {
                HtmlStrm << "<button type='submit' name='restoreDefault' value='" << name
                    << "' style='color:green; margin-left:4px; white-space:nowrap;'>"
                    << "<b>Restore Default</b></button>";
            }
            HtmlStrm  << "</form>";
        }
        TABLED() { HtmlStrm << !isDefault; }
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

ui64 TControlBoardTableHtmlRenderer::GetChangedCount() const {
    return ChangedCount;
}

} // NKikimr
