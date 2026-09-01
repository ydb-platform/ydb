#include "svg.h"

#include "config.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NPlan2Svg {

TString SvgEscape(TStringBuf text) {
    size_t extra = 0;
    for (char c : text) {
        switch (c) {
            case '&': extra += 4; break; // &amp;
            case '<': extra += 3; break; // &lt;
            case '>': extra += 3; break; // &gt;
            default: break;
        }
    }

    if (extra == 0) {
        return TString(text);
    }

    TString result;
    result.reserve(text.size() + extra);
    for (char c : text) {
        switch (c) {
            case '&': result += "&amp;"; break;
            case '<': result += "&lt;"; break;
            case '>': result += "&gt;"; break;
            default: result += c; break;
        }
    }
    return result;
}

TString SvgRect(ui32 x, ui32 y, ui32 w, const TString& h, const TString& cssClass) {
    return TStringBuilder()
        << "<rect x='" << x << "' y='" << y << "' width='" << w << "' height='" << h
        << "' class='" << cssClass << "'/>" << Endl;
}

TString SvgRect(ui32 x, ui32 y, ui32 w, ui32 h, const TString& cssClass) {
    return SvgRect(x, y, w, ToString(h), cssClass);
}

TString SvgText(const TString& x, const TString& y, const TString& cssClass, TStringBuf text) {
    return TStringBuilder() << "<text x='" << x << "' y='" << y << "' class='" << cssClass << "'>" << SvgEscape(text) << "</text>" << Endl;
}

TString SvgText(ui32 x, const TString& y, const TString& cssClass, TStringBuf text) {
    return SvgText(ToString(x), y, cssClass, text);
}

TString SvgText(ui32 x, ui32 y, const TString& cssClass, TStringBuf text) {
    return SvgText(ToString(x), ToString(y), cssClass, text);
}

TString SvgTextS(ui32 x, ui32 y, TStringBuf text) {
    return SvgText(x, y, "texts", text);
}

TString SvgTextM(ui32 x, ui32 y, TStringBuf text) {
    return SvgText(x, y, "textm", text);
}

TString SvgTextE(ui32 x, ui32 y, TStringBuf text) {
    return SvgText(x, y, "texte", text);
}

TString SvgCircle(ui32 x, ui32 y, const TString& cssClass, const TString& opacity) {
    TStringBuilder builder;
    builder << "<circle cx='" << x << "' cy='" << y << "' r='" << INTERNAL_WIDTH / 2 - 1 << "' class='" << cssClass;
    if (opacity) {
        builder << "' opacity='" << opacity;
    }
    builder << "' />" << Endl;
    return builder;
}

TString SvgStageId(ui32 x, ui32 y, const TString& id, const TString& opacity) {
    return TStringBuilder() << SvgCircle(x, y, "stage", opacity) <<  SvgTextM(x, y + INTERNAL_TEXT_HEIGHT / 2, id);
}

TString SvgLine(ui32 x1, ui32 y1, ui32 x2, ui32 y2, const TString& cssClass) {
    return TStringBuilder() << "<line x1='" << x1 << "' y1='" << y1 << "' x2='" << x2 << "' y2='" << y2 << "' class='" << cssClass << "' />" << Endl;
}

} // namespace NPlan2Svg
