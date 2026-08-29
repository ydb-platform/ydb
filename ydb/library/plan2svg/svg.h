#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NPlan2Svg {

TString SvgRect(ui32 x, ui32 y, ui32 w, const TString& h, const TString& cssClass);
TString SvgRect(ui32 x, ui32 y, ui32 w, ui32 h, const TString& cssClass);
TString SvgText(const TString& x, const TString& y, const TString& cssClass, TStringBuf text);
TString SvgText(ui32 x, const TString& y, const TString& cssClass, TStringBuf text);
TString SvgText(ui32 x, ui32 y, const TString& cssClass, TStringBuf text);
TString SvgTextS(ui32 x, ui32 y, TStringBuf text);
TString SvgTextM(ui32 x, ui32 y, TStringBuf text);
TString SvgTextE(ui32 x, ui32 y, TStringBuf text);
TString SvgCircle(ui32 x, ui32 y, const TString& cssClass, const TString& opacity = "");
TString SvgStageId(ui32 x, ui32 y, const TString& id, const TString& opacity = "");
TString SvgLine(ui32 x1, ui32 y1, ui32 x2, ui32 y2, const TString& cssClass);

} // namespace NPlan2Svg
