#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NPlan2Svg {

constexpr ui32 INDENT_X = 8;
constexpr ui32 GAP_X = 3;
constexpr ui32 GAP_Y = 3;
constexpr ui32 TIME_HEIGHT = 10;
constexpr ui32 INTERNAL_GAP_Y = 2;
constexpr ui32 INTERNAL_GAP_X = 2;
constexpr ui32 CONN_SIZE = 14;
constexpr ui32 INTERNAL_HEIGHT = 14;
constexpr ui32 INTERNAL_WIDTH = 16;
constexpr ui32 INTERNAL_TEXT_HEIGHT = 8;
constexpr ui32 TIME_SERIES_RANGES = 32;
constexpr ui32 CONN_ARROW = 4;

struct TColorPalette {
    TColorPalette();
    TString StageMain;
    TString StageClone;
    TString StageText;
    TString StageTextHighlight;
    TString StageGrid;
    TString IngressDark;
    TString IngressMedium;
    TString IngressLight;
    TString InputDark;
    TString InputMedium;
    TString InputLight;
    TString EgressDark;
    TString EgressMedium;
    TString EgressLight;
    TString OutputDark;
    TString OutputMedium;
    TString OutputLight;
    TString MemMedium;
    TString MemLight;
    TString CpuMedium;
    TString CpuLight;
    TString ConnectionFill;
    TString ConnectionLine;
    TString ConnectionText;
    TString MinMaxLine;
    TString TextLight;
    TString TextInverted;
    TString TextSummary;
    TString SpillingBytesMedium;
    TString SpillingBytesLight;
    TString SpillingTimeMedium;
    TString BlockMedium;
};

struct TPlanViewConfig {
    TPlanViewConfig();
    ui32 HeaderLeft;
    ui32 HeaderWidth;
    ui32 OperatorLeft;
    ui32 OperatorWidth;
    ui32 TaskLeft;
    ui32 TaskWidth;
    ui32 SummaryLeft;
    ui32 SummaryWidth;
    ui32 TimelineLeft;
    ui32 TimelineWidth;
    ui32 Width;
    TColorPalette Palette;
    bool Simplified = false;
};

} // namespace NPlan2Svg
