#pragma once

#include <util/generic/strbuf.h>
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

// Every renderer that draws one kind of thing - a data flow, memory, CPU - wants
// the same shades of one color, so they travel together instead of being passed
// one at a time. Dark is empty for the groups that never draw a derivative curve
// over their bar.
struct TColorTriple {
    TStringBuf Medium;
    TStringBuf Light;
    TStringBuf Dark;
};

// Each entry names a CSS custom property with the built-in shade as its
// fallback, so a page embedding the SVG can restyle it without regenerating.
struct TColorPalette {
    TStringBuf StageMain          = "var(--stage-main, #F2F2F2)";
    TStringBuf StageClone         = "var(--stage-clone, #D9D9D9)";
    TStringBuf StageText          = "var(--stage-text, #262626)";
    TStringBuf StageTextHighlight = "var(--stage-texthl, #FC2824)";
    TStringBuf StageGrid          = "var(--stage-grid, #B2B2B2)";

    TColorTriple Ingress = {"var(--ingress-medium, #466364)", "var(--ingress-light, #5A8183)", "var(--ingress-dark, #384F50)"};
    TColorTriple Input   = {"var(--input-medium, #5A8183)",   "var(--input-light, #7CA3A5)",   "var(--input-dark, #466364)"};
    TColorTriple Egress  = {"var(--egress-medium, #3C6090)",  "var(--egress-light, #4B78B4)",  "var(--egress-dark, #2D486C)"};
    TColorTriple Output  = {"var(--output-medium, #5781B9)",  "var(--output-light, #6F93C3)",  "var(--output-dark, #41689C)"};

    TColorTriple Mem = {"var(--mem-medium, #7E4E5B)", "var(--mem-light, #AA7785)", ""};
    TColorTriple Cpu = {"var(--cpu-medium, #A36D7B)", "var(--cpu-light, #B78C98)", ""};
    TColorTriple SpillingBytes = {"var(--spill-medium, #FFC522)", "var(--spill-light, #FFD766)", ""};

    TStringBuf ConnectionFill = "var(--conn-fill, #BFBFBF)";
    TStringBuf ConnectionLine = "var(--conn-line, #BFBFBF)";
    TStringBuf ConnectionText = "var(--conn-text, #393939)";
    TStringBuf MinMaxLine     = "var(--minmax-line, #FFDB4D)";
    TStringBuf TextLight      = "var(--text-light, #FFFFFF)";
    TStringBuf TextInverted   = "var(--text-inv, #FFFFFF)";
    TStringBuf TextSummary    = "var(--text-summary, #262626)";

    TStringBuf SpillingTimeMedium = "var(--spill-medium, #FFC522)";
    TStringBuf BlockMedium        = "var(--block-medium, #D9AE61)";
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
