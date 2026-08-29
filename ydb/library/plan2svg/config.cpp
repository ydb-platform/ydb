#include "config.h"

namespace NPlan2Svg {

TColorPalette::TColorPalette() {
    StageMain     = "var(--stage-main, #F2F2F2)";
    StageClone    = "var(--stage-clone, #D9D9D9)";
    StageText     = "var(--stage-text, #262626)";
    StageTextHighlight = "var(--stage-texthl, #FC2824)";
    StageGrid     = "var(--stage-grid, #B2B2B2)";

    IngressDark   = "var(--ingress-dark, #384F50)";
    IngressMedium = "var(--ingress-medium, #466364)";
    IngressLight  = "var(--ingress-light, #5A8183)";
    InputDark     = "var(--input-dark, #466364)";
    InputMedium   = "var(--input-medium, #5A8183)";
    InputLight    = "var(--input-light, #7CA3A5)";

    EgressDark    = "var(--egress-dark, #2D486C)";
    EgressMedium  = "var(--egress-medium, #3C6090)";
    EgressLight   = "var(--egress-light, #4B78B4)";
    OutputDark    = "var(--output-dark, #41689C)";
    OutputMedium  = "var(--output-medium, #5781B9)";
    OutputLight   = "var(--output-light, #6F93C3)";

    MemMedium     = "var(--mem-medium, #7E4E5B)";
    MemLight      = "var(--mem-light, #AA7785)";
    CpuMedium     = "var(--cpu-medium, #A36D7B)";
    CpuLight      = "var(--cpu-light, #B78C98)";

    ConnectionFill= "var(--conn-fill, #BFBFBF)";
    ConnectionLine= "var(--conn-line, #BFBFBF)";
    ConnectionText= "var(--conn-text, #393939)";
    MinMaxLine    = "var(--minmax-line, #FFDB4D)";
    TextLight     = "var(--text-light, #FFFFFF)";
    TextInverted  = "var(--text-inv, #FFFFFF)";
    TextSummary   = "var(--text-summary, #262626)";

    SpillingBytesMedium = "var(--spill-medium, #FFC522)";
    SpillingBytesLight  = "var(--spill-light, #FFD766)";
    SpillingTimeMedium  = "var(--spill-medium, #FFC522)";

    BlockMedium = "var(--block-medium, #D9AE61)";
}

TPlanViewConfig::TPlanViewConfig() {
    Width = 1280;
    HeaderLeft = 0;
    HeaderWidth = 300 - INTERNAL_GAP_X;
    OperatorLeft = HeaderLeft + HeaderWidth + GAP_X;
    OperatorWidth = 64;
    TaskLeft = OperatorLeft + OperatorWidth + GAP_X;
    TaskWidth = 24;
    SummaryLeft = TaskLeft + TaskWidth + GAP_X;
    SummaryWidth = 200;
    TimelineLeft = SummaryLeft + SummaryWidth + GAP_X;
    TimelineWidth = Width - TimelineLeft;
}


} // namespace NPlan2Svg
