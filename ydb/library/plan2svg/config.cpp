#include "config.h"

namespace NPlan2Svg {

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
