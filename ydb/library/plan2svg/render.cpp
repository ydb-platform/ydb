#include "visualizer.h"

#include "format.h"
#include "parse.h"
#include "svg.h"

#include <library/cpp/resource/resource.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#include <limits>

#include <limits>
#include <iterator>
#include <set>

namespace NPlan2Svg {

void TPlan::PrintTimeline(TStringBuilder& background, TStringBuilder& canvas, const TString& title, TAggregation& firstMessage, TAggregation& lastMessage, ui32 x, ui32 y, ui32 w, ui32 h, TStringBuf color, bool backgroundRect) {

    auto firstMin = firstMessage.Min * w / MaxTime;
    auto lastMax = lastMessage.Max * w / MaxTime;

    background
        << "<g><title>" << title << ", Duration: " << FormatTimeMs(lastMessage.Max - firstMessage.Min) << " (" << FormatTimeAgg(firstMessage) << " - " << FormatTimeAgg(lastMessage) << ")</title>";

    if (backgroundRect) {
        background << SvgRect(Config.TimelineLeft, y, Config.TimelineWidth, h, "background");
    }

    background
        << "<rect x='" << x + firstMin << "' y='" << y
        << "' width='" << lastMax - firstMin + 1 << "' height='" << h
        << "' stroke-width='0' fill='" << color << "'/>" << Endl;

    if (firstMessage.Min < firstMessage.Max) {
        auto firstAvg = firstMessage.Avg * w / MaxTime;
        auto firstMax = firstMessage.Max * w / MaxTime;
        canvas
            << "  <line x1='" << x + firstMin << "' y1='" << y + 2
            << "' x2='" << x + firstMax << "' y2='" << y + 2
            << "' stroke-width='3' stroke='" << Config.Palette.MinMaxLine << "' stroke-dasharray='1,1' />" << Endl
            << "  <line x1='" << x + firstAvg << "' y1='" << y
            << "' x2='" << x + firstAvg << "' y2='" << y + h / 2
            << "' stroke-width='3' stroke='" << Config.Palette.MinMaxLine << "' />" << Endl;
    }

    if (lastMessage.Min < lastMessage.Max) {
        auto lastMin = lastMessage.Min * w / MaxTime;
        auto lastAvg = lastMessage.Avg * w / MaxTime;
        canvas
            << "  <line x1='" << x + lastMin << "' y1='" << y + h - 2
            << "' x2='" << x + lastMax << "' y2='" << y + h - 2
            << "' stroke-width='3' stroke='" << Config.Palette.MinMaxLine << "' stroke-dasharray='1,1' />" << Endl
            << "  <line x1='" << x + lastAvg << "' y1='" << y + h / 2
            << "' x2='" << x + lastAvg << "' y2='" << y + h
            << "' stroke-width='3' stroke='" << Config.Palette.MinMaxLine << "' />" << Endl;
    }

    background
        << "</g>" << Endl;
}

void TPlan::PrintWaitTime(TStringBuilder& background, const std::shared_ptr<TSingleMetric>& metric, ui32 x, ui32 y, ui32 w, ui32 h, TStringBuf fillColor) {

    if (metric->WaitTime.MaxDeriv == 0) {
        return;
    }

    background
        << "<polygon points='"
        << x + metric->WaitTime.Deriv.front().first * w / MaxTime << "," << y + (h - 1) << " ";

    for (auto& item : metric->WaitTime.Deriv) {
        background << x + item.first * w / MaxTime << "," << y + (h - std::max<ui32>(item.second * h / metric->WaitTime.MaxDeriv, 1)) << " ";
    }

    background
        << x + metric->WaitTime.Deriv.back().first * w / MaxTime << "," << y + (h - 1) << " "
        << "' stroke='none' fill='" << fillColor << "' />" << Endl;
}

void TPlan::PrintSeries(TStringBuilder& canvas, const std::vector<std::pair<ui64, ui64>>& series, ui64 maxValue, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, TStringBuf lineColor, TStringBuf fillColor, bool closed) {
    if (MaxTime == 0 || maxValue == 0 || series.empty()) {
        return;
    }
    if (title) {
        canvas << "<g><title>" << title << "</title>" << Endl;
    }
    i32 px0 = x + series.front().first * w / MaxTime;
    i32 py0 = y + (h - 1);
    if (!closed) {
        py0 = y + (h - std::max<ui32>(series.front().second * h / maxValue, 1));
    }
    canvas << "<path d='M" << px0 << ',' << py0;
    for (auto& item : series) {
        i32 px = x + item.first * w / MaxTime;
        i32 py = y + (h - std::max<ui32>(item.second * h / maxValue, 1));
        if (px != px0 || py != py0) {
            // we use integer arithmetics, ignore low-resolution spikes
            canvas
                << "c" << (px0 * 2 + px) / 3 - px0 << ',' << py0 - py0 << ',' << (px0 + px * 2) / 3 - px0 << ',' << py - py0 << ',' << px - px0 << ',' << py - py0;
            px0 = px;
            py0 = py;
        }
    }
    if (closed) {
        i32 px = x + series.back().first * w / MaxTime;
        i32 py = y + (h - 1);
        canvas
        << "c" << (px0 * 2 + px) / 3 - px0 << ',' << py0 - py0 << ',' << (px0 + px * 2) / 3 - px0 << ',' << py - py0 << ',' << px - px0 << ',' << py - py0
        << 'z';
    }
    canvas
        << "' stroke-width='1' stroke='" << lineColor << "' fill='" << (fillColor ? fillColor : "none") << "' />" << Endl;

    if (title) {
        canvas << "</g>" << Endl;
    }
}


void TPlan::PrintDeriv(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, TStringBuf lineColor, TStringBuf fillColor) {
    if (history.MaxDeriv != 0) {
        PrintSeries(canvas, history.Deriv, history.MaxDeriv, x, y, w, h, title, lineColor, fillColor);
    }
}

void TPlan::PrintValues(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, TStringBuf lineColor, TStringBuf fillColor) {
    if (history.MaxValue != 0) {
        PrintSeries(canvas, history.Values, history.MaxValue, x, y, w, h, title, lineColor, fillColor);
    }
}

void TPlan::PrintStageSummary(TStringBuilder& background, const TViewBox& box, const TStageSummary& bar) {

    ui32 x0 = box.Left + INTERNAL_GAP_X;
    ui32 width = box.Width - INTERNAL_GAP_X * 2;
    if (bar.Icon.Ref) {
        x0 += INTERNAL_WIDTH;
        width -= INTERNAL_WIDTH;
    }
    if (bar.Metric->Details.Sum == 0) {
        width = 0;
    } else if (bar.Metric->Summary && bar.Metric->Summary->Max) {
        width = bar.Metric->Details.Sum * width / bar.Metric->Summary->Max;
    }
    if (width == 0) {
        width = 1;
    }
    if (bar.Tooltip) {
        background
        << "<g><title>" << bar.Tooltip << "</title>" << Endl;
    }
    if (bar.BackgroundRect) {
        background << SvgRect(box.Left, box.Top, box.Width, box.Height, "background");
    }
    if (bar.Icon.Ref) {
        background
        << "<use href='" << bar.Icon.Ref << "' transform='translate(" << box.Left << ' ' << box.Top << ") scale(" << bar.Icon.Scale << ")' fill='" << bar.Icon.Color << "'/>" << Endl;
    }
    if (bar.PeerId) {
        background
        << SvgTextM(box.Left + INTERNAL_WIDTH / 2, box.Top + INTERNAL_HEIGHT / 2 + INTERNAL_TEXT_HEIGHT / 2, bar.PeerId);
    }
    if (bar.Metric->MinMaxDistribution && bar.Metric->Details.Max) {
        auto wavg = width / 2;
        if (bar.Metric->Details.Max > bar.Metric->Details.Min) {
            wavg = (bar.Metric->Details.Avg - bar.Metric->Details.Min) * width / (bar.Metric->Details.Max - bar.Metric->Details.Min);
        }
        background
        << "  <rect x='" << x0 << "' y='" << box.Top
        << "' width='" << width << "' height='" << box.Height
        << "' stroke-width='0' fill='" << bar.Colors.Light << "'/>"
        << "  <polygon points='" << x0 << "," << box.Top << " "
        << x0 + wavg << "," << box.Top + box.Height - bar.Metric->Details.Avg * box.Height / bar.Metric->Details.Max << " "
        << x0 + width << "," << box.Top + box.Height - bar.Metric->Details.Min * box.Height / bar.Metric->Details.Max << " "
        << x0 + width << "," << box.Top + box.Height << " "
        << x0 << "," << box.Top + box.Height
        << "' stroke='none' fill='" << bar.Colors.Medium << "'/>" << Endl;
    } else {
        background
        << "  <rect x='" << x0 << "' y='" << box.Top
        << "' width='" << width << "' height='" << box.Height
        << "' stroke-width='0' fill='" << bar.Colors.Medium << "'/>" << Endl;
    }
    if (bar.Split && bar.Split < bar.Metric->Details.Sum) {
        auto xs = x0 + width - bar.Split * width / bar.Metric->Details.Sum;
        background
        << "  <line x1='" << xs << "' y1='" << box.Top << "' x2='" << xs << "' y2='" << box.Top + box.Height
        << "' stroke-width='2' stroke='" << bar.Colors.Light << "'/>" << Endl;
    }
    if (bar.Scalar) {
        ui32 width = box.Width - INTERNAL_GAP_X * 2;
        if (bar.Icon.Ref) {
            width -= INTERNAL_WIDTH;
        }
        auto x2 = x0 + width - (bar.Scalar->Summary->Max ? bar.Scalar->Value * width / bar.Scalar->Summary->Max : 0);
        background
        << "  <line x1='" << x0 << "' y1='" << box.Top + box.Height - 3 << "' x2='" << x2 << "' y2='" << box.Top + box.Height - 3
        << "' stroke-width='3' stroke='" << bar.Colors.Light << "' stroke-dasharray='1,1'/>" << Endl;
    }
    if (bar.Text) {
        background
        << "<rect x='" << x0 << "' y='" << box.Top + (box.Height - INTERNAL_TEXT_HEIGHT) / 2
        << "' width='" << bar.Text.size() * INTERNAL_TEXT_HEIGHT * 7 / 10 << "' height='" << INTERNAL_TEXT_HEIGHT + 1
        << "' stroke-width='0' opacity='0.5' fill='" << Config.Palette.StageMain << "'/>" << Endl
        << "<text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextSummary << "' x='" << x0
        << "' y='" << box.Top + INTERNAL_TEXT_HEIGHT + (box.Height - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << bar.Text << "</text>" << Endl;
    }
    if (bar.Tooltip) {
        background
        << "</g>" << Endl;
    }

    if (bar.TaskCount) {
        TStringBuilder warn;
        TString w = "";

        if (bar.Metric->Details.Count != bar.TaskCount && (bar.Metric->Details.Sum || bar.Metric->Details.Count)) {
            warn << "Only " << bar.Metric->Details.Count << " task(s) of " << bar.TaskCount << " reported this metric";
            w = ToString(bar.Metric->Details.Count);
        }

        // SKEW is not reported for small values (less than 10% of max per graph)
        if (bar.Metric->Summary && bar.Metric->Details.Sum * 10 >= bar.Metric->Summary->Max) {
            // Define SKEW as following:
            //   1. Max > 4 * Min, i.e. there is LARGE DIFFERENCE between minimal and maximal metric values
            // or
            //   1. Max > 2 * Min, i.e. there is SIGNIFICANT DIFFERENCE between minimal and maximal metric values
            //   2. (Max - Avg) > 2 * (Avg - Min), i.e. OVERLOADED tasks are in MINORITY
            // Skewing ratio (x2 and x4) may be tuned later
            if ((bar.Metric->Details.Max > 4 * bar.Metric->Details.Min) || (bar.Metric->Details.Max > 2 * bar.Metric->Details.Min
                && bar.Metric->Details.Max - bar.Metric->Details.Avg > 2 * (bar.Metric->Details.Avg - bar.Metric->Details.Min))) {
                if (w) {
                    warn << ", ";
                } else {
                    w = "S";
                }
                warn << "Significant skew in metric";
            }
        }

        if (w) {
            PrintWarningBadge(background, (box.Left + box.Width) - INTERNAL_WIDTH / 2, box.Top + INTERNAL_WIDTH, warn, w);
        }
    }
}

void TPlan::PrintStageSummary(TStringBuilder& background, const TViewBox& box, std::initializer_list<std::pair<TMutableMetric*, TStringBuf>> history, ui64 scale, const TIcon& icon) {
    ui32 x0 = box.Left + INTERNAL_GAP_X;
    ui32 width = box.Width - INTERNAL_GAP_X * 2;

    TStringBuilder titleBuilder;
    TStringBuilder textBuilder;

    ui64 lastScale = 0;
    bool firstItem = true;
    for (auto& item : history) {
        auto itemScale = item.first->Average();
        if (itemScale) {
            if (!firstItem) {
                textBuilder << " / ";
                titleBuilder << "; ";
            }
            if (!item.first->IsLine) {
                auto nextScale = itemScale;
                itemScale -= lastScale;
                lastScale = nextScale;
            }
            textBuilder << FormatBytes(itemScale * 1_MB);
            titleBuilder << item.first->Title << ": Avg=" << FormatBytes(itemScale * 1_MB) << ", Max=" << FormatBytes(item.first->DisplayMaxValue * 1_MB);
            firstItem = false;
        }
    }

    background << "<g><title>" << titleBuilder << "</title>" << Endl;

    if (icon.Ref) {
        x0 += INTERNAL_WIDTH;
        width -= INTERNAL_WIDTH;
    }
    if (icon.Ref) {
        background
        << "<use href='" << icon.Ref << "' transform='translate(" << box.Left << ' ' << box.Top << ") scale(" << icon.Scale << ")' fill='" << icon.Color << "'/>" << Endl;
    }

    for (auto it = std::rbegin(history); it != std::rend(history); it++) {
        auto itemScale = it->first->Average();
        if (itemScale) {
            if (scale == 0) {
                scale = itemScale;
            }

            auto x = x0;
            auto w = width * itemScale / scale;

            if (it->first->IsLine) {
                x = x + w - 2;
                w = 2;
            }

            background
            << "<rect x='" << x << "' y='" << box.Top << "' width='" << w << "' height='" << box.Height
            << "' fill='" << it->second << "'/>" << Endl;
        }
    }

    TString text = textBuilder;

    if (text) {
        background
        << "<rect x='" << x0 << "' y='" << box.Top + (box.Height - INTERNAL_TEXT_HEIGHT) / 2
        << "' width='" << (text.size() + 1) * INTERNAL_TEXT_HEIGHT * 6 / 10 << "' height='" << INTERNAL_TEXT_HEIGHT + 1
        << "' stroke-width='0' opacity='0.5' fill='" << Config.Palette.StageMain << "'/>" << Endl
        << "<text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextSummary << "' x='" << x0
        << "' y='" << box.Top + INTERNAL_TEXT_HEIGHT + (box.Height - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << text << "</text>" << Endl;
    }

    background << "</g>" << Endl;
}


void TPlan::PrintDataFlowTimeline(TStringBuilder& builder, const TString& title, const std::shared_ptr<TSingleMetric>& bytes,
    ui32 x, ui32 y, ui32 w, const TColorTriple& colors, bool backgroundRect)
{
    TStringBuilder connCanvas;

    PrintTimeline(builder, connCanvas, title, bytes->FirstMessage, bytes->LastMessage, x, y, w, INTERNAL_HEIGHT, colors.Medium, backgroundRect);

    if (!bytes->WaitTime.Deriv.empty()) {
        PrintWaitTime(builder, bytes, x, y, w, INTERNAL_HEIGHT, colors.Light);
    }

    builder << connCanvas;

    if (!bytes->History.Deriv.empty()) {
        PrintDeriv(builder, bytes->History, x, y, w, INTERNAL_HEIGHT, "", colors.Dark);
    }
}

// PrintSeries turned on its side: the nodes run down the Y axis, each owning
// an equal band of the height, and the value of a node is the X extent at the
// centre of its band, given here in pixels. The area is curved between the
// bands and closed against the left edge. Both control points of a curve sit
// at the mid height, so the area steps from one band to the next rather than
// ramping between them. Y counts half bands, not pixels - band i spans
// [2i, 2i + 2] - so that the centres and the mid heights stay exact with about
// as many nodes as pixels: rounded to pixels, the control points of a one
// pixel step landed on its start and bent the step into a hook.
static void PrintNodeTasksArea(TStringBuilder& builder, const std::vector<i32>& extents, TStringBuf color) {
    ui32 count = extents.size();
    i32 px0 = extents.front();
    builder << "<path d='M0,0L" << px0 << ",0L" << px0 << ",1";
    for (ui32 i = 1; i < count; i++) {
        i32 pxi = extents[i];
        builder << "c0,1," << pxi - px0 << ",1," << pxi - px0 << ",2";
        px0 = pxi;
    }
    builder << 'L' << px0 << ',' << 2 * count << "L0," << 2 * count << "z' stroke='none' fill='" << color << "' opacity='0.5'/>" << Endl;
}

void TPlan::PrintNodeTasks(TStringBuilder& builder, const std::vector<TStageNodeTasks>& nodes) {
    ui64 total = 0;
    for (const auto& node : nodes) {
        total += node.Tasks;
    }
    if (total == 0) {
        return;
    }
    // A node with the average task count takes a third of the column, busier
    // ones grow past it up to the column width; the count text sits at the
    // right. The extent, value * count / total thirds of the column, is
    // rounded to the nearest pixel, as rounding down would short the narrow
    // nodes more than the wide ones. Anything non-zero keeps at least a pixel.
    ui64 count = nodes.size();
    auto extent = [&](ui32 value) -> i32 {
        return value ? std::clamp<ui64>((2 * count * value * Config.TaskWidth + 3 * total) / (6 * total), 1, Config.TaskWidth) : 0;
    };
    std::vector<i32> all;
    std::vector<i32> running;
    bool anyRunning = false;
    for (const auto& node : nodes) {
        all.push_back(extent(node.Tasks));
        running.push_back(extent(node.Tasks - std::min(node.Finished, node.Tasks)));
        anyRunning |= running.back() > 0;
    }
    // Its own viewport, pixels across and half bands down, stretched over the
    // stage box, so that the plot follows the box when the script slims it, as
    // the percent-based overlays do. The two areas share one translucent
    // colour: the running share is where they overlap. The pointer passes
    // through to the wait and throughput bands under the plot, whose tooltips
    // it would hide otherwise; the task tooltip stays on the count.
    builder
        << "<svg x='" << Config.TaskLeft << "' y='0' width='" << Config.TaskWidth << "' height='100%' viewBox='0 0 "
        << Config.TaskWidth << ' ' << 2 * count << "' preserveAspectRatio='none' pointer-events='none'>" << Endl;
    PrintNodeTasksArea(builder, all, Config.Palette.Cpu.Medium);
    if (anyRunning) {
        PrintNodeTasksArea(builder, running, Config.Palette.Cpu.Medium);
    }
    builder << "</svg>" << Endl;
}

void TPlan::PrintTasks(TStringBuilder& builder, ui32 tasks, ui32 finishedTasks) {
    PrintNodeTasks(builder, {{.Tasks = tasks, .Finished = finishedTasks}});
}

// The tooltip of the per-node profile: a summary, a list of every node would
// not fit a tooltip on a large cluster. The busiest node is named when it
// stands alone - the one to look at when the profile has a spike.
static void PrintNodeTasksSummary(TStringBuilder& builder, const std::vector<TStageNodeTasks>& nodes, ui32 tasks) {
    auto counted = [&](ui64 n, TStringBuf word) {
        builder << n << ' ' << word << (n == 1 ? "" : "s");
    };
    ui32 nodeTasks = 0;
    ui32 minTasks = nodes.front().Tasks;
    ui32 maxTasks = 0;
    ui32 maxNodes = 0;
    ui32 maxNodeId = 0;
    ui32 runningNodes = 0;
    for (const auto& node : nodes) {
        nodeTasks += node.Tasks;
        minTasks = std::min(minTasks, node.Tasks);
        if (node.Tasks > maxTasks) {
            maxTasks = node.Tasks;
            maxNodes = 0;
            maxNodeId = node.NodeId;
        }
        maxNodes += node.Tasks == maxTasks;
        runningNodes += node.Finished < node.Tasks;
    }
    if (nodes.size() == 1) {
        builder << "; on node " << nodes.front().NodeId;
    } else {
        builder << "; ";
        counted(nodes.size(), "node");
        builder << ", ";
        if (minTasks == maxTasks) {
            counted(maxTasks, "task");
            builder << " each";
        } else {
            builder << minTasks << " to " << maxTasks << " tasks each, " << maxTasks << " on ";
            if (maxNodes == 1) {
                builder << "node " << maxNodeId;
            } else {
                counted(maxNodes, "node");
            }
        }
        if (runningNodes && runningNodes < nodes.size()) {
            builder << "; running on ";
            counted(runningNodes, "node");
        }
    }
    if (nodeTasks < tasks) {
        builder << "; not started: " << tasks - nodeTasks;
    }
}

void TPlan::PrintWarningBadge(TStringBuilder& builder, ui32 cx, ui32 bottom, const TString& title, TStringBuf label) {
    builder
    << "<g><title>" << title << "</title>" << Endl
    << "  <circle cx='" << cx
    << "' cy='" << bottom - INTERNAL_WIDTH / 2
    << "' r='" << INTERNAL_WIDTH / 2 - 1
    << "' stroke='none' fill='" << Config.Palette.StageTextHighlight << "' />" << Endl
    << "  <text text-anchor='middle' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT
    << "px' fill='" << Config.Palette.TextLight
    << "' x='" << cx
    << "' y='" << bottom - (INTERNAL_WIDTH - INTERNAL_TEXT_HEIGHT) / 2
    << "'>" << label << "</text>" << Endl
    << "</g>" << Endl;
}

void TPlan::PrintSpillingBadge(TStringBuilder& builder, ui32 top, const TString& label, TSingleMetric* bytes) {
    if (!bytes || !bytes->Details.Sum) {
        return;
    }

    builder
    << "<g><title>";

    // The tooltip goes straight into the title element being opened above.
    auto textSum = FormatTooltip(builder, label, bytes, FormatBytes);
    auto x1 = Config.SummaryLeft + Config.SummaryWidth - INTERNAL_GAP_X;
    auto x0 = x1 - textSum.size() * INTERNAL_TEXT_HEIGHT * 7 / 10;

    builder
    << "</title>" << Endl
    << "  <rect x='" << x0 << "' y='" << top + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2
    << "' width='" << x1 - x0 << "' height='" << INTERNAL_TEXT_HEIGHT + 1
    << "' stroke-width='0' fill='" << Config.Palette.SpillingBytes.Light << "'/>" << Endl
    << "  <text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextSummary << "' x='" << x1 - 1
    << "' y='" << top + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << textSum << "</text>" << Endl
    << "</g>" << Endl;
}

// The plan-wide row above its stages: totals for every metric the stages break
// down, and the two critical paths the totals link to.
void TPlan::PrintPlanSummary(ui64 maxTime, ui32 timelineDelta, ui32& offsetY) {
    auto* p = this;
    auto planName = NodeType;

    if (Stages.empty() && CtePlan != nullptr) {
        p = CtePlan;
        planName = planName + " (reference to " + p->NodeType + ')';
    }

    offsetY += GAP_Y;

    ui32 summary3 = (Config.SummaryWidth - INTERNAL_GAP_X * 2) / 3;
    auto titleHeight = INTERNAL_GAP_Y + (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2;

    SummaryBuilder
        << "<g data-group='g" << GroupId << "' class='selectable'><title> " << SvgEscape(planName) << "</title>" << Endl
        << SvgRect(Config.HeaderLeft, 0, Config.HeaderWidth, TIME_HEIGHT + INTERNAL_HEIGHT, "background")
        << SvgTextS(Config.HeaderLeft + INTERNAL_GAP_X + INTERNAL_WIDTH * 2 + 2, titleHeight, "Query - " + planName)
        << "</g>" << Endl;

    SummaryBuilder
        << "<g class='ardn button'>"
        << SvgRect(INTERNAL_GAP_X, 0, CONN_SIZE, CONN_SIZE, "transparent")
        << "<use href='#icon_arrowdn' transform='translate(" << INTERNAL_GAP_X << ' ' << 0 << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/></g>" << Endl
        << "<g class='aruu button'>"
        << SvgRect(INTERNAL_GAP_X, CONN_SIZE, CONN_SIZE, CONN_SIZE, "transparent")
        << "<use href='#icon_arrowup' transform='translate(" << INTERNAL_GAP_X << ' ' << CONN_SIZE << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/></g>" << Endl;

    // The columns are named once by the document header strip, so only the value
    // belongs here.
    SummaryBuilder
        << SvgTextE(Config.TaskLeft + Config.TaskWidth - 2, titleHeight + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT, ToString(p->Tasks));

    SummaryBuilder
        << "<g><title>Ingress "
        << FormatBytes(p->IngressBytes.Value) << ", Rows " << FormatIntegerValue(p->IngressRows.Value);
    if (p->IngressRows.Value) {
    SummaryBuilder
        << ", Width " << p->IngressBytes.Value / p->IngressRows.Value << "B";
    }
    if (p->MaxTime) {
    SummaryBuilder
        << ", Avg " << FormatBytes(p->IngressBytes.Value * 1000 / p->MaxTime) << "/s";
    }
    SummaryBuilder
        << "</title>" << Endl
        << "  <rect x='" << Config.SummaryLeft << "' y='" << titleHeight + INTERNAL_GAP_Y
        << "' width='" << summary3 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.Ingress.Medium << "'/>" << Endl
        << "  <text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight
        << "' x='" << Config.SummaryLeft + 2
        << "' y='" << titleHeight + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT << "'>" << FormatBytes(p->IngressBytes.Value) << "</text>" << Endl
        << "</g>" << Endl;

    SummaryBuilder
        << "<g><title>CPU Usage " << FormatUsage(p->CpuTime.Value);
    if (p->MaxTime) {
        auto usagePS = p->CpuTime.Value / p->MaxTime;
        usagePS /= 10;
    SummaryBuilder
        << ", Avg " << Sprintf("%lu.%.2lu", usagePS / 100, usagePS % 100) << " CPU/s";
    }
    auto cpuGroups = GetCriticalCpuGroups();
    SummaryBuilder
        << "</title>" << Endl
        << "  <rect class='cpupath' data-groups='" << cpuGroups << "' x='" << Config.SummaryLeft + INTERNAL_GAP_X + summary3 << "' y='" << titleHeight + INTERNAL_GAP_Y
        << "' width='" << Config.SummaryWidth - (summary3 + INTERNAL_GAP_X) * 2 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.Cpu.Medium << "'/>" << Endl
        << "  <text class='cpupath' data-groups='" << cpuGroups << "' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight
        << "' x='" << Config.SummaryLeft + INTERNAL_GAP_X + summary3 + 2
        << "' y='" << titleHeight + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT << "'>" << FormatUsage(p->CpuTime.Value) << "</text>" << Endl
        << "</g>" << Endl;

    SummaryBuilder
        << "<g><title>Memory " << FormatBytes(p->MaxMemoryUsage.Value) << "</title>" << Endl
        << "  <rect x='" << Config.SummaryLeft + Config.SummaryWidth - summary3 << "' y='" << titleHeight + INTERNAL_GAP_Y
        << "' width='" << summary3 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.Mem.Medium << "'/>" << Endl
        << "  <text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight
        << "' x='" << Config.SummaryLeft + Config.SummaryWidth - summary3 + 2
        << "' y='" << titleHeight + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT << "'>" << FormatBytes(p->MaxMemoryUsage.Value) << "</text>" << Endl
        << "</g>" << Endl;

    auto timeGroups = GetCriticalTimeGroups();
    auto x = Config.TimelineLeft + (Config.TimelineWidth - timelineDelta) * (p->TimeOffset + p->MaxTime) / maxTime;
    SummaryBuilder
        << "<g><title>" << "Duration: " << FormatTimeMs(p->MaxTime) << ", Total " << FormatTimeMs(p->MaxTime + p->TimeOffset) << "</title>" << Endl
        << "  <rect class='timepath' data-groups='" << timeGroups << "' x='" << x - summary3 << "' y='" << INTERNAL_GAP_Y + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2
        << "' width='" << summary3 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.StageGrid << "'/>" << Endl
        << "  <text class='timepath' data-groups='" << timeGroups << "' text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextInverted << "' x='" << x - 2
        << "' y='" << titleHeight << "'>" << FormatTimeMs(p->MaxTime + p->TimeOffset) << "</text>" << Endl
        << "</g>" << Endl;

    offsetY += titleHeight + INTERNAL_GAP_Y;
    if (!p->TotalCpuTime.Deriv.empty() && p->TotalCpuTime.MaxTime > p->TotalCpuTime.MinTime) {

        // auto tx0 = Config.TimelineLeft;
        // auto tw = Config.TimelineWidth;

        auto xmin = Config.TimelineLeft + (Config.TimelineWidth - timelineDelta) * (p->TotalCpuTime.MinTime + p->TimeOffset) / maxTime;
        auto xmax = Config.TimelineLeft + (Config.TimelineWidth - timelineDelta) * (p->TotalCpuTime.MaxTime + p->TimeOffset) / maxTime;

        auto maxCpu = p->TotalCpuTime.MaxDeriv * TIME_SERIES_RANGES / (p->TotalCpuTime.MaxTime - p->TotalCpuTime.MinTime);
        p->PrintDeriv(SummaryBuilder, p->TotalCpuTime, xmin, titleHeight + INTERNAL_GAP_Y, xmax - xmin, TIME_HEIGHT, "Max CPU " + FormatMCpu(maxCpu), Config.Palette.Cpu.Medium, Config.Palette.Cpu.Light);
    }
    offsetY += TIME_HEIGHT;
}

// The operator column: one box per operator, with the estimates and row counts
// it reported and the lines linking it to the operators feeding it.
void TPlan::PrintStageOperators(const std::shared_ptr<TStage>& s) {
    ui32 y0 = INTERNAL_GAP_Y;
    ui32 index = 0;
    for (const auto& op : s->Operators) {
        ui32 yt = y0 + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2;
        s->Svg
            << "<g><title>" << SvgEscape(op.Name) << ": " << SvgEscape(op.Info) << (op.Blocks ? " Blocks: True" : "") << "</title>";
        if (op.Blocks) {
            auto h = INTERNAL_TEXT_HEIGHT * 2 + INTERNAL_GAP_Y * 2;
            if (index == s->Operators.size() - 1) {
                h = s->Height - yt;
            }
            s->Svg
            << SvgRect(Config.HeaderLeft + s->IndentX, yt, INTERNAL_WIDTH * 2, h, "blocks");
        }

        s->Svg
            << SvgText(Config.HeaderLeft + s->IndentX + INTERNAL_GAP_X + INTERNAL_WIDTH * 2 + 2, yt + INTERNAL_TEXT_HEIGHT, "texts clipped", op.Name + ": " + op.Info);
        if (op.OutputRows) {
            TStringBuilder tooltip;
            auto textSum = FormatTooltip(tooltip, "Output Rows", op.OutputRows.get(), FormatInteger);
            if (op.Estimations) {
                tooltip
                << ", " << op.Estimations;
            }
            PrintStageSummary(s->Svg, {Config.OperatorLeft, Config.OperatorWidth, y0, INTERNAL_HEIGHT}, {
                .Metric = op.OutputRows.get(),
                .Colors = Config.Palette.Output,
                .Text = textSum,
                .Tooltip = tooltip,
                .TaskCount = s->Tasks,
            });
        }
        s->Svg
            << "</g>" << Endl;

        if (!op.Inputs.empty()) {
            auto opX = Config.HeaderLeft + Config.HeaderWidth - INTERNAL_WIDTH * (1 + 2 * (op.Inputs.size() - 1)) / 2;
            auto opY = y0 + INTERNAL_HEIGHT / 2;
            for (auto& input : op.Inputs) {
                if (input.StageId) {
                    s->Svg
                        << "<g data-group='g" << NodeToConnection.at(input.PlanNodeId)->GroupId << "' class='selectable'><title>Input from Stage " << *input.StageId << "</title>" << Endl
                        << SvgStageId(opX, opY, ToString(*input.StageId))
                        << "</g>" << Endl;
                } else if (input.PrecomputeRef) {
                    auto it = Viz.CteSubPlans.find(input.PrecomputeRef);
                    if (it != Viz.CteSubPlans.end()) {
                        s->Svg
                        << "<g data-group='g" << it->second->GroupId << "' class='selectable'><title>Data from precompute " << SvgEscape(it->second->NodeType) << "</title>" << Endl
                        << SvgStageId(opX, opY, "P")
                        << "</g>" << Endl;
                    }
                }
                opX += INTERNAL_WIDTH;
            }
        }

        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
        index++;
    }
}

// The strips behind the timeline, one per data flow the stage will draw over
// them, marking where each flow is expected to be busy.
void TPlan::PrintStageBackground(const std::shared_ptr<TStage>& s) {
    ui32 y0 = INTERNAL_GAP_Y;
    if (s->EgressBytes) {
        if (s->External) {
            s->Svg
            << "<g data-group='g" << StageToExternalConnection[s.get()]->GroupId << "' class='selectable'><title>Egress</title>" << Endl
            << SvgRect(Config.TimelineLeft, y0, Config.TimelineWidth, INTERNAL_HEIGHT, "background")
            << "</g>" << Endl;
        }
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    }
    if (s->OutputBytes) {
        if (s->OutputPlanNodeId) {
            s->Svg
            << "<g data-group='g" << NodeToConnection[s->OutputPlanNodeId]->GroupId << "' class='selectable'><title>Output</title>" << Endl
            << SvgRect(Config.TimelineLeft, y0, Config.TimelineWidth, INTERNAL_HEIGHT, "background")
            << "</g>" << Endl;
        }
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    }
    // memory
    y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    // cpu
    y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    for (auto& c : s->Connections) {
        if (c->InputBytes) {
            s->Svg
            << "<g data-group='g" << c->GroupId << "' class='selectable'><title>Input</title>" << Endl
            << SvgRect(Config.TimelineLeft, y0, Config.TimelineWidth, INTERNAL_HEIGHT, "background")
            << "</g>" << Endl;
            y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
        }
    }
    if (s->IngressBytes) {
        if (s->IngressConnection) {
            s->Svg
            << "<g data-group='g" << s->IngressConnection->GroupId << "' class='selectable'><title>Ingress</title>" << Endl
            << SvgRect(Config.TimelineLeft, y0, Config.TimelineWidth, INTERNAL_HEIGHT, "background")
            << "</g>" << Endl;
        }
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    }
}

// Egress leaves the cluster, so it has a summary bar and a timeline but no
// receiving stage to link to.
void TPlan::PrintEgressStrip(const std::shared_ptr<TStage>& s, ui32& y0, ui64 px, ui64 pw) {
    if (s->EgressBytes) {
        TStringBuilder& builder = s->Svg;
        builder << "<g data-group='g" << (s->External ? StageToExternalConnection[s.get()]->GroupId : s->GroupId) << "' class='selectable'><title>Egress</title>" << Endl;

        TStringBuilder tooltip;
        auto textSum = FormatDataFlowTooltip(tooltip, "Egress", s->EgressBytes, s->EgressRows, 0, 0, nullptr);
        PrintStageSummary(builder, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
            .Metric = s->EgressBytes.get(),
            .Colors = Config.Palette.Egress,
            .Text = textSum,
            .Tooltip = tooltip,
            .TaskCount = s->Tasks,
            .Icon = {"#icon_egress", Config.Palette.Egress.Medium, "0.9 0.9"},
            .BackgroundRect = s->External,
        });

        auto title = FormatDataFlowRate("Egress", s->EgressBytes, s->EgressRows);

        PrintDataFlowTimeline(builder, title, s->EgressBytes, px, y0, pw,
            Config.Palette.Egress);

        builder << "</g>" << Endl;
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    }
}

// Output goes to another stage, so its bar also carries that peer stage id, the
// local/remote split, and the chunk size dashes.
void TPlan::PrintOutputStrip(const std::shared_ptr<TStage>& s, ui32& y0, ui64 px, ui64 pw) {
    if (s->OutputBytes) {
        TStringBuilder& builder = s->Svg;
        builder << "<g data-group='g" << (s->OutputPlanNodeId ? NodeToConnection[s->OutputPlanNodeId]->GroupId : GroupId) << "' class='selectable'><title>Output</title>" << Endl;

        TStringBuilder tooltip;
        auto textSum = FormatDataFlowTooltip(tooltip, "Output", s->OutputBytes, s->OutputRows,
            s->OutputLocalBytes, s->OutputChunks, s->OutputChunkSize);
        PrintStageSummary(builder, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
            .Metric = s->OutputBytes.get(),
            .Colors = Config.Palette.Output,
            .Text = textSum,
            .Tooltip = tooltip,
            .TaskCount = s->Tasks,
            .Icon = {"#icon_output", Config.Palette.Output.Light, "0.0325 0.0325"},
            .BackgroundRect = true,
            .PeerId = s->OutputPhysicalStageId ? ToString(s->OutputPhysicalStageId) : "",
            .Split = s->OutputLocalBytes,
            .Scalar = s->OutputChunkSize.get(),
        });

        PrintSpillingBadge(builder, y0, "Channel Spilling", s->SpillingChannelBytes.get());

        auto title = FormatDataFlowRate("Output", s->OutputBytes, s->OutputRows);

        PrintDataFlowTimeline(builder, title, s->OutputBytes, px, y0, pw,
            Config.Palette.Output);

        builder << "</g>" << Endl;
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    }
}

// Memory: the peak as a bar, the usage over time as a curve, and whatever the
// stage had to spill because the memory was not enough.
void TPlan::PrintMemoryStrip(const std::shared_ptr<TStage>& s, ui32& y0, ui64 px, ui64 pw) {
    if (s->MaxMemoryUsage) {
        TString tooltip;
        auto textSum = FormatTooltip(tooltip, "Memory", s->MaxMemoryUsage.get(), FormatBytes);
        PrintStageSummary(s->Svg, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
            .Metric = s->MaxMemoryUsage.get(),
            .Colors = Config.Palette.Mem,
            .Text = textSum,
            .Tooltip = tooltip,
            .TaskCount = s->Tasks,
            .Icon = {"#icon_memory", Config.Palette.Mem.Medium, "0.6 0.6"},
        });

        PrintSpillingBadge(s->Svg, y0, "Compute Spilling", s->SpillingComputeBytes.get());
    }

    if (s->MemoryUsage && !s->MemoryUsage->History.Values.empty()) {
        PrintValues(s->Svg, s->MemoryUsage->History, px, y0, pw, INTERNAL_HEIGHT, "Max MEM " + FormatBytes(s->MemoryUsage->History.MaxValue * 1_MB), Config.Palette.Mem.Medium, Config.Palette.Mem.Medium);
    } else if (s->MaxMemoryUsage && !s->MaxMemoryUsage->History.Values.empty()) {
        PrintValues(s->Svg, s->MaxMemoryUsage->History, px, y0, pw, INTERNAL_HEIGHT, "Max MEM " + FormatBytes(s->MaxMemoryUsage->History.MaxValue), Config.Palette.Mem.Medium, Config.Palette.Mem.Medium);
    }

    if (s->SpillingComputeBytes && !s->SpillingComputeBytes->History.Deriv.empty()) {
        PrintDeriv(s->Svg, s->SpillingComputeBytes->History, px, y0, pw, INTERNAL_HEIGHT, "Spilling Compute", Config.Palette.SpillingBytes.Medium, Config.Palette.SpillingBytes.Light);
    }

    y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
}

// CPU: usage as a bar, the derivative as a curve, and the wait times that
// explain the gaps in it. The wait-output badge hangs off the bottom of the
// whole stage rather than off this strip.
void TPlan::PrintCpuStrip(const std::shared_ptr<TStage>& s, ui32& y0, ui64 px, ui64 pw) {
    if (s->CpuTime) {
        TString tooltip;
        auto textSum = FormatTooltip(tooltip, "CPU Usage", s->CpuTime.get(), FormatUsage);
        PrintStageSummary(s->Svg, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
            .Metric = s->CpuTime.get(),
            .Colors = Config.Palette.Cpu,
            .Text = textSum,
            .Tooltip = tooltip,
            .TaskCount = s->Tasks,
            .Icon = {"#icon_cpu", Config.Palette.Cpu.Medium, "0.6 0.6"},
        });

        auto totalTime = s->CpuTime->Details.Sum;
        if (s->WaitInputTime) {
            totalTime += s->WaitInputTime->Details.Sum;
        }
        if (s->WaitOutputTime) {
            totalTime += s->WaitOutputTime->Details.Sum;
        }

        ui32 activePercentsMin = 0;
        ui32 activePercentsMax = 100;

        if (s->WaitInputTime) {
            if (totalTime) {
                auto heightPercents = s->WaitInputTime->Details.Sum * 100 / totalTime;
                activePercentsMax -= heightPercents;
            s->Svg
                << "<g><title>";
                FormatTooltip(s->Svg, "Wait Input Time", s->WaitInputTime.get(), FormatUsage, totalTime);
            s->Svg
                << "</title>" << Endl
                << "  <rect x='" << Config.TaskLeft << "' y='" << activePercentsMax
                << "%' width='" << Config.TaskWidth << "' height='" << heightPercents
                << "%' stroke-width='0' fill='" << Config.Palette.Input.Light << "'/>" << Endl
                << "</g>" << Endl;
            }
            if(!s->WaitInputTime->History.Deriv.empty()) {
                PrintDeriv(s->Svg, s->WaitInputTime->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.Input.Medium, Config.Palette.Input.Light);
            }

            // consider only 10% or more waiting times
            if (totalTime && s->WaitInputTime->Details.Sum * 10 > totalTime) {
                TStringBuilder waitOutputPeers;
                for (auto& c : s->Connections) {
                    if (c->FromStage && c->FromStage->WaitOutputTime) {
                        auto peerTotalTime = c->FromStage->WaitOutputTime->Details.Sum;
                        if (c->FromStage->WaitInputTime) {
                            peerTotalTime += c->FromStage->WaitInputTime->Details.Sum;
                        }
                        if (c->FromStage->CpuTime) {
                            peerTotalTime += c->FromStage->CpuTime->Details.Sum;
                        }
                        if (peerTotalTime && c->FromStage->WaitOutputTime->Details.Sum * 10 > peerTotalTime) {
                            if (waitOutputPeers) {
                                waitOutputPeers << ", ";
                            }
                            waitOutputPeers << c->FromStage->PhysicalStageId;
                        }
                    }
                }
                if (waitOutputPeers) {
                    // Only remembered here: the badge sits at the bottom of the stage,
                    // in the task column that later strips keep painting over, so it is
                    // emitted last by PrepareStageSvg.
                    s->WaitInputWarning = TStringBuilder()
                        << "Wait input with peer stage(s) " << waitOutputPeers << " wait output";
                }
            }
        }

        if (s->WaitOutputTime) {
            if (totalTime) {
                auto heightPercents = s->WaitOutputTime->Details.Sum * 100 / totalTime;
                activePercentsMin += heightPercents;
            s->Svg
                << "<g><title>";
                FormatTooltip(s->Svg, "Wait Output Time", s->WaitOutputTime.get(), FormatUsage, totalTime);
            s->Svg
                << "</title>" << Endl
                << "  <rect x='" << Config.TaskLeft << "' y='0%' width='" << Config.TaskWidth << "' height='" << heightPercents
                << "%' stroke-width='0' fill='" << Config.Palette.Output.Light << "'/>" << Endl
                << "</g>" << Endl;
            }
            if (!s->WaitOutputTime->History.Deriv.empty()) {
                PrintDeriv(s->Svg, s->WaitOutputTime->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.Output.Medium, Config.Palette.Output.Light);
            }
        }

        if (activePercentsMax > activePercentsMin && s->InputThroughput) {
            auto opacity = s->InputThroughput->Details.Sum / static_cast<double>(s->InputThroughput->Summary->Max * 2);
            s->Svg
            << "<g><title>Input Throughput " << FormatInteger(s->InputThroughput->Details.Sum) << "/s</title>" << Endl
            << "  <rect x='" << Config.TaskLeft << "' y='" << activePercentsMin
            << "%' width='" << Config.TaskWidth << "' height='" << activePercentsMax - activePercentsMin
            << "%' stroke-width='0' fill='" << Config.Palette.Cpu.Light << "' opacity='" << opacity  << "'/>" << Endl
            << "</g>" << Endl;
        }

        if (!s->CpuTime->History.Deriv.empty() && s->CpuTime->History.MaxTime > s->CpuTime->History.MinTime) {
            auto maxCpu = s->CpuTime->History.MaxDeriv * TIME_SERIES_RANGES / (s->CpuTime->History.MaxTime - s->CpuTime->History.MinTime);
            PrintDeriv(s->Svg, s->CpuTime->History, px, y0, pw, INTERNAL_HEIGHT, "Max CPU " + FormatMCpu(maxCpu), Config.Palette.Cpu.Medium, Config.Palette.Cpu.Light);
        }

        if (s->SpillingComputeTime && !s->SpillingComputeTime->History.Deriv.empty()) {
            PrintDeriv(s->Svg, s->SpillingComputeTime->History, px, y0, pw, INTERNAL_HEIGHT, "Spilling Compute", Config.Palette.SpillingTimeMedium);
        }
    }
}

// One strip per incoming connection: the bar, the timeline, and the box in the
// header column naming the connection and the stage on the other end.
void TPlan::PrintStageConnections(const std::shared_ptr<TStage>& s, ui32& y0, ui64 px, ui64 pw) {
    for (auto& c : s->Connections) {

        auto x = c->CteConnection ? c->CteIndentX : c->FromStage->IndentX;
        ui32 y = 0;

        c->Svg << "<g data-group='g" << c->GroupId << "' class='selectable'><title>" << SvgEscape(c->NodeType) << " connection";
        if (!c->KeyColumns.empty()) {
            c->Svg << " KeyColumns: ";
            bool first = true;
            for (const auto& keyColumn : c->KeyColumns) {
                if (first) {
                    first = false;
                } else {
                    c->Svg << ", ";
                }
                c->Svg << SvgEscape(keyColumn);
            }
        }
        if (!c->SortColumns.empty()) {
            c->Svg << " SortColumns: ";
            bool first = true;
            for (const auto& sortColumn : c->SortColumns) {
                if (first) {
                    first = false;
                } else {
                    c->Svg << ", ";
                }
                c->Svg << SvgEscape(sortColumn);
            }
        }
        if (c->Blocks) {
            c->Svg << " Blocks: True";
        }
        if (c->HashFunc) {
            c->Svg << " HashFunc: " << SvgEscape(c->HashFunc);
        }
        if (c->Parallel) {
            c->Svg << " Parallel: True";
        }
        c->Svg
            << "</title>" << Endl;

        if (c->CteConnection) {
            c->CteSvg
                << "<g data-group='g" << c->FromStage->GroupId << "' class='selectable'><title>Stage " << (c->FromStage->External ? "E" : ToString(c->FromStage->PhysicalStageId)) << "</title>" << Endl
                << SvgRect(Config.TaskLeft, y, Config.TaskWidth, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone")
                << SvgRect(Config.HeaderLeft + x, y, Config.HeaderWidth - x, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone")
                << SvgRect(Config.SummaryLeft, y, Config.SummaryWidth, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone")
                << SvgRect(Config.OperatorLeft, y, Config.OperatorWidth, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone");

            if (c->CteOperatorOutputRows) {
                TStringBuilder tooltip;
                auto textSum = FormatTooltip(tooltip, "Output Rows", c->CteOperatorOutputRows.get(), FormatInteger);
                PrintStageSummary(c->CteSvg, {Config.OperatorLeft, Config.OperatorWidth, y, INTERNAL_HEIGHT}, {
                    .Metric = c->CteOperatorOutputRows.get(),
                    .Colors = Config.Palette.Output,
                    .Text = textSum,
                    .Tooltip = tooltip,
                });
            }

            c->CteSvg
                << SvgRect(Config.TimelineLeft, y, Config.TimelineWidth, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone")
                << SvgStageId(Config.HeaderLeft + x + INTERNAL_GAP_X + INTERNAL_WIDTH * 3 / 2, y + INTERNAL_GAP_Y + INTERNAL_HEIGHT / 2, ToString(c->FromStage->PhysicalStageId))
                << SvgText(Config.HeaderLeft + x + INTERNAL_GAP_X + INTERNAL_WIDTH * 2 + 2, y + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2, "texts clipped", c->FromStage->Operators[0].Name + ": " + c->FromStage->Operators[0].Info)
                << "</g>" << Endl;

            if (c->CteOutputBytes) {
                c->CteSvg << "<g data-group='g" << c->GroupId << "' class='selectable'><title>Output</title>" << Endl;

                TStringBuilder tooltip;
                auto textSum = FormatDataFlowTooltip(tooltip, "Output", c->CteOutputBytes, c->CteOutputRows,
                    c->CteOutputLocalBytes, c->CteOutputChunks, c->CteOutputChunkSize);
                PrintStageSummary(c->CteSvg, {Config.SummaryLeft, Config.SummaryWidth, y + INTERNAL_GAP_Y, INTERNAL_HEIGHT}, {
                    .Metric = c->CteOutputBytes.get(),
                    .Colors = Config.Palette.Output,
                    .Text = textSum,
                    .Tooltip = tooltip,
                    .Icon = {"#icon_output", Config.Palette.Output.Light, "0.0325 0.0325"},
                    .BackgroundRect = true,
                    .PeerId = ToString(s->PhysicalStageId),
                    .Split = c->CteOutputLocalBytes,
                    .Scalar = c->CteOutputChunkSize.get(),
                });

                auto title = FormatDataFlowRate("Output", c->CteOutputBytes, c->CteOutputRows);

                PrintDataFlowTimeline(c->CteSvg, title, c->CteOutputBytes, px, y + INTERNAL_GAP_Y, pw,
                    Config.Palette.Output, true);
                c->CteSvg << "</g>" << Endl;
            }
        }

        TString mark;
        if (c->NodeType == "HashShuffle")     mark = "H";
        else if (c->NodeType == "Merge")      mark = "Me";
        else if (c->NodeType == "Map")        mark = "Ma";
        else if (c->NodeType == "UnionAll")   mark = "U";
        else if (c->NodeType == "Broadcast")  mark = "B";
        else if (c->NodeType == "External")   mark = "E";
        else if (c->NodeType == "Table")      mark = "T";
        else if (c->NodeType == "Lookup")     mark = "L";
        else if (c->NodeType == "LookupJoin") mark = "LJ";
        else                                  mark = "?";

        if (s->Connections.size() == 1) {
            c->Svg
            << "  <path d='M" << Config.HeaderLeft + x + INTERNAL_WIDTH << ',' << y + GAP_Y + INTERNAL_GAP_Y + INTERNAL_HEIGHT << "l-" << CONN_SIZE << ",0"
            << "l0,-" << CONN_SIZE << "l" << CONN_SIZE / 2 << ",-" << CONN_ARROW << 'l' << CONN_SIZE / 2 << ',' << CONN_ARROW
            << "z' class='" << (c->Blocks ? "conn blocks": "conn") << "' />" << Endl;
        } else {
            c->Svg
            << "  <path d='M" << Config.HeaderLeft + x + INTERNAL_WIDTH << ',' << y + GAP_Y + INTERNAL_GAP_Y + INTERNAL_HEIGHT << "l-" << CONN_SIZE << ",0"
            << "l-" << CONN_ARROW << ",-" << CONN_SIZE / 2 << 'l' << CONN_ARROW << ",-" << CONN_SIZE / 2 << 'l' << CONN_SIZE << ",0"
            << "z' class='" << (c->Blocks ? "conn blocks": "conn") << "' />" << Endl;
        }

        c->Svg
            << SvgText(Config.HeaderLeft + x + INTERNAL_WIDTH - CONN_SIZE / 2, y + GAP_Y + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT  + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2, "conn", mark);

        c->Svg << "</g>" << Endl;

        if (c->InputBytes) {

            s->Svg << "<g data-group='g" << c->GroupId << "' class='selectable'><title>Input</title>" << Endl;

            TStringBuilder tooltip;
            auto textSum = FormatDataFlowTooltip(tooltip, "Input", c->InputBytes, c->InputRows,
                c->InputLocalBytes, c->InputChunks, c->InputChunkSize);
            PrintStageSummary(s->Svg, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
                .Metric = c->InputBytes.get(),
                .Colors = Config.Palette.Input,
                .Text = textSum,
                .Tooltip = tooltip,
                .TaskCount = s->Tasks,
                .Icon = {"#icon_input", Config.Palette.Input.Light, "0.0325 0.0325"},
                .BackgroundRect = true,
                .PeerId = ToString(c->FromStage->PhysicalStageId),
                .Split = c->InputLocalBytes,
                .Scalar = c->InputChunkSize.get(),
            });

            auto title = FormatDataFlowRate("Input", c->InputBytes, c->InputRows);

            PrintDataFlowTimeline(s->Svg, title, c->InputBytes, px, y0, pw,
                Config.Palette.Input);

            y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

            s->Svg << "</g>" << Endl;
        }
    }
}

// Ingress enters the cluster, either through a source built into the stage or
// over the External connection standing in for the storage layer.
void TPlan::PrintIngressStrip(const std::shared_ptr<TStage>& s, ui32& y0, ui64 px, ui64 pw) {
    if (s->IngressBytes) {
        TStringBuilder& builder = s->Svg;
        builder << "<g data-group='g" << (s->IngressConnection ? s->IngressConnection->GroupId : s->GroupId) << "' class='selectable'><title>Ingress</title>" << Endl;

        TStringBuilder tooltip;
        auto textSum = FormatDataFlowTooltip(tooltip, "Ingress", s->IngressBytes, s->IngressRows, 0, 0, nullptr);
        PrintStageSummary(builder, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
            .Metric = s->IngressBytes.get(),
            .Colors = Config.Palette.Ingress,
            .Text = textSum,
            .Tooltip = tooltip,
            .TaskCount = s->Tasks,
            .Icon = {"#icon_ingress", Config.Palette.Ingress.Medium, "0.9 0.9"},
            .BackgroundRect = s->IngressConnection != nullptr,
        });

        auto title = FormatDataFlowRate("Ingress", s->IngressBytes, s->IngressRows);

        PrintDataFlowTimeline(builder, title, s->IngressBytes, px, y0, pw,
            Config.Palette.Ingress);

        builder << "</g>" << Endl;
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    }
}

// One stage: the boxes it occupies in every column, then a strip for each of
// its data flows, memory and CPU, then its incoming connections.
void TPlan::PrepareStageSvg(const std::shared_ptr<TStage>& s, ui64 maxTime, ui32 timelineDelta) {
    s->Svg
        << "<g data-group='g" << s->GroupId << "' class='selectable'><title>Stage " << (s->External ? "E" : ToString(s->PhysicalStageId)) << "</title>" << Endl;
    auto stageClass = s->External ? "clone" : "stage";

    s->Svg
        << SvgRect(Config.HeaderLeft + s->IndentX, 0, Config.HeaderWidth - s->IndentX, "100%", stageClass)
        << SvgRect(Config.OperatorLeft, 0, Config.OperatorWidth, "100%", stageClass)
        << SvgRect(Config.SummaryLeft, 0, Config.SummaryWidth, "100%", stageClass)
        << SvgRect(Config.TaskLeft, 0, Config.TaskWidth, "100%", stageClass)
        << SvgRect(Config.TimelineLeft, 0, Config.TimelineWidth, "100%", stageClass);

    PrintStageOperators(s);

    s->Svg
        << SvgStageId(Config.HeaderLeft + s->IndentX + INTERNAL_GAP_X + INTERNAL_WIDTH * 3 / 2, INTERNAL_GAP_Y + INTERNAL_HEIGHT / 2, s->External ? "E" : ToString(s->PhysicalStageId));

    if (s->ColumnShardHeight) {
        s->Svg << "<g class='cs-stage-toggle button' role='button' tabindex='0' aria-expanded='false' data-cs-stage='"
            << s->PhysicalStageId << "' transform='translate(" << Config.HeaderLeft + s->IndentX + INTERNAL_GAP_X
            << ',' << INTERNAL_GAP_Y + 2 << ")'><title>Show ColumnShard read statistics for Stage "
            << s->PhysicalStageId << "</title><rect width='10' height='10' fill='transparent'/>"
            << "<path d='M1 1 L7 5 L1 9 Z' fill='" << Config.Palette.ConnectionText << "'/></g>" << Endl;
    }

    PrintStageBackground(s);

    for (auto& region : s->HotRegions) {
        auto px = Config.TimelineLeft + (TimeOffset + region.first) * (Config.TimelineWidth - timelineDelta) / maxTime;
        auto pw = (region.second - region.first) * (Config.TimelineWidth - timelineDelta) / maxTime;
        s->Svg
        << SvgRect(px, 0, pw, "100%", "hot");
    }

    ui32 y0 = INTERNAL_GAP_Y;

    auto tx0 = Config.TimelineLeft;
    auto px = tx0 + TimeOffset * (Config.TimelineWidth - timelineDelta) / maxTime;
    auto pw = MaxTime * (Config.TimelineWidth - timelineDelta) / maxTime;

    PrintEgressStrip(s, y0, px, pw);
    PrintOutputStrip(s, y0, px, pw);
    PrintMemoryStrip(s, y0, px, pw);
    PrintCpuStrip(s, y0, px, pw);

    if (s->Tasks) {
        s->Svg << "<g><title>";
        if (s->External) {
            s->Svg << "External Source partitions: ";
        } else {
            s->Svg << "Stage " << s->PhysicalStageId << " tasks: ";
        }
        s->Svg << "finished " << s->FinishedTasks << " of " << s->Tasks;
        if (!s->Nodes.empty()) {
            PrintNodeTasksSummary(s->Svg, s->Nodes, s->Tasks);
        }
        s->Svg << "</title>" << Endl;
        if (s->Nodes.empty()) {
            PrintTasks(s->Svg, s->Tasks, s->FinishedTasks);
        } else {
            PrintNodeTasks(s->Svg, s->Nodes);
        }
        s->Svg
        << "  " << SvgText(Config.TaskLeft + Config.TaskWidth - 2, "50%", "textc", ToString(s->Tasks))
        << "</g>" << Endl;
    }

    if (!s->Connections.empty()) {
        s->Svg
        << "<g class='plus button'>"
        << SvgRect(s->IndentX + INTERNAL_GAP_X, INTERNAL_GAP_Y * 3 + INTERNAL_HEIGHT, CONN_SIZE, CONN_SIZE, "transparent")
        << "<use href='#icon_minus' class='icon_minus' transform='translate(" << s->IndentX + INTERNAL_GAP_X << ' ' << INTERNAL_GAP_Y * 3 + INTERNAL_HEIGHT << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/>" << Endl
        << "<use href='#icon_plus' class='icon_plus' transform='translate(" << s->IndentX + INTERNAL_GAP_X << ' ' << INTERNAL_GAP_Y * 3 + INTERNAL_HEIGHT << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/></g>" << Endl;
    }
    s->Svg
        << "<g class='arup button'>"
        << SvgRect(s->IndentX + INTERNAL_GAP_X, s->Height - (INTERNAL_GAP_Y + CONN_SIZE), CONN_SIZE, CONN_SIZE, "transparent")
        << "<use href='#icon_arrowup' transform='translate(" << s->IndentX + INTERNAL_GAP_X << ' ' << s->Height - (INTERNAL_GAP_Y + CONN_SIZE) << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/></g>" << Endl;

    y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

    PrintStageConnections(s, y0, px, pw);

    PrintIngressStrip(s, y0, px, pw);

    // Last, so that nothing drawn for the stage can hide it: the badge shares the
    // task column with the throughput overlay, which spans the whole stage box.
    // Still inside the selectable group, so clicking the badge selects the stage.
    // The stage draws into its own nested svg, so the badge is placed relative to
    // that stage box, not to its offset in the whole plan.
    if (s->WaitInputWarning) {
        PrintWarningBadge(s->Svg, Config.TaskLeft + Config.TaskWidth / 2, s->Height, s->WaitInputWarning, "W");
    }

    s->Svg << "</g>" << Endl;
}

void TPlan::PrepareSvg(ui64 maxTime, ui32 timelineDelta, ui32& offsetY) {

    OffsetY = offsetY;

    PrintPlanSummary(maxTime, timelineDelta, offsetY);

    for (auto& s : Stages) {
        PrepareStageSvg(s, maxTime, timelineDelta);
    }

    offsetY += Height;
}

void TPlan::PrintStage(TStringBuilder& builder, const std::shared_ptr<TStage>& stage, TConnection* c) {

    if (stage->Connections.size() > 1) {
        builder
            << "<g data-group='g" << stage->GroupId << "' class='selectable'><title>Stage " << (stage->External ? "E" : ToString(stage->PhysicalStageId)) << "</title>" << Endl
            << SvgRect(Config.HeaderLeft + stage->IndentX, GAP_Y, INDENT_X, "100%", "stage")
            << "</g>" << Endl;
    }

    builder << "<svg class='slimable' data-stage='inner " << stage->PhysicalStageId << "' data-height='" << stage->Height << "' width='" << Config.Width << "' height='" << stage->Height << "' x='0' y='" << GAP_Y << "'>" << Endl;
    builder << stage->Svg;
    if (stage->ColumnShardHeight) {
        builder << "<svg data-cs-panel='" << stage->PhysicalStageId << "' data-height='" << stage->ColumnShardHeight
            << "' x='0' y='" << stage->Height << "' width='" << Config.Width << "' height='0' style='display:none'>"
            << stage->ColumnShardSvg << "</svg>" << Endl;
    }
    builder << "</svg>" << Endl;

    auto y = stage->Height + GAP_Y;
    for (const auto& c : stage->Connections) {
        if (c->CteConnection) {
            builder << "<svg data-stage='outer cte' data-height='" << GAP_Y + INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' width='" << Config.Width << "' height='" << GAP_Y + INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' x='0' y='" << y << "'>" << Endl;
            builder << "<svg data-stage='inner cte' data-height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' width='" << Config.Width << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' x='0' y='" << GAP_Y << "'>" << Endl;
            builder << c->CteSvg;
            builder << "</svg>" << Endl;
            builder << c->Svg;
            builder << "</svg>" << Endl;
            y += INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 + GAP_Y;
        } else {
            builder << "<svg data-stage='outer " << c->FromStage->PhysicalStageId << "' data-height='" << c->FromStage->IndentY - c->FromStage->OffsetY << "' width='" << Config.Width << "' height='" << c->FromStage->IndentY - c->FromStage->OffsetY << "' x='0' y='" << y << "'>" << Endl;
            PrintStage(builder, c->FromStage, c.get());
            builder << "</svg>" << Endl;
            y += (c->FromStage->IndentY - c->FromStage->OffsetY); // GAP_Y included
        }
    }

    if (c) {
        builder << c->Svg;
    }
}

void TPlan::PrintNodes(TStringBuilder& builder, ui64 maxTime, ui32 timelineDelta) {
    builder << SvgRect(0, GAP_Y + INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, INDENT_X, "100%", "stage");
    builder << "<svg data-stage='inner cluster' class='folded' data-height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' width='" << Config.Width << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' x='0' y='" << GAP_Y << "'>" << Endl;

    builder
        << SvgRect(Config.HeaderLeft, 0, Config.HeaderWidth, "100%", "stage")
        << SvgRect(Config.OperatorLeft, 0, Config.OperatorWidth, "100%", "stage")
        << SvgRect(Config.SummaryLeft, 0, Config.SummaryWidth, "100%", "stage")
        << SvgRect(Config.TaskLeft, 0, Config.TaskWidth, "100%", "stage")
        << SvgRect(Config.TimelineLeft, 0, Config.TimelineWidth, "100%", "stage")
        << SvgTextS(Config.HeaderLeft + INTERNAL_GAP_X + INTERNAL_WIDTH * 2 + 2, INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2, TStringBuilder() << "Cluster of " << Nodes.size() << " node(s)")
        << "<g><g class='plus button'>"
        << SvgRect(INTERNAL_GAP_X, GAP_Y, CONN_SIZE, CONN_SIZE, "transparent")
        << "<use href='#icon_minus' class='icon_minus' transform='translate(" << INTERNAL_GAP_X << ' ' << INTERNAL_GAP_Y << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/>" << Endl
        << "<use href='#icon_plus' class='icon_plus' transform='translate(" << INTERNAL_GAP_X << ' ' << INTERNAL_GAP_Y << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/></g></g>" << Endl
        ;

    builder << "</svg>" << Endl;

    ui64 physicalScale = 0;
    ui64 memoryScale = 0;
    ui64 dataScale = 0;

    for (auto& node : Nodes) {
        physicalScale = std::max(physicalScale, node->MemPhysicalUsage.Average());
        physicalScale = std::max(physicalScale, node->MemSysAllocated.Average());
        memoryScale = std::max(memoryScale, node->MemArrowDefault.Average());
        memoryScale = std::max(memoryScale, node->MemMkqlAllocated.Average());
        dataScale = std::max(dataScale, node->OutputInflightBytes.Average());
    }

    for (auto& node : Nodes) {
        builder
            << "<svg data-stage='outer node' data-height='" << GAP_Y + node->Height << "' width='" << Config.Width << "' height='" << GAP_Y + node->Height << "' x='0' y='" << node->OffsetY << "'>" << Endl
            << "<svg data-stage='inner node' data-height='" << node->Height << "' width='" << Config.Width << "' height='" << node->Height << "' x='0' y='" << GAP_Y << "'>" << Endl
            << SvgRect(Config.HeaderLeft + INDENT_X + GAP_X, 0, Config.HeaderWidth - (INDENT_X + GAP_X), "100%", "stage")
            << SvgRect(Config.OperatorLeft, 0, Config.OperatorWidth, "100%", "stage")
            << SvgRect(Config.SummaryLeft, 0, Config.SummaryWidth, "100%", "stage")
            << SvgRect(Config.TaskLeft, 0, Config.TaskWidth, "100%", "stage")
            << SvgRect(Config.TimelineLeft, 0, Config.TimelineWidth, "100%", "stage")
            << SvgTextS(Config.HeaderLeft + INTERNAL_GAP_X + INTERNAL_WIDTH * 2 + 2, INTERNAL_GAP_Y + (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2, "NodeId = " + ToString(node->NodeId));

        ui32 y0 = INTERNAL_GAP_Y;
        ui32 px = Config.TimelineLeft;
        ui32 pw = Config.TimelineWidth - timelineDelta;

        if (node->MemPhysicalUsage.Values.size()) {
            PrintStageSummary(builder, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
                { &node->MemSysFragmented, Config.Palette.Mem.Medium },
                { &node->MemSysAllocated, Config.Palette.Mem.Light },
                { &node->MemPhysicalUsage, "red" },
            }, physicalScale, {"#icon_memory", "red", "0.6 0.6"});

            px += (TimeOffset + node->MemPhysicalUsage.MinTime) * pw / maxTime;
            pw = (node->MemPhysicalUsage.MaxTime - node->MemPhysicalUsage.MinTime) * pw / maxTime;

            auto maxValue = std::max(node->MemPhysicalUsage.MaxValue, node->MemSysAllocated.MaxValue);
            builder
                << "<g><title>"
                << "Max Fragmented " << FormatBytes(node->MemSysFragmented.DisplayMaxValue * 1_MB)
                << ", Max Allocated " << FormatBytes(node->MemSysAllocated.DisplayMaxValue * 1_MB)
                << ", Max RSS " + FormatBytes(node->MemPhysicalUsage.DisplayMaxValue * 1_MB)
                << "</title>" << Endl;
            PrintSeries(builder, node->MemSysAllocated.Values, maxValue, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.Mem.Light, Config.Palette.Mem.Light);
            PrintSeries(builder, node->MemSysFragmented.Values, maxValue, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.Mem.Medium, Config.Palette.Mem.Medium);
            PrintSeries(builder, node->MemPhysicalUsage.Values, maxValue, px, y0, pw, INTERNAL_HEIGHT, "", "red", "none", false);
            builder << "</g>" << Endl;
        }

        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        if (node->MemArrowDefault.Values.size() || node->MemMkqlAllocated.Values.size() || node->MemMkqlFreeList.Values.size()) {
            PrintStageSummary(builder, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
                { &node->MemMkqlFreeList, Config.Palette.Mem.Medium },
                { &node->MemMkqlAllocated, Config.Palette.Mem.Light },
                { &node->MemArrowDefault, Config.Palette.BlockMedium },
            }, memoryScale, {"#icon_memory", Config.Palette.Mem.Medium, "0.6 0.6"});

            auto maxValue = std::max(node->MemArrowDefault.MaxValue, node->MemMkqlAllocated.MaxValue);
            builder
                << "<g><title>"
                << "Max MKQL FreeList " <<  FormatBytes(node->MemMkqlFreeList.DisplayMaxValue * 1_MB)
                << ", Max MKQL Allocated " <<  FormatBytes(node->MemMkqlAllocated.DisplayMaxValue * 1_MB)
                << ", Max Arrow " << FormatBytes(node->MemArrowDefault.DisplayMaxValue * 1_MB)
                << "</title>" << Endl;
            PrintSeries(builder, node->MemArrowDefault.Values, maxValue, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.BlockMedium, Config.Palette.BlockMedium);
            PrintSeries(builder, node->MemMkqlAllocated.Values, maxValue, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.Mem.Light, Config.Palette.Mem.Light);
            PrintSeries(builder, node->MemMkqlFreeList.Values, maxValue, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.Mem.Medium, Config.Palette.Mem.Medium);
            builder << "</g>" << Endl;
        }

        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        if (node->OutputInflightBytes.Values.size() || node->LocalInflightBytes.Values.size() || node->InputInflightBytes.Values.size()) {
            PrintStageSummary(builder, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
                { &node->InputInflightBytes, Config.Palette.Input.Medium },
                { &node->LocalInflightBytes, Config.Palette.Mem.Light },
                { &node->OutputInflightBytes, Config.Palette.Output.Medium },
            }, dataScale, {"#icon_memory", Config.Palette.Input.Dark, "0.6 0.6"});

            auto maxValue = node->OutputInflightBytes.MaxValue;
            builder
                << "<g><title>"
                << "Max Input " <<  FormatBytes(node->InputInflightBytes.DisplayMaxValue * 1_MB)
                << ", Max Local " <<  FormatBytes(node->LocalInflightBytes.DisplayMaxValue * 1_MB)
                << ", Max Output " << FormatBytes(node->OutputInflightBytes.DisplayMaxValue * 1_MB)
                << "</title>" << Endl;
            PrintSeries(builder, node->OutputInflightBytes.Values, maxValue, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.Output.Medium, Config.Palette.Output.Medium);
            PrintSeries(builder, node->LocalInflightBytes.Values, maxValue, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.Mem.Light, Config.Palette.Mem.Light);
            PrintSeries(builder, node->InputInflightBytes.Values, maxValue, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.Input.Medium, Config.Palette.Input.Medium);
            builder << "</g>" << Endl;
        }

        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        if (node->CpuTime) {
            TString tooltip;
            auto textSum = FormatTooltip(tooltip, "CPU Usage", node->CpuTime.get(), FormatUsage);
            PrintStageSummary(builder, {Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT}, {
                .Metric = node->CpuTime.get(),
                .Colors = Config.Palette.Cpu,
                .Text = textSum,
                .Tooltip = tooltip,
                .Icon = {"#icon_cpu", Config.Palette.Cpu.Medium, "0.6 0.6"},
            });

            if (!node->CpuTime->History.Deriv.empty() && node->CpuTime->History.MaxTime > node->CpuTime->History.MinTime) {
                auto px = Config.TimelineLeft + (TimeOffset + node->CpuTime->History.MinTime) * (Config.TimelineWidth - timelineDelta) / maxTime;
                auto pw = (node->CpuTime->History.MaxTime - node->CpuTime->History.MinTime) * (Config.TimelineWidth - timelineDelta) / maxTime;
                auto maxCpu = node->CpuTime->History.MaxDeriv * TIME_SERIES_RANGES / (node->CpuTime->History.MaxTime - node->CpuTime->History.MinTime);
                PrintDeriv(builder, node->CpuTime->History, px, y0, pw, INTERNAL_HEIGHT, "Max CPU " + FormatMCpu(maxCpu), Config.Palette.Cpu.Medium, Config.Palette.Cpu.Light);
            }
        }
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
        if (node->Tasks) {
            PrintTasks(builder, node->Tasks, node->FinishedTasks);
            builder
            << SvgText(Config.TaskLeft + Config.TaskWidth - 2, "50%", "textc", ToString(node->Tasks));
        }
        builder
            << "</svg>" << Endl
            << "</svg>" << Endl;
    }
}

void TPlan::PrintSvg(TStringBuilder& builder, ui64 maxTime, ui32 timelineDelta) {
    auto headerHeight = GAP_Y + TIME_HEIGHT + INTERNAL_HEIGHT;
    auto clusterHeight = Nodes.empty() ? 0 : GAP_Y + INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2;
    builder << "<svg data-height='" << Height + clusterHeight + headerHeight << "' width='" << Config.Width << "' height='" << Height + clusterHeight + headerHeight << "' x='0' y='" << OffsetY << "'>" << Endl;
    if (!Nodes.empty()) {
        builder << "<svg data-stage='outer cluster' data-height='" << NodeIndentY << "' width='" << Config.Width << "' height='" << clusterHeight << "' x='0' y='" << 0 << "'>" << Endl;
        PrintNodes(builder, maxTime, timelineDelta);
        builder << "</svg>" << Endl;
    }
    builder << "<svg width='" << Config.Width << "' height='" << Height + headerHeight << "' x='0' y='" << clusterHeight << "'>" << Endl;
    builder << SummaryBuilder;
    if (!Stages.empty()) {
        auto& stage = Stages.front();
        builder << "<svg data-stage='outer " << stage->PhysicalStageId << "' data-height='" << stage->IndentY - stage->OffsetY << "' width='" << Config.Width << "' height='" << stage->IndentY - stage->OffsetY << "' x='0' y='" << headerHeight << "'>" << Endl;
        PrintStage(builder, stage, nullptr);
        builder << "</svg>" << Endl;
    }
    builder << "</svg>" << Endl;
    builder << "</svg>" << Endl;
}

// A document carrying the message, rather than a partial render of a plan that
// could not be drawn. It has to be well-formed too: this is what a browser gets
// to show when everything else failed.
static TString ErrorSvg(TStringBuf message) {
    return TStringBuilder()
        << "<svg width='1024' height='256' xmlns='http://www.w3.org/2000/svg'>" << Endl
        << "<text x='4' y='16' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px'>"
        << SvgEscape(message) << "</text>" << Endl
        << "</svg>" << Endl;
}

// The column titles shared by every plan in the document. Only the strip itself
// is placed here; keeping it pinned to the top of the viewport while the
// document scrolls is left to the script, because CSS positioning does not
// apply to elements laid out by SVG. The group wrapper is what the script
// translates, and it also keeps the strip out of the sibling walk that the fold
// and slim handlers do over adjacent <svg> elements.
void TVisualizer::PrintColumnHeaders(TStringBuilder& svg, ui64 maxSec, ui64 deltaSec, ui32 x, ui32 w) {
    auto titleHeight = INTERNAL_GAP_Y + (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2;
    svg << "<g id='columnHeaders'>" << Endl
        << "<svg width='" << Config.Width << "' height='" << COLUMN_HEADER_HEIGHT << "' x='0' y='0'>" << Endl
        // One box per column, laid out like the boxes of a stage row below, so the
        // strip reads as the top of the same grid rather than a banner over it.
        << SvgRect(Config.HeaderLeft, 0, Config.HeaderWidth, COLUMN_HEADER_HEIGHT, "columns")
        << SvgRect(Config.OperatorLeft, 0, Config.OperatorWidth, COLUMN_HEADER_HEIGHT, "columns")
        << SvgRect(Config.TaskLeft, 0, Config.TaskWidth, COLUMN_HEADER_HEIGHT, "columns")
        << SvgRect(Config.SummaryLeft, 0, Config.SummaryWidth, COLUMN_HEADER_HEIGHT, "columns")
        << SvgRect(Config.TimelineLeft, 0, Config.TimelineWidth, COLUMN_HEADER_HEIGHT, "columns")
        << SvgTextS(Config.HeaderLeft + INTERNAL_GAP_X, titleHeight, "Stages and Operators")
        << SvgTextS(Config.OperatorLeft + 2, titleHeight, "Rows")
        << SvgTextE(Config.TaskLeft + Config.TaskWidth - 2, titleHeight, "Tasks")
        << SvgTextS(Config.SummaryLeft + 2, titleHeight, "Statistics");
    // The timeline column is titled by its own scale: the ticks the grid lines
    // running down the document are drawn at. Each mark ends at its grid line,
    // so the label reads as the time elapsed up to it; the origin is the one
    // exception, a bare "0" starting at the line, since nothing lies to its left.
    for (ui64 t = 0; t <= maxSec; t += deltaSec) {
        ui64 x1 = t * w * 1000 / MaxTime;
        svg << "<g><title>" << TInstant::MilliSeconds(BaseTime + t * 1000) << "</title>" << Endl
            << (t ? SvgTextE(x + x1 - 2, titleHeight, Sprintf("%lu:%.2lu", t / 60, t % 60))
                  : SvgTextS(x + 2, titleHeight, "0"))
            << "</g>" << Endl;
    }
    svg << "</svg>" << Endl
        // Outside the strip's own viewport, which would clip half of it away.
        << SvgLine(0, COLUMN_HEADER_HEIGHT, Config.Width, COLUMN_HEADER_HEIGHT, "columns")
        << "</g>" << Endl;
}

TString TVisualizer::PrintSvgSafe() {
    if (LoadError) {
        return ErrorSvg(LoadError);
    }
    try {
        return PrintSvg();
    } catch (std::exception& e) {
        return ErrorSvg(e.what());
    }
}

namespace {

std::optional<ui64> StepValue(const NJson::TJsonValue& node, TStringBuf path) {
    const auto* value = node.GetValueByPath(path);
    if (value && (value->GetType() == NJson::JSON_UINTEGER ||
        (value->GetType() == NJson::JSON_INTEGER && value->GetIntegerSafe() >= 0))) {
        return value->GetUIntegerSafe();
    }
    return {};
}

TString StepText(const NJson::TJsonValue& node, TStringBuf path) {
    const auto* value = node.GetValueByPath(path);
    return value && value->GetType() == NJson::JSON_STRING ? value->GetStringSafe() : TString();
}

struct TStepMetric {
    ui64 Count = 0;
    ui64 Sum = 0;
    ui64 Min = 0;
    ui64 Max = 0;
    bool Overflow = false;
    const NJson::TJsonValue* MinStep = nullptr;
    const NJson::TJsonValue* MaxStep = nullptr;

    void Add(const NJson::TJsonValue& step, TStringBuf path) {
        if (auto value = StepValue(step, path)) {
            if (!Count || *value < Min) {
                Min = *value;
                MinStep = &step;
            }
            if (!Count || *value > Max) {
                Max = *value;
                MaxStep = &step;
            }
            Overflow |= *value > std::numeric_limits<ui64>::max() - Sum;
            if (!Overflow) {
                Sum += *value;
            }
            ++Count;
        }
    }

    TString Total(TString (*format)(ui64)) const {
        return !Count ? TString("—") : Overflow ? TString("overflow") : format(Sum);
    }

    TString Summary() const {
        return Count ? Total(FormatDurationUs) + " / " + FormatDurationUs(Max) : TString("—");
    }

    void Tooltip(TStringBuilder& text, TStringBuf name, ui64 population, TString (*format)(ui64)) const {
        text << '\n' << name << ": coverage " << Count << '/' << population;
        if (Count) {
            auto minId = StepValue(*MinStep, "StepId");
            auto maxId = StepValue(*MaxStep, "StepId");
            text << ", sum " << Total(format) << ", min " << format(Min)
                << ", max " << format(Max) << ", mean " << (Overflow ? TString("overflow") : format(Sum / Count))
                << ", argmin StepId " << (minId ? ToString(*minId) : TString("—"))
                << ", argmax StepId " << (maxId ? ToString(*maxId) : TString("—"));
        }
    }
};

struct TStepGroup {
    std::vector<const NJson::TJsonValue*> Steps;
    TStepMetric Active;
    TStepMetric Wait;
    TStepMetric Read;

    TStepMetric Metric(TStringBuf path) const {
        TStepMetric metric;
        for (const auto* step : Steps) {
            metric.Add(*step, path);
        }
        return metric;
    }
};

TString StepDetails(const TStepGroup& group, TStringBuf kind, TStringBuilder& tooltip) {
    auto field = [&](TStringBuf path, TStringBuf label, TString (*format)(ui64)) {
        auto metric = group.Metric(path);
        metric.Tooltip(tooltip, label, group.Steps.size(), format);
        return metric.Total(format);
    };
    if (kind == "Filter" || kind == "IndexCheck" || kind == "Aggregation") {
        auto input = group.Metric("Details.InputRows");
        auto output = group.Metric("Details.OutputRows");
        TString result = "rows " + field("Details.InputRows", "Input rows", FormatInteger) + " → "
            + field("Details.OutputRows", "Output rows", FormatInteger);
        if (kind == "Aggregation") {
            result += " output groups";
        }
        if (kind != "Aggregation" && input.Count == group.Steps.size() && output.Count == input.Count &&
            !input.Overflow && !output.Overflow && input.Sum) {
            result += Sprintf(" (%.1f%%)", 100.0 * output.Sum / input.Sum);
        }
        return result;
    }
    if (kind == "Decode") {
        auto input = group.Metric("Details.EncodedInputBytes");
        auto output = group.Metric("Details.DecodedOutputBytes");
        TString result = field("Details.EncodedInputBytes", "Encoded input", FormatBytes) + " → "
            + field("Details.DecodedOutputBytes", "Decoded output", FormatBytes);
        if (input.Count == group.Steps.size() && output.Count == input.Count &&
            !input.Overflow && !output.Overflow && input.Sum) {
            result += Sprintf(" (%.1fx)", double(output.Sum) / input.Sum);
        }
        return result;
    }
    if (kind == "StorageRead") {
        const auto& example = *group.Steps.front();
        return "requested " + field("Details.RequestedRangeBytes", "Requested ranges", FormatBytes)
            + "; example " + StepText(example, "Details.StorageTier") + '/' + StepText(example, "Details.BackendKind");
    }
    if (kind == "FetchColumns") {
        return "requested " + field("Details.RequestedBytes", "Requested logical ranges", FormatBytes);
    }
    if (kind == "MemoryReserve") {
        return "quota " + field("Details.ReservationBytes", "Requested quota (not RSS)", FormatBytes);
    }
    if (kind == "BuildResult") {
        return "result " + field("Details.ResultRows", "Result rows", FormatInteger) + " rows, "
            + field("Details.ResultBytes", "Result bytes", FormatBytes);
    }
    if (kind == "Calculation") {
        return field("Details.RowsCount", "Rows", FormatInteger) + " rows";
    }
    return {};
}

void StepCell(TStringBuilder& svg, ui32 x, ui32 width, TStringBuf text) {
    svg << "<svg x='" << x << "' y='0' width='" << width << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y << "'>"
        << SvgTextS(INTERNAL_GAP_X, (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2, text) << "</svg>";
}

TString ColumnShardKind(TString kind) {
    if (kind == "FetchColumns") {
        return "Fetching";
    }
    if (kind == "MemoryReserve") {
        return "Reserve";
    }
    return kind ? kind : TString("Unknown");
}

TString ColumnShardAttribute(TStringBuf value) {
    TStringBuilder result;
    for (char c : SvgEscape(value)) {
        if (c == '\'') {
            result << "&apos;";
        } else if (c == '"') {
            result << "&quot;";
        } else {
            result << c;
        }
    }
    return result;
}

TString ColumnShardIdentity(const NJson::TJsonValue& value, TStringBuf field) {
    if (auto text = StepText(value, field)) {
        return text;
    }
    if (auto number = StepValue(value, field)) {
        return ToString(*number);
    }
    return {};
}

bool ColumnShardFlag(const NJson::TJsonValue& value, TStringBuf field, bool fallback = false) {
    const auto* flag = value.GetValueByPath(field);
    return flag && flag->GetType() == NJson::JSON_BOOLEAN ? flag->GetBooleanSafe() : fallback;
}

bool ColumnShardSkipped(const NJson::TJsonValue& step) {
    auto outcome = StepText(step, "Outcome");
    return outcome == "Skipped" || outcome == "skipped" || outcome == "SKIPPED";
}

struct TColumnShardTabletMetric {
    ui64 Sum = 0;
    ui64 Known = 0;
    ui64 Expected = 0;
    bool Overflow = false;

    void Add(std::optional<ui64> value) {
        ++Expected;
        if (value) {
            ++Known;
            if (*value > std::numeric_limits<ui64>::max() - Sum) {
                Overflow = true;
            } else {
                Sum += *value;
            }
        }
    }
};

struct TColumnShardMetric {
    std::map<TString, TColumnShardTabletMetric> Tablets;

    struct TSummary {
        ui64 Sum = 0;
        ui64 KnownSum = 0;
        ui64 KnownCalls = 0;
        ui64 ExpectedCalls = 0;
        ui64 KnownShards = 0;
        ui64 EligibleShards = 0;
        ui64 Min = std::numeric_limits<ui64>::max();
        ui64 Max = 0;
        TString Argmax;
        bool Overflow = false;
    };

    TSummary Summary() const {
        TSummary result;
        for (const auto& [tablet, metric] : Tablets) {
            ++result.EligibleShards;
            result.KnownCalls += metric.Known;
            result.ExpectedCalls += metric.Expected;
            if (metric.Overflow || metric.Sum > std::numeric_limits<ui64>::max() - result.Sum) {
                result.Overflow = true;
            } else {
                result.Sum += metric.Sum;
            }
            if (!metric.Overflow && metric.Known == metric.Expected) {
                if (metric.Sum > std::numeric_limits<ui64>::max() - result.KnownSum) {
                    result.Overflow = true;
                } else {
                    result.KnownSum += metric.Sum;
                }
                ++result.KnownShards;
                result.Min = std::min(result.Min, metric.Sum);
                if (!result.Argmax || metric.Sum > result.Max) {
                    result.Max = metric.Sum;
                    result.Argmax = tablet;
                }
            }
        }
        return result;
    }

    TString Cell(TString (*format)(ui64), bool distribution = true) const {
        const auto s = Summary();
        if (!s.KnownCalls) {
            return "—";
        }
        if (s.Overflow) {
            return "overflow";
        }
        TString text = format(s.Sum);
        if (s.KnownShards != s.EligibleShards) {
            text += " partial";
        }
        if (distribution && s.KnownShards) {
            text += " / " + format(s.KnownSum / s.KnownShards) + " / " + format(s.Max);
        }
        return text;
    }

    void Tooltip(TStringBuilder& out, TStringBuf label) const {
        const auto s = Summary();
        out << label << ": observed sum=" << (s.Overflow ? TString("overflow") : ToString(s.Sum))
            << "; known calls=" << s.KnownCalls << '/' << s.ExpectedCalls
            << "; complete tablet totals=" << s.KnownShards << '/' << s.EligibleShards;
        if (s.KnownShards && !s.Overflow) {
            out << "; min/mean/max=" << s.Min << '/'
                << Sprintf("%.3f", double(s.KnownSum) / s.KnownShards) << '/' << s.Max
                << "; argmax TabletId=" << s.Argmax;
        }
        out << ". ";
    }
};

TString ColumnShardSemanticNames(const NJson::TJsonValue& step, TStringBuf path) {
    const auto* value = step.GetValueByPath(path);
    if (!value || value->GetType() != NJson::JSON_ARRAY) {
        return {};
    }
    std::set<TString> names;
    for (const auto& name : value->GetArraySafe()) {
        if (name.GetType() == NJson::JSON_STRING) {
            names.insert(name.GetStringSafe());
        }
    }
    TString result;
    for (const auto& name : names) {
        if (result) { result += ", "; }
        result += name;
    }
    return result;
}

TString ColumnShardSemanticDescription(const NJson::TJsonValue& step) {
    const TString kind = StepText(step, "StepKind");
    if (kind == "Filter") {
        const TString predicate = StepText(step, "Details.Predicate");
        if (predicate) { return predicate; }
        const TString operation = StepText(step, "Operation");
        if (operation.StartsWith("Filter(")) { return operation.substr(7, operation.size() - 8); }
        return StepText(step, "Details.Purpose");
    }
    if (kind == "IndexCheck") {
        const TString type = StepText(step, "Details.IndexType");
        return (type ? type + ": " : TString()) + StepText(step, "Operation");
    }
    if (kind == "FetchColumns" || kind == "Assemble" || kind == "MemoryReserve") {
        return ColumnShardSemanticNames(step, "Details.Columns");
    }
    if (kind == "Aggregation") {
        const TString keys = ColumnShardSemanticNames(step, "Details.Keys");
        const TString functions = ColumnShardSemanticNames(step, "Details.Functions");
        return functions ? (keys ? "GROUP BY " + keys + ": " : TString()) + functions : StepText(step, "Operation");
    }
    if (kind == "Projection" || kind == "BuildResult") {
        return ColumnShardSemanticNames(step, "Details.OutputColumns");
    }
    if (kind == "Calculation") {
        const TString expression = StepText(step, "Details.Expression");
        return expression ? expression : StepText(step, "Operation");
    }
    if (kind == "Decode") {
        const TString column = StepText(step, "Details.Column");
        const TString codec = StepText(step, "Details.Codec");
        return column + (codec ? " [" + codec + "]" : TString());
    }
    if (kind == "StorageRead") {
        return StepText(step, "Details.DataKind") + " / " + StepText(step, "Details.BackendKind") + " / " + StepText(step, "Details.StorageTier");
    }
    return {};
}

struct TColumnShardNode {
    TString Name;
    TString Meta;
    TString Path;
    TString Kind;
    TString Row;
    std::map<TString, TColumnShardNode> Children;
    std::set<TString> ActiveTablets;
    ui64 Calls = 0;
    ui64 Skipped = 0;
    ui64 Failed = 0;
    ui64 Cancelled = 0;
    TColumnShardMetric Active;
    TColumnShardMetric Inclusive;
    TColumnShardMetric Wait;
    TColumnShardMetric Read;
    TColumnShardMetric SelectedPortions;
    TColumnShardMetric SelectedVersionRows;
    TColumnShardMetric SelectedBlobBytes;
    TColumnShardMetric SentRows;
    TColumnShardMetric SentArrowBytes;
    TColumnShardMetric SentDataBatches;
    TColumnShardMetric ReaderOverhead;
    TColumnShardMetric ReaderOverviewWait;
    TStepGroup Details;
    std::set<TString> Descriptions;
    std::map<TString, std::set<TString>> SemanticDescriptions;
    bool SemanticDescriptionsTruncated = false;

    TColumnShardNode& Child(TString name, TString meta, TString path, TString kind, TString row) {
        auto& child = Children[name];
        child.Name = std::move(name);
        child.Meta = std::move(meta);
        child.Path = std::move(path);
        child.Kind = std::move(kind);
        child.Row = std::move(row);
        return child;
    }

    void Add(const NJson::TJsonValue& step, const TString& tablet, bool summary, bool directWait) {
        const TString semantic = ColumnShardSemanticDescription(step);
        if (semantic) {
            auto& descriptions = SemanticDescriptions[StepText(step, "StepKind")];
            if (descriptions.size() < 64 || descriptions.contains(semantic)) {
                descriptions.insert(semantic);
            } else {
                SemanticDescriptionsTruncated = true;
            }
        }
        if (ColumnShardSkipped(step)) {
            ++Skipped;
            return;
        }
        ++Calls;
        ActiveTablets.insert(tablet);
        auto outcome = StepText(step, "Outcome");
        Failed += outcome == "Failed" || outcome == "failed" || outcome == "FAILED";
        Cancelled += outcome == "Cancelled" || outcome == "cancelled" || outcome == "CANCELLED";
        Active.Tablets[tablet].Add(StepValue(step, "OwnedExecutionDuration"));
        if (directWait) {
            Wait.Tablets[tablet].Add(StepValue(step, "IntegralWaitDuration"));
        }
        const TString kind = ColumnShardKind(StepText(step, "StepKind"));
        if (kind == "SelectPortions") {
            SelectedPortions.Tablets[tablet].Add(StepValue(step, "Details.SelectedPortions"));
            SelectedVersionRows.Tablets[tablet].Add(StepValue(step, "Details.SelectedVersionRows"));
            SelectedBlobBytes.Tablets[tablet].Add(StepValue(step, "Details.SelectedBlobBytes"));
        }
        if (kind == "Send") {
            const auto rows = StepValue(step, "Details.Rows");
            const auto batches = StepValue(step, "Details.DataBatches");
            const auto bytes = StepValue(step, "Details.ArrowBytes");
            SentRows.Tablets[tablet].Add(rows);
            SentDataBatches.Tablets[tablet].Add(batches);
            // Exclude only proven controls; an unmeasured data payload remains unknown.
            if (bytes || !rows || !batches || *rows || *batches) {
                SentArrowBytes.Tablets[tablet].Add(bytes);
            }
        }
        const bool core = summary && ((Name == "PKFilter" && kind == "Filter") ||
            (Name == "IndexFilter" && kind == "IndexCheck") || (Name == "Filter" && kind == "Filter") ||
            (Name == "FetchOtherColumns" && kind == "Fetching") || (Name == "Aggregation" && kind == "Aggregation") ||
            (Name == "BuildResult" && kind == "BuildResult") || (Name == "Preparation" && kind == "SelectPortions") ||
            (Name == "Output" && kind == "Send"));
        if (!summary) {
            Inclusive.Tablets[tablet].Add(StepValue(step, "IntegralExecutionDuration"));
            Read.Tablets[tablet].Add(StepValue(step, "ReadBytes"));
        }
        if (!summary || core) {
            Details.Steps.push_back(&step);
            for (TStringBuf field : {TStringBuf("Operation"), TStringBuf("StepName"), TStringBuf("Details.Predicate"),
                     TStringBuf("Details.Column"), TStringBuf("Details.IndexType"), TStringBuf("Details.Purpose"),
                     TStringBuf("Details.Codec"), TStringBuf("Details.BackendKind"), TStringBuf("Details.StorageTier")}) {
                if (Descriptions.size() < 12) {
                    if (auto description = StepText(step, field)) {
                        Descriptions.insert(std::move(description));
                    }
                }
            }
            for (TStringBuf field : {TStringBuf("Details.Columns"), TStringBuf("Details.Keys"), TStringBuf("Details.Functions"),
                     TStringBuf("Details.InputColumns"), TStringBuf("Details.OutputColumns")}) {
                const auto* values = step.GetValueByPath(field);
                if (values && values->GetType() == NJson::JSON_ARRAY) {
                    for (const auto& value : values->GetArray()) {
                        if (value.GetType() == NJson::JSON_STRING && Descriptions.size() < 12) {
                            Descriptions.insert(TStringBuilder() << field << ": " << value.GetStringSafe());
                        }
                    }
                }
            }
        }
    }
};

struct TColumnShardAttempt {
    const NJson::TJsonValue* Last = nullptr;
    const NJson::TJsonValue* Cache = nullptr;
    TString Tablet;
    TString Reader;
    TString StatsGroupId;
    std::optional<ui64> ParticipatingShards;
    std::set<TString> ParticipatingTablets;
    bool HasRoster = false;
    bool ConflictingHeader = false;
    std::map<ui64, const NJson::TJsonValue*> Steps;
    TVector<const NJson::TJsonValue*> UnidentifiedSteps;
    bool MissingIdentity = false;
};

void ReadColumnShardCohortHeader(TColumnShardAttempt& attempt, const NJson::TJsonValue& scan) {
    if (auto group = StepText(scan, "StatsGroupId")) {
        if (attempt.StatsGroupId && attempt.StatsGroupId != group) {
            attempt.ConflictingHeader = true;
        } else {
            attempt.StatsGroupId = std::move(group);
        }
    }
    if (auto expected = StepValue(scan, "ParticipatingShards")) {
        if (attempt.ParticipatingShards && attempt.ParticipatingShards != expected) {
            attempt.ConflictingHeader = true;
        } else {
            attempt.ParticipatingShards = expected;
        }
    }
    const auto* roster = scan.GetValueByPath("ParticipatingTablets");
    const bool expectedEmpty = ColumnShardFlag(scan, "ExpectedContextOnly") && attempt.ParticipatingShards &&
        !*attempt.ParticipatingShards && (!roster || (roster->GetType() == NJson::JSON_ARRAY && roster->GetArray().empty()));
    if ((roster && roster->GetType() == NJson::JSON_ARRAY && !roster->GetArray().empty()) || expectedEmpty) {
        std::set<TString> tablets;
        if (roster) {
            for (const auto& value : roster->GetArray()) {
                if (value.GetType() == NJson::JSON_STRING && value.GetStringSafe()) {
                    tablets.insert(value.GetStringSafe());
                } else if (value.GetType() == NJson::JSON_UINTEGER ||
                    (value.GetType() == NJson::JSON_INTEGER && value.GetIntegerSafe() >= 0)) {
                    tablets.insert(ToString(value.GetUIntegerSafe()));
                } else {
                    attempt.ConflictingHeader = true;
                }
            }
        }
        if (attempt.HasRoster && attempt.ParticipatingTablets != tablets) {
            attempt.ConflictingHeader = true;
        } else {
            attempt.ParticipatingTablets = std::move(tablets);
            attempt.HasRoster = true;
        }
    } else if (roster && roster->GetType() != NJson::JSON_ARRAY) {
        attempt.ConflictingHeader = true;
    }
}

struct TColumnShardRead {
    struct TCohort {
        ui64 Expected = 0;
        std::set<TString> Tablets;
        std::set<TString> Roster;
        bool HasRoster = false;
        bool Conflict = false;
    };

    TString Family;
    std::set<TString> Tablets;
    std::set<TString> ReadTablets;
    std::map<TString, TColumnShardNode> Blocks;
    std::map<TString, TColumnShardTabletMetric> CacheFields[4];
    std::set<TString> CacheTablets;
    std::map<TString, TCohort> Cohorts;
    bool MissingCohortHeader = false;
    bool ConflictingCohortHeader = false;
    bool ExpectedKnown = false;
    ui64 ExpectedShards = 0;
    std::set<TString> KnownExpectedTablets;
    bool Partial = false;
    bool Unclassified = false;
    ui64 Attempts = 0;
    ui64 PendingAttempts = 0;
    ui64 MaxPendingOperations = 0;

    TColumnShardRead() {
        Blocks["Preparation"].Name = "Preparation";
        Blocks["Preparation"].Meta = "Preparation";
        Blocks["Preparation"].Row = "meta";
        Blocks["Execution"].Name = "Execution";
        Blocks["Execution"].Row = "block";
        Blocks["Output"].Name = "Output";
        Blocks["Output"].Meta = "Output";
        Blocks["Output"].Row = "meta";
        for (TStringBuf meta : {TStringBuf("PKFilter"), TStringBuf("IndexFilter"), TStringBuf("Filter"),
                 TStringBuf("FetchOtherColumns"), TStringBuf("Aggregation"), TStringBuf("Projection"), TStringBuf("BuildResult")}) {
            Blocks["Execution"].Child(TString(meta), TString(meta), {}, {}, "meta");
        }
    }

    void AddCohort(const TColumnShardAttempt& attempt, const TString& tablet, bool contextOnly = false) {
        if (!attempt.StatsGroupId || !attempt.ParticipatingShards || (!contextOnly && !attempt.Tablet)) {
            MissingCohortHeader = true;
            return;
        }
        auto [it, inserted] = Cohorts.try_emplace(attempt.StatsGroupId);
        auto& cohort = it->second;
        if (inserted) {
            cohort.Expected = *attempt.ParticipatingShards;
        } else if (cohort.Expected != *attempt.ParticipatingShards) {
            cohort.Conflict = true;
        }
        if (!contextOnly) {
            cohort.Tablets.insert(tablet);
        }
        cohort.Conflict = cohort.Conflict || attempt.ConflictingHeader;
        if (attempt.HasRoster) {
            if (cohort.HasRoster && cohort.Roster != attempt.ParticipatingTablets) {
                cohort.Conflict = true;
            } else {
                cohort.Roster = attempt.ParticipatingTablets;
                cohort.HasRoster = true;
            }
        }
    }

    void FinalizeCohorts() {
        ExpectedKnown = !Cohorts.empty() && !MissingCohortHeader;
        for (const auto& [_, cohort] : Cohorts) {
            bool valid = cohort.HasRoster && !cohort.Conflict && cohort.Expected == cohort.Roster.size();
            for (const auto& tablet : cohort.Tablets) {
                valid = valid && cohort.Roster.contains(tablet);
            }
            if (valid) {
                KnownExpectedTablets.insert(cohort.Roster.begin(), cohort.Roster.end());
            } else {
                ExpectedKnown = false;
                ConflictingCohortHeader = ConflictingCohortHeader || cohort.Conflict || cohort.HasRoster;
            }
        }
        ExpectedShards = KnownExpectedTablets.size();
        ExpectedKnown = ExpectedKnown && !ConflictingCohortHeader && ExpectedShards >= Tablets.size();
        Partial = Partial || ConflictingCohortHeader;
    }

    ui64 Denominator() const {
        return ExpectedKnown ? ExpectedShards : ReceivedShards();
    }

    ui64 ReceivedShards() const {
        return ReadTablets.empty() ? Tablets.size() : ReadTablets.size();
    }

    void Add(const NJson::TJsonValue& step, const TString& tablet) {
        if (!ColumnShardSkipped(step) && !StepValue(step, "OwnedExecutionDuration")) {
            Partial = true;
        }
        TString meta = StepText(step, "MetaStep");
        const TString kind = ColumnShardKind(StepText(step, "StepKind"));
        const TString path = StepText(step, "CanonicalPath");
        if (!meta && kind == "Reader" && !path) {
            auto& execution = Blocks["Execution"];
            execution.Add(step, tablet, true, false);
            if (!ColumnShardSkipped(step)) {
                execution.ReaderOverhead.Tablets[tablet].Add(StepValue(step, "OwnedExecutionDuration"));
                execution.ReaderOverviewWait.Tablets[tablet].Add(StepValue(step, "IntegralWaitDuration"));
                execution.Inclusive.Tablets[tablet].Add(StepValue(step, "IntegralExecutionDuration"));
            }
            return;
        }
        if (meta != "Preparation" && meta != "Output" && !Blocks["Execution"].Children.contains(meta)) {
            meta = "Unclassified";
            Unclassified = true;
        }
        const bool directWait = !path && kind != "Reader";
        TColumnShardNode* parent;
        if (meta == "Preparation" || meta == "Output") {
            parent = &Blocks[meta];
        } else {
            auto& execution = Blocks["Execution"];
            execution.Add(step, tablet, true, directWait);
            parent = &execution.Child(meta, meta, {}, {}, "meta");
        }
        parent->Add(step, tablet, true, directWait);
        TString prefix;
        size_t offset = 0;
        unsigned depth = 0;
        while (offset < path.size() && depth++ < 16) {
            auto end = path.find('/', offset);
            if (end == TString::npos) {
                end = path.size();
            }
            TString ancestor = ColumnShardKind(path.substr(offset, end - offset));
            parent = &parent->Child(ancestor, meta, prefix, ancestor, "step");
            if (prefix) {
                prefix += '/';
            }
            prefix += ancestor;
            offset = end + 1;
        }
        if (offset < path.size()) {
            Partial = true;
        }
        auto& leaf = parent->Child(kind, meta, prefix, kind, "step");
        leaf.Add(step, tablet, false, true);
    }
};

unsigned ColumnShardOrder(TStringBuf name, const TColumnShardNode& parent) {
    static constexpr TStringBuf metas[] = {"PKFilter", "IndexFilter", "Filter", "FetchOtherColumns", "Aggregation", "Projection", "BuildResult"};
    static constexpr TStringBuf steps[] = {"SourcePreparation", "Reader", "FetchAccessors", "Reserve", "Fetching", "Assemble", "Decode",
        "Calculation", "IndexCheck", "Filter", "DuplicateFilter", "Merge", "Aggregation", "Projection", "PrepareResult", "BuildResult",
        "Ready", "Limit", "Cursor", "Send", "Ack", "Finish", "Cleanup"};
    static constexpr TStringBuf preparation[] = {"ParseProgram", "SelectPortions", "ReaderInit", "Reader", "FetchAccessors", "Reserve",
        "Fetching", "Assemble", "Decode", "Calculation"};
    static constexpr TStringBuf result[] = {"Reader", "PrepareResult", "BuildResult", "Assemble", "Decode", "Calculation", "Cleanup"};
    static constexpr TStringBuf output[] = {"Reader", "Ready", "Limit", "Cursor", "Send", "Ack", "Finish", "Cleanup"};
    const TStringBuf* order = steps;
    size_t count = std::size(steps);
    if (parent.Row == "block" && parent.Name == "Execution") {
        order = metas;
        count = std::size(metas);
    } else if (parent.Row == "meta" && parent.Name == "Preparation") {
        order = preparation;
        count = std::size(preparation);
    } else if (parent.Row == "meta" && parent.Name == "BuildResult") {
        order = result;
        count = std::size(result);
    } else if (parent.Row == "meta" && parent.Name == "Output") {
        order = output;
        count = std::size(output);
    }
    for (unsigned i = 0; i < count; ++i) {
        if (order[i] == name) {
            return i;
        }
    }
    return count;
}

TString ColumnShardDetails(const TColumnShardNode& node, bool partial, TStringBuilder& tooltip) {
    if (node.ReaderOverhead.Summary().ExpectedCalls) {
        node.ReaderOverhead.Tooltip(tooltip, "Owned reader dispatcher overhead, us");
        node.ReaderOverviewWait.Tooltip(tooltip, "Inclusive reader wait (overview only, excluded from primary), us");
        return "reader overhead " + node.ReaderOverhead.Cell(FormatDurationUs, false);
    }
    if (node.SentRows.Summary().ExpectedCalls) {
        node.SentRows.Tooltip(tooltip, "Output event rows");
        node.SentDataBatches.Tooltip(tooltip, "Output data batches (events with rows)");
        node.SentArrowBytes.Tooltip(tooltip, "Represented Arrow payload bytes (not serialized/network bytes)");
        tooltip << "Metadata/control events have zero data batches. Controls without Arrow payload are outside Arrow-byte eligibility. "
            << "A measured zero Arrow payload is included; a data event without an Arrow-byte measurement is unknown. "
            << "Output Arrow bytes and BuildResult result bytes describe different boundaries and must not be added.";
        return "rows " + node.SentRows.Cell(FormatInteger, false) + "; data batches "
            + node.SentDataBatches.Cell(FormatInteger, false) + "; Arrow payload " + node.SentArrowBytes.Cell(FormatBytes, false);
    }
    if (node.SelectedPortions.Summary().ExpectedCalls) {
        node.SelectedPortions.Tooltip(tooltip, "Metadata selected portions");
        node.SelectedVersionRows.Tooltip(tooltip, "Metadata physical version rows (estimate)");
        node.SelectedBlobBytes.Tooltip(tooltip, "Metadata blob bytes (estimate, not actual read)");
        return "portions " + node.SelectedPortions.Cell(FormatInteger, false) + "; metadata versions "
            + node.SelectedVersionRows.Cell(FormatInteger, false) + "; metadata blobs " + node.SelectedBlobBytes.Cell(FormatBytes, false);
    }
    if (node.Details.Steps.empty()) {
        return {};
    }
    auto field = [&](TStringBuf path, TStringBuf label, TString (*format)(ui64)) {
        auto metric = node.Details.Metric(path);
        metric.Tooltip(tooltip, label, node.Details.Steps.size(), format);
        return metric.Total(format);
    };
    TString kind = node.Kind;
    if (!kind) {
        if (node.Name == "PKFilter" || node.Name == "Filter") {
            kind = "Filter";
        } else if (node.Name == "IndexFilter") {
            kind = "IndexCheck";
        } else if (node.Name == "FetchOtherColumns") {
            kind = "Fetching";
        } else {
            kind = node.Name;
        }
    }
    if (kind == "Filter" || kind == "IndexCheck" || kind == "Aggregation") {
        TString result = kind == "IndexCheck" ? "processed check candidates (calls) "
            : kind == "Aggregation" ? "aggregate call rows " : "processed filter candidates (calls) ";
        result += field("Details.InputRows", "Call input rows", FormatInteger) + " → "
            + field("Details.OutputRows", "Call output rows", FormatInteger);
        auto input = node.Details.Metric("Details.InputRows");
        auto output = node.Details.Metric("Details.OutputRows");
        if (node.Meta == "Filter" && kind == "Filter" && !partial && input.Count == node.Details.Steps.size() &&
            output.Count == input.Count && !input.Overflow && !output.Overflow && input.Sum) {
            result += Sprintf(" (call-weighted %.1f%%)", 100.0 * output.Sum / input.Sum);
        }
        tooltip << "These are processed call populations, not unique rows of the whole metastep. ";
        if (kind == "Aggregation") {
            tooltip << "Scalar output rows are not groups. Keyed output rows may be partial groups; no final unique-group count is inferred.";
        }
        return result;
    }
    if (kind == "Decode") {
        return field("Details.EncodedInputBytes", "Encoded input", FormatBytes) + " → "
            + field("Details.DecodedOutputBytes", "Decoded output", FormatBytes);
    }
    if (kind == "Fetching") {
        return "requested " + field("Details.RequestedBytes", "Requested logical ranges", FormatBytes)
            + "; delivered " + field("ReadBytes", "Delivered serialized slices", FormatBytes);
    }
    if (kind == "StorageRead") {
        return "requested " + field("Details.RequestedRangeBytes", "Requested backend ranges", FormatBytes);
    }
    if (kind == "Reserve") {
        return "quota " + field("Details.ReservationBytes", "Requested quota (not RSS)", FormatBytes);
    }
    if (kind == "BuildResult") {
        return field("Details.ResultRows", "Result rows", FormatInteger) + " rows / "
            + field("Details.ResultBytes", "Arrow result bytes", FormatBytes);
    }
    if (kind == "Calculation") {
        return field("Details.RowsCount", "Processed calculation rows", FormatInteger) + " rows";
    }
    return {};
}

TString ColumnShardSemanticLabel(const TColumnShardNode& node, TStringBuilder& tooltip) {
    TString kind = node.Kind;
    if (kind == "Reserve") { kind = "MemoryReserve"; }
    if (kind == "Fetching") { kind = "FetchColumns"; }
    if (node.Row != "step") {
        if (node.Name == "PKFilter" || node.Name == "Filter") { kind = "Filter"; }
        else if (node.Name == "IndexFilter") { kind = "IndexCheck"; }
        else if (node.Name == "FetchOtherColumns") { kind = "FetchColumns"; }
        else { kind = node.Name; }
    }
    const auto descriptions = node.SemanticDescriptions.find(kind);
    if (descriptions == node.SemanticDescriptions.end()) { return {}; }
    tooltip << "Semantic descriptions (distinct observed variants; variants are not a conjunction): ";
    TString label;
    ui32 count = 0;
    for (const auto& description : descriptions->second) {
        tooltip << description << "; ";
        if (count++ < 2) {
            if (label) { label += " | "; }
            label += description;
        }
    }
    if (count > 2) { label += " (+" + ToString(count - 2) + " variants)"; }
    if (node.SemanticDescriptionsTruncated) { tooltip << "Description limit reached; additional variants omitted. "; }
    return label;
}

ui32 PrintColumnShardNode(TStringBuilder& svg, const TVisualizer& viz, const TColumnShardRead& read,
    const TColumnShardNode& node, ui32 top, ui32 depth) {
    constexpr ui32 rowHeight = INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    std::vector<const TColumnShardNode*> children;
    for (const auto& [_, child] : node.Children) {
        children.push_back(&child);
    }
    std::stable_sort(children.begin(), children.end(), [&node](const auto* left, const auto* right) {
        return std::make_pair(ColumnShardOrder(left->Name, node), left->Name) < std::make_pair(ColumnShardOrder(right->Name, node), right->Name);
    });
    TStringBuilder body;
    ui32 height = rowHeight;
    for (const auto* child : children) {
        height += PrintColumnShardNode(body, viz, read, *child, height - rowHeight, depth + 1);
    }
    TStringBuilder tooltip;
    tooltip << node.Meta << '/' << node.Path << '/' << node.Kind << "; calls=" << node.Calls
        << "; active tablets=" << node.ActiveTablets.size() << '/' << read.Denominator()
        << (read.ExpectedKnown ? " planned physical tablets" : " received physical tablets; expected total unknown")
        << "; received in logical read=" << read.ReceivedShards() << "; received in this family=" << read.Tablets.size()
        << "; known expected roster union=" << read.KnownExpectedTablets.size()
        << "; skipped=" << node.Skipped << "; failed=" << node.Failed << "; cancelled=" << node.Cancelled << ". "
        << "Primary Active is owned elapsed work; step rows show their own work, and metasteps include each descendant once. "
        << "Inclusive parent/child timers are not added. Metastep Wait uses the direct blocked-operation frontier. "
        << "Displayed order is semantic; independent branches and tablets may overlap.";
    node.Active.Tooltip(tooltip, "Owned active, us");
    node.Inclusive.Tooltip(tooltip, "Inclusive active (details only), us");
    node.Wait.Tooltip(tooltip, "Wait, us");
    node.Read.Tooltip(tooltip, "Received bytes (this boundary)");
    TString details = ColumnShardDetails(node, read.Partial, tooltip);
    const TString semanticLabel = ColumnShardSemanticLabel(node, tooltip);
    if (semanticLabel) {
        details = semanticLabel + (details ? "; " + details : TString());
    }
    if (!node.Descriptions.empty()) {
        tooltip << "Examples / descriptions (up to 12): ";
    }
    for (const auto& description : node.Descriptions) {
        tooltip << description << "; ";
    }
    if (read.Partial) {
        tooltip << "PARTIAL population: observed work only; absent measurements are not zero. ";
    }
    if (read.PendingAttempts) {
        tooltip << read.PendingAttempts << " latest attempt reports have unfinalized operations; maximum pending="
            << read.MaxPendingOperations << ". Shared async work may continue after LIMIT/cancel; later costs are absent from this snapshot. ";
    }
    tooltip << "Expected roster covers observed StatsGroupId cohorts only; a wholly missing cohort requires external stage/query coverage.";
    if (read.ExpectedKnown && read.ExpectedShards > read.ReceivedShards()) {
        tooltip << ' ' << read.ExpectedShards - read.ReceivedShards()
            << " planned tablets have no runtime report; they may not have started (for example due to LIMIT). "
            << "This difference alone does not establish lost runtime data.";
    }
    const bool structural = node.Row == "step" && !node.Calls && !node.Skipped && !children.empty();
    if (!node.Calls) {
        details = structural ? "parent context (no separate call)" : !read.Attempts ? "runtime not observed"
            : read.Partial || read.Unclassified ? "not observed" : "not executed in received runtimes";
        if (read.Family == "PLAIN" && node.Name == "FetchOtherColumns") {
            details = "inputs materialized before merge";
        }
        if (read.Family == "PLAIN" && node.Name == "IndexFilter") {
            details = "no physical index fetching after merge";
        }
    }
    if (read.Family == "PLAIN" && node.Meta == "IndexFilter" && node.Calls) {
        tooltip << "PLAIN checker visits use the program's fake data source; they do not prove physical index usage.";
        if (node.Row == "meta") {
            details = "program-only checks (no physical index fetching after merge); " + details;
        }
    }
    const auto active = node.Active.Summary();
    const auto wait = node.Wait.Summary();
    svg << "<svg data-cs-row='" << node.Row << "' data-cs-family='" << ColumnShardAttribute(read.Family)
        << "' data-cs-meta='" << ColumnShardAttribute(node.Meta) << "' data-cs-path='" << ColumnShardAttribute(node.Path)
        << "' data-cs-kind='" << ColumnShardAttribute(node.Kind) << "' data-cs-active-shards='" << node.ActiveTablets.size()
        << "' data-cs-participating-shards='" << read.Denominator() << "' data-cs-received-shards='" << read.ReceivedShards()
        << "' data-cs-family-shards='" << read.Tablets.size()
        << "' data-cs-expected-known='" << (read.ExpectedKnown ? "true" : "false")
        << "' data-cs-known-expected-shards='" << read.KnownExpectedTablets.size()
        << "' data-cs-denominator='" << (read.ExpectedKnown ? "expected" : "received")
        << "' data-cs-structural='" << (structural ? "true" : "false") << "' data-cs-calls='" << node.Calls
        << "' data-cs-active-sum='" << active.Sum << "' data-cs-owned-sum='" << active.Sum
        << "' data-cs-active-overflow='" << (active.Overflow ? "true" : "false")
        << "' data-cs-active-known-shards='" << active.KnownShards
        << "' data-cs-active-eligible-shards='" << active.EligibleShards << "' data-cs-active-mean='"
        << (active.KnownShards ? double(active.KnownSum) / active.KnownShards : 0)
        << "' data-cs-active-min='" << (active.KnownShards ? active.Min : 0) << "' data-cs-active-max='" << active.Max
        << "' data-cs-wait-sum='" << wait.Sum << "' data-cs-partial='" << (read.Partial ? "true" : "false");
    if (node.SelectedPortions.Summary().ExpectedCalls) {
        const auto portions = node.SelectedPortions.Summary();
        const auto versions = node.SelectedVersionRows.Summary();
        const auto blobs = node.SelectedBlobBytes.Summary();
        svg << "' data-cs-selected-portions='" << portions.Sum << "' data-cs-selected-portions-known-shards='" << portions.KnownShards
            << "' data-cs-selected-version-rows='" << versions.Sum << "' data-cs-selected-version-rows-known-shards='" << versions.KnownShards
            << "' data-cs-selected-blob-bytes='" << blobs.Sum << "' data-cs-selected-blob-bytes-known-shards='" << blobs.KnownShards;
    }
    if (node.SentRows.Summary().ExpectedCalls) {
        const auto rows = node.SentRows.Summary();
        const auto batches = node.SentDataBatches.Summary();
        const auto bytes = node.SentArrowBytes.Summary();
        svg << "' data-cs-output-rows='" << rows.Sum << "' data-cs-output-rows-known-shards='" << rows.KnownShards
            << "' data-cs-output-data-batches='" << batches.Sum << "' data-cs-output-data-batches-known-shards='" << batches.KnownShards
            << "' data-cs-output-arrow-bytes='" << bytes.Sum << "' data-cs-output-arrow-bytes-known-shards='" << bytes.KnownShards
            << "' data-cs-output-arrow-bytes-eligible-shards='" << bytes.EligibleShards;
    }
    const auto readBytes = node.Read.Summary();
    if (node.ReaderOverhead.Summary().ExpectedCalls) {
        const auto overhead = node.ReaderOverhead.Summary();
        svg << "' data-cs-reader-overhead-owned='" << overhead.Sum << "' data-cs-reader-overhead-known-shards='" << overhead.KnownShards;
    }
    if (node.Row == "step" && readBytes.KnownCalls) {
        const TStringBuf boundary = node.Kind == "Fetching" ? TStringBuf("delivered")
            : node.Kind == "StorageRead" ? TStringBuf("backend") : TStringBuf("operation");
        svg << "' data-cs-read-bytes='" << readBytes.Sum << "' data-cs-read-known-shards='" << readBytes.KnownShards
            << "' data-cs-read-boundary='" << boundary;
    }
    const auto callInput = node.Details.Metric("Details.InputRows");
    const auto callOutput = node.Details.Metric("Details.OutputRows");
    if (callInput.Count || callOutput.Count) {
        svg << "' data-cs-call-row-samples='" << node.Details.Steps.size()
            << "' data-cs-call-input-known='" << callInput.Count << "' data-cs-call-output-known='" << callOutput.Count;
        if (callInput.Count && !callInput.Overflow) {
            svg << "' data-cs-call-input-rows='" << callInput.Sum;
        }
        if (callOutput.Count && !callOutput.Overflow) {
            svg << "' data-cs-call-output-rows='" << callOutput.Sum;
        }
    }
    svg << "' x='0' y='" << top << "' width='" << viz.Config.Width << "' height='" << height << "'>"
        << "<svg width='" << viz.Config.Width << "' height='" << rowHeight << "'><g><title>" << SvgEscape(tooltip) << "</title>"
        << SvgRect(0, 0, viz.Config.Width, rowHeight, node.Row == "step" ? "stage" : "columns");
    if (!children.empty()) {
        svg << "<g class='plus button' data-cs-toggle='row'>" << SvgRect(depth * INTERNAL_WIDTH, 0, CONN_SIZE, CONN_SIZE, "transparent")
            << "<use href='#icon_minus' class='icon_minus' transform='translate(" << depth * INTERNAL_WIDTH
            << ",0) scale(0.014)' fill='" << viz.Config.Palette.ConnectionText << "'/>"
            << "<use href='#icon_plus' class='icon_plus' transform='translate(" << depth * INTERNAL_WIDTH
            << ",0) scale(0.014)' fill='" << viz.Config.Palette.ConnectionText << "'/></g>";
    }
    const ui32 indent = std::min<ui32>((depth + 1) * INTERNAL_WIDTH, 270);
    StepCell(svg, indent, 315 - indent, node.Name + (semanticLabel ? " (" + semanticLabel + ")" : TString()));
    StepCell(svg, 315, 90, TStringBuilder() << node.ActiveTablets.size() << '/' << read.Denominator()
        << (read.ExpectedKnown ? "" : " recv"));
    StepCell(svg, 405, 65, FormatInteger(node.Calls));
    StepCell(svg, 470, 205, node.Active.Cell(FormatDurationUs));
    StepCell(svg, 675, 205, node.Wait.Cell(FormatDurationUs));
    StepCell(svg, 880, 110, node.Read.Cell(FormatBytes, false));
    StepCell(svg, 990, viz.Config.Width > 990 ? viz.Config.Width - 990 : 0, details);
    svg << "</g></svg>";
    if (!children.empty()) {
        svg << "<svg x='0' y='" << rowHeight << "' width='" << viz.Config.Width << "' height='" << height - rowHeight << "'>"
            << body << "</svg>";
    }
    svg << "</svg>" << Endl;
    return height;
}

bool HasColumnShardMetaStats(const TVisualizer& viz, const TStage* onlyStage) {
    for (const auto& plan : viz.Plans) {
        for (const auto& stage : plan->Stages) {
            if (stage.get() != onlyStage) {
                continue;
            }
            for (const auto& scan : stage->ColumnShardScans.GetArray()) {
                if (scan.GetType() != NJson::JSON_MAP) {
                    continue;
                }
                if (ColumnShardFlag(scan, "ExpectedContextOnly")) {
                    return true;
                }
                if (StepText(scan, "ReaderKind")) {
                    return true;
                }
                const auto* steps = scan.GetValueByPath("Steps");
                if (steps && steps->GetType() == NJson::JSON_ARRAY) {
                    for (const auto& step : steps->GetArray()) {
                        if (step.GetType() == NJson::JSON_MAP && StepText(step, "MetaStep")) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    return false;
}

ui32 PrintColumnShardCache(TStringBuilder& svg, const TVisualizer& viz, const TColumnShardRead& read, ui32 top) {
    constexpr ui32 rowHeight = INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    TStringBuilder tooltip;
    ui64 sums[4] = {};
    bool known = !read.Partial;
    for (unsigned i = 0; i < 4; ++i) {
        TColumnShardMetric metric;
        metric.Tablets = read.CacheFields[i];
        const auto s = metric.Summary();
        sums[i] = s.Sum;
        known = known && !s.Overflow && s.KnownShards == read.Tablets.size();
        static constexpr TStringBuf labels[] = {"CompletedReadRanges", "CacheServedReadRanges", "ReturnedBytes", "CacheServedBytes"};
        metric.Tooltip(tooltip, labels[i]);
    }
    TStringBuilder details;
    if (known && sums[0] && sums[2] && sums[1] <= sums[0] && sums[3] <= sums[2]) {
        details << Sprintf("cache served ranges %.1f%%; bytes %.1f%%", 100.0 * sums[1] / sums[0], 100.0 * sums[3] / sums[2]);
    } else {
        details << "cache ratios unavailable";
    }
    tooltip << "Successful cache reply population; ratios are ratios of sums, not average tablet percentages. "
        << "These counters cover received runtime reports of the displayed family. "
        << "Cache-served bytes are not automatically saved physical I/O. Active here means tablets with successful cache replies.";
    svg << "<svg data-cs-row='cache' data-cs-family='" << ColumnShardAttribute(read.Family)
        << "' data-cs-active-shards='" << read.CacheTablets.size() << "' data-cs-participating-shards='" << read.Denominator()
        << "' data-cs-received-shards='" << read.ReceivedShards() << "' data-cs-family-shards='" << read.Tablets.size()
        << "' data-cs-expected-known='" << (read.ExpectedKnown ? "true" : "false")
        << "' data-cs-denominator='" << (read.ExpectedKnown ? "expected" : "received")
        << "' data-cs-cache-ranges='" << sums[0] << "' data-cs-cache-served-ranges='" << sums[1]
        << "' data-cs-cache-bytes='" << sums[2] << "' data-cs-cache-served-bytes='" << sums[3]
        << "' x='0' y='" << top << "' width='" << viz.Config.Width << "' height='" << rowHeight << "'><g>"
        << "<title>" << SvgEscape(tooltip) << "</title>" << SvgRect(0, 0, viz.Config.Width, rowHeight, "columns");
    StepCell(svg, INTERNAL_WIDTH, 315 - INTERNAL_WIDTH, "Caches · blob cache");
    StepCell(svg, 315, 90, TStringBuilder() << read.CacheTablets.size() << '/' << read.Denominator()
        << (read.ExpectedKnown ? "" : " recv"));
    StepCell(svg, 405, 65, FormatInteger(sums[0]));
    StepCell(svg, 470, 205, "—");
    StepCell(svg, 675, 205, "—");
    StepCell(svg, 880, 110, FormatBytes(sums[2]));
    StepCell(svg, 990, viz.Config.Width > 990 ? viz.Config.Width - 990 : 0, details);
    svg << "</g></svg>" << Endl;
    return rowHeight;
}

ui32 PrintAggregatedColumnShardStats(TStringBuilder& svg, const TVisualizer& viz, ui32 top, const TStage* onlyStage) {
    constexpr ui32 rowHeight = INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    ui32 height = 0;
    for (const auto& plan : viz.Plans) {
        for (const auto& stage : plan->Stages) {
            if (stage.get() != onlyStage) {
                continue;
            }
            std::map<TString, TColumnShardAttempt> attempts;
            TColumnShardRead cohortContext;
            bool expectedContextSeen = false;
            ui64 unidentified = 0;
            for (const auto& scan : stage->ColumnShardScans.GetArray()) {
                if (scan.GetType() != NJson::JSON_MAP) {
                    continue;
                }
                if (ColumnShardFlag(scan, "ExpectedContextOnly")) {
                    TColumnShardAttempt header;
                    ReadColumnShardCohortHeader(header, scan);
                    cohortContext.AddCohort(header, {}, true);
                    expectedContextSeen = true;
                    continue;
                }
                TString identity = ColumnShardIdentity(scan, "StatsScanId");
                bool missingIdentity = !identity;
                if (missingIdentity) {
                    identity = TStringBuilder() << "unidentified-" << ++unidentified;
                }
                auto& attempt = attempts[identity];
                if (attempt.Last && ColumnShardFlag(*attempt.Last, "Finished") && !ColumnShardFlag(scan, "Finished")) {
                    ReadColumnShardCohortHeader(attempt, scan);
                    continue;
                }
                attempt.Last = &scan;
                attempt.MissingIdentity = attempt.MissingIdentity || missingIdentity;
                if (auto tablet = ColumnShardIdentity(scan, "TabletId")) {
                    attempt.Tablet = std::move(tablet);
                }
                if (auto reader = StepText(scan, "ReaderKind")) {
                    attempt.Reader = std::move(reader);
                }
                ReadColumnShardCohortHeader(attempt, scan);
                const auto* cache = scan.GetValueByPath("CacheStats");
                if (cache && cache->GetType() == NJson::JSON_MAP) {
                    attempt.Cache = cache;
                }
                const auto* steps = scan.GetValueByPath("Steps");
                if (steps && steps->GetType() == NJson::JSON_ARRAY) {
                    for (const auto& step : steps->GetArray()) {
                        if (step.GetType() != NJson::JSON_MAP) {
                            continue;
                        }
                        if (auto id = StepValue(step, "StepId")) {
                            attempt.Steps[*id] = &step;
                        } else {
                            attempt.UnidentifiedSteps.push_back(&step);
                            attempt.MissingIdentity = true;
                        }
                    }
                }
            }
            std::map<TString, TColumnShardRead> reads;
            for (const auto& [identity, attempt] : attempts) {
                TString family = attempt.Reader == "PLAIN" ? TString("PLAIN")
                    : attempt.Reader == "SIMPLE" || attempt.Reader == "TRIVIAL" ? TString("SIMPLE/TRIVIAL") : TString("UNKNOWN");
                auto& read = reads[family];
                read.Family = family;
                ++read.Attempts;
                TString tablet = attempt.Tablet ? attempt.Tablet : "unknown-" + identity;
                read.Tablets.insert(tablet);
                cohortContext.Tablets.insert(tablet);
                cohortContext.AddCohort(attempt, tablet);
                cohortContext.Partial = cohortContext.Partial || attempt.MissingIdentity || !attempt.Tablet ||
                    !ColumnShardFlag(*attempt.Last, "Finished") || ColumnShardFlag(*attempt.Last, "Truncated", true);
                if (const auto pending = StepValue(*attempt.Last, "PendingOperations"); pending && *pending) {
                    ++cohortContext.PendingAttempts;
                    cohortContext.MaxPendingOperations = std::max(cohortContext.MaxPendingOperations, *pending);
                    cohortContext.Partial = true;
                }
                read.Partial = read.Partial || family == "UNKNOWN" || attempt.MissingIdentity || !attempt.Tablet ||
                    !ColumnShardFlag(*attempt.Last, "Finished") || ColumnShardFlag(*attempt.Last, "Truncated", true);
                for (const auto& [_, step] : attempt.Steps) {
                    read.Add(*step, tablet);
                }
                for (const auto* step : attempt.UnidentifiedSteps) {
                    read.Add(*step, tablet);
                }
                static constexpr TStringBuf cacheFields[] = {"CompletedReadRanges", "CacheServedReadRanges", "ReturnedBytes", "CacheServedBytes"};
                for (unsigned i = 0; i < std::size(cacheFields); ++i) {
                    read.CacheFields[i][tablet].Add(attempt.Cache ? StepValue(*attempt.Cache, cacheFields[i]) : std::nullopt);
                }
                if (attempt.Cache && (StepValue(*attempt.Cache, "CompletedReadRanges").value_or(0) ||
                        StepValue(*attempt.Cache, "ReturnedBytes").value_or(0))) {
                    read.CacheTablets.insert(tablet);
                }
            }
            cohortContext.FinalizeCohorts();
            if (reads.empty() && expectedContextSeen) {
                reads["UNKNOWN"].Family = "UNKNOWN";
            }
            for (auto& [_, read] : reads) {
                read.ReadTablets = cohortContext.Tablets;
                read.ExpectedKnown = cohortContext.ExpectedKnown;
                read.ExpectedShards = cohortContext.ExpectedShards;
                read.KnownExpectedTablets = cohortContext.KnownExpectedTablets;
                read.Partial = read.Partial || cohortContext.Partial;
                read.PendingAttempts = cohortContext.PendingAttempts;
                read.MaxPendingOperations = cohortContext.MaxPendingOperations;
                TStringBuilder body;
                ui32 readHeight = 2 * rowHeight;
                for (TStringBuf block : {TStringBuf("Preparation"), TStringBuf("Execution"), TStringBuf("Output")}) {
                    readHeight += PrintColumnShardNode(body, viz, read, read.Blocks.at(TString(block)), readHeight - rowHeight, 0);
                }
                readHeight += PrintColumnShardCache(body, viz, read, readHeight - rowHeight);
                TStringBuilder title;
                title << "ColumnShard · Stage " << stage->PhysicalStageId << " · " << read.Family << " · " << read.Tablets.size()
                    << " family tablets · " << read.ReceivedShards() << " received read tablets · " << read.Attempts << " attempts";
                if (read.ExpectedKnown) {
                    title << " · " << read.ExpectedShards << " planned (known cohorts)";
                    if (read.ExpectedShards > read.ReceivedShards()) {
                        title << " · " << read.ExpectedShards - read.ReceivedShards() << " not observed / may not have started";
                    }
                } else {
                    title << " · expected unknown (known roster union " << read.KnownExpectedTablets.size() << ')';
                }
                if (read.Partial) {
                    title << " · PARTIAL population (received runtime statistics)";
                } else if (read.Attempts) {
                    title << " · received runtime reports finished";
                }
                if (!read.Attempts) {
                    title << " · reader/runtime not observed";
                }
                if (read.PendingAttempts) {
                    title << " · unfinalized operations: max " << read.MaxPendingOperations << " in " << read.PendingAttempts << " attempts";
                }
                if (read.Unclassified) {
                    title << " · unclassified operations";
                }
                svg << "<svg data-columnshard='read' data-cs-family='" << ColumnShardAttribute(read.Family)
                    << "' data-cs-attempts='" << read.Attempts
                    << "' data-cs-runtime-observed='" << (read.Attempts ? "true" : "false")
                    << "' data-cs-pending-attempts='" << read.PendingAttempts
                    << "' data-cs-max-pending-operations='" << read.MaxPendingOperations
                    << "' data-cs-received-shards='" << read.ReceivedShards()
                    << "' data-cs-participating-shards='" << read.Denominator()
                    << "' data-cs-expected-known='" << (read.ExpectedKnown ? "true" : "false")
                    << "' data-cs-denominator='" << (read.ExpectedKnown ? "expected" : "received")
                    << "' data-cs-known-expected-shards='" << read.KnownExpectedTablets.size()
                    << "' x='0' y='" << top + height << "' width='" << viz.Config.Width << "' height='" << readHeight << "'>"
                    << "<svg width='" << viz.Config.Width << "' height='" << rowHeight << "'><g>"
                    << "<title>Semantic display order, not a global barrier or timeline. Independent tablets and DAG branches may overlap. "
                    << "Active is accumulated owned elapsed work, not OS CPU. Min/mean/max compare active physical tablet totals.</title>"
                    << SvgRect(0, 0, viz.Config.Width, rowHeight, "columns")
                    << "<g class='plus button' data-cs-toggle='read'>" << SvgRect(0, 0, CONN_SIZE, CONN_SIZE, "transparent")
                    << "<use href='#icon_minus' class='icon_minus' transform='scale(0.014)' fill='" << viz.Config.Palette.ConnectionText << "'/>"
                    << "<use href='#icon_plus' class='icon_plus' transform='scale(0.014)' fill='" << viz.Config.Palette.ConnectionText << "'/></g>"
                    << SvgTextS(INTERNAL_WIDTH, (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2, title)
                    << "</g></svg><svg x='0' y='" << rowHeight << "' width='" << viz.Config.Width << "' height='" << readHeight - rowHeight << "'>"
                    << "<svg width='" << viz.Config.Width << "' height='" << rowHeight << "'>";
                StepCell(svg, INTERNAL_WIDTH, 315 - INTERNAL_WIDTH, "Metastep / kind");
                StepCell(svg, 315, 90, "Shards");
                StepCell(svg, 405, 65, "Calls");
                StepCell(svg, 470, 205, "Active sum / mean / max");
                StepCell(svg, 675, 205, "Wait sum / mean / max");
                StepCell(svg, 880, 110, "Read bytes");
                StepCell(svg, 990, viz.Config.Width > 990 ? viz.Config.Width - 990 : 0, "Details · own boundary");
                svg << "</svg>" << body << "</svg></svg>" << Endl;
                height += readHeight + INTERNAL_GAP_Y;
            }
        }
    }
    return height;
}

ui32 PrintColumnShardStats(TStringBuilder& svg, const TVisualizer& viz, ui32 top, const TStage* onlyStage) {
    if (HasColumnShardMetaStats(viz, onlyStage)) {
        return PrintAggregatedColumnShardStats(svg, viz, top, onlyStage);
    }
    constexpr ui32 rowHeight = INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    ui32 height = 0;
    for (const auto& plan : viz.Plans) {
        for (const auto& stage : plan->Stages) {
            if (stage.get() != onlyStage) {
                continue;
            }
            for (const auto& scan : stage->ColumnShardScans.GetArray()) {
                if (scan.GetType() != NJson::JSON_MAP) {
                    continue;
                }
                std::map<std::pair<TString, TString>, TStepGroup> groups;
                const auto* steps = scan.GetValueByPath("Steps");
                if (steps && steps->GetType() == NJson::JSON_ARRAY) {
                    for (const auto& step : steps->GetArray()) {
                        if (step.GetType() != NJson::JSON_MAP) {
                            continue;
                        }
                        auto& group = groups[{StepText(step, "StepKind"), StepText(step, "Operation")}];
                        group.Steps.push_back(&step);
                        group.Active.Add(step, "IntegralExecutionDuration");
                        group.Wait.Add(step, "IntegralWaitDuration");
                        group.Read.Add(step, "ReadBytes");
                    }
                }
                const auto* finished = scan.GetValueByPath("Finished");
                const auto* truncated = scan.GetValueByPath("Truncated");
                bool complete = finished && finished->GetType() == NJson::JSON_BOOLEAN && finished->GetBooleanSafe();
                bool partial = !complete || !truncated || truncated->GetType() != NJson::JSON_BOOLEAN || truncated->GetBooleanSafe();
                TStringBuilder title;
                title << "ColumnShard · Stage " << stage->PhysicalStageId << " · Tablet " << StepText(scan, "TabletId")
                    << " · Scan " << StepText(scan, "StatsScanId")
                    << (partial ? " · PARTIAL population" : " · complete");
                if (truncated && truncated->GetType() == NJson::JSON_BOOLEAN && truncated->GetBooleanSafe()) {
                    title << " (truncated)";
                }
                const auto* cache = scan.GetValueByPath("CacheStats");
                bool hasCache = cache && cache->GetType() == NJson::JSON_MAP;
                ui32 scanHeight = (2 + groups.size() + hasCache) * rowHeight;
                svg << "<svg data-columnshard='scan' x='0' y='" << top + height << "' width='" << viz.Config.Width
                    << "' height='" << scanHeight << "'>" << Endl
                    << "<svg x='0' y='0' width='" << viz.Config.Width << "' height='" << rowHeight << "'><g>"
                    << "<title>" << SvgEscape(title) << ". Active/wait are accumulated elapsed work, not CPU or a timeline. "
                    << "Inclusive parent and child times must not be added. Raw steps remain in query statistics.</title>"
                    << SvgRect(0, 0, viz.Config.Width, rowHeight, "columns")
                    << "<g class='plus button'>" << SvgRect(0, 0, CONN_SIZE, CONN_SIZE, "transparent")
                    << "<use href='#icon_minus' class='icon_minus' transform='scale(0.014)' fill='" << viz.Config.Palette.ConnectionText << "'/>"
                    << "<use href='#icon_plus' class='icon_plus' transform='scale(0.014)' fill='" << viz.Config.Palette.ConnectionText << "'/></g>"
                    << SvgTextS(INTERNAL_WIDTH, (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2, title)
                    << "</g></svg>" << Endl
                    << "<svg x='0' y='" << rowHeight << "' width='" << viz.Config.Width << "' height='" << scanHeight - rowHeight << "'>"
                    << SvgRect(0, 0, viz.Config.Width, "100%", "stage")
                    << "<svg width='" << viz.Config.Width << "' height='" << rowHeight << "'>";
                StepCell(svg, 0, 400, "Operation / kind");
                StepCell(svg, 400, 55, "Count");
                StepCell(svg, 455, 155, "Active sum / max");
                StepCell(svg, 610, 155, "Wait sum / max");
                StepCell(svg, 765, 100, "Read bytes");
                StepCell(svg, 865, viz.Config.Width - 865, "Details (same boundary)");
                svg << "</svg>" << Endl;
                ui32 y = rowHeight;
                for (const auto& [key, group] : groups) {
                    TStringBuilder tooltip;
                    tooltip << key.first << ": " << key.second << "\nStatsScanId " << StepText(scan, "StatsScanId")
                        << "\nCount " << group.Steps.size() << (partial ? " (partial population)" : "");
                    group.Active.Tooltip(tooltip, "Active elapsed", group.Steps.size(), FormatDurationUs);
                    group.Wait.Tooltip(tooltip, "Wait", group.Steps.size(), FormatDurationUs);
                    group.Read.Tooltip(tooltip, "Read bytes (this boundary only)", group.Steps.size(), FormatBytes);
                    auto details = StepDetails(group, key.first, tooltip);
                    std::map<TString, ui64> outcomes;
                    for (const auto* step : group.Steps) {
                        ++outcomes[StepText(*step, "Outcome")];
                    }
                    for (const auto& [outcome, count] : outcomes) {
                        tooltip << '\n' << (outcome ? outcome : TString("unknown outcome")) << ": " << count;
                    }
                    if (group.Active.MaxStep) {
                        tooltip << "\nMax active example: " << group.Active.MaxStep->GetStringRobust();
                    }
                    if (group.Wait.MaxStep) {
                        tooltip << "\nMax wait example: " << group.Wait.MaxStep->GetStringRobust();
                    }
                    if (!group.Active.MaxStep && !group.Wait.MaxStep) {
                        tooltip << "\nExample: " << group.Steps.front()->GetStringRobust();
                    }
                    svg << "<svg x='0' y='" << y << "' width='" << viz.Config.Width << "' height='" << rowHeight << "'>"
                        << "<g><title>" << SvgEscape(tooltip) << "</title>"
                        << "<rect x='455' y='0' width='155' height='" << rowHeight << "' fill='" << viz.Config.Palette.Cpu.Light << "' opacity='0.25'/>"
                        << "<rect x='610' y='0' width='155' height='" << rowHeight << "' fill='" << viz.Config.Palette.Input.Light << "' opacity='0.25'/>";
                    StepCell(svg, 0, 400, key.first + ": " + key.second);
                    StepCell(svg, 400, 55, ToString(group.Steps.size()));
                    StepCell(svg, 455, 155, group.Active.Summary());
                    StepCell(svg, 610, 155, group.Wait.Summary());
                    StepCell(svg, 765, 100, group.Read.Total(FormatBytes));
                    StepCell(svg, 865, viz.Config.Width - 865, details);
                    svg << "</g></svg>" << Endl;
                    y += rowHeight;
                }
                if (hasCache) {
                    auto ranges = StepValue(*cache, "CompletedReadRanges");
                    auto cachedRanges = StepValue(*cache, "CacheServedReadRanges");
                    auto bytes = StepValue(*cache, "ReturnedBytes");
                    auto cachedBytes = StepValue(*cache, "CacheServedBytes");
                    bool classified = ranges && cachedRanges && bytes && cachedBytes && *cachedRanges <= *ranges && *cachedBytes <= *bytes;
                    TStringBuilder text;
                    text << "Cache served: " << (cachedBytes ? FormatBytes(*cachedBytes) : TString("—")) << " / "
                        << (bytes ? FormatBytes(*bytes) : TString("—")) << "; ranges "
                        << (cachedRanges ? ToString(*cachedRanges) : TString("—")) << " / " << (ranges ? ToString(*ranges) : TString("—"));
                    if (!partial && classified && *bytes && *ranges) {
                        text << Sprintf(" (bytes %.1f%%, ranges %.1f%%)", 100.0 * *cachedBytes / *bytes, 100.0 * *cachedRanges / *ranges);
                    } else {
                        text << " · ratios unavailable (partial or unclassified population)";
                    }
                    svg << "<svg x='0' y='" << y << "' width='" << viz.Config.Width << "' height='" << rowHeight << "'><g>"
                        << "<title>Cache response origin; shared reads are not cache hits. Cache-served bytes do not measure saved physical traffic.</title>";
                    StepCell(svg, 0, viz.Config.Width, text);
                    svg << "</g></svg>" << Endl;
                }
                svg << "</svg></svg>" << Endl;
                height += scanHeight + GAP_Y;
            }
        }
    }
    return height;
}

} // namespace

TString TVisualizer::PrintSvg() {
    // Reached only by mixing LoadPlansSafe with the throwing printer; report the
    // load failure rather than rendering the half-loaded plans behind it.
    if (LoadError) {
        ythrow yexception() << LoadError;
    }

    TStringBuilder background;
    TStringBuilder svg;

    // The plans start below the column header strip, which is drawn once for all
    // of them at the top of the document.
    ui32 offsetY = COLUMN_HEADER_HEIGHT;
    ui32 timelineDelta = (UpdateTime > MaxTime) ? std::min<ui32>(Config.TimelineWidth * (UpdateTime - MaxTime) / UpdateTime, Config.TimelineWidth / 10) : 0;

    ui64 maxSec = MaxTime / 1000;
    ui64 deltaSec = 0;

            if (maxSec <=  10) deltaSec = 1;
    else if (maxSec <=  20) deltaSec = 2;
    else if (maxSec <=  30) deltaSec = 3;
    else if (maxSec <=  40) deltaSec = 4;
    else if (maxSec <=  50) deltaSec = 5;
    else if (maxSec <=  60) deltaSec = 6;
    else if (maxSec <= 100) deltaSec = 10;
    else if (maxSec <= 150) deltaSec = 15;
    else if (maxSec <= 200) deltaSec = 20;
    else if (maxSec <= 300) deltaSec = 30;
    else if (maxSec <= 600) deltaSec = 60;
    else if (maxSec <= 1200) deltaSec = 120;
    else if (maxSec <= 1800) deltaSec = 180;
    else if (maxSec <= 3600) deltaSec = 360;
    else {
        ui64 stepSec = maxSec / 10;
        deltaSec = stepSec - (stepSec % 60);
    }

    auto x = Config.TimelineLeft + INTERNAL_GAP_X;
    auto w = Config.TimelineWidth - timelineDelta - INTERNAL_GAP_X * 2;

    for (auto& plan : Plans) {
        for (const auto& stage : plan->Stages) {
            stage->ColumnShardSvg.clear();
            stage->ColumnShardHeight = PrintColumnShardStats(stage->ColumnShardSvg, *this, 0, stage.get());
        }
        plan->PrepareSvg(MaxTime, timelineDelta, offsetY);
    }

    for (auto& plan : Plans) {
        plan->PrintSvg(background, MaxTime, timelineDelta);
    }

    svg << "<svg width='" << Config.Width << "' height='" << offsetY << "' xmlns='http://www.w3.org/2000/svg'>" << Endl;
    svg << "<clipPath id='clipTextPath'><rect x='" << Config.HeaderLeft
        << "' y='0' width='" << Config.HeaderWidth << "' height='" << offsetY << "'/>"
        << "</clipPath>" << Endl;
    svg << Endl << NResource::Find(TStringBuf("plan2svg/icons.svg"));
    svg << "<style type='text/css'>" << Endl
        << "  rect.stage { stroke-width:0; fill:" << Config.Palette.StageMain << "; }" << Endl
        << "  rect.clone { stroke-width:0; fill:" << Config.Palette.StageClone << "; }" << Endl
        << "  rect.blocks { stroke-width:0; fill:" << Config.Palette.BlockMedium << "; }" << Endl
        << "  rect.hot { stroke-width:0; fill:" << Config.Palette.StageTextHighlight << "; opacity:0.3; }" << Endl
        << "  .texts { text-anchor:start; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.StageText << "; }" << Endl
        << "  .textm { text-anchor:middle; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.StageText << "; }" << Endl
        << "  .texte { text-anchor:end; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.StageText << "; }" << Endl
        << "  .textc { text-anchor:end; dominant-baseline:middle; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.StageText << "; }" << Endl
        << "  circle.stage { stroke:" << Config.Palette.StageMain << "; stroke-width:1; fill:" << Config.Palette.StageClone << "; }" << Endl
        << "  line.opdiv { stroke-width:1; stroke:" << Config.Palette.StageGrid << "; stroke-dasharray:1,2; }" << Endl
        << "  #columnHeaders text { font-weight:bold; }" << Endl
        << "  rect.columns { stroke-width:0; fill:" << Config.Palette.ColumnHeader << "; }" << Endl
        << "  line.columns { stroke-width:1; stroke:" << Config.Palette.StageGrid << "; }" << Endl
        << "  text.clipped { clip-path:url(#clipTextPath); }" << Endl
        << "  polygon.conn { stroke-width:0; fill:" << Config.Palette.ConnectionFill << "; }" << Endl
        << "  path.conn { stroke-width:1; stroke:" << Config.Palette.ConnectionLine << "; fill:" << Config.Palette.ConnectionFill << "; }" << Endl
        << "  rect.conn { stroke-width:1; stroke:" << Config.Palette.ConnectionLine << "; fill:none; }" << Endl
        << "  path.conn.blocks { stroke-width:1; stroke:" << Config.Palette.ConnectionLine << "; fill:" << Config.Palette.BlockMedium << "; }" << Endl
        << "  text.conn { text-anchor:middle; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.ConnectionText << "; }" << Endl
        << "  rect.background { stroke-width:0; fill:#33FFFF; opacity:0; }" << Endl
        << "  rect.transparent { stroke-width:0; fill:#33FFFF; opacity:0; }" << Endl
        << "  g.selected circle.stage { fill:#33FFFF; }" << Endl
        << "  g.selected polygon.conn { fill:#33FFFF; }" << Endl
        << "  g.selected path.conn { stroke:#33FFFF; fill:#33FFFF; }" << Endl
        << "  g.selected rect.stage { fill:#33FFFF; }" << Endl
        << "  g.selected rect.clone { fill:#33FFFF; }" << Endl
        << "  g.selected rect.background { opacity:1; }" << Endl
        << "  g.selected path.blocks { stroke:#33FFFF; }" << Endl
        << "  g.selected circle.stage.selected { fill:#33FFFF; }" << Endl
        << "  polygon.conn.selected { fill:#33FFFF; }" << Endl
        << "  path.conn.selected { stroke:#33FFFF; fill:#33FFFF; }" << Endl
        << "  rect.stage.selected { fill:#33FFFF; }" << Endl
        << "  rect.clone.selected { fill:#33FFFF; }" << Endl
        << "  rect.background.selected { opacity:1; }" << Endl
        << "  path.blocks.selected { stroke:#33FFFF; }" << Endl
        << "  svg:not(.folded) > g > g.button .icon_plus { opacity:0; }" << Endl
        << "  svg.folded > g > g.button .icon_minus { opacity:0; }" << Endl
        << "</style>" << Endl;
    svg << Endl << NResource::Find(TStringBuf("plan2svg/plan2svg.js"));
    if (timelineDelta) {
        ui32 summary3 = (Config.SummaryWidth - INTERNAL_GAP_X * 2) / 3;
        svg
        << "<g><title>" << "Last Update: " << FormatTimeMs(UpdateTime) << "</title>" << Endl
        << "  <rect x='" << Config.TimelineLeft + Config.TimelineWidth - summary3 << "' y='" << COLUMN_HEADER_HEIGHT + GAP_Y
        << "' width='" << summary3 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.StageTextHighlight << "'/>" << Endl
        << "  <text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextInverted << "' x='" << Config.TimelineLeft + Config.TimelineWidth - 2
        << "' y='" << COLUMN_HEADER_HEIGHT + GAP_Y + INTERNAL_TEXT_HEIGHT << "'>" << FormatTimeMs(UpdateTime) << "</text>" << Endl
        << "</g>" << Endl;
    }

    svg << TString(background) << Endl;

    for (ui64 t = 0; t <= maxSec; t += deltaSec) {
        ui64 x1 = t * w * 1000 / MaxTime;
        svg
            << "<line x1='" << x + x1 << "' y1='0' x2='" << x + x1 << "' y2='" << "100%" // offsetY
            << "' stroke-width='1' stroke='" << Config.Palette.StageGrid << "' stroke-dasharray='1,2'/>" << Endl;
    }

    if (timelineDelta) {
        auto opacity = MaxTime ? std::min(0.5, static_cast<double>(UpdateTime - MaxTime) / (2 * MaxTime)) : 0.5;
        svg
        << "<rect x='" << Config.TimelineLeft + Config.TimelineWidth - timelineDelta << "' y='" << 0
            << "' width='" << timelineDelta << "' height='" << offsetY
        << "' stroke-width='0' opacity='" << opacity << "' fill='" << Config.Palette.StageTextHighlight << "'/>" << Endl;
    }

    PrintColumnHeaders(svg, maxSec, deltaSec, x, w);

    svg << "</svg>" << Endl;

    return svg;
}

} // namespace NPlan2Svg
