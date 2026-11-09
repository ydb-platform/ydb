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
            << SvgText(Config.HeaderLeft + s->IndentX + INTERNAL_GAP_X + INTERNAL_WIDTH * 2 + 2
                + (index == 0 && s->ColumnShardHeight ? INTERNAL_WIDTH : 0), yt + INTERNAL_TEXT_HEIGHT, "texts clipped", op.Name + ": " + op.Info);
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
            << s->PhysicalStageId << "' transform='translate(" << Config.HeaderLeft + s->IndentX + INTERNAL_GAP_X + INTERNAL_WIDTH * 2
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

    builder << "<svg class='slimable' data-stage='inner " << stage->PhysicalStageId << "' data-height='" << stage->Height << "' width='" << Config.Width << "' height='" << stage->Height << "' x='0' y='" << GAP_Y << '\''
        << (stage->ColumnShardHeight ? " style='overflow:hidden'" : "") << '>' << Endl;
    builder << stage->Svg;
    if (stage->ColumnShardHeight) {
        builder << "<svg data-cs-panel='" << stage->PhysicalStageId << "' data-cs-plan-index='" << stage->ColumnShardPlanIndex.value()
            << "' data-height='" << stage->ColumnShardHeight
            << "' x='0' y='" << stage->Height << "' width='" << Config.Width << "' height='0' style='display:none'>"
            << "<rect class='cs-panel-surface' width='100%' height='100%' fill='#ffffff'/>"
            << "<style>g.selected [data-cs-panel] rect.stage{fill:var(--stage-main,#F2F2F2)}"
            << "g.selected [data-cs-panel] rect.clone{fill:var(--stage-clone,#D9D9D9)}"
            << "g.selected [data-cs-panel] rect.background{opacity:0}</style>"
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
            << ", max " << format(Max) << ", avg " << (Overflow ? TString("overflow") : format(Sum / Count))
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
    if (kind == "MemoryReserve" || kind == "ReserveMemory") {
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
    const ui32 available = width > INTERNAL_GAP_X * 2 ? width - INTERNAL_GAP_X * 2 : 0;
    const size_t limit = available * 10 / (INTERNAL_TEXT_HEIGHT * 7);
    TString display(text);
    if (text.size() > limit) {
        size_t cut = limit > 3 ? limit - 3 : 0;
        while (cut && (static_cast<unsigned char>(text[cut]) & 0xc0) == 0x80) {
            --cut;
        }
        display = TString(text.substr(0, cut)) + "...";
    }
    svg << "<svg x='" << x << "' y='0' width='" << width << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y
        << "' style='overflow:hidden'><title>" << SvgEscape(text) << "</title>"
        << SvgTextS(INTERNAL_GAP_X, (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2, display) << "</svg>";
}

TString ColumnShardKind(TString kind) {
    if (kind == "FetchColumns") {
        return "Fetching";
    }
    if (kind == "MemoryReserve" || kind == "Reserve") {
        return "ReserveMemory";
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
                out << "; min/avg/max=" << s.Min << '/'
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

struct TColumnShardSourceTotals {
    ui64 Bytes = 0;
    ui64 Reads = 0;
    ui64 Unknown = 0;
    bool Overflow = false;
    bool Shared = false;
    std::set<TString> DataKinds;

    void AddValue(ui64& target, ui64 value) {
        if (value > std::numeric_limits<ui64>::max() - target) {
            target = std::numeric_limits<ui64>::max();
            Overflow = true;
        } else {
            target += value;
        }
    }

    void Add(const TColumnShardSourceTotals& value) {
        AddValue(Bytes, value.Bytes);
        AddValue(Reads, value.Reads);
        AddValue(Unknown, value.Unknown);
        Overflow = Overflow || value.Overflow;
        Shared = Shared || value.Shared;
        DataKinds.insert(value.DataKinds.begin(), value.DataKinds.end());
    }
};

using TColumnShardSourceKey = std::pair<TString, TString>;
using TColumnShardSources = std::map<TColumnShardSourceKey, TColumnShardSourceTotals>;

TColumnShardSourceKey ColumnShardSourceKey(const NJson::TJsonValue& value) {
    TString kind = StepText(value, "SourceKind");
    return {kind ? kind : TString("Unknown"), StepText(value, "SourceName")};
}

TColumnShardSourceTotals ColumnShardSourceTotals(const NJson::TJsonValue& value) {
    TColumnShardSourceTotals result;
    result.Bytes = StepValue(value, "ReadBytes").value_or(0);
    result.Reads = StepValue(value, "ReadCount").value_or(0);
    result.Unknown = StepValue(value, "UnknownByteReads").value_or(0);
    result.Shared = ColumnShardFlag(value, "Shared");
    if (auto kind = StepText(value, "DataKind")) {
        result.DataKinds.insert(std::move(kind));
    }
    return result;
}

TString ColumnShardKeyPart(TStringBuf value) {
    return TStringBuilder() << value.size() << ':' << value;
}

struct TColumnShardIndexContext {
    TString Key;
    TString Description;
    std::set<TString> Members;
    bool Shared = false;
};

TColumnShardIndexContext ColumnShardIndexContext(const NJson::TJsonValue& step, TStringBuf table) {
    TColumnShardIndexContext result;
    const auto* descriptors = step.GetValueByPath("IndexDescriptors");
    if (!descriptors || descriptors->GetType() != NJson::JSON_ARRAY) {
        return result;
    }
    std::map<TString, TString> members;
    for (const auto& descriptor : descriptors->GetArray()) {
        if (descriptor.GetType() != NJson::JSON_MAP) {
            continue;
        }
        const TString name = StepText(descriptor, "Name");
        const TString type = StepText(descriptor, "Type");
        const TString columns = ColumnShardSemanticNames(descriptor, "Columns");
        const TString id = ColumnShardIdentity(descriptor, "IndexId");
        const TString identity = ColumnShardKeyPart(table) + (id ? "id:" + id
            : "description:" + ColumnShardKeyPart(name) + ColumnShardKeyPart(type) + ColumnShardKeyPart(columns));
        TString description = name ? name : id ? "index #" + id : TString("unnamed index");
        if (type) {
            description += " [" + type + "]";
        }
        if (columns) {
            description += " (" + columns + ")";
        }
        members.emplace(identity, description);
    }
    result.Shared = members.size() > 1 || ColumnShardFlag(step, "SharedIndexInputs");
    if (!members.empty()) { result.Key = result.Shared ? "shared:" : "index:"; }
    for (const auto& [identity, description] : members) {
        result.Key += ColumnShardKeyPart(identity);
        result.Members.insert(identity);
        if (result.Description) {
            result.Description += "; ";
        }
        result.Description += description;
    }
    return result;
}

struct TColumnShardNode {
    TString Name;
    TStepGroup PresentationSteps;
    std::map<TString, NJson::TJsonValue> IndexDescriptors;
    TString Meta;
    TString Path;
    TString Kind;
    TString Row;
    TColumnShardSources Sources;
    std::set<TString> SourceEvents;
    TString IndexKey;
    std::set<TString> IndexMembers;
    std::set<TString> IndexDescriptions;
    std::set<ui64> IndexOrders;
    ui64 IndexPosition = std::numeric_limits<ui64>::max();
    std::set<TString> SharedInputReferences;
    bool SharedIndexInputs = false;
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

    TColumnShardNode& Child(TString name, TString meta, TString path, TString kind, TString row, TString key = {}) {
        auto [childIt, inserted] = Children.try_emplace(key ? key : name);
        auto& child = childIt->second;
        child.Name = std::move(name);
        child.Meta = std::move(meta);
        child.Path = std::move(path);
        child.Kind = std::move(kind);
        child.Row = std::move(row);
        if (inserted && Meta == "IndexFilter") {
            child.IndexKey = IndexKey;
            child.IndexMembers = IndexMembers;
            child.IndexDescriptions = IndexDescriptions;
            child.IndexDescriptors = IndexDescriptors;
            child.SharedIndexInputs = SharedIndexInputs;
        }
        return child;
    }

    void DescribeIndex(const TColumnShardIndexContext& index, std::optional<ui64> order, bool owner = false) {
        if (!index.Key) {
            return;
        }
        if (owner) {
            IndexKey = index.Key;
        }
        IndexMembers.insert(index.Members.begin(), index.Members.end());
        IndexDescriptions.insert(index.Description);
        SharedIndexInputs = SharedIndexInputs || index.Shared;
        if (order) {
            IndexOrders.insert(*order);
            IndexPosition = std::min(IndexPosition, *order);
        }
    }

    void AddSources(const NJson::TJsonValue& step, const TString& event) {
        const auto* sources = step.GetValueByPath("DataSourceReads");
        if (!sources || sources->GetType() != NJson::JSON_ARRAY) {
            return;
        }
        for (const auto& source : sources->GetArray()) {
            if (source.GetType() != NJson::JSON_MAP) {
                continue;
            }
            const auto key = ColumnShardSourceKey(source);
            const TString identity = event + ColumnShardKeyPart(key.first) + ColumnShardKeyPart(key.second);
            if (SourceEvents.insert(identity).second) {
                Sources[key].Add(ColumnShardSourceTotals(source));
            }
        }
    }

    void Add(const NJson::TJsonValue& step, const TString& tablet, bool summary, bool directWait) {
        const auto* descriptors = step.GetValueByPath("IndexDescriptors");
        if (descriptors && descriptors->GetType() == NJson::JSON_ARRAY) {
            for (const auto& descriptor : descriptors->GetArray()) {
                if (descriptor.GetType() == NJson::JSON_MAP) {
                    IndexDescriptors.emplace(descriptor.GetStringRobust(), descriptor);
                }
            }
        }
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
        PresentationSteps.Steps.push_back(&step);
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
    std::map<const NJson::TJsonValue*, std::vector<TColumnShardNode*>> StepNodes;
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

    void Add(const NJson::TJsonValue& step, const TString& tablet, const TString& table = {}) {
        if (!ColumnShardSkipped(step) && !StepValue(step, "OwnedExecutionDuration")) {
            Partial = true;
        }
        TString meta = StepText(step, "MetaStep");
        const TString kind = ColumnShardKind(StepText(step, "StepKind"));
        const TString path = StepText(step, "CanonicalPath");
        if (!meta && kind == "Reader" && !path) {
            auto& execution = Blocks["Execution"];
            StepNodes[&step].push_back(&execution);
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
            const auto index = meta == "IndexFilter" ? ColumnShardIndexContext(step, table) : TColumnShardIndexContext();
            parent = &execution.Child(meta, meta, {}, {}, "meta", index.Key ? meta + ':' + index.Key : meta);
            parent->DescribeIndex(index, StepValue(step, "ApplicationOrder"), true);
            StepNodes[&step].push_back(&execution);
        }
        parent->Add(step, tablet, true, directWait);
        StepNodes[&step].push_back(parent);
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
            StepNodes[&step].push_back(parent);
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
        leaf.DescribeIndex(ColumnShardIndexContext(step, table), StepValue(step, "ApplicationOrder"));
        StepNodes[&step].push_back(&leaf);
        leaf.Add(step, tablet, false, true);
    }
};

void AddColumnShardSourceDetails(TColumnShardRead& read, const TColumnShardAttempt& attempt, const TString& identity) {
    ui64 unidentified = 0;
    auto add = [&](const NJson::TJsonValue& step) {
        const TString event = ColumnShardKeyPart(identity) + ColumnShardKeyPart(
            ColumnShardIdentity(step, "StepId") ? ColumnShardIdentity(step, "StepId") : "unidentified-" + ToString(++unidentified));
        const NJson::TJsonValue* ancestor = &step;
        std::set<ui64> visited;
        bool owner = true;
        while (ancestor) {
            if (const auto nodes = read.StepNodes.find(ancestor); nodes != read.StepNodes.end()) {
                if (owner) {
                    for (auto* node : nodes->second) {
                        node->AddSources(step, event);
                    }
                } else {
                    const TString kind = ColumnShardKind(StepText(*ancestor, "StepKind"));
                    if (!nodes->second.empty() && (kind == "Reader" || kind == "Fetching" ||
                        kind == "FetchAccessors" || kind == "StorageRead")) {
                        auto* node = nodes->second.back();
                        if (node->Row == "step") {
                            node->AddSources(step, event);
                        }
                    }
                }
            }
            owner = false;
            const auto parent = StepValue(*ancestor, "ParentStepId");
            if (!parent || !visited.insert(*parent).second) {
                break;
            }
            const auto found = attempt.Steps.find(*parent);
            ancestor = found == attempt.Steps.end() ? nullptr : found->second;
        }
    };
    for (const auto& [_, step] : attempt.Steps) {
        add(*step);
    }
    for (const auto* step : attempt.UnidentifiedSteps) {
        add(*step);
    }
}

void LinkColumnShardSharedIndexInputs(TColumnShardRead& read) {
    auto& metas = read.Blocks["Execution"].Children;
    std::vector<TColumnShardNode*> owners;
    auto collect = [&](auto&& self, TColumnShardNode& node) -> void {
        if ((node.Row == "meta" && node.Meta == "IndexFilter" && node.SharedIndexInputs) ||
            (node.Row == "step" && node.Meta != "IndexFilter" && !node.IndexMembers.empty())) {
            owners.push_back(&node);
        }
        for (auto& [_, child] : node.Children) { self(self, child); }
    };
    collect(collect, read.Blocks["Execution"]);
    for (auto* owner : owners) {
        for (auto& [indexKey, index] : metas) {
            if (index.Meta != "IndexFilter" || index.SharedIndexInputs || index.IndexMembers.size() != 1 ||
                !owner->IndexMembers.contains(*index.IndexMembers.begin())) {
                continue;
            }
            if (owner->Row == "meta") {
                owner->IndexPosition = std::min(owner->IndexPosition, index.IndexPosition);
            }
            TString reference = owner->Meta;
            if (owner->Path) { reference += '/' + owner->Path; }
            if (owner->Kind) { reference += '/' + owner->Kind; }
            for (const auto& description : owner->IndexDescriptions) { reference += " [" + description + ']'; }
            index.SharedInputReferences.insert(std::move(reference));
        }
    }
}

unsigned ColumnShardOrder(TStringBuf name, const TColumnShardNode& parent) {
    static constexpr TStringBuf metas[] = {"PKFilter", "IndexFilter", "Filter", "FetchOtherColumns", "Aggregation", "Projection", "BuildResult"};
    static constexpr TStringBuf steps[] = {"SourcePreparation", "Reader", "FetchAccessors", "ReserveMemory", "Fetching", "Assemble", "Decode",
        "Calculation", "IndexCheck", "Filter", "DuplicateFilter", "Merge", "Aggregation", "Projection", "PrepareResult", "BuildResult",
        "Ready", "Limit", "Cursor", "Send", "Ack", "Finish", "Cleanup"};
    static constexpr TStringBuf preparation[] = {"ParseProgram", "SelectPortions", "ReaderInit", "Reader", "FetchAccessors", "ReserveMemory",
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

TString ColumnShardSourcesAttribute(const TColumnShardNode& node) {
    NJson::TJsonValue sources(NJson::JSON_ARRAY);
    for (const auto& [source, value] : node.Sources) {
        if (!value.Bytes && !value.Unknown) { continue; }
        NJson::TJsonValue item(NJson::JSON_MAP);
        item["SourceKind"] = source.first;
        item["SourceName"] = source.second;
        item["ReadBytes"] = value.Bytes;
        item["ReadCount"] = value.Reads;
        item["UnknownByteReads"] = value.Unknown;
        item["Shared"] = value.Shared;
        item["Overflow"] = value.Overflow;
        sources.AppendValue(std::move(item));
    }
    return sources.GetStringRobust();
}

TString ColumnShardWidgetStyles() {
    return TStringBuilder() << "<style>.cs-ui,.cs-names,.cs-heading{font-family:Verdana;font-size:"
        << INTERNAL_TEXT_HEIGHT << "px}"
        << R"CS(
      .cs-ui{color:var(--stage-text,#262626);line-height:1.375;box-sizing:border-box}
      .cs-row-background{fill:var(--stage-main,#f2f2f2);stroke:#ffffff;stroke-width:2px}
      .cs-column-separator{fill:#ffffff;pointer-events:none}
      .cs-ui *{box-sizing:border-box;min-width:0}.cs-ui .cs-label{color:var(--stage-text,#262626);font-weight:normal}
      .cs-ui .cs-value,.cs-ui b,.cs-ui strong{font-weight:normal}.cs-ui .cs-unit{margin-left:2px;font-weight:normal}
      .cs-number,.cs-stat>.cs-value{background:var(--stage-clone,#d9d9d9);color:var(--stage-text,#262626);padding:0 2px}
      .cs-number.cs-bytes,.cs-stat-read>.cs-number{background:var(--input-medium,#5a8183);color:var(--text-light,#ffffff)}
      .cs-stat-read>.cs-number{grid-column:2/4;justify-self:start}
      .cs-stat>.cs-value{background:var(--stage-grid,#b2b2b2)}
      .cs-flow .cs-field:first-child .cs-number{background:var(--input-medium,#5a8183);color:var(--text-light,#ffffff)}
      .cs-flow .cs-field:last-child .cs-number{background:var(--output-medium,#5781b9);color:var(--text-light,#ffffff)}
      .cs-field.cs-memory .cs-number{background:var(--mem-medium,#7e4e5b);color:var(--text-light,#ffffff)}
      .cs-field.cs-input .cs-number{background:var(--input-medium,#5a8183);color:var(--text-light,#ffffff)}
      .cs-field.cs-output .cs-number{background:var(--output-medium,#5781b9);color:var(--text-light,#ffffff)}
      .cs-ui .cs-observed{color:#87620a}.cs-ui .cs-unknown{color:#7f7f7f;font-weight:normal}
      .cs-stats{padding:2px;display:grid;align-content:start;gap:0}
      .cs-reader-header{padding:2px;display:flex;flex-wrap:wrap;align-items:baseline;gap:4px}
      .cs-stat{display:grid;grid-template-columns:29px repeat(4,minmax(60px,1fr));align-items:baseline;column-gap:3px;min-height:11px}
      .cs-stat-head{color:#7f7f7f;text-align:right}.cs-stat-head>span:first-child{text-align:left}
      .cs-stat>.cs-number,.cs-stat>.cs-value{text-align:right}.cs-stat>.cs-unknown{text-align:right;overflow-wrap:anywhere}
      .cs-tablets,.cs-calls{padding:2px;display:flex;flex-direction:column;align-items:flex-start;gap:1px}
      .cs-tablets .cs-count{flex-wrap:wrap;gap:1px 3px}
      .cs-chevron{transform:rotate(90deg);transform-origin:3px 4px}
      svg.folded>g>.cs-row-toggle .cs-chevron{transform:none}
      .cs-coverage{height:4px;width:24px;background:var(--stage-clone,#d9d9d9);display:inline-block;align-self:center}
      .cs-coverage>i{display:block;height:100%;background:var(--stage-grid,#b2b2b2)}
      .cs-shards{display:flex;align-items:baseline;gap:4px;flex-wrap:wrap}
      .cs-count{display:inline-flex;gap:3px;align-items:baseline}.cs-count b,.cs-count small{font-size:inherit;font-weight:normal}
      .cs-count b{background:var(--stage-clone,#d9d9d9);color:var(--stage-text,#262626);padding:0 2px}
      .cs-count small{color:#7f7f7f}.cs-number{display:inline-block;white-space:nowrap}
      .cs-details{padding:2px;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));align-content:flex-start;align-items:stretch;gap:2px;background:#ffffff}
      .cs-field{display:grid;grid-template-columns:auto minmax(0,1fr);align-items:baseline;column-gap:5px;padding:3px;order:3;background:var(--stage-main,#f2f2f2)}
      .cs-field:is([data-cs-field='Columns'],[data-cs-field='Column'],[data-cs-field='Input columns'],[data-cs-field='Output columns'],[data-cs-field='Predicate'],[data-cs-field='Expression'],[data-cs-field='Grouping keys'],[data-cs-field='Aggregate functions'],[data-cs-field='Index']){background:#eee6f7}
      .cs-field>.cs-label{display:block}.cs-field-value{overflow-wrap:anywhere}
      .cs-field.cs-wide{grid-column:1/-1;display:block;order:1}.cs-field.cs-wide>.cs-label{margin-bottom:1px}
      .cs-field[data-cs-field='Predicate'],.cs-field[data-cs-field='Expression']{order:0}
      .cs-expression{font:inherit;white-space:pre-wrap;overflow-wrap:anywhere}
      .cs-expression+.cs-expression{margin-top:3px}
      .cs-chips{display:flex;flex-wrap:wrap;gap:1px 8px}.cs-chip{overflow-wrap:anywhere}
      .cs-flow{display:grid;grid-template-columns:minmax(0,1fr) 10px minmax(0,1fr);align-items:stretch;gap:2px;grid-column:1/-1;order:2}
      .cs-flow .cs-field{grid-template-columns:minmax(0,1fr) auto;align-items:baseline;order:0}
      .cs-flow svg{width:10px;height:10px;margin-top:1px}.cs-flow path{fill:none;stroke:var(--stage-grid,#b2b2b2);stroke-width:1.5}
      .cs-meter{height:2px;background:var(--stage-main,#f2f2f2);margin-top:1px}
      .cs-meter i{display:block;height:100%;background:var(--stage-grid,#b2b2b2)}
      .cs-index,.cs-provider{grid-column:1/-1;order:0;grid-template-columns:46px minmax(0,1fr)}
      .cs-index>.cs-field-value{display:flex;flex-wrap:wrap;align-items:baseline;gap:2px 10px}
      .cs-index .cs-chips{flex:0 1 auto}.cs-provider{order:4}.cs-provider>.cs-field-value{display:flex;flex-wrap:wrap;align-items:baseline;gap:2px 8px}
      .cs-state{display:inline-flex;align-items:baseline;gap:3px;color:#7f7f7f;order:0;flex-wrap:wrap}
      .cs-state i{width:4px;height:4px;background:var(--stage-grid,#b2b2b2);display:inline-block;align-self:center}
      .cs-state.cs-warning i{background:var(--block-medium,#d9ae61)}
      .cs-availability{display:flex;flex-wrap:wrap;align-items:baseline;gap:2px 8px}
      .cs-names{fill:var(--stage-text,#262626)}.cs-metastep{font-weight:bold}.cs-heading{fill:#7f7f7f}
      .cs-details>.cs-state,.cs-details>.cs-unknown{grid-column:1/-1;padding:3px;background:var(--stage-main,#f2f2f2)}
      .cs-storage-values{display:flex;flex-wrap:wrap;padding:2px;align-items:baseline;gap:3px 6px;background:transparent}.cs-storage-values .cs-shards{width:100%}
    </style>)CS";
}

TString ColumnShardWidgetNumber(ui64 value, bool bytes) {
    const TString text = bytes ? FormatBytes(value) : FormatInteger(value);
    const size_t split = text.find_first_not_of("0123456789.,");
    const TString amount = text.substr(0, split);
    const TString unit = split == TString::npos ? TString() : text.substr(split);
    return TStringBuilder() << "<span class='cs-number" << (bytes ? " cs-bytes" : "") << "'><span class='cs-value'>" << SvgEscape(amount) << "</span>"
        << "<span class='cs-unit'>" << SvgEscape(unit) << "</span></span>";
}

TString ColumnShardWidgetField(TStringBuf name, const TString& value, TStringBuf extra = {}) {
    return TStringBuilder() << "<div class='cs-field " << extra << "' data-cs-field='" << ColumnShardAttribute(name)
        << "'><span class='cs-label'>" << SvgEscape(name) << "</span><div class='cs-field-value'>" << value << "</div></div>";
}

TStepGroup ColumnShardWidgetGroup(const TColumnShardNode& node, TStringBuf kind) {
    TStepGroup result;
    for (const auto* step : node.PresentationSteps.Steps) {
        if (!kind || ColumnShardKind(StepText(*step, "StepKind")) == kind) { result.Steps.push_back(step); }
    }
    return result;
}

std::set<TString> ColumnShardWidgetValues(const TStepGroup& group, TStringBuf path) {
    std::set<TString> result;
    for (const auto* step : group.Steps) {
        const auto* value = step->GetValueByPath(path);
        if (!value) { continue; }
        if (value->GetType() == NJson::JSON_STRING && value->GetStringSafe()) { result.insert(value->GetStringSafe()); }
        if (value->GetType() == NJson::JSON_ARRAY) {
            for (const auto& item : value->GetArray()) {
                if (item.GetType() == NJson::JSON_STRING && item.GetStringSafe()) { result.insert(item.GetStringSafe()); }
            }
        }
    }
    return result;
}

TString ColumnShardWidgetChips(const std::set<TString>& values) {
    TStringBuilder result;
    result << "<div class='cs-chips'>";
    for (const auto& value : values) { result << "<span class='cs-chip'>" << SvgEscape(value) << "</span>"; }
    result << "</div>";
    return result;
}

TString ColumnShardWidgetMetric(const TStepGroup& group, TStringBuf path, TStringBuf name, bool bytes = false) {
    const auto metric = group.Metric(path);
    if (!metric.Count) { return {}; }
    TString css = metric.Overflow || metric.Count < group.Steps.size() ? "cs-observed" : "";
    if (path == "Details.ReservationBytes") { css += " cs-memory"; }
    if (path == "Details.PageRows" || path == "Details.PageInputBytes") { css += " cs-input"; }
    if (path == "Details.ResultRows" || path == "Details.ResultBytes" || path == "Details.DecodedRows") { css += " cs-output"; }
    return ColumnShardWidgetField(name, metric.Overflow
        ? TString("<span class='cs-unknown'>Overflow</span>") : ColumnShardWidgetNumber(metric.Sum, bytes),
        css);
}

TString ColumnShardWidgetFlow(const TStepGroup& group, TStringBuf input, TStringBuf output, TStringBuf title, bool bytes = false) {
    const auto left = group.Metric(input), right = group.Metric(output);
    if (!left.Count && !right.Count) { return {}; }
    ui64 paired = 0;
    for (const auto* step : group.Steps) { paired += StepValue(*step, input).has_value() && StepValue(*step, output).has_value(); }
    const bool comparable = paired && paired == left.Count && paired == right.Count && !left.Overflow && !right.Overflow;
    const ui64 scale = std::max(left.Sum, right.Sum);
    auto value = [&](const auto& metric) {
        TStringBuilder result;
        result << (metric.Overflow ? TString("<span class='cs-unknown'>Overflow</span>")
            : metric.Count ? ColumnShardWidgetNumber(metric.Sum, bytes)
            : TString("<span class='cs-unknown'>Not recorded</span>"));
        if (comparable && scale) {
            result << "<div class='cs-meter'><i style='width:" << 100.0 * metric.Sum / scale << "%'></i></div>";
        }
        return TString(result);
    };
    return TStringBuilder() << "<div class='cs-flow' data-cs-flow='" << ColumnShardAttribute(title) << "'>"
        << ColumnShardWidgetField(TStringBuilder() << title << " input", value(left))
        << "<svg viewBox='0 0 22 12'><path d='M1 6H20M15 1L20 6L15 11'/></svg>"
        << ColumnShardWidgetField(TStringBuilder() << title << " output", value(right)) << "</div>";
}

TString ColumnShardWidgetAvailability(const TStepGroup& group, TStringBuf path, TStringBuf title,
    TStringBuf trueLabel = "present", TStringBuf falseLabel = "absent") {
    ui64 known = 0, available = 0;
    for (const auto* step : group.Steps) {
        const auto* value = step->GetValueByPath(path);
        if (value && value->GetType() == NJson::JSON_BOOLEAN) { ++known; available += value->GetBooleanSafe(); }
    }
    if (!known) { return {}; }
    TStringBuilder value;
    value << "<div class='cs-availability'><span class='cs-state " << (available == known ? "cs-good" : "cs-warning") << "'><i></i>"
        << ColumnShardWidgetNumber(available, false) << "<span class='cs-label'>" << trueLabel << "</span></span>"
        << "<span class='cs-count'>" << ColumnShardWidgetNumber(known - available, false)
        << "<small>" << falseLabel << "</small></span></div>";
    return ColumnShardWidgetField(title, value);
}

TString ColumnShardWidgetDetails(const TColumnShardNode& node, bool partial) {
    TStringBuilder html;
    TString kind = node.Kind ? node.Kind : node.Name;
    if (kind == "PKFilter") { kind = "Filter"; }
    if (kind == "IndexFilter") { kind = "IndexCheck"; }
    if (kind == "FetchOtherColumns") { kind = "Fetching"; }
    const auto group = ColumnShardWidgetGroup(node, kind);
    auto chips = [&](TStringBuf path, TStringBuf title, const TStepGroup& from) {
        const auto values = ColumnShardWidgetValues(from, path);
        if (!values.empty()) { html << ColumnShardWidgetField(title, ColumnShardWidgetChips(values), "cs-wide"); }
    };
    if (node.Skipped) { html << ColumnShardWidgetField("Skipped calls", ColumnShardWidgetNumber(node.Skipped, false)); }
    if (node.Failed) { html << ColumnShardWidgetField("Failed calls", ColumnShardWidgetNumber(node.Failed, false), "cs-observed"); }
    if (node.Cancelled) { html << ColumnShardWidgetField("Cancelled calls", ColumnShardWidgetNumber(node.Cancelled, false), "cs-observed"); }
    if (node.Row == "step" && (kind == "Ack" || kind == "Limit" || kind == "Ready" || kind == "Finish" ||
            kind == "PrepareResult" || kind == "Merge" || kind == "ReaderInit" || kind == "FetchAccessors" || kind == "ParseProgram")) {
        std::set<TString> actions;
        for (const auto& stepName : ColumnShardWidgetValues(group, "StepName")) {
            const TString label = stepName == "WAIT_CREDIT" ? "Wait for credit"
                : stepName == "SYNC_LIMIT" ? "Synchronize limit"
                : stepName == "BUILD_STAGE_RESULT" ? "Prepare source page"
                : stepName == "MERGE_INTERVAL" ? "Merge interval" : stepName;
            actions.insert(label);
        }
        if (!actions.empty()) { html << ColumnShardWidgetField("Action", ColumnShardWidgetChips(actions)); }
    }
    if (node.SharedIndexInputs) {
        html << "<span class='cs-state cs-good'><i></i>Shared input owner</span>";
    } else if (!node.SharedInputReferences.empty()) {
        html << "<span class='cs-state'><i></i>Shared inputs charged to their owner</span>";
    }
    const bool indexContext = node.Meta == "IndexFilter" || node.Kind == "IndexCheck" ||
        (node.SharedIndexInputs && node.IndexKey);
    for (const auto& [identity, descriptor] : node.IndexDescriptors) {
            if (!indexContext) { break; }
            Y_UNUSED(identity);
            TStringBuilder value;
            value << "<strong class='cs-value'>" << SvgEscape(StepText(descriptor, "Name")) << "</strong>"
                << "<span class='cs-label'>" << SvgEscape(StepText(descriptor, "Type")) << "</span>";
            const TString indexId = ColumnShardIdentity(descriptor, "IndexId");
            if (indexId) { value << "<span class='cs-label'>Index id <b>" << SvgEscape(indexId) << "</b></span>"; }
            const auto* columns = descriptor.GetValueByPath("Columns");
            if (columns && columns->GetType() == NJson::JSON_ARRAY) {
                std::set<TString> names;
                for (const auto& column : columns->GetArray()) { names.insert(column.GetStringSafe()); }
                value << ColumnShardWidgetChips(names);
            }
            html << ColumnShardWidgetField("Index", value, "cs-index");
    }
    if (kind == "Filter" || kind == "IndexCheck") {
        auto predicates = ColumnShardWidgetValues(group, "Details.Predicate");
        if (predicates.empty()) {
            for (const auto* step : group.Steps) {
                TString operation = StepText(*step, "Operation");
                if (operation.StartsWith("Filter(") && operation.size() > 7 && operation.back() == ')') {
                    predicates.insert(operation.substr(7, operation.size() - 8));
                } else if (kind == "IndexCheck" && operation.StartsWith("Index ")) {
                    predicates.insert(std::move(operation));
                }
            }
        }
        TStringBuilder expressions;
        for (const auto& predicate : predicates) {
            expressions << "<div class='cs-expression'>" << SvgEscape(predicate) << "</div>";
        }
        if (!predicates.empty()) { html << ColumnShardWidgetField("Predicate", expressions, "cs-wide"); }
        if (kind == "IndexCheck") {
        const auto checkerTypes = ColumnShardWidgetValues(group, "Details.IndexType");
        if (!checkerTypes.empty()) {
            const bool compact = checkerTypes.size() == 1 && checkerTypes.begin()->size() <= 24;
            html << ColumnShardWidgetField("Checker type", ColumnShardWidgetChips(checkerTypes), compact ? "" : "cs-wide");
        }
            html << ColumnShardWidgetFlow(group, "Details.CandidateInputRows", "Details.CandidateOutputRows", "Candidates");
            html << ColumnShardWidgetAvailability(group, "Details.IndexAvailable", "Index metadata")
                << ColumnShardWidgetAvailability(group, "Details.IndexDataAvailable", "Index payload")
                << ColumnShardWidgetAvailability(group, "Details.AppliedToResourceFilter", "Direct mask application", "applied", "not applied");
        }
        if (node.Row != "step" && kind == "Filter") {
            std::map<TString, TStepGroup> masks;
            for (const auto* step : group.Steps) {
                masks[StepText(*step, "Details.Purpose")].Steps.push_back(step);
            }
            for (const auto& [purpose, mask] : masks) {
                const TString label = purpose == "pk_range" ? "PK range" : purpose == "sql" ? "Predicate"
                    : purpose == "deletion" ? "Deletion" : purpose == "duplicate" ? "Duplicate"
                    : purpose == "snapshot" ? "Snapshot" : purpose == "limit" ? "Source limit" : "Mask";
                html << ColumnShardWidgetFlow(mask, "Details.InputRows", "Details.OutputRows", label);
            }
        } else {
            chips("Details.Purpose", "Mask", group);
            html << ColumnShardWidgetFlow(group, "Details.InputRows", "Details.OutputRows", "Rows");
        }
    } else if (kind == "Fetching" || kind == "Assemble" || kind == "ReserveMemory") {
        chips("Details.Columns", "Columns", group);
        html << ColumnShardWidgetMetric(group, "Details.ReservationBytes", "Requested memory quota", true)
            << ColumnShardWidgetMetric(group, "Details.RequestedBytes", "Requested ranges", true)
            << ColumnShardWidgetMetric(group, "Details.RequestedRangeBytes", "Requested ranges", true)
            << ColumnShardWidgetMetric(group, "ReadBytes", "Delivered payload", true);
    } else if (kind == "StorageRead") {
        chips("Details.BackendKind", "Backend", group);
        chips("Details.StorageTier", "Storage", group);
        chips("Details.DataKind", "Payload kind", group);
        html << ColumnShardWidgetMetric(group, "Details.RequestedRangeBytes", "Requested ranges", true)
            << ColumnShardWidgetMetric(group, "ReadBytes", "Returned payload", true)
            << ColumnShardWidgetAvailability(group, "Details.IsRetry", "Read attempts", "retry", "initial");
    } else if (kind == "Decode") {
        chips("Details.Column", "Column", group);
        chips("Details.Codec", "Codec", group);
        html << ColumnShardWidgetFlow(group, "Details.EncodedInputBytes", "Details.DecodedOutputBytes", "Bytes", true)
            << ColumnShardWidgetMetric(group, "Details.DecodedRows", "Rows");
    } else if (kind == "Aggregation") {
        chips("Details.Keys", "Grouping keys", group);
        chips("Details.Functions", "Aggregate functions", group);
        html << ColumnShardWidgetFlow(group, "Details.InputRows", "Details.OutputRows", "Aggregate call rows");
    } else if (kind == "Calculation") {
        auto expressions = ColumnShardWidgetValues(group, "Details.Expression");
        if (expressions.empty()) { expressions = ColumnShardWidgetValues(group, "Operation"); }
        for (const auto& expression : expressions) {
            html << ColumnShardWidgetField("Expression", "<div class='cs-expression'>" + SvgEscape(expression) + "</div>", "cs-wide");
        }
        html << ColumnShardWidgetMetric(group, "Details.RowsCount", "Rows");
    } else if (kind == "Projection" || kind == "BuildResult") {
        if (kind == "Projection") {
            chips("Details.InputColumns", "Input columns", group);
            chips("Details.OutputColumns", "Output columns", group);
        }
        html << ColumnShardWidgetMetric(group, "Details.PageRows", "Input page rows")
            << ColumnShardWidgetMetric(group, "Details.PageInputBytes", "Input page payload", true);
        html << ColumnShardWidgetMetric(group, "Details.ResultRows", "Produced rows")
            << ColumnShardWidgetMetric(group, "Details.ResultBytes", "Arrow result payload", true);
    }
    if (node.Row == "meta" && kind != "ReserveMemory") {
        html << ColumnShardWidgetMetric(ColumnShardWidgetGroup(node, "ReserveMemory"),
            "Details.ReservationBytes", "Requested memory quota", true);
    }
    if (!node.IndexOrders.empty()) {
        TStringBuilder values;
        for (ui64 order : node.IndexOrders) { values << "<span class='cs-chip'>" << order << "</span>"; }
        html << ColumnShardWidgetField("Observed source-local check positions", "<div class='cs-chips'>" + values + "</div>");
    }
    auto summary = [&](const auto& metric, TStringBuf title, bool bytes = false, TStringBuf unit = {}) {
        const auto value = metric.Summary();
        if (value.ExpectedCalls) {
            TString content = value.KnownCalls ? ColumnShardWidgetNumber(value.Sum, bytes)
                : TString("<span class='cs-unknown'>Not recorded</span>");
            if (unit && value.KnownCalls) { content += "<span class='cs-unit'>" + SvgEscape(unit) + "</span>"; }
            html << ColumnShardWidgetField(title, content);
        }
    };
    summary(node.SelectedPortions, "Selected portions");
    summary(node.SelectedVersionRows, "Metadata version rows");
    summary(node.SelectedBlobBytes, "Metadata blob estimate", true);
    summary(node.SentRows, "Sent rows");
    summary(node.SentDataBatches, "Data batches");
    summary(node.SentArrowBytes, "Arrow payload", true);
    summary(node.ReaderOverhead, "Reader dispatcher time", false, "us");
    for (const auto& [source, value] : node.Sources) {
        if (!value.Bytes && !value.Unknown) { continue; }
        TStringBuilder content;
        content << "<strong class='cs-value'>" << SvgEscape(source.first) << "</strong><div class='cs-label'>"
            << SvgEscape(source.second) << "</div>";
        content << (value.Bytes || !value.Unknown ? ColumnShardWidgetNumber(value.Bytes, true)
            : TString("<span class='cs-unknown'>Volume not recorded</span>"));
        if (value.Unknown || value.Overflow || partial) { content << "<div class='cs-state cs-warning'><i></i>Observed volume</div>"; }
        html << ColumnShardWidgetField("Read origin", content, "cs-provider");
    }
    if (!html) { html << "<span class='cs-unknown'>No recorded attributes</span>"; }
    return html;
}

TString ColumnShardWidgetTablets(const TColumnShardNode& node, const TColumnShardRead& read) {
    TStringBuilder html;
    const ui64 total = read.Denominator(), active = node.ActiveTablets.size();
    const double coverage = total ? std::min(100.0, 100.0 * active / total) : 0;
    html << "<span class='cs-count'><b>" << active << "</b><small>active</small></span>"
        << "<span class='cs-count'><b>" << total << "</b><small>" << (read.ExpectedKnown ? "planned" : "received")
        << "</small></span><div class='cs-coverage'><i style='width:" << coverage << "%'></i></div>";
    return html;
}

TString ColumnShardWidgetStatistics(const TColumnShardNode& node, const TColumnShardRead& read) {
    TStringBuilder html;
    const ui64 active = node.ActiveTablets.size();
    if (!node.Calls && !node.Active.Summary().ExpectedCalls && !node.Wait.Summary().ExpectedCalls && !node.Read.Summary().ExpectedCalls) {
        return html;
    }
    html << "<div class='cs-stat cs-stat-head'><span></span><span>Total</span><span>Min</span><span>Avg</span><span>Max</span></div>";
    auto timing = [&](const auto& metric, TStringBuf name) {
        const auto value = metric.Summary();
        auto amount = [&](double number) -> TString {
            const double scale = number >= 31536000000000000.0 ? 31536000000000000.0 : number >= 31536000000000.0 ? 31536000000000.0
                : number >= 86400000000.0 ? 86400000000.0 : number >= 3600000000.0 ? 3600000000.0
                : number >= 60000000.0 ? 60000000.0 : number >= 1000000 ? 1000000 : number >= 1000 ? 1000 : 1;
            return TStringBuilder() << "<span class='cs-value' title='" << Sprintf("%.2f", number) << " us'>" << Sprintf("%.2f", number / scale)
                << "<span class='cs-unit'>" << (scale == 1 ? "us" : scale == 1000 ? "ms" : scale == 1000000 ? "s"
                    : scale == 60000000 ? "m" : scale == 3600000000 ? "h" : scale == 86400000000.0 ? "d"
                    : scale == 31536000000000.0 ? "y" : "ky") << "</span></span>";
        };
        const TString unknown = "<span class='cs-unknown'>Not recorded</span>";
        html << "<div class='cs-stat" << (value.KnownCalls < value.ExpectedCalls ? " cs-observed" : "")
            << "' title='Measured calls " << value.KnownCalls << " of " << value.ExpectedCalls
            << ". Complete shard totals " << value.KnownShards << " of " << active << ".'>"
            << "<span class='cs-label'>" << name << "</span>"
            << (value.KnownCalls ? TString(amount(value.Sum)) : unknown)
            << (value.KnownShards ? TString(amount(value.Min)) : unknown)
            << (value.KnownShards ? TString(amount(double(value.KnownSum) / value.KnownShards)) : unknown)
            << (value.KnownShards ? TString(amount(value.Max)) : unknown) << "</div>";
    };
    timing(node.Active, "Active");
    timing(node.Wait, "Wait");
    const auto bytes = node.Read.Summary();
    html << "<div class='cs-stat cs-stat-read'><span class='cs-label'>Read</span>"
        << (bytes.KnownCalls ? ColumnShardWidgetNumber(bytes.Sum, true)
            : TString("<span class='cs-unknown' style='grid-column:2/4;text-align:left'>Not recorded</span>"));
    if (read.Partial || node.Failed || node.Cancelled || bytes.KnownCalls < bytes.ExpectedCalls) {
        html << "<span class='cs-state cs-warning' style='grid-column:4/6'><i></i>Observed</span>";
    }
    html << "</div>";
    return html;
}

void PrintColumnShardWidget(TStringBuilder& svg, ui32 x, ui32 y, ui32 width, ui32 height,
    TStringBuf css, const TString& html) {
    svg << "<foreignObject x='" << x << "' y='" << y << "' width='" << width << "' height='" << height
        << "' data-cs-cell='" << ColumnShardAttribute(css) << "'><div xmlns='http://www.w3.org/1999/xhtml' class='cs-ui " << css
        << "'>" << html << "</div></foreignObject>";
    if (x > INTERNAL_WIDTH) {
        svg << "<rect class='cs-column-separator' x='" << x - 1 << "' width='2' height='100%'/>";
    }
}

std::vector<const TColumnShardNode*> ColumnShardOrderedChildren(const TColumnShardNode& node) {
    std::vector<const TColumnShardNode*> children;
    const bool splitIndexes = std::any_of(node.Children.begin(), node.Children.end(), [](const auto& entry) {
        return entry.second.Row == "meta" && entry.second.Meta == "IndexFilter" && entry.second.IndexKey;
    });
    for (const auto& [_, child] : node.Children) {
        if (splitIndexes && child.Row == "meta" && child.Meta == "IndexFilter" && !child.IndexKey &&
            !child.Calls && !child.Skipped && child.Children.empty()) {
            continue;
        }
        children.push_back(&child);
    }
    std::stable_sort(children.begin(), children.end(), [&node](const auto* left, const auto* right) {
        const auto leftOrder = ColumnShardOrder(left->Name, node), rightOrder = ColumnShardOrder(right->Name, node);
        if (leftOrder != rightOrder) { return leftOrder < rightOrder; }
        if (left->Row == "meta" && right->Row == "meta" && left->Meta == "IndexFilter" && right->Meta == "IndexFilter") {
            if (left->IndexPosition != right->IndexPosition) { return left->IndexPosition < right->IndexPosition; }
            if (left->SharedIndexInputs != right->SharedIndexInputs) { return left->SharedIndexInputs; }
        }
        return std::make_pair(left->Name, left->IndexKey) < std::make_pair(right->Name, right->IndexKey);
    });
    return children;
}

void PrintColumnShardNodeMetrics(TStringBuilder& svg, const TColumnShardRead& read,
    const TColumnShardNode& node, bool structural) {
    const auto active = node.Active.Summary();
    const auto wait = node.Wait.Summary();
    NJson::TJsonValue indexPositions(NJson::JSON_ARRAY);
    for (ui64 order : node.IndexOrders) { indexPositions.AppendValue(order); }
    svg << "<svg data-cs-index-key='" << ColumnShardAttribute(node.IndexKey)
        << "' data-cs-index-shared='" << (node.SharedIndexInputs ? "true" : "false")
        << "' data-cs-index-positions='" << ColumnShardAttribute(indexPositions.GetStringRobust())
        << "' data-cs-source-reads='" << ColumnShardAttribute(ColumnShardSourcesAttribute(node))
        << "' data-cs-row='" << node.Row << "' data-cs-family='" << ColumnShardAttribute(read.Family)
        << "' data-cs-meta='" << ColumnShardAttribute(node.Meta) << "' data-cs-path='" << ColumnShardAttribute(node.Path)
        << "' data-cs-kind='" << ColumnShardAttribute(node.Kind) << "' data-cs-active-shards='" << node.ActiveTablets.size()
        << "' data-cs-participating-shards='" << read.Denominator() << "' data-cs-received-shards='" << read.ReceivedShards()
        << "' data-cs-family-shards='" << read.Tablets.size()
        << "' data-cs-expected-known='" << (read.ExpectedKnown ? "true" : "false")
        << "' data-cs-known-expected-shards='" << read.KnownExpectedTablets.size()
        << "' data-cs-denominator='" << (read.ExpectedKnown ? "expected" : "received")
        << "' data-cs-structural='" << (structural ? "true" : "false") << "' data-cs-calls='" << node.Calls
        << "' data-cs-skipped='" << node.Skipped << "' data-cs-failed='" << node.Failed << "' data-cs-cancelled='" << node.Cancelled
        << "' data-cs-active-sum='" << active.Sum << "' data-cs-owned-sum='" << active.Sum
        << "' data-cs-active-overflow='" << (active.Overflow ? "true" : "false")
        << "' data-cs-active-known-shards='" << active.KnownShards
        << "' data-cs-active-eligible-shards='" << active.EligibleShards << "' data-cs-active-mean='"
        << (active.KnownShards ? double(active.KnownSum) / active.KnownShards : 0)
        << "' data-cs-active-min='" << (active.KnownShards ? active.Min : 0) << "' data-cs-active-max='" << active.Max
        << "' data-cs-wait-sum='" << wait.Sum << "' data-cs-wait-min='" << (wait.KnownShards ? wait.Min : 0)
        << "' data-cs-wait-known-shards='" << wait.KnownShards << "' data-cs-partial='" << (read.Partial ? "true" : "false");
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
}

void PrintColumnShardBoundaryMetrics(TStringBuilder& svg, const TColumnShardNode& node) {
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
    const auto candidatesIn = node.Details.Metric("Details.CandidateInputRows");
    const auto candidatesOut = node.Details.Metric("Details.CandidateOutputRows");
    if (candidatesIn.Count || candidatesOut.Count) {
        svg << "' data-cs-index-candidate-input-known='" << candidatesIn.Count
            << "' data-cs-index-candidate-output-known='" << candidatesOut.Count;
        if (candidatesIn.Count && !candidatesIn.Overflow) {
            svg << "' data-cs-index-candidate-input-rows='" << candidatesIn.Sum;
        }
        if (candidatesOut.Count && !candidatesOut.Overflow) {
            svg << "' data-cs-index-candidate-output-rows='" << candidatesOut.Sum;
        }
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
}

ui32 PrintColumnShardNode(TStringBuilder& svg, const TVisualizer& viz, const TColumnShardRead& read,
    const TColumnShardNode& node, ui32 top, ui32 depth) {
    constexpr ui32 rowHeight = 60;
    const auto children = ColumnShardOrderedChildren(node);
    TStringBuilder body;
    ui32 height = rowHeight;
    for (size_t index = 0; index < children.size(); ++index) {
        height += PrintColumnShardNode(body, viz, read, *children[index], height - rowHeight, depth + 1);
    }
    const bool structural = node.Row == "step" && !node.Calls && !node.Skipped && !children.empty();
    const bool collapsed = node.Row == "meta" && !children.empty();
    const ui32 visibleHeight = collapsed ? rowHeight : height;
    PrintColumnShardNodeMetrics(svg, read, node, structural);
    PrintColumnShardBoundaryMetrics(svg, node);
    svg << "' data-cs-row-height='" << rowHeight << "' data-cs-row-min-height='24' x='0' y='" << top << "' width='" << viz.Config.Width
        << "' height='" << visibleHeight << "'><svg class='" << (collapsed ? "folded" : "")
        << "' data-cs-header='true' width='" << viz.Config.Width << "' height='" << rowHeight << "'><g>"
        << "<rect class='cs-row-background' width='" << viz.Config.Width << "' height='" << rowHeight
        << "' fill='var(--stage-main,#f2f2f2)'/>";
    if (!children.empty()) {
        svg << "<g class='plus button cs-row-toggle' data-cs-toggle='row' transform='translate(" << depth * INDENT_X + 4
            << ",3)'><rect width='12' height='12' fill='transparent'/>"
            << "<path class='cs-chevron' d='M0 0L6 4L0 8Z' fill='var(--stage-text,#262626)'/></g>";
    }
    const ui32 statisticsEnd = std::min(std::max(viz.Config.TimelineLeft, viz.Config.SummaryLeft + 285), viz.Config.Width * 3 / 4);
    const ui32 nameEnd = viz.Config.OperatorLeft;
    const ui32 indent = std::min<ui32>(depth * INDENT_X + INTERNAL_WIDTH, nameEnd > 70 ? nameEnd - 70 : 0);
    TString name = node.Row == "step" && node.Kind ? node.Kind : node.Name;
    svg << "<text data-cs-name='true' class='cs-names" << (node.Row == "step" ? "" : " cs-metastep")
        << "' x='" << indent << "' y='11'>" << SvgEscape(name) << "</text>";
    if (!node.Calls || node.Failed || node.Cancelled) {
        const TString status = structural ? "Context" : node.Failed ? "Failed" : node.Cancelled ? "Cancelled"
            : node.Skipped ? "Skipped" : !read.Attempts || read.Partial ? "Not observed" : "Not executed";
        svg << "<text class='cs-heading' x='" << indent << "' y='22'>" << status << "</text>";
    }
    PrintColumnShardWidget(svg, nameEnd, 0, viz.Config.TaskLeft - nameEnd, rowHeight,
        "cs-tablets", ColumnShardWidgetTablets(node, read));
    PrintColumnShardWidget(svg, viz.Config.TaskLeft, 0, viz.Config.SummaryLeft - viz.Config.TaskLeft, rowHeight,
        "cs-calls", ColumnShardWidgetNumber(node.Calls, false));
    PrintColumnShardWidget(svg, viz.Config.SummaryLeft, 0, statisticsEnd - viz.Config.SummaryLeft, rowHeight,
        "cs-stats", ColumnShardWidgetStatistics(node, read));
    PrintColumnShardWidget(svg, statisticsEnd, 0, viz.Config.Width - statisticsEnd, rowHeight,
        "cs-details", ColumnShardWidgetDetails(node, read.Partial));
    svg << "</g></svg>";
    if (!children.empty()) {
        svg << "<svg data-cs-children='true' x='0' y='" << rowHeight << "' width='" << viz.Config.Width << "' height='" << height - rowHeight << "'>"
            << body << "</svg>";
    }
    svg << "</svg>" << Endl;
    return visibleHeight;
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

bool HasColumnShardStorageStats(const TVisualizer& viz) {
    for (const auto& plan : viz.Plans) {
        for (const auto& stage : plan->Stages) {
            for (const auto& scan : stage->ColumnShardScans.GetArray()) {
                if (StepValue(scan, "StorageStatsVersion").value_or(0)) {
                    return true;
                }
            }
        }
    }
    return false;
}

struct TColumnShardStorage {
    struct TAttempt {
        TColumnShardAttempt Context;
        TString Table;
        bool VersionKnown = false;
        bool Truncated = false;
        bool Conflict = false;
        std::map<TString, TString> Scopes;
        TColumnShardSources Snapshot;
    };
    std::map<TColumnShardSourceKey, std::map<TString, TColumnShardSourceTotals>> Reads;
    TColumnShardSources Sources;
    std::map<TString, TString> Labels;
    TColumnShardSourceTotals Total;
    bool Present = false;
    bool PopulationPartial = false;
    bool Partial = false;
};

TColumnShardStorage CollectColumnShardStorage(const TVisualizer& viz, const TStage* onlyStage) {
    TColumnShardStorage result;
    std::map<TString, TColumnShardStorage::TAttempt> attempts;
    TColumnShardRead coverage;
    ui64 unidentified = 0;
    size_t planIndex = 0;
    for (const auto& plan : viz.Plans) {
        for (const auto& stage : plan->Stages) {
            if (stage.get() != onlyStage) {
                continue;
            }
            const TString scope = TStringBuilder() << "plan " << planIndex << " / stage " << stage->PhysicalStageId;
            for (const auto& scan : stage->ColumnShardScans.GetArray()) {
                if (scan.GetType() != NJson::JSON_MAP) {
                    continue;
                }
                if (ColumnShardFlag(scan, "ExpectedContextOnly")) {
                    TColumnShardAttempt header;
                    ReadColumnShardCohortHeader(header, scan);
                    coverage.AddCohort(header, {}, true);
                    continue;
                }
                TString identity = ColumnShardIdentity(scan, "StatsScanId");
                if (!identity) {
                    identity = "unidentified-storage-" + ToString(++unidentified);
                    result.Partial = true;
                }
                auto& attempt = attempts[identity];
                if (!attempt.Context.Last || !ColumnShardFlag(*attempt.Context.Last, "Finished") ||
                    ColumnShardFlag(scan, "Finished")) {
                    attempt.Context.Last = &scan;
                }
                attempt.Truncated = attempt.Truncated || ColumnShardFlag(scan, "Truncated");
                ReadColumnShardCohortHeader(attempt.Context, scan);
                if (auto tablet = ColumnShardIdentity(scan, "TabletId")) {
                    attempt.Context.Tablet = std::move(tablet);
                }
                if (auto table = StepText(scan, "TablePath")) {
                    attempt.Conflict = attempt.Conflict || (attempt.Table && attempt.Table != table);
                    attempt.Table = std::move(table);
                }
                attempt.Scopes.emplace(scope, scope);
                if (!StepValue(scan, "StorageStatsVersion").value_or(0)) {
                    continue;
                }
                result.Present = true;
                attempt.VersionKnown = true;
                const auto* sources = scan.GetValueByPath("StorageReads");
                if (!sources || sources->GetType() != NJson::JSON_ARRAY) {
                    continue;
                }
                for (const auto& source : sources->GetArray()) {
                    if (source.GetType() != NJson::JSON_MAP) {
                        attempt.Conflict = true;
                        continue;
                    }
                    auto& previous = attempt.Snapshot[ColumnShardSourceKey(source)];
                    const auto value = ColumnShardSourceTotals(source);
                    previous.Bytes = std::max(previous.Bytes, value.Bytes);
                    previous.Reads = std::max(previous.Reads, value.Reads);
                    previous.Unknown = std::max(previous.Unknown, value.Unknown);
                    previous.Shared = previous.Shared || value.Shared;
                    previous.DataKinds.insert(value.DataKinds.begin(), value.DataKinds.end());
                }
            }
        }
        ++planIndex;
    }
    for (const auto& [identity, attempt] : attempts) {
        coverage.Tablets.insert(attempt.Context.Tablet);
        coverage.AddCohort(attempt.Context, attempt.Context.Tablet);
        result.Partial = result.Partial || attempt.Truncated;
        result.Partial = result.Partial || !attempt.VersionKnown || attempt.Conflict || !attempt.Context.Tablet ||
            !attempt.Context.Last || !ColumnShardFlag(*attempt.Context.Last, "Finished") ||
            ColumnShardFlag(*attempt.Context.Last, "Truncated") || StepValue(*attempt.Context.Last, "PendingOperations").value_or(0);
        const TString scope = attempt.Scopes.empty() ? TString("unknown read") : attempt.Scopes.begin()->first;
        const TString read = ColumnShardKeyPart(scope) + ColumnShardKeyPart(attempt.Table);
        result.Labels[read] = (attempt.Table ? attempt.Table + " · " : TString()) + scope;
        for (const auto& [source, value] : attempt.Snapshot) {
            result.Reads[source][read].Add(value);
            result.Sources[source].Add(value);
            result.Total.Add(value);
        }
    }
    coverage.FinalizeCohorts();
    result.Partial = result.Partial || !coverage.ExpectedKnown || coverage.Partial;
    for (const auto& [_, cohort] : coverage.Cohorts) {
        result.Partial = result.Partial || cohort.Tablets.size() < cohort.Roster.size();
    }
    result.PopulationPartial = result.Partial;
    result.Partial = result.PopulationPartial || result.Total.Unknown || result.Total.Overflow;
    return result;
}

ui32 PrintColumnShardStorage(TStringBuilder& svg, const TVisualizer& viz, ui32 top, const TStage* onlyStage) {
    const auto storage = CollectColumnShardStorage(viz, onlyStage);
    if (!storage.Present) {
        return 0;
    }
    constexpr ui32 rowHeight = 20;
    constexpr ui32 headerHeight = INTERNAL_HEIGHT + INTERNAL_GAP_Y;
    const ui32 sourceEnd = viz.Config.Width * 32 / 100;
    const ui32 readEnd = viz.Config.Width * 69 / 100;
    const ui32 bytesEnd = viz.Config.Width * 91 / 100;
    TStringBuilder rows;
    ui32 height = 2 * headerHeight;
    auto row = [&](TStringBuf kind, const TColumnShardSourceKey& source, const TString& read,
                   const TColumnShardSourceTotals& value) {
        const bool partial = storage.PopulationPartial || value.Unknown || value.Overflow;
        rows << "<svg data-cs-storage-row='" << kind << "' data-cs-source-kind='" << ColumnShardAttribute(source.first)
            << "' data-cs-source-name='" << ColumnShardAttribute(source.second) << "' data-cs-storage-read='"
            << ColumnShardAttribute(read) << "' data-cs-storage-bytes='" << value.Bytes << "' data-cs-storage-reads='" << value.Reads
            << "' data-cs-storage-unknown='" << value.Unknown << "' data-cs-storage-partial='" << (partial ? "true" : "false")
            << "' data-cs-row-min-height='" << rowHeight
            << "' x='0' y='" << height << "' width='" << viz.Config.Width << "' height='" << rowHeight << "'><g>"
            << "<rect class='cs-row-background' width='" << viz.Config.Width << "' height='" << rowHeight << "' fill='"
            << (kind == "grand-total" ? "var(--stage-main,#f2f2f2)" : "#ffffff") << "'/>";
        TStringBuilder provider;
        provider << "<strong class='cs-value'>" << SvgEscape(source.first) << "</strong><div class='cs-label'>"
            << SvgEscape(source.second) << "</div>";
        PrintColumnShardWidget(rows, INTERNAL_WIDTH, 0, sourceEnd - INTERNAL_WIDTH, rowHeight, "cs-details cs-storage-values", provider);
        const ui64 known = value.Reads >= value.Unknown ? value.Reads - value.Unknown : 0;
        TStringBuilder coverage;
        coverage << "<div class='cs-shards'><div class='cs-coverage'><i style='width:"
            << (value.Reads ? 100.0 * known / value.Reads : 0) << "%'></i></div>"
            << "<span class='cs-count'><b>" << known << "</b><small>measured</small></span>"
            << "<span class='cs-count'><b>" << value.Unknown << "</b><small>unknown volume</small></span></div>";
        PrintColumnShardWidget(rows, sourceEnd, 0, readEnd - sourceEnd, rowHeight, "cs-details cs-storage-values", coverage);
        TStringBuilder volume;
        volume << (!value.Bytes && (value.Overflow || (value.Unknown && !known)) ? TString("<span class='cs-unknown'>Not recorded</span>")
            : ColumnShardWidgetNumber(value.Bytes, true));
        if (partial) { volume << "<div class='cs-state cs-warning'><i></i>Observed</div>"; }
        PrintColumnShardWidget(rows, readEnd, 0, bytesEnd - readEnd, rowHeight, "cs-details cs-storage-values", volume);
        PrintColumnShardWidget(rows, bytesEnd, 0, viz.Config.Width - bytesEnd, rowHeight,
            "cs-details cs-storage-values", ColumnShardWidgetNumber(value.Reads, false));
        rows << "</g></svg>" << Endl;
        height += rowHeight;
    };
    for (const auto& [source, value] : storage.Sources) {
        if (!value.Bytes && !value.Unknown) { continue; }
        row("source-total", source, {}, value);
    }
    row("grand-total", {"Stage total", {}}, {}, storage.Total);
    svg << "<svg data-cs-storage='stage' data-cs-storage-partial='" << (storage.Partial ? "true" : "false")
        << "' data-cs-plan-index='" << onlyStage->ColumnShardPlanIndex.value() << "' data-cs-stage-id='" << onlyStage->PhysicalStageId
        << "' x='0' y='" << top << "' width='" << viz.Config.Width << "' height='" << height << "'>"
        << SvgRect(0, 0, viz.Config.Width, 2 * headerHeight, "columns");
    StepCell(svg, INTERNAL_WIDTH, viz.Config.Width - INTERNAL_WIDTH, "DataSource");
    svg << "<svg data-cs-storage-header='true' x='0' y='" << headerHeight << "' width='" << viz.Config.Width << "' height='" << headerHeight << "'>";
    StepCell(svg, INTERNAL_WIDTH, sourceEnd > INTERNAL_WIDTH ? sourceEnd - INTERNAL_WIDTH : 0, "Source");
    StepCell(svg, sourceEnd, readEnd - sourceEnd, "Volume coverage");
    StepCell(svg, readEnd, bytesEnd - readEnd, "Read bytes");
    StepCell(svg, bytesEnd, viz.Config.Width - bytesEnd, "Reads");
    for (ui32 x : {sourceEnd, readEnd, bytesEnd}) {
        svg << "<rect class='cs-column-separator' x='" << x - 1 << "' width='2' height='100%'/>";
    }
    svg << "</svg>" << rows << "</svg>" << Endl;
    return height;
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

void CollectColumnShardAttemptData(TColumnShardAttempt& attempt, const NJson::TJsonValue& scan, bool missingIdentity) {
    if (attempt.Last && ColumnShardFlag(*attempt.Last, "Finished") && !ColumnShardFlag(scan, "Finished")) {
        ReadColumnShardCohortHeader(attempt, scan);
        return;
    }
    attempt.Last = &scan;
    attempt.MissingIdentity = attempt.MissingIdentity || missingIdentity;
    if (auto tablet = ColumnShardIdentity(scan, "TabletId")) { attempt.Tablet = std::move(tablet); }
    if (auto reader = StepText(scan, "ReaderKind")) { attempt.Reader = std::move(reader); }
    ReadColumnShardCohortHeader(attempt, scan);
    const auto* cache = scan.GetValueByPath("CacheStats");
    if (cache && cache->GetType() == NJson::JSON_MAP) { attempt.Cache = cache; }
    const auto* steps = scan.GetValueByPath("Steps");
    if (steps && steps->GetType() == NJson::JSON_ARRAY) {
        for (const auto& step : steps->GetArray()) {
            if (step.GetType() != NJson::JSON_MAP) { continue; }
            if (auto id = StepValue(step, "StepId")) {
                attempt.Steps[*id] = &step;
            } else {
                attempt.UnidentifiedSteps.push_back(&step);
                attempt.MissingIdentity = true;
            }
        }
    }
}

std::map<TString, TColumnShardAttempt> CollectColumnShardAttempts(const TStage& stage,
    TColumnShardRead& cohortContext, bool& expectedContextSeen) {
    std::map<TString, TColumnShardAttempt> attempts;
    ui64 unidentified = 0;
    for (const auto& scan : stage.ColumnShardScans.GetArray()) {
        if (scan.GetType() != NJson::JSON_MAP) { continue; }
        if (ColumnShardFlag(scan, "ExpectedContextOnly")) {
            TColumnShardAttempt header;
            ReadColumnShardCohortHeader(header, scan);
            cohortContext.AddCohort(header, {}, true);
            expectedContextSeen = true;
            continue;
        }
        TString identity = ColumnShardIdentity(scan, "StatsScanId");
        const bool missingIdentity = !identity;
        if (missingIdentity) { identity = TStringBuilder() << "unidentified-" << ++unidentified; }
        CollectColumnShardAttemptData(attempts[identity], scan, missingIdentity);
    }
    return attempts;
}

std::map<TString, TColumnShardRead> BuildColumnShardReads(const std::map<TString, TColumnShardAttempt>& attempts,
    TColumnShardRead& cohortContext) {
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
                    read.Add(*step, tablet, attempt.Last ? StepText(*attempt.Last, "TablePath") : TString());
                }
                for (const auto* step : attempt.UnidentifiedSteps) {
                    read.Add(*step, tablet, attempt.Last ? StepText(*attempt.Last, "TablePath") : TString());
                }
                AddColumnShardSourceDetails(read, attempt, identity);
                static constexpr TStringBuf cacheFields[] = {"CompletedReadRanges", "CacheServedReadRanges", "ReturnedBytes", "CacheServedBytes"};
                for (unsigned i = 0; i < std::size(cacheFields); ++i) {
                    read.CacheFields[i][tablet].Add(attempt.Cache ? StepValue(*attempt.Cache, cacheFields[i]) : std::nullopt);
                }
                if (attempt.Cache && (StepValue(*attempt.Cache, "CompletedReadRanges").value_or(0) ||
                        StepValue(*attempt.Cache, "ReturnedBytes").value_or(0))) {
                    read.CacheTablets.insert(tablet);
                }
            }
    return reads;
}

TString ColumnShardWidgetReadTitle(const TColumnShardRead& read, ui64 stageId,
    const std::map<TString, TColumnShardAttempt>& attempts) {
    TStringBuilder title;
    title << "<strong class='cs-value'>ColumnShard</strong>";
    std::set<TString> readerNames;
    for (const auto& [identity, attempt] : attempts) {
        Y_UNUSED(identity);
        const TString family = attempt.Reader == "PLAIN" ? "PLAIN"
            : attempt.Reader == "SIMPLE" || attempt.Reader == "TRIVIAL" ? "SIMPLE/TRIVIAL" : "UNKNOWN";
        if (family == read.Family && attempt.Reader) { readerNames.insert(attempt.Reader); }
    }
    title << ColumnShardWidgetChips(readerNames)
        << "<span class='cs-label'>Stage</span><b>" << stageId
        << "</b><span class='cs-label'>Family tablets</span><b>" << read.Tablets.size()
        << "</b><span class='cs-label'>Received</span><b>" << read.ReceivedShards()
        << "</b><span class='cs-label'>Planned</span><b>";
    if (read.ExpectedKnown) { title << read.ExpectedShards; } else { title << "Not recorded"; }
    title << "</b><span class='cs-state " << (read.Partial || !read.Attempts ? "cs-warning" : "cs-good")
        << "'><i></i>" << (!read.Attempts ? "Runtime not observed" : read.Partial ? "Incomplete snapshots" : "Complete snapshots") << "</span>";
    if (read.ExpectedKnown && read.ExpectedShards > read.ReceivedShards()) {
        title << "<span class='cs-label'>Not observed</span><b>" << read.ExpectedShards - read.ReceivedShards() << "</b>";
    }
    if (read.PendingAttempts) {
        title << "<span class='cs-label'>Pending operations maximum</span><b>" << read.MaxPendingOperations << "</b>";
    }
    if (read.Unclassified) {
        title << "<span class='cs-state cs-warning'><i></i>Unclassified operations</span>";
    }
    return title;
}

ui32 PrintAggregatedColumnShardStats(TStringBuilder& svg, const TVisualizer& viz, ui32 top, const TStage* onlyStage) {
    constexpr ui32 rowHeight = COLUMN_HEADER_HEIGHT;
    ui32 height = 0;
    for (const auto& plan : viz.Plans) {
        for (const auto& stage : plan->Stages) {
            if (stage.get() != onlyStage) {
                continue;
            }
            TColumnShardRead cohortContext;
            bool expectedContextSeen = false;
            const auto attempts = CollectColumnShardAttempts(*stage, cohortContext, expectedContextSeen);
            auto reads = BuildColumnShardReads(attempts, cohortContext);
            cohortContext.FinalizeCohorts();
            if (reads.empty() && expectedContextSeen) {
                reads["UNKNOWN"].Family = "UNKNOWN";
            }
            for (auto& [_, read] : reads) {
                LinkColumnShardSharedIndexInputs(read);
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
                if (!HasColumnShardStorageStats(viz)) {
                    readHeight += PrintColumnShardCache(body, viz, read, readHeight - rowHeight);
                }
                const TString title = ColumnShardWidgetReadTitle(read, stage->PhysicalStageId, attempts);
                svg << "<svg data-columnshard='read' data-cs-plan-index='" << stage->ColumnShardPlanIndex.value()
                    << "' data-cs-family='" << ColumnShardAttribute(read.Family)
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
                    << "<svg data-cs-read-header='true' width='" << viz.Config.Width << "' height='" << rowHeight << "'><g>"
                    << "<title>Semantic display order, not a global barrier or timeline. Independent tablets and DAG branches may overlap. "
                    << "Active is accumulated owned elapsed work, not OS CPU. Min/Avg/Max compare active physical tablet totals.</title>"
                    << SvgRect(0, 0, viz.Config.Width, rowHeight, "columns")
                    << "<g class='plus button' data-cs-toggle='read'>" << SvgRect(0, 0, CONN_SIZE, CONN_SIZE, "transparent")
                    << "<use href='#icon_minus' class='icon_minus' transform='scale(0.014)' fill='" << viz.Config.Palette.ConnectionText << "'/>"
                    << "<use href='#icon_plus' class='icon_plus' transform='scale(0.014)' fill='" << viz.Config.Palette.ConnectionText << "'/></g>"
                    << "<foreignObject data-cs-cell='cs-reader-header' x='" << INTERNAL_WIDTH << "' width='" << viz.Config.Width - INTERNAL_WIDTH
                    << "' height='" << rowHeight
                    << "'><div xmlns='http://www.w3.org/1999/xhtml' class='cs-ui cs-reader-header'>" << title
                    << "</div></foreignObject>"
                    << "</g></svg><svg data-cs-read-body='true' x='0' y='" << rowHeight << "' width='" << viz.Config.Width << "' height='" << readHeight - rowHeight << "'>"
                    << "<svg data-cs-column-header='true' width='" << viz.Config.Width << "' height='" << rowHeight << "'>";
                {
                    ui32 left = 0;
                    const ui32 end = std::min(std::max(viz.Config.TimelineLeft, viz.Config.SummaryLeft + 285), viz.Config.Width * 3 / 4);
                    for (ui32 right : {viz.Config.OperatorLeft, viz.Config.TaskLeft, viz.Config.SummaryLeft, end, viz.Config.Width}) {
                        svg << "<rect x='" << left + 1 << "' y='1' width='" << right - left - 2
                            << "' height='" << rowHeight - 2 << "' fill='var(--stage-main,#f2f2f2)'/>";
                        left = right;
                    }
                }
    const ui32 statisticsEnd = std::min(std::max(viz.Config.TimelineLeft, viz.Config.SummaryLeft + 285), viz.Config.Width * 3 / 4);
    const ui32 nameEnd = viz.Config.OperatorLeft;
    StepCell(svg, INTERNAL_WIDTH, nameEnd - INTERNAL_WIDTH, "Step");
    StepCell(svg, nameEnd, viz.Config.TaskLeft - nameEnd, "Tablets");
    StepCell(svg, viz.Config.TaskLeft, viz.Config.SummaryLeft - viz.Config.TaskLeft, "Calls");
    StepCell(svg, viz.Config.SummaryLeft, statisticsEnd - viz.Config.SummaryLeft, "Statistics");
    StepCell(svg, statisticsEnd, viz.Config.Width - statisticsEnd, "Details");
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
                        TString kind = StepText(step, "StepKind");
                        if (kind == "MemoryReserve" || kind == "Reserve") { kind = "ReserveMemory"; }
                        auto& group = groups[{kind, StepText(step, "Operation")}];
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
                svg << "<svg data-columnshard='scan' data-cs-plan-index='" << stage->ColumnShardPlanIndex.value()
                    << "' x='0' y='" << top + height << "' width='" << viz.Config.Width
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

    ui32 columnShardPlanIndex = 0;
    for (auto& plan : Plans) {
        for (const auto& stage : plan->Stages) {
            stage->ColumnShardPlanIndex = columnShardPlanIndex;
            stage->ColumnShardSvg.clear();
            stage->ColumnShardSvg << ColumnShardWidgetStyles();
            stage->ColumnShardHeight = PrintColumnShardStats(stage->ColumnShardSvg, *this, 0, stage.get());
            if (stage->ColumnShardHeight) {
                stage->ColumnShardHeight += PrintColumnShardStorage(stage->ColumnShardSvg, *this,
                    stage->ColumnShardHeight, stage.get());
            }
        }
        plan->PrepareSvg(MaxTime, timelineDelta, offsetY);
        ++columnShardPlanIndex;
    }

    for (auto& plan : Plans) {
        plan->PrintSvg(background, MaxTime, timelineDelta);
    }

    TStringBuilder storage;
    const ui32 storageHeight = 0;

    svg << "<svg width='" << Config.Width << "' height='" << offsetY + storageHeight << "' xmlns='http://www.w3.org/2000/svg'>" << Endl;
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

    if (storageHeight) {
        svg << "<svg data-cs-plan-area='true' width='" << Config.Width << "' height='" << offsetY << "' x='0' y='0'>" << Endl;
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

    if (storageHeight) {
        svg << "</svg>" << storage << Endl;
    }
    PrintColumnHeaders(svg, maxSec, deltaSec, x, w);

    svg << "</svg>" << Endl;

    return svg;
}

} // namespace NPlan2Svg
