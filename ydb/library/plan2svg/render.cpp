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

void TPlan::PrintUnfinishedTasks(TStringBuilder& builder, ui32 tasks, ui32 finishedTasks) {
    if (finishedTasks && finishedTasks <= tasks) {
        auto unfinishedPercent = 100 * (tasks - finishedTasks) / tasks;
        auto xx = Config.TaskLeft + Config.TaskWidth / 8;
        builder
        << "<line x1='" << xx << "' y1='" << unfinishedPercent << "%' x2='" << xx << "' y2='100%'"
        << " stroke-width='" << Config.TaskWidth / 4 << "' stroke='" << Config.Palette.StageText << "' stroke-dasharray='1,1' />" << Endl;
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

    SummaryBuilder
        << SvgTextS(Config.OperatorLeft + 2, titleHeight, "Rows")
        << SvgTextS(Config.SummaryLeft + 2, titleHeight, "Statistics")
        << SvgTextE(Config.TaskLeft + Config.TaskWidth - 2, titleHeight, "Tasks")
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
// explain the gaps in it. offsetY places the wait-output badge, which hangs off
// the bottom of the whole stage rather than off this strip.
void TPlan::PrintCpuStrip(const std::shared_ptr<TStage>& s, ui32& y0, ui64 px, ui64 pw, ui32 offsetY) {
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
                    PrintWarningBadge(s->Svg, Config.TaskLeft + Config.TaskWidth / 2,
                        s->OffsetY + offsetY + s->Height,
                        TStringBuilder() << "Wait input with peer stage(s) " << waitOutputPeers << " wait output", "W");
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
void TPlan::PrepareStageSvg(const std::shared_ptr<TStage>& s, ui64 maxTime, ui32 timelineDelta, ui32 offsetY) {
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

    PrintStageBackground(s);

    for (auto& region : s->HotRegions) {
        auto px = Config.TimelineLeft + region.first * (Config.TimelineWidth - timelineDelta) / maxTime;
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
    PrintCpuStrip(s, y0, px, pw, offsetY);

    if (s->Tasks) {
        s->Svg << "<g><title>";
        if (s->External) {
            s->Svg << "External Source, partitions: " << s->Tasks;
        } else {
            s->Svg << "Stage " << s->PhysicalStageId << ", tasks: " << s->Tasks;
        }
        s->Svg << ", finished: " << s->FinishedTasks << "</title>" << Endl;
        PrintUnfinishedTasks(s->Svg, s->Tasks, s->FinishedTasks);
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
    s->Svg << "</g>" << Endl;
}

void TPlan::PrepareSvg(ui64 maxTime, ui32 timelineDelta, ui32& offsetY) {

    OffsetY = offsetY;

    PrintPlanSummary(maxTime, timelineDelta, offsetY);

    for (auto& s : Stages) {
        PrepareStageSvg(s, maxTime, timelineDelta, offsetY);
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
            PrintUnfinishedTasks(builder, node->Tasks, node->FinishedTasks);
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

TString TVisualizer::PrintSvg() {
    // Reached only by mixing LoadPlansSafe with the throwing printer; report the
    // load failure rather than rendering the half-loaded plans behind it.
    if (LoadError) {
        ythrow yexception() << LoadError;
    }

    TStringBuilder background;
    TStringBuilder svg;

    ui32 offsetY = 0;
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

    for (ui64 t = 0; t <= maxSec; t += deltaSec) {
        ui64 x1 = t * w * 1000 / MaxTime;
        TString timeLabel = TStringBuilder()
            << "<g><title>" << TInstant::MilliSeconds(BaseTime + t * 1000) << "</title>" << Endl
            << SvgTextS(x + x1 + 2, INTERNAL_GAP_Y + (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2, Sprintf("%lu:%.2lu", t / 60, t % 60)) << Endl
            << "</g>" << Endl;
        for (auto& plan : Plans) {
            plan->SummaryBuilder << timeLabel;
        }
    }
    for (auto& plan : Plans) {
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
        << "  <rect x='" << Config.TimelineLeft + Config.TimelineWidth - summary3 << "' y='" << GAP_Y
        << "' width='" << summary3 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.StageTextHighlight << "'/>" << Endl
        << "  <text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextInverted << "' x='" << Config.TimelineLeft + Config.TimelineWidth - 2
        << "' y='" << GAP_Y + INTERNAL_TEXT_HEIGHT << "'>" << FormatTimeMs(UpdateTime) << "</text>" << Endl
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

    svg << "</svg>" << Endl;

    return svg;
}

} // namespace NPlan2Svg
