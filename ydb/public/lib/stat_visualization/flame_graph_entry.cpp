#include "flame_graph_entry.h"
#include "svg_script.h"

#include <ydb/public/lib/ydb_cli/common/common.h>
#include <library/cpp/json/json_reader.h>
#include <util/folder/path.h>
#include <util/generic/fwd.h>
#include <util/generic/utility.h>
#include <util/generic/strbuf.h>
#include <util/string/printf.h>

namespace NKikimr::NVisual {
namespace {
TMap<EFlameGraphType, TString> TypeName = {
        {CPU,          "CPU"},
        {TIME,         "TIME_MS"},
        {BYTES_OUTPUT, "OUT_B"},
        {TASKS,        "TASKS"}
};
}

void TPlanGraphEntry::SerializeToSvg(TOFStream &stageStream,TStringOutput &taskStream, double viewportHeight, EFlameGraphType type) const {
    auto baseParentWeight = Weights[type];

    SerializeToSvgImpl(stageStream, taskStream,
                       HORIZONTAL_OFFSET, viewportHeight - VERTICAL_OFFSET - RECT_HEIGHT,
                       static_cast<double>(baseParentWeight.Total), static_cast<double>(baseParentWeight.Total),
                       type, VIEWPORT_WIDTH - 2 * HORIZONTAL_OFFSET);
}

ui32 TPlanGraphEntry::CalculateDepth(ui32 curDepth) {
    ui32 depthStep = Tasks.empty() ? 1 : 2;

    ui32 maxDepth = curDepth + depthStep;
    for (auto &child: Children) {
        maxDepth = Max(child->CalculateDepth(curDepth + depthStep), maxDepth);
    }

    return maxDepth;
}

TCombinedWeights TPlanGraphEntry::CalculateWeight() {
    TCombinedWeights childrenWeights;
    for (auto &child: Children) {
        childrenWeights = childrenWeights + child->CalculateWeight();
    }

    Weights = Weights + childrenWeights;
    Weights.AddSelfToTotal();

    return Weights;
}

double
TPlanGraphEntry::SerializeToSvgImpl(TOFStream &stageStream, TStringOutput &taskStream, double xOffset, double yOffset,
                                    double parentWeight, double visibleWeight,
                                    EFlameGraphType type, double parentWidth) const {
    double width = parentWidth * (visibleWeight / parentWeight);
    auto weight = Weights[type];

    bool shouldShowTaskProfile = !Tasks.empty() && type != TASKS;
    double thisRectHeight = shouldShowTaskProfile ? 2 * RECT_HEIGHT : RECT_HEIGHT;

    double xChildOffset = xOffset;

    auto parentVisibleWeight = static_cast<double>(weight.Total);
    for (const auto &child: Children) {
        if (static_cast<double>(child->Weights[type].Total) / static_cast<double>(weight.Total) < 0.05) {
            parentVisibleWeight += static_cast<double>(weight.Total) * 0.05f;
        }
    }

    for (const auto &child: Children) {
        xChildOffset += child->SerializeToSvgImpl(stageStream, taskStream, xChildOffset,
                                                  yOffset - thisRectHeight - INTER_ELEMENT_OFFSET,
                                                  parentVisibleWeight, Max(static_cast<double>(weight.Total) * 0.05f,
                                                                           static_cast<double>(child->Weights[type].Total)),
                                                  type, width);
    }


    if (shouldShowTaskProfile) {
        SerializeTaskProfile(taskStream,
                             xOffset, yOffset - RECT_HEIGHT,
                             type, width);
    }
    SerializeStage(stageStream,
                   xOffset, yOffset,
                   type, weight, width);


    return width;
}

void TPlanGraphEntry::SerializeTaskProfile(TStringOutput &stream, double xOffset, double yOffset, EFlameGraphType type,
                                           double parentWidth) const {
    Y_ENSURE(type == CPU || type == BYTES_OUTPUT || type == TIME, "Unsupported task profile type");
    if (Tasks.empty()) {
        return;
    }
    double total = 0;
    TVector<double> widthOfElements;
    for (const auto &task: Tasks) {
        total += task.TaskStats.Value(type, 0);
    }

    const ui8 startColorOffset = 100;
    const ui8 endColorOffset = 160;

    int i = 0;
    ui8 colorOffset = startColorOffset;
    auto TasksByStat = Tasks;
    std::sort(TasksByStat.begin(), TasksByStat.end(), [&type](const TTaskInfo& a, const TTaskInfo& b)
    {
        return a.TaskStats.Value(type, 0) > b.TaskStats.Value(type, 0);
    });

    const double minVisibleWidth = 30.0;
    double additionalWidth = 0.0;
    for (const auto &task: TasksByStat) {
        double width;
        if (total == 0) {
            // Corner case, when metrics for all tasks are 0(can happen for MS metrics for example)
            width = parentWidth / TasksByStat.size();
        } else {
            width = (task.TaskStats.Value(type, 0) / total) * parentWidth;
        }
        // After zooming, object should be at least minVisibleWidth pixes wide, to be clickable
        auto minWidth = minVisibleWidth / VIEWPORT_WIDTH * parentWidth;
        if (width < minWidth) {
            additionalWidth += minWidth - width;
            width = minWidth;
        }
        widthOfElements.emplace_back(width);
    }

    for (auto &width: widthOfElements) {
        width *= parentWidth / (parentWidth + additionalWidth);
    }

    for (const auto &task: TasksByStat) {
        auto stepName = Sprintf("TaskId: %lu", task.TaskId);
        auto stepDescription = Sprintf("%s (%s %.0f)", stepName.c_str(),
                                       TypeName.Value(type, "").c_str(),
                                       task.TaskStats.Value(type, 0));
        stepName = CutTextForAvailableWidth(stepName, widthOfElements[i]);

        stream << Sprintf(FG_SVG_TASK_PROFILE_ELEMENT.data(),
                          TypeName.Value(type, "").c_str(),
                          StageId,
                          stepDescription.c_str(), // full text
                          TypeName.Value(type, "").c_str(),
                          task.TaskStats.Value(type, 0.0),
                          xOffset, yOffset, // position
                          widthOfElements[i], RECT_HEIGHT, // width and height
                          colorOffset,
                          xOffset + TEXT_SIDE_OFFSET, yOffset + TEXT_TOP_OFFSET, // Text position
                          stepName.c_str() // short text
        );
        xOffset += widthOfElements[i];
        i++;
        colorOffset = colorOffset == endColorOffset ? startColorOffset : endColorOffset;
    }
}

void TPlanGraphEntry::SerializeStage(TOFStream &stream, double xOffset, double yOffset, EFlameGraphType type,
                                     const TWeight &weight, double width) const {

    // Full description of step
    TString stepInfo = Sprintf("%s %s(self: %lu, total: %lu)",
                               Name.c_str(), TypeName.Value(type, "").c_str(), weight.Self, weight.Total);

    auto stepName = CutTextForAvailableWidth(Name, width);

    // Color falls more to red, if step takes more cpu, than it's children
    double selfToChildren = 0;
    if (weight.Total > weight.Self) {
        selfToChildren =
                1 -
                Min(static_cast<double>(weight.Self) / static_cast<double>(weight.Total - weight.Self),
                    1.0);
    }
    TString color = Sprintf("rgb(255, %ld, 0)", std::lround(selfToChildren * 255));

    stream << Sprintf(FG_SVG_GRAPH_ELEMENT.data(),
                      stepInfo.c_str(), // full text
                      TypeName.Value(type, "").c_str(),
                      StageId,
                      xOffset, yOffset, // position
                      width, RECT_HEIGHT, // width and height
                      color.c_str(), // element background color
                      xOffset + TEXT_SIDE_OFFSET, yOffset + TEXT_TOP_OFFSET, // Text position
                      stepName.c_str() // short text
    );
}

TMap<EFlameGraphType, TString> &TPlanGraphEntry::PlanGraphTypeName() {
    return TypeName;
}

TString TPlanGraphEntry::CutTextForAvailableWidth(const TString &text, double width) {
    // Step name(we have to manually cut it, according to available space
    // 7 is found empirically
    auto symbolsAvailable = std::lround((width - 2 * TEXT_SIDE_OFFSET) / 7);
    if (symbolsAvailable <= 2) {
        return "";
    } else if (static_cast<ui64>(symbolsAvailable) < text.length()) {
        return text.substr(0, symbolsAvailable - 2) + "..";
    } else {
        return text;
    }
}

void TPlanGraphEntry::AddChild(THolder<TPlanGraphEntry> &&child) {
    Children.emplace_back(std::move(child));
}
}

