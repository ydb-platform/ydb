#pragma once

#include <util/generic/string.h>

namespace NKikimr::NVisual {

    enum EFlameGraphType {
        CPU,
        TIME,
        BYTES_OUTPUT,
        TASKS,
        ALL
    };
    /// Generates svg file with flame graph for current usage report
    void GenerateFlameGraphSvg(const TString& statReportJson, const TString& resultFile);
}
