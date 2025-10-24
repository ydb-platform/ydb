#pragma once

#include <util/generic/string.h>

namespace NTestUtils {

struct TSolomonLocation {
    TString ProjectId; // Used only if IsCloud=false
    TString FolderId;  // Used as cluster ID if IsCloud=false
    TString Service;
    bool IsCloud = false;
};

void CleanupSolomon(const TSolomonLocation& location);

TString GetSolomonMetrics(const TSolomonLocation& location);

}  // namespace NTestUtils
