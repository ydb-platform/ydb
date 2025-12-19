#include "histogram_borders.h"
#include <util/string/cast.h>

namespace NKikimr::NColumnShard {
const std::map<i64, TString> THistorgamBorders::BlobSizeBorders = {{0, "0"}, {512 * 1024, "512kb"}, {1024 * 1024, "1Mb"},
    {2 * 1024 * 1024, "2Mb"}, {4 * 1024 * 1024, "4Mb"},
    {5 * 1024 * 1024, "5Mb"}, {6 * 1024 * 1024, "6Mb"},
    {7 * 1024 * 1024, "7Mb"}, {8 * 1024 * 1024, "8Mb"}};

const std::map<i64, TString> THistorgamBorders::BytesBorders = [] {
    std::map<i64, TString> map;
    map[0] = "0";
    ui64 base = 1024;
    for (auto i = 0; i < 20; i++, base *= 2) {
        if (base >= 1024 * 1024) {
            map[base] = ToString(base / 1024 / 1024) + "Mb";
        }
        else {
            map[base] = ToString(base /  1024) + "Kb";
        }
    }
    return map;
}();

const std::map<i64, TString> THistorgamBorders::TimeBordersMicroseconds = [] {
    std::map<i64, TString> map;
    map[0] = "0";
    ui64 base = 1;
    for (auto i = 0; i < 9; i++, base *= 10) {
        if (base >= 1'000'000) {
            map[base] = ToString(base / 1'000'000) + "s";
        }
        else if (base >= 1'000) {
            map[base] = ToString(base / 1'000) + "ms";
        }
        else {
            map[base] = ToString(base) + "us";
        }
    }
    return map;
}();

const std::set<i64> THistorgamBorders::PortionRecordBorders = {0, 2500, 5000, 7500, 9000, 10000, 20000, 40000, 80000, 160000, 320000, 640000, 1024000};
}
