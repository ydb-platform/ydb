#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace {
    constexpr TStringBuf DEFAULT_COUNTER_NAME_STR = "name";

    TVector<TString> SplitPathForFuzzing(TString path) {
        TVector<TString> result;
        if (path.empty()) {
            return result;
        }

        size_t prevPos = 0;
        size_t pos = 0;
        size_t len = path.size();
        while (pos < len) {
            if (path[pos] == '/') {
                if (pos != prevPos) {
                    result.emplace_back(path.substr(prevPos, pos - prevPos));
                }
                ++pos;
                prevPos = pos;
            } else {
                ++pos;
            }
        }
        if (pos != prevPos) {
            result.emplace_back(path.substr(prevPos, pos - prevPos));
        }
        return result;
    }

    std::vector<std::pair<TString, TString>> ParseCounterPath(const TString& path) {
        const auto pathComponents = SplitPathForFuzzing(path);
        std::vector<std::pair<TString, TString>> ret(pathComponents.size());
        for (size_t i = 0; i < pathComponents.size(); ++i) {
            const size_t pos = pathComponents[i].find('=');
            if (pos == TString::npos) {
                ret[i].first = TString(DEFAULT_COUNTER_NAME_STR);
                ret[i].second = pathComponents[i];
            } else {
                ret[i].first = pathComponents[i].substr(0, pos);
                ret[i].second = pathComponents[i].substr(pos + 1);
            }
        }
        return ret;
    }
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 16 * 1024) {
        return 0;
    }

    const TString input(reinterpret_cast<const char*>(data), size);
    try {
        auto result = ParseCounterPath(input);
        (void)result;
    } catch (...) {
    }
    return 0;
}
