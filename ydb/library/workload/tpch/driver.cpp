#include "driver.h"
#include <library/cpp/resource/resource.h>
#include <util/generic/map.h>
#include <util/string/cast.h>

struct TDist {
    TVector<set_member> Members;
    long Max = 0;
};

extern "C" void ReadDistFromResource(const char* name, distribution* target) {
    static auto distributions = [] () {
        TMap<TStringBuf, TDist> result;
        static const auto resource = NResource::Find("dists.dss");
        TStringBuf in(resource);
        TStringBuf line;
        TDist* dist = nullptr;
        while (in.ReadLine(line)) {
            if (line.empty() || line.find('#') != TStringBuf::npos) {
                continue;
            }
            if (!dist) {
                constexpr TStringBuf prefix = "begin";
                if (to_lower(TString(line.substr(0, prefix.length()))) == prefix) {
                    const auto pos = line.find_first_of("\t ");
                    if (pos != TStringBuf::npos) {
                        dist = &result[line.substr(pos + 1)];
                    }
                }
            } else {
                constexpr TStringBuf prefix = "end";
                if (to_lower(TString(line.substr(0, prefix.length()))) == prefix) {
                    dist = nullptr;
                    continue;
                }
                TStringBuf token, weightStr;
                line.Split('|', token, weightStr);
                while(weightStr.SkipPrefix(" "));
                while(weightStr.ChopSuffix(" "));
                long weight = FromString(weightStr);
                if (to_lower(TString(token)) == "count") {
                    dist->Members.reserve(weight);
                } else {
                    dist->Max += weight;
                    dist->Members.emplace_back();
                    dist->Members.back().weight = dist->Max;
                    dist->Members.back().text = const_cast<char*>(token.data());
                    dist->Members.back().text[token.length()] = '\0';
                }
            }
        }
        return result;
    }();

    if (auto* dist = MapFindPtr(distributions, name)) {
        target->count = dist->Members.size();
        target->list = dist->Members.data();
        target->permute = nullptr;
        target->max = dist->Max;
    }
}
