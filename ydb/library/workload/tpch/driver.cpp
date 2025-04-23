#include "driver.h"
#include <library/cpp/resource/resource.h>
#include <util/generic/map.h>
#include <util/string/cast.h>

class TDistributions {
public:
    TDistributions() {
        const auto resource = NResource::Find("dists.dss");
        TStringInput in(resource);
        TString line;
        TDists::iterator dist = Dists.end();
        while (in.ReadLine(line)) {
            if (line.empty() || line.find('#') != TStringBuf::npos) {
                continue;
            }
            if (dist == Dists.end()) {
                constexpr TStringBuf prefix = "begin";
                if (to_lower(line.substr(0, prefix.length())) == prefix) {
                    const auto pos = line.find_first_of("\t ");
                    if (pos != TStringBuf::npos) {
                        const TString key(line.substr(pos + 1));
                        dist = Dists.emplace(key, TDist()).first;
                    }
                }
            } else {
                constexpr TStringBuf prefix = "end";
                if (to_lower(line.substr(0, prefix.length())) == prefix) {
                    dist = Dists.end();
                    continue;
                }
                TStringBuf token, weightStr;
                TStringBuf(line).Split('|', token, weightStr);
                while(weightStr.SkipPrefix(" "));
                while(weightStr.ChopSuffix(" "));
                long weight = FromString(weightStr);
                if (to_lower(TString(token)) == "count") {
                    dist->second.Members.reserve(weight);
                } else {
                    dist->second.Max += weight;
                    dist->second.Members.emplace_back();
                    Strings.emplace_back(token);
                    dist->second.Members.back().weight = dist->second.Max;
                    dist->second.Members.back().text = Strings.back().begin();
                }
            }
        }
    }

    void Fill(const char* name, distribution* target) {
        if (auto* dist = MapFindPtr(Dists, name)) {
            target->count = dist->Members.size();
            target->list = dist->Members.data();
            target->permute = nullptr;
            target->max = dist->Max;
        }
    }

private:
    struct TDist {
        TVector<set_member> Members;
        long Max = 0;
    };
    using TDists = TMap<TString, TDist>;
    TVector<TString> Strings;
    TDists Dists;
};

extern "C" void ReadDistFromResource(const char* name, distribution* target) {
    Singleton<TDistributions>()->Fill(name, target);
}
