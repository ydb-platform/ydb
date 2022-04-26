#include "group_layout_checker.h"
#include "group_geometry_info.h"

namespace NKikimr::NBsController {

    TLayoutCheckResult CheckGroupLayout(const TGroupGeometryInfo& geom, const THashMap<TVDiskIdShort, std::pair<TNodeLocation, TPDiskId>>& layout) {
        using namespace NLayoutChecker;

        if (layout.empty()) {
            return {};
        }

        TGroupLayout group(geom.GetNumFailRealms(), geom.GetNumFailDomainsPerFailRealm());
        TDomainMapper mapper;
        THashMap<TVDiskIdShort, TPDiskLayoutPosition> map;
        for (const auto& [vdiskId, p] : layout) {
            const auto& [location, pdiskId] = p;
            TPDiskLayoutPosition pos(mapper, location, pdiskId, geom);
            group.AddDisk(pos, vdiskId.FailRealm, vdiskId.FailDomain);
            map.emplace(vdiskId, pos);
        }

        std::vector<std::pair<TScore, TVDiskIdShort>> scoreboard;
        for (const auto& [vdiskId, pos] : map) {
            scoreboard.emplace_back(group.GetCandidateScore(pos, vdiskId.FailRealm, vdiskId.FailDomain), vdiskId);
        }

        auto comp1 = [](const auto& x, const auto& y) { return x.second < y.second; };
        std::sort(scoreboard.begin(), scoreboard.end(), comp1);
        for (const auto& [score, vdiskId] : scoreboard) {
            Cerr << vdiskId << "@" << map[vdiskId].ToString() << " -> " << score.ToString() << Endl;
        }

        auto comp = [](const auto& x, const auto& y) { return x.first.BetterThan(y.first); };
        std::sort(scoreboard.begin(), scoreboard.end(), comp);
        TLayoutCheckResult res;
        const auto reference = scoreboard.back().first;
        if (!reference.SameAs({})) { // not perfectly correct layout
            for (; !scoreboard.empty() && !scoreboard.back().first.BetterThan(reference); scoreboard.pop_back()) {
                Cerr << "candidate# " << scoreboard.back().second << Endl;
                res.Candidates.push_back(scoreboard.back().second);
            }
        }
        return res;
    }

} // NKikimr::NBsController

Y_DECLARE_OUT_SPEC(, NKikimr::NBsController::NLayoutChecker::TEntityId, stream, value) { value.Output(stream); }
