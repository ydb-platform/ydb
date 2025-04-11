#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

#include <map>
#include <memory>
#include <set>

namespace NKikimr::NOlap::NReader::NSimple {

class TIntervalSet {
private:
    using TPosition = ui32;
    using TIntervalId = ui64;

private:
    class TBorder {
    private:
        YDB_READONLY_DEF(bool, IsEnd);
        YDB_READONLY_DEF(TPosition, Position);
        YDB_READONLY_DEF(TIntervalId, Interval);

        TBorder(const TPosition position, const TIntervalId interval, const bool isEnd)
            : IsEnd(isEnd)
            , Position(position)
            , Interval(interval) {
        }

    public:
        static TBorder Begin(const TPosition position, const TIntervalId interval) {
            return TBorder(position, interval, false);
        }
        static TBorder End(const TPosition position, const TIntervalId interval) {
            return TBorder(position, interval, true);
        }

        std::partial_ordering operator<=>(const TBorder& other) const {
            return std::tie(IsEnd, Position, Interval) <=> std::tie(other.IsEnd, Position, other.Interval);
        }
    };

public:
    class TCursor;

private:
    using TIntervalsByPosition = std::map<TPosition, THashSet<TIntervalId>>;
    std::set<TBorder> Borders;
    std::set<std::weak_ptr<TCursor>, std::owner_less<std::weak_ptr<TCursor>>> Cursors;
    TIntervalsByPosition IntervalsByLeft;
    TIntervalsByPosition IntervalsByRight;

public:
    class TCursor {
    private:
        using TIdsByIdx = THashMap<ui32, std::vector<ui64>>;

        const TIntervalSet* Container;
        YDB_READONLY(ui32, IntervalIdx, 0);
        YDB_READONLY_DEF(THashSet<ui64>, IntersectingSources);

        void NextImpl(const bool init) {
            if (!init) {
                if (const auto findIntervals = Container->IntervalsByRight.find(IntervalIdx);
                    findIntervals != Container->IntervalsByRight.end()) {
                    for (const TIntervalId source : findIntervals->second) {
                        AFL_VERIFY(IntersectingSources.erase(source));
                    }
                }

                ++IntervalIdx;
            }

            if (const auto findIntervals = Container->IntervalsByLeft.find(IntervalIdx); findIntervals != Container->IntervalsByLeft.end()) {
                for (const TIntervalId source : findIntervals->second) {
                    AFL_VERIFY(IntersectingSources.emplace(source).second);
                }
            }
        }

        friend TIntervalSet;
        TCursor(const TIntervalSet* container)
            : Container(container) {
            AFL_VERIFY(Container);
            NextImpl(true);
        }

    public:
        void Next() {
            NextImpl(false);
        }

        void Prev() {
            AFL_VERIFY(IntervalIdx);
            if (const auto findIntervals = Container->IntervalsByLeft.find(IntervalIdx); findIntervals != Container->IntervalsByLeft.end()) {
                for (const TIntervalId source : findIntervals->second) {
                    AFL_VERIFY(IntersectingSources.erase(source));
                }
            }
            --IntervalIdx;
            if (const auto findIntervals = Container->IntervalsByRight.find(IntervalIdx); findIntervals != Container->IntervalsByRight.end()) {
                for (const TIntervalId source : findIntervals->second) {
                    AFL_VERIFY(IntersectingSources.emplace(source).second);
                }
            }
        }

        void Move(const ui64 intervalIdx) {
            while (IntervalIdx < intervalIdx) {
                Next();
            }
            while (IntervalIdx > intervalIdx) {
                Prev();
            }
        }
    };

    template <typename F>
    void UpdateCursors(const F& update) {
        for (auto it = Cursors.begin(); it != Cursors.end();) {
            if (auto locked = it->lock()) {
                update(locked);
                ++it;
            } else {
                it = Cursors.erase(it);
            }
        }
    }

public:
    void Insert(const TPosition l, const TPosition r, const TIntervalId id) {
        AFL_VERIFY(Borders.insert(TBorder::Begin(l, id)).second);
        AFL_VERIFY(Borders.insert(TBorder::End(r, id)).second);

        AFL_VERIFY(IntervalsByLeft[l].emplace(id).second);
        AFL_VERIFY(IntervalsByRight[r].emplace(id).second);

        const auto addInterval = [l, r, id](const std::shared_ptr<TCursor>& cursor) {
            if (cursor->IntervalIdx >= l && cursor->IntervalIdx <= r) {
                AFL_VERIFY(cursor->IntersectingSources.emplace(id).second);
            }
        };
        UpdateCursors(addInterval);
    }

    void Erase(const TPosition l, const TPosition r, const TIntervalId id) {
        AFL_VERIFY(Borders.erase(TBorder::Begin(l, id)));
        AFL_VERIFY(Borders.erase(TBorder::End(r, id)));

        {
            auto findInterval = IntervalsByLeft.find(l);
            AFL_VERIFY(findInterval != IntervalsByLeft.end());
            AFL_VERIFY(findInterval->second.erase(id));
            if (findInterval->second.empty()) {
                IntervalsByLeft.erase(findInterval);
            }
        }
        {
            auto findInterval = IntervalsByRight.find(r);
            AFL_VERIFY(findInterval != IntervalsByRight.end());
            AFL_VERIFY(findInterval->second.erase(id));
            if (findInterval->second.empty()) {
                IntervalsByRight.erase(findInterval);
            }
        }

        const auto eraseInterval = [l, r, id](const std::shared_ptr<TCursor>& cursor) {
            if (cursor->IntervalIdx >= l && cursor->IntervalIdx <= r) {
                AFL_VERIFY(cursor->IntersectingSources.erase(id));
            }
        };
        UpdateCursors(eraseInterval);
    }

    THashSet<ui64> GetIntersections(const ui32 l, const ui32 r, const std::shared_ptr<TCursor>& cursor) const {
        AFL_VERIFY(cursor->GetIntervalIdx() >= l && cursor->GetIntervalIdx() <= r);
        THashSet<ui64> intervals = cursor->GetIntersectingSources();
        for (auto it = Borders.lower_bound(TBorder::Begin(l, std::numeric_limits<TIntervalId>::min()));
             it != Borders.end() && *it <= TBorder::End(r, std::numeric_limits<TIntervalId>::max()); ++it) {
            intervals.insert(it->GetInterval());
        }
        return intervals;
    }

    std::shared_ptr<TCursor> MakeCursor() {
        std::shared_ptr<TCursor> cursor = std::make_shared<TCursor>(TCursor(this));
        AFL_VERIFY(Cursors.emplace(cursor).second);
        return cursor;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
