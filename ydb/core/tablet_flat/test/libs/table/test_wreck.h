#pragma once

#include "test_envs.h"
#include <ydb/core/tablet_flat/test/libs/rows/mass.h>

#include <util/random/mersenne.h>
#include <functional>

namespace NKikimr {
namespace NTable {
namespace NTest {

    enum class EWreck {
        Cached  = 0,    /* take all pages cached in env */
        Evicted = 1,    /* simulate random page loads   */
        Forward = 2,    /* use forward cache loads      */
    };

    template<typename TWrap, typename TEggs, EDirection Direction = EDirection::Forward>
    class TWreck {
    public:
        TWreck(const TMass &mass, ui64 seed = 666)
            : Mass(mass)
            , Rnd(seed)
        {

        }

        template<class... TArgs>
        void Do(EWreck cache, const TEggs &eggs, TArgs&&... args)
        {
            if (cache == EWreck::Cached) {
                auto make = []() { return new TTestEnv; };

                TWrap wrap(eggs, { make(), 0 /* no retries allowed */, false }, std::forward<TArgs>(args)...);

                DoPointReads(wrap), DoRangedScans(wrap, make, true);

            } else if (cache == EWreck::Evicted) {
                TWrap wrap(eggs, { nullptr, 66 /* try at most */, false }, std::forward<TArgs>(args)...);

                auto make = [this]() {
                    return new TFailEnv<decltype(Rnd)>(Rnd, 0.25);
                };

                wrap.template Displace<IPages>(make());

                DoPointReads(wrap), DoRangedScans(wrap, make, true);

            } else if (cache == EWreck::Forward) {
                Y_ABORT_UNLESS(Direction == EDirection::Forward, "ForwardEnv may only be used with forward iteration");

                TWrap wrap(eggs, { nullptr, 10 /* worst case: main, next, groups, blobs, plus b-tree index */, false }, std::forward<TArgs>(args)...);

                auto make = []() { return new TForwardEnv(512, 1024); };

                DoRangedScans(wrap, make, false /* only for scans */);
            }
        }

        void DoPointReads(TWrap &wrap)
        {
            { /*_ Before test check for existance of all saved rows */
                wrap.To(10);

                for (auto &row: Mass.Saved) wrap.Has(row);
            }

            { /*_ ... and absense of skipped keys in produced part */
                wrap.To(12);

                for (auto &row: Mass.Holes) wrap.NoKey(row);
            }

            /*_ ... and lower-upper bounds lookups on holes keys */
            for (size_t it = 0; it < 1024; it++) {
                wrap.To(10000 + it);

                auto &row = *Mass.Holes.Any(Rnd);

                if (auto *after = Mass.SnapBy(row, Direction == EDirection::Forward, true)) {
                    wrap.Seek(row, ESeek::Upper).Is(*after);
                    wrap.Seek(row, ESeek::Lower).Is(*after);
                } else {
                    wrap.Seek(row, ESeek::Upper).Is(EReady::Gone);
                    wrap.Seek(row, ESeek::Lower).Is(EReady::Gone);
                }
            }

            /*_ ... and lower-upper bounds lookups on saved keys */
            for (size_t it = 0; it < 1024; it++) {
                auto &row = *Mass.Saved.Any(Rnd);

                wrap.To(20000 + it).Seek(row, ESeek::Lower).Is(row);

                if (auto *after = Mass.SnapBy(row, Direction == EDirection::Forward, false)) {
                    wrap.Seek(row, ESeek::Upper).Is(*after);
                } else {
                    wrap.Seek(row, ESeek::Upper).Is(EReady::Gone);
                }
            }

            /*_ 3x_xxx Lookups of existing rows, saved collection */
            for (size_t it = 0; it < 8192; it++)
                wrap.To(30000 + it).Has(*Mass.Saved.Any(Rnd));

            /*_ 4x_xxx Lookups of absents rows, holes collection */
            for (size_t it = 0; it < 1024; it++)
                wrap.To(40000 + it).NoKey(*Mass.Holes.Any(Rnd));

            /*_ 5x_xxx +inf lookups logic. It could seem test has lack of it
                but inf logic completely implemented in key cmp functions and
                isn't related to iterators. Each key cmp function should have
                special tests for its +inf.
             */
        }

        void DoRangedScans(TWrap &wrap, std::function<IPages*()> make, bool rnd)
        {
            auto originEnv = wrap.template Displace<IPages>(make());

            { /*_ 60_000 Full sequence scan of all saved records */
                wrap.To(60000);

                // load index
                for (ui32 attempt = 0; attempt < 20; attempt++) {
                    wrap.Seek({}, ESeek::Lower);
                    if (wrap.GetReady() != EReady::Page) {
                        break;
                    }
                }

                if constexpr (Direction == EDirection::Reverse) {
                    auto it = Mass.Saved.end();
                    while (it != Mass.Saved.begin()) {
                        wrap.Is(*--it).Next();
                    }
                } else {
                    for (auto i : xrange(Mass.Saved.Size())) {
                        wrap.Is(Mass.Saved[i]).Next();
                    }
                }

                wrap.Is(EReady::Gone); /* should have no more rows */
            }

            /*_ 61_xxx Sequential reads with random starts   */

            if (rnd) wrap.template Displace<IPages>(make());

            for (size_t seq = 0; seq < 60; seq++) {
                if (!rnd) wrap.template Displace<IPages>(make());

                ui64 len = Rnd.Uniform(256, 999);
                auto it = Mass.Saved.Any(Rnd);

                wrap.To(61000 + seq);

                // load index
                for (ui32 attempt = 0; attempt < 20; attempt++) {
                    wrap.Seek(*it, ESeek::Lower);
                    if (wrap.GetReady() != EReady::Page) {
                        break;
                    }
                }

                if constexpr (Direction == EDirection::Reverse) {
                    while (len-- > 0) {
                        wrap.Is(*it).Next();
                        if (it == Mass.Saved.begin()) {
                            break;
                        }
                        --it;
                    }
                } else {
                    while (len-- > 0 && it != Mass.Saved.end())
                        wrap.Is(*it++).Next();
                }
            }

            wrap.template Displace<IPages>(originEnv);
        }

    private:
        const TMass &Mass;
        TMersenne<ui64> Rnd;
    };
}
}
}
