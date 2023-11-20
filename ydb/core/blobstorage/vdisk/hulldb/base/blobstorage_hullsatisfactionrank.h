#pragma once

#include "defs.h"
#include "blobstorage_hulldefs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_dbtype.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/base/html.h>

#include <util/stream/format.h>

namespace NKikimr {

    namespace NPDisk {
        struct TEvConfigureSchedulerResult;
    } // NPDisk


    ///////////////////////////////////////////////////////////////////////////////////////
    // ESatisfactionRankType
    ///////////////////////////////////////////////////////////////////////////////////////
    enum class ESatisfactionRankType {
        Fresh,
        Level
    };

    namespace NSat {

        ////////////////////////////////////////////////////////////////////////////
        // TDecimalViaDouble
        // Simple reference implementation of rank decimal
        ////////////////////////////////////////////////////////////////////////////
        class TDecimalViaDouble {
        public:
            using TThis = TDecimalViaDouble;

            static TThis MkZero() noexcept {
                return TThis(0);
            }

            TDecimalViaDouble(double v) {
                Val = v;
            }
            TDecimalViaDouble(ui64 intPart, ui64 fractPart);

            TDecimalViaDouble(const TDecimalViaDouble &) = default;
            TDecimalViaDouble &operator=(const TDecimalViaDouble &) = default;

            bool operator < (const TThis &v) const { return Val < v.Val; }
            bool operator <=(const TThis &v) const { return Val <= v.Val; }
            bool operator > (const TThis &v) const { return Val > v.Val; }
            bool operator >=(const TThis &v) const { return Val >= v.Val; }
            bool operator ==(const TThis &v) const { return Val == v.Val; }
            bool operator !=(const TThis &v) const { return Val != v.Val; }

            TThis operator * (const TThis &v) const {
                return TThis(Val * v.Val);
            }

            TThis operator * (ui64 i) const {
                return TThis(Val * i);
            }

            ui64 RoundToInt(ui64 base, ui64 discreteness) const {
                ui64 v = ui64(Val);
                ui64 offs = (v - base) / discreteness * discreteness;
                ui64 c1 = base + offs;
                ui64 c2 = c1 + offs;
                if (v - c1 <= c2 - v)
                    return c1;
                else
                    return c2;
            }

            void Output(IOutputStream &str) const {
                str << Val;
            }

        private:
            double Val;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TDecimal
        ////////////////////////////////////////////////////////////////////////////
        template <ui64 NumbersAfterPoint = 3>
        class TDecimal {
        public:
            using TThis = TDecimal;

            // this method is actually error prone:
            // MkDecimal(1, 4) -> 1.004
            // MkDecimal(1, 400) -> 1.4
            // Be carefull!!!
            constexpr static TThis MkDecimal(ui64 intPart, ui64 fractPart) {
                return TThis(intPart * Base + fractPart);
            }

            constexpr static TThis MkRatio(ui64 one, ui64 toAnother) noexcept {
                return TThis(one * Base / toAnother);
            }

            constexpr static TThis MkZero() noexcept {
                return TThis(0);
            }

            TDecimal() : Val(0) {}
            TDecimal(const TThis &) = default;
            TDecimal &operator=(const TThis &) = default;

            bool operator < (const TThis &v) const { return Val < v.Val; }
            bool operator <=(const TThis &v) const { return Val <= v.Val; }
            bool operator > (const TThis &v) const { return Val > v.Val; }
            bool operator >=(const TThis &v) const { return Val >= v.Val; }
            bool operator ==(const TThis &v) const { return Val == v.Val; }
            bool operator !=(const TThis &v) const { return Val != v.Val; }

            TThis operator * (const TThis &v) const {
                return TThis(Val * v.Val / Base);
            }

            TThis operator + (const TThis &v) const {
                return TThis(Val + v.Val);
            }

            TThis operator * (ui64 i) const {
                return TThis(Val * i);
            }

            ui64 RoundToInt(ui64 base, ui64 discreteness) const {
                ui64 v = Val / Base;
                if (v < base)
                    return base;
                ui64 offs = (v - base) / discreteness * discreteness;
                ui64 c1 = base + offs;
                ui64 c2 = c1 + offs;
                if (v - c1 <= c2 - v)
                    return c1;
                else
                    return c2;
            }

            ui64 ToUi64() const {
                return Val / Base;
            }

            void Output(IOutputStream &str) const {
                ui64 intPart = Val / Base;
                ui64 fractPart = Val % Base;

                str << intPart << "." << LeftPad(fractPart, NumbersAfterPoint, '0');
            }

            ui64 GetRaw() const {
                return Val;
            }

        private:
            ui64 Val;

            static constexpr ui64 CalculateBase(ui64 n) {
                return n == 0 ? 1ull : 10ull * CalculateBase(n - 1);
            }

            static constexpr ui64 Base = CalculateBase(NumbersAfterPoint);

            explicit TDecimal(ui64 raw)
                : Val(raw)
            {}
        };

        template <ui64 NumbersAfterPoint>
        const ui64 TDecimal<NumbersAfterPoint>::Base;


        template <ui64 NumbersAfterPoint>
        TDecimal<NumbersAfterPoint> Max(TDecimal<NumbersAfterPoint> x, TDecimal<NumbersAfterPoint> y) {
            return x > y ? x : y;
        }


        ////////////////////////////////////////////////////////////////////////////
        // TLinearTrackBar
        // It manages weight depending linearly on TSatisfactionRank between
        // defWeight and maxWeight with selected discreteness
        ////////////////////////////////////////////////////////////////////////////
        template <class TDecimal>
        class TLinearTrackBar {
        public:
            // status of update
            struct TStatus {
                bool Changed = false;
                ui64 Weight = 0;

                TStatus() = default;
                TStatus(bool changed, ui64 weight)
                    : Changed(changed)
                    , Weight(weight)
                {}

                bool operator ==(const TStatus &s) const {
                    return Changed == s.Changed && Weight == s.Weight;
                }

                void Output(IOutputStream &str) const {
                    str << "[" << (Changed ? "true" : "false") << ", " << Weight << "]";
                }

                TString ToString() const {
                    TStringStream str;
                    Output(str);
                    return str.Str();
                }
            };

            TLinearTrackBar(ui64 defWeight,
                            ui64 maxWeight,
                            ui64 discreteness,
                            TDecimal yellowThreshold,
                            TDecimal redThreshold)
                : DefWeight(defWeight)
                , MaxWeight(maxWeight)
                , Discreteness(discreteness)
                , YellowThreshold(yellowThreshold)
                , RedThreshold(redThreshold)
                , CurWeight(defWeight)
                , CurRank(TDecimal::MkZero())
            {}

            TStatus Update(TDecimal rank) {
                CurRank = rank;
                ui64 newWeight = CalculateWeight(rank);
                bool changed = newWeight != CurWeight;
                CurWeight = newWeight;
                return TStatus(changed, CurWeight);
            }

            ui64 GetCurWeight() const {
                return CurWeight;
            }

            TDecimal GetCurRank() const {
                return CurRank;
            }

            void RenderHtml(IOutputStream &str) const {
                NKikimrWhiteboard::EFlag light;
                if (CurRank <= YellowThreshold)
                    light = NKikimrWhiteboard::EFlag::Green;
                else if (CurRank <= RedThreshold)
                    light = NKikimrWhiteboard::EFlag::Yellow;
                else
                    light = NKikimrWhiteboard::EFlag::Red;

                TStringStream s;
                s << "Rank: " << CurRank << " Weight: [" << DefWeight
                << "<=" << CurWeight << "<=" << MaxWeight << "]";

                THtmlLightSignalRenderer(light, s.Str()).Output(str);
            }

        public: // constants
            const ui64 DefWeight;
            const ui64 MaxWeight;
            const ui64 Discreteness;
            const TDecimal YellowThreshold;
            const TDecimal RedThreshold;

        private:
            // current saved weight
            ui64 CurWeight;
            TDecimal CurRank;

            ui64 CalculateWeight(TDecimal rank) const {
                if (rank <= YellowThreshold)
                    return DefWeight;

                TDecimal weight = rank * DefWeight;
                ui64 w = weight.RoundToInt(DefWeight, Discreteness);
                if (w > MaxWeight)
                    w = MaxWeight;
                return w;
            }
        };

    } // NSat

    ////////////////////////////////////////////////////////////////////////////
    // TSatisfactionRank
    ////////////////////////////////////////////////////////////////////////////
    class TSatisfactionRank {
    public:
        using TValue = NSat::TDecimal<3>;

        TSatisfactionRank() = default;
        TSatisfactionRank(const TSatisfactionRank &) = default;
        TSatisfactionRank &operator=(const TSatisfactionRank &) = default;

        explicit TSatisfactionRank(TValue val) noexcept
            : Val(val)
        {}

        static TSatisfactionRank MkRatio(ui64 one, ui64 toAnother) noexcept {
            return TSatisfactionRank(TValue::MkRatio(one, toAnother));
        }

        static TSatisfactionRank MkZero() noexcept {
            return TSatisfactionRank(TValue::MkZero());
        }

        friend TSatisfactionRank Worst(const TSatisfactionRank &r1, const TSatisfactionRank &r2);

        TValue GetValue() const {
            return Val;
        }

    private:
        TValue Val;
    };

    inline TSatisfactionRank Worst(const TSatisfactionRank &r1, const TSatisfactionRank &r2) {
        return r1.Val > r2.Val ? r1 : r2;
    }

    ////////////////////////////////////////////////////////////////////////////
    // TAllDbsRank
    ////////////////////////////////////////////////////////////////////////////
    class TAllDbsRank {
    public:
        using TDecimal = TSatisfactionRank::TValue;

        void Update(EHullDbType dbType, const TSatisfactionRank &rank) {
            Y_DEBUG_ABORT_UNLESS(dbType < EHullDbType::Max);
            CurRanks[ui32(dbType)] = rank.GetValue();
        }

        void ApplyUpdates() {
            const ui32 f = ui32(EHullDbType::First);
            const ui32 m = ui32(EHullDbType::Max);
            TDecimal r = CurRanks[f];
            for (ui32 t = f + 1; t < m; t++) {
                r = Max(r, CurRanks[t]);
            }
            Rank = r;
        }

        TDecimal GetRank() const {
            return Rank;
        }

    private:
        // current fresh rank value for every hull database
        std::array<TDecimal, ui32(EHullDbType::Max)> CurRanks;
        TDecimal Rank;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TDynamicPDiskWeightsManager
    ////////////////////////////////////////////////////////////////////////////
    // Workflow:
    // Update*, Update*, ..., Update*
    // ApplyUpdates
    // Feedback
    class TDynamicPDiskWeightsManager {
    public:
        // types
        using TDecimal = TSatisfactionRank::TValue;
        using TLinearTrackBar = ::NKikimr::NSat::TLinearTrackBar<TDecimal>;

        TDynamicPDiskWeightsManager(const TVDiskContextPtr &vctx, const TPDiskCtxPtr &pdiskCtx);
        void ApplyUpdates(const TActorContext &ctx);
        void Feedback(const NPDisk::TEvConfigureSchedulerResult &res, const TActorContext &ctx);
        void RenderHtml(IOutputStream &str) const;
        void ToWhiteboard(NKikimrWhiteboard::TVDiskSatisfactionRank &v) const;
        static void DefWhiteboard(NKikimrWhiteboard::TVDiskSatisfactionRank &v);

        void UpdateFreshSatisfactionRank(EHullDbType dbType, const TSatisfactionRank &rank) {
            FreshRank.Update(dbType, rank);
        }

        void UpdateLevelSatisfactionRank(EHullDbType dbType, const TSatisfactionRank &rank) {
            LevelRank.Update(dbType, rank);
        }

        bool StopPuts() const {
            return FreshRank.GetRank() > FreshRankRed;
        }

        bool AcceptWrites() const {
            TAtomicBase val = AtomicGet(AcceptWritesState);
            return val;
        }

        TDecimal GetFreshRank() const {
            return FreshRank.GetRank();
        }

        TDecimal GetLevelRank() const {
            return LevelRank.GetRank();
        }

    private:
        TVDiskContextPtr VCtx;
        TPDiskCtxPtr PDiskCtx;

        // fresh
        TAllDbsRank FreshRank;
        TLinearTrackBar FreshWeight;
        static const TDecimal FreshRankYellow;
        static const TDecimal FreshRankRed;
        static const ui64 FreshWeightDefault;
        // level
        TAllDbsRank LevelRank;
        TLinearTrackBar LevelWeight;
        static const TDecimal LevelRankYellow;
        static const TDecimal LevelRankRed;
        static const ui64 LevelWeightDefault;

        TAtomic AcceptWritesState = 0;
    };

} // NKikimr
