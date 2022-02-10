#pragma once 
 
#include "defs.h" 
 
namespace NKikimr { 
    namespace NHullComp { 
 
        //////////////////////////////////////////////////////////////////////////// 
        // NHullComp::TSstRatio 
        //////////////////////////////////////////////////////////////////////////// 
        struct TSstRatio : public TThrRefBase { 
            TInstant Time; 
            // items 
            ui64 IndexItemsTotal = 0; 
            ui64 IndexItemsKeep = 0; 
            // bytes 
            ui64 IndexBytesTotal = 0; 
            ui64 IndexBytesKeep = 0; 
            ui64 InplacedDataTotal = 0; 
            ui64 InplacedDataKeep = 0; 
            ui64 HugeDataTotal = 0; 
            ui64 HugeDataKeep = 0; 
 
            void Clear() { 
                // items 
                IndexItemsTotal = 0; 
                IndexItemsKeep = 0; 
                // bytes 
                IndexBytesTotal = 0; 
                IndexBytesKeep = 0; 
                InplacedDataTotal = 0; 
                InplacedDataKeep = 0; 
                HugeDataTotal = 0; 
                HugeDataKeep = 0; 
            } 
 
            TSstRatio() = default; 
            TSstRatio(TInstant time) 
                : Time(time) 
            {} 
 
            TSstRatio &operator +=(const TSstRatio &ratio) { 
                Time = Max(Time, ratio.Time); 
                IndexItemsTotal += ratio.IndexItemsTotal; 
                IndexItemsKeep += ratio.IndexItemsKeep; 
                IndexBytesTotal += ratio.IndexBytesTotal; 
                IndexBytesKeep += ratio.IndexBytesKeep; 
                InplacedDataTotal += ratio.InplacedDataTotal; 
                InplacedDataKeep += ratio.InplacedDataKeep; 
                HugeDataTotal += ratio.HugeDataTotal; 
                HugeDataKeep += ratio.HugeDataKeep; 
                return *this; 
            } 
 
            bool CanDeleteSst() const { 
                return IndexItemsKeep == 0; 
            } 
 
            void Output(IOutputStream &str) const {
                str << "{IndexItemsTotal: " << IndexItemsTotal 
                    << " IndexItemsKeep: " << IndexItemsKeep 
                    << " IndexBytesTotal: " << IndexBytesTotal 
                    << " IndexBytesKeep: " << IndexBytesKeep 
                    << " InplacedDataTotal: " << InplacedDataTotal 
                    << " InplacedDataKeep: " << InplacedDataKeep 
                    << " HugeDataTotal: " << HugeDataTotal 
                    << " HugeDataKeep: " << HugeDataKeep 
                    << "}"; 
            } 
 
            TString ToString() const {
                TStringStream str; 
                Output(str); 
                return str.Str(); 
            } 
 
            TString MonSummary() const {
                TStringStream str; 
                str << Fraction(IndexItemsKeep, IndexItemsTotal) << " / " 
                    << Fraction(IndexBytesKeep, IndexBytesTotal) << " / " 
                    << Fraction(InplacedDataKeep, InplacedDataTotal) << " / " 
                    << Fraction(HugeDataKeep, HugeDataTotal); 
                return str.Str(); 
            } 
 
            static const char *MonHeader() { 
                return "Idx% / IdxB% / InplB% / HugeB%"; 
            } 
 
            static TString Fraction(ui64 keep, ui64 total) {
                Y_VERIFY(keep <= total); 
                if (total == 0) 
                    return "NA"; 
                ui64 percent = keep * 100 / total; 
                if (percent) { 
                    return Sprintf("%" PRIu64, percent); 
                } 
                // percent == 0 
                ui64 promille = keep * 1000 / total; 
                if (promille) { 
                    return Sprintf("0.%" PRIu64, promille); 
                } 
                // promille == 0 
                return keep ? "0.1" : "0"; 
            } 
        }; 
 
        using TSstRatioPtr = TIntrusivePtr<TSstRatio>; 
 
 
        //////////////////////////////////////////////////////////////////////////// 
        // NHullComp::TSstRatioThreadSafeHolder 
        //////////////////////////////////////////////////////////////////////////// 
        class TSstRatioThreadSafeHolder { 
        public: 
            void Set(TSstRatioPtr ratio) { 
                TGuard<TSpinLock> g(Lock); 
                Ratio = ratio; 
            } 
 
            TSstRatioPtr Get() const { 
                TGuard<TSpinLock> g(Lock); 
                return Ratio; 
            } 
 
            TString MonSummary() const {
                TSstRatioPtr ratio = Get(); 
                return ratio ? ratio->MonSummary() : "UNK"; 
            } 
 
            TInstant GetTime() const { 
                TSstRatioPtr ratio = Get(); 
                return ratio ? ratio->Time : TInstant(); 
            } 
 
        private: 
            TSpinLock Lock; 
            TIntrusivePtr<TSstRatio> Ratio; 
        }; 
 
 
    } // NHullComp 
} // NKikimr 
