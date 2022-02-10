#pragma once

#include "histogram.h"
#include "auto_histogram.h"

#include <library/cpp/histogram/adaptive/protos/histo.pb.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <utility>

namespace NKiwiAggr {
    template <class TMyHistogram>
    class TMultiHistogram {
    private:
        static const size_t DEFAULT_INTERVALS = 100;

        typedef THashMap<ui64, IHistogramPtr> THistogramsMap;
        THistogramsMap Histograms;
        size_t Intervals;

    public:
        TMultiHistogram(size_t intervals = DEFAULT_INTERVALS)
            : Intervals(intervals)
        {
        }

        TMultiHistogram(const THistograms& histograms, size_t defaultIntervals = DEFAULT_INTERVALS)
            : Intervals(defaultIntervals)
        {
            FromProto(histograms);
        }

        virtual ~TMultiHistogram() {
        }

        void Clear() {
            Histograms.clear();
        }

        void Add(const THistoRecs& histoRecs) {
            for (size_t i = 0; i < histoRecs.HistoRecsSize(); ++i) {
                Add(histoRecs.GetHistoRecs(i).GetId(), histoRecs.GetHistoRecs(i).GetValue(), histoRecs.GetHistoRecs(i).GetWeight());
            }
        }

        void Add(const THistoRec& histoRec) {
            Add(histoRec.GetId(), histoRec.GetValue(), histoRec.GetWeight());
        }

        void Add(ui64 id, double value, double weight) {
            THistogramsMap::const_iterator it = Histograms.find(id);
            if (it == Histograms.end()) {
                it = Histograms.insert(std::make_pair(id, IHistogramPtr(new TMyHistogram(Intervals, id)))).first;
            }
            it->second->Add(value, weight);
        }

        void Multiply(double factor) {
            for (THistogramsMap::iterator it = Histograms.begin(); it != Histograms.end(); ++it) {
                it->second->Multiply(factor);
            }
        }

        TVector<ui64> GetIds() const {
            TVector<ui64> result(0);
            for (THistogramsMap::const_iterator it = Histograms.begin(); it != Histograms.end(); ++it) {
                result.push_back(it->first);
            }
            return result;
        }

        IHistogramPtr GetHistogram(ui64 id) const {
            THistogramsMap::const_iterator it = Histograms.find(id);
            if (it != Histograms.end()) {
                return it->second;
            }
            return IHistogramPtr();
        }

        double GetMaxHistoSum() const { 
            double sum = 0.0; 
            for (THistogramsMap::const_iterator it = Histograms.begin(); it != Histograms.end(); ++it) { 
                sum = std::max(sum, it->second->GetSum());
            } 
            return sum; 
        } 
 
        bool Empty() {
            for (THistogramsMap::iterator it = Histograms.begin(); it != Histograms.end(); ++it) {
                if (!it->second->Empty()) {
                    return false;
                }
            }
            return true;
        }

        virtual double OverallSum() {
            double sum = 0.0;
            for (THistogramsMap::iterator it = Histograms.begin(); it != Histograms.end(); ++it) {
                sum += it->second->GetSum();
            }
            return sum;
        }

        void FromProto(const THistograms& histograms) {
            for (size_t i = 0; i < histograms.HistoRecsSize(); ++i) {
                IHistogramPtr newHisto(new TMyHistogram(histograms.GetHistoRecs(i), Intervals));
                if (!newHisto->Empty()) {
                    Histograms[newHisto->GetId()] = newHisto;
                }
            }
        }

        void ToProto(THistograms& histograms) {
            histograms.Clear();
            for (THistogramsMap::iterator it = Histograms.begin(); it != Histograms.end(); ++it) {
                THistogram* histo = histograms.AddHistoRecs();
                it->second->ToProto(*histo);
            }
        }

        void PrecomputePartialSums() {
            for (auto& it : Histograms) {
                it.second->PrecomputePartialSums();
            }
        }
    };

    template <class TMerger, class TSomeMultiHistogram>
    static void MergeToMultiHistogram(const void* data, size_t size, TSomeMultiHistogram& multiHistogram, ui32 intervals = 300) {
        TMerger merger(intervals);
        merger.Add(data, size);
        THistograms histograms;
        merger.GetResult(histograms);
        multiHistogram.FromProto(histograms);
    }

    // Good for parsing from THistograms protobuf
    typedef TMultiHistogram<TAutoHistogram> TAutoMultiHistogram;
    typedef TAtomicSharedPtr<TAutoMultiHistogram> TAutoMultiHistogramPtr;

}
