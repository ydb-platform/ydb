#include "tablet_counters.h"

////////////////////////////////////////////
namespace NKikimr {

////////////////////////////////////////////
/// The TTabletCountersBase class
////////////////////////////////////////////
TAutoPtr<TTabletCountersBase>
TTabletCountersBase::MakeDiffForAggr(const TTabletCountersBase& baseLine) const {
    TAutoPtr<TTabletCountersBase> retVal = new TTabletCountersBase(*this);
    if (baseLine.HasCounters()) {
        Y_DEBUG_ABORT_UNLESS(baseLine.SimpleCounters.Size() == SimpleCounters.Size());
        Y_DEBUG_ABORT_UNLESS(baseLine.CumulativeCounters.Size() == CumulativeCounters.Size());
        Y_DEBUG_ABORT_UNLESS(baseLine.PercentileCounters.Size() == PercentileCounters.Size());

        Y_DEBUG_ABORT_UNLESS(baseLine.SimpleCountersMetaInfo == SimpleCountersMetaInfo);
        Y_DEBUG_ABORT_UNLESS(baseLine.CumulativeCountersMetaInfo == CumulativeCountersMetaInfo);
        Y_DEBUG_ABORT_UNLESS(baseLine.PercentileCountersMetaInfo == PercentileCountersMetaInfo);

        retVal->SimpleCounters.AdjustToBaseLine(baseLine.SimpleCounters);
        retVal->CumulativeCounters.AdjustToBaseLine(baseLine.CumulativeCounters);
        retVal->PercentileCounters.AdjustToBaseLine(baseLine.PercentileCounters);
    }
    return retVal;
}

////////////////////////////////////////////
void TTabletCountersBase::RememberCurrentStateAsBaseline(/*out*/ TTabletCountersBase& baseLine) const {
    baseLine = *this;
}

////////////////////////////////////////////
// private
////////////////////////////////////////////
TTabletCountersBase::TTabletCountersBase(const TTabletCountersBase& rp)
    : TTabletCountersBase()
{
    *this = rp;
}

////////////////////////////////////////////
TTabletCountersBase&
TTabletCountersBase::operator = (const TTabletCountersBase& rp) {
    if (&rp == this)
        return *this;

    if (!HasCounters()) {
        SimpleCounters.Reset(rp.SimpleCounters);
        SimpleCountersMetaInfo = rp.SimpleCountersMetaInfo;

        CumulativeCounters.Reset(rp.CumulativeCounters);
        CumulativeCountersMetaInfo = rp.CumulativeCountersMetaInfo;

        PercentileCounters.Reset(rp.PercentileCounters);
        PercentileCountersMetaInfo = rp.PercentileCountersMetaInfo;
    } else {
        SimpleCounters.SetTo(rp.SimpleCounters);
        CumulativeCounters.SetTo(rp.CumulativeCounters);
        PercentileCounters.SetTo(rp.PercentileCounters);
    }

    return *this;
}

void TTabletSimpleCounterBase::OutputHtml(IOutputStream &os, const char* name) const {
    HTML(os) {PRE() {os << name << ": " << Value;}}
}

void TTabletPercentileCounter::OutputHtml(IOutputStream &os, const char* name) const {
    HTML(os) {
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") {TAG(TH4) {os << name;}}
        }

        DIV_CLASS("row") {
            for (auto i: xrange(Ranges.size())) {
                DIV_CLASS("col-md-3") {
                    PRE() {
                        os << Ranges[i].RangeName << ": " << Values[i];
                    }
                }
            }
        }
    }
}

void TTabletCountersBase::OutputHtml(IOutputStream &os) const {
    HTML(os) {
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") {OutputHtml(os, "Simple", SimpleCountersMetaInfo, "col-md-3", SimpleCounters);}
            DIV_CLASS("col-md-12") {OutputHtml(os, "Cumulative", CumulativeCountersMetaInfo, "col-md-3", CumulativeCounters);}
            DIV_CLASS("col-md-12") {OutputHtml(os, "Percentile", PercentileCountersMetaInfo, "col-md-12", PercentileCounters);}

        }
    }
}

////////////////////////////////////////////
template<typename T>
void TTabletCountersBase::OutputHtml(IOutputStream &os, const char* sectionName, const char* const* counterNames, const char* counterClass, const TCountersArray<T>& counters) const {
    HTML(os) {
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") {TAG(TH3) {os << sectionName; }}
        }
        DIV_CLASS("row") {
            for (ui32 i = 0, e = counters.Size(); i < e; ++i) {
                if (counterNames[i]) {
                    DIV_CLASS(counterClass) {counters[i].OutputHtml(os, counterNames[i]);}
                }
            }
        }
    }
}

void TTabletCountersBase::OutputProto(NKikimrTabletBase::TTabletCountersBase& op) const {
    if (HasCounters()) {
        for (ui32 idx = 0; idx < SimpleCounters.Size(); ++idx) {
            if (SimpleCounterName(idx)) {
                auto& counter = *op.AddSimpleCounters();
                counter.SetName(SimpleCounterName(idx));
                counter.SetValue(SimpleCounters[idx].Get());
            }
        }
        for (ui32 idx = 0; idx < CumulativeCounters.Size(); ++idx) {
            if (CumulativeCounterName(idx)) {
                auto& counter = *op.AddCumulativeCounters();
                counter.SetName(CumulativeCounterName(idx));
                counter.SetValue(CumulativeCounters[idx].Get());
            }
        }
        for (ui32 idx = 0; idx < PercentileCounters.Size(); ++idx) {
            if (PercentileCounterName(idx)) {
                auto& counter = *op.AddPercentileCounters();
                counter.SetName(PercentileCounterName(idx));
                const auto& percentileCounter = PercentileCounters[idx];
                for (ui32 idxRange = 0; idxRange < percentileCounter.GetRangeCount(); ++idxRange) {
                    counter.AddRanges(percentileCounter.GetRangeName(idxRange));
                    counter.AddValues(percentileCounter.GetRangeValue(idxRange));
                }
            }
        }
    }
}

////////////////////////////////////////////
/// The TTabletLabeledCountersBase class
////////////////////////////////////////////

////////////////////////////////////////////
// private
////////////////////////////////////////////
TTabletLabeledCountersBase::TTabletLabeledCountersBase(const TTabletLabeledCountersBase& rp)
    : TTabletLabeledCountersBase()
{
    *this = rp;
}

////////////////////////////////////////////
TTabletLabeledCountersBase&
TTabletLabeledCountersBase::operator = (const TTabletLabeledCountersBase& rp) {
    if (&rp == this)
        return *this;

    if (!HasCounters()) {
        Counters.Reset(rp.Counters);
        Ids.Reset(rp.Ids);
        MetaInfo = rp.MetaInfo;
        Types = rp.Types;
        GroupNames = rp.GroupNames;
        AggregateFunc = rp.AggregateFunc;
    } else {
        Counters.SetTo(rp.Counters);
        Ids.SetTo(rp.Ids);
    }
    Group = rp.Group;
    Drop = rp.Drop;
    return *this;
}

void TTabletLabeledCountersBase::OutputHtml(IOutputStream &os) const {
    HTML(os) {
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") {TAG(TH3) {os << Group; }}

        }
        DIV_CLASS("row") {
            for (ui32 i = 0, e = Counters.Size(); i < e; ++i) {
                if (MetaInfo[i]) {
                    DIV_CLASS("col-md-3") {Counters[i].OutputHtml(os, MetaInfo[i]);}
                    DIV_CLASS("col-md-3") {Ids[i].OutputHtml(os, "id");}
                }
            }
        }
    }
}

void TTabletLabeledCountersBase::AggregateWith(const TTabletLabeledCountersBase& rp) {
    if (!HasCounters()) {
        *this = rp;
        return;
    }
    if (rp.Counters.Size() != Counters.Size()) //do not merge different versions of counters; this can be on rolling update
        return;
    for (ui32 i = 0, e = Counters.Size(); i < e; ++i) {
        if (AggregateFunc[i] != rp.AggregateFunc[i]) //do not merge different versions of counters
            return;
        switch (AggregateFunc[i]) {
            case static_cast<ui8>(EAggregateFunc::EAF_MIN):
                if (Counters[i].Get() > rp.Counters[i].Get()) {
                    Counters[i].Set(rp.Counters[i].Get());
                    Ids[i].Set(rp.Ids[i].Get());
                }
                break;
            case static_cast<ui8>(EAggregateFunc::EAF_MAX):
                if (Counters[i].Get() < rp.Counters[i].Get()) {
                    Counters[i].Set(rp.Counters[i].Get());
                    Ids[i].Set(rp.Ids[i].Get());
                }
                break;
            case static_cast<ui8>(EAggregateFunc::EAF_SUM):
                Counters[i].Add(rp.GetCounters()[i].Get());
                Ids[i].Set(0);
                break;
            default:
                Y_ABORT("unknown aggregate func");
        }
    }
    Drop = Drop || rp.Drop;
}

} // end of NKikimr namespace

template<>
void Out<NKikimr::TTabletLabeledCountersBase::EAggregateFunc>(IOutputStream& out, NKikimr::TTabletLabeledCountersBase::EAggregateFunc func) {
    switch(func) {
        case NKikimr::TTabletLabeledCountersBase::EAggregateFunc::EAF_MIN:
            out << "EAF_MIN";
            break;
        case NKikimr::TTabletLabeledCountersBase::EAggregateFunc::EAF_MAX:
            out << "EAF_MAX";
            break;
        case NKikimr::TTabletLabeledCountersBase::EAggregateFunc::EAF_SUM:
            out << "EAF_SUM";
            break;
        default:
            out << (ui32)func;
    }
}
