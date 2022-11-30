#include "unistat.h"
#include <util/generic/strbuf.h>

using namespace NUnistat;

TIntervalsBuilder& TIntervalsBuilder::Append(double value) {
    Y_ENSURE(Intervals_.empty() || Intervals_.back() < value);
    Intervals_.push_back(value);
    return *this;
}

TIntervalsBuilder& TIntervalsBuilder::AppendFlat(size_t count, double step) {
    Y_ENSURE(Intervals_.size() > 0 && step > 0);
    double x = Intervals_.back();
    for (size_t i = 0; i < count; ++i) {
        x += step;
        Intervals_.push_back(x);
    }
    return *this;
}

TIntervalsBuilder& TIntervalsBuilder::AppendExp(size_t count, double base) {
    Y_ENSURE(Intervals_.size() > 0 && base > 1);
    double x = Intervals_.back();
    for (size_t i = 0; i < count; ++i) {
        x *= base;
        Intervals_.push_back(x);
    }
    return *this;
}

TIntervalsBuilder& TIntervalsBuilder::FlatRange(size_t count, double start, double stop) {
    Y_ENSURE(count > 0 && start < stop);
    Append(start);
    AppendFlat(count - 1, (stop - start) / count);
    return *this;
}

TIntervalsBuilder& TIntervalsBuilder::ExpRange(size_t count, double start, double stop) {
    Y_ENSURE(count > 0 && start < stop);
    Append(start);
    AppendExp(count - 1, std::pow(stop / start, 1. / count));
    return *this;
}

TIntervals TIntervalsBuilder::Build() {
    Y_ENSURE(Intervals_.size() <= MAX_HISTOGRAM_SIZE);
    Y_ENSURE(IsSorted(Intervals_.begin(), Intervals_.end()));
    return std::move(Intervals_);
}

IHolePtr TUnistat::DrillFloatHole(const TString& name, const TString& suffix, TPriority priority, TStartValue startValue, EAggregationType type, bool alwaysVisible) {
    return DrillFloatHole(name, "", suffix, priority, startValue, type, alwaysVisible);
}

IHolePtr TUnistat::DrillFloatHole(const TString& name, const TString& description, const TString& suffix, TPriority priority, TStartValue startValue, EAggregationType type, bool alwaysVisible) {
    {
        TReadGuard guard(Mutex);
        IHolePtr* exhole = Holes.FindPtr(name);
        if (exhole) {
            return *exhole;
        }
    }
    {
        TWriteGuard guard(Mutex);
        IHolePtr* exhole = Holes.FindPtr(name);
        if (exhole) {
            return *exhole;
        }
        IHolePtr hole = new TFloatHole(name, description, suffix, type, priority.Priority, startValue.StartValue, alwaysVisible);
        for (const auto& tag : GlobalTags) {
            hole->AddTag(tag.first, tag.second);
        }
        Holes[name] = hole;
        HolesByPriorityAndTags.insert(hole.Get());
        return hole;
    }
}

IHolePtr TUnistat::DrillHistogramHole(const TString& name, const TString& suffix, TPriority priority, const NUnistat::TIntervals& intervals, EAggregationType type, bool alwaysVisible) {
    return DrillHistogramHole(name, "", suffix, priority, intervals, type, alwaysVisible);
}

IHolePtr TUnistat::DrillHistogramHole(const TString& name, const TString& description, const TString& suffix, TPriority priority, const NUnistat::TIntervals& intervals, EAggregationType type, bool alwaysVisible) {
    {
        TReadGuard guard(Mutex);
        IHolePtr* exhole = Holes.FindPtr(name);
        if (exhole) {
            return *exhole;
        }
    }
    {
        TWriteGuard guard(Mutex);
        IHolePtr* exhole = Holes.FindPtr(name);
        if (exhole) {
            return *exhole;
        }
        IHolePtr hole = new THistogramHole(name, description, suffix, type, priority.Priority, intervals, alwaysVisible);
        for (const auto& tag : GlobalTags) {
            hole->AddTag(tag.first, tag.second);
        }
        Holes[name] = hole;
        HolesByPriorityAndTags.insert(hole.Get());
        return hole;
    }
}

void TUnistat::AddGlobalTag(const TString& tagName, const TString& tagValue) {
    TWriteGuard guard(Mutex);
    GlobalTags[tagName] = tagValue;
    for (IHole* hole : HolesByPriorityAndTags) {
        hole->AddTag(tagName, tagValue);
    }
}

bool TUnistat::PushSignalUnsafeImpl(const TStringBuf holename, double signal) {
    IHolePtr* hole = Holes.FindPtr(holename);
    if (!hole) {
        return false;
    }

    (*hole)->PushSignal(signal);
    return true;
}

TMaybe<TInstanceStats::TMetric> TUnistat::GetSignalValueUnsafeImpl(const TStringBuf holename) {
    IHolePtr* hole = Holes.FindPtr(holename);
    if (!hole) {
        return Nothing();
    }

    TInstanceStats stat;
    (*hole)->ExportToProto(stat);
    if (!stat.GetMetric().size()) {
        return Nothing();
    }
    return std::move(*stat.MutableMetric(0));
}

bool TUnistat::ResetSignalUnsafeImpl(const TStringBuf holename) {
    IHolePtr* hole = Holes.FindPtr(holename);
    if (!hole) {
        return false;
    }

    (*hole)->ResetSignal();
    return true;
}

TString TUnistat::CreateInfoDump(int level) const {
    TReadGuard guard(Mutex);
    NJsonWriter::TBuf json;
    json.BeginObject();
    for (IHole* hole : HolesByPriorityAndTags) {
        if (hole->GetPriority() < level) {
            break;
        }
        hole->PrintInfo(json);
    }
    json.EndObject();
    return json.Str();
}

TString TUnistat::CreateJsonDump(int level, bool allHoles) const {
    TReadGuard guard(Mutex);
    NJsonWriter::TBuf json;
    json.BeginList();
    bool filterZeroHoles = !allHoles && level >= 0;
    for (IHole* hole : HolesByPriorityAndTags) {
        if (hole->GetPriority() < level) {
            break;
        }
        hole->PrintValue(json, filterZeroHoles);
    }
    json.EndList();
    return json.Str();
}

TString TUnistat::CreatePushDump(int level, const NUnistat::TTags& tags, ui32 ttl, bool allHoles) const {
    TReadGuard guard(Mutex);
    NJsonWriter::TBuf json;
    json.BeginList();
    bool filterZeroHoles = !allHoles && level >= 0;
    int prevPriority = Max<int>();
    TString prevTagsStr;
    for (IHole* hole : HolesByPriorityAndTags) {
        int priority = hole->GetPriority();
        TString tagStr = hole->GetTagsStr();
        if (priority < level) {
            break;
        }
        if (prevPriority != priority || prevTagsStr != tagStr) {
            if (prevPriority != Max<int>()) {
                json.EndList();
                json.EndObject();
            }
            json.BeginObject();
            if (ttl) {
                json.WriteKey("ttl").WriteULongLong(ttl);
            }
            json.WriteKey("tags").BeginObject()
                .WriteKey("_priority").WriteInt(hole->GetPriority());
            for (const auto& tag : tags) {
                json.WriteKey(tag.first);
                json.WriteString(tag.second);
            }
            for (const auto& tag : hole->GetTags()) {
                json.WriteKey(tag.first);
                json.WriteString(tag.second);
            }
            json.EndObject();
            json.WriteKey("values").BeginList();
            prevPriority = priority;
            prevTagsStr = tagStr;
        }
        hole->PrintToPush(json, filterZeroHoles);
    }
    if (prevPriority != Max<int>()) {
        json.EndList();
        json.EndObject();
    }
    json.EndList();
    return json.Str();
}

void TUnistat::ExportToProto(TInstanceStats& stats, int level) const {
    TReadGuard guard(Mutex);

    for (IHole* hole : HolesByPriorityAndTags) {
        if (hole->GetPriority() < level) {
            break;
        }
        hole->ExportToProto(stats);
    }
}

TString TUnistat::GetSignalDescriptions() const {
    TReadGuard guard(Mutex);
    NJsonWriter::TBuf json;
    json.BeginObject();

    for (IHole* hole : HolesByPriorityAndTags) {
        json.WriteKey(hole->GetName());
        json.WriteString(hole->GetDescription());
    }

    json.EndObject();
    return json.Str();
}

TVector<TString> TUnistat::GetHolenames() const {
    TReadGuard guard(Mutex);

    TVector<TString> holenames;
    for (const auto& [holeName, holeData] : Holes) {
        holenames.emplace_back(holeName);
    }

    return holenames;
}

bool TUnistat::EraseHole(const TString& name) {
    TWriteGuard guard(Mutex);
    auto holeIterator = Holes.find(name);
    if (holeIterator == Holes.end()) {
        return false;
    }

    HolesByPriorityAndTags.erase(holeIterator->second.Get());
    Holes.erase(holeIterator);
    return true;
}

void TFloatHole::PrintInfo(NJsonWriter::TBuf& json) const {
    TValue value;
    value.Atomic = AtomicGet(Value.Atomic);
    json.WriteKey(Name)
        .BeginObject()
        .WriteKey(TStringBuf("Priority"))
        .WriteInt(Priority)
        .WriteKey(TStringBuf("Value"))
        .WriteDouble(value.Value)
        .WriteKey(TStringBuf("Type"))
        .WriteString(ToString(Type))
        .WriteKey(TStringBuf("Suffix"))
        .WriteString(Suffix)
        .WriteKey(TStringBuf("Tags"))
        .WriteString(GetTagsStr())
        .EndObject();
}

void TFloatHole::PrintValue(NJsonWriter::TBuf& json, bool check) const {
    if (check && !AtomicGet(Pushed)) {
        return;
    }

    TValue value;
    value.Atomic = AtomicGet(Value.Atomic);
    json.BeginList()
        .WriteString(TString::Join(GetTagsStr(), Name, TStringBuf("_"), Suffix))
        .WriteDouble(value.Value)
        .EndList();
}

void TFloatHole::PrintToPush(NJsonWriter::TBuf& json, bool check) const {
    if (check && !AtomicGet(Pushed)) {
        return;
    }

    TValue value;
    value.Atomic = AtomicGet(Value.Atomic);
    json.BeginObject()
        .WriteKey("name").WriteString(TString::Join(Name, TStringBuf("_"), Suffix))
        .WriteKey("val").WriteDouble(value.Value)
        .EndObject();
}


void TFloatHole::ExportToProto(TInstanceStats& stats) const {
    if (!AtomicGet(Pushed)) {
        return;
    }

    TValue value;
    value.Atomic = AtomicGet(Value.Atomic);
    TInstanceStats::TMetric* metric = stats.AddMetric();
    metric->SetName(TString::Join(GetTagsStr(), Name, TStringBuf("_"), Suffix));
    metric->SetNumber(value.Value);
}

void TFloatHole::PushSignal(double signal) {
    AtomicSet(Pushed, 1);
    TValue old, toset;
    do {
        old.Atomic = AtomicGet(Value.Atomic);
        switch (Type) {
            case EAggregationType::Max:
                toset.Value = Max(signal, old.Value);
                break;
            case EAggregationType::Min:
                toset.Value = Min(signal, old.Value);
                break;
            case EAggregationType::Sum:
                toset.Value = signal + old.Value;
                break;
            case EAggregationType::LastValue:
                toset.Value = signal;
                break;
            default:
                assert(0);
        }
    } while (!AtomicCas(&Value.Atomic, toset.Atomic, old.Atomic));
}

void TFloatHole::ResetSignal() {
    AtomicSet(Value.Atomic, 0);
    AtomicSet(Pushed, 0);
}

void THistogramHole::PrintInfo(NJsonWriter::TBuf& json) const {
    json.WriteKey(Name)
        .BeginObject()
        .WriteKey(TStringBuf("Priority"))
        .WriteInt(Priority)
        .WriteKey(TStringBuf("Value"));

    PrintWeights(json);

    json.WriteKey(TStringBuf("Type"))
        .WriteString(ToString(Type));
    json.WriteKey(TStringBuf("Suffix"))
        .WriteString(Suffix);
    json.WriteKey(TStringBuf("Tags"))
        .WriteString(GetTagsStr())
        .EndObject();
}

void THistogramHole::PrintValue(NJsonWriter::TBuf& json, bool check) const {
    if (check && !AtomicGet(Pushed)) {
        return;
    }

    json.BeginList()
        .WriteString(TString::Join(GetTagsStr(), Name, TStringBuf("_"), Suffix));

    PrintWeights(json);

    json.EndList();
}

void THistogramHole::PrintToPush(NJsonWriter::TBuf& json, bool check) const {
    if (check && !AtomicGet(Pushed)) {
        return;
    }

    json.BeginObject()
        .WriteKey("name").WriteString(TString::Join(Name, TStringBuf("_"), Suffix))
        .WriteKey("val");
    PrintWeights(json);
    json.EndObject();
}

void THistogramHole::PrintWeights(NJsonWriter::TBuf& json) const {
    json.BeginList();
    for (size_t i = 0, size = Weights.size(); i < size; ++i) {
        json.BeginList()
            .WriteDouble(Intervals[i])
            .WriteLongLong(AtomicGet(Weights[i]))
            .EndList();
    }
    json.EndList();
}

void THistogramHole::ExportToProto(TInstanceStats& stats) const {
    if (!AtomicGet(Pushed)) {
        return;
    }

    TInstanceStats::TMetric* metric = stats.AddMetric();
    metric->SetName(TString::Join(GetTagsStr(), Name, TStringBuf("_"), Suffix));

    TInstanceStats::THistogram* hgram = metric->MutableHgram();
    for (size_t i = 0, size = Weights.size(); i < size; ++i) {
        TInstanceStats::THistogram::TBucket* bucket = hgram->AddBucket();
        bucket->SetBoundary(Intervals[i]);
        bucket->SetWeight(AtomicGet(Weights[i]));
    }
}

void THistogramHole::PushSignal(double signal) {
    AtomicSet(Pushed, 1);

    const size_t i = UpperBound(Intervals.cbegin(), Intervals.cend(), signal) - Intervals.cbegin(); // Intervals[i - 1] <= signal < Intervals[i]
    if (i > 0) {
        AtomicIncrement(Weights[i - 1]);
    }
}

void THistogramHole::ResetSignal() {
    for (size_t i = 0; i < Intervals.size(); ++i) {
        SetWeight(i, 0);
    }
    AtomicSet(Pushed, 0);
}

void THistogramHole::SetWeight(ui32 index, TAtomicBase value) {
    AtomicSet(Weights.at(index), value);
}

void TUnistat::Reset() {
    TWriteGuard guard(Mutex);
    Holes.clear();
    HolesByPriorityAndTags.clear();
    GlobalTags.clear();
}

void TUnistat::ResetSignals() {
    for (auto& hole : Holes) {
        hole.second->ResetSignal();
    }
}
