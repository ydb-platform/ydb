#include "memory_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hp_timer.h>
#include <util/system/thread.h>

namespace NActors {
namespace NMemory {

Y_UNIT_TEST_SUITE(TMemoryTrackerTest) {

#if defined(ENABLE_MEMORY_TRACKING)

using namespace NPrivate;

size_t FindLabelIndex(const char* label) {
    auto indices = TMemoryTracker::Instance()->GetMetricIndices();
    auto it = indices.find(label);
    UNIT_ASSERT(it != indices.end());
    return it->second;
}


struct TTypeLabeled
    : public NActors::NMemory::TTrack<TTypeLabeled>
{
    char payload[16];
};

static constexpr char NamedLabel[] = "NamedLabel";

struct TNameLabeled
    : public NActors::NMemory::TTrack<TNameLabeled, NamedLabel>
{
    char payload[32];
};

#ifndef _win_
Y_UNIT_TEST(Gathering)
{
    TMemoryTracker::Instance()->Initialize();

    auto* typed = new TTypeLabeled;
    auto* typedArray = new TTypeLabeled[3];

    auto* named = new TNameLabeled;
    auto* namedArray = new TNameLabeled[5];
    NActors::NMemory::TLabel<NamedLabel>::Add(100);

    std::vector<TMetric> metrics;
    TMemoryTracker::Instance()->GatherMetrics(metrics);

    auto typeIndex = FindLabelIndex(TypeName<TTypeLabeled>().c_str());
    UNIT_ASSERT(typeIndex < metrics.size());
    UNIT_ASSERT(metrics[typeIndex].GetMemory() == sizeof(TTypeLabeled) * 4 + sizeof(size_t));
    UNIT_ASSERT(metrics[typeIndex].GetCount() == 2);

    auto nameIndex = FindLabelIndex(NamedLabel);
    UNIT_ASSERT(nameIndex < metrics.size());
    UNIT_ASSERT(metrics[nameIndex].GetMemory() == sizeof(TNameLabeled) * 6 + sizeof(size_t) + 100);
    UNIT_ASSERT(metrics[nameIndex].GetCount() == 3);

    NActors::NMemory::TLabel<NamedLabel>::Sub(100);
    delete [] namedArray;
    delete named;

    delete [] typedArray;
    delete typed;

    TMemoryTracker::Instance()->GatherMetrics(metrics);

    UNIT_ASSERT(metrics[typeIndex].GetMemory() == 0);
    UNIT_ASSERT(metrics[typeIndex].GetCount() == 0);

    UNIT_ASSERT(metrics[nameIndex].GetMemory() == 0);
    UNIT_ASSERT(metrics[nameIndex].GetCount() == 0);
}
#endif

static constexpr char InContainerLabel[] = "InContainerLabel";

struct TInContainer {
    char payload[16];
};

Y_UNIT_TEST(Containers) {
    TMemoryTracker::Instance()->Initialize();

    std::vector<TInContainer, NActors::NMemory::TAlloc<TInContainer>> vecT;
    vecT.resize(5);

    std::vector<TInContainer, NActors::NMemory::TAlloc<TInContainer, InContainerLabel>> vecN;
    vecN.resize(7);

    using TKey = int;

    std::map<TKey, TInContainer, std::less<TKey>,
        NActors::NMemory::TAlloc<std::pair<const TKey, TInContainer>>> mapT;
    mapT.emplace(0, TInContainer());
    mapT.emplace(1, TInContainer());

    std::map<TKey, TInContainer, std::less<TKey>,
        NActors::NMemory::TAlloc<std::pair<const TKey, TInContainer>, InContainerLabel>> mapN;
    mapN.emplace(0, TInContainer());

    std::unordered_map<TKey, TInContainer, std::hash<TKey>, std::equal_to<TKey>,
        NActors::NMemory::TAlloc<std::pair<const TKey, TInContainer>>> umapT;
    umapT.emplace(0, TInContainer());

    std::unordered_map<TKey, TInContainer, std::hash<TKey>, std::equal_to<TKey>,
        NActors::NMemory::TAlloc<std::pair<const TKey, TInContainer>, InContainerLabel>> umapN;
    umapN.emplace(0, TInContainer());
    umapN.emplace(1, TInContainer());

    std::vector<TMetric> metrics;
    TMemoryTracker::Instance()->GatherMetrics(metrics);

    auto indices = TMemoryTracker::Instance()->GetMetricIndices();
    for (auto& [name, index] : indices) {
        Cerr << "---- " << name
            << ": memory = " << metrics[index].GetMemory()
            << ", count = " << metrics[index].GetCount() << Endl;
    }

    auto vecTIndex = FindLabelIndex(TypeName<TInContainer>().c_str());
    UNIT_ASSERT(metrics[vecTIndex].GetMemory() >= ssize_t(sizeof(TInContainer) * 5));
    UNIT_ASSERT(metrics[vecTIndex].GetCount() == 1);

    auto labelIndex = FindLabelIndex(InContainerLabel);
    UNIT_ASSERT(metrics[labelIndex].GetCount() == 5);
    UNIT_ASSERT(metrics[labelIndex].GetMemory() >= ssize_t(
        sizeof(TInContainer) * 7 +
        sizeof(decltype(mapN)::value_type) +
        sizeof(decltype(umapN)::value_type) * 2));
}


static constexpr char InThreadLabel[] = "InThreadLabel";

struct TInThread
    : public NActors::NMemory::TTrack<TInThread, InThreadLabel>
{
    char payload[16];
};

void* ThreadProc(void*) {
    return new TInThread;
}

Y_UNIT_TEST(Threads) {
    TMemoryTracker::Instance()->Initialize();

    auto index = FindLabelIndex(InThreadLabel);

    auto* object1 = new TInThread;

    std::vector<TMetric> metrics;
    TMemoryTracker::Instance()->GatherMetrics(metrics);
    UNIT_ASSERT(metrics[index].GetMemory() == sizeof(TInThread));
    UNIT_ASSERT(metrics[index].GetCount() == 1);

    TThread thread(&ThreadProc, nullptr);
    thread.Start();
    auto* object2 = static_cast<TInThread*>(thread.Join());

    TMemoryTracker::Instance()->GatherMetrics(metrics);
    UNIT_ASSERT(metrics[index].GetMemory() == sizeof(TInThread) * 2);
    UNIT_ASSERT(metrics[index].GetCount() == 2);

    delete object2;

    TMemoryTracker::Instance()->GatherMetrics(metrics);
    UNIT_ASSERT(metrics[index].GetMemory() == sizeof(TInThread));
    UNIT_ASSERT(metrics[index].GetCount() == 1);

    delete object1;
}


struct TNotTracked {
    char payload[16];
};

struct TTracked
    : public NActors::NMemory::TTrack<TTracked>
{
    char payload[16];
};

template <typename T>
double MeasureAllocations() {
    constexpr size_t objectsCount = 1 << 20;

    std::vector<T*> objects;
    objects.resize(objectsCount);

    THPTimer timer;

    for (size_t i = 0; i < objectsCount; ++i) {
        objects[i] = new T;
    }

    for (size_t i = 0; i < objectsCount; ++i) {
        delete objects[i];
    }

    auto seconds = timer.Passed();
    Cerr << "---- objects: " << objectsCount << ", time: " << seconds << Endl;
    return seconds;
}

Y_UNIT_TEST(Performance) {
    TMemoryTracker::Instance()->Initialize();

    constexpr size_t Runs = 8;

    Cerr << "---- warmup" << Endl;
    MeasureAllocations<TNotTracked>();
    MeasureAllocations<TTracked>();

    std::vector<double> noTrack;
    std::vector<double> track;

    for (size_t run = 0; run < Runs; ++run) {
        Cerr << "---- no track" << Endl;
        auto time = MeasureAllocations<TNotTracked>();
        noTrack.push_back(time);

        Cerr << "---- track" << Endl;
        time = MeasureAllocations<TTracked>();
        track.push_back(time);
    }

    double meanNoTrack = 0, stddevNoTrack = 0;
    double meanTrack = 0, stddevTrack = 0;
    for (size_t i = 0; i < Runs; ++i) {
        meanNoTrack += noTrack[i];
        meanTrack += track[i];
    }
    meanNoTrack /= Runs;
    meanTrack /= Runs;

    auto sqr = [](double val) { return val * val; };

    for (size_t i = 0; i < Runs; ++i) {
        stddevNoTrack += sqr(noTrack[i] - meanNoTrack);
        stddevTrack += sqr(track[i] - meanTrack);
    }
    stddevNoTrack = sqrt(stddevNoTrack / (Runs - 1));
    stddevTrack = sqrt(stddevTrack / (Runs - 1));

    Cerr << "---- no track - mean: " << meanNoTrack << ", stddev: " << stddevNoTrack << Endl;
    Cerr << "---- track - mean: " << meanTrack << ", stddev: " << stddevTrack << Endl;
    Cerr << "---- tracking is slower by " << int((meanTrack / meanNoTrack - 1.0) * 100) << "%" << Endl;
}

#endif

}

}
}
