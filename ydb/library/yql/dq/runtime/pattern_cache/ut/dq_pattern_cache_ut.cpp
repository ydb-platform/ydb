#include <ydb/library/yql/dq/runtime/pattern_cache/dq_pattern_cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

namespace {

class TMockComputationPattern final: public IComputationPattern {
public:
    explicit TMockComputationPattern(size_t codeSize)
        : Size_(codeSize)
    {
    }

    void Compile(TString, IStatsRegistry*) override {
        // A mock built with a zero code size stands for a pattern that does not fit into the codegen limits.
        Status_ = Size_ ? ECompileStatus::Compiled : ECompileStatus::RejectedBySize;
    }
    ECompileStatus GetCompileStatus() const override {
        return Status_;
    }
    size_t CompiledCodeSize() const override {
        return Size_;
    }
    void RemoveCompiledCode() override {
        if (Status_ != ECompileStatus::Compiled) {
            return;
        }
        Status_ = ECompileStatus::NoCompilationStarted;
    }
    THolder<IComputationGraph> Clone(const TComputationOptsFull&) override {
        return {};
    }
    bool GetSuitableForCache() const override {
        return true;
    }

private:
    const size_t Size_;
    ECompileStatus Status_ = ECompileStatus::NoCompilationStarted;
};

TPatternCacheEntryPtr MakeMockEntry(size_t codeSize = 1, size_t payloadBytes = 0) {
    auto entry = std::make_shared<TPatternCacheEntry>();
    entry->Pattern = MakeIntrusive<TMockComputationPattern>(codeSize);

    if (payloadBytes) {
        with_lock (entry->Alloc) {
            entry->Env.AllocateBuffer(payloadBytes);
        }
        // There is no way to tell whether the allocator takes the free pages of the global pool or maps new ones, so
        // release the free pages to keep the size of the entry stable.
        entry->Alloc.ReleaseFreePages();
    }

    return entry;
}

TProgramKey MakeKey(const TString& program) {
    return TProgramKey{NYql::UnknownLangVersion, {}, program};
}

NYql::TRuntimeSettingsStableHash MakeStableHash(ui8 fill) {
    NYql::TRuntimeSettingsStableHash hash;
    constexpr size_t arbitraryLength = 12;
    hash->resize(arbitraryLength, fill);
    return hash;
}

size_t TakePatternsToCompile(TComputationPatternCache& cache) {
    THashMap<TProgramKey, TPatternCacheEntryPtr> toCompile;
    cache.GetPatternsToCompile(toCompile);
    return toCompile.size();
}

} // namespace

Y_UNIT_TEST_SUITE(DqComputationPatternCache) {

Y_UNIT_TEST(Smoke) {
    constexpr size_t cacheItems = 10;
    TComputationPatternCache cache({10'000'000, 10'000'000});

    TVector<TPatternCacheEntryPtr> entries;
    for (size_t i = 0; i < cacheItems; ++i) {
        entries.push_back(MakeMockEntry());
        cache.EmplacePattern(MakeKey(ToString(i)), entries.back());
    }

    UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), cacheItems);
    for (size_t i = 0; i < cacheItems; ++i) {
        UNIT_ASSERT_EQUAL(cache.Find(MakeKey(ToString(i))), entries[i]);
    }
}

Y_UNIT_TEST(DoubleNotifyPatternCompiled) {
    const TProgramKey key = MakeKey("program");
    const ui32 cacheSize = 2;
    TComputationPatternCache cache({cacheSize, cacheSize});

    auto entry = MakeMockEntry(/*codeSize=*/1);
    cache.EmplacePattern(key, entry);

    for (ui32 i = 0; i < cacheSize + 1; ++i) {
        entry->Pattern->Compile("", /*stats=*/nullptr);
        cache.NotifyPatternCompiled(key);
    }

    entry = MakeMockEntry(cacheSize + 1);
    entry->Pattern->Compile("", /*stats=*/nullptr);
    cache.EmplacePattern(key, entry);
}

Y_UNIT_TEST(UpdateConfigurationResize) {
    constexpr size_t patternSize = 100;
    constexpr size_t patternCount = 4;
    constexpr size_t initialMaxBytes = patternSize * patternCount;

    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto maxSizeBytes = counters->GetCounter("PatternCache/MaxSizeBytes", /*derivative=*/false);
    auto maxCompiledSizeBytes = counters->GetCounter("PatternCache/MaxCompiledSizeBytes", /*derivative=*/false);
    auto sizeCompiledBytes = counters->GetCounter("PatternCache/SizeCompiledBytes", /*derivative=*/false);

    TComputationPatternCache cache({initialMaxBytes, initialMaxBytes, 0}, counters);

    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxSizeBytes), initialMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxCompiledSizeBytes), initialMaxBytes);

    TVector<TProgramKey> keys;
    for (size_t i = 0; i < patternCount; ++i) {
        keys.push_back(MakeKey("p" + ToString(i)));
        auto entry = MakeMockEntry(patternSize);
        entry->Pattern->Compile("", /*stats=*/nullptr);
        cache.EmplacePattern(keys.back(), entry);
    }
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*sizeCompiledBytes), patternSize * patternCount);

    // Resize up: entries preserved, sensors reflect new limit.
    constexpr size_t expandedMaxBytes = initialMaxBytes * 2;
    cache.UpdateConfiguration({expandedMaxBytes, expandedMaxBytes, 0});
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxSizeBytes), expandedMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxCompiledSizeBytes), expandedMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(cache.GetConfiguration().MaxSizeBytes, expandedMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*sizeCompiledBytes), patternSize * patternCount);
    for (const auto& key : keys) {
        auto entry = cache.Find(key);
        UNIT_ASSERT(entry);
        UNIT_ASSERT(entry->Pattern->GetCompileStatus() == ECompileStatus::Compiled);
    }

    // Resize down past current compiled usage: oldest compiled code evicted.
    constexpr size_t shrunkMaxBytes = patternSize * 2;
    cache.UpdateConfiguration({expandedMaxBytes, shrunkMaxBytes, 0});
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxSizeBytes), expandedMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxCompiledSizeBytes), shrunkMaxBytes);
    UNIT_ASSERT_LE(static_cast<size_t>(*sizeCompiledBytes), shrunkMaxBytes);

    // Entries themselves still resolvable; only the compiled code of the LRU entries was dropped.
    size_t stillCompiled = 0;
    for (const auto& key : keys) {
        auto entry = cache.Find(key);
        UNIT_ASSERT(entry);
        if (entry->Pattern->GetCompileStatus() == ECompileStatus::Compiled) {
            ++stillCompiled;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(stillCompiled, shrunkMaxBytes / patternSize);
}

Y_UNIT_TEST(TripletKeyFieldsDistinguishEntries) {
    // Four entries that differ from each other in exactly one field at a time
    const NYql::TLangVersion ver1 = NYql::MakeLangVersion(2025, 1);
    const NYql::TLangVersion ver2 = NYql::MakeLangVersion(2025, 2);
    const NYql::TRuntimeSettingsStableHash hash1 = MakeStableHash(0xAA);
    const NYql::TRuntimeSettingsStableHash hash2 = MakeStableHash(0xBB);

    const TProgramKey keyBase{ver1, hash1, "prog"};
    const TProgramKey keyDiffVer{ver2, hash1, "prog"};   // only lang version differs
    const TProgramKey keyDiffHash{ver1, hash2, "prog"};  // only stable hash differs
    const TProgramKey keyDiffProg{ver1, hash1, "other"}; // only program differs

    TComputationPatternCache cache({1'000'000, 1'000'000});

    auto entryBase = MakeMockEntry();
    auto entryDiffVer = MakeMockEntry();
    auto entryDiffHash = MakeMockEntry();
    auto entryDiffProg = MakeMockEntry();

    cache.EmplacePattern(keyBase, entryBase);
    cache.EmplacePattern(keyDiffVer, entryDiffVer);
    cache.EmplacePattern(keyDiffHash, entryDiffHash);
    cache.EmplacePattern(keyDiffProg, entryDiffProg);

    UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 4);

    UNIT_ASSERT_EQUAL(cache.Find(keyBase), entryBase);
    UNIT_ASSERT_EQUAL(cache.Find(keyDiffVer), entryDiffVer);
    UNIT_ASSERT_EQUAL(cache.Find(keyDiffHash), entryDiffHash);
    UNIT_ASSERT_EQUAL(cache.Find(keyDiffProg), entryDiffProg);

    UNIT_ASSERT(!cache.Find(TProgramKey{ver2, hash2, "missing"}));
}

Y_UNIT_TEST(TripletKeyNotifyPatternCompiled) {
    const TProgramKey key{NYql::MakeLangVersion(2025, 1), MakeStableHash(0x10), "prog"};
    TComputationPatternCache cache({1'000'000, 1'000'000});

    auto entry = MakeMockEntry(512);
    cache.EmplacePattern(key, entry);

    entry->Pattern->Compile("", /*stats=*/nullptr);
    cache.NotifyPatternCompiled(key);

    auto found = cache.Find(key);
    UNIT_ASSERT_EQUAL(found, entry);
    UNIT_ASSERT(found->Pattern->GetCompileStatus() == ECompileStatus::Compiled);
}

Y_UNIT_TEST(TripletKeyNotifyPatternMissing) {
    // NotifyPatternMissing releases waiters for the specific triplet
    const TProgramKey key{NYql::MakeLangVersion(2025, 1), MakeStableHash(0x20), "prog"};
    TComputationPatternCache cache({1'000'000, 1'000'000});

    // Register as the first subscriber (gets nothing to wait for, so it has to create the entry)
    auto firstFuture = cache.FindOrSubscribe(key);
    UNIT_ASSERT(!firstFuture);

    // Register a second subscriber (gets a promise future)
    auto secondFuture = cache.FindOrSubscribe(key);
    UNIT_ASSERT(secondFuture);
    UNIT_ASSERT(!secondFuture->HasValue());

    // Notify missing - second subscriber should receive nullptr
    cache.NotifyPatternMissing(key);
    UNIT_ASSERT(secondFuture->HasValue());
    UNIT_ASSERT(!secondFuture->GetValue());
}

Y_UNIT_TEST(TripletKeyFindOrSubscribeDistinctKeys) {
    // FindOrSubscribe distinguishes entries by full triplet
    const NYql::TLangVersion ver1 = NYql::MakeLangVersion(2025, 1);
    const NYql::TLangVersion ver2 = NYql::MakeLangVersion(2025, 2);
    const NYql::TRuntimeSettingsStableHash hash = {};
    const TString program = "prog";

    TComputationPatternCache cache({1'000'000, 1'000'000});

    auto entry1 = MakeMockEntry();
    auto entry2 = MakeMockEntry();

    cache.EmplacePattern(TProgramKey{ver1, hash, program}, entry1);
    cache.EmplacePattern(TProgramKey{ver2, hash, program}, entry2);

    auto future1 = cache.FindOrSubscribe(TProgramKey{ver1, hash, program});
    auto future2 = cache.FindOrSubscribe(TProgramKey{ver2, hash, program});

    UNIT_ASSERT(future1 && future1->HasValue());
    UNIT_ASSERT(future2 && future2->HasValue());
    UNIT_ASSERT_EQUAL(future1->GetValue(), entry1);
    UNIT_ASSERT_EQUAL(future2->GetValue(), entry2);
}

Y_UNIT_TEST(SubscribersGetTheEmplacedEntry) {
    const TProgramKey key = MakeKey("program");
    TComputationPatternCache cache({1'000'000, 1'000'000});

    UNIT_ASSERT(!cache.FindOrSubscribe(key));

    auto waiter = cache.FindOrSubscribe(key);
    UNIT_ASSERT(waiter);
    UNIT_ASSERT(!waiter->HasValue());

    auto entry = MakeMockEntry();
    cache.EmplacePattern(key, entry);

    UNIT_ASSERT(waiter->HasValue());
    UNIT_ASSERT_EQUAL(waiter->GetValue(), entry);
}

Y_UNIT_TEST(PatternWithoutCompiledCodeIsNotTracked) {
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto sizeCompiledItems = counters->GetCounter("PatternCache/SizeCompiledItems", /*derivative=*/false);
    auto sizeCompiledBytes = counters->GetCounter("PatternCache/SizeCompiledBytes", /*derivative=*/false);

    constexpr size_t maxBytes = 1000;
    TComputationPatternCache cache({maxBytes, maxBytes}, counters);

    const TProgramKey key = MakeKey("program");

    // A pattern that has not fit into the codegen limits holds no code at all.
    auto entry = MakeMockEntry(/*codeSize=*/0);
    entry->Pattern->Compile("", /*stats=*/nullptr);
    UNIT_ASSERT(entry->Pattern->GetCompileStatus() == ECompileStatus::RejectedBySize);

    cache.EmplacePattern(key, entry);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*sizeCompiledItems), 0);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*sizeCompiledBytes), 0);

    // The same holds for the pattern reported as compiled after it got into the cache.
    cache.NotifyPatternCompiled(key);
    cache.UpdatePatternCurrentUsageInfo();
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*sizeCompiledItems), 0);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*sizeCompiledBytes), 0);
}

Y_UNIT_TEST(EvictedCompiledCodeIsNotCompiledAgain) {
    constexpr size_t patternSize = 100;
    constexpr size_t accessTimesBeforeCompile = 2;

    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto compiledCodeEvictions = counters->GetCounter("PatternCache/CompiledCodeEvictions", /*derivative=*/true);

    // The compiled code budget only fits a single pattern.
    TComputationPatternCache cache({10 * patternSize, patternSize, accessTimesBeforeCompile}, counters);

    const TProgramKey key = MakeKey("program");
    auto entry = MakeMockEntry(patternSize);
    cache.EmplacePattern(key, entry);

    // Accessing the pattern often enough queues it for compilation ...
    for (size_t i = 0; i < accessTimesBeforeCompile; ++i) {
        cache.FindOrSubscribe(key);
    }
    UNIT_ASSERT_VALUES_EQUAL(TakePatternsToCompile(cache), 1);

    entry->Pattern->Compile("", /*stats=*/nullptr);
    cache.NotifyPatternCompiled(key);
    UNIT_ASSERT(entry->Pattern->GetCompileStatus() == ECompileStatus::Compiled);

    // ... and its code is dropped as soon as another pattern needs the budget.
    auto otherEntry = MakeMockEntry(patternSize);
    otherEntry->Pattern->Compile("", /*stats=*/nullptr);
    cache.EmplacePattern(MakeKey("other"), otherEntry);
    UNIT_ASSERT(entry->Pattern->GetCompileStatus() == ECompileStatus::NoCompilationStarted);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*compiledCodeEvictions), 1);

    // Once the code has been evicted, the pattern must not be queued for compilation over and over again.
    for (size_t i = 0; i < 10 * accessTimesBeforeCompile; ++i) {
        cache.FindOrSubscribe(key);
    }
    UNIT_ASSERT_VALUES_EQUAL(TakePatternsToCompile(cache), 0);
}

Y_UNIT_TEST(EvictedCodeOfPrecompiledPatternIsNotCompiledAgain) {
    constexpr size_t patternSize = 100;

    // Zero is what asks for the compilation on the very first access.
    for (const size_t accessTimesBeforeCompile : {size_t{0}, size_t{2}}) {
        // The compiled code budget only fits a single pattern.
        TComputationPatternCache cache({10 * patternSize, patternSize, accessTimesBeforeCompile});

        // A pattern may come into the cache already compiled, and then nobody has ever counted accesses to it.
        const TProgramKey key = MakeKey("program");
        auto entry = MakeMockEntry(patternSize);
        entry->Pattern->Compile("", /*stats=*/nullptr);
        cache.EmplacePattern(key, entry);

        auto otherEntry = MakeMockEntry(patternSize);
        otherEntry->Pattern->Compile("", /*stats=*/nullptr);
        cache.EmplacePattern(MakeKey("other"), otherEntry);
        UNIT_ASSERT(entry->Pattern->GetCompileStatus() == ECompileStatus::NoCompilationStarted);

        // Losing the code under the budget pressure is final all the same - its accesses start being counted only
        // now, but reaching the threshold must not get it compiled again.
        for (size_t i = 0; i < 10 * Max<size_t>(accessTimesBeforeCompile, 1); ++i) {
            cache.FindOrSubscribe(key);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(TakePatternsToCompile(cache), 0, "threshold " << accessTimesBeforeCompile);
    }
}

Y_UNIT_TEST(EvictedEntryIsMarkedAsNotCached) {
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto sizeBytes = counters->GetCounter("PatternCache/SizeBytes", /*derivative=*/false);
    auto evictions = counters->GetCounter("PatternCache/Evictions", /*derivative=*/true);
    auto evictedUnused = counters->GetCounter("PatternCache/EvictedUnused", /*derivative=*/true);

    constexpr size_t accessTimesBeforeCompile = 1;
    constexpr size_t initialMaxBytes = 100'000'000;
    constexpr size_t payloadBytes = 1'000'000;
    TComputationPatternCache cache({initialMaxBytes, initialMaxBytes, accessTimesBeforeCompile}, counters);

    const TProgramKey firstKey = MakeKey("first");
    auto first = MakeMockEntry(/*codeSize=*/1, payloadBytes);
    cache.EmplacePattern(firstKey, first);
    UNIT_ASSERT(first->IsInCache.load());

    // Shrink the cache down to a single entry, so that the next one pushes this one out. The limit is given some
    // headroom, so that the second entry is guaranteed to fit on its own once the first one is gone.
    const size_t oneEntryBytes = *sizeBytes;
    UNIT_ASSERT(oneEntryBytes > 0);
    cache.UpdateConfiguration({oneEntryBytes + oneEntryBytes / 2, initialMaxBytes, accessTimesBeforeCompile});

    auto second = MakeMockEntry(/*codeSize=*/1, payloadBytes);
    cache.EmplacePattern(MakeKey("second"), second);

    // The evicted entry has never been compiled, so it is exactly the one that may be sitting in the compilation
    // queue - and IsInCache is what stops it from being compiled after it has left the cache.
    UNIT_ASSERT(!cache.Find(firstKey));
    UNIT_ASSERT(!first->IsInCache.load());
    UNIT_ASSERT(second->IsInCache.load());

    // Nobody ever got the evicted entry out of the cache, so building it was work done for nothing.
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*evictions), 1);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*evictedUnused), 1);
}

Y_UNIT_TEST(WastedCompilationsAreCounted) {
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto wastedCompilations = counters->GetCounter("PatternCache/WastedCompilations", /*derivative=*/true);

    constexpr size_t maxBytes = 1'000'000;
    TComputationPatternCache cache({maxBytes, maxBytes}, counters);

    // Compiled by the service after the entry had left the cache: there is nothing to attribute the code to.
    cache.NotifyPatternCompiled(MakeKey("gone"));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*wastedCompilations), 1);

    // Compiled by the service while a new entry has taken the key: the code belongs to the old entry, which is not
    // the one the cache holds now.
    const TProgramKey key = MakeKey("replaced");
    cache.EmplacePattern(key, MakeMockEntry());
    cache.NotifyPatternCompiled(key);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*wastedCompilations), 2);

    // And a compilation that does land in the cache is not counted.
    auto entry = MakeMockEntry();
    entry->Pattern->Compile("", /*stats=*/nullptr);
    const TProgramKey liveKey = MakeKey("live");
    cache.EmplacePattern(liveKey, entry);
    cache.NotifyPatternCompiled(liveKey);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*wastedCompilations), 2);
}

Y_UNIT_TEST(DuplicateEmplaceKeepsTheCachedEntry) {
    const TProgramKey key = MakeKey("program");
    TComputationPatternCache cache({1'000'000, 1'000'000});

    auto cachedEntry = MakeMockEntry();
    cache.EmplacePattern(key, cachedEntry);

    // Emplacing a duplicate for a known key keeps the entry already in the cache ...
    auto duplicateEntry = MakeMockEntry();
    cache.EmplacePattern(key, duplicateEntry);

    UNIT_ASSERT_EQUAL(cache.Find(key), cachedEntry);
    UNIT_ASSERT(cachedEntry->IsInCache.load());

    // ... and the duplicate is dropped on the floor, so nobody may be handed it as if it were cached.
    UNIT_ASSERT(!duplicateEntry->IsInCache.load());
}

Y_UNIT_TEST(CleanCacheMarksEntriesAsNotCached) {
    const TProgramKey key = MakeKey("program");
    auto entry = MakeMockEntry();

    {
        constexpr size_t maxBytes = 1000;
        TComputationPatternCache cache({maxBytes, maxBytes});
        cache.EmplacePattern(key, entry);
        UNIT_ASSERT(entry->IsInCache.load());
    }

    // The entry outlives the cache it was stored in, and the cache has to walk its LRU lists before dropping the
    // holders those lists point at.
    UNIT_ASSERT(!entry->IsInCache.load());
}

} // Y_UNIT_TEST_SUITE(DqComputationPatternCache)

} // namespace NYql::NDq
