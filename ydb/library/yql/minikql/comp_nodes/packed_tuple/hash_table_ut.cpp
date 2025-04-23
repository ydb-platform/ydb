#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <random>
#include <vector>
#include <fstream>

#include <util/stream/null.h>
#include <util/system/compiler.h>
#include <util/system/fs.h>
#include <util/system/mem_info.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/neumann_hash_table.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/page_hash_table.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/robin_hood_table.h>

#include <cxxabi.h> // demangled names

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = true;
#define CTEST (IsVerbose ? Cerr : Cnull)

namespace {

constexpr ui64 CachelineBits = 9;
constexpr ui64 CachelineSize = ui64(1) << CachelineBits;

template <typename Alloc> class TBloomFilter {
    std::vector<ui64, Alloc> Storage_;
    ui64 *Ptr_;
    ui64 Bits_;
    bool Finalized_ = false;

  public:
    static constexpr ui64 BlockBits = CachelineBits;
    static constexpr ui64 BlockSize = CachelineSize;

    TBloomFilter() {}
    TBloomFilter(ui64 size) { Resize(size); }

    void Resize(ui64 size) {
        size = std::max(size, CachelineSize);
        Bits_ = 6;

        for (; (ui64(1) << Bits_) < size; ++Bits_)
            ;

        Bits_ += 3; // -> multiply by 8
        size = 1u << (Bits_ - 6);

        Storage_.assign(size + CachelineSize / sizeof(ui64) - 1, 0);

        // align Ptr_ up to BlockSize
        Ptr_ = (ui64 *)((uintptr_t(Storage_.data()) + BlockSize - 1) &
                        ~(BlockSize - 1));
        Finalized_ = false;
    }

    void Add(ui64 hash) {
        Y_DEBUG_ABORT_UNLESS(!Finalized_);

        auto bit = (hash >> (64 - Bits_));
        Ptr_[bit / 64] |= (ui64(1) << (bit % 64));
        // replace low BlockBits with next part of hash
        auto low = hash >> (64 - Bits_ - BlockBits);
        bit &= ~(BlockSize - 1);
        bit ^= low & (BlockSize - 1);
        Ptr_[bit / 64] |= (ui64(1) << (bit % 64));
    }

    bool IsMissing(ui64 hash) const {
        Y_DEBUG_ABORT_UNLESS(Finalized_);

        auto bit = (hash >> (64 - Bits_));
        if (!(Ptr_[bit / 64] & (ui64(1) << (bit % 64))))
            return true;
        // replace low BlockBits with next part of hash
        auto low = hash >> (64 - Bits_ - BlockBits);
        bit &= ~(BlockSize - 1);
        bit ^= low & (BlockSize - 1);
        if (!(Ptr_[bit / 64] & (ui64(1) << (bit % 64))))
            return true;
        return false;
    }

    constexpr bool IsFinalized() const { return Finalized_; }

    void Finalize() { Finalized_ = true; }

    void Shrink() {
        Finalized_ = false;
        Bits_ = 1;
        Storage_.clear();
        Storage_.resize(1, ~ui64(0));
        Storage_.shrink_to_fit();
        Ptr_ = Storage_.data();
    }
};

template <typename Alloc> class TBloomFilter2 {
    using THash = ui32;
    using TBloom = ui16;

    static constexpr unsigned kBloomBits = 16;
    static constexpr unsigned kBloomMaskBits = 4;

    static_assert(kBloomBits != 0 && kBloomMaskBits != 0 &&
                  kBloomMaskBits < kBloomBits &&
                  kBloomBits <= sizeof(TBloom) * 8);

    alignas(64) static constexpr auto kBloomTags =
        TBloomFilterMasks<TBloom>::template Gen<kBloomBits, kBloomMaskBits>();
    static constexpr unsigned kBloomHashBits =
        std::countr_zero(kBloomTags.size());

    static constexpr unsigned kHashBucketLogSize = 4;

  public:
    TBloomFilter2() {}
    TBloomFilter2(ui64 size) { Resize(size); }

    void Resize(ui64 size) {
        Y_ASSERT(size);

        BucketBits_ = sizeof(size) * 8 - std::countl_zero(size - 1);
        BucketBits_ = BucketBits_ < kHashBucketLogSize
                          ? 0
                          : BucketBits_ - kHashBucketLogSize;
        Storage_.assign(1ull << BucketBits_, 0);
    }

    void Add(THash hash) {
        const auto [bucket, tagInd] = GetBucket(hash);
        Storage_[bucket] |= kBloomTags[tagInd];
    }

    bool IsMissing(THash hash) const {
        const auto [bucket, tagInd] = GetBucket(hash);
        return (~Storage_[bucket]) & kBloomTags[tagInd];
    }

    constexpr bool IsFinalized() const { return true; }

    void Finalize() {}

    void Shrink() {
        Storage_.clear();
        Storage_.shrink_to_fit();
    }

  private:
    auto GetBucket(THash hash) const {
        const auto bucket = hash & ((THash(1) << BucketBits_) - 1);
        hash = (hash >> BucketBits_) & ((1ul << kBloomHashBits) - 1);
        return std::pair{bucket, hash};
    }

  private:
    std::vector<TBloom, Alloc> Storage_;
    THash BucketBits_;
};

template <class T, class F> class TBloomedTable {
  public:
    explicit TBloomedTable(const TTupleLayout *layout)
        : Layout_(layout), Table_(layout) {}

    void Apply(const ui8 *const tuple, const ui8 *const overflow,
               auto &&onMatch) {
        const auto hash = TTupleHash{}(Layout_, tuple, overflow);
        if (!Filter_.IsMissing(hash)) {
            Table_.Apply(tuple, overflow, std::move(onMatch));
        }
    }

    void Build(const ui8 *const tuples, const ui8 *const overflow,
               ui32 nItems) {
        Filter_.Resize(nItems);
        for (ui32 i = 0; i != nItems; ++i) {
            const auto hash = TTupleHash{}(
                Layout_, tuples + i * Layout_->TotalRowSize, overflow);
            Filter_.Add(hash);
        }
        Filter_.Finalize();

        Table_.Build(tuples, overflow, nItems);
    }

    void Clear() {
        Filter_.Shrink();
        Table_.Clear();
    }

  private:
    const TTupleLayout *Layout_;
    F Filter_;
    T Table_;
};

} // namespace

namespace {

class IDistribution {
  public:
    virtual ui32 operator()(std::mt19937 &gen) = 0;
    virtual ui32 Min() const = 0;
    virtual ui32 Max() const = 0;
};

class TSingleValueDistribution : public IDistribution {
  public:
    TSingleValueDistribution(ui32 a) : Value_(a) {}

    virtual ui32 operator()(std::mt19937 &) override { return Value_; }
    virtual ui32 Min() const override { return Value_; }
    virtual ui32 Max() const override { return Value_; }

  private:
    const ui32 Value_;
};

class TUniformDistribution : public IDistribution {
  public:
    TUniformDistribution(ui32 a, ui32 b) : Distribution_(a, b) {}

    virtual ui32 operator()(std::mt19937 &gen) override {
        return Distribution_(gen);
    }
    virtual ui32 Min() const override { return Distribution_.min(); }
    virtual ui32 Max() const override { return Distribution_.max(); }

  private:
    std::uniform_int_distribution<ui32> Distribution_;
};

class TNormalDistribution : public IDistribution {
  public:
    TNormalDistribution(ui32 mean, ui32 std)
        : Distribution_(mean, std), Min_{mean > 3 * std ? mean - 3 * std : 0},
          Max_{mean + 3 * std} {}

    virtual ui32 operator()(std::mt19937 &gen) override {
        return std::max(Min_,
                        std::min(Max_, ui32(std::lround(Distribution_(gen)))));
    }
    virtual ui32 Min() const override { return Min_; }
    virtual ui32 Max() const override { return Max_; }

  private:
    std::normal_distribution<double> Distribution_;
    ui32 Min_;
    ui32 Max_;
};

class TMixtureDistribution : public IDistribution {
  public:
    TMixtureDistribution(IDistribution &left, IDistribution &right, float pLeft)
        : Left_(left), Right_(right), PLeft_(pLeft) {}

    virtual ui32 operator()(std::mt19937 &gen) override {
        return PLeft_(gen) ? Left_(gen) : Right_(gen);
    }
    virtual ui32 Min() const override {
        return std::min(Left_.Min(), Right_.Min());
    }
    virtual ui32 Max() const override {
        return std::max(Left_.Max(), Right_.Max());
    }

  private:
    IDistribution &Left_;
    IDistribution &Right_;
    std::bernoulli_distribution PLeft_;
};

class TRepeatDistribution : public IDistribution {
  public:
    TRepeatDistribution(IDistribution &distribution, ui32 repeatNum)
        : Distribution_(distribution), RepeatNum_(std::max(1u, repeatNum)), Cnt_(0) {}

    virtual ui32 operator()(std::mt19937 &gen) override {
        if (Cnt_ == 0) {
            Cnt_ = RepeatNum_;
            Val_ = Distribution_(gen);
        }
        --Cnt_;
        return Val_;
    }
    virtual ui32 Min() const override {
        return Distribution_.Min();
    }
    virtual ui32 Max() const override {
        return Distribution_.Max();
    }

  private:
    IDistribution &Distribution_;
    const ui32 RepeatNum_;
    ui32 Val_;
    ui32 Cnt_;
};

} // namespace

namespace {

template <size_t Batch, typename... Args> class TBenchmark {
    struct TResult {
        ui64 coldBuildTime = 0;
        ui64 warmBuildTime = 0;
        ui64 lookupTime = 0;
        ui64 batchedLookupTime = 0;
        ui64 memUsed = 0;
    };

    friend std::ostream &operator<<(std::ostream &out, TResult res) {
        out << res.coldBuildTime << ',' << res.warmBuildTime << ','
            << res.lookupTime << ',' << res.batchedLookupTime
            << ',' << res.memUsed;
        return out;
    }

    struct TInfo {
        ui64 uniqueMatches = 0;
        ui64 totalMatches = 0;
        ui32 checksum = 0;
    };

    static constexpr auto kBuildLookupSize = std::array{
        std::pair<ui32, ui32>{10000, 2000000},
        std::pair<ui32, ui32>{40000, 2000000},
        std::pair<ui32, ui32>{100000, 2000000},
        std::pair<ui32, ui32>{400000, 2000000},
        std::pair<ui32, ui32>{1000000, 2000000},
        std::pair<ui32, ui32>{2000000, 2000000},
        std::pair<ui32, ui32>{4000000, 2000000},
        std::pair<ui32, ui32>{8000000, 2000000},
    };
    static constexpr ui32 kIters = 3;

  public:
    struct TConfig {
        TString name;
        IDistribution &buildKeyDistribution;
        IDistribution &lookupKeyDistribution;
    };

    TBenchmark(ui32 payloadSize, const char *benchName)
        : Alloc_(__LOCATION__), PayloadSize_(payloadSize),
          BenchName_(benchName) {
        UNIT_ASSERT(payloadSize != 0);
        Init();
    }

    void Register(TConfig config) { Configs_.emplace_back(std::move(config)); }

    void Run(bool toCSV = false) {
        CTEST << "================================\n";
        CTEST << "======== " << BenchName_ << "\n";
        CTEST << "Hash tables being benchmarked:\n";
        ApplyNumbered([&]<class Arg, size_t Ind> {
            PrintArg<Arg>("  (" + std::to_string(Ind) + ") ",
                          "(" + std::to_string(PayloadSize_) + ")\n");
        });
        CTEST << Endl;

        for (const auto &config : Configs_) {
            CTEST << "--------------------------------\n";

            std::ofstream out;
            if (toCSV) {
                const auto dir = std::filesystem::path(__FILE__).parent_path() /= "results";
                std::filesystem::create_directory(dir);
                out = std::ofstream(dir.string() + "/" + BenchName_ + "_" + config.name + ".csv");
            }
    
            for (const auto [buildSize, lookupSize] : kBuildLookupSize) {
                auto [buildKeys, buildOverflow] =
                    GenData(buildSize, config.buildKeyDistribution);
                auto [lookupKeys, lookupOverflow] =
                    GenData(lookupSize, config.lookupKeyDistribution);
                const auto info =
                    GetInfo(config.buildKeyDistribution.Max(), buildKeys.data(),
                            buildSize, lookupKeys.data(), lookupSize);

                const auto results = RunTest(config, info, buildKeys.data(), buildOverflow.data(),
                        buildSize, lookupKeys.data(), lookupOverflow.data(),
                        lookupSize);

                if (toCSV) {
                    out << buildSize << ',' << lookupSize << ',';
                    for (size_t ind = 0; ind != results.size(); ++ind) {
                        out << results[ind] << (ind + 1 == results.size() ? '\n' : ',');
                    }
                    out << std::flush;
                }
            }
        }
    }

  private:
    void Init() {
        TColumnDesc kc1, pc1;

        kc1.Role = EColumnRole::Key;
        kc1.DataSize = 4;

        pc1.Role = EColumnRole::Payload;
        pc1.DataSize = PayloadSize_;

        std::vector<TColumnDesc> columns{kc1, pc1};
        Layout_ = TTupleLayout::Create(columns);
    }

    auto GenData(ui32 size, IDistribution &distribution) {
        std::mt19937 gen(11);

        std::vector<ui32> col1(size);
        std::vector<ui8> col2(size * PayloadSize_);

        for (ui32 i = 0; i < size; ++i) {
            col1[i] = distribution(gen);
            col2[i * PayloadSize_] = 1;
        }

        const ui8 *cols[2];
        cols[0] = (ui8 *)col1.data();
        cols[1] = (ui8 *)col2.data();

        std::vector<ui8> colValid1((size + 7) / 8, ~0);
        std::vector<ui8> colValid2((size + 7) / 8, ~0);
        const ui8 *colsValid[2] = {
            colValid1.data(),
            colValid2.data(),
        };

        const ui64 DataSize = Layout_->TotalRowSize * size;
        auto result = std::make_pair(std::vector<ui8>(DataSize + 64, 0),
                                     std::vector<ui8, TMKQLAllocator<ui8>>{});
        Layout_->Pack(cols, colsValid, result.first.data(), result.second, 0,
                      size);

        return result;
    }

    TInfo GetInfo(ui32 maxKey, ui8 *leftData, ui32 leftSize, ui8 *rightData,
                  ui32 rightSize) {
        std::vector<bool> keyFound(maxKey + 1, 0);
        std::vector<ui32> keyCount(maxKey + 1, 0);

        for (ui32 ind = 0; ind != leftSize; ++ind) {
            const ui32 key =
                ReadUnaligned<ui32>(leftData + ind * Layout_->TotalRowSize +
                                    Layout_->KeyColumnsOffset);
            ++keyCount[key];
        }

        TInfo result;
        for (ui32 ind = 0; ind != rightSize; ++ind) {
            const ui32 hash =
                ReadUnaligned<ui32>(rightData + ind * Layout_->TotalRowSize);
            const ui32 key =
                ReadUnaligned<ui32>(rightData + ind * Layout_->TotalRowSize +
                                    Layout_->KeyColumnsOffset);

            if (maxKey < key) {
                continue;
            }
            if (keyCount[key] && !keyFound[key]) {
                keyFound[key] = true;
                result.uniqueMatches++;
            }
            result.totalMatches += keyCount[key];
            result.checksum += keyCount[key] * hash;
        }

        return result;
    }

    auto RunTest(const TConfig &config, const TInfo info, ui8 *buildKeys,
                 ui8 *buildOverflow, ui32 buildSize, ui8 *lookupKeys,
                 ui8 *lookupOverflow, ui32 lookupSize) {
        std::array<TResult, sizeof...(Args)> results;
        ApplyNumbered([&]<class Arg, size_t Ind> {
            results[Ind] =
                RunArgTest<Arg>(buildKeys, buildOverflow, buildSize, lookupKeys,
                                lookupOverflow, lookupSize, info);
        });

        CTEST << "--------" << Endl;
        CTEST << "Params\t" << " config: " << config.name
              << ", keys: " << buildSize << ", lookups: " << lookupSize << Endl;
        CTEST << "Info\t" << " unique matches: " << info.uniqueMatches
              << ", total matches: " << info.totalMatches << Endl;
        CTEST << "\nTime measurements (us):" << Endl;
        PrintResult("1st build", results,
                    [](TResult arg) { return arg.coldBuildTime; });
        PrintResult("2nd build", results,
                    [](TResult arg) { return arg.warmBuildTime; });
        PrintResult("lookups", results,
                    [](TResult arg) { return arg.lookupTime; });
        if constexpr (Batch > 1) {
            PrintResult("batch " + std::to_string(Batch), results,
                [](TResult arg) { return arg.batchedLookupTime; });
        }
        CTEST << "\nMemory measurements (KiB):" << Endl;
        PrintResult("used by ht", results,
                    [](TResult arg) { return arg.memUsed; });

        return results;
    }

    void PrintResult(const std::string &name,
                     const std::array<TResult, sizeof...(Args)> &results,
                     auto getter) {
        const size_t minInd =
            std::min_element(
                results.begin(), results.end(),
                [&](auto lhs, auto rhs) { return getter(lhs) < getter(rhs); }) -
            results.begin();

        CTEST << "  " << name << ":\t";
        for (size_t ind = 0; ind != sizeof...(Args); ++ind) {
            if (ind == minInd) {
                CTEST << "> (" << ind << ") " << getter(results[ind]) << " <\t";
            } else {
                CTEST << "  (" << ind << ") " << getter(results[ind]) << "  \t";
            }
        }
        CTEST << Endl;
    }

    template <typename Arg>
    TResult RunArgTest(ui8 *buildKeys, ui8 *buildOverflow, ui32 buildSize,
                       ui8 *lookupKeys, ui8 *lookupOverflow, ui32 lookupSize,
                       TInfo info) {
        TResult result;

        for (ui32 iter = 0; iter != kIters; ++iter) {
            UNIT_ASSERT_LE(Alloc_.GetUsed(), 1024);

            Arg arg(Layout_.Get());

            result.coldBuildTime += Measure(
                [&] { arg.Build(buildKeys, buildOverflow, buildSize); });
            arg.Clear();
            result.warmBuildTime += Measure(
                [&] { arg.Build(buildKeys, buildOverflow, buildSize); });
            result.memUsed = Alloc_.GetUsed() / 1024;

            ui64 matches = 0;
            ui32 checksum = 0;
            result.lookupTime = Measure([&] {
                ui8 *const end =
                    lookupKeys + Layout_->TotalRowSize * lookupSize;
                for (ui8 *it = lookupKeys; it != end; it += Layout_->TotalRowSize) {
                    arg.Apply(it, lookupOverflow, [&](const ui8 *const row) {
                        checksum += ReadUnaligned<ui32>(it);
                        matches += 
                            ReadUnaligned<ui8>(row + 2 * sizeof(ui32) + (1 + 7) / 8);
                    });
                }
            });
            UNIT_ASSERT_EQUAL(matches, info.totalMatches);
            UNIT_ASSERT_EQUAL(checksum, info.checksum);

            if constexpr (Batch > 1) {
                matches = 0;
                checksum = 0;
                result.batchedLookupTime = Measure([&] {
                    for (size_t batchInd = 0; batchInd < lookupSize; batchInd += Batch) {
                        std::array<const ui8 *, Batch> rows;
                        for (size_t i = 0; i < Batch && batchInd + i < lookupSize; ++i) {
                            rows[i] = lookupKeys + Layout_->TotalRowSize * (batchInd + i);
                        }

                        std::array<typename Arg::TIterator, Batch> iters =
                            arg.FindBatch(rows, lookupOverflow);
                        for (size_t i = 0; i < Batch && batchInd + i < lookupSize; ++i) {
                            while (auto match = arg.NextMatch(iters[i], lookupOverflow)) {
                                checksum += ReadUnaligned<ui32>(rows[i]);
                                matches +=
                                    ReadUnaligned<ui8>(match + 2 * sizeof(ui32) + (1 + 7) / 8);    
                            }
                        }
                    }
                });
                UNIT_ASSERT_EQUAL(matches, info.totalMatches);
                UNIT_ASSERT_EQUAL(checksum, info.checksum);
            }

            arg.Clear();
        }

        result.coldBuildTime /= kIters;
        result.warmBuildTime /= kIters;
        result.lookupTime /= kIters;
        result.batchedLookupTime /= kIters;

        return result;
    }

    void ApplyNumbered(auto f) {
        [&]<size_t... Inds>(std::index_sequence<Inds...>) {
            (f.template operator()<Args, Inds>(), ...);
        }(std::make_index_sequence<sizeof...(Args)>{});
    }

    template <typename Arg>
    void PrintArg(std::string prefix, std::string suffix) {
        int status;
        CTEST << prefix
              << abi::__cxa_demangle(typeid(Arg).name(), 0, 0, &status)
              << suffix;
    }

    ui64 Measure(auto f) {
        const auto begin = std::chrono::steady_clock::now();
        f();
        const auto end = std::chrono::steady_clock::now();

        ui64 us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - begin)
                .count();
        us = (us == 0 ? 1 : us);
        return us;
    }

  private:
    TScopedAlloc Alloc_;

    ui32 PayloadSize_;
    const char *BenchName_;

    THolder<TTupleLayout> Layout_;

    std::vector<TConfig> Configs_;
};

} // namespace

// -----------------------------------------------------------------

using TRobinHoodTableSeq = TRobinHoodHashBase<true, false>;
using TRobinHoodTableSeqPref = TRobinHoodHashBase<true, true>;

using TNeumannTable = TNeumannHashTable<false, false>;
using TNeumannTableSeq = TNeumannHashTable<true, false>;

using TNeumannTablePref = TNeumannHashTable<false, true>;
using TNeumannTableSeqPref = TNeumannHashTable<true, true>;

using TPageTableSSE = TPageHashTableImpl<NSimd::TSimdSSE42Traits, false>;
using TPageTableSSEPref = TPageHashTableImpl<NSimd::TSimdSSE42Traits, true>;

using TPageTableAVX2 = TPageHashTableImpl<NSimd::TSimdAVX2Traits, false>;
using TPageTableAVX2Pref = TPageHashTableImpl<NSimd::TSimdAVX2Traits, true>;

// -----------------------------------------------------------------

using TTablesBenchmark =
    TBenchmark<0, TRobinHoodTableSeq, TRobinHoodTableSeqPref,
               TNeumannTable, TNeumannTableSeq, TPageTableSSE>;

template <typename... Args> struct TTablesCase {
    template <size_t Batch> using TBenchmark = TBenchmark<Batch, Args...>;
};
using TTablesBatched =
    TTablesCase<TRobinHoodTableSeqPref, TNeumannTablePref, TNeumannTableSeqPref, TPageTableSSEPref, TPageTableAVX2Pref>;

// -----------------------------------------------------------------

Y_UNIT_TEST_SUITE(HashTablesBenchmark) {

    static constexpr bool toCSV = false;

    Y_UNIT_TEST(Uniform) {
        TUniformDistribution uni1M(0, 999999);
        TUniformDistribution uni1B(0, 999999999); /// oof, info may take some

        auto benchmark = TTablesBenchmark(4, Name_);

        benchmark.Register({"uniform 1M", uni1M, uni1M});
        // benchmark.Register({"uniform 1M, 1B", uni1M, uni1B});
        // benchmark.Register({"uniform 1B, 1M", uni1B, uni1M});
        // benchmark.Register({"uniform 1B", uni1B, uni1B});

        benchmark.Run(toCSV);
    }

    Y_UNIT_TEST(UniformBatchedOutplace) {
        TUniformDistribution uni1M(0, 9999999);
        TRepeatDistribution uni1Mrpt8(uni1M, 8);
        TUniformDistribution uni10M(0, 9999999);
        TUniformDistribution uni200M(0, 199999999);

        auto benchmark = TTablesBatched::TBenchmark<16>(4, Name_);

        benchmark.Register({"uniform 1M", uni1Mrpt8, uni1M});
        benchmark.Register({"uniform 10M", uni10M, uni10M});
        benchmark.Register({"uniform 10M, 200M", uni10M, uni200M});

        benchmark.Run(toCSV);
    }

    Y_UNIT_TEST(UniformPayloaded) {
        TUniformDistribution uni1M(0, 999999);
        TUniformDistribution uni1B(0, 999999999); /// oof, info may take some

        auto benchmark = TTablesBenchmark(21, Name_);

        benchmark.Register({"uniform 1M", uni1M, uni1M});
        // benchmark.Register({"uniform 1M, 1B", uni1M, uni1B});
        // benchmark.Register({"uniform 1B, 1M", uni1B, uni1M});
        // benchmark.Register({"uniform 1B", uni1B, uni1B});

        // benchmark.Run(toCSV);
    }

    Y_UNIT_TEST(UniformPayloadedBatched) {
        TUniformDistribution uni1M(0, 999999);
        TUniformDistribution uni1B(0, 999999999); /// oof, info may take some

        auto benchmark = TTablesBatched::TBenchmark<16>(21, Name_);

        benchmark.Register({"uniform 1M", uni1M, uni1M});
        // benchmark.Register({"uniform 1M, 1B", uni1M, uni1B});
        // benchmark.Register({"uniform 1B, 1M", uni1B, uni1M});
        // benchmark.Register({"uniform 1B", uni1B, uni1B});

        // benchmark.Run(toCSV);
    }

    Y_UNIT_TEST(NormalPayloaded) {
        TNormalDistribution norm1M(1000000 / 2, 1000000 / 6);
        TNormalDistribution norm1B(1000000000 / 2,
                                   1000000000 / 6); /// oof, info may take some

        auto benchmark = TTablesBenchmark(21, Name_);

        benchmark.Register({"norm 1M", norm1M, norm1M});
        // benchmark.Register({"norm 1M, 1B", norm1M, norm1B});
        // benchmark.Register({"norm 1B, 1M", norm1M, norm1B});
        // benchmark.Register({"norm 1B", norm1B, norm1B});

        // benchmark.Run(toCSV);
    }

    Y_UNIT_TEST(SingleMixPayloaded) {
        TSingleValueDistribution single(1);
        TUniformDistribution uni1M(0, 999999);
        TMixtureDistribution mixSingleUni1B(single, uni1M, 0.1);

        auto benchmark = TTablesBenchmark(21, Name_);

        benchmark.Register({"uniform 1M, mix", uni1M, mixSingleUni1B});
        benchmark.Register({"mix, uniform 1M", mixSingleUni1B, uni1M});

        // benchmark.Run(toCSV);
    }

} // Y_UNIT_TEST_SUITE(RobinHoodCheck)

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr