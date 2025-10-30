#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <random>
#include <vector>
#include <fstream>

#include <util/stream/null.h>
#include <util/system/compiler.h>
#include <util/system/fs.h>
#include <util/system/mem_info.h>

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/neumann_hash_table.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/page_hash_table.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/robin_hood_table.h>

#include <cxxabi.h> // demangled names

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = false;
#define CTEST (IsVerbose ? Cerr : Cnull)

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
        std::pair<ui32, ui32>{1000, 20000},
        std::pair<ui32, ui32>{4000, 20000},
        std::pair<ui32, ui32>{10000, 20000},
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
            result.lookupTime += Measure([&] {
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
                result.batchedLookupTime += Measure([&] {
                    ui8 *it = lookupKeys;
                    for (size_t batchInd = 0; batchInd < lookupSize; batchInd += Batch) {
                        std::array<const ui8 *, Batch> rows;
                        for (size_t i = 0; i < Batch && batchInd + i < lookupSize; ++i) {
                            rows[i] = it;
                            it += Layout_->TotalRowSize;
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
        int status = -123123;
        char* str = abi::__cxa_demangle(typeid(Arg).name(), 0, 0, &status);
        CTEST << prefix
              << str
              << suffix;
        free(str);
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

using TRobinHoodTable = TRobinHoodHashBase<false, false>;
using TRobinHoodTableSeq = TRobinHoodHashBase<true, false>;
using TRobinHoodTableSeqPref = TRobinHoodHashBase<true, true>;

using TNeumannTable = TNeumannHashTable<false, false>;
using TNeumannTableSeq = TNeumannHashTable<true, false>;

using TNeumannTablePref = TNeumannHashTable<false, true>;
using TNeumannTableSeqPref = TNeumannHashTable<true, true>;

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
using TPageTableSSE = TPageHashTableImpl<NSimd::TSimdSSE42Traits, false>;
using TPageTableSSEPref = TPageHashTableImpl<NSimd::TSimdSSE42Traits, true>;

using TPageTableAVX2 = TPageHashTableImpl<NSimd::TSimdAVX2Traits, false>;
using TPageTableAVX2Pref = TPageHashTableImpl<NSimd::TSimdAVX2Traits, true>;
#endif

using TPageTableFallback = TPageHashTableImpl<NSimd::TSimdFallbackTraits, false>;
using TPageTableFallbackPref = TPageHashTableImpl<NSimd::TSimdFallbackTraits, true>;

// -----------------------------------------------------------------

template <typename... Args> struct TTablesCase {
    template <size_t Batch> using TBenchmark = TBenchmark<Batch, Args...>;
};
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
using TTablesBenchmark =
    TTablesCase<TPageTableSSEPref, TRobinHoodTableSeqPref, TNeumannTablePref>;
#else
using TTablesBenchmark =
    TTablesCase<TPageTableFallbackPref, TRobinHoodTableSeqPref, TNeumannTablePref>;
#endif

// -----------------------------------------------------------------

Y_UNIT_TEST_SUITE(HashTablesBenchmark) {

    static constexpr size_t batchSize = 16;
    static constexpr bool toCSV = false;

    Y_UNIT_TEST(Uniform) {
        TUniformDistribution uni1M(0, 1e6 - 1);

        auto benchmark = TTablesBenchmark::TBenchmark<batchSize>(4, Name_);

        benchmark.Register({"uniform 1M", uni1M, uni1M});

        benchmark.Run(toCSV);
    }

    Y_UNIT_TEST(UniformPayloaded) {
        TUniformDistribution uni1M(0, 1e6 - 1);

        auto benchmark = TTablesBenchmark::TBenchmark<batchSize>(21, Name_);

        benchmark.Register({"uniform 1M", uni1M, uni1M});

        benchmark.Run(toCSV);
    }

    Y_UNIT_TEST(NormalPayloaded) {
        TNormalDistribution norm1M(1000000 / 2, 1000000 / 6);

        auto benchmark = TTablesBenchmark::TBenchmark<batchSize>(21, Name_);

        benchmark.Register({"norm 1M", norm1M, norm1M});

        benchmark.Run(toCSV);
    }

} // Y_UNIT_TEST_SUITE(RobinHoodCheck)

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr