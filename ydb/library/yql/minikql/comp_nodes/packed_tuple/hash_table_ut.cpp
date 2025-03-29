#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <random>
#include <vector>

#include <util/stream/null.h>
#include <util/system/compiler.h>
#include <util/system/fs.h>
#include <util/system/mem_info.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/neumann_hash_table.h>
// #include <ydb/library/yql/minikql/comp_nodes/packed_tuple/page_hash_table.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/robin_hood_table.h>

#include <cxxabi.h> // demangled names

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = true;
#define CTEST (IsVerbose ? Cerr : Cnull)

namespace {

struct TTupleEqual {
    bool operator()(const TTupleLayout *layout, const ui8 *lhsRow,
                    const ui8 *lhsOverflow, const ui8 *rhsRow,
                    const ui8 *rhsOverflow) {
        return layout->KeysEqual(lhsRow, lhsOverflow, rhsRow, rhsOverflow);
    }
};

struct TTupleHash {
    ui32 operator()(const TTupleLayout * /*layout*/, const ui8 *tuple,
                    const ui8 * /*overflow*/) {
        return ((const ui32 *)tuple)[0];
    }
};

using TRobinHoodTable =
    TRobinHoodHashBase<TTupleEqual, TTupleHash, TMKQLAllocator<ui8>>;

} // namespace

namespace {

class IDistribution {
  public:
    virtual ui32 operator()(std::mt19937 &gen) = 0;
    virtual ui32 Max() const = 0;
    virtual ui32 Min() const = 0;
};

class TSingleValueDistribution : public IDistribution {
  public:
    TSingleValueDistribution(ui32 a) : Value_(a) {}

    virtual ui32 operator()(std::mt19937 &) override { return Value_; }
    virtual ui32 Max() const override { return Value_; }
    virtual ui32 Min() const override { return Value_; }

  private:
    const ui32 Value_;
};

class TUniformDistribution : public IDistribution {
  public:
    TUniformDistribution(ui32 a, ui32 b) : Distribution_(a, b) {}

    virtual ui32 operator()(std::mt19937 &gen) override {
        return Distribution_(gen);
    }
    virtual ui32 Max() const override { return Distribution_.max(); }
    virtual ui32 Min() const override { return Distribution_.min(); }

  private:
    std::uniform_int_distribution<ui32> Distribution_;
};

class TMixtureDistribution : public IDistribution {
  public:
    TMixtureDistribution(IDistribution &left, IDistribution &right, float pLeft)
        : Left_(left), Right_(right), PLeft_(pLeft) {}

    virtual ui32 operator()(std::mt19937 &gen) override {
        return PLeft_(gen) ? Left_(gen) : Right_(gen);
    }
    virtual ui32 Max() const override {
        return std::max(Left_.Max(), Right_.Max());
    }
    virtual ui32 Min() const override {
        return std::min(Left_.Min(), Right_.Min());
    }

  private:
    IDistribution &Left_;
    IDistribution &Right_;
    std::bernoulli_distribution PLeft_;
};

} // namespace

namespace {

template <typename... Args> class Benchmark {
    struct TResult {
        ui64 coldBuildTime;
        ui64 warmBuildTime;
        ui64 lookupTime;
    };

    struct TInfo {
        ui64 uniqueMatches = 0;
        ui64 totalMatches = 0;
    };

  public:
    struct TConfig {
        TString name;
        IDistribution &buildKeyDistribution;
        IDistribution &lookupKeyDistribution;
    };

    Benchmark(ui32 payloadSize) : PayloadSize_(payloadSize) {
        Init();

        CTEST << "Hash tables being benchmarked:\n";
        ApplyNumbered([&]<class Arg, size_t Ind> {
            PrintArg<Arg>("  (" + std::to_string(Ind) + ") ",
                          "(" + std::to_string(payloadSize) + ")\n");
        });
        CTEST << Endl;
    }

    void Register(TConfig config) { Configs_.emplace_back(std::move(config)); }

    void Run() {
        static constexpr auto buildLookupSize = std::array{
            std::pair<ui32, ui32>{1000, 4000},
            std::pair<ui32, ui32>{100000, 400000},
            std::pair<ui32, ui32>{10000000, 40000000},
        };

        for (const auto &config : Configs_) {
            for (const auto [buildSize, lookupSize] : buildLookupSize) {
                auto [buildKeys, buildOverflow] =
                    GenData(buildSize, config.buildKeyDistribution);
                auto [lookupKeys, lookupOverflow] =
                    GenData(lookupSize, config.lookupKeyDistribution);
                const auto info =
                    GetInfo(config.buildKeyDistribution.Max(), buildKeys.data(),
                            buildSize, lookupKeys.data(), lookupSize);

                RunTest(config, info, buildKeys.data(), buildOverflow.data(),
                        buildSize, lookupKeys.data(), lookupOverflow.data(),
                        lookupSize);
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
        auto result = std::make_pair(
            std::vector<ui8, TMKQLAllocator<ui8>>(DataSize + 64, 0),
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
        }

        return result;
    }

    void RunTest(const TConfig &config, const TInfo info, ui8 *buildKeys,
                 ui8 *buildOverflow, ui32 buildSize, ui8 *lookupKeys,
                 ui8 *lookupOverflow, ui32 lookupSize) {
        CTEST << "------------------------------------------" << Endl;

        CTEST << "Params\t" << " config: " << config.name
              << ", keys: " << buildSize << ", lookups: " << lookupSize << Endl;

        CTEST << "Info\t" << " unique matches: " << info.uniqueMatches
              << ", total matches: " << info.totalMatches << Endl;

        CTEST << "\nTime measurements (us):" << Endl;

        std::array<TResult, sizeof...(Args)> results;
        ApplyNumbered([&]<class Arg, size_t Ind> {
            results[Ind] =
                RunArgTest<Arg>(buildKeys, buildOverflow, buildSize, lookupKeys,
                                lookupOverflow, lookupSize, info.totalMatches);
        });

        PrintResult("1st build", results,
                    [](TResult arg) { return arg.coldBuildTime; });
        PrintResult("2nd build", results,
                    [](TResult arg) { return arg.warmBuildTime; });
        PrintResult("lookups", results,
                    [](TResult arg) { return arg.lookupTime; });
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
                       size_t expectedMatches) {
        Arg arg(Layout_.Get());

        const ui64 coldBuildTime =
            Measure([&] { arg.Build(buildKeys, buildOverflow, buildSize); });
        arg.Clear();
        const ui64 warmBuildTime =
            Measure([&] { arg.Build(buildKeys, buildOverflow, buildSize); });

        size_t matches = 0;
        const ui64 lookupTime = Measure([&] {
            ui8 *const end = lookupKeys + Layout_->TotalRowSize * lookupSize;
            for (ui8 *it = lookupKeys; it != end; it += Layout_->TotalRowSize) {
                arg.Apply(it, lookupOverflow, [&](...) { ++matches; });
            }
        });
        arg.Clear();

        UNIT_ASSERT_EQUAL(matches, expectedMatches);

        return {coldBuildTime, warmBuildTime, lookupTime};
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
    ui32 PayloadSize_;
    THolder<TTupleLayout> Layout_;

    TVector<TConfig> Configs_;
};

} // namespace

// -----------------------------------------------------------------
Y_UNIT_TEST_SUITE(HashTable) {

    Y_UNIT_TEST(TablesBenchmark) {
        TScopedAlloc alloc(__LOCATION__);

        TSingleValueDistribution single(1);
        TUniformDistribution uni1M(0, 999999);
        TUniformDistribution uni1B(0, 999999999); /// oof, info may take some
        TMixtureDistribution mixSingleUni1B(single, uni1B, 0.2);

        auto benchmark = Benchmark<TRobinHoodTable, TNeumannHashTable>(4);

        benchmark.Register({"uniform 1M", uni1M, uni1M});
        benchmark.Register({"uniform 1M, 1B", uni1M, uni1B});
        benchmark.Register({"uniform 1B", uni1B, uni1B});

        benchmark.Run();
    }

} // Y_UNIT_TEST_SUITE(RobinHoodCheck)

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
