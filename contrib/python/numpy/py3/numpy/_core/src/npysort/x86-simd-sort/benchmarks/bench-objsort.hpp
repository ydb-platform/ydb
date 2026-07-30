#include <cmath>

static constexpr char x[] = "x";
static constexpr char euclidean[] = "euclidean";
static constexpr char taxicab[] = "taxicab";
static constexpr char chebyshev[] = "chebyshev";

template <typename T, const char *val>
struct Point3D {
    T x;
    T y;
    T z;
    static constexpr std::string_view name {val};
    Point3D()
    {
        x = (T)rand() / RAND_MAX;
        y = (T)rand() / RAND_MAX;
        z = (T)rand() / RAND_MAX;
    }
    T distance()
    {
        if constexpr (name == "x") { return x; }
        else if constexpr (name == "euclidean") {
            return std::sqrt(x * x + y * y + z * z);
        }
        else if constexpr (name == "taxicab") {
            return std::abs(x) + std::abs(y) + std::abs(z);
        }
        else if constexpr (name == "chebyshev") {
            return std::max(std::max(std::abs(x), std::abs(y)), std::abs(z));
        }
    }
};

template <typename T>
std::vector<T> init_data(const int size)
{
    srand(42);
    std::vector<T> arr;
    for (auto ii = 0; ii < size; ++ii) {
        T temp;
        arr.push_back(temp);
    }
    return arr;
}

template <typename T>
struct less_than_key {
    inline bool operator()(T &p1, T &p2)
    {
        return (p1.distance() < p2.distance());
    }
};

template <typename T>
static void scalarobjsort(benchmark::State &state)
{
    // set up array
    std::vector<T> arr = init_data<T>(state.range(0));
    std::vector<T> arr_bkp = arr;
    // benchmark
    for (auto _ : state) {
        std::sort(arr.begin(), arr.end(), less_than_key<T>());
        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

template <typename T>
static void simdobjsort(benchmark::State &state)
{
    // set up array
    std::vector<T> arr = init_data<T>(state.range(0));
    std::vector<T> arr_bkp = arr;
    // benchmark
    for (auto _ : state) {
        x86simdsort::object_qsort(
                arr.data(), arr.size(), [](T p) { return p.distance(); });
        state.PauseTiming();
        if (!std::is_sorted(arr.begin(), arr.end(), less_than_key<T>())) {
            std::cout << "sorting failed \n";
        }
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

#define BENCHMARK_OBJSORT(func, T, type, dist) \
    BENCHMARK_TEMPLATE(func, T<type, dist>) \
            ->Arg(10e1) \
            ->Arg(10e2) \
            ->Arg(10e3) \
            ->Arg(10e4) \
            ->Arg(10e5) \
            ->Arg(10e6);

#define BENCH_ALL(dtype) \
    BENCHMARK_OBJSORT(simdobjsort, Point3D, dtype, x) \
    BENCHMARK_OBJSORT(scalarobjsort, Point3D, dtype, x) \
    BENCHMARK_OBJSORT(simdobjsort, Point3D, dtype, taxicab) \
    BENCHMARK_OBJSORT(scalarobjsort, Point3D, dtype, taxicab) \
    BENCHMARK_OBJSORT(simdobjsort, Point3D, dtype, euclidean) \
    BENCHMARK_OBJSORT(scalarobjsort, Point3D, dtype, euclidean) \
    BENCHMARK_OBJSORT(simdobjsort, Point3D, dtype, chebyshev) \
    BENCHMARK_OBJSORT(scalarobjsort, Point3D, dtype, chebyshev)

BENCH_ALL(double)
BENCH_ALL(float)
