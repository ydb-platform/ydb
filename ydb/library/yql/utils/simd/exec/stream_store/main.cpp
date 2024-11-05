#include <util/generic/ptr.h>
#include <util/system/cpu_id.h>
#include <util/system/types.h>

#include <ydb/library/yql/utils/simd/simd.h>

struct TPerfomancer {
    TPerfomancer() = default;

    struct TWrapWorker {
        virtual int StoreStream(bool log) = 0;
        virtual ~TWrapWorker() = default;
    };

    template<typename TTraits>
    struct TWorker : TWrapWorker {
        template<typename T>
        using TSimd = typename TTraits::template TSimd8<T>;
        TWorker() = default;

        void Info() {
            if (TTraits::Size == 8) {
                Cerr << "Fallback implementation:" << Endl;
            } else if (TTraits::Size == 16) {
                Cerr << "SSE42 implementation:" << Endl;
            } else if (TTraits::Size == 32) {
                Cerr << "AVX2 implementation:" << Endl;
            }
        }

        int StoreStream(bool log = true) override {
            const size_t batch = 32 / TTraits::Size;
            const size_t batch_size = TTraits::Size / 8;
            size_t log_batch_size = 0;
            if (TTraits::Size == 8) {
                log_batch_size = 0;
            } else if (TTraits::Size == 16) {
                log_batch_size = 1;
            } else {
                log_batch_size = 2;
            }

            const size_t size = (32LL << 21);
            const size_t arrSize = size / 8;

            i64* buf __attribute__((aligned(32))) = new i64[arrSize];

            for (size_t i = 0; i < arrSize; i += 1) {
                buf[i] = 0;
            }


            i64 tmp[4];
            for (size_t i = 0; i < 4; i += 1) {
                tmp[i] = i;
            }
            TSimd<i8> tmpSimd[batch];
            for (int i = 0; i < 4; i += batch_size) {
                tmpSimd[i >> log_batch_size] = TSimd<i8>((i8*) (tmp + i));
            }

            std::chrono::steady_clock::time_point begin01 =
                    std::chrono::steady_clock::now();

            for (size_t i = 0; i < arrSize; i += 4) {
                for (size_t j = 0; j < batch; j += 1) {
                    tmpSimd[j].StoreStream((i8*)(buf + i + j * batch_size));
                }
            }

            std::chrono::steady_clock::time_point end01 =
                std::chrono::steady_clock::now();

            bool is_ok = true;

            for (size_t i = 0; i < arrSize; i += 1) {
                if (buf[i] != i % 4) {
                    is_ok = false;
                }
            }

            ui64 microseconds =
                std::chrono::duration_cast<std::chrono::microseconds>(end01 - begin01)
                    .count();
            if (log) {
                Info();
                Cerr << "Time for stream load = " << microseconds << "[microseconds]"
                        << Endl;
                Cerr << "Data size =  " << (size / (1024 * 1024))
                        << " [MB]" << Endl;
                Cerr << "Stream load/save/accum speed = "
                        << (size * 1000 * 1000) /
                            (1024 * 1024 * (microseconds + 1))
                        << " MB/sec" << Endl;
                Cerr << Endl;
            }
            delete [] buf;
            return is_ok;
        }

        TSimd<ui8> ShuffleMask(ui32 v[8]) {
            ui8 det[32];
            for (size_t i = 0; i < 32; i += 1) {
                det[i] = v[i / 4] + i % 4;
            }
            return det;
        }

        ~TWorker() = default;
    };

    template<typename TTraits>
    THolder<TWrapWorker> Create() const {
        return MakeHolder<TWorker<TTraits>>();
    };
};

template
__attribute__((target("avx2")))
int TPerfomancer::TWorker<NSimd::TSimdAVX2Traits>::StoreStream(bool);

template
__attribute__((target("sse4.2")))
int TPerfomancer::TWorker<NSimd::TSimdSSE42Traits>::StoreStream(bool);

int main() {
    TPerfomancer tp;
    auto worker = NSimd::SelectSimdTraits(tp);

    bool fine = true;
    fine &= worker->StoreStream(false);
    return !fine;
}
