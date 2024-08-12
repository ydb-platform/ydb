
#include <util/generic/ptr.h>
#include <util/system/cpu_id.h>
#include <util/system/types.h>

#include <ydb/library/yql/utils/simd/simd.h>

struct TPerfomancer {
    TPerfomancer() = default;

    struct TWrapWorker {
        virtual int TuplesToBucket(bool log) = 0;
        virtual ~TWrapWorker() = default;
    };

    template<typename TTraits>
    struct TWorker : TWrapWorker {
        template<typename T>
        using TSimd = typename TTraits::template TSimd8<T>;
        using TRegister = typename TTraits::TRegister;
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

        int TuplesToBucket(bool log = true) override {
            const ui64 NTuples = 32 << 18;
            const ui64 TupleSize = 4 * sizeof(ui64);

            ui64* arrHash __attribute__((aligned(32))) = new ui64[NTuples];
            ui64* arrData __attribute__((aligned(32))) = new ui64[4 * NTuples];


            for (ui32 i = 0; i < NTuples; i++) {
                arrHash[i] = std::rand();
                arrData[4*i] = i;
                arrData[4*i+1] = i+1;
                arrData[4*i+2] = i+2;
                arrData[4*i+3] = i+3;
            }

            const ui64 NBuckets = 1 << 11;
            const ui64 BufSize = 64;
            const ui64 BucketSize = 1024 * 1024;

            ui64* arrBuckets __attribute__((aligned(32))) = new ui64[BucketSize * NBuckets / sizeof(ui64)];

            for (ui32 i = 0; i < (BucketSize * NBuckets) / sizeof(ui64); i ++) {
                arrBuckets[i] = i;
            }

            ui32 offsets[NBuckets];
            ui32 bigOffsets[NBuckets];
            ui64* accum __attribute__((aligned(32))) = new ui64[NBuckets * (BufSize / sizeof(ui64))];

            for (ui32 i = 0; i < NBuckets; i++ ) {
                offsets[i] = 0;
                bigOffsets[i] = 0;
                accum[i] = 0;
            }

            TSimd<ui8> readReg1;

            TRegister* addr1 = (TRegister*) arrData;

            std::chrono::steady_clock::time_point begin01 =
                std::chrono::steady_clock::now();

            ui64 hash1 = 0;
            ui64 bucketNum = 0;
            ui64* bufAddr;
            ui64* bucketAddr;

            for (ui32 i = 0; i < NTuples / 2; i += 2) {
                hash1 = arrHash[i];
                bucketNum = hash1 & (NBuckets - 1);
                bufAddr = accum + bucketNum * (BufSize / sizeof(ui64));
                readReg1 = TSimd<ui8>((ui8*) addr1);
                ui32 currOffset = offsets[bucketNum];
                readReg1.Store((ui8*) (bufAddr + currOffset));
                offsets[bucketNum] += 4;
                if (currOffset + 4 >= (BufSize / sizeof(ui64))) {
                    offsets[bucketNum] = 0;
                    bucketAddr = arrBuckets + bucketNum * (BucketSize / sizeof(ui64));
                    for (ui32 j = 0; j < BufSize / TTraits::Size; j++ ) {
                        readReg1 = TSimd<ui8>((ui8*) (bufAddr + 4 * j));
                        readReg1.StoreStream((ui8*) (bucketAddr + bigOffsets[bucketNum] + 4 * j));
                    }
                    bigOffsets[bucketNum] += (BufSize / sizeof(ui64));
                }
                addr1++;
            }

            std::chrono::steady_clock::time_point end01 =
                std::chrono::steady_clock::now();

            ui64 microseconds =
                std::chrono::duration_cast<std::chrono::microseconds>(end01 - begin01).count();
            if (log) {
                Info();
                Cerr << "hash accum[12] arrBuckets[12]: " << hash1 << " " << accum[12] << " " << arrBuckets[12] << Endl;
                Cerr << "Time for stream load = " << microseconds << "[microseconds]"
                    << Endl;
                Cerr << "Data size =  " << ((NTuples * TupleSize) / (1024 * 1024))
                    << " [MB]" << Endl;
                Cerr << "Stream load/save/accum speed = "
                    << (NTuples * TupleSize * 1000 * 1000) /
                            (1024 * 1024 * (microseconds + 1))
                    << " MB/sec" << Endl;
                Cerr << Endl;
            }


            delete[] arrHash;
            delete[] arrData;
            delete[] arrBuckets;
            delete[] accum;

            return 1;
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
int TPerfomancer::TWorker<NSimd::TSimdAVX2Traits>::TuplesToBucket(bool);

template
__attribute__((target("sse4.2")))
int TPerfomancer::TWorker<NSimd::TSimdSSE42Traits>::TuplesToBucket(bool);

int main() {
    TPerfomancer tp;
    auto worker = NSimd::SelectSimdTraits(tp);
    return !worker->TuplesToBucket(false);
}
