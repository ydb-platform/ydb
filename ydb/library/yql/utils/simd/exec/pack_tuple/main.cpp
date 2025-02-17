#include <util/generic/ptr.h>
#include <util/system/cpu_id.h>
#include <util/system/types.h>

#include <ydb/library/yql/utils/simd/simd.h>

struct TPerfomancer {
    TPerfomancer() = default;

    struct TWrapWorker {
        virtual int PackTuple(bool log) = 0;
        virtual ~TWrapWorker() = default;
    };

    template<typename TTraits>
    struct TWorker : TWrapWorker {
        template<typename T>
        using TSimd = typename TTraits::template TSimd8<T>;
        TWorker() = default;

        TSimd<ui8> ShuffleMask(ui32 v[8]) {
            ui8 det[32];
            for (size_t i = 0; i < 32; i += 1) {
                det[i] = v[i / 4] == ui32(-1) ? ui8(-1) : 4 * v[i / 4] + i % 4;
            }
            return det;
        }

        int PackTupleImpl(bool log = true) {
            if (TTraits::Size != 32)
                return 1;
            const ui64 NTuples = 32 << 18;
            const ui64 TupleSize =  sizeof(ui32) + sizeof(ui64);

            ui32 *arrUi32 __attribute__((aligned(32))) = new ui32[NTuples];
            ui64 *arrUi64 __attribute__((aligned(32))) = new ui64[NTuples];

            for (ui32 i = 0; i < NTuples; i++) {
                arrUi32[i] = 2 * i;
            }

            for (ui32 i = 0; i < NTuples; i++) {
                arrUi64[i] = 2 * i + 1;
            }

            TSimd<ui8> readReg1, readReg2, readReg1Fwd;

            TSimd<ui8> permReg11, permReg21;
            TSimd<ui8> permReg12, permReg22;

            TSimd<ui8> permIdx11(ShuffleMask((ui32[8]) {0, 0, 0, 1, 0, 0, 0, 0}));
            TSimd<ui8> permIdx12(ShuffleMask((ui32[8]) {2, 0, 0, 3, 0, 0, 0, 0}));
            TSimd<ui8> permIdx1f(ShuffleMask((ui32[8]) {4, 5, 6, 7, 7, 7, 7, 7}));

            TSimd<ui8> permIdx21(ShuffleMask((ui32[8]) {0, 0, 1, 0, 2, 3, 0, 0}));
            TSimd<ui8> permIdx22(ShuffleMask((ui32[8]) {0, 4, 5, 0, 6, 7, 0, 0}));

            ui32 val1[8], val2[8]; // val3[8];

            using TReg = typename TTraits::TRegister;
            TSimd<ui8> blended1, blended2;

            TReg *addr1 = (TReg*) arrUi32;
            TReg *addr2 = (TReg*) arrUi64;

            std::chrono::steady_clock::time_point begin01 =
                std::chrono::steady_clock::now();

            ui64 accum1 = 0;
            ui64 accum2 = 0;
            ui64 accum3 = 0;
            ui64 accum4 = 0;

            const int blendMask = 0b00110110;

            ui32 hash1 = 0;
            ui32 hash2 = 0;
            ui32 hash3 = 0;
            ui32 hash4 = 0;

            for (ui32 i = 0; i < NTuples; i += 8) {
                readReg1 = TSimd<ui8>((ui8*) addr1);
                for (ui32 j = 0; j < 2; j++) {

                    permReg11 = readReg1.Shuffle(permIdx11);
                    readReg2 = TSimd<ui8>((ui8*) addr2);
                    addr2++;
                    permReg21 = readReg2.Shuffle(permIdx21);
                    blended1 = permReg11.template Blend32<blendMask>(permReg21);
                    blended1.Store((ui8*) val1);

                    hash1 = TSimd<ui8>::CRC32u32(0, val1[0]);
                    hash2 = TSimd<ui8>::CRC32u32(0, val1[3]);

                    accum1 += hash1;
                    accum2 += hash2;

                    permReg12 = readReg1.Shuffle(permIdx12);
                    permReg22 = readReg2.Shuffle(permIdx22);
                    blended2 = permReg12.template Blend32<blendMask>(permReg22);
                    blended2.Store((ui8*) val2);

                    hash3 = TSimd<ui8>::CRC32u32(0, val2[0]);
                    hash4 = TSimd<ui8>::CRC32u32(0, val2[3]);

                    accum3 += hash3;
                    accum4 += hash4;

                    readReg1Fwd = readReg1.Shuffle(permIdx1f);
                    readReg1Fwd.Store((ui8*) &readReg1.Value);

                }
                addr1++;
            }


            std::chrono::steady_clock::time_point end01 =
                std::chrono::steady_clock::now();

            Cerr << "Loaded col1 ";
            readReg1.template Log<ui32>(Cerr);
            Cerr << "Loaded col2 ";
            readReg2.template Log<ui32>(Cerr);;
            Cerr << "Permuted col1 ";
            permReg11.template Log<ui32>(Cerr);;
            Cerr << "Permuted col2 ";
            permReg21.template Log<ui32>(Cerr);
            Cerr << "Blended ";
            blended1.template Log<ui32>(Cerr);

            ui64 microseconds =
                std::chrono::duration_cast<std::chrono::microseconds>(end01 - begin01).count();
            if (log) {
                Cerr << "Accum 1 2 hash: " << accum1 << " " << accum2 << " "  << accum3 << " " << accum4 << " "
                << hash1 << " " << hash2 << " " << hash3 << " " << hash4 << Endl;
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
            delete[] arrUi32;
            delete[] arrUi64;

            return 1;
        }

        int PackTuple(bool log = true) override {
            return PackTupleImpl(log);
        }

        ~TWorker() = default;
    };

    template<typename TTraits>
    THolder<TWrapWorker> Create() const {
        return MakeHolder<TWorker<TTraits>>();
    };
};

int main() {
    if (!NX86::HaveAVX2())
        return 0;

    TPerfomancer tp;
    auto worker = tp.Create<NSimd::TSimdAVX2Traits>();

    bool fine = true;
    fine &= worker->PackTuple(false);
    return !fine;
}