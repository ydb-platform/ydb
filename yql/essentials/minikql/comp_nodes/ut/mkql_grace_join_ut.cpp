#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/mkql_grace_join_imp.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>

#include <cstring>
#include <vector>
#include <cassert>
#include <cstdlib>
#include <stdlib.h>
#include <random>

#include <util/system/compiler.h>
#include <util/stream/null.h>
#include <util/system/mem_info.h>

namespace NKikimr {
namespace NMiniKQL {

constexpr bool IsVerbose = false;
#define CTEST (IsVerbose ? Cerr : Cnull)


Y_UNIT_TEST_SUITE(TMiniKQLGraceJoinMemTest) {
    Y_UNIT_TEST(TestMem1) {

    const ui64 TupleSize = 1024;
    const ui64 NBuckets = 128;
    const ui64 NTuples = 100000;
    const ui64 BucketSize = (2* NTuples * (TupleSize + 1) ) / NBuckets;

    ui64 *bigTuple = (ui64 * ) malloc(TupleSize * sizeof(ui64));
    ui64 *buckets[NBuckets];
    ui64 tuplesPos[NBuckets];

    std::mt19937_64 rng;
    std::uniform_int_distribution<ui64> dist(0, 10000 - 1);

    for (ui64 i = 0; i < TupleSize; i++)
    {
        bigTuple[i] = dist(rng);
    }

    ui64 bucket = 0;
    ui64 milliseconds = 0;

    const ui64 BitsForData = 30;

    char* a = (char * )malloc(1 << BitsForData);
    char* b = (char *) malloc(1 << BitsForData);
    UNIT_ASSERT(a);
    UNIT_ASSERT(b);

    memset(a, 1, 1 << BitsForData);
    memset(b, 2, 1 << BitsForData);

    std::chrono::steady_clock::time_point begin01 = std::chrono::steady_clock::now();

    memcpy(b, a, 1 << BitsForData);

    std::chrono::steady_clock::time_point end01 = std::chrono::steady_clock::now();

    UNIT_ASSERT(*a == 1);
    UNIT_ASSERT(*b == 1);

    Y_DO_NOT_OPTIMIZE_AWAY(a);
    Y_DO_NOT_OPTIMIZE_AWAY(b);

    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end01 - begin01).count();
    CTEST  << "Time for memcpy = " << microseconds  << "[microseconds]" << Endl;
    CTEST  << "Data size =  " << (1<<BitsForData) / (1024 * 1024) << "[MB]" << Endl;
    CTEST  << "Memcpy speed = " << ( (1<<BitsForData) ) / (microseconds) << "MB/sec" << Endl;
    CTEST  << Endl;

    std::vector<std::vector<ui64>> vec_buckets;
    vec_buckets.resize(NBuckets);
    for (ui64 i = 0; i < NBuckets; i++)
    {
        vec_buckets[i].resize(2 * TupleSize * NTuples / (NBuckets - 1), 0);
        vec_buckets[i].clear();
//        vec_buckets[i].reserve( 2 * TupleSize * NTuples / (NBuckets - 1));
    }

    for (ui64 i = 0; i < NBuckets; i++) {
        buckets[i] = (ui64 * ) malloc( (BucketSize * sizeof(ui64) * 32) / 32);
        memset( buckets[i], 1,  (BucketSize * sizeof(ui64) * 32) / 32);
        tuplesPos[i] = 0;
    }


    std::chrono::steady_clock::time_point begin02 = std::chrono::steady_clock::now();

    std::uniform_int_distribution<ui64> bucketDist(0, NBuckets - 1);
    for (ui64 i = 0; i < NTuples; i++)
    {
        bucket = i % NBuckets;
//        bucket = bucketDist(rng);
        std::vector<ui64> &curr_vec = vec_buckets[bucket];
        curr_vec.insert(curr_vec.end(), bigTuple, bigTuple + TupleSize);
    }

    std::chrono::steady_clock::time_point end02 = std::chrono::steady_clock::now();

    milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(end02 - begin02).count();
    CTEST << "Time for std::insert = " << milliseconds << "[ms]" << Endl;
    CTEST  << "Total MB = " << (TupleSize * NTuples * sizeof(ui64) / (1024 * 1024)) << Endl;
    CTEST  << "std::insert speed = " << (TupleSize * NTuples * sizeof(ui64) * 1000) / (milliseconds * 1024 * 1024) << "MB/sec" << Endl;
    CTEST  << Endl;

    std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();

    for (ui64 i = 0; i < NTuples; i++)
    {

        bucket = i % NBuckets;
//        bucket = bucketDist(rng);

        ui64 * dst = buckets[bucket] + tuplesPos[bucket];
        std::memcpy(dst, bigTuple, TupleSize*sizeof(ui64));
        tuplesPos[bucket] += TupleSize;
    }

    std::chrono::steady_clock::time_point end03 = std::chrono::steady_clock::now();

     milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(end03 - begin03).count();
    CTEST  << "Time for std::memcpy = " << milliseconds << "[ms]" << Endl;
    CTEST  << "Total MB = " << (TupleSize * NTuples * sizeof(ui64) / (1024 * 1024)) << Endl;
    CTEST  << "std:memcpy speed = " << (TupleSize * NTuples * sizeof(ui64) * 1000) / (milliseconds * 1024 * 1024) << "MB/sec" << Endl;
    CTEST  << Endl;

    for (ui64 i = 0; i < NBuckets; i++) {
        tuplesPos[i] = 0;
    }


    std::chrono::steady_clock::time_point begin04 = std::chrono::steady_clock::now();

    for (ui64 i = 0; i < NTuples; i++)
    {
        bucket = bucketDist(rng);

        ui64 * dst = buckets[bucket] + tuplesPos[bucket];

        ui64 *dst1 = dst + 1;
        ui64 *dst2 = dst + 2;
        ui64 *dst3 = dst + 3;
        ui64 *src = bigTuple;
        ui64 *src1 = bigTuple + 1;
        ui64 *src2 = bigTuple + 2;
        ui64 *src3 = bigTuple + 3;

        for (ui64 i = 0; i < TupleSize; i += 4)
        {
            *dst++ = *src++;
            *dst1++ = *src1++;
            *dst2++ = *src2++;
            *dst3++ = *src3++;

        }
        tuplesPos[bucket] += TupleSize;
    }

    std::chrono::steady_clock::time_point end04 = std::chrono::steady_clock::now();

    milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(end04 - begin04).count();
    CTEST  << "Time for loop copy = " << milliseconds << "[ms]" << Endl;
    CTEST  << "Total MB = " << (TupleSize * NTuples * sizeof(ui64) / (1024 * 1024)) << Endl;
    CTEST  << "Loop copy speed = " << (TupleSize * NTuples * sizeof(ui64) * 1000) / (milliseconds * 1024 * 1024) << "MB/sec" << Endl;
    CTEST  << Endl;

    for (ui64 i = 0; i < NBuckets; i++) {
        free(buckets[i]);
    }

    free(b);
    free(a);
    free(bigTuple);


    UNIT_ASSERT(true);

    }

}


Y_UNIT_TEST_SUITE(TMiniKQLGraceJoinImpTest) {
    constexpr ui64 BigTableTuples = 600000;
    constexpr ui64 SmallTableTuples = 150000;
    constexpr ui64 BigTupleSize = 40;

    Y_UNIT_TEST_TWIN(TestTryToPreallocateMemoryForJoin, EXCEPTION) {
        TSetup<false> setup;
        ui64 tuple[11] = {0,1,2,3,4,5,6,7,8,9,10};
        ui32 strSizes[2] = {4, 4};
        char * strVals[] = {(char *)"aaaaa", (char *)"bbbb"};

        char * bigStrVal[] = {(char *)"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                             (char *)"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"};
        ui32 bigStrSize[2] = {151, 151};


        GraceJoin::TTable bigTable(nullptr,0,1,1,1,1);
        GraceJoin::TTable smallTable(nullptr,0,1,1,1,1);
        GraceJoin::TTable joinTable(nullptr,0,1,1,1,1);

        const ui64 TupleSize = 1024;

        ui64 bigTuple[TupleSize];

        std::mt19937_64 rng; // deterministic PRNG

        std::uniform_int_distribution<ui64> dist(0, 10000 - 1);

        for (ui64 i = 0; i < TupleSize; i++) {
            bigTuple[i] = dist(rng);
        }


        std::uniform_int_distribution<ui64> smallDist(0, SmallTableTuples - 1);

        smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

        for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
            tuple[1] = smallDist(rng);
            tuple[2] = tuple[1];
            smallTable.AddTuple(tuple, strVals, strSizes);
        }


        for ( ui64 i = 0; i < BigTableTuples; i++) {
            tuple[1] = smallDist(rng);
            tuple[2] = tuple[1];
            bigTable.AddTuple(tuple, strVals, strSizes);
        }

        ui64 allocationsCount = 0;
        if (EXCEPTION) {
            TlsAllocState->SetLimit(1);
            TlsAllocState->SetIncreaseMemoryLimitCallback([&allocationsCount](ui64, ui64 required) {
                // Preallocate memory for some buckets before fail
                if (allocationsCount++ > 5) {
                    throw TMemoryLimitExceededException();
                }
                TlsAllocState->SetLimit(required);
            });
        }

        bool preallocationResult = joinTable.TryToPreallocateMemoryForJoin(smallTable, bigTable, EJoinKind::Inner, true, true);
        UNIT_ASSERT_EQUAL(preallocationResult, !EXCEPTION);
    }

    Y_UNIT_TEST_LLVM(TestImp1) {
            TSetup<LLVM> setup;
            ui64 tuple[11] = {0,1,2,3,4,5,6,7,8,9,10};
            ui32 strSizes[2] = {4, 4};
            char * strVals[] = {(char *)"aaaaa", (char *)"bbbb"};

            char * bigStrVal[] = {(char *)"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                 (char *)"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"};
            ui32 bigStrSize[2] = {151, 151};


            NMemInfo::TMemInfo mi = NMemInfo::GetMemInfo();
            CTEST << "Mem usage before tables tuples added (MB): " << mi.RSS / (1024 * 1024) << Endl;

            GraceJoin::TTable bigTable(nullptr,0,1,1,1,1);
            GraceJoin::TTable smallTable(nullptr,0,1,1,1,1);
            GraceJoin::TTable joinTable(nullptr,0,1,1,1,1);

            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            const ui64 TupleSize = 1024;

            ui64 bigTuple[TupleSize];

            std::mt19937_64 rng; // deterministic PRNG

            std::uniform_int_distribution<ui64> dist(0, 10000 - 1);

            for (ui64 i = 0; i < TupleSize; i++) {
                bigTuple[i] = dist(rng);
            }

            ui64 milliseconds = 0;



            std::uniform_int_distribution<ui64> smallDist(0, SmallTableTuples - 1);

            std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();

            smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

            for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                tuple[1] = smallDist(rng);
                tuple[2] = tuple[1];
                smallTable.AddTuple(tuple, strVals, strSizes);
            }


            for ( ui64 i = 0; i < BigTableTuples; i++) {
                tuple[1] = smallDist(rng);
                tuple[2] = tuple[1];
                bigTable.AddTuple(tuple, strVals, strSizes);
            }

            std::chrono::steady_clock::time_point end03 = std::chrono::steady_clock::now();
            milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(end03 - begin03).count();
            CTEST << "Time for hash = " << milliseconds << "[ms]" << Endl;
            CTEST << "Adding tuples speed: " << (BigTupleSize * (BigTableTuples + SmallTableTuples) * 1000) / ( milliseconds * 1024 * 1024) << "MB/sec" << Endl;
            CTEST << Endl;

            mi = NMemInfo::GetMemInfo();
            CTEST << "Mem usage after tables tuples added (MB): " << mi.RSS / (1024 * 1024) << Endl;


            bigTable.Clear();
            smallTable.Clear();

            begin03 = std::chrono::steady_clock::now();

            smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

            for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                tuple[1] = smallDist(rng);
                tuple[2] = tuple[1];
                smallTable.AddTuple(tuple, strVals, strSizes);
            }


            for ( ui64 i = 0; i < BigTableTuples; i++) {
                tuple[1] = smallDist(rng);
                tuple[2] = tuple[1];
                bigTable.AddTuple(tuple, strVals, strSizes);
            }

            end03 = std::chrono::steady_clock::now();
            milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(end03 - begin03).count();
            CTEST << "Time for hash = " << milliseconds << "[ms]" << Endl;
            CTEST << "Adding tuples speed: " << (BigTupleSize * (BigTableTuples + SmallTableTuples) * 1000) / ( milliseconds * 1024 * 1024) << "MB/sec" << Endl;
            CTEST << Endl;

            mi = NMemInfo::GetMemInfo();
            CTEST << "Mem usage after tables tuples added (MB): " << mi.RSS / (1024 * 1024) << Endl;


            std::vector<ui64> vals1, vals2;
            std::vector<char *> strVals1, strVals2;
            std::vector<ui32> strSizes1, strSizes2;
            GraceJoin::TupleData td1, td2;
            vals1.resize(100);
            vals2.resize(100);
            strVals1.resize(100);
            strVals2.resize(100);
            strSizes1.resize(100);
            strSizes2.resize(100);
            td1.IntColumns = vals1.data();
            td1.StrColumns = strVals1.data();
            td1.StrSizes = strSizes1.data();
            td2.IntColumns = vals2.data();
            td2.StrColumns = strVals2.data();
            td2.StrSizes = strSizes2.data();

            ui64 numBigTuples = 0;
            bigTable.ResetIterator();

            std::chrono::steady_clock::time_point begin04 = std::chrono::steady_clock::now();

            while(bigTable.NextTuple(td1)) { numBigTuples++; }

            CTEST << "Num of big tuples 1: " << numBigTuples << Endl;

            std::chrono::steady_clock::time_point end04 = std::chrono::steady_clock::now();
            CTEST << "Time for get 1 = " << std::chrono::duration_cast<std::chrono::milliseconds>(end04 - begin04).count() << "[ms]" << Endl;
            CTEST << Endl;

            numBigTuples = 0;
            bigTable.ResetIterator();

            std::chrono::steady_clock::time_point begin041 = std::chrono::steady_clock::now();

            while(bigTable.NextTuple(td2)) { numBigTuples++; }

            CTEST << "Num of big tuples 2: " << numBigTuples << Endl;

            std::chrono::steady_clock::time_point end041 = std::chrono::steady_clock::now();
            CTEST << "Time for get 2 = " << std::chrono::duration_cast<std::chrono::milliseconds>(end041 - begin041).count() << "[ms]" << Endl;
            CTEST << Endl;


            std::chrono::steady_clock::time_point begin05 = std::chrono::steady_clock::now();

            joinTable.Join(smallTable,bigTable);

            std::chrono::steady_clock::time_point end05 = std::chrono::steady_clock::now();
            CTEST << "Time for join = " << std::chrono::duration_cast<std::chrono::milliseconds>(end05 - begin05).count() << "[ms]" << Endl;
            CTEST << Endl;

            mi = NMemInfo::GetMemInfo();
            CTEST << "Mem usage after tables join (MB): " << mi.RSS / (1024 * 1024) << Endl;


            joinTable.ResetIterator();
            ui64 numJoinedTuples = 0;


            std::chrono::steady_clock::time_point begin042 = std::chrono::steady_clock::now();

            while(joinTable.NextJoinedData(td1, td2)) { numJoinedTuples++; }

            CTEST << "Num of joined tuples : " << numJoinedTuples << Endl;

            std::chrono::steady_clock::time_point end042 = std::chrono::steady_clock::now();
            CTEST << "Time for get joined tuples: = " << std::chrono::duration_cast<std::chrono::milliseconds>(end042 - begin042).count() << "[ms]" << Endl;
            CTEST << Endl;


            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            CTEST << "Time difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << Endl;
            CTEST << Endl;


    }

    Y_UNIT_TEST_LLVM(TestImp1Batch) {
            TSetup<LLVM> setup;
            ui64 tuple[11] = {0,1,2,3,4,5,6,7,8,9,10};
            ui32 strSizes[2] = {4, 4};
            char * strVals[] = {(char *)"aaaaa", (char *)"bbbb"};

            char * bigStrVal[] = {(char *)"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                 (char *)"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"};
            ui32 bigStrSize[2] = {151, 151};


            NMemInfo::TMemInfo mi = NMemInfo::GetMemInfo();
            CTEST << "Mem usage before tables tuples added (MB): " << mi.RSS / (1024 * 1024) << Endl;

            GraceJoin::TTable bigTable(nullptr,0,1,1,1,1);
            GraceJoin::TTable smallTable(nullptr,0,1,1,1,1);
            GraceJoin::TTable joinTable(nullptr,0,1,1,1,1);

            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            const ui64 TupleSize = 1024;

            ui64 bigTuple[TupleSize];

            std::mt19937_64 rng; // deterministic PRNG

            std::uniform_int_distribution<ui64> dist(0, 10000 - 1);

            for (ui64 i = 0; i < TupleSize; i++) {
                bigTuple[i] = dist(rng);
            }

            ui64 millisecondsAdd = 0;
            ui64 millisecondsJoin = 0;
            ui64 millisecondsNextJoinTuple = 0;
            ui64 millisecondsNextTuple = 0;



            const ui64 BatchTuples = 100000;

            std::uniform_int_distribution<ui64> smallDist(0, SmallTableTuples - 1);

            {
                std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();

                smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

                for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                    tuple[1] = smallDist(rng);
                    tuple[2] = tuple[1];
                    smallTable.AddTuple(tuple, strVals, strSizes);
                }

                std::chrono::steady_clock::time_point end03 = std::chrono::steady_clock::now();
                millisecondsAdd += std::chrono::duration_cast<std::chrono::milliseconds>(end03 - begin03).count();
            }


            for ( ui64 pos = 0; pos < BigTableTuples; ) {
                std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();
                ui64 limit = std::min(pos + BatchTuples, BigTableTuples);
                for (; pos < limit; ++pos) {
                    tuple[1] = smallDist(rng);
                    tuple[2] = tuple[1];
                    bigTable.AddTuple(tuple, strVals, strSizes);
                }
                bigTable.Clear();
                std::chrono::steady_clock::time_point end03 = std::chrono::steady_clock::now();
                millisecondsAdd += std::chrono::duration_cast<std::chrono::milliseconds>(end03 - begin03).count();
            }

            CTEST << "Time for hash = " << millisecondsAdd << "[ms]" << Endl;
            CTEST << "Adding tuples speed: " << (BigTupleSize * (BigTableTuples + SmallTableTuples) * 1000) / ( millisecondsAdd * 1024 * 1024) << "MB/sec" << Endl;
            CTEST << Endl;

            mi = NMemInfo::GetMemInfo();
            CTEST << "Mem usage after tables tuples added (MB): " << mi.RSS / (1024 * 1024) << Endl;
            millisecondsAdd = 0;

            smallTable.Clear();

            {
                auto begin03 = std::chrono::steady_clock::now();

                smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

                for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                    tuple[1] = smallDist(rng);
                    tuple[2] = tuple[1];
                    smallTable.AddTuple(tuple, strVals, strSizes);
                }

                auto end03 = std::chrono::steady_clock::now();
                millisecondsAdd += std::chrono::duration_cast<std::chrono::milliseconds>(end03 - begin03).count();

            }
            std::vector<ui64> vals1, vals2;
            std::vector<char *> strVals1, strVals2;
            std::vector<ui32> strSizes1, strSizes2;
            GraceJoin::TupleData td1, td2;
            vals1.resize(100);
            vals2.resize(100);
            strVals1.resize(100);
            strVals2.resize(100);
            strSizes1.resize(100);
            strSizes2.resize(100);
            td1.IntColumns = vals1.data();
            td1.StrColumns = strVals1.data();
            td1.StrSizes = strSizes1.data();
            td2.IntColumns = vals2.data();
            td2.StrColumns = strVals2.data();
            td2.StrSizes = strSizes2.data();

            ui64 numJoinedTuples = 0;
            ui64 numBigTuples = 0;

            for ( ui64 pos = 0; pos < BigTableTuples; ) {
                std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();
                bigTable.Clear();
                ui64 limit = std::min(pos + BatchTuples, BigTableTuples);
                for (; pos < limit; ++pos) {
                    tuple[1] = smallDist(rng);
                    tuple[2] = tuple[1];
                    bigTable.AddTuple(tuple, strVals, strSizes);
                }
                auto end03 = std::chrono::steady_clock::now();
                millisecondsAdd += std::chrono::duration_cast<std::chrono::milliseconds>(end03 - begin03).count();

                bigTable.ResetIterator();

                std::chrono::steady_clock::time_point begin04 = std::chrono::steady_clock::now();

                while(bigTable.NextTuple(td1)) { numBigTuples++; }
                std::chrono::steady_clock::time_point end04 = std::chrono::steady_clock::now();

                millisecondsNextTuple += std::chrono::duration_cast<std::chrono::milliseconds>(end04 - begin04).count();


                std::chrono::steady_clock::time_point begin05 = std::chrono::steady_clock::now();

                joinTable.Join(smallTable, bigTable, EJoinKind::Inner, false, pos < BigTableTuples);

                std::chrono::steady_clock::time_point end05 = std::chrono::steady_clock::now();
                millisecondsJoin += std::chrono::duration_cast<std::chrono::milliseconds>(end05 - begin05).count();


                joinTable.ResetIterator();


                std::chrono::steady_clock::time_point begin042 = std::chrono::steady_clock::now();
                while(joinTable.NextJoinedData(td1, td2)) { numJoinedTuples++; }

                std::chrono::steady_clock::time_point end042 = std::chrono::steady_clock::now();
                millisecondsNextJoinTuple += std::chrono::duration_cast<std::chrono::milliseconds>(end042 - begin042).count();
            }
            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

            CTEST << "Num of big tuples 1: " << numBigTuples << Endl;

            CTEST << "Time for get 1 = " << millisecondsNextTuple << "[ms]" << Endl;
            CTEST << Endl;
            CTEST << "Time for join = " << millisecondsJoin << "[ms]" << Endl;
            CTEST << Endl;
            CTEST << "Time for get joined tuples: = " << millisecondsNextJoinTuple << "[ms]" << Endl;
            CTEST << Endl;

            mi = NMemInfo::GetMemInfo();
            CTEST << "Mem usage after tables add and join (MB): " << mi.RSS / (1024 * 1024) << Endl;
            CTEST << "Time for hash = " << millisecondsAdd << "[ms]" << Endl;
            CTEST << "Adding tuples speed: " << (BigTupleSize * (BigTableTuples + SmallTableTuples) * 1000) / ( millisecondsAdd * 1024 * 1024) << "MB/sec" << Endl;
            CTEST << Endl;

            CTEST << "Num of joined tuples : " << numJoinedTuples << Endl;



            CTEST << "Time difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << Endl;
            CTEST << Endl;


    }
}

Y_UNIT_TEST_SUITE(TMiniKQLGraceJoinAnyTest) {
    Y_UNIT_TEST_LLVM(TestImp2) {
            TSetup<LLVM> setup;
            ui64 tuple[11] = {0,1,2,3,4,5,6,7,8,9,10};
            ui32 strSizes[2] = {4, 4};
            char * strVals[] = {(char *)"aaaaa", (char *)"bbbb"};

            char * bigStrVal[] = {(char *)"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                 (char *)"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"};
            ui32 bigStrSize[2] = {151, 151};



            GraceJoin::TTable bigTable  (nullptr, 0, 1,1,1,1,0,0,1, nullptr, true);
            GraceJoin::TTable smallTable(nullptr, 0, 1,1,1,1,0,0,1, nullptr, true);
            GraceJoin::TTable joinTable (nullptr, 0, 1,1,1,1,0,0,1, nullptr, true);

            std::mt19937_64 rng;
            std::uniform_int_distribution<ui64> dist(0, 10000 - 1);

            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            const ui64 TupleSize = 1024;

            ui64 bigTuple[TupleSize];

            for (ui64 i = 0; i < TupleSize; i++) {
                bigTuple[i] = dist(rng);
            }

            ui64 milliseconds = 0;



            const ui64 BigTableTuples = 600000;
            const ui64 SmallTableTuples = 150000;
            const ui64 BigTupleSize = 40;

            std::uniform_int_distribution<ui64> smallDist(0, SmallTableTuples - 1);

            std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();


            smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

            for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                tuple[1] = i;
                tuple[2] = tuple[1];
                smallTable.AddTuple(tuple, strVals, strSizes);
            }

            for ( ui64 i = 0; i < BigTableTuples; i++) {
                tuple[1] = i % SmallTableTuples;
                tuple[2] = tuple[1];
                bigTable.AddTuple(tuple, strVals, strSizes);
            }

            std::chrono::steady_clock::time_point end03 = std::chrono::steady_clock::now();
            milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(end03 - begin03).count();
            CTEST << "Time for hash = " << milliseconds << "[ms]" << Endl;
            CTEST << "Adding tuples speed: " << (BigTupleSize * (BigTableTuples + SmallTableTuples) * 1000) / ( milliseconds * 1024 * 1024) << "MB/sec" << Endl;
            CTEST << Endl;

            std::vector<ui64> vals1, vals2;
            std::vector<char *> strVals1, strVals2;
            std::vector<ui32> strSizes1, strSizes2;
            GraceJoin::TupleData td1, td2;
            vals1.resize(100);
            vals2.resize(100);
            strVals1.resize(100);
            strVals2.resize(100);
            strSizes1.resize(100);
            strSizes2.resize(100);
            td1.IntColumns = vals1.data();
            td1.StrColumns = strVals1.data();
            td1.StrSizes = strSizes1.data();
            td2.IntColumns = vals2.data();
            td2.StrColumns = strVals2.data();
            td2.StrSizes = strSizes2.data();

            ui64 numBigTuples = 0;
            bigTable.ResetIterator();

            std::chrono::steady_clock::time_point begin04 = std::chrono::steady_clock::now();

            while(bigTable.NextTuple(td1)) { numBigTuples++; }

            CTEST << "Num of big tuples 1: " << numBigTuples << Endl;

            std::chrono::steady_clock::time_point end04 = std::chrono::steady_clock::now();
            CTEST << "Time for get 1 = " << std::chrono::duration_cast<std::chrono::milliseconds>(end04 - begin04).count() << "[ms]" << Endl;
            CTEST << Endl;

            numBigTuples = 0;
            bigTable.ResetIterator();

            std::chrono::steady_clock::time_point begin041 = std::chrono::steady_clock::now();

            while(bigTable.NextTuple(td2)) { numBigTuples++; }

            CTEST << "Num of big tuples 2: " << numBigTuples << Endl;

            std::chrono::steady_clock::time_point end041 = std::chrono::steady_clock::now();
            CTEST << "Time for get 2 = " << std::chrono::duration_cast<std::chrono::milliseconds>(end041 - begin041).count() << "[ms]" << Endl;
            CTEST << Endl;


            std::chrono::steady_clock::time_point begin05 = std::chrono::steady_clock::now();

            joinTable.Join(smallTable,bigTable);

            std::chrono::steady_clock::time_point end05 = std::chrono::steady_clock::now();
            CTEST << "Time for join = " << std::chrono::duration_cast<std::chrono::milliseconds>(end05 - begin05).count() << "[ms]" << Endl;
            CTEST << Endl;

            joinTable.ResetIterator();
            ui64 numJoinedTuples = 0;


            std::chrono::steady_clock::time_point begin042 = std::chrono::steady_clock::now();

            while(joinTable.NextJoinedData(td1, td2)) { numJoinedTuples++; }

            CTEST << "Num of joined tuples : " << numJoinedTuples << Endl;

            std::chrono::steady_clock::time_point end042 = std::chrono::steady_clock::now();
            CTEST << "Time for get joined tuples: = " << std::chrono::duration_cast<std::chrono::milliseconds>(end042 - begin042).count() << "[ms]" << Endl;
            CTEST << Endl;


            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            CTEST << "Time difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << Endl;
            CTEST << Endl;


    }
}

Y_UNIT_TEST_SUITE(TMiniKQLGraceSelfJoinTest) {
    Y_UNIT_TEST_LLVM(TestImp3) {
            TSetup<LLVM> setup;
            ui64 tuple[11] = {0,1,2,3,4,5,6,7,8,9,10};
            ui32 strSizes[2] = {4, 4};
            char * strVals[] = {(char *)"aaaaa", (char *)"bbbb"};

            char * bigStrVal[] = {(char *)"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                 (char *)"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"};
            ui32 bigStrSize[2] = {151, 151};



            GraceJoin::TTable bigTable  (nullptr, 0, 1,1,1,1,0,0,1, nullptr, false);
            GraceJoin::TTable smallTable(nullptr, 0, 1,1,1,1,0,0,1, nullptr, false);
            GraceJoin::TTable joinTable (nullptr, 0, 1,1,1,1,0,0,1, nullptr, false);

            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            const ui64 TupleSize = 1024;

            ui64 bigTuple[TupleSize];

            std::mt19937_64 rng;
            std::uniform_int_distribution<ui64> dist(0, 10000 - 1);

            for (ui64 i = 0; i < TupleSize; i++) {
                bigTuple[i] = dist(rng);
            }

            ui64 milliseconds = 0;



            const ui64 BigTableTuples = 600000;
            const ui64 SmallTableTuples = 150000;
            const ui64 BigTupleSize = 40;

            std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();


            smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

            for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                tuple[1] = i;
                tuple[2] = tuple[1];
                smallTable.AddTuple(tuple, strVals, strSizes);
            }

            for ( ui64 i = 0; i < BigTableTuples; i++) {
                tuple[1] = i % SmallTableTuples;
                tuple[2] = tuple[1];
                bigTable.AddTuple(tuple, strVals, strSizes);
            }

            std::chrono::steady_clock::time_point end03 = std::chrono::steady_clock::now();
            milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(end03 - begin03).count();
            CTEST << "Time for hash = " << milliseconds << "[ms]" << Endl;
            CTEST << "Adding tuples speed: " << (BigTupleSize * (BigTableTuples + SmallTableTuples) * 1000) / ( milliseconds * 1024 * 1024) << "MB/sec" << Endl;
            CTEST << Endl;

            std::vector<ui64> vals1, vals2;
            std::vector<char *> strVals1, strVals2;
            std::vector<ui32> strSizes1, strSizes2;
            GraceJoin::TupleData td1, td2;
            vals1.resize(100);
            vals2.resize(100);
            strVals1.resize(100);
            strVals2.resize(100);
            strSizes1.resize(100);
            strSizes2.resize(100);
            td1.IntColumns = vals1.data();
            td1.StrColumns = strVals1.data();
            td1.StrSizes = strSizes1.data();
            td2.IntColumns = vals2.data();
            td2.StrColumns = strVals2.data();
            td2.StrSizes = strSizes2.data();

            ui64 numBigTuples = 0;
            bigTable.ResetIterator();

            std::chrono::steady_clock::time_point begin04 = std::chrono::steady_clock::now();

            while(bigTable.NextTuple(td1)) { numBigTuples++; }

            CTEST << "Num of big tuples 1: " << numBigTuples << Endl;

            std::chrono::steady_clock::time_point end04 = std::chrono::steady_clock::now();
            CTEST << "Time for get 1 = " << std::chrono::duration_cast<std::chrono::milliseconds>(end04 - begin04).count() << "[ms]" << Endl;
            CTEST << Endl;

            numBigTuples = 0;
            bigTable.ResetIterator();

            std::chrono::steady_clock::time_point begin041 = std::chrono::steady_clock::now();

            while(bigTable.NextTuple(td2)) { numBigTuples++; }

            CTEST << "Num of big tuples 2: " << numBigTuples << Endl;

            std::chrono::steady_clock::time_point end041 = std::chrono::steady_clock::now();
            CTEST << "Time for get 2 = " << std::chrono::duration_cast<std::chrono::milliseconds>(end041 - begin041).count() << "[ms]" << Endl;
            CTEST << Endl;


            std::chrono::steady_clock::time_point begin05 = std::chrono::steady_clock::now();

            joinTable.Join(bigTable,bigTable);

            std::chrono::steady_clock::time_point end05 = std::chrono::steady_clock::now();
            CTEST << "Time for join = " << std::chrono::duration_cast<std::chrono::milliseconds>(end05 - begin05).count() << "[ms]" << Endl;
            CTEST << Endl;

            joinTable.ResetIterator();
            ui64 numJoinedTuples = 0;


            std::chrono::steady_clock::time_point begin042 = std::chrono::steady_clock::now();

            while(joinTable.NextJoinedData(td1, td2)) { numJoinedTuples++; }

            CTEST << "Num of joined tuples : " << numJoinedTuples << Endl;

            std::chrono::steady_clock::time_point end042 = std::chrono::steady_clock::now();
            CTEST << "Time for get joined tuples: = " << std::chrono::duration_cast<std::chrono::milliseconds>(end042 - begin042).count() << "[ms]" << Endl;
            CTEST << Endl;


            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            CTEST << "Time difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << Endl;
            CTEST << Endl;


    }
}

Y_UNIT_TEST_SUITE(TMiniKQLSelfJoinTest) {

    Y_UNIT_TEST_LLVM_SPILLING(TestInner1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(4);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3}),
                pb.NewTuple({key4, payload4})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceSelfJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceSelfJoin", "GraceSelfJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), TString(t1.AsStringRef()) )];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("C"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("X"), TString("C"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("X"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("B"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("A"), TString("A"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 6);

        }


    }

    Y_UNIT_TEST_LLVM_SPILLING(TestDiffKeys) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(4);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto key11 = pb.NewDataLiteral<ui32>(1);
            const auto key21 = pb.NewDataLiteral<ui32>(1);
            const auto key31 = pb.NewDataLiteral<ui32>(2);
            const auto key41 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, key11, payload1}),
                pb.NewTuple({key2, key21, payload2}),
                pb.NewTuple({key3, key31, payload3}),
                pb.NewTuple({key4, key41, payload4})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceSelfJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                EJoinKind::Inner, {0U}, {1U}, {2U, 0U}, {2U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceSelfJoin", "GraceSelfJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), TString(t1.AsStringRef()) )];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("A"), TString("A"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("A"), TString("B"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("C"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 3);
        }


    }


}

Y_UNIT_TEST_SUITE(TMiniKQLGraceJoinTest) {

    Y_UNIT_TEST_LLVM_SPILLING(TestInner1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(4);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), TString(t1.AsStringRef()) )];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Z"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 3);
        }


    }

    Y_UNIT_TEST_LLVM_SPILLING(TestInnerDoubleCondition1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(4);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType1 = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto tupleType2 = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });


            const auto list1 = pb.NewList(tupleType1, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType2, {
                pb.NewTuple({key2, key2, payload4}),
                pb.NewTuple({key3, key2, payload5}),
                pb.NewTuple({key4, key1, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                EJoinKind::Inner, {0U, 0U}, {0U, 1U}, {1U, 0U}, {2U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), TString(t1.AsStringRef()) )];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 1);

        }


    }

    Y_UNIT_TEST_LLVM_SPILLING(TestInnerManyKeyStrings) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A1");
            const auto key2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A2");
            const auto key3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A3");
            const auto key4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B1");
            const auto key5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B2");
            const auto key6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B3");


            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType1 = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto tupleType2 = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });


            const auto list1 = pb.NewList(tupleType1, {
                pb.NewTuple({key1, key4, payload1}),
                pb.NewTuple({key2, key5, payload2}),
                pb.NewTuple({key3, key6, payload3})
            });

            const auto list2 = pb.NewList(tupleType2, {
                pb.NewTuple({key4, key1, payload4}),
                pb.NewTuple({key5, key2, payload5}),
                pb.NewTuple({key6, key6, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                EJoinKind::Inner, {0U, 1U}, {1U, 0U}, {1U, 0U}, {2U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), TString(t1.AsStringRef()) )];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B2"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B1"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);

        }


    }

    Y_UNIT_TEST_LLVM_SPILLING(TestInnerManyKeyUuid) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("A1A1A1A1A1A1A1A1");
            const auto key2 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("A2A2A2A2A2A2A2A2");
            const auto key3 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("A3A3A3A3A3A3A3A3");
            const auto key4 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("B1B1B1B1B1B1B1B1");
            const auto key5 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("B2B2B2B2B2B2B2B2");
            const auto key6 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("B3B3B3B3B3B3B3B3");


            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("AAAAAAAAAAAAAAAA");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("BBBBBBBBBBBBBBBB");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("CCCCCCCCCCCCCCCC");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("XXXXXXXXXXXXXXXX");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("YYYYYYYYYYYYYYYY");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::Uuid>("ZZZZZZZZZZZZZZZZ");

            const auto tupleType1 = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<NUdf::TUuid>::Id),
                pb.NewDataType(NUdf::TDataType<NUdf::TUuid>::Id),
                pb.NewDataType(NUdf::TDataType<NUdf::TUuid>::Id)
            });

            const auto tupleType2 = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<NUdf::TUuid>::Id),
                pb.NewDataType(NUdf::TDataType<NUdf::TUuid>::Id),
                pb.NewDataType(NUdf::TDataType<NUdf::TUuid>::Id)
            });


            const auto list1 = pb.NewList(tupleType1, {
                pb.NewTuple({key1, key4, payload1}),
                pb.NewTuple({key2, key5, payload2}),
                pb.NewTuple({key3, key6, payload3})
            });

            const auto list2 = pb.NewList(tupleType2, {
                pb.NewTuple({key4, key1, payload4}),
                pb.NewTuple({key5, key2, payload5}),
                pb.NewTuple({key6, key6, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<NUdf::TUuid>::Id),
                pb.NewDataType(NUdf::TDataType<NUdf::TUuid>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                EJoinKind::Inner, {0U, 1U}, {1U, 0U}, {1U, 0U}, {2U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), TString(t1.AsStringRef()) )];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(
                        TString("B2B2B2B2B2B2B2B2"),
                        TString("YYYYYYYYYYYYYYYY")
                        )], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(
                        TString("B1B1B1B1B1B1B1B1"),
                        TString("XXXXXXXXXXXXXXXX")
                        )], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);

        }


    }


    Y_UNIT_TEST_LLVM_SPILLING(TestInnerStringKey1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("1");
            const auto key2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("2");
            const auto key3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("4");
            const auto key4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("4");
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), TString(t1.AsStringRef()) )];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Z"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 3);

        }


    }



    Y_UNIT_TEST_LLVM_SPILLING(TMiniKQLGraceJoinTestInnerMulti1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));


            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), TString(t1.AsStringRef()) )];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 4);
        }
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestLeft1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(3);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));


            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Left, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;
            // use empty TString as replacement for NULL

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                UNIT_ASSERT(!t0 || !t0.AsStringRef().Empty()); // ensure no empty strings
                UNIT_ASSERT(!t1 || !t1.AsStringRef().Empty());
                ++u[std::make_pair(t0 ? TString(t0.AsStringRef()) : TString(), t1 ? TString(t1.AsStringRef()) : TString())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("A"), TString())], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 3);
        }
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestLeftMulti1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Left, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                UNIT_ASSERT(!t0 || !t0.AsStringRef().Empty()); // ensure no empty strings
                UNIT_ASSERT(!t1 || !t1.AsStringRef().Empty());
                ++u[std::make_pair(t0 ? TString(t0.AsStringRef()) : TString(), t1 ? TString(t1.AsStringRef()) : TString())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("A"), TString())], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 5);
        }
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestLeftSemi1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));


            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::LeftSemi, {0U}, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, ui32>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), t1.Get<ui32>())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), 2)], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), 2)], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);
        }
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestLeftOnly1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto key5 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("D");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3}),
                pb.NewTuple({key4, payload4}),
                pb.NewTuple({key5, payload4})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload5}),
                pb.NewTuple({key3, payload6}),
                pb.NewTuple({key4, payload7})
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::LeftOnly, {0U}, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, ui32>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), t1.Get<ui32>())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("D"), 4)], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("A"), 1)], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);
        }
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestLeftSemiWithNullKey1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key0 = pb.NewEmptyOptional(pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
            const auto key1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
            const auto key2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key3 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key4 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id, true),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload4}),
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload3}),
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));


            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::LeftSemi, {0U}, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            std::map<std::pair<TString, ui32>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), t1.Get<ui32>())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), 2)], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), 2)], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);
        }
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestLeftOnlyWithNullKey1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key0 = pb.NewEmptyOptional(pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
            const auto key1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
            const auto key2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key3 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key4 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id, true),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload4}),
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload3}),
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));


            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::LeftOnly, {0U}, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;
            std::map<std::pair<TString, ui64>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), t1 ? t1.Get<ui32>() : std::numeric_limits<ui64>::max())];
                // replace NULL with <ui64>::max()
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("A"), 1)], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("X"), std::numeric_limits<ui64>::max())], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);

        }
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestRight1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(3);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));


            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Right, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;
            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                UNIT_ASSERT(!t0 || !t0.AsStringRef().Empty()); // ensure no empty strings
                UNIT_ASSERT(!t1 || !t1.AsStringRef().Empty());
                ++u[std::make_pair(t0 ? TString(t0.AsStringRef()) : TString(), t1 ? TString(t1.AsStringRef()) : TString())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString(), TString("Z"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 3);

        }
    }


    Y_UNIT_TEST_LLVM_SPILLING(TestRightOnly1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::RightOnly, {0U}, {0U}, {}, {1U, 0U, 0U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;
            std::map<std::pair<TString, ui32>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), t1.Get<ui32>())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("Z"), 3)], 1);
            UNIT_ASSERT_EQUAL(u.size(), 1);
        }
    }



    Y_UNIT_TEST_LLVM_SPILLING(TestRightSemi1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));


            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::RightSemi, {0U}, {0U}, {}, {1U, 0U, 0U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;
            std::map<std::pair<TString, ui32>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), t1.Get<ui32>())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("X"), 2)], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("Y"), 2)], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);
        }
    }


    Y_UNIT_TEST_LLVM_SPILLING(TestRightMulti1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Right, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;
            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                UNIT_ASSERT(!t0 || !t0.AsStringRef().Empty()); // ensure no empty strings
                UNIT_ASSERT(!t1 || !t1.AsStringRef().Empty());
                ++u[std::make_pair(t0 ? TString(t0.AsStringRef()) : TString(), t1 ? TString(t1.AsStringRef()) : TString())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString(), TString("Z"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 5);
        }
    }


    Y_UNIT_TEST_LLVM_SPILLING(TestRightSemiWithNullKey1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key0 = pb.NewEmptyOptional(pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
            const auto key1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
            const auto key2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key3 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key4 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id, true),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload4}),
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload3}),
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));


            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::RightSemi, {0U}, {0U}, {}, {1U, 0U, 0U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;
            std::map<std::pair<TString, ui32>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), t1.Get<ui32>())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("X"), 2)], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("Y"), 2)], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);
        }
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestRightOnlyWithNullKey1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key0 = pb.NewEmptyOptional(pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
            const auto key1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
            const auto key2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key3 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key4 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id, true),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload4}),
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload3}),
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));


            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::RightOnly, {0U}, {0U}, {}, {1U, 0U, 0U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;
            std::map<std::pair<TString, ui64>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                ++u[std::make_pair(TString(t0.AsStringRef()), t1 ? t1.Get<ui32>() : std::numeric_limits<ui64>::max())];
                // replace NULL with <ui64>::max()
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("Z"), 3)], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), std::numeric_limits<ui64>::max())], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);
        }
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestFull1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Full, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;
            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                UNIT_ASSERT(!t0 || !t0.AsStringRef().Empty()); // ensure no empty strings
                UNIT_ASSERT(!t1 || !t1.AsStringRef().Empty());
                ++u[std::make_pair(t0 ? TString(t0.AsStringRef()) : TString(), t1 ? TString(t1.AsStringRef()) : TString())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("B"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("X"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("C"), TString("Y"))], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("A"), TString())], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString(), TString("Z"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 6);
        }
    }


    Y_UNIT_TEST_LLVM_SPILLING(TestExclusion1) {
        if (SPILLING && RuntimeVersion < 50) return;

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM, SPILLING> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });


            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.GraceJoin(
                pb.ExpandMap(pb.ToFlow(list1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                pb.ExpandMap(pb.ToFlow(list2), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                EJoinKind::Exclusion, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );
            if (SPILLING) {
                setup.RenameCallable(pgmReturn, "GraceJoin", "GraceJoinWithSpilling");
            }

            const auto graph = setup.BuildGraph(pgmReturn);
            if (SPILLING) {
                graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
            }

            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;
            std::map<std::pair<TString, TString>, ui32> u;

            while (iterator.Next(tuple)) {
                auto t0 = tuple.GetElement(0);
                auto t1 = tuple.GetElement(1);
                UNIT_ASSERT(!t0 || !t0.AsStringRef().Empty()); // ensure no empty strings
                UNIT_ASSERT(!t1 || !t1.AsStringRef().Empty());
                ++u[std::make_pair(t0 ? TString(t0.AsStringRef()) : TString(), t1 ? TString(t1.AsStringRef()) : TString())];
            }
            UNIT_ASSERT(!iterator.Next(tuple));

            UNIT_ASSERT_EQUAL(u[std::make_pair(TString("A"), TString())], 1);
            UNIT_ASSERT_EQUAL(u[std::make_pair(TString(), TString("Z"))], 1);
            UNIT_ASSERT_EQUAL(u.size(), 2);
        }
    }

}

constexpr std::string_view LeftStreamName = "LeftTestStream";
constexpr std::string_view RightStreamName = "RightTestStream";

struct TTestStreamParams {
    ui64 MaxAllowedNumberOfFetches;
    ui64 StreamSize;
};

class TTestStreamWrapper: public TMutableComputationNode<TTestStreamWrapper> {
using TBaseComputation = TMutableComputationNode<TTestStreamWrapper>;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, TTestStreamParams& params)
            : TBase(memInfo), CompCtx(compCtx), Params(params)
        {}
    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            ++TotalFetches;

            UNIT_ASSERT_LE(TotalFetches, Params.MaxAllowedNumberOfFetches);

            if (TotalFetches > Params.StreamSize) {
                return NUdf::EFetchStatus::Finish;
            }

            NUdf::TUnboxedValue* items = nullptr;
            result = CompCtx.HolderFactory.CreateDirectArrayHolder(2, items);
            items[0] = NUdf::TUnboxedValuePod(TotalFetches);
            items[1] = MakeString(ToString(TotalFetches) * 5);

            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& CompCtx;
        TTestStreamParams& Params;
        ui64 TotalFetches = 0;
    };

    TTestStreamWrapper(TComputationMutables& mutables, TTestStreamParams& params)
        : TBaseComputation(mutables)
        , Params(params)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Params);
    }
private:
    void RegisterDependencies() const final {}

    TTestStreamParams& Params;
};

IComputationNode* WrapTestStream(const TComputationNodeFactoryContext& ctx, TTestStreamParams& params) {
    return new TTestStreamWrapper(ctx.Mutables, params);
}

TComputationNodeFactory GetNodeFactory(TTestStreamParams& leftParams, TTestStreamParams& rightParams) {
    return [&leftParams, &rightParams](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == LeftStreamName) {
            return WrapTestStream(ctx, leftParams);
        } else if (callable.GetType()->GetName() == RightStreamName) {
            return WrapTestStream(ctx, rightParams);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

TRuntimeNode MakeStream(TSetup<false>& setup, bool isRight) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    TCallableBuilder callableBuilder(*setup.Env, isRight ? RightStreamName : LeftStreamName,
            pb.NewStreamType(
                pb.NewTupleType({
                    pb.NewDataType(NUdf::TDataType<ui32>::Id),
                    pb.NewDataType(NUdf::TDataType<char*>::Id)
                    })
                ));

    return TRuntimeNode(callableBuilder.Build(), false);
}

Y_UNIT_TEST_SUITE(TMiniKQLGraceJoinEmptyInputTest) {

    void RunGraceJoinEmptyCaseTest(EJoinKind joinKind, bool emptyLeft, bool emptyRight) {
        const ui64 streamSize = 5;

        ui64 leftStreamSize = streamSize;
        ui64 rightStreamSize = streamSize;
        ui64 maxExpectedFetchesFromLeftStream = leftStreamSize + 1;
        ui64 maxExpectedFetchesFromRightStream = rightStreamSize + 1;

        if (emptyLeft) {
            leftStreamSize = 0;
            if (GraceJoin::ShouldSkipRightIfLeftEmpty(joinKind)) {
                maxExpectedFetchesFromRightStream = 1;
            }
        }

        if (emptyRight) {
            rightStreamSize = 0;
            if (GraceJoin::ShouldSkipLeftIfRightEmpty(joinKind)) {
                maxExpectedFetchesFromLeftStream = 1;
            }
        }

        TTestStreamParams leftParams(maxExpectedFetchesFromLeftStream, leftStreamSize);
        TTestStreamParams rightParams(maxExpectedFetchesFromRightStream, rightStreamSize);
        TSetup<false> setup(GetNodeFactory(leftParams, rightParams));
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto leftStream = MakeStream(setup, false);
        const auto rightStream = MakeStream(setup, true);

        const auto resultType = pb.NewFlowType(pb.NewMultiType({
            pb.NewDataType(NUdf::TDataType<char*>::Id),
            pb.NewDataType(NUdf::TDataType<char*>::Id)
        }));

        const auto joinFlow = pb.GraceJoin(
            pb.ExpandMap(pb.ToFlow(leftStream), [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            pb.ExpandMap(pb.ToFlow(rightStream), [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            joinKind,
            {0U}, {0U},
            {1U, 0U}, {1U, 1U},
            resultType);

        const auto pgmReturn = pb.Collect(pb.NarrowMap(joinFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return pb.NewTuple(items);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue tuple;
        while (iterator.Next(tuple)) {
            // Consume results if any
        }
    }

#define ADD_JOIN_TESTS_FOR_KIND(Kind)                               \
    Y_UNIT_TEST(Kind##_EmptyLeft) {                                 \
        RunGraceJoinEmptyCaseTest(EJoinKind::Kind, true, false);    \
    }                                                               \
    Y_UNIT_TEST(Kind##_EmptyRight) {                                \
        RunGraceJoinEmptyCaseTest(EJoinKind::Kind, false, true);    \
    }                                                               \

    ADD_JOIN_TESTS_FOR_KIND(Inner)
    ADD_JOIN_TESTS_FOR_KIND(Left)
    ADD_JOIN_TESTS_FOR_KIND(LeftOnly)
    ADD_JOIN_TESTS_FOR_KIND(LeftSemi)
    ADD_JOIN_TESTS_FOR_KIND(Right)
    ADD_JOIN_TESTS_FOR_KIND(RightOnly)
    ADD_JOIN_TESTS_FOR_KIND(RightSemi)
    ADD_JOIN_TESTS_FOR_KIND(Full)
    ADD_JOIN_TESTS_FOR_KIND(Exclusion)

#undef ADD_JOIN_TESTS_FOR_KIND
}

// Tests for parallel reading optimization (two-table approach)
Y_UNIT_TEST_SUITE(TMiniKQLGraceJoinParallelTest) {



    TRuntimeNode CreateMockFlow(TSetup<false>& setup, const std::vector<std::vector<NUdf::TUnboxedValue>>& data) {
        TProgramBuilder& pb = *setup.PgmBuilder;
        
        const auto tupleType = pb.NewTupleType({
            pb.NewDataType(NUdf::TDataType<ui32>::Id),
            pb.NewDataType(NUdf::TDataType<char*>::Id)
        });

        std::vector<TRuntimeNode> tuples;
        for (const auto& row : data) {
            auto key = pb.NewDataLiteral<ui32>(row[0].Get<ui32>());
            auto value = pb.NewDataLiteral<NUdf::EDataSlot::String>(row[1].AsStringRef());
            tuples.push_back(pb.NewTuple({key, value}));
        }
        
        const auto list = pb.NewList(tupleType, tuples);
        return pb.ToFlow(list);
    }

    Y_UNIT_TEST(TestAsymmetricDataFinish) {
        // This is the KEY test case for our optimization!
        // Test when one stream finishes much earlier than the other
        TSetup<false> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        // Case 1: Left finishes early, right continues
        {
            // Left: only 3 rows
            std::vector<std::vector<NUdf::TUnboxedValue>> leftData = {
                {NUdf::TUnboxedValuePod(1u), MakeString("L1")},
                {NUdf::TUnboxedValuePod(2u), MakeString("L2")},
                {NUdf::TUnboxedValuePod(3u), MakeString("L3")}
            };

            // Right: 100 rows (much longer stream)
            std::vector<std::vector<NUdf::TUnboxedValue>> rightData;
            for (ui32 i = 1; i <= 100; ++i) {
                rightData.push_back({
                    NUdf::TUnboxedValuePod(i % 3 + 1),
                    MakeString("R" + ToString(i))
                });
            }

            // Create flows
            const auto leftFlow = CreateMockFlow(setup, leftData);
            const auto rightFlow = CreateMockFlow(setup, rightData);

            // Define result type
            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            // Create GraceJoin
            const auto joinFlow = pb.GraceJoin(
                pb.ExpandMap(leftFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                    return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
                }),
                pb.ExpandMap(rightFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                    return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
                }),
                EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType);

            const auto pgmReturn = pb.Collect(pb.NarrowMap(joinFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
                return pb.NewTuple(items);
            }));

            // Execute and collect results
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();

            ui32 resultCount = 0;
            NUdf::TUnboxedValue tuple;
            while (iterator.Next(tuple)) {
                resultCount++;
                // Verify join result structure
                auto leftElem = tuple.GetElement(0);
                auto rightElem = tuple.GetElement(1);
                auto leftValue = leftElem.AsStringRef();
                auto rightValue = rightElem.AsStringRef();
                UNIT_ASSERT(leftValue.size() > 0);
                UNIT_ASSERT(rightValue.size() > 0);
            }

            // Verify we got the expected number of results
            // Each left row (3) should match ~33 right rows = ~100 results
            UNIT_ASSERT_VALUES_EQUAL(resultCount, 100);
        }
    }

    Y_UNIT_TEST(TestTableSwapWithContinuousReading) {
        // Test the core optimization: while returning results from one table,
        // we read data into the other table, then swap them
        TSetup<false> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        // Create data that will require multiple table swaps
        std::vector<std::vector<NUdf::TUnboxedValue>> leftData;
        std::vector<std::vector<NUdf::TUnboxedValue>> rightData;

        // Small left side (finishes early)
        for (ui32 i = 1; i <= 5; ++i) {
            leftData.push_back({
                NUdf::TUnboxedValuePod(i),
                MakeString("L" + ToString(i))
            });
        }

        // Large right side (continues after left finishes)
        for (ui32 i = 1; i <= 50; ++i) {
            rightData.push_back({
                NUdf::TUnboxedValuePod(i % 5 + 1), // Keys 1-5, so all will match left
                MakeString("R" + ToString(i))
            });
        }

        // Create flows
        const auto leftFlow = CreateMockFlow(setup, leftData);
        const auto rightFlow = CreateMockFlow(setup, rightData);

        // Define result type
        const auto resultType = pb.NewFlowType(pb.NewMultiType({
            pb.NewDataType(NUdf::TDataType<char*>::Id),
            pb.NewDataType(NUdf::TDataType<char*>::Id)
        }));

        // Create GraceJoin
        const auto joinFlow = pb.GraceJoin(
            pb.ExpandMap(leftFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            pb.ExpandMap(rightFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType);

        const auto pgmReturn = pb.Collect(pb.NarrowMap(joinFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return pb.NewTuple(items);
        }));

        // Execute and collect results
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        ui32 resultCount = 0;
        NUdf::TUnboxedValue tuple;
        while (iterator.Next(tuple)) {
            resultCount++;
            // Verify join result: should be (left_value, right_value)
            auto leftVal = tuple.GetElement(0);
            auto rightVal = tuple.GetElement(1);
            UNIT_ASSERT(leftVal.AsStringRef().StartsWith("L"));
            UNIT_ASSERT(rightVal.AsStringRef().StartsWith("R"));
        }

        // Expected result: 50 joined rows total (every right row matches some left row)
        UNIT_ASSERT_VALUES_EQUAL(resultCount, 50);
    }

    Y_UNIT_TEST(TestRealWorldAsymmetricJoin) {
        // Simulate real-world scenario: small dimension table joined with large fact table
        TSetup<false> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        // Dimension table: small, finishes quickly
        std::vector<std::vector<NUdf::TUnboxedValue>> dimData = {
            {NUdf::TUnboxedValuePod(1u), MakeString("Category_A")},
            {NUdf::TUnboxedValuePod(2u), MakeString("Category_B")},
            {NUdf::TUnboxedValuePod(3u), MakeString("Category_C")}
        };

        // Fact table: large, continues streaming
        std::vector<std::vector<NUdf::TUnboxedValue>> factData;
        for (ui32 i = 0; i < 300; ++i) { // Reduced size for faster test
            factData.push_back({
                NUdf::TUnboxedValuePod((i % 3) + 1), // category_id: 1, 2, or 3
                MakeString("fact_" + ToString(i))
            });
        }

        // Create flows
        const auto dimFlow = CreateMockFlow(setup, dimData);
        const auto factFlow = CreateMockFlow(setup, factData);

        // Define result type
        const auto resultType = pb.NewFlowType(pb.NewMultiType({
            pb.NewDataType(NUdf::TDataType<char*>::Id),
            pb.NewDataType(NUdf::TDataType<char*>::Id)
        }));

        // Create GraceJoin (dimension LEFT, fact RIGHT)
        const auto joinFlow = pb.GraceJoin(
            pb.ExpandMap(dimFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            pb.ExpandMap(factFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType);

        const auto pgmReturn = pb.Collect(pb.NarrowMap(joinFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return pb.NewTuple(items);
        }));

        // Execute and collect results
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        ui32 resultCount = 0;
        std::map<TString, ui32> categoryCount;
        NUdf::TUnboxedValue tuple;
        while (iterator.Next(tuple)) {
            resultCount++;
            
            // Verify join result: should be (category_name, fact_data)
            auto categoryElem = tuple.GetElement(0);
            auto factElem = tuple.GetElement(1);
            auto categoryName = categoryElem.AsStringRef();
            auto factData = factElem.AsStringRef();
            
            UNIT_ASSERT(categoryName.StartsWith("Category_")); // Dimension value
            UNIT_ASSERT(factData.StartsWith("fact_")); // Fact value
            
            categoryCount[TString(categoryName)]++;
        }

        // Verify all categories are represented
        UNIT_ASSERT_VALUES_EQUAL(categoryCount.size(), 3);
        UNIT_ASSERT(categoryCount["Category_A"] > 0);
        UNIT_ASSERT(categoryCount["Category_B"] > 0);
        UNIT_ASSERT(categoryCount["Category_C"] > 0);

        // Expected: 300 joined rows (every fact row matches some dimension row)
        UNIT_ASSERT_VALUES_EQUAL(resultCount, 300);
    }



    Y_UNIT_TEST(TestBatchProcessingWithAsymmetricFinish) {
        // Test that when one stream finishes early, the other stream
        // continues to be processed in batches with proper table swapping
        TSetup<false> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        // Left: finishes after just 2 rows
        std::vector<std::vector<NUdf::TUnboxedValue>> leftData = {
            {NUdf::TUnboxedValuePod(1u), MakeString("DimA")},
            {NUdf::TUnboxedValuePod(2u), MakeString("DimB")}
        };

        // Right: continues with many rows that will need multiple batch processing
        std::vector<std::vector<NUdf::TUnboxedValue>> rightData;
        for (ui32 i = 1; i <= 200; ++i) {
            rightData.push_back({
                NUdf::TUnboxedValuePod(i % 2 + 1), // Alternates between 1 and 2
                MakeString("Fact" + ToString(i))
            });
        }

        const auto leftFlow = CreateMockFlow(setup, leftData);
        const auto rightFlow = CreateMockFlow(setup, rightData);

        const auto resultType = pb.NewFlowType(pb.NewMultiType({
            pb.NewDataType(NUdf::TDataType<char*>::Id),
            pb.NewDataType(NUdf::TDataType<char*>::Id)
        }));

        const auto joinFlow = pb.GraceJoin(
            pb.ExpandMap(leftFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            pb.ExpandMap(rightFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType);

        const auto pgmReturn = pb.Collect(pb.NarrowMap(joinFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return pb.NewTuple(items);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        ui32 resultCount = 0;
        ui32 dimACount = 0, dimBCount = 0;
        NUdf::TUnboxedValue tuple;
        
        while (iterator.Next(tuple)) {
            resultCount++;
            
            auto leftElem = tuple.GetElement(0);
            auto rightElem = tuple.GetElement(1);
            auto leftValue = leftElem.AsStringRef();
            auto rightValue = rightElem.AsStringRef();
            
            UNIT_ASSERT(leftValue.StartsWith("Dim"));
            UNIT_ASSERT(rightValue.StartsWith("Fact"));
            
            // Count dimension usage
            if (leftValue == "DimA") dimACount++;
            if (leftValue == "DimB") dimBCount++;
        }

        // Verify correct batch processing behavior:
        // 1. All 200 fact rows should be joined
        UNIT_ASSERT_VALUES_EQUAL(resultCount, 200);
        
        // 2. Both dimensions should be used equally (100 each)
        UNIT_ASSERT_VALUES_EQUAL(dimACount, 100);
        UNIT_ASSERT_VALUES_EQUAL(dimBCount, 100);
        
        // This verifies that the left table (dimensions) was properly reused
        // for multiple batches of the right table (facts)
    }

    Y_UNIT_TEST(TestIterativeResultRetrieval) {
        // Test that demonstrates the key optimization: getting results one by one
        // should trigger continuous reading of input data
        TSetup<false> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        // Small left side - will finish early
        std::vector<std::vector<NUdf::TUnboxedValue>> leftData = {
            {NUdf::TUnboxedValuePod(1u), MakeString("Key1")}
        };

        // Large right side - continues after left finishes
        std::vector<std::vector<NUdf::TUnboxedValue>> rightData;
        for (ui32 i = 1; i <= 100; ++i) {
            rightData.push_back({
                NUdf::TUnboxedValuePod(1u), // All match the single left key
                MakeString("Value" + ToString(i))
            });
        }

        const auto leftFlow = CreateMockFlow(setup, leftData);
        const auto rightFlow = CreateMockFlow(setup, rightData);

        const auto resultType = pb.NewFlowType(pb.NewMultiType({
            pb.NewDataType(NUdf::TDataType<char*>::Id),
            pb.NewDataType(NUdf::TDataType<char*>::Id)
        }));

        const auto joinFlow = pb.GraceJoin(
            pb.ExpandMap(leftFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            pb.ExpandMap(rightFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType);

        const auto pgmReturn = pb.Collect(pb.NarrowMap(joinFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return pb.NewTuple(items);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        ui32 resultCount = 0;
        NUdf::TUnboxedValue tuple;
        
        // Get results one by one to test iterative behavior
        // In the optimized version, this should trigger parallel reading
        while (iterator.Next(tuple)) {
            resultCount++;
            
            // Verify each result
            auto leftElem = tuple.GetElement(0);
            auto rightElem = tuple.GetElement(1);
            auto leftValue = leftElem.AsStringRef();
            auto rightValue = rightElem.AsStringRef();
            
            UNIT_ASSERT_VALUES_EQUAL(leftValue, "Key1");
            UNIT_ASSERT(rightValue.StartsWith("Value"));
            

        }


        UNIT_ASSERT_VALUES_EQUAL(resultCount, 100);
    }

    Y_UNIT_TEST(TestMemoryEfficiencyWithLargeAsymmetricData) {


        TSetup<false> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;


        std::vector<std::vector<NUdf::TUnboxedValue>> dimData = {
            {NUdf::TUnboxedValuePod(100u), MakeString("Electronics")},
            {NUdf::TUnboxedValuePod(200u), MakeString("Books")}
        };

        // Facts: Large number of transactions
        std::vector<std::vector<NUdf::TUnboxedValue>> factData;
        for (ui32 i = 0; i < 500; ++i) {
            ui32 categoryId = (i % 2 == 0) ? 100u : 200u; // Alternate between categories
            factData.push_back({
                NUdf::TUnboxedValuePod(categoryId),
                MakeString("Transaction" + ToString(i))
            });
        }

        const auto dimFlow = CreateMockFlow(setup, dimData);
        const auto factFlow = CreateMockFlow(setup, factData);

        const auto resultType = pb.NewFlowType(pb.NewMultiType({
            pb.NewDataType(NUdf::TDataType<char*>::Id),
            pb.NewDataType(NUdf::TDataType<char*>::Id)
        }));

        const auto joinFlow = pb.GraceJoin(
            pb.ExpandMap(dimFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            pb.ExpandMap(factFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
            }),
            EJoinKind::Inner, {0U}, {0U}, {1U, 0U}, {1U, 1U}, resultType);

        const auto pgmReturn = pb.Collect(pb.NarrowMap(joinFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return pb.NewTuple(items);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        ui32 electronicsCount = 0, booksCount = 0;
        NUdf::TUnboxedValue tuple;
        
        while (iterator.Next(tuple)) {
            auto categoryElem = tuple.GetElement(0);
            auto transactionElem = tuple.GetElement(1);
            auto category = categoryElem.AsStringRef();
            auto transaction = transactionElem.AsStringRef();
            
            UNIT_ASSERT(transaction.StartsWith("Transaction"));
            
            if (category == "Electronics") {
                electronicsCount++;
            } else if (category == "Books") {
                booksCount++;
            }
        }

        // Verify balanced distribution and complete processing
        UNIT_ASSERT_VALUES_EQUAL(electronicsCount, 250);
        UNIT_ASSERT_VALUES_EQUAL(booksCount, 250);
        UNIT_ASSERT_VALUES_EQUAL(electronicsCount + booksCount, 500);
    }
}

}

}
