#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_grace_join_imp.h>


#include <chrono>
#include <iostream>
#include <cstring>
#include <vector>
#include <cassert>
#include <cstdlib>
#include <stdlib.h>

#include <util/system/compiler.h>
#include <util/stream/null.h>
#include <util/system/mem_info.h>

#include <cstdint>

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

    for (ui64 i = 0; i < TupleSize; i++)
    {
        bigTuple[i] = std::rand() / (RAND_MAX / 10000);
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

    for (ui64 i = 0; i < NTuples; i++)
    {
        bucket = i % NBuckets;
//        bucket = std::rand() / ( RAND_MAX / (NBuckets-1));
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
//        bucket = std::rand() / ( RAND_MAX / (NBuckets-1));

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
        bucket = std::rand() / ( RAND_MAX / (NBuckets-1));

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

            GraceJoin::TTable bigTable(1,1,1,1);
            GraceJoin::TTable smallTable(1,1,1,1);
            GraceJoin::TTable joinTable(1,1,1,1);

            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            const ui64 TupleSize = 1024;

            ui64 bigTuple[TupleSize];

            for (ui64 i = 0; i < TupleSize; i++) {
                bigTuple[i] = std::rand() / ( RAND_MAX / 10000 );
            }

            ui64 milliseconds = 0;



            const ui64 BigTableTuples = 600000;
            const ui64 SmallTableTuples = 150000;
            const ui64 BigTupleSize = 40;

            std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();


            for ( ui64 i = 0; i < BigTableTuples; i++) {
                tuple[1] = std::rand() % SmallTableTuples;
                tuple[2] = tuple[1];
                bigTable.AddTuple(tuple, strVals, strSizes);
            }

            smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

            for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                tuple[1] = std::rand() % SmallTableTuples;
                tuple[2] = tuple[1];
                smallTable.AddTuple(tuple, strVals, strSizes);
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


            for ( ui64 i = 0; i < BigTableTuples; i++) {
                tuple[1] = std::rand() % SmallTableTuples;
                tuple[2] = tuple[1];
                bigTable.AddTuple(tuple, strVals, strSizes);
            }

            smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

            for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                tuple[1] = std::rand() % SmallTableTuples;
                tuple[2] = tuple[1];
                smallTable.AddTuple(tuple, strVals, strSizes);
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



            GraceJoin::TTable bigTable  (1,1,1,1,0,0,1, nullptr, true);
            GraceJoin::TTable smallTable(1,1,1,1,0,0,1, nullptr, true);
            GraceJoin::TTable joinTable (1,1,1,1,0,0,1, nullptr, true);

            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            const ui64 TupleSize = 1024;

            ui64 bigTuple[TupleSize];

            for (ui64 i = 0; i < TupleSize; i++) {
                bigTuple[i] = std::rand() / ( RAND_MAX / 10000 );
            }

            ui64 milliseconds = 0;



            const ui64 BigTableTuples = 600000;
            const ui64 SmallTableTuples = 150000;
            const ui64 BigTupleSize = 40;

            std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();


            for ( ui64 i = 0; i < BigTableTuples; i++) {
                tuple[1] = i % SmallTableTuples;
                tuple[2] = tuple[1];
                bigTable.AddTuple(tuple, strVals, strSizes);
            }

            smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

            for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                tuple[1] = i;
                tuple[2] = tuple[1];
                smallTable.AddTuple(tuple, strVals, strSizes);
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



            GraceJoin::TTable bigTable  (1,1,1,1,0,0,1, nullptr, false);
            GraceJoin::TTable smallTable(1,1,1,1,0,0,1, nullptr, false);
            GraceJoin::TTable joinTable (1,1,1,1,0,0,1, nullptr, false);

            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            const ui64 TupleSize = 1024;

            ui64 bigTuple[TupleSize];

            for (ui64 i = 0; i < TupleSize; i++) {
                bigTuple[i] = std::rand() / ( RAND_MAX / 10000 );
            }

            ui64 milliseconds = 0;



            const ui64 BigTableTuples = 600000;
            const ui64 SmallTableTuples = 150000;
            const ui64 BigTupleSize = 40;

            std::chrono::steady_clock::time_point begin03 = std::chrono::steady_clock::now();


            for ( ui64 i = 0; i < BigTableTuples; i++) {
                tuple[1] = i % SmallTableTuples;
                tuple[2] = tuple[1];
                bigTable.AddTuple(tuple, strVals, strSizes);
            }

            smallTable.AddTuple(tuple, bigStrVal, bigStrSize);

            for ( ui64 i = 0; i < SmallTableTuples + 1; i++) {
                tuple[1] = i;
                tuple[2] = tuple[1];
                smallTable.AddTuple(tuple, strVals, strSizes);
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

#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 40u
Y_UNIT_TEST_SUITE(TMiniKQLSelfJoinTest) {

    Y_UNIT_TEST_LLVM(TestInner1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "C");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "X");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "C");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "X");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "B");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "A");
            UNIT_ASSERT(!iterator.Next(tuple));

        }


    }

    Y_UNIT_TEST_LLVM(TestDiffKeys) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "C");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "A");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "B");
            UNIT_ASSERT(!iterator.Next(tuple));

        }


    }


}
#endif

Y_UNIT_TEST_SUITE(TMiniKQLGraceJoinTest) {

    Y_UNIT_TEST_LLVM(TestInner1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Z");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(!iterator.Next(tuple));

        }


    }

    Y_UNIT_TEST_LLVM(TestInnerDoubleCondition1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(!iterator.Next(tuple));

        }


    }

    Y_UNIT_TEST_LLVM(TestInnerManyKeyStrings) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B2");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B1");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(!iterator.Next(tuple));

        }


    }

    Y_UNIT_TEST_LLVM(TestInnerManyKeyUuid) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B2B2B2B2B2B2B2B2");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "YYYYYYYYYYYYYYYY");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B1B1B1B1B1B1B1B1");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "XXXXXXXXXXXXXXXX");
            UNIT_ASSERT(!iterator.Next(tuple));

        }


    }


    Y_UNIT_TEST_LLVM(TestInnerStringKey1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);

            const auto iterator = graph->GetValue().GetListIterator();

            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Z");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(!iterator.Next(tuple));

        }


    }



    Y_UNIT_TEST_LLVM(TMiniKQLGraceJoinTestInnerMulti1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeft1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;


            for (ui32 i = 0; i < 3; i++) {
                iterator.Next(tuple);
                const auto cell = (tuple.GetElement(0));
                if (cell.AsStringRef() == "A") {
                    UNIT_ASSERT(!tuple.GetElement(1));
                }
                if (cell.AsStringRef() == "B") {
                    UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
                }
                if (cell.AsStringRef() == "C") {
                    UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
                }
            }

            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));

        }
    }

    Y_UNIT_TEST_LLVM(TestLeftMulti1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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


            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;


            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftSemi1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftOnly1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "D");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 4);

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 1);

            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftSemiWithNullKey1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftOnlyWithNullKey1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 1);
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "X");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestRight1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;


            for (ui32 i = 0; i < 3; i++) {
                iterator.Next(tuple);
                const auto cell = (tuple.GetElement(1));
                if (cell.AsStringRef() == "Z") {
                    UNIT_ASSERT(!tuple.GetElement(0));
                }
                if (cell.AsStringRef() == "X") {
                    UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
                }
                if (cell.AsStringRef() == "Y") {
                    UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
                }
            }

            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));

        }
    }


        Y_UNIT_TEST_LLVM(TestRightOnly1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "Z");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 3);
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }



    Y_UNIT_TEST_LLVM(TestRightSemi1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "X");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "Y");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }


    Y_UNIT_TEST_LLVM(TestRightMulti1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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


            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;


            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT(!tuple.GetElement(0));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Z");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }


   Y_UNIT_TEST_LLVM(TestRightSemiWithNullKey1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "X");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "Y");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestRightOnlyWithNullKey1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "Z");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 3);
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestFull1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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


            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;


            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Z");
            UNIT_ASSERT(!tuple.GetElement(0));
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }


    Y_UNIT_TEST_LLVM(TestExclusion1) {

        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
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


            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;


            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Z");
            UNIT_ASSERT(!tuple.GetElement(0));
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

}


}

}
