#pragma once

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/entropy.h>
#include <util/random/mersenne64.h>
#include <util/stream/null.h>
#include <util/string/printf.h>

IOutputStream& Ctest = Cnull;

#define VERBOSE_COUT(a) Ctest << a

inline TString PrintArr(ui32 *arr, ui32 n) {
    TStringStream out;
    if (n == 0) {
        out << "-";
    }
    for (ui32 i = 0; i < n; ++i) {
        out << arr[i] << " ";
    }
    out << Endl;
    return out.Str();
}

inline const char *BoolToStr(bool val) {
    return val ? "true " : "false";
}

inline ui32 Fact(ui32 n) {
    ui32 res = 1;
    while (n > 1) {
        res *= n;
        n--;
    }
    return res;
}

inline void GenFirstCombination(ui32 *variants, ui32 const k) {
    for (ui32 i = 0; i < k; ++i) {
        variants[i] = i;
    }
}

inline void GenNextCombination(ui32 *variants, ui32 const k, ui32 const n) {
    for (ui32 i = k-1; i != (ui32)-1; --i) {
        if ( variants[i] < n - 1 - (k - 1 - i)) {
            ui32 tmp = ++variants[i];
            for (ui32 j = i+1; j < k; ++j) {
                variants[j] = ++tmp;
            }
            break;
        }
    }
}

inline TString GenerateRandomString(NPrivate::TMersenne64 &randGen, size_t dataSize) {
    TString testString;
    testString.resize(dataSize);
    char *writePosChar = (char *)testString.data();
    ui32 charParts = testString.size() % sizeof(ui64);
    for (ui32 i = 0; i < charParts; ++i) {
        writePosChar[i] = (char)randGen.GenRand();
    }
    ui64 *writePos64 = (ui64 *)writePosChar;
    ui32 ui64Parts = testString.size() / sizeof(ui64);
    for (ui32 i = 0; i < ui64Parts; ++i) {
        writePos64[i] = randGen.GenRand();
    }
    return testString;
}
