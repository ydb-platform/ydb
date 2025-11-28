#pragma once

#include "format.h"

#include <yql/essentials/utils/log/ut/log_parser.h>

#define Y_UNIT_TEST_ON_EACH_LOG_FORMAT(N)                                                             \
    template <TLogRow (*ParseLogRow)(TStringBuf str), TString (*Format)(const TLogRecord&)>           \
    void N(NUnitTest::TTestContext&);                                                                 \
    struct TTestRegistration##N {                                                                     \
        TTestRegistration##N() {                                                                      \
            TCurrentTest::AddTest(                                                                    \
                #N "Json",                                                                            \
                static_cast<void (*)(NUnitTest::TTestContext&)>(&N<ParseJsonLogRow, JsonFormat>),     \
                /* forceFork = */ false);                                                             \
            TCurrentTest::AddTest(                                                                    \
                #N "Legacy",                                                                          \
                static_cast<void (*)(NUnitTest::TTestContext&)>(&N<ParseLegacyLogRow, LegacyFormat>), \
                /* forceFork = */ false);                                                             \
        }                                                                                             \
    };                                                                                                \
    static TTestRegistration##N testRegistration##N;                                                  \
    template <TLogRow (*ParseLogRow)(TStringBuf str), TString (*Format)(const TLogRecord&)>           \
    void N(NUnitTest::TTestContext&)
