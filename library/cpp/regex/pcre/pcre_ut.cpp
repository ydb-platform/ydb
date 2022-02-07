#include <library/cpp/regex/pcre/pcre.h>

#include <library/cpp/testing/unittest/registar.h>

template <class T>
inline IOutputStream& operator<<(IOutputStream& out, const TVector<T>& value) {
    size_t size = value.size();
    out << "[";
    for (size_t i = 0; i < size; ++i) {
        if (i) {
            out << ",";
        }
        out << value[i];
    }
    out << "]";
    return out;
}

template <class T, class U>
inline IOutputStream& operator<<(IOutputStream& out, const std::pair<T, U>& value) {
    out << "{" << value.first << "," << value.second << "}";
    return out;
}

// char8_t
#define OPTIMIZE NPcre::EOptimize::None
#define TEST_NAME(S) S
#define STRING(S) S
#define CHAR_TYPE char
#include "pcre_ut_base.h"

#undef OPTIMIZE
#define OPTIMIZE NPcre::EOptimize::Study
#undef TEST_NAME
#define TEST_NAME(S) S ## Study
#include "pcre_ut_base.h"

#undef OPTIMIZE
#define OPTIMIZE NPcre::EOptimize::JIT
#undef TEST_NAME
#define TEST_NAME(S) S ## JIT
#include "pcre_ut_base.h"

// char16_t
#undef OPTIMIZE
#define OPTIMIZE NPcre::EOptimize::None
#undef TEST_NAME
#define TEST_NAME(S) S ## 16
#undef STRING
#define STRING(S) u ## S
#undef CHAR_TYPE
#define CHAR_TYPE wchar16
#include "pcre_ut_base.h"

#undef OPTIMIZE
#define OPTIMIZE NPcre::EOptimize::Study
#undef TEST_NAME
#define TEST_NAME(S) S ## Study16
#include "pcre_ut_base.h"

#undef OPTIMIZE
#define OPTIMIZE NPcre::EOptimize::JIT
#undef TEST_NAME
#define TEST_NAME(S) S ## JIT16
#include "pcre_ut_base.h"

// char32_t
#undef OPTIMIZE
#define OPTIMIZE NPcre::EOptimize::None
#undef TEST_NAME
#define TEST_NAME(S) S ## 32
#undef STRING
#define STRING(S) U ## S
#undef CHAR_TYPE
#define CHAR_TYPE wchar32
#include "pcre_ut_base.h"

#undef OPTIMIZE
#define OPTIMIZE NPcre::EOptimize::Study
#undef TEST_NAME
#define TEST_NAME(S) S ## Study32
#include "pcre_ut_base.h"

#undef OPTIMIZE
#define OPTIMIZE NPcre::EOptimize::JIT
#undef TEST_NAME
#define TEST_NAME(S) S ## JIT32
#include "pcre_ut_base.h"

