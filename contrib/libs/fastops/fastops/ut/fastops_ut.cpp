#include <fastops/avx/ops_avx.h>
#include <fastops/avx2/ops_avx2.h>
#include <fastops/plain/ops_plain.h>

#include <cstring>
#include <exception>
#include <iostream>
#include <sstream>
#include <random>
#include <string>
#include <vector>

#include <math.h>
#include <xmmintrin.h>
#include <pmmintrin.h>

class FastOpsTestException : public std::exception {
protected:
    const std::string msg;

public:
    FastOpsTestException(const std::string& m)
        : msg(m)
    {
    }

    const char* what() const noexcept override {
        return msg.c_str();
    }
};

#define UNIT_ASSERT(condition)                                                                                         \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            std::ostringstream out;                                                                                    \
            out << "assertion failed " << #condition << " at " << __FUNCTION__ << ", " << __FILE__ << ":" << __LINE__; \
            throw FastOpsTestException(out.str());                                                                     \
        }                                                                                                              \
    } while (false)

static double GetRelError(double real, double approx) {
    return std::abs(approx - real) / (std::abs(real) + 1e-100);
}

enum class EFunc {
    Exp,
    Log,
    Sigmoid,
    Tanh
};

struct TTestExpOpts {
    EFunc Func = EFunc::Exp;

    float Min = -150;
    float Max = 150;
    size_t N = 1000;
    size_t VecSize = 1;
    size_t PadSize = 0;
    size_t InputAlignBytes = 0;
    size_t OutputAlignBytes = 0;
    bool IsRandom = false;
    bool Inplace = false;
};

template <class T>
inline T ReadUnaligned(const void* from) noexcept {
    T ret;
    std::memcpy(&ret, from, sizeof(T));
    return ret;
}

template <class T>
inline void WriteUnaligned(void* to, const T& t) noexcept {
    std::memcpy(to, &t, sizeof(T));
}

template <class TFunc, class TCompare, class T>
void TestFunc(std::mt19937_64& rng, TTestExpOpts opts) {
    std::vector<char> inV((opts.VecSize + opts.PadSize) * sizeof(T) + 32);
    std::vector<char> outV((opts.VecSize + opts.PadSize + 4) * sizeof(T) + 32);
    std::uniform_real_distribution<> dis(0, 1.0);

    size_t padOffset = opts.PadSize / 2;
    padOffset *= sizeof(T);

    char* inVCharData = inV.data();
    char* outVCharData = outV.data();
    while ((size_t)(inVCharData + padOffset) % 32 != opts.InputAlignBytes) {
        ++inVCharData;
    }
    while ((size_t)(outVCharData + padOffset) % 32 != opts.OutputAlignBytes) {
        ++outVCharData;
    }

    T* inDataStart = (T*)inVCharData;
    T* inData = inDataStart + opts.PadSize / 2;
    T* origInData = inData;
    T* outDataStart = (T*)outVCharData;
    T* outData = outDataStart + opts.PadSize / 2;

    if (opts.Inplace) {
        inData = outData;
    }

    for (size_t i = 0; i < opts.N / opts.VecSize + 1; ++i) {
        std::memset(inV.data(), 0, inV.size());
        std::memset(outV.data(), 0, outV.size());
        for (size_t j = 0; j < opts.VecSize; ++j) {
            T val;
            if (opts.IsRandom) {
                val = dis(rng) * (opts.Max - opts.Min) + opts.Min;
            } else {
                size_t idx = opts.VecSize * i + j;
                val = opts.Min + idx * 1.0 / opts.N * (opts.Max - opts.Min) + (2 * dis(rng) - 1) * 1.0 / opts.N;
            }

            switch (opts.Func) {
                case EFunc::Log:
                    val = exp(val);
                    break;
                case EFunc::Exp:
                case EFunc::Sigmoid:
                case EFunc::Tanh:
                    break;
                default:
                    UNIT_ASSERT(false);
            }

            WriteUnaligned(inData + j, val);
            WriteUnaligned(origInData + j, val);
        }

        TFunc::Apply(inData, opts.VecSize, outData);
        if (!opts.Inplace) {
            for (size_t j = 0; j < opts.VecSize + opts.PadSize; ++j) {
                if (inDataStart + j < inData || inDataStart + j >= inData + opts.VecSize) {
                    UNIT_ASSERT(ReadUnaligned<T>(inDataStart + j) == 0);
                }
            }
        }

        for (size_t j = 0; j < opts.VecSize + opts.PadSize; ++j) {
            if (outDataStart + j < outData || outDataStart + j >= outData + opts.VecSize) {
                UNIT_ASSERT(ReadUnaligned<T>(outDataStart + j) == 0);
            } else {
                size_t j2 = outDataStart + j - outData;
                T trueVal;
                T approxVal;
                switch (opts.Func) {
                    case EFunc::Log: {
                        trueVal = log(ReadUnaligned<T>(origInData + j2));
                        approxVal = ReadUnaligned<T>(outDataStart + j);
                        break;
                    }
                    case EFunc::Exp: {
                        trueVal = exp(ReadUnaligned<T>(origInData + j2));
                        approxVal = ReadUnaligned<T>(outDataStart + j);
                        break;
                    }
                    case EFunc::Sigmoid: {
                        trueVal = 1.0 / (1.0 + exp(-ReadUnaligned<T>(origInData + j2)));
                        approxVal = ReadUnaligned<T>(outDataStart + j);
                        break;
                    }
                    case EFunc::Tanh: {
                        trueVal = tanh(ReadUnaligned<T>(origInData + j2));
                        approxVal = ReadUnaligned<T>(outDataStart + j);
                        break;
                    }
                    default:
                        UNIT_ASSERT(false);
                }

                if (!TCompare::Compare(ReadUnaligned<T>(origInData + j2), trueVal, approxVal)) {
                    std::cerr << ReadUnaligned<T>(origInData + j2) << "\t" << trueVal << "\t" << approxVal << "\t" << GetRelError(trueVal, approxVal) << std::endl;
                    UNIT_ASSERT(false);
                }
            }
        }
    }
}

template <bool Exact, bool IsDouble>
struct TExpCompare {
    static bool Compare(double input, double trueVal, double approxVal) {
        if (!Exact && !IsDouble) {
            if (input < -87) {
                if (approxVal <= 1.0001 * trueVal) {
                    return true;
                } else {
                    if (approxVal < 1.2e-38 && trueVal == 0 && input > -87.3366 && input < -87.3365) {
                        // banned denormals boundary
                        return true;
                    } else {
                        return false;
                    }
                }
            } else if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                return GetRelError(trueVal, approxVal) < 7.21e-06;
            }
        }
        if (!Exact && IsDouble) {
            if (input < -708.39) {
                if (approxVal <= 1.0001 * trueVal) {
                    return true;
                } else {
                    return false;
                }
            } else if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                return GetRelError(trueVal, approxVal) < 3.5e-06;
            }
        }
        if (Exact && !IsDouble) {
            if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else if (input <= -87) {
                // values most of the time are correct, but there are some insignificant differences
                // due to the rounding errors, that may make relative error even 1 (like approx is 0, exact is 1e-50)
                // (but this means nothing, it is just a rounding error near very small values)
                // when denormals are banned, error does not exceed 3.9e-06 everywhere
                if (approxVal == 0 || approxVal / 2.01 < trueVal) {
                    return true;
                } else {
                    if (approxVal < 1.2e-38 && trueVal == 0 && input > -87.3366 && input < -87.3365) {
                        // banned denormals boundary
                        return true;
                    } else {
                        return false;
                    }
                }
            } else {
                return GetRelError(trueVal, approxVal) < 3.92e-06;
            }
        }
        if (Exact && IsDouble) {
            if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else if (trueVal > 709.7824921 && std::isinf(approxVal)) {
                return true;
            } else {
                return GetRelError(trueVal, approxVal) < 2.3e-9;
            }
        }
        UNIT_ASSERT(false);
    }
};

template <class TFunc, class TCompare, class T>
void UnitTestFunc() {
    try {
        float min = -150;
        float max = 150;
        if (sizeof(T) != 4) {
            min = -1000;
            max = 1000;
        }

        if (TFunc::GetType() == EFunc::Sigmoid || TFunc::GetType() == EFunc::Tanh) {
            min = -20;
            max = 20;
        }

        for (bool enableDenormals : {false, true}) {
            for (bool inplace : {false, true}) {
                if (enableDenormals) {
                    _MM_SET_FLUSH_ZERO_MODE(_MM_FLUSH_ZERO_OFF);
                    _MM_SET_DENORMALS_ZERO_MODE(_MM_DENORMALS_ZERO_OFF);
                } else {
                    _MM_SET_FLUSH_ZERO_MODE(_MM_FLUSH_ZERO_ON);
                    _MM_SET_DENORMALS_ZERO_MODE(_MM_DENORMALS_ZERO_ON);
                }

                std::mt19937_64 rng(15);
                // there should be no error on zero-size input
                {
                    T in = 2;
                    T out = 2;
                    TFunc::Apply(&in, 0, &out);
                    UNIT_ASSERT(in == 2 && out == 2);

                    TFunc::Apply((float*)nullptr, 0, (float*)nullptr);
                }

                // random test (near numbers must be vastly different)
                for (size_t i = 1; i < 500; ++i) {
                    TTestExpOpts opts;
                    opts.Func = TFunc::GetType();

                    // othewise we will almost always get 1
                    if (TFunc::GetType() == EFunc::Sigmoid || TFunc::GetType() == EFunc::Tanh) {
                        opts.Min = -3;
                        opts.Max = 3;
                    }

                    opts.N = 2000;
                    opts.VecSize = i;
                    opts.PadSize = 50;
                    opts.IsRandom = true;
                    opts.Inplace = inplace;
                    TestFunc<TFunc, TCompare, T>(rng, opts);
                }

                // roughly test precision + if there is no memory issues
                for (size_t i = 1; i < 500; ++i) {
                    TTestExpOpts opts;
                    opts.Func = TFunc::GetType();
                    opts.Min = min;
                    opts.Max = max;
                    opts.N = 2000;
                    opts.VecSize = i;
                    opts.PadSize = 50;
                    opts.Inplace = inplace;
                    TestFunc<TFunc, TCompare, T>(rng, opts);
                }

                // roughly test precision + that there is no memory issues in an unaligned case
                if (!TFunc::Aligned) {
                    std::vector<size_t> alignments;
                    for (size_t i = 1; i < 32; ++i) {
                        alignments.push_back(i);
                    }
                    TTestExpOpts opts;
                    opts.Func = TFunc::GetType();
                    opts.Min = min;
                    opts.Max = max;
                    opts.N = 1000;
                    opts.IsRandom = true;
                    opts.PadSize = 50;
                    for (size_t alignment : alignments) {
                        for (size_t i = 1; i < 200; ++i) {
                            opts.VecSize = i;
                            opts.OutputAlignBytes = 0;
                            opts.InputAlignBytes = alignment;
                            TestFunc<TFunc, TCompare, T>(rng, opts);
                        }
                    }
                    if (!TFunc::Aligned) {
                        for (size_t alignment : alignments) {
                            for (size_t i = 1; i < 200; ++i) {
                                opts.VecSize = i;
                                opts.OutputAlignBytes = alignment;
                                opts.Inplace = inplace;
                                TestFunc<TFunc, TCompare, T>(rng, opts);
                            }
                        }
                        for (size_t iterIdx = 0; iterIdx < 30; ++iterIdx) {
                            (void)(iterIdx);
                            for (size_t i = 1; i < 100; ++i) {
                                opts.VecSize = i;
                                opts.InputAlignBytes = rng() % 31 + 1;
                                opts.OutputAlignBytes = rng() % 31 + 1;
                                TestFunc<TFunc, TCompare, T>(rng, opts);
                            }
                        }
                    }
                }

                // Test precision in more detail
                for (size_t i = 1; i < 200; ++i) {
                    TTestExpOpts opts;
                    opts.Func = TFunc::GetType();
                    opts.Min = min;
                    opts.Max = max;
                    opts.N = 10000;
                    opts.VecSize = i;
                    opts.Inplace = inplace;
                    TestFunc<TFunc, TCompare, T>(rng, opts);
                }

                // Test precision in even more detail
                TTestExpOpts opts;
                opts.Func = TFunc::GetType();
                opts.Min = min;
                opts.Max = max;
                opts.N = 5000000;
                opts.VecSize = 16;
                opts.Inplace = inplace;
                TestFunc<TFunc, TCompare, T>(rng, opts);

                opts.Min = -5;
                opts.Max = 5;
                opts.Inplace = inplace;
                TestFunc<TFunc, TCompare, T>(rng, opts);

                // analyze behavior near zero
                opts.Min = -0.000005;
                opts.Max = 0.000005;
                opts.N = 500000;
                opts.Inplace = inplace;
                TestFunc<TFunc, TCompare, T>(rng, opts);
            }
        }
    } catch (...) {
        _MM_SET_FLUSH_ZERO_MODE(_MM_FLUSH_ZERO_OFF);
        _MM_SET_DENORMALS_ZERO_MODE(_MM_DENORMALS_ZERO_OFF);
        throw;
    }
}

template <bool EXACT, bool ALIGNED>
struct TAvx2Exp {
    static constexpr bool Aligned = ALIGNED;

    static EFunc GetType() {
        return EFunc::Exp;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::ExpAvx2<EXACT, ALIGNED>(from, size, to);
    }
};

template <bool EXACT, bool ALIGNED>
struct TAvxExp {
    static constexpr bool Aligned = ALIGNED;

    static EFunc GetType() {
        return EFunc::Exp;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::ExpAvx<EXACT, ALIGNED>(from, size, to);
    }
};

struct TPlainExp {
    static constexpr bool Aligned = false;

    static EFunc GetType() {
        return EFunc::Exp;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::ExpPlain(from, size, to);
    }
};

void TestFastExp() {
    std::cerr << "  AVX2 INEXACT UNALIGNED FLOAT (1/18)..." << std::endl;
    UnitTestFunc<TAvx2Exp<false, false>, TExpCompare<false, false>, float>();
    std::cerr << "  AVX2 INEXACT ALIGNED FLOAT (2/18)..." << std::endl;
    UnitTestFunc<TAvx2Exp<false, true>, TExpCompare<false, false>, float>();
    std::cerr << "  AVX2 EXACT UNALIGNED FLOAT (3/18)..." << std::endl;
    UnitTestFunc<TAvx2Exp<true, false>, TExpCompare<true, false>, float>();
    std::cerr << "  AVX2 EXACT ALIGNED FLOAT (4/18)..." << std::endl;
    UnitTestFunc<TAvx2Exp<true, true>, TExpCompare<true, false>, float>();

    std::cerr << "  AVX INEXACT UNALIGNED FLOAT (5/18)..." << std::endl;
    UnitTestFunc<TAvxExp<false, false>, TExpCompare<false, false>, float>();
    std::cerr << "  AVX INEXACT ALIGNED FLOAT (6/18)..." << std::endl;
    UnitTestFunc<TAvxExp<false, true>, TExpCompare<false, false>, float>();
    std::cerr << "  AVX EXACT UNALIGNED FLOAT (7/18)..." << std::endl;
    UnitTestFunc<TAvxExp<true, false>, TExpCompare<true, false>, float>();
    std::cerr << "  AVX EXACT ALIGNED FLOAT (8/18)..." << std::endl;
    UnitTestFunc<TAvxExp<true, true>, TExpCompare<true, false>, float>();

    std::cerr << "  BASELINE FLOAT (9/18)..." << std::endl;
    UnitTestFunc<TPlainExp, TExpCompare<true, false>, float>();

    std::cerr << "  AVX2 INEXACT UNALIGNED FLOAT (10/18)..." << std::endl;
    UnitTestFunc<TAvx2Exp<false, false>, TExpCompare<false, true>, double>();
    std::cerr << "  AVX2 INEXACT ALIGNED DOUBLE (11/18)..." << std::endl;
    UnitTestFunc<TAvx2Exp<false, true>, TExpCompare<false, true>, double>();
    std::cerr << "  AVX2 EXACT UNALIGNED DOUBLE (12/18)..." << std::endl;
    UnitTestFunc<TAvx2Exp<true, false>, TExpCompare<true, true>, double>();
    std::cerr << "  AVX2 EXACT ALIGNED DOUBLE (13/18)..." << std::endl;
    UnitTestFunc<TAvx2Exp<true, true>, TExpCompare<true, true>, double>();

    std::cerr << "  AVX INEXACT UNALIGNED DOUBLE (14/18)..." << std::endl;
    UnitTestFunc<TAvxExp<false, false>, TExpCompare<false, true>, double>();
    std::cerr << "  AVX INEXACT ALIGNED DOUBLE (15/18)..." << std::endl;
    UnitTestFunc<TAvxExp<false, true>, TExpCompare<false, true>, double>();
    std::cerr << "  AVX EXACT UNALIGNED DOUBLE (16/18)..." << std::endl;
    UnitTestFunc<TAvxExp<true, false>, TExpCompare<true, true>, double>();
    std::cerr << "  AVX EXACT ALIGNED DOUBLE (17/18)..." << std::endl;
    UnitTestFunc<TAvxExp<true, true>, TExpCompare<true, true>, double>();

    std::cerr << "  BASELINE DOUBLE (18/18)..." << std::endl;
    UnitTestFunc<TPlainExp, TExpCompare<true, true>, double>();
}

bool isneginf(double val) {
    return std::isinf(val) && val < std::numeric_limits<float>::lowest();
}

template <bool Exact, bool IsDouble>
struct TLogCompare {
    static bool Compare(double input, double trueVal, double approxVal) {
        if (!Exact && !IsDouble) {
            if (input < 1.17613e-38) {
                if (isneginf(approxVal)) {
                    return true;
                } else if ((trueVal < approxVal && approxVal < -87.3365) || GetRelError(trueVal, approxVal) < 1e-5) {
                    return true;
                } else {
                    return false;
                }
            } else if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                return GetRelError(trueVal, approxVal) < 1e-5;
            }
        }
        if (!Exact && IsDouble) {
            if (input < 2.99279772e-308) {
                if (isneginf(approxVal)) {
                    return true;
                } else if ((trueVal < approxVal && approxVal < -707.65) || GetRelError(trueVal, approxVal) < 1e-5) {
                    return true;
                } else {
                    return false;
                }
            } else if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                return GetRelError(trueVal, approxVal) < 1e-5;
            }
        }
        if (Exact && !IsDouble) {
            if (isneginf(trueVal)) {
                return isneginf(approxVal);
            } else if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                return GetRelError(trueVal, approxVal) < 4e-7;
            }
        }
        if (Exact && IsDouble) {
            if (isneginf(trueVal)) {
                return isneginf(approxVal);
            } else if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                return GetRelError(trueVal, approxVal) < 2e-7;
            }
        }
        UNIT_ASSERT(false);
    }
};

template <bool EXACT, bool ALIGNED>
struct TAvx2Log {
    static constexpr bool Aligned = ALIGNED;

    static EFunc GetType() {
        return EFunc::Log;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::LogAvx2<EXACT, ALIGNED>(from, size, to);
    }
};

template <bool EXACT, bool ALIGNED>
struct TAvxLog {
    static constexpr bool Aligned = ALIGNED;

    static EFunc GetType() {
        return EFunc::Log;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::LogAvx<EXACT, ALIGNED>(from, size, to);
    }
};

struct TPlainLog {
    static constexpr bool Aligned = false;

    static EFunc GetType() {
        return EFunc::Log;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::LogPlain(from, size, to);
    }
};

void TestFastLog() {
    std::cerr << "  AVX2 INEXACT UNALIGNED FLOAT (1/18)..." << std::endl;
    UnitTestFunc<TAvx2Log<false, false>, TLogCompare<false, false>, float>();
    std::cerr << "  AVX2 INEXACT ALIGNED FLOAT (2/18)..." << std::endl;
    UnitTestFunc<TAvx2Log<false, true>, TLogCompare<false, false>, float>();
    std::cerr << "  AVX2 EXACT UNALIGNED FLOAT (3/18)..." << std::endl;
    UnitTestFunc<TAvx2Log<true, false>, TLogCompare<true, false>, float>();
    std::cerr << "  AVX2 EXACT ALIGNED FLOAT (4/18)..." << std::endl;
    UnitTestFunc<TAvx2Log<true, true>, TLogCompare<true, false>, float>();

    std::cerr << "  AVX INEXACT UNALIGNED FLOAT (5/18)..." << std::endl;
    UnitTestFunc<TAvxLog<false, false>, TLogCompare<false, false>, float>();
    std::cerr << "  AVX INEXACT ALIGNED FLOAT (6/18)..." << std::endl;
    UnitTestFunc<TAvxLog<false, true>, TLogCompare<false, false>, float>();
    std::cerr << "  AVX EXACT UNALIGNED FLOAT (7/18)..." << std::endl;
    UnitTestFunc<TAvxLog<true, false>, TLogCompare<true, false>, float>();
    std::cerr << "  AVX EXACT ALIGNED FLOAT (8/18)..." << std::endl;
    UnitTestFunc<TAvxLog<true, true>, TLogCompare<true, false>, float>();

    std::cerr << "  BASELINE FLOAT (9/18)..." << std::endl;
    UnitTestFunc<TPlainLog, TLogCompare<true, false>, float>();

    std::cerr << "  AVX2 INEXACT UNALIGNED FLOAT (10/18)..." << std::endl;
    UnitTestFunc<TAvx2Log<false, false>, TLogCompare<false, true>, double>();
    std::cerr << "  AVX2 INEXACT ALIGNED DOUBLE (11/18)..." << std::endl;
    UnitTestFunc<TAvx2Log<false, true>, TLogCompare<false, true>, double>();
    std::cerr << "  AVX2 EXACT UNALIGNED DOUBLE (12/18)..." << std::endl;
    UnitTestFunc<TAvx2Log<true, false>, TLogCompare<true, true>, double>();
    std::cerr << "  AVX2 EXACT ALIGNED DOUBLE (13/18)..." << std::endl;
    UnitTestFunc<TAvx2Log<true, true>, TLogCompare<true, true>, double>();

    std::cerr << "  AVX INEXACT UNALIGNED DOUBLE (14/18)..." << std::endl;
    UnitTestFunc<TAvxLog<false, false>, TLogCompare<false, true>, double>();
    std::cerr << "  AVX INEXACT ALIGNED DOUBLE (15/18)..." << std::endl;
    UnitTestFunc<TAvxLog<false, true>, TLogCompare<false, true>, double>();
    std::cerr << "  AVX EXACT UNALIGNED DOUBLE (16/18)..." << std::endl;
    UnitTestFunc<TAvxLog<true, false>, TLogCompare<true, true>, double>();
    std::cerr << "  AVX EXACT ALIGNED DOUBLE (17/18)..." << std::endl;
    UnitTestFunc<TAvxLog<true, true>, TLogCompare<true, true>, double>();

    std::cerr << "  BASELINE DOUBLE (18/18)..." << std::endl;
    UnitTestFunc<TPlainLog, TLogCompare<true, true>, double>();
}

template <bool Exact, bool IsDouble>
struct TSigmCompare {
    static bool Compare(double input, double trueVal, double approxVal) {
        if (!Exact && !IsDouble) {
            if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                if (GetRelError(trueVal, approxVal) < 8e-06) {
                    return true;
                } else {
                    return approxVal < 1.2e-38 && trueVal == 0 && input > -87.3366 && input < -87.3365;
                }
            }
        }
        if (!Exact && IsDouble) {
            if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                if (GetRelError(trueVal, approxVal) < 4e-06) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        if (Exact && !IsDouble) {
            if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                if (GetRelError(trueVal, approxVal) < 4.5e-06) {
                    return true;
                } else {
                    return approxVal < 1.2e-38 && trueVal == 0 && input > -87.3366 && input < -87.3365;
                }
            }
        }
        if (Exact && IsDouble) {
            if (std::isinf(trueVal)) {
                return std::isinf(approxVal);
            } else {
                if (GetRelError(trueVal, approxVal) < 1e-12) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        UNIT_ASSERT(false);
    }
};

template <bool EXACT, bool ALIGNED>
struct TAvx2Sigm {
    static constexpr bool Aligned = ALIGNED;

    static EFunc GetType() {
        return EFunc::Sigmoid;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::SigmoidAvx2<EXACT, ALIGNED>(from, size, to);
    }
};

template <bool EXACT, bool ALIGNED>
struct TAvxSigm {
    static constexpr bool Aligned = ALIGNED;

    static EFunc GetType() {
        return EFunc::Sigmoid;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::SigmoidAvx<EXACT, ALIGNED>(from, size, to);
    }
};

struct TPlainSigm {
    static constexpr bool Aligned = false;

    static EFunc GetType() {
        return EFunc::Sigmoid;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::SigmoidPlain(from, size, to);
    }
};

void TestFastSigm() {
    std::cerr << "  AVX2 INEXACT UNALIGNED FLOAT (1/18)..." << std::endl;
    UnitTestFunc<TAvx2Sigm<false, false>, TSigmCompare<false, false>, float>();
    std::cerr << "  AVX2 INEXACT ALIGNED FLOAT (2/18)..." << std::endl;
    UnitTestFunc<TAvx2Sigm<false, true>, TSigmCompare<false, false>, float>();
    std::cerr << "  AVX2 EXACT UNALIGNED FLOAT (3/18)..." << std::endl;
    UnitTestFunc<TAvx2Sigm<true, false>, TSigmCompare<true, false>, float>();
    std::cerr << "  AVX2 EXACT ALIGNED FLOAT (4/18)..." << std::endl;
    UnitTestFunc<TAvx2Sigm<true, true>, TSigmCompare<true, false>, float>();

    std::cerr << "  AVX INEXACT UNALIGNED FLOAT (5/18)..." << std::endl;
    UnitTestFunc<TAvxSigm<false, false>, TSigmCompare<false, false>, float>();
    std::cerr << "  AVX INEXACT ALIGNED FLOAT (6/18)..." << std::endl;
    UnitTestFunc<TAvxSigm<false, true>, TSigmCompare<false, false>, float>();
    std::cerr << "  AVX EXACT UNALIGNED FLOAT (7/18)..." << std::endl;
    UnitTestFunc<TAvxSigm<true, false>, TSigmCompare<true, false>, float>();
    std::cerr << "  AVX EXACT ALIGNED FLOAT (8/18)..." << std::endl;
    UnitTestFunc<TAvxSigm<true, true>, TSigmCompare<true, false>, float>();

    std::cerr << "  BASELINE FLOAT (9/18)..." << std::endl;
    UnitTestFunc<TPlainSigm, TSigmCompare<true, false>, float>();

    std::cerr << "  AVX2 INEXACT UNALIGNED FLOAT (10/18)..." << std::endl;
    UnitTestFunc<TAvx2Sigm<false, false>, TSigmCompare<false, true>, double>();
    std::cerr << "  AVX2 INEXACT ALIGNED DOUBLE (11/18)..." << std::endl;
    UnitTestFunc<TAvx2Sigm<false, true>, TSigmCompare<false, true>, double>();
    std::cerr << "  AVX2 EXACT UNALIGNED DOUBLE (12/18)..." << std::endl;
    UnitTestFunc<TAvx2Sigm<true, false>, TSigmCompare<true, true>, double>();
    std::cerr << "  AVX2 EXACT ALIGNED DOUBLE (13/18)..." << std::endl;
    UnitTestFunc<TAvx2Sigm<true, true>, TSigmCompare<true, true>, double>();

    std::cerr << "  AVX INEXACT UNALIGNED DOUBLE (14/18)..." << std::endl;
    UnitTestFunc<TAvxSigm<false, false>, TSigmCompare<false, true>, double>();
    std::cerr << "  AVX INEXACT ALIGNED DOUBLE (15/18)..." << std::endl;
    UnitTestFunc<TAvxSigm<false, true>, TSigmCompare<false, true>, double>();
    std::cerr << "  AVX EXACT UNALIGNED DOUBLE (16/18)..." << std::endl;
    UnitTestFunc<TAvxSigm<true, false>, TSigmCompare<true, true>, double>();
    std::cerr << "  AVX EXACT ALIGNED DOUBLE (17/18)..." << std::endl;
    UnitTestFunc<TAvxSigm<true, true>, TSigmCompare<true, true>, double>();

    std::cerr << "  BASELINE DOUBLE (18/18)..." << std::endl;
    UnitTestFunc<TPlainSigm, TSigmCompare<true, true>, double>();
}

template <bool Exact, bool IsDouble>
struct TTanhCompare {
    static bool Compare(double input, double trueVal, double approxVal) {
        if (!Exact && !IsDouble) {
            if (input < -1 || input > 1) {
                return GetRelError(trueVal, approxVal) < 1.1e-06;
            } else if (input < -0.1 || input > 0.1) {
                return GetRelError(trueVal, approxVal) < 1.8e-05;
            } else if (input < -0.01 || input > 0.01) {
                return GetRelError(trueVal, approxVal) < 1.4e-04;
            } else if (input < -0.001 || input > 0.001) {
                return GetRelError(trueVal, approxVal) < 3e-04;
            } else if (input < -0.0001 || input > 0.0001) {
                return GetRelError(trueVal, approxVal) < 1e-03;
            } else if (input < -0.00001 || input > 0.00001) {
                return GetRelError(trueVal, approxVal) < 9e-03;
            } else if (input < -0.000001 || input > 0.000001) {
                return GetRelError(trueVal, approxVal) < 0.09;
            } else if (input < -0.0000001 || input > 0.0000001) {
                return GetRelError(trueVal, approxVal) < 0.6;
            } else {
                if (input > 0) {
                    return approxVal >= 0 && approxVal < 1.2e-07;
                } else {
                    return approxVal <= 0 && approxVal > -1.2e-07;
                }
            }
        }
        if (!Exact && IsDouble) {
            if (input < -1 || input > 1) {
                return GetRelError(trueVal, approxVal) < 1e-06;
            } else if (input < -0.11 || input > 0.11) {
                return GetRelError(trueVal, approxVal) < 1.6e-05;
            } else if (input < -0.01 || input > 0.01) {
                return GetRelError(trueVal, approxVal) < 1.2e-04;
            } else if (input < -0.00000000001 || input > 0.00000000001) {
                return GetRelError(trueVal, approxVal) < 2e-04;
            } else if (input < -0.000000000001 || input > 0.000000000001) {
                return GetRelError(trueVal, approxVal) < 3e-04;
            } else {
                if (input > 0) {
                    return approxVal >= 0 && approxVal < 1e-12;
                } else {
                    return approxVal <= 0 && approxVal > -1e-12;
                }
            }
        }
        if (Exact && !IsDouble) {
            if (input < -1 || input > 1) {
                return GetRelError(trueVal, approxVal) < 2.5e-07;
            } else if (input < -0.1 || input > 0.1) {
                return GetRelError(trueVal, approxVal) < 1.25e-06;
            } else if (input < -0.01 || input > 0.01) {
                return GetRelError(trueVal, approxVal) < 1.2e-05;
            } else if (input < -0.001 || input > 0.001) {
                return GetRelError(trueVal, approxVal) < 1.2e-04;
            } else if (input < -0.0001 || input > 0.0001) {
                return GetRelError(trueVal, approxVal) < 1e-03;
            } else if (input < -0.00001 || input > 0.00001) {
                return GetRelError(trueVal, approxVal) < 9e-03;
            } else if (input < -0.000001 || input > 0.000001) {
                return GetRelError(trueVal, approxVal) < 0.09;
            } else if (input < -0.0000001 || input > 0.0000001) {
                return GetRelError(trueVal, approxVal) < 0.6;
            } else {
                if (input > 0) {
                    return approxVal >= 0 && approxVal < 1.2e-07;
                } else {
                    return approxVal <= 0 && approxVal > -1.2e-07;
                }
            }
        }
        if (Exact && IsDouble) {
            if (input < -1 || input > 1) {
                return GetRelError(trueVal, approxVal) < 2.5e-13;
            } else if (input < -0.1 || input > 0.1) {
                return GetRelError(trueVal, approxVal) < 4e-12;
            } else if (input < -0.01 || input > 0.01) {
                return GetRelError(trueVal, approxVal) < 5e-11;
            } else if (input < -0.001 || input > 0.001) {
                return GetRelError(trueVal, approxVal) < 1.5e-10;
            } else if (input < -0.0001 || input > 0.0001) {
                return GetRelError(trueVal, approxVal) < 1.5e-10;
            } else if (input < -0.00001 || input > 0.00001) {
                return GetRelError(trueVal, approxVal) < 1.6e-10;
            } else if (input < -0.000001 || input > 0.000001) {
                return GetRelError(trueVal, approxVal) < 3.6e-10;
            } else if (input < -0.0000001 || input > 0.0000001) {
                return GetRelError(trueVal, approxVal) < 2.3e-9;
            } else if (input < -1e-8 || input > 1e-8) {
                return GetRelError(trueVal, approxVal) < 2e-8;
            } else if (input < -1e-9 || input > 1e-9) {
                return GetRelError(trueVal, approxVal) < 1.8e-7;
            } else if (input < -1e-10 || input > 1e-10) {
                return GetRelError(trueVal, approxVal) < 1.8e-6;
            } else if (input < -1e-11 || input > 1e-11) {
                return GetRelError(trueVal, approxVal) < 1.8e-5;
            } else if (input < -1e-12 || input > 1e-12) {
                return GetRelError(trueVal, approxVal) < 1.8e-4;
            } else if (input < -1e-13 || input > 1e-13) {
                return GetRelError(trueVal, approxVal) < 1.8e-3;
            } else if (input < -1e-14 || input > 1e-14) {
                return GetRelError(trueVal, approxVal) < 1.8e-2;
            } else if (input < -1e-15 || input > 1e-15) {
                return GetRelError(trueVal, approxVal) < 1.4e-1;
            } else {
                if (input > 0) {
                    return approxVal >= 0 && approxVal < 1.2e-15;
                } else {
                    return approxVal <= 0 && approxVal > -1.2e-15;
                }
            }
        }
        UNIT_ASSERT(false);
    }
};

template <bool EXACT, bool ALIGNED>
struct TAvx2Tanh {
    static constexpr bool Aligned = ALIGNED;

    static EFunc GetType() {
        return EFunc::Tanh;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::TanhAvx2<EXACT, ALIGNED>(from, size, to);
    }
};

template <bool EXACT, bool ALIGNED>
struct TAvxTanh {
    static constexpr bool Aligned = ALIGNED;

    static EFunc GetType() {
        return EFunc::Tanh;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::TanhAvx<EXACT, ALIGNED>(from, size, to);
    }
};

struct TPlainTanh {
    static constexpr bool Aligned = false;

    static EFunc GetType() {
        return EFunc::Tanh;
    }

    template <class T>
    static void Apply(const T* from, size_t size, T* to) {
        NFastOps::TanhPlain(from, size, to);
    }
};

void TestFastTanh() {
    std::cerr << "  AVX2 INEXACT UNALIGNED FLOAT (1/18)..." << std::endl;
    UnitTestFunc<TAvx2Tanh<false, false>, TTanhCompare<false, false>, float>();
    std::cerr << "  AVX2 INEXACT ALIGNED FLOAT (2/18)..." << std::endl;
    UnitTestFunc<TAvx2Tanh<false, true>, TTanhCompare<false, false>, float>();
    std::cerr << "  AVX2 EXACT UNALIGNED FLOAT (3/18)..." << std::endl;
    UnitTestFunc<TAvx2Tanh<true, false>, TTanhCompare<true, false>, float>();
    std::cerr << "  AVX2 EXACT ALIGNED FLOAT (4/18)..." << std::endl;
    UnitTestFunc<TAvx2Tanh<true, true>, TTanhCompare<true, false>, float>();

    std::cerr << "  AVX INEXACT UNALIGNED FLOAT (5/18)..." << std::endl;
    UnitTestFunc<TAvxTanh<false, false>, TTanhCompare<false, false>, float>();
    std::cerr << "  AVX INEXACT ALIGNED FLOAT (6/18)..." << std::endl;
    UnitTestFunc<TAvxTanh<false, true>, TTanhCompare<false, false>, float>();
    std::cerr << "  AVX EXACT UNALIGNED FLOAT (7/18)..." << std::endl;
    UnitTestFunc<TAvxTanh<true, false>, TTanhCompare<true, false>, float>();
    std::cerr << "  AVX EXACT ALIGNED FLOAT (8/18)..." << std::endl;
    UnitTestFunc<TAvxTanh<true, true>, TTanhCompare<true, false>, float>();

    std::cerr << "  BASELINE FLOAT (9/18)..." << std::endl;
    UnitTestFunc<TPlainTanh, TTanhCompare<true, false>, float>();

    std::cerr << "  AVX2 INEXACT UNALIGNED FLOAT (10/18)..." << std::endl;
    UnitTestFunc<TAvx2Tanh<false, false>, TTanhCompare<false, true>, double>();
    std::cerr << "  AVX2 INEXACT ALIGNED DOUBLE (11/18)..." << std::endl;
    UnitTestFunc<TAvx2Tanh<false, true>, TTanhCompare<false, true>, double>();
    std::cerr << "  AVX2 EXACT UNALIGNED DOUBLE (12/18)..." << std::endl;
    UnitTestFunc<TAvx2Tanh<true, false>, TTanhCompare<true, true>, double>();
    std::cerr << "  AVX2 EXACT ALIGNED DOUBLE (13/18)..." << std::endl;
    UnitTestFunc<TAvx2Tanh<true, true>, TTanhCompare<true, true>, double>();

    std::cerr << "  AVX INEXACT UNALIGNED DOUBLE (14/18)..." << std::endl;
    UnitTestFunc<TAvxTanh<false, false>, TTanhCompare<false, true>, double>();
    std::cerr << "  AVX INEXACT ALIGNED DOUBLE (15/18)..." << std::endl;
    UnitTestFunc<TAvxTanh<false, true>, TTanhCompare<false, true>, double>();
    std::cerr << "  AVX EXACT UNALIGNED DOUBLE (16/18)..." << std::endl;
    UnitTestFunc<TAvxTanh<true, false>, TTanhCompare<true, true>, double>();
    std::cerr << "  AVX EXACT ALIGNED DOUBLE (17/18)..." << std::endl;
    UnitTestFunc<TAvxTanh<true, true>, TTanhCompare<true, true>, double>();

    std::cerr << "  BASELINE DOUBLE (18/18)..." << std::endl;
    UnitTestFunc<TPlainTanh, TTanhCompare<true, true>, double>();
}

int main() {
    try {
        std::cerr << "Testing exp (1/4)..." << std::endl;
        TestFastExp();
        std::cerr << "Testing log (2/4)..." << std::endl;
        TestFastLog();
        std::cerr << "Testing sigm (3/4)..." << std::endl;
        TestFastSigm();
        std::cerr << "Testing tanh (4/4)..." << std::endl;
        TestFastTanh();
        std::cerr << "All tests PASSED" << std::endl;
        return 0;
    } catch (std::exception& ex) {
        std::cerr << "Test FAILED" << std::endl;
        std::cerr << ex.what() << std::endl;
        return -1;
    }
}
