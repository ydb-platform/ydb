#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/util/source_location.h>

namespace NYdb::NUt {
class TTestContextBase {
public:
  TTestContextBase(const NKikimr::NCompat::TSourceLocation &loc =
                       NKikimr::NCompat::TSourceLocation::current())
      : Location_(loc) {}

  virtual ~TTestContextBase() = default;
  virtual TString Format() const = 0;

protected:
  TString FormatLocation() const {
    return Sprintf("%s:%d, %s", Location_.file_name(), Location_.line(),
                   Location_.function_name());
  }

  NKikimr::NCompat::TSourceLocation Location_;
};

// Simple default context - just location
struct TTestContext : public TTestContextBase {
  TTestContext(const NKikimr::NCompat::TSourceLocation &loc =
                   NKikimr::NCompat::TSourceLocation::current())
      : TTestContextBase(loc) {}

  TString Format() const override { return FormatLocation(); }
};
} // namespace NYdb::NUt

// Context-aware fail implementation
// TODO: After library/cpp/testing/unittest extension we won't need any custom
// CTX_UNIT redefinition (we just use customization point and replace original
// UNIT_FAIL_IMPL with context aware version)
#define CTX_UNIT_FAIL_IMPL(R, M)                                               \
  do {                                                                         \
    ::TStringBuilder locationInfo;                                             \
    if constexpr (                                                             \
        requires { testCtx; } &&                                               \
        std::derived_from<decltype(testCtx),                                   \
                          ::NYdb::NUt::TTestContextBase> &&              \
        requires {                                                             \
          { testCtx.Format() } -> std::convertible_to<TString>;                \
        }) {                                                                   \
      locationInfo << testCtx.Format();                                        \
    } else {                                                                   \
      locationInfo << __LOCATION__ << ", " << __PRETTY_FUNCTION__;             \
    }                                                                          \
    ::NUnitTest::NPrivate::RaiseError(                                         \
        R, ::TStringBuilder() << R << " at " << locationInfo << ": " << M,     \
        true);                                                                 \
  } while (false)

#define CTX_UNIT_ASSERT_C(A, C)                                                \
  do {                                                                         \
    if (!(A)) {                                                                \
      CTX_UNIT_FAIL_IMPL(                                                      \
          "assertion failed",                                                  \
          Sprintf("(%s) %s", #A, (::TStringBuilder() << C).data()));           \
    }                                                                          \
  } while (false)

#define CTX_UNIT_ASSERT(A) CTX_UNIT_ASSERT_C(A, "")

// values with context
#define CTX_UNIT_ASSERT_VALUES_EQUAL_IMPL(A, B, C, EQflag, EQstr, NEQstr)      \
  do {                                                                         \
    /* NOLINTBEGIN(bugprone-reserved-identifier,                               \
     * readability-identifier-naming) */                                       \
    TString _as;                                                               \
    TString _bs;                                                               \
    TString _asInd;                                                            \
    TString _bsInd;                                                            \
    bool _usePlainDiff;                                                        \
    if (!::NUnitTest::NPrivate::CompareAndMakeStrings(                         \
            A, B, _as, _asInd, _bs, _bsInd, _usePlainDiff, EQflag)) {          \
      auto &&failMsg = Sprintf("(%s %s %s) failed: (%s %s %s) %s", #A, EQstr,  \
                               #B, _as.data(), NEQstr, _bs.data(),             \
                               (::TStringBuilder() << C).data());              \
      if (EQflag && !_usePlainDiff) {                                          \
        failMsg += ", with diff:\n";                                           \
        failMsg += ::NUnitTest::ColoredDiff(_asInd, _bsInd);                   \
      }                                                                        \
      CTX_UNIT_FAIL_IMPL("assertion failed", failMsg);                         \
    }                                                                          \
    /* NOLINTEND(bugprone-reserved-identifier, readability-identifier-naming)  \
     */                                                                        \
  } while (false)

#define CTX_UNIT_ASSERT_VALUES_EQUAL_C(A, B, C)                                \
  CTX_UNIT_ASSERT_VALUES_EQUAL_IMPL(A, B, C, true, "==", "!=")

#define CTX_UNIT_ASSERT_VALUES_UNEQUAL_C(A, B, C)                              \
  CTX_UNIT_ASSERT_VALUES_EQUAL_IMPL(A, B, C, false, "!=", "==")

#define CTX_UNIT_ASSERT_VALUES_EQUAL(A, B)                                     \
  CTX_UNIT_ASSERT_VALUES_EQUAL_C(A, B, "")
#define CTX_UNIT_ASSERT_VALUES_UNEQUAL(A, B)                                   \
  CTX_UNIT_ASSERT_VALUES_UNEQUAL_C(A, B, "")
