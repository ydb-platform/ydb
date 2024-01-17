/** Implementation of string conversions.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <locale>
#include <system_error>

#if __cplusplus < 201703
// This is not C++17 or better.  Don't use to_chars/from_chars; the code which
// uses those also relies on other C++17 features.
#if defined(PQXX_HAVE_CHARCONV_INT)
#undef PQXX_HAVE_CHARCONV_INT
#endif
#if defined(PQXX_HAVE_CHARCONV_FLOAT)
#undef PQXX_HAVE_CHARCONV_FLOAT
#endif
#endif

#if defined(PQXX_HAVE_CHARCONV_INT) || defined(PQXX_HAVE_CHARCONV_FLOAT)
#include <charconv>
#endif

#if __cplusplus >= 201703
#include <string_view>
#endif

#include "pqxx/except"
#include "pqxx/strconv"


using namespace pqxx::internal;


namespace
{
/// C string comparison.
inline bool equal(const char lhs[], const char rhs[])
{
  return strcmp(lhs, rhs) == 0;
}
} // namespace


namespace pqxx
{
namespace internal
{
void throw_null_conversion(const std::string &type)
{
  throw conversion_error{"Attempt to convert null to " + type + "."};
}
} // namespace pqxx::internal
} // namespace pqxx


#if defined(PQXX_HAVE_CHARCONV_INT) || defined(PQXX_HAVE_CHARCONV_FLOAT)
namespace
{
template<typename T> void wrap_from_chars(std::string_view in, T &out)
{
  using traits = pqxx::string_traits<T>;
  const char *end = in.data() + in.size();
  const auto res = std::from_chars(in.data(), end, out);
  if (res.ec == std::errc() and res.ptr == end) return;

  std::string msg;
  if (res.ec == std::errc())
  {
    msg = "Could not parse full string.";
  }
  else switch (res.ec)
  {
  case std::errc::result_out_of_range:
    msg = "Value out of range.";
    break;
  case std::errc::invalid_argument:
    msg = "Invalid argument.";
    break;
  default:
    break;
  }

  const std::string base =
	"Could not convert '" + std::string(in) + "' "
	"to " + traits::name();
  if (msg.empty()) throw pqxx::conversion_error{base + "."};
  else throw pqxx::conversion_error{base + ": " + msg};
}


/// How big of a buffer do we want for representing a T?
template<typename T> constexpr int size_buffer()
{
  using lim = std::numeric_limits<T>;
  // Allocate room for how many digits?  There's "max_digits10" for
  // floating-point numbers, but only "digits10" for integer types.
  constexpr auto digits = std::max({lim::digits10, lim::max_digits10});
  // Leave a little bit of extra room for signs, decimal points, and the like.
  return digits + 4;
}


/// Call @c std::to_chars.  It differs for integer vs. floating-point types.
template<typename TYPE, bool INTEGRAL> struct to_chars_caller;

#if defined(PQXX_HAVE_CHARCONV_INT)
/// For integer types, we pass "base 10" to @c std::to_chars.
template<typename TYPE> struct to_chars_caller<TYPE, true>
{
  static std::to_chars_result call(char *begin, char *end, TYPE in)
	{ return std::to_chars(begin, end, in, 10); }
};
#endif

#if defined(PQXX_HAVE_CHARCONV_FLOAT)
/// For floating-point types, we pass "general format" to @c std::to_chars.
template<typename TYPE>
template<typename TYPE> struct to_chars_caller<TYPE, true>
{
  static std::to_chars_result call(char *begin, char *end, TYPE in)
	{ return std::to_chars(begin, end, in, std::chars_format::general); }
};
#endif
} // namespace


namespace pqxx
{
namespace internal
{
template<typename T> std::string builtin_traits<T>::to_string(T in)
{
  using traits = pqxx::string_traits<T>;
  char buf[size_buffer<T>()];

  // Annoying: we need to make slightly different calls to std::to_chars
  // depending on whether this is an integral type or a floating-point type.
  // Use to_chars_caller to hide the difference.
  constexpr bool is_integer = std::numeric_limits<T>::is_integer;
  const auto res = to_chars_caller<T, is_integer>::call(
	buf, buf + sizeof(buf), in);
  if (res.ec == std::errc()) return std::string(buf, res.ptr);

  std::string msg;
  switch (res.ec)
  {
  case std::errc::value_too_large:
    msg = "Value too large.";
    break;
  default:
    break;
  }

  const std::string base =
    std::string{"Could not convert "} + traits::name() + " to string";
  if (msg.empty()) throw pqxx::conversion_error{base + "."};
  else throw pqxx::conversion_error{base + ": " + msg};
}


/// Translate @c from_string calls to @c wrap_from_chars calls.
/** The only difference is the type of the string.
 */
template<typename TYPE>
void builtin_traits<TYPE>::from_string(const char Str[], TYPE &Obj)
	{ wrap_from_chars(std::string_view{Str}, Obj); }
} // namespace pqxx::internal
} // namespace pqxx
#endif // PQXX_HAVE_CHARCONV_INT || PQXX_HAVE_CHARCONV_FLOAT


#if !defined(PQXX_HAVE_CHARCONV_FLOAT)
namespace
{
template<typename T> inline void set_to_Inf(T &t, int sign=1)
{
  T value = std::numeric_limits<T>::infinity();
  if (sign < 0) value = -value;
  t = value;
}
} // namespace
#endif // !PQXX_HAVE_CHARCONV_FLOAT


#if !defined(PQXX_HAVE_CHARCONV_INT)
namespace
{
[[noreturn]] void report_overflow()
{
  throw pqxx::conversion_error{
	"Could not convert string to integer: value out of range."};
}


/** Helper to check for underflow before multiplying a number by 10.
 *
 * Needed just so the compiler doesn't get to complain about an "if (n < 0)"
 * clause that's pointless for unsigned numbers.
 */
template<typename T, bool is_signed> struct underflow_check;

/* Specialization for signed types: check.
 */
template<typename T> struct underflow_check<T, true>
{
  static void check_before_adding_digit(T n)
  {
    constexpr T ten{10};
    if (n < 0 and (std::numeric_limits<T>::min() / ten) > n) report_overflow();
  }
};

/* Specialization for unsigned types: no check needed becaue negative
 * numbers don't exist.
 */
template<typename T> struct underflow_check<T, false>
{
  static void check_before_adding_digit(T) {}
};


/// Return 10*n, or throw exception if it overflows.
template<typename T> T safe_multiply_by_ten(T n)
{
  using limits = std::numeric_limits<T>;
  constexpr T ten{10};
  if (n > 0 and (limits::max() / n) < ten) report_overflow();
  underflow_check<T, limits::is_signed>::check_before_adding_digit(n);
  return T(n * ten);
}


/// Add a digit d to n, or throw exception if it overflows.
template<typename T> T safe_add_digit(T n, T d)
{
  assert((n >= 0 and d >= 0) or (n <=0 and d <= 0));
  if ((n > 0) and (n > (std::numeric_limits<T>::max() - d))) report_overflow();
  if ((n < 0) and (n < (std::numeric_limits<T>::min() - d))) report_overflow();
  return n + d;
}


/// For use in string parsing: add new numeric digit to intermediate value
template<typename L, typename R>
  inline L absorb_digit(L value, R digit)
{
  return L(safe_multiply_by_ten(value) + L(digit));
}


template<typename T> void from_string_signed(const char Str[], T &Obj)
{
  int i = 0;
  T result = 0;

  if (not isdigit(Str[i]))
  {
    if (Str[i] != '-')
      throw pqxx::conversion_error{
        "Could not convert string to integer: '" + std::string{Str} + "'."};

    for (++i; isdigit(Str[i]); ++i)
      result = absorb_digit(result, -digit_to_number(Str[i]));
  }
  else
  {
    for (; isdigit(Str[i]); ++i)
      result = absorb_digit(result, digit_to_number(Str[i]));
  }

  if (Str[i])
    throw pqxx::conversion_error{
      "Unexpected text after integer: '" + std::string{Str} + "'."};

  Obj = result;
}

template<typename T> void from_string_unsigned(const char Str[], T &Obj)
{
  int i = 0;
  T result = 0;

  if (not isdigit(Str[i]))
    throw pqxx::conversion_error{
      "Could not convert string to unsigned integer: '" +
      std::string{Str} + "'."};

  for (; isdigit(Str[i]); ++i)
    result = absorb_digit(result, digit_to_number(Str[i]));

  if (Str[i])
    throw pqxx::conversion_error{
      "Unexpected text after integer: '" + std::string{Str} + "'."};

  Obj = result;
}
} // namespace
#endif // !PQXX_HAVE_CHARCONV_INT


#if !defined(PQXX_HAVE_CHARCONV_FLOAT)
namespace
{
bool valid_infinity_string(const char str[]) noexcept
{
  return
	equal("infinity", str) or
	equal("Infinity", str) or
	equal("INFINITY", str) or
	equal("inf", str);
}


/// Wrapper for std::stringstream with C locale.
/** Some of our string conversions use the standard library.  But, they must
 * _not_ obey the system's locale settings, or a value like 1000.0 might end
 * up looking like "1.000,0".
 *
 * Initialising the stream (including locale and tweaked precision) seems to
 * be expensive though.  So, create thread-local instances which we re-use.
 * It's a lockless way of keeping global variables thread-safe, basically.
 *
 * The stream initialisation happens once per thread, in the constructor.
 * And that's why we need to wrap this in a class.  We can't just do it at the
 * call site, or we'd still be doing it for every call.
 */
template<typename T> class dumb_stringstream : public std::stringstream
{
public:
  // Do not initialise the base-class object using "stringstream{}" (with curly
  // braces): that breaks on Visual C++.  The classic "stringstream()" syntax
  // (with parentheses) does work.
  dumb_stringstream()
  {
    this->imbue(std::locale::classic());
    this->precision(std::numeric_limits<T>::max_digits10);
  }
};


/* These are hard.  Sacrifice performance of specialized, nonflexible,
 * non-localized code and lean on standard library.  Some special-case code
 * handles NaNs.
 */
template<typename T> inline void from_string_float(const char Str[], T &Obj)
{
  bool ok = false;
  T result;

  switch (Str[0])
  {
  case 'N':
  case 'n':
    // Accept "NaN," "nan," etc.
    ok = (
      (Str[1]=='A' or Str[1]=='a') and
      (Str[2]=='N' or Str[2]=='n') and
      (Str[3] == '\0'));
    result = std::numeric_limits<T>::quiet_NaN();
    break;

  case 'I':
  case 'i':
    ok = valid_infinity_string(Str);
    set_to_Inf(result);
    break;

  default:
    if (Str[0] == '-' and valid_infinity_string(&Str[1]))
    {
      ok = true;
      set_to_Inf(result, -1);
    }
    else
    {
      thread_local dumb_stringstream<T> S;
      // Visual Studio 2017 seems to fail on repeated conversions if the
      // clear() is done before the seekg().  Still don't know why!  See #124
      // and #125.
      S.seekg(0);
      S.clear();
      S.str(Str);
      ok = static_cast<bool>(S >> result);
    }
    break;
  }

  if (not ok)
    throw pqxx::conversion_error{
      "Could not convert string to numeric value: '" +
      std::string{Str} + "'."};

  Obj = result;
}
} // namespace
#endif // !PQXX_HAVE_CHARCONV_FLOAT


#if !defined(PQXX_HAVE_CHARCONV_INT)
namespace
{
template<typename T> inline std::string to_string_unsigned(T Obj)
{
  if (not Obj) return "0";

  // Every byte of width on T adds somewhere between 3 and 4 digits to the
  // maximum length of our decimal string.
  char buf[4*sizeof(T)+1];

  char *p = &buf[sizeof(buf)];
  *--p = '\0';
  while (Obj > 0)
  {
    *--p = number_to_digit(int(Obj%10));
    Obj = T(Obj / 10);
  }
  return p;
}
} // namespace
#endif // !PQXX_HAVE_CHARCONV_INT


#if !defined(PQXX_HAVE_CHARCONV_INT) || !defined(PQXX_HAVE_CHARCONV_FLOAT)
namespace
{
template<typename T> inline std::string to_string_fallback(T Obj)
{
  thread_local dumb_stringstream<T> S;
  S.str("");
  S << Obj;
  return S.str();
}
} // namespace
#endif // !PQXX_HAVE_CHARCONV_INT || !PQXX_HAVE_CHARCONV_FLOAT


#if !defined(PQXX_HAVE_CHARCONV_FLOAT)
namespace
{
template<typename T> inline std::string to_string_float(T Obj)
{
  if (std::isnan(Obj)) return "nan";
  if (std::isinf(Obj)) return Obj > 0 ? "infinity" : "-infinity";
  return to_string_fallback(Obj);
}
} // namespace
#endif // !PQXX_HAVE_CHARCONV_FLOAT


#if !defined(PQXX_HAVE_CHARCONV_INT)
namespace
{
template<typename T> inline std::string to_string_signed(T Obj)
{
  if (Obj < 0)
  {
    // Remember--the smallest negative number for a given two's-complement type
    // cannot be negated.
    const bool negatable = (Obj != std::numeric_limits<T>::min());
    if (negatable)
      return '-' + to_string_unsigned(-Obj);
    else
      return to_string_fallback(Obj);
  }

  return to_string_unsigned(Obj);
}
} // namespace
#endif // !PQXX_HAVE_CHARCONV_INT


#if defined(PQXX_HAVE_CHARCONV_INT)
namespace pqxx
{
template void
builtin_traits<short>::from_string(const char[], short &);
template void
builtin_traits<unsigned short>::from_string(const char[], unsigned short &);
template void
builtin_traits<int>::from_string(const char[], int &);
template void
builtin_traits<unsigned int>::from_string(const char[], unsigned int &);
template void
builtin_traits<long>::from_string(const char[], long &);
template void
builtin_traits<unsigned long>::from_string(const char[], unsigned long &);
template void
builtin_traits<long long>::from_string(const char[], long long &);
template void
builtin_traits<unsigned long long>::from_string(
	const char[], unsigned long long &);
} // namespace pqxx
#endif // PQXX_HAVE_CHARCONV_INT


#if defined(PQXX_HAVE_CHARCONV_FLOAT)
namespace pqxx
{
template
void string_traits<float>::from_string(const char Str[], float &Obj);
template
void string_traits<double>::from_string(const char Str[], double &Obj);
template
void string_traits<long double>::from_string(
	const char Str[],
	long double &Obj);
} // namespace pqxx
#endif // PQXX_HAVE_CHARCONV_FLOAT


#if defined(PQXX_HAVE_CHARCONV_INT)
namespace pqxx
{
namespace internal
{
template
std::string builtin_traits<short>::to_string(short Obj);
template
std::string builtin_traits<unsigned short>::to_string(unsigned short Obj);
template
std::string builtin_traits<int>::to_string(int Obj);
template
std::string builtin_traits<unsigned int>::to_string(unsigned int Obj);
template
std::string builtin_traits<long>::to_string(long Obj);
template
std::string builtin_traits<unsigned long>::to_string(unsigned long Obj);
template
std::string builtin_traits<long long>::to_string(long long Obj);
template
std::string builtin_traits<unsigned long long>::to_string(
	unsigned long long Obj);
} // namespace pqxx::internal
} // namespace pqxx
#endif // PQXX_HAVE_CHARCONV_INT


#if defined(PQXX_HAVE_CHARCONV_FLOAT)
namespace pqxx
{
namespace internal
{
template
std::string builtin_traits<float>::to_string(float Obj);
template
std::string builtin_traits<double>::to_string(double Obj);
template
std::string builtin_traits<long double>::to_string(long double Obj);
} // namespace pqxx::internal
} // namespace pqxx
#endif // PQXX_HAVE_CHARCONV_FLOAT


#if !defined(PQXX_HAVE_CHARCONV_INT)
namespace pqxx
{
namespace internal
{
template<>
void builtin_traits<short>::from_string(const char Str[], short &Obj)
	{ from_string_signed(Str, Obj); }
template<>
std::string builtin_traits<short>::to_string(short Obj)
	{ return to_string_signed(Obj); }
template<>
void builtin_traits<unsigned short>::from_string(
	const char Str[],
	unsigned short &Obj)
	{ from_string_unsigned(Str, Obj); }
template<>
std::string builtin_traits<unsigned short>::to_string(unsigned short Obj)
	{ return to_string_unsigned(Obj); }
template<>
void builtin_traits<int>::from_string(const char Str[], int &Obj)
	{ from_string_signed(Str, Obj); }
template<>
std::string builtin_traits<int>::to_string(int Obj)
	{ return to_string_signed(Obj); }
template<>
void builtin_traits<unsigned int>::from_string(
	const char Str[],
	unsigned int &Obj)
	{ from_string_unsigned(Str, Obj); }
template<>
std::string builtin_traits<unsigned int>::to_string(unsigned int Obj)
	{ return to_string_unsigned(Obj); }
template<>
void builtin_traits<long>::from_string(const char Str[], long &Obj)
	{ from_string_signed(Str, Obj); }
template<>
std::string builtin_traits<long>::to_string(long Obj)
	{ return to_string_signed(Obj); }
template<>
void builtin_traits<unsigned long>::from_string(
	const char Str[],
	unsigned long &Obj)
	{ from_string_unsigned(Str, Obj); }
template<>
std::string builtin_traits<unsigned long>::to_string(unsigned long Obj)
	{ return to_string_unsigned(Obj); }
template<>
void builtin_traits<long long>::from_string(const char Str[], long long &Obj)
	{ from_string_signed(Str, Obj); }
template<>
std::string builtin_traits<long long>::to_string(long long Obj)
	{ return to_string_signed(Obj); }
template<>
void builtin_traits<unsigned long long>::from_string(
	const char Str[],
	unsigned long long &Obj)
	{ from_string_unsigned(Str, Obj); }
template<>
std::string builtin_traits<unsigned long long>::to_string(
        unsigned long long Obj)
	{ return to_string_unsigned(Obj); }
} // namespace pqxx::internal
} // namespace pqxx
#endif // !PQXX_HAVE_CHARCONV_INT


#if !defined(PQXX_HAVE_CHARCONV_FLOAT)
namespace pqxx
{
namespace internal
{
template<>
void builtin_traits<float>::from_string(const char Str[], float &Obj)
	{ from_string_float(Str, Obj); }
template<>
std::string builtin_traits<float>::to_string(float Obj)
	{ return to_string_float(Obj); }
template<>
void builtin_traits<double>::from_string(const char Str[], double &Obj)
	{ from_string_float(Str, Obj); }
template<>
std::string builtin_traits<double>::to_string(double Obj)
	{ return to_string_float(Obj); }
template<>
void builtin_traits<long double>::from_string(
	const char Str[], long double &Obj)
	{ from_string_float(Str, Obj); }
template<>
std::string builtin_traits<long double>::to_string(long double Obj)
	{ return to_string_float(Obj); }
} // namespace pqxx::internal
} // namespace pqxx
#endif // !PQXX_HAVE_CHARCONV_FLOAT


namespace pqxx
{
namespace internal
{
template<> void builtin_traits<bool>::from_string(const char Str[], bool &Obj)
{
  bool OK, result=false;

  switch (Str[0])
  {
  case 0:
    result = false;
    OK = true;
    break;

  case 'f':
  case 'F':
    result = false;
    OK = not (
	(Str[1] != '\0') and
	(not equal(Str+1, "alse")) and
	(not equal(Str+1, "ALSE")));
    break;

  case '0':
    {
      int I;
      string_traits<int>::from_string(Str, I);
      result = (I != 0);
      OK = ((I == 0) or (I == 1));
    }
    break;

  case '1':
    result = true;
    OK = (Str[1] == '\0');
    break;

  case 't':
  case 'T':
    result = true;
    OK = not (
	(Str[1] != '\0') and
	(not equal(Str+1, "rue")) and
	(not equal(Str+1, "RUE")));
    break;

  default:
    OK = false;
  }

  if (not OK)
    throw conversion_error{
      "Failed conversion to bool: '" + std::string{Str} + "'."};

  Obj = result;
}


template<> std::string builtin_traits<bool>::to_string(bool Obj)
{
  return Obj ? "true" : "false";
}
} // namespace pqxx::internal
} // namespace pqxx
