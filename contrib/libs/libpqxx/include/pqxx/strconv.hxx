/** String conversion definitions.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/stringconv instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_STRINGCONV
#define PQXX_H_STRINGCONV

#include "pqxx/compiler-public.hxx"

#include <limits>
#include <sstream>
#include <stdexcept>


namespace pqxx
{

/**
 * @defgroup stringconversion String conversion
 *
 * The PostgreSQL server accepts and represents data in string form.  It has
 * its own formats for various data types.  The string conversions define how
 * various C++ types translate to and from their respective PostgreSQL text
 * representations.
 *
 * Each conversion is defined by a specialisation of the @c string_traits
 * template.  This template implements some basic functions to support the
 * conversion, ideally in both directions.
 *
 * If you need to convert a type which is not supported out of the box, define
 * your own @c string_traits specialisation for that type, similar to the ones
 * defined here.  Any conversion code which "sees" your specialisation will now
 * support your conversion.  In particular, you'll be able to read result
 * fields into a variable of the new type.
 *
 * There is a macro to help you define conversions for individual enumeration
 * types.  The conversion will represent enumeration values as numeric strings.
 */
//@{

/// Traits class for use in string conversions
/** Specialize this template for a type that you wish to add to_string and
 * from_string support for.
 */
template<typename T, typename = void> struct string_traits;

namespace internal
{
/// Throw exception for attempt to convert null to given type.
[[noreturn]] PQXX_LIBEXPORT void throw_null_conversion(
	const std::string &type);

/// Give a human-readable name for a type, at compile time.
/** Each instantiation contains a static member called @c value which is the
 * type's name, as a string.
 *
 * This template should not be around for long.  C++14's variable templates
 * make it easier (eliminating the cumbersome struct) and C++20's introspection
 * should obviate it completely.
 */
template<typename TYPE> struct type_name;
#define PQXX_DECLARE_TYPE_NAME(TYPE) \
  template<> struct type_name<TYPE> \
  { static constexpr const char *value = #TYPE; }

PQXX_DECLARE_TYPE_NAME(bool);
PQXX_DECLARE_TYPE_NAME(short);
PQXX_DECLARE_TYPE_NAME(unsigned short);
PQXX_DECLARE_TYPE_NAME(int);
PQXX_DECLARE_TYPE_NAME(unsigned int);
PQXX_DECLARE_TYPE_NAME(long);
PQXX_DECLARE_TYPE_NAME(unsigned long);
PQXX_DECLARE_TYPE_NAME(long long);
PQXX_DECLARE_TYPE_NAME(unsigned long long);
PQXX_DECLARE_TYPE_NAME(float);
PQXX_DECLARE_TYPE_NAME(double);
PQXX_DECLARE_TYPE_NAME(long double);
PQXX_DECLARE_TYPE_NAME(char *);
PQXX_DECLARE_TYPE_NAME(const char *);
PQXX_DECLARE_TYPE_NAME(std::string);
PQXX_DECLARE_TYPE_NAME(const std::string);
PQXX_DECLARE_TYPE_NAME(std::stringstream);
#undef PQXX_DECLARE_TYPE_NAME

template<size_t N> struct type_name<char[N]>
{ static constexpr const char *value = "char[]"; };


/// Helper: string traits implementation for built-in types.
/** These types all look much alike, so they can share much of their traits
 * classes (though templatised, of course).
 *
 * The actual `to_string` and `from_string` are implemented in the library,
 * but the rest is defined inline.
 */
template<typename TYPE> struct PQXX_LIBEXPORT builtin_traits
{
  static constexpr const char *name() noexcept
	{ return internal::type_name<TYPE>::value; }
  static constexpr bool has_null() noexcept { return false; }
  static bool is_null(TYPE) { return false; }
  [[noreturn]] static TYPE null() { throw_null_conversion(name()); }
  static void from_string(const char Str[], TYPE &Obj);
  static std::string to_string(TYPE Obj);
};
} // namespace pqxx::internal


/// Helper: declare a string_traits specialisation for a builtin type.
#define PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(TYPE) \
  template<> struct PQXX_LIBEXPORT string_traits<TYPE> : \
    internal::builtin_traits<TYPE> {};

PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(bool)

PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(short)
PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(unsigned short)
PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(int)
PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(unsigned int)
PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(long)
PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(unsigned long)
PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(long long)
PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(unsigned long long)

PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(float)
PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(double)
PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION(long double)

#undef PQXX_DECLARE_STRING_TRAITS_SPECIALIZATION


/// Helper class for defining enum conversions.
/** The conversion will convert enum values to numeric strings, and vice versa.
 *
 * To define a string conversion for an enum type, derive a @c string_traits
 * specialisation for the enum from this struct.
 *
 * There's usually an easier way though: the @c PQXX_DECLARE_ENUM_CONVERSION
 * macro.  Use @c enum_traits manually only if you need to customise your
 * traits type in more detail, e.g. if your enum has a "null" value built in.
 */
template<typename ENUM>
struct enum_traits
{
  using underlying_type = typename std::underlying_type<ENUM>::type;
  using underlying_traits = string_traits<underlying_type>;

  static constexpr bool has_null() noexcept { return false; }
  [[noreturn]] static ENUM null()
	{ internal::throw_null_conversion("enum type"); }

  static void from_string(const char Str[], ENUM &Obj)
  {
    underlying_type tmp;
    underlying_traits::from_string(Str, tmp);
    Obj = ENUM(tmp);
  }

  static std::string to_string(ENUM Obj)
	{ return underlying_traits::to_string(underlying_type(Obj)); }
};


/// Macro: Define a string conversion for an enum type.
/** This specialises the @c pqxx::string_traits template, so use it in the
 * @c ::pqxx namespace.
 *
 * For example:
 *
 *      #include <iostream>
 *      #include <pqxx/strconv>
 *      enum X { xa, xb };
 *      namespace pqxx { PQXX_DECLARE_ENUM_CONVERSION(x); }
 *      int main() { std::cout << to_string(xa) << std::endl; }
 */
#define PQXX_DECLARE_ENUM_CONVERSION(ENUM) \
template<> \
struct string_traits<ENUM> : pqxx::enum_traits<ENUM> \
{ \
  static constexpr const char *name() noexcept { return #ENUM; } \
  [[noreturn]] static ENUM null() \
	{ internal::throw_null_conversion(name()); } \
}


/// String traits for C-style string ("pointer to const char")
template<> struct PQXX_LIBEXPORT string_traits<const char *>
{
  static constexpr const char *name() noexcept { return "const char *"; }
  static constexpr bool has_null() noexcept { return true; }
  static bool is_null(const char *t) { return t == nullptr; }
  static const char *null() { return nullptr; }
  static void from_string(const char Str[], const char *&Obj) { Obj = Str; }
  static std::string to_string(const char *Obj) { return Obj; }
};

/// String traits for non-const C-style string ("pointer to char")
template<> struct PQXX_LIBEXPORT string_traits<char *>
{
  static constexpr const char *name() noexcept { return "char *"; }
  static constexpr bool has_null() noexcept { return true; }
  static bool is_null(const char *t) { return t == nullptr; }
  static const char *null() { return nullptr; }

  // Don't allow this conversion since it breaks const-safety.
  // static void from_string(const char Str[], char *&Obj);

  static std::string to_string(char *Obj) { return Obj; }
};

/// String traits for C-style string constant ("array of char")
template<size_t N> struct PQXX_LIBEXPORT string_traits<char[N]>
{
  static constexpr const char *name() noexcept { return "char[]"; }
  static constexpr bool has_null() noexcept { return true; }
  static bool is_null(const char t[]) { return t == nullptr; }
  static const char *null() { return nullptr; }
  static std::string to_string(const char Obj[]) { return Obj; }
};

template<> struct PQXX_LIBEXPORT string_traits<std::string>
{
  static constexpr const char *name() noexcept { return "string"; }
  static constexpr bool has_null() noexcept { return false; }
  static bool is_null(const std::string &) { return false; }
  [[noreturn]] static std::string null()
	{ internal::throw_null_conversion(name()); }
  static void from_string(const char Str[], std::string &Obj) { Obj=Str; }
  static std::string to_string(const std::string &Obj) { return Obj; }
};

template<> struct PQXX_LIBEXPORT string_traits<const std::string>
{
  static constexpr const char *name() noexcept { return "const string"; }
  static constexpr bool has_null() noexcept { return false; }
  static bool is_null(const std::string &) { return false; }
  [[noreturn]] static const std::string null()
	{ internal::throw_null_conversion(name()); }
  static const std::string to_string(const std::string &Obj) { return Obj; }
};

template<> struct PQXX_LIBEXPORT string_traits<std::stringstream>
{
  static constexpr const char *name() noexcept { return "stringstream"; }
  static constexpr bool has_null() noexcept { return false; }
  static bool is_null(const std::stringstream &) { return false; }
  [[noreturn]] static std::stringstream null()
	{ internal::throw_null_conversion(name()); }
  static void from_string(const char Str[], std::stringstream &Obj)
	{ Obj.clear(); Obj << Str; }
  static std::string to_string(const std::stringstream &Obj)
	{ return Obj.str(); }
};


// TODO: Implement date conversions.

/// Attempt to convert postgres-generated string to given built-in type
/** If the form of the value found in the string does not match the expected
 * type, e.g. if a decimal point is found when converting to an integer type,
 * the conversion fails.  Overflows (e.g. converting "9999999999" to a 16-bit
 * C++ type) are also treated as errors.  If in some cases this behaviour should
 * be inappropriate, convert to something bigger such as @c long @c int first
 * and then truncate the resulting value.
 *
 * Only the simplest possible conversions are supported.  No fancy features
 * such as hexadecimal or octal, spurious signs, or exponent notation will work.
 * No whitespace is stripped away.  Only the kinds of strings that come out of
 * PostgreSQL and out of to_string() can be converted.
 */
template<typename T>
  inline void from_string(const char Str[], T &Obj)
{
  if (Str == nullptr) throw std::runtime_error{"Attempt to read null string."};
  string_traits<T>::from_string(Str, Obj);
}


/// Conversion with known string length (for strings that may contain nuls)
/** This is only used for strings, where embedded nul bytes should not determine
 * the end of the string.
 *
 * For all other types, this just uses the regular, nul-terminated version of
 * from_string().
 */
template<typename T> inline void from_string(const char Str[], T &Obj, size_t)
{
  return from_string(Str, Obj);
}

template<>
  inline void from_string<std::string>(					//[t00]
	const char Str[],
	std::string &Obj,
	size_t len)
{
  if (Str == nullptr) throw std::runtime_error{"Attempt to read null string."};
  Obj.assign(Str, len);
}

template<typename T>
  inline void from_string(const std::string &Str, T &Obj)		//[t45]
	{ from_string(Str.c_str(), Obj); }

template<typename T>
  inline void from_string(const std::stringstream &Str, T &Obj)		//[t00]
	{ from_string(Str.str(), Obj); }

template<> inline void
from_string(const std::string &Str, std::string &Obj)			//[t46]
	{ Obj = Str; }


namespace internal
{
/// Compute numeric value of given textual digit (assuming that it is a digit)
constexpr int digit_to_number(char c) noexcept { return c-'0'; }
constexpr char number_to_digit(int i) noexcept
	{ return static_cast<char>(i+'0'); }
} // namespace pqxx::internal


/// Convert built-in type to a readable string that PostgreSQL will understand
/** No special formatting is done, and any locale settings are ignored.  The
 * resulting string will be human-readable and in a format suitable for use in
 * SQL queries.
 */
template<typename T> inline std::string to_string(const T &Obj)
	{ return string_traits<T>::to_string(Obj); }

//@}

} // namespace pqxx

#endif
