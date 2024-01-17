/** Definitions for the pqxx::field class.
 *
 * pqxx::field refers to a field in a query result.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/field instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_FIELD
#define PQXX_H_FIELD

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"
#include "pqxx/internal/type_utils.hxx"

#if defined(PQXX_HAVE_OPTIONAL)
#include <optional>

/* Use std::experimental::optional as a fallback for std::optional, if
 * present.
 *
 * This may break compilation for some software, if using a libpqxx that was
 * configured for a different language version.  To stop libpqxx headers from
 * using or supporting std::experimental::optional, define a macro
 * PQXX_HIDE_EXP_OPTIONAL when building your software.
 */
#elif defined(PQXX_HAVE_EXP_OPTIONAL) && !defined(PQXX_HIDE_EXP_OPTIONAL)
#error #include <experimental/optional>
#endif

#include "pqxx/array.hxx"
#include "pqxx/result.hxx"
#include "pqxx/strconv.hxx"
#include "pqxx/types.hxx"


// Methods tested in eg. test module test01 are marked with "//[t01]".

namespace pqxx
{
/// Reference to a field in a result set.
/** A field represents one entry in a row.  It represents an actual value
 * in the result set, and can be converted to various types.
 */
class PQXX_LIBEXPORT field
{
public:
  using size_type = field_size_type;

  /// Constructor.
  /** Create field as reference to a field in a result set.
   * @param R Row that this field is part of.
   * @param C Column number of this field.
   */
  field(const row &R, row_size_type C) noexcept;			//[t01]

  /**
   * @name Comparison
   */
  //@{
  /// Byte-by-byte comparison of two fields (all nulls are considered equal)
  /** @warning null handling is still open to discussion and change!
   *
   * Handling of null values differs from that in SQL where a comparison
   * involving a null value yields null, so nulls are never considered equal
   * to one another or even to themselves.
   *
   * Null handling also probably differs from the closest equivalent in C++,
   * which is the NaN (Not-a-Number) value, a singularity comparable to
   * SQL's null.  This is because the builtin == operator demands that a == a.
   *
   * The usefulness of this operator is questionable.  No interpretation
   * whatsoever is imposed on the data; 0 and 0.0 are considered different,
   * as are null vs. the empty string, or even different (but possibly
   * equivalent and equally valid) encodings of the same Unicode character
   * etc.
   */
  bool operator==(const field &) const;				//[t75]

  /// Byte-by-byte comparison (all nulls are considered equal)
  /** @warning See operator==() for important information about this operator
   */
  bool operator!=(const field &rhs) const				//[t82]
						   {return not operator==(rhs);}
  //@}

  /**
   * @name Column information
   */
  //@{
  /// Column name
  const char *name() const;						//[t11]

  /// Column type
  oid type() const;							//[t07]

  /// What table did this column come from?
  oid table() const;							//[t02]

  row_size_type num() const { return col(); }				//[t82]

  /// What column number in its originating table did this column come from?
  row_size_type table_column() const;					//[t93]
  //@}

  /**
   * @name Content access
   */
  //@{
  /// Read as plain C string
  /** Since the field's data is stored internally in the form of a
   * zero-terminated C string, this is the fastest way to read it.  Use the
   * to() or as() functions to convert the string to other types such as
   * @c int, or to C++ strings.
   */
  const char *c_str() const;						//[t02]

  /// Is this field's value null?
  bool is_null() const noexcept;					//[t12]

  /// Return number of bytes taken up by the field's value.
  /**
   * Includes the terminating zero byte.
   */
  size_type size() const noexcept;					//[t11]

  /// Read value into Obj; or leave Obj untouched and return @c false if null
  /** Note this can be used with optional types (except pointers other than
   * C-strings)
   */
  template<typename T> auto to(T &Obj) const				//[t03]
    -> typename std::enable_if<(
      not std::is_pointer<T>::value
      or std::is_same<T, const char*>::value
    ), bool>::type
  {
    const char *const bytes = c_str();
    if (bytes[0] == '\0' and is_null()) return false;
    from_string(bytes, Obj);
    return true;
  }

  /// Read value into Obj; or leave Obj untouched and return @c false if null
  template<typename T> bool operator>>(T &Obj) const			//[t07]
      { return to(Obj); }

  /// Read value into Obj; or use Default & return @c false if null
  /** Note this can be used with optional types (except pointers other than
   * C-strings)
   */
  template<typename T> auto to(T &Obj, const T &Default) const	//[t12]
    -> typename std::enable_if<(
      not std::is_pointer<T>::value
      or std::is_same<T, const char*>::value
    ), bool>::type
  {
    const bool NotNull = to(Obj);
    if (not NotNull) Obj = Default;
    return NotNull;
  }

  /// Return value as object of given type, or Default if null
  /** Note that unless the function is instantiated with an explicit template
   * argument, the Default value's type also determines the result type.
   */
  template<typename T> T as(const T &Default) const			//[t01]
  {
    T Obj;
    to(Obj, Default);
    return Obj;
  }

  /// Return value as object of given type, or throw exception if null
  /** Use as `as<std::optional<int>>()` or `as<my_untemplated_optional_t>()` as
   * an alternative to `get<int>()`; this is disabled for use with raw pointers
   * (other than C-strings) because storage for the value can't safely be
   * allocated here
   */
  template<typename T> T as() const					//[t45]
  {
    T Obj;
    if (not to(Obj)) Obj = string_traits<T>::null();
    return Obj;
  }

  /// Return value wrapped in some optional type (empty for nulls)
  /** Use as `get<int>()` as before to obtain previous behavior (i.e. only
   * usable when `std::optional` or `std::experimental::optional` are
   * available), or specify container type with `get<int, std::optional>()`
   */
  template<typename T, template<typename> class O
#if defined(PQXX_HAVE_OPTIONAL)
    = std::optional
#elif defined(PQXX_HAVE_EXP_OPTIONAL) && !defined(PQXX_HIDE_EXP_OPTIONAL)
    = std::experimental::optional
#endif
  > constexpr O<T> get() const { return as<O<T>>(); }

  /// Parse the field as an SQL array.
  /** Call the parser to retrieve values (and structure) from the array.
   *
   * Make sure the @c result object stays alive until parsing is finished.  If
   * you keep the @c row of @c field object alive, it will keep the @c result
   * object alive as well.
   */
  array_parser as_array() const
        { return array_parser{c_str(), m_home.m_encoding}; }
  //@}


protected:
  const result &home() const noexcept { return m_home; }
  size_t idx() const noexcept { return m_row; }
  row_size_type col() const noexcept { return row_size_type(m_col); }

  /**
   * You'd expect this to be a size_t, but due to the way reverse iterators
   * are related to regular iterators, it must be allowed to underflow to -1.
   */
  long m_col;

private:
  result m_home;
  size_t m_row;
};


/// Specialization: <tt>to(string &)</tt>.
template<>
inline bool field::to<std::string>(std::string &Obj) const
{
  const char *const bytes = c_str();
  if (bytes[0] == '\0' and is_null()) return false;
  Obj = std::string{bytes, size()};
  return true;
}

/// Specialization: <tt>to(const char *&)</tt>.
/** The buffer has the same lifetime as the data in this result (i.e. of this
 * result object, or the last remaining one copied from it etc.), so take care
 * not to use it after the last result object referring to this query result is
 * destroyed.
 */
template<>
inline bool field::to<const char *>(const char *&Obj) const
{
  if (is_null()) return false;
  Obj = c_str();
  return true;
}


template<typename CHAR=char, typename TRAITS=std::char_traits<CHAR>>
  class field_streambuf :
  public std::basic_streambuf<CHAR, TRAITS>
{
public:
  using char_type = CHAR;
  using traits_type = TRAITS;
  using int_type = typename traits_type::int_type;
  using pos_type = typename traits_type::pos_type;
  using off_type = typename traits_type::off_type;
  using openmode = std::ios::openmode;
  using seekdir = std::ios::seekdir;

  explicit field_streambuf(const field &F) :			//[t74]
    m_field{F}
  {
    initialize();
  }

protected:
  virtual int sync() override { return traits_type::eof(); }

protected:
  virtual pos_type seekoff(off_type, seekdir, openmode) override
	{ return traits_type::eof(); }
  virtual pos_type seekpos(pos_type, openmode) override
	{return traits_type::eof();}
  virtual int_type overflow(int_type) override
	{ return traits_type::eof(); }
  virtual int_type underflow() override
	{ return traits_type::eof(); }

private:
  const field &m_field;

  int_type initialize()
  {
    char_type *G =
      reinterpret_cast<char_type *>(const_cast<char *>(m_field.c_str()));
    this->setg(G, G, G + m_field.size());
    return int_type(m_field.size());
  }
};


/// Input stream that gets its data from a result field
/** Use this class exactly as you would any other istream to read data from a
 * field.  All formatting and streaming operations of @c std::istream are
 * supported.  What you'll typically want to use, however, is the fieldstream
 * alias (which defines a basic_fieldstream for @c char).  This is similar to
 * how e.g. @c std::ifstream relates to @c std::basic_ifstream.
 *
 * This class has only been tested for the char type (and its default traits).
 */
template<typename CHAR=char, typename TRAITS=std::char_traits<CHAR>>
  class basic_fieldstream :
    public std::basic_istream<CHAR, TRAITS>
{
  using super = std::basic_istream<CHAR, TRAITS>;

public:
  using char_type = CHAR;
  using traits_type = TRAITS;
  using int_type = typename traits_type::int_type;
  using pos_type = typename traits_type::pos_type;
  using off_type = typename traits_type::off_type;

  basic_fieldstream(const field &F) : super{nullptr}, m_buf{F}
	{ super::init(&m_buf); }

private:
  field_streambuf<CHAR, TRAITS> m_buf;
};

using fieldstream = basic_fieldstream<char>;

/// Write a result field to any type of stream
/** This can be convenient when writing a field to an output stream.  More
 * importantly, it lets you write a field to e.g. a @c stringstream which you
 * can then use to read, format and convert the field in ways that to() does not
 * support.
 *
 * Example: parse a field into a variable of the nonstandard
 * "<tt>long long</tt>" type.
 *
 * @code
 * extern result R;
 * long long L;
 * stringstream S;
 *
 * // Write field's string into S
 * S << R[0][0];
 *
 * // Parse contents of S into L
 * S >> L;
 * @endcode
 */
template<typename CHAR>
inline std::basic_ostream<CHAR> &operator<<(
	std::basic_ostream<CHAR> &S, const field &F)		        //[t46]
{
  S.write(F.c_str(), std::streamsize(F.size()));
  return S;
}


/// Convert a field's string contents to another type.
template<typename T>
inline void from_string(const field &F, T &Obj)				//[t46]
	{ from_string(F.c_str(), Obj, F.size()); }

/// Convert a field to a string.
template<> PQXX_LIBEXPORT std::string to_string(const field &Obj);	//[t74]

} // namespace pqxx
#include "pqxx/compiler-internal-post.hxx"
#endif
