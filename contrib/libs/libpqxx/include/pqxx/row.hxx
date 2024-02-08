/** Definitions for the pqxx::result class and support classes.
 *
 * pqxx::result represents the set of result rows from a database query.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/result instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_ROW
#define PQXX_H_ROW

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include "pqxx/except.hxx"
#include "pqxx/field.hxx"
#include "pqxx/result.hxx"


// Methods tested in eg. test module test01 are marked with "//[t01]".

namespace pqxx
{
/// Reference to one row in a result.
/** A row represents one row (also called a row) in a query result set.
 * It also acts as a container mapping column numbers or names to field
 * values (see below):
 *
 * @code
 *	cout << row["date"].c_str() << ": " << row["name"].c_str() << endl;
 * @endcode
 *
 * The row itself acts like a (non-modifyable) container, complete with its
 * own const_iterator and const_reverse_iterator.
 */
class PQXX_LIBEXPORT row
{
public:
  using size_type = row_size_type;
  using difference_type = row_difference_type;
  using const_iterator = const_row_iterator;
  using iterator = const_iterator;
  using reference = field;
  using pointer = const_row_iterator;
  using const_reverse_iterator = const_reverse_row_iterator;
  using reverse_iterator = const_reverse_iterator;

  row() =default;

  /// @deprecated Do not use this constructor.  It will become private.
  row(result r, size_t i) noexcept;

  ~row() noexcept =default; // Yes Scott Meyers, you're absolutely right[1]

  /**
   * @name Comparison
   */
  //@{
  PQXX_PURE bool operator==(const row &) const noexcept;		//[t75]
  bool operator!=(const row &rhs) const noexcept			//[t75]
      { return not operator==(rhs); }
  //@}

  const_iterator begin() const noexcept;				//[t82]
  const_iterator cbegin() const noexcept;
  const_iterator end() const noexcept;					//[t82]
  const_iterator cend() const noexcept;

  /**
   * @name Field access
   */
  //@{
  reference front() const noexcept;					//[t74]
  reference back() const noexcept;					//[t75]

  const_reverse_row_iterator rbegin() const;				//[t82]
  const_reverse_row_iterator crbegin() const;
  const_reverse_row_iterator rend() const;				//[t82]
  const_reverse_row_iterator crend() const;

  reference operator[](size_type) const noexcept;			//[t11]
  reference operator[](int) const noexcept;				//[t02]
  /** Address field by name.
   * @warning This is much slower than indexing by number, or iterating.
   */
  reference operator[](const char[]) const;				//[t11]
  /** Address field by name.
   * @warning This is much slower than indexing by number, or iterating.
   */
  reference operator[](const std::string &) const;			//[t11]
  reference at(size_type) const; 					//[t11]
  reference at(int) const;						//[t11]
  /** Address field by name.
   * @warning This is much slower than indexing by number, or iterating.
   */
  reference at(const char[]) const;					//[t11]
  /** Address field by name.
   * @warning This is much slower than indexing by number, or iterating.
   */
  reference at(const std::string &) const;				//[t11]
  //@}

  size_type size() const noexcept					//[t11]
						     { return m_end-m_begin; }

  void swap(row &) noexcept;						//[t11]

  /// Row number, assuming this is a real row and not end()/rend().
  size_t rownumber() const noexcept { return size_t(m_index); }		//[t11]

  /**
   * @name Column information
   */
  //@{
  /// Number of given column (throws exception if it doesn't exist)
  size_type column_number(const std::string &ColName) const		//[t30]
      { return column_number(ColName.c_str()); }

  /// Number of given column (throws exception if it doesn't exist)
  size_type column_number(const char[]) const;       			//[t30]

  /// Type of given column
  oid column_type(size_type) const;					//[t07]

  /// Type of given column
  oid column_type(int ColNum) const					//[t07]
      { return column_type(size_type(ColNum)); }

  /// Type of given column
  oid column_type(const std::string &ColName) const			//[t07]
      { return column_type(column_number(ColName)); }

  /// Type of given column
  oid column_type(const char ColName[]) const				//[t07]
      { return column_type(column_number(ColName)); }

  /// What table did this column come from?
  oid column_table(size_type ColNum) const;				//[t02]

  /// What table did this column come from?
  oid column_table(int ColNum) const					//[t02]
      { return column_table(size_type(ColNum)); }
  /// What table did this column come from?
  oid column_table(const std::string &ColName) const			//[t02]
      { return column_table(column_number(ColName)); }

  /// What column number in its table did this result column come from?
  /** A meaningful answer can be given only if the column in question comes
   * directly from a column in a table.  If the column is computed in any
   * other way, a logic_error will be thrown.
   *
   * @param ColNum a zero-based column number in this result set
   * @return a zero-based column number in originating table
   */
  size_type table_column(size_type) const;				//[t93]

  /// What column number in its table did this result column come from?
  size_type table_column(int ColNum) const				//[t93]
      { return table_column(size_type(ColNum)); }

  /// What column number in its table did this result column come from?
  size_type table_column(const std::string &ColName) const		//[t93]
      { return table_column(column_number(ColName)); }
  //@}

  size_t num() const { return rownumber(); }				//[t01]

  /** Produce a slice of this row, containing the given range of columns.
   *
   * The slice runs from the range's starting column to the range's end
   * column, exclusive.  It looks just like a normal result row, except
   * slices can be empty.
   *
   * @warning Slicing is a relatively new feature, and not all software may be
   * prepared to deal with empty slices.  If there is any chance that your
   * program might be creating empty slices and passing them to code that may
   * not be designed with the possibility of empty rows in mind, be sure to
   * test for that case.
   */
  row slice(size_type Begin, size_type End) const;

  // Is this an empty slice?
  PQXX_PURE bool empty() const noexcept;

protected:
  friend class field;
  /// Result set of which this is one row.
  result m_result;
  /// Row number.
  /**
   * You'd expect this to be a size_t, but due to the way reverse iterators
   * are related to regular iterators, it must be allowed to underflow to -1.
   */
  long m_index = 0;
  /// First column in slice.  This row ignores lower-numbered columns.
  size_type m_begin = 0;
  /// End column in slice.  This row only sees lower-numbered columns.
  size_type m_end = 0;
};


/// Iterator for fields in a row.  Use as row::const_iterator.
class PQXX_LIBEXPORT const_row_iterator : public field
{
public:
  using iterator_category = std::random_access_iterator_tag;
  using value_type = const field;
  using pointer = const field *;
  using size_type = row_size_type;
  using difference_type = row_difference_type;
  using reference = field;

  const_row_iterator(const row &T, row_size_type C) noexcept :		//[t82]
    field{T, C} {}
  const_row_iterator(const field &F) noexcept : field{F} {}		//[t82]

  /**
   * @name Dereferencing operators
   */
  //@{
  pointer operator->() const { return this; }				//[t82]
  reference operator*() const { return field{*this}; }			//[t82]
  //@}

  /**
   * @name Manipulations
   */
  //@{
  const_row_iterator operator++(int);					//[t82]
  const_row_iterator &operator++() { ++m_col; return *this; }		//[t82]
  const_row_iterator operator--(int);					//[t82]
  const_row_iterator &operator--() { --m_col; return *this; }		//[t82]

  const_row_iterator &operator+=(difference_type i)			//[t82]
      { m_col = size_type(difference_type(m_col) + i); return *this; }
  const_row_iterator &operator-=(difference_type i)			//[t82]
      { m_col = size_type(difference_type(m_col) - i); return *this; }
  //@}

  /**
   * @name Comparisons
   */
  //@{
  bool operator==(const const_row_iterator &i) const			//[t82]
      {return col()==i.col();}
  bool operator!=(const const_row_iterator &i) const			//[t82]
      {return col()!=i.col();}
  bool operator<(const const_row_iterator &i) const			//[t82]
      {return col()<i.col();}
  bool operator<=(const const_row_iterator &i) const			//[t82]
      {return col()<=i.col();}
  bool operator>(const const_row_iterator &i) const			//[t82]
      {return col()>i.col();}
  bool operator>=(const const_row_iterator &i) const			//[t82]
      {return col()>=i.col();}
  //@}

  /**
   * @name Arithmetic operators
   */
  //@{
  inline const_row_iterator operator+(difference_type) const;		//[t82]

  friend const_row_iterator operator+(					//[t82]
	difference_type,
	const_row_iterator);

  inline const_row_iterator operator-(difference_type) const;		//[t82]
  inline difference_type operator-(const_row_iterator) const;		//[t82]
  //@}
};


/// Reverse iterator for a row.  Use as row::const_reverse_iterator.
class PQXX_LIBEXPORT const_reverse_row_iterator : private const_row_iterator
{
public:
  using super = const_row_iterator;
  using iterator_type = const_row_iterator;
  using iterator_type::iterator_category;
  using iterator_type::difference_type;
  using iterator_type::pointer;
  using value_type = iterator_type::value_type;
  using reference = iterator_type::reference;

  const_reverse_row_iterator(const const_reverse_row_iterator &r) :	//[t82]
    const_row_iterator{r} {}
  explicit
    const_reverse_row_iterator(const super &rhs) noexcept :		//[t82]
      const_row_iterator{rhs} { super::operator--(); }

  PQXX_PURE iterator_type base() const noexcept;			//[t82]

  /**
   * @name Dereferencing operators
   */
  //@{
  using iterator_type::operator->;					//[t82]
  using iterator_type::operator*;					//[t82]
  //@}

  /**
   * @name Manipulations
   */
  //@{
  const_reverse_row_iterator &
    operator=(const const_reverse_row_iterator &r)			//[t82]
      { iterator_type::operator=(r); return *this; }
  const_reverse_row_iterator operator++()				//[t82]
      { iterator_type::operator--(); return *this; }
  const_reverse_row_iterator operator++(int);				//[t82]
  const_reverse_row_iterator &operator--()				//[t82]
      { iterator_type::operator++(); return *this; }
  const_reverse_row_iterator operator--(int);				//[t82]
  const_reverse_row_iterator &operator+=(difference_type i)		//[t82]
      { iterator_type::operator-=(i); return *this; }
  const_reverse_row_iterator &operator-=(difference_type i)		//[t82]
      { iterator_type::operator+=(i); return *this; }
  //@}

  /**
   * @name Arithmetic operators
   */
  //@{
  const_reverse_row_iterator operator+(difference_type i) const		//[t82]
      { return const_reverse_row_iterator{base()-i}; }
  const_reverse_row_iterator operator-(difference_type i)		//[t82]
      { return const_reverse_row_iterator{base()+i}; }
  difference_type
    operator-(const const_reverse_row_iterator &rhs) const		//[t82]
      { return rhs.const_row_iterator::operator-(*this); }
  //@}

  /**
   * @name Comparisons
   */
  //@{
  bool operator==(const const_reverse_row_iterator &rhs) const noexcept	//[t82]
      { return iterator_type::operator==(rhs); }
  bool operator!=(const const_reverse_row_iterator &rhs) const noexcept	//[t82]
      { return !operator==(rhs); }

  bool operator<(const const_reverse_row_iterator &rhs) const		//[t82]
      { return iterator_type::operator>(rhs); }
  bool operator<=(const const_reverse_row_iterator &rhs) const		//[t82]
      { return iterator_type::operator>=(rhs); }
  bool operator>(const const_reverse_row_iterator &rhs) const		//[t82]
      { return iterator_type::operator<(rhs); }
  bool operator>=(const const_reverse_row_iterator &rhs) const		//[t82]
      { return iterator_type::operator<=(rhs); }
  //@}
};


inline const_row_iterator
const_row_iterator::operator+(difference_type o) const
{
  return const_row_iterator{
	row(home(), idx()),
	size_type(difference_type(col()) + o)};
}

inline const_row_iterator
operator+(const_row_iterator::difference_type o, const_row_iterator i)
	{ return i + o; }

inline const_row_iterator
const_row_iterator::operator-(difference_type o) const
{
  return const_row_iterator{
	row(home(), idx()),
	size_type(difference_type(col()) - o)};
}

inline const_row_iterator::difference_type
const_row_iterator::operator-(const_row_iterator i) const
	{ return difference_type(num() - i.num()); }


} // namespace pqxx


/*
[1] Scott Meyers, in one of his essential books, "Effective C++" and "More
Effective C++", points out that it is good style to have any class containing
a member of pointer type define a destructor--just to show that it knows what
it is doing with the pointer.  This helps prevent nasty memory leak / double
deletion bugs typically resulting from programmers' omission to deal with such
issues in their destructors.

The @c -Weffc++ option in gcc generates warnings for noncompliance with Scott's
style guidelines, and hence necessitates the definition of this destructor,
trivial as it may be.
*/


#include "pqxx/compiler-internal-post.hxx"

#endif
