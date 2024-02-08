/** Definition of the pqxx::stream_to class.
 *
 * pqxx::stream_to enables optimized batch updates to a database table.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/stream_to.hxx instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_STREAM_TO
#define PQXX_H_STREAM_TO

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"
#include "pqxx/transaction_base.hxx"
#include "pqxx/stream_base.hxx"
#include "pqxx/stream_from.hxx"
#include "pqxx/internal/type_utils.hxx"

#include <string>


namespace pqxx
{

/// Efficiently write data directly to a database table.
/** If you wish to insert rows of data into a table, you can compose INSERT
 * statements and execute them.  But it's slow and tedious, and you need to
 * worry about quoting and escaping the data.
 *
 * If you're just inserting a single row, it probably won't matter much.  You
 * can use prepared or parameterised statements to take care of the escaping
 * for you.  But if you're inserting large numbers of rows you will want
 * something better.
 *
 * Inserting rows one by one tends to take a lot of time, especially when you
 * are working with a remote database server over the network.  Every single
 * row involves sending the data over the network, and waiting for a reply.
 * Do it "in bulk" using @c stream_to, and you may find that it goes many times
 * faster, sometimes even by orders of magnitude.
 *
 * Here's how it works: you create a @c stream_to stream to start writing to
 * your table.  You will probably want to specify the columns.  Then, you
 * feed your data into the stream one row at a time.  And finally, you call the
 * stream's @c complete() to tell it to finalise the operation, wait for
 * completion, and check for errors.
 *
 * You insert data using the @c << ("shift-left") operator.  Each row must be
 * something that can be iterated in order to get its constituent fields: a
 * @c std::tuple, a @c std::vector, or anything else with a @c begin and
 * @c end.  It could be a class of your own.  Of course the fields have to
 * match the columns you specified when creating the stream.
 *
 * There is also a matching stream_from for reading data in bulk.
 */
class PQXX_LIBEXPORT stream_to : public stream_base
{
public:
  /// Create a stream, without specifying columns.
  /** Fields will be inserted in whatever order the columns have in the
   * database.
   *
   * You'll probably want to specify the columns, so that the mapping between
   * your data fields and the table is explicit in your code, and not hidden
   * in an "implicit contract" between your code and your schema.
   */
  stream_to(transaction_base &, const std::string &table_name);

  /// Create a stream, specifying column names as a container of strings.
  template<typename Columns> stream_to(
    transaction_base &,
    const std::string &table_name,
    const Columns& columns
  );

  /// Create a stream, specifying column names as a sequence of strings.
  template<typename Iter> stream_to(
    transaction_base &,
    const std::string &table_name,
    Iter columns_begin,
    Iter columns_end
  );

  ~stream_to() noexcept;

  /// Complete the operation, and check for errors.
  /** Always call this to close the stream in an orderly fashion, even after
   * an error.  (In the case of an error, abort the transaction afterwards.)
   *
   * The only circumstance where it's safe to skip this is after an error, if
   * you're discarding the entire connection right away.
   */
  void complete() override;

  /// Insert a row of data.
  /** The data can be any type that can be iterated.  Each iterated item
   * becomes a field in the row, in the same order as the columns you
   * specified when creating the stream.
   *
   * Each field will be converted into the database's format using
   * @c pqxx::to_string.
   */
  template<typename Tuple> stream_to & operator<<(const Tuple &);

  /// Stream a `stream_from` straight into a `stream_to`.
  /** This can be useful when copying between different databases.  If the
   * source and the destination are on the same database, you'll get better
   * performance doing it all in a regular query.
   */
  stream_to &operator<<(stream_from &);

private:
  /// Write a row of data, as a line of text.
  void write_raw_line(const std::string &);

  void set_up(transaction_base &, const std::string &table_name);
  void set_up(
    transaction_base &,
    const std::string &table_name,
    const std::string &columns
  );

  void close() override;
};


template<typename Columns> inline stream_to::stream_to(
  transaction_base &tb,
  const std::string &table_name,
  const Columns& columns
) : stream_to{
  tb,
  table_name,
  std::begin(columns),
  std::end(columns)
}
{}


template<typename Iter> inline stream_to::stream_to(
  transaction_base &tb,
  const std::string &table_name,
  Iter columns_begin,
  Iter columns_end
) :
  namedclass{"stream_from", table_name},
  stream_base{tb}
{
  set_up(
    tb,
    table_name,
    columnlist(columns_begin, columns_end)
  );
}


namespace internal
{

class PQXX_LIBEXPORT TypedCopyEscaper
{
  static std::string escape(const std::string &);
public:
  template<typename T> std::string operator()(const T* t) const
  {
    return string_traits<T>::is_null(*t) ? "\\N" : escape(to_string(*t));
  }
};

// Explicit specialization so we don't need a string_traits<> for nullptr_t
template<> inline std::string TypedCopyEscaper::operator()<std::nullptr_t>(
  const std::nullptr_t*
) const
{ return "\\N"; }

} // namespace pqxx::internal


template<typename Tuple> stream_to & stream_to::operator<<(const Tuple &t)
{
  write_raw_line(separated_list("\t", t, internal::TypedCopyEscaper()));
  return *this;
}

} // namespace pqxx


#include "pqxx/compiler-internal-post.hxx"
#endif
