/** Implementation of libpqxx STL-style cursor classes.
 *
 * These classes wrap SQL cursors in STL-like interfaces.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <iterator>

#include "pqxx/cursor"

#include "pqxx/internal/encodings.hxx"
#include "pqxx/internal/gates/connection-sql_cursor.hxx"
#include "pqxx/internal/gates/transaction-sql_cursor.hxx"

using namespace pqxx;
using namespace pqxx::internal;


namespace
{
/// Is this character a "useless trailing character" in a query?
/** A character is "useless" at the end of a query if it is either whitespace or
 * a semicolon.
 */
inline bool useless_trail(char c)
{
  return isspace(c) or c==';';
}


/// Find end of nonempty query, stripping off any trailing semicolon.
/** When executing a normal query, a trailing semicolon is meaningless but
 * won't hurt.  That's why we can't rule out that some code may include one.
 *
 * But for cursor queries, a trailing semicolon is a problem.  The query gets
 * embedded in a larger statement, which a semicolon would break into two.
 * We'll have to remove it if present.
 *
 * A trailing semicolon may not actually be at the end.  It could be masked by
 * subsequent whitespace.  If there's also a comment though, that's the
 * caller's own lookout.  We can't guard against every possible mistake, and
 * text processing is actually remarkably sensitive to mistakes in a
 * multi-encoding world.
 *
 * If there is a trailing semicolon, this function returns its offset.  If
 * there are more than one, it returns the offset of the first one.  If there
 * is no trailing semicolon, it returns the length of the query string.
 *
 * The query must be nonempty.
 */
std::string::size_type find_query_end(
	const std::string &query,
	encoding_group enc)
{
  const auto text = query.c_str();
  const auto size = query.size();
  std::string::size_type end;
  if (enc == encoding_group::MONOBYTE)
  {
    // This is an encoding where we can scan backwards from the end.
    for (end = query.size(); end > 0 and useless_trail(text[end-1]); --end);
  }
  else
  {
    // Complex encoding.  We only know how to iterate forwards, so start from
    // the beginning.
    end = 0;

    pqxx::internal::for_glyphs(
        enc,
        [text, &end](const char *gbegin, const char *gend)
        {
          if (gend - gbegin > 1 or not useless_trail(*gbegin))
            end = std::string::size_type(gend - text);
        },
        text, size);
  }

  return end;
}
} // namespace


pqxx::internal::sql_cursor::sql_cursor(
	transaction_base &t,
	const std::string &query,
	const std::string &cname,
	cursor_base::accesspolicy ap,
	cursor_base::updatepolicy up,
	cursor_base::ownershippolicy op,
	bool hold,
	result_format format) :
  cursor_base{t.conn(), cname},
  m_home{t.conn()},
  m_adopted{false},
  m_at_end{-1},
  m_pos{0}
{
  if (&t.conn() != &m_home) throw internal_error{"Cursor in wrong connection"};

#include "pqxx/internal/ignore-deprecated-pre.hxx"
  m_home.activate();
#include "pqxx/internal/ignore-deprecated-post.hxx"

  if (query.empty()) throw usage_error{"Cursor has empty query."};
  const auto enc = enc_group(t.conn().encoding_id());
  const auto qend = find_query_end(query, enc);
  if (qend == 0) throw usage_error{"Cursor has effectively empty query."};

  std::stringstream cq, qn;

  cq << "DECLARE " << t.quote_name(name()) << " ";

  if (format == result_format::binary) {
    cq << "BINARY ";
  }

  if (ap == cursor_base::forward_only) cq << "NO ";
  cq << "SCROLL ";

  cq << "CURSOR ";

  if (hold) cq << "WITH HOLD ";

  cq << "FOR ";
  cq.write(query.c_str(), std::streamsize(qend));
  cq << ' ';

  if (up != cursor_base::update) cq << "FOR READ ONLY ";
  else cq << "FOR UPDATE ";

  qn << "[DECLARE " << name() << ']';
  t.exec(cq, qn.str());

  // Now that we're here in the starting position, keep a copy of an empty
  // result.  That may come in handy later, because we may not be able to
  // construct an empty result with all the right metadata due to the weird
  // meaning of "FETCH 0."
  init_empty_result(t);

  // If we're creating a WITH HOLD cursor, noone is going to destroy it until
  // after this transaction.  That means the connection cannot be deactivated
  // without losing the cursor.
  if (hold)
    gate::connection_sql_cursor{t.conn()}.add_reactivation_avoidance_count(1);

  m_ownership = op;
}


pqxx::internal::sql_cursor::sql_cursor(
	transaction_base &t,
	const std::string &cname,
	cursor_base::ownershippolicy op) :
  cursor_base{t.conn(), cname, false},
  m_home{t.conn()},
  m_empty_result{},
  m_adopted{true},
  m_at_end{0},
  m_pos{-1}
{
  // If we take responsibility for destroying the cursor, that's one less
  // reason not to allow the connection to be deactivated and reactivated.
  // TODO: Go over lifetime/reactivation rules again to be sure they work.
  if (op==cursor_base::owned)
    gate::connection_sql_cursor{t.conn()}.add_reactivation_avoidance_count(-1);
  m_adopted = true;
  m_ownership = op;
}


void pqxx::internal::sql_cursor::close() noexcept
{
  if (m_ownership==cursor_base::owned)
  {
    try
    {
      gate::connection_sql_cursor{m_home}.exec(
	("CLOSE " + m_home.quote_name(name())).c_str(),
	0);
    }
    catch (const std::exception &)
    {
    }

    if (m_adopted)
      gate::connection_sql_cursor{m_home}.add_reactivation_avoidance_count(-1);

    m_ownership = cursor_base::loose;
  }
}


void pqxx::internal::sql_cursor::init_empty_result(transaction_base &t)
{
  if (pos() != 0) throw internal_error{"init_empty_result() from bad pos()."};
  m_empty_result = t.exec("FETCH 0 IN " + m_home.quote_name(name()));
}


/// Compute actual displacement based on requested and reported displacements.
internal::sql_cursor::difference_type
pqxx::internal::sql_cursor::adjust(difference_type hoped,
	difference_type actual)
{
  if (actual < 0) throw internal_error{"Negative rows in cursor movement."};
  if (hoped == 0) return 0;
  const int direction = ((hoped < 0) ? -1 : 1);
  bool hit_end = false;
  if (actual != labs(hoped))
  {
    if (actual > labs(hoped))
      throw internal_error{"Cursor displacement larger than requested."};

    // If we see fewer rows than requested, then we've hit an end (on either
    // side) of the result set.  Wether we make an extra step to a one-past-end
    // position or whether we're already there depends on where we were
    // previously: if our last move was in the same direction and also fell
    // short, we're already at a one-past-end row.
    if (m_at_end != direction) ++actual;

    // If we hit the beginning, make sure our position calculation ends up
    // at zero (even if we didn't previously know where we were!), and if we
    // hit the other end, register the fact that we now know where the end
    // of the result set is.
    if (direction > 0) hit_end = true;
    else if (m_pos == -1) m_pos = actual;
    else if (m_pos != actual)
      throw internal_error{
	"Moved back to beginning, but wrong position: "
        "hoped=" + to_string(hoped) + ", "
        "actual=" + to_string(actual) + ", "
        "m_pos=" + to_string(m_pos) + ", "
        "direction=" + to_string(direction) + "."};

    m_at_end = direction;
  }
  else
  {
    m_at_end = 0;
  }

  if (m_pos >= 0) m_pos += direction*actual;
  if (hit_end)
  {
    if (m_endpos >= 0 and m_pos != m_endpos)
      throw internal_error{"Inconsistent cursor end positions."};
    m_endpos = m_pos;
  }
  return direction*actual;
}


result pqxx::internal::sql_cursor::fetch(
	difference_type rows,
	difference_type &displacement)
{
  if (rows == 0)
  {
    displacement = 0;
    return m_empty_result;
  }
  const std::string query =
      "FETCH " + stridestring(rows) + " IN " + m_home.quote_name(name());
  const result r{gate::connection_sql_cursor{m_home}.exec(query.c_str(), 0)};
  displacement = adjust(rows, difference_type(r.size()));
  return r;
}


cursor_base::difference_type pqxx::internal::sql_cursor::move(
	difference_type rows,
	difference_type &displacement)
{
  if (rows == 0)
  {
    displacement = 0;
    return 0;
  }

  const std::string query =
      "MOVE " + stridestring(rows) + " IN " + m_home.quote_name(name());
  const result r(gate::connection_sql_cursor{m_home}.exec(query.c_str(), 0));
  difference_type d = difference_type(r.affected_rows());
  displacement = adjust(rows, d);
  return d;
}


std::string pqxx::internal::sql_cursor::stridestring(difference_type n)
{
  /* Some special-casing for ALL and BACKWARD ALL here.  We used to use numeric
   * "infinities" for difference_type for this (the highest and lowest possible
   * values for "long"), but for PostgreSQL 8.0 at least, the backend appears to
   * expect a 32-bit number and fails to parse large 64-bit numbers.
   * We could change the alias to match this behaviour, but that would break
   * if/when Postgres is changed to accept 64-bit displacements.
   */
  static const std::string All{"ALL"}, BackAll{"BACKWARD ALL"};
  if (n >= cursor_base::all()) return All;
  else if (n <= cursor_base::backward_all()) return BackAll;
  return to_string(n);
}
