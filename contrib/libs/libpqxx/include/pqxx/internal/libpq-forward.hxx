/** Minimal forward declarations of libpq types needed in libpqxx headers.
 *
 * DO NOT INCLUDE THIS FILE when building client programs.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
extern "C"
{
struct pg_conn;
struct pg_result;
struct pgNotify;
}

namespace pqxx
{
namespace internal
{
/// Forward declarations of libpq types as needed in libpqxx headers
namespace pq
{
using PGconn = pg_conn;
using PGresult = pg_result;
using PGnotify = pgNotify;
using PQnoticeProcessor = void (*)(void *, const char *);
}
}

/// PostgreSQL database row identifier
using oid = unsigned int;
} // extern "C"
