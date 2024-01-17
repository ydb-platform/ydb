/**
 * Basic type aliases and forward declarations.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this
 * mistake, or contact the author.
 */
#ifndef PQXX_H_TYPES
#define PQXX_H_TYPES

#include <cstddef>

namespace pqxx
{
/// Number of rows in a result set.
using result_size_type = unsigned long;

/// Difference between result sizes.
using result_difference_type = signed long;

/// Number of fields in a row of database data.
using row_size_type = unsigned int;

/// Difference between row sizes.
using row_difference_type = signed int;

/// Number of bytes in a field of database data.
using field_size_type = std::size_t;

/// Number of bytes in a large object.  (Unusual: it's signed.)
using large_object_size_type = long;


// Forward declarations, to help break compilation dependencies.
// These won't necessarily include all classes in libpqxx.
class binarystring;
class connectionpolicy;
class connection_base;
class const_result_iterator;
class const_reverse_result_iterator;
class const_reverse_row_iterator;
class const_row_iterator;
class dbtransaction;
class field;
class largeobjectaccess;
class notification_receiver;
class range_error;
class result;
class row;
class tablereader;
class transaction_base;

} // namespace pqxx

#endif
