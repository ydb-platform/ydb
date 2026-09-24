#pragma once

// The declarations live in the interface module so that consumers which only
// call these helpers (SchemeShard) do not have to depend on the SQL grammar.
// See view_query_iface/view_query.h for how the implementation is selected.
#include <ydb/public/lib/ydb_cli/dump/util/view_query_iface/view_query.h>
