#pragma once

#include <util/system/compiler.h>

#if !defined(INCLUDE_YDB_INTERNAL_H)
#error "you are trying to use internal ydb header"
#endif // INCLUDE_YDB_INTERNAL_H

// This macro allow to send credentials data via insecure channel
#define YDB_GRPC_UNSECURE_AUTH

// This macro is used for grpc debug purpose only
//#define YDB_GRPC_BYPASS_CHANNEL_POOL

// gRpc issue temporal workaround.
// In case of early TryCancel call and sharing shannel interfaces
// grpc doesn`t free allocated memory at the client shutdown
#if defined(_asan_enabled_)
#define YDB_GRPC_BYPASS_CHANNEL_POOL
#endif
