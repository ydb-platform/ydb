#pragma once 
 
#if !defined(INCLUDE_YDB_INTERNAL_H) 
#error "you are trying to use internal ydb header" 
#endif // INCLUDE_YDB_INTERNAL_H 
 
// This macro allow to send credentials data via insecure channel 
#define YDB_GRPC_UNSECURE_AUTH 
 
// This macro is used for grpc debug purpose only 
//#define YDB_GRPC_BYPASS_CHANNEL_POOL 
