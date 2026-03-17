#if defined(_MSC_VER)

#if _MSC_VER <= 1600
#define _Out_writes_( x )
#endif

#if _MSC_VER <= 1500
#pragma include_alias( <stdint.h>, <stddef.h> )
typedef signed   __int8  int8_t;
typedef unsigned __int8  uint8_t;
typedef signed   __int16 int16_t;
typedef unsigned __int16 uint16_t;
typedef signed   __int32 int32_t;
typedef unsigned __int32 uint32_t;
typedef signed   __int64 int64_t;
typedef unsigned __int64 uint64_t;
#endif

#endif
