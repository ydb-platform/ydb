#pragma once

#include <inttypes.h>

typedef signed char               schar;

typedef int8_t                    int8;
typedef int16_t                   int16;
typedef int32_t                   int32;
typedef int64_t                   int64;

typedef uint8_t                   uint8;
typedef uint16_t                  uint16;
typedef uint32_t                  uint32;
typedef uint64_t                  uint64;

const uint16 kuint16max = (   (uint16) 0xFFFF);
const uint32 kuint32max = (   (uint32) 0xFFFFFFFF);
const uint64 kuint64max = ( (((uint64) kuint32max) << 32) | kuint32max );

const  int8  kint8max   = (   (  int8) 0x7F);
const  int16 kint16max  = (   ( int16) 0x7FFF);
const  int32 kint32max  = (   ( int32) 0x7FFFFFFF);
const  int64 kint64max  = ( ((( int64) kint32max) << 32) | kuint32max );

const  int8  kint8min   = (   (  int8) 0x80);
const  int16 kint16min  = (   ( int16) 0x8000);
const  int32 kint32min  = (   ( int32) 0x80000000);
const  int64 kint64min  = ( ((( int64) kint32min) << 32) | 0 );

#define DISALLOW_EVIL_CONSTRUCTORS(TypeName)    \
  TypeName(const TypeName&);                    \
  void operator=(const TypeName&)
