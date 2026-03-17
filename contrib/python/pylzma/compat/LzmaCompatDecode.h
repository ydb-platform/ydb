/* 
LzmaDecode.h
LZMA Decoder interface
LZMA SDK 4.01 Copyright (c) 1999-2004 Igor Pavlov (2004-02-15)

Converted to a state machine by Amir Szekely
*/

#ifndef __LZMADECODE_H
#define __LZMADECODE_H

#include "7zTypes.h"

/***********************
 *    Configuration    *
 ***********************/

/* #define _LZMA_PROB32 */
/* It can increase speed on some 32-bit CPUs, 
   but memory usage will be doubled in that case */

/***********************
 *  Configuration End  *
 ***********************/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef lzmaalloc
#define lzmaalloc malloc
#endif

#ifndef lzmafree
#define lzmafree free
#endif

#ifndef LZMACALL
#  define LZMACALL
#endif

#ifndef malloc
# ifdef __APPLE__
#include <malloc/malloc.h>
# else
#include <malloc.h>
# endif
#endif

#ifndef UInt32
#ifdef _LZMA_UINT32_IS_ULONG
#define UInt32 unsigned long
#else
#define UInt32 unsigned int
#endif
#endif

#ifndef SizeT
#ifdef _LZMA_SYSTEM_SIZE_T
#include <stddef.h>
#define SizeT size_t
#else
#define SizeT UInt32
#endif
#endif

#ifndef CProb
#ifdef _LZMA_PROB32
#define CProb UInt32
#else
#define CProb unsigned short
#endif
#endif

#define LZMA_STREAM_END 1
#define LZMA_OK 0
#define LZMA_DATA_ERROR -1
#define LZMA_NOT_ENOUGH_MEM -2

typedef struct
{
  /* mode control */
  int mode;
  int last;
  int last2;
  int last3;

  /* properties */
  UInt32 dynamicDataSize;
  UInt32 dictionarySize;

  /* io */
  Byte *next_in;    /* next input byte */
  UInt32 avail_in;  /* number of bytes available at next_in */

  Byte *next_out;   /* next output byte should be put there */
  UInt32 avail_out; /* remaining free space at next_out */

  UInt32 totalOut;  /* total output */

  /* saved state */
  Byte previousByte;
  Byte matchByte;
  CProb *probs;
  CProb *prob;
  int mi;
  int posState;
  int temp1;
  int temp2;
  int temp3;
  int lc;
  int state;
  int isPreviousMatch;
  int len;
  UInt32 rep0;
  UInt32 rep1;
  UInt32 rep2;
  UInt32 rep3;
  UInt32 posStateMask;
  UInt32 literalPosMask;
  UInt32 dictionaryPos;

  /* range coder */
  UInt32 range;
  UInt32 code;

  /* allocated buffers */
  Byte *dictionary;
  Byte *dynamicData;
} lzma_stream;

void LZMACALL lzmaCompatInit(lzma_stream *);
int LZMACALL lzmaCompatDecode(lzma_stream *);

#ifdef __cplusplus
}
#endif

#endif
