/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef D4UTIL_H
#define D4UTIL_H 1

/* This is intended to be big enough to work as
   an offset/position/size for a file or a memory block.
*/
typedef unsigned long long d4size_t;

/* Define a (size, memory) pair */
typedef struct D4blob {d4size_t size; void* memory;} D4blob;

/* Empty blob constant */
#define NULLBLOB(blob) {blob.size = 0; blob.memory = NULL;}

#define OFFSET2BLOB(blob,offset) do{(blob).size = (d4size_t)((offset)->limit - (offset)->base); (blob).memory = (offset)->base; }while(0)
#define BLOB2OFFSET(offset,blob) do{\
(offset)->base = (blob).memory; \
(offset)->limit = ((char*)(blob).memory) + (blob).size; \
(offset)->offset = (offset)->base; \
} while(0)

/**************************************************/

/* signature: void swapinline16(void* ip) */
#define swapinline16(ip) \
{ \
    char b[2]; \
    char* src = (char*)(ip); \
    b[0] = src[1]; \
    b[1] = src[0]; \
    memcpy(ip, b, 2); \
}

/* signature: void swapinline32(void* ip) */
#define swapinline32(ip) \
{ \
    char b[4]; \
    char* src = (char*)(ip); \
    b[0] = src[3]; \
    b[1] = src[2]; \
    b[2] = src[1]; \
    b[3] = src[0]; \
    memcpy(ip, b, 4); \
}

/* signature: void swapinline64(void* ip) */
#define swapinline64(ip) \
{ \
    char b[8]; \
    char* src = (char*)(ip); \
    b[0] = src[7]; \
    b[1] = src[6]; \
    b[2] = src[5]; \
    b[3] = src[4]; \
    b[4] = src[3]; \
    b[5] = src[2]; \
    b[6] = src[1]; \
    b[7] = src[0]; \
    memcpy(ip, b, 8); \
}

/***************************************************/
/* Define the NCD4node.data.flags */

#define HASNIL   (0) /* no flags set */
#define HASSEQ   (1) /* transitively contains sequence(s)*/
#define HASSTR   (2) /* transitively contains strings */
#define HASOPFIX (4) /* transitively contains fixed size opaques */
#define HASOPVAR (8) /* transitively contains variable size opaques */
#define LEAFSEQ (16) /* mark leaf sequences */
#define HASANY   (HASNIL|HASSEQ|HASSTR|HASOPTFIX|HASOPVAR)
/***************************************************/

extern int ncd4__testurl(const char* path, char** basename);

#endif /*D4UTIL_H*/
