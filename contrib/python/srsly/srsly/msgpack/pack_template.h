/*
 * MessagePack packing routine template
 *
 * Copyright (C) 2008-2010 FURUHASHI Sadayuki
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#if defined(__LITTLE_ENDIAN__)
#define TAKE8_8(d)  ((uint8_t*)&d)[0]
#define TAKE8_16(d) ((uint8_t*)&d)[0]
#define TAKE8_32(d) ((uint8_t*)&d)[0]
#define TAKE8_64(d) ((uint8_t*)&d)[0]
#elif defined(__BIG_ENDIAN__)
#define TAKE8_8(d)  ((uint8_t*)&d)[0]
#define TAKE8_16(d) ((uint8_t*)&d)[1]
#define TAKE8_32(d) ((uint8_t*)&d)[3]
#define TAKE8_64(d) ((uint8_t*)&d)[7]
#endif

#ifndef msgpack_pack_append_buffer
#error msgpack_pack_append_buffer callback is not defined
#endif


/*
 * Integer
 */

#define msgpack_pack_real_uint16(x, d) \
do { \
    if(d < (1<<7)) { \
        /* fixnum */ \
        msgpack_pack_append_buffer(x, &TAKE8_16(d), 1); \
    } else if(d < (1<<8)) { \
        /* unsigned 8 */ \
        unsigned char buf[2] = {0xcc, TAKE8_16(d)}; \
        msgpack_pack_append_buffer(x, buf, 2); \
    } else { \
        /* unsigned 16 */ \
        unsigned char buf[3]; \
        buf[0] = 0xcd; _msgpack_store16(&buf[1], (uint16_t)d); \
        msgpack_pack_append_buffer(x, buf, 3); \
    } \
} while(0)

#define msgpack_pack_real_uint32(x, d) \
do { \
    if(d < (1<<8)) { \
        if(d < (1<<7)) { \
            /* fixnum */ \
            msgpack_pack_append_buffer(x, &TAKE8_32(d), 1); \
        } else { \
            /* unsigned 8 */ \
            unsigned char buf[2] = {0xcc, TAKE8_32(d)}; \
            msgpack_pack_append_buffer(x, buf, 2); \
        } \
    } else { \
        if(d < (1<<16)) { \
            /* unsigned 16 */ \
            unsigned char buf[3]; \
            buf[0] = 0xcd; _msgpack_store16(&buf[1], (uint16_t)d); \
            msgpack_pack_append_buffer(x, buf, 3); \
        } else { \
            /* unsigned 32 */ \
            unsigned char buf[5]; \
            buf[0] = 0xce; _msgpack_store32(&buf[1], (uint32_t)d); \
            msgpack_pack_append_buffer(x, buf, 5); \
        } \
    } \
} while(0)

#define msgpack_pack_real_uint64(x, d) \
do { \
    if(d < (1ULL<<8)) { \
        if(d < (1ULL<<7)) { \
            /* fixnum */ \
            msgpack_pack_append_buffer(x, &TAKE8_64(d), 1); \
        } else { \
            /* unsigned 8 */ \
            unsigned char buf[2] = {0xcc, TAKE8_64(d)}; \
            msgpack_pack_append_buffer(x, buf, 2); \
        } \
    } else { \
        if(d < (1ULL<<16)) { \
            /* unsigned 16 */ \
            unsigned char buf[3]; \
            buf[0] = 0xcd; _msgpack_store16(&buf[1], (uint16_t)d); \
            msgpack_pack_append_buffer(x, buf, 3); \
        } else if(d < (1ULL<<32)) { \
            /* unsigned 32 */ \
            unsigned char buf[5]; \
            buf[0] = 0xce; _msgpack_store32(&buf[1], (uint32_t)d); \
            msgpack_pack_append_buffer(x, buf, 5); \
        } else { \
            /* unsigned 64 */ \
            unsigned char buf[9]; \
            buf[0] = 0xcf; _msgpack_store64(&buf[1], d); \
            msgpack_pack_append_buffer(x, buf, 9); \
        } \
    } \
} while(0)

#define msgpack_pack_real_int16(x, d) \
do { \
    if(d < -(1<<5)) { \
        if(d < -(1<<7)) { \
            /* signed 16 */ \
            unsigned char buf[3]; \
            buf[0] = 0xd1; _msgpack_store16(&buf[1], (int16_t)d); \
            msgpack_pack_append_buffer(x, buf, 3); \
        } else { \
            /* signed 8 */ \
            unsigned char buf[2] = {0xd0, TAKE8_16(d)}; \
            msgpack_pack_append_buffer(x, buf, 2); \
        } \
    } else if(d < (1<<7)) { \
        /* fixnum */ \
        msgpack_pack_append_buffer(x, &TAKE8_16(d), 1); \
    } else { \
        if(d < (1<<8)) { \
            /* unsigned 8 */ \
            unsigned char buf[2] = {0xcc, TAKE8_16(d)}; \
            msgpack_pack_append_buffer(x, buf, 2); \
        } else { \
            /* unsigned 16 */ \
            unsigned char buf[3]; \
            buf[0] = 0xcd; _msgpack_store16(&buf[1], (uint16_t)d); \
            msgpack_pack_append_buffer(x, buf, 3); \
        } \
    } \
} while(0)

#define msgpack_pack_real_int32(x, d) \
do { \
    if(d < -(1<<5)) { \
        if(d < -(1<<15)) { \
            /* signed 32 */ \
            unsigned char buf[5]; \
            buf[0] = 0xd2; _msgpack_store32(&buf[1], (int32_t)d); \
            msgpack_pack_append_buffer(x, buf, 5); \
        } else if(d < -(1<<7)) { \
            /* signed 16 */ \
            unsigned char buf[3]; \
            buf[0] = 0xd1; _msgpack_store16(&buf[1], (int16_t)d); \
            msgpack_pack_append_buffer(x, buf, 3); \
        } else { \
            /* signed 8 */ \
            unsigned char buf[2] = {0xd0, TAKE8_32(d)}; \
            msgpack_pack_append_buffer(x, buf, 2); \
        } \
    } else if(d < (1<<7)) { \
        /* fixnum */ \
        msgpack_pack_append_buffer(x, &TAKE8_32(d), 1); \
    } else { \
        if(d < (1<<8)) { \
            /* unsigned 8 */ \
            unsigned char buf[2] = {0xcc, TAKE8_32(d)}; \
            msgpack_pack_append_buffer(x, buf, 2); \
        } else if(d < (1<<16)) { \
            /* unsigned 16 */ \
            unsigned char buf[3]; \
            buf[0] = 0xcd; _msgpack_store16(&buf[1], (uint16_t)d); \
            msgpack_pack_append_buffer(x, buf, 3); \
        } else { \
            /* unsigned 32 */ \
            unsigned char buf[5]; \
            buf[0] = 0xce; _msgpack_store32(&buf[1], (uint32_t)d); \
            msgpack_pack_append_buffer(x, buf, 5); \
        } \
    } \
} while(0)

#define msgpack_pack_real_int64(x, d) \
do { \
    if(d < -(1LL<<5)) { \
        if(d < -(1LL<<15)) { \
            if(d < -(1LL<<31)) { \
                /* signed 64 */ \
                unsigned char buf[9]; \
                buf[0] = 0xd3; _msgpack_store64(&buf[1], d); \
                msgpack_pack_append_buffer(x, buf, 9); \
            } else { \
                /* signed 32 */ \
                unsigned char buf[5]; \
                buf[0] = 0xd2; _msgpack_store32(&buf[1], (int32_t)d); \
                msgpack_pack_append_buffer(x, buf, 5); \
            } \
        } else { \
            if(d < -(1<<7)) { \
                /* signed 16 */ \
                unsigned char buf[3]; \
                buf[0] = 0xd1; _msgpack_store16(&buf[1], (int16_t)d); \
                msgpack_pack_append_buffer(x, buf, 3); \
            } else { \
                /* signed 8 */ \
                unsigned char buf[2] = {0xd0, TAKE8_64(d)}; \
                msgpack_pack_append_buffer(x, buf, 2); \
            } \
        } \
    } else if(d < (1<<7)) { \
        /* fixnum */ \
        msgpack_pack_append_buffer(x, &TAKE8_64(d), 1); \
    } else { \
        if(d < (1LL<<16)) { \
            if(d < (1<<8)) { \
                /* unsigned 8 */ \
                unsigned char buf[2] = {0xcc, TAKE8_64(d)}; \
                msgpack_pack_append_buffer(x, buf, 2); \
            } else { \
                /* unsigned 16 */ \
                unsigned char buf[3]; \
                buf[0] = 0xcd; _msgpack_store16(&buf[1], (uint16_t)d); \
                msgpack_pack_append_buffer(x, buf, 3); \
            } \
        } else { \
            if(d < (1LL<<32)) { \
                /* unsigned 32 */ \
                unsigned char buf[5]; \
                buf[0] = 0xce; _msgpack_store32(&buf[1], (uint32_t)d); \
                msgpack_pack_append_buffer(x, buf, 5); \
            } else { \
                /* unsigned 64 */ \
                unsigned char buf[9]; \
                buf[0] = 0xcf; _msgpack_store64(&buf[1], d); \
                msgpack_pack_append_buffer(x, buf, 9); \
            } \
        } \
    } \
} while(0)


static inline int msgpack_pack_short(msgpack_packer* x, short d)
{
#if defined(SIZEOF_SHORT)
#if SIZEOF_SHORT == 2
    msgpack_pack_real_int16(x, d);
#elif SIZEOF_SHORT == 4
    msgpack_pack_real_int32(x, d);
#else
    msgpack_pack_real_int64(x, d);
#endif

#elif defined(SHRT_MAX)
#if SHRT_MAX == 0x7fff
    msgpack_pack_real_int16(x, d);
#elif SHRT_MAX == 0x7fffffff
    msgpack_pack_real_int32(x, d);
#else
    msgpack_pack_real_int64(x, d);
#endif

#else
if(sizeof(short) == 2) {
    msgpack_pack_real_int16(x, d);
} else if(sizeof(short) == 4) {
    msgpack_pack_real_int32(x, d);
} else {
    msgpack_pack_real_int64(x, d);
}
#endif
}

static inline int msgpack_pack_int(msgpack_packer* x, int d)
{
#if defined(SIZEOF_INT)
#if SIZEOF_INT == 2
    msgpack_pack_real_int16(x, d);
#elif SIZEOF_INT == 4
    msgpack_pack_real_int32(x, d);
#else
    msgpack_pack_real_int64(x, d);
#endif

#elif defined(INT_MAX)
#if INT_MAX == 0x7fff
    msgpack_pack_real_int16(x, d);
#elif INT_MAX == 0x7fffffff
    msgpack_pack_real_int32(x, d);
#else
    msgpack_pack_real_int64(x, d);
#endif

#else
if(sizeof(int) == 2) {
    msgpack_pack_real_int16(x, d);
} else if(sizeof(int) == 4) {
    msgpack_pack_real_int32(x, d);
} else {
    msgpack_pack_real_int64(x, d);
}
#endif
}

static inline int msgpack_pack_long(msgpack_packer* x, long d)
{
#if defined(SIZEOF_LONG)
#if SIZEOF_LONG == 4
    msgpack_pack_real_int32(x, d);
#else
    msgpack_pack_real_int64(x, d);
#endif

#elif defined(LONG_MAX)
#if LONG_MAX == 0x7fffffffL
    msgpack_pack_real_int32(x, d);
#else
    msgpack_pack_real_int64(x, d);
#endif

#else
    if (sizeof(long) == 4) {
        msgpack_pack_real_int32(x, d);
    } else {
        msgpack_pack_real_int64(x, d);
    }
#endif
}

static inline int msgpack_pack_long_long(msgpack_packer* x, long long d)
{
    msgpack_pack_real_int64(x, d);
}

static inline int msgpack_pack_unsigned_long_long(msgpack_packer* x, unsigned long long d)
{
    msgpack_pack_real_uint64(x, d);
}


/*
 * Float
 */

static inline int msgpack_pack_float(msgpack_packer* x, float d)
{
    unsigned char buf[5];
    buf[0] = 0xca;

#if PY_VERSION_HEX >= 0x030B00A7
    PyFloat_Pack4(d, (char *)&buf[1], 0);
#else
    _PyFloat_Pack4(d, &buf[1], 0);
#endif
    msgpack_pack_append_buffer(x, buf, 5);
}

static inline int msgpack_pack_double(msgpack_packer* x, double d)
{
    unsigned char buf[9];
    buf[0] = 0xcb;
#if PY_VERSION_HEX >= 0x030B00A7
    PyFloat_Pack8(d, (char *)&buf[1], 0);
#else
    _PyFloat_Pack8(d, &buf[1], 0);
#endif
    msgpack_pack_append_buffer(x, buf, 9);
}


/*
 * Nil
 */

static inline int msgpack_pack_nil(msgpack_packer* x)
{
    static const unsigned char d = 0xc0;
    msgpack_pack_append_buffer(x, &d, 1);
}


/*
 * Boolean
 */

static inline int msgpack_pack_true(msgpack_packer* x)
{
    static const unsigned char d = 0xc3;
    msgpack_pack_append_buffer(x, &d, 1);
}

static inline int msgpack_pack_false(msgpack_packer* x)
{
    static const unsigned char d = 0xc2;
    msgpack_pack_append_buffer(x, &d, 1);
}


/*
 * Array
 */

static inline int msgpack_pack_array(msgpack_packer* x, unsigned int n)
{
    if(n < 16) {
        unsigned char d = 0x90 | n;
        msgpack_pack_append_buffer(x, &d, 1);
    } else if(n < 65536) {
        unsigned char buf[3];
        buf[0] = 0xdc; _msgpack_store16(&buf[1], (uint16_t)n);
        msgpack_pack_append_buffer(x, buf, 3);
    } else {
        unsigned char buf[5];
        buf[0] = 0xdd; _msgpack_store32(&buf[1], (uint32_t)n);
        msgpack_pack_append_buffer(x, buf, 5);
    }
}


/*
 * Map
 */

static inline int msgpack_pack_map(msgpack_packer* x, unsigned int n)
{
    if(n < 16) {
        unsigned char d = 0x80 | n;
        msgpack_pack_append_buffer(x, &TAKE8_8(d), 1);
    } else if(n < 65536) {
        unsigned char buf[3];
        buf[0] = 0xde; _msgpack_store16(&buf[1], (uint16_t)n);
        msgpack_pack_append_buffer(x, buf, 3);
    } else {
        unsigned char buf[5];
        buf[0] = 0xdf; _msgpack_store32(&buf[1], (uint32_t)n);
        msgpack_pack_append_buffer(x, buf, 5);
    }
}


/*
 * Raw
 */

static inline int msgpack_pack_raw(msgpack_packer* x, size_t l)
{
    if (l < 32) {
        unsigned char d = 0xa0 | (uint8_t)l;
        msgpack_pack_append_buffer(x, &TAKE8_8(d), 1);
    } else if (x->use_bin_type && l < 256) {  // str8 is new format introduced with bin.
        unsigned char buf[2] = {0xd9, (uint8_t)l};
        msgpack_pack_append_buffer(x, buf, 2);
    } else if (l < 65536) {
        unsigned char buf[3];
        buf[0] = 0xda; _msgpack_store16(&buf[1], (uint16_t)l);
        msgpack_pack_append_buffer(x, buf, 3);
    } else {
        unsigned char buf[5];
        buf[0] = 0xdb; _msgpack_store32(&buf[1], (uint32_t)l);
        msgpack_pack_append_buffer(x, buf, 5);
    }
}

/*
 * bin
 */
static inline int msgpack_pack_bin(msgpack_packer *x, size_t l)
{
    if (!x->use_bin_type) {
        return msgpack_pack_raw(x, l);
    }
    if (l < 256) {
        unsigned char buf[2] = {0xc4, (unsigned char)l};
        msgpack_pack_append_buffer(x, buf, 2);
    } else if (l < 65536) {
        unsigned char buf[3] = {0xc5};
        _msgpack_store16(&buf[1], (uint16_t)l);
        msgpack_pack_append_buffer(x, buf, 3);
    } else {
        unsigned char buf[5] = {0xc6};
        _msgpack_store32(&buf[1], (uint32_t)l);
        msgpack_pack_append_buffer(x, buf, 5);
    }
}

static inline int msgpack_pack_raw_body(msgpack_packer* x, const void* b, size_t l)
{
    if (l > 0) msgpack_pack_append_buffer(x, (const unsigned char*)b, l);
    return 0;
}

/*
 * Ext
 */
static inline int msgpack_pack_ext(msgpack_packer* x, char typecode, size_t l)
{
    if (l == 1) {
        unsigned char buf[2];
        buf[0] = 0xd4;
        buf[1] = (unsigned char)typecode;
        msgpack_pack_append_buffer(x, buf, 2);
    }
    else if(l == 2) {
        unsigned char buf[2];
        buf[0] = 0xd5;
        buf[1] = (unsigned char)typecode;
        msgpack_pack_append_buffer(x, buf, 2);
    }
    else if(l == 4) {
        unsigned char buf[2];
        buf[0] = 0xd6;
        buf[1] = (unsigned char)typecode;
        msgpack_pack_append_buffer(x, buf, 2);
    }
    else if(l == 8) {
        unsigned char buf[2];
        buf[0] = 0xd7;
        buf[1] = (unsigned char)typecode;
        msgpack_pack_append_buffer(x, buf, 2);
    }
    else if(l == 16) {
        unsigned char buf[2];
        buf[0] = 0xd8;
        buf[1] = (unsigned char)typecode;
        msgpack_pack_append_buffer(x, buf, 2);
    }
    else if(l < 256) {
        unsigned char buf[3];
        buf[0] = 0xc7;
        buf[1] = l;
        buf[2] = (unsigned char)typecode;
        msgpack_pack_append_buffer(x, buf, 3);
    } else if(l < 65536) {
        unsigned char buf[4];
        buf[0] = 0xc8;
        _msgpack_store16(&buf[1], (uint16_t)l);
        buf[3] = (unsigned char)typecode;
        msgpack_pack_append_buffer(x, buf, 4);
    } else {
        unsigned char buf[6];
        buf[0] = 0xc9;
        _msgpack_store32(&buf[1], (uint32_t)l);
        buf[5] = (unsigned char)typecode;
        msgpack_pack_append_buffer(x, buf, 6);
    }

}

/*
 * Pack Timestamp extension type. Follows msgpack-c pack_template.h.
 */
static inline int msgpack_pack_timestamp(msgpack_packer* x, int64_t seconds, uint32_t nanoseconds)
{
    if ((seconds >> 34) == 0) {
        /* seconds is unsigned and fits in 34 bits */
        uint64_t data64 = ((uint64_t)nanoseconds << 34) | (uint64_t)seconds;
        if ((data64 & 0xffffffff00000000L) == 0) {
            /* no nanoseconds and seconds is 32bits or smaller. timestamp32. */
            unsigned char buf[4];
            uint32_t data32 = (uint32_t)data64;
            msgpack_pack_ext(x, -1, 4);
            _msgpack_store32(buf, data32);
            msgpack_pack_raw_body(x, buf, 4);
        } else {
            /* timestamp64 */
            unsigned char buf[8];
            msgpack_pack_ext(x, -1, 8);
            _msgpack_store64(buf, data64);
            msgpack_pack_raw_body(x, buf, 8);

        }
    } else {
       /* seconds is signed or >34bits */
       unsigned char buf[12];
       _msgpack_store32(&buf[0], nanoseconds);
       _msgpack_store64(&buf[4], seconds);
       msgpack_pack_ext(x, -1, 12);
       msgpack_pack_raw_body(x, buf, 12);
    }
    return 0;
}


#undef msgpack_pack_append_buffer

#undef TAKE8_8
#undef TAKE8_16
#undef TAKE8_32
#undef TAKE8_64

#undef msgpack_pack_real_uint16
#undef msgpack_pack_real_uint32
#undef msgpack_pack_real_uint64
#undef msgpack_pack_real_int16
#undef msgpack_pack_real_int32
#undef msgpack_pack_real_int64
