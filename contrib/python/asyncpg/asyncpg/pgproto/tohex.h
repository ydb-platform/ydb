#define HEX_PRELUDE \
    const char *__hexm = "0123456789abcdef";

#define HEX_1_BYTE(buf, dest)                   \
    {                                           \
        char byte = (buf)[0];                   \
        (dest)[0] = __hexm[(byte >> 4) & 0x0F]; \
        (dest)[1] = __hexm[byte & 0x0F];        \
    }

#define HEX_2_BYTES(buf, dest)                  \
    {                                           \
        HEX_1_BYTE(buf, dest)                   \
        HEX_1_BYTE(buf + 1, dest + 2)           \
    }

#define HEX_4_BYTES(buf, dest)                  \
    {                                           \
        HEX_2_BYTES(buf, dest)                  \
        HEX_2_BYTES(buf + 2, dest + 4)          \
    }

#define HEX_8_BYTES(buf, dest)                  \
    {                                           \
        HEX_4_BYTES(buf, dest)                  \
        HEX_4_BYTES(buf + 4, dest + 8)          \
    }


static inline void
uuid_to_str(const char *source, char *dest)
{
    HEX_PRELUDE

    HEX_4_BYTES(source, dest)
    dest[8] = '-';
    HEX_2_BYTES(source + 4, dest + 9)
    dest[13] = '-';
    HEX_2_BYTES(source + 6, dest + 14)
    dest[18] = '-';
    HEX_2_BYTES(source + 8, dest + 19)
    dest[23] = '-';
    HEX_4_BYTES(source + 10, dest + 24)
    HEX_2_BYTES(source + 14, dest + 32)
}


static inline void
uuid_to_hex(const char *source, char *dest)
{
    HEX_PRELUDE
    HEX_8_BYTES(source, dest)
    HEX_8_BYTES(source + 8, dest + 16)
}
