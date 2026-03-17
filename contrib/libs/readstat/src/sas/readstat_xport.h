
typedef struct xport_header_record_s {
    char    name[9];

    int     num1;
    int     num2;
    int     num3;
    int     num4;
    int     num5;
    int     num6;
} xport_header_record_t;

extern char _xport_months[12][4];

#pragma pack(push, 1)
typedef struct xport_namestr_s {
    uint16_t    ntype;
    uint16_t    nhfun;
    uint16_t    nlng;
    uint16_t    nvar0;
    char        nname[8];
    char        nlabel[40];
    char        nform[8];
    uint16_t    nfl;
    uint16_t    nfd;
    uint16_t    nfj;
    char        nfill[2];
    char        niform[8];
    uint16_t    nifl;
    uint16_t    nifd;
    uint32_t    npos;
    char        longname[32];
    uint16_t    labeln;
    char        rest[18];
} xport_namestr_t;
#pragma pack(pop)

typedef struct xport_format_s {
    char         name[32];
    int          width;
    int          decimals;
} xport_format_t;

#define XPORT_MIN_DOUBLE_SIZE   3
#define XPORT_MAX_DOUBLE_SIZE   8

void xport_namestr_bswap(xport_namestr_t *namestr);
