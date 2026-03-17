#include "common.h"
#include "ec.h"
#include "endianess.h"

#define BITS    256
#define BYTES   BITS/8
#define WORDS   BITS/64

static void print_64bit_array(uint64_t *x, unsigned len)
{
    unsigned i, j;

    for (i=0; i<len; i++) {
        printf("0x");
        for (j=0; j<8; j++) {
            printf("%02X", (uint8_t)(x[i] >> ((7-j)*8)));
        }
        printf("ULL");
        if (i!=(len-1))
            printf(",");
    }
}

int main(void)
{
    const uint8_t p256_mod[32] = "\xff\xff\xff\xff\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";
    const uint8_t  b[32] = "\x5a\xc6\x35\xd8\xaa\x3a\x93\xe7\xb3\xeb\xbd\x55\x76\x98\x86\xbc\x65\x1d\x06\xb0\xcc\x53\xb0\xf6\x3b\xce\x3c\x3e\x27\xd2\x60\x4b";
    const uint8_t order[32] = "\xff\xff\xff\xff\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xbc\xe6\xfa\xad\xa7\x17\x9e\x84\xf3\xb9\xca\xc2\xfc\x63\x25\x51";
    const uint8_t p256_Gx[32] = "\x6b\x17\xd1\xf2\xe1\x2c\x42\x47\xf8\xbc\xe6\xe5\x63\xa4\x40\xf2\x77\x03\x7d\x81\x2d\xeb\x33\xa0\xf4\xa1\x39\x45\xd8\x98\xc2\x96";
    const uint8_t p256_Gy[32] = "\x4f\xe3\x42\xe2\xfe\x1a\x7f\x9b\x8e\xe7\xeb\x4a\x7c\x0f\x9e\x16\x2b\xce\x33\x57\x6b\x31\x5e\xce\xcb\xb6\x40\x68\x37\xbf\x51\xf5";
    uint8_t xz[32] = { 0 }, yz[32] = { 0 };
    EcContext *ec_ctx;
    EcPoint *g = NULL;
    EcPoint **window = NULL;
    unsigned i, j;
    unsigned n_tables, points_per_table, window_size;

    ec_ws_new_context(&ec_ctx, p256_mod, b, order, 32, 0);
    ec_ws_new_point(&g, p256_Gx, p256_Gy, 32, ec_ctx);

    /** TODO: accept this as input **/
    window_size = 5;

    points_per_table = 1U << window_size;
    n_tables = (256+window_size-1)/window_size;

    /** Create table with points 0, G, 2G, 3G, .. (2**window_size-1)G **/
    window = (EcPoint**)calloc(points_per_table, sizeof(EcPoint*));
    ec_ws_new_point(&window[0], xz, yz, 32, ec_ctx);
    for (i=1; i<points_per_table; i++) {
        ec_ws_clone(&window[i], window[i-1]);
        ec_ws_add(window[i], g);
    }

    printf("/* This file was automatically generated, do not edit */\n");
    printf("#include \"common.h\"\n");
    printf("#include \"p256_table.h\"\n");
    printf("const unsigned p256_n_tables = %u;\n", n_tables);
    printf("const unsigned p256_window_size = %u;\n", window_size);
    printf("const unsigned p256_points_per_table = %u;\n", points_per_table);
    printf("/* Affine coordinates in Montgomery form */\n");
    printf("/* Table size: %u kbytes */\n", (unsigned)(n_tables*points_per_table*2*WORDS*sizeof(uint64_t)));
    printf("const uint64_t p256_tables[%u][%u][2][4] = {\n", n_tables, points_per_table);

    for (i=0; i<n_tables; i++) {

        printf(" { /* Table #%u */\n", i);
        for (j=0; j<points_per_table; j++) {
            uint64_t xw[4], yw[4];

            if (j == 0) {
                memcpy(xw, xz, sizeof xw);
                memcpy(yw, yz, sizeof yw);
            } else {
                ec_ws_normalize(window[j]);
                memcpy(xw, window[j]->x, sizeof xw);
                memcpy(yw, window[j]->y, sizeof yw);
            }

            printf("  { /* Point #%u */\n", j);
            printf("    { ");
            print_64bit_array(xw, 4);
            printf(" },\n");
            printf("    { ");
            print_64bit_array(yw, 4);
            printf(" }\n");
            printf("  }%s\n", j==points_per_table-1 ? "" : ",");
        }
        printf(" }%s\n", i==n_tables-1 ? "" : ",");

        /* Move from G to G*2^{w} */
        for (j=0; j<window_size; j++)
            ec_ws_double(g);

        for (j=1; j<points_per_table; j++) {
            ec_ws_copy(window[j], window[j-1]);
            ec_ws_add(window[j], g);
        }
    }

    printf("};\n");

    for (i=0; i<points_per_table; i++) {
        ec_ws_free_point(window[i]);
    }
    free(window);
    ec_ws_free_point(g);
    ec_ws_free_context(ec_ctx);

    return 0;
}
