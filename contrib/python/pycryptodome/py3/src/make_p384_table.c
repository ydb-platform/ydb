#include "common.h"
#include "ec.h"
#include "endianess.h"

#define BITS    384
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
    const uint8_t p384_mod[BYTES] = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfe\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff";
    const uint8_t  b[BYTES] = "\xb3\x31\x2f\xa7\xe2\x3e\xe7\xe4\x98\x8e\x05\x6b\xe3\xf8\x2d\x19\x18\x1d\x9c\x6e\xfe\x81\x41\x12\x03\x14\x08\x8f\x50\x13\x87\x5a\xc6\x56\x39\x8d\x8a\x2e\xd1\x9d\x2a\x85\xc8\xed\xd3\xec\x2a\xef";
    const uint8_t order[BYTES] = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xc7\x63\x4d\x81\xf4\x37\x2d\xdf\x58\x1a\x0d\xb2\x48\xb0\xa7\x7a\xec\xec\x19\x6a\xcc\xc5\x29\x73";
    const uint8_t p384_Gx[BYTES] = "\xaa\x87\xca\x22\xbe\x8b\x05\x37\x8e\xb1\xc7\x1e\xf3\x20\xad\x74\x6e\x1d\x3b\x62\x8b\xa7\x9b\x98\x59\xf7\x41\xe0\x82\x54\x2a\x38\x55\x02\xf2\x5d\xbf\x55\x29\x6c\x3a\x54\x5e\x38\x72\x76\x0a\xb7";
    const uint8_t p384_Gy[BYTES] = "\x36\x17\xde\x4a\x96\x26\x2c\x6f\x5d\x9e\x98\xbf\x92\x92\xdc\x29\xf8\xf4\x1d\xbd\x28\x9a\x14\x7c\xe9\xda\x31\x13\xb5\xf0\xb8\xc0\x0a\x60\xb1\xce\x1d\x7e\x81\x9d\x7a\x43\x1d\x7c\x90\xea\x0e\x5f";
    uint8_t xz[BYTES] = { 0 }, yz[BYTES] = { 0 };
    EcContext *ec_ctx;
    EcPoint *g = NULL;
    EcPoint **window = NULL;
    unsigned i, j;
    unsigned n_tables, points_per_table, window_size;

    ec_ws_new_context(&ec_ctx, p384_mod, b, order, BYTES, 0);
    ec_ws_new_point(&g, p384_Gx, p384_Gy, BYTES, ec_ctx);

    /** TODO: accept this as input **/
    window_size = 5;

    points_per_table = 1U << window_size;
    n_tables = (BITS+window_size-1)/window_size;

    /** Create table with points 0, G, 2G, 3G, .. (2**window_size-1)G **/
    window = (EcPoint**)calloc(points_per_table, sizeof(EcPoint*));
    ec_ws_new_point(&window[0], xz, yz, BYTES, ec_ctx);
    for (i=1; i<points_per_table; i++) {
        ec_ws_clone(&window[i], window[i-1]);
        ec_ws_add(window[i], g);
    }

    printf("/* This file was automatically generated, do not edit */\n");
    printf("#include \"common.h\"\n");
    printf("#include \"p384_table.h\"\n");
    printf("const unsigned p384_n_tables = %u;\n", n_tables);
    printf("const unsigned p384_window_size = %u;\n", window_size);
    printf("const unsigned p384_points_per_table = %u;\n", points_per_table);
    printf("/* Affine coordinates in Montgomery form */\n");
    printf("/* Table size: %u kbytes */\n", (unsigned)(n_tables*points_per_table*2*WORDS*sizeof(uint64_t)));
    printf("const uint64_t p384_tables[%u][%u][2][%d] = {\n", n_tables, points_per_table, WORDS);

    for (i=0; i<n_tables; i++) {

        printf(" { /* Table #%u */\n", i);
        for (j=0; j<points_per_table; j++) {
            uint64_t xw[WORDS], yw[WORDS];

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
            print_64bit_array(xw, 6);
            printf(" },\n");
            printf("    { ");
            print_64bit_array(yw, 6);
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
