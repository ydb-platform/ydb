#include "common.h"
#include "ec.h"
#include "endianess.h"

#define BITS    521
#define BYTES   66
#define WORDS   9

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
    const uint8_t p521_mod[BYTES] = "\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";
    const uint8_t b[BYTES] = "\x00\x51\x95\x3e\xb9\x61\x8e\x1c\x9a\x1f\x92\x9a\x21\xa0\xb6\x85\x40\xee\xa2\xda\x72\x5b\x99\xb3\x15\xf3\xb8\xb4\x89\x91\x8e\xf1\x09\xe1\x56\x19\x39\x51\xec\x7e\x93\x7b\x16\x52\xc0\xbd\x3b\xb1\xbf\x07\x35\x73\xdf\x88\x3d\x2c\x34\xf1\xef\x45\x1f\xd4\x6b\x50\x3f\x00";
    const uint8_t order[BYTES] = "\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfa\x51\x86\x87\x83\xbf\x2f\x96\x6b\x7f\xcc\x01\x48\xf7\x09\xa5\xd0\x3b\xb5\xc9\xb8\x89\x9c\x47\xae\xbb\x6f\xb7\x1e\x91\x38\x64\x09";
    const uint8_t p521_Gx[BYTES] = "\x00\xc6\x85\x8e\x06\xb7\x04\x04\xe9\xcd\x9e\x3e\xcb\x66\x23\x95\xb4\x42\x9c\x64\x81\x39\x05\x3f\xb5\x21\xf8\x28\xaf\x60\x6b\x4d\x3d\xba\xa1\x4b\x5e\x77\xef\xe7\x59\x28\xfe\x1d\xc1\x27\xa2\xff\xa8\xde\x33\x48\xb3\xc1\x85\x6a\x42\x9b\xf9\x7e\x7e\x31\xc2\xe5\xbd\x66";
    const uint8_t p521_Gy[BYTES] = "\x01\x18\x39\x29\x6a\x78\x9a\x3b\xc0\x04\x5c\x8a\x5f\xb4\x2c\x7d\x1b\xd9\x98\xf5\x44\x49\x57\x9b\x44\x68\x17\xaf\xbd\x17\x27\x3e\x66\x2c\x97\xee\x72\x99\x5e\xf4\x26\x40\xc5\x50\xb9\x01\x3f\xad\x07\x61\x35\x3c\x70\x86\xa2\x72\xc2\x40\x88\xbe\x94\x76\x9f\xd1\x66\x50";
    uint8_t xz[WORDS*8] = { 0 }, yz[WORDS*8] = { 0 };
    EcContext *ec_ctx;
    EcPoint *g = NULL;
    EcPoint **window = NULL;
    unsigned i, j;
    unsigned n_tables, points_per_table, window_size;

    ec_ws_new_context(&ec_ctx, p521_mod, b, order, BYTES, 0);
    ec_ws_new_point(&g, p521_Gx, p521_Gy, BYTES, ec_ctx);

    /** TODO: accept this as input **/
    window_size = 4;

    points_per_table = 1U << window_size;
    n_tables = (BITS+window_size-1)/window_size;

    /** Create table with points 0, G, 2G, 3G, .. (2**window_size-1)G **/
    window = (EcPoint**)calloc(points_per_table, sizeof(EcPoint*));
    ec_ws_new_point(&window[0], xz, yz, sizeof xz, ec_ctx);
    for (i=1; i<points_per_table; i++) {
        ec_ws_clone(&window[i], window[i-1]);
        ec_ws_add(window[i], g);
    }

    printf("/* This file was automatically generated, do not edit */\n");
    printf("#include \"common.h\"\n");
    printf("#include \"p521_table.h\"\n");
    printf("const unsigned p521_n_tables = %u;\n", n_tables);
    printf("const unsigned p521_window_size = %u;\n", window_size);
    printf("const unsigned p521_points_per_table = %u;\n", points_per_table);
    printf("/* Affine coordinates in plain form (not Montgomery) */\n");
    printf("/* Table size: %u kbytes */\n", (unsigned)(n_tables*points_per_table*2*WORDS*sizeof(uint64_t)));
    printf("const uint64_t p521_tables[%u][%u][2][%d] = {\n", n_tables, points_per_table, WORDS);

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
            print_64bit_array(xw, WORDS);
            printf(" },\n");
            printf("    { ");
            print_64bit_array(yw, WORDS);
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
