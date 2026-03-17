//
//  sav.c
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <stdint.h>
#include <math.h>
#include <float.h>
#include <time.h>

#include "../readstat.h"
#include "../readstat_bits.h"
#include "../readstat_iconv.h"
#include "../readstat_malloc.h"

#include "readstat_sav.h"

#define SAV_VARINFO_INITIAL_CAPACITY  512

sav_ctx_t *sav_ctx_init(sav_file_header_record_t *header, readstat_io_t *io) {
    sav_ctx_t *ctx = readstat_calloc(1, sizeof(sav_ctx_t));
    if (ctx == NULL) {
        return NULL;
    }

    if (memcmp(&header->rec_type, "$FL2", 4) == 0) {
        ctx->format_version = 2;
    } else if (memcmp(&header->rec_type, "$FL3", 4) == 0) {
        ctx->format_version = 3;
    } else {
        sav_ctx_free(ctx);
        return NULL;
    }
    
    ctx->bswap = !(header->layout_code == 2 || header->layout_code == 3);
    ctx->endianness = (machine_is_little_endian() ^ ctx->bswap) ? READSTAT_ENDIAN_LITTLE : READSTAT_ENDIAN_BIG;

    if (header->compression == 1 || byteswap4(header->compression) == 1) {
        ctx->compression = READSTAT_COMPRESS_ROWS;
    } else if (header->compression == 2 || byteswap4(header->compression) == 2) {
        ctx->compression = READSTAT_COMPRESS_BINARY;
    }
    ctx->record_count = ctx->bswap ? byteswap4(header->ncases) : header->ncases;
    ctx->fweight_index = ctx->bswap ? byteswap4(header->weight_index) : header->weight_index;

    ctx->missing_double = SAV_MISSING_DOUBLE;
    ctx->lowest_double = SAV_LOWEST_DOUBLE;
    ctx->highest_double = SAV_HIGHEST_DOUBLE;
    
    ctx->bias = ctx->bswap ? byteswap_double(header->bias) : header->bias;
    
    ctx->varinfo_capacity = SAV_VARINFO_INITIAL_CAPACITY;
    
    if ((ctx->varinfo = readstat_calloc(ctx->varinfo_capacity, sizeof(spss_varinfo_t *))) == NULL) {
        sav_ctx_free(ctx);
        return NULL;
    }

    ctx->io = io;
    
    return ctx;
}

void sav_ctx_free(sav_ctx_t *ctx) {
    if (ctx->varinfo) {
        int i;
        for (i=0; i<ctx->var_index; i++) {
            spss_varinfo_free(ctx->varinfo[i]);
        }
        free(ctx->varinfo);
    }
    if (ctx->variables) {
        int i;
        for (i=0; i<ctx->var_count; i++) {
            if (ctx->variables[i])
                free(ctx->variables[i]);
        }
        free(ctx->variables);
    }
    if (ctx->raw_string)
        free(ctx->raw_string);
    if (ctx->utf8_string)
        free(ctx->utf8_string);
    if (ctx->converter)
        iconv_close(ctx->converter);
    if (ctx->variable_display_values) {
        free(ctx->variable_display_values);
    }
    free(ctx);
}

