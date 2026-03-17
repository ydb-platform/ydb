#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>

#include "../../../readstat.h"
#include "../../util/readstat_sav_date.h"
#include "../../util/readstat_dta_days.h"

#include "../../extract_metadata.h"
#include "write_missing_values.h"

static void handle_missing_discrete(struct context *ctx, readstat_variable_t *variable) {
    const char *format = readstat_variable_get_format(variable);
    int spss_date = format && (strcmp(format, "EDATE40") == 0) && variable->type == READSTAT_TYPE_DOUBLE;
    int missing_ranges_count = readstat_variable_get_missing_ranges_count(variable);
    fprintf(ctx->fp, ", \"missing\": { \"type\": \"DISCRETE\", \"values\": [");
    
    for (int i=0; i<missing_ranges_count; i++) {
        readstat_value_t lo_val = readstat_variable_get_missing_range_lo(variable, i);
        readstat_value_t hi_val = readstat_variable_get_missing_range_hi(variable, i);
        if (i>=1) {
            fprintf(ctx->fp, ", ");
        }

        if (readstat_value_type(lo_val) == READSTAT_TYPE_DOUBLE) {
            double lo = readstat_double_value(lo_val);
            double hi = readstat_double_value(hi_val);
            if (lo == hi && spss_date) {
                char buf[255];
                char *s = readstat_sav_date_string(lo, buf, sizeof(buf)-1);
                if (!s) {
                    fprintf(stderr, "Could not parse date %lf\n", lo);
                    exit(EXIT_FAILURE);
                }
                fprintf(ctx->fp, "\"%s\"", s);
            } else if (lo == hi) {
                fprintf(ctx->fp, "%g", lo);
            } else {
                fprintf(stderr, "%s:%d column %s unsupported lo %lf hi %lf\n", __FILE__, __LINE__, variable->name, lo, hi);
                exit(EXIT_FAILURE);
            }
        } else {
            fprintf(stderr, "%s:%d unsupported missing type\n", __FILE__, __LINE__);
            exit(EXIT_FAILURE);
        }
    }
    fprintf(ctx->fp, "]} ");
}

static void handle_missing_range(struct context *ctx, readstat_variable_t *variable) {
    const char *format = readstat_variable_get_format(variable);
    int spss_date = format && (strcmp(format, "EDATE40") == 0) && variable->type == READSTAT_TYPE_DOUBLE;
    int missing_ranges_count = readstat_variable_get_missing_ranges_count(variable);
    fprintf(ctx->fp, ", \"missing\": { \"type\": \"RANGE\", ");
    
    for (int i=0; i<missing_ranges_count; i++) {
        readstat_value_t lo_val = readstat_variable_get_missing_range_lo(variable, i);
        readstat_value_t hi_val = readstat_variable_get_missing_range_hi(variable, i);
        if (i>=1) {
            fprintf(ctx->fp, ", ");
        }

        if (readstat_value_type(lo_val) == READSTAT_TYPE_DOUBLE) {
            double lo = readstat_double_value(lo_val);
            double hi = readstat_double_value(hi_val);
            if (spss_date) {
                char buf[255];
                char buf2[255];
                char *s = readstat_sav_date_string(lo, buf, sizeof(buf)-1);
                char *s2 = readstat_sav_date_string(hi, buf2, sizeof(buf2)-1);
                if (!s) {
                    fprintf(stderr, "Could not parse date %lf\n", lo);
                    exit(EXIT_FAILURE);
                }
                if (!s2) {
                    fprintf(stderr, "Could not parse date %lf\n", hi);
                    exit(EXIT_FAILURE);
                }
                if (lo == hi) {
                    fprintf(ctx->fp, "\"discrete-value\": \"%s\"", s);
                } else {
                    fprintf(ctx->fp, "\"low\": \"%s\", \"high\": \"%s\"", s, s2);
                }
            } else {
                if (lo == hi) {
                    fprintf(ctx->fp, "\"discrete-value\": %lf", lo);
                } else {
                    fprintf(ctx->fp, "\"low\": %lf, \"high\": %lf", lo, hi);
                }
            }
        } else {
            fprintf(stderr, "%s:%d unsupported missing type\n", __FILE__, __LINE__);
            exit(EXIT_FAILURE);
        }
    }
    fprintf(ctx->fp, "} ");
}

void add_missing_values(struct context *ctx, readstat_variable_t *variable) {
    int missing_ranges_count = readstat_variable_get_missing_ranges_count(variable);
    if (missing_ranges_count == 0) {
        return;
    }
    
    int is_range = 0;
    int discrete = 0;
    int only_double = 1;

    for (int i=0; i<missing_ranges_count; i++) {
        readstat_value_t lo_val = readstat_variable_get_missing_range_lo(variable, i);
        readstat_value_t hi_val = readstat_variable_get_missing_range_hi(variable, i);
        if (readstat_value_type(lo_val) == READSTAT_TYPE_DOUBLE && readstat_value_type(hi_val) == READSTAT_TYPE_DOUBLE) {
            double lo = readstat_double_value(lo_val);
            double hi = readstat_double_value(hi_val);
            if (lo != hi) {
                is_range = 1;
            } else {
                discrete = 1;
            }
        } else {
            only_double = 0;
        }
    }

    if (!only_double) {
        fprintf(stderr, "%s:%d only implemented double support for missing values\n", __FILE__, __LINE__);
        exit(EXIT_FAILURE);
    }

    if (is_range || (is_range && discrete)) {
        handle_missing_range(ctx, variable);
    } else if (discrete) {
        handle_missing_discrete(ctx, variable);
    } else {
        fprintf(stderr, "%s:%d unexpected state\n", __FILE__, __LINE__);
        exit(EXIT_FAILURE);
    }
}
