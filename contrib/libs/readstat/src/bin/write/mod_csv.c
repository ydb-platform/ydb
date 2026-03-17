#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>

#include "../../readstat.h"
#include "module_util.h"
#include "module.h"
#include "../util/readstat_dta_days.h"
#include "../util/readstat_sav_date.h"
#include "double_decimals.h"

typedef struct mod_csv_ctx_s {
    FILE *out_file;
    long var_count;
} mod_csv_ctx_t;

static int accept_file(const char *filename);
static void *ctx_init(const char *filename);
static void finish_file(void *ctx);
static int handle_metadata(readstat_metadata_t *metadata, void *ctx);
static int handle_variable(int index, readstat_variable_t *variable,
                           const char *val_labels, void *ctx);
static int handle_value(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx);

rs_module_t rs_mod_csv = {
    .accept = accept_file,
    .init = ctx_init,
    .finish = finish_file,
    .handle = {
        .metadata = handle_metadata,
        .variable = handle_variable,
        .value = handle_value
    }
};

static int accept_file(const char *filename) {
    return strcmp(filename, "-") == 0 || rs_ends_with(filename, ".csv");
}

static void *ctx_init(const char *filename) {
    mod_csv_ctx_t *mod_ctx = malloc(sizeof(mod_csv_ctx_t));
    if (strcmp(filename, "-") == 0) {
        mod_ctx->out_file = stdout;
    } else {
        mod_ctx->out_file = fopen(filename, "w");
    }
    if (mod_ctx->out_file == NULL) {
        fprintf(stderr, "Error opening %s for writing: %s\n", filename, strerror(errno));
        return NULL;
    }
    return mod_ctx;
}

static void finish_file(void *ctx) {
    mod_csv_ctx_t *mod_ctx = (mod_csv_ctx_t *)ctx;
    if (mod_ctx) {
        if (mod_ctx->out_file == stdout) {
            fflush(mod_ctx->out_file);
        } else if (mod_ctx->out_file != NULL) {
            fclose(mod_ctx->out_file);
        }
    }
}

static int handle_metadata(readstat_metadata_t *metadata, void *ctx) {
    mod_csv_ctx_t *mod_ctx = (mod_csv_ctx_t *)ctx;
    mod_ctx->var_count = readstat_get_var_count(metadata);
    return mod_ctx->var_count == 0;
}

static void emit_escaped_string(mod_csv_ctx_t *mod_ctx, const char *string) {
    if (string == NULL) {
        fprintf(mod_ctx->out_file, "\"\"");
    } else {
        fprintf(mod_ctx->out_file, "\"");
        const char *p = NULL;
        while ((string = strchr(p = string, '"'))) {
            fwrite(p, string - p, 1, mod_ctx->out_file);
            fprintf(mod_ctx->out_file, "\"\"");
            string++;
        }
        fprintf(mod_ctx->out_file, "%s\"", p);
    }
}

static int handle_variable(int index, readstat_variable_t *variable,
                           const char *val_labels, void *ctx) {
    mod_csv_ctx_t *mod_ctx = (mod_csv_ctx_t *)ctx;
    const char *name = readstat_variable_get_name(variable);
    if (index > 0) {
        fprintf(mod_ctx->out_file, ",");
    }
    emit_escaped_string(mod_ctx, name);
    if (index == mod_ctx->var_count - 1) {
        fprintf(mod_ctx->out_file, "\n");
    }
    return 0;
}

static int handle_value(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx) {
    mod_csv_ctx_t *mod_ctx = (mod_csv_ctx_t *)ctx;
    readstat_type_t type = readstat_value_type(value);
    const char *format = readstat_variable_get_format(variable);
    int var_index = readstat_variable_get_index(variable);
    if (var_index > 0) {
        fprintf(mod_ctx->out_file, ",");
    }
    if (readstat_value_is_system_missing(value)) {
        /* void */
    } else if (readstat_value_is_tagged_missing(value)) {
        /* void */
    } else if (type == READSTAT_TYPE_STRING) {
        emit_escaped_string(mod_ctx, readstat_string_value(value));
    } else if (type == READSTAT_TYPE_INT8) {
        #ifdef __MINGW32__
        __mingw_fprintf(mod_ctx->out_file, "%hhd", readstat_int8_value(value));
        #else
        fprintf(mod_ctx->out_file, "%hhd", readstat_int8_value(value));
        #endif
    } else if (type == READSTAT_TYPE_INT16) {
        fprintf(mod_ctx->out_file, "%hd", readstat_int16_value(value));
    } else if (type == READSTAT_TYPE_INT32 && format && 0 == strncmp("%td", format, strlen("%td"))) {
        int days = readstat_int32_value(value);
        char days_str[255];
        readstat_dta_days_string(days, days_str, sizeof(days_str)-1);
        fprintf(mod_ctx->out_file, "%s", days_str);
    } else if (type == READSTAT_TYPE_DOUBLE && format && 0 == strncmp("EDATE40", format, strlen("EDATE40"))) {
        double v = readstat_double_value(value);
        char date_str[255];
        char *s = readstat_sav_date_string(v, date_str, sizeof(date_str)-1);
        if (!s) {
            fprintf(stderr, "%s:%d Could not parse SPSS date double: %lf\n", __FILE__, __LINE__, v);
            exit(EXIT_FAILURE);
        }
        fprintf(mod_ctx->out_file, "%s", s);
    } else if (type == READSTAT_TYPE_INT32) {
        fprintf(mod_ctx->out_file, "%d", readstat_int32_value(value));
    } else if (type == READSTAT_TYPE_FLOAT) {
        fprintf(mod_ctx->out_file, "%f", readstat_float_value(value));
    } else if (type == READSTAT_TYPE_DOUBLE) {
        double v = readstat_double_value(value);
        int decimals = double_decimals(v);
        if (decimals <= 6) {
            fprintf(mod_ctx->out_file, "%lf", v);
        } else {
            fprintf(mod_ctx->out_file, "%.14f", v);
        }
    }
    if (var_index == mod_ctx->var_count - 1) {
        fprintf(mod_ctx->out_file, "\n");
    }
    return 0;
}
