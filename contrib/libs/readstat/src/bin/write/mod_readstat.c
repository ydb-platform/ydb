#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>

#include "../../readstat.h"
#include "../../CKHashTable.h"
#include "module_util.h"
#include "module.h"

typedef struct mod_readstat_ctx_s {
    readstat_writer_t *writer;
    ck_hash_table_t *label_set_dict;

    long fweight_index;
    long var_count;
    long row_count;

    FILE *out_file;

    unsigned int is_sav:1;
    unsigned int is_zsav:1;
    unsigned int is_dta:1;
    unsigned int is_por:1;
    unsigned int is_sas7bdat:1;
    unsigned int is_xport:1;
} mod_readstat_ctx_t;

static ssize_t write_data(const void *bytes, size_t len, void *ctx);

static int accept_file(const char *filename);
static void *ctx_init(const char *filename);
static void finish_file(void *ctx);

static int handle_fweight(readstat_variable_t *variable, void *ctx);
static int handle_metadata(readstat_metadata_t *metadata, void *ctx);
static int handle_note(int note_index, const char *note, void *ctx);
static int handle_value_label(const char *val_labels, readstat_value_t value,
                              const char *label, void *ctx);
static int handle_variable(int index, readstat_variable_t *variable,
                           const char *val_labels, void *ctx);
static int handle_value(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx);

rs_module_t rs_mod_readstat = {
    .accept = accept_file,
    .init = ctx_init,
    .finish = finish_file,
    .handle = {
        .metadata = handle_metadata,
        .note = handle_note,
        .variable = handle_variable,
        .fweight = handle_fweight,
        .value = handle_value,
        .value_label = handle_value_label
    }
};

static ssize_t write_data(const void *bytes, size_t len, void *ctx) {
    mod_readstat_ctx_t *mod_ctx = (mod_readstat_ctx_t *)ctx;
    return fwrite(bytes, 1, len, mod_ctx->out_file);
}

static int accept_file(const char *filename) {
    return (rs_ends_with(filename, ".dta") ||
            rs_ends_with(filename, ".sav") ||
            rs_ends_with(filename, ".zsav") ||
            rs_ends_with(filename, ".por") ||
            rs_ends_with(filename, ".sas7bdat") ||
            rs_ends_with(filename, ".xpt"));
}

static void *ctx_init(const char *filename) {
    mod_readstat_ctx_t *mod_ctx = malloc(sizeof(mod_readstat_ctx_t));
    mod_ctx->label_set_dict = ck_hash_table_init(1024, 16);
    mod_ctx->is_sav = rs_ends_with(filename, ".sav");
    mod_ctx->is_zsav = rs_ends_with(filename, ".zsav");
    mod_ctx->is_dta = rs_ends_with(filename, ".dta");
    mod_ctx->is_por = rs_ends_with(filename, ".por");
    mod_ctx->is_sas7bdat = rs_ends_with(filename, ".sas7bdat");
    mod_ctx->is_xport = rs_ends_with(filename, ".xpt");
    mod_ctx->out_file = fopen(filename, "wb");
    if (mod_ctx->out_file == NULL) {
        fprintf(stderr, "Error opening %s for writing: %s\n", filename, strerror(errno));
        return NULL;
    }

    mod_ctx->writer = readstat_writer_init();
    readstat_writer_set_file_label(mod_ctx->writer, "Created by ReadStat <https://github.com/WizardMac/ReadStat>");
    readstat_set_data_writer(mod_ctx->writer, &write_data);

    return mod_ctx;
}

void finish_file(void *ctx) {
    mod_readstat_ctx_t *mod_ctx = (mod_readstat_ctx_t *)ctx;
    if (mod_ctx) {
        if (mod_ctx->out_file)
            fclose(mod_ctx->out_file);
        if (mod_ctx->label_set_dict)
            ck_hash_table_free(mod_ctx->label_set_dict);
        if (mod_ctx->writer)
            readstat_writer_free(mod_ctx->writer);
        free(mod_ctx);
    }
}

static int handle_fweight(readstat_variable_t *variable, void *ctx) {
    mod_readstat_ctx_t *mod_ctx = (mod_readstat_ctx_t *)ctx;
    readstat_writer_t *writer = mod_ctx->writer;
    readstat_variable_t *new_variable = readstat_get_variable(writer, readstat_variable_get_index(variable));
    readstat_writer_set_fweight_variable(writer, new_variable);
    return READSTAT_HANDLER_OK;
}

static int handle_metadata(readstat_metadata_t *metadata, void *ctx) {
    mod_readstat_ctx_t *mod_ctx = (mod_readstat_ctx_t *)ctx;
    readstat_writer_t *writer = mod_ctx->writer;
    mod_ctx->var_count = readstat_get_var_count(metadata);
    mod_ctx->row_count = readstat_get_row_count(metadata);

    if (mod_ctx->var_count == 0 || mod_ctx->row_count == 0)
        return READSTAT_HANDLER_ABORT;

    readstat_writer_set_file_label(writer, readstat_get_file_label(metadata));
    return READSTAT_HANDLER_OK;
}

static int handle_note(int note_index, const char *note, void *ctx) {
    mod_readstat_ctx_t *mod_ctx = (mod_readstat_ctx_t *)ctx;
    readstat_writer_t *writer = mod_ctx->writer;
    readstat_add_note(writer, note);
    return READSTAT_HANDLER_OK;
}

static int handle_value_label(const char *val_labels, readstat_value_t value,
                              const char *label, void *ctx) {
    mod_readstat_ctx_t *mod_ctx = (mod_readstat_ctx_t *)ctx;
    readstat_writer_t *writer = mod_ctx->writer;
    readstat_label_set_t *label_set = NULL;
    readstat_type_t type = readstat_value_type(value);

    label_set = (readstat_label_set_t *)ck_str_hash_lookup(val_labels, mod_ctx->label_set_dict);
    if (label_set == NULL) {
        label_set = readstat_add_label_set(writer, type, val_labels);
        ck_str_hash_insert(val_labels, label_set, mod_ctx->label_set_dict);
    }

    if (mod_ctx->is_dta && readstat_value_is_tagged_missing(value)) {
        readstat_label_tagged_value(label_set, readstat_value_tag(value), label);
    } else if (type == READSTAT_TYPE_INT32) {
        readstat_label_int32_value(label_set, readstat_int32_value(value), label);
    } else if (type == READSTAT_TYPE_DOUBLE) {
        readstat_label_double_value(label_set, readstat_double_value(value), label);
    } else if (type == READSTAT_TYPE_STRING) {
        readstat_label_string_value(label_set, readstat_string_value(value), label);
    }

    return READSTAT_HANDLER_OK;
}

static int handle_variable(int index, readstat_variable_t *variable,
                           const char *val_labels, void *ctx) {
    mod_readstat_ctx_t *mod_ctx = (mod_readstat_ctx_t *)ctx;
    readstat_writer_t *writer = mod_ctx->writer;

    readstat_type_t type = readstat_variable_get_type(variable);
    const char *name = readstat_variable_get_name(variable);
    const char *label = readstat_variable_get_label(variable);
    size_t storage_width = readstat_variable_get_storage_width(variable);
    int display_width = readstat_variable_get_display_width(variable);
    int missing_ranges_count = readstat_variable_get_missing_ranges_count(variable);
    readstat_alignment_t alignment = readstat_variable_get_alignment(variable);
    readstat_measure_t measure = readstat_variable_get_measure(variable);
    // TODO format translation (readstat_variable_get_format + readstat_variable_set_format)
    
    readstat_variable_t *new_variable = readstat_add_variable(writer, name, type, storage_width);

    if (val_labels) {
        readstat_label_set_t *label_set = (readstat_label_set_t *)ck_str_hash_lookup(val_labels, mod_ctx->label_set_dict);
        readstat_variable_set_label_set(new_variable, label_set);
        if (mod_ctx->is_sas7bdat) {
            readstat_variable_set_format(new_variable, val_labels);
        }
    }

    int i;
    for (i=0; i<missing_ranges_count; i++) {
        readstat_value_t lo_val = readstat_variable_get_missing_range_lo(variable, i);
        readstat_value_t hi_val = readstat_variable_get_missing_range_hi(variable, i);
        if (readstat_value_type(lo_val) == READSTAT_TYPE_DOUBLE) {
            double lo = readstat_double_value(lo_val);
            double hi = readstat_double_value(hi_val);
            if (lo == hi) {
                readstat_variable_add_missing_double_value(new_variable, lo);
            } else {
                readstat_variable_add_missing_double_range(new_variable, lo, hi);
            }
        }
    }

    readstat_variable_set_alignment(new_variable, alignment);
    readstat_variable_set_measure(new_variable, measure);
    readstat_variable_set_display_width(new_variable, display_width);
    readstat_variable_set_label(new_variable, label);

    return READSTAT_HANDLER_OK;
}

static int handle_value(int obs_index, readstat_variable_t *old_variable, readstat_value_t value, void *ctx) {
    mod_readstat_ctx_t *mod_ctx = (mod_readstat_ctx_t *)ctx;
    readstat_writer_t *writer = mod_ctx->writer;

    int var_index = readstat_variable_get_index(old_variable);
    readstat_variable_t *variable = readstat_get_variable(writer, var_index);
    readstat_type_t type = readstat_value_type(value);
    readstat_error_t error = READSTAT_OK;

    if (var_index == 0) {
        if (obs_index == 0) {
            if (mod_ctx->is_sav) {
                readstat_writer_set_compression(writer, READSTAT_COMPRESS_ROWS);
                error = readstat_begin_writing_sav(writer, mod_ctx, mod_ctx->row_count);
            } else if (mod_ctx->is_zsav) {
                readstat_writer_set_compression(writer, READSTAT_COMPRESS_BINARY);
                error = readstat_begin_writing_sav(writer, mod_ctx, mod_ctx->row_count);
            } else if (mod_ctx->is_dta) {
                error = readstat_begin_writing_dta(writer, mod_ctx, mod_ctx->row_count);
            } else if (mod_ctx->is_por) {
                error = readstat_begin_writing_por(writer, mod_ctx, mod_ctx->row_count);
            } else if (mod_ctx->is_sas7bdat) {
                error = readstat_begin_writing_sas7bdat(writer, mod_ctx, mod_ctx->row_count);
            } else if (mod_ctx->is_xport) {
                error = readstat_begin_writing_xport(writer, mod_ctx, mod_ctx->row_count);
            }
            if (error != READSTAT_OK) {
                fprintf(stderr, "Error beginning file: %s\n", readstat_error_message(error));
                goto cleanup;
            }
        }
        error = readstat_begin_row(writer);
        if (error != READSTAT_OK) {
            fprintf(stderr, "Error beginning row #%d: %s\n", obs_index, readstat_error_message(error));
            goto cleanup;
        }
    }

    if (readstat_value_is_system_missing(value)) {
        error = readstat_insert_missing_value(writer, variable);
    } else if (mod_ctx->is_dta && readstat_value_is_tagged_missing(value)) {
        error = readstat_insert_tagged_missing_value(writer, variable, value.tag);
    } else if (type == READSTAT_TYPE_STRING) {
        error = readstat_insert_string_value(writer, variable, readstat_string_value(value));
    } else if (type == READSTAT_TYPE_INT8) {
        error = readstat_insert_int8_value(writer, variable, readstat_int8_value(value));
    } else if (type == READSTAT_TYPE_INT16) {
        error = readstat_insert_int16_value(writer, variable, readstat_int16_value(value));
    } else if (type == READSTAT_TYPE_INT32) {
        error = readstat_insert_int32_value(writer, variable, readstat_int32_value(value));
    } else if (type == READSTAT_TYPE_FLOAT) {
        error = readstat_insert_float_value(writer, variable, readstat_float_value(value));
    } else if (type == READSTAT_TYPE_DOUBLE) {
        error = readstat_insert_double_value(writer, variable, readstat_double_value(value));
    }
    if (error != READSTAT_OK) {
        fprintf(stderr, "Error inserting value: %s\n", readstat_error_message(error));
        goto cleanup;
    }

    if (var_index == mod_ctx->var_count - 1) {
        error = readstat_end_row(writer);
        if (error != READSTAT_OK) {
            fprintf(stderr, "Error ending row: %s\n", readstat_error_message(error));
            goto cleanup;
        }

        if (obs_index == mod_ctx->row_count - 1) {
            error = readstat_end_writing(writer);
            if (error != READSTAT_OK) {
                fprintf(stderr, "Error ending file: %s\n", readstat_error_message(error));
                goto cleanup;
            }
        }
    }

cleanup:
    if (error != READSTAT_OK) {
        return READSTAT_HANDLER_ABORT;
    }

    return READSTAT_HANDLER_OK;
}
