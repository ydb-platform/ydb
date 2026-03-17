#include <stdio.h>
#include <stdlib.h>

#include "../../readstat.h"
#include "read_module.h"
#include "csv_metadata.h"
#include "json_metadata.h"
#include "value.h"
#include "../util/file_format.h"

#include "../util/readstat_sav_date.h"

void produce_column_header_sav(void *csv_metadata, const char *column, readstat_variable_t* var);
void produce_value_label_sav(void *csv_metadata, const char* column);
void produce_missingness_sav(void *csv_metadata, const char* column);
void produce_csv_value_sav(void *csv_metadata, const char *s, size_t len);

rs_read_module_t rs_read_mod_sav = {
    .format = RS_FORMAT_SAV,
    .header = &produce_column_header_sav,
    .missingness = &produce_missingness_sav,
    .value_label = &produce_value_label_sav,
    .csv_value = &produce_csv_value_sav
};

static double get_double_date_missing_sav(const char *js, jsmntok_t* missing_value_token) {
    // SAV missing date
    char buf[255];
    char *dest;
    int len = missing_value_token->end - missing_value_token->start;
    snprintf(buf, sizeof(buf), "%.*s", len, js + missing_value_token->start);
    double val = readstat_sav_date_parse(buf, &dest);
    if (buf == dest) {
        fprintf(stderr, "%s:%d failed to parse double: %s\n", __FILE__, __LINE__, buf);
        exit(EXIT_FAILURE);
    } else {
        fprintf(stdout, "added double date missing %s\n", buf);
    }
    return val;
}

static void produce_missingness_discrete_sav(struct csv_metadata *c, jsmntok_t* missing, const char* column) {
    readstat_variable_t* var = &c->variables[c->columns];
    int is_date = c->is_date[c->columns];
    const char *js = c->json_md->js;

    jsmntok_t* values = find_object_property(js, missing, "values");
    if (!values) {
        fprintf(stderr, "%s:%d Expected to find missing 'values' property\n", __FILE__, __LINE__);
        exit(EXIT_FAILURE);
    }

    int j = 1;
    for (int i=0; i<values->size; i++) {
        jsmntok_t* missing_value_token = values + j;
        if (is_date) {
            readstat_variable_add_missing_double_value(var, get_double_date_missing_sav(js, missing_value_token));
        } else if (var->type == READSTAT_TYPE_DOUBLE) {
            readstat_variable_add_missing_double_value(var, get_double_from_token(js, missing_value_token));
        } else if (var->type == READSTAT_TYPE_STRING) {
        } else {
            fprintf(stderr, "%s:%d Unsupported column type %d\n", __FILE__, __LINE__, var->type);
            exit(EXIT_FAILURE);
        }
        j += slurp_object(missing_value_token);
    }
}

static void produce_missingness_range_sav(struct csv_metadata *c, jsmntok_t* missing, const char* column) {
    readstat_variable_t* var = &c->variables[c->columns];
    int is_date = c->is_date[c->columns];
    const char *js = c->json_md->js;

    jsmntok_t* low = find_object_property(js, missing, "low");
    jsmntok_t* high = find_object_property(js, missing, "high");
    jsmntok_t* discrete = find_object_property(js, missing, "discrete-value");

    if (low && !high) {
        fprintf(stderr, "%s:%d missing.low specified for column %s, but missing.high not specified\n", __FILE__, __LINE__, column);
        exit(EXIT_FAILURE);
    }
    if (high && !low) {
        fprintf(stderr, "%s:%d missing.high specified for column %s, but missing.low not specified\n", __FILE__, __LINE__, column);
        exit(EXIT_FAILURE);
    }

    if (low && high) {
        double lo = is_date ? get_double_date_missing_sav(js, low) : get_double_from_token(js, low);
        double hi = is_date ? get_double_date_missing_sav(js, high) :  get_double_from_token(js, high);
        readstat_variable_add_missing_double_range(var, lo, hi);
    }

    if (discrete) {
        double v = is_date ? get_double_date_missing_sav(js, discrete) : get_double_from_token(js, discrete);
        readstat_variable_add_missing_double_value(var, v);
    }
}

void produce_missingness_sav(void *csv_metadata, const char* column) {
    struct csv_metadata *c = (struct csv_metadata *)csv_metadata;
    const char *js = c->json_md->js;
    readstat_variable_t* var = &c->variables[c->columns];
    var->missingness.missing_ranges_count = 0;

    jsmntok_t* missing = find_variable_property(js, c->json_md->tok, column, "missing");
    if (!missing) {
        return;
    }

    jsmntok_t* missing_type = find_object_property(js, missing, "type");
    if (!missing_type) {
        fprintf(stderr, "%s:%d expected to find missing.type for column %s\n", __FILE__, __LINE__, column);
        exit(EXIT_FAILURE);
    }

    if (match_token(js, missing_type, "DISCRETE")) {
        produce_missingness_discrete_sav(c, missing, column);
    } else if (match_token(js, missing_type, "RANGE")) {
        produce_missingness_range_sav(c, missing, column);
    } else {
        fprintf(stderr, "%s:%d unknown missing type %.*s\n", __FILE__, __LINE__, missing_type->end - missing_type->start, js+missing_type->start);
        exit(EXIT_FAILURE);
    }
}

void produce_column_header_sav(void *csv_metadata, const char *column, readstat_variable_t* var) {
    struct csv_metadata *c = (struct csv_metadata *)csv_metadata;
    extract_metadata_type_t coltype = column_type(c->json_md, column, c->output_format);
    if (coltype == EXTRACT_METADATA_TYPE_NUMERIC) {
        extract_metadata_format_t colformat = column_format(c->json_md, column);
        switch (colformat) {
        case EXTRACT_METADATA_FORMAT_NUMBER:
        case EXTRACT_METADATA_FORMAT_PERCENT:
        case EXTRACT_METADATA_FORMAT_CURRENCY:
            var->type = READSTAT_TYPE_DOUBLE;
            snprintf(var->format, sizeof(var->format), "F8.%d", get_decimals(c->json_md, column));
        break;
        case EXTRACT_METADATA_FORMAT_DATE:
        case EXTRACT_METADATA_FORMAT_TIME:
        case EXTRACT_METADATA_FORMAT_DATE_TIME:
            var->type = READSTAT_TYPE_DOUBLE;
            snprintf(var->format, sizeof(var->format), "%s", "EDATE40");
        break;
        default:
            var->type = READSTAT_TYPE_DOUBLE;
            snprintf(var->format, sizeof(var->format), "F8.%d", get_decimals(c->json_md, column));
        }
    } else if (coltype == EXTRACT_METADATA_TYPE_STRING) {
        var->type = READSTAT_TYPE_STRING;
    }
}

static void produce_value_label_double_date_sav(const char* column, struct csv_metadata *c, const char *code, const char *label) {
    char *endptr;
    double v = readstat_sav_date_parse(code, &endptr);
    if (endptr == code) {
        fprintf(stderr, "%s:%d not a valid date: %s\n", __FILE__, __LINE__, code);
        exit(EXIT_FAILURE);
    }
    readstat_value_t value = {
        .v = { .double_value = v },
        .type = READSTAT_TYPE_DOUBLE,
    };
    c->handle.value_label(column, value, label, c->user_ctx);
}

static void produce_value_label_string(const char* column, struct csv_metadata *c, const char *code, const char *label) {
    readstat_value_t value = {
        .v = { .string_value = code },
        .type = READSTAT_TYPE_STRING,
    };
    c->handle.value_label(column, value, label, c->user_ctx);
}

static void produce_value_label_double_sav(const char* column, struct csv_metadata *c, const char *code, const char *label) {
    char *endptr;
    double v = strtod(code, &endptr);
    if (endptr == code) {
        fprintf(stderr, "%s:%d not a number: %s\n", __FILE__, __LINE__, code);
        exit(EXIT_FAILURE);
    }
    readstat_value_t value = {
        .v = { .double_value = v },
        .type = READSTAT_TYPE_DOUBLE,
    };
    c->handle.value_label(column, value, label, c->user_ctx);
}

void produce_value_label_sav(void *csv_metadata, const char* column) {
    struct csv_metadata *c = (struct csv_metadata *)csv_metadata;
    readstat_variable_t* variable = &c->variables[c->columns];
    readstat_type_t coltype = variable->type;
    jsmntok_t* categories = find_variable_property(c->json_md->js, c->json_md->tok, column, "categories");
    if (categories==NULL) {
        return;
    }
    int is_date = c->is_date[c->columns];
    int j = 1;
    char code_buf[1024];
    char label_buf[1024];
    for (int i=0; i<categories->size; i++) {
        jsmntok_t* tok = categories+j;
        char* code = get_object_property(c->json_md->js, tok, "code", code_buf, sizeof(code_buf));
        char* label = get_object_property(c->json_md->js, tok, "label", label_buf, sizeof(label_buf));
        if (!code || !label) {
            fprintf(stderr, "%s:%d bogus JSON metadata input. Missing code/label for column %s\n", __FILE__, __LINE__, column);
            exit(EXIT_FAILURE);
        }
        if (is_date) {
            produce_value_label_double_date_sav(column, c, code, label);
        } else if (coltype == READSTAT_TYPE_DOUBLE) {
            produce_value_label_double_sav(column, c, code, label);
        } else if (coltype == READSTAT_TYPE_STRING) {
            produce_value_label_string(column, c, code, label);
        } else {
            fprintf(stderr, "%s:%d unsupported column type %d for value label %s\n", __FILE__, __LINE__, coltype, column);
            exit(EXIT_FAILURE);
        }
        j += slurp_object(tok);
    }
}

static readstat_value_t value_double_date_sav(const char *s, size_t len, struct csv_metadata *c) {
    char *dest;
    double val = readstat_sav_date_parse(s, &dest);
    if (dest == s) {
        fprintf(stderr, "%s:%d not a valid date: %s\n", __FILE__, __LINE__, (char*)s);
        exit(EXIT_FAILURE);
    }
    readstat_value_t value = {
        .type = READSTAT_TYPE_DOUBLE,
        .v = { .double_value = val }
    };
    return value;
}

void produce_csv_value_sav(void *csv_metadata, const char *s, size_t len) {
    struct csv_metadata *c = (struct csv_metadata *)csv_metadata;
    readstat_variable_t *var = &c->variables[c->columns];
    int is_date = c->is_date[c->columns];
    int obs_index = c->rows - 1; // TODO: ???
    readstat_value_t value;

    if (len == 0) {
        value = value_sysmiss(s, len, c);
    } else if (is_date) {
        value = value_double_date_sav(s, len, c);
    } else if (var->type == READSTAT_TYPE_DOUBLE) {
        value = value_double(s, len, c);
    } else if (var->type == READSTAT_TYPE_STRING) {
        value = value_string(s, len, c);
    } else {
        fprintf(stderr, "%s:%d unsupported variable type %d\n", __FILE__, __LINE__, var->type);
        exit(EXIT_FAILURE);
    }

    c->handle.value(obs_index, var, value, c->user_ctx);
}
