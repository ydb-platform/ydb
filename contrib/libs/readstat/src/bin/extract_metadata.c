#include "../readstat.h"
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>

#include "util/readstat_sav_date.h"
#include "util/readstat_dta_days.h"
#include "util/quote_and_escape.h"
#include "util/file_format.h"
#include "util/main.h"
#include "extract_metadata.h"
#include "write/json/write_missing_values.h"
#include "write/json/write_value_labels.h"

static const char* extract_metadata_type_str(extract_metadata_type_t t) {
    switch (t) {
     case EXTRACT_METADATA_TYPE_NUMERIC:
        return "NUMERIC";
     case EXTRACT_METADATA_TYPE_STRING:
        return "STRING";
     case EXTRACT_METADATA_TYPE_UNKNOWN:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}

static const char* extract_metadata_format_str(extract_metadata_format_t format) {
    switch (format) {
     case EXTRACT_METADATA_FORMAT_NUMBER:
        return "NUMBER";
     case EXTRACT_METADATA_FORMAT_PERCENT:
        return "PERCENT";
     case EXTRACT_METADATA_FORMAT_CURRENCY:
        return "CURRENCY";
     case EXTRACT_METADATA_FORMAT_DATE:
        return "DATE";
     case EXTRACT_METADATA_FORMAT_TIME:
        return "TIME";
     case EXTRACT_METADATA_FORMAT_DATE_TIME:
        return "DATE_TIME";
     case EXTRACT_METADATA_FORMAT_UNSPECIFIED:
        return "UNSPECIFIED";
    }
    return "UNSPECIFIED";
}

static const char* readstat_type_str(readstat_type_t type) {
    switch (type) {
     case READSTAT_TYPE_STRING:
        return "READSTAT_TYPE_STRING";
     case READSTAT_TYPE_INT8:
        return "READSTAT_TYPE_INT8";
     case READSTAT_TYPE_INT16:
        return "READSTAT_TYPE_INT16";
     case READSTAT_TYPE_INT32:
        return "READSTAT_TYPE_INT32";
     case READSTAT_TYPE_FLOAT:
        return "READSTAT_TYPE_FLOAT";
     case READSTAT_TYPE_DOUBLE:
        return "READSTAT_TYPE_DOUBLE";
     case READSTAT_TYPE_STRING_REF:
        return "READSTAT_TYPE_STRING_REF";
    }
    return "UNKNOWN TYPE";
}

static int extract_decimals(const char *s, char prefix) {
    if (s && s[0] && s[0]==prefix) {
        int decimals;
        if (sscanf(s, "%*c%*d.%d", &decimals) == 1) {
            if (decimals < 0 || decimals > 16) {
                fprintf(stderr, "%s:%d decimals was %d, expected to be [0, 16]\n", __FILE__, __LINE__, decimals);
                exit(EXIT_FAILURE);
            }
            return decimals;
        }
        fprintf(stderr, "%s:%d not a number: %s\n", __FILE__, __LINE__, &s[1]);
        exit(EXIT_FAILURE);
    } else {
        return -1;
    }
}

int hasPrefix(const char *str, char *prefix) {
    return strncmp(str, prefix, sizeof(prefix)-1);
}

static int handle_variable_sav(int index, readstat_variable_t *variable, const char *val_labels, struct context *ctx) {
    extract_metadata_type_t type = EXTRACT_METADATA_TYPE_UNKNOWN;
    extract_metadata_format_t format = EXTRACT_METADATA_FORMAT_UNSPECIFIED;
    char *pattern = "";

    int decimals = -1;
    const char *vformat = readstat_variable_get_format(variable);
    const char *label = readstat_variable_get_label(variable);

    switch (readstat_variable_get_type_class(variable)) {
    case READSTAT_TYPE_CLASS_STRING:
        type = EXTRACT_METADATA_TYPE_STRING;
        break;
    case READSTAT_TYPE_CLASS_NUMERIC:
        type = EXTRACT_METADATA_TYPE_NUMERIC;

        // Extract format
        // SPSS data types: https://libguides.library.kent.edu/SPSS/DatesTime
        // Pattern formats: https://unicode.org/reports/tr35/tr35-dates.html#Date_Format_Patterns
        // TODO: Extract currency
        if (vformat) {
            if (hasPrefix(vformat, "DATE9") == 0) {
                // e.g. 31-JAN-13
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "dd-MMM-yy";
            } else if (hasPrefix(vformat, "DATE11") == 0) {
                // e.g. 31-JAN-13
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "dd-MMM-yyyy";
            } else if (hasPrefix(vformat, "ADATE8") == 0) {
                // e.g. 01/31/13
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "MM/dd/yy";
            } else if (hasPrefix(vformat, "ADATE10") == 0) {
                // e.g. 01/31/2013
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "MM/dd/yyyy";
            } else if (hasPrefix(vformat, "EDATE8") == 0) {
                // e.g. 31.01.13
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "dd.MM.yy";
            } else if (hasPrefix(vformat, "EDATE10") == 0) {
                // e.g. 31.01.2013
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "dd.MM.yyyy";
            } else if (hasPrefix(vformat, "SDATE8") == 0) {
                // e.g. 13/01/31
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "yy/MM/dd";
            } else if (hasPrefix(vformat, "SDATE10") == 0) {
                // e.g. 2013/01/31
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "yyyy/MM/dd";
            } else if (hasPrefix(vformat, "DATETIME17") == 0) {
                // e.g. 31-JAN-2013 01:02
                format = EXTRACT_METADATA_FORMAT_DATE_TIME;
                pattern = "dd-MMM-yyyy hh:mm";
            } else if (hasPrefix(vformat, "DATETIME20") == 0) {
                // e.g. 31-JAN-2013 01:02:33
                format = EXTRACT_METADATA_FORMAT_DATE_TIME;
                pattern = "dd-MMM-yyyy hh:mm:ss";
            } else if (hasPrefix(vformat, "DATETIME23.2") == 0) {
                // e.g. 31-JAN-2013 01:02:33.72
                format = EXTRACT_METADATA_FORMAT_DATE_TIME;
                pattern = "dd-MMM-yyyy hh:mm:ss.SS+";
            } else if (hasPrefix(vformat, "YMDHMS16") == 0) {
                // e.g. 2013-01-31 1:02
                format = EXTRACT_METADATA_FORMAT_DATE_TIME;
                pattern = "yyyy-MM-dd h:mm";
            } else if (hasPrefix(vformat, "YMDHMS19") == 0) {
                // e.g. 2013-01-31 1:02:33
                format = EXTRACT_METADATA_FORMAT_DATE_TIME;
                pattern = "yyyy-MM-dd h:mm:ss";
            } else if (hasPrefix(vformat, "YMDHMS19.2") == 0) {
                // e.g. 2013-01-31 1:02:33.72
                format = EXTRACT_METADATA_FORMAT_DATE_TIME;
                pattern = "yyyy-MM-dd h:mm:ss.SS+";
            } else if (hasPrefix(vformat, "MTIME5") == 0) {
                // e.g. 1754:36
                format = EXTRACT_METADATA_FORMAT_TIME;
                pattern = "[m+]:[s+]";
            } else if (hasPrefix(vformat, "MTIME8.2") == 0) {
                // e.g. 1754:36.58
                format = EXTRACT_METADATA_FORMAT_TIME;
                pattern = "[m+]:[s+]";
            } else if (hasPrefix(vformat, "TIME5") == 0) {
                // e.g. 29:14
                format = EXTRACT_METADATA_FORMAT_TIME;
                pattern = "[h+]:[m+]";
            } else if (hasPrefix(vformat, "TIME8") == 0) {
                // e.g. 29:14:36
                format = EXTRACT_METADATA_FORMAT_TIME;
                pattern = "[h+]:[m+]:[s+]";
            } else if (hasPrefix(vformat, "TIME11.2") == 0) {
                // e.g. 29:14:36.58
                format = EXTRACT_METADATA_FORMAT_TIME;
                pattern = "[h+]:[m+]:[s+]";
            } else if (hasPrefix(vformat, "DTIME9") == 0) {
                // e.g. 1 05:14
                format = EXTRACT_METADATA_FORMAT_TIME;
                pattern = "[d+] [h+]:[m+]";
            } else if (hasPrefix(vformat, "DTIME12") == 0) {
                // e.g. 1 05:14:36
                format = EXTRACT_METADATA_FORMAT_TIME;
                pattern = "[d+] [h+]:[m+]:[s+]";
            } else if (hasPrefix(vformat, "DTIME15.2") == 0) {
                // e.g. 1 05:14:36.58
                format = EXTRACT_METADATA_FORMAT_TIME;
                pattern = "[d+] [h+]:[m+]:[s+]";
            } else if (hasPrefix(vformat, "JDATE5") == 0) {
                // e.g. 13031
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "yyddd";
            } else if (hasPrefix(vformat, "JDATE7") == 0) {
                // e.g. 2013031
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "yyyyddd";
            } else if (hasPrefix(vformat, "QYR6") == 0) {
                // e.g. 1 Q 13
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "Q 'Q' y";
            } else if (hasPrefix(vformat, "QYR8") == 0) {
                // e.g. 1 Q 2013
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "Q 'Q' yyyy";
            } else if (hasPrefix(vformat, "MOYR6") == 0) {
                // e.g. JAN 13
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "mmm yy";
            } else if (hasPrefix(vformat, "MOYR8") == 0) {
                // e.g. JAN 2013
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "mmm yyyy";
            } else if (hasPrefix(vformat, "WKYR8") == 0) {
                // e.g. 5 WK 13
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "w 'WK' yy";
            } else if (hasPrefix(vformat, "WKYR10") == 0) {
                // e.g. 5 WK 2013
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "w 'WK' yyyy";
            } else if (hasPrefix(vformat, "WKDAY3") == 0) {
                // Day of the week, three letter abbreviation (e.g. "Mon").
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "eee";
            } else if (hasPrefix(vformat, "WKDAY9") == 0) {
                // Day of the week, full name. (e.g. "Monday")
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "eeee";
            } else if (hasPrefix(vformat, "MONTH3") == 0) {
                // Three letter month abbreviation (e.g. "Feb").
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "MMM";
            } else if (hasPrefix(vformat, "MONTH9") == 0) {
                // Full month name. (e.g. "February")
                format = EXTRACT_METADATA_FORMAT_DATE;
                pattern = "MMMM";
            } else {
                format = EXTRACT_METADATA_FORMAT_NUMBER;
                decimals = extract_decimals(vformat, 'F');
            }
        } else {
            format = EXTRACT_METADATA_FORMAT_UNSPECIFIED;
        }
        break;
    default:
        fprintf(stderr, "%s:%d unhandled type %s\n", __FILE__, __LINE__, readstat_type_str(variable->type));
        exit(EXIT_FAILURE);
        break;
    }

    if (ctx->count == 0) {
        ctx->count = 1;
        fprintf(ctx->fp, "{\"type\": \"SPSS\",\n  \"variables\": [\n");
    } else {
        fprintf(ctx->fp, ",\n");
    }

    fprintf(ctx->fp, "{\"type\": \"%s\", \"name\": \"%s\"",
        extract_metadata_type_str(type),
        variable->name
    );
    if (type == EXTRACT_METADATA_TYPE_NUMERIC) {
        fprintf(ctx->fp, ", \"format\": \"%s\"", extract_metadata_format_str(format));
        if (pattern && pattern[0]) {
            fprintf(ctx->fp, ", \"pattern\": \"%s\"", pattern);
        }
    }
    if (decimals > 0) {
        fprintf(ctx->fp, ", \"decimals\": %d", decimals);
    }
    if (label) {
        char* quoted_label = quote_and_escape(label);
        fprintf(ctx->fp, ", \"label\": %s", quoted_label);
        free(quoted_label);
    }

    add_val_labels(ctx, variable, val_labels);
    add_missing_values(ctx, variable);

    fprintf(ctx->fp, "}");
    return 0;
}

static int handle_variable_dta(int index, readstat_variable_t *variable, const char *val_labels, struct context *ctx) {
    extract_metadata_type_t type = EXTRACT_METADATA_TYPE_UNKNOWN;
    extract_metadata_format_t format = EXTRACT_METADATA_FORMAT_UNSPECIFIED;
    char *pattern = "";

    const char *vformat = readstat_variable_get_format(variable);
    const char *label = readstat_variable_get_label(variable);
    int decimals = -1;

    switch (readstat_variable_get_type_class(variable)) {
    case READSTAT_TYPE_CLASS_STRING:
        type = EXTRACT_METADATA_TYPE_STRING;
        break;
    case READSTAT_TYPE_CLASS_NUMERIC:
        type = EXTRACT_METADATA_TYPE_NUMERIC;

        // Extract format
        // Pattern formats: https://unicode.org/reports/tr35/tr35-dates.html#Date_Format_Patterns
        if (vformat) {
            if (strcmp(vformat, "%d") == 0) {
                format = EXTRACT_METADATA_FORMAT_DATE;
            } else if (strcmp(vformat, "%td") == 0) {
                format = EXTRACT_METADATA_FORMAT_DATE_TIME;
            }
        } else {
            format = EXTRACT_METADATA_FORMAT_NUMBER;
            decimals = extract_decimals(vformat, '%');
        }
        break;
    default:
        fprintf(stderr, "%s:%d unhandled type %s\n", __FILE__, __LINE__, readstat_type_str(variable->type));
        exit(EXIT_FAILURE);
        break;
    }

    if (ctx->count == 0) {
        ctx->count = 1;
        fprintf(ctx->fp, "{\"type\": \"STATA\",\n  \"variables\": [\n");
    } else {
        fprintf(ctx->fp, ",\n");
    }

    fprintf(ctx->fp, "{\"type\": \"%s\", \"name\": \"%s\"",
        extract_metadata_type_str(type),
        variable->name
    );
    if (type == EXTRACT_METADATA_TYPE_NUMERIC) {
        fprintf(ctx->fp, ", \"format\": \"%s\"", extract_metadata_format_str(format));
        if (pattern && pattern[0]) {
            fprintf(ctx->fp, ", \"pattern\": \"%s\"", pattern);
        }
    }
    if (decimals > 0) {
        fprintf(ctx->fp, ", \"decimals\": %d", decimals);
    }
    if (label) {
        char* quoted_label = quote_and_escape(label);
        fprintf(ctx->fp, ", \"label\": %s", quoted_label);
        free(quoted_label);
    }

    add_val_labels(ctx, variable, val_labels);
    add_missing_values(ctx, variable);
    fprintf(ctx->fp, "}");
    return 0;
}

static int handle_variable(int index, readstat_variable_t *variable, const char *val_labels, void *my_ctx) {
    struct context *ctx = (struct context *)my_ctx;
    if (ctx->input_format == RS_FORMAT_SAV) {
        return handle_variable_sav(index, variable, val_labels, ctx);
    } else if (ctx->input_format == RS_FORMAT_DTA) {
        return handle_variable_dta(index, variable, val_labels, ctx);
    } else {
        fprintf(stderr, "%s:%d unsupported output format %d\n", __FILE__, __LINE__, ctx->input_format);
        exit(EXIT_FAILURE);
    }
}

static readstat_label_set_t * get_or_create_label_set(const char *val_labels, struct context *ctx) {
    for (int i=0; i<ctx->variable_count; i++) {
        readstat_label_set_t * lbl = &ctx->label_set[i];
        if (0 == strcmp(lbl->name, val_labels)) {
            return lbl;
        }
    }
    ctx->variable_count++;
    ctx->label_set = realloc(ctx->label_set, ctx->variable_count*sizeof(readstat_label_set_t));
    if (!ctx->label_set) {
        fprintf(stderr, "%s:%d realloc error: %s\n", __FILE__, __LINE__, strerror(errno));
        return NULL;
    }
    readstat_label_set_t * lbl = &ctx->label_set[ctx->variable_count-1];
    memset(lbl, 0, sizeof(readstat_label_set_t));
    snprintf(lbl->name, sizeof(lbl->name), "%s", val_labels);
    return lbl;
}


static int handle_value_label(const char *val_labels, readstat_value_t value, const char *label, void *c) {
    struct context *ctx = (struct context*)c;
    if (value.type == READSTAT_TYPE_DOUBLE || value.type == READSTAT_TYPE_STRING || value.type == READSTAT_TYPE_INT32) {
        readstat_label_set_t * label_set = get_or_create_label_set(val_labels, ctx);
        if (!label_set) {
            return READSTAT_ERROR_MALLOC;
        }
        long label_idx = label_set->value_labels_count;
        label_set->value_labels = realloc(label_set->value_labels, (1 + label_idx) * sizeof(readstat_value_label_t));
        if (!label_set->value_labels) {
            fprintf(stderr, "%s:%d realloc error: %s\n", __FILE__, __LINE__, strerror(errno));
            return READSTAT_ERROR_MALLOC;
        }
        readstat_value_label_t* value_label = &label_set->value_labels[label_idx];
        memset(value_label, 0, sizeof(readstat_value_label_t));
        if (value.type == READSTAT_TYPE_DOUBLE) {
            value_label->double_key = value.v.double_value;
        } else if (value.type == READSTAT_TYPE_STRING) {
            char *string_key = malloc(strlen(value.v.string_value) + 1);
            strcpy(string_key, value.v.string_value);
            value_label->string_key = string_key;
            value_label->string_key_len = strlen(value.v.string_value);
        } else if (value.type == READSTAT_TYPE_INT32) {
            value_label->int32_key = value.v.i32_value;
        } else {
            fprintf(stderr, "%s:%d unsupported type!\n", __FILE__, __LINE__);
            exit(EXIT_FAILURE);
        }
        char *lbl = malloc(strlen(label) + 1);
        strcpy(lbl, label);
        value_label->label = lbl;
        value_label->label_len = strlen(label);
        label_set->value_labels_count++;
    } else {
        fprintf(stderr, "%s:%d Unhandled value.type %d\n", __FILE__, __LINE__, value.type);
        exit(EXIT_FAILURE);
    }
    return READSTAT_OK;
}

int pass(struct context *ctx, char *input, char *output, int pass) {
    if (pass==2) {
        FILE* fp = fopen(output, "w");
        if (fp == NULL) {
            fprintf(stderr, "Could not open %s for writing: %s\n", output, strerror(errno));
            exit(EXIT_FAILURE);
        }
        ctx->fp = fp;
    } else {
        ctx->fp = NULL;
    }

    int ret = 0;

    readstat_error_t error = READSTAT_OK;
    readstat_parser_t *parser = readstat_parser_init();
    if (pass == 1) {
        readstat_set_value_label_handler(parser, &handle_value_label);
    } else if (pass == 2) {
        readstat_set_variable_handler(parser, &handle_variable);
    }

    const char *filename = input;
    size_t len = strlen(filename);

    if (len < sizeof(".dta") -1) {
        fprintf(stderr, "Unknown input format\n");
        ret = 1;
        goto cleanup;
    }

    if (strncmp(filename + len - 4, ".sav", 4) == 0) {
        fprintf(stdout, "parsing sav file\n");
        error = readstat_parse_sav(parser, input, ctx);
    } else if (strncmp(filename + len - 4, ".dta", 4) == 0) {
        fprintf(stdout, "parsing dta file\n");
        error = readstat_parse_dta(parser, input, ctx);
    } else {
        fprintf(stderr, "Unsupported input format\n");
        ret = 1;
        goto cleanup;
    }

    if (error != READSTAT_OK) {
        fprintf(stderr, "Error processing %s: %s (%d)\n", input, readstat_error_message(error), error);
        ret = 1;
    } else {
        if (ctx->fp) {
            fprintf(ctx->fp, "]}\n");
            fprintf(ctx->fp, "\n");
        }
    }

cleanup: readstat_parser_free(parser);

    if (ctx->fp) {
        fclose(ctx->fp);
    }
    if (pass==2 && ctx->variable_count >=1) {
        for (int i=0; i<ctx->variable_count; i++) {
            readstat_label_set_t * label_set = &ctx->label_set[i];
            for (int j=0; j<label_set->value_labels_count; j++) {
                readstat_value_label_t* value_label = &label_set->value_labels[j];
                if (value_label->string_key) {
                    free(value_label->string_key);
                }
                if (value_label->label) {
                    free(value_label->label);
                }
            }
            free(label_set->value_labels);
        }
        free(ctx->label_set);
    }

    fprintf(stdout, "pass %d done\n", pass);
    return ret;
}

int portable_main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("Usage: %s <input-filename.(dta|sav)> <output-metadata.json>\n", argv[0]);
        return 1;
    }
    int ret = 0;
    struct context ctx;
    memset(&ctx, 0, sizeof(struct context));
    ctx.input_format = readstat_format(argv[1]);

    ret = pass(&ctx, argv[1], argv[2], 1);
    if (!ret) {
        ret = pass(&ctx, argv[1], argv[2], 2);
    }
    printf("extract_metadata exiting\n");
    return ret;
}
