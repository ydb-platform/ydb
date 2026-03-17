#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>

#include "../../../readstat.h"
#include "../../../readstat_iconv.h"
#include "../../../stata/readstat_dta.h"

#include "../../util/readstat_sav_date.h"
#include "../../util/readstat_dta_days.h"
#include "../../util/quote_and_escape.h"
#include "../../util/file_format.h"

#include "../../extract_metadata.h"
#include "write_value_labels.h"

static readstat_label_set_t * get_label_set(const char *val_labels, struct context *ctx) {
    for (int i=0; i<ctx->variable_count; i++) {
        readstat_label_set_t * lbl = &ctx->label_set[i];
        if (0 == strcmp(lbl->name, val_labels)) {
            return lbl;
        }
    }
    return NULL;
}

void add_val_labels(struct context *ctx, readstat_variable_t *variable, const char *val_labels) {
    if (!val_labels) {
        return;
    } else {
        fprintf(stdout, "extracting value labels for %s\n", val_labels);
    }
    const char *format = readstat_variable_get_format(variable);
    int sav_date = format && (strcmp(variable->format, "EDATE40") == 0) && variable->type == READSTAT_TYPE_DOUBLE;
    int dta_date = format && (strcmp(variable->format, "%td") == 0) && variable->type == READSTAT_TYPE_INT32;
    readstat_label_set_t * label_set = get_label_set(val_labels, ctx);
    if (!label_set) {
        fprintf(stderr, "Could not find label set %s!\n", val_labels);
        exit(EXIT_FAILURE);
    }
    fprintf(ctx->fp, ", \"categories\": [");
    for (int i=0; i<label_set->value_labels_count; i++) {
        readstat_value_label_t* value_label = &label_set->value_labels[i];
        if (i>0) {
            fprintf(ctx->fp, ", ");
        }
        if (sav_date) {
            char* lbl = quote_and_escape(value_label->label);
            char buf[255];
            char *s = readstat_sav_date_string(value_label->double_key, buf, sizeof(buf)-1);
            if (!s) {
                fprintf(stderr, "%s:%d could not parse double value %lf to date\n", __FILE__, __LINE__, value_label->double_key);
                exit(EXIT_FAILURE);
            }
            fprintf(ctx->fp, "{ \"code\": \"%s\", \"label\": %s} ", s, lbl);
            free(lbl);
        } else if (dta_date) {
            char* lbl = quote_and_escape(value_label->label);
            char buf[255];
            int k = value_label->int32_key;
            char tag = 0;
            if (k >= DTA_113_MISSING_INT32_A) {
                tag = (k-DTA_113_MISSING_INT32_A)+'a';
            }
            char *s = readstat_dta_days_string(value_label->int32_key, buf, sizeof(buf)-1);
            if (!s) {
                fprintf(stderr, "%s:%d could not parse int32 value %d to date\n", __FILE__, __LINE__, value_label->int32_key);
                exit(EXIT_FAILURE);
            }
            if (tag) {
                fprintf(ctx->fp, "{ \"code\": \".%c\", \"label\": %s} ", tag, lbl);
            } else {
                fprintf(ctx->fp, "{ \"code\": \"%s\", \"label\": %s} ", s, lbl);
            }
            free(lbl);
        } else if (readstat_variable_get_type_class(variable) == READSTAT_TYPE_CLASS_NUMERIC
                && ctx->input_format == RS_FORMAT_DTA) {
            char* lbl = quote_and_escape(value_label->label);
            int k = value_label->int32_key;
            char tag = 0;
            if (k >= DTA_113_MISSING_INT32_A) {
                tag = (k-DTA_113_MISSING_INT32_A)+'a';
            }
            if (tag) {
                fprintf(ctx->fp, "{ \"code\": \".%c\", \"label\": %s} ", tag, lbl);
            } else {
                fprintf(ctx->fp, "{ \"code\": %d, \"label\": %s} ", k, lbl);
            }
            free(lbl);
        } else if (variable->type == READSTAT_TYPE_DOUBLE && ctx->input_format == RS_FORMAT_SAV) {
            char* lbl = quote_and_escape(value_label->label);
            fprintf(ctx->fp, "{ \"code\": %lf, \"label\": %s} ", value_label->double_key, lbl);
            free(lbl);
        } else if (variable->type == READSTAT_TYPE_STRING) {
            char* lbl = quote_and_escape(value_label->label);
            char* stringkey = quote_and_escape(value_label->string_key);
            fprintf(ctx->fp, "{ \"code\": %s, \"label\": %s} ", stringkey, lbl);
            free(lbl);
            free(stringkey);
        } else {
            fprintf(stderr, "%s:%d Unsupported type %d\n", __FILE__, __LINE__, variable->type);
            exit(EXIT_FAILURE);
        }
    }
    fprintf(ctx->fp, "] ");
}
