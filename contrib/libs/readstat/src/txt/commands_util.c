#include <stdlib.h>

#include "../readstat.h"
#include "readstat_schema.h"
#include "commands_util.h"

readstat_error_t submit_value_label(readstat_parser_t *parser, const char *labelset,
        label_type_t label_type, int64_t first_integer, int64_t last_integer,
        double double_value, const char *string_value, const char *buf, void *user_ctx) {
    if (!parser->handlers.value_label)
        return READSTAT_OK;

    int cb_retval = READSTAT_HANDLER_OK;
    if (label_type == LABEL_TYPE_RANGE) {
        int64_t i;
        for (i=first_integer; i<=last_integer; i++) {
            readstat_value_t value = { 
                .type = READSTAT_TYPE_DOUBLE,
                .v = { .double_value = i } };
            cb_retval = parser->handlers.value_label(labelset, value, buf, user_ctx);
            if (cb_retval != READSTAT_HANDLER_OK)
                goto cleanup;
        }
    } else if (label_type != LABEL_TYPE_OTHER) {
        readstat_value_t value = { { 0 } };
        if (label_type == LABEL_TYPE_DOUBLE) {
            value.type = READSTAT_TYPE_DOUBLE;
            value.v.double_value = double_value;
        } else if (label_type == LABEL_TYPE_STRING) {
            value.type = READSTAT_TYPE_STRING;
            value.v.string_value = string_value;
        } else if (label_type == LABEL_TYPE_NAN) {
            value.type = READSTAT_TYPE_DOUBLE;
            value.v.double_value = NAN;
        }

        cb_retval = parser->handlers.value_label(labelset, value, buf, user_ctx);
    }

cleanup:
    return (cb_retval == READSTAT_HANDLER_OK) ? READSTAT_OK : READSTAT_ERROR_USER_ABORT;
}

readstat_error_t submit_columns(readstat_parser_t *parser, readstat_schema_t *dct, void *user_ctx) {
    int i;
    int partial_entry_count = 0;
    for (i=0; i<dct->entry_count; i++) {
        readstat_schema_entry_t *entry = &dct->entries[i];
        if (dct->rows_per_observation < entry->row + 1) {
            dct->rows_per_observation = entry->row + 1;
        }
    }

    if (!parser->handlers.variable)
        return READSTAT_OK;

    for (i=0; i<dct->entry_count; i++) {
        readstat_schema_entry_t *entry = &dct->entries[i];
        entry->variable.index = i;
        entry->variable.index_after_skipping = partial_entry_count;
        if (entry->variable.type == READSTAT_TYPE_STRING)
            entry->variable.storage_width = entry->len;
        int cb_retval = parser->handlers.variable(i, &entry->variable,
            entry->labelset[0] ? entry->labelset : NULL, user_ctx);
        if (cb_retval == READSTAT_HANDLER_SKIP_VARIABLE) {
            entry->skip = 1;
        } else if (cb_retval == READSTAT_HANDLER_ABORT) {
            return READSTAT_ERROR_USER_ABORT;
        } else {
            partial_entry_count++;
        }
    }
    return READSTAT_OK;
}

