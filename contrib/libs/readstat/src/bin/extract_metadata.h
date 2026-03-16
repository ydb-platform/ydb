#ifndef __EXTRACT_METADATA_H
#define __EXTRACT_METADATA_H

#include "../readstat.h"

typedef struct context {
    int count;
    FILE* fp;
    int variable_count;
    int input_format;
    readstat_label_set_t *label_set;
} context;

typedef enum extract_metadata_type_e {
    EXTRACT_METADATA_TYPE_NUMERIC,
    EXTRACT_METADATA_TYPE_STRING,
    EXTRACT_METADATA_TYPE_UNKNOWN
} extract_metadata_type_t;

typedef enum extract_metadata_format_e {
    EXTRACT_METADATA_FORMAT_NUMBER,
    EXTRACT_METADATA_FORMAT_PERCENT,
    EXTRACT_METADATA_FORMAT_CURRENCY,
    EXTRACT_METADATA_FORMAT_DATE,
    EXTRACT_METADATA_FORMAT_TIME,
    EXTRACT_METADATA_FORMAT_DATE_TIME,
    EXTRACT_METADATA_FORMAT_UNSPECIFIED
} extract_metadata_format_t;

#endif
