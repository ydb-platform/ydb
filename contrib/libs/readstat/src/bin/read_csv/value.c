#include <stdio.h>
#include <stdlib.h>

#include "../../readstat.h"
#include "read_module.h"
#include "csv_metadata.h"

readstat_value_t value_sysmiss(const char *s, size_t len, struct csv_metadata *c) {
    readstat_variable_t *var = &c->variables[c->columns];
    readstat_value_t value = {
        .is_system_missing = 1,
        .is_tagged_missing = 0,
        .type = var->type
    };
    return value;
}

readstat_value_t value_string(const char *s, size_t len, struct csv_metadata *c) {
    readstat_value_t value = {
            .is_system_missing = 0,
            .is_tagged_missing = 0,
            .v = { .string_value = s },
            .type = READSTAT_TYPE_STRING
        };
    return value;
}

readstat_value_t value_double(const char *s, size_t len, struct csv_metadata *c) {
    char *dest;
    double val = strtod(s, &dest);
    if (dest == s) {
        fprintf(stderr, "%s:%d not a number: %s\n", __FILE__, __LINE__, (char*)s);
        exit(EXIT_FAILURE);
    }
    readstat_value_t value = {
        .type = READSTAT_TYPE_DOUBLE,
        .v = { .double_value = val }
    };
    return value;
}
