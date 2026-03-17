
#include <stdlib.h>
#include "readstat.h"

static readstat_value_t make_blank_value(void);
static readstat_value_t make_double_value(double dval);

static readstat_value_t make_blank_value(void) {
    readstat_value_t value = { .is_system_missing = 1, .v = { .double_value = NAN }, .type = READSTAT_TYPE_DOUBLE };
    return value;
}

static readstat_value_t make_double_value(double dval) {
    readstat_value_t value = { .v = { .double_value = dval }, .type = READSTAT_TYPE_DOUBLE };
    return value;
}

static readstat_value_t make_string_value(const char *string) {
    readstat_value_t value = { .v = { .string_value = string }, .type = READSTAT_TYPE_STRING };
    return value;
}

const char *readstat_variable_get_name(const readstat_variable_t *variable) {
    if (variable->name[0])
        return variable->name;

    return NULL;
}

const char *readstat_variable_get_label(const readstat_variable_t *variable) {
    if (variable->label[0])
        return variable->label;

    return NULL;
}

const char *readstat_variable_get_format(const readstat_variable_t *variable) {
    if (variable->format[0])
        return variable->format;

    return NULL;
}

readstat_type_t readstat_variable_get_type(const readstat_variable_t *variable) {
    return variable->type;
}

readstat_type_class_t readstat_variable_get_type_class(const readstat_variable_t *variable) {
    return readstat_type_class(variable->type);
}

int readstat_variable_get_index(const readstat_variable_t *variable) {
    return variable->index;
}

int readstat_variable_get_index_after_skipping(const readstat_variable_t *variable) {
    return variable->index_after_skipping;
}

size_t readstat_variable_get_storage_width(const readstat_variable_t *variable) {
    return variable->storage_width;
}

readstat_measure_t readstat_variable_get_measure(const readstat_variable_t *variable) {
    return variable->measure;
}

readstat_alignment_t readstat_variable_get_alignment(const readstat_variable_t *variable) {
    return variable->alignment;
}

int readstat_variable_get_display_width(const readstat_variable_t *variable) {
    return variable->display_width;
}

int readstat_variable_get_missing_ranges_count(const readstat_variable_t *variable) {
    return variable->missingness.missing_ranges_count;
}

readstat_value_t readstat_variable_get_missing_range_lo(const readstat_variable_t *variable, int i) {
    if (i < variable->missingness.missing_ranges_count &&
            2*i+1 < sizeof(variable->missingness.missing_ranges)/sizeof(variable->missingness.missing_ranges[0])) {
        return variable->missingness.missing_ranges[2*i];
    }

    return make_blank_value();
}

readstat_value_t readstat_variable_get_missing_range_hi(const readstat_variable_t *variable, int i) {
    if (i < variable->missingness.missing_ranges_count &&
            2*i+1 < sizeof(variable->missingness.missing_ranges)/sizeof(variable->missingness.missing_ranges[0])) {
        return variable->missingness.missing_ranges[2*i+1];
    }

    return make_blank_value();
}

static readstat_error_t readstat_variable_add_missing_value_range(readstat_variable_t *variable, readstat_value_t lo, readstat_value_t hi) {
    int i = readstat_variable_get_missing_ranges_count(variable);
    if (2*i < sizeof(variable->missingness.missing_ranges)/sizeof(variable->missingness.missing_ranges[0])) {
        variable->missingness.missing_ranges[2*i] = lo;
        variable->missingness.missing_ranges[2*i+1] = hi;
        variable->missingness.missing_ranges_count++;
        return READSTAT_OK;
    }
    return READSTAT_ERROR_TOO_MANY_MISSING_VALUE_DEFINITIONS;
}

readstat_error_t readstat_variable_add_missing_double_range(readstat_variable_t *variable, double lo, double hi) {
    return readstat_variable_add_missing_value_range(variable, make_double_value(lo), make_double_value(hi));
}

readstat_error_t readstat_variable_add_missing_double_value(readstat_variable_t *variable, double value) {
    return readstat_variable_add_missing_value_range(variable, make_double_value(value), make_double_value(value));
}

readstat_error_t readstat_variable_add_missing_string_range(readstat_variable_t *variable, const char *lo, const char *hi) {
    return readstat_variable_add_missing_value_range(variable, make_string_value(lo), make_string_value(hi));
}

readstat_error_t readstat_variable_add_missing_string_value(readstat_variable_t *variable, const char *value) {
    return readstat_variable_add_missing_value_range(variable, make_string_value(value), make_string_value(value));
}
