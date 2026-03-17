
#include <stdlib.h>
#include <time.h>
#include "readstat.h"
#include "readstat_writer.h"

#define VARIABLES_INITIAL_CAPACITY    50
#define LABEL_SETS_INITIAL_CAPACITY   50
#define NOTES_INITIAL_CAPACITY        50
#define VALUE_LABELS_INITIAL_CAPACITY 10
#define STRING_REFS_INITIAL_CAPACITY 100
#define LABEL_SET_VARIABLES_INITIAL_CAPACITY 2

static readstat_error_t readstat_write_row_default_callback(void *writer_ctx, void *bytes, size_t len) {
    return readstat_write_bytes((readstat_writer_t *)writer_ctx, bytes, len);
}

static int readstat_compare_string_refs(const void *elem1, const void *elem2) {
    readstat_string_ref_t *ref1 = *(readstat_string_ref_t **)elem1;
    readstat_string_ref_t *ref2 = *(readstat_string_ref_t **)elem2;

    if (ref1->first_o == ref2->first_o)
        return ref1->first_v - ref2->first_v;

    return ref1->first_o - ref2->first_o;
}

readstat_string_ref_t *readstat_string_ref_init(const char *string) {
    size_t len = strlen(string) + 1;
    readstat_string_ref_t *ref = calloc(1, sizeof(readstat_string_ref_t) + len);
    ref->first_o = -1;
    ref->first_v = -1;
    ref->len = len;
    memcpy(&ref->data[0], string, len);
    return ref;
}

readstat_writer_t *readstat_writer_init(void) {
    readstat_writer_t *writer = calloc(1, sizeof(readstat_writer_t));

    writer->variables = calloc(VARIABLES_INITIAL_CAPACITY, sizeof(readstat_variable_t *));
    writer->variables_capacity = VARIABLES_INITIAL_CAPACITY;

    writer->label_sets = calloc(LABEL_SETS_INITIAL_CAPACITY, sizeof(readstat_label_set_t *));
    writer->label_sets_capacity = LABEL_SETS_INITIAL_CAPACITY;

    writer->notes = calloc(NOTES_INITIAL_CAPACITY, sizeof(char *));
    writer->notes_capacity = NOTES_INITIAL_CAPACITY;

    writer->string_refs = calloc(STRING_REFS_INITIAL_CAPACITY, sizeof(readstat_string_ref_t *));
    writer->string_refs_capacity = STRING_REFS_INITIAL_CAPACITY;

    writer->timestamp = time(NULL);
    writer->is_64bit = 1;
    writer->callbacks.write_row = &readstat_write_row_default_callback;

    return writer;
}

static void readstat_variable_free(readstat_variable_t *variable) {
    free(variable);
}

static void readstat_label_set_free(readstat_label_set_t *label_set) {
    int i;
    for (i=0; i<label_set->value_labels_count; i++) {
        readstat_value_label_t *value_label = readstat_get_value_label(label_set, i);
        if (value_label->label)
            free(value_label->label);
        if (value_label->string_key)
            free(value_label->string_key);
    }
    free(label_set->value_labels);
    free(label_set->variables);
    free(label_set);
}

static void readstat_copy_label(readstat_value_label_t *value_label, const char *label) {
    if (label && strlen(label)) {
        value_label->label_len = strlen(label);
        value_label->label = malloc(value_label->label_len);
        memcpy(value_label->label, label, value_label->label_len);
    }
}

static readstat_value_label_t *readstat_add_value_label(readstat_label_set_t *label_set, const char *label) {
    if (label_set->value_labels_count == label_set->value_labels_capacity) {
        label_set->value_labels_capacity *= 2;
        label_set->value_labels = realloc(label_set->value_labels, 
                label_set->value_labels_capacity * sizeof(readstat_value_label_t));
    }
    readstat_value_label_t *new_value_label = &label_set->value_labels[label_set->value_labels_count++];
    memset(new_value_label, 0, sizeof(readstat_value_label_t));
    readstat_copy_label(new_value_label, label);
    return new_value_label;
}

readstat_error_t readstat_validate_variable(readstat_writer_t *writer, const readstat_variable_t *variable) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;

    if (writer->callbacks.variable_ok)
        return writer->callbacks.variable_ok(variable);

    return READSTAT_OK;
}

readstat_error_t readstat_validate_metadata(readstat_writer_t *writer) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;

    if (writer->callbacks.metadata_ok)
        return writer->callbacks.metadata_ok(writer);

    return READSTAT_OK;
}

static readstat_error_t readstat_begin_writing_data(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;

    size_t row_len = 0;
    int i;

    retval = readstat_validate_metadata(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *variable = readstat_get_variable(writer, i);
        variable->storage_width = writer->callbacks.variable_width(variable->type, variable->user_width);
        variable->offset = row_len;
        row_len += variable->storage_width;
    }
    if (writer->callbacks.variable_ok) {
        for (i=0; i<writer->variables_count; i++) {
            retval = readstat_validate_variable(writer, readstat_get_variable(writer, i));
            if (retval != READSTAT_OK)
                goto cleanup;
        }
    }
    writer->row_len = row_len;
    writer->row = malloc(writer->row_len);
    if (writer->callbacks.begin_data) {
        retval = writer->callbacks.begin_data(writer);
    }

cleanup:
    return retval;
}

void readstat_writer_free(readstat_writer_t *writer) {
    int i;
    if (writer) {
        if (writer->callbacks.module_ctx_free && writer->module_ctx) {
            writer->callbacks.module_ctx_free(writer->module_ctx);
        }
        if (writer->variables) {
            for (i=0; i<writer->variables_count; i++) {
                readstat_variable_free(writer->variables[i]);
            }
            free(writer->variables);
        }
        if (writer->label_sets) {
            for (i=0; i<writer->label_sets_count; i++) {
                readstat_label_set_free(writer->label_sets[i]);
            }
            free(writer->label_sets);
        }
        if (writer->notes) {
            for (i=0; i<writer->notes_count; i++) {
                free(writer->notes[i]);
            }
            free(writer->notes);
        }
        if (writer->string_refs) {
            for (i=0; i<writer->string_refs_count; i++) {
                free(writer->string_refs[i]);
            }
            free(writer->string_refs);
        }
        if (writer->row) {
            free(writer->row);
        }
        free(writer);
    }
}

readstat_error_t readstat_set_data_writer(readstat_writer_t *writer, readstat_data_writer data_writer) {
    writer->data_writer = data_writer;
    return READSTAT_OK;
}

readstat_error_t readstat_write_bytes(readstat_writer_t *writer, const void *bytes, size_t len) {
    size_t bytes_written = writer->data_writer(bytes, len, writer->user_ctx);
    if (bytes_written < len) {
        return READSTAT_ERROR_WRITE;
    }
    writer->bytes_written += bytes_written;
    return READSTAT_OK;
}

readstat_error_t readstat_write_bytes_as_lines(readstat_writer_t *writer,
        const void *bytes, size_t len, size_t line_len, const char *line_sep) {
    size_t line_sep_len = strlen(line_sep);
    readstat_error_t retval = READSTAT_OK;
    size_t bytes_written = 0;
    while (bytes_written < len) {
        ssize_t bytes_left_in_line = line_len - (writer->bytes_written % (line_len + line_sep_len));
        if (len - bytes_written < bytes_left_in_line) {
            retval = readstat_write_bytes(writer, ((const char *)bytes) + bytes_written,
                    len - bytes_written);
            bytes_written = len;
        } else {
            retval = readstat_write_bytes(writer, ((const char *)bytes) + bytes_written,
                    bytes_left_in_line);
            bytes_written += bytes_left_in_line;
        }
        if (retval != READSTAT_OK)
            break;

        if (writer->bytes_written % (line_len + line_sep_len) == line_len) {
            if ((retval = readstat_write_bytes(writer, line_sep, line_sep_len)) != READSTAT_OK)
                break;
        }
    }
    return retval;
}

readstat_error_t readstat_write_line_padding(readstat_writer_t *writer, char pad,
        size_t line_len, const char *line_sep) {
    size_t line_sep_len = strlen(line_sep);
    if (writer->bytes_written % (line_len + line_sep_len) == 0)
        return READSTAT_OK;

    readstat_error_t error = READSTAT_OK;
    ssize_t bytes_left_in_line = line_len - (writer->bytes_written % (line_len + line_sep_len));
    char *bytes = malloc(bytes_left_in_line);
    memset(bytes, pad, bytes_left_in_line);

    if ((error = readstat_write_bytes(writer, bytes, bytes_left_in_line)) != READSTAT_OK)
        goto cleanup;

    if ((error = readstat_write_bytes(writer, line_sep, line_sep_len)) != READSTAT_OK)
        goto cleanup;

cleanup:
    if (bytes)
        free(bytes);

    return READSTAT_OK;
}

readstat_error_t readstat_write_string(readstat_writer_t *writer, const char *bytes) {
    return readstat_write_bytes(writer, bytes, strlen(bytes));
}

static readstat_error_t readstat_write_repeated_byte(readstat_writer_t *writer, char byte, size_t len) {
    if (len == 0)
        return READSTAT_OK;

    char *zeros = malloc(len);

    memset(zeros, byte, len);
    readstat_error_t error = readstat_write_bytes(writer, zeros, len);

    free(zeros);
    return error;
}

readstat_error_t readstat_write_zeros(readstat_writer_t *writer, size_t len) {
    return readstat_write_repeated_byte(writer, '\0', len);
}

readstat_error_t readstat_write_spaces(readstat_writer_t *writer, size_t len) {
    return readstat_write_repeated_byte(writer, ' ', len);
}

readstat_error_t readstat_write_space_padded_string(readstat_writer_t *writer, const char *string, size_t max_len) {
    readstat_error_t retval = READSTAT_OK;
    if (string == NULL || string[0] == '\0')
        return readstat_write_spaces(writer, max_len);

    size_t len = strlen(string);
    if (len > max_len)
        len = max_len;

    if ((retval = readstat_write_bytes(writer, string, len)) != READSTAT_OK)
        return retval;

    return readstat_write_spaces(writer, max_len - len);
}

readstat_label_set_t *readstat_add_label_set(readstat_writer_t *writer, readstat_type_t type, const char *name) {
    if (writer->label_sets_count == writer->label_sets_capacity) {
        writer->label_sets_capacity *= 2;
        writer->label_sets = realloc(writer->label_sets, 
                writer->label_sets_capacity * sizeof(readstat_label_set_t *));
    }
    readstat_label_set_t *new_label_set = calloc(1, sizeof(readstat_label_set_t));
    
    writer->label_sets[writer->label_sets_count++] = new_label_set;

    new_label_set->type = type;
    snprintf(new_label_set->name, sizeof(new_label_set->name), "%s", name);

    new_label_set->value_labels = calloc(VALUE_LABELS_INITIAL_CAPACITY, sizeof(readstat_value_label_t));
    new_label_set->value_labels_capacity = VALUE_LABELS_INITIAL_CAPACITY;

    new_label_set->variables = calloc(LABEL_SET_VARIABLES_INITIAL_CAPACITY, sizeof(readstat_variable_t *));
    new_label_set->variables_capacity = LABEL_SET_VARIABLES_INITIAL_CAPACITY;

    return new_label_set;
}

readstat_label_set_t *readstat_get_label_set(readstat_writer_t *writer, int index) {
    if (index < writer->label_sets_count) {
        return writer->label_sets[index];
    }
    return NULL;
}

void readstat_sort_label_set(readstat_label_set_t *label_set,
        int (*compare)(const readstat_value_label_t *, const readstat_value_label_t *)) {
    qsort(label_set->value_labels, label_set->value_labels_count, sizeof(readstat_value_label_t),
            (int (*)(const void *, const void *))compare);
}

readstat_value_label_t *readstat_get_value_label(readstat_label_set_t *label_set, int index) {
    if (index < label_set->value_labels_count) {
        return &label_set->value_labels[index];
    }
    return NULL;
}

readstat_variable_t *readstat_get_label_set_variable(readstat_label_set_t *label_set, int index) {
    if (index < label_set->variables_count) {
        return ((readstat_variable_t **)label_set->variables)[index];
    }
    return NULL;
}

void readstat_label_double_value(readstat_label_set_t *label_set, double value, const char *label) {
    readstat_value_label_t *new_value_label = readstat_add_value_label(label_set, label);
    new_value_label->double_key = value;
    new_value_label->int32_key = value;
}

void readstat_label_int32_value(readstat_label_set_t *label_set, int32_t value, const char *label) {
    readstat_value_label_t *new_value_label = readstat_add_value_label(label_set, label);
    new_value_label->double_key = value;
    new_value_label->int32_key = value;
}

void readstat_label_string_value(readstat_label_set_t *label_set, const char *value, const char *label) {
    readstat_value_label_t *new_value_label = readstat_add_value_label(label_set, label);
    if (value && strlen(value)) {
        new_value_label->string_key_len = strlen(value);
        new_value_label->string_key = malloc(new_value_label->string_key_len);
        memcpy(new_value_label->string_key, value, new_value_label->string_key_len);
    }
}

void readstat_label_tagged_value(readstat_label_set_t *label_set, char tag, const char *label) {
    readstat_value_label_t *new_value_label = readstat_add_value_label(label_set, label);
    new_value_label->tag = tag;
}

readstat_variable_t *readstat_add_variable(readstat_writer_t *writer, const char *name, readstat_type_t type, size_t width) {
    if (writer->variables_count == writer->variables_capacity) {
        writer->variables_capacity *= 2;
        writer->variables = realloc(writer->variables,
                writer->variables_capacity * sizeof(readstat_variable_t *));
    }
    readstat_variable_t *new_variable = calloc(1, sizeof(readstat_variable_t));

    new_variable->index = writer->variables_count++;
    
    writer->variables[new_variable->index] = new_variable;

    new_variable->user_width = width;
    new_variable->type = type;

    if (readstat_variable_get_type_class(new_variable) == READSTAT_TYPE_CLASS_STRING) {
        new_variable->alignment = READSTAT_ALIGNMENT_LEFT;
    } else {
        new_variable->alignment = READSTAT_ALIGNMENT_RIGHT;
    }
    new_variable->measure = READSTAT_MEASURE_UNKNOWN;

    if (name) {
        snprintf(new_variable->name, sizeof(new_variable->name), "%s", name);
    }

    return new_variable;
}

static void readstat_append_string_ref(readstat_writer_t *writer, readstat_string_ref_t *ref) {
    if (writer->string_refs_count == writer->string_refs_capacity) {
        writer->string_refs_capacity *= 2;
        writer->string_refs = realloc(writer->string_refs,
                writer->string_refs_capacity * sizeof(readstat_string_ref_t *));
    }
    writer->string_refs[writer->string_refs_count++] = ref;
}

readstat_string_ref_t *readstat_add_string_ref(readstat_writer_t *writer, const char *string) {
    readstat_string_ref_t *ref = readstat_string_ref_init(string);
    readstat_append_string_ref(writer, ref);
    return ref;
}

void readstat_add_note(readstat_writer_t *writer, const char *note) {
    if (writer->notes_count == writer->notes_capacity) {
        writer->notes_capacity *= 2;
        writer->notes = realloc(writer->notes,
                writer->notes_capacity * sizeof(const char *));
    }
    char *note_copy = malloc(strlen(note) + 1);
    strcpy(note_copy, note);
    writer->notes[writer->notes_count++] = note_copy;
}

void readstat_variable_set_label(readstat_variable_t *variable, const char *label) {
    if (label) {
        snprintf(variable->label, sizeof(variable->label), "%s", label);
    } else {
        memset(variable->label, '\0', sizeof(variable->label));
    }
}

void readstat_variable_set_format(readstat_variable_t *variable, const char *format) {
    if (format) {
        snprintf(variable->format, sizeof(variable->format), "%s", format);
    } else {
        memset(variable->format, '\0', sizeof(variable->format));
    }
}

void readstat_variable_set_measure(readstat_variable_t *variable, readstat_measure_t measure) {
    variable->measure = measure;
}

void readstat_variable_set_alignment(readstat_variable_t *variable, readstat_alignment_t alignment) {
    variable->alignment = alignment;
}

void readstat_variable_set_display_width(readstat_variable_t *variable, int display_width) {
    variable->display_width = display_width;
}

void readstat_variable_set_label_set(readstat_variable_t *variable, readstat_label_set_t *label_set) {
    variable->label_set = label_set;
    if (label_set) {
        if (label_set->variables_count == label_set->variables_capacity) {
            label_set->variables_capacity *= 2;
            label_set->variables = realloc(label_set->variables,
                    label_set->variables_capacity * sizeof(readstat_variable_t *));
        }
        ((readstat_variable_t **)label_set->variables)[label_set->variables_count++] = variable;
    }
}

readstat_variable_t *readstat_get_variable(readstat_writer_t *writer, int index) {
    if (index < writer->variables_count) {
        return writer->variables[index];
    }
    return NULL;
}

readstat_string_ref_t *readstat_get_string_ref(readstat_writer_t *writer, int index) {
    if (index < writer->string_refs_count) {
        return writer->string_refs[index];
    }
    return NULL;
}

readstat_error_t readstat_writer_set_file_label(readstat_writer_t *writer, const char *file_label) {
    snprintf(writer->file_label, sizeof(writer->file_label), "%s", file_label);
    return READSTAT_OK;
}

readstat_error_t readstat_writer_set_file_timestamp(readstat_writer_t *writer, time_t timestamp) {
    writer->timestamp = timestamp;
    return READSTAT_OK;
}

readstat_error_t readstat_writer_set_table_name(readstat_writer_t *writer, const char *table_name) {
    snprintf(writer->table_name, sizeof(writer->table_name), "%s", table_name);
    return READSTAT_OK;
}

readstat_error_t readstat_writer_set_fweight_variable(readstat_writer_t *writer, const readstat_variable_t *variable) {
    if (readstat_variable_get_type_class(variable) == READSTAT_TYPE_CLASS_STRING)
        return READSTAT_ERROR_BAD_FREQUENCY_WEIGHT;

    writer->fweight_variable = variable;
    return READSTAT_OK;
}

readstat_error_t readstat_writer_set_file_format_version(readstat_writer_t *writer, uint8_t version) {
    writer->version = version;
    return READSTAT_OK;
}

readstat_error_t readstat_writer_set_file_format_is_64bit(readstat_writer_t *writer, int is_64bit) {
    writer->is_64bit = is_64bit;
    return READSTAT_OK;
}

readstat_error_t readstat_writer_set_compression(readstat_writer_t *writer, 
        readstat_compress_t compression) {
    writer->compression = compression;
    return READSTAT_OK;
}

readstat_error_t readstat_writer_set_error_handler(readstat_writer_t *writer, 
        readstat_error_handler error_handler) {
    writer->error_handler = error_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_begin_writing_file(readstat_writer_t *writer, void *user_ctx, long row_count) {
    writer->row_count = row_count;
    writer->user_ctx = user_ctx;

    writer->initialized = 1;

    return readstat_validate_metadata(writer);
}

readstat_error_t readstat_begin_row(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;

    if (writer->current_row == 0)
        retval = readstat_begin_writing_data(writer);

    memset(writer->row, '\0', writer->row_len);
    return retval;
}

// Then call one of these for each variable
readstat_error_t readstat_insert_int8_value(readstat_writer_t *writer, const readstat_variable_t *variable, int8_t value) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;
    if (variable->type != READSTAT_TYPE_INT8)
        return READSTAT_ERROR_VALUE_TYPE_MISMATCH;

    return writer->callbacks.write_int8(&writer->row[variable->offset], variable, value);
}

readstat_error_t readstat_insert_int16_value(readstat_writer_t *writer, const readstat_variable_t *variable, int16_t value) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;
    if (variable->type != READSTAT_TYPE_INT16)
        return READSTAT_ERROR_VALUE_TYPE_MISMATCH;

    return writer->callbacks.write_int16(&writer->row[variable->offset], variable, value);
}

readstat_error_t readstat_insert_int32_value(readstat_writer_t *writer, const readstat_variable_t *variable, int32_t value) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;
    if (variable->type != READSTAT_TYPE_INT32)
        return READSTAT_ERROR_VALUE_TYPE_MISMATCH;

    return writer->callbacks.write_int32(&writer->row[variable->offset], variable, value);
}

readstat_error_t readstat_insert_float_value(readstat_writer_t *writer, const readstat_variable_t *variable, float value) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;
    if (variable->type != READSTAT_TYPE_FLOAT)
        return READSTAT_ERROR_VALUE_TYPE_MISMATCH;

    return writer->callbacks.write_float(&writer->row[variable->offset], variable, value);
}

readstat_error_t readstat_insert_double_value(readstat_writer_t *writer, const readstat_variable_t *variable, double value) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;
    if (variable->type != READSTAT_TYPE_DOUBLE)
        return READSTAT_ERROR_VALUE_TYPE_MISMATCH;

    return writer->callbacks.write_double(&writer->row[variable->offset], variable, value);
}

readstat_error_t readstat_insert_string_value(readstat_writer_t *writer, const readstat_variable_t *variable, const char *value) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;
    if (variable->type != READSTAT_TYPE_STRING)
        return READSTAT_ERROR_VALUE_TYPE_MISMATCH;

    return writer->callbacks.write_string(&writer->row[variable->offset], variable, value);
}

readstat_error_t readstat_insert_string_ref(readstat_writer_t *writer, const readstat_variable_t *variable, readstat_string_ref_t *ref) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;
    if (variable->type != READSTAT_TYPE_STRING_REF)
        return READSTAT_ERROR_VALUE_TYPE_MISMATCH;
    if (!writer->callbacks.write_string_ref)
        return READSTAT_ERROR_STRING_REFS_NOT_SUPPORTED;

    if (ref && ref->first_o == -1 && ref->first_v == -1) {
        ref->first_o = writer->current_row + 1;
        ref->first_v = variable->index + 1;
    }

    return writer->callbacks.write_string_ref(&writer->row[variable->offset], variable, ref);
}

readstat_error_t readstat_insert_missing_value(readstat_writer_t *writer, const readstat_variable_t *variable) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;

    if (variable->type == READSTAT_TYPE_STRING) {
        return writer->callbacks.write_missing_string(&writer->row[variable->offset], variable);
    }
    if (variable->type == READSTAT_TYPE_STRING_REF) {
        return readstat_insert_string_ref(writer, variable, NULL);
    }

    return writer->callbacks.write_missing_number(&writer->row[variable->offset], variable);
}

readstat_error_t readstat_insert_tagged_missing_value(readstat_writer_t *writer, const readstat_variable_t *variable, char tag) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;
    if (!writer->callbacks.write_missing_tagged) {
        /* Write out a missing number but return an error */
        writer->callbacks.write_missing_number(&writer->row[variable->offset], variable);
        return READSTAT_ERROR_TAGGED_VALUES_NOT_SUPPORTED;
    }

    return writer->callbacks.write_missing_tagged(&writer->row[variable->offset], variable, tag);
}

readstat_error_t readstat_end_row(readstat_writer_t *writer) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;

    readstat_error_t error = writer->callbacks.write_row(writer, writer->row, writer->row_len);
    if (error == READSTAT_OK)
        writer->current_row++;
    return error;
}

readstat_error_t readstat_end_writing(readstat_writer_t *writer) {
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;

    if (writer->current_row != writer->row_count)
        return READSTAT_ERROR_ROW_COUNT_MISMATCH;

    if (writer->row_count == 0) {
        readstat_error_t retval = readstat_begin_writing_data(writer);
        if (retval != READSTAT_OK)
            return retval;
    }

    /* Sort if out of order */
    int i;
    for (i=1; i<writer->string_refs_count; i++) {
        if (readstat_compare_string_refs(&writer->string_refs[i-1], &writer->string_refs[i]) > 0) {
            qsort(writer->string_refs, writer->string_refs_count, 
                    sizeof(readstat_string_ref_t *), &readstat_compare_string_refs);
            break;
        }
    }

    if (!writer->callbacks.end_data)
        return READSTAT_OK;

    return writer->callbacks.end_data(writer);
}
