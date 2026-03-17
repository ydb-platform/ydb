#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>

#include "../readstat.h"
#include "../readstat_iconv.h"
#include "../readstat_convert.h"
#include "readstat_schema.h"

#if defined _MSC_VER
#define restrict __restrict
#endif

typedef struct txt_ctx_s {
    int                rows;
    iconv_t            converter;
    readstat_schema_t *schema;
} txt_ctx_t;

static readstat_error_t handle_value(readstat_parser_t *parser, iconv_t converter,
        int obs_index, readstat_schema_entry_t *entry, char *bytes, size_t len, void *ctx) {
    readstat_error_t error = READSTAT_OK;
    char *converted_value = NULL;
    readstat_variable_t *variable = &entry->variable;
    readstat_value_t value = { .type = variable->type };
    if (readstat_type_class(variable->type) == READSTAT_TYPE_CLASS_STRING) {
        converted_value = malloc(4*len+1);
        error = readstat_convert(converted_value, 4 * len + 1, bytes, len, converter);
        if (error != READSTAT_OK)
            goto cleanup;
        value.v.string_value = converted_value;
    } else {
        char *endptr = NULL;
        if (variable->type == READSTAT_TYPE_DOUBLE) {
            value.v.double_value = strtod(bytes, &endptr);
        } else if (variable->type == READSTAT_TYPE_FLOAT) {
            value.v.float_value = strtof(bytes, &endptr);
        } else {
            value.v.i32_value = strtol(bytes, &endptr, 10);
            value.type = READSTAT_TYPE_INT32;
        }
        value.is_system_missing = (endptr == bytes);
    }
    if (parser->handlers.value(obs_index, variable, value, ctx) == READSTAT_HANDLER_ABORT) {
        error = READSTAT_ERROR_USER_ABORT;
    }
cleanup:
    free(converted_value);
    return error;
}

static ssize_t txt_getdelim(char ** restrict linep, size_t * restrict linecapp,
        int delimiter, readstat_io_t *io) {
    char *value_buffer = *linep;
    size_t value_buffer_len = *linecapp;
    ssize_t i = 0;
    ssize_t bytes_read = 0;
    while ((bytes_read = io->read(&value_buffer[i], 1, io->io_ctx)) == 1 && value_buffer[i++] != delimiter) {
        if (i == value_buffer_len) {
            value_buffer = realloc(value_buffer, value_buffer_len *= 2);
        }
    }
    *linep = value_buffer;
    *linecapp = value_buffer_len;

    if (bytes_read == -1)
        return -1;

    return i;
}

static readstat_error_t txt_parse_delimited(readstat_parser_t *parser,
        txt_ctx_t *ctx, void *user_ctx) {
    size_t value_buffer_len = 4096;
    char *value_buffer = malloc(value_buffer_len);
    readstat_schema_t *schema = ctx->schema;
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = parser->io;
    int k=0;
    
    while (1) {
        for (int j=0; j<schema->entry_count; j++) {
            readstat_schema_entry_t *entry = &schema->entries[j];
            int delimiter = (j == schema->entry_count-1) ? '\n' : schema->field_delimiter;
            ssize_t chars_read = txt_getdelim(&value_buffer, &value_buffer_len, delimiter, io);
            if (chars_read == 0)
                goto cleanup;

            if (chars_read == -1) {
                retval = READSTAT_ERROR_READ;
                goto cleanup;
            }
            if (parser->handlers.value && !entry->skip) {
                chars_read--; // delimiter
                if (chars_read > 0 && value_buffer[chars_read-1] == '\r') {
                    chars_read--; // CRLF
                }
                value_buffer[chars_read] = '\0';

                retval = handle_value(parser, ctx->converter, k, entry, value_buffer, chars_read, user_ctx);
                if (retval != READSTAT_OK)
                    goto cleanup;
            }
        }
        if (++k == parser->row_limit)
            break;
    }

cleanup:
    ctx->rows = k;

    if (value_buffer)
        free(value_buffer);
    
    return retval;
}

static readstat_error_t txt_parse_fixed_width(readstat_parser_t *parser,
        txt_ctx_t *ctx, void *user_ctx, const size_t *line_lens, char *line_buffer) {
    char   value_buffer[4096];
    readstat_schema_t *schema = ctx->schema;
    readstat_io_t *io = parser->io;
    readstat_error_t retval = READSTAT_OK;
    int k=0;
    while (1) {
        int j=0;
        for (int i=0; i<schema->rows_per_observation; i++) {
            ssize_t bytes_read = io->read(line_buffer, line_lens[i], io->io_ctx);
            if (bytes_read == 0)
                goto cleanup;
            
            if (bytes_read < line_lens[i]) {
                retval = READSTAT_ERROR_READ;
                goto cleanup;
            }
            for (; j<schema->entry_count && schema->entries[j].row == i; j++) {
                readstat_schema_entry_t *entry = &schema->entries[j];
                size_t field_len = schema->entries[j].len;
                size_t field_offset = schema->entries[j].col;
                if (field_len < sizeof(value_buffer) && parser->handlers.value && !entry->skip) {
                    memcpy(value_buffer, &line_buffer[field_offset], field_len);
                    value_buffer[field_len] = '\0';
                    retval = handle_value(parser, ctx->converter, k, entry, value_buffer, field_len, user_ctx);
                    if (retval != READSTAT_OK) {
                        goto cleanup;
                    }
                }
            }

            if (schema->cols_per_observation == 0) {
                char throwaway = '\0';
                while (io->read(&throwaway, 1, io->io_ctx) == 1 && throwaway != '\n');
            }
        }
        
        if (++k == parser->row_limit)
            break;
    }

cleanup:
    ctx->rows = k;
    return retval;
}

readstat_error_t readstat_parse_txt(readstat_parser_t *parser, const char *filename, 
        readstat_schema_t *schema, void *user_ctx) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = parser->io;
    int i;
        
    size_t *line_lens = NULL;
    
    size_t line_buffer_len = 0;
    char  *line_buffer = NULL;
    txt_ctx_t ctx = { .schema = schema };

    if (parser->output_encoding && parser->input_encoding) {
        ctx.converter = iconv_open(parser->output_encoding, parser->input_encoding);
        if (ctx.converter == (iconv_t)-1) {
            ctx.converter = NULL;
            retval = READSTAT_ERROR_UNSUPPORTED_CHARSET;
            goto cleanup;
        }
    }
    
    if (io->open(filename, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_OPEN;
        goto cleanup;
    }
    
    if ((line_lens = malloc(schema->rows_per_observation * sizeof(size_t))) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }
    
    for (i=0; i<schema->rows_per_observation; i++) {
        line_lens[i] = schema->cols_per_observation;
    }
    
    for (i=0; i<schema->entry_count; i++) {
        readstat_schema_entry_t *entry = &schema->entries[i];
        if (line_lens[entry->row] < entry->col + entry->len)
            line_lens[entry->row] = entry->col + entry->len;
    }
    
    for (i=0; i<schema->rows_per_observation; i++) {
        if (line_buffer_len < line_lens[i])
            line_buffer_len = line_lens[i];
    }
    
    line_buffer_len += 2; /* CRLF */
    
    if ((line_buffer = malloc(line_buffer_len)) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }
    
    if (schema->first_line > 1) {
        int throwaway_lines = schema->first_line - 1;
        char throwaway_char = '\0';
        
        while (throwaway_lines--) {
            while (io->read(&throwaway_char, 1, io->io_ctx) == 1 && throwaway_char != '\n');
        }
    }
    
    if (schema->field_delimiter) {
        retval = txt_parse_delimited(parser, &ctx, user_ctx);
    } else {
        retval = txt_parse_fixed_width(parser, &ctx, user_ctx, line_lens, line_buffer);
    }

    if (retval != READSTAT_OK)
        goto cleanup;

    if (parser->handlers.metadata) {
        readstat_metadata_t metadata = {
            .row_count = ctx.rows,
            .var_count = schema->entry_count
        };
        int cb_retval = parser->handlers.metadata(&metadata, user_ctx);
        if (cb_retval == READSTAT_HANDLER_ABORT)
            retval = READSTAT_ERROR_USER_ABORT;
    }

cleanup:
    
    io->close(io->io_ctx);
    if (line_buffer)
        free(line_buffer);
    if (line_lens)
        free(line_lens);
    if (ctx.converter)
        iconv_close(ctx.converter);
    
    return retval;
}

