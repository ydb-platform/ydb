
#include <stdlib.h>
#include "readstat.h"
#include "readstat_io_unistd.h"

readstat_parser_t *readstat_parser_init(void) {
    readstat_parser_t *parser = calloc(1, sizeof(readstat_parser_t));
    parser->io = calloc(1, sizeof(readstat_io_t));
    if (unistd_io_init(parser) != READSTAT_OK) {
        readstat_parser_free(parser);
        return NULL;
    }
    parser->output_encoding = "UTF-8";
    return parser;
}

void readstat_parser_free(readstat_parser_t *parser) {
    if (parser) {
        if (parser->io) {
            readstat_set_io_ctx(parser, NULL);
            free(parser->io);
        }
        free(parser);
    }
}

readstat_error_t readstat_set_metadata_handler(readstat_parser_t *parser, readstat_metadata_handler metadata_handler) {
    parser->handlers.metadata = metadata_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_note_handler(readstat_parser_t *parser, readstat_note_handler note_handler) {
    parser->handlers.note = note_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_variable_handler(readstat_parser_t *parser, readstat_variable_handler variable_handler) {
    parser->handlers.variable = variable_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_value_handler(readstat_parser_t *parser, readstat_value_handler value_handler) {
    parser->handlers.value = value_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_value_label_handler(readstat_parser_t *parser, readstat_value_label_handler label_handler) {
    parser->handlers.value_label = label_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_error_handler(readstat_parser_t *parser, readstat_error_handler error_handler) {
    parser->handlers.error = error_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_progress_handler(readstat_parser_t *parser, readstat_progress_handler progress_handler) {
    parser->handlers.progress = progress_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_fweight_handler(readstat_parser_t *parser, readstat_fweight_handler fweight_handler) {
    parser->handlers.fweight = fweight_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_open_handler(readstat_parser_t *parser, readstat_open_handler open_handler) {
    parser->io->open = open_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_close_handler(readstat_parser_t *parser, readstat_close_handler close_handler) {
    parser->io->close = close_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_seek_handler(readstat_parser_t *parser, readstat_seek_handler seek_handler) {
    parser->io->seek = seek_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_read_handler(readstat_parser_t *parser, readstat_read_handler read_handler) {
    parser->io->read = read_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_update_handler(readstat_parser_t *parser, readstat_update_handler update_handler) {
    parser->io->update = update_handler;
    return READSTAT_OK;
}

readstat_error_t readstat_set_io_ctx(readstat_parser_t *parser, void *io_ctx) {
    if (parser->io->io_ctx_needs_free) {
        free(parser->io->io_ctx);
    }

    parser->io->io_ctx = io_ctx;
    parser->io->io_ctx_needs_free = 0;

    return READSTAT_OK;
}

readstat_error_t readstat_set_file_character_encoding(readstat_parser_t *parser, const char *encoding) {
    parser->input_encoding = encoding;
    return READSTAT_OK;
}

readstat_error_t readstat_set_handler_character_encoding(readstat_parser_t *parser, const char *encoding) {
    parser->output_encoding = encoding;
    return READSTAT_OK;
}

readstat_error_t readstat_set_row_limit(readstat_parser_t *parser, long row_limit) {
    parser->row_limit = row_limit;
    return READSTAT_OK;
}

readstat_error_t readstat_set_row_offset(readstat_parser_t *parser, long row_offset) {
    parser->row_offset = row_offset;
    return READSTAT_OK;
}
