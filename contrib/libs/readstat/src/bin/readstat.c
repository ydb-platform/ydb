#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#if !defined _MSC_VER
#   include <unistd.h>
#   include <sys/time.h>
#else
#   include <sys/timeb.h>
#   include <sys/types.h>
#   include <winsock2.h>

#   define __need_clock_t
#   include <time.h>

int gettimeofday(struct timeval* t, void* timezone)
{
    struct _timeb timebuffer;
    _ftime_s(&timebuffer);
    t->tv_sec = timebuffer.time;
    t->tv_usec = 1000 * timebuffer.millitm;
    return 0;
}
#endif
#include <sys/stat.h>

#include "../readstat.h"
#include "../txt/readstat_schema.h"

#include "write/module.h"
#include "write/mod_readstat.h"
#include "write/mod_csv.h"

#if HAVE_CSVREADER
#include "read_csv/json_metadata.h"
#include "read_csv/read_module.h"
#include "read_csv/csv_metadata.h"
#error #include "read_csv/read_csv.h"
#endif

#if HAVE_XLSXWRITER
#error #include "write/mod_xlsx.h"
#endif

#include "util/file_format.h"
#include "util/main.h"

#if defined _MSC_VER
#define unlink _unlink
#endif

typedef struct rs_ctx_s {
    rs_module_t *module;
    void        *module_ctx;
    const char  *error_filename;
    long         row_count;
    long         var_count;
} rs_ctx_t;

rs_module_t *rs_module_for_filename(rs_module_t *modules, long module_count, const char *filename) {
    int i;
    for (i=0; i<module_count; i++) {
        rs_module_t mod = modules[i];
        if (mod.accept(filename))
            return &modules[i];
    }
    return NULL;
}

int can_write(rs_module_t *modules, long modules_count, char *filename) {
    return (rs_module_for_filename(modules, modules_count, filename) != NULL);
}

static void handle_error(const char *msg, void *ctx) {
    fprintf(stderr, "%s\n", msg);
}

static int handle_fweight(readstat_variable_t *variable, void *ctx) {
    rs_ctx_t *rs_ctx = (rs_ctx_t *)ctx;
    if (rs_ctx->module->handle.fweight) {
        return rs_ctx->module->handle.fweight(variable, rs_ctx->module_ctx);
    }
    return READSTAT_HANDLER_OK;
}

static int handle_metadata(readstat_metadata_t *metadata, void *ctx) {
    rs_ctx_t *rs_ctx = (rs_ctx_t *)ctx;
    if (rs_ctx->module->handle.metadata) {
        return rs_ctx->module->handle.metadata(metadata, rs_ctx->module_ctx);
    }
    return READSTAT_HANDLER_OK;
}

static int handle_note(int note_index, const char *note, void *ctx) {
    rs_ctx_t *rs_ctx = (rs_ctx_t *)ctx;
    if (rs_ctx->module->handle.note) {
        return rs_ctx->module->handle.note(note_index, note, rs_ctx->module_ctx);
    }
    return READSTAT_HANDLER_OK;
}

static int handle_value_label(const char *val_labels, readstat_value_t value,
                              const char *label, void *ctx) {
    rs_ctx_t *rs_ctx = (rs_ctx_t *)ctx;
    if (rs_ctx->module->handle.value_label) {
        return rs_ctx->module->handle.value_label(val_labels, value, label, rs_ctx->module_ctx);
    }
    return READSTAT_HANDLER_OK;
}

static int handle_variable(int index, readstat_variable_t *variable,
                           const char *val_labels, void *ctx) {
    rs_ctx_t *rs_ctx = (rs_ctx_t *)ctx;
    if (rs_ctx->module->handle.variable) {
        return rs_ctx->module->handle.variable(index, variable, val_labels, rs_ctx->module_ctx);
    }
    return READSTAT_HANDLER_OK;
}

static int handle_value(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx) {
    rs_ctx_t *rs_ctx = (rs_ctx_t *)ctx;
    int var_index = readstat_variable_get_index(variable);
    if (var_index == 0) {
        rs_ctx->row_count++;
    }
    if (obs_index == 0) {
        rs_ctx->var_count++;
    }
    if (rs_ctx->module->handle.value) {
        return rs_ctx->module->handle.value(obs_index, variable, value, rs_ctx->module_ctx);
    }
    return READSTAT_HANDLER_OK;
}

readstat_error_t parse_file(readstat_parser_t *parser, const char *input_filename, int input_format, void *ctx) {
    readstat_error_t error = READSTAT_OK;

    if (input_format == RS_FORMAT_DTA) {
        error = readstat_parse_dta(parser, input_filename, ctx);
    } else if (input_format == RS_FORMAT_SAV ||
            input_format == RS_FORMAT_ZSAV) {
        error = readstat_parse_sav(parser, input_filename, ctx);
    } else if (input_format == RS_FORMAT_POR) {
        error = readstat_parse_por(parser, input_filename, ctx);
    } else if (input_format == RS_FORMAT_SAS_DATA) {
        error = readstat_parse_sas7bdat(parser, input_filename, ctx);
    } else if (input_format == RS_FORMAT_SAS_CATALOG) {
        error = readstat_parse_sas7bcat(parser, input_filename, ctx);
    } else if (input_format == RS_FORMAT_XPORT) {
        error = readstat_parse_xport(parser, input_filename, ctx);
    }

    return error;
}

static void print_version(void) {
    fprintf(stdout, "ReadStat version " READSTAT_VERSION "\n");
}

#if HAVE_ZLIB
#define INPUT_FORMATS "dta|por|sav|sas7bdat|xpt|zsav"
#else
#define INPUT_FORMATS "dta|por|sav|sas7bdat|xpt"
#endif

#if HAVE_XLSXWRITER
#define OUTPUT_FORMATS INPUT_FORMATS "|csv|xlsx"
#else
#define OUTPUT_FORMATS INPUT_FORMATS "|csv"
#endif

static void print_usage(const char *cmd) {
    print_version();

    fprintf(stdout, "\n  View a file's metadata:\n");
    fprintf(stdout, "\n     %s input.(" INPUT_FORMATS ")\n", cmd);

    fprintf(stdout, "\n  Read a file, and write CSV to standard out:\n");
    fprintf(stdout, "\n     %s input.(" INPUT_FORMATS ") -\n", cmd);

    fprintf(stdout, "\n  Convert a file:\n");
    fprintf(stdout, "\n     %s input.(" INPUT_FORMATS ") output.(" OUTPUT_FORMATS ")\n", cmd);

#if HAVE_CSVREADER
    fprintf(stdout, "\n  Convert a CSV file with column metadata stored in a separate JSON file (see extract_metadata):\n");
    fprintf(stdout, "\n     %s input.csv metadata.json output.(" OUTPUT_FORMATS ")\n", cmd);
#endif

    fprintf(stdout, "\n  Convert a text file with column metadata stored in a SAS command files, SPSS command file, or Stata dictionary file:\n");
    fprintf(stdout, "\n     %s input.xxx metadata.(dct|sas|sps) output.(" OUTPUT_FORMATS ")\n", cmd);

    fprintf(stdout, "\n  Convert a SAS7BDAT file with value labels stored in a separate SAS catalog file:\n");
    fprintf(stdout, "\n     %s input.sas7bdat catalog.sas7bcat output.(dta|por|sav|xpt"
#if HAVE_ZLIB
            "|zsav"
#endif
            "|csv"
#if HAVE_XLSXWRITER
            "|xlsx"
#endif
            ")\n\n", cmd);
}

#if HAVE_CSVREADER
static readstat_error_t parse_csv_plus_json(const char *input_filename,
        const char *json_filename, int output_format, rs_ctx_t *rs_ctx) {
    readstat_error_t error = READSTAT_OK;
    struct csv_metadata csv_meta = { .output_format = output_format };
    struct json_metadata *json_md = NULL;
    readstat_parser_t *pass1_parser = NULL;
    readstat_parser_t *pass2_parser = NULL;

    json_md = get_json_metadata(json_filename);
    if (json_md == NULL) {
        rs_ctx->error_filename = json_filename;
        error = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    csv_meta.json_md = json_md;

    rs_ctx->error_filename = input_filename;

    // The two passes are necessary because we need to set the variable storage
    // width and # rows before passing the actual values to the write API
    pass1_parser = readstat_parser_init();
    readstat_set_error_handler(pass1_parser, &handle_error);
    readstat_set_value_label_handler(pass1_parser, &handle_value_label);
    readstat_set_metadata_handler(pass1_parser, &handle_metadata);
    error = readstat_parse_csv(pass1_parser, input_filename, &csv_meta, rs_ctx);
    if (error != READSTAT_OK)
        goto cleanup;

    pass2_parser = readstat_parser_init();
    readstat_set_error_handler(pass2_parser, &handle_error);
    readstat_set_variable_handler(pass2_parser, &handle_variable);
    readstat_set_value_handler(pass2_parser, &handle_value);
    error = readstat_parse_csv(pass2_parser, input_filename, &csv_meta, rs_ctx);
    if (error != READSTAT_OK)
        goto cleanup;

cleanup:
    if (json_md)
        free_json_metadata(json_md);
    if (pass1_parser)
        readstat_parser_free(pass1_parser);
    if (pass2_parser)
        readstat_parser_free(pass2_parser);
    if (csv_meta.column_width)
        free(csv_meta.column_width);

    return error;
}
#endif

static readstat_error_t parse_text_plus_dct(const char *input_filename,
        const char *dct_filename, rs_ctx_t *rs_ctx) {
    rs_format_e dct_format = readstat_format(dct_filename);
    readstat_error_t error = READSTAT_OK;
    readstat_schema_t *schema = NULL;
    readstat_parser_t *parser = NULL;

    parser = readstat_parser_init();
    readstat_set_error_handler(parser, &handle_error);
    readstat_set_value_label_handler(parser, &handle_value_label);
    readstat_set_variable_handler(parser, &handle_variable);
    if (dct_format == RS_FORMAT_STATA_DICTIONARY) {
        schema = readstat_parse_stata_dictionary(parser, dct_filename, rs_ctx, &error);
    } else if (dct_format == RS_FORMAT_SAS_COMMANDS) {
        schema = readstat_parse_sas_commands(parser, dct_filename, rs_ctx, &error);
    } else if (dct_format == RS_FORMAT_SPSS_COMMANDS) {
        schema = readstat_parse_spss_commands(parser, dct_filename, rs_ctx, &error);
    }
    rs_ctx->error_filename = dct_filename;
    readstat_parser_free(parser);

    if (schema == NULL)
        goto cleanup;

    rs_ctx->error_filename = input_filename;

    parser = readstat_parser_init();
    readstat_set_error_handler(parser, &handle_error);
    readstat_set_metadata_handler(parser, &handle_metadata);
    error = readstat_parse_txt(parser, input_filename, schema, rs_ctx);
    readstat_parser_free(parser);
    if (error != READSTAT_OK)
        goto cleanup;

    parser = readstat_parser_init();
    readstat_set_error_handler(parser, &handle_error);
    readstat_set_value_handler(parser, &handle_value);
    error = readstat_parse_txt(parser, input_filename, schema, rs_ctx);
    readstat_parser_free(parser);
    if (error != READSTAT_OK)
        goto cleanup;

cleanup:
    if (schema)
        readstat_schema_free(schema);

    return error;
}

static readstat_error_t parse_binary_file(const char *input_filename,
        const char *catalog_filename, rs_ctx_t *rs_ctx) {
    readstat_error_t error = READSTAT_OK;
    rs_format_e input_format = readstat_format(input_filename);
    readstat_parser_t *pass1_parser = readstat_parser_init();
    readstat_parser_t *pass2_parser = readstat_parser_init();

    // Pass 1 - Collect fweight and value labels
    readstat_set_error_handler(pass1_parser, &handle_error);
    readstat_set_value_label_handler(pass1_parser, &handle_value_label);
    readstat_set_fweight_handler(pass1_parser, &handle_fweight);

    if (catalog_filename) {
        error = parse_file(pass1_parser, catalog_filename, RS_FORMAT_SAS_CATALOG, rs_ctx);
        rs_ctx->error_filename = catalog_filename;
    } else {
        error = parse_file(pass1_parser, input_filename, input_format, rs_ctx);
        rs_ctx->error_filename = input_filename;
    }
    if (error != READSTAT_OK)
        goto cleanup;

    // Pass 2 - Parse full file
    readstat_set_error_handler(pass2_parser, &handle_error);
    readstat_set_metadata_handler(pass2_parser, &handle_metadata);
    readstat_set_note_handler(pass2_parser, &handle_note);
    readstat_set_variable_handler(pass2_parser, &handle_variable);
    readstat_set_value_handler(pass2_parser, &handle_value);

    error = parse_file(pass2_parser, input_filename, input_format, rs_ctx);
    rs_ctx->error_filename = input_filename;
    if (error != READSTAT_OK)
        goto cleanup;

cleanup:
    if (pass1_parser)
        readstat_parser_free(pass1_parser);
    if (pass2_parser)
        readstat_parser_free(pass2_parser);

    return error;
}

static int convert_file(const char *input_filename, const char *catalog_filename, const char *output_filename,
        rs_module_t *modules, int modules_count, int force) {
    readstat_error_t error = READSTAT_OK;
    struct timeval start_time, end_time;
    rs_module_t *module = rs_module_for_filename(modules, modules_count, output_filename);
    rs_ctx_t *rs_ctx = calloc(1, sizeof(rs_ctx_t));
    void *module_ctx = NULL;
    int file_exists = 0;
    struct stat filestat;

    gettimeofday(&start_time, NULL);

    if (!force && stat(output_filename, &filestat) == 0) {
        error = READSTAT_ERROR_OPEN;
        file_exists = 1;
        goto cleanup;
    }
    
    module_ctx = module->init(output_filename);

    if (module_ctx == NULL) {
        error = READSTAT_ERROR_OPEN;
        rs_ctx->error_filename = output_filename;
        goto cleanup;
    }

    rs_ctx->module = module;
    rs_ctx->module_ctx = module_ctx;

    if (is_json(catalog_filename)) {
#if HAVE_CSVREADER
        error = parse_csv_plus_json(input_filename, catalog_filename, readstat_format(output_filename), rs_ctx);
#endif
    } else if (is_dictionary(catalog_filename)) {
        error = parse_text_plus_dct(input_filename, catalog_filename, rs_ctx);
    } else {
        error = parse_binary_file(input_filename, catalog_filename, rs_ctx);
    }

    gettimeofday(&end_time, NULL);

    fprintf(stderr, "Converted %ld variables and %ld rows in %.2lf seconds\n",
            rs_ctx->var_count, rs_ctx->row_count, 
            (end_time.tv_sec + 1e-6 * end_time.tv_usec) -
            (start_time.tv_sec + 1e-6 * start_time.tv_usec));

cleanup:
    if (module->finish) {
        module->finish(rs_ctx->module_ctx);
    }

    if (error != READSTAT_OK) {
        if (file_exists) {
            fprintf(stderr, "Error opening %s: File exists (Use -f to overwrite)\n", output_filename);
        } else {
            fprintf(stderr, "Error processing %s: %s\n", rs_ctx->error_filename, readstat_error_message(error));
            unlink(output_filename);
        }

	free(rs_ctx);

        return 1;
    }

    free(rs_ctx);

    return 0;
}

size_t readstat_strftime(char *s, size_t maxsize, const char *format, time_t timestamp) {
#if !defined _MSC_VER
    return strftime(s, maxsize, format, localtime(&timestamp));
#else
    struct tm ltm;
    localtime_s(&ltm, &timestamp);
    return strftime(s, maxsize, format, &ltm);
#endif
}

static int dump_metadata(readstat_metadata_t *metadata, void *ctx) {
    printf("Columns: %d\n", readstat_get_var_count(metadata));
    printf("Rows: %d\n", readstat_get_row_count(metadata));
    const char *table_name = readstat_get_table_name(metadata);
    const char *file_label = readstat_get_file_label(metadata);
    const char *orig_encoding = readstat_get_file_encoding(metadata);
    long version = readstat_get_file_format_version(metadata);
    time_t timestamp = readstat_get_creation_time(metadata);
    readstat_compress_t compression = readstat_get_compression(metadata);
    readstat_endian_t endianness = readstat_get_endianness(metadata);

    if (table_name && table_name[0]) {
        if (*(rs_format_e *)ctx == RS_FORMAT_SAS_CATALOG) {
            printf("Catalog name: %s\n", table_name);
        } else {
            printf("Table name: %s\n", table_name);
        }
    }
    if (file_label && file_label[0]) {
        printf("Table label: %s\n", file_label);
    }
    if (version) {
        printf("Format version: %ld\n", version);
    }
    if (orig_encoding) {
        printf("Text encoding: %s\n", orig_encoding);
    }
    if (compression == READSTAT_COMPRESS_ROWS) {
        printf("Compression: rows\n");
    } else if (compression == READSTAT_COMPRESS_BINARY) {
        printf("Compression: binary\n");
    }
    if (endianness == READSTAT_ENDIAN_LITTLE) {
        printf("Byte order: little-endian\n");
    } else if (endianness == READSTAT_ENDIAN_BIG) {
        printf("Byte order: big-endian\n");
    }
    if (timestamp) {
        char buffer[128];
        readstat_strftime(buffer, sizeof(buffer), "%d %b %Y %H:%M", timestamp);
        printf("Timestamp: %s\n", buffer);
    }
    return 0;
}

static int dump_file(const char *input_filename) {
    rs_format_e input_format = readstat_format(input_filename);
    readstat_parser_t *parser = readstat_parser_init();
    readstat_error_t error = READSTAT_OK;

    printf("Format: %s\n", readstat_format_name(input_format));

    readstat_set_error_handler(parser, &handle_error);
    readstat_set_metadata_handler(parser, &dump_metadata);

    error = parse_file(parser, input_filename, input_format, &input_format);

    readstat_parser_free(parser);

    if (error != READSTAT_OK) {
        fprintf(stderr, "Error processing %s: %s\n", input_filename, readstat_error_message(error));
        return 1;
    }

    return 0;
}

int portable_main(int argc, char** argv) {
    char *input_filename = NULL;
    char *catalog_filename = NULL;
    char *output_filename = NULL;

    rs_module_t *modules = NULL;
    long modules_count = 2;
    long module_index = 0;
    int force = 0;

#if HAVE_XLSXWRITER
    modules_count++;
#endif

    modules = calloc(modules_count, sizeof(rs_module_t));

    modules[module_index++] = rs_mod_readstat;
    modules[module_index++] = rs_mod_csv;

#if HAVE_XLSXWRITER
    modules[module_index++] = rs_mod_xlsx;
#endif

    if (argc == 2 && (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0)) {
        print_version();
        return 0;
    }
    if (argc == 2 && (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)) {
        print_usage(argv[0]);
        return 0;
    }
    if (argc > 1) {
        int argpos = 1;
        if (strcmp(argv[argpos], "-f") == 0) {
            force = 1;
            argpos++;
        }
        if (argpos + 1 == argc) {
            if (can_read(argv[argpos])) {
                input_filename = argv[argpos];
            }
        } else if (argpos + 2 == argc) {
            if (can_read(argv[argpos]) && can_write(modules, modules_count, argv[argpos+1])) {
                input_filename = argv[argpos];
                output_filename = argv[argpos+1];
            }
        } else if (argpos + 3 == argc) {
            if (can_write(modules, modules_count, argv[argpos+2]) &&
                    (is_dictionary(argv[argpos+1]) ||
                     (can_read(argv[argpos]) && 
                      (is_json(argv[argpos+1]) || is_catalog(argv[argpos+1]))))) {
                input_filename = argv[argpos];
                catalog_filename = argv[argpos+1];
                output_filename = argv[argpos+2];
            }
        }
    }

    int ret;
    if (output_filename) {
        ret = convert_file(input_filename, catalog_filename, output_filename,
                modules, modules_count, force);
    } else if (input_filename) {
        ret = dump_file(input_filename); 
    } else {
        print_usage(argv[0]);
        ret = 1;
    }
    free(modules);
    return ret;
}

