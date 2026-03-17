
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "../readstat.h"
#include "../readstat_writer.h"
#include "readstat_sas.h"
#include "readstat_xport.h"
#include "readstat_xport_parse_format.h"
#include "ieee.h"

#define XPORT_DEFAULT_VERISON   8
#define RECORD_LEN 80

#if defined _MSC_VER
#define restrict __restrict
#endif

static void copypad(char * restrict dst, size_t dst_len, const char * restrict src) {
    char *dst_end = dst + dst_len;
    while (dst < dst_end && *src)
        *dst++ = *src++;
    while (dst < dst_end)
        *dst++ = ' ';
}

static readstat_error_t xport_write_bytes(readstat_writer_t *writer, const void *bytes, size_t len) {
    return readstat_write_bytes_as_lines(writer, bytes, len, RECORD_LEN, "");
}

static readstat_error_t xport_finish_record(readstat_writer_t *writer) {
    return readstat_write_line_padding(writer, ' ', RECORD_LEN, "");
}

static readstat_error_t xport_write_record(readstat_writer_t *writer, const char *record) {
    size_t len = strlen(record);
    readstat_error_t retval = READSTAT_OK;
    
    retval = xport_write_bytes(writer, record, len);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_finish_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t xport_write_header_record_v8(readstat_writer_t *writer, 
        xport_header_record_t *xrecord) {
    char record[RECORD_LEN+1];
    snprintf(record, sizeof(record),
            "HEADER RECORD*******%-8sHEADER RECORD!!!!!!!%-30d",
            xrecord->name, xrecord->num1);
    return xport_write_record(writer, record);
}

static readstat_error_t xport_write_header_record(readstat_writer_t *writer, 
        xport_header_record_t *xrecord) {
    char record[RECORD_LEN+1];
    snprintf(record, sizeof(record),
            "HEADER RECORD*******%-8sHEADER RECORD!!!!!!!" "%05d%05d%05d" "%05d%05d%05d",
            xrecord->name, xrecord->num1, xrecord->num2, xrecord->num3,
            xrecord->num4, xrecord->num5, xrecord->num6);
    return xport_write_record(writer, record);
}

static size_t xport_variable_width(readstat_type_t type, size_t user_width) {
    if (type == READSTAT_TYPE_STRING)
        return user_width;

    if (user_width >= XPORT_MAX_DOUBLE_SIZE || user_width == 0)
        return XPORT_MAX_DOUBLE_SIZE;

    if (user_width <= XPORT_MIN_DOUBLE_SIZE)
        return XPORT_MIN_DOUBLE_SIZE;

    return user_width;
}

static readstat_error_t xport_write_variables(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;
    int i;
    long offset = 0;
    int num_long_labels = 0;
    int any_has_long_format = 0;
    for (i=0; i<writer->variables_count; i++) {
        int needs_long_record = 0;
        readstat_variable_t *variable = readstat_get_variable(writer, i);
        size_t width = xport_variable_width(variable->type, variable->user_width);
        xport_namestr_t namestr = { 
            .nvar0 = i+1,
            .nlng = width,
            .npos = offset,
            .niform = "        ",
            .nform = "        "
        };
        if (readstat_variable_get_type_class(variable) == READSTAT_TYPE_CLASS_STRING) {
            namestr.ntype = SAS_COLUMN_TYPE_CHR;
        } else {
            namestr.ntype = SAS_COLUMN_TYPE_NUM;
        }

        copypad(namestr.nname, sizeof(namestr.nname), variable->name);
        copypad(namestr.nlabel, sizeof(namestr.nlabel), variable->label);

        if (variable->format[0]) {
            xport_format_t format; 

            retval = xport_parse_format(variable->format, strlen(variable->format),
                    &format, NULL, NULL);
            if (retval != READSTAT_OK)
                goto cleanup;

            copypad(namestr.nform, sizeof(namestr.nform), format.name);
            namestr.nfl = format.width;
            namestr.nfd = format.decimals;

            copypad(namestr.niform, sizeof(namestr.niform), format.name);
            namestr.nifl = format.width;
            namestr.nifd = format.decimals;

            if (strlen(format.name) > 8) {
                any_has_long_format = 1;
                needs_long_record = 1;
            }
        } else if (variable->display_width) {
            namestr.nfl = variable->display_width;
        }

        namestr.nfj = (variable->alignment == READSTAT_ALIGNMENT_RIGHT);

        if (writer->version == 8) {
            copypad(namestr.longname, sizeof(namestr.longname), variable->name);

            size_t label_len = strlen(variable->label);
            if (label_len > 40) {
                needs_long_record = 1;
            }
            namestr.labeln = label_len;
        }

        if (needs_long_record) {
            num_long_labels++;
        }

        offset += width;

        xport_namestr_bswap(&namestr);

        retval = xport_write_bytes(writer, &namestr, sizeof(xport_namestr_t));
        if (retval != READSTAT_OK)
            goto cleanup;
    }

    retval = xport_finish_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    if (writer->version == 8 && num_long_labels) {
        xport_header_record_t header = { 
            .name = "LABELV8",
            .num1 = num_long_labels };
        if (any_has_long_format) {
            strcpy(header.name, "LABELV9");
        }
        retval = xport_write_header_record_v8(writer, &header);
        if (retval != READSTAT_OK)
            goto cleanup;

        for (i=0; i<writer->variables_count; i++) {
            readstat_variable_t *variable = readstat_get_variable(writer, i);
            size_t label_len = strlen(variable->label);
            size_t name_len = strlen(variable->name);
            size_t format_len = strlen(variable->format);
            int has_long_label = 0;
            int has_long_format = 0;

            has_long_label = (label_len > 40);

            if (variable->format[0]) {
                xport_format_t format; 

                retval = xport_parse_format(variable->format, strlen(variable->format),
                        &format, NULL, NULL);
                if (retval != READSTAT_OK)
                    goto cleanup;

                if (strlen(format.name) > 8) {
                    has_long_format = 1;
                }
            }

            if (has_long_format) {
                uint16_t labeldef[5] = { i+1, name_len, label_len, format_len, format_len };

                if (machine_is_little_endian()) {
                    labeldef[0] = byteswap2(labeldef[0]);
                    labeldef[1] = byteswap2(labeldef[1]);
                    labeldef[2] = byteswap2(labeldef[2]);
                    labeldef[3] = byteswap2(labeldef[3]);
                    labeldef[4] = byteswap2(labeldef[4]);
                }

                retval = readstat_write_bytes(writer, labeldef, sizeof(labeldef));
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_string(writer, variable->name);
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_string(writer, variable->label);
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_string(writer, variable->format);
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_string(writer, variable->format);
                if (retval != READSTAT_OK)
                    goto cleanup;

            } else if (has_long_label) {
                uint16_t labeldef[3] = { i+1, name_len, label_len };

                if (machine_is_little_endian()) {
                    labeldef[0] = byteswap2(labeldef[0]);
                    labeldef[1] = byteswap2(labeldef[1]);
                    labeldef[2] = byteswap2(labeldef[2]);
                }

                retval = readstat_write_bytes(writer, labeldef, sizeof(labeldef));
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_string(writer, variable->name);
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_string(writer, variable->label);
                if (retval != READSTAT_OK)
                    goto cleanup;
            }
        }

        retval = xport_finish_record(writer);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

cleanup:

    return retval;
}

static readstat_error_t xport_write_first_header_record(readstat_writer_t *writer) {
    xport_header_record_t xrecord = { .name = "LIBRARY" };
    if (writer->version == 8) {
        strcpy(xrecord.name, "LIBV8");
    }
    return xport_write_header_record(writer, &xrecord);
}

static readstat_error_t xport_write_first_real_header_record(readstat_writer_t *writer,
        const char *timestamp) {
    char real_record[RECORD_LEN+1];
    snprintf(real_record, sizeof(real_record),
            "%-8.8s" "%-8.8s" "%-8.8s"  "%-8.8s" "%-8.8s"  "%-24.24s" "%16.16s",
            "SAS",   "SAS",   "SASLIB", "6.06",  "bsd4.2", "",        timestamp);

    return xport_write_record(writer, real_record);
}

static readstat_error_t xport_write_member_header_record(readstat_writer_t *writer) {
    xport_header_record_t xrecord = { 
        .name = "MEMBER",
        .num4 = 160, .num6 = 140
    };
    if (writer->version == 8) {
        strcpy(xrecord.name, "MEMBV8");
    }
    return xport_write_header_record(writer, &xrecord);
}

static readstat_error_t xport_write_descriptor_header_record(readstat_writer_t *writer) {
    xport_header_record_t xrecord = { 
        .name = "DSCRPTR"
    };
    if (writer->version == 8) {
        strcpy(xrecord.name, "DSCPTV8");
    }
    return xport_write_header_record(writer, &xrecord);
}

static readstat_error_t xport_write_member_record_v8(readstat_writer_t *writer,
        char *timestamp) {
    readstat_error_t retval = READSTAT_OK;
    char member_header[RECORD_LEN+1];
    char *ds_name = "DATASET";
    if (writer->table_name[0])
        ds_name = writer->table_name;

    snprintf(member_header, sizeof(member_header),
            "%-8.8s" "%-32.32s"   "%-8.8s"   "%-8.8s" "%-8.8s" "%16.16s",
            "SAS",   ds_name, "SASDATA", "6.06",  "bsd4.2", timestamp);

    retval = xport_write_record(writer, member_header);

    return retval;
}

static readstat_error_t xport_write_member_record(readstat_writer_t *writer,
        char *timestamp) {
    if (writer->version == 8)
        return xport_write_member_record_v8(writer, timestamp);

    readstat_error_t retval = READSTAT_OK;
    char member_header[RECORD_LEN+1];
    char *ds_name = "DATASET";
    if (writer->table_name[0])
        ds_name = writer->table_name;

    snprintf(member_header, sizeof(member_header),
            "%-8.8s" "%-8.8s" "%-8.8s"   "%-8.8s" "%-8.8s"  "%-24.24s" "%16.16s",
            "SAS",   ds_name, "SASDATA", "6.06",  "bsd4.2", "",        timestamp);

    retval = xport_write_record(writer, member_header);

    return retval;
}

static readstat_error_t xport_write_file_label_record(readstat_writer_t *writer,
        char *timestamp) {
    char member_header[RECORD_LEN+1];
    snprintf(member_header, sizeof(member_header),
            "%16.16s"  "%16.16s"  "%-40.40s"           "%-8.8s",
            timestamp, "",        writer->file_label,  "" /* dstype? */);

    return xport_write_record(writer, member_header);
}

static readstat_error_t xport_write_namestr_header_record(readstat_writer_t *writer) {
    xport_header_record_t xrecord = { 
        .name = "NAMESTR",
        .num2 = writer->variables_count
    };
    if (writer->version == 8) {
        strcpy(xrecord.name, "NAMSTV8");
    }
    return xport_write_header_record(writer, &xrecord);
}

static readstat_error_t xport_write_obs_header_record(readstat_writer_t *writer) {
    xport_header_record_t xrecord = { 
        .name = "OBS"
    };
    if (writer->version == 8) {
        strcpy(xrecord.name, "OBSV8");
    }
    return xport_write_header_record(writer, &xrecord);
}

static readstat_error_t xport_format_timestamp(char *output, size_t output_len, time_t timestamp) {
    struct tm *ts = localtime(&timestamp);

    if (!ts)
        return READSTAT_ERROR_BAD_TIMESTAMP_VALUE;

    snprintf(output, output_len, 
            "%02d%3.3s%02d:%02d:%02d:%02d",
            (unsigned int)ts->tm_mday % 100, 
            _xport_months[ts->tm_mon],
            (unsigned int)ts->tm_year % 100,
            (unsigned int)ts->tm_hour % 100,
            (unsigned int)ts->tm_min % 100,
            (unsigned int)ts->tm_sec % 100
            );

    return READSTAT_OK;
}

static readstat_error_t xport_begin_data(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    readstat_error_t retval = READSTAT_OK;
    char timestamp[17];

    retval = xport_format_timestamp(timestamp, sizeof(timestamp), writer->timestamp);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_first_header_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_first_real_header_record(writer, timestamp);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_record(writer, timestamp);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_member_header_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_descriptor_header_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_member_record(writer, timestamp);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_file_label_record(writer, timestamp);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_namestr_header_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_variables(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = xport_write_obs_header_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t xport_end_data(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    readstat_error_t retval = READSTAT_OK;

    retval = xport_finish_record(writer);

    return retval;
}

static readstat_error_t xport_write_row(void *writer_ctx, void *row, size_t row_len) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    return xport_write_bytes(writer, row, row_len);
}

static readstat_error_t xport_write_double(void *row, const readstat_variable_t *var, double value) {
    char full_value[8];
    int rc = cnxptiee(&value, CN_TYPE_NATIVE, full_value, CN_TYPE_XPORT);

    if (rc)
        return READSTAT_ERROR_CONVERT;

    memcpy(row, full_value, var->storage_width);

    return READSTAT_OK;
}

static readstat_error_t xport_write_float(void *row, const readstat_variable_t *var, float value) {
    return xport_write_double(row, var, value);
}

static readstat_error_t xport_write_int32(void *row, const readstat_variable_t *var, int32_t value) {
    return xport_write_double(row, var, value);
}

static readstat_error_t xport_write_int16(void *row, const readstat_variable_t *var, int16_t value) {
    return xport_write_double(row, var, value);
}

static readstat_error_t xport_write_int8(void *row, const readstat_variable_t *var, int8_t value) {
    return xport_write_double(row, var, value);
}

static readstat_error_t xport_write_string(void *row, const readstat_variable_t *var, const char *string) {
    memset(row, ' ', var->storage_width);
    if (string != NULL && string[0]) {
        size_t value_len = strlen(string);
        if (value_len > var->storage_width)
            return READSTAT_ERROR_STRING_VALUE_IS_TOO_LONG;

        memcpy(row, string, value_len);
    }
    return READSTAT_OK;
}

static readstat_error_t xport_write_missing_numeric(void *row, const readstat_variable_t *var) {
    char *row_bytes = (char *)row;
    row_bytes[0] = 0x2e;
    return READSTAT_OK;
}   
    
static readstat_error_t xport_write_missing_string(void *row, const readstat_variable_t *var) {
    return xport_write_string(row, var, NULL);
}

static readstat_error_t xport_write_missing_tagged(void *row, const readstat_variable_t *var, char tag) {
    char *row_bytes = (char *)row;
    readstat_error_t error = sas_validate_tag(tag);
    if (error == READSTAT_OK) {
        row_bytes[0] = tag;
    }
    return error;
}

static readstat_error_t xport_metadata_ok(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;

    if (writer->version != 5 && writer->version != 8)
        return READSTAT_ERROR_UNSUPPORTED_FILE_FORMAT_VERSION;

    if (writer->table_name[0]) {
        if (writer->version == 8) {
            return sas_validate_name(writer->table_name, 32);
        }
        if (writer->version == 5) {
            return sas_validate_name(writer->table_name, 8);
        }
    }

    return READSTAT_OK;
}

readstat_error_t readstat_begin_writing_xport(readstat_writer_t *writer, void *user_ctx, long row_count) {

    if (writer->version == 0)
        writer->version = XPORT_DEFAULT_VERISON;

    writer->callbacks.metadata_ok = &xport_metadata_ok;
    writer->callbacks.write_int8 = &xport_write_int8;
    writer->callbacks.write_int16 = &xport_write_int16;
    writer->callbacks.write_int32 = &xport_write_int32;
    writer->callbacks.write_float = &xport_write_float;
    writer->callbacks.write_double = &xport_write_double;

    writer->callbacks.write_string = &xport_write_string;
    writer->callbacks.write_missing_string = &xport_write_missing_string;
    writer->callbacks.write_missing_number = &xport_write_missing_numeric;
    writer->callbacks.write_missing_tagged = &xport_write_missing_tagged;

    writer->callbacks.variable_width = &xport_variable_width;
    writer->callbacks.variable_ok = &sas_validate_variable;

    writer->callbacks.begin_data = &xport_begin_data;
    writer->callbacks.end_data = &xport_end_data;

    writer->callbacks.write_row = &xport_write_row;

    return readstat_begin_writing_file(writer, user_ctx, row_count);
}
