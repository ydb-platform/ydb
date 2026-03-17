
#include <stdlib.h>
#include <stdio.h>

#include "../readstat.h"
#include "../readstat_iconv.h"
#include "../readstat_convert.h"
#include "readstat_spss.h"
#include "readstat_spss_parse.h"

static char spss_type_strings[][16] = {
    [SPSS_FORMAT_TYPE_A] = "A",
    [SPSS_FORMAT_TYPE_AHEX] = "AHEX",
    [SPSS_FORMAT_TYPE_COMMA] = "COMMA",
    [SPSS_FORMAT_TYPE_DOLLAR] = "DOLLAR",
    [SPSS_FORMAT_TYPE_F] = "F",
    [SPSS_FORMAT_TYPE_IB] = "IB",
    [SPSS_FORMAT_TYPE_PIBHEX] = "PIBHEX",
    [SPSS_FORMAT_TYPE_P] = "P",
    [SPSS_FORMAT_TYPE_PIB] = "PIB",
    [SPSS_FORMAT_TYPE_PK] = "PK",
    [SPSS_FORMAT_TYPE_RB] = "RB",
    [SPSS_FORMAT_TYPE_RBHEX] = "RBHEX",
    [SPSS_FORMAT_TYPE_Z] = "Z",
    [SPSS_FORMAT_TYPE_N] = "N",
    [SPSS_FORMAT_TYPE_E] = "E",
    [SPSS_FORMAT_TYPE_DATE] = "DATE",
    [SPSS_FORMAT_TYPE_TIME] = "TIME",
    [SPSS_FORMAT_TYPE_DATETIME] = "DATETIME",
    [SPSS_FORMAT_TYPE_ADATE] = "ADATE",
    [SPSS_FORMAT_TYPE_JDATE] = "JDATE",
    [SPSS_FORMAT_TYPE_DTIME] = "DTIME",
    [SPSS_FORMAT_TYPE_WKDAY] = "WKDAY",
    [SPSS_FORMAT_TYPE_MONTH] = "MONTH",
    [SPSS_FORMAT_TYPE_MOYR] = "MOYR",
    [SPSS_FORMAT_TYPE_QYR] = "QYR",
    [SPSS_FORMAT_TYPE_WKYR] = "WKYR",
    [SPSS_FORMAT_TYPE_PCT] = "PCT",
    [SPSS_FORMAT_TYPE_DOT] = "DOT",
    [SPSS_FORMAT_TYPE_CCA] = "CCA",
    [SPSS_FORMAT_TYPE_CCB] = "CCB",
    [SPSS_FORMAT_TYPE_CCC] = "CCC",
    [SPSS_FORMAT_TYPE_CCD] = "CCD",
    [SPSS_FORMAT_TYPE_CCE] = "CCE",
    [SPSS_FORMAT_TYPE_EDATE] = "EDATE",
    [SPSS_FORMAT_TYPE_SDATE] = "SDATE",
    [SPSS_FORMAT_TYPE_MTIME] = "MTIME",
    [SPSS_FORMAT_TYPE_YMDHMS] = "YMDHMS",
};

int spss_format(char *buffer, size_t len, spss_format_t *format) {
    if (format->type < 0 
            || format->type >= sizeof(spss_type_strings)/sizeof(spss_type_strings[0])
            || spss_type_strings[format->type][0] == '\0') {
        return 0;
    }
    char *string = spss_type_strings[format->type];

    if (format->decimal_places || format->type == SPSS_FORMAT_TYPE_F) {
        snprintf(buffer, len, "%s%d.%d", string, format->width, format->decimal_places);
    } else if (format->width) {
        snprintf(buffer, len, "%s%d", string, format->width);
    } else {
        snprintf(buffer, len, "%s", string);
    }

    return 1;
}

int spss_varinfo_compare(const void *elem1, const void *elem2) {
    int offset = *(int *)elem1;
    const spss_varinfo_t *v = *(const spss_varinfo_t **)elem2;
    if (offset < v->offset)
        return -1;
    return (offset > v->offset);
}

void spss_varinfo_free(spss_varinfo_t *info) {
    if (info) {
        if (info->label)
            free(info->label);
        free(info);
    }
}

uint64_t spss_64bit_value(readstat_value_t value) {
    double dval = readstat_double_value(value);
    uint64_t special_val;
    memcpy(&special_val, &dval, sizeof(double));

    if (isinf(dval)) {
        if (dval < 0.0) {
            special_val = SAV_LOWEST_DOUBLE;
        } else {
            special_val = SAV_HIGHEST_DOUBLE;
        }
    } else if (isnan(dval)) {
        special_val = SAV_MISSING_DOUBLE;
    }
    return special_val;
}

static readstat_value_t spss_boxed_double_value(double fp_value) {
    readstat_value_t value = {
        .type = READSTAT_TYPE_DOUBLE,
        .v = { .double_value = fp_value },
        .is_system_missing = isnan(fp_value)
    };
    return value;
}

static readstat_value_t spss_boxed_string_value(const char *string) {
    readstat_value_t value = {
        .type = READSTAT_TYPE_STRING,
        .v = { .string_value = string }
    };
    return value;
}

static readstat_value_t spss_boxed_missing_value(spss_varinfo_t *info, int i) {
    if (info->type == READSTAT_TYPE_DOUBLE) {
        return spss_boxed_double_value(info->missing_double_values[i]);
    }
    return spss_boxed_string_value(info->missing_string_values[i]);
}

readstat_missingness_t spss_missingness_for_info(spss_varinfo_t *info) {
    readstat_missingness_t missingness;
    memset(&missingness, '\0', sizeof(readstat_missingness_t));

    if (info->missing_range) {
        missingness.missing_ranges_count++;
        missingness.missing_ranges[0] = spss_boxed_missing_value(info, 0);
        missingness.missing_ranges[1] = spss_boxed_missing_value(info, 1);

        if (info->n_missing_values == 3) {
            missingness.missing_ranges_count++;
            missingness.missing_ranges[2] = missingness.missing_ranges[3] = spss_boxed_missing_value(info, 2);
        }
    } else if (info->n_missing_values > 0) {
        missingness.missing_ranges_count = info->n_missing_values;
        int i=0;
        for (i=0; i<info->n_missing_values; i++) {
            missingness.missing_ranges[2*i] = missingness.missing_ranges[2*i+1] = spss_boxed_missing_value(info, i);
        }
    }
    return missingness;
}

readstat_variable_t *spss_init_variable_for_info(spss_varinfo_t *info, int index_after_skipping,
        iconv_t converter) {
    readstat_variable_t *variable = calloc(1, sizeof(readstat_variable_t));

    variable->index = info->index;
    variable->index_after_skipping = index_after_skipping;
    variable->type = info->type;
    if (info->string_length) {
        variable->storage_width = info->string_length;
    } else {
        variable->storage_width = 8 * info->width;
    }

    if (info->longname[0]) {
        readstat_convert(variable->name, sizeof(variable->name),
                info->longname, sizeof(info->longname), converter);
    } else {
        readstat_convert(variable->name, sizeof(variable->name),
                info->name, sizeof(info->name), converter);
    }
    if (info->label) {
        snprintf(variable->label, sizeof(variable->label), "%s", info->label);
    }

    spss_format(variable->format, sizeof(variable->format), &info->print_format);

    variable->missingness = spss_missingness_for_info(info);
    variable->measure = info->measure;
    if (info->display_width) {
        variable->display_width = info->display_width;
    } else {
        variable->display_width = info->print_format.width;
    }

    return variable;
}

uint32_t spss_measure_from_readstat_measure(readstat_measure_t measure) {
    uint32_t sav_measure = SAV_MEASURE_UNKNOWN;
    if (measure == READSTAT_MEASURE_NOMINAL) {
        sav_measure = SAV_MEASURE_NOMINAL;
    } else if (measure == READSTAT_MEASURE_ORDINAL) {
        sav_measure = SAV_MEASURE_ORDINAL;
    } else if (measure == READSTAT_MEASURE_SCALE) {
        sav_measure = SAV_MEASURE_SCALE;
    }
    return sav_measure;
}

readstat_measure_t spss_measure_to_readstat_measure(uint32_t sav_measure) {
    if (sav_measure == SAV_MEASURE_NOMINAL)
        return READSTAT_MEASURE_NOMINAL;
    if (sav_measure == SAV_MEASURE_ORDINAL)
        return READSTAT_MEASURE_ORDINAL;
    if (sav_measure == SAV_MEASURE_SCALE)
        return READSTAT_MEASURE_SCALE;
    return READSTAT_MEASURE_UNKNOWN;
}

uint32_t spss_alignment_from_readstat_alignment(readstat_alignment_t alignment) {
    uint32_t sav_alignment = 0;
    if (alignment == READSTAT_ALIGNMENT_LEFT) {
        sav_alignment = SAV_ALIGNMENT_LEFT;
    } else if (alignment == READSTAT_ALIGNMENT_CENTER) {
        sav_alignment = SAV_ALIGNMENT_CENTER;
    } else if (alignment == READSTAT_ALIGNMENT_RIGHT) {
        sav_alignment = SAV_ALIGNMENT_RIGHT;
    }
    return sav_alignment;
}

readstat_alignment_t spss_alignment_to_readstat_alignment(uint32_t sav_alignment) {
    if (sav_alignment == SAV_ALIGNMENT_LEFT)
        return READSTAT_ALIGNMENT_LEFT;
    if (sav_alignment == SAV_ALIGNMENT_CENTER)
        return READSTAT_ALIGNMENT_CENTER;
    if (sav_alignment == SAV_ALIGNMENT_RIGHT)
        return READSTAT_ALIGNMENT_RIGHT;
    return READSTAT_ALIGNMENT_UNKNOWN;
}

readstat_error_t spss_format_for_variable(readstat_variable_t *r_variable,
        spss_format_t *spss_format) {
    readstat_error_t retval = READSTAT_OK;

    memset(spss_format, 0, sizeof(spss_format_t));

    if (r_variable->type == READSTAT_TYPE_STRING) {
        spss_format->type = SPSS_FORMAT_TYPE_A;
        if (r_variable->display_width) {
            spss_format->width = r_variable->display_width;
        } else if (r_variable->user_width) {
            spss_format->width = r_variable->user_width;
        } else {
            spss_format->width = r_variable->storage_width;
        }
    } else {
        spss_format->type = SPSS_FORMAT_TYPE_F;
        if (r_variable->display_width) {
            spss_format->width = r_variable->display_width;
        } else {
            spss_format->width = 8;
        }
        if (r_variable->type == READSTAT_TYPE_DOUBLE ||
                r_variable->type == READSTAT_TYPE_FLOAT) {
            spss_format->decimal_places = 2;
        }
    }

    if (r_variable->format[0]) {
        spss_format->decimal_places = 0;
        const char *fmt = r_variable->format;
        if (spss_parse_format(fmt, strlen(fmt), spss_format) != READSTAT_OK) {
            retval = READSTAT_ERROR_BAD_FORMAT_STRING;
            goto cleanup;
        }
    }

cleanup:
    return retval;
}
