/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"
#include "action_class_concept.h"
#include <float.h>
#include <string>
#include <sstream>

typedef enum
{
    eROUND_ANGLE_UP,
    eROUND_ANGLE_DOWN
} RoundingPolicy;

static void set_total_length(unsigned char* buffer, long* section_length, const long* section_offset, int edition, size_t totalLength)
{
    long off;
    switch (edition) {
        case 1:
            if (totalLength < 0x800000) {
                off = 32;
                grib_encode_unsigned_long(buffer, (unsigned long)totalLength, &off, 24);
            }
            else {
                long s4len, t120;
                totalLength -= 4;
                t120        = (totalLength + 119) / 120;
                s4len       = t120 * 120 - totalLength;
                totalLength = 0x800000 | t120;
                off         = 32;
                grib_encode_unsigned_long(buffer, (unsigned long)totalLength, &off, 24);
                off = section_offset[4] * 8;
                grib_encode_unsigned_long(buffer, (unsigned long)s4len, &off, 24);
            }
            break;
        case 2:
            off = 64;
            grib_encode_unsigned_long(buffer, (unsigned long)totalLength, &off, 64);
            break;
    }
}

static grib_handle* grib_sections_copy_internal(grib_handle* hfrom, grib_handle* hto, int sections[], int* err)
{
    int i;
    size_t totalLength = 0;
    unsigned char* buffer;
    unsigned char* p;
    long edition                          = 0;
    long section_length[MAX_NUM_SECTIONS] = {0,};
    long section_offset[MAX_NUM_SECTIONS] = {0,};
    long off = 0;
    grib_handle* h;
    char section_length_str[64] = "section0Length";
    char section_offset_str[64] = "offsetSection0";
    long length, offset;

    *err = grib_get_long(hfrom, "edition", &edition);
    if (*err)
        return NULL;

    for (i = 0; i <= hfrom->sections_count; i++) {
        if (sections[i]) {
            h = hfrom;
        }
        else {
            h = hto;
        }

        snprintf(section_length_str, sizeof(section_length_str), "section%dLength", i);
        if (grib_get_long(h, section_length_str, &length))
            continue;
        section_length[i] = length;

        snprintf(section_offset_str, sizeof(section_offset_str), "offsetSection%d", i);
        if (grib_get_long(h, section_offset_str, &offset))
            continue;
        section_offset[i] = offset;

        totalLength += section_length[i];
    }

    buffer = (unsigned char*)grib_context_malloc_clear(hfrom->context, totalLength * sizeof(char));

    p   = buffer;
    off = 0;
    for (i = 0; i <= hfrom->sections_count; i++) {
        const grib_handle* hand = NULL;
        if (sections[i])
            hand = hfrom;
        else
            hand = hto;
        p = (unsigned char*)memcpy(p, hand->buffer->data + section_offset[i], section_length[i]);
        section_offset[i] = off;
        off += section_length[i];
        p += section_length[i];
    }

    // copy section 3 present flag
    if (edition == 1) {
        const void* buffer_to = NULL;
        size_t size_to        = 0;
        grib_get_message(hto, &buffer_to, &size_to);
        memcpy(buffer + 15, ((unsigned char*)buffer_to) + 15, 1);
    }

    set_total_length(buffer, section_length, section_offset, edition, totalLength);

    h = grib_handle_new_from_message(hfrom->context, buffer, totalLength);

    // to allow freeing of buffer
    h->buffer->property = CODES_MY_BUFFER;

    switch (edition) {
        case 1:
            if (sections[1] && sections[2])
                break;

            if (sections[1]) {
                long PVPresent;
                grib_get_long(hfrom, "PVPresent", &PVPresent);
                if (PVPresent) {
                    double* pv;
                    long numberOfVerticalCoordinateValues;
                    size_t size = 0;

                    grib_get_long(hfrom, "numberOfVerticalCoordinateValues", &numberOfVerticalCoordinateValues);
                    size = numberOfVerticalCoordinateValues;
                    pv   = (double*)grib_context_malloc_clear(hfrom->context, numberOfVerticalCoordinateValues * sizeof(double));
                    grib_get_double_array(hfrom, "pv", pv, &size);
                    grib_set_long(h, "PVPresent", 1);
                    grib_set_double_array(h, "pv", pv, size);

                    grib_context_free(hfrom->context, pv);
                }
                else {
                    grib_set_long(h, "PVPresent", 0);
                }
            }
            if (sections[2]) {
                long PVPresent;
                grib_get_long(hto, "PVPresent", &PVPresent);
                if (PVPresent) {
                    double* pv;
                    long numberOfVerticalCoordinateValues;
                    size_t size = 0;

                    grib_get_long(hto, "numberOfVerticalCoordinateValues", &numberOfVerticalCoordinateValues);
                    size = numberOfVerticalCoordinateValues;
                    pv   = (double*)grib_context_malloc_clear(hto->context, numberOfVerticalCoordinateValues * sizeof(double));
                    grib_get_double_array(hto, "pv", pv, &size);
                    grib_set_long(h, "PVPresent", 1);
                    grib_set_double_array(h, "pv", pv, size);

                    grib_context_free(hto->context, pv);
                }
                else {
                    grib_set_long(h, "PVPresent", 0);
                }
            }
            break;
        case 2:
            if (sections[1]) {
                long discipline;
                grib_get_long(hfrom, "discipline", &discipline);
                grib_set_long(h, "discipline", discipline);
            }
            break;
    }

    return h;
}

grib_handle* grib_util_sections_copy(grib_handle* hfrom, grib_handle* hto, int what, int* err)
{
    long edition_from                      = 0;
    long edition_to                        = 0;
    long localDefinitionNumber             = -1;
    int sections_to_copy[MAX_NUM_SECTIONS] = {0,};

    *err = grib_get_long(hfrom, "edition", &edition_from);
    if (*err)
        return NULL;
    *err = grib_get_long(hto, "edition", &edition_to);
    if (*err)
        return NULL;

    if (hfrom->context->debug) {
        fprintf(stderr, "ECCODES DEBUG %s: Copying the following sections: ", __func__);
        if (what & GRIB_SECTION_GRID)    fprintf(stderr, "Grid, ");
        if (what & GRIB_SECTION_PRODUCT) fprintf(stderr, "Product, ");
        if (what & GRIB_SECTION_LOCAL)   fprintf(stderr, "Local, ");
        if (what & GRIB_SECTION_DATA)    fprintf(stderr, "Data, ");
        if (what & GRIB_SECTION_BITMAP)  fprintf(stderr, "Bitmap, ");
        fprintf(stderr, "\n");
    }

    if (edition_to != 1 && edition_to != 2) {
        *err = GRIB_NOT_IMPLEMENTED;
        return NULL;
    }

    if (edition_from != edition_to) {
        *err = GRIB_DIFFERENT_EDITION;
        return NULL;
    }

    if (what & GRIB_SECTION_GRID) {
        switch (edition_from) {
            case 1:
                sections_to_copy[2] = 1;
                break;
            case 2:
                sections_to_copy[3] = 1;
                break;
        }
    }

    if (what & GRIB_SECTION_DATA) {
        switch (edition_from) {
            case 1:
                sections_to_copy[3] = 1;
                sections_to_copy[4] = 1;
                break;
            case 2:
                sections_to_copy[5] = 1;
                sections_to_copy[6] = 1;
                sections_to_copy[7] = 1;
                break;
        }
    }

    if (what & GRIB_SECTION_LOCAL) {
        switch (edition_from) {
            case 1:
                sections_to_copy[1] = 1;
                break;
            case 2:
                sections_to_copy[2] = 1;
                break;
        }
    }

    if (what & GRIB_SECTION_PRODUCT) {
        switch (edition_from) {
            case 1:
                grib_get_long(hfrom, "localDefinitionNumber", &localDefinitionNumber);
                if (localDefinitionNumber == 13) {
                    sections_to_copy[4] = 1;
                }
                sections_to_copy[1] = 1;
                break;
            case 2:
                sections_to_copy[1] = 1;
                sections_to_copy[4] = 1;
                break;
        }
    }

    if (what & GRIB_SECTION_BITMAP) {
        switch (edition_from) {
            case 1:
                sections_to_copy[3] = 1;
                break;
            case 2:
                sections_to_copy[6] = 1;
                break;
        }
    }

    return grib_sections_copy_internal(hfrom, hto, sections_to_copy, err);
}

static grib_trie* init_list(const char* name);
static grib_trie* param_id_list   = NULL;
static grib_trie* mars_param_list = NULL;

grib_string_list* grib_util_get_param_id(const char* mars_param)
{
    if (!mars_param_list && (mars_param_list = init_list("mars_param.table")) == NULL)
        return NULL;
    return (grib_string_list*)grib_trie_get(mars_param_list, mars_param);
}

grib_string_list* grib_util_get_mars_param(const char* param_id)
{
    if (!param_id_list && (param_id_list = init_list("param_id.table")) == NULL)
        return NULL;
    return (grib_string_list*)grib_trie_get(param_id_list, param_id);
}

static grib_trie* init_list(const char* name)
{
    char* full_path = 0;
    FILE* fh;
    char s[101];
    char param[101];
    grib_string_list* list = 0;
    grib_string_list* next = 0;
    grib_trie* trie_list;
    grib_context* c = grib_context_get_default();
    full_path       = grib_context_full_defs_path(c, name);

    fh = codes_fopen(full_path, "r");
    if (!fh) {
        grib_context_log(c, GRIB_LOG_PERROR, "unable to read %s", full_path);
        return NULL;
    }

    list      = (grib_string_list*)grib_context_malloc_clear(c, sizeof(grib_string_list));
    trie_list = grib_trie_new(c);
    if (fscanf(fh, "%100s", param) == EOF) {
        fclose(fh);
        return NULL;
    }
    while (fscanf(fh, "%100s", s) != EOF) {
        if (!strcmp(s, "|")) {
            grib_trie_insert(trie_list, param, list);
            if (fscanf(fh, "%100s", param) == EOF) {
                fclose(fh);
                return trie_list;
            }
            list = NULL;
        }
        else {
            if (!list) {
                list        = (grib_string_list*)grib_context_malloc_clear(c, sizeof(grib_string_list));
                list->value = grib_context_strdup(c, s);
            }
            else {
                next = list;
                while (next->next)
                    next = next->next;
                next->next        = (grib_string_list*)grib_context_malloc_clear(c, sizeof(grib_string_list));
                next->next->value = grib_context_strdup(c, s);
            }
        }
    }

    fclose(fh);
    return 0;
}

static const char* get_packing_spec_packing_name(long packing_spec_packing)
{
    if (GRIB_UTIL_PACKING_USE_PROVIDED == packing_spec_packing)
        return "GRIB_UTIL_PACKING_USE_PROVIDED";
    if (GRIB_UTIL_PACKING_SAME_AS_INPUT == packing_spec_packing)
        return "GRIB_UTIL_PACKING_SAME_AS_INPUT";
    ECCODES_ASSERT(!"get_packing_spec_packing_name: invalid packing");
    return NULL;
}

static const char* get_packing_spec_packing_type_name(long packing_spec_packing_type)
{
    if (GRIB_UTIL_PACKING_TYPE_SAME_AS_INPUT == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_SAME_AS_INPUT";
    if (GRIB_UTIL_PACKING_TYPE_SPECTRAL_COMPLEX == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_SPECTRAL_COMPLEX";
    if (GRIB_UTIL_PACKING_TYPE_SPECTRAL_SIMPLE == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_SPECTRAL_SIMPLE";
    if (GRIB_UTIL_PACKING_TYPE_JPEG == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_JPEG";
    if (GRIB_UTIL_PACKING_TYPE_GRID_COMPLEX == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_GRID_COMPLEX";
    if (GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE";
    if (GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE_MATRIX == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE_MATRIX";
    if (GRIB_UTIL_PACKING_TYPE_GRID_SECOND_ORDER == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_GRID_SECOND_ORDER";
    if (GRIB_UTIL_PACKING_TYPE_CCSDS == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_CCSDS";
    if (GRIB_UTIL_PACKING_TYPE_IEEE == packing_spec_packing_type)
        return "GRIB_UTIL_PACKING_TYPE_IEEE";
    ECCODES_ASSERT(!"get_packing_spec_packing_type_name: invalid packing_type");
    return NULL;
}

// For debugging purposes
static void print_values(const grib_context* c,
                         const grib_util_grid_spec* spec,
                         const grib_util_packing_spec* packing_spec,
                         const char* input_packing_type,
                         const double* data_values, const size_t data_values_count, // the data pay load
                         const grib_values* keyval_pairs, const size_t count)       // keys and their values
{
    size_t i       = 0;
    int isConstant = 1;
    double v = 0, minVal = DBL_MAX, maxVal = -DBL_MAX;
    fprintf(stderr, "ECCODES DEBUG grib_util: input_packing_type = %s\n", input_packing_type);
    fprintf(stderr, "ECCODES DEBUG grib_util: grib_set_values, setting %zu key/value pairs\n", count);

    for (i = 0; i < count; i++) {
        switch (keyval_pairs[i].type) {
            case GRIB_TYPE_LONG:
                fprintf(stderr, "ECCODES DEBUG grib_util: => %s =  %ld;\n", keyval_pairs[i].name, keyval_pairs[i].long_value);
                break;
            case GRIB_TYPE_DOUBLE:
                fprintf(stderr, "ECCODES DEBUG grib_util: => %s = %.16e;\n", keyval_pairs[i].name, keyval_pairs[i].double_value);
                break;
            case GRIB_TYPE_STRING:
                fprintf(stderr, "ECCODES DEBUG grib_util: => %s = \"%s\";\n", keyval_pairs[i].name, keyval_pairs[i].string_value);
                break;
        }
    }

    fprintf(stderr, "ECCODES DEBUG grib_util: data_values_count=%zu;\n", data_values_count);
    for (i = 0; i < data_values_count; i++) {
        if (i == 0)
            v = data_values[i];
        if (data_values[i] != spec->missingValue) {
            if (v == spec->missingValue) {
                v = data_values[i];
            }
            else if (v != data_values[i]) {
                isConstant = 0;
                break;
            }
        }
    }

    for (i = 0; i < data_values_count; i++) {
        v = data_values[i];
        if (v != spec->missingValue) {
            if (v < minVal)
                minVal = v;
            if (v > maxVal)
                maxVal = v;
        }
    }
    fprintf(stderr, "ECCODES DEBUG grib_util: data_values are CONSTANT? %d\t(min=%.16e, max=%.16e)\n",
            isConstant, minVal, maxVal);
    if (c->gribex_mode_on)
        fprintf(stderr, "ECCODES DEBUG grib_util: GRIBEX mode is turned on!\n");

    fprintf(stderr, "ECCODES DEBUG grib_util: packing_spec->editionNumber = %ld\n",
            packing_spec->editionNumber);
    fprintf(stderr, "ECCODES DEBUG grib_util: packing_spec->packing = %s\n",
            get_packing_spec_packing_name(packing_spec->packing));
    fprintf(stderr, "ECCODES DEBUG grib_util: packing_spec->packing_type = %s\n",
            get_packing_spec_packing_type_name(packing_spec->packing_type));

//         if (spec->bitmapPresent) {
//             int missing = 0;
//             size_t j = 0;
//             double min = 1e100;
//             for(j = 0; j < data_values_count ; j++)
//             {
//                 double d = data_values[j] - spec->missingValue;
//                 if(d < 0) d = -d;
//                 if(d < min) {
//                     min = d;
//                 }
//                 if(data_values[j] == spec->missingValue)
//                     missing++;
//             }
//         }

}

// static int DBL_EQUAL(double d1, double d2, double tolerance)
// {
//     return fabs(d1-d2) < tolerance;
// }

// Returns a boolean: 1 if angle can be encoded, 0 otherwise
// static int grib1_angle_can_be_encoded(const double angle)
// {
//     const double angle_milliDegrees = angle * 1000;
//     double rounded = (int)(angle_milliDegrees+0.5)/1000.0;
//     if (angle<0) {
//         rounded = (int)(angle_milliDegrees-0.5)/1000.0;
//     }
//     if (angle == rounded) return 1;
//     return 0; // sub millidegree. Cannot be encoded in grib1
// }

// Returns a boolean: 1 if angle can be encoded, 0 otherwise
// static int angle_can_be_encoded(const double angle, const double angular_precision)
// {
//     const double angle_expanded = angle * angular_precision;
//     ECCODES_ASSERT(angular_precision>0);
//     double rounded = (long)(angle_expanded+0.5)/angular_precision;
//     if (angle<0) {
//         rounded = (long)(angle_expanded-0.5)/angular_precision;
//     }
//     if (angle == rounded) return 1;
//     //printf("      ......... angle cannot be encoded: %.10e\n", angle);
//     return 0; // Cannot be encoded
// }

// Returns a boolean: 1 if angle can be encoded, 0 otherwise
static int angle_can_be_encoded(const grib_handle* h, const double angle)
{
    int ret              = 0;
    int retval           = 1;
    grib_handle* h2      = NULL;
    char sample_name[16] = {0,};
    long angle_subdivisions = 0; // e.g. 1e3 for grib1 and 1e6 for grib2
    long edition = 0, coded = 0;
    double expanded, diff;

    if ((ret = grib_get_long(h, "edition", &edition)) != 0)
        return ret;
    if ((ret = grib_get_long(h, "angleSubdivisions", &angle_subdivisions)) != 0)
        return ret;
    ECCODES_ASSERT(angle_subdivisions > 0);

    snprintf(sample_name, sizeof(sample_name), "GRIB%ld", edition);
    h2 = grib_handle_new_from_samples(0, sample_name);
    if ((ret = grib_set_double(h2, "latitudeOfFirstGridPointInDegrees", angle)) != 0)
        return ret;
    if ((ret = grib_get_long(h2, "latitudeOfFirstGridPoint", &coded)) != 0)
        return ret;
    grib_handle_delete(h2);

    expanded = angle * angle_subdivisions;
    diff     = fabs(expanded - coded);
    if (diff < 1.0 / angle_subdivisions)
        retval = 1;
    else
        retval = 0;

    return retval;
}

static double adjust_angle(const double angle, const RoundingPolicy policy, const double angle_subdivisions)
{
    double result = 0;
    ECCODES_ASSERT(angle_subdivisions > 0);
    result = angle * angle_subdivisions;
    if (policy == eROUND_ANGLE_UP)
        result = round(result + 0.5);
    else
        result = round(result - 0.5);
    result = result / angle_subdivisions;
    return result;
}

// Search key=value array for:
//  *  latitudeOfFirstGridPointInDegrees
//  *  longitudeOfFirstGridPointInDegrees
//  *  latitudeOfLastGridPointInDegrees
//  *  longitudeOfLastGridPointInDegrees
// and change their values to expand the bounding box
static int expand_bounding_box(const grib_handle* h, grib_values* values, const size_t count)
{
    int ret                       = GRIB_SUCCESS;
    size_t i                      = 0;
    double new_angle              = 0;
    RoundingPolicy roundingPolicy = eROUND_ANGLE_UP;
    long angle_subdivisions       = 0; // e.g. 1e3 for grib1 and 1e6 for grib2
    if ((ret = grib_get_long(h, "angleSubdivisions", &angle_subdivisions)) != 0)
        return ret;

    for (i = 0; i < count; i++) {
        int is_angle = 0;
        if (strcmp(values[i].name, "longitudeOfFirstGridPointInDegrees") == 0) {
            roundingPolicy = eROUND_ANGLE_DOWN;
            is_angle       = 1;
        }
        else if (strcmp(values[i].name, "longitudeOfLastGridPointInDegrees") == 0) {
            roundingPolicy = eROUND_ANGLE_UP;
            is_angle       = 1;
        }
        else if (strcmp(values[i].name, "latitudeOfFirstGridPointInDegrees") == 0) {
            roundingPolicy = eROUND_ANGLE_UP;
            is_angle       = 1;
        }
        else if (strcmp(values[i].name, "latitudeOfLastGridPointInDegrees") == 0) {
            roundingPolicy = eROUND_ANGLE_DOWN;
            is_angle       = 1;
        }

        if (is_angle && !angle_can_be_encoded(h, values[i].double_value)) {
            new_angle = adjust_angle(values[i].double_value, roundingPolicy, angle_subdivisions);
            if (h->context->debug) {
                fprintf(stderr, "ECCODES DEBUG grib_util: EXPAND_BOUNDING_BOX %s: old=%.15e new=%.15e (%s)\n",
                        values[i].name, values[i].double_value, new_angle,
                        (roundingPolicy == eROUND_ANGLE_UP ? "Up" : "Down"));
            }
            values[i].double_value = new_angle;
        }
    }
    return ret;
}

/* Returns a boolean: 1 if angle is too small, 0 otherwise */
/*static int angle_too_small(const double angle, const double angular_precision)
{
    const double a = fabs(angle);
    if (a > 0 && a < angular_precision) return 1;
    return 0;
}

static double normalise_angle(double angle)
{
    while (angle<0)   angle += 360;
    while (angle>360) angle -= 360;
    return angle;
}
static int check_values(const double* data_values, size_t data_values_count)
{
    size_t i = 0;
    for (i=0; i<data_values_count; i++) {
        const double val = data_values[i];
        if ( val >= DBL_MAX   ||
             val <= -DBL_MAX  ||
             isnan(val) )
        {
            fprintf(stderr,"GRIB_UTIL_SET_SPEC: Invalid data value: i=%lu, val=%g\n",i, val);
            return GRIB_ENCODING_ERROR;
        }
    }
    return GRIB_SUCCESS;
}*/

static int check_geometry(grib_handle* handle, const grib_util_grid_spec* spec,
                          size_t data_values_count, bool specified_as_global)
{
    int err = 0;

    if (spec->pl && spec->pl_size != 0 &&
        (spec->grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_GG || spec->grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_ROTATED_GG)) {
        if (specified_as_global) {
            char msg[100] = {0,};
            size_t sum = 0;
            strcpy(msg, "Specified to be global (in spec)");
            sum = sum_of_pl_array(spec->pl, spec->pl_size);
            if (sum != data_values_count) {
                fprintf(stderr, "%s: Invalid reduced gaussian grid: %s but data_values_count != sum_of_pl_array (%zu!=%zu)\n",
                        __func__, msg, data_values_count, sum);
                return GRIB_WRONG_GRID;
            }
        }
    }
    return err;
}

#if defined(CHECK_HANDLE_AGAINST_SPEC)
/* Check what is coded in the handle is what is requested by the spec. */
/* Return GRIB_SUCCESS if the geometry matches, otherwise the error code */
static int check_handle_against_spec(const grib_handle* handle, const long edition,
        const grib_util_grid_spec* spec, int global_grid)
{
    int err = 0;
    int check_latitudes = 1;
    int check_longitudes = 1;
    long angleSubdivisions = 0;
    double angular_precision = 1.0/1000.0; /* millidegree by default */
    double tolerance = 0;

    if (edition == 2) {
        return GRIB_SUCCESS;  /* For now only do checks on edition 1 */
    }

    if ((err = grib_get_long(handle, "angleSubdivisions", &angleSubdivisions))==GRIB_SUCCESS) {
        angular_precision = 1.0/angleSubdivisions;
    }
    tolerance = angular_precision/2.0;

    if (spec->grid_type == GRIB_UTIL_GRID_SPEC_POLAR_STEREOGRAPHIC ||
        spec->grid_type == GRIB_UTIL_GRID_SPEC_SH)
    {
        return GRIB_SUCCESS;
    }

    /* Cannot check latitudes of Gaussian grids because are always sub-millidegree */
    /* and for GRIB1 will differ from the encoded values. We accept this discrepancy! */
    if (spec->grid_type == GRIB_UTIL_GRID_SPEC_REGULAR_GG ||
        spec->grid_type == GRIB_UTIL_GRID_SPEC_ROTATED_GG ||
        spec->grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_GG ||
        spec->grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_ROTATED_GG)
    {
        if (edition == 1) {
            check_latitudes = 0;
        }
    }

    /* GRIB-922 */
    /* Specification was to make the resulting grid global so no point checking for */
    /* input lat/lon values as they would be reset by setting the "global" key to 1 */
    if (global_grid)
    {
        check_latitudes = check_longitudes = 0;
    }

    if (check_latitudes) {
        double lat1, lat2;
        const double lat1spec = normalise_angle(spec->latitudeOfFirstGridPointInDegrees);
        const double lat2spec = normalise_angle(spec->latitudeOfLastGridPointInDegrees);
        if ((err = grib_get_double(handle, "latitudeOfFirstGridPointInDegrees", &lat1))!=0) return err;
        if ((err = grib_get_double(handle, "latitudeOfLastGridPointInDegrees", &lat2))!=0) return err;
        lat1 = normalise_angle(lat1);
        lat2 = normalise_angle(lat2);

        if (angle_too_small(lat1spec, angular_precision)) {
            fprintf(stderr, "Failed to encode latitude of first grid point %.10e: less than angular precision\n",lat1spec);
            return GRIB_WRONG_GRID;
        }
        if (angle_too_small(lat2spec, angular_precision)) {
            fprintf(stderr, "Failed to encode latitude of last grid point %.10e: less than angular precision\n", lat2spec);
            return GRIB_WRONG_GRID;
        }

        if (!DBL_EQUAL(lat1spec, lat1, tolerance)) {
            fprintf(stderr, "Failed to encode latitude of first grid point: spec=%.10e val=%.10e\n", lat1spec, lat1);
            return GRIB_WRONG_GRID;
        }
        if (!DBL_EQUAL(lat2spec, lat2, tolerance)) {
            fprintf(stderr, "Failed to encode latitude of last grid point: spec=%.10e val=%.10e\n", lat2spec, lat2);
            return GRIB_WRONG_GRID;
        }
    }

    if (check_longitudes) {
        double lon1, lon2;
        const double lon1spec = normalise_angle(spec->longitudeOfFirstGridPointInDegrees);
        const double lon2spec = normalise_angle(spec->longitudeOfLastGridPointInDegrees);
        if ((err = grib_get_double(handle, "longitudeOfFirstGridPointInDegrees", &lon1))!=0) return err;
        if ((err = grib_get_double(handle, "longitudeOfLastGridPointInDegrees", &lon2))!=0) return err;
        lon1 = normalise_angle(lon1);
        lon2 = normalise_angle(lon2);

        if (angle_too_small(lon1spec, angular_precision)) {
            fprintf(stderr, "Failed to encode longitude of first grid point %.10e: less than angular precision\n", lon1spec);
            return GRIB_WRONG_GRID;
        }
        if (angle_too_small(lon2spec, angular_precision)) {
            fprintf(stderr, "Failed to encode longitude of last grid point %.10e: less than angular precision\n", lon2spec);
            return GRIB_WRONG_GRID;
        }

        if (!DBL_EQUAL(lon1spec, lon1, tolerance)) {
            fprintf(stderr, "Failed to encode longitude of first grid point: spec=%.10e val=%.10e\n", lon1spec, lon1);
            return GRIB_WRONG_GRID;
        }
        if (!DBL_EQUAL(lon2spec, lon2, tolerance)){
            fprintf(stderr, "Failed to encode longitude of last grid point: spec=%.10e val=%.10e\n",  lon2spec, lon2);
            return GRIB_WRONG_GRID;
        }
    }

    if (spec->grid_type == GRIB_UTIL_GRID_SPEC_ROTATED_LL ||
        spec->grid_type == GRIB_UTIL_GRID_SPEC_ROTATED_GG ||
        spec->grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_ROTATED_GG)
    {
        double latp, lonp;
        const double latspec = normalise_angle(spec->latitudeOfSouthernPoleInDegrees);
        const double lonspec = normalise_angle(spec->longitudeOfSouthernPoleInDegrees);
        if ((err = grib_get_double(handle, "latitudeOfSouthernPoleInDegrees", &latp))!=0)  return err;
        if ((err = grib_get_double(handle, "longitudeOfSouthernPoleInDegrees", &lonp))!=0)  return err;
        latp = normalise_angle(latp);
        lonp = normalise_angle(lonp);

        if (!DBL_EQUAL(latspec, latp, tolerance)) {
            fprintf(stderr, "Failed to encode latitude of southern pole: spec=%.10e val=%.10e\n",latspec,latp);
            return GRIB_WRONG_GRID;
        }
        if (!DBL_EQUAL(lonspec, lonp, tolerance)) {
            fprintf(stderr, "Failed to encode longitude of southern pole: spec=%.10e val=%.10e\n",lonspec,lonp);
            return GRIB_WRONG_GRID;
        }
    }
    return GRIB_SUCCESS;
}
#endif

static bool grid_type_is_supported_in_edition(const int spec_grid_type, const long edition)
{
    if (edition == 1) {
        if (spec_grid_type == GRIB_UTIL_GRID_SPEC_UNSTRUCTURED ||
            spec_grid_type == GRIB_UTIL_GRID_SPEC_HEALPIX ||
            spec_grid_type == GRIB_UTIL_GRID_SPEC_LAMBERT_AZIMUTHAL_EQUAL_AREA)
        {
            return false;
        }
    }
    return true;
}

static const char* get_grid_type_name(const int spec_grid_type)
{
    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_REGULAR_LL)
        return "regular_ll";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_ROTATED_LL)
        return "rotated_ll";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_REGULAR_GG)
        return "regular_gg";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_ROTATED_GG)
        return "rotated_gg";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_LL)
        return "reduced_ll";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_POLAR_STEREOGRAPHIC)
        return "polar_stereographic";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_GG)
        return "reduced_gg";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_SH)
        return "sh";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_ROTATED_GG)
        return "reduced_rotated_gg";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_LAMBERT_AZIMUTHAL_EQUAL_AREA)
        return "lambert_azimuthal_equal_area";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_LAMBERT_CONFORMAL)
        return "lambert";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_HEALPIX)
        return "healpix";

    if (spec_grid_type == GRIB_UTIL_GRID_SPEC_UNSTRUCTURED)
        return "unstructured_grid";

    return NULL;
}

static bool is_constant_field(const double missingValue, const double* data_values, size_t data_values_count)
{
    size_t ii    = 0;
    bool constant = true;
    double value = missingValue;

    for (ii = 0; ii < data_values_count; ii++) {
        if (data_values[ii] != missingValue) {
            if (value == missingValue) {
                value = data_values[ii];
            }
            else {
                if (value != data_values[ii]) {
                    constant = false;
                    break;
                }
            }
        }
    }
    return constant;
}

// Utility function for when we fail to set the GRIB data values.
// Write out a text file called error.data containing the count of values
// and the actual values as doubles
static int write_out_error_data_file(const double* data_values, size_t data_values_count)
{
    FILE* ferror = fopen("error.data", "w");
    size_t lcount = 0;
    fprintf(ferror, "# data_values_count=%zu\n", data_values_count);
    fprintf(ferror, "set values={ ");
    for (size_t ii = 0; ii < data_values_count - 1; ii++) {
        fprintf(ferror, "%g, ", data_values[ii]);
        if (lcount > 10) {
            fprintf(ferror, "\n");
            lcount = 0;
        }
        lcount++;
    }
    fprintf(ferror, "%g }", data_values[data_values_count - 1]);
    fclose(ferror);
    return GRIB_SUCCESS;
}

static long get_bitsPerValue_for_packingType(const int specPackingType, const long specBitsPerValue)
{
    if (specPackingType == GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE) {
        if (specBitsPerValue > 60) return 60;
    }
    else if (specPackingType == GRIB_UTIL_PACKING_TYPE_GRID_SECOND_ORDER) {
        if (specBitsPerValue > 60) return 32;
    }
    else if (specPackingType == GRIB_UTIL_PACKING_TYPE_CCSDS) {
        if (specBitsPerValue > 32) return 32;
    }
    return specBitsPerValue; //original
}

static int get_grib_sample_name(grib_handle* h, long editionNumber,
                                const grib_util_grid_spec* spec, const char* grid_type, char* sample_name)
{
    const size_t sample_name_len = 1024;
    switch (spec->grid_type) {
        case GRIB_UTIL_GRID_SPEC_REDUCED_GG:
        case GRIB_UTIL_GRID_SPEC_REDUCED_ROTATED_GG:
            // Choose a sample with the right Gaussian number and edition
            snprintf(sample_name, sample_name_len, "%s_pl_%ld_grib%ld", grid_type, spec->N, editionNumber);
            if (spec->pl && spec->pl_size) {
                // GRIB-834: pl is given so can use any of the reduced_gg_pl samples
                snprintf(sample_name, sample_name_len, "%s_pl_grib%ld", grid_type, editionNumber);
            }
            break;
        case GRIB_UTIL_GRID_SPEC_LAMBERT_AZIMUTHAL_EQUAL_AREA:
        case GRIB_UTIL_GRID_SPEC_UNSTRUCTURED:
        case GRIB_UTIL_GRID_SPEC_LAMBERT_CONFORMAL:
        case GRIB_UTIL_GRID_SPEC_HEALPIX:
            snprintf(sample_name, sample_name_len, "GRIB%ld", editionNumber);
            break;
        case GRIB_UTIL_GRID_SPEC_REDUCED_LL:
            snprintf(sample_name, sample_name_len, "%s_sfc_grib%ld", grid_type, editionNumber);
            break;
        default:
            snprintf(sample_name, sample_name_len, "%s_pl_grib%ld", grid_type, editionNumber);
    }

    if (spec->pl && spec->grid_name) {
        // Cannot have BOTH pl and grid name specified
        fprintf(stderr, "%s: Cannot set BOTH spec.pl and spec.grid_name!\n", __func__);
        return GRIB_INTERNAL_ERROR;
    }
    if (spec->grid_name) {
        snprintf(sample_name, sample_name_len, "%s_grib%ld", spec->grid_name, editionNumber);
    }

    return GRIB_SUCCESS;
}

grib_handle* grib_util_set_spec(grib_handle* h,
                                 const grib_util_grid_spec* spec,
                                 const grib_util_packing_spec* packing_spec,
                                 int flags,
                                 const double* data_values,
                                 size_t data_values_count,
                                 int* err)
{
#define SET_LONG_VALUE(n, v)                       \
    do {                                           \
        ECCODES_ASSERT(count < 1024);                      \
        values[count].name       = n;              \
        values[count].type       = GRIB_TYPE_LONG; \
        values[count].long_value = v;              \
        count++;                                   \
    } while (0)
#define SET_DOUBLE_VALUE(n, v)                         \
    do {                                               \
        ECCODES_ASSERT(count < 1024);                          \
        values[count].name         = n;                \
        values[count].type         = GRIB_TYPE_DOUBLE; \
        values[count].double_value = v;                \
        count++;                                       \
    } while (0)
#define SET_STRING_VALUE(n, v)                         \
    do {                                               \
        ECCODES_ASSERT(count < 1024);                          \
        values[count].name         = n;                \
        values[count].type         = GRIB_TYPE_STRING; \
        values[count].string_value = v;                \
        count++;                                       \
    } while (0)

#define COPY_SPEC_LONG(x)                          \
    do {                                           \
        ECCODES_ASSERT(count < 1024);                      \
        values[count].name       = #x;             \
        values[count].type       = GRIB_TYPE_LONG; \
        values[count].long_value = spec->x;        \
        count++;                                   \
    } while (0)
#define COPY_SPEC_DOUBLE(x)                            \
    do {                                               \
        ECCODES_ASSERT(count < 1024);                          \
        values[count].name         = #x;               \
        values[count].type         = GRIB_TYPE_DOUBLE; \
        values[count].double_value = spec->x;          \
        count++;                                       \
    } while (0)

    grib_values values[1024] = {{0,},};
    const grib_context* c = grib_context_get_default();
    grib_handle* h_out    = NULL;
    grib_handle* h_sample = NULL;
    const char* grid_type = NULL;
    char sample_name[1024]; // name of the GRIB sample file
    char input_grid_type[100];
    char input_packing_type[100];
    long editionNumber = 0;
    size_t count = 0, len = 100, slen = 20, input_grid_type_len = 100;
    double laplacianOperator;
    int i = 0, packingTypeIsSet = 0, setSecondOrder = 0, setJpegPacking = 0, setCcsdsPacking = 0;
    bool convertEditionEarlier     = false; // For cases when we cannot set some keys without converting
    bool grib1_high_resolution_fix = false; // See GRIB-863
    bool global_grid               = false;
    int expandBoundingBox         = 0;

    ECCODES_ASSERT(h);

    // Get edition number from input handle
    if ((*err = grib_get_long(h, "edition", &editionNumber)) != 0) {
        if (c->write_on_fail) grib_write_message(h, "error.grib", "w");
        return NULL;
    }

    if (packing_spec->deleteLocalDefinition) {
        SET_LONG_VALUE("deleteLocalDefinition", 1);
    }

    grib_get_string(h, "packingType", input_packing_type, &len);

    // ECC-1201, ECC-1529, ECC-1530: Make sure input packing type is preserved
    if (packing_spec->packing == GRIB_UTIL_PACKING_SAME_AS_INPUT &&
        packing_spec->packing_type == GRIB_UTIL_PACKING_TYPE_SAME_AS_INPUT)
    {
        if (STR_EQUAL(input_packing_type, "grid_ieee")) {
            SET_STRING_VALUE("packingType", input_packing_type);
        }
        if (STR_EQUAL(input_packing_type, "grid_ccsds")) {
            setCcsdsPacking = 1;
        }
        if (STR_EQUAL(input_packing_type, "grid_second_order")) {
            setSecondOrder = 1;
        }
    }

    /*if ( (*err=check_values(data_values, data_values_count))!=GRIB_SUCCESS ) {
        fprintf(stderr,"GRIB_UTIL_SET_SPEC: Data values check failed! %s\n", grib_get_error_message(*err));
        goto cleanup;
    }*/

    /* ECC-1269:
     *  Code that was here was moved to "deprecated" directory
     *  See grib_util.GRIB_UTIL_SET_SPEC_FLAGS_ONLY_PACKING.
     *  Dealing with obsolete option GRIB_UTIL_SET_SPEC_FLAGS_ONLY_PACKING
    */

    grid_type = get_grid_type_name(spec->grid_type);
    if (!grid_type) {
        fprintf(stderr, "%s: Unknown spec.grid_type (%d)\n", __func__, spec->grid_type);
        *err = GRIB_NOT_IMPLEMENTED;
        return NULL;
    }
    SET_STRING_VALUE("gridType", grid_type);

    // The "pl" is given from the template, but "section_copy" will take care of setting the right headers
    if (get_grib_sample_name(h, editionNumber, spec, grid_type, sample_name) != GRIB_SUCCESS) {
        goto cleanup;
    }

    if (!grid_type_is_supported_in_edition(spec->grid_type, editionNumber)) {
        fprintf(stderr, "ECCODES WARNING %s: '%s' specified "
                        "but input is GRIB edition %ld. Output must be a higher edition!\n",
                        __func__, grid_type, editionNumber);
        convertEditionEarlier = true;
    }

    h_sample = grib_handle_new_from_samples(NULL, sample_name);
    if (!h_sample) {
        *err = GRIB_INVALID_FILE;
        return NULL;
    }

    // Set grid
    switch (spec->grid_type) {
        case GRIB_UTIL_GRID_SPEC_REGULAR_LL:
        case GRIB_UTIL_GRID_SPEC_ROTATED_LL:

            COPY_SPEC_LONG(bitmapPresent);
            if (spec->missingValue)
                COPY_SPEC_DOUBLE(missingValue);

            SET_LONG_VALUE("ijDirectionIncrementGiven", 1);
            if (editionNumber == 1) {
                // GRIB-863: GRIB1 cannot represent increments less than a millidegree
                if (!angle_can_be_encoded(h, spec->iDirectionIncrementInDegrees) ||
                    !angle_can_be_encoded(h, spec->jDirectionIncrementInDegrees)) {
                    grib1_high_resolution_fix = true;
                    // Set flag to compute the increments
                    SET_LONG_VALUE("ijDirectionIncrementGiven", 0);
                }
            }

            // default iScansNegatively=0 jScansPositively=0 is ok
            COPY_SPEC_LONG(iScansNegatively);
            COPY_SPEC_LONG(jScansPositively);

            COPY_SPEC_LONG(Ni);
            COPY_SPEC_LONG(Nj);

            COPY_SPEC_DOUBLE(iDirectionIncrementInDegrees);
            COPY_SPEC_DOUBLE(jDirectionIncrementInDegrees);

            COPY_SPEC_DOUBLE(longitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(longitudeOfLastGridPointInDegrees);

            COPY_SPEC_DOUBLE(latitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(latitudeOfLastGridPointInDegrees);

            break;

        case GRIB_UTIL_GRID_SPEC_REGULAR_GG:
        case GRIB_UTIL_GRID_SPEC_ROTATED_GG:

            COPY_SPEC_LONG(bitmapPresent);
            if (spec->missingValue) COPY_SPEC_DOUBLE(missingValue);
            SET_LONG_VALUE("ijDirectionIncrementGiven", 1);

            // TODO(masn): add ECCODES_ASSERT
            COPY_SPEC_LONG(Ni);
            COPY_SPEC_DOUBLE(iDirectionIncrementInDegrees);
            COPY_SPEC_LONG(Nj);
            COPY_SPEC_LONG(N);

            COPY_SPEC_DOUBLE(longitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(longitudeOfLastGridPointInDegrees);

            COPY_SPEC_DOUBLE(latitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(latitudeOfLastGridPointInDegrees);
            break;

        case GRIB_UTIL_GRID_SPEC_REDUCED_LL:
            COPY_SPEC_LONG(bitmapPresent);
            if (spec->missingValue) COPY_SPEC_DOUBLE(missingValue);
            SET_LONG_VALUE("ijDirectionIncrementGiven", 0);

            COPY_SPEC_LONG(Nj);
            COPY_SPEC_DOUBLE(longitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(longitudeOfLastGridPointInDegrees);

            COPY_SPEC_DOUBLE(latitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(latitudeOfLastGridPointInDegrees);
            break;

        case GRIB_UTIL_GRID_SPEC_POLAR_STEREOGRAPHIC:
            COPY_SPEC_LONG(bitmapPresent);
            if (spec->missingValue) COPY_SPEC_DOUBLE(missingValue);

            COPY_SPEC_DOUBLE(longitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(latitudeOfFirstGridPointInDegrees);
            COPY_SPEC_LONG(Ni);
            COPY_SPEC_LONG(Nj);

            // default iScansNegatively=0 jScansPositively=0 is ok
            COPY_SPEC_LONG(iScansNegatively);
            COPY_SPEC_LONG(jScansPositively);
            COPY_SPEC_DOUBLE(orientationOfTheGridInDegrees);
            COPY_SPEC_LONG(DxInMetres);
            COPY_SPEC_LONG(DyInMetres);
            break;

        case GRIB_UTIL_GRID_SPEC_LAMBERT_AZIMUTHAL_EQUAL_AREA:
            COPY_SPEC_LONG(bitmapPresent);
            if (spec->missingValue) COPY_SPEC_DOUBLE(missingValue);

            COPY_SPEC_DOUBLE(longitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(latitudeOfFirstGridPointInDegrees);
            COPY_SPEC_LONG(Ni); // same as Nx
            COPY_SPEC_LONG(Nj); // same as Ny
            COPY_SPEC_LONG(iScansNegatively);
            COPY_SPEC_LONG(jScansPositively);

            // TODO(masn): pass in extra keys e.g. Dx, Dy, standardParallel and centralLongitude
            // COPY_SPEC_LONG(DxInMetres);
            // COPY_SPEC_LONG(DyInMetres);
            // COPY_SPEC_LONG(xDirectionGridLengthInMillimetres);
            // COPY_SPEC_LONG(yDirectionGridLengthInMillimetres);
            // COPY_SPEC_LONG(standardParallelInMicrodegrees);
            // COPY_SPEC_LONG(centralLongitudeInMicrodegrees);

            break;
        case GRIB_UTIL_GRID_SPEC_UNSTRUCTURED:
            COPY_SPEC_LONG(bitmapPresent);
            if (spec->missingValue) COPY_SPEC_DOUBLE(missingValue);
            // TODO(masn): Other keys
            break;
        case GRIB_UTIL_GRID_SPEC_LAMBERT_CONFORMAL:
            COPY_SPEC_LONG(bitmapPresent);
            if (spec->missingValue) COPY_SPEC_DOUBLE(missingValue);
            COPY_SPEC_DOUBLE(longitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(latitudeOfFirstGridPointInDegrees);
            COPY_SPEC_LONG(Ni); // same as Nx
            COPY_SPEC_LONG(Nj); // same as Ny

            COPY_SPEC_LONG(iScansNegatively);
            COPY_SPEC_LONG(jScansPositively);
            COPY_SPEC_DOUBLE(latitudeOfSouthernPoleInDegrees);
            COPY_SPEC_DOUBLE(longitudeOfSouthernPoleInDegrees);
            COPY_SPEC_LONG(uvRelativeToGrid);

            // Note: DxInMetres and DyInMetres
            // should be 'double' and not integer. WMO GRIB2 uses millimetres!
            // TODO(masn): Add other keys like Latin1, LoV etc
            break;
        case GRIB_UTIL_GRID_SPEC_HEALPIX:
            COPY_SPEC_LONG(bitmapPresent);
            if (spec->missingValue) COPY_SPEC_DOUBLE(missingValue);
            COPY_SPEC_LONG(N); // Nside
            COPY_SPEC_DOUBLE(longitudeOfFirstGridPointInDegrees);
            break;

        case GRIB_UTIL_GRID_SPEC_REDUCED_GG:
        case GRIB_UTIL_GRID_SPEC_REDUCED_ROTATED_GG:

            COPY_SPEC_LONG(bitmapPresent);
            if (spec->missingValue) COPY_SPEC_DOUBLE(missingValue);
            SET_LONG_VALUE("ijDirectionIncrementGiven", 0);

            COPY_SPEC_LONG(Nj);
            COPY_SPEC_LONG(N);
            COPY_SPEC_DOUBLE(longitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(longitudeOfLastGridPointInDegrees);
            COPY_SPEC_DOUBLE(latitudeOfFirstGridPointInDegrees);
            COPY_SPEC_DOUBLE(latitudeOfLastGridPointInDegrees);

            break;

        case GRIB_UTIL_GRID_SPEC_SH:
            *err = grib_get_string(h, "gridType", input_grid_type, &input_grid_type_len);

            SET_LONG_VALUE("J", spec->truncation);
            SET_LONG_VALUE("K", spec->truncation);
            SET_LONG_VALUE("M", spec->truncation);

            if (packing_spec->packing_type == GRIB_UTIL_PACKING_TYPE_SPECTRAL_COMPLEX) {
                const long JS = spec->truncation < 20 ? spec->truncation : 20;
                SET_STRING_VALUE("packingType", "spectral_complex");
                packingTypeIsSet = 1;
                SET_LONG_VALUE("JS", JS);
                SET_LONG_VALUE("KS", JS);
                SET_LONG_VALUE("MS", JS);
                if (packing_spec->packing == GRIB_UTIL_PACKING_USE_PROVIDED && editionNumber == 2) {
                    SET_LONG_VALUE("computeLaplacianOperator", 1);
                }
                else if ((!(*err) && strcmp(input_grid_type, "sh")) || packing_spec->computeLaplacianOperator) {
                    SET_LONG_VALUE("computeLaplacianOperator", 1);
                    if (packing_spec->truncateLaplacian)
                        SET_LONG_VALUE("truncateLaplacian", 1);
                }
                else {
                    SET_LONG_VALUE("computeLaplacianOperator", 0);
                    *err = grib_get_double(h, "laplacianOperator", &laplacianOperator);
                    if (packing_spec->truncateLaplacian)
                        SET_LONG_VALUE("truncateLaplacian", 1);
                    SET_DOUBLE_VALUE("laplacianOperator", packing_spec->laplacianOperator);
                    if (laplacianOperator) {
                        SET_DOUBLE_VALUE("laplacianOperator", laplacianOperator);
                    }
                }
            }
            break;
    }

    // Set rotation
    switch (spec->grid_type) {
        case GRIB_UTIL_GRID_SPEC_ROTATED_LL:
        case GRIB_UTIL_GRID_SPEC_ROTATED_GG:
        case GRIB_UTIL_GRID_SPEC_REDUCED_ROTATED_GG:
            COPY_SPEC_LONG(uvRelativeToGrid);
            COPY_SPEC_DOUBLE(latitudeOfSouthernPoleInDegrees);
            COPY_SPEC_DOUBLE(longitudeOfSouthernPoleInDegrees);
            COPY_SPEC_DOUBLE(angleOfRotationInDegrees);
            break;
    }

    // process packing options
    if (!packingTypeIsSet &&
        packing_spec->packing == GRIB_UTIL_PACKING_USE_PROVIDED &&
        strcmp(input_packing_type, "grid_simple_matrix")) {
        switch (packing_spec->packing_type) {
            case GRIB_UTIL_PACKING_TYPE_SPECTRAL_COMPLEX:
                if (strcmp(input_packing_type, "spectral_complex") && !strcmp(input_packing_type, "spectral_simple"))
                    SET_STRING_VALUE("packingType", "spectral_complex");
                break;
            case GRIB_UTIL_PACKING_TYPE_SPECTRAL_SIMPLE:
                if (strcmp(input_packing_type, "spectral_simple") && !strcmp(input_packing_type, "spectral_complex"))
                    SET_STRING_VALUE("packingType", "spectral_simple");
                break;
            case GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE:
                if (strcmp(input_packing_type, "grid_simple") && !strcmp(input_packing_type, "grid_complex"))
                    SET_STRING_VALUE("packingType", "grid_simple");
                break;
            case GRIB_UTIL_PACKING_TYPE_GRID_COMPLEX:
                if (!STR_EQUAL(input_packing_type, "grid_complex")) {
                    SET_STRING_VALUE("packingType", "grid_complex");
                    convertEditionEarlier = true;
                }
                break;
            case GRIB_UTIL_PACKING_TYPE_JPEG:
                /* Have to delay JPEG packing:
                 * Reason 1: It is not available in GRIB1 and so we have to wait until we change edition
                 * Reason 2: It has to be done AFTER we set the data values
                 */
                if (strcmp(input_packing_type, "grid_jpeg"))
                    setJpegPacking = 1;
                break;
            case GRIB_UTIL_PACKING_TYPE_CCSDS:
                /* Have to delay CCSDS packing:
                 * Reason 1: It is not available in GRIB1 and so we have to wait until we change edition
                 * Reason 2: It has to be done AFTER we set the data values
                 */
                if (!STR_EQUAL(input_packing_type, "grid_ccsds"))
                    setCcsdsPacking = 1;
                break;
            case GRIB_UTIL_PACKING_TYPE_IEEE:
                if ( !STR_EQUAL(input_packing_type, "grid_ieee") )
                    SET_STRING_VALUE("packingType", "grid_ieee");
                break;
            case GRIB_UTIL_PACKING_TYPE_GRID_SECOND_ORDER:
                /* we delay the set of grid_second_order because we don't want
                   to do it on a field with bitsPerValue=0 */
                setSecondOrder = 1;
                break;
            default:
                fprintf(stderr, "%s: invalid packing_spec.packing_type (%ld)\n", __func__, packing_spec->packing_type);
                *err = GRIB_INTERNAL_ERROR;
                goto cleanup;
        }
    }
    if (strcmp(input_packing_type, "grid_simple_matrix") == 0) {
        long numberOfDirections, numberOfFrequencies;
        int keep_matrix = h->context->keep_matrix;
        if (packing_spec->packing_type == GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE) {
            keep_matrix = 0; // ECC-911
        }
        if (keep_matrix) {
            SET_STRING_VALUE("packingType", "grid_simple_matrix");
            if (GRIB_SUCCESS == grib_get_long(h, "numberOfDirections", &numberOfDirections)) {
                grib_get_long(h, "numberOfDirections", &numberOfDirections);
                SET_LONG_VALUE("NC1", numberOfDirections);
                grib_get_long(h, "numberOfFrequencies", &numberOfFrequencies);
                SET_LONG_VALUE("NC2", numberOfFrequencies);
                SET_LONG_VALUE("physicalFlag1", 1);
                SET_LONG_VALUE("physicalFlag2", 2);
                SET_LONG_VALUE("NR", 1);
                SET_LONG_VALUE("NC", 1);
            }
        }
        else {
            SET_STRING_VALUE("packingType", "grid_simple");
        }
    }

    switch (packing_spec->accuracy) {
        case GRIB_UTIL_ACCURACY_SAME_BITS_PER_VALUES_AS_INPUT: {
            long bitsPerValue = 0;
            if ((packing_spec->packing_type == GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE ||
                 packing_spec->packing_type == GRIB_UTIL_PACKING_TYPE_CCSDS)      &&
                strcmp(input_packing_type, "grid_ieee")==0)
            {
                SET_LONG_VALUE("bitsPerValue", 32);
            }
            else
            {
                ECCODES_ASSERT(grib_get_long(h, "bitsPerValue", &bitsPerValue) == 0);
                SET_LONG_VALUE("bitsPerValue", bitsPerValue);
            }
        }
        break;

        case GRIB_UTIL_ACCURACY_USE_PROVIDED_BITS_PER_VALUES: {
            // See ECC-1921
            const long bitsPerValue = get_bitsPerValue_for_packingType(packing_spec->packing_type, packing_spec->bitsPerValue);
            if (bitsPerValue != packing_spec->bitsPerValue) {
                fprintf(stderr, "ECCODES WARNING :  Cannot pack with requested bitsPerValue (%ld). Using %ld\n",
                        packing_spec->bitsPerValue, bitsPerValue);
            }
            SET_LONG_VALUE("bitsPerValue", bitsPerValue);
        }
        break;

        case GRIB_UTIL_ACCURACY_SAME_DECIMAL_SCALE_FACTOR_AS_INPUT: {
            long decimalScaleFactor = 0;
            ECCODES_ASSERT(grib_get_long(h, "decimalScaleFactor", &decimalScaleFactor) == 0);
            SET_LONG_VALUE("decimalScaleFactor", decimalScaleFactor);
        }
        break;

        case GRIB_UTIL_ACCURACY_USE_PROVIDED_DECIMAL_SCALE_FACTOR:
            SET_LONG_VALUE("decimalScaleFactor", packing_spec->decimalScaleFactor);
            break;

        default:
            fprintf(stderr, "%s: invalid packing_spec.accuracy (%ld)\n", __func__, packing_spec->accuracy);
            grib_handle_delete(h_sample);
            *err = GRIB_INTERNAL_ERROR;
            goto cleanup;
    }

    if (packing_spec->extra_settings_count) {
        for (i = 0; i < packing_spec->extra_settings_count; i++) {
            ECCODES_ASSERT(count < 1024);
            if (strcmp(packing_spec->extra_settings[i].name, "expandBoundingBox") == 0) {
                if (packing_spec->extra_settings[i].long_value == 1) {
                    /* ECC-625: Request is for expansion of bounding box (sub-area).
                     * This is also called the "snap-out" policy */
                    expandBoundingBox = 1;
                }
            }
            else {
                values[count++] = packing_spec->extra_settings[i];
                if (strcmp(packing_spec->extra_settings[i].name, "global") == 0 &&
                    packing_spec->extra_settings[i].long_value == 1) {
                    /* GRIB-922: Request is for a global grid. Setting this key will
                     * calculate the lat/lon values. So the spec's lat/lon can be ignored */
                    global_grid = true;
                }
            }
        }
    }
    // grib_write_message(h,"input.grib","w");
    // grib_write_message(h_sample,"geo.grib","w");
    // copy product and local sections from h to h_sample handle and store in h_out
    if ((h_out = grib_util_sections_copy(h, h_sample, GRIB_SECTION_PRODUCT | GRIB_SECTION_LOCAL, err)) == NULL) {
        goto cleanup;
    }

    grib_handle_delete(h_sample);
    ECCODES_ASSERT(*err == 0);

    // GRIB-857: Set "pl" array if provided (For reduced Gaussian grids)
    ECCODES_ASSERT(spec->pl_size >= 0);
    if (spec->pl && spec->pl_size == 0) {
        fprintf(stderr, "%s: pl array not NULL but pl_size == 0!\n", __func__);
        goto cleanup;
    }
    if (spec->pl_size > 0 && spec->pl == NULL) {
        fprintf(stderr, "%s: pl_size not zero but pl array == NULL!\n", __func__);
        goto cleanup;
    }

    if (spec->pl_size != 0 && (spec->grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_GG || spec->grid_type == GRIB_UTIL_GRID_SPEC_REDUCED_ROTATED_GG)) {
        *err = grib_set_long_array(h_out, "pl", spec->pl, spec->pl_size);
        if (*err) {
            fprintf(stderr, "%s: Cannot set pl: %s\n", __func__, grib_get_error_message(*err));
            goto cleanup;
        }
        if (global_grid) {
            size_t sum = sum_of_pl_array(spec->pl, spec->pl_size);
            if (data_values_count != sum) {
                fprintf(stderr, "%s: invalid reduced gaussian grid: "
                        "specified as global, data_values_count=%zu but sum of pl array=%zu\n",
                        __func__, data_values_count, sum);
                *err = GRIB_WRONG_GRID;
                goto cleanup;
            }
        }
    }

    if (h->context->debug == -1) {
        fprintf(stderr, "ECCODES DEBUG grib_util: global_grid = %d\n", global_grid);
        fprintf(stderr, "ECCODES DEBUG grib_util: expandBoundingBox = %d\n", expandBoundingBox);
        print_values(h->context, spec, packing_spec, input_packing_type, data_values, data_values_count, values, count);
    }

    // Apply adjustments to bounding box if needed
    if (expandBoundingBox) {
        if ((*err = expand_bounding_box(h_out, values, count)) != 0) {
            fprintf(stderr, "%s: Cannot expand bounding box: %s\n", __func__, grib_get_error_message(*err));
            if (h->context->write_on_fail)
                grib_write_message(h_out, "error.grib", "w");
            goto cleanup;
        }
    }

    if (convertEditionEarlier && packing_spec->editionNumber > 1) {
        // Note:
        // If the input is GRIB1 and the requested grid type is HealPix or ORCA etc,
        // we deliberately fail unless the user specifies edition conversion.
        // i.e., we do not automatically convert edition
        // If we later change our mind, we need to change editionNumber to 2 here:
        //   long new_edition = packing_spec->editionNumber;
        //   if (new_edition == 0) new_edition = 2;
        //
        *err = grib_set_long(h_out, "edition", packing_spec->editionNumber);
        if (*err) {
            fprintf(stderr, "%s: Cannot convert to edition %ld.\n", __func__, packing_spec->editionNumber);
            goto cleanup;
        }
    }

    if ((*err = grib_set_values(h_out, values, count)) != 0) {
        fprintf(stderr, "%s: Cannot set key values: %s\n", __func__, grib_get_error_message(*err));
        for (i = 0; i < count; i++)
            if (values[i].error) fprintf(stderr, " %s %s\n", values[i].name, grib_get_error_message(values[i].error));
        goto cleanup;
    }

    if ((*err = grib_set_double_array(h_out, "values", data_values, data_values_count)) != GRIB_SUCCESS) {
        write_out_error_data_file(data_values, data_values_count);
        if (c->write_on_fail) grib_write_message(h_out, "error.grib", "w");
        goto cleanup;
    }

    /* grib_write_message(h_out,"h.grib","w"); */
    /* if the field is empty GRIBEX is packing as simple*/
    /*    if (!strcmp(input_packing_type,"grid_simple_matrix")) {
        long numberOfValues;
        grib_get_long(h_out,"numberOfValues",&numberOfValues);
        if (numberOfValues==0)  {
            slen=11;
            grib_set_string(h_out,"packingType","grid_simple",&slen);
        }
    }   */

    if (grib1_high_resolution_fix) {
        // GRIB-863: must set increments to MISSING
        // increments are not coded in message but computed
        if ((*err = grib_set_missing(h_out, "iDirectionIncrement")) != 0) {
            fprintf(stderr, "%s: Cannot set Di to missing: %s\n", __func__, grib_get_error_message(*err));
            goto cleanup;
        }
        if ((*err = grib_set_missing(h_out, "jDirectionIncrement")) != 0) {
            fprintf(stderr, "%s: Cannot set Dj to missing: %s\n", __func__, grib_get_error_message(*err));
            goto cleanup;
        }
    }

    //grib_dump_content(h_out, stdout,"debug", ~0, NULL);
    // convert to second_order if not constant field. (Also see ECC-326)
    if (setSecondOrder) {
        double missingValue = 0;
        grib_get_double(h_out, "missingValue", &missingValue);
        bool constant = is_constant_field(missingValue, data_values, data_values_count);

        if (!constant) {
            if (editionNumber == 1) {
                long numberOfGroups = 0;
                grib_handle* htmp   = grib_handle_clone(h_out);

                slen = 17;
                grib_set_string(htmp, "packingType", "grid_second_order", &slen);
                grib_get_long(htmp, "numberOfGroups", &numberOfGroups);
                // GRIBEX is not able to decode overflown numberOfGroups with SPD
                if (numberOfGroups > 65534 && h_out->context->no_spd) {
                    slen = 24;
                    grib_set_string(h_out, "packingType", "grid_second_order_no_SPD", &slen);
                    grib_handle_delete(htmp);
                }
                else {
                    grib_handle_delete(h_out);
                    h_out = htmp;
                }
            }
            else {
                slen = 17;
                grib_set_string(h_out, "packingType", "grid_second_order", &slen);
                *err = grib_set_double_array(h_out, "values", data_values, data_values_count);
                if (*err != GRIB_SUCCESS) {
                    fprintf(stderr, "%s: setting data values failed: %s\n", __func__, grib_get_error_message(*err));
                    goto cleanup;
                }
            }
        }
        else {
            if (h_out->context->gribex_mode_on) {
                h_out->context->gribex_mode_on = 0;
                grib_set_double_array(h_out, "values", data_values, data_values_count);
                h_out->context->gribex_mode_on = 1;
            }
        }
    }

    if (packing_spec->editionNumber && packing_spec->editionNumber != editionNumber) {
        *err = grib_set_long(h_out, "edition", packing_spec->editionNumber);
        if (*err != GRIB_SUCCESS) {
            fprintf(stderr, "%s: Failed to change edition to %ld: %s\n",
                    __func__, packing_spec->editionNumber, grib_get_error_message(*err));
            if (h->context->write_on_fail)
                grib_write_message(h_out, "error.grib", "w");
            goto cleanup;
        }
    }

    if (editionNumber > 1 || packing_spec->editionNumber > 1) { // ECC-353
        // Some packing types are not available in GRIB1 and have to be done AFTER we set data values
        if (setJpegPacking == 1) {
            *err = grib_set_string(h_out, "packingType", "grid_jpeg", &slen);
            if (*err != GRIB_SUCCESS) {
                fprintf(stderr, "%s: Failed to change packingType to JPEG: %s\n",
                        __func__, grib_get_error_message(*err));
                goto cleanup;
            }
        }
        if (setCcsdsPacking == 1) {
            *err = grib_set_string(h_out, "packingType", "grid_ccsds", &slen);
            if (*err != GRIB_SUCCESS) {
                fprintf(stderr, "%s: Failed to change packingType to CCSDS: %s\n",
                        __func__, grib_get_error_message(*err));
                goto cleanup;
            }
        }
    }

    if (packing_spec->deleteLocalDefinition) {
        grib_set_long(h_out, "deleteLocalDefinition", 1);
    }

    // ECC-445
    if (expandBoundingBox) {
        ECCODES_ASSERT(!global_grid); // ECC-576: "global" should not be set
    }

    if ((*err = check_geometry(h_out, spec, data_values_count, global_grid)) != GRIB_SUCCESS) {
        fprintf(stderr, "%s: Geometry check failed: %s\n", __func__, grib_get_error_message(*err));
        if (h->context->write_on_fail)
            grib_write_message(h_out, "error.grib", "w");
        goto cleanup;
    }

    /* Disable check: need to re-examine GRIB-864 */
//     if ( (*err = check_handle_against_spec(h_out, editionNumber, spec, global_grid)) != GRIB_SUCCESS) {
//         fprintf(stderr,"GRIB_UTIL_SET_SPEC: Geometry check failed: %s\n", grib_get_error_message(*err));
//         if (editionNumber == 1) {
//             fprintf(stderr,"Note: in GRIB edition 1 latitude and longitude values cannot be represented with sub-millidegree precision.\n");
//         }
//         if (c->write_on_fail) grib_write_message(h_out,"error.grib","w");
//         goto cleanup;
//     }

    if (h->context->debug == -1) fprintf(stderr, "ECCODES DEBUG grib_util: %s end\n",__func__);

    return h_out;

cleanup:
    grib_handle_delete(h_out);
    return NULL;
}

// int grib_moments(grib_handle* h, double east, double north, double west, double south, int order, double* moments, long* count)
// {
//     grib_iterator* iter = NULL;
//     int ret             = 0, i, j, l;
//     size_t n = 0, numberOfPoints = 0;
//     double *lat, *lon, *values;
//     double vlat, vlon, val;
//     double dx, dy, ddx, ddy;
//     double mass, centroidX, centroidY;
//     double missingValue;
//     grib_context* c = grib_context_get_default();

//     ret = grib_get_size(h, "values", &n);
//     if (ret)
//         return ret;

//     lat    = (double*)grib_context_malloc_clear(c, sizeof(double) * n);
//     lon    = (double*)grib_context_malloc_clear(c, sizeof(double) * n);
//     values = (double*)grib_context_malloc_clear(c, sizeof(double) * n);

//     iter           = grib_iterator_new(h, 0, &ret);
//     numberOfPoints = 0;
//     while (grib_iterator_next(iter, &vlat, &vlon, &val)) {
//         if (vlon >= east && vlon <= west && vlat >= south && vlat <= north) {
//             lat[numberOfPoints]    = vlat;
//             lon[numberOfPoints]    = vlon;
//             values[numberOfPoints] = val;
//             numberOfPoints++;
//         }
//     }
//     grib_iterator_delete(iter);
//     ret = grib_get_double(h, "missingValue", &missingValue);
//     centroidX = 0;
//     centroidY = 0;
//     mass      = 0;
//     *count    = 0;
//     for (i = 0; i < numberOfPoints; i++) {
//         if (values[i] != missingValue) {
//             centroidX += lon[i] * values[i];
//             centroidY += lat[i] * values[i];
//             mass += values[i];
//             (*count)++;
//         }
//     }
//     centroidX /= mass;
//     centroidY /= mass;
//     mass /= *count;
//     for (j = 0; j < order * order; j++)
//         moments[j] = 0;
//     for (i = 0; i < numberOfPoints; i++) {
//         if (values[i] != missingValue) {
//             dx  = (lon[i] - centroidX);
//             dy  = (lat[i] - centroidY);
//             ddx = 1;
//             for (j = 0; j < order; j++) {
//                 ddy = 1;
//                 for (l = 0; l < order; l++) {
//                     moments[j * order + l] += ddx * ddy * values[i];
//                     ddy *= dy;
//                 }
//                 ddx *= dx;
//             }
//         }
//     }
//     for (j = 0; j < order; j++) {
//         for (l = 0; l < order; l++) {
//             if (j + l > 1) {
//                 moments[j * order + l] = pow(fabs(moments[j * order + l]), 1.0 / (j + l)) / *count;
//             }
//             else {
//                 moments[j * order + l] /= *count;
//             }
//         }
//     }

//     grib_context_free(c, lat);
//     grib_context_free(c, lon);
//     grib_context_free(c, values);
//     (void)mass;

//     return ret;
// }

// Helper function for 'parse_keyval_string'
static void set_value(grib_values* value, char* str, int equal)
{
    char *p = 0, *q = 0, *s = 0;
    char buf[1000] = {0,};
    const grib_context* c = grib_context_get_default();

    value->equal = equal;
    q            = str;

    while (*q != '/' && *q != 0)
        q++;
    if (*q == '/') {
        s                 = grib_context_strdup(c, q + 1);
        value->next       = (grib_values*)grib_context_malloc_clear(c, sizeof(grib_values));
        value->next->type = value->type;
        value->next->name = grib_context_strdup(c, value->name);
        set_value(value->next, s, equal);
        grib_context_free(c, s);
    }

    memcpy(buf, str, q - str);

    switch (value->type) {
        case GRIB_TYPE_DOUBLE:
            value->double_value = strtod(buf, &p);
            if (*p != 0)
                value->has_value = 1;
            else if (!strcmp(str, "missing") ||
                     !strcmp(str, "MISSING") ||
                     !strcmp(str, "Missing")) {
                value->type      = GRIB_TYPE_MISSING;
                value->has_value = 1;
            }
            break;
        case GRIB_TYPE_LONG:
            errno = 0; // must clear errno before calling strtol
            value->long_value = strtol(buf, &p, 10);
            if (*p != 0)
                value->has_value = 1;
            else if (!strcmp(buf, "missing") ||
                     !strcmp(buf, "MISSING") ||
                     !strcmp(buf, "Missing")) {
                value->type      = GRIB_TYPE_MISSING;
                value->has_value = 1;
            }
            break;
        case GRIB_TYPE_STRING:
            if (!strcmp(buf, "missing") ||
                !strcmp(buf, "MISSING") ||
                !strcmp(buf, "Missing")) {
                value->type      = GRIB_TYPE_MISSING;
                value->has_value = 1;
            }
            else {
                value->string_value = grib_context_strdup(c, buf);
                value->has_value    = 1;
            }
            break;
        case GRIB_TYPE_UNDEFINED:
            errno = 0; // must clear errno before calling strtol
            value->long_value = strtol(buf, &p, 10);
            if (*p == 0) {
                // check the conversion from string to long
                if (errno == ERANGE && (value->long_value == LONG_MAX || value->long_value == LONG_MIN)) {
                    fprintf(stderr, "ECCODES WARNING :  Setting %s=%s causes overflow/underflow\n", value->name, buf);
                    fprintf(stderr, "ECCODES WARNING :  Value adjusted to %ld\n", value->long_value);
                    //perror("strtol");
                }
                value->type      = GRIB_TYPE_LONG;
                value->has_value = 1;
            }
            else {
                value->double_value = strtod(buf, &p);
                if (*p == 0) {
                    value->type      = GRIB_TYPE_DOUBLE;
                    value->has_value = 1;
                }
                else if (!strcmp(buf, "missing") ||
                         !strcmp(buf, "MISSING") ||
                         !strcmp(buf, "Missing")) {
                    value->type      = GRIB_TYPE_MISSING;
                    value->has_value = 1;
                }
                else {
                    value->string_value = grib_context_strdup(c, buf);
                    value->type         = GRIB_TYPE_STRING;
                    value->has_value    = 1;
                }
            }
            break;
    }
}

//
//  'grib_tool'        Optional tool name which is printed on error. Can be NULL
//  'arg'              The string to be parsed e.g. key1=value1,key2!=value2 etc (cannot be const)
//  'values_required'  If true then each key must have a value after it
//  'default_type'     The default type e.g. GRIB_TYPE_UNDEFINED or GRIB_TYPE_DOUBLE
//  'values'           The array we populate and return (output)
//  'count'            Number of elements (output). Must be initialised to the size of the values array
//
int parse_keyval_string(const char* grib_tool,
                        char* arg, int values_required, int default_type,
                        grib_values values[], int* count)
{
    char* p = NULL;
    char* lasts = NULL;
    int i = 0;
    if (arg == NULL) {
        *count = 0;
        return GRIB_SUCCESS;
    }
    /* Note: strtok modifies its input argument 'arg'
     * so it cannot be 'const'
     */
    p = strtok_r(arg, ",", &lasts);
    while (p != NULL) {
        values[i].name = (char*)calloc(1, strlen(p) + 1);
        ECCODES_ASSERT(values[i].name);
        strcpy((char*)values[i].name, p);
        p = strtok_r(NULL, ",", &lasts);
        i++;
        if (i >= *count) {
            fprintf(stderr, "Input string contains too many entries (max=%d)\n", *count);
            return GRIB_ARRAY_TOO_SMALL;
        }
    }
    *count = i;

    for (i = 0; i < *count; i++) {
        int equal   = 1;
        char* value = NULL;
        if (values_required) {
            // Can be either k=v or k!=v
            p = (char*)values[i].name;
            while (*p != '=' && *p != '!' && *p != '\0')
                p++;
            if (*p == '=') {
                *p = '\0';
                p++;
                value = p;
                equal = 1;
            }
            else if (*p == '!' && *(++p) == '=') {
                *p       = '\0';
                *(p - 1) = '\0';
                p++;
                value = p;
                equal = 0;
            }
            else {
                return GRIB_INVALID_ARGUMENT;
            }
        }
        p = (char*)values[i].name;
        while (*p != ':' && *p != '\0')
            p++;
        if (*p == ':') {
            values[i].type = grib_type_to_int(*(p + 1));
            if (*(p + 1) == 'n')
                values[i].type = CODES_NAMESPACE;
            *p = '\0';
            p++;
        }
        else {
            values[i].type = default_type;
        }
        if (values_required) {
            if (strlen(value) == 0) {
                if (grib_tool)
                    fprintf(stderr, "%s error: no value provided for key \"%s\"\n", grib_tool, values[i].name);
                else
                    fprintf(stderr, "Error: no value provided for key \"%s\"\n", values[i].name);
                return GRIB_INVALID_ARGUMENT;
            }
            set_value(&values[i], value, equal);
        }
    }
    return GRIB_SUCCESS;
}

// Return 1 if the productDefinitionTemplateNumber (GRIB2) is for EPS (ensemble) products
int grib2_is_PDTN_EPS(long pdtn)
{
    static int eps_pdtns[] = { 1, 11, 33, 34, 41, 43, 45, 47,
                               49, 54, 56, 58, 59, 60, 61, 63, 68, 71, 73, 77, 79,
                               81, 83, 84, 85, 92, 94, 96, 98 };
    size_t i = 0, num_epss = (sizeof(eps_pdtns) / sizeof(eps_pdtns[0]));
    for (i = 0; i < num_epss; ++i) {
        if (eps_pdtns[i] == pdtn) return 1;
    }
    return 0;
}

// Return 1 if the productDefinitionTemplateNumber (GRIB2) is for plain (vanilla) products
int grib2_is_PDTN_Plain(long pdtn)
{
    return (
        pdtn == 0 ||
        pdtn == 1 ||
        pdtn == 8 ||
        pdtn == 11);
}

// Return 1 if the productDefinitionTemplateNumber (GRIB2) is for atmospheric chemical constituents
int grib2_is_PDTN_Chemical(long pdtn)
{
    return (
        pdtn == 40 ||
        pdtn == 41 ||
        pdtn == 42 ||
        pdtn == 43);
}

// Return 1 if the productDefinitionTemplateNumber (GRIB2) is for
// atmospheric chemical constituents with source or sink
int grib2_is_PDTN_ChemicalSourceSink(long pdtn)
{
    return (
        pdtn == 76 ||
        pdtn == 77 ||
        pdtn == 78 ||
        pdtn == 79);
}

// Return 1 if the productDefinitionTemplateNumber (GRIB2) is for
// atmospheric chemical constituents based on a distribution function
int grib2_is_PDTN_ChemicalDistFunc(long pdtn)
{
    return (
        pdtn == 57 ||
        pdtn == 58 ||
        pdtn == 67 ||
        pdtn == 68);
}

// Return 1 if the productDefinitionTemplateNumber (GRIB2) is for aerosols
int grib2_is_PDTN_Aerosol(long pdtn)
{
    // Notes: PDT 44 is deprecated and replaced by 50
    //        PDT 47 is deprecated and replaced by 85
    return (
        pdtn == 44 ||
        pdtn == 48 ||
        pdtn == 49 ||
        pdtn == 50 ||
        pdtn == 45 ||
        pdtn == 46 ||
        pdtn == 47 ||
        pdtn == 85);
}

// Return 1 if the productDefinitionTemplateNumber (GRIB2) is for optical properties of aerosol
int grib2_is_PDTN_AerosolOptical(long pdtn)
{
    // Note: PDT 48 can be used for both plain aerosols as well as optical properties of aerosol.
    // For the former user must set the optical wavelength range to missing
    return (
        pdtn == 48 ||
        pdtn == 49);
}

// Arguments:
//  is_det:     true for deterministic, false for ensemble
//  is_instant: true for instantaneous (point-in-time), false for interval-based (statistically processed)
int grib2_choose_PDTN(int current_PDTN, bool is_det, bool is_instant)
{
    const bool is_ens      = !is_det;
    const bool is_interval = !is_instant;

    if (grib2_is_PDTN_Plain(current_PDTN)) {
        if (is_instant  && is_ens) return 1;
        if (is_instant  && is_det) return 0;
        if (is_interval && is_ens) return 11;
        if (is_interval && is_det) return 8;
    }

    if (grib2_is_PDTN_Chemical(current_PDTN)) {
        if (is_instant  && is_ens) return 41;
        if (is_instant  && is_det) return 40;
        if (is_interval && is_ens) return 43;
        if (is_interval && is_det) return 42;
    }

    if (grib2_is_PDTN_ChemicalSourceSink(current_PDTN)) {
        if (is_instant  && is_ens) return 77;
        if (is_instant  && is_det) return 76;
        if (is_interval && is_ens) return 79;
        if (is_interval && is_det) return 78;
    }

    if (grib2_is_PDTN_ChemicalDistFunc(current_PDTN)) {
        if (is_instant  && is_ens) return 58;
        if (is_instant  && is_det) return 57;
        if (is_interval && is_ens) return 68;
        if (is_interval && is_det) return 67;
    }

    if (current_PDTN == 45 || current_PDTN == 48) {
        if (is_instant  && is_ens) return 45;
        if (is_instant  && is_det) return 48;
        if (is_interval && is_ens) return 85;
        if (is_interval && is_det) return 46;
    }
    if (current_PDTN == 50) {
        if (is_instant && is_ens) return 45;
    }

    return current_PDTN;  // no change
}

// Given some information about the type of grib2 parameter, return the productDefinitionTemplateNumber to use.
// All arguments are booleans (0 or 1)
//  is_eps:     ensemble or deterministic
//  is_instant: instantaneous or interval-based
//  etc...
int grib2_select_PDTN(int is_eps, int is_instant,
                      int is_chemical,
                      int is_chemical_srcsink,
                      int is_chemical_distfn,
                      int is_aerosol,
                      int is_aerosol_optical)
{
    // At most one has to be set. All could be 0
    // Unfortunately if PDTN=48 then both aerosol and aerosol_optical can be 1!
    const int sum = is_chemical + is_chemical_srcsink + is_chemical_distfn + is_aerosol + is_aerosol_optical;
    ECCODES_ASSERT(sum == 0 || sum == 1 || sum == 2);

    if (is_chemical) {
        if (is_eps) {
            if (is_instant)
                return 41;
            else
                return 43;
        }
        else {
            if (is_instant)
                return 40;
            else
                return 42;
        }
    }

    if (is_chemical_srcsink) {
        if (is_eps) {
            if (is_instant)
                return 77;
            else
                return 79;
        }
        else {
            if (is_instant)
                return 76;
            else
                return 78;
        }
    }

    if (is_chemical_distfn) {
        if (is_eps) {
            if (is_instant)
                return 58;
            else
                return 68;
        }
        else {
            if (is_instant)
                return 57;
            else
                return 67;
        }
    }

    if (is_aerosol_optical) {
        if (is_eps) {
            if (is_instant)
                return 49;
            // WMO does not have a non-instantaneous case here!
        }
        else {
            if (is_instant)
                return 48;
            // WMO does not have a non-instantaneous case here!
        }
    }

    if (is_aerosol) {
        if (is_eps) {
            if (is_instant)
                return 45;
            else
                return 85; // PDT 47 is deprecated
        }
        else {
            if (is_instant)
                return 50; // ECC-1963: 44 is deprecated
            else
                return 46;
        }
    }

    // Fallthru case: default
    if (is_eps) {
        if (is_instant)
            return 1;
        else
            return 11;
    }
    else {
        if (is_instant)
            return 0;
        else
            return 8;
    }
}

// Input argument must be an entry in Code Table 4.5 (Fixed surface types and units)
// Output is:
//  1 = means the surface type needs its scaledValue/scaleFactor i.e., has a level
//  0 = means scaledValue/scaleFactor must be set to MISSING
int codes_grib_surface_type_requires_value(int edition, int type_of_surface_code, int* err)
{
    static const int types_with_values[] = {
        17,  // Departure level of the most unstable parcel of air (MUDL)
        18,  // Departure level of a mixed layer parcel of air with specified layer depth (Pa)
        19,  // Lowest level where cloud cover exceeds the specified percentage (%)
        20,  // Isothermal level (K)
        102, // Specific altitude above mean sea level (m)
        103, // Specified height level above ground (m)
        106, // Depth below land surface (m)
        107, // Isentropic (theta) level (K)
        117, // Mixed layer depth (m)
        160, // Depth below sea level (m)
        161, // Depth below water surface (m)
        169, // Ocean level defined by water density (sigma-theta) difference from near-surface to level (kg m-3)
        170, // Ocean level defined by water potential temperature difference from near-surface to level (K)
        171, // Ocean level defined by vertical eddy diffusivity difference from near-surface to level (m2 s-1)
    };
    *err = GRIB_SUCCESS;

    if (edition != 2) {
        *err = GRIB_NOT_IMPLEMENTED;
        return 0;
    }

    // Surface type keys are 1 octet and cannot be -ve
    if (type_of_surface_code < 0 || type_of_surface_code > 255) {
        *err = GRIB_INVALID_ARGUMENT;
        return 0;
    }
    static const size_t num = sizeof(types_with_values)/sizeof(types_with_values[0]);
    for (size_t i=0; i<num; ++i) {
        if (type_of_surface_code == types_with_values[i])
            return 1;
    }
    return 0;
}

size_t sum_of_pl_array(const long* pl, size_t plsize)
{
    long i, count = 0;
    for (i = 0; i < plsize; i++) {
        count += pl[i];
    }
    return count;
}

int grib_is_earth_oblate(const grib_handle* h)
{
    long oblate = 0;
    int err     = grib_get_long(h, "earthIsOblate", &oblate);
    if (!err && oblate == 1) {
        return 1;
    }
    return 0;
}

int grib_check_data_values_minmax(grib_handle* h, const double min_val, const double max_val)
{
    int result = GRIB_SUCCESS;
    const grib_context* ctx = h->context;

    if (!(min_val < DBL_MAX && min_val > -DBL_MAX)) {
        grib_context_log(ctx, GRIB_LOG_ERROR, "Minimum value out of range: %g", min_val);
        return GRIB_ENCODING_ERROR;
    }
    if (!(max_val < DBL_MAX && max_val > -DBL_MAX)) {
        grib_context_log(ctx, GRIB_LOG_ERROR, "Maximum value out of range: %g", max_val);
        return GRIB_ENCODING_ERROR;
    }

    // Data Quality checks
    if (ctx->grib_data_quality_checks) {
        result = grib_util_grib_data_quality_check(h, min_val, max_val);
    }

    return result;
}

// Return true(1) if large constant fields are to be created, otherwise false(0)
int grib_producing_large_constant_fields(const grib_handle* h, int edition)
{
    // First check if the transient key is set
    const grib_context* c = h->context;
    long produceLargeConstantFields = 0;
    if (grib_get_long(h, "produceLargeConstantFields", &produceLargeConstantFields) == GRIB_SUCCESS &&
        produceLargeConstantFields != 0) {
        return 1;
    }

    if (c->gribex_mode_on == 1 && edition == 1) {
        return 1;
    }

    // Finally check the environment variable via the context
    return c->large_constant_fields;
}

static std::string grib_data_quality_check_extra_info(const grib_handle* h)
{
    char step[32]       = "unknown";
    char marsClass[32]  = {0,};
    char marsStream[32] = {0,};
    char marsType[32]   = {0,};
    std::string result;
    std::stringstream ss;
    size_t len = 32;
    int err1 = grib_get_string(h, "step", step, &len);
    len = 32;
    int err2 = grib_get_string(h, "class", marsClass, &len);
    len = 32;
    int err3 = grib_get_string(h, "stream", marsStream, &len);
    len = 32;
    int err4 = grib_get_string(h, "type", marsType, &len);
    if (!err1 && !err2 && !err3 && !err4) {
        ss << "step=" << step << ", class=" << marsClass << ", stream=" << marsStream << ", type=" << marsType;
        result = ss.str();
    }

    return result;
}

int grib_util_grib_data_quality_check(grib_handle* h, double min_val, double max_val)
{
    int err = 0;
    double min_field_value_allowed = 0, max_field_value_allowed = 0;
    long paramId           = 0;
    const grib_context* ctx = h->context;
    bool is_error          = true;
    char description[1024] = {0,};
    char shortName[64]     = {0,};
    char name[526]         = {0,};
    size_t len             = 0;
    const char* invalid_shortName = "unknown";
    const char* invalid_name      = "Experimental product";

    // If grib_data_quality_checks == 1, limits failure results in an error
    // If grib_data_quality_checks == 2, limits failure results in a warning

    ECCODES_ASSERT(ctx->grib_data_quality_checks == 1 || ctx->grib_data_quality_checks == 2);
    is_error = (ctx->grib_data_quality_checks == 1);

    len = sizeof(shortName);
    err = grib_get_string(h, "shortName", shortName, &len);
    if (err || STR_EQUAL(shortName, invalid_shortName)) {
        std::string info( grib_data_quality_check_extra_info(h) );
        fprintf(stderr, "ECCODES %s   :  (%s) Invalid metadata: shortName='%s'\n",
                    (is_error ? "ERROR" : "WARNING"), info.c_str(), invalid_shortName);
        if (is_error) return GRIB_INVALID_MESSAGE;
    }

    len = sizeof(name);
    err = grib_get_string(h, "name", name, &len);
    if (err || STR_EQUAL(name, invalid_name)) {
        fprintf(stderr, "ECCODES %s   :  Invalid metadata: name='%s'\n",
                    (is_error ? "ERROR" : "WARNING"), invalid_name);
        if (is_error) return GRIB_INVALID_MESSAGE;
    }

    // The limit keys must exist if we are here
    err = grib_get_double(h, "param_value_min", &min_field_value_allowed);
    if (err) {
        grib_context_log(ctx, GRIB_LOG_ERROR, "grib_data_quality_check: Could not get param_value_min");
        return err;
    }
    err = grib_get_double(h, "param_value_max", &max_field_value_allowed);
    if (err) {
        grib_context_log(ctx, GRIB_LOG_ERROR, "grib_data_quality_check: Could not get param_value_max");
        return err;
    }

    if (ctx->debug) {
        if (get_concept_condition_string(h, "param_value_max", NULL, description) == GRIB_SUCCESS) {
            printf("ECCODES DEBUG grib_data_quality_check: Checking condition '%s' (allowed=%g, %g) (actual=%g, %g)\n",
                   description, min_field_value_allowed, max_field_value_allowed,
                   min_val, max_val);
        }
    }

    if (min_val < min_field_value_allowed) {
        std::string info( grib_data_quality_check_extra_info(h) );
        if (get_concept_condition_string(h, "param_value_min", NULL, description) == GRIB_SUCCESS) {
            fprintf(stderr, "ECCODES %s   :  (%s, %s): minimum (%g) is less than the allowable limit (%g)\n",
                    (is_error ? "ERROR" : "WARNING"), description, info.c_str(), min_val, min_field_value_allowed);
        }
        else {
            if (grib_get_long(h, "paramId", &paramId) == GRIB_SUCCESS) {
                fprintf(stderr, "ECCODES %s   :  (paramId=%ld, %s): minimum (%g) is less than the default allowable limit (%g)\n",
                        (is_error ? "ERROR" : "WARNING"), paramId, info.c_str(), min_val, min_field_value_allowed);
            }
        }
        if (is_error) {
            return GRIB_OUT_OF_RANGE; // Failure
        }
    }
    if (max_val > max_field_value_allowed) {
        std::string info( grib_data_quality_check_extra_info(h) );
        if (get_concept_condition_string(h, "param_value_max", NULL, description) == GRIB_SUCCESS) {
            fprintf(stderr, "ECCODES %s   :  (%s, %s): maximum (%g) is more than the allowable limit (%g)\n",
                    (is_error ? "ERROR" : "WARNING"), description, info.c_str(), max_val, max_field_value_allowed);
        }
        else {
            if (grib_get_long(h, "paramId", &paramId) == GRIB_SUCCESS) {
                fprintf(stderr, "ECCODES %s   :  (paramId=%ld, %s): maximum (%g) is more than the default allowable limit (%g)\n",
                        (is_error ? "ERROR" : "WARNING"), paramId, info.c_str(), max_val, max_field_value_allowed);
            }
        }
        if (is_error) {
            return GRIB_OUT_OF_RANGE; // Failure
        }
    }

    return GRIB_SUCCESS;
}
