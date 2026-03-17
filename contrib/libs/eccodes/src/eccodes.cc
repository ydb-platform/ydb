/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "eccodes.h"

/* Generic functions */
/******************************************************************************/
char* codes_samples_path(const codes_context* c)
{
    return grib_samples_path(c);
}
char* codes_definition_path(const codes_context* c)
{
    return grib_definition_path(c);
}
long codes_get_api_version(void)
{
    return grib_get_api_version();
}
const char* codes_get_git_sha1()
{
    return grib_get_git_sha1();
}
const char* codes_get_git_branch()
{
    return grib_get_git_branch();
}

const char* codes_get_package_name(void)
{
    return grib_get_package_name();
}
void codes_print_api_version(FILE* out)
{
    grib_print_api_version(out);
}
int codes_count_in_file(codes_context* c, FILE* f, int* n)
{
    return grib_count_in_file(c, f, n);
}
int codes_count_in_filename(codes_context* c, const char* filename, int* n)
{
    return grib_count_in_filename(c, filename, n);
}

codes_context* codes_context_get_default(void)
{
    return grib_context_get_default();
}
const char* codes_get_error_message(int code)
{
    return grib_get_error_message(code);
}
const char* codes_get_type_name(int type)
{
    return grib_get_type_name(type);
}
int codes_get_native_type(const grib_handle* h, const char* name, int* type)
{
    return grib_get_native_type(h, name, type);
}
void codes_check(const char* call, const char* file, int line, int e, const char* msg)
{
    grib_check(call, file, line, e, msg);
}


/* Fieldsets */
/******************************************************************************/
grib_fieldset* codes_fieldset_new_from_files(codes_context* c, const char* filenames[], int nfiles, const char** keys, int nkeys, const char* where_string, const char* order_by_string, int* err)
{
    return grib_fieldset_new_from_files(c, filenames, nfiles, keys, nkeys, where_string, order_by_string, err);
}
void codes_fieldset_delete(grib_fieldset* set)
{
    grib_fieldset_delete(set);
}
void codes_fieldset_rewind(grib_fieldset* set)
{
    grib_fieldset_rewind(set);
}
int codes_fieldset_apply_order_by(grib_fieldset* set, const char* order_by_string)
{
    return grib_fieldset_apply_order_by(set, order_by_string);
}
grib_handle* codes_fieldset_next_handle(grib_fieldset* set, int* err)
{
    return grib_fieldset_next_handle(set, err);
}
int codes_fieldset_count(const grib_fieldset* set)
{
    return grib_fieldset_count(set);
}

/* Indexing */
/******************************************************************************/
grib_index* codes_index_new_from_file(codes_context* c, const char* filename, const char* keys, int* err)
{
    return grib_index_new_from_file(c, filename, keys, err);
}
grib_index* codes_index_new(codes_context* c, const char* keys, int* err)
{
    return grib_index_new(c, keys, err);
}
int codes_index_add_file(grib_index* index, const char* filename)
{
    return grib_index_add_file(index, filename);
}
int codes_index_write(grib_index* index, const char* filename)
{
    return grib_index_write(index, filename);
}
grib_index* codes_index_read(codes_context* c, const char* filename, int* err)
{
    return grib_index_read(c, filename, err);
}
int codes_index_get_size(const grib_index* index, const char* key, size_t* size)
{
    return grib_index_get_size(index, key, size);
}
int codes_index_get_long(const grib_index* index, const char* key, long* values, size_t* size)
{
    return grib_index_get_long(index, key, values, size);
}
int codes_index_get_double(const grib_index* index, const char* key, double* values, size_t* size)
{
    return grib_index_get_double(index, key, values, size);
}
int codes_index_get_string(const grib_index* index, const char* key, char** values, size_t* size)
{
    return grib_index_get_string(index, key, values, size);
}
int codes_index_select_long(grib_index* index, const char* key, long value)
{
    return grib_index_select_long(index, key, value);
}
int codes_index_select_double(grib_index* index, const char* key, double value)
{
    return grib_index_select_double(index, key, value);
}
int codes_index_select_string(grib_index* index, const char* key, const char* value)
{
    return grib_index_select_string(index, key, value);
}
grib_handle* codes_handle_new_from_index(grib_index* index, int* err)
{
    return grib_handle_new_from_index(index, err);
}
void codes_index_delete(grib_index* index)
{
    grib_index_delete(index);
}

/* Create handle */
/******************************************************************************/
int codes_write_message(const grib_handle* h, const char* file, const char* mode)
{
    return grib_write_message(h, file, mode);
}
grib_handle* codes_handle_new_from_message(codes_context* c, const void* data, size_t data_len)
{
    return grib_handle_new_from_message(c, data, data_len);
}
grib_handle* codes_handle_new_from_message_copy(codes_context* c, const void* data, size_t data_len)
{
    return grib_handle_new_from_message_copy(c, data, data_len);
}
grib_handle* codes_grib_handle_new_from_samples(codes_context* c, const char* sample_name)
{
    return grib_handle_new_from_samples(c, sample_name);
}
grib_handle* codes_handle_clone(const grib_handle* h)
{
    return grib_handle_clone(h);
}
grib_handle* codes_handle_clone_headers_only(const grib_handle* h)
{
    return grib_handle_clone_headers_only(h);
}

int codes_handle_delete(grib_handle* h)
{
    return grib_handle_delete(h);
}
grib_handle* codes_handle_new_from_partial_message_copy(codes_context* c, const void* data, size_t size)
{
    return grib_handle_new_from_partial_message_copy(c, data, size);
}
grib_handle* codes_handle_new_from_partial_message(codes_context* c, const void* data, size_t buflen)
{
    return grib_handle_new_from_partial_message(c, data, buflen);
}
int codes_get_message(const grib_handle* h, const void** message, size_t* message_length)
{
    return grib_get_message(h, message, message_length);
}
int codes_get_message_copy(const grib_handle* h, void* message, size_t* message_length)
{
    return grib_get_message_copy(h, message, message_length);
}

/* Specific to GRIB */
/******************************************************************************/
grib_handle* codes_grib_util_set_spec(grib_handle* h,
                                      const grib_util_grid_spec* grid_spec,
                                      const grib_util_packing_spec* packing_spec,
                                      int flags,
                                      const double* data_values,
                                      size_t data_values_count,
                                      int* err)
{
    return grib_util_set_spec(h, grid_spec, packing_spec, flags, data_values, data_values_count, err);
}
grib_handle* codes_grib_util_sections_copy(grib_handle* hfrom, grib_handle* hto, int what, int* err)
{
    return grib_util_sections_copy(hfrom, hto, what, err);
}
grib_string_list* codes_grib_util_get_param_id(const char* mars_param)
{
    return grib_util_get_param_id(mars_param);
}
grib_string_list* codes_grib_util_get_mars_param(const char* param_id)
{
    return grib_util_get_mars_param(param_id);
}
void codes_grib_multi_support_on(codes_context* c)
{
    grib_multi_support_on(c);
}
void codes_grib_multi_support_off(codes_context* c)
{
    grib_multi_support_off(c);
}
void codes_grib_multi_support_reset_file(codes_context* c, FILE* f)
{
    grib_multi_support_reset_file(c, f);
}
grib_handle* codes_grib_handle_new_from_multi_message(codes_context* c, void** data, size_t* data_len, int* error)
{
    return grib_handle_new_from_multi_message(c, data, data_len, error);
}
grib_multi_handle* codes_grib_multi_handle_new(codes_context* c)
{
    return grib_multi_handle_new(c);
}
int codes_grib_multi_handle_append(grib_handle* h, int start_section, grib_multi_handle* mh)
{
    return grib_multi_handle_append(h, start_section, mh);
}
int codes_grib_multi_handle_delete(grib_multi_handle* mh)
{
    return grib_multi_handle_delete(mh);
}
int codes_grib_multi_handle_write(grib_multi_handle* mh, FILE* f)
{
    return grib_multi_handle_write(mh, f);
}

/* Lat/Lon iterator and nearest (GRIB specific) */
/******************************************************************************/
grib_iterator* codes_grib_iterator_new(const grib_handle* h, unsigned long flags, int* error)
{
    return grib_iterator_new(h, flags, error);
}
int codes_grib_get_data(const grib_handle* h, double* lats, double* lons, double* values)
{
    return grib_get_data(h, lats, lons, values);
}
int codes_grib_iterator_next(grib_iterator* i, double* lat, double* lon, double* value)
{
    return grib_iterator_next(i, lat, lon, value);
}
int codes_grib_iterator_previous(grib_iterator* i, double* lat, double* lon, double* value)
{
    return grib_iterator_previous(i, lat, lon, value);
}
int codes_grib_iterator_has_next(grib_iterator* i)
{
    return grib_iterator_has_next(i);
}
int codes_grib_iterator_reset(grib_iterator* i)
{
    return grib_iterator_reset(i);
}
int codes_grib_iterator_delete(grib_iterator* i)
{
    return grib_iterator_delete(i);
}

grib_nearest* codes_grib_nearest_new(const grib_handle* h, int* error)
{
    return grib_nearest_new(h, error);
}
int codes_grib_nearest_find(grib_nearest* nearest, const grib_handle* h, double inlat, double inlon,
                            unsigned long flags, double* outlats, double* outlons,
                            double* values, double* distances, int* indexes, size_t* len)
{
    return grib_nearest_find(nearest, h, inlat, inlon, flags, outlats, outlons, values, distances, indexes, len);
}
int codes_grib_nearest_find_multiple(const grib_handle* h, int is_lsm,
                                     const double* inlats, const double* inlons, long npoints,
                                     double* outlats, double* outlons,
                                     double* values, double* distances, int* indexes)
{
    return grib_nearest_find_multiple(h, is_lsm, inlats, inlons, npoints, outlats, outlons, values, distances, indexes);
}
int codes_grib_nearest_delete(grib_nearest* nearest)
{
    return grib_nearest_delete(nearest);
}


/* get/set keys */
/******************************************************************************/
int codes_is_missing(const grib_handle* h, const char* key, int* err)
{
    return grib_is_missing(h, key, err);
}
int codes_is_defined(const grib_handle* h, const char* key)
{
    return grib_is_defined(h, key);
}
int codes_set_missing(grib_handle* h, const char* key)
{
    return grib_set_missing(h, key);
}
int codes_get_offset(const codes_handle* h, const char* key, size_t* offset)
{
    return grib_get_offset(h, key, offset);
}
int codes_get_size(const grib_handle* h, const char* key, size_t* size)
{
    return grib_get_size(h, key, size);
}
int codes_get_length(const grib_handle* h, const char* key, size_t* length)
{
    return grib_get_length(h, key, length);
}
int codes_get_long(const grib_handle* h, const char* key, long* value)
{
    return grib_get_long(h, key, value);
}
int codes_get_double(const grib_handle* h, const char* key, double* value)
{
    return grib_get_double(h, key, value);
}
int codes_get_float(const grib_handle* h, const char* key, float* value)
{
    return grib_get_float(h, key, value);
}

int codes_get_double_element(const grib_handle* h, const char* key, int i, double* value)
{
    return grib_get_double_element(h, key, i, value);
}
int codes_get_double_elements(const grib_handle* h, const char* key, const int* index_array, long size, double* value)
{
    return grib_get_double_elements(h, key, index_array, size, value);
}
int codes_get_float_element(const grib_handle* h, const char* key, int i, float* value)
{
    return grib_get_float_element(h, key, i, value);
}
int codes_get_float_elements(const grib_handle* h, const char* key, const int* index_array, long size, float* value)
{
    return grib_get_float_elements(h, key, index_array, size, value);
}

int codes_get_string(const grib_handle* h, const char* key, char* mesg, size_t* length)
{
    return grib_get_string(h, key, mesg, length);
}
int codes_get_string_array(const grib_handle* h, const char* key, char** vals, size_t* length)
{
    return grib_get_string_array(h, key, vals, length);
}
int codes_get_bytes(const grib_handle* h, const char* key, unsigned char* bytes, size_t* length)
{
    return grib_get_bytes(h, key, bytes, length);
}
int codes_get_double_array(const grib_handle* h, const char* key, double* vals, size_t* length)
{
    return grib_get_double_array(h, key, vals, length);
}
int codes_get_float_array(const grib_handle* h, const char* key, float* vals, size_t* length)
{
    return grib_get_float_array(h, key, vals, length);
}
int codes_get_long_array(const grib_handle* h, const char* key, long* vals, size_t* length)
{
    return grib_get_long_array(h, key, vals, length);
}
int codes_copy_namespace(grib_handle* dest, const char* name, grib_handle* src)
{
    return grib_copy_namespace(dest, name, src);
}
int codes_set_long(grib_handle* h, const char* key, long val)
{
    return grib_set_long(h, key, val);
}
int codes_set_double(grib_handle* h, const char* key, double val)
{
    return grib_set_double(h, key, val);
}
int codes_set_string(grib_handle* h, const char* key, const char* mesg, size_t* length)
{
    return grib_set_string(h, key, mesg, length);
}
int codes_set_bytes(grib_handle* h, const char* key, const unsigned char* bytes, size_t* length)
{
    return grib_set_bytes(h, key, bytes, length);
}

int codes_set_double_array(grib_handle* h, const char* key, const double* vals, size_t length)
{
    return grib_set_double_array(h, key, vals, length);
}
int codes_set_force_double_array(grib_handle* h, const char* key, const double* vals, size_t length)
{
    return grib_set_force_double_array(h, key, vals, length);
}
int codes_set_float_array(grib_handle* h, const char* key, const float* vals, size_t length)
{
    return grib_set_float_array(h, key, vals, length);
}
int codes_set_force_float_array(grib_handle* h, const char* key, const float* vals, size_t length)
{
    return grib_set_force_float_array(h, key, vals, length);
}

int codes_set_long_array(grib_handle* h, const char* key, const long* vals, size_t length)
{
    return grib_set_long_array(h, key, vals, length);
}

int codes_set_string_array(grib_handle* h, const char* key, const char** vals, size_t length)
{
    return grib_set_string_array(h, key, vals, length);
}

int codes_set_values(grib_handle* h, grib_values* grib_values, size_t arg_count)
{
    return grib_set_values(h, grib_values, arg_count);
}
int codes_get_message_offset(const grib_handle* h, off_t* offset)
{
    return grib_get_message_offset(h, offset);
}
int codes_get_message_size(const grib_handle* h, size_t* size)
{
    return grib_get_message_size(h, size);
}
void codes_dump_content(const grib_handle* h, FILE* out, const char* mode, unsigned long option_flags, void* arg)
{
    grib_dump_content(h, out, mode, option_flags, arg);
}
void codes_dump_action_tree(codes_context* c, FILE* f)
{
    grib_dump_action_tree(c, f);
}
/* GTS, GRIBEX */
/******************************************************************************/
void codes_gts_header_off(codes_context* c)
{
    grib_gts_header_off(c);
}
void codes_gts_header_on(codes_context* c)
{
    grib_gts_header_on(c);
}

void codes_gribex_mode_on(codes_context* c)
{
    grib_gribex_mode_on(c);
}
int codes_get_gribex_mode(const codes_context* c)
{
    return grib_get_gribex_mode(c);
}
void codes_gribex_mode_off(codes_context* c)
{
    grib_gribex_mode_off(c);
}

/* keys iterator */
/******************************************************************************/
grib_keys_iterator* codes_keys_iterator_new(grib_handle* h, unsigned long filter_flags, const char* name_space)
{
    return grib_keys_iterator_new(h, filter_flags, name_space);
}
int codes_keys_iterator_next(grib_keys_iterator* kiter)
{
    return grib_keys_iterator_next(kiter);
}
const char* codes_keys_iterator_get_name(const grib_keys_iterator* kiter)
{
    return grib_keys_iterator_get_name(kiter);
}
int codes_keys_iterator_delete(grib_keys_iterator* kiter)
{
    return grib_keys_iterator_delete(kiter);
}
int codes_keys_iterator_rewind(grib_keys_iterator* kiter)
{
    return grib_keys_iterator_rewind(kiter);
}
int codes_keys_iterator_set_flags(grib_keys_iterator* kiter, unsigned long flags)
{
    return grib_keys_iterator_set_flags(kiter, flags);
}
int codes_keys_iterator_get_long(const grib_keys_iterator* kiter, long* v, size_t* len)
{
    return grib_keys_iterator_get_long(kiter, v, len);
}

int codes_keys_iterator_get_double(const grib_keys_iterator* kiter, double* v, size_t* len)
{
    return grib_keys_iterator_get_double(kiter, v, len);
}
int codes_keys_iterator_get_float(const grib_keys_iterator* kiter, float* v, size_t* len)
{
    return grib_keys_iterator_get_float(kiter, v, len);
}

int codes_keys_iterator_get_string(const grib_keys_iterator* kiter, char* v, size_t* len)
{
    return grib_keys_iterator_get_string(kiter, v, len);
}
int codes_keys_iterator_get_bytes(const grib_keys_iterator* kiter, unsigned char* v, size_t* len)
{
    return grib_keys_iterator_get_bytes(kiter, v, len);
}

/* Utility functions */
/******************************************************************************/
int codes_values_check(grib_handle* h, grib_values* values, int count)
{
    return grib_values_check(h, values, count);
}
void codes_update_sections_lengths(grib_handle* h)
{
    grib_update_sections_lengths(h);
}
int codes_get_gaussian_latitudes(long truncation, double* latitudes)
{
    return grib_get_gaussian_latitudes(truncation, latitudes);
}
int codes_julian_to_datetime(double jd, long* year, long* month, long* day, long* hour, long* minute, long* second)
{
    return grib_julian_to_datetime(jd, year, month, day, hour, minute, second);
}
int codes_datetime_to_julian(long year, long month, long day, long hour, long minute, long second, double* jd)
{
    return grib_datetime_to_julian(year, month, day, hour, minute, second, jd);
}
long codes_julian_to_date(long jdate)
{
    return grib_julian_to_date(jdate);
}
long codes_date_to_julian(long ddate)
{
    return grib_date_to_julian(ddate);
}
void codes_get_reduced_row(long pl, double lon_first, double lon_last, long* npoints, long* ilon_first, long* ilon_last)
{
    grib_get_reduced_row(pl, lon_first, lon_last, npoints, ilon_first, ilon_last);
}
void codes_get_reduced_row_p(long pl, double lon_first, double lon_last, long* npoints, double* olon_first, double* olon_last)
{
    grib_get_reduced_row_p(pl, lon_first, lon_last, npoints, olon_first, olon_last);
}

void codes_context_delete(codes_context* c)
{
    grib_context_delete(c);
}
void codes_context_set_definitions_path(codes_context* c, const char* path)
{
    grib_context_set_definitions_path(c, path);
}
void codes_context_set_samples_path(codes_context* c, const char* path)
{
    grib_context_set_samples_path(c, path);
}

void codes_context_set_debug(codes_context* c, int mode)
{
    grib_context_set_debug(c, mode);
}
void codes_context_set_data_quality_checks(codes_context* c, int val)
{
    grib_context_set_data_quality_checks(c, val);
}

void codes_context_set_print_proc(codes_context* c, grib_print_proc p_print)
{
    grib_context_set_print_proc(c, p_print);
}
void codes_context_set_logging_proc(codes_context* c, grib_log_proc p_log)
{
    grib_context_set_logging_proc(c, p_log);
}
