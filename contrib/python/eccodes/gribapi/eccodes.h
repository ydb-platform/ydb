/*
 * (C) Copyright 2017- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

typedef struct grib_handle    codes_handle;
typedef struct grib_context   codes_context;

grib_handle* codes_handle_new_from_file(codes_context* c, FILE* f, ProductKind product, int* error);

codes_handle* codes_bufr_handle_new_from_samples(codes_context* c, const char* sample_name);
codes_handle* codes_handle_new_from_samples(codes_context* c, const char* sample_name);

int codes_bufr_copy_data(grib_handle* hin, grib_handle* hout);

void codes_bufr_multi_element_constant_arrays_on(codes_context* c);
void codes_bufr_multi_element_constant_arrays_off(codes_context* c);
int codes_bufr_extract_headers_malloc(codes_context* c, const char* filename, codes_bufr_header** result, int* num_messages, int strict_mode);
int codes_extract_offsets_malloc(codes_context* c, const char* filename, ProductKind product, long int** offsets, int* num_messages, int strict_mode);
int codes_extract_offsets_sizes_malloc(codes_context* c, const char* filename, ProductKind product, long int** offsets, size_t** sizes, int* num_messages, int strict_mode);
int codes_bufr_key_is_header(const codes_handle* h, const char* key, int* err);
int codes_bufr_key_is_coordinate(const codes_handle* h, const char* key, int* err);

char* codes_samples_path(const codes_context *c);
char* codes_definition_path(const codes_context *c);
