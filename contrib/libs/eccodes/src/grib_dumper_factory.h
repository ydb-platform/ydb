/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#pragma once

#include "dumper/grib_dumper.h"

extern eccodes::Dumper* grib_dumper_bufr_decode_c;
extern eccodes::Dumper* grib_dumper_bufr_decode_filter;
extern eccodes::Dumper* grib_dumper_bufr_decode_fortran;
extern eccodes::Dumper* grib_dumper_bufr_decode_python;
extern eccodes::Dumper* grib_dumper_bufr_encode_c;
extern eccodes::Dumper* grib_dumper_bufr_encode_filter;
extern eccodes::Dumper* grib_dumper_bufr_encode_fortran;
extern eccodes::Dumper* grib_dumper_bufr_encode_python;
extern eccodes::Dumper* grib_dumper_bufr_simple;
extern eccodes::Dumper* grib_dumper_debug;
extern eccodes::Dumper* grib_dumper_default;
extern eccodes::Dumper* grib_dumper_file;
extern eccodes::Dumper* grib_dumper_grib_encode_c;
extern eccodes::Dumper* grib_dumper_json;
extern eccodes::Dumper* grib_dumper_serialize;
extern eccodes::Dumper* grib_dumper_string;
extern eccodes::Dumper* grib_dumper_wmo;

eccodes::Dumper* grib_dumper_factory(const char* op, const grib_handle* h, FILE* out, unsigned long option_flags, void* arg);
int grib_print(grib_handle* h, const char* name, eccodes::Dumper* d);
void grib_dump_keys(grib_handle* h, FILE* f, const char* mode, unsigned long flags, void* data, const char** keys, size_t num_keys);

eccodes::Dumper* grib_dump_content_with_dumper(grib_handle* h, eccodes::Dumper* dumper, FILE* f, const char* mode, unsigned long flags, void* data);
void codes_dump_bufr_flat(grib_accessors_list* al, grib_handle* h, FILE* f, const char* mode, unsigned long flags, void* data);

void grib_dump_accessors_block(eccodes::Dumper* dumper, grib_block_of_accessors* block);
void grib_dump_accessors_list(eccodes::Dumper* dumper, grib_accessors_list* al);

void grib_dump_content(const grib_handle* h, FILE* f, const char* mode, unsigned long flags, void* data);
