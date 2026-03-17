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

#include "grib_accessor_class_gen.h"
#include "grib_accessor_class_expanded_descriptors.h"

typedef struct bitmap_s
{
    grib_accessors_list* cursor;
    grib_accessors_list* referredElement;
    grib_accessors_list* referredElementStart;
} bitmap_s;


class grib_accessor_bufr_data_array_t;

class grib_accessor_bufr_data_array_t : public grib_accessor_gen_t
{
public:
    grib_accessor_bufr_data_array_t() :
        grib_accessor_gen_t() { class_name_ = "bufr_data_array"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_bufr_data_array_t{}; }
    long get_native_type() override;
    int pack_double(const double* val, size_t* len) override;
    int pack_long(const long* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    long byte_count() override;
    long byte_offset() override;
    long next_offset() override;
    int value_count(long*) override;
    void destroy(grib_context*) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;

    void accessor_bufr_data_array_set_unpackMode(int);
    grib_accessors_list* accessor_bufr_data_array_get_dataAccessors();
    grib_trie_with_rank* accessor_bufr_data_array_get_dataAccessorsTrie();
    grib_vsarray* accessor_bufr_data_array_get_stringValues();

private:
    const char* bufrDataEncodedName_ = nullptr;
    const char* numberOfSubsetsName_ = nullptr;
    const char* expandedDescriptorsName_ = nullptr;
    const char* flagsName_ = nullptr;
    const char* unitsName_ = nullptr;
    const char* elementsDescriptorsIndexName_ = nullptr;
    const char* compressedDataName_ = nullptr;
    bufr_descriptors_array* expanded_ = nullptr;
    grib_accessor_expanded_descriptors_t* expandedAccessor_ = nullptr;
    int* canBeMissing_ = nullptr;
    long numberOfSubsets_ = 0;
    long compressedData_ = 0;
    grib_vdarray* numericValues_ = nullptr;
    grib_vsarray* stringValues_ = nullptr;
    grib_viarray* elementsDescriptorsIndex_ = nullptr;
    int do_decode_ = 0;
    int bitmapStartElementsDescriptorsIndex_ = 0;
    int bitmapCurrentElementsDescriptorsIndex_ = 0;
    int bitmapSize_ = 0;
    int bitmapStart_ = 0;
    int bitmapCurrent_ = 0;
    grib_accessors_list* dataAccessors_ = nullptr;
    int unpackMode_ = 0;
    int bitsToEndData_ = 0;
    grib_section* dataKeys_ = nullptr;
    double* inputBitmap_ = nullptr;
    int nInputBitmap_ = 0;
    int iInputBitmap_ = 0;
    long* inputReplications_ = nullptr;
    int nInputReplications_ = 0;
    int iInputReplications_ = 0;
    long* inputExtendedReplications_ = nullptr;
    int nInputExtendedReplications_ = 0;
    int iInputExtendedReplications_ = 0;
    long* inputShortReplications_ = nullptr;
    int nInputShortReplications_ = 0;
    int iInputShortReplications_ = 0;
    grib_iarray* iss_list_ = nullptr;
    grib_trie_with_rank* dataAccessorsTrie_ = nullptr;
    grib_sarray* tempStrings_ = nullptr;
    grib_vdarray* tempDoubleValues_ = nullptr;
    int change_ref_value_operand_ = 0;
    size_t refValListSize_ = 0;
    long* refValList_ = nullptr;
    long refValIndex_ = 0;
    bufr_tableb_override* tableb_override_ = nullptr;
    int set_to_missing_if_out_of_range_ = 0;

    void restart_bitmap();
    void cancel_bitmap();
    int is_bitmap_start_defined();
    size_t get_length();
    void tableB_override_store_ref_val(grib_context*, int, long);
    int tableB_override_get_ref_val(int, long*);
    void tableB_override_clear(grib_context*);
    int tableB_override_set_key(grib_handle*);
    int get_descriptors();
    int decode_string_array(grib_context*, unsigned char*, long*, bufr_descriptor*);
    int encode_string_array(grib_context*, grib_buffer*, long*, bufr_descriptor*, grib_sarray*);
    int encode_double_array(grib_context*, grib_buffer*, long*, bufr_descriptor*, grib_darray*);
    int encode_double_value(grib_context*, grib_buffer*, long*, bufr_descriptor*, double);
    char* decode_string_value(grib_context*, unsigned char*, long*, bufr_descriptor*, int*);
    double decode_double_value(grib_context*, unsigned char*, long*, bufr_descriptor*, int, int*);
    int encode_new_bitmap(grib_context*, grib_buffer*, long*, int);
    int encode_overridden_reference_value(grib_context*, grib_buffer*, long*, bufr_descriptor*);
    int build_bitmap(unsigned char*, long*, int, grib_iarray*, int);
    int consume_bitmap(int);
    int build_bitmap_new_data(unsigned char*, long*, int, grib_iarray*, int);
    int get_next_bitmap_descriptor_index_new_bitmap(grib_iarray*, int);
    int get_next_bitmap_descriptor_index(grib_iarray*, grib_darray*);
    void push_zero_element(grib_darray*);
    grib_accessor* create_attribute_variable(const char*, grib_section*, int, char*, double, long, unsigned long);
    grib_accessor* create_accessor_from_descriptor(grib_accessor*, grib_section*, long, long, int, int, int, int);
    grib_iarray* set_subset_list( grib_context*, long, long, long, const long*, size_t);
    void print_bitmap_debug_info(grib_context*, bitmap_s*, grib_accessors_list*, int);
    int create_keys(long, long, long);
    void set_input_replications(grib_handle*);
    void set_input_bitmap(grib_handle*);
    int process_elements(int, long, long, long);
    void self_clear();
    grib_darray* decode_double_array(grib_context* c, unsigned char* data, long* pos, bufr_descriptor* bd, int canBeMissing, int*);

    friend int check_end_data(grib_context*, bufr_descriptor*, grib_accessor_bufr_data_array_t*, int);
    friend int decode_element(grib_context*, grib_accessor_bufr_data_array_t*, int, grib_buffer*, unsigned char*, long*, int, bufr_descriptor*, long, grib_darray*, grib_sarray*);
    friend int decode_replication(grib_context*, grib_accessor_bufr_data_array_t*, int, grib_buffer*, unsigned char*, long*, int, long, grib_darray*, long*);
    friend int encode_new_element(grib_context*, grib_accessor_bufr_data_array_t*, int, grib_buffer*, unsigned char*, long*, int, bufr_descriptor*, long, grib_darray*, grib_sarray*);
    friend int encode_new_replication(grib_context*, grib_accessor_bufr_data_array_t*, int, grib_buffer*, unsigned char*, long*, int, long, grib_darray*, long*);
    friend int encode_element(grib_context*, grib_accessor_bufr_data_array_t*, int, grib_buffer*, unsigned char*, long*, int, bufr_descriptor*, long, grib_darray*, grib_sarray*);
    friend int encode_replication(grib_context*, grib_accessor_bufr_data_array_t*, int, grib_buffer*, unsigned char*, long*, int, long, grib_darray*, long*);
};
