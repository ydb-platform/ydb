/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#pragma once

#include "grib_api_internal.h"
#include "dumper/grib_dumper.h"

namespace eccodes {
class Dumper;
}

class grib_accessor {
public:
  grib_accessor()
      : context_(nullptr),
        name_(nullptr),
        class_name_(nullptr),
        name_space_(nullptr),
        h_(nullptr),
        creator_(nullptr),
        length_(0),
        offset_(0),
        parent_(nullptr),
        next_(nullptr),
        previous_(nullptr),
        flags_(0),
        sub_section_(nullptr),
        dirty_(0),
        same_(nullptr),
        loop_(0),
        vvalue_(nullptr),
        set_(nullptr),
        parent_as_attribute_(nullptr) {}

  grib_accessor(const char *name)
      : context_(nullptr),
        name_(name),
        class_name_(nullptr),
        name_space_(nullptr),
        h_(nullptr),
        creator_(nullptr),
        length_(0),
        offset_(0),
        parent_(nullptr),
        next_(nullptr),
        previous_(nullptr),
        flags_(0),
        sub_section_(nullptr),
        dirty_(0),
        same_(nullptr),
        loop_(0),
        vvalue_(nullptr),
        set_(nullptr),
        parent_as_attribute_(nullptr) {}
  virtual ~grib_accessor() {}

  virtual void init_accessor(const long, grib_arguments *) = 0;
  virtual void dump(eccodes::Dumper *f) = 0;
  virtual int pack_missing() = 0;
  // virtual int grib_pack_zero(grib_accessor* a) = 0;
  virtual int is_missing_internal() = 0;
  virtual int pack_double(const double *v, size_t *len) = 0;
  virtual int pack_float(const float *v, size_t *len) = 0;
  virtual int pack_expression(grib_expression *e) = 0;
  virtual int pack_string(const char *v, size_t *len) = 0;
  virtual int pack_string_array(const char **v, size_t *len) = 0;
  virtual int pack_long(const long *v, size_t *len) = 0;
  virtual int pack_bytes(const unsigned char *v, size_t *len) = 0;
  virtual int unpack_bytes(unsigned char *v, size_t *len) = 0;
  virtual int unpack_double_subarray(double *v, size_t start, size_t len) = 0;
  virtual int unpack_double(double *v, size_t *len) = 0;
  virtual int unpack_float(float *v, size_t *len) = 0;
  virtual int unpack_double_element(size_t i, double *v) = 0;
  virtual int unpack_float_element(size_t i, float *v) = 0;
  virtual int unpack_double_element_set(const size_t *index_array, size_t len, double *val_array) = 0;
  virtual int unpack_float_element_set(const size_t *index_array, size_t len, float *val_array) = 0;
  virtual int unpack_string(char *v, size_t *len) = 0;
  virtual int unpack_string_array(char **v, size_t *len) = 0;
  virtual int unpack_long(long *v, size_t *len) = 0;
  virtual long get_native_type() = 0;
  virtual long get_next_position_offset() = 0;
  virtual size_t string_length() = 0;
  virtual long byte_offset() = 0;
  virtual long byte_count() = 0;
  virtual int value_count(long *count) = 0;
  virtual int notify_change(grib_accessor *changed) = 0;
  virtual grib_accessor *clone(grib_section *s, int *err) = 0;
  virtual void update_size(size_t len) = 0;
  virtual int nearest_smaller_value(double val, double *nearest) = 0;
  virtual size_t preferred_size(int from_handle) = 0;
  virtual grib_accessor *next_accessor() = 0;
  virtual void resize(size_t new_size) = 0;
  virtual void destroy(grib_context *ct) = 0;
  virtual int compare_accessors(grib_accessor *a2, int compare_flags);
  virtual int compare(grib_accessor *) = 0;
  virtual int add_attribute(grib_accessor *attr, int nest_if_clash);
  virtual grib_accessor *get_attribute_index(const char *name, int *index);
  virtual int has_attributes();
  virtual grib_accessor *get_attribute(const char *name);
  virtual void init(const long, grib_arguments *) = 0;
  virtual void post_init() = 0;
  virtual grib_section *sub_section() = 0;
  virtual grib_accessor *create_empty_accessor() = 0;
  virtual int is_missing() = 0;
  virtual long next_offset() = 0;
  virtual grib_accessor *next(grib_accessor *, int) = 0;
  virtual int clear() = 0;
  virtual grib_accessor *make_clone(grib_section *, int *) = 0;

public:
  // TODO(maee): make private
  grib_context *context_     = nullptr;
  const char *name_          = nullptr; // name of the accessor
  const char *class_name_    = nullptr; // name of the class (Artifact from C version of ecCodes)
  const char *name_space_    = nullptr; // namespace to which the accessor belongs
  grib_handle *h_            = nullptr;
  grib_action *creator_      = nullptr; // action that created the accessor
  long length_               = 0;       // byte length of the accessor
  long offset_               = 0;       // offset of the data in the buffer
  grib_section *parent_      = nullptr; // section to which the accessor is attached
  grib_accessor *next_       = nullptr; // next accessor in list
  grib_accessor *previous_   = nullptr; // next accessor in list
  unsigned long flags_       = 0;       // Various flags
  grib_section *sub_section_ = nullptr;

  const char *all_names_[MAX_ACCESSOR_NAMES]       = {0,}; // name of the accessor
  const char *all_name_spaces_[MAX_ACCESSOR_NAMES] = {0,}; // namespace to which the accessor belongs
  int dirty_ = 0;

  grib_accessor *same_        = nullptr; // accessors with the same name
  long loop_                  = 0;       // used in lists
  grib_virtual_value *vvalue_ = nullptr; // virtual value used when transient flag on
  const char *set_            = nullptr;
  grib_accessor *attributes_[MAX_ACCESSOR_ATTRIBUTES] = {0,}; // attributes are accessors
  grib_accessor *parent_as_attribute_ = nullptr;
};
