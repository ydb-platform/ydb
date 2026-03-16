/*
 * (C) Copyright 2017- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

typedef enum ProductKind {PRODUCT_ANY, PRODUCT_GRIB, PRODUCT_BUFR, PRODUCT_METAR, PRODUCT_GTS, PRODUCT_TAF} ProductKind;

#define GRIB_TYPE_UNDEFINED 0
#define GRIB_TYPE_LONG 1
#define GRIB_TYPE_DOUBLE 2
#define GRIB_TYPE_STRING 3
#define GRIB_TYPE_BYTES 4
#define GRIB_TYPE_SECTION 5
#define GRIB_TYPE_LABEL 6
#define GRIB_TYPE_MISSING 7

#define GRIB_KEYS_ITERATOR_SKIP_READ_ONLY          1
#define GRIB_KEYS_ITERATOR_SKIP_EDITION_SPECIFIC   4
#define GRIB_KEYS_ITERATOR_SKIP_CODED              8
#define GRIB_KEYS_ITERATOR_SKIP_COMPUTED           16
#define GRIB_KEYS_ITERATOR_SKIP_DUPLICATES         32
#define GRIB_KEYS_ITERATOR_SKIP_FUNCTION           64

typedef struct grib_values grib_values;

struct grib_values {
  const char* name;
  int         type;
  long        long_value;
  double      double_value;
  const char* string_value;
  int         error;
  int         has_value;
  int         equal;
  grib_values* next;
};

typedef struct grib_handle    grib_handle;
typedef struct grib_multi_handle    grib_multi_handle;
typedef struct grib_context   grib_context;
typedef struct grib_iterator  grib_iterator;
typedef struct grib_nearest  grib_nearest;
typedef struct grib_keys_iterator    grib_keys_iterator;
typedef struct bufr_keys_iterator    bufr_keys_iterator;
typedef struct grib_index grib_index;

grib_index* grib_index_new_from_file(grib_context* c,
                            char* filename,const char* keys,int *err);

int grib_index_add_file(grib_index *index, const char *filename);
int grib_index_write(grib_index *index, const char *filename);
grib_index* grib_index_read(grib_context* c,const char* filename,int *err);

int grib_index_get_size(const grib_index* index,const char* key,size_t* size);
int grib_index_get_long(const grib_index* index,const char* key,
                        long* values,size_t *size);
int grib_index_get_double(const grib_index* index, const char* key,
                          double* values, size_t* size);
int grib_index_get_string(const grib_index* index,const char* key,
                          char** values,size_t *size);
int grib_index_select_long(grib_index* index,const char* key,long value);
int grib_index_select_double(grib_index* index,const char* key,double value);
int grib_index_select_string(grib_index* index,const char* key,char* value);
grib_handle* grib_handle_new_from_index(grib_index* index,int *err);

void grib_index_delete(grib_index* index);

int grib_count_in_file(grib_context* c, FILE* f,int* n);
grib_handle* grib_handle_new_from_file(grib_context* c, FILE* f, int* error);
grib_handle* grib_handle_new_from_message_copy(grib_context* c, const void* data, size_t data_len);
grib_handle* grib_handle_new_from_samples (grib_context* c, const char* sample_name);
grib_handle* grib_handle_clone(const grib_handle* h);
grib_handle* grib_handle_clone_headers_only(const grib_handle* h);
int grib_handle_delete(grib_handle* h);
grib_multi_handle* grib_multi_handle_new(grib_context* c);
int grib_multi_handle_append(grib_handle* h,int start_section,grib_multi_handle* mh);
int grib_multi_handle_delete(grib_multi_handle* mh);
int grib_multi_handle_write(grib_multi_handle* mh,FILE* f);

int grib_get_message(const grib_handle* h ,const void** message, size_t *message_length);

grib_iterator* grib_iterator_new(const grib_handle* h, unsigned long flags, int* error);
int grib_iterator_next(grib_iterator *i, double* lat,double* lon,double* value);
int grib_iterator_delete(grib_iterator *i);
grib_nearest* grib_nearest_new(const grib_handle* h, int* error);

int grib_nearest_find(grib_nearest *nearest, const grib_handle* h, double inlat, double inlon,
                      unsigned long flags,double* outlats,double* outlons,
                      double* values,double* distances,int* indexes,size_t *len);

int grib_nearest_delete(grib_nearest *nearest);

int grib_nearest_find_multiple(const grib_handle* h, int is_lsm,
    const double* inlats, const double* inlons, long npoints,
    double* outlats, double* outlons,
    double* values, double* distances, int* indexes);

int grib_get_offset(const grib_handle* h, const char* key, size_t* offset);
int grib_get_size(const grib_handle* h, const char* key,size_t *size);

int grib_get_length(const grib_handle* h, const char* key,size_t *length);
int grib_get_long(const grib_handle* h, const char* key, long* value);
int grib_get_double(const grib_handle* h, const char* key, double* value);
int grib_get_double_element(const grib_handle* h, const char* key, int i, double* value);
int grib_get_double_elements(const grib_handle* h, const char* key, const int* index_array, long size, double* value);
int grib_get_string(const grib_handle* h, const char* key, char* value, size_t *length);
int grib_get_string_array(const grib_handle* h, const char* key, char** vals, size_t *length);
int grib_get_double_array(const grib_handle* h, const char* key, double* vals, size_t *length);
int grib_get_float_array(const grib_handle* h, const char* key, float* vals, size_t *length);
int grib_get_long_array(const grib_handle* h, const char* key, long* vals, size_t *length);

int grib_copy_namespace(grib_handle* dest, const char* name, grib_handle* src);
int grib_set_long(grib_handle* h, const char* key, long val);
int grib_set_double(grib_handle* h, const char* key, double val);
int grib_set_string(grib_handle* h, const char* key, const char* mesg, size_t *length);
int grib_set_double_array(grib_handle* h, const char*  key , const double*        vals   , size_t length);
int grib_set_long_array(grib_handle* h, const char*  key , const long* vals, size_t length);

int grib_set_string_array(grib_handle* h, const char *key, const char **vals, size_t length);

void grib_dump_content(const grib_handle* h, FILE* out, const char* mode, unsigned long option_flags, void* arg);
grib_context* grib_context_get_default(void);
void grib_context_delete(grib_context* c);

void grib_gts_header_on(grib_context* c);
void grib_gts_header_off(grib_context* c);
void grib_gribex_mode_on(grib_context* c);
void grib_gribex_mode_off(grib_context* c);
void grib_context_set_definitions_path(grib_context* c, const char* path);
void grib_context_set_debug(grib_context* c, int mode);
void grib_context_set_data_quality_checks(grib_context* c, int val);
void grib_context_set_samples_path(grib_context* c, const char* path);
void grib_multi_support_on(grib_context* c);
void grib_multi_support_off(grib_context* c);
void grib_multi_support_reset_file(grib_context* c, FILE* f);
long grib_get_api_version(void);

char* grib_samples_path(const grib_context *c);
char* grib_definition_path(const grib_context *c);

grib_keys_iterator* grib_keys_iterator_new(grib_handle* h,unsigned long filter_flags, const char* name_space);
bufr_keys_iterator* codes_bufr_keys_iterator_new(grib_handle* h, unsigned long filter_flags);

int grib_keys_iterator_next(grib_keys_iterator* kiter);
int codes_bufr_keys_iterator_next(bufr_keys_iterator* kiter);

const char* grib_keys_iterator_get_name(const grib_keys_iterator *kiter);
char* codes_bufr_keys_iterator_get_name(const bufr_keys_iterator* kiter);

int grib_keys_iterator_delete(grib_keys_iterator* kiter);
int codes_bufr_keys_iterator_delete(bufr_keys_iterator* kiter);

int grib_keys_iterator_rewind(grib_keys_iterator* kiter);
int codes_bufr_keys_iterator_rewind(bufr_keys_iterator* kiter);

int grib_keys_iterator_set_flags(grib_keys_iterator *kiter,unsigned long flags);
const char* grib_get_error_message(int code);

int grib_get_native_type(const grib_handle* h, const char* name,int* type);

/* aa: changed off_t to long int */
int grib_get_message_offset(const grib_handle* h,long int* offset);

int grib_set_values(grib_handle* h,grib_values*  grib_values , size_t arg_count);
int grib_is_missing(const grib_handle* h, const char* key, int* err);
int grib_is_defined(const grib_handle* h, const char* key);
int grib_set_missing(grib_handle* h, const char* key);

int grib_get_message_size(const grib_handle* h,size_t* size);
int parse_keyval_string(const char *grib_tool, char *arg, int values_required, int default_type, grib_values values[], int *count);

int grib_get_data(const grib_handle *h, double *lats, double *lons, double *values);
int grib_get_gaussian_latitudes(long trunc, double* lats);

int codes_is_feature_enabled(const char* feature);
int codes_get_features(char* result, size_t* length, int select);

/* EXPERIMENTAL */
typedef struct codes_bufr_header {
    unsigned long message_offset;
    unsigned long message_size;

    /* Section 0 keys */
    long edition;

    /* Section 1 keys */
    long masterTableNumber;
    long bufrHeaderSubCentre;
    long bufrHeaderCentre;
    long updateSequenceNumber;
    long dataCategory;
    long dataSubCategory;
    long masterTablesVersionNumber;
    long localTablesVersionNumber;

    long typicalYear;
    long typicalMonth;
    long typicalDay;
    long typicalHour;
    long typicalMinute;
    long typicalSecond;
    long typicalDate;
    long typicalTime;

    long internationalDataSubCategory;

    long localSectionPresent;
    long ecmwfLocalSectionPresent;

    /* ECMWF local section keys */
    long rdbType;
    long oldSubtype;
    long rdbSubtype;
    char ident[9];
    long localYear;
    long localMonth;
    long localDay;
    long localHour;
    long localMinute;
    long localSecond;

    long rdbtimeDay;
    long rdbtimeHour;
    long rdbtimeMinute;
    long rdbtimeSecond;

    long rectimeDay;
    long rectimeHour;
    long rectimeMinute;
    long rectimeSecond;
    long restricted;

    long   isSatellite;
    double localLongitude1;
    double localLatitude1;
    double localLongitude2;
    double localLatitude2;
    double localLatitude;
    double localLongitude;
    long   localNumberOfObservations;
    long   satelliteID;
    long   qualityControl;
    long   newSubtype;
    long   daLoop;

    /* Section 3 keys */
    unsigned long numberOfSubsets;
    long observedData;
    long compressedData;

} codes_bufr_header;


/** No error */
#define GRIB_SUCCESS		0
/** End of resource reached */
#define GRIB_END_OF_FILE		-1
/** Internal error */
#define GRIB_INTERNAL_ERROR		-2
/** Passed buffer is too small */
#define GRIB_BUFFER_TOO_SMALL		-3
/** Function not yet implemented */
#define GRIB_NOT_IMPLEMENTED		-4
/** Missing 7777 at end of message */
#define GRIB_7777_NOT_FOUND		-5
/** Passed array is too small */
#define GRIB_ARRAY_TOO_SMALL		-6
/** File not found */
#define GRIB_FILE_NOT_FOUND		-7
/** Code not found in code table */
#define GRIB_CODE_NOT_FOUND_IN_TABLE		-8
/** Array size mismatch */
#define GRIB_WRONG_ARRAY_SIZE		-9
/** Key/value not found */
#define GRIB_NOT_FOUND		-10
/** Input output problem */
#define GRIB_IO_PROBLEM		-11
/** Message invalid */
#define GRIB_INVALID_MESSAGE		-12
/** Decoding invalid */
#define GRIB_DECODING_ERROR		-13
/** Encoding invalid */
#define GRIB_ENCODING_ERROR		-14
/** Code cannot unpack because of string too small */
#define GRIB_NO_MORE_IN_SET		-15
/** Problem with calculation of geographic attributes */
#define GRIB_GEOCALCULUS_PROBLEM		-16
/** Memory allocation error */
#define GRIB_OUT_OF_MEMORY		-17
/** Value is read only */
#define GRIB_READ_ONLY		-18
/** Invalid argument */
#define GRIB_INVALID_ARGUMENT		-19
/** Null handle */
#define GRIB_NULL_HANDLE		-20
/** Invalid section number */
#define GRIB_INVALID_SECTION_NUMBER		-21
/** Value cannot be missing */
#define GRIB_VALUE_CANNOT_BE_MISSING		-22
/** Wrong message length */
#define GRIB_WRONG_LENGTH		-23
/** Invalid key type */
#define GRIB_INVALID_TYPE		-24
/** Unable to set step */
#define GRIB_WRONG_STEP		-25
/** Wrong units for step (step must be integer) */
#define GRIB_WRONG_STEP_UNIT		-26
/** Invalid file id */
#define GRIB_INVALID_FILE		-27
/** Invalid grib id */
#define GRIB_INVALID_GRIB		-28
/** Invalid index id */
#define GRIB_INVALID_INDEX		-29
/** Invalid iterator id */
#define GRIB_INVALID_ITERATOR		-30
/** Invalid keys iterator id */
#define GRIB_INVALID_KEYS_ITERATOR		-31
/** Invalid nearest id */
#define GRIB_INVALID_NEAREST		-32
/** Invalid order by */
#define GRIB_INVALID_ORDERBY		-33
/** Missing a key from the fieldset */
#define GRIB_MISSING_KEY		-34
/** The point is out of the grid area */
#define GRIB_OUT_OF_AREA		-35
/** Concept no match */
#define GRIB_CONCEPT_NO_MATCH		-36
/** Hash array no match */
#define GRIB_HASH_ARRAY_NO_MATCH		-37
/** Definitions files not found */
#define GRIB_NO_DEFINITIONS		-38
/** Wrong type while packing */
#define GRIB_WRONG_TYPE		-39
/** End of resource */
#define GRIB_END		-40
/** Unable to code a field without values */
#define GRIB_NO_VALUES		-41
/** Grid description is wrong or inconsistent */
#define GRIB_WRONG_GRID		-42
/** End of index reached */
#define GRIB_END_OF_INDEX		-43
/** Null index */
#define GRIB_NULL_INDEX		-44
/** End of resource reached when reading message */
#define GRIB_PREMATURE_END_OF_FILE		-45
/** An internal array is too small */
#define GRIB_INTERNAL_ARRAY_TOO_SMALL		-46
/** Message is too large for the current architecture */
#define GRIB_MESSAGE_TOO_LARGE		-47
/** Constant field */
#define GRIB_CONSTANT_FIELD		-48
/** Switch unable to find a matching case */
#define GRIB_SWITCH_NO_MATCH		-49
/** Underflow */
#define GRIB_UNDERFLOW		-50
/** Message malformed */
#define GRIB_MESSAGE_MALFORMED		-51
/** Index is corrupted */
#define GRIB_CORRUPTED_INDEX		-52
/** Invalid number of bits per value */
#define GRIB_INVALID_BPV		-53
/** Edition of two messages is different */
#define GRIB_DIFFERENT_EDITION		-54
/** Value is different */
#define GRIB_VALUE_DIFFERENT		-55
/** Invalid key value */
#define GRIB_INVALID_KEY_VALUE		-56
/** String is smaller than requested */
#define GRIB_STRING_TOO_SMALL		-57
/** Wrong type conversion */
#define GRIB_WRONG_CONVERSION		-58
/** Missing BUFR table entry for descriptor */
#define GRIB_MISSING_BUFR_ENTRY		-59
/** Null pointer */
#define GRIB_NULL_POINTER		-60
/** Attribute is already present, cannot add */
#define GRIB_ATTRIBUTE_CLASH		-61
/** Too many attributes. Increase MAX_ACCESSOR_ATTRIBUTES */
#define GRIB_TOO_MANY_ATTRIBUTES		-62
/** Attribute not found. */
#define GRIB_ATTRIBUTE_NOT_FOUND		-63
/** Edition not supported. */
#define GRIB_UNSUPPORTED_EDITION		-64
/** Value out of coding range */
#define GRIB_OUT_OF_RANGE		-65
/** Size of bitmap is incorrect */
#define GRIB_WRONG_BITMAP_SIZE		-66
/** Functionality not enabled */
#define GRIB_FUNCTIONALITY_NOT_ENABLED		-67
