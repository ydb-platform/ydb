/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_scaling.h"
#include "grib_accessor_class_bufr_data_array.h"
#include "grib_accessor_class_expanded_descriptors.h"
#include "grib_accessor_class_bufr_data_element.h"
#include "grib_accessor_class_variable.h"
#include "ecc_numeric_limits.h"

grib_accessor_bufr_data_array_t _grib_accessor_bufr_data_array{};
grib_accessor* grib_accessor_bufr_data_array = &_grib_accessor_bufr_data_array;

typedef int (*codec_element_proc)(grib_context*, grib_accessor_bufr_data_array_t*, int, grib_buffer*, unsigned char*, long*, int, bufr_descriptor*, long, grib_darray*, grib_sarray*);
typedef int (*codec_replication_proc)(grib_context*, grib_accessor_bufr_data_array_t*, int, grib_buffer*, unsigned char*, long*, int, long, grib_darray*, long*);

#define MAX_NESTED_REPLICATIONS 8

#define PROCESS_DECODE   0
#define PROCESS_NEW_DATA 1
#define PROCESS_ENCODE   2

#define OVERRIDDEN_REFERENCE_VALUES_KEY "inputOverriddenReferenceValues"

/* Set the error code, if it is bad and we should fail (default case), return */
/* variable 'err' is assumed to be pointer to int */
/* If BUFRDC mode is enabled, then we tolerate problems like wrong data section length */
#define CHECK_END_DATA_RETURN(ctx, bd, b, size, retval) \
    {                                                   \
        *err = check_end_data(ctx, bd, b, size);        \
        if (*err != 0 && ctx->bufrdc_mode == 0)         \
            return retval;                              \
    }


void grib_accessor_bufr_data_array_t::restart_bitmap()
{
    bitmapCurrent_                         = -1;
    bitmapCurrentElementsDescriptorsIndex_ = bitmapStartElementsDescriptorsIndex_ - 1;
}

void grib_accessor_bufr_data_array_t::cancel_bitmap()
{
    bitmapCurrent_ = -1;
    bitmapStart_   = -1;
}

int grib_accessor_bufr_data_array_t::is_bitmap_start_defined()
{
    return bitmapStart_ == -1 ? 0 : 1;
}

size_t grib_accessor_bufr_data_array_t::get_length()
{
    size_t len           = 0;
    const grib_handle* h = grib_handle_of_accessor(this);

    grib_get_size(h, bufrDataEncodedName_, &len);

    return len;
}

/* Operator 203YYY: Store the TableB code and changed reference value in linked list */
void grib_accessor_bufr_data_array_t::tableB_override_store_ref_val(grib_context* c, int code, long new_ref_val)
{
    bufr_tableb_override* tb = (bufr_tableb_override*)grib_context_malloc_clear(c, sizeof(bufr_tableb_override));
    tb->code                 = code;
    tb->new_ref_val          = new_ref_val;
    if (!tableb_override_) {
        tableb_override_ = tb;
    }
    else {
        /*Add to end of linked list*/
        bufr_tableb_override* q = tableb_override_;
        while (q->next)
            q = q->next;
        q->next = tb;
    }
}

/* Operator 203YYY: Retrieve changed reference value from linked list */
int grib_accessor_bufr_data_array_t::tableB_override_get_ref_val(int code, long* out_ref_val)
{
    bufr_tableb_override* p = tableb_override_;
    while (p) {
        if (p->code == code) {
            *out_ref_val = p->new_ref_val;
            return GRIB_SUCCESS;
        }
        p = p->next;
    }
    return GRIB_NOT_FOUND;
}

/* Operator 203YYY: Clear and free linked list */
void grib_accessor_bufr_data_array_t::tableB_override_clear(grib_context* c)
{
    bufr_tableb_override* tb = tableb_override_;
    while (tb) {
        bufr_tableb_override* n = tb->next;
        grib_context_free(c, tb);
        tb = n;
    }
    tableb_override_ = NULL;
}

/* Operator 203YYY: Copy contents of linked list to the transient array key */
int grib_accessor_bufr_data_array_t::tableB_override_set_key(grib_handle* h)
{
    int err                  = GRIB_SUCCESS;
    size_t size              = 0;
    long* refVals            = NULL;
    grib_iarray* refValArray = grib_iarray_new(10, 10);
    bufr_tableb_override* p  = tableb_override_;
    while (p) {
        grib_iarray_push(refValArray, p->new_ref_val);
        p = p->next;
    }
    size = grib_iarray_used_size(refValArray);
    if (size > 0) {
        refVals = grib_iarray_get_array(refValArray);
        err     = grib_set_long_array(h, OVERRIDDEN_REFERENCE_VALUES_KEY, refVals, size);
        grib_context_free(h->context, refVals);
    }
    grib_iarray_delete(refValArray);
    return err;
}
/* Check numBits is sufficient for entries in the overridden reference values list*/
static int check_overridden_reference_values(const grib_context* c, long* refValList, size_t refValListSize, int numBits)
{
    const long maxval = NumericLimits<long>::max(numBits);
    const long minval = NumericLimits<long>::min(numBits);
    size_t i          = 0;
    for (i = 0; i < refValListSize; ++i) {
        grib_context_log(c, GRIB_LOG_DEBUG, "check_overridden_reference_values: refValList[%ld]=%ld", i, refValList[i]);
        if (refValList[i] < minval || refValList[i] > maxval) {
            grib_context_log(c, GRIB_LOG_ERROR, "Overridden reference value: entry %ld (%ld) does not fit in %d bits (specified by operator 203)",
                             refValList[i], i, numBits);
            return GRIB_OUT_OF_RANGE;
        }
    }
    return GRIB_SUCCESS;
}

// void tableB_override_dump(grib_accessor_bufr_data_array_t *self)
// {
//     bufr_tableb_override* p = self->tableb_override_ ;
//     int i = 1;
//     while (p) {
//         printf("ECCODES DEBUG: Table B Override: [%d] code=%d, rv=%ld\n", i, p->code, p->new_ref_val);
//         p = p->next;
//         ++i;
//     }
// }

#define DYN_ARRAY_SIZE_INIT 1000 /* Initial size for grib_iarray_new and grib_darray_new */
#define DYN_ARRAY_SIZE_INCR 1000 /* Increment size for grib_iarray_new and grib_darray_new */

void grib_accessor_bufr_data_array_t::init(const long v, grib_arguments* params)
{
    grib_accessor_gen_t::init(v, params);
    int n                      = 0;
    const char* dataKeysName   = NULL;
    grib_accessor* dataKeysAcc = NULL;

    unitsName_                             = NULL;
    canBeMissing_                          = NULL;
    numberOfSubsets_                       = 0;
    compressedData_                        = 0;
    bitmapStartElementsDescriptorsIndex_   = 0;
    bitmapCurrentElementsDescriptorsIndex_ = 0;
    bitmapSize_                            = 0;
    bitmapStart_                           = 0;
    bitmapCurrent_                         = 0;
    dataAccessors_                         = NULL;
    nInputBitmap_                          = 0;
    iInputBitmap_                          = 0;
    inputReplications_                     = NULL;
    nInputReplications_                    = 0;
    iInputReplications_                    = 0;
    inputExtendedReplications_             = NULL;
    nInputExtendedReplications_            = 0;
    iInputExtendedReplications_            = 0;
    inputShortReplications_                = NULL;
    nInputShortReplications_               = 0;
    iInputShortReplications_               = 0;
    iss_list_                              = NULL;
    tempStrings_                           = NULL;


    bufrDataEncodedName_          = params->get_name(grib_handle_of_accessor(this), n++);
    numberOfSubsetsName_          = params->get_name(grib_handle_of_accessor(this), n++);
    expandedDescriptorsName_      = params->get_name(grib_handle_of_accessor(this), n++);
    flagsName_                    = params->get_name(grib_handle_of_accessor(this), n++);
    elementsDescriptorsIndexName_ = params->get_name(grib_handle_of_accessor(this), n++);
    compressedDataName_           = params->get_name(grib_handle_of_accessor(this), n++);
    dataKeysName                  = params->get_name(grib_handle_of_accessor(this), n++);

    dataKeysAcc               = grib_find_accessor(grib_handle_of_accessor(this), dataKeysName);
    dataKeys_                 = dataKeysAcc->parent_;
    do_decode_                = 1;
    elementsDescriptorsIndex_ = 0;
    numericValues_            = 0;
    tempDoubleValues_         = 0;
    stringValues_             = 0;
    cancel_bitmap();
    expanded_                       = 0;
    expandedAccessor_               = 0;
    dataAccessorsTrie_              = 0;
    change_ref_value_operand_       = 0;    /* Operator 203YYY: 0, 255 or YYY */
    refValListSize_                 = 0;    /* Operator 203YYY: size of overridden reference values array */
    refValList_                     = NULL; /* Operator 203YYY: overridden reference values array */
    refValIndex_                    = 0;    /* Operator 203YYY: index into overridden reference values array */
    tableb_override_                = NULL; /* Operator 203YYY: Table B lookup linked list */
    set_to_missing_if_out_of_range_ = 0;    /* By default fail if out of range */

    length_        = 0;
    bitsToEndData_ = get_length() * 8;
    unpackMode_    = CODES_BUFR_UNPACK_STRUCTURE;
    inputBitmap_   = NULL;
    /* ECCODES_ASSERT(length_ >=0); */
}

// void clean_string(char* s,int len)
// {
//     int i=len-1;
//     while (i) {
//         if (!isalnum(s[i])) s[i]=0;
//         else break;
//         i--;
//     }
// }

int check_end_data(grib_context* c, bufr_descriptor* bd, grib_accessor_bufr_data_array_t* self, int size)
{
    const int saved_bitsToEndData = self->bitsToEndData_;
    if (c->debug == 1)
        grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: \tbitsToEndData=%d elementSize=%d", self->bitsToEndData_, size);
    self->bitsToEndData_ -= size;
    if (self->bitsToEndData_ < 0) {
        grib_context_log(c, GRIB_LOG_ERROR, "BUFR data decoding: Number of bits left=%d but element size=%d", saved_bitsToEndData, size);
        if (bd)
            grib_context_log(c, GRIB_LOG_ERROR, "BUFR data decoding: code=%06ld key=%s", bd->code, bd->shortName);
        return GRIB_DECODING_ERROR;
    }
    return 0;
}

void grib_accessor_bufr_data_array_t::self_clear()
{
    grib_context_free(context_, canBeMissing_);
    grib_vdarray_delete_content(numericValues_);
    grib_vdarray_delete(numericValues_);

    if (stringValues_) {
        /*printf("dbg self_clear: clear %p\n", (void*)(stringValues_ ));*/
        grib_vsarray_delete_content(stringValues_);
        grib_vsarray_delete(stringValues_);
        stringValues_ = NULL;
    }
    grib_viarray_delete_content(elementsDescriptorsIndex_);
    grib_viarray_delete(elementsDescriptorsIndex_);
    if (inputReplications_)
        grib_context_free(context_, inputReplications_);
    if (inputExtendedReplications_)
        grib_context_free(context_, inputExtendedReplications_);
    if (inputShortReplications_)
        grib_context_free(context_, inputShortReplications_);
    change_ref_value_operand_ = 0;
    refValListSize_           = 0;
    if (refValList_)
        grib_context_free(context_, refValList_);
    refValIndex_ = 0;
    tableB_override_clear(context_);
    set_to_missing_if_out_of_range_ = 0;
    if (inputBitmap_) grib_context_free(context_, inputBitmap_);
}

long grib_accessor_bufr_data_array_t::get_native_type()
{
    return GRIB_TYPE_DOUBLE;
}

long grib_accessor_bufr_data_array_t::byte_count()
{
    return 0;
}

long grib_accessor_bufr_data_array_t::byte_offset()
{
    return offset_;
}

long grib_accessor_bufr_data_array_t::next_offset()
{
    return offset_;
}

int grib_accessor_bufr_data_array_t::pack_long(const long* val, size_t* len)
{
    do_decode_ = 1;

    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_bufr_data_array_t::pack_double(const double* val, size_t* len)
{
    do_decode_ = 1;
    return process_elements(PROCESS_ENCODE, 0, 0, 0);
}

grib_vsarray* grib_accessor_bufr_data_array_t::accessor_bufr_data_array_get_stringValues()
{
    process_elements(PROCESS_DECODE, 0, 0, 0);
    return stringValues_;
}

grib_accessors_list* grib_accessor_bufr_data_array_t::accessor_bufr_data_array_get_dataAccessors()
{
    return dataAccessors_;
}

grib_trie_with_rank* grib_accessor_bufr_data_array_t::accessor_bufr_data_array_get_dataAccessorsTrie()
{
    return dataAccessorsTrie_;
}

void grib_accessor_bufr_data_array_t::accessor_bufr_data_array_set_unpackMode(int unpackMode)
{
    unpackMode_ = unpackMode;
}

int grib_accessor_bufr_data_array_t::get_descriptors()
{
    int ret         = 0, i, numberOfDescriptors;
    grib_handle* h  = grib_handle_of_accessor(this);
    grib_context* c = context_;

    if (!expandedAccessor_)
        expandedAccessor_ = dynamic_cast<grib_accessor_expanded_descriptors_t*>(grib_find_accessor(grib_handle_of_accessor(this), expandedDescriptorsName_));
    expanded_ = expandedAccessor_->grib_accessor_expanded_descriptors_get_expanded(&ret);
    if (ret != GRIB_SUCCESS)
        return ret;

    numberOfDescriptors = grib_bufr_descriptors_array_used_size(expanded_);
    if (canBeMissing_) grib_context_free(c, canBeMissing_);
    canBeMissing_ = (int*)grib_context_malloc_clear(c, numberOfDescriptors * sizeof(int));
    for (i = 0; i < numberOfDescriptors; i++)
        canBeMissing_[i] = grib_bufr_descriptor_can_be_missing(expanded_->v[i]);

    ret = grib_get_long(h, numberOfSubsetsName_, &(numberOfSubsets_));
    if (ret != GRIB_SUCCESS)
        return ret;
    ret = grib_get_long(h, compressedDataName_, &(compressedData_));

    return ret;
}

int grib_accessor_bufr_data_array_t::decode_string_array(grib_context* c, unsigned char* data, long* pos, bufr_descriptor* bd)
{
    int ret    = 0;
    int* err   = &ret;
    char* sval = 0;
    int j, modifiedWidth, width;
    grib_sarray* sa                        = grib_sarray_new(numberOfSubsets_, 10);
    int bufr_multi_element_constant_arrays = c->bufr_multi_element_constant_arrays;

    modifiedWidth = bd->width;

    sval = (char*)grib_context_malloc_clear(c, modifiedWidth / 8 + 1);
    CHECK_END_DATA_RETURN(c, bd, this, modifiedWidth, *err);

    if (*err) {
        grib_sarray_push(sa, sval);
        grib_vsarray_push(stringValues_, sa);
        return ret;
    }
    grib_decode_string(data, pos, modifiedWidth / 8, sval);
    CHECK_END_DATA_RETURN(c, bd, this, 6, *err);
    if (*err) {
        grib_sarray_push(sa, sval);
        grib_vsarray_push(stringValues_, sa);
        return ret;
    }
    width = grib_decode_unsigned_long(data, pos, 6);
    if (width) {
        CHECK_END_DATA_RETURN(c, bd, this, width * 8 * numberOfSubsets_, *err);
        if (*err) {
            grib_sarray_push(sa, sval);
            grib_vsarray_push(stringValues_, sa);
            return ret;
        }
        grib_context_free(c, sval);
        for (j = 0; j < numberOfSubsets_; j++) {
            sval = (char*)grib_context_malloc_clear(c, width + 1);
            grib_decode_string(data, pos, width, sval);
            grib_sarray_push(sa, sval);
        }
    }
    else {
        if (bufr_multi_element_constant_arrays) {
            for (j = 0; j < numberOfSubsets_; j++) {
                char* pStr = sval;
                if (j > 0)
                    pStr = strdup(sval);
                grib_sarray_push(sa, pStr);
            }
        }
        else {
            grib_sarray_push(sa, sval);
        }
    }
    grib_vsarray_push(stringValues_, sa);
    return ret;
}

grib_darray* grib_accessor_bufr_data_array_t::decode_double_array(grib_context* c, unsigned char* data, long* pos,
                                                                  bufr_descriptor* bd, int canBeMissing,
                                                                  int* err)
{
    grib_darray* ret = NULL;
    int j;
    size_t lval;
    int localReference, localWidth, modifiedWidth, modifiedReference;
    double modifiedFactor, dval;
    int bufr_multi_element_constant_arrays = c->bufr_multi_element_constant_arrays;

    *err = 0;

    modifiedReference = bd->reference;
    modifiedFactor    = bd->factor;
    modifiedWidth     = bd->width;

    CHECK_END_DATA_RETURN(c, bd, this, modifiedWidth + 6, NULL);
    if (*err) {
        dval = GRIB_MISSING_DOUBLE;
        lval = 0;
        grib_context_log(c, GRIB_LOG_DEBUG, " modifiedWidth=%d lval=%ld dval=%g", modifiedWidth, lval, dval);
        ret = grib_darray_new(DYN_ARRAY_SIZE_INIT, DYN_ARRAY_SIZE_INCR);
        grib_darray_push(ret, dval);
        *err = 0;
        return ret;
    }
    lval           = grib_decode_size_t(data, pos, modifiedWidth);
    localReference = (long)lval + modifiedReference;
    localWidth     = grib_decode_unsigned_long(data, pos, 6);
    grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: \tlocalWidth=%d", localWidth);
    ret = grib_darray_new(numberOfSubsets_, 50);
    if (localWidth) {
        CHECK_END_DATA_RETURN(c, bd, this, localWidth * numberOfSubsets_, NULL);
        if (*err) {
            dval = GRIB_MISSING_DOUBLE;
            lval = 0;
            grib_context_log(c, GRIB_LOG_DEBUG, " modifiedWidth=%d lval=%ld dval=%g", modifiedWidth, lval, dval);
            ret = grib_darray_new(DYN_ARRAY_SIZE_INIT, DYN_ARRAY_SIZE_INCR);
            grib_darray_push(ret, dval);
            *err = 0;
            return ret;
        }
        for (j = 0; j < numberOfSubsets_; j++) {
            lval = grib_decode_size_t(data, pos, localWidth);
            if (canBeMissing && grib_is_all_bits_one(lval, localWidth)) {
                dval = GRIB_MISSING_DOUBLE;
            }
            else {
                dval = ((long)lval + localReference) * modifiedFactor;
            }
            grib_darray_push(ret, dval);
        }
    }
    else {
        /* ECC-428 */
        if (canBeMissing && grib_is_all_bits_one(lval, modifiedWidth)) {
            dval = GRIB_MISSING_DOUBLE;
        }
        else {
            dval = localReference * modifiedFactor;
        }

        /* dataPresentIndicator is special and has to have SINGLE VALUE if constant array */
        if (bufr_multi_element_constant_arrays == 1 && bd->code == 31031) {
            bufr_multi_element_constant_arrays = 0;
        }

        if (bufr_multi_element_constant_arrays) {
            grib_context_log(c, GRIB_LOG_DEBUG, " modifiedWidth=%d lval=%ld dval=%g (const array multi values) %6.6ld", modifiedWidth, lval, dval, bd->code);
            for (j = 0; j < numberOfSubsets_; j++) {
                grib_darray_push(ret, dval);
            }
        }
        else {
            grib_context_log(c, GRIB_LOG_DEBUG, " modifiedWidth=%d lval=%ld dval=%g (const array single value) %6.6ld", modifiedWidth, lval, dval, bd->code);
            grib_darray_push(ret, dval);
        }
    }

    return ret;
}

int grib_accessor_bufr_data_array_t::encode_string_array(grib_context* c, grib_buffer* buff, long* pos, bufr_descriptor* bd, grib_sarray* stringValues)
{
    int err = 0, n, ival;
    int k, j, modifiedWidth, width;

    if (iss_list_ == NULL) {
        grib_context_log(c, GRIB_LOG_ERROR, "encode_string_array: iss_list_ ==NULL");
        return GRIB_INTERNAL_ERROR;
    }
    if (!stringValues) {
        return GRIB_INTERNAL_ERROR;
    }
    n = grib_iarray_used_size(iss_list_);

    if (n <= 0)
        return GRIB_NO_VALUES;

    if (grib_sarray_used_size(stringValues) == 1) {
        n    = 1;
        ival = 0;
    }
    else {
        ival = iss_list_->v[0];
    }

    if (n > grib_sarray_used_size(stringValues))
        return GRIB_ARRAY_TOO_SMALL;

    modifiedWidth = bd->width;

    grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + modifiedWidth);
    err = grib_encode_string(buff->data, pos, modifiedWidth / 8, stringValues->v[ival]);
    if (err) {
        grib_context_log(c, GRIB_LOG_ERROR, "encode_string_array: %s. Failed to encode '%s'",
                         bd->shortName, stringValues->v[ival]);
        return err;
    }
    width = n > 1 ? modifiedWidth : 0;

    grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + 6);
    grib_encode_unsigned_longb(buff->data, width / 8, pos, 6);
    if (width) {
        grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + width * n);
        for (j = 0; j < n; j++) {
            k   = iss_list_->v[j];
            err = grib_encode_string(buff->data, pos, width / 8, stringValues->v[k]);
            if (err) {
                grib_context_log(c, GRIB_LOG_ERROR, "encode_string_array: %s. Failed to encode '%s'",
                                 bd->shortName, stringValues->v[k]);
                return err;
            }
        }
    }
    return err;
}

void set_missing_long_to_double(grib_darray* dvalues)
{
    size_t i, n = grib_darray_used_size(dvalues);
    for (i = 0; i < n; i++) {
        if (dvalues->v[i] == GRIB_MISSING_LONG)
            dvalues->v[i] = GRIB_MISSING_DOUBLE;
    }
}

/* ECC-750: The 'factor' argument is 10^-scale */
static int descriptor_get_min_max(bufr_descriptor* bd, long width, long reference, double factor,
                                  double* minAllowed, double* maxAllowed)
{
    /* Maximum value is allowed to be the largest number (all bits 1) which means it's MISSING */
    const size_t max1 = (1ULL << width) - 1; /* Highest value for number with 'width' bits */

    if (width <= 0)
        return GRIB_MISSING_BUFR_ENTRY; /* ECC-1395 */

    DEBUG_ASSERT(width > 0 && width < 64);

    *maxAllowed = (max1 + reference) * factor;
    *minAllowed = reference * factor;
    return GRIB_SUCCESS;
}

int grib_accessor_bufr_data_array_t::encode_double_array(grib_context* c, grib_buffer* buff, long* pos, bufr_descriptor* bd,
                                                         grib_darray* dvalues)
{
    int err = 0;
    int j, i;
    size_t lval;
    long localReference = 0, localWidth = 0, modifiedWidth, modifiedReference;
    long reference, allone;
    double localRange, modifiedFactor, inverseFactor;
    size_t ii, index_of_min, index_of_max;
    int nvals  = 0;
    double min = 0, max = 0, maxAllowed, minAllowed;
    double* v            = NULL;
    double* values       = NULL;
    bool thereIsAMissing = false;
    bool is_constant     = true;
    double val0;
    /* ECC-379, ECC-830 */
    const int dont_fail_if_out_of_range = set_to_missing_if_out_of_range_;

    if (iss_list_ == NULL) {
        grib_context_log(c, GRIB_LOG_ERROR, "encode_double_array: iss_list_ ==NULL");
        return GRIB_INTERNAL_ERROR;
    }

    modifiedReference = bd->reference;
    modifiedFactor    = bd->factor;
    inverseFactor     = codes_power<double>(bd->scale, 10);
    modifiedWidth     = bd->width;

    err = descriptor_get_min_max(bd, modifiedWidth, modifiedReference, modifiedFactor, &minAllowed, &maxAllowed);
    if (err) return err;

    nvals = grib_iarray_used_size(iss_list_);
    if (nvals <= 0)
        return GRIB_NO_VALUES;

    if (!dvalues)
        return GRIB_ENCODING_ERROR;

    set_missing_long_to_double(dvalues);

    v = dvalues->v;

    /* is constant */
    if (grib_darray_is_constant(dvalues, modifiedFactor * .5)) {
        localWidth = 0;
        grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + modifiedWidth);
        if (*v == GRIB_MISSING_DOUBLE) {
            grib_set_bits_on(buff->data, pos, modifiedWidth);
        }
        else {
            if (*v > maxAllowed || *v < minAllowed) {
                if (dont_fail_if_out_of_range) {
                    fprintf(stderr,
                            "ECCODES WARNING :  encode_double_array: %s (%06ld). Value (%g) out of range (minAllowed=%g, maxAllowed=%g)."
                            " Setting it to missing value\n",
                            bd->shortName, bd->code, *v, minAllowed, maxAllowed);
                    grib_set_bits_on(buff->data, pos, modifiedWidth);
                }
                else {
                    grib_context_log(c, GRIB_LOG_ERROR, "encode_double_array: %s (%06ld). Value (%g) out of range (minAllowed=%g, maxAllowed=%g).",
                                     bd->shortName, bd->code, *v, minAllowed, maxAllowed);
                    return GRIB_OUT_OF_RANGE; /* ECC-611 */
                }
            }
            else {
                lval = round(*v * inverseFactor) - modifiedReference;
                grib_encode_size_tb(buff->data, lval, pos, modifiedWidth);
            }
        }
        grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + 6);
        grib_encode_unsigned_longb(buff->data, localWidth, pos, 6);
        return GRIB_SUCCESS;
    }

    if (nvals > grib_darray_used_size(dvalues))
        return GRIB_ARRAY_TOO_SMALL;
    values      = (double*)grib_context_malloc_clear(c, sizeof(double) * nvals);
    val0        = dvalues->v[iss_list_->v[0]];
    is_constant = true;
    for (i = 0; i < nvals; i++) {
        values[i] = dvalues->v[iss_list_->v[i]];
        if (val0 != values[i])
            is_constant = false;
    }
    v = values;

    /* encoding a range with constant values*/
    if (is_constant) {
        localWidth = 0;
        grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + modifiedWidth);
        if (*v == GRIB_MISSING_DOUBLE) {
            grib_set_bits_on(buff->data, pos, modifiedWidth);
        }
        else {
            lval = round(*v * inverseFactor) - modifiedReference;
            grib_encode_size_tb(buff->data, lval, pos, modifiedWidth);
        }
        grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + 6);
        grib_encode_unsigned_longb(buff->data, localWidth, pos, 6);
        grib_context_free(c, values);
        return err;
    }

    ii = 0;
    while (ii < nvals && *v == GRIB_MISSING_DOUBLE) {
        thereIsAMissing = true;
        v++;
        ii++;
    }
    /* ECC-379: First determine if we need to change any out-of-range value */
    if (dont_fail_if_out_of_range) {
        while (ii < nvals) {
            /* Turn out-of-range values into 'missing' */
            if (*v != GRIB_MISSING_DOUBLE && (*v < minAllowed || *v > maxAllowed)) {
                fprintf(stderr,
                        "ECCODES WARNING :  encode_double_array: %s (%06ld). Value at index %ld (%g) out of range (minAllowed=%g, maxAllowed=%g)."
                        " Setting it to missing value\n",
                        bd->shortName, bd->code, (long)ii, *v, minAllowed, maxAllowed);
                *v = GRIB_MISSING_DOUBLE;
            }
            ii++;
            v++;
        }
    }
    /* Determine min and max values. */
    /* Note: value[0] could be missing so cannot set min/max to this.
     * Find first value which is non-missing as a starting point */
    for (i = 0; i < nvals; i++) {
        if (values[i] != GRIB_MISSING_DOUBLE) {
            min = max = values[i];
            break;
        }
    }
    ii           = 0;
    index_of_min = index_of_max = 0;
    v                           = values;
    while (ii < nvals) {
        if (*v < min && *v != GRIB_MISSING_DOUBLE) {
            min          = *v;
            index_of_min = ii;
        }
        if (*v > max && *v != GRIB_MISSING_DOUBLE) {
            max          = *v;
            index_of_max = ii;
        }
        if (*v == GRIB_MISSING_DOUBLE)
            thereIsAMissing = true;
        ii++;
        v++;
    }
    if (max > maxAllowed && max != GRIB_MISSING_DOUBLE) {
        grib_context_log(c, GRIB_LOG_ERROR, "encode_double_array: %s (%06ld). Maximum value (value[%lu]=%g) out of range (maxAllowed=%g).",
                         bd->shortName, bd->code, index_of_max, max, maxAllowed);
        return GRIB_OUT_OF_RANGE;
    }
    if (min < minAllowed && min != GRIB_MISSING_DOUBLE) {
        grib_context_log(c, GRIB_LOG_ERROR, "encode_double_array: %s (%06ld). Minimum value (value[%lu]=%g) out of range (minAllowed=%g).",
                         bd->shortName, bd->code, index_of_min, min, minAllowed);
        return GRIB_OUT_OF_RANGE;
    }

    reference      = round(min * inverseFactor);
    localReference = reference - modifiedReference;
    if (max != min) {
        localRange = (max - min) * inverseFactor + 1;
        localWidth = ceil(log(localRange) / log(2.0));
        lval       = round(max * inverseFactor) - reference;
        allone     = codes_power<double>(localWidth, 2) - 1;
        while (allone <= lval) {
            localWidth++;
            allone = codes_power<double>(localWidth, 2) - 1;
        }
        if (localWidth == 1)
            localWidth++;
    }
    else {
        if (thereIsAMissing)
            localWidth = 1;
        else
            localWidth = 0;
    }

    grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + modifiedWidth);
    if (localWidth) {
        grib_encode_unsigned_longb(buff->data, localReference, pos, modifiedWidth);
    }
    else {
        if (min == GRIB_MISSING_DOUBLE) {
            grib_set_bits_on(buff->data, pos, modifiedWidth);
        }
        else {
            lval = localReference - modifiedReference;
            grib_encode_size_tb(buff->data, lval, pos, modifiedWidth);
        }
    }
    grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + 6);
    grib_encode_unsigned_longb(buff->data, localWidth, pos, 6);

    if (localWidth) {
        grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + nvals * localWidth);
        for (j = 0; j < nvals; j++) {
            if (values[j] == GRIB_MISSING_DOUBLE) {
                grib_set_bits_on(buff->data, pos, localWidth);
            }
            else {
                lval = round(values[j] * inverseFactor) - reference;
                grib_encode_size_tb(buff->data, lval, pos, localWidth);
            }
        }
    }

    grib_context_free(c, values);

    return err;
}

int grib_accessor_bufr_data_array_t::encode_double_value(grib_context* c, grib_buffer* buff, long* pos, bufr_descriptor* bd,
                                                         double value)
{
    size_t lval;
    double maxAllowed, minAllowed;
    int err = 0;
    int modifiedWidth, modifiedReference;
    double modifiedFactor;
    /* ECC-379, ECC-830 */
    const int dont_fail_if_out_of_range = set_to_missing_if_out_of_range_;

    modifiedReference = bd->reference;
    modifiedFactor    = bd->factor;
    modifiedWidth     = bd->width;

    err = descriptor_get_min_max(bd, modifiedWidth, modifiedReference, modifiedFactor, &minAllowed, &maxAllowed);
    if (err) return err;

    grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + modifiedWidth);
    if (value == GRIB_MISSING_DOUBLE) {
        grib_set_bits_on(buff->data, pos, modifiedWidth);
    }
    else if (value > maxAllowed || value < minAllowed) {
        if (dont_fail_if_out_of_range) {
            fprintf(stderr,
                    "ECCODES WARNING :  encode_double_value: %s (%06ld). Value (%g) out of range (minAllowed=%g, maxAllowed=%g)."
                    " Setting it to missing value\n",
                    bd->shortName, bd->code, value, minAllowed, maxAllowed);
            /* Ignore the bad value and instead use 'missing' */
            grib_set_bits_on(buff->data, pos, modifiedWidth);
        }
        else {
            grib_context_log(c, GRIB_LOG_ERROR, "encode_double_value: %s (%06ld). Value (%g) out of range (minAllowed=%g, maxAllowed=%g).",
                             bd->shortName, bd->code, value, minAllowed, maxAllowed);
            return GRIB_OUT_OF_RANGE;
        }
    }
    else {
        lval = round(value / modifiedFactor) - modifiedReference;
        if (c->debug)
            grib_context_log(c, GRIB_LOG_DEBUG, "encode_double_value %s: value=%.15f lval=%lu\n", bd->shortName, value, lval);
        grib_encode_size_tb(buff->data, lval, pos, modifiedWidth);
    }

    return GRIB_SUCCESS;
}

static int encode_string_value(grib_context* c, grib_buffer* buff, long* pos, bufr_descriptor* bd, char* sval)
{
    int err = 0;
    int len;

    len = bd->width / 8;
    grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + bd->width);
    err = grib_encode_string(buff->data, pos, len, sval);
    if (err) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: %s. Failed to encode '%s'", __func__, bd->shortName, sval);
    }

    return err;
}

char* grib_accessor_bufr_data_array_t::decode_string_value(grib_context* c, unsigned char* data, long* pos, bufr_descriptor* bd,
                                                           int* err)
{
    char* sval = 0;
    int len;

    *err = 0;

    len = bd->width / 8;

    CHECK_END_DATA_RETURN(c, bd, this, bd->width, NULL);
    sval = (char*)grib_context_malloc_clear(c, len + 1);
    if (*err) {
        *err = 0;
        return sval;
    }
    grib_decode_string(data, pos, len, sval);

    /* clean_string(sval,len); */

    return sval;
}

double grib_accessor_bufr_data_array_t::decode_double_value(grib_context* c, unsigned char* data, long* pos,
                                                            bufr_descriptor* bd, int canBeMissing,
                                                            int* err)
{
    size_t lval;
    int modifiedWidth, modifiedReference;
    double modifiedFactor;
    double dval = 0;

    *err = 0;

    modifiedReference = bd->reference;
    modifiedFactor    = bd->factor;
    modifiedWidth     = bd->width;

    CHECK_END_DATA_RETURN(c, bd, this, modifiedWidth, 0);
    if (*err) {
        *err = 0;
        return GRIB_MISSING_DOUBLE;
    }

    lval = grib_decode_size_t(data, pos, modifiedWidth);
    if (canBeMissing && grib_is_all_bits_one(lval, modifiedWidth)) {
        dval = GRIB_MISSING_DOUBLE;
    }
    else {
        dval = ((int64_t)lval + modifiedReference) * modifiedFactor;
    }
    return dval;
}


int decode_element(grib_context* c, grib_accessor_bufr_data_array_t* self, int subsetIndex,
                   grib_buffer* b, unsigned char* data, long* pos, int i, bufr_descriptor* descriptor, long elementIndex,
                   grib_darray* dval, grib_sarray* sval)
{
    grib_darray* dar    = 0;
    grib_sarray* sar    = 0;
    int index           = 0, ii, stringValuesLen;
    char* csval         = 0;
    double cdval        = 0, x;
    int err             = 0;
    bufr_descriptor* bd = descriptor == NULL ? self->expanded_->v[i] : descriptor;
    /* ECCODES_ASSERT( b->data == data); */

    if (self->change_ref_value_operand_ > 0 && self->change_ref_value_operand_ != 255) {
        /* Operator 203YYY: Change Reference Values: Definition phase */
        const int number_of_bits = self->change_ref_value_operand_;
        long new_ref_val         = grib_decode_signed_longb(data, pos, number_of_bits);
        grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: -**- \tcode=203YYY width=%d pos=%ld -> %ld",
                         number_of_bits, (long)*pos, (long)(*pos - self->offset_ * 8));
        grib_context_log(c, GRIB_LOG_DEBUG, "Operator 203YYY: Store for code %6.6ld => new ref val %ld", bd->code, new_ref_val);
        self->tableB_override_store_ref_val(c, bd->code, new_ref_val);
        bd->nokey = 1;
        err       = check_end_data(c, NULL, self, number_of_bits); /*advance bitsToEnd*/
        return err;
    }
    grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: -%d- \tcode=%6.6ld width=%ld scale=%ld ref=%ld type=%d (pos=%ld -> %ld)",
                     i, bd->code, bd->width, bd->scale, bd->reference, bd->type,
                     (long)*pos, (long)(*pos - self->offset_ * 8));
    if (bd->type == BUFR_DESCRIPTOR_TYPE_STRING) {
        /* string */
        if (self->compressedData_) {
            err   = self->decode_string_array(c, data, pos, bd);
            index = grib_vsarray_used_size(self->stringValues_);
            dar   = grib_darray_new(self->numberOfSubsets_, 10);
            index = self->numberOfSubsets_ * (index - 1);
            for (ii = 1; ii <= self->numberOfSubsets_; ii++) {
                x = (index + ii) * 1000 + bd->width / 8;
                grib_darray_push(dar, x);
            }
            grib_vdarray_push(self->numericValues_, dar);
        }
        else {
            csval = self->decode_string_value(c, data, pos, bd, &err);
            grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: \t %s = %s", bd->shortName, csval);
            sar = grib_sarray_push(sar, csval);
            grib_vsarray_push(self->stringValues_, sar);
            stringValuesLen = grib_vsarray_used_size(self->stringValues_);
            index           = 0;
            for (ii = 0; ii < stringValuesLen; ii++) {
                index += grib_sarray_used_size(self->stringValues_->v[ii]);
            }
            cdval = index * 1000 + bd->width / 8;
            grib_darray_push(dval, cdval);
        }
    }
    else {
        /* numeric or codetable or flagtable */
        /* Operator 203YYY: Check if we have changed ref value for this element. If so modify bd->reference */
        if (self->change_ref_value_operand_ != 0 && self->tableB_override_get_ref_val(bd->code, &(bd->reference)) == GRIB_SUCCESS) {
            grib_context_log(c, GRIB_LOG_DEBUG, "Operator 203YYY: For code %6.6ld, changed ref val: %ld", bd->code, bd->reference);
        }

        if (bd->width > 64) {
            grib_context_log(c, GRIB_LOG_ERROR, "Descriptor %6.6ld has bit width %ld!", bd->code, bd->width);
            return GRIB_DECODING_ERROR;
        }
        if (self->compressedData_) {
            dar = self->decode_double_array(c, data, pos, bd, self->canBeMissing_[i], &err);
            grib_vdarray_push(self->numericValues_, dar);
        }
        else {
            /* Uncompressed */
            cdval = self->decode_double_value(c, data, pos, bd, self->canBeMissing_[i], &err);
            grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: \t %s = %g",
                             bd->shortName, cdval);
            grib_darray_push(dval, cdval);
        }
    }
    return err;
}


int decode_replication(grib_context* c, grib_accessor_bufr_data_array_t* self, int subsetIndex, grib_buffer* buff, unsigned char* data, long* pos, int i, long elementIndex, grib_darray* dval, long* numberOfRepetitions)
{
    int ret = 0;
    int* err;
    int localReference, width;
    bufr_descriptor** descriptors = 0;
    err                           = &ret;
    descriptors                   = self->expanded_->v;

    /* ECCODES_ASSERT(buff->data == data); */

    grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: -%d- \tcode=%6.6ld width=%ld ",
                     i, self->expanded_->v[i]->code, self->expanded_->v[i]->width);
    if (self->compressedData_) {
        grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: \tdelayed replication localReference width=%ld", descriptors[i]->width);
        CHECK_END_DATA_RETURN(c, NULL, self, descriptors[i]->width + 6, *err);
        if (*err) {
            *numberOfRepetitions = 0;
        }
        else {
            localReference = grib_decode_unsigned_long(data, pos, descriptors[i]->width) + descriptors[i]->reference;
            grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: \tdelayed replication localWidth width=6");
            width = grib_decode_unsigned_long(data, pos, 6);
            if (width) {
                grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: \tdelayed replication is NOT constant for compressed data!");
                /* delayed replication number is not constant. NOT IMPLEMENTED */
                return GRIB_NOT_IMPLEMENTED;
            }
            else {
                *numberOfRepetitions = localReference * descriptors[i]->factor;
                grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: \tdelayed replication value=%ld", *numberOfRepetitions);
            }
        }
    }
    else {
        CHECK_END_DATA_RETURN(c, NULL, self, descriptors[i]->width, *err);
        if (*err) {
            *numberOfRepetitions = 0;
        }
        else {
            *numberOfRepetitions = grib_decode_unsigned_long(data, pos, descriptors[i]->width) +
                                   descriptors[i]->reference * descriptors[i]->factor;
            grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data decoding: \tdelayed replication value=%ld", *numberOfRepetitions);
        }
    }
    if (self->compressedData_) {
        dval = grib_darray_new(1, 100);
        if (c->bufr_multi_element_constant_arrays) {
            long j;
            for (j = 0; j < self->numberOfSubsets_; j++) {
                grib_darray_push(dval, (double)(*numberOfRepetitions));
            }
        }
        else {
            grib_darray_push(dval, (double)(*numberOfRepetitions));
        }
        grib_vdarray_push(self->numericValues_, dval);
    }
    else {
        grib_darray_push(dval, (double)(*numberOfRepetitions));
    }
    return ret;
}


int grib_accessor_bufr_data_array_t::encode_new_bitmap(grib_context* c, grib_buffer* buff, long* pos, int idx)
{
    grib_darray* doubleValues = NULL;
    int err                   = 0;
    double cdval              = 0;
    if (nInputBitmap_ > 0) {
        if (nInputBitmap_ < iInputBitmap_)
            return GRIB_ARRAY_TOO_SMALL;
        cdval = inputBitmap_[iInputBitmap_++];
    }
    if (compressedData_) {
        doubleValues = grib_darray_new(1, 1);
        grib_darray_push(doubleValues, cdval);
        err = encode_double_array(c, buff, pos, expanded_->v[idx], doubleValues);
        grib_darray_delete(doubleValues);
    }
    else {
        err = encode_double_value(c, buff, pos, expanded_->v[idx], cdval);
    }
    return err;
}

/* Operator 203YYY: Change Reference Values: Encoding definition phase */
int grib_accessor_bufr_data_array_t::encode_overridden_reference_value(grib_context* c,
                                                                       grib_buffer* buff, long* pos, bufr_descriptor* bd)
{
    int err         = 0;
    long currRefVal = -1;
    long numBits    = change_ref_value_operand_;
    /* We must be encoding between 203YYY and 203255 */
    ECCODES_ASSERT(change_ref_value_operand_ > 0 && change_ref_value_operand_ != 255);
    if (refValListSize_ == 0) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "encode_new_element: Overridden Reference Values array is empty! "
                         "(Hint: set the key '%s')",
                         OVERRIDDEN_REFERENCE_VALUES_KEY);
        grib_context_log(c, GRIB_LOG_ERROR,
                         "The number of overridden reference values must be equal to "
                         "number of descriptors between operator 203YYY and 203255");
        return GRIB_ENCODING_ERROR;
    }
    if (refValIndex_ >= refValListSize_) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "encode_new_element: Overridden Reference Values: index=%ld, size=%ld. "
                         "\nThe number of overridden reference values must be equal to "
                         "number of descriptors between operator 203YYY and 203255",
                         refValIndex_, refValListSize_);
        return GRIB_ENCODING_ERROR;
    }
    currRefVal = refValList_[refValIndex_];
    grib_context_log(c, GRIB_LOG_DEBUG, "encode_new_element: Operator 203YYY: writing ref val %ld (refValIndex_ =%ld)",
                     currRefVal, refValIndex_);
    grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + numBits);
    err = grib_encode_signed_longb(buff->data, currRefVal, pos, numBits);
    if (err) {
        grib_context_log(c, GRIB_LOG_ERROR, "Encoding overridden reference value %ld for %s (code=%6.6ld)",
                         currRefVal, bd->shortName, bd->code);
    }
    refValIndex_++;
    return err;
}

int encode_new_element(grib_context* c, grib_accessor_bufr_data_array_t* self, int subsetIndex,
                       grib_buffer* buff, unsigned char* data, long* pos, int i, bufr_descriptor* descriptor,
                       long elementIndex, grib_darray* dval, grib_sarray* sval)
{
    int ii;
    char* csval               = 0;
    unsigned char missingChar = 0xFF;
    double cdval              = GRIB_MISSING_DOUBLE;
    int err                   = 0;
    size_t slen;
    bufr_descriptor* bd = descriptor == NULL ? self->expanded_->v[i] : descriptor;

    grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data encoding: \tcode=%6.6ld width=%ld pos=%ld ulength=%ld ulength_bits=%ld",
                     bd->code, bd->width, (long)*pos, buff->ulength, buff->ulength_bits);

    if (self->change_ref_value_operand_ > 0 && self->change_ref_value_operand_ != 255) {
        /* Operator 203YYY: Change Reference Values: Encoding definition phase */
        err = self->encode_overridden_reference_value(c, buff, pos, bd);
        return err;
    }

    if (bd->type == BUFR_DESCRIPTOR_TYPE_STRING) {
        /* string */
        slen  = bd->width / 8;
        csval = (char*)grib_context_malloc_clear(c, slen + 1);
        for (ii = 0; ii < slen; ii++)
            csval[ii] = missingChar;
        grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data encoding: \t %s = %s",
                         bd->shortName, csval);
        if (self->compressedData_) {
            grib_sarray* stringValues = grib_sarray_new(1, 1);
            grib_sarray_push(stringValues, csval);
            err = self->encode_string_array(c, buff, pos, bd, stringValues);
            grib_sarray_delete_content(stringValues);
            grib_sarray_delete(stringValues);
        }
        else {
            err = encode_string_value(c, buff, pos, bd, csval);
            grib_context_free(c, csval);
        }
    }
    else {
        /* numeric or codetable or flagtable */
        grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data encoding: \t %s = %g",
                         bd->shortName, cdval);
        if (bd->code == 31031)
            return self->encode_new_bitmap(c, buff, pos, i);
        if (self->compressedData_) {
            grib_darray* doubleValues = grib_darray_new(1, 1);
            grib_darray_push(doubleValues, cdval);
            err = self->encode_double_array(c, buff, pos, bd, doubleValues);
            grib_darray_delete(doubleValues);
        }
        else {
            err = self->encode_double_value(c, buff, pos, bd, cdval);
        }
    }
    return err;
}


int encode_new_replication(grib_context* c, grib_accessor_bufr_data_array_t* self, int subsetIndex,
                           grib_buffer* buff, unsigned char* data, long* pos, int i, long elementIndex, grib_darray* dval, long* numberOfRepetitions)
{
    int err                       = 0;
    unsigned long repetitions     = 1;
    bufr_descriptor** descriptors = self->expanded_->v;
    DEBUG_ASSERT(buff->data == data);

    switch (descriptors[i]->code) {
        case 31000:
            if (self->nInputShortReplications_ >= 0) {
                if (self->iInputShortReplications_ >= self->nInputShortReplications_) {
                    grib_context_log(c, GRIB_LOG_ERROR, "Array inputShortDelayedDescriptorReplicationFactor: dimension mismatch (nInputShortReplications=%d)",
                                     self->nInputShortReplications_);
                    return GRIB_ARRAY_TOO_SMALL;
                }
                repetitions = self->inputShortReplications_[self->iInputShortReplications_];
                self->iInputShortReplications_++;
            }
            break;
        case 31001:
            if (self->nInputReplications_ >= 0) {
                if (self->iInputReplications_ >= self->nInputReplications_) {
                    grib_context_log(c, GRIB_LOG_ERROR, "Array inputDelayedDescriptorReplicationFactor: dimension mismatch (nInputReplications=%d)",
                                     self->nInputReplications_);
                    return GRIB_ARRAY_TOO_SMALL;
                }
                repetitions = self->inputReplications_[self->iInputReplications_];
                self->iInputReplications_++;
            }
            break;
        case 31002:
            if (self->nInputExtendedReplications_ >= 0) {
                if (self->iInputExtendedReplications_ >= self->nInputExtendedReplications_) {
                    grib_context_log(c, GRIB_LOG_ERROR, "Array inputExtendedDelayedDescriptorReplicationFactor: dimension mismatch (nInputExtendedReplications=%d)",
                                     self->nInputExtendedReplications_);
                    return GRIB_ARRAY_TOO_SMALL;
                }
                repetitions = self->inputExtendedReplications_[self->iInputExtendedReplications_];
                self->iInputExtendedReplications_++;
            }
            break;
        default:
            grib_context_log(c, GRIB_LOG_ERROR, "Unsupported descriptor code %ld\n", descriptors[i]->code);
            return GRIB_INTERNAL_ERROR;
    }

    grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data encoding replication: \twidth=%ld pos=%ld ulength=%ld ulength_bits=%ld",
                     (long)descriptors[i]->width, (long)*pos, (long)buff->ulength, (long)buff->ulength_bits);
    grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + descriptors[i]->width);
    data = buff->data; /* ECC-1347 */
    grib_encode_unsigned_longb(data, repetitions, pos, descriptors[i]->width);

    *numberOfRepetitions = repetitions;

    if (self->compressedData_) {
        grib_buffer_set_ulength_bits(c, buff, buff->ulength_bits + 6);
        grib_encode_unsigned_longb(buff->data, 0, pos, 6);
    }

    return err;
}


int encode_element(grib_context* c, grib_accessor_bufr_data_array_t* self, int subsetIndex,
                   grib_buffer* buff, unsigned char* data, long* pos, int i, bufr_descriptor* descriptor,
                   long elementIndex, grib_darray* dval, grib_sarray* sval)
{
    int idx, j;
    int err             = 0;
    bufr_descriptor* bd = descriptor == NULL ? self->expanded_->v[i] : descriptor;
    /* ECCODES_ASSERT( buff->data == data); */

    grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data encoding: -%d- \tcode=%6.6ld width=%ld pos=%ld ulength=%ld ulength_bits=%ld",
                     i, bd->code, bd->width, (long)*pos, buff->ulength, buff->ulength_bits);

    if (self->change_ref_value_operand_ > 0 && self->change_ref_value_operand_ != 255) {
        /* Operator 203YYY: Change Reference Values: Encoding definition phase */
        err = self->encode_overridden_reference_value(c, buff, pos, bd);
        return err;
    }

    if (bd->type == BUFR_DESCRIPTOR_TYPE_STRING) {
        /* string */
        /* grib_context_log(c, GRIB_LOG_DEBUG,"BUFR data encoding: \t %s = %s",
                 bd->shortName,csval); */
        if (self->compressedData_) {
            idx = ((int)self->numericValues_->v[elementIndex]->v[0] / 1000 - 1) / self->numberOfSubsets_;
            if (idx >= self->stringValues_->size) { // ECC-2024: BUFR: Repeated subset extraction segfaults
                grib_context_log(c, GRIB_LOG_ERROR, "encode_element '%s': Invalid index %d", bd->shortName, idx);
                return GRIB_INTERNAL_ERROR;
            }
            err = self->encode_string_array(c, buff, pos, bd, self->stringValues_->v[idx]);
        }
        else {
            if (self->numericValues_->v[subsetIndex] == NULL) {
                grib_context_log(c, GRIB_LOG_ERROR, "Invalid subset index %d (number of subsets=%ld)", subsetIndex, self->numberOfSubsets_);
                return GRIB_INVALID_ARGUMENT;
            }
            idx = (int)self->numericValues_->v[subsetIndex]->v[elementIndex] / 1000 - 1;
            if (idx < 0 || idx >= self->stringValues_->n) {
                grib_context_log(c, GRIB_LOG_ERROR, "encode_element '%s': Invalid index %d", bd->shortName, idx);
                return GRIB_INVALID_ARGUMENT;
            }
            err = encode_string_value(c, buff, pos, bd, self->stringValues_->v[idx]->v[0]);
        }
    }
    else {
        /* numeric or codetable or flagtable */
        if (self->compressedData_) {
            err = self->encode_double_array(c, buff, pos, bd, self->numericValues_->v[elementIndex]);
            if (err) {
                grib_darray* varr = self->numericValues_->v[elementIndex];
                grib_context_log(c, GRIB_LOG_ERROR, "Encoding key '%s' ( code=%6.6ld width=%ld scale=%ld reference=%ld )",
                                 bd->shortName, bd->code, bd->width,
                                 bd->scale, bd->reference);
                if (varr) {
                    for (j = 0; j < grib_darray_used_size(varr); j++)
                        grib_context_log(c, GRIB_LOG_ERROR, "value[%d]\t= %g", j, varr->v[j]);
                }
                else {
                    grib_context_log(c, GRIB_LOG_ERROR, "Empty array: Check the order of keys being set!");
                }
            }
        }
        else {
            if (self->numericValues_->v[subsetIndex] == NULL) {
                grib_context_log(c, GRIB_LOG_ERROR, "Invalid subset index %d (number of subsets=%ld)", subsetIndex, self->numberOfSubsets_);
                return GRIB_INVALID_ARGUMENT;
            }
            err = self->encode_double_value(c, buff, pos, bd, self->numericValues_->v[subsetIndex]->v[elementIndex]);
            if (err) {
                grib_context_log(c, GRIB_LOG_ERROR, "Cannot encode %s=%g (subset=%d)", /*subsetIndex starts from 0*/
                                 bd->shortName, self->numericValues_->v[subsetIndex]->v[elementIndex], subsetIndex + 1);
            }
        }
    }
    return err;
}


int encode_replication(grib_context* c, grib_accessor_bufr_data_array_t* self, int subsetIndex,
                       grib_buffer* buff, unsigned char* data, long* pos, int i, long elementIndex,
                       grib_darray* dval, long* numberOfRepetitions)
{
    /* ECCODES_ASSERT( buff->data == data); */
    if (self->compressedData_) {
        DEBUG_ASSERT(grib_darray_used_size(self->numericValues_->v[elementIndex]) == 1);
        *numberOfRepetitions = self->numericValues_->v[elementIndex]->v[0];
    }
    else {
        *numberOfRepetitions = self->numericValues_->v[subsetIndex]->v[elementIndex];
    }

    return encode_element(c, self, subsetIndex, buff, data, pos, i, 0, elementIndex, dval, 0);
}


int grib_accessor_bufr_data_array_t::build_bitmap(unsigned char* data, long* pos,
                                                  int iel, grib_iarray* elementsDescriptorsIndex, int iBitmapOperator)
{
    int bitmapSize = 0, iDelayedReplication = 0;
    int i, localReference, width, bitmapEndElementsDescriptorsIndex;
    long ppos, n;
    bufr_descriptor** descriptors = expanded_->v;
    const long* edi               = elementsDescriptorsIndex->v;
    /* int iel=grib_iarray_used_size(elementsDescriptorsIndex)-1; */
    int err = 0;

    switch (descriptors[iBitmapOperator]->code) {
        case 222000:
        case 223000:
        case 236000:
            cancel_bitmap();
            if (iel < 0) {
                return GRIB_ENCODING_ERROR;
            }
            while (descriptors[edi[iel]]->code >= 100000 || iel == 0) {
                iel--;
                if (iel < 0) {
                    return GRIB_ENCODING_ERROR;
                }
            }
            bitmapEndElementsDescriptorsIndex = iel;
            /*looking for another bitmap and pointing before it.
          This behaviour is not documented in the Manual on codes it is copied from BUFRDC
          ECC-243
         */
            while (iel > 0) {
                while (descriptors[edi[iel]]->code != 236000 && descriptors[edi[iel]]->code != 222000 && descriptors[edi[iel]]->code != 223000 && iel != 0)
                    iel--;
                if (iel != 0) {
                    while (descriptors[edi[iel]]->code >= 100000 && iel != 0)
                        iel--;
                    bitmapEndElementsDescriptorsIndex = iel;
                }
            }

            i = iBitmapOperator + 1;
            if (descriptors[i]->code == 101000) {
                iDelayedReplication = iBitmapOperator + 2;
                ECCODES_ASSERT(descriptors[iDelayedReplication]->code == 31001 ||
                       descriptors[iDelayedReplication]->code == 31002);
                i = iDelayedReplication;
                if (compressedData_) {
                    ppos = *pos;
                    if (err)
                        return err;
                    localReference = grib_decode_unsigned_long(data, pos, descriptors[i]->width) + descriptors[i]->reference;
                    width          = grib_decode_unsigned_long(data, pos, 6);
                    *pos           = ppos;
                    if (width) {
                        /* delayed replication number is not constant. NOT IMPLEMENTED */
                        grib_context_log(context_, GRIB_LOG_ERROR, "Delayed replication number is not constant");
                        return GRIB_NOT_IMPLEMENTED;
                    }
                    else {
                        bitmapSize = localReference * descriptors[i]->factor;
                    }
                }
                else {
                    ppos = *pos;
                    if (err)
                        return err;
                    bitmapSize = grib_decode_unsigned_long(data, pos, descriptors[i]->width) +
                                 descriptors[i]->reference * descriptors[i]->factor;
                    *pos = ppos;
                }
            }
            else if (descriptors[i]->code == 31031) {
                bitmapSize = 0;
                while (descriptors[i]->code == 31031) {
                    bitmapSize++;
                    i++;
                }
            }
            iel = bitmapEndElementsDescriptorsIndex;
            n   = bitmapSize - 1;
            while (n > 0 && iel >= 0) {
                if (descriptors[edi[iel]]->code < 100000)
                    n--;
                iel--;
            }
            bitmapStartElementsDescriptorsIndex_ = iel;
            restart_bitmap();
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR, "build_bitmap: unsupported operator %ld\n",
                             descriptors[iBitmapOperator]->code);
            return GRIB_INTERNAL_ERROR;
    }
    return GRIB_SUCCESS;
}

int grib_accessor_bufr_data_array_t::consume_bitmap(int iBitmapOperator)
{
    int bitmapSize = 0, iDelayedReplication;
    int i;
    bufr_descriptor** descriptors = expanded_->v;

    i = iBitmapOperator + 1;
    if (descriptors[i]->code == 101000) {
        iDelayedReplication = iBitmapOperator + 2;
        switch (descriptors[iDelayedReplication]->code) {
            case 31001:
                bitmapSize = inputReplications_[iInputReplications_];
                break;
            case 31002:
                bitmapSize = inputExtendedReplications_[iInputExtendedReplications_];
                break;
            default:
                ECCODES_ASSERT(0);
        }
    }
    else if (descriptors[i]->code == 31031) {
        bitmapSize = 0;
        while (descriptors[i]->code == 31031) {
            bitmapSize++;
            i++;
        }
    }
    bitmapCurrent_ += bitmapSize;
    return GRIB_SUCCESS;
}

int grib_accessor_bufr_data_array_t::build_bitmap_new_data(unsigned char* data, long* pos,
                                                           int iel, grib_iarray* elementsDescriptorsIndex, int iBitmapOperator)
{
    int bitmapSize = 0, iDelayedReplication = 0;
    int i, bitmapEndElementsDescriptorsIndex;
    long n;
    bufr_descriptor** descriptors = expanded_->v;
    const long* edi               = elementsDescriptorsIndex->v;

    switch (descriptors[iBitmapOperator]->code) {
        case 222000:
        case 223000:
        case 236000:
            if (iel < 0) {
                return GRIB_ENCODING_ERROR;
            }
            while (descriptors[edi[iel]]->code >= 100000) {
                iel--;
                if (iel < 0) {
                    return GRIB_ENCODING_ERROR;
                }
            }
            bitmapEndElementsDescriptorsIndex = iel;
            /*looking for another bitmap and pointing before it.
          This behaviour is not documented in the Manual on codes it is copied from BUFRDC
          ECC-243
         */
            while (iel > 0) {
                while (descriptors[edi[iel]]->code != 236000 && descriptors[edi[iel]]->code != 222000 && descriptors[edi[iel]]->code != 223000 && iel != 0)
                    iel--;
                if (iel != 0) {
                    while (descriptors[edi[iel]]->code >= 100000 && iel != 0)
                        iel--;
                    bitmapEndElementsDescriptorsIndex = iel;
                }
            }

            i = iBitmapOperator + 1;
            if (descriptors[i]->code == 101000) {
                iDelayedReplication = iBitmapOperator + 2;
                switch (descriptors[iDelayedReplication]->code) {
                    case 31001:
                        if (!inputReplications_) {
                            grib_context_log(context_, GRIB_LOG_ERROR, "build_bitmap_new_data: No inputReplications");
                            return GRIB_ENCODING_ERROR;
                        }
                        bitmapSize = inputReplications_[iInputReplications_];
                        break;
                    case 31002:
                        if (!inputExtendedReplications_) {
                            grib_context_log(context_, GRIB_LOG_ERROR, "build_bitmap_new_data: No inputExtendedReplications");
                            return GRIB_ENCODING_ERROR;
                        }
                        bitmapSize = inputExtendedReplications_[iInputExtendedReplications_];
                        break;
                    default:
                        ECCODES_ASSERT(0);
                }
            }
            else if (descriptors[i]->code == 31031) {
                bitmapSize = 0;
                while (descriptors[i]->code == 31031) {
                    bitmapSize++;
                    i++;
                }
            }
            iel = bitmapEndElementsDescriptorsIndex;
            n   = bitmapSize - 1;
            while (n > 0 && iel >= 0) {
                if (descriptors[edi[iel]]->code < 100000)
                    n--;
                iel--;
            }
            bitmapStartElementsDescriptorsIndex_   = iel;
            bitmapCurrentElementsDescriptorsIndex_ = iel - 1;
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR, "build_bitmap_new_data: unsupported operator %ld\n",
                             descriptors[iBitmapOperator]->code);
            return GRIB_INTERNAL_ERROR;
    }
    return GRIB_SUCCESS;
}

/* ECC-1304: Will return an index if successful. In case of an error, a negative number is returned e.g. GRIB_WRONG_BITMAP_SIZE */
int grib_accessor_bufr_data_array_t::get_next_bitmap_descriptor_index_new_bitmap(grib_iarray* elementsDescriptorsIndex, int compressedData)
{
    int i;
    bufr_descriptor** descriptors = expanded_->v;

    bitmapCurrent_++;
    bitmapCurrentElementsDescriptorsIndex_++;
    i = bitmapCurrent_;

    if (compressedData_) {
        DEBUG_ASSERT(i < nInputBitmap_);
        if (i >= nInputBitmap_)
            return GRIB_WRONG_BITMAP_SIZE;
        while (inputBitmap_[i] == 1) {
            bitmapCurrent_++;
            bitmapCurrentElementsDescriptorsIndex_++;
            while (descriptors[elementsDescriptorsIndex->v[bitmapCurrentElementsDescriptorsIndex_]]->code > 100000)
                bitmapCurrentElementsDescriptorsIndex_++;
            i++;
        }
    }
    else {
        if (i >= nInputBitmap_)
            return GRIB_WRONG_BITMAP_SIZE;
        while (inputBitmap_[i] == 1) {
            bitmapCurrent_++;
            bitmapCurrentElementsDescriptorsIndex_++;
            while (descriptors[elementsDescriptorsIndex->v[bitmapCurrentElementsDescriptorsIndex_]]->code > 100000)
                bitmapCurrentElementsDescriptorsIndex_++;
            i++;
        }
    }
    while (descriptors[elementsDescriptorsIndex->v[bitmapCurrentElementsDescriptorsIndex_]]->code > 100000)
        bitmapCurrentElementsDescriptorsIndex_++;
    return elementsDescriptorsIndex->v[bitmapCurrentElementsDescriptorsIndex_];
}

/* ECC-1304: Will return an index if successful. In case of an error, a negative number is returned e.g. GRIB_WRONG_BITMAP_SIZE */
int grib_accessor_bufr_data_array_t::get_next_bitmap_descriptor_index(grib_iarray* elementsDescriptorsIndex, grib_darray* numericValues)
{
    int i;
    bufr_descriptor** descriptors = expanded_->v;

    if (compressedData_) {
        if (numericValues_->n == 0)
            return get_next_bitmap_descriptor_index_new_bitmap(elementsDescriptorsIndex, 1);

        bitmapCurrent_++;
        bitmapCurrentElementsDescriptorsIndex_++;
        i = bitmapCurrent_ + bitmapStart_;
        DEBUG_ASSERT(i < numericValues_->n);
        while (numericValues_->v[i]->v[0] == 1) {
            bitmapCurrent_++;
            bitmapCurrentElementsDescriptorsIndex_++;
            while (descriptors[elementsDescriptorsIndex->v[bitmapCurrentElementsDescriptorsIndex_]]->code > 100000)
                bitmapCurrentElementsDescriptorsIndex_++;
            i++;
        }
    }
    else {
        if (numericValues->n == 0)
            return get_next_bitmap_descriptor_index_new_bitmap(elementsDescriptorsIndex, 0);

        bitmapCurrent_++;
        bitmapCurrentElementsDescriptorsIndex_++;
        i = bitmapCurrent_ + bitmapStart_;
        DEBUG_ASSERT(i < numericValues->n);
        while (numericValues->v[i] == 1) {
            bitmapCurrent_++;
            bitmapCurrentElementsDescriptorsIndex_++;
            while (descriptors[elementsDescriptorsIndex->v[bitmapCurrentElementsDescriptorsIndex_]]->code > 100000)
                bitmapCurrentElementsDescriptorsIndex_++;
            i++;
        }
    }
    while (descriptors[elementsDescriptorsIndex->v[bitmapCurrentElementsDescriptorsIndex_]]->code > 100000)
        bitmapCurrentElementsDescriptorsIndex_++;
    return elementsDescriptorsIndex->v[bitmapCurrentElementsDescriptorsIndex_];
}

void grib_accessor_bufr_data_array_t::push_zero_element(grib_darray* dval)
{
    grib_darray* d = 0;
    if (compressedData_) {
        d = grib_darray_new(1, 100);
        grib_darray_push(d, 0);
        grib_vdarray_push(numericValues_, d);
    }
    else {
        grib_darray_push(dval, 0);
    }
}

grib_accessor* grib_accessor_bufr_data_array_t::create_attribute_variable(const char* name, grib_section* section, int type, char* sval, double dval, long lval, unsigned long flags)
{
    grib_action creator;
    size_t len;
    creator.op_         = (char*)"variable";
    creator.name_space_ = (char*)"";
    creator.flags_      = GRIB_ACCESSOR_FLAG_READ_ONLY | GRIB_ACCESSOR_FLAG_BUFR_DATA | flags;
    creator.set_        = 0;
    creator.name_       = (char*)name;
    grib_accessor* a    = grib_accessor_factory(section, &creator, 0, NULL);
    a->parent_          = NULL;
    a->h_               = section->h;
    grib_accessor_variable_t* va = dynamic_cast<grib_accessor_variable_t*>(a);
    va->accessor_variable_set_type(type);
    len = 1;
    switch (type) {
        case GRIB_TYPE_LONG:
            a->pack_long(&lval, &len);
            break;
        case GRIB_TYPE_DOUBLE:
            a->pack_double(&dval, &len);
            break;
        case GRIB_TYPE_STRING:
            if (!sval)
                return NULL;
            /* Performance: No need for len=strlen(sval). It's not used. */
            /* See grib_accessor_class_variable.c, pack_string() */
            len = 0;
            a->pack_string(sval, &len);
            break;
    }

    return a;
}

static void set_creator_name(grib_action* creator, int code)
{
    switch (code) {
        case 222000:
            creator->name_ = (char*)"qualityInformationFollows";
            break;
        case 223000:
            creator->name_ = (char*)"substitutedValuesOperator";
            break;
        case 223255:
            creator->name_ = (char*)"substitutedValue";
            break;
        case 224000:
            creator->name_ = (char*)"firstOrderStatiticalValuesFollow";
            break;
        case 224255:
            creator->name_ = (char*)"firstOrderStatisticalValue";
            break;
        case 225000:
            creator->name_ = (char*)"differenceStatisticalValuesFollow";
            break;
        case 225255:
            creator->name_ = (char*)"differenceStatisticalValue";
            break;
        case 232000:
            creator->name_ = (char*)"replacedRetainedValuesFollow";
            break;
        case 232255:
            creator->name_ = (char*)"replacedRetainedValue";
            break;
        case 235000:
            creator->name_ = (char*)"cancelBackwardDataReference";
            break;
        case 236000:
            creator->name_ = (char*)"defineDataPresentBitmap";
            break;
        case 237000:
            creator->name_ = (char*)"useDefinedDataPresentBitmap";
            break;
        case 237255:
            creator->name_ = (char*)"cancelUseDefinedDataPresentBitmap";
            break;
        case 241000:
            creator->name_ = (char*)"defineEvent";
            break;
        case 241255:
            creator->name_ = (char*)"cancelDefineEvent";
            break;
        case 242000:
            creator->name_ = (char*)"defineConditioningEvent";
            break;
        case 242255:
            creator->name_ = (char*)"canceDefineConditioningEvent";
            break;
        case 243000:
            creator->name_ = (char*)"categoricalForecastValuesFollow";
            break;
        case 243255:
            creator->name_ = (char*)"cancelCategoricalForecastValuesFollow";
            break;
        case 999999:
            creator->name_ = (char*)"associatedField";
            break;
        default:
            if (code > 204999 && code < 206000)
                creator->name_ = (char*)"text";
            else
                creator->name_ = (char*)"operator";
            break;
    }
}

/* See ECC-741 */
static int adding_extra_key_attributes(grib_handle* h)
{
    long skip = 0; /* default is to add */
    int err   = 0;
    err       = grib_get_long(h, "skipExtraKeyAttributes", &skip);
    if (err)
        return 1;
    return (!skip);
}

grib_accessor* grib_accessor_bufr_data_array_t::create_accessor_from_descriptor(grib_accessor* attribute, grib_section* section,
                                                                                long ide, long subset, int add_dump_flag, int add_coord_flag,
                                                                                int count, int add_extra_attributes)
{
    char code[10] = {0,};
    char* temp_str              = NULL;
    int idx                     = 0;
    unsigned long flags         = GRIB_ACCESSOR_FLAG_READ_ONLY;
    grib_action operatorCreator;
    grib_accessor* accessor = NULL;
    grib_accessor_bufr_data_element_t* elementAccessor = NULL;
    grib_accessor_variable_t* variableAccessor = NULL;
    grib_action creator;
    creator.op_         = (char*)"bufr_data_element";
    creator.name_space_ = (char*)"";
    creator.set_        = 0;

    operatorCreator.op_         = (char*)"variable";
    operatorCreator.name_space_ = (char*)"";
    operatorCreator.flags_      = GRIB_ACCESSOR_FLAG_READ_ONLY;
    operatorCreator.set_        = 0;
    operatorCreator.name_       = (char*)"operator";

    if (attribute) {
        DEBUG_ASSERT(attribute->parent_ == NULL);
    }

    if (add_dump_flag) {
        creator.flags_ = GRIB_ACCESSOR_FLAG_DUMP;
        operatorCreator.flags_ |= GRIB_ACCESSOR_FLAG_DUMP;
    }
    if (add_coord_flag) {
        creator.flags_ |= GRIB_ACCESSOR_FLAG_BUFR_COORD;  // ECC-1611
    }

    idx = compressedData_ ? elementsDescriptorsIndex_->v[0]->v[ide] : elementsDescriptorsIndex_->v[subset]->v[ide];

    switch (expanded_->v[idx]->F) {
        case 0:
        case 1:
            creator.name_ = grib_context_strdup(context_, expanded_->v[idx]->shortName);

            /* ECC-325: store alloc'd string (due to strdup) for clean up later */
            grib_sarray_push(tempStrings_, creator.name_);
            accessor = grib_accessor_factory(section, &creator, 0, NULL);
            if (canBeMissing_[idx])
                accessor->flags_ |= GRIB_ACCESSOR_FLAG_CAN_BE_MISSING;
            if (expanded_->v[idx]->code == 31000 || expanded_->v[idx]->code == 31001 || expanded_->v[idx]->code == 31002 || expanded_->v[idx]->code == 31031)
                accessor->flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
            elementAccessor = dynamic_cast<grib_accessor_bufr_data_element_t*>(accessor);
            elementAccessor->index(ide);
            elementAccessor->descriptors(expanded_);
            elementAccessor->elementsDescriptorsIndex(elementsDescriptorsIndex_);
            elementAccessor->numericValues(numericValues_);
            elementAccessor->stringValues(stringValues_);
            elementAccessor->compressedData(compressedData_);
            elementAccessor->type(expanded_->v[idx]->type);
            elementAccessor->numberOfSubsets(numberOfSubsets_);
            elementAccessor->subsetNumber(subset);

            expanded_->v[idx]->a = accessor;

            if (attribute) {
                /* attribute->parent=accessor->parent; */
                /*
            for (i=0;i<MAX_ACCESSOR_ATTRIBUTES;i++) {
                if (attribute->attributes[i]) attribute->attributes[i]->parent=accessor->parent;
            }
            */
                accessor->add_attribute(attribute, 0);
            }

            attribute = create_attribute_variable("index", section, GRIB_TYPE_LONG, 0, 0, count, flags);
            if (!attribute)
                return NULL;
            accessor->add_attribute(attribute, 0);

            snprintf(code, sizeof(code), "%06ld", expanded_->v[idx]->code);
            temp_str  = grib_context_strdup(context_, code);
            attribute = create_attribute_variable("code", section, GRIB_TYPE_STRING, temp_str, 0, 0, flags);
            if (!attribute)
                return NULL;
            grib_sarray_push(tempStrings_, temp_str); /* ECC-325: store alloc'd string (due to strdup) for clean up later */
            accessor->add_attribute(attribute, 0);

            if (add_extra_attributes) {
                attribute = create_attribute_variable("units", section, GRIB_TYPE_STRING, expanded_->v[idx]->units, 0, 0, GRIB_ACCESSOR_FLAG_DUMP | flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);

                attribute = create_attribute_variable("scale", section, GRIB_TYPE_LONG, 0, 0, expanded_->v[idx]->scale, flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);

                attribute = create_attribute_variable("reference", section, GRIB_TYPE_DOUBLE, 0, expanded_->v[idx]->reference, 0, flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);

                attribute = create_attribute_variable("width", section, GRIB_TYPE_LONG, 0, 0, expanded_->v[idx]->width, flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);
            }
            break;
        case 2:
            set_creator_name(&creator, expanded_->v[idx]->code);
            if (bufr_descriptor_is_marker(expanded_->v[idx])) {
                accessor = grib_accessor_factory(section, &creator, 0, NULL);
                if (canBeMissing_[idx])
                    accessor->flags_ |= GRIB_ACCESSOR_FLAG_CAN_BE_MISSING;
                elementAccessor = dynamic_cast<grib_accessor_bufr_data_element_t*>(accessor);
                elementAccessor->index(ide);
                elementAccessor->descriptors(expanded_);
                elementAccessor->elementsDescriptorsIndex(elementsDescriptorsIndex_);
                elementAccessor->numericValues(numericValues_);
                elementAccessor->stringValues(stringValues_);
                elementAccessor->compressedData(compressedData_);
                elementAccessor->type(expanded_->v[idx]->type);
                elementAccessor->numberOfSubsets(numberOfSubsets_);
                elementAccessor->subsetNumber(subset);

                attribute = create_attribute_variable("index", section, GRIB_TYPE_LONG, 0, 0, count, flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);
            }
            else {
                accessor = grib_accessor_factory(section, &operatorCreator, 0, NULL);
                variableAccessor = dynamic_cast<grib_accessor_variable_t*>(accessor);
                variableAccessor->accessor_variable_set_type(GRIB_TYPE_LONG);

                attribute = create_attribute_variable("index", section, GRIB_TYPE_LONG, 0, 0, count, flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);

                snprintf(code, sizeof(code), "%06ld", expanded_->v[idx]->code);
                attribute = create_attribute_variable("code", section, GRIB_TYPE_STRING, code, 0, 0, flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);
            }
            expanded_->v[idx]->a = accessor;
            break;
        case 9:
            set_creator_name(&creator, expanded_->v[idx]->code);
            accessor = grib_accessor_factory(section, &creator, 0, NULL);
            elementAccessor = dynamic_cast<grib_accessor_bufr_data_element_t*>(accessor);

            elementAccessor->index(ide);
            elementAccessor->descriptors(expanded_);
            elementAccessor->elementsDescriptorsIndex(elementsDescriptorsIndex_);
            elementAccessor->numericValues(numericValues_);
            elementAccessor->stringValues(stringValues_);
            elementAccessor->compressedData(compressedData_);
            elementAccessor->type(expanded_->v[idx]->type);
            elementAccessor->numberOfSubsets(numberOfSubsets_);
            elementAccessor->subsetNumber(subset);

            attribute = create_attribute_variable("index", section, GRIB_TYPE_LONG, 0, 0, count, flags);
            if (!attribute)
                return NULL;
            accessor->add_attribute(attribute, 0);

            snprintf(code, sizeof(code), "%06ld", expanded_->v[idx]->code);
            attribute = create_attribute_variable("code", section, GRIB_TYPE_STRING, code, 0, 0, flags);
            if (!attribute)
                return NULL;
            accessor->add_attribute(attribute, 0);

            if (add_extra_attributes) {
                attribute = create_attribute_variable("units", section, GRIB_TYPE_STRING, expanded_->v[idx]->units, 0, 0, GRIB_ACCESSOR_FLAG_DUMP);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);

                attribute = create_attribute_variable("scale", section, GRIB_TYPE_LONG, 0, 0, expanded_->v[idx]->scale, flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);

                attribute = create_attribute_variable("reference", section, GRIB_TYPE_DOUBLE, 0, expanded_->v[idx]->reference, 0, flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);

                attribute = create_attribute_variable("width", section, GRIB_TYPE_LONG, 0, 0, expanded_->v[idx]->width, flags);
                if (!attribute)
                    return NULL;
                accessor->add_attribute(attribute, 0);
            }
            break;
    }

    return accessor;
}

/* Section 3.1.2.2 of WMO BUFR guide: classes 03 and 09 at present reserved for future use */
#define IS_COORDINATE_DESCRIPTOR(a)       (a == 8 || a == 1 || a == 2 || a == 4 || a == 5 || a == 6 || a == 7)
#define NUMBER_OF_QUALIFIERS_PER_CATEGORY 256
#define NUMBER_OF_QUALIFIERS_CATEGORIES   7
#define MAX_NUMBER_OF_BITMAPS             8  // See ECC-1699

static const int number_of_qualifiers              = NUMBER_OF_QUALIFIERS_PER_CATEGORY * NUMBER_OF_QUALIFIERS_CATEGORIES;
static const int significanceQualifierIndexArray[] = { -1, 0, 1, -1, 2, 3, 4, 5, 6 };

static GRIB_INLINE void reset_deeper_qualifiers(
    grib_accessor* significanceQualifierGroup[],
    const int* const significanceQualifierDepth,
    int numElements, int depth)
{
    int i;
    for (i = 0; i < numElements; i++) {
        if (significanceQualifierDepth[i] > depth) {
            significanceQualifierGroup[i] = NULL;
        }
    }
}


static grib_accessor* get_element_from_bitmap(bitmap_s* bitmap)
{
    int ret;
    long bitmapVal = 1;
    size_t len;

    while (bitmapVal) {
        len = 1;
        if (bitmap->cursor && bitmap->cursor->accessor) {
            ret = bitmap->cursor->accessor->unpack_long(&bitmapVal, &len);
        }
        else {
            return NULL;
        }
        if (ret != 0)
            return NULL;
        bitmap->cursor = bitmap->cursor->next_;
        if (bitmap->referredElement)
            bitmap->referredElement = bitmap->referredElement->next_;
    }

    return bitmap->referredElement ? bitmap->referredElement->prev_->accessor : NULL;
}

// static GRIB_INLINE void reset_qualifiers(grib_accessor* significanceQualifierGroup[])
// {
//     int i;
//     for (i=0;i<number_of_qualifiers;i++)
//         significanceQualifierGroup[i]=0;
// }

static void grib_convert_to_attribute(grib_accessor* a)
{
    if (a->h_ == NULL && a->parent_ != NULL) {
        a->h_      = grib_handle_of_accessor(a);
        a->parent_ = NULL;
    }
}

/* subsetList can be NULL in which case subsetListSize will be 0 */
grib_iarray* grib_accessor_bufr_data_array_t::set_subset_list(
    grib_context* c,
    long onlySubset, long startSubset, long endSubset, const long* subsetList, size_t subsetListSize)
{
    grib_iarray* list = grib_iarray_new(numberOfSubsets_, 10);
    long s            = 0;

#ifdef DEBUG
    if (subsetList == NULL) {
        ECCODES_ASSERT(subsetListSize == 0);
    }
    if (subsetListSize == 0) {
        ECCODES_ASSERT(subsetList == NULL);
    }
#endif
    if (startSubset > 0) {
        s = startSubset;
        while (s <= endSubset) {
            grib_iarray_push(list, s - 1);
            s++;
        }
    }

    if (onlySubset > 0)
        grib_iarray_push(list, onlySubset - 1);

    if (subsetList && subsetList[0] > 0) {
        for (s = 0; s < subsetListSize; s++)
            grib_iarray_push(list, subsetList[s] - 1);
    }

    if (grib_iarray_used_size(list) == 0) {
        for (s = 0; s < numberOfSubsets_; s++)
            grib_iarray_push(list, s);
    }

    return list;
}

static int bitmap_ref_skip(grib_accessors_list* al, int* err)
{
    grib_accessor* acode = NULL;
    long code[1];
    size_t l = 1;

    if (!al || !al->accessor)
        return 0;

    acode = al->accessor->get_attribute("code");

    if (acode)
        *err = acode->unpack_long(code, &l);
    else
        return 1;

    switch (code[0]) {
        case 222000:
        case 223000:
        case 224000:
        case 225000:
        case 232000:
        case 236000:
        case 237000:
        case 243000:
        case 31000:
        case 31001:
        case 31002:
            return 1;
    }
    return 0;
}

/* Return 1 if the descriptor is an operator marking the start of a bitmap */
static int is_bitmap_start_descriptor(grib_accessors_list* al, int* err)
{
    grib_accessor* acode = NULL;
    long code[1];
    size_t l = 1;
    if (!al || !al->accessor)
        return 0;

    acode = al->accessor->get_attribute("code");
    if (acode)
        *err = acode->unpack_long(code, &l);
    else
        return 1;

    switch (code[0]) {
        case 222000:
        case 223000:
        case 224000:
        case 225000:
        case 232000:
            /*case 236000:*/
        case 237000:
            /*case 243000:*/
            {
                // long index[1];
                // grib_accessor* anindex=grib_accessor_get_attribute(al->accessor,"index");
                // anindex->unpack_long(index,&l);
                return 1;
            }
    }
    return 0;
}

static void print_bitmap_debug_info(grib_context* c, bitmap_s* bitmap, grib_accessors_list* bitmapStart, int bitmapSize)
{
    int i = 0, ret = 0;
    fprintf(stderr, "ECCODES DEBUG: bitmap_init: bitmapSize=%d\n", bitmapSize);
    bitmap->cursor          = bitmapStart->next_;
    bitmap->referredElement = bitmapStart;

    while (bitmap_ref_skip(bitmap->referredElement, &ret)) {
        int is_bmp = 0;
        if (is_bitmap_start_descriptor(bitmap->referredElement, &ret)) {
            is_bmp = 1;
        }
        bitmap->referredElement = bitmap->referredElement->prev_;
        if (is_bmp) {
            break;
        }
    }

    for (i = 1; i < bitmapSize; i++) {
        if (bitmap->referredElement) {
            fprintf(stderr, "ECCODES DEBUG:\t bitmap_init: i=%d |%s|\n", i, bitmap->referredElement->accessor->name_);
            bitmap->referredElement = bitmap->referredElement->prev_;
        }
    }
}

static int bitmap_init(grib_context* c, bitmap_s* bitmap,
                       grib_accessors_list* bitmapStart, int bitmapSize, grib_accessors_list* lastAccessorInList)
{
    int ret        = 0, i;
    bitmap->cursor = bitmapStart->next_;
    if (bitmap->referredElementStart != NULL) {
        bitmap->referredElement = bitmap->referredElementStart;
        return ret;
    }
    bitmap->referredElement = bitmapStart;
    /*while (bitmap_ref_skip(bitmap->referredElement,&ret)) bitmap->referredElement=bitmap->referredElement->prev_;*/
    /* See ECC-869
     * We have to INCLUDE the replication factors that come after the bitmap operators
     */
    while (bitmap_ref_skip(bitmap->referredElement, &ret)) {
        int is_bmp = 0;
        if (is_bitmap_start_descriptor(bitmap->referredElement, &ret)) {
            is_bmp = 1;
        }
        bitmap->referredElement = bitmap->referredElement->prev_;
        if (is_bmp) {
            break;
        }
    }
    /*printf("bitmap_init: bitmapSize=%d\n", bitmapSize);*/
    for (i = 1; i < bitmapSize; i++) {
        if (bitmap->referredElement == NULL) {
            grib_context_log(c, GRIB_LOG_ERROR, "bitmap_init: bitmap->referredElement==NULL");
            if (c->debug)
                print_bitmap_debug_info(c, bitmap, bitmapStart, bitmapSize);
            return GRIB_INTERNAL_ERROR;
        }
        /*printf("  bitmap_init: i=%d  |%s|\n", i,bitmap->referredElement->accessor->name);*/
        bitmap->referredElement = bitmap->referredElement->prev_;
    }
    bitmap->referredElementStart = bitmap->referredElement;
    return ret;
}

static grib_accessor* accessor_or_attribute_with_same_name(grib_accessor* a, const char* name)
{
    if (a->has_attributes() == 0) {
        return a;
    }
    else {
        grib_accessor* ok = a;
        grib_accessor* next;
        while ((next = ok->get_attribute(name)) != NULL) {
            ok = next;
        }
        return ok;
    }
}

// static int get_key_rank(grib_trie* accessorsRank,grib_accessor* a)
// {
//     int* r=(int*)grib_trie_get(accessorsRank,name_ );
//     if (r) (*r)++;
//     else {
//         r=(int*)grib_context_malloc(context_ ,sizeof(int));
//         *r=1;
//         grib_trie_insert(accessorsRank,name_ ,(void*)r);
//     }
//     return *r;
// }

static int grib_data_accessors_trie_push(grib_trie_with_rank* accessorsTrie, grib_accessor* a)
{
    return grib_trie_with_rank_insert(accessorsTrie, a->name_, a);
}

int grib_accessor_bufr_data_array_t::create_keys(long onlySubset, long startSubset, long endSubset)
{
    int err = 0;
    int rank;
    grib_accessor* elementAccessor                     = 0;
    grib_accessor* associatedFieldAccessor             = 0;
    grib_accessor* associatedFieldSignificanceAccessor = 0;
    long iss, end, elementsInSubset, ide;
    grib_section* section = NULL;
    /*grib_section* rootSection=NULL;*/
    bufr_descriptor* descriptor;
    /*grib_section* sectionUp=0;*/
    grib_section* groupSection = 0;
    // long groupNumber           = 0;
    /*long indexOfGroupNumber=0;*/
    int depth;
    int max_depth = -1; /* highest value of depth */
    int idx;
    grib_context* c    = context_;
    int qualityPresent = 0;
    bitmap_s bitmap    = {
        0,
    };
    int extraElement         = 0;
    int add_extra_attributes = 1;

    grib_accessor* gaGroup   = 0;
    grib_action creatorGroup;
    grib_accessor* significanceQualifierGroup[NUMBER_OF_QUALIFIERS_PER_CATEGORY * NUMBER_OF_QUALIFIERS_CATEGORIES] = {
        0,
    };
    int significanceQualifierDepth[NUMBER_OF_QUALIFIERS_PER_CATEGORY * NUMBER_OF_QUALIFIERS_CATEGORIES] = {
        0,
    };

    grib_accessor* bitmapGroup[MAX_NUMBER_OF_BITMAPS] = {
        0,
    };
    int bitmapDepth[MAX_NUMBER_OF_BITMAPS] = {
        0,
    };
    int bitmapSize[MAX_NUMBER_OF_BITMAPS] = {
        0,
    };
    grib_accessors_list* bitmapStart[MAX_NUMBER_OF_BITMAPS] = {
        0,
    };
    grib_accessors_list* lastAccessorInList = NULL;
    int bitmapIndex                         = -1;
    int incrementBitmapIndex                = 1;
    grib_accessor* elementFromBitmap        = NULL;
    grib_handle* hand                       = grib_handle_of_accessor(this);
    /*int reuseBitmap=0;*/
    int add_dump_flag = 1, add_coord_flag = 0, count = 0;
    /*int forceGroupClosure=0;*/

    creatorGroup.op_         = (char*)"bufr_group";
    creatorGroup.name_       = (char*)"groupNumber";
    creatorGroup.name_space_ = (char*)"";
    creatorGroup.flags_      = GRIB_ACCESSOR_FLAG_DUMP;
    creatorGroup.set_        = 0;

    if (dataAccessors_) {
        grib_accessors_list_delete(c, dataAccessors_);
    }
    dataAccessors_ = grib_accessors_list_create(c);

    if (dataAccessorsTrie_) {
        /* ECC-989: do not call grib_trie_with_rank_delete */
        grib_trie_with_rank_delete_container(dataAccessorsTrie_);
    }
    dataAccessorsTrie_ = grib_trie_with_rank_new(c);

    if (tempStrings_) {
        grib_sarray_delete_content(tempStrings_);
        grib_sarray_delete(tempStrings_);
        tempStrings_ = NULL;
    }
    tempStrings_ = numberOfSubsets_ ? grib_sarray_new(numberOfSubsets_, 500) : NULL;

    end = compressedData_ ? 1 : numberOfSubsets_;
    // groupNumber = 1;

    gaGroup = grib_accessor_factory(dataKeys_, &creatorGroup, 0, NULL);
    // gaGroup->bufr_group_number = groupNumber;
    gaGroup->sub_section_ = grib_section_create(hand, gaGroup);
    section               = gaGroup->sub_section_;
    /*rootSection=section;*/
    /*sectionUp=self->dataKeys_;*/
    // accessor_constant_set_type(gaGroup, GRIB_TYPE_LONG);
    // accessor_constant_set_dval(gaGroup, groupNumber);
    /* ECC-765: Don't empty out the section_4 keys otherwise there will be memory leaks. */
    /* Setting first and last to zero effectively masks out those section 4 keys! */
    /* self->dataKeys_->block->first=0; */
    /* dataKeys_->block->last=0;  */
    grib_push_accessor(gaGroup, dataKeys_->block); /* Add group accessors to section 4 */

    /*indexOfGroupNumber=0;*/
    depth                = 0;
    extraElement         = 0;
    add_extra_attributes = adding_extra_key_attributes(hand);

    for (iss = 0; iss < end; iss++) {
        qualityPresent = 0;
        /*forceGroupClosure=0;*/
        elementsInSubset = compressedData_ ? grib_iarray_used_size(elementsDescriptorsIndex_->v[0]) : grib_iarray_used_size(elementsDescriptorsIndex_->v[iss]);
        /*if (associatedFieldAccessor) grib_accessor_delete(c, associatedFieldAccessor);*/
        associatedFieldAccessor = NULL;
        if (associatedFieldSignificanceAccessor) {
            associatedFieldSignificanceAccessor->destroy(c);
            delete associatedFieldSignificanceAccessor;
            associatedFieldSignificanceAccessor = nullptr;
        }
        for (ide = 0; ide < elementsInSubset; ide++) {
            idx = compressedData_ ? elementsDescriptorsIndex_->v[0]->v[ide] : elementsDescriptorsIndex_->v[iss]->v[ide];

            descriptor = expanded_->v[idx];
            if (descriptor->nokey == 1) {
                continue; /* Descriptor does not have an associated key e.g. inside op 203YYY */
            }
            elementFromBitmap = NULL;
            add_coord_flag    = 0;
            if (descriptor->F == 0 && IS_COORDINATE_DESCRIPTOR(descriptor->X) &&
                unpackMode_ == CODES_BUFR_UNPACK_STRUCTURE) {
                const int sidx = descriptor->Y + significanceQualifierIndexArray[descriptor->X] * NUMBER_OF_QUALIFIERS_PER_CATEGORY;
                DEBUG_ASSERT(sidx > 0);
                // groupNumber++;
                add_coord_flag = 1;

                if (significanceQualifierGroup[sidx]) {
                    groupSection = significanceQualifierGroup[sidx]->parent_;
                    depth        = significanceQualifierDepth[sidx];
                    if (depth < max_depth) {
                        /* If depth >= max_depth, then no entry will be deeper so no need for call */
                        reset_deeper_qualifiers(significanceQualifierGroup, significanceQualifierDepth,
                                                number_of_qualifiers, depth);
                    }
                }
                else {
                    /* if (forceGroupClosure) { */
                    /* groupSection=sectionUp; */
                    /* forceGroupClosure=0; */
                    /* depth=0; */
                    /* } else { */
                    groupSection = section;
                    depth++;
                    /* } */
                }

                gaGroup               = grib_accessor_factory(groupSection, &creatorGroup, 0, NULL);
                gaGroup->sub_section_ = grib_section_create(hand, gaGroup);
                // gaGroup->bufr_group_number = groupNumber;

                // accessor_constant_set_type(gaGroup, GRIB_TYPE_LONG);
                // accessor_constant_set_dval(gaGroup, groupNumber);
                grib_push_accessor(gaGroup, groupSection->block);

                section = gaGroup->sub_section_;
                /*sectionUp=gaGroup->parent;*/

                significanceQualifierGroup[sidx] = gaGroup;
                significanceQualifierDepth[sidx] = depth;
                if (depth > max_depth)
                    max_depth = depth;
                incrementBitmapIndex = 1;
                add_dump_flag        = 1;
            }
            else if (descriptor->code == 31031 && incrementBitmapIndex != 0) {
                /* bitmap */
                bitmapIndex++;
                // groupNumber++;
                incrementBitmapIndex = 0;
                if (bitmapIndex >= MAX_NUMBER_OF_BITMAPS) {
                    // grib_context_log(c, GRIB_LOG_ERROR, "Bitmap error: bitmap index=%d, max num bitmaps=%d\n", bitmapIndex, MAX_NUMBER_OF_BITMAPS);
                    // err = GRIB_DECODING_ERROR;
                    // return err;
                    bitmapIndex--;
                }
                bitmapStart[bitmapIndex] = dataAccessors_->last();
                bitmapSize[bitmapIndex]  = 1;
                if (expanded_->v[idx - 1]->code == 31002 || expanded_->v[idx - 1]->code == 31001)
                    extraElement += 1;

                if (bitmapGroup[bitmapIndex]) {
                    groupSection = bitmapGroup[bitmapIndex]->parent_;
                    depth        = bitmapDepth[bitmapIndex];
                    reset_deeper_qualifiers(significanceQualifierGroup, significanceQualifierDepth,
                                            number_of_qualifiers, depth);
                    /* TODO: This branch is not reached in our tests! */
                    reset_deeper_qualifiers(bitmapGroup, bitmapDepth, MAX_NUMBER_OF_BITMAPS, depth);
                }
                else {
                    groupSection = section;
                    depth++;
                }
                gaGroup               = grib_accessor_factory(groupSection, &creatorGroup, 0, NULL);
                gaGroup->sub_section_ = grib_section_create(hand, gaGroup);
                // gaGroup->bufr_group_number = groupNumber;
                // accessor_constant_set_type(gaGroup, GRIB_TYPE_LONG);
                // accessor_constant_set_dval(gaGroup, groupNumber);
                grib_push_accessor(gaGroup, groupSection->block);

                section = gaGroup->sub_section_;
                /*sectionUp=gaGroup->parent;*/
                bitmapGroup[bitmapIndex] = gaGroup;
                bitmapDepth[bitmapIndex] = depth;
                add_dump_flag            = 1;
            }
            else if (descriptor->code == 31031) {
                add_dump_flag = 1;
                bitmapSize[bitmapIndex]++;
                bitmap.cursor = 0;
            }
            else if (descriptor->code == 222000 || descriptor->code == 223000 || descriptor->code == 224000 || descriptor->code == 225000) {
                bitmap.referredElement = NULL;
                qualityPresent         = 1;
                incrementBitmapIndex   = 1;
                add_dump_flag          = 1;
                bitmap.cursor          = 0;
                extraElement += 1;
            }
            else if (descriptor->code == 236000 || descriptor->code == 237000) {
                bitmap.referredElement = NULL;
                bitmap.cursor          = 0;
                /*reuseBitmap=1;*/
                extraElement += 1;
                add_dump_flag = 1;
            }
            else if (descriptor->code == 237255) {
                /*reuseBitmap=0;*/
                incrementBitmapIndex = 1;
                bitmap.cursor        = 0;
                add_dump_flag        = 1;
            }
            else if ((descriptor->X == 33 || bufr_descriptor_is_marker(descriptor)) && qualityPresent) {
                if (!bitmap.referredElement)
                    bitmap_init(c, &bitmap, bitmapStart[bitmapIndex], bitmapSize[bitmapIndex], lastAccessorInList);
                elementFromBitmap = get_element_from_bitmap(&bitmap);
                add_dump_flag     = 1;
                /* } else if ( descriptor->Y==1 && IS_COORDINATE_DESCRIPTOR(self->expanded_ ->v[idx-1]->X)==0) { */
                /* forceGroupClosure=1; */
                /* reset_qualifiers(significanceQualifierGroup); */
            }
            else if (descriptor->X == 33 && !qualityPresent) {
                add_dump_flag = 1; /* ECC-690: percentConfidence WITHOUT a bitmap! e.g. NOAA GOES16 BUFR */
            }

            if (ide == 0 && !compressedData_) {
                long subsetNumber     = iss + 1;
                size_t len            = 1;
                grib_action creatorsn;
                creatorsn.op_         = (char*)"variable";
                creatorsn.name_space_ = (char*)"";
                creatorsn.flags_      = GRIB_ACCESSOR_FLAG_READ_ONLY | GRIB_ACCESSOR_FLAG_DUMP;
                creatorsn.set_        = 0;

                creatorsn.name_                = (char*)"subsetNumber";
                grib_accessor* a              = grib_accessor_factory(section, &creatorsn, 0, NULL);
                grib_accessor_variable_t* asn = dynamic_cast<grib_accessor_variable_t*>(a);
                asn->accessor_variable_set_type(GRIB_TYPE_LONG);
                asn->pack_long(&subsetNumber, &len);
                grib_push_accessor(asn, section->block);
                rank = grib_data_accessors_trie_push(dataAccessorsTrie_, asn);
                dataAccessors_->push(asn, rank);
            }
            count++;
            elementAccessor = create_accessor_from_descriptor(associatedFieldAccessor, section, ide, iss,
                                                              add_dump_flag, add_coord_flag, count, add_extra_attributes);
            if (!elementAccessor) {
                err = GRIB_DECODING_ERROR;
                return err;
            }
            if (elementAccessor->name_ == NULL) {
                return GRIB_DECODING_ERROR;
            }

            /*if (associatedFieldAccessor) grib_accessor_delete(c, associatedFieldAccessor);*/
            associatedFieldAccessor = NULL;
            if (elementFromBitmap && unpackMode_ == CODES_BUFR_UNPACK_STRUCTURE) {
                if (descriptor->code != 33007 && descriptor->code != 223255) {
                    char* aname                = grib_context_strdup(c, elementFromBitmap->name_);
                    grib_accessor* newAccessor = elementAccessor->clone(section, &err);
                    newAccessor->parent_       = groupSection;
                    newAccessor->name_         = aname;
                    grib_sarray_push(tempStrings_, aname);
                    grib_push_accessor(newAccessor, groupSection->block);
                    rank = grib_data_accessors_trie_push(dataAccessorsTrie_, newAccessor);
                    dataAccessors_->push(newAccessor, rank);
                }

                // err = grib_accessor_add_attribute(accessor_or_attribute_with_same_name(elementFromBitmap, elementAccessor->name), elementAccessor, 1);
                err = accessor_or_attribute_with_same_name(elementFromBitmap, elementAccessor->name_)->add_attribute(elementAccessor, 1);
            }
            else if (elementAccessor) {
                int add_key = 1;
                switch (descriptor->code) {
                    case 999999:
                        /*if (associatedFieldAccessor) grib_accessor_delete(c, associatedFieldAccessor);*/
                        associatedFieldAccessor = elementAccessor;
                        grib_convert_to_attribute(associatedFieldAccessor);
                        if (associatedFieldSignificanceAccessor) {
                            grib_accessor* newAccessor = associatedFieldSignificanceAccessor->clone(section, &err);
                            if (err) {
                                grib_context_log(context_, GRIB_LOG_ERROR, "Unable to clone accessor '%s'\n", associatedFieldSignificanceAccessor->name_);
                                return err;
                            }
                            associatedFieldAccessor->add_attribute(newAccessor, 1);
                            //newAccessor->flags_ |= GRIB_ACCESSOR_FLAG_BUFR_DATA;
                            //associatedFieldAccessor->flags_ |= GRIB_ACCESSOR_FLAG_BUFR_DATA;
                        }
                        break;
                    case 31021:
                        if (associatedFieldSignificanceAccessor) {
                            associatedFieldSignificanceAccessor->destroy(c);
                            delete associatedFieldSignificanceAccessor;
                            associatedFieldSignificanceAccessor = nullptr;
                        }
                        associatedFieldSignificanceAccessor = elementAccessor;
                        break;
                        /*case 33007:*/
                        /* ECC-690: See later */
                        /* break; */
                    default:
                        add_key = 1;
                        /* ECC-690: percentConfidence WITHOUT a bitmap! e.g. NOAA GOES16 BUFR */
                        if (descriptor->code == 33007) {
                            add_key = 0; /* Standard behaviour */
                            if (!qualityPresent) {
                                add_key = 1;
                            }
                        }
                        if (add_key) {
                            grib_push_accessor(elementAccessor, section->block);
                            rank = grib_data_accessors_trie_push(dataAccessorsTrie_, elementAccessor);
                            dataAccessors_->push(elementAccessor, rank);
                            lastAccessorInList = dataAccessors_->last();
                        }
                }
            }
        }
    }
    (void)extraElement;
    return err;
}

void grib_accessor_bufr_data_array_t::set_input_replications(grib_handle* h)
{
    size_t nInputReplications;
    size_t nInputExtendedReplications;
    size_t nInputShortReplications;
    nInputReplications_         = -1;
    nInputExtendedReplications_ = -1;
    nInputShortReplications_    = -1;
    iInputReplications_         = 0;
    iInputExtendedReplications_ = 0;
    iInputShortReplications_    = 0;
    if (grib_get_size(h, "inputDelayedDescriptorReplicationFactor", &nInputReplications) == 0 && nInputReplications != 0) {
        if (inputReplications_)
            grib_context_free(h->context, inputReplications_);
        inputReplications_ = (long*)grib_context_malloc_clear(h->context, sizeof(long) * nInputReplications);
        grib_get_long_array(h, "inputDelayedDescriptorReplicationFactor", inputReplications_, &nInputReplications);
        /* default-> no input replications*/
        if (inputReplications_[0] < 0)
            nInputReplications_ = -1;
        else
            nInputReplications_ = nInputReplications;
    }
    if (grib_get_size(h, "inputExtendedDelayedDescriptorReplicationFactor", &nInputExtendedReplications) == 0 && nInputExtendedReplications != 0) {
        if (inputExtendedReplications_)
            grib_context_free(h->context, inputExtendedReplications_);
        inputExtendedReplications_ = (long*)grib_context_malloc_clear(h->context, sizeof(long) * nInputExtendedReplications);
        grib_get_long_array(h, "inputExtendedDelayedDescriptorReplicationFactor", inputExtendedReplications_, &nInputExtendedReplications);
        /* default-> no input replications*/
        if (inputExtendedReplications_[0] < 0)
            nInputExtendedReplications_ = -1;
        else
            nInputExtendedReplications_ = nInputExtendedReplications;
    }
    if (grib_get_size(h, "inputShortDelayedDescriptorReplicationFactor", &nInputShortReplications) == 0 && nInputShortReplications != 0) {
        if (inputShortReplications_)
            grib_context_free(h->context, inputShortReplications_);
        inputShortReplications_ = (long*)grib_context_malloc_clear(h->context, sizeof(long) * nInputShortReplications);
        grib_get_long_array(h, "inputShortDelayedDescriptorReplicationFactor", inputShortReplications_, &nInputShortReplications);
        /* default-> no input replications*/
        if (inputShortReplications_[0] < 0)
            nInputShortReplications_ = -1;
        else
            nInputShortReplications_ = nInputShortReplications;
    }
}

void grib_accessor_bufr_data_array_t::set_input_bitmap(grib_handle* h)
{
    size_t nInputBitmap;
    nInputBitmap_ = -1;
    iInputBitmap_ = 0;
    if (grib_get_size(h, "inputDataPresentIndicator", &nInputBitmap) == 0 && nInputBitmap != 0) {
        if (inputBitmap_)
            grib_context_free(h->context, inputBitmap_);
        inputBitmap_ = (double*)grib_context_malloc_clear(h->context, sizeof(double) * nInputBitmap);
        grib_get_double_array(h, "inputDataPresentIndicator", inputBitmap_, &nInputBitmap);
        /* default-> no input bitmap*/
        if (inputBitmap_[0] < 0)
            nInputBitmap_ = -1;
        else
            nInputBitmap_ = nInputBitmap;
    }
}

static int set_to_missing_if_out_of_range(grib_handle* h)
{
    /* First check if the transient key is set */
    long setToMissingIfOutOfRange = 0;
    if (grib_get_long(h, "setToMissingIfOutOfRange", &setToMissingIfOutOfRange) == GRIB_SUCCESS &&
        setToMissingIfOutOfRange != 0) {
        return 1;
    }
    /* Then check the environment variable via the context */
    return h->context->bufr_set_to_missing_if_out_of_range;
}

int grib_accessor_bufr_data_array_t::process_elements(int flag, long onlySubset, long startSubset, long endSubset)
{
    int err = 0;
    long inr, innr, ir, ip;
    long n[MAX_NESTED_REPLICATIONS] = {0,};
    long nn[MAX_NESTED_REPLICATIONS] = {0,};
    long numberOfElementsToRepeat[MAX_NESTED_REPLICATIONS] = {0,};
    long numberOfRepetitions[MAX_NESTED_REPLICATIONS] = {0,};
    long startRepetition[MAX_NESTED_REPLICATIONS] = {0,};
    long numberOfNestedRepetitions = 0;
    unsigned char* data            = 0;
    size_t subsetListSize          = 0;
    long* subsetList               = 0;
    int i;
    grib_iarray* elementsDescriptorsIndex = 0;

    long pos = 0, dataOffset = 0;
    long iiss, iss, end, elementIndex, index;
    long numberOfDescriptors;
    long totalSize;
    bufr_descriptor** descriptors = 0;
    long icount;
    int decoding = 0, do_clean = 1;
    grib_buffer* buffer = NULL;
    codec_element_proc codec_element;
    codec_replication_proc codec_replication;
    grib_accessor* dataAccessor = NULL;
    bufr_descriptor* bd         = 0;

    grib_darray* dval = NULL;
    grib_sarray* sval = NULL;

    grib_handle* h  = grib_handle_of_accessor(this);
    grib_context* c = h->context;

    totalSize = bitsToEndData_;

    switch (flag) {
        case PROCESS_DECODE:
            if (!do_decode_)
                return 0;
            do_decode_   = 0;
            buffer       = h->buffer;
            decoding     = 1;
            do_clean     = 1;
            dataAccessor = grib_find_accessor(grib_handle_of_accessor(this), bufrDataEncodedName_);
            DEBUG_ASSERT(dataAccessor);
            dataOffset        = accessor_raw_get_offset(dataAccessor);
            pos               = dataOffset * 8;
            codec_element     = &decode_element;
            codec_replication = &decode_replication;
            break;
        case PROCESS_NEW_DATA:
            buffer                          = grib_create_growable_buffer(c);
            decoding                        = 0;
            do_clean                        = 1;
            do_decode_                      = 1;
            set_to_missing_if_out_of_range_ = set_to_missing_if_out_of_range(h);
            pos                             = 0;
            codec_element                   = &encode_new_element;
            codec_replication               = &encode_new_replication;

            set_input_replications(h);
            set_input_bitmap(h);

            break;
        case PROCESS_ENCODE:
            buffer                          = grib_create_growable_buffer(c);
            decoding                        = 0;
            do_clean                        = 0;
            do_decode_                      = 0;
            set_to_missing_if_out_of_range_ = set_to_missing_if_out_of_range(h);
            pos                             = 0;
            codec_element                   = &encode_element;
            grib_get_long(grib_handle_of_accessor(this), "extractSubset", &onlySubset);
            grib_get_long(grib_handle_of_accessor(this), "extractSubsetIntervalStart", &startSubset);
            grib_get_long(grib_handle_of_accessor(this), "extractSubsetIntervalEnd", &endSubset);
            err = grib_get_size(grib_handle_of_accessor(this), "extractSubsetList", &subsetListSize);
            if (err)
                return err;
            if (subsetList)
                grib_context_free(c, subsetList);
            if (subsetListSize) {
                subsetList = (long*)grib_context_malloc_clear(c, subsetListSize * sizeof(long));
                err        = grib_get_long_array(grib_handle_of_accessor(this), "extractSubsetList", subsetList, &subsetListSize);
                if (err) return err;
            }
            codec_replication = &encode_replication;
            break;
        default:
            return GRIB_NOT_IMPLEMENTED;
    }
    data = buffer->data;

    err = get_descriptors();
    if (err) return err;

    descriptors = expanded_->v;
    if (!descriptors) {
        grib_context_log(c, GRIB_LOG_ERROR, "No descriptors found!");
        return GRIB_INTERNAL_ERROR;
    }

    if (do_clean == 1 && numericValues_) {
        grib_vdarray_delete_content(numericValues_);
        grib_vdarray_delete(numericValues_);
        /*printf("dbg process_elements: clear %p\n", (void*)(stringValues_ ));*/
        grib_vsarray_delete_content(stringValues_);
        grib_vsarray_delete(stringValues_);
        stringValues_ = NULL;
    }

    if (flag != PROCESS_ENCODE) {
        numericValues_ = grib_vdarray_new(1000, 1000);
        stringValues_  = grib_vsarray_new(10, 10);

        if (elementsDescriptorsIndex_) {
            grib_viarray_delete_content(elementsDescriptorsIndex_);
            grib_viarray_delete(elementsDescriptorsIndex_);
        }
        elementsDescriptorsIndex_ = grib_viarray_new(100, 100);
    }
    if (flag == PROCESS_NEW_DATA) {
        tempDoubleValues_ = grib_vdarray_new(1000, 1000);
    }

    if (flag != PROCESS_DECODE) { /* Operator 203YYY: key OVERRIDDEN_REFERENCE_VALUES_KEY */
        err = grib_get_size(h, OVERRIDDEN_REFERENCE_VALUES_KEY, &refValListSize_);
        if (err) return err;
        if (refValList_)
            grib_context_free(c, refValList_);
        if (refValListSize_ > 0) {
            refValList_ = (long*)grib_context_malloc_clear(c, refValListSize_ * sizeof(long));
            err         = grib_get_long_array(grib_handle_of_accessor(this), OVERRIDDEN_REFERENCE_VALUES_KEY, refValList_, &refValListSize_);
            if (err) return err;
        }
    }

    numberOfDescriptors = grib_bufr_descriptors_array_used_size(expanded_);

    if (iss_list_) {
        grib_iarray_delete(iss_list_);
        iss_list_ = 0;
    }
    end = compressedData_ == 1 ? 1 : numberOfSubsets_;

    if (flag != PROCESS_DECODE) {
        iss_list_ = set_subset_list(c, onlySubset, startSubset, endSubset, subsetList, subsetListSize);
        end       = compressedData_ == 1 ? 1 : grib_iarray_used_size(iss_list_);
    }

    /* Go through all subsets */
    for (iiss = 0; iiss < end; iiss++) {
        icount = 1;
        if (compressedData_ == 0 && iss_list_) {
            iss = iss_list_->v[iiss];
        }
        else {
            iss = iiss;
        }

        grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data processing: subsetNumber=%ld", iss + 1);
        refValIndex_ = 0;

        if (flag != PROCESS_ENCODE) {
            elementsDescriptorsIndex = grib_iarray_new(DYN_ARRAY_SIZE_INIT, DYN_ARRAY_SIZE_INCR);
            if (!compressedData_) {
                dval = grib_darray_new(DYN_ARRAY_SIZE_INIT, DYN_ARRAY_SIZE_INCR);
            }
        }
        else {
            if (elementsDescriptorsIndex_ == NULL) {
                grib_buffer_delete(c, buffer);
                return GRIB_ENCODING_ERROR; /* See ECC-359 */
            }
            elementsDescriptorsIndex = elementsDescriptorsIndex_->v[iss];
            dval                     = numericValues_->v[iss];
        }
        elementIndex = 0;

        numberOfNestedRepetitions = 0;

        for (i = 0; i < numberOfDescriptors; i++) {
            int op203_definition_phase = 0;
            if (c->debug) grib_context_log(c, GRIB_LOG_DEBUG, "BUFR data processing: elementNumber=%ld code=%6.6ld", icount++, descriptors[i]->code);
            switch (descriptors[i]->F) {
                case 0:
                    /* Table B element */
                    op203_definition_phase = (change_ref_value_operand_ > 0 && change_ref_value_operand_ != 255);

                    if (flag != PROCESS_ENCODE) {
                        if (!op203_definition_phase)
                            grib_iarray_push(elementsDescriptorsIndex, i);
                    }
                    if (descriptors[i]->code == 31031 && !is_bitmap_start_defined()) {
                        /* bitmapStart_ =grib_iarray_used_size(elementsDescriptorsIndex)-1; */
                        bitmapStart_ = elementIndex;
                    }

                    err = codec_element(c, this, iss, buffer, data, &pos, i, 0, elementIndex, dval, sval);
                    if (err) return err;
                    if (!op203_definition_phase)
                        elementIndex++;
                    break;
                case 1:
                    /* Delayed replication */
                    inr = numberOfNestedRepetitions;
                    numberOfNestedRepetitions++;
                    DEBUG_ASSERT(numberOfNestedRepetitions <= MAX_NESTED_REPLICATIONS);
                    numberOfElementsToRepeat[inr] = descriptors[i]->X;
                    n[inr]                        = numberOfElementsToRepeat[inr];
                    i++;

                    data = buffer->data; /* ECC-517 */
                    err  = codec_replication(c, this, iss, buffer, data, &pos, i, elementIndex, dval, &(numberOfRepetitions[inr]));
                    if (err) return err;

                    startRepetition[inr] = i;
                    nn[inr]              = numberOfRepetitions[inr];
                    if (flag != PROCESS_ENCODE)
                        grib_iarray_push(elementsDescriptorsIndex, i);
                    elementIndex++;
                    if (numberOfRepetitions[inr] == 0) {
                        i += numberOfElementsToRepeat[inr];
                        if (inr > 0) {
                            n[inr - 1] -= numberOfElementsToRepeat[inr] + 2;
                            /* if the empty nested repetition is at the end of the nesting repetition
                           we need to re-point to the start of the nesting repetition */
                            ip = inr - 1;
                            while (ip >= 0 && n[ip] == 0) {
                                nn[ip]--;
                                if (nn[ip] <= 0) {
                                    numberOfNestedRepetitions--;
                                }
                                else {
                                    n[ip] = numberOfElementsToRepeat[ip];
                                    i     = startRepetition[ip];
                                }
                                ip--;
                            }
                        }
                        numberOfNestedRepetitions--;
                    }
                    continue;
                case 2:
                    /* Operator */
                    switch (descriptors[i]->X) {
                        case 3: /* Change reference values */
                            if (compressedData_ == 1 && flag != PROCESS_DECODE) {
                                grib_context_log(c, GRIB_LOG_ERROR, "process_elements: operator %d not supported for encoding compressed data", descriptors[i]->X);
                                return GRIB_INTERNAL_ERROR;
                            }
                            if (descriptors[i]->Y == 255) {
                                grib_context_log(c, GRIB_LOG_DEBUG, "Operator 203YYY: Y=255, definition of new reference values is concluded");
                                change_ref_value_operand_ = 255;
                                /*if (c->debug) tableB_override_dump(self);*/
                                if (iss == 0 && flag == PROCESS_DECODE) {
                                    /*Write out the contents of the TableB overridden reference values to the transient array key*/
                                    err = tableB_override_set_key(h);
                                    if (err) return err;
                                }
                                if (flag != PROCESS_DECODE) {
                                    /* Encoding operator 203YYY */
                                    if (refValIndex_ != refValListSize_) {
                                        grib_context_log(c, GRIB_LOG_ERROR,
                                                         "process_elements: The number of overridden reference values (%ld) different from"
                                                         " number of descriptors between operator 203YYY and 203255 (%ld)",
                                                         refValListSize_, refValIndex_);
                                        return GRIB_ENCODING_ERROR;
                                    }
                                }
                            }
                            else if (descriptors[i]->Y == 0) {
                                grib_context_log(c, GRIB_LOG_DEBUG, "Operator 203YYY: Y=0, clearing override of table B");
                                tableB_override_clear(c);
                                change_ref_value_operand_ = 0;
                            }
                            else {
                                const int numBits = descriptors[i]->Y;
                                grib_context_log(c, GRIB_LOG_DEBUG, "Operator 203YYY: Definition phase: Num bits=%d", numBits);
                                change_ref_value_operand_ = numBits;
                                tableB_override_clear(c);
                                if (flag != PROCESS_DECODE) {
                                    err = check_overridden_reference_values(c, refValList_, refValListSize_, numBits);
                                    if (err) return err;
                                }
                            }
                            /*grib_iarray_push(elementsDescriptorsIndex,i);*/
                            break;

                        case 5: /* Signify character */
                            descriptors[i]->width = descriptors[i]->Y * 8;
                            descriptors[i]->type  = BUFR_DESCRIPTOR_TYPE_STRING;
                            err                   = codec_element(c, this, iss, buffer, data, &pos, i, 0, elementIndex, dval, sval);
                            if (err) return err;
                            if (flag != PROCESS_ENCODE)
                                grib_iarray_push(elementsDescriptorsIndex, i);
                            elementIndex++;
                            break;
                        case 22: /* Quality information follows */
                            if (descriptors[i]->Y == 0) {
                                if (flag == PROCESS_DECODE) {
                                    grib_iarray_push(elementsDescriptorsIndex, i);
                                    push_zero_element(dval);
                                }
                                else if (flag == PROCESS_ENCODE) {
                                    if (descriptors[i + 1] && descriptors[i + 1]->code != 236000 && descriptors[i + 1]->code != 237000)
                                        restart_bitmap();
                                }
                                else if (flag == PROCESS_NEW_DATA) {
                                    grib_iarray_push(elementsDescriptorsIndex, i);
                                    if (descriptors[i + 1] && descriptors[i + 1]->code != 236000 && descriptors[i + 1]->code != 237000)
                                        consume_bitmap(i);
                                }
                                elementIndex++;
                            }
                            break;
                        case 26:
                        case 27:
                        case 29:
                        case 30:
                        case 31:
                        case 33:
                        case 34:
                        case 38:
                        case 39:
                        case 40:
                        case 41:
                        case 42:
                            if (flag != PROCESS_ENCODE)
                                grib_iarray_push(elementsDescriptorsIndex, i);
                            if (decoding)
                                push_zero_element(dval);
                            elementIndex++;
                            break;
                        case 24: /* First-order statistical values marker operator */
                        case 32: /* Replaced/retained values marker operator */
                            if (descriptors[i]->Y == 255) {
                                index = get_next_bitmap_descriptor_index(elementsDescriptorsIndex, dval);
                                if (index < 0) { /* Return value is an error code not an index */
                                    err = index;
                                    return err;
                                }
                                err = codec_element(c, this, iss, buffer, data, &pos, index, 0, elementIndex, dval, sval);
                                if (err) return err;
                                /* expanded_ ->v[index] */
                                if (flag != PROCESS_ENCODE)
                                    grib_iarray_push(elementsDescriptorsIndex, i);
                                elementIndex++;
                            }
                            else {
                                if (flag != PROCESS_ENCODE)
                                    grib_iarray_push(elementsDescriptorsIndex, i);
                                if (decoding) {
                                    push_zero_element(dval);
                                }
                                elementIndex++;
                            }
                            break;
                        case 23: /* Substituted values operator */
                            if (descriptors[i]->Y == 255) {
                                index = get_next_bitmap_descriptor_index(elementsDescriptorsIndex, dval);
                                if (index < 0) { /* Return value is an error code not an index */
                                    err = index;
                                    return err;
                                }
                                err = codec_element(c, this, iss, buffer, data, &pos, index, 0, elementIndex, dval, sval);
                                if (err) return err;
                                /* expanded_ ->v[index] */
                                if (flag != PROCESS_ENCODE)
                                    grib_iarray_push(elementsDescriptorsIndex, i);
                                elementIndex++;
                            }
                            else {
                                if (flag == PROCESS_DECODE) {
                                    grib_iarray_push(elementsDescriptorsIndex, i);
                                    push_zero_element(dval);
                                    if (descriptors[i + 1] && descriptors[i + 1]->code != 236000 && descriptors[i + 1]->code != 237000) {
                                        err = build_bitmap(data, &pos, elementIndex, elementsDescriptorsIndex, i);
                                        if (err) return err;
                                    }
                                }
                                else if (flag == PROCESS_ENCODE) {
                                    if (descriptors[i + 1] && descriptors[i + 1]->code != 236000 && descriptors[i + 1]->code != 237000)
                                        restart_bitmap();
                                }
                                else if (flag == PROCESS_NEW_DATA) {
                                    grib_iarray_push(elementsDescriptorsIndex, i);
                                    if (descriptors[i + 1] && descriptors[i + 1]->code != 236000 && descriptors[i + 1]->code != 237000) {
                                        err = build_bitmap_new_data(data, &pos, elementIndex, elementsDescriptorsIndex, i);
                                        if (err) return err;
                                    }
                                }
                                elementIndex++;
                            }
                            break;
                        case 25: /* Difference statistical values marker operator */
                            if (descriptors[i]->Y == 255) {
                                index = get_next_bitmap_descriptor_index(elementsDescriptorsIndex, dval);
                                if (index < 0) { /* Return value is an error code not an index */
                                    err = index;
                                    return err;
                                }
                                bd            = grib_bufr_descriptor_clone(expanded_->v[index]);
                                bd->reference = -codes_power<double>(bd->width, 2);
                                bd->width++;

                                err = codec_element(c, this, iss, buffer, data, &pos, index, bd, elementIndex, dval, sval);
                                grib_bufr_descriptor_delete(bd);
                                if (err) return err;
                                /* expanded_ ->v[index] */
                                if (flag != PROCESS_ENCODE)
                                    grib_iarray_push(elementsDescriptorsIndex, i);
                                elementIndex++;
                            }
                            else {
                                if (flag != PROCESS_ENCODE)
                                    grib_iarray_push(elementsDescriptorsIndex, i);
                                if (decoding)
                                    push_zero_element(dval);
                                elementIndex++;
                            }
                            break;
                        case 35: /* Cancel backward data reference (cancel bitmap) */
                            if (flag != PROCESS_ENCODE) {
                                grib_iarray_push(elementsDescriptorsIndex, i);
                                if (decoding)
                                    push_zero_element(dval);
                                if (descriptors[i]->Y == 0)
                                    cancel_bitmap();
                            }
                            elementIndex++;
                            break;
                        case 36: /* Define data present bit-map */
                            if (flag == PROCESS_DECODE) {
                                grib_iarray_push(elementsDescriptorsIndex, i);
                                if (decoding)
                                    push_zero_element(dval);
                                err = build_bitmap(data, &pos, elementIndex, elementsDescriptorsIndex, i);
                                if (err) return err;
                            }
                            else if (flag == PROCESS_ENCODE) {
                                restart_bitmap();
                            }
                            else if (flag == PROCESS_NEW_DATA) {
                                grib_iarray_push(elementsDescriptorsIndex, i);
                                err = build_bitmap_new_data(data, &pos, elementIndex, elementsDescriptorsIndex, i);
                                if (err) return err;
                            }
                            elementIndex++;
                            break;
                        case 37: /* Use defined data present bit-map = reuse defined bitmap */
                            if (flag != PROCESS_ENCODE) {
                                grib_iarray_push(elementsDescriptorsIndex, i);
                                if (decoding)
                                    push_zero_element(dval);
                            }
                            if (descriptors[i]->Y == 0)
                                restart_bitmap();
                            /* cancel reuse */
                            else
                                cancel_bitmap();
                            elementIndex++;
                            break;
                        default:
                            grib_context_log(c, GRIB_LOG_ERROR, "process_elements: unsupported operator %d\n", descriptors[i]->X);
                            return GRIB_INTERNAL_ERROR;
                    } /* F == 2 */
                    break;
                case 9:
                    /* Associated field */
                    if (descriptors[i]->X == 99 && descriptors[i]->Y == 999) {
                        err = codec_element(c, this, iss, buffer, data, &pos, i, 0, elementIndex, dval, sval);
                        if (err) return err;
                        if (flag != PROCESS_ENCODE)
                            grib_iarray_push(elementsDescriptorsIndex, i);
                        elementIndex++;
                    }
                    else {
                        return GRIB_INTERNAL_ERROR;
                    }
                    break;
                default:
                    err = GRIB_INTERNAL_ERROR;
                    return err;
            } /* switch F */

            /* Delayed repetition check */
            innr = numberOfNestedRepetitions - 1;
            for (ir = innr; ir >= 0; ir--) {
                if (nn[ir]) {
                    if (n[ir] > 1) {
                        n[ir]--;
                        break;
                    }
                    else {
                        n[ir] = numberOfElementsToRepeat[ir];
                        nn[ir]--;
                        if (nn[ir]) {
                            i = startRepetition[ir];
                            break;
                        }
                        else {
                            if (ir > 0) {
                                n[ir - 1] -= numberOfElementsToRepeat[ir] + 1;
                            }
                            i = startRepetition[ir] + numberOfElementsToRepeat[ir];
                            numberOfNestedRepetitions--;
                        }
                    }
                }
                else {
                    if (ir == 0) {
                        i                         = startRepetition[ir] + numberOfElementsToRepeat[ir] + 1;
                        numberOfNestedRepetitions = 0;
                    }
                    else {
                        numberOfNestedRepetitions--;
                    }
                }
            }
        } /* for all descriptors */

        if (flag != PROCESS_ENCODE) {
            grib_viarray_push(elementsDescriptorsIndex_, elementsDescriptorsIndex);
            /*grib_iarray_print("DBG process_elements::elementsDescriptorsIndex", elementsDescriptorsIndex);*/
        }
        if (decoding && !compressedData_) {
            grib_vdarray_push(numericValues_, dval);
            /*grib_darray_print("DBG process_elements::dval", dval);*/
        }
        if (flag == PROCESS_NEW_DATA && !compressedData_) {
            grib_vdarray_push(tempDoubleValues_, dval); /* ECC-1172 */
        }
    } /* for all subsets */

    /*grib_vdarray_print("DBG process_elements: numericValues",            numericValues_ );*/
    /*grib_viarray_print("DBG process_elements: elementsDescriptorsIndex", elementsDescriptorsIndex_ );*/

    if (decoding) {
        err            = create_keys(0, 0, 0);
        bitsToEndData_ = totalSize;
    }
    else {
        bitsToEndData_ = buffer->ulength * 8;
        grib_set_bytes(grib_handle_of_accessor(this), bufrDataEncodedName_, buffer->data, &(buffer->ulength));
        grib_buffer_delete(c, buffer);
        if (numberOfSubsets_ != grib_iarray_used_size(iss_list_)) {
            grib_set_long(h, numberOfSubsetsName_, grib_iarray_used_size(iss_list_));
        }
    }

    if (subsetList)
        grib_context_free(c, subsetList); /* ECC-1498 */

    return err;
}

void grib_accessor_bufr_data_array_t::dump(eccodes::Dumper* dumper)
{
    // grib_accessor_bufr_data_array_t *self =(grib_accessor_bufr_data_array_t*)a;
    // int err=process_elements(a,PROCESS_DECODE);
    // dumper->dump_section(a,self->dataKeys_ ->block);
    return;
}

int grib_accessor_bufr_data_array_t::value_count(long* count)
{
    int err = 0, l;
    long i;

    err = process_elements(PROCESS_DECODE, 0, 0, 0);
    if (err)
        return err;

    if (compressedData_) {
        l = grib_vdarray_used_size(numericValues_);

        *count = l * numberOfSubsets_;
    }
    else {
        *count = 0;
        for (i = 0; i < numberOfSubsets_; i++)
            *count += grib_iarray_used_size(elementsDescriptorsIndex_->v[i]);
    }

    return err;
}

int grib_accessor_bufr_data_array_t::unpack_double(double* val, size_t* len)
{
    int err              = 0, i, k, ii;
    int proc_flag        = PROCESS_DECODE;
    size_t l             = 0, elementsInSubset;
    long numberOfSubsets = 0;

    if (unpackMode_ == CODES_BUFR_NEW_DATA)
        proc_flag = PROCESS_NEW_DATA;

    err = process_elements(proc_flag, 0, 0, 0);
    if (err)
        return err;
    if (!val)
        return GRIB_SUCCESS;

    /* When we set unpack=1, then the 'val' argument is NULL and we return
     * but when client requests a key like 'numericValues', then we end up here
     */

    l   = grib_vdarray_used_size(numericValues_);
    err = grib_get_long(grib_handle_of_accessor(this), numberOfSubsetsName_, &numberOfSubsets);
    if (err)
        return err;

    if (compressedData_) {
        const size_t rlen = l * numberOfSubsets_;
        ii                = 0;
        if (*len < rlen) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "wrong size (%ld) for %s, it contains %ld values ", *len, name_, rlen);
            *len = 0;
            return GRIB_ARRAY_TOO_SMALL;
        }
        for (k = 0; k < numberOfSubsets; k++) {
            for (i = 0; i < l; i++) {
                val[ii++] = numericValues_->v[i]->n > 1 ? numericValues_->v[i]->v[k] : numericValues_->v[i]->v[0];
            }
        }
    }
    else {
        ii = 0;
        for (k = 0; k < numberOfSubsets; k++) {
            elementsInSubset = grib_iarray_used_size(elementsDescriptorsIndex_->v[k]);
            for (i = 0; i < elementsInSubset; i++) {
                val[ii++] = numericValues_->v[k]->v[i];
            }
        }
    }

    return GRIB_SUCCESS;
}

void grib_accessor_bufr_data_array_t::destroy(grib_context* c)
{
    self_clear();
    if (dataAccessors_)
        grib_accessors_list_delete(c, dataAccessors_);
    if (dataAccessorsTrie_) {
        grib_trie_with_rank_delete_container(dataAccessorsTrie_);
        dataAccessorsTrie_ = NULL;
    }
    if (tempStrings_) {
        grib_sarray_delete_content(tempStrings_);
        grib_sarray_delete(tempStrings_);
    }
    if (tempDoubleValues_) {
        /* ECC-1172: Clean up to avoid memory leaks */
        grib_vdarray_delete_content(tempDoubleValues_);
        grib_vdarray_delete(tempDoubleValues_);
        tempDoubleValues_ = NULL;
    }

    grib_iarray_delete(iss_list_);
    grib_accessor_gen_t::destroy(c);
}
