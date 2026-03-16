/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g22order_packing.h"

grib_accessor_data_g22order_packing_t _grib_accessor_data_g22order_packing{};
grib_accessor* grib_accessor_data_g22order_packing = &_grib_accessor_data_g22order_packing;

void grib_accessor_data_g22order_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_values_t::init(v, args);
    grib_handle* gh = grib_handle_of_accessor(this);

    numberOfValues_        = args->get_name(gh, carg_++);
    bits_per_value_        = args->get_name(gh, carg_++);
    reference_value_       = args->get_name(gh, carg_++);
    binary_scale_factor_   = args->get_name(gh, carg_++);
    decimal_scale_factor_  = args->get_name(gh, carg_++);
    optimize_scale_factor_ = args->get_name(gh, carg_++);

    typeOfOriginalFieldValues_                = args->get_name(gh, carg_++);
    groupSplittingMethodUsed_                 = args->get_name(gh, carg_++);
    missingValueManagementUsed_               = args->get_name(gh, carg_++);
    primaryMissingValueSubstitute_            = args->get_name(gh, carg_++);
    secondaryMissingValueSubstitute_          = args->get_name(gh, carg_++);
    numberOfGroupsOfDataValues_               = args->get_name(gh, carg_++);
    referenceForGroupWidths_                  = args->get_name(gh, carg_++);
    numberOfBitsUsedForTheGroupWidths_        = args->get_name(gh, carg_++);
    referenceForGroupLengths_                 = args->get_name(gh, carg_++);
    lengthIncrementForTheGroupLengths_        = args->get_name(gh, carg_++);
    trueLengthOfLastGroup_                    = args->get_name(gh, carg_++);
    numberOfBitsUsedForTheScaledGroupLengths_ = args->get_name(gh, carg_++);

    orderOfSpatialDifferencing_     = args->get_name(gh, carg_++);
    numberOfOctetsExtraDescriptors_ = args->get_name(gh, carg_++);
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

#define ONES (~(int)0)

// #define UNDEFINED 9.999e20
// #define UNDEFINED_LOW 9.9989e20
// #define UNDEFINED_HIGH 9.9991e20
#define UNDEFINED        9999.0
#define UNDEFINED_LOW    9998.9
#define UNDEFINED_HIGH   9999.1
#define UNDEFINED_VAL(x) ((x) >= UNDEFINED_LOW && (x) <= UNDEFINED_HIGH)
#define DEFINED_VAL(x)   ((x) < UNDEFINED_LOW || (x) > UNDEFINED_HIGH)
#define UNDEFINED_ANGLE  999.0

struct bitstream_context
{
    unsigned char* bitstream;
    int rbits;
    int reg;
    int n_bitstream;
};

void init_bitstream(bitstream_context* ctx, unsigned char* new_bitstream)
{
    ctx->bitstream   = new_bitstream;
    ctx->n_bitstream = ctx->reg = ctx->rbits = 0;
}

void finish_bitstream(bitstream_context* ctx)
{
    if (ctx->rbits) {
        ctx->n_bitstream++;
        *ctx->bitstream++ = (ctx->reg << (8 - ctx->rbits)) & 255;
        ctx->rbits        = 0;
    }
}

void add_many_bitstream(bitstream_context* ctx, grib_accessor* a, int* t, int n, int n_bits)
{
    unsigned int jmask;
    int i;
    const int max_numbits = 25;

    if (n_bits > max_numbits) {
        grib_context_log(a->context_, GRIB_LOG_FATAL, "grid_complex packing: n_bits=%d exceeds the maximum=%d", n_bits, max_numbits);
    }
    jmask = (1 << n_bits) - 1;

    for (i = 0; i < n; i++) {
        unsigned int tt = (unsigned int)*t++;
        ctx->rbits += n_bits;
        ctx->reg = (ctx->reg << n_bits) | (tt & jmask);

        while (ctx->rbits >= 8) {
            ctx->rbits -= 8;
            *ctx->bitstream++ = (ctx->reg >> ctx->rbits) & 255;
            ctx->n_bitstream++;
        }
    }
}

void add_bitstream(bitstream_context* ctx, grib_accessor* a, int t, int n_bits)
{
    unsigned int jmask;
    const int max_numbits = 25;

    if (n_bits > 16) {
        add_bitstream(ctx, a, t >> 16, n_bits - 16);
        n_bits = 16;
    }
    if (n_bits > max_numbits) {
        grib_context_log(a->context_, GRIB_LOG_FATAL, "grid_complex packing: n_bits=%d exceeds the maximum=%d", n_bits, max_numbits);
    }
    jmask = (1 << n_bits) - 1;
    ctx->rbits += n_bits;
    ctx->reg = (ctx->reg << n_bits) | (t & jmask);
    while (ctx->rbits >= 8) {
        *ctx->bitstream++ = (ctx->reg >> (ctx->rbits = ctx->rbits - 8)) & 255;
        ctx->n_bitstream++;
    }
    return;
}

// find min/max of an integer array
// return 0:  if min max found
// return 1:  if min max not found, min = max = 0
static int int_min_max_array(int* data, unsigned int n, int* min, int* max)
{
    unsigned int first;
    int mn, mx;

    if (n == 0) {
        return 1;
    }

    for (first = 0; first < n; first++) {
        if (data[first] != INT_MAX) {
            mx = mn = data[first];
            break;
        }
    }
    if (first >= n) return 1;

    mn = mx = data[first];

    {
        int max_val;
        int min_val;
        min_val = max_val = data[first];

        for (unsigned int i = first + 1; i < n; i++) {
            if (data[i] != INT_MAX) {
                min_val = (min_val > data[i]) ? data[i] : min_val;
                max_val = (max_val < data[i]) ? data[i] : max_val;
            }
        }

        {
            if (min_val < mn) mn = min_val;
            if (max_val > mx) mx = max_val;
        }
    }

    *min = mn;
    *max = mx;
    return 0;
}

static double Int_Power(double x, int y)
{
    double value;

    if (y < 0) {
        y = -y;
        x = 1.0 / x;
    }
    value = 1.0;

    while (y) {
        if (y & 1) {
            value *= x;
        }
        x = x * x;
        y >>= 1;
    }
    return value;
}

static int min_max_array(double* data, unsigned int n, double* min, double* max)
{
    unsigned int first;
    double mn, mx;

    if (n == 0) {
        *min = *max = 0.0;
        return GRIB_DECODING_ERROR;
    }

    for (first = 0; first < n; first++) {
        if (DEFINED_VAL(data[first])) break;
    }
    if (first >= n) {
        *min = *max = 0.0;
        return GRIB_DECODING_ERROR;
    }

    mn = mx = data[first];

    {
        double min_val;
        double max_val;
        min_val = max_val = data[first];

        for (unsigned int i = first + 1; i < n; i++) {
            if (DEFINED_VAL(data[i])) {
                min_val = (min_val > data[i]) ? data[i] : min_val;
                max_val = (max_val < data[i]) ? data[i] : max_val;
            }
        }

        {
            if (min_val < mn) mn = min_val;
            if (max_val > mx) mx = max_val;
        }
    }

    *min = mn;
    *max = mx;
    return GRIB_SUCCESS;
}

// static void uint_char(unsigned int i, unsigned char* p)
// {
//     p[0] = (i >> 24) & 255;
//     p[1] = (i >> 16) & 255;
//     p[2] = (i >> 8) & 255;
//     p[3] = (i)&255;
// }

// static unsigned char* mk_bms(grib_accessor* a, double* data, unsigned int* ndata)
// {
//     int bms_size;
//     unsigned char *bms, *cbits;
//     unsigned int nn, i, start, c, imask, i0;

//     nn = *ndata;

//     /* find first grid point with undefined data */
//     for (i = 0; i < nn; i++) {
//         if (UNDEFINED_VAL(data[i])) break;
//     }

//     if (i == nn) { /* all defined values, no need for bms */
//         bms = reinterpret_cast<unsigned char*>(grib_context_malloc(context_ , 6));
//         if (bms == NULL)
//             grib_context_log(context_ , GRIB_LOG_ERROR, "mk_bms: memory allocation problem", "");
//         uint_char(6, bms);  // length of section 6
//         bms[4] = 6;         // section 6
//         bms[5] = 255;       // no bitmap
//         return bms;
//     }

//     bms_size = 6 + (nn + 7) / 8;
//     bms      = reinterpret_cast<unsigned char*>(grib_context_malloc(context_ , bms_size));
//     if (bms == NULL)
//         grib_context_log(context_ , GRIB_LOG_ERROR, "mk_bms: memory allocation problem", "");

//     uint_char(bms_size, bms);  // length of section 6
//     bms[4] = 6;                // section 6
//     bms[5] = 0;                // has bitmap

//     /* bitmap is accessed by bytes, make i0=i/8 bytes of bitmap */
//     cbits = bms + 6;
//     i0    = i >> 3;  // Number of bytes, required to store the bitmap
//     for (i = 0; i < i0; i++) {
//         // Set all bits in the bitmap to 1
//         *cbits++ = 255;
//     }

//     /* start processing data, skip i0*8 */

//     c     = 0;        // counter: c += imask
//     imask = 128;      // 100.0000
//     i0    = i0 << 3;  // Number of bits in the bitmap
//     start = i0;
//     for (i = i0; i < nn; i++) {
//         if (DEFINED_VAL(data[i])) {
//             c += imask;
//             data[start++] = data[i];
//         }
//         if ((imask >>= 1) == 0) {
//             *cbits++ = c;
//             c        = 0;
//             imask    = 128;
//         }
//     }
//     if (imask != 128) *cbits = c;
//     *ndata = start;
//     return bms;
// }

static int post_process(grib_context* c, long* vals, long len, long order, long bias, const unsigned long extras[2])
{
    unsigned long last, penultimate = 0, j = 0;
    ECCODES_ASSERT(order > 0);
    ECCODES_ASSERT(order <= 3);
    if (!vals)
        return GRIB_INTERNAL_ERROR;

    if (order == 1) {
        last = extras[0];
        while (j < len) {
            if (vals[j] == LONG_MAX) {
                j++;
            }
            else {
                vals[j++] = extras[0];
                break;
            }
        }
        while (j < len) {
            if (vals[j] == LONG_MAX) {
                j++;
            }
            else {
                vals[j] += last + bias;
                last = vals[j++];
            }
        }
    }
    else if (order == 2) {
        penultimate = extras[0];
        last        = extras[1];
        while (j < len) {
            if (vals[j] == LONG_MAX) {
                j++;
            }
            else {
                vals[j++] = extras[0];
                break;
            }
        }
        while (j < len) {
            if (vals[j] == LONG_MAX) {
                j++;
            }
            else {
                vals[j++] = extras[1];
                break;
            }
        }
        for (; j < len; j++) {
            if (vals[j] != LONG_MAX) {
                vals[j]     = vals[j] + bias + last + last - penultimate;
                penultimate = last;
                last        = vals[j];
            }
        }
    }
    return GRIB_SUCCESS;
}

static int find_nbits(unsigned int i)
{
#if !defined __GNUC__ || __GNUC__ < 4
    int j;
    j = 0;

    while (i > 65535) {
        i = i >> 16;
        j += 16;
    }
    // i = 16 bits
    if (i > 255) {
        i = i >> 8;
        j += 8;
    }
    // i = 8 bits
    if (i > 15) {
        i = i >> 4;
        j += 4;
    }
    // i = 4 bits
    if (i > 3) {
        i = i >> 2;
        j += 2;
    }
    // i = 2 bits
    return (i >= 2) ? j + 2 : j + i;
#else
    return (i == 0) ? 0 : 8 * sizeof(unsigned int) - __builtin_clz(i);
#endif
}

struct section
{
    int mn, mx, missing;  // stats
    int i0, i1;           // pointers to data[]
    struct section *head, *tail;
};

static int sizeofsection(struct section* s, int ref_bits, int width_bits, int has_undef)
{
    if (s->mn == INT_MAX) {  // all undef
        return ref_bits + width_bits;
    }

    if (s->mn == s->mx) {
        if (s->missing == 0) {  // constant and no missings
            return ref_bits + width_bits;
        }
        else {  // constant and missing
            return (s->i1 - s->i0 + 1) * has_undef + ref_bits + width_bits;
        }
    }

    return find_nbits(s->mx - s->mn + has_undef) * (s->i1 - s->i0 + 1) + ref_bits + width_bits;
}

static int sizeofsection2(int mn, int mx, int n, int ref_bits, int width_bits,
                          int has_undef_sec, int has_undef)
{
    if (mn == INT_MAX) return ref_bits + width_bits;
    if (mn == mx) {
        if (has_undef_sec == 0) return ref_bits + width_bits;
        return n * has_undef + ref_bits + width_bits;
    }
    return find_nbits(mx - mn + has_undef) * n + ref_bits + width_bits;
}

static int size_all(struct section* s, int ref_bits, int width_bits, int has_undef)
{
    int bits;

    bits = 0;
    while (s) {
        bits += sizeofsection(s, ref_bits, width_bits, has_undef);
        s = s->tail;
    }
    return (bits + 7) / 8;
}

static void move_one_left(struct section* s, int* v)
{
    struct section* t;
    int val, i, j, k;

    t = s->tail;
    s->i1 += 1;
    t->i0 += 1;
    val = v[s->i1];

    // update s statistics
    if (val == INT_MAX)
        s->missing = 1;
    else {
        s->mx = s->mx > val ? s->mx : val;
        s->mn = s->mn < val ? s->mn : val;
    }

    // remove t?
    if (t->i0 > t->i1) {
        s->tail = t->tail;
        t       = s->tail;
        if (t) t->head = s;
        return;
    }

    // update s statistics
    if (val == INT_MAX) {
        for (i = t->i0; i <= t->i1; i++) {
            if (v[i] == INT_MAX) return;
        }
        t->missing = 0;
        return;
    }
    if (val == t->mx) {
        k = INT_MAX;
        for (j = 0, i = t->i0; i <= t->i1; i++) {
            if (v[i] != INT_MAX) {
                if (j == 0) {
                    k = v[i];
                    j++;
                }
                else
                    k = k < v[i] ? v[i] : k;
            }
        }
        t->mx = k;
        return;
    }
    if (val == t->mn) {
        k = INT_MAX;
        for (j = 0, i = t->i0; i <= t->i1; i++) {
            if (v[i] != INT_MAX) {
                if (j == 0) {
                    k = v[i];
                    j++;
                }
                else
                    k = k > v[i] ? v[i] : k;
            }
        }
        t->mn = k;
        return;
    }
}

static void move_one_right(struct section* s, int* v)
{
    struct section* t;
    int val, i, j, k;

    t = s->tail;
    s->i1 -= 1;
    t->i0 -= 1;
    val = v[t->i0];

    // update t statistics
    if (val == INT_MAX)
        t->missing = 1;
    else {
        t->mx = t->mx > val ? t->mx : val;
        t->mn = t->mn < val ? t->mn : val;
    }

    // if s is empty, copy t to s and recalculate
    if (s->i0 > s->i1) {
        s->i0   = t->i0;
        s->i1   = t->i1;
        s->tail = t->tail;

        s->mx = s->mn = INT_MAX;
        j = s->missing = 0;
        for (i = s->i0; i <= s->i1; i++) {
            if (v[i] == INT_MAX)
                s->missing = 1;
            else if (j == 0) {
                s->mx = s->mn = v[i];
                j++;
            }
            else {
                s->mx = s->mx > v[i] ? s->mx : v[i];
                s->mn = s->mn < v[i] ? s->mx : v[i];
            }
        }
        return;
    }

    // update s statistics
    if (val == INT_MAX) {
        for (i = s->i0; i <= s->i1; i++) {
            if (v[i] == INT_MAX) return;
        }
        s->missing = 0;
        return;
    }
    if (val == s->mx) {
        k = INT_MAX;
        for (j = 0, i = s->i0; i <= s->i1; i++) {
            if (v[i] != INT_MAX) {
                if (j == 0) {
                    k = v[i];
                    j++;
                }
                else
                    k = k < v[i] ? v[i] : k;
            }
        }
        s->mx = k;
        return;
    }
    if (val == s->mn) {
        k = INT_MAX;
        for (j = 0, i = s->i0; i <= s->i1; i++) {
            if (v[i] != INT_MAX) {
                if (j == 0) {
                    k = v[i];
                    j++;
                }
                else
                    k = k > v[i] ? v[i] : k;
            }
        }
        s->mn = k;
        return;
    }
    return;
}

static void exchange(struct section* s, int* v, int has_undef, int LEN_SEC_MAX)
{
    struct section* t;
    int val0, val1, nbit_s, nbit_t;

    if (s == NULL) return;
    while ((t = s->tail) != NULL) {
        // nbit_s = find_nbits(s->mx - s->mn + has_undef);
        // nbit_t = find_nbits(t->mx - t->mn + has_undef);

        if (s->mn == INT_MAX)
            nbit_s = 0;
        else if (s->mn == s->mx)
            nbit_s = s->missing;
        else
            nbit_s = find_nbits(s->mx - s->mn + has_undef);

        if (t->mn == INT_MAX)
            nbit_t = 0;
        else if (t->mn == t->mx)
            nbit_t = t->missing;
        else
            nbit_t = find_nbits(t->mx - t->mn + has_undef);

        if (nbit_s == nbit_t) {
            s = t;
            continue;
        }

        val0 = v[s->i1];
        val1 = v[t->i0];

        if (s->missing == 1 || t->missing == 1) {
            s = t;
            continue;
        }
        // if (val0 == INT_MAX || val1 == INT_MAX) { s=t; continue; }

        if (nbit_s < nbit_t && val1 == INT_MAX) {
            if ((s->i1 - s->i0) < LEN_SEC_MAX && s->mx != s->mn) {
                move_one_left(s, v);
            }
            else {
                s = t;
            }
            continue;
        }

        if (nbit_s > nbit_t && val0 == INT_MAX) {
            if ((t->i1 - t->i0) < LEN_SEC_MAX && t->mn != t->mx) {
                move_one_right(s, v);
            }
            else {
                s = t;
            }
            continue;
        }

        // if (s->missing == 1 || t->missing == 1) { s=t; continue; }
        // 3/2014   val0 = v[s->i1];
        // 3/2014   val1 = v[t->i0];

        if (nbit_s < nbit_t && (s->i1 - s->i0) < LEN_SEC_MAX && val1 >= s->mn &&
            val1 <= s->mx) {
            move_one_left(s, v);
        }
        else if (nbit_s > nbit_t && (t->i1 - t->i0) < LEN_SEC_MAX &&
                 val0 >= t->mn && val0 <= t->mx) {
            move_one_right(s, v);
        }
        else {
            s = s->tail;
        }
    }
}

static void merge_j(struct section* h, int ref_bits, int width_bits, int has_undef, int param, int LEN_SEC_MAX)
{
    struct section *t, *m;
    int size_head, size_mid, size_tail, saving_mt, saving_hm;
    int min0, max0, min1, max1;

    size_head = size_mid = size_tail = 0;

    while (h && (m = h->tail)) {
        t = m->tail;

        // h -> m -> t

        // find savings of merged h - m
        saving_hm = -1;
        min0 = max0 = min1 = max1 = 0;  // turn off error warnings
        if (m->i1 - h->i0 < LEN_SEC_MAX) {
            if (m->mn == INT_MAX) {
                max0 = h->mx;
                min0 = h->mn;
            }
            else if (h->mn == INT_MAX) {
                max0 = m->mx;
                min0 = m->mn;
            }
            else {
                min0 = h->mn < m->mn ? h->mn : m->mn;
                max0 = h->mx > m->mx ? h->mx : m->mx;
            }
            if (max0 - min0 <= param) {
                if (size_head == 0)
                    size_head =
                        sizeofsection(h, ref_bits, width_bits, has_undef);
                if (size_mid == 0)
                    size_mid =
                        sizeofsection(m, ref_bits, width_bits, has_undef);
                saving_hm = size_head + size_mid -
                            sizeofsection2(min0, max0, m->i1 - h->i0 + 1,
                                           ref_bits, width_bits,
                                           h->missing || m->missing, has_undef);
            }
        }

        // find savings of merged m-t
        saving_mt = -1;
        if (t && t->i1 - m->i0 < LEN_SEC_MAX) {
            if (m->mn == INT_MAX) {
                max1 = t->mx;
                min1 = t->mn;
            }
            else if (t->mn == INT_MAX) {
                max1 = m->mx;
                min1 = m->mn;
            }
            else {
                min1 = m->mn < t->mn ? m->mn : t->mn;
                max1 = m->mx > t->mx ? m->mx : t->mx;
            }
            if (max1 - min1 <= param) {
                if (size_mid == 0)
                    size_mid =
                        sizeofsection(m, ref_bits, width_bits, has_undef);
                if (size_tail == 0)
                    size_tail =
                        sizeofsection(t, ref_bits, width_bits, has_undef);
                saving_mt = size_mid + size_tail -
                            sizeofsection2(min1, max1, t->i1 - m->i0 + 1,
                                           ref_bits, width_bits,
                                           m->missing || t->missing, has_undef);
            }
        }

        if (saving_hm >= saving_mt && saving_hm >= 0) {
            // merge h and m
            h->i1      = m->i1;
            h->tail    = m->tail;
            h->mn      = min0;
            h->mx      = max0;
            h->missing = h->missing || m->missing;
            m          = h->tail;
            if (m) m->head = h;
            if (h->head) h = h->head;
            size_head = size_mid = size_tail = 0;
        }
        else if (saving_mt >= saving_hm && saving_mt >= 0) {
            // merge m and t
            m->i1      = t->i1;
            m->tail    = t->tail;
            m->mn      = min1;
            m->mx      = max1;
            m->missing = m->missing || t->missing;
            t          = m->tail;
            if (t) t->head = m;
            size_head = size_mid = size_tail = 0;
        }
        else {
            // no merging
            h         = h->tail;
            size_head = size_mid;
            size_mid  = size_tail;
            size_tail = 0;
        }
    }
}

int grib_accessor_data_g22order_packing_t::pack_double(const double* val, size_t* len)
{
    grib_handle* gh = grib_handle_of_accessor(this);

    int err = 0;

    // double reference_value = 0;
    // long nvals_per_group     = 0;
    // long nbits_per_group_val = 0;

    long binary_scale_factor, decimal_scale_factor, typeOfOriginalFieldValues, optimize_scale_factor;
    // long groupSplittingMethodUsed, numberOfGroupsOfDataValues, referenceForGroupWidths;
    long missingValueManagementUsed, primaryMissingValueSubstitute, secondaryMissingValueSubstitute;
    long numberOfBitsUsedForTheGroupWidths, numberOfBitsUsedForTheScaledGroupLengths, orderOfSpatialDifferencing;
    long numberOfOctetsExtraDescriptors, bits_per_value = 0, bitmap_present = 0;

    int dec_scale, bin_scale, wanted_bits, max_bits, use_bitmap,
        j, j0, k, *v, binary_scale, nbits, has_undef, extra_0, extra_1, vmn, vmx, vbits;
    // Sections
    double max_val, min_val, ref, frange, dec_factor, scale, mn, mx;
    struct section start, *list, *list_backup, *s;
    // Group
    int ngroups, grefmx, glenmn, glenmx, gwidmn, gwidmx, len_last, size_sec7;
    int *refs, *lens, *widths, *itmp, *itmp2;
    int est_group_width = 6;

    size_t ndef = 0, nndata = 0, nstruct, i, ii;

    int LEN_SEC_MAX = 127;
    int LEN_BITS    = 7;

    if (*len == 0)
        return GRIB_NO_VALUES;

    if ((err = grib_get_long_internal(gh, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;

    // ECC-1968: For bits_per_value > 23, the complex packing algorithm may use more than 25 bits per value.
    // However, this exceeds the maximum number of bits for the packing algorithm.
    if (bits_per_value > 23) {
        bits_per_value = 23;
    }

    if ((err = grib_get_long_internal(gh, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, optimize_scale_factor_, &optimize_scale_factor)) != GRIB_SUCCESS)
        return err;

    int use_scale = !optimize_scale_factor;

    if ((err = grib_get_long_internal(gh, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, typeOfOriginalFieldValues_, &typeOfOriginalFieldValues)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, missingValueManagementUsed_, &missingValueManagementUsed)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, primaryMissingValueSubstitute_, &primaryMissingValueSubstitute)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, secondaryMissingValueSubstitute_, &secondaryMissingValueSubstitute)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, numberOfBitsUsedForTheGroupWidths_, &numberOfBitsUsedForTheGroupWidths)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, numberOfBitsUsedForTheScaledGroupLengths_, &numberOfBitsUsedForTheScaledGroupLengths)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, orderOfSpatialDifferencing_, &orderOfSpatialDifferencing)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, numberOfOctetsExtraDescriptors_, &numberOfOctetsExtraDescriptors)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, "bitmapPresent", &bitmap_present)) != GRIB_SUCCESS)
        return err;

    max_bits = bits_per_value;  // TODO(masn)

    // Note:
    // orderOfSpatialDifferencing = 0 means "grid_complex"
    // orderOfSpatialDifferencing = 1 means "grid_complex_spatial_differencing" with orderOfSpatialDifferencing=1
    // orderOfSpatialDifferencing = 2 means "grid_complex_spatial_differencing" with orderOfSpatialDifferencing=2

    use_bitmap  = bitmap_present;
    wanted_bits = bits_per_value;

    for (i = 0; i < *len; i++) {
        if (DEFINED_VAL(val[i])) {
            ndef = ndef + 1;
        }
    }

    if (ndef == 0) {  // Special case: All undefined values
        // Section 5
        const char* packing_type = "grid_complex";
        size_t packing_type_len  = strlen(packing_type);
        grib_set_string_internal(gh, "packingType", packing_type, &packing_type_len);

        if ((err = grib_set_double_internal(gh, reference_value_, grib_ieee_to_long(0.0))) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, binary_scale_factor_, 0)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, decimal_scale_factor_, 0)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, bits_per_value_, 8)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, typeOfOriginalFieldValues_, 0)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, groupSplittingMethodUsed_, 1)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, missingValueManagementUsed_, 1)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, primaryMissingValueSubstitute_, grib_ieee_to_long(static_cast<float>(9.999e20)))) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, secondaryMissingValueSubstitute_, 0xFFFFFFFF)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, numberOfGroupsOfDataValues_, 1)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, referenceForGroupWidths_, grib_ieee_to_long(0.0))) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, numberOfBitsUsedForTheGroupWidths_, 8)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, referenceForGroupLengths_, *len)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, lengthIncrementForTheGroupLengths_, 1)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, trueLengthOfLastGroup_, *len)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, numberOfBitsUsedForTheScaledGroupLengths_, 8)) != GRIB_SUCCESS)
            return err;

        // Section 6
        if ((err = grib_set_long_internal(gh, "bitmapPresent", 0)) != GRIB_SUCCESS) return err;

        // Section 7
        constexpr size_t sec7_size          = 3;
        unsigned char empty_sec7[sec7_size] = { 255, 0, 0 };  // group reference, group width, group length
        grib_buffer_replace(this, empty_sec7, sec7_size, 1, 1);
        return GRIB_SUCCESS;
    }

    size_t ndata = *len;
    double* data = reinterpret_cast<double*>(grib_context_malloc_clear(context_, ndata * sizeof(double)));
    if (data == NULL) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s packing: unable to allocate %zu bytes", class_name_, ndata * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }
    memcpy(data, val, sizeof(*data) * ndata);

    dec_scale = -decimal_scale_factor;
    bin_scale = binary_scale_factor;

    // compute bitmap section
    // if (use_bitmap == 0 || ndef == ndata) {
    //     if ((err = grib_set_long_internal(gh, "bitmapPresent", 0)) != GRIB_SUCCESS) return err;
    // } else {
    //     if ((err = grib_set_long_internal(gh, "bitmapPresent", 1)) != GRIB_SUCCESS) return err;
    // }

    nndata    = use_bitmap ? ndef : ndata;
    has_undef = use_bitmap ? 0 : ndata != ndef;

    v = reinterpret_cast<int*>(grib_context_malloc(context_, nndata * sizeof(int)));
    if (v == NULL) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s packing: unable to allocate %zu bytes", class_name_, nndata * sizeof(int));
        return GRIB_OUT_OF_MEMORY;
    }
    if (min_max_array(data, ndata, &mn, &mx) != GRIB_SUCCESS) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s packing: failed to get min max of data", class_name_);
        return GRIB_ENCODING_ERROR;
    }
    min_val = mn;
    max_val = mx;

    binary_scale = bin_scale;

    if (use_scale) {  // ECMWF style
        ref       = min_val;
        frange    = max_val - ref;
        dec_scale = 0;
        if (frange != 0.0) {
            frexp(frange, &j);               // Required bits
            binary_scale = j - wanted_bits;  // Scale factor
            nbits        = wanted_bits;
            scale        = ldexp(1.0, -binary_scale);
            frange       = floor((max_val - ref) * scale + 0.5);
            frexp(frange, &j);
            if (j != nbits) binary_scale++;
        }
        else {
            binary_scale = nbits = 0;
            scale                = 1;
        }
    }
    else {
        if (dec_scale) {
            dec_factor = Int_Power(10.0, -dec_scale);
            min_val *= dec_factor;
            max_val *= dec_factor;
            if (has_undef) {
                for (i = 0; i < nndata; i++) {
                    if (DEFINED_VAL(data[i])) data[i] *= dec_factor;
                }
            }
            else {
                for (i = 0; i < nndata; i++) {
                    data[i] *= dec_factor;
                }
            }
        }
        scale = ldexp(1.0, -binary_scale);
        // ref = floor(min_val*scale)/scale;
        ref    = min_val;
        frange = floor((max_val - ref) * scale + 0.5);
        frexp(frange, &nbits);
        if (nbits > max_bits) {
            binary_scale += (nbits - max_bits);
            nbits = max_bits;
        }
    }

    if (binary_scale) {
        scale = ldexp(1.0, -binary_scale);
        if (has_undef) {
            for (i = 0; i < nndata; i++) {
                if (DEFINED_VAL(data[i])) {
                    v[i] = floor((data[i] - ref) * scale + 0.5);
                    v[i] = v[i] >= 0 ? v[i] : 0;
                }
                else
                    v[i] = INT_MAX;
            }
        }
        else {
            for (i = 0; i < nndata; i++) {
                v[i] = floor((data[i] - ref) * scale + 0.5);
                v[i] = v[i] >= 0 ? v[i] : 0;
            }
        }
    }
    else {
        // scale = 1.0;
        if (has_undef) {
            for (i = 0; i < nndata; i++) {
                if (DEFINED_VAL(data[i])) {
                    v[i] = floor(data[i] - ref + 0.5);
                    v[i] = v[i] >= 0 ? v[i] : 0;
                }
                else
                    v[i] = INT_MAX;
            }
        }
        else {
            for (i = 0; i < nndata; i++) {
                v[i] = floor(data[i] - ref + 0.5);
                v[i] = v[i] >= 0 ? v[i] : 0;
            }
        }
    }

    vmx = vmn = 0;
    extra_0 = extra_1 = 0;  // turn off warnings

    if (orderOfSpatialDifferencing == 2) {
        // delta_delta(v, nndata, &vmn, &vmx, &extra_0, &extra_1);
        // single core version
        {
            int last, last0, penultimate = 0;
            for (i = 0; i < nndata; i++) {
                if (v[i] != INT_MAX) {
                    extra_0 = penultimate = v[i];
                    v[i++]                = 0;
                    break;
                }
            }
            for (; i < nndata; i++) {
                if (v[i] != INT_MAX) {
                    extra_1 = last = v[i];
                    v[i++]         = 0;
                    break;
                }
            }

            for (; i < nndata; i++) {
                if (v[i] != INT_MAX) {
                    last0       = v[i];
                    v[i]        = v[i] - 2 * last + penultimate;
                    penultimate = last;
                    last        = last0;
                    vmn         = vmn > v[i] ? v[i] : vmn;
                    vmx         = vmx < v[i] ? v[i] : vmx;
                }
            }
        }
    }
    else if (orderOfSpatialDifferencing == 1) {
        // delta(v, nndata, &vmn, &vmx, &extra_0);
        // single core version
        {
            int last, last0;

            for (i = 0; i < nndata; i++) {
                if (v[i] != INT_MAX) {
                    extra_0 = last = v[i];
                    v[i++]         = 0;
                    break;
                }
            }
            for (; i < nndata; i++) {
                if (v[i] != INT_MAX) {
                    last0 = v[i];
                    v[i]  = v[i] - last;
                    last  = last0;
                    vmn   = vmn > v[i] ? v[i] : vmn;
                    vmx   = vmx < v[i] ? v[i] : vmx;
                }
            }
        }
    }
    else if (orderOfSpatialDifferencing == 0) {
        // find min/max
        int_min_max_array(v, nndata, &vmn, &vmx);
    }

#ifdef DEBUG
    grib_context_log(context_, GRIB_LOG_DEBUG, "COMPLEX: 2: vmx %d vmn %d nbits %d", vmx, vmn,
                     find_nbits(vmx - vmn + has_undef));
#endif

    for (i = 0; i < nndata; i++) {
        v[i] = (v[i] != INT_MAX) ? v[i] - vmn : INT_MAX;
    }
    vmx   = vmx - vmn;
    vbits = find_nbits(vmx + has_undef);

    // size of merged struct
    ii      = 0;
    nstruct = 1;
    for (i = 1; i < nndata; i++) {
        if (((i - ii + 1) > LEN_SEC_MAX) || (v[i] != v[ii])) {
            nstruct++;
            ii = i;
        }
    }

    list = reinterpret_cast<section*>(grib_context_malloc_clear(context_, nstruct * sizeof(section)));
    if (list == NULL) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s packing: memory allocation of list failed", class_name_);
        return GRIB_OUT_OF_MEMORY;
    }

    // initialize linked list

    // The first element in the list is initialized with the first element from v
    ii         = 0;  // section counter
    list[0].mn = list[0].mx = v[0];
    list[0].missing         = (v[0] == INT_MAX);
    list[0].i0 = list[0].i1 = 0;
    for (i = 1; i < nndata; i++) {
        // join last section
        // where all values in the section are the same
        if ((i - list[ii].i0 < LEN_SEC_MAX) && (v[i] == list[ii].mn)) {
            list[ii].i1 = i;
        }
        // make new section
        else {
            ii++;
            list[ii].mn = list[ii].mx = v[i];
            list[ii].missing          = (v[i] == INT_MAX);
            list[ii].i0 = list[ii].i1 = i;
        }
    }
    list[0].head  = NULL;
    list[ii].tail = NULL;
    start.tail    = &list[0];

    if (nstruct != ii + 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s packing: nstruct=%zu wanted %zu", class_name_, nstruct, ii + 1);
        return GRIB_ENCODING_ERROR;
    }
    for (i = 1; i < nstruct; i++) {
        list[i].head     = &list[i - 1];
        list[i - 1].tail = &list[i];
    }

    // sequence : has_undef == 0 :   2**n - 1       1, 3, 7, ..
    // sequence : has_undef == 1 :   2**n - 2       0, 2, 6

    k = has_undef ? 2 : 1;

    while (k < vmx / 2) {
        merge_j(start.tail, vbits, LEN_BITS + est_group_width, has_undef, k,
                LEN_SEC_MAX);
#ifdef DEBUG
        j = size_all(start.tail, vbits, LEN_BITS + est_group_width, has_undef);
        grib_context_log(context_, GRIB_LOG_DEBUG, "COMPLEX: complex start %d %d bytes", k, j);
#endif
        k = 2 * k + 1 + has_undef;
    }

    //  try making segment sizes larger
    //  12/2015 need to segment size less 25 bits, bitstream software limitation

    list_backup = reinterpret_cast<section*>(grib_context_malloc(context_, nstruct * sizeof(section)));
    if (list_backup == NULL) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s packing: memory allocation of list_backup failed", class_name_);
        return GRIB_OUT_OF_MEMORY;
    }

    j  = size_all(start.tail, vbits, LEN_BITS + est_group_width, has_undef);
    j0 = j + 1;
#ifdef DEBUG
    grib_context_log(context_, GRIB_LOG_DEBUG, "COMPLEX: complex start inc segments size0 %d segsize %d", j, LEN_SEC_MAX);
#endif
    while (j < j0 && LEN_BITS < 25) {
        j0 = j;
        LEN_BITS++;
        LEN_SEC_MAX = LEN_SEC_MAX + LEN_SEC_MAX + 1;
        memcpy(list_backup, list, nstruct * sizeof(struct section));
        merge_j(start.tail, vbits, LEN_BITS + est_group_width, has_undef, k,
                LEN_SEC_MAX);
        j = size_all(start.tail, vbits, LEN_BITS + est_group_width, has_undef);
#ifdef DEBUG
        grib_context_log(context_, GRIB_LOG_DEBUG,
                         "COMPLEX: complex inc segments size size0 %d size1 %d segsize %d "
                         "LEN_BITS=%d",
                         j0, j, LEN_SEC_MAX, LEN_BITS);
#endif
        if (j > j0) {
            memcpy(list, list_backup, nstruct * sizeof(struct section));
            LEN_BITS--;
            LEN_SEC_MAX = (LEN_SEC_MAX - 1) / 2;
        }
    }
    grib_context_free(context_, list_backup);

    exchange(start.tail, v, has_undef, LEN_SEC_MAX);
#ifdef DEBUG
    j = size_all(start.tail, vbits, LEN_BITS + est_group_width, has_undef);
    grib_context_log(context_, GRIB_LOG_DEBUG, "COMPLEX: exchange  %d bytes", j);
#endif

    merge_j(start.tail, vbits, LEN_BITS + est_group_width, has_undef, vmx,
            LEN_SEC_MAX);
#ifdef DEBUG
    j = size_all(start.tail, vbits, LEN_BITS + est_group_width, has_undef);
    grib_context_log(context_, GRIB_LOG_DEBUG, "COMPLEX: complex start %d %d bytes", vmx, j);
#endif

    // finished making segments
    // find out number of bytes for extra info (orderOfSpatialDifferencing 2/3)

    if (orderOfSpatialDifferencing != 0) {  // packing modes 2/3
        k = vmn >= 0 ? find_nbits(vmn) + 1 : find_nbits(-vmn) + 1;
        // + 1 work around for NCEP bug
        j = find_nbits(extra_0) + 1;
        if (j > k) k = j;

        if (orderOfSpatialDifferencing == 2) {
            // + 1 work around for NCEP bug
            j = find_nbits(extra_1) + 1;
            if (j > k) k = j;
        }
        numberOfOctetsExtraDescriptors = (k + 7) / 8;  // number of bytes for extra and vmn
    }

    // scale the linked list
    s = start.tail;
    if (s == NULL) {
        return GRIB_INTERNAL_ERROR;
    }
    ngroups = 0;  // number of groups

    while (s) {
        ngroups++;
        s = s->tail;
    }

    lens   = reinterpret_cast<int*>(grib_context_malloc(context_, ngroups * sizeof(int)));
    widths = reinterpret_cast<int*>(grib_context_malloc(context_, ngroups * sizeof(int)));
    refs   = reinterpret_cast<int*>(grib_context_malloc(context_, ngroups * sizeof(int)));
    itmp   = reinterpret_cast<int*>(grib_context_malloc(context_, ngroups * sizeof(int)));
    itmp2  = reinterpret_cast<int*>(grib_context_malloc(context_, ngroups * sizeof(int)));

    if (lens == NULL || widths == NULL || refs == NULL || itmp == NULL || itmp2 == NULL) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s packing: memory alloc of lens/widths/refs/itmp/itmp2 failed", class_name_);
        return GRIB_OUT_OF_MEMORY;
    }

    // make vectors so we can OpenMP the loop
    for (i = ii = 0, s = start.tail; ii < ngroups; ii++, s = s->tail) {
        lens[ii] = s->i1 - s->i0 + 1;
        i += lens[ii];
        refs[ii]  = s->mn;
        itmp[ii]  = s->mx;
        itmp2[ii] = s->missing;
    }
    if (i != nndata)
        return GRIB_INTERNAL_ERROR;

    for (i = 0; i < ngroups; i++) {
        if (refs[i] == INT_MAX)
            widths[i] = 0;
        else if (refs[i] == itmp[i])
            widths[i] = itmp2[i];
        else
            widths[i] = find_nbits(itmp[i] - refs[i] + has_undef);
    }

    // group lengths
    len_last = lens[ngroups - 1];  // length of last segment

    glenmn = glenmx = lens[0];
    gwidmx = gwidmn = widths[0];
    grefmx          = refs[0] != INT_MAX ? refs[0] : 0;

    {
        int glenmn_thread, glenmx_thread, gwidmx_thread, gwidmn_thread,
            grefmx_thread;
        glenmn_thread = glenmx_thread = lens[0];
        gwidmn_thread = gwidmx_thread = widths[0];
        grefmx_thread                 = refs[0] != INT_MAX ? refs[0] : 0;

        for (i = 1; i < ngroups; i++) {
            glenmx_thread = glenmx_thread >= lens[i] ? glenmx_thread : lens[i];
            glenmn_thread = glenmn_thread <= lens[i] ? glenmn_thread : lens[i];
            gwidmx_thread =
                gwidmx_thread >= widths[i] ? gwidmx_thread : widths[i];
            gwidmn_thread =
                gwidmn_thread <= widths[i] ? gwidmn_thread : widths[i];
            if (refs[i] != INT_MAX && refs[i] > grefmx_thread)
                grefmx_thread = refs[i];
        }
        {
            glenmx = glenmx >= glenmx_thread ? glenmx : glenmx_thread;
            glenmn = glenmn <= glenmn_thread ? glenmn : glenmn_thread;
            gwidmx = gwidmx >= gwidmx_thread ? gwidmx : gwidmx_thread;
            gwidmn = gwidmn <= gwidmn_thread ? gwidmn : gwidmn_thread;
            grefmx = grefmx >= grefmx_thread ? grefmx : grefmx_thread;
        }
    }

    bits_per_value                    = find_nbits(grefmx + has_undef);
    numberOfBitsUsedForTheGroupWidths = find_nbits(gwidmx - gwidmn + has_undef);

    if ((err = grib_set_long_internal(gh, bits_per_value_, bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_double_internal(gh, reference_value_, static_cast<double>(ref))) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, binary_scale_factor_, binary_scale)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, decimal_scale_factor_, -dec_scale)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, typeOfOriginalFieldValues_, 0)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, groupSplittingMethodUsed_, 1)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, missingValueManagementUsed_, has_undef)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, primaryMissingValueSubstitute_, grib_ieee_to_long(static_cast<float>(9.999e20)))) != GRIB_SUCCESS)
        return err;
    // if ((err = grib_set_long_internal(gh, secondaryMissingValueSubstitute_ , 0xFFFFFFFF)) != GRIB_SUCCESS) return err;

    if ((err = grib_set_long_internal(gh, numberOfGroupsOfDataValues_, ngroups)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, referenceForGroupWidths_, gwidmn)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, numberOfBitsUsedForTheGroupWidths_, find_nbits(gwidmx - gwidmn + has_undef))) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, referenceForGroupLengths_, glenmn)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, lengthIncrementForTheGroupLengths_, 1)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, trueLengthOfLastGroup_, len_last)) != GRIB_SUCCESS)
        return err;
    numberOfBitsUsedForTheScaledGroupLengths = find_nbits(glenmx - glenmn);
    if ((err = grib_set_long_internal(gh, numberOfBitsUsedForTheScaledGroupLengths_, numberOfBitsUsedForTheScaledGroupLengths)) != GRIB_SUCCESS)
        return err;

    size_sec7 = 5;

    if (orderOfSpatialDifferencing == 1) {
        size_sec7 += 2 * numberOfOctetsExtraDescriptors;
    }
    else if (orderOfSpatialDifferencing == 2) {
        size_sec7 += 3 * numberOfOctetsExtraDescriptors;
    }
    if (orderOfSpatialDifferencing > 0) {
        if ((err = grib_set_long_internal(gh, orderOfSpatialDifferencing_, orderOfSpatialDifferencing)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(gh, numberOfOctetsExtraDescriptors_, numberOfOctetsExtraDescriptors)) != GRIB_SUCCESS)
            return err;
    }

    // group reference value
    size_sec7 += (ngroups * bits_per_value + 7) / 8;

    // group widths
    size_sec7 += (ngroups * numberOfBitsUsedForTheGroupWidths + 7) / 8;

    // group lengths
    size_sec7 += (ngroups * numberOfBitsUsedForTheScaledGroupLengths + 7) / 8;

    k = 0;
    {
        j = 0;
        for (i = 0; i < ngroups; i++) {
            j += lens[i] * widths[i];
            size_sec7 += (j >> 3);
            j        = (j & 7);
            refs[i]  = (refs[i] != INT_MAX) ? refs[i] : ONES;
            itmp[i]  = widths[i] - gwidmn;
            itmp2[i] = lens[i] - glenmn;
        }
        k += j;
    }
    size_sec7 += (k >> 3) + ((k & 7) ? 1 : 0);

    unsigned char* sec7 = reinterpret_cast<unsigned char*>(grib_context_malloc(context_, size_sec7));
    if (sec7 == NULL) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s: Unable to allocate %d bytes", class_name_, size_sec7);
        return GRIB_OUT_OF_MEMORY;
    }

    // pack the values into a bitstream

    bitstream_context ctx;
    init_bitstream(&ctx, sec7);
    add_bitstream(&ctx, this, size_sec7 >> 16, 16);
    add_bitstream(&ctx, this, size_sec7, 16);
    add_bitstream(&ctx, this, 7, 8);

    // write extra octets
    if (orderOfSpatialDifferencing == 1 || orderOfSpatialDifferencing == 2) {
        add_bitstream(&ctx, this, extra_0, 8 * numberOfOctetsExtraDescriptors);
        if (orderOfSpatialDifferencing == 2) add_bitstream(&ctx, this, extra_1, 8 * numberOfOctetsExtraDescriptors);
        k = vmn;
        if (k < 0) {
            k = -vmn | (1 << (8 * numberOfOctetsExtraDescriptors - 1));
        }
        add_bitstream(&ctx, this, k, 8 * numberOfOctetsExtraDescriptors);
        finish_bitstream(&ctx);
    }

    // write the group reference values
    add_many_bitstream(&ctx, this, refs, ngroups, bits_per_value);
    finish_bitstream(&ctx);

    // write the group widths
    add_many_bitstream(&ctx, this, itmp, ngroups, numberOfBitsUsedForTheGroupWidths);
    finish_bitstream(&ctx);

    // write the group lengths
    add_many_bitstream(&ctx, this, itmp2, ngroups, numberOfBitsUsedForTheScaledGroupLengths);
    finish_bitstream(&ctx);

    s = start.tail;
    for (i = 0; i < ngroups; i++, s = s->tail) {
        itmp[i] = s->i0;
        refs[i] = s->mn;
    }

    for (i = 0; i < ngroups; i++) {
        if (widths[i]) {
            for (j = 0; j < lens[i]; j++) {
                v[j + itmp[i]] = (v[j + itmp[i]] == INT_MAX)
                                     ? ONES
                                     : v[j + itmp[i]] - refs[i];
            }
        }
    }
    for (i = 0; i < ngroups; i++) {
        if (widths[i]) {
            add_many_bitstream(&ctx, this, v + itmp[i], lens[i], widths[i]);
        }
    }

    finish_bitstream(&ctx);

    grib_buffer_replace(this, sec7 + 5, size_sec7 - 5, 1, 1);

    grib_context_free(context_, sec7);
    grib_context_free(context_, list);
    grib_context_free(context_, v);
    grib_context_free(context_, lens);
    grib_context_free(context_, widths);
    grib_context_free(context_, refs);
    grib_context_free(context_, itmp);
    grib_context_free(context_, itmp2);
    grib_context_free(context_, data);

    // ECC-259: Set correct number of values
    if ((err = grib_set_long_internal(gh, numberOfValues_, *len)) != GRIB_SUCCESS)
        return err;

    return GRIB_SUCCESS;
}

template <typename T>
int grib_accessor_data_g22order_packing_t::unpack(T* val, size_t* len)
{
    static_assert(std::is_floating_point<T>::value, "Requires floating points numbers");
    grib_handle* gh = grib_handle_of_accessor(this);

    size_t i                  = 0;
    size_t j                  = 0;
    long n_vals               = 0;
    long vcount               = 0;
    int err                   = GRIB_SUCCESS;
    long* sec_val             = NULL;
    unsigned char* buf        = reinterpret_cast<unsigned char*>(gh->buffer->data);
    unsigned char* buf_ref    = NULL;
    unsigned char* buf_width  = NULL;
    unsigned char* buf_length = NULL;
    unsigned char* buf_vals   = NULL;

    long length_p            = 0;
    long ref_p               = 0;
    long width_p             = 0;
    long vals_p              = 0;
    long nvals_per_group     = 0;
    long nbits_per_group_val = 0;
    long group_ref_val       = 0;

    long bits_per_value    = 0;
    T binary_s             = 0;
    T decimal_s            = 0;
    double reference_value = 0;

    long binary_scale_factor;
    long decimal_scale_factor;
    long typeOfOriginalFieldValues;
    long groupSplittingMethodUsed;
    long missingValueManagementUsed;
    long primaryMissingValueSubstitute;
    long secondaryMissingValueSubstitute;
    long numberOfGroupsOfDataValues;
    long referenceForGroupWidths;
    long numberOfBitsUsedForTheGroupWidths;
    long referenceForGroupLengths;
    long lengthIncrementForTheGroupLengths;
    long trueLengthOfLastGroup;
    long numberOfBitsUsedForTheScaledGroupLengths;
    long orderOfSpatialDifferencing     = 0;
    long numberOfOctetsExtraDescriptors = 0;
    double missingValue                 = 0;

    err = value_count(&n_vals);
    if (err)
        return err;

    if (*len < static_cast<size_t>(n_vals))
        return GRIB_ARRAY_TOO_SMALL;

    if ((err = grib_get_long_internal(gh, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(gh, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, typeOfOriginalFieldValues_, &typeOfOriginalFieldValues)) != GRIB_SUCCESS)
        return err;

    // Don't call grib_get_long_internal to suppress error message being output
    if ((err = grib_get_long(gh, groupSplittingMethodUsed_, &groupSplittingMethodUsed)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, missingValueManagementUsed_, &missingValueManagementUsed)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, primaryMissingValueSubstitute_, &primaryMissingValueSubstitute)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, secondaryMissingValueSubstitute_, &secondaryMissingValueSubstitute)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, numberOfGroupsOfDataValues_, &numberOfGroupsOfDataValues)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, referenceForGroupWidths_, &referenceForGroupWidths)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, numberOfBitsUsedForTheGroupWidths_, &numberOfBitsUsedForTheGroupWidths)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, referenceForGroupLengths_, &referenceForGroupLengths)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, lengthIncrementForTheGroupLengths_, &lengthIncrementForTheGroupLengths)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, trueLengthOfLastGroup_, &trueLengthOfLastGroup)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, numberOfBitsUsedForTheScaledGroupLengths_, &numberOfBitsUsedForTheScaledGroupLengths)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, orderOfSpatialDifferencing_, &orderOfSpatialDifferencing)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, numberOfOctetsExtraDescriptors_, &numberOfOctetsExtraDescriptors)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(gh, "missingValue", &missingValue)) != GRIB_SUCCESS)
        return err;

    dirty_ = 0;

    if (bits_per_value == 0) {
        for (i = 0; i < n_vals; i++) {
            val[i] = reference_value;
        }
        *len = n_vals;
        return GRIB_SUCCESS;
    }

    sec_val = (long*)grib_context_malloc(context_, (n_vals) * sizeof(long));
    if (!sec_val) return GRIB_OUT_OF_MEMORY;
    memset(sec_val, 0, (n_vals) * sizeof(long));  // See SUP-718

    buf_ref = buf + offset_;

    ref_p = (numberOfGroupsOfDataValues * bits_per_value);

    if (orderOfSpatialDifferencing)
        ref_p += (1 + orderOfSpatialDifferencing) * (numberOfOctetsExtraDescriptors * 8);

    buf_width = buf_ref + (ref_p / 8) + ((ref_p % 8) ? 1 : 0);

    width_p    = (numberOfGroupsOfDataValues * numberOfBitsUsedForTheGroupWidths);
    buf_length = buf_width + (width_p / 8) + ((width_p % 8) ? 1 : 0);

    length_p = (numberOfGroupsOfDataValues * numberOfBitsUsedForTheScaledGroupLengths);
    buf_vals = buf_length + (length_p / 8) + ((length_p % 8) ? 1 : 0);

    length_p = 0;
    ref_p    = orderOfSpatialDifferencing ? (orderOfSpatialDifferencing + 1) * (numberOfOctetsExtraDescriptors * 8) : 0;
    width_p  = 0;
    vals_p   = 0;
    vcount   = 0;

    for (i = 0; i < numberOfGroupsOfDataValues; i++) {
        group_ref_val       = grib_decode_unsigned_long(buf_ref, &ref_p, bits_per_value);
        nvals_per_group     = grib_decode_unsigned_long(buf_length, &length_p, numberOfBitsUsedForTheScaledGroupLengths);
        nbits_per_group_val = grib_decode_unsigned_long(buf_width, &width_p, numberOfBitsUsedForTheGroupWidths);

        nvals_per_group *= lengthIncrementForTheGroupLengths;
        nvals_per_group += referenceForGroupLengths;
        nbits_per_group_val += referenceForGroupWidths;

        if (i == numberOfGroupsOfDataValues - 1)
            nvals_per_group = trueLengthOfLastGroup;
        if (n_vals < vcount + nvals_per_group) {
            return GRIB_DECODING_ERROR;
        }

        // grib_decode_long_array(buf_vals, &vals_p, nbits_per_group_val, nvals_per_group, &sec_val[vcount]);
        if (missingValueManagementUsed == 0) {
            // No explicit missing values included within data values
            for (j = 0; j < nvals_per_group; j++) {
                DEBUG_ASSERT_ACCESS(sec_val, (long)(vcount + j), n_vals);
                sec_val[vcount + j] = group_ref_val + grib_decode_unsigned_long(buf_vals, &vals_p, nbits_per_group_val);
                // printf("sec_val[%ld]=%ld\n", vcount+j, sec_val[vcount+j]);
            }
        }
        else if (missingValueManagementUsed == 1) {
            // Primary missing values included within data values
            long maxn = 0;  // (1 << bits_per_value) - 1;
            for (j = 0; j < nvals_per_group; j++) {
                if (nbits_per_group_val == 0) {
                    maxn = (1 << bits_per_value) - 1;
                    if (group_ref_val == maxn) {
                        sec_val[vcount + j] = LONG_MAX;  // missing value
                    }
                    else {
                        long temp           = grib_decode_unsigned_long(buf_vals, &vals_p, nbits_per_group_val);
                        sec_val[vcount + j] = group_ref_val + temp;
                    }
                }
                else {
                    long temp = grib_decode_unsigned_long(buf_vals, &vals_p, nbits_per_group_val);
                    maxn      = (1 << nbits_per_group_val) - 1;
                    if (temp == maxn) {
                        sec_val[vcount + j] = LONG_MAX;  // missing value
                    }
                    else {
                        sec_val[vcount + j] = group_ref_val + temp;
                    }
                }
            }
        }
        else if (missingValueManagementUsed == 2) {
            // Primary and secondary missing values included within data values
            long maxn  = (1 << bits_per_value) - 1;
            long maxn2 = 0;  // maxn - 1
            for (j = 0; j < nvals_per_group; j++) {
                if (nbits_per_group_val == 0) {
                    maxn2 = maxn - 1;
                    if (group_ref_val == maxn || group_ref_val == maxn2) {
                        sec_val[vcount + j] = LONG_MAX;  // missing value
                    }
                    else {
                        long temp           = grib_decode_unsigned_long(buf_vals, &vals_p, nbits_per_group_val);
                        sec_val[vcount + j] = group_ref_val + temp;
                    }
                }
                else {
                    long temp = grib_decode_unsigned_long(buf_vals, &vals_p, nbits_per_group_val);
                    maxn      = (1 << nbits_per_group_val) - 1;
                    maxn2     = maxn - 1;
                    if (temp == maxn || temp == maxn2) {
                        sec_val[vcount + j] = LONG_MAX;  // missing value
                    }
                    else {
                        sec_val[vcount + j] = group_ref_val + temp;
                    }
                }
            }
        }

        vcount += nvals_per_group;
    }

    if (orderOfSpatialDifferencing) {
        long bias               = 0;
        unsigned long extras[2] = {
            0,
        };
        ref_p = 0;

        // For Complex packing, order == 0
        // For Complex packing and spatial differencing, order == 1 or 2 (code table 5.6)
        if (orderOfSpatialDifferencing != 1 && orderOfSpatialDifferencing != 2) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "%s unpacking: Unsupported order of spatial differencing %ld", class_name_, orderOfSpatialDifferencing);
            return GRIB_INTERNAL_ERROR;
        }

        for (i = 0; i < orderOfSpatialDifferencing; i++) {
            extras[i] = grib_decode_unsigned_long(buf_ref, &ref_p, numberOfOctetsExtraDescriptors * 8);
        }

        bias = grib_decode_signed_longb(buf_ref, &ref_p, numberOfOctetsExtraDescriptors * 8);

        post_process(context_, sec_val, n_vals, orderOfSpatialDifferencing, bias, extras);
        // de_spatial_difference (context_ , sec_val, n_vals, orderOfSpatialDifferencing, bias);
    }

    binary_s  = (T)codes_power<T>(binary_scale_factor, 2);
    decimal_s = (T)codes_power<T>(-decimal_scale_factor, 10);

    for (i = 0; i < n_vals; i++) {
        if (sec_val[i] == LONG_MAX) {
            val[i] = (T)missingValue;
        }
        else {
            val[i] = (T)((((T)sec_val[i]) * binary_s) + reference_value) * decimal_s;
        }
    }

    grib_context_free(context_, sec_val);
    return err;
}

int grib_accessor_data_g22order_packing_t::unpack_double(double* val, size_t* len)
{
    return unpack<double>(val, len);
}

int grib_accessor_data_g22order_packing_t::unpack_float(float* val, size_t* len)
{
    return unpack<float>(val, len);
}

int grib_accessor_data_g22order_packing_t::unpack_double_element(size_t idx, double* val)
{
    size_t size    = 0;
    double* values = NULL;
    int err        = grib_get_size(grib_handle_of_accessor(this), "codedValues", &size);
    if (err)
        return err;
    if (idx > size)
        return GRIB_INVALID_ARGUMENT;

    values = reinterpret_cast<double*>(grib_context_malloc_clear(context_, size * sizeof(double)));
    err    = grib_get_double_array(grib_handle_of_accessor(this), "codedValues", values, &size);
    if (err) {
        grib_context_free(context_, values);
        return err;
    }
    *val = values[idx];
    grib_context_free(context_, values);
    return GRIB_SUCCESS;
}

int grib_accessor_data_g22order_packing_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    size_t size = 0, i = 0;
    double* values;
    int err = 0;

    // GRIB-564: The indexes in index_array relate to codedValues NOT values!
    err = grib_get_size(grib_handle_of_accessor(this), "codedValues", &size);
    if (err)
        return err;

    for (i = 0; i < len; i++) {
        if (index_array[i] > size) return GRIB_INVALID_ARGUMENT;
    }

    values = reinterpret_cast<double*>(grib_context_malloc_clear(context_, size * sizeof(double)));
    err    = grib_get_double_array(grib_handle_of_accessor(this), "codedValues", values, &size);
    if (err) {
        grib_context_free(context_, values);
        return err;
    }
    for (i = 0; i < len; i++) {
        val_array[i] = values[index_array[i]];
    }
    grib_context_free(context_, values);
    return GRIB_SUCCESS;
}

int grib_accessor_data_g22order_packing_t::value_count(long* count)
{
    *count = 0;
    return grib_get_long_internal(grib_handle_of_accessor(this), numberOfValues_, count);
}
