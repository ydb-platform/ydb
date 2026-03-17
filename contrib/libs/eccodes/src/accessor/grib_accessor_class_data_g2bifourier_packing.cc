/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g2bifourier_packing.h"
#include "grib_scaling.h"
#include <algorithm>

grib_accessor_data_g2bifourier_packing_t _grib_accessor_data_g2bifourier_packing{};
grib_accessor* grib_accessor_data_g2bifourier_packing = &_grib_accessor_data_g2bifourier_packing;

void grib_accessor_data_g2bifourier_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_simple_packing_t::init(v, args);
    grib_handle* gh = grib_handle_of_accessor(this);

    ieee_floats_                         = args->get_name(gh, carg_++);
    laplacianOperatorIsSet_              = args->get_name(gh, carg_++);
    laplacianOperator_                   = args->get_name(gh, carg_++);
    biFourierTruncationType_             = args->get_name(gh, carg_++);
    sub_i_                               = args->get_name(gh, carg_++);
    sub_j_                               = args->get_name(gh, carg_++);
    bif_i_                               = args->get_name(gh, carg_++);
    bif_j_                               = args->get_name(gh, carg_++);
    biFourierSubTruncationType_          = args->get_name(gh, carg_++);
    biFourierDoNotPackAxes_              = args->get_name(gh, carg_++);
    biFourierMakeTemplate_               = args->get_name(gh, carg_++);
    totalNumberOfValuesInUnpackedSubset_ = args->get_name(gh, carg_++);
    /*numberOfValues             = args->get_name(gh, carg_++);*/

    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
    dirty_ = 1;
}

int grib_accessor_data_g2bifourier_packing_t::value_count(long* numberOfValues)
{
    grib_handle* gh = grib_handle_of_accessor(this);
    *numberOfValues = 0;

    return grib_get_long_internal(gh, number_of_values_, numberOfValues);
}

static void ellipse(long ni, long nj, long itrunc[], long jtrunc[])
{
    const double zeps   = 1.E-10;
    const double zauxil = 0.;
    int i, j;
    double zi, zj;

    /*
     * 1. Computing meridional limit wavenumbers along zonal wavenumbers
     */

    for (j = 1; j < nj; j++) {
        zi        = (double)ni / (double)nj * sqrt(std::max(zauxil, (double)(nj * nj - j * j)));
        itrunc[j] = (int)(zi + zeps);
    }

    if (nj == 0) {
        itrunc[0] = ni;
    }
    else {
        itrunc[0]  = ni;
        itrunc[nj] = 0;
    }

    /*
     * 2. Computing zonal limit wavenumbers along meridional wavenumbers
     */

    for (i = 1; i < ni; i++) {
        zj        = (double)nj / (double)ni * sqrt(std::max(zauxil, (double)(ni * ni - i * i)));
        jtrunc[i] = (int)(zj + zeps);
    }

    if (ni == 0) {
        jtrunc[0] = nj;
    }
    else {
        jtrunc[0]  = nj;
        jtrunc[ni] = 0;
    }
}

static void rectangle(long ni, long nj, long itrunc[], long jtrunc[])
{
    int i, j;

    /*
     * 1. Computing meridional limit wavenumbers along zonal wavenumbers
     */

    for (j = 0; j <= nj; j++)
        itrunc[j] = ni;

    /*
     * 2. Computing zonal limit wavenumbers along meridional wavenumbers
     */

    for (i = 0; i <= ni; i++)
        jtrunc[i] = nj;
}

static void diamond(long ni, long nj, long itrunc[], long jtrunc[])
{
    int i, j;

    if (nj == 0)
        itrunc[0] = -1;
    else
        for (j = 0; j <= nj; j++)
            itrunc[j] = ni - (j * ni) / nj;

    if (ni == 0)
        jtrunc[0] = -1;
    else
        for (i = 0; i <= ni; i++)
            jtrunc[i] = nj - (i * nj) / ni;
}

#define scals(i, j) pow((double)((i) * (i) + (j) * (j)), bt->laplacianOperator)

#define for_ij()                     \
    for (j = 0; j <= bt->bif_j; j++) \
        for (i = 0; i <= bt->itruncation_bif[j]; i++)

#define calc_insub()                                    \
    do {                                                \
        insub = (i <= bt->sub_i) && (j <= bt->sub_j);   \
        if (insub) {                                    \
            int insubi = (i <= bt->itruncation_sub[j]); \
            int insubj = (j <= bt->jtruncation_sub[i]); \
            insub      = insubi && insubj;              \
        }                                               \
        if (bt->keepaxes)                               \
            insub = insub || (i == 0) || (j == 0);      \
    } while (0)


/*
 * Total number of coefficients
 */
static size_t size_bif(bif_trunc_t* bt)
{
    size_t n_vals = 0;
    int j;
    for (j = 0; j <= bt->bif_j; j++)
        n_vals += 4 * (bt->itruncation_bif[j] + 1);
    return n_vals;
}

/*
 * Number of unpacked coefficients
 */
static size_t size_sub(bif_trunc_t* bt)
{
    size_t n_vals = 0;
    int i, j;
    for_ij()
    {
        int insub;

        calc_insub();

        if (insub)
            n_vals += 4;
    }
    return n_vals;
}

static double laplam(bif_trunc_t* bt, const double val[])
{
    /*
     * For bi-Fourier spectral fields, the Laplacian operator is a multiplication by (i*i+j*j)
     */

    const double zeps = 1E-15;
    double *znorm = NULL, *zw = NULL;
    int kmax   = 1 + bt->bif_i * bt->bif_i + bt->bif_j * bt->bif_j, lmax;
    int *itab1 = NULL, *itab2 = NULL;
    int i, j, k, l, isp;
    double zxmw, zymw, zwsum, zx, zy, zsum1, zsum2, zbeta1, zp;

    itab1 = (int*)calloc(kmax, sizeof(int));
    itab2 = (int*)malloc(sizeof(int) * ((1 + bt->bif_i) * (1 + bt->bif_j)));

    /*
     * Keep record of the possible values of i**2+j**2 outside the non-packed truncation
     */
    for_ij()
    {
        int insub;

        calc_insub();

        if (!insub) {
            const int kk = i * i + j * j;
            itab1[kk]    = 1;
        }
    }

    l = 0;
    for (k = 0; k < kmax; k++) {
        if (itab1[k]) {
            itab2[l] = k;
            itab1[k] = l;
            l++;
        }
    }
    lmax = l;

    if (lmax == 0) { /* ECC-1207 */
        free(itab1);
        free(itab2);
        return 0.;
    }
    ECCODES_ASSERT(lmax > 0);

    /*
     * Now, itab2 contains all possible values of i*i+j*j, and itab1 contains
     * the rank of all i*i+j*j
     */
    znorm = (double*)calloc(lmax, sizeof(double));
    zw    = (double*)malloc(sizeof(double) * lmax);

    /*
     * Compute norms of input field, gathered by values of i**2+j**2; we have to
     * go through the unpacked truncation again
     */
    isp = 0;
    for_ij()
    {
        int insub;

        calc_insub();

        if (insub) {
            isp += 4;
        }
        else {
            int m, ll = itab1[i * i + j * j];
            for (m = 0; m < 4; m++, isp++) {
                DEBUG_ASSERT_ACCESS(znorm, (long)ll, (long)lmax);
                DEBUG_ASSERT_ACCESS(val, (long)isp, (long)bt->n_vals_bif);
                if (ll < lmax && isp < bt->n_vals_bif) {
                    znorm[ll] = std::max(znorm[ll], fabs(val[isp]));
                }
            }
        }
    }

    /*
     * Compute weights, fix very small norms to avoid problems with log function
     */
    for (l = 0; l < lmax; l++) {
        zw[l] = (double)lmax / (double)(l + 1);
        if (znorm[l] < zeps) {
            znorm[l] = zeps;
            zw[l]    = 100. * zeps;
        }
    }

    /*
     * Sum weights
     */
    zxmw  = 0.;
    zymw  = 0.;
    zwsum = 0.;

    for (l = 0; l < lmax; l++) {
        zx = log(itab2[l]);
        zy = log(znorm[l]);
        zxmw += zx * zw[l];
        zymw += zy * zw[l];
        zwsum += zw[l];
    }

    /*
     * Least square regression
     */
    zxmw  = zxmw / zwsum;
    zymw  = zymw / zwsum;
    zsum1 = 0.;
    zsum2 = 0.;

    for (l = 0; l < lmax; l++) {
        zx = log(itab2[l]);
        zy = log(znorm[l]);
        zsum1 += zw[l] * (zy - zymw) * (zx - zxmw);
        zsum2 += zw[l] * (zx - zxmw) * (zx - zxmw);
    }

    zbeta1 = zsum1 / zsum2;
    zp     = -zbeta1;
    zp     = std::max(-9.999, std::min(9.999, zp));

    free(itab1);
    free(itab2);

    free(znorm);
    free(zw);

    /*zp = ((long)(zp * 1000.)) / 1000.; FAPULA rounds Laplacian power to 1/1000th*/

    return zp;
}

static void free_bif_trunc(bif_trunc_t* bt, grib_accessor* a)
{
    grib_handle* gh = grib_handle_of_accessor(a);
    if (bt == NULL)
        return;
    if (bt->itruncation_bif != NULL)
        free(bt->itruncation_bif);
    if (bt->jtruncation_bif != NULL)
        free(bt->jtruncation_bif);
    if (bt->itruncation_sub != NULL)
        free(bt->itruncation_sub);
    if (bt->jtruncation_sub != NULL)
        free(bt->jtruncation_sub);
    memset(bt, 0, sizeof(bif_trunc_t));
    grib_context_free(gh->context, bt);
}

bif_trunc_t* grib_accessor_data_g2bifourier_packing_t::new_bif_trunc()
{
    int ret;

    grib_handle* gh = grib_handle_of_accessor(this);
    bif_trunc_t* bt = (bif_trunc_t*)grib_context_malloc(gh->context, sizeof(bif_trunc_t));

    memset(bt, 0, sizeof(bif_trunc_t));

    if ((ret = grib_get_double_internal(gh, reference_value_, &bt->reference_value)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, bits_per_value_, &bt->bits_per_value)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, binary_scale_factor_, &bt->binary_scale_factor)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, decimal_scale_factor_, &bt->decimal_scale_factor)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, ieee_floats_, &bt->ieee_floats)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, laplacianOperatorIsSet_, &bt->laplacianOperatorIsSet)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_double_internal(gh, laplacianOperator_, &bt->laplacianOperator)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, sub_i_, &bt->sub_i)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, sub_j_, &bt->sub_j)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, bif_i_, &bt->bif_i)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, bif_j_, &bt->bif_j)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, biFourierTruncationType_, &bt->biFourierTruncationType)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, biFourierSubTruncationType_, &bt->biFourierSubTruncationType)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, biFourierDoNotPackAxes_, &bt->keepaxes)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_get_long_internal(gh, biFourierMakeTemplate_, &bt->maketemplate)) != GRIB_SUCCESS)
        goto cleanup;

    switch (bt->ieee_floats) {
        case 0:
            bt->decode_float = grib_long_to_ibm;
            bt->encode_float = grib_ibm_to_long;
            bt->bytes        = 4;
            break;
        case 1:
            bt->decode_float = grib_long_to_ieee;
            bt->encode_float = grib_ieee_to_long;
            bt->bytes        = 4;
            break;
        case 2:
            bt->decode_float = grib_long_to_ieee64;
            bt->encode_float = grib_ieee64_to_long;
            bt->bytes        = 8;
            break;
        default:
            ret = GRIB_NOT_IMPLEMENTED;
            goto cleanup;
    }

    bt->itruncation_sub = (long*)grib_context_malloc(gh->context, sizeof(long) * (1 + bt->sub_j));
    bt->jtruncation_sub = (long*)grib_context_malloc(gh->context, sizeof(long) * (1 + bt->sub_i));
    bt->itruncation_bif = (long*)grib_context_malloc(gh->context, sizeof(long) * (1 + bt->bif_j));
    bt->jtruncation_bif = (long*)grib_context_malloc(gh->context, sizeof(long) * (1 + bt->bif_i));

#define RECTANGLE 77
#define ELLIPSE   88
#define DIAMOND   99

    switch (bt->biFourierTruncationType) {
        case RECTANGLE:
            rectangle(bt->bif_i, bt->bif_j, bt->itruncation_bif, bt->jtruncation_bif);
            break;
        case ELLIPSE:
            ellipse(bt->bif_i, bt->bif_j, bt->itruncation_bif, bt->jtruncation_bif);
            break;
        case DIAMOND:
            diamond(bt->bif_i, bt->bif_j, bt->itruncation_bif, bt->jtruncation_bif);
            break;
        default:
            ret = GRIB_INVALID_KEY_VALUE;
            goto cleanup;
    }
    switch (bt->biFourierSubTruncationType) {
        case RECTANGLE:
            rectangle(bt->sub_i, bt->sub_j, bt->itruncation_sub, bt->jtruncation_sub);
            break;
        case ELLIPSE:
            ellipse(bt->sub_i, bt->sub_j, bt->itruncation_sub, bt->jtruncation_sub);
            break;
        case DIAMOND:
            diamond(bt->sub_i, bt->sub_j, bt->itruncation_sub, bt->jtruncation_sub);
            break;
        default:
            ret = GRIB_INVALID_KEY_VALUE;
            goto cleanup;
    }

    bt->n_vals_bif = size_bif(bt);
    bt->n_vals_sub = size_sub(bt);

    return bt;

cleanup:

    free_bif_trunc(bt, this);
    if (ret) fprintf(stderr, "ERROR: new_bif_trunc: %s\n", grib_get_error_message(ret));

    return NULL;
}

int grib_accessor_data_g2bifourier_packing_t::unpack_double(double* val, size_t* len)
{
    grib_handle* gh = grib_handle_of_accessor(this);

    unsigned char* buf  = NULL;
    unsigned char* hres = NULL;
    unsigned char* lres = NULL;
    unsigned long packed_offset;

    long hpos = 0;
    long lpos = 0;

    int isp;

    bif_trunc_t* bt = NULL;

    long count = 0;

    long offsetdata = 0;

    double s = 0;
    double d = 0;
    int ret  = GRIB_SUCCESS;
    int i, j, k;

    if ((ret = value_count(&count)) != GRIB_SUCCESS)
        goto cleanup;

    bt = new_bif_trunc();

    if (bt == NULL) {
        ret = GRIB_INTERNAL_ERROR;
        goto cleanup;
    }

    if (bt->n_vals_bif != count) {
        ret = GRIB_INTERNAL_ERROR;
        goto cleanup;
    }

    if ((ret = grib_get_long_internal(gh, offsetdata_, &offsetdata)) != GRIB_SUCCESS)
        goto cleanup;
    if (*len < bt->n_vals_bif) {
        *len = (long)bt->n_vals_bif;
        ret  = GRIB_ARRAY_TOO_SMALL;
        goto cleanup;
    }

    dirty_ = 0;

    buf = (unsigned char*)gh->buffer->data;
    buf += byte_offset();
    s = codes_power<double>(bt->binary_scale_factor, 2);
    d = codes_power<double>(-bt->decimal_scale_factor, 10);

    /*
     * Decode data
     */
    hres = buf;
    lres = buf;

    packed_offset = byte_offset() + bt->bytes * bt->n_vals_sub;

    lpos = 8 * (packed_offset - offsetdata);
    hpos = 0;

    isp = 0;
    for_ij()
    {
        int insub;

        calc_insub();

        if (insub)
            for (k = 0; k < 4; k++) {
                val[isp + k] = bt->decode_float(grib_decode_unsigned_long(hres, &hpos, 8 * bt->bytes));
            }
        else
            for (k = 0; k < 4; k++) {
                double S     = scals(i, j);
                long dec_val = grib_decode_unsigned_long(lres, &lpos, bt->bits_per_value);
                val[isp + k] = (double)(((dec_val * s) + bt->reference_value) * d) / S;
            }

        isp += 4;
    }

    ECCODES_ASSERT(*len >= isp);
    *len = isp;

cleanup:

    free_bif_trunc(bt, this);

    return ret;
}

int grib_accessor_data_g2bifourier_packing_t::pack_double(const double* val, size_t* len)
{
    grib_handle* gh = grib_handle_of_accessor(this);

    size_t buflen       = 0;
    size_t hsize        = 0;
    size_t lsize        = 0;
    unsigned char* buf  = NULL;
    unsigned char* hres = NULL;
    unsigned char* lres = NULL;
    long hpos           = 0;
    long lpos           = 0;
    int isp;
    bif_trunc_t* bt = NULL;

    double max = 0;
    double min = 0;
    /*grib_context* c = gh->context;*/

    int ret = GRIB_SUCCESS;
    int i, j, k;
    int minmax_set;
    double d = 0., s = 0.;

    if (*len == 0) {
        ret = GRIB_NO_VALUES;
        goto cleanup;
    }

    bt = new_bif_trunc();

    if (bt == NULL) {
        long makeTemplate = 0;
        if ((ret = grib_get_long_internal(gh, biFourierMakeTemplate_, &makeTemplate)) != GRIB_SUCCESS)
            goto cleanup;
        if (!makeTemplate) {
            ret = GRIB_INTERNAL_ERROR;
            goto cleanup;
        }
        else {
            printf("Assuming we are creating a template\n");
            ret = GRIB_SUCCESS;
            goto cleanup;
        }
    }

    dirty_ = 1;

    if (*len != bt->n_vals_bif) {
        grib_context_log(gh->context, GRIB_LOG_ERROR, "BIFOURIER_PACKING: wrong number of values, expected %lu - got %lu",
                         bt->n_vals_bif, *len);
        ret = GRIB_INTERNAL_ERROR;
        goto cleanup;
    }

    if (!bt->laplacianOperatorIsSet) {
        bt->laplacianOperator = laplam(bt, val);

        if ((ret = grib_set_double_internal(gh, laplacianOperator_, bt->laplacianOperator)) != GRIB_SUCCESS)
            goto cleanup;

        grib_get_double_internal(gh, laplacianOperator_, &bt->laplacianOperator);
    }

    /*
     * Scan all values that will be truncated and find their minimum and maximum
     */
    minmax_set = 0;

    isp = 0;
    for_ij()
    {
        int insub;

        calc_insub();

        if (!insub) {
            for (k = 0; k < 4; k++) {
                double current_val = val[isp + k] * scals(i, j);
                if (!minmax_set) {
                    min = current_val;
                    max = current_val;
                    minmax_set++;
                }
                if (current_val < min)
                    min = current_val;
                if (current_val > max)
                    max = current_val;
            }
        }

        isp += 4;
    }

    if (bt->n_vals_bif != bt->n_vals_sub) {
        ret = grib_optimize_decimal_factor(this, reference_value_,
                                           max, min, bt->bits_per_value, 0, 1,
                                           &bt->decimal_scale_factor,
                                           &bt->binary_scale_factor,
                                           &bt->reference_value);
        if (ret != GRIB_SUCCESS)
            goto cleanup;

        s = codes_power<double>(-bt->binary_scale_factor, 2);
        d = codes_power<double>(+bt->decimal_scale_factor, 10);
    }
    else {
        bt->decimal_scale_factor = 0;
        bt->binary_scale_factor  = 0;
        bt->reference_value      = 0.;
    }

    /*
     * Encode values
     */
    hsize = bt->bytes * bt->n_vals_sub;
    lsize = ((bt->n_vals_bif - bt->n_vals_sub) * bt->bits_per_value) / 8;

    buflen = hsize + lsize;

    buf = (unsigned char*)grib_context_malloc(gh->context, buflen);

    hres = buf;
    lres = buf + hsize;

    lpos = 0;
    hpos = 0;

    isp = 0;

    for_ij()
    {
        double current_val;
        int insub;

        calc_insub();

        if (insub)
            for (k = 0; k < 4; k++) {
                current_val = val[isp + k];
                grib_encode_unsigned_long(hres, bt->encode_float(current_val), &hpos, 8 * bt->bytes);
            }
        else
            for (k = 0; k < 4; k++) {
                double S    = scals(i, j);
                current_val = (((((val[isp + k] * d) * S) - bt->reference_value) * s) + 0.5);

                if (current_val < 0)
                    grib_context_log(gh->context, GRIB_LOG_ERROR, "BIFOURIER_PACKING : negative coput before packing (%g)", current_val);

                if (bt->bits_per_value % 8)
                    grib_encode_unsigned_longb(lres, current_val, &lpos, bt->bits_per_value);
                else
                    grib_encode_unsigned_long(lres, current_val, &lpos, bt->bits_per_value);
            }

        isp += 4;
    }

    if (((hpos / 8) != hsize) && ((lpos / 8) != lsize)) {
        grib_context_log(gh->context, GRIB_LOG_ERROR, "BIFOURIER_PACKING : Mismatch in packing between high resolution and low resolution part");
        ret = GRIB_INTERNAL_ERROR;
        goto cleanup;
    }

    buflen = ((hpos + lpos) / 8);

    if ((ret = grib_set_double_internal(gh, reference_value_, bt->reference_value)) != GRIB_SUCCESS)
        goto cleanup;

    {
        // Make sure we can decode it again
        double ref = 1e-100;
        grib_get_double_internal(gh, reference_value_, &ref);
        if (ref != bt->reference_value) {
            grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: %s (ref=%.10e != reference_value=%.10e)",
                             class_name_, __func__, reference_value_, ref, bt->reference_value);
            return GRIB_INTERNAL_ERROR;
        }
    }

    if ((ret = grib_set_long_internal(gh, binary_scale_factor_, bt->binary_scale_factor)) != GRIB_SUCCESS)
        goto cleanup;
    if ((ret = grib_set_long_internal(gh, decimal_scale_factor_, bt->decimal_scale_factor)) != GRIB_SUCCESS)
        goto cleanup;

    grib_buffer_replace(this, buf, buflen, 1, 1);

    if ((ret = grib_set_long_internal(gh, totalNumberOfValuesInUnpackedSubset_, bt->n_vals_sub)) != GRIB_SUCCESS)
        goto cleanup;

    if ((ret = grib_set_long_internal(gh, number_of_values_, bt->n_vals_bif)) != GRIB_SUCCESS)
        goto cleanup;

cleanup:

    free_bif_trunc(bt, this);

    if (buf != NULL)
        grib_context_free(gh->context, buf);

    return ret;
}
