/* Copyright (c) 2006-2012 Filip Wasilewski <http://en.ig.ma/>
 * Copyright (c) 2012-2016 The PyWavelets Developers
 *                         <https://github.com/PyWavelets/pywt>
 * See COPYING for license details.
 */

/* Allocating, setting properties and destroying wavelet structs */
#include "wavelets.h"
#include "wavelets_coeffs.h"

#define SWAP(t, x, y) {t tmp = x; x = y; y = tmp;}
#define NELEMS(x) (sizeof(x) / sizeof(*x))

int is_discrete_wavelet(WAVELET_NAME name)
{
    switch(name){
        case HAAR:
            return 1;
        case RBIO:
            return 1;
        case DB:
            return 1;
        case SYM:
            return 1;
        case COIF:
            return 1;
        case BIOR:
            return 1;
        case DMEY:
            return 1;
        case GAUS:
            return 0;
        case MEXH:
            return 0;
        case MORL:
            return 0;
        case CGAU:
            return 0;
        case SHAN:
            return 0;
        case FBSP:
            return 0;
        case CMOR:
            return 0;
        default:
            return -1;
    }

}


DiscreteWavelet* discrete_wavelet(WAVELET_NAME name, unsigned int order)
{
    DiscreteWavelet *w;
    /* Haar wavelet */
    if(name == HAAR){

        /* the same as db1 */
        w = discrete_wavelet(DB, 1);
        w->base.family_name = "Haar";
        w->base.short_name = "haar";
        return w;

    /* Reverse biorthogonal wavelets family */
    } else if (name == RBIO) {
        /* rbio is like bior, only with switched filters */
        w = discrete_wavelet(BIOR, order);
        if (w == NULL) return NULL;

        SWAP(size_t, w->dec_len, w->rec_len);
        SWAP(float*, w->rec_lo_float, w->dec_lo_float);
        SWAP(float*, w->rec_hi_float, w->dec_hi_float);
        SWAP(double*, w->rec_lo_double, w->dec_lo_double);
        SWAP(double*, w->rec_hi_double, w->dec_hi_double);

        {
            size_t i, j;
            for(i = 0, j = w->rec_len - 1; i < j; i++, j--){
                SWAP(float, w->rec_lo_float[i], w->rec_lo_float[j]);
                SWAP(float, w->rec_hi_float[i], w->rec_hi_float[j]);
                SWAP(float, w->dec_lo_float[i], w->dec_lo_float[j]);
                SWAP(float, w->dec_hi_float[i], w->dec_hi_float[j]);

                SWAP(double, w->rec_lo_double[i], w->rec_lo_double[j]);
                SWAP(double, w->rec_hi_double[i], w->rec_hi_double[j]);
                SWAP(double, w->dec_lo_double[i], w->dec_lo_double[j]);
                SWAP(double, w->dec_hi_double[i], w->dec_hi_double[j]);
            }
        }

        w->base.family_name = "Reverse biorthogonal";
        w->base.short_name = "rbio";

        return w;
    }

    switch(name){
        /* Daubechies wavelets family */
        case DB: {
            size_t coeffs_idx = order - 1;
            if (coeffs_idx >= NELEMS(db_float) ||
                coeffs_idx >= NELEMS(db_double))
                return NULL;
            w = blank_discrete_wavelet(2 * order);
            if(w == NULL) return NULL;

            w->vanishing_moments_psi = order;
            w->vanishing_moments_phi = 0;
            w->base.support_width = 2*order - 1;
            w->base.orthogonal = 1;
            w->base.biorthogonal = 1;
            w->base.symmetry = ASYMMETRIC;
            w->base.compact_support = 1;
            w->base.family_name = "Daubechies";
            w->base.short_name = "db";
            {
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_float[i] = db_float[coeffs_idx][i];
                    w->dec_lo_float[i] = db_float[coeffs_idx][w->dec_len-1-i];
                    w->rec_hi_float[i] = ((i % 2) ? -1 : 1)
                      * db_float[coeffs_idx][w->dec_len-1-i];
                    w->dec_hi_float[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * db_float[coeffs_idx][i];
                }
            }

            {
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_double[i] = db_double[coeffs_idx][i];
                    w->dec_lo_double[i] = db_double[coeffs_idx][w->dec_len-1-i];
                    w->rec_hi_double[i] = ((i % 2) ? -1 : 1)
                      * db_double[coeffs_idx][w->dec_len-1-i];
                    w->dec_hi_double[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * db_double[coeffs_idx][i];
                }
            }

            break;
        }

        /* Symlets wavelets family */
        case SYM: {
            size_t coeffs_idx = order - 2;
            if (coeffs_idx >= NELEMS(sym_float) ||
                coeffs_idx >= NELEMS(sym_double))
                return NULL;

            w = blank_discrete_wavelet(2 * order);
            if(w == NULL) return NULL;

            w->vanishing_moments_psi = order;
            w->vanishing_moments_phi = 0;
            w->base.support_width = 2*order - 1;
            w->base.orthogonal = 1;
            w->base.biorthogonal = 1;
            w->base.symmetry = NEAR_SYMMETRIC;
            w->base.compact_support = 1;
            w->base.family_name = "Symlets";
            w->base.short_name = "sym";
            {
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_float[i] = sym_float[coeffs_idx][i];
                    w->dec_lo_float[i] = sym_float[coeffs_idx][w->dec_len-1-i];
                    w->rec_hi_float[i] = ((i % 2) ? -1 : 1)
                      * sym_float[coeffs_idx][w->dec_len-1-i];
                    w->dec_hi_float[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * sym_float[coeffs_idx][i];
                }
            }

            {
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_double[i] = sym_double[coeffs_idx][i];
                    w->dec_lo_double[i] = sym_double[coeffs_idx][w->dec_len-1-i];
                    w->rec_hi_double[i] = ((i % 2) ? -1 : 1)
                      * sym_double[coeffs_idx][w->dec_len-1-i];
                    w->dec_hi_double[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * sym_double[coeffs_idx][i];
                }
            }
            break;
        }

        /* Coiflets wavelets family */
        case COIF: {
            size_t coeffs_idx = order - 1;
            if (coeffs_idx >= NELEMS(coif_float) ||
                coeffs_idx >= NELEMS(coif_double))
                return NULL;
            w = blank_discrete_wavelet(6 * order);
            if(w == NULL) return NULL;

            w->vanishing_moments_psi = 2*order;
            w->vanishing_moments_phi = 2*order -1;
            w->base.support_width = 6*order - 1;
            w->base.orthogonal = 1;
            w->base.biorthogonal = 1;
            w->base.symmetry = NEAR_SYMMETRIC;
            w->base.compact_support = 1;
            w->base.family_name = "Coiflets";
            w->base.short_name = "coif";
            {
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_float[i] = coif_float[coeffs_idx][i] * sqrt2_float;
                    w->dec_lo_float[i] = coif_float[coeffs_idx][w->dec_len-1-i]
                      * sqrt2_float;
                    w->rec_hi_float[i] = ((i % 2) ? -1 : 1)
                      * coif_float[coeffs_idx][w->dec_len-1-i] * sqrt2_float;
                    w->dec_hi_float[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * coif_float[coeffs_idx][i] * sqrt2_float;
                }
            }

            {
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_double[i] = coif_double[coeffs_idx][i] * sqrt2_double;
                    w->dec_lo_double[i] = coif_double[coeffs_idx][w->dec_len-1-i]
                      * sqrt2_double;
                    w->rec_hi_double[i] = ((i % 2) ? -1 : 1)
                      * coif_double[coeffs_idx][w->dec_len-1-i] * sqrt2_double;
                    w->dec_hi_double[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * coif_double[coeffs_idx][i] * sqrt2_double;
                }
            }
            break;
        }

        /* Biorthogonal wavelets family */
        case BIOR: {
            unsigned int N = order / 10, M = order % 10;
            size_t M_idx;
            size_t M_max;
            switch (N) {
            case 1:
                if (M % 2 != 1 || M > 5) return NULL;
                M_idx = M / 2;
                M_max = 5;
                break;
            case 2:
                if (M % 2 != 0 || M < 2 || M > 8) return NULL;
                M_idx = M / 2 - 1;
                M_max = 8;
                break;
            case 3:
                if (M % 2 != 1) return NULL;
                M_idx = M / 2;
                M_max = 9;
                break;
            case 4:
            case 5:
                if (M != N) return NULL;
                M_idx = 0;
                M_max = M;
                break;
            case 6:
                if (M != 8) return NULL;
                M_idx = 0;
                M_max = 8;
                break;
            default:
                return NULL;
            }

            w = blank_discrete_wavelet((N == 1) ? 2 * M : 2 * M + 2);
            if(w == NULL) return NULL;

            w->vanishing_moments_psi = order/10;
            w->vanishing_moments_phi = order % 10;
            w->base.support_width = -1;
            w->base.orthogonal = 0;
            w->base.biorthogonal = 1;
            w->base.symmetry = SYMMETRIC;
            w->base.compact_support = 1;
            w->base.family_name = "Biorthogonal";
            w->base.short_name = "bior";
            {
                size_t n = M_max - M;
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_float[i] = bior_float[N - 1][0][i+n];
                    w->dec_lo_float[i] = bior_float[N - 1][M_idx+1][w->dec_len-1-i];
                    w->rec_hi_float[i] = ((i % 2) ? -1 : 1)
                      * bior_float[N - 1][M_idx+1][w->dec_len-1-i];
                    w->dec_hi_float[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * bior_float[N - 1][0][i+n];
                }
            }

            {
                size_t n = M_max - M;
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_double[i] = bior_double[N - 1][0][i+n];
                    w->dec_lo_double[i] = bior_double[N - 1][M_idx+1][w->dec_len-1-i];
                    w->rec_hi_double[i] = ((i % 2) ? -1 : 1)
                      * bior_double[N - 1][M_idx+1][w->dec_len-1-i];
                    w->dec_hi_double[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * bior_double[N - 1][0][i+n];
                }
            }

            break;
        }

        /* Discrete FIR filter approximation of Meyer wavelet */
        case DMEY:
            w = blank_discrete_wavelet(62);
            if(w == NULL) return NULL;

            w->vanishing_moments_psi = -1;
            w->vanishing_moments_phi = -1;
            w->base.support_width = -1;
            w->base.orthogonal = 1;
            w->base.biorthogonal = 1;
            w->base.symmetry = SYMMETRIC;
            w->base.compact_support = 1;
            w->base.family_name = "Discrete Meyer (FIR Approximation)";
            w->base.short_name = "dmey";
            {
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_float[i] = dmey_float[i];
                    w->dec_lo_float[i] = dmey_float[w->dec_len-1-i];
                    w->rec_hi_float[i] = ((i % 2) ? -1 : 1)
                      * dmey_float[w->dec_len-1-i];
                    w->dec_hi_float[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * dmey_float[i];
                }
            }

            {
                size_t i;
                for(i = 0; i < w->rec_len; ++i){
                    w->rec_lo_double[i] = dmey_double[i];
                    w->dec_lo_double[i] = dmey_double[w->dec_len-1-i];
                    w->rec_hi_double[i] = ((i % 2) ? -1 : 1)
                      * dmey_double[w->dec_len-1-i];
                    w->dec_hi_double[i] = (((w->dec_len-1-i) % 2) ? -1 : 1)
                      * dmey_double[i];
                }
            }
            break;
        default:
            return NULL;
    }
    return w;
}

ContinuousWavelet* continuous_wavelet(WAVELET_NAME name, unsigned int order)
{
    ContinuousWavelet *w;
    switch(name){
            /* Gaussian Wavelets */
        case GAUS:
            if (order > 8)
                return NULL;
            w = blank_continuous_wavelet();
            if(w == NULL) return NULL;

            w->base.support_width = -1;
            w->base.orthogonal = 0;
            w->base.biorthogonal = 0;
            if (order % 2 == 0)
                w->base.symmetry = SYMMETRIC;
            else
                w->base.symmetry = ANTI_SYMMETRIC;
            w->base.compact_support = 0;
            w->base.family_name = "Gaussian";
            w->base.short_name = "gaus";
            w->complex_cwt = 0;
            w->lower_bound = -5;
            w->upper_bound = 5;
            w->center_frequency = 0;
            w->bandwidth_frequency = 0;
            w->fbsp_order = 0;
            break;
        case MEXH:
            w = blank_continuous_wavelet();
            if(w == NULL) return NULL;

            w->base.support_width = -1;
            w->base.orthogonal = 0;
            w->base.biorthogonal = 0;
            w->base.symmetry = SYMMETRIC;
            w->base.compact_support = 0;
            w->base.family_name = "Mexican hat wavelet";
            w->base.short_name = "mexh";
            w->complex_cwt = 0;
            w->lower_bound = -8;
            w->upper_bound = 8;
            w->center_frequency = 0;
            w->bandwidth_frequency = 0;
            w->fbsp_order = 0;
            break;
        case MORL:
            w = blank_continuous_wavelet();
            if(w == NULL) return NULL;

            w->base.support_width = -1;
            w->base.orthogonal = 0;
            w->base.biorthogonal = 0;
            w->base.symmetry = SYMMETRIC;
            w->base.compact_support = 0;
            w->base.family_name = "Morlet wavelet";
            w->base.short_name = "morl";
            w->complex_cwt = 0;
            w->lower_bound = -8;
            w->upper_bound = 8;
            w->center_frequency = 0;
            w->bandwidth_frequency = 0;
            w->fbsp_order = 0;
            break;
        case CGAU:
            if (order > 8)
                return NULL;
            w = blank_continuous_wavelet();
            if(w == NULL) return NULL;

            w->base.support_width = -1;
            w->base.orthogonal = 0;
            w->base.biorthogonal = 0;
            if (order % 2 == 0)
                w->base.symmetry = SYMMETRIC;
            else
                w->base.symmetry = ANTI_SYMMETRIC;
            w->base.compact_support = 0;
            w->base.family_name = "Complex Gaussian wavelets";
            w->base.short_name = "cgau";
            w->complex_cwt = 1;
            w->lower_bound = -5;
            w->upper_bound = 5;
            w->center_frequency = 0;
            w->bandwidth_frequency = 0;
            w->fbsp_order = 0;
            break;
        case SHAN:

            w = blank_continuous_wavelet();
            if(w == NULL) return NULL;

            w->base.support_width = -1;
            w->base.orthogonal = 0;
            w->base.biorthogonal = 0;
            w->base.symmetry = ASYMMETRIC;
            w->base.compact_support = 0;
            w->base.family_name = "Shannon wavelets";
            w->base.short_name = "shan";
            w->complex_cwt = 1;
            w->lower_bound = -20;
            w->upper_bound = 20;
            w->center_frequency = 1;
            w->bandwidth_frequency = 0.5;
            w->fbsp_order = 0;
            break;
        case FBSP:

            w = blank_continuous_wavelet();
            if(w == NULL) return NULL;

            w->base.support_width = -1;
            w->base.orthogonal = 0;
            w->base.biorthogonal = 0;
            w->base.symmetry = ASYMMETRIC;
            w->base.compact_support = 0;
            w->base.family_name = "Frequency B-Spline wavelets";
            w->base.short_name = "fbsp";
            w->complex_cwt = 1;
            w->lower_bound = -20;
            w->upper_bound = 20;
            w->center_frequency = 0.5;
            w->bandwidth_frequency = 1;
            w->fbsp_order = 2;
            break;
        case CMOR:

            w = blank_continuous_wavelet();
            if(w == NULL) return NULL;

            w->base.support_width = -1;
            w->base.orthogonal = 0;
            w->base.biorthogonal = 0;
            w->base.symmetry = ASYMMETRIC;
            w->base.compact_support = 0;
            w->base.family_name = "Complex Morlet wavelets";
            w->base.short_name = "cmor";
            w->complex_cwt = 1;
            w->lower_bound = -8;
            w->upper_bound = 8;
            w->center_frequency = 0.5;
            w->bandwidth_frequency = 1;
            w->fbsp_order = 0;
            break;
        default:
            return NULL;
    }
    return w;
}


DiscreteWavelet* blank_discrete_wavelet(size_t filters_length)
{
    DiscreteWavelet* w;


    /* pad to even length */
    if(filters_length > 0 && filters_length % 2)
        ++filters_length;

    w = wtmalloc(sizeof(DiscreteWavelet));
    if(w == NULL) return NULL;

    w->dec_len = w->rec_len = filters_length;
    if (filters_length > 0)
    {
        w->dec_lo_float = wtcalloc(filters_length, sizeof(float));
        w->dec_hi_float = wtcalloc(filters_length, sizeof(float));
        w->rec_lo_float = wtcalloc(filters_length, sizeof(float));
        w->rec_hi_float = wtcalloc(filters_length, sizeof(float));

        w->dec_lo_double = wtcalloc(filters_length, sizeof(double));
        w->dec_hi_double = wtcalloc(filters_length, sizeof(double));
        w->rec_lo_double = wtcalloc(filters_length, sizeof(double));
        w->rec_hi_double = wtcalloc(filters_length, sizeof(double));

        if(w->dec_lo_float == NULL || w->dec_hi_float == NULL ||
           w->rec_lo_float == NULL || w->rec_hi_float == NULL ||
           w->dec_lo_double == NULL || w->dec_hi_double == NULL ||
           w->rec_lo_double == NULL || w->rec_hi_double == NULL){
            free_discrete_wavelet(w);
            return NULL;
        }
    }
    else
    {
        w->dec_lo_float = NULL;
        w->dec_hi_float = NULL;
        w->rec_lo_float = NULL;
        w->rec_hi_float = NULL;

        w->dec_lo_double = NULL;
        w->dec_hi_double = NULL;
        w->rec_lo_double = NULL;
        w->rec_hi_double = NULL;
    }
    /* set w->base properties to "blank" values */
    w->base.support_width = -1;
    w->base.orthogonal = 0;
    w->base.biorthogonal = 0;
    w->base.symmetry = UNKNOWN;
    w->base.compact_support = 0;
    w->base.family_name = "";
    w->base.short_name = "";
    w->vanishing_moments_psi = 0;
    w->vanishing_moments_phi = 0;
    return w;
}

ContinuousWavelet* blank_continuous_wavelet(void)
{
    ContinuousWavelet* w;



    w = wtmalloc(sizeof(ContinuousWavelet));
    if(w == NULL) return NULL;


    /* set properties to "blank" values */
    w->center_frequency = -1;
    w->bandwidth_frequency = -1;
    w->fbsp_order = 0;
    return w;
}

DiscreteWavelet* copy_discrete_wavelet(DiscreteWavelet* base)
{
    DiscreteWavelet* w;

    if(base == NULL) return NULL;

    w = wtmalloc(sizeof(DiscreteWavelet));
    if(w == NULL) return NULL;

    memcpy(w, base, sizeof(DiscreteWavelet));
    if (base->dec_len > 0)
    {
        w->dec_lo_float = wtmalloc(w->dec_len * sizeof(float));
        w->dec_hi_float = wtmalloc(w->dec_len * sizeof(float));
        w->dec_lo_double = wtmalloc(w->dec_len * sizeof(double));
        w->dec_hi_double = wtmalloc(w->dec_len * sizeof(double));
        if(w->dec_lo_float == NULL || w->dec_hi_float == NULL ||
            w->dec_lo_double == NULL || w->dec_hi_double == NULL){
           free_discrete_wavelet(w);
           return NULL;
        }
    }
    else
    {
        w->dec_lo_float = NULL;
        w->dec_hi_float = NULL;

        w->dec_lo_double = NULL;
        w->dec_hi_double = NULL;
    }
    if (base->rec_len > 0)
    {
        w->rec_lo_float = wtmalloc(w->rec_len * sizeof(float));
        w->rec_hi_float = wtmalloc(w->rec_len * sizeof(float));
        w->rec_lo_double = wtmalloc(w->rec_len * sizeof(double));
        w->rec_hi_double = wtmalloc(w->rec_len * sizeof(double));
        if( w->rec_lo_float == NULL || w->rec_hi_float == NULL ||
            w->rec_lo_double == NULL || w->rec_hi_double == NULL){
           free_discrete_wavelet(w);
           return NULL;
        }
    }
    else
    {
        w->rec_lo_float = NULL;
        w->rec_hi_float = NULL;

        w->rec_lo_double = NULL;
        w->rec_hi_double = NULL;
    }


    // FIXME: Test coverage, the only use in `wavelet` overwrites the filter
    if (base->dec_len > 0)
    {
        memcpy(w->dec_lo_float, base->dec_lo_float, w->dec_len * sizeof(float));
        memcpy(w->dec_hi_float, base->dec_hi_float, w->dec_len * sizeof(float));
        memcpy(w->dec_lo_double, base->dec_lo_double, w->dec_len * sizeof(double));
        memcpy(w->dec_hi_double, base->dec_hi_double, w->dec_len * sizeof(double));
    }
    if (base->rec_len > 0)
    {
        memcpy(w->rec_lo_float, base->rec_lo_float, w->rec_len * sizeof(float));
        memcpy(w->rec_hi_float, base->rec_hi_float, w->rec_len * sizeof(float));
        memcpy(w->rec_lo_double, base->rec_lo_double, w->rec_len * sizeof(double));
        memcpy(w->rec_hi_double, base->rec_hi_double, w->rec_len * sizeof(double));
    }
    return w;
}

void free_discrete_wavelet(DiscreteWavelet *w){

    /* deallocate filters */
    if (w->dec_len > 0)
    {
        wtfree(w->dec_lo_float);
        wtfree(w->dec_hi_float);
        wtfree(w->dec_lo_double);
        wtfree(w->dec_hi_double);
    }
    if (w->rec_len > 0)
    {
        wtfree(w->rec_lo_float);
        wtfree(w->rec_hi_float);
        wtfree(w->rec_lo_double);
        wtfree(w->rec_hi_double);
    }
    /* finally free struct */
    wtfree(w);
}

void free_continuous_wavelet(ContinuousWavelet *w){

    /* finally free struct */
    wtfree(w);
}







#undef SWAP
#undef NELEMS
