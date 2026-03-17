/* Copyright (c) 2016 Holger Nahrstaedt */
/* See COPYING for license details. */


#include "templating.h"

#ifndef TYPE
#error TYPE must be defined here.
#else

#include "cwt.h"

#if defined _MSC_VER
#define restrict __restrict
#elif defined __GNUC__
#define restrict __restrict__
#endif


void CAT(TYPE, _gaus)(const TYPE * const restrict input,
                              TYPE * const restrict output, const size_t N,
                              const size_t number);


void CAT(TYPE, _mexh)(const TYPE * const restrict input, TYPE * const restrict output, const size_t N);

void CAT(TYPE, _morl)(const TYPE * const restrict input, TYPE * const restrict output, const size_t N);

void CAT(TYPE, _cgau)(const TYPE * const restrict input,
                              TYPE * const restrict output_r, TYPE * const restrict output_i, const size_t N,
                              const size_t number);


void CAT(TYPE, _shan)(const TYPE * const restrict input, TYPE * const restrict output_r, TYPE * const restrict output_i, const size_t N,
                              const TYPE  FB, const TYPE  FC);

void CAT(TYPE, _fbsp)(const TYPE * const restrict input, TYPE * const restrict output_r, TYPE * const restrict output_i, const size_t N,
                              const unsigned int M, const TYPE  FB, const TYPE  FC);

void CAT(TYPE, _cmor)(const TYPE * const restrict input, TYPE * const restrict output_r, TYPE * const restrict output_i, const size_t N,
                              const TYPE  FB, const TYPE  FC);
#endif /* TYPE */
#undef restrict
