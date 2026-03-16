#pragma once

#include "common.h"
#include "convolution.h"
#include "wavelets.h"


#ifdef TYPE
#error TYPE should not be defined here.
#else


#define TYPE float
#include "cwt.template.h"
#undef TYPE

#define TYPE double
#include "cwt.template.h"
#undef TYPE

#endif /* TYPE */
