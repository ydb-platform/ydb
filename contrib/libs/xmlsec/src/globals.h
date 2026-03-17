/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Internal header only used during the compilation,
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */

#ifndef __XMLSEC_GLOBALS_H__
#define __XMLSEC_GLOBALS_H__

/**
 * Use autoconf defines if present.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif /* HAVE_CONFIG_H */

#define IN_XMLSEC
#define XMLSEC_PRIVATE

/* Include common error helper macros. */
#include "errors_helpers.h"

#endif /* __XMLSEC_GLOBALS_H__ */
