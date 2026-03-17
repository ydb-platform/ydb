/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
/**
 * SECTION:version
 * @Short_description: Version macros.
 * @Stability: Stable
 *
 */
#ifndef __XMLSEC_VERSION_H__
#define __XMLSEC_VERSION_H__

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/**
 * XMLSEC_VERSION:
 *
 * The library version string in the format
 * "$major_number.$minor_number.$sub_minor_number".
 */
#define XMLSEC_VERSION            "1.2.37"

/**
 * XMLSEC_VERSION_MAJOR:
 *
 * The library major version number.
 */
#define XMLSEC_VERSION_MAJOR        1

/**
 * XMLSEC_VERSION_MINOR:
 *
 * The library minor version number.
 */
#define XMLSEC_VERSION_MINOR        2

/**
 * XMLSEC_VERSION_SUBMINOR:
 *
 * The library sub-minor version number.
 */
#define XMLSEC_VERSION_SUBMINOR        37

/**
 * XMLSEC_VERSION_INFO:
 *
 * The library version info string in the format
 * "$major_number+$minor_number:$sub_minor_number:$minor_number".
 */
#define XMLSEC_VERSION_INFO        "3:37:2"


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_VERSION_H__ */

