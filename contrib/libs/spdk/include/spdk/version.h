/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
 *   Copyright (c) 2019 Mellanox Technologies LTD. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/** \file
 * SPDK version number definitions
 */

#ifndef SPDK_VERSION_H
#define SPDK_VERSION_H

/**
 * Major version number (year of original release minus 2000).
 */
#define SPDK_VERSION_MAJOR	21

/**
 * Minor version number (month of original release).
 */
#define SPDK_VERSION_MINOR	4

/**
 * Patch level.
 *
 * Patch level is incremented on maintenance branch releases and reset to 0 for each
 * new major.minor release.
 */
#define SPDK_VERSION_PATCH	0

/**
 * Version string suffix.
 */
#define SPDK_VERSION_SUFFIX	""

/**
 * Single numeric value representing a version number for compile-time comparisons.
 *
 * Example usage:
 *
 * \code
 * #if SPDK_VERSION >= SPDK_VERSION_NUM(17, 7, 0)
 *   Use feature from SPDK v17.07
 * #endif
 * \endcode
 */
#define SPDK_VERSION_NUM(major, minor, patch) \
	(((major) * 100 + (minor)) * 100 + (patch))

/**
 * Current version as a SPDK_VERSION_NUM.
 */
#define SPDK_VERSION	SPDK_VERSION_NUM(SPDK_VERSION_MAJOR, SPDK_VERSION_MINOR, SPDK_VERSION_PATCH)

#define SPDK_VERSION_STRINGIFY_x(x)	#x
#define SPDK_VERSION_STRINGIFY(x)	SPDK_VERSION_STRINGIFY_x(x)

#define SPDK_VERSION_MAJOR_STRING	SPDK_VERSION_STRINGIFY(SPDK_VERSION_MAJOR)

#if SPDK_VERSION_MINOR < 10
#define SPDK_VERSION_MINOR_STRING	".0" SPDK_VERSION_STRINGIFY(SPDK_VERSION_MINOR)
#else
#define SPDK_VERSION_MINOR_STRING	"." SPDK_VERSION_STRINGIFY(SPDK_VERSION_MINOR)
#endif

#if SPDK_VERSION_PATCH != 0
#define SPDK_VERSION_PATCH_STRING	"." SPDK_VERSION_STRINGIFY(SPDK_VERSION_PATCH)
#else
#define SPDK_VERSION_PATCH_STRING	""
#endif

#ifdef SPDK_GIT_COMMIT
#define SPDK_GIT_COMMIT_STRING SPDK_VERSION_STRINGIFY(SPDK_GIT_COMMIT)
#define SPDK_GIT_COMMIT_STRING_SHA1 " git sha1 " SPDK_GIT_COMMIT_STRING
#else
#define SPDK_GIT_COMMIT_STRING ""
#define SPDK_GIT_COMMIT_STRING_SHA1 ""
#endif

/**
 * Human-readable version string.
 */
#define SPDK_VERSION_STRING	\
	"SPDK v" \
	SPDK_VERSION_MAJOR_STRING \
	SPDK_VERSION_MINOR_STRING \
	SPDK_VERSION_PATCH_STRING \
	SPDK_VERSION_SUFFIX \
	SPDK_GIT_COMMIT_STRING_SHA1

#endif
