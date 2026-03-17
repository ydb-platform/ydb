/*
 * Header file with preprocessor magic to figure out, which compiler the user has been calling!
 *
 * This code is adapted from the file other/portable_platform.h of GASnet-1.14.0:
 *  - Ripping out the required parts.
 *  - Get rid of brackets as it messes up autoconf
 *  - Delete version tests for older PGI versions (#include "omp.h" not acceptabe)
 *  - Indent ('#' should be in column 0)
 *
 * External packages (i.e., romio) depend on top_build_dir/ompi/include, therefore
 * although this is not changed in the configure process, this has to be set as
 * a .in file...
 * ---------------------------------------------------------------------------
 */
#ifndef OPAL_PORTABLE_PLATFORM_H
#define OPAL_PORTABLE_PLATFORM_H

/* All files in this directory and all sub-directories (except where otherwise noted)
 * are subject to the following licensing terms:
 *
 * ---------------------------------------------------------------------------
 * "Copyright (c) 2000-2003 The Regents of the University of California.
 * All rights reserved.
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without written agreement is
 * hereby granted, provided that the above copyright notice and the following
 * two paragraphs appear in all copies of this software.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES ARISING OUT
 * OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE UNIVERSITY OF
 * CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATION TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS."
 * ---------------------------------------------------------------------------
 *
 * Please see the license.txt files within the gm-conduit, lapi-conduit and
 * vapi-conduit directories for the licensing terms governing those
 * contributed components.
 *
 * The authors/contributors of GASNet include:
 *
 *  Dan Bonachea <bonachea@cs.berkeley.edu>:
 *    General infrastructure & documentation
 *    mpi-conduit
 *    elan-conduit
 *    smp-conduit
 *    udp-conduit
 *    extended-ref
 *    template-conduit
 *  Christian Bell <csbell@cs.berkeley.edu>: gm-conduit, shmem-conduit
 *  Mike Welcome <mlwelcome@lbl.gov>: lapi-conduit, portals-conduit
 *  Paul H. Hargrove <phhargrove@lbl.gov>: vapi-conduit, ibv-conduit
 *  Rajesh Nishtala <rajeshn@cs.berkeley.edu>: collectives, dcmf-conduit
 *  Parry Husbands (PJRHusbands@lbl.gov): lapi-conduit
 *
 * For more information about GASNet, visit our home page at:
 *  http://gasnet.cs.berkeley.edu/
 * Or send email to:
 *  <upc@lbl.gov>
 *
 * Source code contributions (fixes, patches, extensions etc.) should be
 * sent to <upc@lbl.gov> to be reviewed for acceptance into the primary
 * distribution. Contributions are most likely to be accepted if they
 * are provided as public domain, or under a BSD-style license such as
 * the one above.
 *
 */
#ifndef _STRINGIFY
#define _STRINGIFY_HELPER(x) #x
#define _STRINGIFY(x) _STRINGIFY_HELPER(x)
#endif

#if defined(__INTEL_COMPILER)
#    define PLATFORM_COMPILER_FAMILYNAME INTEL
#    define PLATFORM_COMPILER_FAMILYID 2
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_INTEL_CXX  1
#    else
#        define PLATFORM_COMPILER_INTEL_C  1
#    endif
#    define _PLATFORM_COMPILER_INTEL_MIN_BUILDDATE 19700000 /* year 1970: predates most intel products :) */
#    ifdef __INTEL_COMPILER_BUILD_DATE
#        define _PLATFORM_INTEL_COMPILER_BUILD_DATE __INTEL_COMPILER_BUILD_DATE
#    else
#        define _PLATFORM_INTEL_COMPILER_BUILD_DATE _PLATFORM_COMPILER_INTEL_MIN_BUILDDATE
#    endif
     /* patch number is a decimal build date: YYYYMMDD */
#    define PLATFORM_COMPILER_VERSION_INT(maj,min,pat)         \
         (((((maj) * 10) | (min)) << 20) |                      \
            ((pat) < _PLATFORM_COMPILER_INTEL_MIN_BUILDDATE ?   \
             _PLATFORM_COMPILER_INTEL_MIN_BUILDDATE : ((pat)-_PLATFORM_COMPILER_INTEL_MIN_BUILDDATE)))
#    define PLATFORM_COMPILER_VERSION \
         PLATFORM_COMPILER_VERSION_INT(__INTEL_COMPILER/10, __INTEL_COMPILER/100, _PLATFORM_INTEL_COMPILER_BUILD_DATE)
#    define PLATFORM_COMPILER_VERSION_STR \
         _STRINGIFY(__INTEL_COMPILER) "." _STRINGIFY(_PLATFORM_INTEL_COMPILER_BUILD_DATE)

#elif defined(__PATHSCALE__)
#    define PLATFORM_COMPILER_PATHSCALE  1
#    define PLATFORM_COMPILER_FAMILYNAME PATHSCALE
#    define PLATFORM_COMPILER_FAMILYID 3
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_PATHSCALE_CXX  1
#    else
#        define PLATFORM_COMPILER_PATHSCALE_C  1
#    endif
#    define PLATFORM_COMPILER_VERSION \
         PLATFORM_COMPILER_VERSION_INT(__PATHCC__,__PATHCC_MINOR__,__PATHCC_PATCHLEVEL__)
#    define PLATFORM_COMPILER_VERSION_STR __PATHSCALE__

#elif defined(__PGI)
#    define PLATFORM_COMPILER_PGI  1
#    define PLATFORM_COMPILER_FAMILYNAME PGI
#    define PLATFORM_COMPILER_FAMILYID 4
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_PGI_CXX  1
#    else
#        define PLATFORM_COMPILER_PGI_C  1
#    endif
#    if __PGIC__ == 99
      /* bug 2230: PGI versioning was broken for some platforms in 7.0
         no way to know exact version, but provide something slightly more accurate */
#      define PLATFORM_COMPILER_VERSION 0x070000
#      define PLATFORM_COMPILER_VERSION_STR "7.?-?"
#    elif defined(__PGIC__) && defined(__PGIC_MINOR__) && defined(__PGIC_PATCHLEVEL__)
#      define PLATFORM_COMPILER_VERSION \
            PLATFORM_COMPILER_VERSION_INT(__PGIC__,__PGIC_MINOR__,__PGIC_PATCHLEVEL__)
#      define PLATFORM_COMPILER_VERSION_STR \
            _STRINGIFY(__PGIC__) "." _STRINGIFY(__PGIC_MINOR__) "-" _STRINGIFY(__PGIC_PATCHLEVEL__)
#    else
       /* PGI before 6.1-4 lacks any version ID preprocessor macros - so use this filthy hack */
       /* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        * We cannot do these within mpi.h.in, as we should not include ompi.h
        * Hopefully, compilers with integrated preprocessors will not analyse code within the #if 0-block
        * XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        */
#if 0
#        ifdef PLATFORM_PGI_IS_ANCIENT
         /* Include below might fail for ancient versions lacking this header, but testing shows it
            works back to at least 5.1-3 (Nov 2003), and based on docs probably back to 3.2 (Sep 2000) */
#            define PLATFORM_COMPILER_VERSION 0
#        elif defined(__x86_64__) /* bug 1753 - 64-bit omp.h upgrade happenned in <6.0-8,6.1-1) */
#            include "omp.h"
#            if defined(_PGOMP_H)
                 /* 6.1.1 or newer */
#                define PLATFORM_COMPILER_VERSION 0x060101
#                define PLATFORM_COMPILER_VERSION_STR ">=6.1-1"
#            else
                 /* 6.0.8 or older */
#                define PLATFORM_COMPILER_VERSION 0
#                define PLATFORM_COMPILER_VERSION_STR "<=6.0-8"
#            endif
#        else /* 32-bit omp.h upgrade happenned in <5.2-4,6.0-8 */
#            include "omp.h"
#            if defined(_PGOMP_H)
                 /* 6.0-8 or newer */
#                define PLATFORM_COMPILER_VERSION 0x060008
#                define PLATFORM_COMPILER_VERSION_STR ">=6.0-8"
#            else
                 /* 5.2-4 or older */
#                define PLATFORM_COMPILER_VERSION 0
#                define PLATFORM_COMPILER_VERSION_STR "<=5.2-4"
#            endif
#        endif
#endif /* 0 */
       /* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */
#    endif

#elif defined(__xlC__)
#    define PLATFORM_COMPILER_XLC  1
#    define PLATFORM_COMPILER_FAMILYNAME XLC
#    define PLATFORM_COMPILER_FAMILYID 5
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_XLC_CXX  1
#    else
#        define PLATFORM_COMPILER_XLC_C  1
#    endif
#    define PLATFORM_COMPILER_VERSION __xlC__
#    define PLATFORM_COMPILER_VERSION_INT(maj,min,pat) \
         ( ((maj) << 8) | ((min) << 4) | (pat) )

#elif defined(__DECC) || defined(__DECCXX)
#    define PLATFORM_COMPILER_COMPAQ  1
#    define PLATFORM_COMPILER_FAMILYNAME COMPAQ
#    define PLATFORM_COMPILER_FAMILYID 6
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_COMPAQ_CXX  1
#    else
#        define PLATFORM_COMPILER_COMPAQ_C  1
#    endif
#    if defined(__DECC_VER)
#        define PLATFORM_COMPILER_VERSION __DECC_VER
#    elif defined(__DECCXX_VER)
#        define PLATFORM_COMPILER_VERSION __DECCXX_VER
#    endif

#    define PLATFORM_COMPILER_VERSION_INT(maj,min,pat) \
         ( ((maj) * 10000000) + ((min) * 100000) + (90000) + (pat) )
         /* 90000 = official ver, 80000 = customer special ver, 60000 = field test ver */

#elif defined(__SUNPRO_C) || defined(__SUNPRO_CC)
#    define PLATFORM_COMPILER_SUN  1
#    define PLATFORM_COMPILER_FAMILYNAME SUN
#    define PLATFORM_COMPILER_FAMILYID 7
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_SUN_CXX  1
#    else
#        define PLATFORM_COMPILER_SUN_C  1
#    endif
#    if defined(__SUNPRO_C) && __SUNPRO_C > 0
#        define PLATFORM_COMPILER_VERSION __SUNPRO_C
#    elif defined(__SUNPRO_CC) && __SUNPRO_CC > 0
#        define PLATFORM_COMPILER_VERSION __SUNPRO_CC
#    endif
#    define PLATFORM_COMPILER_VERSION_INT(maj,min,pat) \
         ( ((maj) << 8) | ((min) << 4) | (pat) )

#elif defined(__HP_cc) || defined(__HP_aCC)
#    define PLATFORM_COMPILER_HP  1
#    define PLATFORM_COMPILER_FAMILYNAME HP
#    define PLATFORM_COMPILER_FAMILYID 8
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_HP_CXX  1
#    else
#        define PLATFORM_COMPILER_HP_C  1
#    endif
#    if defined(__HP_cc) && __HP_cc > 0
#        define PLATFORM_COMPILER_VERSION __HP_cc
#    elif defined(__HP_aCC) && __HP_aCC > 0
#        define PLATFORM_COMPILER_VERSION __HP_aCC
#    endif
#    define PLATFORM_COMPILER_VERSION_INT(maj,min,pat) \
         ( ((maj) << 16) | ((min) << 8) | (pat) )

#elif defined(_SGI_COMPILER_VERSION) || \
   (defined(_COMPILER_VERSION) && defined(__sgi) && !defined(__GNUC__)) /* 7.3.0 and earlier lack _SGI_COMPILER_VERSION */
#    define PLATFORM_COMPILER_SGI  1
#    define PLATFORM_COMPILER_FAMILYNAME SGI
#    define PLATFORM_COMPILER_FAMILYID 9
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_SGI_CXX  1
#    else
#        define PLATFORM_COMPILER_SGI_C  1
#    endif
#    if defined(_SGI_COMPILER_VERSION) && _SGI_COMPILER_VERSION > 0
#        define PLATFORM_COMPILER_VERSION _SGI_COMPILER_VERSION
#    elif defined(_COMPILER_VERSION) && _COMPILER_VERSION > 0
#        define PLATFORM_COMPILER_VERSION _COMPILER_VERSION
#    endif
#    define PLATFORM_COMPILER_VERSION_INT(maj,min,pat) \
         ( ((maj) << 8) | ((min) << 4) | (pat) )

#elif defined(_CRAYC)
#    define PLATFORM_COMPILER_CRAY  1
#    define PLATFORM_COMPILER_FAMILYNAME CRAY
#    define PLATFORM_COMPILER_FAMILYID 10
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_CRAY_CXX  1
#    else
#        define PLATFORM_COMPILER_CRAY_C  1
#    endif
#    if defined(_RELEASE) && defined(_RELEASE_MINOR) /* X1 and XT */
#        define PLATFORM_COMPILER_VERSION \
             PLATFORM_COMPILER_VERSION_INT(_RELEASE,_RELEASE_MINOR,0)
#     elif defined(_RELEASE) /* T3E */
#        define PLATFORM_COMPILER_VERSION \
             PLATFORM_COMPILER_VERSION_INT(_RELEASE,0,0)
#     endif
#     ifdef _RELEASE_STRING /* X1 and XT */
#        define PLATFORM_COMPILER_VERSION_STR _RELEASE_STRING
#     endif

#elif defined(__KCC)
#    define PLATFORM_COMPILER_KAI  1
#    define PLATFORM_COMPILER_FAMILYNAME KAI
#    define PLATFORM_COMPILER_FAMILYID 11
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_KAI_CXX  1
#    else
#        define PLATFORM_COMPILER_KAI_C  1
#    endif

#elif defined(__MTA__)
#    define PLATFORM_COMPILER_MTA  1
#    define PLATFORM_COMPILER_FAMILYNAME MTA
#    define PLATFORM_COMPILER_FAMILYID 12
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_MTA_CXX  1
#    else
#        define PLATFORM_COMPILER_MTA_C  1
#    endif

#elif defined(_SX)
#    define PLATFORM_COMPILER_NECSX  1
#    define PLATFORM_COMPILER_FAMILYNAME NECSX
#    define PLATFORM_COMPILER_FAMILYID 13
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_NECSX_CXX  1
#    else
#        define PLATFORM_COMPILER_NECSX_C  1
#    endif

#elif defined(_MSC_VER)
#    define PLATFORM_COMPILER_MICROSOFT  1
#    define PLATFORM_COMPILER_FAMILYNAME MICROSOFT
#    define PLATFORM_COMPILER_FAMILYID 14
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_MICROSOFT_CXX  1
#    else
#        define PLATFORM_COMPILER_MICROSOFT_C  1
#    endif
#    define PLATFORM_COMPILER_VERSION _MSC_VER

#elif defined(__TINYC__)
#    define PLATFORM_COMPILER_TINY  1
#    define PLATFORM_COMPILER_FAMILYNAME TINY
#    define PLATFORM_COMPILER_FAMILYID 15
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_TINY_CXX  1
#    else
#        define PLATFORM_COMPILER_TINY_C  1
#    endif

#elif defined(__LCC__)
#    define PLATFORM_COMPILER_LCC 1
#    define PLATFORM_COMPILER_FAMILYNAME LCC
#    define PLATFORM_COMPILER_FAMILYID 16
#    ifdef __cplusplus
#        define PLATFORM_COMPILER_LCC_CXX  1
#    else
#        define PLATFORM_COMPILER_LCC_C  1
#    endif

#else /* unknown compiler */
#     define PLATFORM_COMPILER_UNKNOWN  1
#endif

/* this stanza comes last, because many vendor compilers lie and claim
   to be GNU C for compatibility reasons and/or because they share a frontend */
#if defined(__GNUC__)
#    undef PLATFORM_COMPILER_UNKNOWN
#    ifndef PLATFORM_COMPILER_FAMILYID
#        define PLATFORM_COMPILER_GNU  1
#        define PLATFORM_COMPILER_FAMILYNAME GNU
#        define PLATFORM_COMPILER_FAMILYID 1
#        ifdef __cplusplus
#            define PLATFORM_COMPILER_GNU_CXX  1
#        else
#            define PLATFORM_COMPILER_GNU_C  1
#        endif
#        if defined(__GNUC_MINOR__) && defined(__GNUC_PATCHLEVEL__)
#            define PLATFORM_COMPILER_VERSION \
                 PLATFORM_COMPILER_VERSION_INT(__GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__)
#        elif defined(__GNUC_MINOR__) /* older versions of egcs lack __GNUC_PATCHLEVEL__ */
#            define PLATFORM_COMPILER_VERSION \
                 PLATFORM_COMPILER_VERSION_INT(__GNUC__,__GNUC_MINOR__,0)
#        else
#            define PLATFORM_COMPILER_VERSION \
                 PLATFORM_COMPILER_VERSION_INT(__GNUC__,0,0)
#        endif
#        define PLATFORM_COMPILER_VERSION_STR __PLATFORM_COMPILER_GNU_VERSION_STR
#    else
#        define _PLATFORM_COMPILER_GNU_VERSION_STR __PLATFORM_COMPILER_GNU_VERSION_STR
#    endif
     /* gather any advertised GNU version number info, even for non-gcc compilers */
#    if defined(__GNUC_MINOR__) && defined(__GNUC_PATCHLEVEL__)
#        define __PLATFORM_COMPILER_GNU_VERSION_STR \
             _STRINGIFY(__GNUC__) "." _STRINGIFY(__GNUC_MINOR__) "." _STRINGIFY(__GNUC_PATCHLEVEL__)
#    elif defined(__GNUC_MINOR__)
#        define __PLATFORM_COMPILER_GNU_VERSION_STR \
             _STRINGIFY(__GNUC__) "." _STRINGIFY(__GNUC_MINOR__) ".?"
#    else
#        define __PLATFORM_COMPILER_GNU_VERSION_STR \
             _STRINGIFY(__GNUC__) ".?.?"
#    endif
#elif defined(PLATFORM_COMPILER_UNKNOWN) /* unknown compiler */
#    define PLATFORM_COMPILER_FAMILYNAME UNKNOWN
#    define PLATFORM_COMPILER_FAMILYID 0
#endif

/* Default Values */
#ifndef PLATFORM_COMPILER_VERSION
#    define PLATFORM_COMPILER_VERSION 0 /* don't know */
#endif

#ifndef PLATFORM_COMPILER_VERSION_STR
#    define PLATFORM_COMPILER_VERSION_STR _STRINGIFY(PLATFORM_COMPILER_VERSION)
#endif

#ifndef PLATFORM_COMPILER_VERSION_INT
#    define PLATFORM_COMPILER_VERSION_INT(maj,min,pat) \
        (((maj) << 16) | ((min) << 8) | (pat))
#endif


#endif /* OPAL_PORTABLE_PLATFORM_H */
