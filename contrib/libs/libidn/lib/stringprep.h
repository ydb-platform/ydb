/* stringprep.h --- Header file for stringprep functions.
   Copyright (C) 2002-2024 Simon Josefsson

   This file is part of GNU Libidn.

   GNU Libidn is free software: you can redistribute it and/or
   modify it under the terms of either:

     * the GNU Lesser General Public License as published by the Free
       Software Foundation; either version 3 of the License, or (at
       your option) any later version.

   or

     * the GNU General Public License as published by the Free
       Software Foundation; either version 2 of the License, or (at
       your option) any later version.

   or both in parallel, as here.

   GNU Libidn is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received copies of the GNU General Public License and
   the GNU Lesser General Public License along with this program.  If
   not, see <https://www.gnu.org/licenses/>. */

#ifndef STRINGPREP_H
# define STRINGPREP_H

/**
 * SECTION:stringprep
 * @title: stringprep.h
 * @short_description: Stringprep-related functions
 *
 * Stringprep-related functions.
 */

# ifndef IDNAPI
#   define IDNAPI
# endif

# include <stddef.h>		/* size_t */
# include <sys/types.h>		/* ssize_t */
# include <idn-int.h>		/* uint32_t */

# ifdef __cplusplus
extern "C"
{
# endif

# define STRINGPREP_VERSION "1.42"

/* Error codes. */
  typedef enum
  {
    STRINGPREP_OK = 0,
    /* Stringprep errors. */
    STRINGPREP_CONTAINS_UNASSIGNED = 1,
    STRINGPREP_CONTAINS_PROHIBITED = 2,
    STRINGPREP_BIDI_BOTH_L_AND_RAL = 3,
    STRINGPREP_BIDI_LEADTRAIL_NOT_RAL = 4,
    STRINGPREP_BIDI_CONTAINS_PROHIBITED = 5,
    /* Error in calling application. */
    STRINGPREP_TOO_SMALL_BUFFER = 100,
    STRINGPREP_PROFILE_ERROR = 101,
    STRINGPREP_FLAG_ERROR = 102,
    STRINGPREP_UNKNOWN_PROFILE = 103,
    STRINGPREP_ICONV_ERROR = 104,
    /* Internal errors. */
    STRINGPREP_NFKC_FAILED = 200,
    STRINGPREP_MALLOC_ERROR = 201
  } Stringprep_rc;

/* Flags used when calling stringprep(). */
  typedef enum
  {
    STRINGPREP_NO_NFKC = 1,
    STRINGPREP_NO_BIDI = 2,
    STRINGPREP_NO_UNASSIGNED = 4
  } Stringprep_profile_flags;

/* Steps in a stringprep profile. */
  typedef enum
  {
    STRINGPREP_NFKC = 1,
    STRINGPREP_BIDI = 2,
    STRINGPREP_MAP_TABLE = 3,
    STRINGPREP_UNASSIGNED_TABLE = 4,
    STRINGPREP_PROHIBIT_TABLE = 5,
    STRINGPREP_BIDI_PROHIBIT_TABLE = 6,
    STRINGPREP_BIDI_RAL_TABLE = 7,
    STRINGPREP_BIDI_L_TABLE = 8
  } Stringprep_profile_steps;

# define STRINGPREP_MAX_MAP_CHARS 4

  /* *INDENT-OFF* */

  /* Why INDENT-OFF?  GTK-DOC has a bug
   * <https://gitlab.gnome.org/GNOME/gtk-doc/-/issues/37> which causes
   * parsing of structs to fail unless the terminating } is at the
   * beginning of the line.  We hard-code the header file to be like
   * that, and add the INDENT-OFF markers so that indent won't restore
   * them.  When that bug is fixed, remove the INDENT-* marker, run
   * 'make indent', and make sure that
   * doc/reference/libidn-decl-list.txt stay the same.
   *
   * Of course, exposing these struct's in the public header file in
   * the first place was a mistake.
   */

  /**
   * Stringprep_table_element:
   * @start: starting codepoint.
   * @end: ending codepoint, 0 if only one character.
   * @map: codepoints to map @start into, NULL if end is not 0.
   *
   * Stringprep profile table element.
   */
  struct Stringprep_table_element
  {
    uint32_t start;
    uint32_t end;
    uint32_t map[STRINGPREP_MAX_MAP_CHARS];
};
  typedef struct Stringprep_table_element Stringprep_table_element;

  /**
   * Stringprep_table:
   * @operation: a #Stringprep_profile_steps value
   * @flags: a #Stringprep_profile_flags value
   * @table: zero-terminated array of %Stringprep_table_element elements.
   * @table_size: size of @table, to speed up searching.
   *
   * Stringprep profile table.
   */
  struct Stringprep_table
  {
    Stringprep_profile_steps operation;
    Stringprep_profile_flags flags;
    const Stringprep_table_element *table;
    size_t table_size;
};
  /**
   * Stringprep_profile:
   *
   * Stringprep profile table.
   */
  typedef struct Stringprep_table Stringprep_profile;

  /**
   * Stringprep_profiles:
   * @name: name of stringprep profile.
   * @tables: zero-terminated array of %Stringprep_profile elements.
   *
   * Element structure
   */
  struct Stringprep_profiles
  {
    const char *name;
    const Stringprep_profile *tables;
};
  typedef struct Stringprep_profiles Stringprep_profiles;
  /* *INDENT-ON* */

  extern IDNAPI const Stringprep_profiles stringprep_profiles[];

/* Profiles */
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_A_1[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_B_1[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_B_2[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_B_3[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_1_1[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_1_2[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_2_1[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_2_2[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_3[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_4[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_5[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_6[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_7[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_8[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_C_9[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_D_1[];
  extern IDNAPI const Stringprep_table_element stringprep_rfc3454_D_2[];

  /* Nameprep */

  extern IDNAPI const Stringprep_profile stringprep_nameprep[];

# define stringprep_nameprep(in, maxlen)			\
  stringprep(in, maxlen, 0, stringprep_nameprep)

# define stringprep_nameprep_no_unassigned(in, maxlen)			\
  stringprep(in, maxlen, STRINGPREP_NO_UNASSIGNED, stringprep_nameprep)

  /* SASL */

  extern IDNAPI const Stringprep_profile stringprep_saslprep[];
  extern IDNAPI const Stringprep_table_element
    stringprep_saslprep_space_map[];
  extern IDNAPI const Stringprep_profile stringprep_plain[];
  extern IDNAPI const Stringprep_profile stringprep_trace[];

# define stringprep_plain(in, maxlen)		\
  stringprep(in, maxlen, 0, stringprep_plain)

  /* Kerberos */

  extern IDNAPI const Stringprep_profile stringprep_kerberos5[];

# define stringprep_kerberos5(in, maxlen)		\
  stringprep(in, maxlen, 0, stringprep_kerberos5)

  /* XMPP */

  extern IDNAPI const Stringprep_profile stringprep_xmpp_nodeprep[];
  extern IDNAPI const Stringprep_profile stringprep_xmpp_resourceprep[];
  extern IDNAPI const Stringprep_table_element
    stringprep_xmpp_nodeprep_prohibit[];

# define stringprep_xmpp_nodeprep(in, maxlen)		\
  stringprep(in, maxlen, 0, stringprep_xmpp_nodeprep)
# define stringprep_xmpp_resourceprep(in, maxlen)		\
  stringprep(in, maxlen, 0, stringprep_xmpp_resourceprep)

  /* iSCSI */

  extern IDNAPI const Stringprep_profile stringprep_iscsi[];
  extern IDNAPI const Stringprep_table_element stringprep_iscsi_prohibit[];

# define stringprep_iscsi(in, maxlen)		\
  stringprep(in, maxlen, 0, stringprep_iscsi)

  /* API */

  extern IDNAPI int stringprep_4i (uint32_t * ucs4, size_t *len,
				   size_t maxucs4len,
				   Stringprep_profile_flags flags,
				   const Stringprep_profile * profile);
  extern IDNAPI int stringprep_4zi (uint32_t * ucs4, size_t maxucs4len,
				    Stringprep_profile_flags flags,
				    const Stringprep_profile * profile);
  extern IDNAPI int stringprep (char *in, size_t maxlen,
				Stringprep_profile_flags flags,
				const Stringprep_profile * profile);

  extern IDNAPI int stringprep_profile (const char *in,
					char **out,
					const char *profile,
					Stringprep_profile_flags flags);

  extern IDNAPI const char *stringprep_strerror (Stringprep_rc rc);

  extern IDNAPI const char *stringprep_check_version (const char
						      *req_version);

/* Utility */

  extern IDNAPI int stringprep_unichar_to_utf8 (uint32_t c, char *outbuf);
  extern IDNAPI uint32_t stringprep_utf8_to_unichar (const char *p);

  extern IDNAPI uint32_t *stringprep_utf8_to_ucs4 (const char *str,
						   ssize_t len,
						   size_t *items_written);
  extern IDNAPI char *stringprep_ucs4_to_utf8 (const uint32_t * str,
					       ssize_t len,
					       size_t *items_read,
					       size_t *items_written);

  extern IDNAPI char *stringprep_utf8_nfkc_normalize (const char *str,
						      ssize_t len);
  extern IDNAPI uint32_t *stringprep_ucs4_nfkc_normalize (const uint32_t *
							  str, ssize_t len);

  extern IDNAPI const char *stringprep_locale_charset (void);
  extern IDNAPI char *stringprep_convert (const char *str,
					  const char *to_codeset,
					  const char *from_codeset);
  extern IDNAPI char *stringprep_locale_to_utf8 (const char *str);
  extern IDNAPI char *stringprep_utf8_to_locale (const char *str);

# ifdef __cplusplus
}
# endif

#endif				/* STRINGPREP_H */
