/* profiles.c --- Definitions of stringprep profiles.
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

#include <config.h>
#include "stringprep.h"
#include "rfc3454.h"

const Stringprep_profiles stringprep_profiles[] = {
  {"Nameprep", stringprep_nameprep},
  {"KRBprep", stringprep_kerberos5},	/* Deprecate? */
  {"Nodeprep", stringprep_xmpp_nodeprep},
  {"Resourceprep", stringprep_xmpp_resourceprep},
  {"plain", stringprep_plain},	/* sasl-anon-00. */
  {"trace", stringprep_trace},	/* sasl-anon-01,02,03. */
  {"SASLprep", stringprep_saslprep},
  {"ISCSIprep", stringprep_iscsi},	/* Obsolete. */
  {"iSCSI", stringprep_iscsi},	/* IANA. */
  {NULL, NULL}
};

/* number of elements within an array */
#define countof(a) (sizeof(a)/sizeof(*(a)))

/* helper for profile definitions */
#define TABLE(x) stringprep_rfc3454_##x, N_STRINGPREP_rfc3454_##x

const Stringprep_profile stringprep_nameprep[] = {
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_1)},
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_2)},
  {STRINGPREP_NFKC, 0, 0, 0},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_1_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_3)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_4)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_5)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_6)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_7)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_9)},
  {STRINGPREP_BIDI, 0, 0, 0},
  {STRINGPREP_BIDI_PROHIBIT_TABLE, ~STRINGPREP_NO_BIDI, TABLE (C_8)},
  {STRINGPREP_BIDI_RAL_TABLE, 0, TABLE (D_1)},
  {STRINGPREP_BIDI_L_TABLE, 0, TABLE (D_2)},
  {STRINGPREP_UNASSIGNED_TABLE, ~STRINGPREP_NO_UNASSIGNED, TABLE (A_1)},
  {0}
};

const Stringprep_profile stringprep_kerberos5[] = {
  /* XXX this is likely to be wrong as the specification is
     a rough draft. */
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_1)},
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_3)},
  {STRINGPREP_NFKC, 0, 0, 0},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_1_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_3)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_4)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_5)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_6)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_7)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_9)},
  {STRINGPREP_BIDI, 0, 0, 0},
  {STRINGPREP_BIDI_PROHIBIT_TABLE, ~STRINGPREP_NO_BIDI, TABLE (C_8)},
  {STRINGPREP_BIDI_RAL_TABLE, 0, TABLE (D_1)},
  {STRINGPREP_BIDI_L_TABLE, 0, TABLE (D_2)},
  {STRINGPREP_UNASSIGNED_TABLE, ~STRINGPREP_NO_UNASSIGNED, TABLE (A_1)},
  {0}
};

const Stringprep_table_element stringprep_xmpp_nodeprep_prohibit[] = {
  {0x000022, 0x000022},		/* #x22 (") */
  {0x000026, 0x000026},		/* #x26 (&) */
  {0x000027, 0x000027},		/* #x27 (') */
  {0x00002F, 0x00002F},		/* #x2F (/) */
  {0x00003A, 0x00003A},		/* #x3A (:) */
  {0x00003C, 0x00003C},		/* #x3C (<) */
  {0x00003E, 0x00003E},		/* #x3E (>) */
  {0x000040, 0x000040},		/* #x40 (@) */
  {0}
};

const Stringprep_profile stringprep_xmpp_nodeprep[] = {
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_1)},
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_2)},
  {STRINGPREP_NFKC, 0, 0, 0},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_1_1)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_1_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_1)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_3)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_4)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_5)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_6)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_7)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_9)},
  {STRINGPREP_PROHIBIT_TABLE, 0, stringprep_xmpp_nodeprep_prohibit,
   countof (stringprep_xmpp_nodeprep_prohibit) - 1},
  {STRINGPREP_BIDI, 0, 0, 0},
  {STRINGPREP_BIDI_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_BIDI_RAL_TABLE, 0, TABLE (D_1)},
  {STRINGPREP_BIDI_L_TABLE, 0, TABLE (D_2)},
  {STRINGPREP_UNASSIGNED_TABLE, ~STRINGPREP_NO_UNASSIGNED, TABLE (A_1)},
  {0}
};

const Stringprep_profile stringprep_xmpp_resourceprep[] = {
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_1)},
  {STRINGPREP_NFKC, 0, 0, 0},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_1_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_1)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_3)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_4)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_5)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_6)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_7)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_9)},
  {STRINGPREP_BIDI, 0, 0, 0},
  {STRINGPREP_BIDI_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_BIDI_RAL_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_1)},
  {STRINGPREP_BIDI_L_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_2)},
  {STRINGPREP_UNASSIGNED_TABLE, ~STRINGPREP_NO_UNASSIGNED, TABLE (A_1)},
  {0}
};

const Stringprep_profile stringprep_plain[] = {
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_1)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_3)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_4)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_5)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_6)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_9)},
  {STRINGPREP_BIDI, 0, 0, 0},
  {STRINGPREP_BIDI_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_BIDI_RAL_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_1)},
  {STRINGPREP_BIDI_L_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_2)},
  {0}
};

const Stringprep_profile stringprep_trace[] = {
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_1)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_3)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_4)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_5)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_6)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_9)},
  {STRINGPREP_BIDI, 0, 0, 0},
  {STRINGPREP_BIDI_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_BIDI_RAL_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_1)},
  {STRINGPREP_BIDI_L_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_2)},
  {0}
};

const Stringprep_table_element stringprep_iscsi_prohibit[] = {
  {0x0000, 0x002C},		/* [ASCII CONTROL CHARACTERS and SPACE through ,] */
  {0x002F, 0x002F},		/* [ASCII /] */
  {0x003B, 0x0040},		/* [ASCII ; through @] */
  {0x005B, 0x0060},		/* [ASCII [ through `] */
  {0x007B, 0x007F},		/* [ASCII { through DEL] */
  {0x3002, 0x3002},		/* ideographic full stop */
  {0}
};

const Stringprep_profile stringprep_iscsi[] = {
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_1)},
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_2)},
  {STRINGPREP_NFKC, 0, 0, 0},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_1_1)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_1_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_1)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_3)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_4)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_5)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_6)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_7)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_9)},
  {STRINGPREP_PROHIBIT_TABLE, 0, stringprep_iscsi_prohibit,
   countof (stringprep_iscsi_prohibit) - 1},
  {STRINGPREP_BIDI, 0, 0, 0},
  {STRINGPREP_BIDI_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_BIDI_RAL_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_1)},
  {STRINGPREP_BIDI_L_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_2)},
  {STRINGPREP_UNASSIGNED_TABLE, ~STRINGPREP_NO_UNASSIGNED, TABLE (A_1)},
  {0}
};

const Stringprep_table_element stringprep_saslprep_space_map[] = {
  {0x00A0, 0x00A0, {0x0020}},	/* 00A0; NO-BREAK SPACE */
  {0x1680, 0x1680, {0x0020}},	/* 1680; OGHAM SPACE MARK */
  {0x2000, 0x200B, {0x0020}},	/* 2000; EN QUAD */
  /* 2001; EM QUAD */
  /* 2002; EN SPACE */
  /* 2003; EM SPACE */
  /* 2004; THREE-PER-EM SPACE */
  /* 2005; FOUR-PER-EM SPACE */
  /* 2006; SIX-PER-EM SPACE */
  /* 2007; FIGURE SPACE */
  /* 2008; PUNCTUATION SPACE */
  /* 2009; THIN SPACE */
  /* 200A; HAIR SPACE */
  /* 200B; ZERO WIDTH SPACE */
  {0x202F, 0x202F, {0x0020}},	/* 202F; NARROW NO-BREAK SPACE */
  {0x205F, 0x205F, {0x0020}},	/* 205F; MEDIUM MATHEMATICAL SPACE */
  {0x3000, 0x3000, {0x0020}},	/* 3000; IDEOGRAPHIC SPACE */
  {0}
};

const Stringprep_profile stringprep_saslprep[] = {
  {STRINGPREP_MAP_TABLE, 0, stringprep_saslprep_space_map,
   countof (stringprep_saslprep_space_map) - 1},
  {STRINGPREP_MAP_TABLE, 0, TABLE (B_1)},
  {STRINGPREP_NFKC, 0, 0, 0},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_1_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_1)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_2_2)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_3)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_4)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_5)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_6)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_7)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_PROHIBIT_TABLE, 0, TABLE (C_9)},
  {STRINGPREP_BIDI, 0, 0, 0},
  {STRINGPREP_BIDI_PROHIBIT_TABLE, 0, TABLE (C_8)},
  {STRINGPREP_BIDI_RAL_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_1)},
  {STRINGPREP_BIDI_L_TABLE, ~STRINGPREP_NO_BIDI, TABLE (D_2)},
  {STRINGPREP_UNASSIGNED_TABLE, ~STRINGPREP_NO_UNASSIGNED, TABLE (A_1)},
  {0}
};
