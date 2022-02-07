#pragma once

//
// See Appendix A.3 at https://tools.ietf.org/html/draft-nir-cfrg-chacha20-poly1305-06
// for source data
//

#include <util/system/types.h>

#define KEY_SIZE 32
#define TAG_SIZE 16


//-----------------------------------------------------------------------------
// TEST CASE #1
//-----------------------------------------------------------------------------

const ui8 tc1_key[KEY_SIZE] = { 0x00 };

const ui8 tc1_data[64] = { 0x00 };

const ui8 tc1_tag[TAG_SIZE] = { 0x00 };

//-----------------------------------------------------------------------------
// TEST CASE #2
//-----------------------------------------------------------------------------

const ui8 tc2_key[KEY_SIZE] = {
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x36, 0xe5, 0xf6, 0xb5, 0xc5, 0xe0, 0x60, 0x70,
    0xf0, 0xef, 0xca, 0x96, 0x22, 0x7a, 0x86, 0x3e,
};

const ui8 tc2_data[] =
    "Any submission to the IETF intended by the Contributor "
    "for publication as all or part of an IETF Internet-Draft"
    "or RFC and any statement made within the context of an IETF "
    "activity is considered an \"IETF Contribution\". Such statements "
    "include oral statements in IETF sessions, as well as written and "
    "electronic communications made at any time or place, which are "
    "addressed to";

const ui8 tc2_tag[TAG_SIZE] = {
    0x36, 0xe5, 0xf6, 0xb5, 0xc5, 0xe0, 0x60, 0x70,
    0xf0, 0xef, 0xca, 0x96, 0x22, 0x7a, 0x86, 0x3e,
};

//-----------------------------------------------------------------------------
// TEST CASE #3
//-----------------------------------------------------------------------------

const ui8 tc3_key[KEY_SIZE] = {
    0x36, 0xe5, 0xf6, 0xb5, 0xc5, 0xe0, 0x60, 0x70,
    0xf0, 0xef, 0xca, 0x96, 0x22, 0x7a, 0x86, 0x3e,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
};

const ui8 tc3_data[] =
    "Any submission to the IETF intended by the Contributor "
    "for publication as all or part of an IETF Internet-Draft "
    "or RFC and any statement made within the context of an IETF "
    "activity is considered an \"IETF Contribution\". Such statements "
    "include oral statements in IETF sessions, as well as written and "
    "electronic communications made at any time or place, which are "
    "addressed to";

const ui8 tc3_tag[TAG_SIZE] = {
    0xf3, 0x47, 0x7e, 0x7c, 0xd9, 0x54, 0x17, 0xaf,
    0x89, 0xa6, 0xb8, 0x79, 0x4c, 0x31, 0x0c, 0xf0,
};

//-----------------------------------------------------------------------------
// TEST CASE #4
//-----------------------------------------------------------------------------

const ui8 tc4_key[KEY_SIZE] = {
    0x1c, 0x92, 0x40, 0xa5, 0xeb, 0x55, 0xd3, 0x8a,
    0xf3, 0x33, 0x88, 0x86, 0x04, 0xf6, 0xb5, 0xf0,
    0x47, 0x39, 0x17, 0xc1, 0x40, 0x2b, 0x80, 0x09,
    0x9d, 0xca, 0x5c, 0xbc, 0x20, 0x70, 0x75, 0xc0,
};

const ui8 tc4_data[] =
    "'Twas brillig, and the slithy toves\n"
    "Did gyre and gimble in the wabe:\n"
    "All mimsy were the borogoves,\n"
    "And the mome raths outgrabe.";

const ui8 tc4_tag[TAG_SIZE] = {
    0x45, 0x41, 0x66, 0x9a, 0x7e, 0xaa, 0xee, 0x61,
    0xe7, 0x08, 0xdc, 0x7c, 0xbc, 0xc5, 0xeb, 0x62,
};
