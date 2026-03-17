/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* NOTE!
 *
 * If you make any changes to H5LTanalyze.l, please run bin/genparser to
 * recreate the output files.
 */

%top{
/* Quiet warnings about integer type macro redefinitions on Visual Studio
 * (MSVC doesn't define STDC_VERSION, but has inttypes.h). This is an
 * issue that is apparently fixed in flex 2.6.5.
 */
#include <stdint.h>
}

%{
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <hdf5.h>

#include "H5private.h"
#include "H5LTparse.h"

static char *trim_quotes(const char *);
static int my_yyinput(char *, int);
#undef YY_INPUT
#define YY_INPUT(b, r, ms) (r=my_yyinput(b, ms))
#define token(x)        (int)x
#define hid(x)          (hid_t)x

#ifdef  YY_BUF_SIZE
#undef  YY_BUF_SIZE
#endif
#define YY_BUF_SIZE        262144    /*Define read buffer to be 256K*/

extern char *myinput;
extern size_t input_len;

%}

%%

H5T_STD_I8BE     {return hid(H5T_STD_I8BE_TOKEN);}
H5T_STD_I8LE     {return hid(H5T_STD_I8LE_TOKEN);}
H5T_STD_I16BE    {return hid(H5T_STD_I16BE_TOKEN);}
H5T_STD_I16LE    {return hid(H5T_STD_I16LE_TOKEN);}
H5T_STD_I32BE    {return hid(H5T_STD_I32BE_TOKEN);}
H5T_STD_I32LE    {return hid(H5T_STD_I32LE_TOKEN);}
H5T_STD_I64BE    {return hid(H5T_STD_I64BE_TOKEN);}
H5T_STD_I64LE    {return hid(H5T_STD_I64LE_TOKEN);}

H5T_STD_U8BE     {return hid(H5T_STD_U8BE_TOKEN);}
H5T_STD_U8LE     {return hid(H5T_STD_U8LE_TOKEN);}
H5T_STD_U16BE    {return hid(H5T_STD_U16BE_TOKEN);}
H5T_STD_U16LE    {return hid(H5T_STD_U16LE_TOKEN);}
H5T_STD_U32BE    {return hid(H5T_STD_U32BE_TOKEN);}
H5T_STD_U32LE    {return hid(H5T_STD_U32LE_TOKEN);}
H5T_STD_U64BE    {return hid(H5T_STD_U64BE_TOKEN);}
H5T_STD_U64LE    {return hid(H5T_STD_U64LE_TOKEN);}

H5T_NATIVE_CHAR  {return hid(H5T_NATIVE_CHAR_TOKEN);}
H5T_NATIVE_SCHAR {return hid(H5T_NATIVE_SCHAR_TOKEN);}
H5T_NATIVE_UCHAR {return hid(H5T_NATIVE_UCHAR_TOKEN);}
H5T_NATIVE_SHORT {return hid(H5T_NATIVE_SHORT_TOKEN);}
H5T_NATIVE_USHORT  {return hid(H5T_NATIVE_USHORT_TOKEN);}
H5T_NATIVE_INT   {return hid(H5T_NATIVE_INT_TOKEN);}
H5T_NATIVE_UINT  {return hid(H5T_NATIVE_UINT_TOKEN);}
H5T_NATIVE_LONG  {return hid(H5T_NATIVE_LONG_TOKEN);}
H5T_NATIVE_ULONG {return hid(H5T_NATIVE_ULONG_TOKEN);}
H5T_NATIVE_LLONG   {return hid(H5T_NATIVE_LLONG_TOKEN);}
H5T_NATIVE_ULLONG  {return hid(H5T_NATIVE_ULLONG_TOKEN);}

H5T_IEEE_F32BE   {return hid(H5T_IEEE_F32BE_TOKEN);}
H5T_IEEE_F32LE   {return hid(H5T_IEEE_F32LE_TOKEN);}
H5T_IEEE_F64BE   {return hid(H5T_IEEE_F64BE_TOKEN);}
H5T_IEEE_F64LE   {return hid(H5T_IEEE_F64LE_TOKEN);}
H5T_NATIVE_FLOAT   {return hid(H5T_NATIVE_FLOAT_TOKEN);}
H5T_NATIVE_DOUBLE  {return hid(H5T_NATIVE_DOUBLE_TOKEN);}
H5T_NATIVE_LDOUBLE {return hid(H5T_NATIVE_LDOUBLE_TOKEN);}

H5T_STRING       {return token(H5T_STRING_TOKEN);}
STRSIZE          {return token(STRSIZE_TOKEN);}
STRPAD           {return token(STRPAD_TOKEN);}
CSET             {return token(CSET_TOKEN);}
CTYPE            {return token(CTYPE_TOKEN);}
H5T_STR_NULLTERM {return token(H5T_STR_NULLTERM_TOKEN);}
H5T_STR_NULLPAD  {return token(H5T_STR_NULLPAD_TOKEN);}
H5T_STR_SPACEPAD {return token(H5T_STR_SPACEPAD_TOKEN);}
H5T_CSET_ASCII   {return token(H5T_CSET_ASCII_TOKEN);}
H5T_CSET_UTF8    {return token(H5T_CSET_UTF8_TOKEN);}
H5T_C_S1         {return token(H5T_C_S1_TOKEN);}
H5T_FORTRAN_S1   {return token(H5T_FORTRAN_S1_TOKEN);}
H5T_VARIABLE     {return token(H5T_VARIABLE_TOKEN);}

H5T_COMPOUND     {return token(H5T_COMPOUND_TOKEN);}
H5T_ENUM         {return token(H5T_ENUM_TOKEN);}
H5T_ARRAY        {return token(H5T_ARRAY_TOKEN);}
H5T_VLEN         {return token(H5T_VLEN_TOKEN);}

H5T_OPAQUE       {return token(H5T_OPAQUE_TOKEN);}
OPQ_SIZE         {return token(OPQ_SIZE_TOKEN);}
OPQ_TAG          {return token(OPQ_TAG_TOKEN);}

[0-9]+           {
                        H5LTyylval.ival = atoi(yytext);
                        return NUMBER;
                 }

["][^\"]+["]     {
                    H5LTyylval.sval = trim_quotes(yytext);
                    return STRING;
                 }

"{"              {return token('{');}
"}"              {return token('}');}
"["              {return token('[');}
"]"              {return token(']');}
":"              {return token(':');}
";"              {return token(';');}
[ \t\n]*         ;

%%

/* Allocate a copy of `quoted` with the double quote character at
 * the beginning and the one at the end both removed.  The caller is
 * responsible for free()ing the copy.
 */
static char *
trim_quotes(const char *quoted)
{
    size_t len = strlen(quoted);
    char *trimmed;

    assert(quoted[0] == '"' && quoted[len - 1] == '"');

    trimmed = strdup(quoted + 1);
    trimmed[len - 2] = '\0';

    return trimmed;
}

static int my_yyinput(char *buf, int max_size)
{
   int ret;

   memcpy(buf, myinput, input_len);
   ret = (int)input_len;
   return ret;
}

int H5LTyyerror(const char *msg)
{
   printf("ERROR: %s before \"%s\".\n", msg, yytext);
   return 0;
}

int yywrap()
{
    return 1;
}
