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
 * If you make any changes to H5LTparse.y, please run bin/genparser to
 * recreate the output files.
 */

%{
#include <stdio.h>
#include <string.h>
#include <hdf5.h>

#include "H5private.h"

extern int yylex(void);
extern int yyerror(const char *);

#define STACK_SIZE      16

/*structure for compound type information*/
struct cmpd_info {
    hid_t       id;             /*type ID*/
    bool        is_field;       /*flag to lexer for compound member*/
    bool        first_memb;     /*flag for first compound member*/
};

/*stack for nested compound type*/
static struct cmpd_info cmpd_stack[STACK_SIZE] = {
    {0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {0, 0, 1},
    {0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {0, 0, 1},
    {0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {0, 0, 1},
    {0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {0, 0, 1} };

static int csindex = -1;                /*pointer to the top of compound stack*/

/*structure for array type information*/
struct arr_info {
    hsize_t             dims[H5S_MAX_RANK];     /*size of each dimension, limited to 32 dimensions*/
    unsigned            ndims;                  /*number of dimensions*/
    bool                is_dim;                 /*flag to lexer for dimension*/
};
/*stack for nested array type*/
static struct arr_info arr_stack[STACK_SIZE];
static int asindex = -1;               /*pointer to the top of array stack*/ 

static H5T_str_t   str_pad;                /*variable for string padding*/
static H5T_cset_t  str_cset;               /*variable for string character set*/
static bool        is_variable = 0;        /*variable for variable-length string*/
static size_t      str_size;               /*variable for string size*/
   
static hid_t       enum_id;                /*type ID*/
static bool        is_enum = 0;            /*flag to lexer for enum type*/
static bool        is_enum_memb = 0;       /*flag to lexer for enum member*/
static char*       enum_memb_symbol;       /*enum member symbol string*/

%}
%union {
    int     ival;         /*for integer token*/
    char    *sval;        /*for name string*/
    hid_t   hid;          /*for hid_t token*/
}

%token <hid> H5T_STD_I8BE_TOKEN H5T_STD_I8LE_TOKEN H5T_STD_I16BE_TOKEN  H5T_STD_I16LE_TOKEN
%token <hid> H5T_STD_I32BE_TOKEN H5T_STD_I32LE_TOKEN H5T_STD_I64BE_TOKEN H5T_STD_I64LE_TOKEN
%token <hid> H5T_STD_U8BE_TOKEN H5T_STD_U8LE_TOKEN H5T_STD_U16BE_TOKEN  H5T_STD_U16LE_TOKEN
%token <hid> H5T_STD_U32BE_TOKEN H5T_STD_U32LE_TOKEN H5T_STD_U64BE_TOKEN H5T_STD_U64LE_TOKEN
%token <hid> H5T_NATIVE_CHAR_TOKEN H5T_NATIVE_SCHAR_TOKEN H5T_NATIVE_UCHAR_TOKEN 
%token <hid> H5T_NATIVE_SHORT_TOKEN H5T_NATIVE_USHORT_TOKEN H5T_NATIVE_INT_TOKEN H5T_NATIVE_UINT_TOKEN 
%token <hid> H5T_NATIVE_LONG_TOKEN H5T_NATIVE_ULONG_TOKEN H5T_NATIVE_LLONG_TOKEN H5T_NATIVE_ULLONG_TOKEN

%token <hid> H5T_IEEE_F32BE_TOKEN H5T_IEEE_F32LE_TOKEN H5T_IEEE_F64BE_TOKEN H5T_IEEE_F64LE_TOKEN
%token <hid> H5T_NATIVE_FLOAT_TOKEN H5T_NATIVE_DOUBLE_TOKEN H5T_NATIVE_LDOUBLE_TOKEN

%token <ival> H5T_STRING_TOKEN STRSIZE_TOKEN STRPAD_TOKEN CSET_TOKEN CTYPE_TOKEN H5T_VARIABLE_TOKEN
%token <ival> H5T_STR_NULLTERM_TOKEN H5T_STR_NULLPAD_TOKEN H5T_STR_SPACEPAD_TOKEN 
%token <ival> H5T_CSET_ASCII_TOKEN H5T_CSET_UTF8_TOKEN H5T_C_S1_TOKEN H5T_FORTRAN_S1_TOKEN

%token <ival> H5T_OPAQUE_TOKEN OPQ_SIZE_TOKEN OPQ_TAG_TOKEN

%token <ival> H5T_COMPOUND_TOKEN
%token <ival> H5T_ENUM_TOKEN
%token <ival> H5T_ARRAY_TOKEN
%token <ival> H5T_VLEN_TOKEN

%token <sval> STRING
%token <ival> NUMBER
%token <ival> '{' '}' '[' ']' ':' ';' 

%%
start   :       { memset(arr_stack, 0, STACK_SIZE*sizeof(struct arr_info)); /*initialize here?*/ }
        |       ddl_type  { return $<hid>$;}
        ;
ddl_type        :       atomic_type
                |       compound_type
                |       array_type
                |       vlen_type
                ;
atomic_type     :       integer_type
                |       fp_type
                |       string_type
                |       enum_type
                |       opaque_type
                ;

integer_type    :       H5T_STD_I8BE_TOKEN  { $<hid>$ = H5Tcopy(H5T_STD_I8BE); }
                |       H5T_STD_I8LE_TOKEN  { $<hid>$ = H5Tcopy(H5T_STD_I8LE); }
                |       H5T_STD_I16BE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_I16BE); }
                |       H5T_STD_I16LE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_I16LE); }
                |       H5T_STD_I32BE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_I32BE); }
                |       H5T_STD_I32LE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_I32LE); }
                |       H5T_STD_I64BE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_I64BE); }
                |       H5T_STD_I64LE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_I64LE); }
                |       H5T_STD_U8BE_TOKEN  { $<hid>$ = H5Tcopy(H5T_STD_U8BE); }
                |       H5T_STD_U8LE_TOKEN  { $<hid>$ = H5Tcopy(H5T_STD_U8LE); }
                |       H5T_STD_U16BE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_U16BE); }
                |       H5T_STD_U16LE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_U16LE); }
                |       H5T_STD_U32BE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_U32BE); }
                |       H5T_STD_U32LE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_U32LE); }
                |       H5T_STD_U64BE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_U64BE); }
                |       H5T_STD_U64LE_TOKEN { $<hid>$ = H5Tcopy(H5T_STD_U64LE); }
                |       H5T_NATIVE_CHAR_TOKEN   { $<hid>$ = H5Tcopy(H5T_NATIVE_CHAR); }
                |       H5T_NATIVE_SCHAR_TOKEN  { $<hid>$ = H5Tcopy(H5T_NATIVE_SCHAR); }
                |       H5T_NATIVE_UCHAR_TOKEN  { $<hid>$ = H5Tcopy(H5T_NATIVE_UCHAR); }
                |       H5T_NATIVE_SHORT_TOKEN  { $<hid>$ = H5Tcopy(H5T_NATIVE_SHORT); }
                |       H5T_NATIVE_USHORT_TOKEN { $<hid>$ = H5Tcopy(H5T_NATIVE_USHORT); }
                |       H5T_NATIVE_INT_TOKEN    { $<hid>$ = H5Tcopy(H5T_NATIVE_INT); }
                |       H5T_NATIVE_UINT_TOKEN   { $<hid>$ = H5Tcopy(H5T_NATIVE_UINT); }
                |       H5T_NATIVE_LONG_TOKEN   { $<hid>$ = H5Tcopy(H5T_NATIVE_LONG); }
                |       H5T_NATIVE_ULONG_TOKEN  { $<hid>$ = H5Tcopy(H5T_NATIVE_ULONG); }
                |       H5T_NATIVE_LLONG_TOKEN  { $<hid>$ = H5Tcopy(H5T_NATIVE_LLONG); }
                |       H5T_NATIVE_ULLONG_TOKEN { $<hid>$ = H5Tcopy(H5T_NATIVE_ULLONG); }
                ;

fp_type         :       H5T_IEEE_F32BE_TOKEN { $<hid>$ = H5Tcopy(H5T_IEEE_F32BE); }
                |       H5T_IEEE_F32LE_TOKEN { $<hid>$ = H5Tcopy(H5T_IEEE_F32LE); }
                |       H5T_IEEE_F64BE_TOKEN { $<hid>$ = H5Tcopy(H5T_IEEE_F64BE); }
                |       H5T_IEEE_F64LE_TOKEN { $<hid>$ = H5Tcopy(H5T_IEEE_F64LE); }
                |       H5T_NATIVE_FLOAT_TOKEN    { $<hid>$ = H5Tcopy(H5T_NATIVE_FLOAT); }
                |       H5T_NATIVE_DOUBLE_TOKEN   { $<hid>$ = H5Tcopy(H5T_NATIVE_DOUBLE); }
                |       H5T_NATIVE_LDOUBLE_TOKEN  { $<hid>$ = H5Tcopy(H5T_NATIVE_LDOUBLE); }
                ;

compound_type   :       H5T_COMPOUND_TOKEN
                            { csindex++; cmpd_stack[csindex].id = H5Tcreate(H5T_COMPOUND, 1); /*temporarily set size to 1*/ } 
                        '{' memb_list '}'     
                            { $<hid>$ = cmpd_stack[csindex].id; 
                              cmpd_stack[csindex].id = 0;
                              cmpd_stack[csindex].first_memb = 1; 
                              csindex--;
                            }
                ;
memb_list       :       
                |       memb_list memb_def
                ;
memb_def        :       ddl_type { cmpd_stack[csindex].is_field = 1; /*notify lexer a compound member is parsed*/ } 
                        field_name field_offset ';'
                        {   
                            size_t origin_size, new_size;
                            hid_t dtype_id = cmpd_stack[csindex].id;

                            /*Adjust size and insert member, consider both member size and offset.*/
                            if(cmpd_stack[csindex].first_memb) { /*reclaim the size 1 temporarily set*/
                                new_size = H5Tget_size($<hid>1) + $<ival>4;
                                H5Tset_size(dtype_id, new_size);
                                /*member name is saved in yylval.sval by lexer*/
                                H5Tinsert(dtype_id, $<sval>3, $<ival>4, $<hid>1);

                                cmpd_stack[csindex].first_memb = 0;
                            } else {
                                origin_size = H5Tget_size(dtype_id);
                                
                                if($<ival>4 == 0) {
                                    new_size = origin_size + H5Tget_size($<hid>1);
                                    H5Tset_size(dtype_id, new_size);
                                    H5Tinsert(dtype_id, $<sval>3, origin_size, $<hid>1);
                                } else {
                                    new_size = $<ival>4 + H5Tget_size($<hid>1);
                                    H5Tset_size(dtype_id, new_size);
                                    H5Tinsert(dtype_id, $<sval>3, $<ival>4, $<hid>1);
                                }
                            }
                            if($<sval>3) {
                                free($<sval>3);
                                $<sval>3 = NULL;
                            }
                            cmpd_stack[csindex].is_field = 0;
                            H5Tclose($<hid>1);
                             
                            new_size = H5Tget_size(dtype_id);
                        }
                ;
field_name      :       STRING
                        {
                            $<sval>$ = strdup(yylval.sval);
                            free(yylval.sval);
                            yylval.sval = NULL;
                        }                            
                ;
field_offset    :       /*empty*/
                        { $<ival>$ = 0; }
                |       ':' offset
                        { $<ival>$ = yylval.ival; }
                ;
offset          :       NUMBER
                ;
array_type      :       H5T_ARRAY_TOKEN { asindex++; /*pushd onto the stack*/ }
                        '{' dim_list ddl_type '}'
                        { 
                          $<hid>$ = H5Tarray_create2($<hid>5, arr_stack[asindex].ndims, arr_stack[asindex].dims);
                          arr_stack[asindex].ndims = 0;
                          asindex--;
                          H5Tclose($<hid>5);
                        }            
                ;
dim_list        :
                |       dim_list dim
                ;
dim             :       '[' { arr_stack[asindex].is_dim = 1; /*notice lexer of dimension size*/ }
                        dimsize { unsigned ndims = arr_stack[asindex].ndims;
                                  arr_stack[asindex].dims[ndims] = (hsize_t)yylval.ival; 
                                  arr_stack[asindex].ndims++;
                                  arr_stack[asindex].is_dim = 0; 
                                } 
                        ']'
                ;
dimsize         :       NUMBER
                ;

vlen_type       :       H5T_VLEN_TOKEN '{' ddl_type '}'
                            { $<hid>$ = H5Tvlen_create($<hid>3); H5Tclose($<hid>3); }
                ;

opaque_type     :       H5T_OPAQUE_TOKEN
                        '{' 
                            OPQ_SIZE_TOKEN opaque_size ';'
                            {   
                                size_t size = (size_t)yylval.ival;
                                $<hid>$ = H5Tcreate(H5T_OPAQUE, size);
                            }
                            OPQ_TAG_TOKEN opaque_tag ';'
                            {  
                                H5Tset_tag($<hid>6, yylval.sval);
                                free(yylval.sval);
                                yylval.sval = NULL;
                            }                             
                        '}' { $<hid>$ = $<hid>6; }
                ;
opaque_size     :       NUMBER
                ;
opaque_tag      :       STRING
                ;
string_type     :       H5T_STRING_TOKEN 
                        '{' 
                            STRSIZE_TOKEN strsize ';'
                            {  
                                if($<ival>4 == H5T_VARIABLE_TOKEN)
                                    is_variable = 1;
                                else 
                                    str_size = yylval.ival;
                            }
                            STRPAD_TOKEN strpad ';'
                            {
                                if($<ival>8 == H5T_STR_NULLTERM_TOKEN)
                                    str_pad = H5T_STR_NULLTERM;
                                else if($<ival>8 == H5T_STR_NULLPAD_TOKEN)
                                    str_pad = H5T_STR_NULLPAD;
                                else if($<ival>8 == H5T_STR_SPACEPAD_TOKEN)
                                    str_pad = H5T_STR_SPACEPAD;
                            }
                            CSET_TOKEN cset ';'
                            {  
                                if($<ival>12 == H5T_CSET_ASCII_TOKEN)
                                    str_cset = H5T_CSET_ASCII;
                                else if($<ival>12 == H5T_CSET_UTF8_TOKEN)
                                    str_cset = H5T_CSET_UTF8;
                            }
                            CTYPE_TOKEN ctype ';'
                            {
                                if($<hid>16 == H5T_C_S1_TOKEN)
                                    $<hid>$ = H5Tcopy(H5T_C_S1);
                                else if($<hid>16 == H5T_FORTRAN_S1_TOKEN)
                                    $<hid>$ = H5Tcopy(H5T_FORTRAN_S1);
                            }
                        '}' 
                            {   
                                hid_t str_id = $<hid>18;

                                /*set string size*/
                                if(is_variable) {
                                    H5Tset_size(str_id, H5T_VARIABLE);
                                    is_variable = 0;
                                } else
                                    H5Tset_size(str_id, str_size);
                                
                                /*set string padding and character set*/
                                H5Tset_strpad(str_id, str_pad);
                                H5Tset_cset(str_id, str_cset);

                                $<hid>$ = str_id; 
                            }
                ;
strsize         :       H5T_VARIABLE_TOKEN     {$<ival>$ = H5T_VARIABLE_TOKEN;}
                |       NUMBER
                ;
strpad          :       H5T_STR_NULLTERM_TOKEN {$<ival>$ = H5T_STR_NULLTERM_TOKEN;}
                |       H5T_STR_NULLPAD_TOKEN  {$<ival>$ = H5T_STR_NULLPAD_TOKEN;}
                |       H5T_STR_SPACEPAD_TOKEN {$<ival>$ = H5T_STR_SPACEPAD_TOKEN;}
                ;
cset            :       H5T_CSET_ASCII_TOKEN {$<ival>$ = H5T_CSET_ASCII_TOKEN;}
                |       H5T_CSET_UTF8_TOKEN {$<ival>$ = H5T_CSET_UTF8_TOKEN;}
                ;
ctype           :       H5T_C_S1_TOKEN         {$<hid>$ = H5T_C_S1_TOKEN;}
                |       H5T_FORTRAN_S1_TOKEN   {$<hid>$ = H5T_FORTRAN_S1_TOKEN;}
                ;

enum_type       :       H5T_ENUM_TOKEN '{' integer_type ';' 
                            { is_enum = 1; enum_id = H5Tenum_create($<hid>3); H5Tclose($<hid>3); }
                        enum_list '}'
                            { is_enum = 0; /*reset*/ $<hid>$ = enum_id; }
                ;
enum_list       :
                |       enum_list enum_def
                ;
enum_def        :       enum_symbol         {
                                                is_enum_memb = 1; /*indicate member of enum*/
                                                enum_memb_symbol = strdup(yylval.sval); 
                                                free(yylval.sval);
                                                yylval.sval = NULL;
                                            }
                        enum_val ';'
                            {
                                char char_val=(char)yylval.ival;
                                short short_val=(short)yylval.ival;
                                int int_val=(int)yylval.ival;
                                long long_val=(long)yylval.ival;
                                long long llong_val=(long long)yylval.ival;
                                hid_t super = H5Tget_super(enum_id);
                                hid_t native = H5Tget_native_type(super, H5T_DIR_ASCEND);
                                H5T_order_t super_order = H5Tget_order(super);
                                H5T_order_t native_order = H5Tget_order(native);
 
                                if(is_enum && is_enum_memb) { /*if it's an enum member*/
                                    /*To handle machines of different endianness*/
                                    if(H5Tequal(native, H5T_NATIVE_SCHAR) || H5Tequal(native, H5T_NATIVE_UCHAR)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &char_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &char_val);
                                    } else if(H5Tequal(native, H5T_NATIVE_SHORT) || H5Tequal(native, H5T_NATIVE_USHORT)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &short_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &short_val);
                                    } else if(H5Tequal(native, H5T_NATIVE_INT) || H5Tequal(native, H5T_NATIVE_UINT)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &int_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &int_val);
                                    } else if(H5Tequal(native, H5T_NATIVE_LONG) || H5Tequal(native, H5T_NATIVE_ULONG)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &long_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &long_val);
                                    } else if(H5Tequal(native, H5T_NATIVE_LLONG) || H5Tequal(native, H5T_NATIVE_ULLONG)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &llong_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &llong_val);
                                    }

                                    is_enum_memb = 0; 
                                    if(enum_memb_symbol) free(enum_memb_symbol);
                                }

                                H5Tclose(super);
                                H5Tclose(native);
                            }
                ;
enum_symbol     :       STRING
                ;
enum_val        :       NUMBER
                ;
