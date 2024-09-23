/* 
 * Legal Notice 
 * 
 * This document and associated source code (the "Work") is a part of a 
 * benchmark specification maintained by the TPC. 
 * 
 * The TPC reserves all right, title, and interest to the Work as provided 
 * under U.S. and international laws, including without limitation all patent 
 * and trademark rights therein. 
 * 
 * No Warranty 
 * 
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
 *     WITH REGARD TO THE WORK. 
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
 * 
 * Contributors:
 * Gradient Systems
 */ 
#ifndef EXPR_H
#define EXPR_H

#include "StringBuffer.h"
#include "list.h"
#include "mathops.h"

typedef struct EXPR_VAL_T {
    int bUseInt;
    StringBuffer_t *pBuf;
    ds_key_t nValue;
    int nQueryNumber;
} Expr_Val_t;

typedef struct EXPR_T
{
    int nFlags;
    list_t *ArgList;
    Expr_Val_t Value;
    int nValueCount;
    int *pPermute;
    ds_key_t *pPermuteKey;
    int nSubElement;
} expr_t;

/* expression flags */
/* NOTE: order of data types is important to makeArithmeticExpr() CHANGE WITH CARE */
#define EXPR_FL_CONST        0x00000001
#define EXPR_FL_UNDEF        0x00000002
#define EXPR_FL_FUNC        0x00000004
#define EXPR_FL_INT            0x00000008    /* argument is an integer */
#define EXPR_FL_CHAR        0x00000010    /* argument is a character string */
#define EXPR_FL_SUBST        0x00000020    /* argument is a pre-defined substitution */
#define EXPR_FL_KEYWORD        0x00000040    /* interpret nValue as a keyword index */
#define EXPR_FL_DATE        0x00000080    /* interpret nValue as a julian date */
#define EXPR_FL_TYPESHIFT    0x00000100    /* take type from arguments */
#define EXPR_TYPE_MASK        0x000001D8    /* different data types */
#define EXPR_FUNC_MASK        0x00000224    /* FUNC, REPL, SUBST */
#define EXPR_FL_REPL        0x00000200    /* replacement pair */
#define EXPR_FL_SUFFIX        0x00000400    /* expression can have begin/end tags */
#define EXPR_FL_LIST        0x00000800    /* expression should return a list of values */
#define EXPR_FL_RANGE        0x00001000    /* expression should return a range of values */
#define EXPR_FL_INIT        0x00002000    /* the expression has been initialized */
#define EXPR_FL_TEXT        0x00004000    /*  substitution is a text value */
#define EXPR_FL_DIST        0x00008000    /*  substitution is taken from a distribution */
#define EXPR_FL_RANDOM        0x00010000    /*  substitution is a random integer */
#define EXPR_FL_DISTMEMBER    0x00020000    /*  substitution returns distribution value */
#define EXPR_FL_DISTWEIGHT    0x00040000    /*  substitution returns distribtuion weight */
#define EXPR_FL_ROWCOUNT    0x00080000    /*  substitution table/distribution rowcount */
#define EXPR_FL_UNIFORM        0x00100000    /*  use a uniform distribution */
#define EXPR_FL_NORMAL        0x00200000    /*  use a normal distribution */
#define EXPR_FL_SALES        0x00400000    /*  skew dates for sales */
#define EXPR_FL_TABLENAME    0x00800000    /*    rowcount() argument is a table name */
#define EXPR_FL_VENDOR        0x01000000    /*  do double substitution for vendor syntax */
#define EXPR_FL_ULIST        0x02000000    /*  return unique value set from list() */
#define EXPR_FL_TYPE_MASK    (EXPR_FL_TEXT|EXPR_FL_RANDOM|EXPR_FL_DIST)    /*  separate substitution types. */
#define EXPR_FL_DIST_MASK    (EXPR_FL_UNIFORM|EXPR_FL_NORMAL)
#define EXPR_FL_SUFFIX_MASK    (EXPR_FL_LIST|EXPR_FL_RANGE|EXPR_FL_ULIST)    /* substitution that allow suffixes */

#define MAX_ARGS    20    /* overly large to allow TEXT() to have a lot of options */

#define DT_NONE        -1
#define DT_INT        0
#define DT_STR        1
#define DT_CHR        2
#define DT_DEC        3
#define DT_DATE        4
#define DT_KEY        5

#define OP_ADD        0
#define OP_SUBTRACT    1
#define OP_MULTIPLY    2
#define OP_DIVIDE    3

void AddArgument(expr_t *pArgList, expr_t *pExpr);
expr_t *MakeStringConstant(char *szText);
expr_t *MakeIntConstant(ds_key_t nValue);
expr_t *MakeFunctionCall(int nKeyword, list_t *pArgList);
expr_t *MakeListExpr(int nModifier, expr_t *pExpr, int nArg);
expr_t *MakeReplacement(char *szText, int nValue);
expr_t *MakeVariableReference(char *szText, int nSuffix);
expr_t *makeArithmeticExpr(int nOp, expr_t *pArg1, expr_t *pArg2);
void PrintExpr(expr_t *pExpr);


int EvalTextExpr(expr_t *pExpr, Expr_Val_t *pBuf, Expr_Val_t *pParams, int bIsParam);
char *GetParam(expr_t *pExpr, int nParam);
int EvalDistExpr(expr_t *pExpr, Expr_Val_t *pBuf, Expr_Val_t *pParams, int bIsParam);
int EvalRandomExpr(expr_t *pExpr, Expr_Val_t *pBuf, Expr_Val_t *pParams, int bIsParam);
int EvalRowcountExpr(expr_t *pExpr, Expr_Val_t *pBuf, Expr_Val_t *pParams);
int EvalDateExpr(expr_t *pExpr, Expr_Val_t *pBuf, Expr_Val_t *pParams, int bIsParam);
int EvalArg(expr_t *pArg, Expr_Val_t *pBuf, Expr_Val_t *pParams);
int EvalExpr(expr_t *pExpr, Expr_Val_t *pValue, int bIsParam, int nQueryNumber);
int EvalArithmetic(expr_t *pExpr, Expr_Val_t *pValue, Expr_Val_t *pParams);
#endif

