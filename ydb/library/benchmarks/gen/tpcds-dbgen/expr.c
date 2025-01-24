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
#include "config.h"
#include "porting.h"
#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#ifndef USE_STRINGS_H
#include <string.h>
#else
#include <strings.h>
#endif
#include "error_msg.h"
#include "StringBuffer.h"
#include "expr.h"
#include "y.tab.h"
#include "substitution.h"
#include "substitution.h"
#include "grammar_support.h"
#include "date.h"
#include "keywords.h"
#include "dist.h"
#include "genrand.h"
#include "permute.h"
#include "list.h"

typedef struct FUNC_INFO_T {
    int nKeyword;
    int nParams;
    int nDataType;
    int nAddFlags;
} func_info_t;

#define MAX_FUNC    10
static func_info_t arFuncInfo[MAX_FUNC + 1] =
{
    {KW_DATE, 3, EXPR_FL_DATE, EXPR_FL_SUFFIX},
    {KW_DIST, 3, EXPR_FL_CHAR, EXPR_FL_SUFFIX},
    {KW_LIST, 2, EXPR_FL_TYPESHIFT, 0},
    {KW_ULIST, 2, EXPR_FL_TYPESHIFT, 0},
    {KW_RANGE, 2, EXPR_FL_TYPESHIFT, 0},
    {KW_RANDOM, 3, EXPR_FL_INT, 0},
    {KW_TEXT, 2, EXPR_FL_CHAR, 0},
    {KW_ROWCOUNT, 1, EXPR_FL_INT, 0},
    {KW_DISTMEMBER, 3, EXPR_FL_CHAR, 0},
    {KW_DISTWEIGHT, 3, EXPR_FL_INT, 0},
    {-1, -1, -1}
};
static int arFuncArgs[MAX_FUNC][MAX_ARGS + 1] =
{
    /* KW_DATE */        {EXPR_FL_CHAR, EXPR_FL_CHAR, EXPR_FL_INT, 0},
    /* KW_DIST */        {EXPR_FL_CHAR, EXPR_FL_TYPESHIFT, EXPR_FL_TYPESHIFT, 0},
    /* KW_LIST */        {EXPR_FL_TYPESHIFT, EXPR_FL_INT, 0},
    /* KW_ULIST */        {EXPR_FL_TYPESHIFT, EXPR_FL_INT, 0},
    /* KW_RANGE */        {EXPR_FL_TYPESHIFT, EXPR_FL_INT, 0},
    /* KW_RANDOM */        {EXPR_FL_INT, EXPR_FL_INT, EXPR_FL_INT, 0},
    /* KW_TEXT */        {EXPR_FL_CHAR, EXPR_FL_INT, 0},
    /* KW_ROWCOUNT */    {EXPR_FL_CHAR, 0},
    /* KW_DISTMEMBER */    {EXPR_FL_CHAR, EXPR_FL_TYPESHIFT, EXPR_FL_TYPESHIFT, 0}, /* do the checking at runtime */
    /* KW_DISTWEIGHT */    {EXPR_FL_CHAR, EXPR_FL_TYPESHIFT, EXPR_FL_TYPESHIFT, 0} /* do the checking at runtime */
};

int ValidateParams(int nFunc, expr_t *pExpr);
extern template_t *pCurrentQuery;

/*
* Routine: 
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
static int
canCast(int nFromDataType, int nToDataType)
{
    int nValidSourceDataType = 0;
    
    switch (nToDataType)
    {
    case EXPR_FL_INT:
        nValidSourceDataType = EXPR_FL_KEYWORD|EXPR_FL_INT|EXPR_FL_CHAR;
        break;
    case EXPR_FL_CHAR:
        nValidSourceDataType = EXPR_FL_CHAR|EXPR_FL_INT;
        break;
    case EXPR_FL_DATE:
        nValidSourceDataType = EXPR_FL_DATE|EXPR_FL_CHAR;
        break;
    case EXPR_FL_TYPESHIFT:
        nValidSourceDataType = EXPR_TYPE_MASK;
        break;
    }
    return(nValidSourceDataType & nFromDataType);
}

/*
* Routine: expr_t *makeExpr(void)
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
expr_t *
makeExpr(void)
{
    expr_t *pResult;

    pResult = (expr_t *)malloc(sizeof(struct EXPR_T));
    MALLOC_CHECK(pResult);
    if (pResult == NULL)
        ReportError(QERR_NO_MEMORY, "in MakeReplacement()", 1);
    memset(pResult, 0, sizeof(struct EXPR_T));
    pResult->nValueCount = 1;
#ifdef MEM_TEST
    fprintf(stderr, "MakeExpr value %x\n", pResult);
#endif
    pResult->Value.pBuf = InitBuffer(10, 10);
    if (pResult->Value.pBuf == NULL)
        ReportError(QERR_NO_MEMORY, "in MakeReplacement()", 1);

    return(pResult);
}



/*
* Routine: MakeReplacement(char *szText, int nValue)
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
expr_t *
MakeReplacement(char *szText, int nValue)
{
    expr_t *pResult;

    pResult = makeExpr();

    AddBuffer(pResult->Value.pBuf, szText);
    pResult->Value.nValue = nValue;
    pResult->nFlags = EXPR_FL_REPL | EXPR_FL_CHAR;

    return(pResult);
}

/*
* Routine: MakeListExpr()
* Purpose: add LIST/RANGE modifiers to an expression
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
expr_t *
MakeListExpr(int nModifier, expr_t *pExpr, int nArg)
{
    expr_t *pArgExpr;

    switch(nModifier)
    {
    case KW_LIST:
        pExpr->nValueCount = nArg;
      pExpr->nFlags |= EXPR_FL_LIST;
        break;
    case KW_ULIST:
        pExpr->nValueCount = nArg;
      pExpr->nFlags |= EXPR_FL_ULIST;
        break;
    case KW_RANGE:
        pExpr->nValueCount = 2;
      pExpr->nFlags |= EXPR_FL_RANGE;
        break;
    default:
        INTERNAL("Bad modifier in MakeListExpr()");
        break;
    }

    pArgExpr = MakeIntConstant(nArg);
    addList(pExpr->ArgList, pArgExpr);

    return(pExpr);
}

/*
* Routine: MakeStringConstant(char *szText)
* Purpose: add an argument to the pre-existing list
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
expr_t *
MakeStringConstant(char *szText)
{
    expr_t *pResult;

    pResult = makeExpr();
    AddBuffer(pResult->Value.pBuf, szText);
    pResult->nFlags = EXPR_FL_CONST | EXPR_FL_CHAR;
    pResult->Value.bUseInt = 0;

    return(pResult);
}

/*
* Routine: makeArithmeticExpr(int nOp, expr_t *pArg1, expr_t *pArg2);
* Purpose: handle simple arithmetic
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
expr_t *
makeArithmeticExpr(int nOp, expr_t *pArg1, expr_t *pArg2)
{
    expr_t *pResult;
    int nDataType1, nDataType2;

    pResult = makeExpr();
    pResult->ArgList = makeList(L_FL_TAIL, NULL);
    addList(pResult->ArgList, pArg1);
    addList(pResult->ArgList, pArg2);
    pResult->Value.nValue = nOp;
    pResult->nFlags = EXPR_FL_FUNC;

    /* now set the data type of the result */
    nDataType1 = pArg1->nFlags & EXPR_TYPE_MASK;
    nDataType2 = pArg2->nFlags & EXPR_TYPE_MASK;
    if (nDataType1 >= nDataType2)
        pResult->nFlags |= nDataType1;
    else
        pResult->nFlags |= nDataType2;

    return(pResult);
}



/*
* Routine: MakeStringConstant(char *szText)
* Purpose: add an argument to the pre-existing list
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
expr_t *
MakeVariableReference(char *szText, int nSuffix)
{
    expr_t *pResult;

    pResult = makeExpr();

    AddBuffer(pResult->Value.pBuf, szText);
    pResult->nFlags = EXPR_FL_SUBST | EXPR_FL_CHAR;
    pResult->nSubElement = (nSuffix >= 1)?nSuffix:1;
    if (!findSubstitution(pCurrentQuery, szText, &nSuffix))
        yywarn("Substitution used before being defined");

    return(pResult);
}
/*
* Routine: MakeIntConstant(int nValue)
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
expr_t *
MakeIntConstant(ds_key_t nValue)
{
    expr_t *pResult;

    pResult = makeExpr();

    pResult->Value.nValue = nValue;
    pResult->nFlags = EXPR_FL_CONST | EXPR_FL_INT;
    pResult->Value.bUseInt = 1;

    return(pResult);
}

/*
* Routine: MakeFunctionCall(int nKeyword, expr_t *pArgList)
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
expr_t *
MakeFunctionCall(int nKeyword, list_t *pArgList)
{
    expr_t *pResult;
    int nFuncNum = -1,
        i;

    pResult = makeExpr();

    for (i=0; arFuncInfo[i].nKeyword >= 0; i++)
    {
        if (nKeyword == arFuncInfo[i].nKeyword)
            nFuncNum = i;
    }
    if (nFuncNum < 0)
        ReportError(QERR_BAD_NAME, NULL, 1);
    pResult->Value.nValue = nKeyword;
    pResult->nFlags = EXPR_FL_FUNC;
    pResult->nFlags |= arFuncInfo[nFuncNum].nDataType;
    pResult->nFlags |= arFuncInfo[nFuncNum].nAddFlags;
    pResult->ArgList = pArgList;

    if (ValidateParams(nFuncNum, pResult))
        ReportError(QERR_SYNTAX, "in MakeFunctionCall()", 1);

    return(pResult);
}

/*
* Routine: PrintExpr(expr_t *)
* Purpose: print out an expression to allow templates to be reconstructed
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
void 
PrintExpr(expr_t *pExpr)
{
    int i,
        bUseComma = 0;
    expr_t *pArg;
   char szFormat[20];

    /* handle the constants */
    if (pExpr->nFlags & EXPR_FL_CONST)
    {
        switch(i = pExpr->nFlags & (EXPR_TYPE_MASK ^ EXPR_FL_KEYWORD))
        {
        case EXPR_FL_INT:
            if (pExpr->nFlags & EXPR_FL_KEYWORD)
                printf("%s", KeywordText((int)pExpr->Value.nValue));
            else
                printf(HUGE_FORMAT, pExpr->Value.nValue);
            break;
        case EXPR_FL_CHAR:
            printf("\"%s\"", GetBuffer(pExpr->Value.pBuf));
            break;
        default:
            fprintf(stderr, "INTERNAL ERROR: unknown constant type %d\n", i);
            exit(1);
        }
        return;
    }
    
    /* handle the parameterized expressions */
    switch(pExpr->nFlags & EXPR_FUNC_MASK)
    {
    case EXPR_FL_FUNC:
        if (pExpr->nFlags & EXPR_FL_FUNC)
        {
            printf("%s(", KeywordText((int)pExpr->Value.nValue));
            for (pArg = (expr_t *)getHead(pExpr->ArgList); pArg; pArg = (expr_t *)getNext(pExpr->ArgList))
            {
                if (bUseComma)
                    printf(",\n\t");
                PrintExpr(pArg);
                bUseComma = 1;
            }
            printf(")");
        }
        break;
    case EXPR_FL_REPL:
        sprintf(szFormat, "{\"%%s\", %s}", HUGE_FORMAT);
        printf(szFormat, GetBuffer(pExpr->Value.pBuf), pExpr->Value.nValue);
        break;
    case EXPR_FL_SUBST:
        printf("[%s]", GetBuffer(pExpr->Value.pBuf));
        break;
    default:
        fprintf(stderr, "INTERNAL ERROR: unknown expression type %x\n", pExpr->nFlags);
        exit(1);
    }
    
    return;
    
}

/*
* Routine: ValidateParams(int nFunc, expr_t *pArgs)
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
int 
ValidateParams(int nFunc, expr_t *pArgs)
{
    int i = 0,
        nArgs;
    expr_t *pCurrentArg;
    char msg[80];
    
    pCurrentArg = getHead(pArgs->ArgList);
    if (pCurrentArg)
    {
        
        if (pCurrentArg->nFlags & EXPR_FL_REPL)    /* replacement sets can be arbitrarily long */
            return(0);
    }

    nArgs = length(pArgs->ArgList);
    if (nArgs != arFuncInfo[nFunc].nParams)
    {
        
        sprintf(msg, "wanted %d args, found %d", arFuncInfo[nFunc].nParams, nArgs);
        ReportError(QERR_BAD_PARAMS, msg, 0);
        return(1);
    }
    
    for (pCurrentArg = (expr_t *)getHead(pArgs->ArgList); pCurrentArg; pCurrentArg = (expr_t *)getNext(pArgs->ArgList))
    {
        if (!canCast(pCurrentArg->nFlags & EXPR_TYPE_MASK,arFuncArgs[nFunc][i]) && 
            (arFuncArgs[nFunc][i] != EXPR_FL_TYPESHIFT))
        {
            sprintf(msg, "type mismatch in call to %s() on parameter %d (%d/%d)", 
                KeywordText(arFuncInfo[nFunc].nKeyword), 
                i + 1, 
                (pCurrentArg->nFlags & EXPR_TYPE_MASK), arFuncArgs[nFunc][i]);
            ReportErrorNoLine(QERR_BAD_PARAMS, msg, 0);
            return(1);
        }
        i += 1;
    }

    return(0);
}
