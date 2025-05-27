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
#include <stdio.h>
#ifdef USE_STRING_H
#include <string.h>
#else
#include <strings.h>
#endif
#include "keywords.h"
#include "StringBuffer.h"
#include "expr.h"
#include "y.tab.h"
#include "substitution.h"
#include "error_msg.h"
#include "query_handler.h"

extern template_t *g_Template;

/* NOTE: current code requires WORKLOAD to be first entry */
static keyword_t KeyWords[] =
{
    {"ID", 0, 0},
    {"SQL", 0, 0},
    {"DEFINE", 0, 0},
    {"RANDOM", 0, 0},
    {"UNIFORM", 0, 0},
    {"RANGE", 0, 0},
    {"DATE", 0, 0},
    {"INCLUDE", 0, 0},
    {"TEXT", 0, 0},
    {"DIST", 0, 0},
    {"LIST", 0, 0},
    {"ROWCOUNT", 0, 0},
    {"BEGIN", 0, 0},
    {"END", 0, 0},
    {"SALES", 0, 0},
    {"RETURNS", 0, 0},
    {"DISTMEMBER", 0, 0},
   {"DISTWEIGHT", 0, 0},
    {"_QUERY", EXPR_FL_INT, 0},
    {"_STREAM", EXPR_FL_INT, 0},
    {"_TEMPLATE", EXPR_FL_CHAR, 0},
    {"_SEED", EXPR_FL_CHAR, 0},
    {"SCALE", 0, 0},
   {"SCALESTEP", 0, 0},
    {"SET", 0, 0},
    {"ADD", 0, 0},
    {"NAMES", 0, 0},
    {"TYPES", 0, 0},
    {"WEIGHTS", 0, 0},
    {"INT", 0, 0},
    {"VARCHAR", 0, 0},
    {"DECIMAL", 0, 0},
    {"_LIMIT", EXPR_FL_INT, 0},
    {"_LIMITA", EXPR_FL_INT, 0},
    {"_LIMITB", EXPR_FL_INT, 0},
    {"_LIMITC", EXPR_FL_INT, 0},
    {"ULIST", 0, 0},
    {NULL, 0}
};


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
void InitKeywords()
{
    int nIndex = TOK_ID;
    keyword_t *pKW;
    expr_t *pExpr;
    
    g_Template = (template_t *)malloc(sizeof(struct TEMPLATE_T));
    MALLOC_CHECK(g_Template);
    if (!g_Template)
        ReportError(QERR_NO_MEMORY, "InitKeywords()", 1);
    memset(g_Template, 0, sizeof(struct TEMPLATE_T));
    g_Template->SubstitutionList = makeList(L_FL_SORT, compareSubstitution);    
    
    for (pKW = &KeyWords[0]; pKW->szName; pKW++)
    {
        pKW->nIndex = nIndex++;
        if (pKW->nFlags)
        {
            pExpr = MakeIntConstant(pKW->nIndex);
            defineSubstitution(g_Template, pKW->szName, pExpr);
            pExpr->nFlags |= EXPR_FL_KEYWORD;
        }
    }
    return;
}

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
int FindKeyword(char *szWord)
{
    keyword_t *pKW;

    for (pKW = &KeyWords[0]; pKW->szName; pKW++)
    {
        if (strcasecmp(pKW->szName, szWord) == 0)
            return(pKW->nIndex);
#ifdef DEBUG
        else fprintf("comparing '%s' and '%s'\n", pKW->szName, szWord);
#endif /* DEBUG */
    }

    return(-1);
}

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
char *KeywordText(int nKeyword)
{
    if (nKeyword < TOK_ID)
        return(NULL);

    nKeyword -= TOK_ID;

    if (nKeyword < 0)
        return(NULL);
    
    return(KeyWords[nKeyword].szName);
}

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
expr_t *
getKeywordValue(int nIndex)
{
    substitution_t *pSub;

    pSub = findSubstitution(pCurrentQuery, KeywordText(nIndex), 0);

    return((pSub)?pSub->pAssignment:NULL);
}
