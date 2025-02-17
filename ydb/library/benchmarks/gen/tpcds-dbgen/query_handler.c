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
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include "StringBuffer.h"
#include "eval.h"
#include "substitution.h"
#include "error_msg.h"
#include "qgen_params.h"
#include "genrand.h"
#include "r_params.h"

extern list_t *TemplateList;
extern StringBuffer_t *g_sbTemplateName;
extern int g_nQueryNumber, g_nStreamNumber;
extern option_t *Options;

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
substitution_t *
defineSubstitution(template_t *pQuery, char *szSubstitutionName, expr_t *pDefinition)
{
    substitution_t *pSub;

    pSub = (substitution_t *)malloc(sizeof(struct SUBSTITUTION_T));
    MALLOC_CHECK(pSub);
    if (pSub == NULL)
        return(NULL);
    memset(pSub, 0, sizeof(struct SUBSTITUTION_T));
    pSub->name = szSubstitutionName;
    pSub->pAssignment = pDefinition;
    pSub->nSubParts = pDefinition->nValueCount;
    addList(pQuery->SubstitutionList, (void *)pSub);

    return(pSub);
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
int
AddQuerySegment(template_t *pQuery, char *szText)
{
    segment_t *pSegment;

    pSegment = (segment_t *)malloc(sizeof(struct SEGMENT_T));
    MALLOC_CHECK(pSegment);
    if (pSegment == NULL)
        return(-1);
    memset(pSegment, 0, sizeof(struct SEGMENT_T));
    pSegment->text = szText;
    addList(pQuery->SegmentList, (void *)pSegment);

    return(0);
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
Expr_Val_t *
findValue(segment_t *pSegment)
{
    Expr_Val_t *pReturnValue;
    substitution_t *pSub;


    pSub = pSegment->pSubstitution;
    pReturnValue = pSub->arValues;

    pReturnValue += pSub->nSubParts * pSegment->nSubCount;
    pReturnValue += pSegment->nSubUse;

    return(pReturnValue);

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
void PrintTemplate(template_t *t)
{
    substitution_t *pSubstitution;
    segment_t *pSegment;
    
    for (pSubstitution = (substitution_t *)getHead(t->SubstitutionList);
        pSubstitution;
        pSubstitution = (substitution_t *)getNext(t->SubstitutionList))
    {
        printf("DEFINE %s = ", pSubstitution->name);
        PrintExpr(pSubstitution->pAssignment);
        printf(";\n");

    }

    printf("\n\n");

    for (pSegment = (segment_t *)getHead(t->SegmentList); pSegment; pSegment = (segment_t *)getNext(t->SegmentList))
    {
        printf("%s", pSegment->text);
        if (pSegment->pSubstitution)
        {
            printf("[%s]", pSegment->pSubstitution->name);
        }
        printf(" ");
    }

    printf(";\n");

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
void GenerateQuery(FILE *pOutFile, FILE *pLogFile, int nQuery)
{
    int i,
        nBufferCount;
    substitution_t *pSub;
    segment_t *pSegment;
    Expr_Val_t *pValue;
    static int nQueryCount = 1;

    if (pOutFile == NULL)
        pOutFile = stdout;

    /* get the template */
    pCurrentQuery = getItem(TemplateList, nQuery);
    if (!pCurrentQuery)
        ReportError(QERR_QUERY_RANGE, NULL, 1);

    if (g_sbTemplateName == NULL)
    {
        g_sbTemplateName = InitBuffer(20, 10);
    }
    ResetBuffer(g_sbTemplateName);
    AddBuffer(g_sbTemplateName, pCurrentQuery->name);
    if (pLogFile)
        fprintf(pLogFile, "Template: %s\n", pCurrentQuery->name);
    if (is_set("DEBUG"))
        printf("STATUS: Generating Template: %s\n", pCurrentQuery->name);

    /* initialize the template if required */
    if (!(pCurrentQuery->flags & QT_INIT))
    {
        for (pSub = (substitution_t *)getHead(pCurrentQuery->SubstitutionList); 
        pSub; 
        pSub = (substitution_t *)getNext(pCurrentQuery->SubstitutionList))
        {
            nBufferCount = ((pSub->nUse)?pSub->nUse:1) * ((pSub->nSubParts)?pSub->nSubParts:1);
            pSub->arValues = (Expr_Val_t *)malloc(nBufferCount * sizeof(struct EXPR_VAL_T));
            MALLOC_CHECK(pSub->arValues);
            for (i=0; i < nBufferCount; i++)
            {
                memset(&pSub->arValues[i], 0, sizeof(struct EXPR_VAL_T));
#ifdef MEM_TEST
    fprintf(stderr, "pSub arValues %d: %x\n", i, &pSub->arValues[i]);
#endif
                pSub->arValues[i].pBuf = InitBuffer(15, 15);
            }
        }
        pCurrentQuery->flags |= QT_INIT;
    }
    
    /* select the values for this query */
    for (pSub = (substitution_t *)getHead(pCurrentQuery->SubstitutionList); 
    pSub; 
    pSub = (substitution_t *)getNext(pCurrentQuery->SubstitutionList))
    {
        nBufferCount = ((pSub->nUse)?pSub->nUse:1) * ((pSub->nSubParts)?pSub->nSubParts:1);
        for (i=0; i < nBufferCount; i++)
        {
            ResetBuffer(pSub->arValues[i].pBuf);
        }
#ifdef MEM_TEST
    if (pSub->pAssignment->Value.pBuf == NULL) fprintf(stderr, "NULL pBuf %x @ %d\n", pSub->pAssignment, __LINE__);
#endif
        pSub->nDataType = EvalExpr(pSub->pAssignment, pSub->arValues, 0, nQueryCount);
        if (pLogFile)
        {
            for (i=0; i < nBufferCount; i++)
            {
                fprintf(pLogFile, "\t%s.%02d = ", pSub->name, i+1);
            if (!pSub->arValues[i].bUseInt)
                fprintf(pLogFile, "%s\n", GetBuffer(pSub->arValues[i].pBuf));
            else
            {
                fprintf(pLogFile, HUGE_FORMAT, pSub->arValues[i].nValue);
                fprintf(pLogFile, "\n");
            }
            }
        }

    }
    
    /* output the query */
    for (pSegment = (segment_t *)getHead(pCurrentQuery->SegmentList); 
    pSegment; 
    pSegment = (segment_t *)getNext(pCurrentQuery->SegmentList))
    {
        if (pSegment->text)
            fprintf(pOutFile, "%s", pSegment->text);
        if (pSegment->pSubstitution)
        {
            pValue = findValue(pSegment);
         if (!pValue->bUseInt)
                fprintf(pOutFile, "%s", GetBuffer(pValue->pBuf));
            else
            {
                fprintf(pOutFile, HUGE_FORMAT, pValue->nValue);
            }
        }
        if (pSegment->flags & QS_EOS)
            fprintf(pOutFile, ";\n");
    }
    
    nQueryCount += 1;

    
    return;
}

