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
#include <stdlib.h>
#include <string.h>
#include "error_msg.h"
#include "dist.h"
#include "date.h"
#include "decimal.h"
#include "misc.h"
#include "genrand.h"
#include "substitution.h"
#include "StringBuffer.h"

extern template_t *pCurrentTemplate,
    *g_Template;
int ParseFile(char *szPath);

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
compareSubstitution(const void *p1, const void *p2)
{
    substitution_t *pS1 = (substitution_t *)p1,
        *pS2 = (substitution_t *)p2;

    if (pS1 == NULL)
    {
        if (pS2 == NULL)
            return(0);
        else
            return(-1);
    }
    
    if (pS2 == NULL)
        return(1);
    
    return(strcasecmp(pS1->name, pS2->name));
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
substitution_t *
findSubstitution(template_t *t, char *name, int *nUse)
{
    int nChars,
        nUseCount;
    substitution_t *pSub;
    static substitution_t tempSubstitution;
    static int bInit = 0;

    if (!bInit)
    {
        memset(&tempSubstitution, 0, sizeof(struct SUBSTITUTION_T));
        tempSubstitution.name = malloc(100 * sizeof(char));
        MALLOC_CHECK(tempSubstitution.name);
        bInit = 1;
    }
    
    /* exclude any numeric suffix from search, but update nUses */
    nChars = strcspn(name, "0123456789");
    if (strlen(name) > 100)
        tempSubstitution.name = realloc(tempSubstitution.name, strlen(name) + 1);
    strncpy(tempSubstitution.name, name, nChars);
    tempSubstitution.name[nChars] = '\0';
    pSub = findList(t->SubstitutionList, (void *)&tempSubstitution);
    if (!pSub) /* the substitution could be global; add a local reference */
    {
        pSub = findList(g_Template->SubstitutionList, (void *)&tempSubstitution);
        if (pSub)
            addList(t->SubstitutionList, pSub);
    }
    if (pSub)
    {
        nUseCount = atoi(name + nChars);
        if (nUseCount == 0)
            nUseCount = 1;
        if (nUseCount > pSub->nUse)
            pSub->nUse = nUseCount;
        if (nUse)    /* we're interested in the usage number */
            *nUse = nUseCount;
        return(pSub);
    }
    
    return(NULL);
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
AddQuerySubstitution(template_t *t, char *szName, int nUse, int bEndSuffix)
{
    substitution_t *pSub;
    segment_t *pSegment;
    int nSegmentCount;
    
    if ((pSub = findSubstitution(t, szName, NULL)) == NULL)
        ReportError(QERR_NO_INIT, szName, 1);
    nSegmentCount = length(t->SegmentList);
    if (nSegmentCount == 0)    /* template starts with a substitution */
        AddQuerySegment(t, "");
    pSegment = (segment_t *)getItem(t->SegmentList, length(t->SegmentList));
    if (pSegment->pSubstitution)
    {
        AddQuerySegment(t, "");
        pSegment = (segment_t *)getItem(t->SegmentList, length(t->SegmentList));
    }
    pSegment->pSubstitution = pSub;
    pSegment->nSubUse = bEndSuffix;
    if (pSub->pAssignment->nFlags & (EXPR_FL_LIST|EXPR_FL_ULIST))
        pSegment->nSubUse -= 1;
    pSegment->nSubCount = nUse;
    
    return(0);
}

