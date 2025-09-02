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
#ifndef SUBSTITUION_T
#define SUBSTITUION_T
#include "StringBuffer.h"
#include "expr.h"
#include "list.h"
#include "eval.h"

/*
* a substitution is the defintion of one of the macros (textual subtitutions) in a query template
*/
typedef struct SUBSTITUTION_T {
    char *name;
    int flags;
    int nUse;    /* how many unique uses of this substitution in the template */
    int nSubParts; /* are there parts within the substitution? */
    expr_t *pAssignment;
    /* selected values are stored in arValues[] */
    struct EXPR_VAL_T  *arValues;
    int *pPermute;    /* each use may need a permutation */
    struct TEMPLATE_T *pTemplate;
    int nDataType;    /* type of the resulting value */
    int nQueryNumber;
} substitution_t;

/* flag defintions */

/*    a segment is a part of a query template. It is comprised of the static preamble 
    (text) and the optional dynamic placeholder (substitution) 
*/
typedef struct SEGMENT_T {
    char *text;    /* the text preamble to a substitution point */
    int flags;
    substitution_t  *pSubstitution; /* the substitution */
    int nSubCount; /* the usage count of the substitution */
    int nSubUse;    /* the sub component of the substitution */
} segment_t;
#define QS_EOS    0x0001

typedef struct TEMPLATE_T {
    char *name;
    int index;
    int flags;
    int nRowLimit;    /* used with [LIMIT] to control number of rows returned */
    list_t *SubstitutionList;
    list_t *SegmentList;
    list_t *DistributionList;
} template_t;
#define QT_INIT                0x0001

extern template_t *pCurrentQuery;

void        PrintQuery(FILE *fp, template_t *t);
int            AddQuerySegment(template_t *pQuery, char *szSQL);
int            AddQuerySubstitution(template_t *Query, char *szSubName, int nUse, int nSubPart);
int            AddSubstitution(template_t *t, char *s, expr_t *pExpr);
int            SetSegmentFlag(template_t *Query, int nSegmentNumber, int nFlag);
substitution_t *findSubstitution(template_t *t, char *stmt, int *nUse);
int            compareSubstitution(const void *p1, const void *p2);
Expr_Val_t *findValue(segment_t *pSeg);

#endif
