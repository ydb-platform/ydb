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
#define DECLARER
#include "config.h"
#include "porting.h"
#include <stdio.h>
#ifdef USE_STRING_H
#include <string.h>
#else
#include <strings.h>
#endif
#include "StringBuffer.h"
#include "expr.h"
#include "grammar_support.h"
#include "keywords.h"
#include "substitution.h"
#include "error_msg.h"
#include "qgen_params.h"
#include "genrand.h"
#include "query_handler.h"
#include "release.h"
#include "list.h"
#include "permute.h"
#include "dist.h"
#include "tdef_functions.h"

template_t *pCurrentQuery,
    *g_Template;
list_t *TemplateList;


int g_nQueryNumber, 
    g_nStreamNumber;
StringBuffer_t *g_sbTemplateName = NULL;


int yydebug;
int yyparse(void);
extern FILE *yyin;
extern     file_ref_t *pCurrentFile;
table_func_t w_tdef_funcs[MAX_TABLE];

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
void
parseTemplate(char *szFileName, int nIndex)
{
    int nWarning,
        nError;
    char szPath[1024];
            
        pCurrentQuery = (template_t *)malloc(sizeof(struct TEMPLATE_T));
        MALLOC_CHECK(pCurrentQuery);
        if (!pCurrentQuery)
            ReportErrorNoLine(QERR_NO_MEMORY, "parseQueries()", 1);
        memset(pCurrentQuery, 0, sizeof(struct TEMPLATE_T));
        pCurrentQuery->SegmentList = makeList(L_FL_TAIL, NULL);
        pCurrentQuery->SubstitutionList = makeList(L_FL_SORT, compareSubstitution);    
        pCurrentQuery->DistributionList = makeList(L_FL_SORT, di_compare);

        /*
         * each query template is parsed as though:
         *    it had explicitly included the dialect template
         *    it began the query with a [_begin] substitution
         *    it ended the query with an [_end] substitution
         */
        pCurrentFile = NULL;
      if (is_set("DIRECTORY"))
           sprintf(szPath, "%s/%s", get_str("DIRECTORY"),szFileName);
      else
         strcpy(szPath, szFileName);
        if (include_file(szPath, pCurrentQuery) < 0)
            ReportErrorNoLine(QERR_NO_FILE, szPath, 1);
        sprintf(szPath, "%s/%s.tpl", get_str("DIRECTORY"), get_str("DIALECT"));
        if (include_file(szPath, pCurrentQuery) < 0)
            ReportErrorNoLine(QERR_NO_FILE, szPath, 1);
    
        /* parse the template file */
        yyparse();

        /* 
         * add in query start substitution, now that it has been defined 
         */
        pCurrentQuery->SegmentList->nFlags &= ~L_FL_TAIL;
        pCurrentQuery->SegmentList->nFlags |= L_FL_HEAD;
        AddQuerySegment(pCurrentQuery, "\n");
        AddQuerySegment(pCurrentQuery, "");
        ((segment_t *)pCurrentQuery->SegmentList->head->pData)->pSubstitution = findSubstitution(pCurrentQuery, "_BEGIN", 0);
        pCurrentQuery->SegmentList->nFlags &= ~L_FL_HEAD;
        pCurrentQuery->SegmentList->nFlags |= L_FL_TAIL;
        
        /* check for any parsing errors */
        GetErrorCounts(&nError, &nWarning);
        if (nError)
        {
            printf("%d Errors encountered parsing %s\n", 
                nError, szFileName);
            exit(1);
        }
        if (nWarning)
        {
            printf("WARNING: %d warnings encountered parsing %s\nWARNING: Query output may not be correct!\n", 
                nWarning, szFileName);
        }
        
        addList(TemplateList, pCurrentQuery);
        pCurrentQuery->index = nIndex;
        pCurrentQuery->name = strdup(szFileName);

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
void
parseQueries(void)
{
    char szFileName[1024],
        *cp;
    FILE *pInputFile;
    int nIndex = 1;
    
    if (!is_set("INPUT"))
    {
        ReportErrorNoLine(QERR_NO_QUERYLIST, NULL, 1);
    }

    strcpy(szFileName, get_str("INPUT"));
    
#ifndef WIN32
    if ((pInputFile = fopen(szFileName, "r")) == NULL)
#else
    if ((pInputFile = fopen(szFileName, "rt")) == NULL)
#endif
    {
        SetErrorGlobals(szFileName, NULL);
        ReportErrorNoLine(QERR_OPEN_FAILED, szFileName, 1);
    }
    
    while (fgets(szFileName, 1024, pInputFile))
    {
        if (strncmp(szFileName, "--", 2) == 0)
            continue;
        if ((cp = strchr(szFileName, '\n')))
            *cp = '\0';
        if (!strlen(szFileName))
            continue;
            
        parseTemplate(szFileName, nIndex++);
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
void
generateQueryStreams(void)
{
    int nStream,
        nQuery,
        nQueryCount,
        *pPermutation = NULL,
        nVersionCount,
        nCount,
        nQID;
    FILE *pOutFile;
    FILE *pLogFile = NULL;
    char szPath[1024];

    nQueryCount = length(TemplateList);
    nVersionCount = get_int("COUNT");
    
    if (is_set("LOG"))
    {
#ifndef WIN32
        if ((pLogFile = fopen(get_str("LOG"), "w")) == NULL)
#else
        if ((pLogFile = fopen(get_str("LOG"), "wt")) == NULL)
#endif
        {
            SetErrorGlobals(get_str("LOG"), NULL);
            ReportErrorNoLine(QERR_OPEN_FAILED, get_str("LOG"), 1);
        }
    }

    for (nStream=0; nStream < get_int("STREAMS"); nStream++)
    {
        /* 
         * use stream 1 for permutation, and stream 0 for all other RNG calls in qgen, 
         * to assure permutation stability regardless of command line seed 
         */
        Streams[1].nInitialSeed = 19620718;
        Streams[1].nSeed = 19620718;
        pPermutation = makePermutation(pPermutation, nQueryCount, 1);
    
        sprintf(szPath, "%s%squery_%d.sql",
            get_str("OUTPUT_DIR"),
            get_str("PATH_SEP"),
            nStream);
        if (!is_set("FILTER"))
        {
#ifndef WIN32
            if ((pOutFile = fopen(szPath, "w")) == NULL)
#else
                if ((pOutFile = fopen(szPath, "wt")) == NULL)
#endif
                {
                    SetErrorGlobals(szPath, NULL);
                    ReportErrorNoLine(QERR_OPEN_FAILED, szPath, 1);
                }
        }
        else
            pOutFile = stdout;

        g_nStreamNumber = nStream;
        if (pLogFile)
            fprintf(pLogFile, "BEGIN STREAM %d\n", nStream);
        for (nQuery = 1; nQuery <= nQueryCount; nQuery++)
        {
            for (nCount = 1; nCount <= nVersionCount; nCount++)
            {
            g_nQueryNumber = nQuery;
            if (is_set("QUALIFY"))
                nQID = nQuery;
            else
                nQID = getPermutationEntry(pPermutation, nQuery);
            GenerateQuery(pOutFile, pLogFile, nQID);
            if (pLogFile)
                fprintf(pLogFile, "\n");
            }
        }
        if (pLogFile)
            fprintf(pLogFile, "END STREAM %d\n", nStream);

        if (!is_set("FILTER"))
            fclose(pOutFile);
    }

    if (pLogFile)
        fclose(pLogFile);

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
int 
main(int ac, char* av[])
{
    template_t *pTemplate;

    process_options (ac, av);

    if (!is_set("QUIET"))
    {
        fprintf (stderr,
        "%s Query Generator (Version %d.%d.%d%s)\n",
        get_str("PROG"), VERSION, RELEASE, MODIFICATION, PATCH);
    fprintf (stderr, "Copyright %s %s\n", COPYRIGHT, C_DATES);
    }

    TemplateList = makeList(L_FL_TAIL, NULL);

    /* sync the keyword defines between lex/yacc/qgen */
    InitKeywords();
    
    if (is_set("YYDEBUG"))
        yydebug = 1;
    

    if (is_set("TEMPLATE"))
      parseTemplate(get_str("TEMPLATE"), 1);
    else
        parseQueries();    /* load the query templates */
    
    
    if (is_set("VERBOSE") && !is_set("QUIET"))
        fprintf(stderr, "Parsed %d templates\n", length(TemplateList));
    if (is_set("DUMP"))
    {
        for (pTemplate = (template_t *)getHead(TemplateList); pTemplate; pTemplate = (template_t *)getNext(TemplateList))
            PrintTemplate(pTemplate);
    }

    init_rand();

    generateQueryStreams();    /* output the resulting SQL */

    exit(0);

}
        

