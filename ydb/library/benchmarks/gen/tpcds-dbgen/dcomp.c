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
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include <sys/stat.h>
#include <fcntl.h>
#ifdef AIX
#include <sys/mode.h>
#endif
#ifndef WIN32
#include <netinet/in.h>
#endif
#include "r_params.h"
#include "dcomp_params.h"
#include "error_msg.h"
#include "grammar.h"
#include "dist.h"
#include "dcgram.h"
#include "dcomp.h"
#include "substitution.h"
#include "grammar_support.h" /* to get definition of file_ref_t */

char *CurrentFileName = NULL;
distindex_t *pDistIndex;
file_ref_t CurrentFile;
file_ref_t *pCurrentFile;

/*
 * Routine: WriteIndex()
 * Purpose: traverse the distributions list and create the binary 
 *        version of distribution output
 * Algorithm:
 * Data Structures:
 *
 * Params: (list_t)
 * Returns:
 * Called By: 
 * Calls: 
 * Assumptions:
 * Side Effects:
 * TODO: 
 *     19990311 add data file format to header
 *     20000112 need to allow for changes to an existing index file
 *     20000112 need to allow for multiple index files
 */
int 
WriteIndex(distindex_t *t)
{
   d_idx_t *idx = NULL;
   dist_t *d;
   int32_t i, j, 
      nDist,
      *pSet,
      err_cnt = 0,
      offset=0,
      data_type,
      temp;
   FILE *fpHeader = NULL;
   FILE *ofp = NULL;
   char *cp;

    if ((ofp = fopen(get_str("OUTPUT"), "wb")) == NULL)
    {
        printf("ERROR: Cannot open output file '%s'\n", 
        get_str("OUTPUT"));
        usage(NULL, NULL);
    }

    /* open the header file */
    if ((fpHeader = fopen(get_str("HEADER"), "w")) == NULL)
        return(99);
    fprintf(fpHeader, "/*\nTHIS IS AN AUTOMATICALLY GENERATED FILE\nDO NOT EDIT\n\nSee distcomp.c for details\n*/\n");
    
    /* output the number of distributions in the file */
    temp = htonl(pDistIndex->nDistCount);
    if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
        return(12);
    offset += sizeof(int32_t);

    /* then walk the distributions and write each one in turn */

    for (nDist=0; nDist < pDistIndex->nDistCount; nDist++)
        {
        idx = pDistIndex->pEntries + nDist;
        d = idx->dist;
        idx->offset = offset;

        /* and then output the distribution to the file */
        /* format is:
         * a v_width data types stored as integers
         * a sequence of weigth sets, each <length> integers
         * a sequence of string offsets, each <length> integers
         * a sequence of aliases for values and weights, with each value NULL terminated
         * the string values for each value vector, with each value NULL
         *    terminated
         */
        /* output type data type vector */
        for (i=0; i < idx->v_width; i++)
            {
            data_type = d->type_vector[i];
            temp = htonl(data_type);
            if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
                err_cnt = 12;
            else 
                offset += sizeof(int32_t);
            }

        /* output the weight sets */
        for (i=0; i < idx->w_width; i++)
            {
            pSet = d->weight_sets[i];
            for (j=0; j < idx->length; j++)
                {
                temp = htonl(pSet[j]);
                if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
                    err_cnt = 6;
                else 
                    offset += sizeof(int32_t);
                }
            }

        /* output the string offsets */
        for (i=0; i < idx->v_width; i++)
            {
            pSet = d->value_sets[i];
            for (j=0; j < idx->length; j++)
                {
                temp = htonl(pSet[j]);
                if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
                    err_cnt = 8;
                else 
                    offset += sizeof(int32_t);
                }
            }

        /* output the column aliases and generated the associated header file entries */
        fprintf(fpHeader, "\n/* aliases for values/weights in the %s distribution */\n", idx->name);
        if (d->names)
        {
            if (fwrite(d->names, 1, idx->name_space, ofp) < (size_t)idx->name_space)
                err_cnt = 8;
            else 
                offset += idx->name_space;
            
            cp = d->names;
            for (i=0; i < idx->v_width + idx->w_width; i++)
            {
                fprintf(fpHeader, "#define %s_%s\t%d\n",
                    idx->name,
                    cp,
                    (i >= idx->v_width)?i - idx->v_width + 1:i + 1);
                cp += strlen(cp) + 1;
            }
        }
        else
            fprintf(fpHeader, "/* NONE DEFINED */\n");

        /* output the strings themselves */
        if (fwrite(d->strings, 1, idx->str_space, ofp) < (size_t)idx->str_space)
            err_cnt = 8;
        else 
            offset += idx->str_space;

        }

    /* finally, re-write the index */
    for (i=0; i < pDistIndex->nDistCount; i++)
        {
        idx = pDistIndex->pEntries + i;
        if (fwrite(idx->name, 1, D_NAME_LEN, ofp) < 0)
            {err_cnt = 9; break;}
        temp = htonl(idx->index);
        if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
            {err_cnt = 10; break;}
        temp = htonl(idx->offset);
        if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
            {err_cnt = 12; break;}
        temp = htonl(idx->str_space);
        if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
            {err_cnt = 13; break;}
        temp = htonl(idx->length);
        if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
            {err_cnt = 15; break;}
        temp = htonl(idx->w_width);
        if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
            {err_cnt = 16; break;}
        temp = htonl(idx->v_width);
        if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
            {err_cnt = 17; break;}
        temp = htonl(idx->name_space);
        if (fwrite(&temp, 1, sizeof(int32_t), ofp) < 0)
            {err_cnt = 18; break;}
        }

    fclose(ofp);
    fclose(fpHeader);

   return(err_cnt);
}    

 /*
 * Routine: main()
 * Purpose: provide command line interface to distcomp routines
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By: 
 * Calls: 
 * Assumptions:
 * Side Effects:
 * TODO: jms 20041013: rework as version/option of qgen
 */

int main(int argc, char* argv[])
{
    int nArgs,
        i;
    char szPath[128];    /*  need file path length define */
    char szHeader[128];
    d_idx_t *pIndexEntry;

    nArgs = process_options(argc, argv);
    if (!is_set("INPUT") || !is_set("OUTPUT"))
        usage(NULL, "Must specify input and output file names");
    if (!is_set("HEADER"))
    {
        strcpy(szHeader, get_str("OUTPUT"));
        strcat(szHeader, ".h");
        set_str("HEADER", szHeader);
    }

    
    
    /* setup the dist index */
    pDistIndex = (distindex_t *)malloc(sizeof(struct DISTINDEX_T));
    MALLOC_CHECK(pDistIndex);
    if (pDistIndex == NULL)
        ReportError(QERR_NO_MEMORY, "main", 1);
    memset((void *)pDistIndex, 0, sizeof(struct DISTINDEX_T));
    /*
    pDistIndex->pEntries = (d_idx_t *)malloc(100 * sizeof(struct D_IDX_T));
    MALLOC_CHECK(pDistIndex->pEntries);
    if (pDistIndex->pEntries == NULL)
        ReportError(QERR_NO_MEMORY, "main", 1);
    pDistIndex->nAllocatedCount = 100;
    for (i=0; i < 100; i++)
        memset(pDistIndex->pEntries + i, 0, sizeof(struct D_IDX_T));
    */
        

    SetTokens(dcomp_tokens);

    if ((i = ParseFile(get_str("INPUT"))) != 0)
    {
        printf("ERROR: Parse failed for %s\n", 
            get_str("INPUT"));
        ReportError(i, NULL, 1);
        exit(1);
    }

    if (is_set("VERBOSE"))
    {
        printf("Defined %d distributions:\n", pDistIndex->nDistCount);
        for (i=0; i < pDistIndex->nDistCount; i++)
        {
            pIndexEntry = pDistIndex->pEntries + i;
            printf("\t%s[%d] --> (%d,%d)",
                pIndexEntry->name,
                pIndexEntry->length,
                pIndexEntry->v_width,
                pIndexEntry->w_width);
            if (pIndexEntry->name_space)
                printf(" with names");
            printf("\n");
        }
    }

    if ((i = WriteIndex(pDistIndex)) > 0)        
        {
        sprintf(szPath, "WriteDist returned %d writing to %s", 
            i, get_str("OUTPUT"));
        ReportError(QERR_WRITE_FAILED, szPath, 1);
        }

    return 0;
}
