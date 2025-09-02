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
#include <ctype.h>
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include "error_msg.h"
#include "grammar.h"
#include "dist.h"
#include "dcomp.h"
#include "r_params.h"
#include "dcgram.h"

#ifdef MEM_CHECK
int nMemTotal = 0;
#define MALLOC(size) malloc(size);fprintf(stderr, "Malloc %d at %d for a total of %d\n", size, __LINE__, nMemTotal += size)
#define REALLOC(locale, size) realloc(locale, size);fprintf(stderr, "Realloc %d at %d\n", size, __LINE__)
#else
#define MALLOC(size) malloc(size)
#define REALLOC(locale, size) realloc(locale, size)
#endif

/*
 * Miscelaneous scratch pad space, used while a distribution is being parsed
 */
extern distindex_t *pDistIndex;
extern int nLineNumber;
extern char *CurrentFileName;
d_idx_t *pCurrentIndexEntry;
int nMaxValueWidth = 0;
char **arValues = NULL;
int *arValueLengths = NULL;
int nMaxWeightWidth = 0;
int *arWeights = NULL;

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
 * TODO: 
 *    20021206 jms This routine should allow builtin integer functions like ROWCOUNT(), but they are domain specific
 */
int
ProcessInt (char *stmt, token_t * tokens)
{
    int nRetCode = 0;
    char *cp;
    
    cp = SafeStrtok(NULL, " \t,");
    if (cp == NULL)
        return(QERR_SYNTAX);
       
    nRetCode = atoi(cp);
    return (nRetCode);
}

/*
 * Routine: AddDistribution()
 * Purpose: Add a new distribution to a DistIndex, and assure uniqueness
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns: pointer to new, empty distribution, or NULL
 * Called By: 
 * Calls: 
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
d_idx_t *
AddDistribution (distindex_t * pDistIndex, char *szName)
{
   d_idx_t *pNewDist;
   int i;

   /*
      * check that arguments are reasonable
    */
   if (strlen (szName) == 0)
      return (NULL);
   if (pDistIndex == NULL)
      return (NULL);

   /***
    * check for name uniqeness, and expand dist set if required
    */
   for (i = 0; i < pDistIndex->nDistCount; i++)
      if (strcasecmp (szName, pDistIndex->pEntries[i].name) == 0)
         ReportError (QERR_NON_UNIQUE, szName, 1);

   if (pDistIndex->nDistCount == pDistIndex->nAllocatedCount)
     {
        pDistIndex->nAllocatedCount += 100;
        pDistIndex->pEntries =
           (d_idx_t *) realloc (pDistIndex->pEntries,
                                pDistIndex->nAllocatedCount *
                                sizeof (struct D_IDX_T));
        if (pDistIndex->pEntries == NULL)
           ReportError (QERR_NO_MEMORY, "main", 1);
     }
   pNewDist = pDistIndex->pEntries + pDistIndex->nDistCount;
   pDistIndex->nDistCount += 1;
   memset(pNewDist, 0, sizeof(d_idx_t));

   /*
      * initialize the distribution
    */
   if (strlen (szName) > D_NAME_LEN)
     {
        szName[D_NAME_LEN] = '\0';
        ReportError (QERR_STR_TRUNCATED, szName, 0);
     }
   strcpy (pNewDist->name, szName);
   pNewDist->index = pDistIndex->nDistCount;
   pNewDist->dist = (dist_t *) MALLOC (sizeof (struct DIST_T));
   if (pNewDist->dist == NULL)
      ReportError (QERR_NO_MEMORY, "MALLOC(dist_t)", 1);
   memset (pNewDist->dist, 0, sizeof (dist_t));

   if (is_set ("VERBOSE"))
      fprintf (stderr, "Created distribution '%s'\n", szName);

   return (pNewDist);
}



/*
 * Routine: ProcessSet
 * Purpose: Read distribution settings
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
 *
 * NOTE: if QERR_SYNTAX can be a valid return value, we have a problem. 
 */
int
ProcessSet (char *stmt, token_t * tokens)
{
   int nRetCode = 0,
     i;
   char *cp = NULL;

   cp = SafeStrtok (NULL, " \t=");
   switch (i = FindToken (cp))
     {
     case TKN_WEIGHTS:
        cp = SafeStrtok (NULL, " \t");    /* discard = */
        pCurrentIndexEntry->w_width = ProcessInt (stmt, tokens);
        if (pCurrentIndexEntry->w_width == QERR_SYNTAX)
           nRetCode = QERR_RANGE_ERROR;
        else
          {
             if (pCurrentIndexEntry->w_width > nMaxWeightWidth)
               {
                  arWeights = (int *) REALLOC (arWeights,
                                               pCurrentIndexEntry->w_width *
                                               sizeof (int));
                  if (arWeights == NULL)
                     nRetCode = QERR_NO_MEMORY;
               }
             else
                nMaxWeightWidth = pCurrentIndexEntry->w_width;
          }
        pCurrentIndexEntry->dist->weight_sets =
           (int **) MALLOC (pCurrentIndexEntry->w_width * sizeof (int *));
        if (pCurrentIndexEntry->dist->weight_sets == NULL)
           nRetCode = QERR_NO_MEMORY;
        memset(pCurrentIndexEntry->dist->weight_sets, 0, pCurrentIndexEntry->w_width * sizeof(int *));
        break;
     case TKN_TYPES:
        pCurrentIndexEntry->v_width = ProcessTypes (stmt, tokens);
        if (pCurrentIndexEntry->v_width == QERR_SYNTAX)
           nRetCode = QERR_RANGE_ERROR;
        else
          {
             if (pCurrentIndexEntry->v_width > nMaxValueWidth)
               {
                  arValues =
                     (char **) REALLOC (arValues,
                                        pCurrentIndexEntry->v_width *
                                        sizeof (char *));
                  arValueLengths =
                     (int *) REALLOC (arValueLengths,
                                      pCurrentIndexEntry->v_width *
                                      sizeof (int));
               }
             if (arValues == NULL || arValueLengths == NULL)
                nRetCode = QERR_NO_MEMORY;
             else
        {
        for (i=nMaxValueWidth; i < pCurrentIndexEntry->v_width; i++)
            {
            arValueLengths[i] = 0;
            arValues[i] = NULL;
            }
                nMaxValueWidth = pCurrentIndexEntry->v_width;
        }
          }
        pCurrentIndexEntry->dist->value_sets =
           (int **) MALLOC (pCurrentIndexEntry->v_width * sizeof (int *));
        if (pCurrentIndexEntry->dist->value_sets == NULL)
           nRetCode = QERR_NO_MEMORY;
        memset(pCurrentIndexEntry->dist->value_sets, 0, pCurrentIndexEntry->v_width * sizeof(int *));
        break;
     case TKN_NAMES:
         if ((pCurrentIndexEntry->v_width <= 0) || (pCurrentIndexEntry->w_width <= 0))
             return(QERR_NAMES_EARLY);
         pCurrentIndexEntry->name_space = ProcessNames(stmt, tokens);
        break;
     default:
        nRetCode = QERR_SYNTAX;
     }

   return (nRetCode);
}

/*
 * Routine: ProcessDistribution
 * Purpose: Handle creation of new dist index entry
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
ProcessDistribution (char *stmt, token_t * tokens)
{
   int nRetCode = 0;
   char *cp;

   /*  Validate the new substitution name and add it to the template */
   cp = SafeStrtok (NULL, " \t=\r;");
   if (cp == NULL)
      return (QERR_SYNTAX);

   pCurrentIndexEntry = AddDistribution (pDistIndex, cp);
   if (pCurrentIndexEntry == NULL)
      return (QERR_DEFINE_OVERFLOW);

   return (nRetCode);
}


/*
 * Routine: ProcessTypes
 * Purpose: Parse the type vector
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
ProcessTypes (char *stmt, token_t * tokens)
{
   char *cp,
    *cp1;
   int nTypeCount = 1,
     nToken,
     i;

   /* get a type count */
   for (cp1 = stmt; (cp1 = strchr (cp1, ',')) != NULL; cp1++)
      nTypeCount += 1;
   pCurrentIndexEntry->dist->type_vector =
      (int *) MALLOC (sizeof (int) * nTypeCount);
   if (pCurrentIndexEntry->dist->type_vector == NULL)
      return (QERR_NO_MEMORY);
    memset(pCurrentIndexEntry->dist->type_vector, 0, sizeof(int) * nTypeCount);

   /* get the type names */
   i = 0;
   while ((cp = strtok (NULL, "=( ,);")) != NULL)
   {
       switch (nToken = FindToken (cp))
       {
/*
 * NOTE NOTE NOTE NOTE NOTE
 * this is manually sync'd with expr.h values
 * NOTE NOTE NOTE NOTE NOTE
 */
       case TKN_INT:
       case TKN_VARCHAR:
           pCurrentIndexEntry->dist->type_vector[i++] = nToken;
           break;
       default:
           return (QERR_SYNTAX);
       }
   }
   
   return (nTypeCount);
}

/*
 * Routine: ProcessNames
 * Purpose: Parse the name vector
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
ProcessNames (char *stmt, token_t * tokens)
{
    char *szResult = NULL;
    char *cp;
    int nCount = 0,
        nWordLength = 0;

   /* get the names */
   while ((cp = strtok (NULL, "=( ,);:")) != NULL)
   {
       if (nCount == 0)
       {
           nWordLength = strlen(cp);
           szResult = malloc(nWordLength + 1);
                   MALLOC_CHECK(szResult);
           nCount = nWordLength + 1;
           strcpy(szResult, cp);
       }
       else
       {
           nWordLength = strlen(cp);
           szResult = realloc(szResult, nCount + nWordLength + 1);
           strcpy(szResult + nCount, cp);
           nCount += nWordLength + 1;

       } 
   }
   
   pCurrentIndexEntry->dist->names = szResult;
   return (nCount);
}

/*
 * Routine: ProcessInclude
 * Purpose: Allow nested files
 * Algorithm:
 * Data Structures:
 *
 * Parindent: Standard input:235: Error:Unexpected end of file
ams:
 * Returns:
 * Called By: 
 * Calls: 
 * Assumptions:
 * Side Effects:
 * TODO: 
 *     20020515: should allow for escaped quotation marks
 */
int
ProcessInclude (char *stmt, token_t * tokens)
{
   char *cp;
   int nRetCode;
   char *szHoldName;
   int nHoldLine;

   cp = ProcessStr (stmt, tokens);
   szHoldName = strdup(CurrentFileName);
   nHoldLine = nLineNumber;
   nRetCode = ParseFile (cp);
   free(CurrentFileName);
   CurrentFileName = szHoldName;
   nLineNumber = nHoldLine;

   return (nRetCode);

}

/*
 * Routine: ProcessAdd
 * Purpose: Handle the entries themselves
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
ProcessAdd (char *stmt, token_t * tokens)
{
   int i,
     nStrSpace = 0,
     nTokenLength,
     nExtendedLength;
   char *cp,
    *cp2,
    *cp3;
   dist_t *pCurrentDist = pCurrentIndexEntry->dist;


   /* confirm distribution dimensions */
   if (pCurrentIndexEntry->v_width == 0)
      return (QERR_NO_TYPE);
   if (pCurrentIndexEntry->w_width == 0)
      return (QERR_NO_WEIGHT);

   /* get the values */
   nStrSpace = 0;
   cp2 = stmt;
   for (i = 0; i < pCurrentIndexEntry->v_width; i++)
     {
        /* check/strip quotes from a varchar entry */
        if (pCurrentDist->type_vector[i] == TKN_VARCHAR)
          {
             while (*cp2)
                if (*cp2 == '"')
                   break;
                else
                   cp2 += 1;
             if (*cp2 == '\0')
                ReportError (QERR_SYNTAX, "string without quotation marks", 1);
             cp = cp2 + 1;
             cp2 = cp;
             while (*cp2)
                if (*cp2 == '"')
                   break;
                else
                   cp2 += 1;
             if (*cp2 == '\0')
                ReportError (QERR_SYNTAX, "non-terminated string", 1);
             *cp2 = '\0';
             cp2 += 1;
          }
        else
          {
             while (*cp2)
                if (isdigit (*cp2) || (*cp2 == '-'))
                   break;
                else
                   cp2 += 1;
             if (*cp2 == '\0')
                ReportError (QERR_SYNTAX, "invalid integer value", 1);
             cp = cp2;
             while (*cp2)
                if (!(isdigit (*cp2) || (*cp2 == '-')))
                   break;
                else
                   cp2 += 1;
             if (*cp2 == '\0')
                ReportError (QERR_SYNTAX, "badly formed integer value", 1);
             *cp2 = '\0';
             cp2 += 1;
          }
        /* remove any escaped characters from the varchar */
        while ((cp3 = strchr(cp, '\\')) != NULL)
            memmove(cp3, cp3+1, strlen(cp3));

        nTokenLength = strlen (cp);
        if (arValues[i] == NULL)
          {
             arValues[i] = (char *) MALLOC (sizeof (char) * (nTokenLength + 1));
             if (arValues[i] == NULL)
                ReportError(QERR_NO_MEMORY, "arValues[]", 1);
             arValueLengths[i] = nTokenLength;
          }
        else if (arValueLengths[i] < nTokenLength)
          {
             arValues[i] =
                (char *) REALLOC (arValues[i],
                                  sizeof (char) * (nTokenLength + 1));
             arValueLengths[i] = nTokenLength;
          }
        strcpy (arValues[i], cp);
        nStrSpace += nTokenLength + 1;
     }

   /* get the weights */
   for (i = 0; i < pCurrentIndexEntry->w_width; i++)
     {
        cp = SafeStrtok (cp2, ":) \t,");
        if (cp == NULL)
           ReportError (QERR_SYNTAX, "invalid weight count", 1);
        nTokenLength = strlen (cp);
        if (nTokenLength == 0)
           ReportError (QERR_SYNTAX, "zero length weight", 1);
        arWeights[i] = atoi (cp);
        cp2 = NULL;
     }


   /* if necessary, extend the distributions storage */
   /* for the weights and offset values */
   if (pCurrentIndexEntry->nAllocatedLength == pCurrentIndexEntry->length)
     {
        nExtendedLength = pCurrentIndexEntry->length + 100;
        for (i = 0; i < pCurrentIndexEntry->w_width; i++)
          {
             if (pCurrentIndexEntry->length == 0)
               {
                  pCurrentDist->weight_sets[i] =
                     (int *) MALLOC (sizeof (int) * nExtendedLength);
               }
             else
               {
                  pCurrentDist->weight_sets[i] =
                     (int *) REALLOC (pCurrentDist->weight_sets[i],
                                      sizeof (int) * nExtendedLength);
               }
             if (pCurrentDist->weight_sets[i] == NULL)
                return (QERR_NO_MEMORY);
          }
        for (i = 0; i < pCurrentIndexEntry->v_width; i++)
          {
             if (pCurrentIndexEntry->length == 0)
               {
                  pCurrentDist->value_sets[i] =
                     (int *) MALLOC (sizeof (int) * nExtendedLength);
               }
             else
               {
                  pCurrentDist->value_sets[i] =
                     (int *) REALLOC (pCurrentDist->value_sets[i],
                                      sizeof (int) * nExtendedLength);
               }
             if (pCurrentDist->value_sets[i] == NULL)
                return (QERR_NO_MEMORY);
          }
        pCurrentIndexEntry->nAllocatedLength = nExtendedLength;
     }

   /* if necessary, extend the distributions storage */
   /* for the string values themselves */

   if (pCurrentIndexEntry->nRemainingStrSpace <= nStrSpace)
     {
        if (pCurrentDist->strings == NULL)
          {
             pCurrentDist->strings = MALLOC (sizeof (char) * 1000);
          }
        else
          {
             pCurrentDist->strings =
                REALLOC (pCurrentDist->strings,
                         pCurrentIndexEntry->str_space + sizeof (char) * 1000);
          }
        if (pCurrentDist->strings == NULL)
           return (QERR_NO_MEMORY);
        pCurrentIndexEntry->nRemainingStrSpace = 1000;

     }

   /* and now add in the new info */
   for (i = 0; i < pCurrentIndexEntry->w_width; i++)
      *(pCurrentDist->weight_sets[i] + pCurrentIndexEntry->length) =
         arWeights[i];
   for (i = 0; i < pCurrentIndexEntry->v_width; i++)
     {
        *(pCurrentDist->value_sets[i] + pCurrentIndexEntry->length) =
           pCurrentIndexEntry->str_space;
        cp = pCurrentDist->strings + pCurrentIndexEntry->str_space;
        strcpy (cp, arValues[i]);
        pCurrentIndexEntry->str_space += strlen (arValues[i]) + 1;
     }
   pCurrentIndexEntry->length += 1;
   pCurrentIndexEntry->nRemainingStrSpace -= nStrSpace;

   return (0);
}

/*
 * Routine: ProcessOther
 * Purpose: Handle any other statements
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
ProcessOther (char *stmt, token_t * tokens)
{
   return (QERR_SYNTAX);
}

