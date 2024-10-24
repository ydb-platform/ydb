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
#ifdef NCR
#include <sys/types.h>
#endif
#ifndef WIN32
#include <netinet/in.h>
#endif
#include "genrand.h"
#include "w_item.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "misc.h"
#include "nulls.h"
#include "tdefs.h"
#include "scd.h"

/* extern tdef w_tdefs[]; */

struct W_ITEM_TBL g_w_item,
    g_OldValues;

/*
* mk_item
*/
int
mk_w_item (void* row, ds_key_t index)
{
    
    int32_t res = 0;
    /* begin locals declarations */
    decimal_t dMinPrice, 
        dMaxPrice,
        dMarkdown;
    static decimal_t dMinMarkdown, dMaxMarkdown;
    int32_t bUseSize,
        bFirstRecord = 0,
        nFieldChangeFlags,
        nMin,
        nMax,
        nIndex,
        nTemp;
    char *cp;
    struct W_ITEM_TBL *r;
    static int32_t bInit = 0;
    struct W_ITEM_TBL *rOldValues = &g_OldValues;
    char *szMinPrice = NULL,
        *szMaxPrice = NULL;
   tdef *pT = getSimpleTdefsByNumber(ITEM);


    if (row == NULL)
        r = &g_w_item;
    else
        r = row;
    
    
    if (!bInit)
    {
        /* some fields are static throughout the data set */
        strtodec(&dMinMarkdown, MIN_ITEM_MARKDOWN_PCT);
        strtodec(&dMaxMarkdown, MAX_ITEM_MARKDOWN_PCT);

        bInit = 1;
    }
    
    memset(r, 0, sizeof(struct W_ITEM_TBL));

    /* build the new value */
    nullSet(&pT->kNullBitMap, I_NULLS);
    r->i_item_sk = index;

    nIndex = pick_distribution(&nMin, "i_manager_id", 2, 1, I_MANAGER_ID);
    dist_member(&nMax, "i_manager_id", nIndex, 3);
    genrand_key(&r->i_manager_id, DIST_UNIFORM, 
        (ds_key_t)nMin, 
        (ds_key_t)nMax, 
        0, I_MANAGER_ID);



    /* if we have generated the required history for this business key and generate a new one 
     * then reset associated fields (e.g., rec_start_date minimums)
     */
    if (setSCDKeys(I_ITEM_ID, index, r->i_item_id, &r->i_rec_start_date_id, &r->i_rec_end_date_id))
    {
    /* 
     * some fields are not changed, even when a new version of the row is written 
     */
        bFirstRecord = 1;
    }
    
     /*
      * this is  where we select the random number that controls if a field changes from 
      * one record to the next. 
      */
    nFieldChangeFlags = next_random(I_SCD);


    /* the rest of the record in a history-keeping dimension can either be a new data value or not;
     * use a random number and its bit pattern to determine which fields to replace and which to retain
     */
    gen_text (r->i_item_desc, 1, RS_I_ITEM_DESC, I_ITEM_DESC);
    changeSCD(SCD_CHAR, &r->i_item_desc, &rOldValues->i_item_desc,  &nFieldChangeFlags,  bFirstRecord);
    
    nIndex = pick_distribution(&szMinPrice, "i_current_price", 2, 1, I_CURRENT_PRICE);
    dist_member(&szMaxPrice, "i_current_price", nIndex, 3);
    strtodec(&dMinPrice, szMinPrice);
    strtodec(&dMaxPrice, szMaxPrice);
    genrand_decimal(&r->i_current_price, DIST_UNIFORM, &dMinPrice, &dMaxPrice, NULL, I_CURRENT_PRICE);
    changeSCD(SCD_INT, &r->i_current_price, &rOldValues->i_current_price,  &nFieldChangeFlags,  bFirstRecord);

    genrand_decimal(&dMarkdown, DIST_UNIFORM, &dMinMarkdown, &dMaxMarkdown, NULL, I_WHOLESALE_COST);
    decimal_t_op(&r->i_wholesale_cost, OP_MULT, &r->i_current_price, &dMarkdown);
    changeSCD(SCD_DEC, &r->i_wholesale_cost, &rOldValues->i_wholesale_cost,  &nFieldChangeFlags,  bFirstRecord);

    hierarchy_item (I_CATEGORY, &r->i_category_id, &r->i_category, index);
    /*
         * changeSCD(SCD_INT, &r->i_category_id, &rOldValues->i_category_id,  &nFieldChangeFlags,  bFirstRecord);
         */

    hierarchy_item (I_CLASS, &r->i_class_id, &r->i_class, index);
    changeSCD(SCD_KEY, &r->i_class_id, &rOldValues->i_class_id,  &nFieldChangeFlags,  bFirstRecord);

    cp = &r->i_brand[0];
    hierarchy_item (I_BRAND, &r->i_brand_id, &cp, index);
    changeSCD(SCD_KEY, &r->i_brand_id, &rOldValues->i_brand_id,  &nFieldChangeFlags,  bFirstRecord);

    /* some categories have meaningful sizes, some don't */
    if (r->i_category_id)
   {
      dist_member(&bUseSize, "categories", (int)r->i_category_id, 3);
    pick_distribution (&r->i_size, "sizes", 1, bUseSize + 2, I_SIZE);
    changeSCD(SCD_PTR, &r->i_size, &rOldValues->i_size,  &nFieldChangeFlags,  bFirstRecord);
   }
   else
   {
      bUseSize = 0;
      r->i_size = NULL;
   }

    nIndex = pick_distribution(&nMin, "i_manufact_id", 2, 1, I_MANUFACT_ID);
    genrand_integer(&nTemp, DIST_UNIFORM, 
        nMin, 
        dist_member(NULL, "i_manufact_id", nIndex, 3), 
        0, I_MANUFACT_ID);
    r->i_manufact_id = nTemp;
    changeSCD(SCD_KEY, &r->i_manufact_id, &rOldValues->i_manufact_id,  &nFieldChangeFlags,  bFirstRecord);

    mk_word (r->i_manufact, "syllables", (int) r->i_manufact_id, RS_I_MANUFACT, ITEM);
    changeSCD(SCD_CHAR, &r->i_manufact, &rOldValues->i_manufact,  &nFieldChangeFlags,  bFirstRecord);

    gen_charset(r->i_formulation, DIGITS, RS_I_FORMULATION, RS_I_FORMULATION, I_FORMULATION);
    embed_string(r->i_formulation, "colors", 1, 2, I_FORMULATION);
    changeSCD(SCD_CHAR, &r->i_formulation, &rOldValues->i_formulation,  &nFieldChangeFlags,  bFirstRecord);

    pick_distribution (&r->i_color, "colors", 1, 2, I_COLOR);
    changeSCD(SCD_PTR, &r->i_color, &rOldValues->i_color,  &nFieldChangeFlags,  bFirstRecord);

    pick_distribution (&r->i_units, "units", 1, 1, I_UNITS);
    changeSCD(SCD_PTR, &r->i_units, &rOldValues->i_units,  &nFieldChangeFlags,  bFirstRecord);

    pick_distribution (&r->i_container, "container", 1, 1, ITEM);
    changeSCD(SCD_PTR, &r->i_container, &rOldValues->i_container,  &nFieldChangeFlags,  bFirstRecord);

    mk_word (r->i_product_name, "syllables", (int) index, RS_I_PRODUCT_NAME,
        ITEM);

    r->i_promo_sk = mk_join(I_PROMO_SK, PROMOTION, 1);
    genrand_integer(&nTemp, DIST_UNIFORM, 1, 100, 0, I_PROMO_SK);
    if (nTemp > I_PROMO_PERCENTAGE)
        r->i_promo_sk = -1;

/* 
 * if this is the first of a set of revisions, then baseline the old values
 */
 if (bFirstRecord)
   memcpy(&g_OldValues, r, sizeof(struct W_ITEM_TBL));

 if (index == 1)
   memcpy(&g_OldValues, r, sizeof(struct W_ITEM_TBL));

    return (res);
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
pr_w_item(void *row)
{
    struct W_ITEM_TBL *r;

    if (row == NULL)
        r = &g_w_item;
    else
        r = row;

    print_start(ITEM);
    print_key(I_ITEM_SK, r->i_item_sk, 1);
    print_varchar(I_ITEM_ID, r->i_item_id, 1);
    print_date(I_REC_START_DATE_ID, r->i_rec_start_date_id, 1);
    print_date(I_REC_END_DATE_ID, r->i_rec_end_date_id, 1);
    print_varchar(I_ITEM_DESC, r->i_item_desc, 1);
    print_decimal(I_CURRENT_PRICE, &r->i_current_price, 1);
    print_decimal(I_WHOLESALE_COST, &r->i_wholesale_cost, 1);
    print_key(I_BRAND_ID, r->i_brand_id, 1);
    print_varchar(I_BRAND, r->i_brand, 1);
    print_key(I_CLASS_ID, r->i_class_id, 1);
    print_varchar(I_CLASS, r->i_class, 1);
    print_key(I_CATEGORY_ID, r->i_category_id, 1);
    print_varchar(I_CATEGORY, r->i_category, 1);
    print_key(I_MANUFACT_ID, r->i_manufact_id, 1);
    print_varchar(I_MANUFACT, r->i_manufact, 1);
    print_varchar(I_SIZE, r->i_size, 1);
    print_varchar(I_FORMULATION, r->i_formulation, 1);
    print_varchar(I_COLOR, r->i_color, 1);
    print_varchar(I_UNITS, r->i_units, 1);
    print_varchar(I_CONTAINER, r->i_container, 1);
    print_key(I_MANAGER_ID, r->i_manager_id, 1);
    print_varchar(I_PRODUCT_NAME, r->i_product_name, 0);
    print_end(ITEM);

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
int 
ld_w_item(void *pSrc)
{
    struct W_ITEM_TBL *r;
        
    if (pSrc == NULL)
        r = &g_w_item;
    else
        r = pSrc;
    
    return(0);
}

