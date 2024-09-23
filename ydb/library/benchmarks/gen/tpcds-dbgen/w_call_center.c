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
#include <assert.h>
#ifdef NCR
#include <sys/types.h>
#endif
#ifndef WIN32
#include <netinet/in.h>
#endif
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include "w_call_center.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "r_params.h"
#include "scaling.h"
#include "columns.h"
#include "tables.h"
#include "misc.h"
#include "dist.h"
#include "build_support.h"
#include "print.h"
#include "tdefs.h"
#include "nulls.h"
#include "scd.h"

struct CALL_CENTER_TBL g_w_call_center;
static struct CALL_CENTER_TBL g_OldValues;

/*
* Routine: mk_w_call_center()
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
* 20020830 jms Need to populate open and close dates
*/
int
mk_w_call_center (void* row, ds_key_t index)
{
    int32_t res = 0;
    static int32_t jDateStart,
        nDaysPerRevision;
    int32_t nSuffix,
        bFirstRecord = 0,
        nFieldChangeFlags,
        jDateEnd,
        nDateRange;
    char *cp,
        *sName1,
        *sName2;
    static decimal_t dMinTaxPercentage, dMaxTaxPercentage;
   tdef *pTdef = getSimpleTdefsByNumber(CALL_CENTER);

    /* begin locals declarations */
    date_t dTemp;
    static int bInit = 0,
        nScale;
    struct CALL_CENTER_TBL *r,
        *rOldValues = &g_OldValues;

    if (row == NULL)
        r = &g_w_call_center;
    else
        r = row;

    if (!bInit)
    {
        /* begin locals allocation/initialization */
        strtodt(&dTemp, DATA_START_DATE);
        jDateStart = dttoj(&dTemp) - WEB_SITE;
        strtodt(&dTemp, DATA_END_DATE);
        jDateEnd = dttoj(&dTemp);
        nDateRange = jDateEnd - jDateStart + 1; 
        nDaysPerRevision = nDateRange / pTdef->nParam + 1;
        nScale = get_int("SCALE");
        
        /* these fields need to be handled as part of SCD code or further definition */
        r->cc_division_id = -1;
        r->cc_closed_date_id = -1;
        strcpy(r->cc_division_name, "No Name");

        strtodec(&dMinTaxPercentage, MIN_CC_TAX_PERCENTAGE);
        strtodec(&dMaxTaxPercentage, MAX_CC_TAX_PERCENTAGE);
      bInit = 1;
    }
    
    nullSet(&pTdef->kNullBitMap, CC_NULLS);
    r->cc_call_center_sk = index;

    /* if we have generated the required history for this business key and generate a new one 
     * then reset associate fields (e.g., rec_start_date minimums)
     */
    if (setSCDKeys(CC_CALL_CENTER_ID, index, r->cc_call_center_id, &r->cc_rec_start_date_id, &r->cc_rec_end_date_id))
    {
        r->cc_open_date_id = jDateStart 
            - genrand_integer(NULL, DIST_UNIFORM, -365, 0, 0, CC_OPEN_DATE_ID);

/* 
 * some fields are not changed, even when a new version of the row is written 
 */
        nSuffix = (int)index / distsize("call_centers");
        dist_member (&cp, "call_centers", (int) (index % distsize("call_centers")) + 1, 1);
        if (nSuffix > 0)
            sprintf(r->cc_name, "%s_%d", cp, nSuffix);
        else
            strcpy(r->cc_name, cp);
            
        mk_address(&r->cc_address, CC_ADDRESS);
        bFirstRecord = 1;
    }
    
 /*
  * this is  where we select the random number that controls if a field changes from 
  * one record to the next. 
  */
    nFieldChangeFlags = next_random(CC_SCD);


    /* the rest of the record in a history-keeping dimension can either be a new data value or not;
     * use a random number and its bit pattern to determine which fields to replace and which to retain
     */
    pick_distribution (&r->cc_class, "call_center_class", 1, 1, CC_CLASS);
    changeSCD(SCD_PTR, &r->cc_class, &rOldValues->cc_class,  &nFieldChangeFlags,  bFirstRecord);

    genrand_integer (&r->cc_employees, DIST_UNIFORM, 1, CC_EMPLOYEE_MAX * nScale * nScale, 0, CC_EMPLOYEES);
    changeSCD(SCD_INT, &r->cc_employees, &rOldValues->cc_employees,  &nFieldChangeFlags,  bFirstRecord);

    genrand_integer(&r->cc_sq_ft, DIST_UNIFORM, 100, 700, 0, CC_SQ_FT);
    r->cc_sq_ft *= r->cc_employees;
    changeSCD(SCD_INT, &r->cc_sq_ft, &rOldValues->cc_sq_ft,  &nFieldChangeFlags,  bFirstRecord);

    pick_distribution (&r->cc_hours, "call_center_hours",
        1, 1, CC_HOURS);
    changeSCD(SCD_PTR, &r->cc_hours, &rOldValues->cc_hours,  &nFieldChangeFlags,  bFirstRecord);

    pick_distribution (&sName1, "first_names", 1, 1, CC_MANAGER);
    pick_distribution (&sName2, "last_names", 1, 1, CC_MANAGER);
    sprintf (&r->cc_manager[0], "%s %s", sName1, sName2);
    changeSCD(SCD_CHAR, &r->cc_manager, &rOldValues->cc_manager,  &nFieldChangeFlags,  bFirstRecord);

    genrand_integer (&r->cc_market_id, DIST_UNIFORM, 1, 6, 0, CC_MARKET_ID);
    changeSCD(SCD_INT, &r->cc_market_id, &rOldValues->cc_market_id,  &nFieldChangeFlags,  bFirstRecord);

    gen_text (r->cc_market_class, 20, RS_CC_MARKET_CLASS, CC_MARKET_CLASS);
    changeSCD(SCD_CHAR, &r->cc_market_class, &rOldValues->cc_market_class,  &nFieldChangeFlags,  bFirstRecord);

    gen_text (r->cc_market_desc, 20, RS_CC_MARKET_DESC, CC_MARKET_DESC);
    changeSCD(SCD_CHAR, &r->cc_market_desc, &rOldValues->cc_market_desc,  &nFieldChangeFlags,  bFirstRecord);

    pick_distribution (&sName1, "first_names", 1, 1, CC_MARKET_MANAGER);
    pick_distribution (&sName2, "last_names", 1, 1, CC_MARKET_MANAGER);
    sprintf (&r->cc_market_manager[0], "%s %s", sName1, sName2);
    changeSCD(SCD_CHAR, &r->cc_market_manager, &rOldValues->cc_market_manager,  &nFieldChangeFlags,  bFirstRecord);

    genrand_integer (&r->cc_company, DIST_UNIFORM, 1, 6, 0, CC_COMPANY);
    changeSCD(SCD_INT, &r->cc_company, &rOldValues->cc_company,  &nFieldChangeFlags,  bFirstRecord);

    genrand_integer (&r->cc_division_id, DIST_UNIFORM, 1, 6, 0, CC_COMPANY);
    changeSCD(SCD_INT, &r->cc_division_id, &rOldValues->cc_division_id,  &nFieldChangeFlags,  bFirstRecord);

    mk_word(r->cc_division_name, "syllables", r->cc_division_id, RS_CC_DIVISION_NAME, CC_DIVISION_NAME);
    changeSCD(SCD_CHAR, &r->cc_division_name, &rOldValues->cc_division_name,  &nFieldChangeFlags,  bFirstRecord);
    
    mk_companyname (r->cc_company_name, CC_COMPANY_NAME, r->cc_company);
    changeSCD(SCD_CHAR, &r->cc_company_name, &rOldValues->cc_company_name,  &nFieldChangeFlags,  bFirstRecord);

    genrand_decimal(&r->cc_tax_percentage, DIST_UNIFORM, &dMinTaxPercentage, &dMaxTaxPercentage, NULL, CC_TAX_PERCENTAGE);
    changeSCD(SCD_DEC, &r->cc_tax_percentage, &rOldValues->cc_tax_percentage,  &nFieldChangeFlags,  bFirstRecord);

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
pr_w_call_center(void *row)
{
    struct CALL_CENTER_TBL *r;
    char szTemp[128];
    
    if (row == NULL)
        r = &g_w_call_center;
    else
        r = row;

    print_start(CALL_CENTER);
    print_key(CC_CALL_CENTER_SK, r->cc_call_center_sk, 1);
    print_varchar(CC_CALL_CENTER_ID, r->cc_call_center_id, 1);
    print_date(CC_REC_START_DATE_ID, r->cc_rec_start_date_id, 1);
    print_date(CC_REC_END_DATE_ID, r->cc_rec_end_date_id, 1);
    print_key(CC_CLOSED_DATE_ID, r->cc_closed_date_id, 1);
    print_key(CC_OPEN_DATE_ID, r->cc_open_date_id, 1);
    print_varchar(CC_NAME, r->cc_name, 1);
    print_varchar(CC_CLASS, &r->cc_class[0], 1);
    print_integer(CC_EMPLOYEES, r->cc_employees, 1);
    print_integer(CC_SQ_FT, r->cc_sq_ft, 1);
    print_varchar(CC_HOURS, r->cc_hours, 1);
    print_varchar(CC_MANAGER, &r->cc_manager[0], 1);
    print_integer(CC_MARKET_ID, r->cc_market_id, 1);
    print_varchar(CC_MARKET_CLASS, &r->cc_market_class[0], 1);
    print_varchar(CC_MARKET_DESC, &r->cc_market_desc[0], 1);
    print_varchar(CC_MARKET_MANAGER, &r->cc_market_manager[0], 1);
    print_integer(CC_DIVISION, r->cc_division_id, 1);
    print_varchar(CC_DIVISION_NAME, &r->cc_division_name[0], 1);
    print_integer(CC_COMPANY, r->cc_company, 1);
    print_varchar(CC_COMPANY_NAME, &r->cc_company_name[0], 1);
    print_integer(CC_ADDRESS, r->cc_address.street_num, 1);
    if (r->cc_address.street_name2)
    {
        sprintf(szTemp, "%s %s", r->cc_address.street_name1, r->cc_address.street_name2);
        print_varchar(CC_ADDRESS, szTemp, 1);
    }
    else
        print_varchar(CC_ADDRESS, r->cc_address.street_name1, 1);
    print_varchar(CC_ADDRESS, r->cc_address.street_type, 1);
    print_varchar(CC_ADDRESS, &r->cc_address.suite_num[0], 1);
    print_varchar(CC_ADDRESS, r->cc_address.city, 1);
    print_varchar(CC_ADDRESS, r->cc_address.county, 1);
    print_varchar(CC_ADDRESS, r->cc_address.state, 1);
    sprintf(szTemp, "%05d", r->cc_address.zip);
    print_varchar(CC_ADDRESS, szTemp, 1);
    print_varchar(CC_ADDRESS, &r->cc_address.country[0], 1);
    print_integer(CC_ADDRESS, r->cc_address.gmt_offset, 1);
    print_decimal(CC_TAX_PERCENTAGE, &r->cc_tax_percentage, 0);
    print_end(CALL_CENTER);

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
ld_w_call_center(void *r)
{
    return(0);
}

