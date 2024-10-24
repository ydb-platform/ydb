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
#include "genrand.h"
#include "s_web_site.h"
#include "w_web_site.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "scaling.h"
#include "decimal.h"
#include "permute.h"
#include "scd.h"

struct W_WEB_SITE_TBL g_w_web_site;


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
mk_s_web_site (void* row, ds_key_t index)
{
   static int bInit = 0;
   static int *pPermutation;
   ds_key_t kIndex;

   if (!bInit)
   {
      pPermutation = makePermutation(NULL, (int)getIDCount(WEB_SITE), S_WSIT_ID);
      bInit = 1;
   }

   kIndex = getPermutationEntry(pPermutation, (int)index);
   mk_w_web_site(NULL,getSKFromID(kIndex, S_WSIT_ID));

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
pr_s_web_site(void *pSrc)
{
    struct W_WEB_SITE_TBL *r;

    if (pSrc == NULL)
        r = &g_w_web_site;
    else
        r = pSrc;
    
    print_start(S_WEB_SITE);
    print_varchar(WEB_SITE_ID, &r->web_site_id[0], 1);
    print_date(WEB_OPEN_DATE, r->web_open_date, 1);
    print_date(WEB_CLOSE_DATE, r->web_close_date, 1);
    print_varchar(WEB_NAME, &r->web_name[0], 1);
    print_varchar(WEB_CLASS, &r->web_class[0], 1);
    print_varchar(WEB_MANAGER, &r->web_manager[0], 1);
    print_decimal(WEB_TAX_PERCENTAGE, &r->web_tax_percentage, 0);
    print_end(S_WEB_SITE);
    
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
ld_s_web_site(void *pSrc)
{
    struct W_WEB_SITE_TBL *r;
        
    if (pSrc == NULL)
        r = &g_w_web_site;
    else
        r = pSrc;
    
    return(0);
}

