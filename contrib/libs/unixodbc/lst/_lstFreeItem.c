#include <config.h>
#include "lst.h"

/***************************
 * _lstFreeItem
 *
 * 1. FREES MEMORY USED BY THE LIST ITEM AND REMOVES IT FROM ITS LIST
 * 2. WILL CLEAN UP root ITEM IF REQUIRED.. NEVER SETS bDelete... lstDelete DOES SET bDelete
 * 3. CALLS _lstAdjustCurrent TO ENSURE THAT CURRENT DOES NOT END UP ON  bDelete ITEM
 ***************************/
int _lstFreeItem( HLSTITEM hItem )
{
    HLST 		hLst;
	HLSTITEM	hItemRoot;
	HLSTITEM	hNewCurrent = NULL;

    if ( !hItem )
        return LST_ERROR;

	hLst = (HLST)hItem->hLst;

	/*************
	 * FREE root ITEM AS REQUIRED
	 *************/
	if ( hLst->hLstBase )
	{
		hItemRoot = (HLSTITEM)hItem->pData;

		/*************
		 * dec ref count in root item
		 *************/
        hItemRoot->nRefs--;

		/*************
		 * DELETE root ITEM IF REF = 0 AND SOMEONE SAID DELETE IT
		 *************/
		if ( hItemRoot->nRefs < 1 && hItemRoot->bDelete )
		{
			_lstFreeItem( hItemRoot );
		}
	}

	/*************
	 * WE ALWAYS FREE hItem
	 *************/
	if ( hItem->pData && hLst->pFree )
		hLst->pFree( hItem->pData );

	if ( !hItem->bDelete )				/* THIS IS REALLY ONLY A FACTOR FOR ROOT ITEMS */
		hLst->nItems--;
	
	if ( hItem == hLst->hFirst )
		hLst->hFirst = hItem->pNext;	

	if ( hItem == hLst->hLast )
		hLst->hLast = hItem->pPrev;	

	if ( hItem->pPrev )
	{
		hItem->pPrev->pNext = hItem->pNext;	
		if ( hItem == hLst->hCurrent )
			hNewCurrent = hItem->pPrev;
	}

	if ( hItem->pNext )
	{
		hItem->pNext->pPrev = hItem->pPrev;	
		if ( !hNewCurrent && hItem == hLst->hCurrent )
			hNewCurrent = hItem->pNext;
	}

	free( hItem );

	hLst->hCurrent = hNewCurrent;

	_lstAdjustCurrent( hLst );	
	

	return LST_SUCCESS;
}



