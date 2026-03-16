#include <config.h>
#include "lst.h"


int lstInsert( HLST hLst, void *pData )
{
	HLSTITEM	hItem;
	
	if ( !hLst )
		return LST_ERROR;

	if ( !hLst->hCurrent )
		return lstAppend( hLst, pData );

	/**********************
	 * CREATE AN ITEM
	 **********************/
	hItem = malloc( sizeof(LSTITEM) );
	if ( !hItem )
		return LST_ERROR;

	hItem->bDelete		= false;
	hItem->bHide		= false;
	hItem->hLst			= hLst;
	hItem->nRefs		= 0;
	hItem->pData		= NULL;
	hItem->pNext		= NULL;
	hItem->pPrev		= NULL;

	if ( hLst->hLstBase )
	{
		/**********************
		 * WE ARE A CURSOR LIST SO...
		 * 1. ADD TO BASE LIST
		 * 2. inc BASE LIST ITEM REF COUNT
		 * 3. ADD TO THIS LIST (ref to base list)
		 **********************/
		lstInsert( hLst->hLstBase, pData );				/* !!! INSERT POS IN BASE LIST IS UNPREDICTABLE !!! */
														/*     BECAUSE hCurrent MAY HAVE CHANGED AND WE 	*/
														/*     ARE NOT TRYING TO PUT IT BACK				*/
		hItem->pData = hLst->hLstBase->hCurrent;		
        hLst->hLstBase->hCurrent->nRefs++;
		_lstInsert( hLst, hItem );
	}
	else
	{
		/**********************
		 * WE ARE THE ROOT SO...
		 * 1. ADD TO THIS LIST
		 **********************/
		hItem->pData = pData;
		_lstInsert( hLst, hItem );
	}


	return LST_SUCCESS;
}



/*************************
 * SIMPLY CONNECTS THE LINKS/POINTERS AND SETS CURRENT
 *************************/
int _lstInsert( HLST hLst, HLSTITEM hItem )
{
	if ( !hLst->hCurrent )
		return _lstAppend( hLst, hItem );
	
	
	hItem->pPrev	= hLst->hCurrent->pPrev;
	hItem->pNext	= hLst->hCurrent;
	
	if ( hLst->hCurrent->pPrev )
		hLst->hCurrent->pPrev->pNext = hItem;
	
	hLst->hCurrent->pPrev = hItem;
	
	if ( hLst->hCurrent == hLst->hFirst )
		hLst->hFirst = hItem;

	hLst->hCurrent		= hItem;
	hLst->nItems++;
	
	return LST_SUCCESS;
}

