#include <config.h>
#include "lst.h"

/*************************
 * lstAppend
 *
 * 1. APPEND TO BASE LIST IF hLst IS A CURSOR
 * 2. APPEND REF TO THIS LIST
 *************************/
int lstAppend( HLST hLst, void *pData )
{
	HLSTITEM	hItem;
	
	if ( !hLst )
		return LST_ERROR;

	/**********************
	 * CREATE AN ITEM
	 **********************/
	hItem = (HLSTITEM) malloc( sizeof(LSTITEM) );
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
		 * 2. ADD TO THIS LIST (ref to base list)
		 **********************/
		lstAppend( hLst->hLstBase, pData );
		hItem->pData = hLst->hLstBase->hCurrent;
        hLst->hLstBase->hCurrent->nRefs++;
		_lstAppend( hLst, hItem );
	}
	else
	{
		/**********************
		 * WE ARE THE ROOT SO...
		 * 1. ADD TO THIS LIST
		 **********************/
		hItem->pData = pData;
		_lstAppend( hLst, hItem );
	}

	return LST_SUCCESS;
}


/*************************
 * SIMPLY CONNECTS THE LINKS/POINTERS AND SETS CURRENT
 *************************/
int _lstAppend( HLST hLst, HLSTITEM hItem )
{

	if ( hLst->hFirst )
	{
		hItem->pPrev 		= hLst->hLast;
        hLst->hLast->pNext	= hItem;
		hLst->hLast			= hItem;
	}
	else
	{
		hItem->pPrev 	= NULL;
		hLst->hFirst	= hItem;
		hLst->hLast		= hItem;
	}
	hLst->hCurrent		= hItem;
	hLst->nItems++;

	return LST_SUCCESS;
}

