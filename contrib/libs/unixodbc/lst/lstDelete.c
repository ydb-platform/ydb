#include <config.h>
#include "lst.h"

int _lstDeleteFlag( HLSTITEM hItem );

/***********************
 * lstDelete
 *
 * Do not call unless you want to delete the item
 * from the cursor (if the list is one) **AND** the
 * root list. In other words; this should not be
 * called from functions such as lstClose() which
 * desire to simply free memory used by a specific
 * the list.
 *
 * This is the only function to set bDelete in the root
 * item (as required).
 *
 * lstFreeItem will decrement ref counters and do a real
 * delete when refs are 0.
 ************************/
int lstDelete( HLST hLst )
{
	HLSTITEM	hItem		= NULL;
	HLSTITEM	hItemRoot	= NULL;

    if ( !hLst )
        return LST_ERROR;

	hItem = hLst->hCurrent;

	if ( !hItem )
		return LST_ERROR;

	/*********************
	 * ARE WE A CURSOR LIST
	 *********************/
	if ( hLst->hLstBase )
	{
		hItemRoot = (HLSTITEM)hItem->pData;
		_lstDeleteFlag( hItemRoot );
		return _lstFreeItem( hItem );
	}

	/*********************
	 * WE ARE root LIST. CHECK FOR REFS BEFORE CALLING FREE
	 *********************/
	_lstDeleteFlag( hItem );
	if ( hItem->nRefs < 1 )
		return _lstFreeItem( hItem );

	return LST_SUCCESS;
}

/***************************
 * FLAG FOR DELETE (should only be called if root list)
 ***************************/
int _lstDeleteFlag( HLSTITEM hItem )
{
	HLST hLst;

	hLst =	(HLST)hItem->hLst;

	if ( !hItem->bDelete )
		hLst->nItems--;

	hItem->bDelete = true;
	
	if ( hLst->hCurrent == hItem )
        _lstAdjustCurrent( hLst );

	return true;
}



