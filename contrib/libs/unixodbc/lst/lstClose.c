#include <config.h>
#include "lst.h"

/*********************
 * lstClose
 *
 * Call for Cursor or root list.
 *********************/
int lstClose( HLST hLst )
{
	HLSTITEM hItem;

    if ( !hLst )
        return LST_ERROR;

    hLst->nRefs--;

	/*********************
	 * We will not really remove the list if we have
	 * refs to it... we will just decrement ref.
	 * We will be deleted when the last ref is being removed.
	 *********************/
	if ( hLst->nRefs > 0 )
		return LST_SUCCESS;

	/************************
	 * DELETE ITEMS (and their refs)
	 * - do not use standard nav funcs because they will skip items where bDelete
	 ************************/
	hItem = hLst->hFirst;
	while ( hItem )
	{
        _lstFreeItem( hItem );
		hItem = hLst->hFirst;
	}

	/************************
	 * RECURSE AS REQUIRED. RECURSION WILL STOP AS SOON AS WE GET TO A LIST WHICH
	 * DOES NOT NEED TO BE DELETED YET (refs >= 0).
	 ************************/
	if ( hLst->hLstBase )						/* we are a cursor 					*/
		lstClose( hLst->hLstBase );				/* dec ref count and close if < 0 	*/

	/************************
	 * FREE LIST HANDLE
	 ************************/
	free( hLst );

	return LST_SUCCESS;
}



