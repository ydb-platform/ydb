#include <config.h>
#include "lst.h"

/***************************
 * ENSURE CURRENT IS NOT ON A bDelete ITEM
 *
 * 1. Should only be required for root list.
 ***************************/
void *_lstAdjustCurrent( HLST hLst )
{
	HLSTITEM h;

	if ( !hLst )
		return NULL;

	if ( !hLst->hCurrent )
		return NULL;
	
	if ( _lstVisible( hLst->hCurrent ) )
		return hLst->hCurrent;
	
	h = hLst->hCurrent;
	while ( !_lstVisible( hLst->hCurrent ) && hLst->hCurrent->pPrev )
	{
        hLst->hCurrent = hLst->hCurrent->pPrev;
	}

	if ( _lstVisible( hLst->hCurrent ) )
		return hLst->hCurrent;
	
    hLst->hCurrent = h;
	while ( !_lstVisible( hLst->hCurrent ) && hLst->hCurrent->pNext )
	{
        hLst->hCurrent = hLst->hCurrent->pNext;
	}

	if ( _lstVisible( hLst->hCurrent ) )
		return hLst->hCurrent;
	
    hLst->hCurrent = NULL;

	return NULL;
}



