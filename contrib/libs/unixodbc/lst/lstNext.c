#include <config.h>
#include "lst.h"

void *lstNext( HLST hLst )
{
    if ( !hLst )
        return NULL;

	if ( !hLst->hCurrent )
		return NULL;

	hLst->hCurrent = hLst->hCurrent->pNext;

	if ( hLst->hCurrent )
	{
		if ( !_lstVisible( hLst->hCurrent ) )
			hLst->hCurrent = _lstNextValidItem( hLst, hLst->hCurrent );
	}

	return hLst->hCurrent;
}


