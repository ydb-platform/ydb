#include <config.h>
#include "lst.h"

void *lstPrev( HLST hLst )
{
    if ( !hLst )
        return NULL;

	if ( !hLst->hCurrent )
		return NULL;

	hLst->hCurrent = hLst->hCurrent->pPrev;

	if ( hLst->hCurrent )
	{
		if ( !_lstVisible( hLst->hCurrent ) )
			hLst->hCurrent = _lstPrevValidItem( hLst, hLst->hCurrent );
	}
	
	return hLst->hCurrent;
}



