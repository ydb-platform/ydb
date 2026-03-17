#include <config.h>
#include "lst.h"

void *lstLast( HLST hLst )
{
    if ( !hLst )
        return NULL;

	if ( !hLst->hLast )
        return NULL;

	if ( !_lstVisible( hLst->hLast ) )
		hLst->hCurrent = _lstPrevValidItem( hLst, hLst->hLast );
	else
		hLst->hCurrent = hLst->hLast;

	return hLst->hCurrent;
}


