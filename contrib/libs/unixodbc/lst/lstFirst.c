#include <config.h>
#include "lst.h"

void *lstFirst( HLST hLst )
{
    if ( !hLst )
        return NULL;

	if ( !hLst->hFirst )
        return NULL;

	if ( !_lstVisible( hLst->hFirst ) )
		hLst->hCurrent = _lstNextValidItem( hLst, hLst->hFirst );
	else
		hLst->hCurrent = hLst->hFirst;

	return hLst->hCurrent;
}


