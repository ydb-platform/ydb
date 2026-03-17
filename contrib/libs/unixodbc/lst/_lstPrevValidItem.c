#include <config.h>
#include "lst.h"

HLSTITEM _lstPrevValidItem( HLST hLst, HLSTITEM hItem )
{

    if ( !hLst )
        return NULL;

    if ( !hItem )
        return NULL;

	hItem = hItem->pPrev;
	while ( hItem )
	{
		if ( _lstVisible( hItem ) )
			return hItem;

		hItem = hItem->pPrev;
	}


    return NULL;
}



