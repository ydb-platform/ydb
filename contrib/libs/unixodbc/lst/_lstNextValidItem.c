#include <config.h>
#include "lst.h"

HLSTITEM _lstNextValidItem( HLST hLst, HLSTITEM hItem )
{

    if ( !hLst )
        return NULL;

    if ( !hItem )
        return NULL;

	hItem = hItem->pNext;
	while ( hItem )
	{
		if ( _lstVisible( hItem ) )
			return hItem;

		hItem = hItem->pNext;
	}

    return NULL;
}



