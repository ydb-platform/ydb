#include <config.h>
#include "lst.h"

int lstSeekItem( HLST hLst, HLSTITEM hItem )
{
    if ( !hLst )
        return false;

	lstFirst( hLst );
	while ( !lstEOL( hLst ) )
	{
		if ( hLst->hCurrent == hItem )
			return true;

		lstNext( hLst );
	}

    return false;
}



