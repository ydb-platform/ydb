#include <config.h>
#include "lst.h"

int lstSeek( HLST hLst, void *pData )
{
    if ( !hLst )
        return false;

	lstFirst( hLst );
	while ( !lstEOL( hLst ) )
	{
		if ( lstGet( hLst ) == pData )
			return true;

		lstNext( hLst );
	}

    return false;
}



