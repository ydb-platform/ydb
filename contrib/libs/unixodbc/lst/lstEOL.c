#include <config.h>
#include "lst.h"

int lstEOL( HLST hLst )
{
    if ( !hLst )
        return true;

	if ( !hLst->hCurrent )
		return true;

	return false;
}


