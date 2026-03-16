#include <config.h>
#include "lst.h"

int lstGetBookMark( HLST hLst, HLSTBOOKMARK hLstBookMark )
{
    if ( !hLst )
        return LST_ERROR;

    if ( !hLstBookMark )
        return LST_ERROR;

	hLstBookMark->hCurrent		= hLst->hCurrent;
	hLstBookMark->hLst			= hLst;

    return LST_SUCCESS;
}



