#include <config.h>
#include "lst.h"

int lstGotoBookMark( HLSTBOOKMARK hLstBookMark )
{
    if ( !hLstBookMark )
        return LST_ERROR;

	hLstBookMark->hLst->hCurrent		= hLstBookMark->hCurrent;

    return LST_SUCCESS;
}



