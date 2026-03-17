#include <config.h>
#include "lst.h"

int _lstVisible( HLSTITEM hItem )
{
    HLST hLst;

	if ( !hItem )
		return false;

	hLst = (HLST)hItem->hLst;

	if ( hItem->bDelete && hLst->bShowDeleted == false )
		return false;
	
	if ( hItem->bHide && hLst->bShowHidden == false )
		return false;
	
	return true;
}



