#include <config.h>
#include "lst.h"
/*
 *
 */
void *lstGet( HLST hLst )
{
	HLSTITEM	hItem;

    if ( !hLst )
        return NULL;

	if ( !hLst->hCurrent )
		return NULL;
	
	if ( hLst->hLstBase )
		hItem = (HLSTITEM)hLst->hCurrent->pData; 	/* cursor pData points directly to the root Item (not the roots pData) 				*/
	else											/* a cursor may be based upon another cursor but pData is always ptr to root item	*/
		hItem = hLst->hCurrent;


    return hItem->pData;
}



