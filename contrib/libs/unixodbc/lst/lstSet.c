#include <config.h>
#include "lst.h"

void *lstSet( HLST hLst, void *pData )
{
	HLSTITEM	hItem;
	HLST		hLstRoot;

    if ( !hLst )
        return NULL;

	if ( !hLst->hCurrent )
		return NULL;
	

	if ( hLst->hLstBase )
		hItem = (HLSTITEM)hLst->hCurrent->pData;
	else
		hItem = hLst->hCurrent;

	hLstRoot = (HLST)hItem->hLst;

	/**************************
	 * SET VALUE
	 **************************/
	if ( hItem->pData && hLstRoot->pFree )
		hLstRoot->pFree( hItem->pData );

	hItem->pData = pData;

    return pData;
}



