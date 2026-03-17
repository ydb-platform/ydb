#include <config.h>
#include "lst.h"

HLST lstOpenCursor( HLST hBase, int (*pFilterFunc)( HLST, void * ), void *pExtras )
{
	HLST hLst = NULL;
	
	if ( !hBase )
		return NULL;

	/*************************
	 * CREATE A NEW LIST
	 *************************/
	hLst = lstOpen();
	if ( !hLst )
		return NULL;
	
	hBase->nRefs++;

	hLst->pFilter		= pFilterFunc;
	hLst->pFree			= NULL;			/* never free pData in a cursor */
	hLst->pExtras		= pExtras;

	/*************************
	 * ADD ITEMS FROM hBase (skipping any bDelete items)
	 *************************/
	lstFirst( hBase );
	if ( pFilterFunc )
	{
		while ( !lstEOL( hBase ) )
		{
			if ( pFilterFunc( hLst, lstGet( hBase ) ) )
				lstAppend( hLst, hBase->hCurrent );

			lstNext( hBase );
		}
	}
	else
	{
		while ( !lstEOL( hBase ) )
		{
			lstAppend( hLst, hBase->hCurrent );

			lstNext( hBase );
		}
	}

	/*************************
	 * THIS *MUST* BE DONE AFTER THE LIST IS LOADED
	 * OTHERWISE lstAppend() WILL APPEND INTO ROOT LIST AND MAKE A REF IN THIS LIST
	 *************************/
	hLst->hLstBase		= hBase;

	return hLst;
}

