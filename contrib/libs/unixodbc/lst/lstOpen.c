#include <config.h>
#include "lst.h"

HLST lstOpen()
{
	HLST hLst = NULL;

	hLst = malloc( sizeof(LST) );

	if ( hLst )
	{
		hLst->bExclusive	= false;
		hLst->hCurrent		= NULL;
		hLst->hFirst		= NULL;
		hLst->hLast			= NULL;
		hLst->hLstBase		= NULL;
		hLst->nRefs			= 1;			/* someone created us so lets assume that it counts as one ref */
		hLst->pFilter		= NULL;
		hLst->pFree			= free;
		hLst->nItems		= 0;
		hLst->bShowDeleted	= false;
		hLst->bShowHidden	= false;
	}

	return hLst;
}

