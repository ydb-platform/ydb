#include <config.h>
#include "lst.h"

int lstSetFreeFunc( HLST hLst, void (*pFree)( void *pData ) )
{
	if ( !hLst )
		return false;
	
	hLst->pFree = pFree;

	return true;
}

