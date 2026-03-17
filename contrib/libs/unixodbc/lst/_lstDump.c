#include <config.h>
#include "lst.h"

void _lstDump( HLST hLst )
{
	HLSTITEM	hItem;
	int			nItem = 0;

	printf( "LST - BEGIN DUMP\n" );

	if ( hLst )
	{
		printf( "\thLst = %p\n", hLst );
		printf( "\t\thLst->hLstBase = %p\n", hLst->hLstBase );

		hItem = hLst->hFirst;
		while ( hItem )
		{
			printf( "\t%d\n", nItem );
			printf( "\t\thItem          = %p\n", hItem );
			printf( "\t\thItem->bDelete = %d\n", hItem->bDelete );
			printf( "\t\thItem->bHide   = %d\n", hItem->bHide );
			printf( "\t\thItem->pData   = %p\n", hItem->pData );
			printf( "\t\thItem->hLst    = %p\n", hItem->hLst );

			nItem++;
			hItem = hItem->pNext;
		}
	}
	printf( "LST - END DUMP\n" );
}

