#include <config.h>
#include "lst.h"

/*! 
 * \brief   Returns the data stored at nIndex.
 * 
 *          This does a scan from first-to-last until nIndex or until EOL. This
 *          can be slow if there are many items in the list.
 *
 *          This does not return the item handle - it returns the user data stored
 *          in the item.
 *
 *          When done; the current item is either the item at nIndex or EOL.
 *
 * \param   hLst    Input. Viable list handle.
 * \param   nIndex  Input. 0-based index of the desired item.
 * 
 * \return  void*
 * \retval  NULL    Item at index could not be found - effectively data is NULL.
 * \retval  !NULL   Reference to the data stored at nIndex
 */
void *lstGoto( HLST hLst, long nIndex )
{
	long n = 0;

    if ( !hLst )
        return NULL;

	lstFirst( hLst );
	while ( n <= nIndex )
	{
        if ( lstEOL( hLst ) )
            break;
        if ( n == nIndex )
            return hLst->hCurrent->pData;
        n++;
		lstNext( hLst );
	}

    return NULL;
}



