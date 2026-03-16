/**********************************************************************************
 * lst.h
 *
 * lib for creating/managing/deleting doubly-linked lists.
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 04.APR.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/

#ifndef INCLUDED_LST_H
#define INCLUDED_LST_H

/*********[ CONSTANTS AND TYPES ]**************************************************/
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>

#ifndef true
#define true    1
#endif

#ifndef false
#define false   0
#endif


#define     LST_NO_DATA             2
#define     LST_SUCCESS             1
#define     LST_ERROR               0

/********************************************
 * tLSTITEM
 *
 ********************************************/

typedef struct	tLSTITEM
{
	struct	tLSTITEM	*pNext;
	struct	tLSTITEM	*pPrev;

	int		bDelete;								/* true if flagged for delete. do delete when refs = 0 				*/
													/* will become invisible for new cursors							*/
													/* ONLY APPLIES TO THE root LIST									*/
	int		bHide;									/* used in nav funcs if HLST bShowHidden=false (default)			*/
	long	nRefs;									/* the number of hItems that refer to this item to get pData		*/
													/* if bDelete and refs = 0 then item is really removed				*/
	void	*hLst;									/* ptr to its list handle. 											*/

	void 	*pData;									/* ptr to user data or (if Cursor item) ptr to some base LSTITEM	*/

} LSTITEM, *HLSTITEM;

/********************************************
 * tLST
 *
 ********************************************/

typedef struct tLST
{
	HLSTITEM		hFirst;
	HLSTITEM		hLast;
	HLSTITEM		hCurrent;
	long			nItems;							/* number of items in the list (not counting where bDelete or bHide)*/
													/* !!! not used anymore !!!!										*/

	long			nRefs;							/* the number of cursors that are based upon this list				*/

	int				bExclusive;						/* set this for exclusive access to list ie when navigating with	*/
													/* hCurrent or when doing an insert or delete						*/
													/* do this only for VERY short periods all other access will loop	*/
													/* until this is set back to false									*/
													/* THIS IS FOR INTERNAL USE... IT IS USED WHEN MAINTAINING INTERNAL	*/
													/* LISTS SUCH AS REFERENCE LISTS DO NOT USE IT TO LOCK A ROOT OR	*/
													/* CURSOR LIST														*/
	int				bShowHidden;					/* true to have nav funcs show bHidden items(default=false)			*/
	int				bShowDeleted;					/* true to have nav funcs show bDeleted items (default=false)		*/
    void			(*pFree)( void *pData );		/* function to use when need to free pData. default is free()		*/

	int				(*pFilter)( struct tLST *, void * );		/* this function returns true if we want the data in our result set	*/
													/* default is all items included. no affect if root list			*/

	struct			tLST	*hLstBase;				/* this list was derived from hLstBase. NULL if root list.			*/
													/* we must use this if we are adding a new item in an empty list	*/
													/* and to dec nRefs in our base list								*/

	void			*pExtras;						/* app can store what ever it wants here. no attempt to interpret it*/
													/* or to free it is made by lst										*/	

} LST, *HLST;


/********************************************
 * tLSTBOOKMARK
 *
 ********************************************/

typedef struct tLSTBOOKMARK
{
	HLST			hLst;
	HLSTITEM		hCurrent;

} LSTBOOKMARK, *HLSTBOOKMARK;


#if defined(__cplusplus)
         extern  "C" { 
#endif

/*********[ PRIMARY INTERFACE ]*****************************************************/

/******************************
 * lstAppend
 *
 * 1. Appends a new item to the end of the list.
 * 2. Makes the new item the current item.
 ******************************/
int lstAppend( HLST hLst, void *pData );

/******************************
 * lstClose
 *
 * 1. free memory previously allocated for HLST
 * 2. Will call lstDelete with bFreeData for each (if any)
 *    existing items.
 ******************************/
int lstClose( HLST hLst );

/******************************
 * lstDelete
 *
 * 1. deletes current item
 * 2. dec ref count in root item
 * 3. deletes root item if ref count < 1 OR sets delete flag
 ******************************/
int lstDelete( HLST hLst );

/******************************
 * lstEOL
 *
 ******************************/
int lstEOL( HLST hLst );

/******************************
 * lstFirst
 *
 * 1. makes First item the current item.
 * 2. returns pData or NULL
 ******************************/
void *lstFirst( HLST hLst );

/******************************
 * lstGet
 *
 * 1. Return pData for current item or NULL
 * 2. Will recurse down to base data if bIsCursor.
 ******************************/
void *lstGet( HLST hLst );

/******************************
 * lstGetBookMark
 *
 * !!! BOOKMARKS ONLY SAFE WHEN READONLY !!!
 ******************************/
int lstGetBookMark( HLST hLst, HLSTBOOKMARK hLstBookMark );

/******************************
 * lstGoto
 *
 * 1. Return pData for current item or NULL
 * 2. IF nIndex is out of range THEN
 *       lstEOL = TRUE
 *       returns NULL
 ******************************/
void *lstGoto( HLST hLst, long nIndex );

/******************************
 * lstGotoBookMark
 *
 * !!! BOOKMARKS ONLY SAFE WHEN READONLY !!!
 ******************************/
int lstGotoBookMark( HLSTBOOKMARK hLstBookMark );

/******************************
 * lstInsert
 *
 * 1. inserts a new item before the current item
 * 2. becomes current
 ******************************/
int lstInsert( HLST hLst, void *pData );

/******************************
 * lstLast
 *
 * 1. makes last item the current item
 * 2. returns pData or NULL
 ******************************/
void *lstLast( HLST hLst );

/******************************
 * lstNext
 *
 * 1. makes next item the current item
 * 2. returns pData or NULL
 ******************************/
void *lstNext( HLST hLst );

/******************************
 * lstOpen
 *
 * 1. Create an empty list.
 *
 * *** MUST CALL lstClose WHEN DONE OR LOSE MEMORY ***
 *
 ******************************/
HLST lstOpen();

/******************************
 * lstOpenCursor
 *
 * 1. If you are going to use cursors then just use cursors. Do
 *    not use move funcs, get funcs, etc on base list and use
 *    cursors as well. Garbage collection only accounts for
 *    cursors when deleting items that have been flagged for
 *    deletion... so direct access could result in the list
 *    changing unexpectedly.
 *
 * 2. pFilterFunc is optional. If you provide this function
 *    pointer the cursor list will be generated to include
 *    all items where pFilterFunc( lstGet( hBase ) ) = true.
 *    Leaving it NULL just means that all items in hBase will
 *    be included in the cursor list.
 *
 * *** MUST CALL lstClose WHEN DONE OR LOSE MEMORY ***
 *
 ******************************/
HLST lstOpenCursor( HLST hBase, int (*pFilterFunc)( HLST, void * ), void *pExtras );

/******************************
 * lstPrev
 *
 * 1. makes prev item the current item
 * 2. returns pData or NULL
 ******************************/
void *lstPrev( HLST hLst );

/******************************
 * lstSet
 *
 * 1. replaces pData pointer
 * 2. returns pData
 * 3. Will recurse down to base data.
 *
 * *** THIS SHOULD BE CHANGED TO AVOID CHANGING THE pData POINTER AND RESIZE THE BUFFER INSTEAD
 *
 ******************************/
void *lstSet( HLST hLst, void *pData );

/******************************
 * lstSetFreeFunc
 *
 * 1. The given function will be called when ever there is a need to free pData
 * 2. The default action is to simply free(pData).
 ******************************/
int lstSetFreeFunc( HLST hLst, void (*pFree)( void *pData ) );

/******************************
 * lstSeek
 *
 * 1. Tries to set hCurrent to the item where pData is at
 * 2. simply scans from 1st to last so lsEOL() = true when not found
 *
 ******************************/
int lstSeek( HLST hLst, void *pData );

/******************************
 * lstSeekItem
 *
 * 1. Tries to set hCurrent to the item where hItem is at
 * 2. simply scans from 1st to last so lsEOL() = true when not found
 *
 ******************************/
int lstSeekItem( HLST hLst, HLSTITEM hItem );

/***************[ FOR INTERNAL USE ]***********************/

/***************************
 * ENSURE CURRENT IS NOT ON A bDelete ITEM
 ***************************/
void *_lstAdjustCurrent( HLST hLst );

/***************************
 *
 ***************************/
void _lstDump( HLST hLst );

/******************************
 * _lstFreeItem
 *
 * 1. Does a real delete. Frees memory used by item.
 *    will delete root item as required.
 *
 ******************************/
int _lstFreeItem( HLSTITEM hItem );

/******************************
 * _lstNextValidItem
 *
 * 1. Starts scanning hLst at hItem until a non-deleted Item found or EOL
 *
 ******************************/
HLSTITEM _lstNextValidItem( HLST hLst, HLSTITEM hItem );

/******************************
 * _lstPrevValidItem
 *
 * 1. Starts scanning hLst at hItem until a non-deleted Item found or EOL
 *
 ******************************/
HLSTITEM _lstPrevValidItem( HLST hLst, HLSTITEM hItem );


/******************************
 * _lstVisible
 *
 *
 ******************************/
int _lstVisible( HLSTITEM hItem );


/******************************
 * _lstAppend
 *
 *
 ******************************/
int _lstAppend( HLST hLst, HLSTITEM hItem );

/******************************
 * _lstInsert
 *
 *
 ******************************/
int _lstInsert( HLST hLst, HLSTITEM hItem );

#if defined(__cplusplus)
         } 
#endif

#endif

