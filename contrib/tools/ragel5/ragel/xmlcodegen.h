/*
 *  Copyright 2005, 2006 Adrian Thurston <thurston@cs.queensu.ca>
 */

/*  This file is part of Ragel.
 *
 *  Ragel is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 * 
 *  Ragel is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with Ragel; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA 
 */

#ifndef _XMLDOTGEN_H
#define _XMLDOTGEN_H

#include <iostream>
#include "avltree.h"
#include "fsmgraph.h"
#include "parsedata.h"

/* Forwards. */
struct TransAp;
struct FsmAp;
struct ParseData;

struct RedActionTable
:
	public AvlTreeEl<RedActionTable>
{
	RedActionTable( const ActionTable &key )
	:	
		key(key), 
		id(0)
	{ }

	const ActionTable &getKey() 
		{ return key; }

	ActionTable key;
	int id;
};

typedef AvlTree<RedActionTable, ActionTable, CmpActionTable> ActionTableMap;

struct NextRedTrans
{
	Key lowKey, highKey;
	TransAp *trans;
	TransAp *next;

	void load() {
		if ( trans != 0 ) {
			next = trans->next;
			lowKey = trans->lowKey;
			highKey = trans->highKey;
		}
	}

	NextRedTrans( TransAp *t ) {
		trans = t;
		load();
	}

	void increment() {
		trans = next;
		load();
	}
};

class XMLCodeGen
{
public:
	XMLCodeGen( char *fsmName, ParseData *pd, FsmAp *fsm, std::ostream &out );
	void writeXML( );

private:
	void appendTrans( TransListVect &outList, Key lowKey, Key highKey, TransAp *trans );
	void writeStateActions( StateAp *state );
	void writeStateList();
	void writeStateConditions( StateAp *state );

	void writeKey( Key key );
	void writeText( InlineItem *item );
	void writeCtrlFlow( InlineItem *item, InlineItem *context );
	void writePtrMod( InlineItem *item, InlineItem *context );
	void writeGoto( InlineItem *item, InlineItem *context );
	void writeGotoExpr( InlineItem *item, InlineItem *context );
	void writeCall( InlineItem *item, InlineItem *context );
	void writeCallExpr( InlineItem *item, InlineItem *context );
	void writeNext( InlineItem *item, InlineItem *context );
	void writeNextExpr( InlineItem *item, InlineItem *context );
	void writeEntry( InlineItem *item );
	void writeLmSetActId( InlineItem *item );
	void writeLmOnLast( InlineItem *item );
	void writeLmOnNext( InlineItem *item );
	void writeLmOnLagBehind( InlineItem *item );

	void writeExports();
	bool writeNameInst( NameInst *nameInst );
	void writeEntryPoints();
	void writeGetKeyExpr();
	void writeAccessExpr();
	void writeCurStateExpr();
	void writeConditions();
	void writeInlineList( InlineList *inlineList, InlineItem *context );
	void writeAlphType();
	void writeActionList();
	void writeActionTableList();
	void reduceTrans( TransAp *trans );
	void reduceActionTables();
	void writeTransList( StateAp *state );
	void writeTrans( Key lowKey, Key highKey, TransAp *defTrans );
	void writeAction( Action *action );
	void writeLmSwitch( InlineItem *item );
	void writeMachine();
	void writeActionExec( InlineItem *item );
	void writeActionExecTE( InlineItem *item );

	char *fsmName;
	ParseData *pd;
	FsmAp *fsm;
	std::ostream &out;
	ActionTableMap actionTableMap;
	int nextActionTableId;
};


#endif /* _XMLDOTGEN_H */
