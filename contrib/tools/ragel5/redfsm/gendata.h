/*
 *  Copyright 2005-2007 Adrian Thurston <thurston@cs.queensu.ca>
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

#ifndef _GENDATA_H
#define _GENDATA_H

#include <iostream>
#include "redfsm.h"
#include "common.h"

using std::ostream;

struct NameInst;
typedef DList<Action> ActionList;

typedef unsigned long ulong;

struct FsmCodeGen;
struct CodeGenData;

typedef AvlMap<char *, CodeGenData*, CmpStr> CodeGenMap;
typedef AvlMapEl<char *, CodeGenData*> CodeGenMapEl;

/*
 * The interface to the parser
 */

/* These functions must be implemented by the code generation executable.
 * The openOutput function is invoked when the root element is opened.  The
 * makeCodeGen function is invoked when a ragel_def element is opened. */
std::ostream *openOutput( char *inputFile );
CodeGenData *makeCodeGen( char *sourceFileName, 
		char *fsmName, ostream &out, bool wantComplete );

void lineDirective( ostream &out, char *fileName, int line );
void genLineDirective( ostream &out );

/*********************************/

struct CodeGenData
{
	/*
	 * The interface to the code generator.
	 */
	virtual void finishRagelDef() {}

	/* These are invoked by the corresponding write statements. */
	virtual void writeData() {};
	virtual void writeInit() {};
	virtual void writeExec() {};
	virtual void writeEOF() {};
	virtual void writeExports() {};

	/* This can also be overwridden to modify the processing of write
	 * statements. */
	virtual void writeStatement( InputLoc &loc, int nargs, char **args );

	/********************/

	CodeGenData( ostream &out );
	virtual ~CodeGenData() {}

	/* 
	 * Collecting the machine.
	 */

	char *sourceFileName;
	char *fsmName;
	ostream &out;
	RedFsmAp *redFsm;
	Action *allActions;
	RedAction *allActionTables;
	Condition *allConditions;
	CondSpace *allCondSpaces;
	RedStateAp *allStates;
	NameInst **nameIndex;
	int startState;
	int errState;
	ActionList actionList;
	ConditionList conditionList;
	CondSpaceList condSpaceList;
	InlineList *getKeyExpr;
	InlineList *accessExpr;
	InlineList *curStateExpr;
	KeyOps thisKeyOps;
	bool wantComplete;
	EntryIdVect entryPointIds;
	EntryNameVect entryPointNames;
	bool hasLongestMatch;
	int codeGenErrCount;
	ExportList exportList;

	/* Write options. */
	bool hasEnd;
	bool dataPrefix;
	bool writeFirstFinal;
	bool writeErr;

	void createMachine();
	void initActionList( unsigned long length );
	void newAction( int anum, char *name, int line, int col, InlineList *inlineList );
	void initActionTableList( unsigned long length );
	void initStateList( unsigned long length );
	void setStartState( unsigned long startState );
	void setErrorState( unsigned long errState );
	void addEntryPoint( char *name, unsigned long entryState );
	void setId( int snum, int id );
	void setFinal( int snum );
	void initTransList( int snum, unsigned long length );
	void newTrans( int snum, int tnum, Key lowKey, Key highKey, 
			long targ, long act );
	void finishTransList( int snum );
	void setStateActions( int snum, long toStateAction, 
			long fromStateAction, long eofAction );
	void setForcedErrorState()
		{ redFsm->forcedErrorState = true; }

	
	void initCondSpaceList( ulong length );
	void condSpaceItem( int cnum, long condActionId );
	void newCondSpace( int cnum, int condSpaceId, Key baseKey );

	void initStateCondList( int snum, ulong length );
	void addStateCond( int snum, Key lowKey, Key highKey, long condNum );

	CondSpace *findCondSpace( Key lowKey, Key highKey );
	Condition *findCondition( Key key );

	bool setAlphType( char *data );

	void resolveTargetStates( InlineList *inlineList );
	Key findMaxKey();

	/* Gather various info on the machine. */
	void analyzeActionList( RedAction *redAct, InlineList *inlineList );
	void analyzeAction( Action *act, InlineList *inlineList );
	void findFinalActionRefs();
	void analyzeMachine();

	void closeMachine();
	void setValueLimits();
	void assignActionIds();

	ostream &source_warning( const InputLoc &loc );
	ostream &source_error( const InputLoc &loc );
};


#endif /* _GENDATA_H */
