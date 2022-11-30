/*
 *  Copyright 2001-2006 Adrian Thurston <thurston@cs.queensu.ca>
 *            2004 Erich Ocean <eric.ocean@ampede.com>
 *            2005 Alan West <alan@alanz.com>
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

#include "rlgen-cd.h"
#include "fgotocodegen.h"
#include "redfsm.h"
#include "gendata.h"
#include "bstmap.h"

std::ostream &FGotoCodeGen::EXEC_ACTIONS()
{
	/* Loop the actions. */
	for ( ActionTableMap::Iter redAct = redFsm->actionMap; redAct.lte(); redAct++ ) {
		if ( redAct->numTransRefs > 0 ) {
			/* 	We are at the start of a glob, write the case. */
			out << "f" << redAct->actListId << ":\n";

			/* Write each action in the list of action items. */
			for ( ActionTable::Iter item = redAct->key; item.lte(); item++ )
				ACTION( out, item->value, 0, false );

			out << "\tgoto _again;\n";
		}
	}
	return out;
}

/* Write out the function switch. This switch is keyed on the values
 * of the func index. */
std::ostream &FGotoCodeGen::TO_STATE_ACTION_SWITCH()
{
	/* Loop the actions. */
	for ( ActionTableMap::Iter redAct = redFsm->actionMap; redAct.lte(); redAct++ ) {
		if ( redAct->numToStateRefs > 0 ) {
			/* Write the entry label. */
			out << "\tcase " << redAct->actListId+1 << ":\n";

			/* Write each action in the list of action items. */
			for ( ActionTable::Iter item = redAct->key; item.lte(); item++ )
				ACTION( out, item->value, 0, false );

			out << "\tbreak;\n";
		}
	}

	genLineDirective( out );
	return out;
}

/* Write out the function switch. This switch is keyed on the values
 * of the func index. */
std::ostream &FGotoCodeGen::FROM_STATE_ACTION_SWITCH()
{
	/* Loop the actions. */
	for ( ActionTableMap::Iter redAct = redFsm->actionMap; redAct.lte(); redAct++ ) {
		if ( redAct->numFromStateRefs > 0 ) {
			/* Write the entry label. */
			out << "\tcase " << redAct->actListId+1 << ":\n";

			/* Write each action in the list of action items. */
			for ( ActionTable::Iter item = redAct->key; item.lte(); item++ )
				ACTION( out, item->value, 0, false );

			out << "\tbreak;\n";
		}
	}

	genLineDirective( out );
	return out;
}

std::ostream &FGotoCodeGen::EOF_ACTION_SWITCH()
{
	/* Loop the actions. */
	for ( ActionTableMap::Iter redAct = redFsm->actionMap; redAct.lte(); redAct++ ) {
		if ( redAct->numEofRefs > 0 ) {
			/* Write the entry label. */
			out << "\tcase " << redAct->actListId+1 << ":\n";

			/* Write each action in the list of action items. */
			for ( ActionTable::Iter item = redAct->key; item.lte(); item++ )
				ACTION( out, item->value, 0, true );

			out << "\tbreak;\n";
		}
	}

	genLineDirective( out );
	return out;
}


std::ostream &FGotoCodeGen::FINISH_CASES()
{
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* States that are final and have an out action need a case. */
		if ( st->eofAction != 0 ) {
			/* Write the case label. */
			out << "\t\tcase " << st->id << ": ";

			/* Jump to the func. */
			out << "goto f" << st->eofAction->actListId << ";\n";
		}
	}

	return out;
}

unsigned int FGotoCodeGen::TO_STATE_ACTION( RedStateAp *state )
{
	int act = 0;
	if ( state->toStateAction != 0 )
		act = state->toStateAction->actListId+1;
	return act;
}

unsigned int FGotoCodeGen::FROM_STATE_ACTION( RedStateAp *state )
{
	int act = 0;
	if ( state->fromStateAction != 0 )
		act = state->fromStateAction->actListId+1;
	return act;
}

unsigned int FGotoCodeGen::EOF_ACTION( RedStateAp *state )
{
	int act = 0;
	if ( state->eofAction != 0 )
		act = state->eofAction->actListId+1;
	return act;
}

void FGotoCodeGen::writeData()
{
	if ( redFsm->anyToStateActions() ) {
		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxActionLoc), TSA() );
		TO_STATE_ACTIONS();
		CLOSE_ARRAY() <<
		"\n";
	}

	if ( redFsm->anyFromStateActions() ) {
		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxActionLoc), FSA() );
		FROM_STATE_ACTIONS();
		CLOSE_ARRAY() <<
		"\n";
	}

	if ( redFsm->anyEofActions() ) {
		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxActionLoc), EA() );
		EOF_ACTIONS();
		CLOSE_ARRAY() <<
		"\n";
	}

	STATE_IDS();
}

void FGotoCodeGen::writeExec()
{
	outLabelUsed = false;

	out << "	{\n";

	if ( redFsm->anyRegCurStateRef() )
		out << "	int _ps = 0;\n";

	if ( redFsm->anyConditions() )
		out << "	" << WIDE_ALPH_TYPE() << " _widec;\n";

	if ( hasEnd ) {
		outLabelUsed = true;
		out << 
			"	if ( " << P() << " == " << PE() << " )\n"
			"		goto _out;\n";
	}

	out << "_resume:\n";

	if ( redFsm->anyFromStateActions() ) {
		out <<
			"	switch ( " << FSA() << "[" << CS() << "] ) {\n";
			FROM_STATE_ACTION_SWITCH();
			SWITCH_DEFAULT() <<
			"	}\n"
			"\n";
	}

	out << 
		"	switch ( " << CS() << " ) {\n";
		STATE_GOTOS();
		SWITCH_DEFAULT() <<
		"	}\n"
		"\n";
		TRANSITIONS() << 
		"\n";

	if ( redFsm->anyRegActions() )
		EXEC_ACTIONS() << "\n";

	out << "_again:\n";

	if ( redFsm->anyToStateActions() ) {
		out <<
			"	switch ( " << TSA() << "[" << CS() << "] ) {\n";
			TO_STATE_ACTION_SWITCH();
			SWITCH_DEFAULT() <<
			"	}\n"
			"\n";
	}

	if ( hasEnd ) {
		out << 
			"	if ( ++" << P() << " != " << PE() << " )\n"
			"		goto _resume;\n";
	}
	else {
		out << 
			"	" << P() << " += 1;\n"
			"	goto _resume;\n";
	}


	if ( outLabelUsed )
		out << "	_out: {}\n";

	out << "	}\n";
}

void FGotoCodeGen::writeEOF()
{
	if ( redFsm->anyEofActions() ) {
		out <<
			"	{\n"
			"	switch ( " << EA() << "[" << CS() << "] ) {\n";
			EOF_ACTION_SWITCH();
			SWITCH_DEFAULT() <<
			"	}\n"
			"	}\n"
			"\n";
	}
}
