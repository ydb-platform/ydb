/*
 *  Copyright 2004-2006 Adrian Thurston <thurston@cs.queensu.ca>
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
#include "fflatcodegen.h"
#include "redfsm.h"
#include "gendata.h"

std::ostream &FFlatCodeGen::TO_STATE_ACTION( RedStateAp *state )
{
	int act = 0;
	if ( state->toStateAction != 0 )
		act = state->toStateAction->actListId+1;
	out << act;
	return out;
}

std::ostream &FFlatCodeGen::FROM_STATE_ACTION( RedStateAp *state )
{
	int act = 0;
	if ( state->fromStateAction != 0 )
		act = state->fromStateAction->actListId+1;
	out << act;
	return out;
}

std::ostream &FFlatCodeGen::EOF_ACTION( RedStateAp *state )
{
	int act = 0;
	if ( state->eofAction != 0 )
		act = state->eofAction->actListId+1;
	out << act;
	return out;
}

/* Write out the function for a transition. */
std::ostream &FFlatCodeGen::TRANS_ACTION( RedTransAp *trans )
{
	int action = 0;
	if ( trans->action != 0 )
		action = trans->action->actListId+1;
	out << action;
	return out;
}

/* Write out the function switch. This switch is keyed on the values
 * of the func index. */
std::ostream &FFlatCodeGen::TO_STATE_ACTION_SWITCH()
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
std::ostream &FFlatCodeGen::FROM_STATE_ACTION_SWITCH()
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

std::ostream &FFlatCodeGen::EOF_ACTION_SWITCH()
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

/* Write out the function switch. This switch is keyed on the values
 * of the func index. */
std::ostream &FFlatCodeGen::ACTION_SWITCH()
{
	/* Loop the actions. */
	for ( ActionTableMap::Iter redAct = redFsm->actionMap; redAct.lte(); redAct++ ) {
		if ( redAct->numTransRefs > 0 ) {
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

void FFlatCodeGen::writeData()
{
	if ( redFsm->anyConditions() ) {
		OPEN_ARRAY( WIDE_ALPH_TYPE(), CK() );
		COND_KEYS();
		CLOSE_ARRAY() <<
		"\n";

		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxCondSpan), CSP() );
		COND_KEY_SPANS();
		CLOSE_ARRAY() <<
		"\n";

		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxCond), C() );
		CONDS();
		CLOSE_ARRAY() <<
		"\n";

		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxCondIndexOffset), CO() );
		COND_INDEX_OFFSET();
		CLOSE_ARRAY() <<
		"\n";
	}

	OPEN_ARRAY( WIDE_ALPH_TYPE(), K() );
	KEYS();
	CLOSE_ARRAY() <<
	"\n";

	OPEN_ARRAY( ARRAY_TYPE(redFsm->maxSpan), SP() );
	KEY_SPANS();
	CLOSE_ARRAY() <<
	"\n";

	OPEN_ARRAY( ARRAY_TYPE(redFsm->maxFlatIndexOffset), IO() );
	FLAT_INDEX_OFFSET();
	CLOSE_ARRAY() <<
	"\n";

	OPEN_ARRAY( ARRAY_TYPE(redFsm->maxIndex), I() );
	INDICIES();
	CLOSE_ARRAY() <<
	"\n";

	OPEN_ARRAY( ARRAY_TYPE(redFsm->maxState), TT() );
	TRANS_TARGS();
	CLOSE_ARRAY() <<
	"\n";

	if ( redFsm->anyActions() ) {
		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxActListId), TA() );
		TRANS_ACTIONS();
		CLOSE_ARRAY() <<
		"\n";
	}

	if ( redFsm->anyToStateActions() ) {
		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxActionLoc),  TSA() );
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
		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxActListId), EA() );
		EOF_ACTIONS();
		CLOSE_ARRAY() <<
		"\n";
	}

	STATE_IDS();
}

void FFlatCodeGen::writeExec()
{
	outLabelUsed = false;

	out << 
		"	{\n"
		"	int _slen";

	if ( redFsm->anyRegCurStateRef() )
		out << ", _ps";
	
	out << ";\n";
	out << "	int _trans";

	if ( redFsm->anyConditions() )
		out << ", _cond";

	out << ";\n";

	out <<
		"	" << PTR_CONST() << WIDE_ALPH_TYPE() << POINTER() << "_keys;\n"
		"	" << PTR_CONST() << ARRAY_TYPE(redFsm->maxIndex) << POINTER() << "_inds;\n";

	if ( redFsm->anyConditions() ) {
		out << 
			"	" << PTR_CONST() << ARRAY_TYPE(redFsm->maxCond) << POINTER() << "_conds;\n"
			"	" << WIDE_ALPH_TYPE() << " _widec;\n";
	}

	if ( hasEnd ) {
		outLabelUsed = true;
		out << 
			"	if ( " << P() << " == " << PE() << " )\n"
			"		goto _out;\n";
	}

	out << "_resume:\n";

	if ( redFsm->errState != 0 ) {
		outLabelUsed = true;
		out << 
			"	if ( " << CS() << " == " << redFsm->errState->id << " )\n"
			"		goto _out;\n";
	}

	if ( redFsm->anyFromStateActions() ) {
		out <<
			"	switch ( " << FSA() << "[" << CS() << "] ) {\n";
			FROM_STATE_ACTION_SWITCH();
			SWITCH_DEFAULT() <<
			"	}\n"
			"\n";
	}

	if ( redFsm->anyConditions() )
		COND_TRANSLATE();

	LOCATE_TRANS();
	
	if ( redFsm->anyRegCurStateRef() )
		out << "	_ps = " << CS() << ";\n";

	out << 
		"	" << CS() << " = " << TT() << "[_trans];\n\n";

	if ( redFsm->anyRegActions() ) {
		out << 
			"	if ( " << TA() << "[_trans] == 0 )\n"
			"		goto _again;\n"
			"\n"
			"	switch ( " << TA() << "[_trans] ) {\n";
			ACTION_SWITCH();
			SWITCH_DEFAULT() <<
			"	}\n"
			"\n";
	}

	if ( redFsm->anyRegActions() || redFsm->anyActionGotos() || 
			redFsm->anyActionCalls() || redFsm->anyActionRets() )
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

void FFlatCodeGen::writeEOF()
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
