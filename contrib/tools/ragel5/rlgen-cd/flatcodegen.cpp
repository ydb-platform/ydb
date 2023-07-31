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
#include "flatcodegen.h"
#include "redfsm.h"
#include "gendata.h"

std::ostream &FlatCodeGen::TO_STATE_ACTION( RedStateAp *state )
{
	int act = 0;
	if ( state->toStateAction != 0 )
		act = state->toStateAction->location+1;
	out << act;
	return out;
}

std::ostream &FlatCodeGen::FROM_STATE_ACTION( RedStateAp *state )
{
	int act = 0;
	if ( state->fromStateAction != 0 )
		act = state->fromStateAction->location+1;
	out << act;
	return out;
}

std::ostream &FlatCodeGen::EOF_ACTION( RedStateAp *state )
{
	int act = 0;
	if ( state->eofAction != 0 )
		act = state->eofAction->location+1;
	out << act;
	return out;
}

std::ostream &FlatCodeGen::TRANS_ACTION( RedTransAp *trans )
{
	/* If there are actions, emit them. Otherwise emit zero. */
	int act = 0;
	if ( trans->action != 0 )
		act = trans->action->location+1;
	out << act;
	return out;
}

std::ostream &FlatCodeGen::TO_STATE_ACTION_SWITCH()
{
	/* Walk the list of functions, printing the cases. */
	for ( ActionList::Iter act = actionList; act.lte(); act++ ) {
		/* Write out referenced actions. */
		if ( act->numToStateRefs > 0 ) {
			/* Write the case label, the action and the case break */
			out << "\tcase " << act->actionId << ":\n";
			ACTION( out, act, 0, false );
			out << "\tbreak;\n";
		}
	}

	genLineDirective( out );
	return out;
}

std::ostream &FlatCodeGen::FROM_STATE_ACTION_SWITCH()
{
	/* Walk the list of functions, printing the cases. */
	for ( ActionList::Iter act = actionList; act.lte(); act++ ) {
		/* Write out referenced actions. */
		if ( act->numFromStateRefs > 0 ) {
			/* Write the case label, the action and the case break */
			out << "\tcase " << act->actionId << ":\n";
			ACTION( out, act, 0, false );
			out << "\tbreak;\n";
		}
	}

	genLineDirective( out );
	return out;
}

std::ostream &FlatCodeGen::EOF_ACTION_SWITCH()
{
	/* Walk the list of functions, printing the cases. */
	for ( ActionList::Iter act = actionList; act.lte(); act++ ) {
		/* Write out referenced actions. */
		if ( act->numEofRefs > 0 ) {
			/* Write the case label, the action and the case break */
			out << "\tcase " << act->actionId << ":\n";
			ACTION( out, act, 0, true );
			out << "\tbreak;\n";
		}
	}

	genLineDirective( out );
	return out;
}


std::ostream &FlatCodeGen::ACTION_SWITCH()
{
	/* Walk the list of functions, printing the cases. */
	for ( ActionList::Iter act = actionList; act.lte(); act++ ) {
		/* Write out referenced actions. */
		if ( act->numTransRefs > 0 ) {
			/* Write the case label, the action and the case break */
			out << "\tcase " << act->actionId << ":\n";
			ACTION( out, act, 0, false );
			out << "\tbreak;\n";
		}
	}

	genLineDirective( out );
	return out;
}


std::ostream &FlatCodeGen::FLAT_INDEX_OFFSET()
{
	out << "\t";
	int totalStateNum = 0, curIndOffset = 0;
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* Write the index offset. */
		out << curIndOffset;
		if ( !st.last() ) {
			out << ", ";
			if ( ++totalStateNum % IALL == 0 )
				out << "\n\t";
		}
		
		/* Move the index offset ahead. */
		if ( st->transList != 0 )
			curIndOffset += keyOps->span( st->lowKey, st->highKey );

		if ( st->defTrans != 0 )
			curIndOffset += 1;
	}
	out << "\n";
	return out;
}

std::ostream &FlatCodeGen::KEY_SPANS()
{
	out << "\t";
	int totalStateNum = 0;
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* Write singles length. */
		unsigned long long span = 0;
		if ( st->transList != 0 )
			span = keyOps->span( st->lowKey, st->highKey );
		out << span;
		if ( !st.last() ) {
			out << ", ";
			if ( ++totalStateNum % IALL == 0 )
				out << "\n\t";
		}
	}
	out << "\n";
	return out;
}

std::ostream &FlatCodeGen::TO_STATE_ACTIONS()
{
	out << "\t";
	int totalStateNum = 0;
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* Write any eof action. */
		TO_STATE_ACTION(st);
		if ( !st.last() ) {
			out << ", ";
			if ( ++totalStateNum % IALL == 0 )
				out << "\n\t";
		}
	}
	out << "\n";
	return out;
}

std::ostream &FlatCodeGen::FROM_STATE_ACTIONS()
{
	out << "\t";
	int totalStateNum = 0;
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* Write any eof action. */
		FROM_STATE_ACTION(st);
		if ( !st.last() ) {
			out << ", ";
			if ( ++totalStateNum % IALL == 0 )
				out << "\n\t";
		}
	}
	out << "\n";
	return out;
}

std::ostream &FlatCodeGen::EOF_ACTIONS()
{
	out << "\t";
	int totalStateNum = 0;
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* Write any eof action. */
		EOF_ACTION(st);
		if ( !st.last() ) {
			out << ", ";
			if ( ++totalStateNum % IALL == 0 )
				out << "\n\t";
		}
	}
	out << "\n";
	return out;
}

std::ostream &FlatCodeGen::COND_KEYS()
{
	out << '\t';
	int totalTrans = 0;
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* Emit just cond low key and cond high key. */
		out << KEY( st->condLowKey ) << ", ";
		out << KEY( st->condHighKey ) << ", ";
		if ( ++totalTrans % IALL == 0 )
			out << "\n\t";
	}

	/* Output one last number so we don't have to figure out when the last
	 * entry is and avoid writing a comma. */
	out << 0 << "\n";
	return out;
}

std::ostream &FlatCodeGen::COND_KEY_SPANS()
{
	out << "\t";
	int totalStateNum = 0;
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* Write singles length. */
		unsigned long long span = 0;
		if ( st->condList != 0 )
			span = keyOps->span( st->condLowKey, st->condHighKey );
		out << span;
		if ( !st.last() ) {
			out << ", ";
			if ( ++totalStateNum % IALL == 0 )
				out << "\n\t";
		}
	}
	out << "\n";
	return out;
}

std::ostream &FlatCodeGen::CONDS()
{
	int totalTrans = 0;
	out << '\t';
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		if ( st->condList != 0 ) {
			/* Walk the singles. */
			unsigned long long span = keyOps->span( st->condLowKey, st->condHighKey );
			for ( unsigned long long pos = 0; pos < span; pos++ ) {
				if ( st->condList[pos] != 0 )
					out << st->condList[pos]->condSpaceId + 1 << ", ";
				else
					out << "0, ";
				if ( ++totalTrans % IALL == 0 )
					out << "\n\t";
			}
		}
	}

	/* Output one last number so we don't have to figure out when the last
	 * entry is and avoid writing a comma. */
	out << 0 << "\n";
	return out;
}

std::ostream &FlatCodeGen::COND_INDEX_OFFSET()
{
	out << "\t";
	int totalStateNum = 0, curIndOffset = 0;
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* Write the index offset. */
		out << curIndOffset;
		if ( !st.last() ) {
			out << ", ";
			if ( ++totalStateNum % IALL == 0 )
				out << "\n\t";
		}
		
		/* Move the index offset ahead. */
		if ( st->condList != 0 )
			curIndOffset += keyOps->span( st->condLowKey, st->condHighKey );
	}
	out << "\n";
	return out;
}


std::ostream &FlatCodeGen::KEYS()
{
	out << '\t';
	int totalTrans = 0;
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		/* Emit just low key and high key. */
		out << KEY( st->lowKey ) << ", ";
		out << KEY( st->highKey ) << ", ";
		if ( ++totalTrans % IALL == 0 )
			out << "\n\t";
	}

	/* Output one last number so we don't have to figure out when the last
	 * entry is and avoid writing a comma. */
	out << 0 << "\n";
	return out;
}

std::ostream &FlatCodeGen::INDICIES()
{
	int totalTrans = 0;
	out << '\t';
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		if ( st->transList != 0 ) {
			/* Walk the singles. */
			unsigned long long span = keyOps->span( st->lowKey, st->highKey );
			for ( unsigned long long pos = 0; pos < span; pos++ ) {
				out << st->transList[pos]->id << ", ";
				if ( ++totalTrans % IALL == 0 )
					out << "\n\t";
			}
		}

		/* The state's default index goes next. */
		if ( st->defTrans != 0 )
			out << st->defTrans->id << ", ";

		if ( ++totalTrans % IALL == 0 )
			out << "\n\t";
	}

	/* Output one last number so we don't have to figure out when the last
	 * entry is and avoid writing a comma. */
	out << 0 << "\n";
	return out;
}

std::ostream &FlatCodeGen::TRANS_TARGS()
{
	/* Transitions must be written ordered by their id. */
	RedTransAp **transPtrs = new RedTransAp*[redFsm->transSet.length()];
	for ( TransApSet::Iter trans = redFsm->transSet; trans.lte(); trans++ )
		transPtrs[trans->id] = trans;

	/* Keep a count of the num of items in the array written. */
	out << '\t';
	int totalStates = 0;
	for ( int t = 0; t < redFsm->transSet.length(); t++ ) {
		/* Write out the target state. */
		RedTransAp *trans = transPtrs[t];
		out << trans->targ->id;
		if ( t < redFsm->transSet.length()-1 ) {
			out << ", ";
			if ( ++totalStates % IALL == 0 )
				out << "\n\t";
		}
	}
	out << "\n";
	delete[] transPtrs;
	return out;
}


std::ostream &FlatCodeGen::TRANS_ACTIONS()
{
	/* Transitions must be written ordered by their id. */
	RedTransAp **transPtrs = new RedTransAp*[redFsm->transSet.length()];
	for ( TransApSet::Iter trans = redFsm->transSet; trans.lte(); trans++ )
		transPtrs[trans->id] = trans;

	/* Keep a count of the num of items in the array written. */
	out << '\t';
	int totalAct = 0;
	for ( int t = 0; t < redFsm->transSet.length(); t++ ) {
		/* Write the function for the transition. */
		RedTransAp *trans = transPtrs[t];
		TRANS_ACTION( trans );
		if ( t < redFsm->transSet.length()-1 ) {
			out << ", ";
			if ( ++totalAct % IALL == 0 )
				out << "\n\t";
		}
	}
	out << "\n";
	delete[] transPtrs;
	return out;
}

void FlatCodeGen::LOCATE_TRANS()
{
	out <<
		"	_keys = " << ARR_OFF( K(), "(" + CS() + "<<1)" ) << ";\n"
		"	_inds = " << ARR_OFF( I(), IO() + "[" + CS() + "]" ) << ";\n"
		"\n"
		"	_slen = " << SP() << "[" << CS() << "];\n"
		"	_trans = _inds[ _slen > 0 && _keys[0] <=" << GET_WIDE_KEY() << " &&\n"
		"		" << GET_WIDE_KEY() << " <= _keys[1] ?\n"
		"		" << GET_WIDE_KEY() << " - _keys[0] : _slen ];\n"
		"\n";
}

void FlatCodeGen::GOTO( ostream &ret, int gotoDest, bool inFinish )
{
	ret << "{" << CS() << " = " << gotoDest << "; " << 
			CTRL_FLOW() << "goto _again;}";
}

void FlatCodeGen::GOTO_EXPR( ostream &ret, InlineItem *ilItem, bool inFinish )
{
	ret << "{" << CS() << " = (";
	INLINE_LIST( ret, ilItem->children, 0, inFinish );
	ret << "); " << CTRL_FLOW() << "goto _again;}";
}

void FlatCodeGen::CURS( ostream &ret, bool inFinish )
{
	ret << "(_ps)";
}

void FlatCodeGen::TARGS( ostream &ret, bool inFinish, int targState )
{
	ret << "(" << CS() << ")";
}

void FlatCodeGen::NEXT( ostream &ret, int nextDest, bool inFinish )
{
	ret << CS() << " = " << nextDest << ";";
}

void FlatCodeGen::NEXT_EXPR( ostream &ret, InlineItem *ilItem, bool inFinish )
{
	ret << CS() << " = (";
	INLINE_LIST( ret, ilItem->children, 0, inFinish );
	ret << ");";
}

void FlatCodeGen::CALL( ostream &ret, int callDest, int targState, bool inFinish )
{
	ret << "{" << STACK() << "[" << TOP() << "++] = " << CS() << "; " << CS() << " = " << 
			callDest << "; " << CTRL_FLOW() << "goto _again;}";
}


void FlatCodeGen::CALL_EXPR( ostream &ret, InlineItem *ilItem, int targState, bool inFinish )
{
	ret << "{" << STACK() << "[" << TOP() << "++] = " << CS() << "; " << CS() << " = (";
	INLINE_LIST( ret, ilItem->children, targState, inFinish );
	ret << "); " << CTRL_FLOW() << "goto _again;}";
}


void FlatCodeGen::RET( ostream &ret, bool inFinish )
{
	ret << "{" << CS() << " = " << STACK() << "[--" << TOP() << "]; " << 
			CTRL_FLOW() << "goto _again;}";
}

void FlatCodeGen::BREAK( ostream &ret, int targState )
{
	outLabelUsed = true;
	ret << CTRL_FLOW() << "goto _out;";
}

void FlatCodeGen::writeData()
{
	/* If there are any transtion functions then output the array. If there
	 * are none, don't bother emitting an empty array that won't be used. */
	if ( redFsm->anyActions() ) {
		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxActArrItem), A() );
		ACTIONS_ARRAY();
		CLOSE_ARRAY() <<
		"\n";
	}

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
		OPEN_ARRAY( ARRAY_TYPE(redFsm->maxActionLoc), TA() );
		TRANS_ACTIONS();
		CLOSE_ARRAY() <<
		"\n";
	}

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

void FlatCodeGen::COND_TRANSLATE()
{
	out << 
		"	_widec = " << GET_KEY() << ";\n";

	out <<
		"	_keys = " << ARR_OFF( CK(), "(" + CS() + "<<1)" ) << ";\n"
		"	_conds = " << ARR_OFF( C(), CO() + "[" + CS() + "]" ) << ";\n"
		"\n"
		"	_slen = " << CSP() << "[" << CS() << "];\n"
		"	_cond = _slen > 0 && _keys[0] <=" << GET_WIDE_KEY() << " &&\n"
		"		" << GET_WIDE_KEY() << " <= _keys[1] ?\n"
		"		_conds[" << GET_WIDE_KEY() << " - _keys[0]] : 0;\n"
		"\n";

	out <<
		"	switch ( _cond ) {\n";
	for ( CondSpaceList::Iter csi = condSpaceList; csi.lte(); csi++ ) {
		CondSpace *condSpace = csi;
		out << "	case " << condSpace->condSpaceId + 1 << ": {\n";
		out << TABS(2) << "_widec = " << CAST(WIDE_ALPH_TYPE()) << "(" <<
				KEY(condSpace->baseKey) << " + (" << GET_KEY() << 
				" - " << KEY(keyOps->minKey) << "));\n";

		for ( CondSet::Iter csi = condSpace->condSet; csi.lte(); csi++ ) {
			out << TABS(2) << "if ( ";
			CONDITION( out, *csi );
			Size condValOffset = ((1 << csi.pos()) * keyOps->alphSize());
			out << " ) _widec += " << condValOffset << ";\n";
		}

		out << "		}\n";
		out << "		break;\n";
	}

	SWITCH_DEFAULT();

	out <<
		"	}\n";
}

void FlatCodeGen::writeExec()
{
	outLabelUsed = false;

	out << 
		"	{\n"
		"	int _slen";

	if ( redFsm->anyRegCurStateRef() )
		out << ", _ps";

	out << 
		";\n"
		"	int _trans";

	if ( redFsm->anyConditions() )
		out << ", _cond";
	out << ";\n";

	if ( redFsm->anyToStateActions() || 
			redFsm->anyRegActions() || redFsm->anyFromStateActions() )
	{
		out << 
			"	" << PTR_CONST() << ARRAY_TYPE(redFsm->maxActArrItem) << POINTER() << "_acts;\n"
			"	" << UINT() << " _nacts;\n"; 
	}

	out <<
		"	" << PTR_CONST() << WIDE_ALPH_TYPE() << POINTER() << "_keys;\n"
		"	" << PTR_CONST() << ARRAY_TYPE(redFsm->maxIndex) << POINTER() << "_inds;\n";

	if ( redFsm->anyConditions() ) {
		out << 
			"	" << PTR_CONST() << ARRAY_TYPE(redFsm->maxCond) << POINTER() << "_conds;\n"
			"	" << WIDE_ALPH_TYPE() << " _widec;\n";
	}

	out << "\n";

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
			"	_acts = " << ARR_OFF( A(), FSA() + "[" + CS() + "]" ) << ";\n"
			"	_nacts = " << CAST(UINT()) << " *_acts++;\n"
			"	while ( _nacts-- > 0 ) {\n"
			"		switch ( *_acts++ ) {\n";
			FROM_STATE_ACTION_SWITCH();
			SWITCH_DEFAULT() <<
			"		}\n"
			"	}\n"
			"\n";
	}

	if ( redFsm->anyConditions() )
		COND_TRANSLATE();

	LOCATE_TRANS();

	if ( redFsm->anyRegCurStateRef() )
		out << "	_ps = " << CS() << ";\n";

	out <<
		"	" << CS() << " = " << TT() << "[_trans];\n"
		"\n";

	if ( redFsm->anyRegActions() ) {
		out <<
			"	if ( " << TA() << "[_trans] == 0 )\n"
			"		goto _again;\n"
			"\n"
			"	_acts = " << ARR_OFF( A(), TA() + "[_trans]" ) << ";\n"
			"	_nacts = " << CAST(UINT()) << " *_acts++;\n"
			"	while ( _nacts-- > 0 ) {\n"
			"		switch ( *(_acts++) )\n		{\n";
			ACTION_SWITCH();
			SWITCH_DEFAULT() <<
			"		}\n"
			"	}\n"
			"\n";
	}

	if ( redFsm->anyRegActions() || redFsm->anyActionGotos() || 
			redFsm->anyActionCalls() || redFsm->anyActionRets() )
		out << "_again:\n";

	if ( redFsm->anyToStateActions() ) {
		out <<
			"	_acts = " << ARR_OFF( A(),  TSA() + "[" + CS() + "]" ) << ";\n"
			"	_nacts = " << CAST(UINT()) << " *_acts++;\n"
			"	while ( _nacts-- > 0 ) {\n"
			"		switch ( *_acts++ ) {\n";
			TO_STATE_ACTION_SWITCH();
			SWITCH_DEFAULT() <<
			"		}\n"
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

void FlatCodeGen::writeEOF()
{
	if ( redFsm->anyEofActions() ) {
		out << 
			"	{\n"
			"	" << PTR_CONST() << ARRAY_TYPE(redFsm->maxActArrItem) << POINTER() << "_acts = " << 
					ARR_OFF( A(), EA() + "[" + CS() + "]" ) << ";\n"
			"	" << UINT() << " _nacts = " << CAST(UINT()) << " *_acts++;\n"
			"	while ( _nacts-- > 0 ) {\n"
			"		switch ( *_acts++ ) {\n";
			EOF_ACTION_SWITCH();
			SWITCH_DEFAULT() <<
			"		}\n"
			"	}\n"
			"	}\n"
			"\n";
	}
}
