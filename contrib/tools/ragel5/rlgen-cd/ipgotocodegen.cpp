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
#include "ipgotocodegen.h"
#include "redfsm.h"
#include "gendata.h"
#include "bstmap.h"

bool IpGotoCodeGen::useAgainLabel()
{
	return redFsm->anyRegActionRets() || 
			redFsm->anyRegActionByValControl() || 
			redFsm->anyRegNextStmt();
}

void IpGotoCodeGen::GOTO( ostream &ret, int gotoDest, bool inFinish )
{
	ret << "{" << CTRL_FLOW() << "goto st" << gotoDest << ";}";
}

void IpGotoCodeGen::CALL( ostream &ret, int callDest, int targState, bool inFinish )
{
	ret << "{" << STACK() << "[" << TOP() << "++] = " << targState << 
			"; " << CTRL_FLOW() << "goto st" << callDest << ";}";
}

void IpGotoCodeGen::RET( ostream &ret, bool inFinish )
{
	ret << "{" << CS() << " = " << STACK() << "[--" << TOP() << "]; " << 
			CTRL_FLOW() << "goto _again;}";
}

void IpGotoCodeGen::GOTO_EXPR( ostream &ret, InlineItem *ilItem, bool inFinish )
{
	ret << "{" << CS() << " = (";
	INLINE_LIST( ret, ilItem->children, 0, inFinish );
	ret << "); " << CTRL_FLOW() << "goto _again;}";
}

void IpGotoCodeGen::CALL_EXPR( ostream &ret, InlineItem *ilItem, int targState, bool inFinish )
{
	ret << "{" << STACK() << "[" << TOP() << "++] = " << targState << "; " << CS() << " = (";
	INLINE_LIST( ret, ilItem->children, 0, inFinish );
	ret << "); " << CTRL_FLOW() << "goto _again;}";
}

void IpGotoCodeGen::NEXT( ostream &ret, int nextDest, bool inFinish )
{
	ret << CS() << " = " << nextDest << ";";
}

void IpGotoCodeGen::NEXT_EXPR( ostream &ret, InlineItem *ilItem, bool inFinish )
{
	ret << CS() << " = (";
	INLINE_LIST( ret, ilItem->children, 0, inFinish );
	ret << ");";
}

void IpGotoCodeGen::CURS( ostream &ret, bool inFinish )
{
	ret << "(_ps)";
}

void IpGotoCodeGen::TARGS( ostream &ret, bool inFinish, int targState )
{
	ret << targState;
}

void IpGotoCodeGen::BREAK( ostream &ret, int targState )
{
	ret << CTRL_FLOW() << "goto _out" << targState << ";";
}

bool IpGotoCodeGen::IN_TRANS_ACTIONS( RedStateAp *state )
{
	bool anyWritten = false;

	/* Emit any transitions that have actions and that go to this state. */
	for ( int it = 0; it < state->numInTrans; it++ ) {
		RedTransAp *trans = state->inTrans[it];
		if ( trans->action != 0 && trans->labelNeeded ) {
			/* Remember that we wrote an action so we know to write the
			 * line directive for going back to the output. */
			anyWritten = true;

			/* Write the label for the transition so it can be jumped to. */
			out << "tr" << trans->id << ":\n";

			/* If the action contains a next, then we must preload the current
			 * state since the action may or may not set it. */
			if ( trans->action->anyNextStmt() )
				out << "	" << CS() << " = " << trans->targ->id << ";\n";

			/* Write each action in the list. */
			for ( ActionTable::Iter item = trans->action->key; item.lte(); item++ )
				ACTION( out, item->value, trans->targ->id, false );

			/* If the action contains a next then we need to reload, otherwise
			 * jump directly to the target state. */
			if ( trans->action->anyNextStmt() )
				out << "\tgoto _again;\n";
			else
				out << "\tgoto st" << trans->targ->id << ";\n";
		}
	}

	return anyWritten;
}

/* Called from GotoCodeGen::STATE_GOTOS just before writing the gotos for each
 * state. */
void IpGotoCodeGen::GOTO_HEADER( RedStateAp *state )
{
	bool anyWritten = IN_TRANS_ACTIONS( state );

	if ( state->labelNeeded ) 
		out << "st" << state->id << ":\n";

	if ( state->toStateAction != 0 ) {
		/* Remember that we wrote an action. Write every action in the list. */
		anyWritten = true;
		for ( ActionTable::Iter item = state->toStateAction->key; item.lte(); item++ )
			ACTION( out, item->value, state->id, false );
	}

	/* Advance and test buffer pos. */
	if ( state->labelNeeded ) {
		if ( hasEnd ) {
			out <<
				"	if ( ++" << P() << " == " << PE() << " )\n"
				"		goto _out" << state->id << ";\n";
		}
		else {
			out << 
				"	" << P() << " += 1;\n";
		}
	}

	/* Give the state a switch case. */
	out << "case " << state->id << ":\n";

	if ( state->fromStateAction != 0 ) {
		/* Remember that we wrote an action. Write every action in the list. */
		anyWritten = true;
		for ( ActionTable::Iter item = state->fromStateAction->key; item.lte(); item++ )
			ACTION( out, item->value, state->id, false );
	}

	if ( anyWritten )
		genLineDirective( out );

	/* Record the prev state if necessary. */
	if ( state->anyRegCurStateRef() )
		out << "	_ps = " << state->id << ";\n";
}

void IpGotoCodeGen::STATE_GOTO_ERROR()
{
	/* In the error state we need to emit some stuff that usually goes into
	 * the header. */
	RedStateAp *state = redFsm->errState;
	bool anyWritten = IN_TRANS_ACTIONS( state );

	/* No case label needed since we don't switch on the error state. */
	if ( anyWritten )
		genLineDirective( out );

	if ( state->labelNeeded ) 
		out << "st" << state->id << ":\n";

	/* Break out here. */
	out << "	goto _out" << state->id << ";\n";
}


/* Emit the goto to take for a given transition. */
std::ostream &IpGotoCodeGen::TRANS_GOTO( RedTransAp *trans, int level )
{
	if ( trans->action != 0 ) {
		/* Go to the transition which will go to the state. */
		out << TABS(level) << "goto tr" << trans->id << ";";
	}
	else {
		/* Go directly to the target state. */
		out << TABS(level) << "goto st" << trans->targ->id << ";";
	}
	return out;
}

std::ostream &IpGotoCodeGen::EXIT_STATES()
{
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		if ( st->outNeeded ) {
			outLabelUsed = true;
			out << "	_out" << st->id << ": " << CS() << " = " << 
					st->id << "; goto _out; \n";
		}
	}
	return out;
}

std::ostream &IpGotoCodeGen::AGAIN_CASES()
{
	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		out << 
			"		case " << st->id << ": goto st" << st->id << ";\n";
	}
	return out;
}

std::ostream &IpGotoCodeGen::FINISH_CASES()
{
	bool anyWritten = false;

	for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ ) {
		if ( st->eofAction != 0 ) {
			if ( st->eofAction->eofRefs == 0 )
				st->eofAction->eofRefs = new IntSet;
			st->eofAction->eofRefs->insert( st->id );
		}
	}

	for ( ActionTableMap::Iter act = redFsm->actionMap; act.lte(); act++ ) {
		if ( act->eofRefs != 0 ) {
			for ( IntSet::Iter pst = *act->eofRefs; pst.lte(); pst++ )
				out << "	case " << *pst << ": \n";

			/* Remember that we wrote a trans so we know to write the
			 * line directive for going back to the output. */
			anyWritten = true;

			/* Write each action in the eof action list. */
			for ( ActionTable::Iter item = act->key; item.lte(); item++ )
				ACTION( out, item->value, STATE_ERR_STATE, true );
			out << "\tbreak;\n";
		}
	}

	if ( anyWritten )
		genLineDirective( out );
	return out;
}

void IpGotoCodeGen::setLabelsNeeded( InlineList *inlineList )
{
	for ( InlineList::Iter item = *inlineList; item.lte(); item++ ) {
		switch ( item->type ) {
		case InlineItem::Goto: case InlineItem::Call: {
			/* Mark the target as needing a label. */
			item->targState->labelNeeded = true;
			break;
		}
		default: break;
		}

		if ( item->children != 0 )
			setLabelsNeeded( item->children );
	}
}

/* Set up labelNeeded flag for each state. */
void IpGotoCodeGen::setLabelsNeeded()
{
	/* If we use the _again label, then we the _again switch, which uses all
	 * labels. */
	if ( useAgainLabel() ) {
		for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ )
			st->labelNeeded = true;
	}
	else {
		/* Do not use all labels by default, init all labelNeeded vars to false. */
		for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ )
			st->labelNeeded = false;

		if ( redFsm->errState != 0 && redFsm->anyLmSwitchError() )
			redFsm->errState->labelNeeded = true;

		/* Walk all transitions and set only those that have targs. */
		for ( TransApSet::Iter trans = redFsm->transSet; trans.lte(); trans++ ) {
			/* If there is no action with a next statement, then the label will be
			 * needed. */
			if ( trans->action == 0 || !trans->action->anyNextStmt() )
				trans->targ->labelNeeded = true;

			/* Need labels for states that have goto or calls in action code
			 * invoked on characters (ie, not from out action code). */
			if ( trans->action != 0 ) {
				/* Loop the actions. */
				for ( ActionTable::Iter act = trans->action->key; act.lte(); act++ ) {
					/* Get the action and walk it's tree. */
					setLabelsNeeded( act->value->inlineList );
				}
			}
		}
	}

	if ( hasEnd ) {
		for ( RedStateList::Iter st = redFsm->stateList; st.lte(); st++ )
			st->outNeeded = st->labelNeeded;
	}
	else {
		if ( redFsm->errState != 0 )
			redFsm->errState->outNeeded = true;

		for ( TransApSet::Iter trans = redFsm->transSet; trans.lte(); trans++ ) {
			/* Any state with a transition in that has a break will need an
			 * out label. */
			if ( trans->action != 0 && trans->action->anyBreakStmt() )
				trans->targ->outNeeded = true;
		}
	}
}

void IpGotoCodeGen::writeData()
{
	STATE_IDS();
}

void IpGotoCodeGen::writeExec()
{
	/* Must set labels immediately before writing because we may depend on the
	 * noend write option. */
	setLabelsNeeded();
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

	if ( useAgainLabel() ) {
		out << 
			"	goto _resume;\n"
			"\n"
			"_again:\n"
			"	switch ( " << CS() << " ) {\n";
			AGAIN_CASES() <<
			"	default: break;\n"
			"	}\n"
			"\n";

		if ( hasEnd ) {
			outLabelUsed = true;
			out << 
				"	if ( ++" << P() << " == " << PE() << " )\n"
				"		goto _out;\n";
		}
		else {
			out << 
				"	" << P() << " += 1;\n";
		}

		out << "_resume:\n";
	}

	out << 
		"	switch ( " << CS() << " )\n	{\n";
		STATE_GOTOS();
		SWITCH_DEFAULT() <<
		"	}\n";
		EXIT_STATES() << 
		"\n";

	if ( outLabelUsed ) 
		out << "	_out: {}\n";

	out <<
		"	}\n";
}

void IpGotoCodeGen::writeEOF()
{
	if ( redFsm->anyEofActions() ) {
		out <<
			"	{\n"
			"	switch ( " << CS() << " ) {\n";
			FINISH_CASES();
			SWITCH_DEFAULT() <<
			"	}\n"
			"	}\n"
			"\n";
	}
}
