/*-------------------------------------------------------------------------
 *
 * nodeModifyTable.c
 *	  routines to handle ModifyTable nodes.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeModifyTable.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitModifyTable - initialize the ModifyTable node
 *		ExecModifyTable		- retrieve the next tuple from the node
 *		ExecEndModifyTable	- shut down the ModifyTable node
 *		ExecReScanModifyTable - rescan the ModifyTable node
 *
 *	 NOTES
 *		The ModifyTable node receives input from its outerPlan, which is
 *		the data to insert for INSERT cases, or the changed columns' new
 *		values plus row-locating info for UPDATE cases, or just the
 *		row-locating info for DELETE cases.
 *
 *		If the query specifies RETURNING, then the ModifyTable returns a
 *		RETURNING tuple after completing each row insert, update, or delete.
 *		It must be called again to continue the operation.  Without RETURNING,
 *		we just loop within the node until all the work is done, then
 *		return NULL.  This avoids useless call/return overhead.
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "rewrite/rewriteHandler.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"


typedef struct MTTargetRelLookup
{
	Oid			relationOid;	/* hash key, must be first */
	int			relationIndex;	/* rel's index in resultRelInfo[] array */
} MTTargetRelLookup;

static void ExecBatchInsert(ModifyTableState *mtstate,
							ResultRelInfo *resultRelInfo,
							TupleTableSlot **slots,
							TupleTableSlot **planSlots,
							int numSlots,
							EState *estate,
							bool canSetTag);
static bool ExecOnConflictUpdate(ModifyTableState *mtstate,
								 ResultRelInfo *resultRelInfo,
								 ItemPointer conflictTid,
								 TupleTableSlot *planSlot,
								 TupleTableSlot *excludedSlot,
								 EState *estate,
								 bool canSetTag,
								 TupleTableSlot **returning);
static TupleTableSlot *ExecPrepareTupleRouting(ModifyTableState *mtstate,
											   EState *estate,
											   PartitionTupleRouting *proute,
											   ResultRelInfo *targetRelInfo,
											   TupleTableSlot *slot,
											   ResultRelInfo **partRelInfo);

/*
 * Verify that the tuples to be produced by INSERT match the
 * target relation's rowtype
 *
 * We do this to guard against stale plans.  If plan invalidation is
 * functioning properly then we should never get a failure here, but better
 * safe than sorry.  Note that this is called after we have obtained lock
 * on the target rel, so the rowtype can't change underneath us.
 *
 * The plan output is represented by its targetlist, because that makes
 * handling the dropped-column case easier.
 *
 * We used to use this for UPDATE as well, but now the equivalent checks
 * are done in ExecBuildUpdateProjection.
 */
static void
ExecCheckPlanOutput(Relation resultRel, List *targetList)
{
	TupleDesc	resultDesc = RelationGetDescr(resultRel);
	int			attno = 0;
	ListCell   *lc;

	foreach(lc, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr;

		Assert(!tle->resjunk);	/* caller removed junk items already */

		if (attno >= resultDesc->natts)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("table row type and query-specified row type do not match"),
					 errdetail("Query has too many columns.")));
		attr = TupleDescAttr(resultDesc, attno);
		attno++;

		if (!attr->attisdropped)
		{
			/* Normal case: demand type match */
			if (exprType((Node *) tle->expr) != attr->atttypid)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Table has type %s at ordinal position %d, but query expects %s.",
								   format_type_be(attr->atttypid),
								   attno,
								   format_type_be(exprType((Node *) tle->expr)))));
		}
		else
		{
			/*
			 * For a dropped column, we can't check atttypid (it's likely 0).
			 * In any case the planner has most likely inserted an INT4 null.
			 * What we insist on is just *some* NULL constant.
			 */
			if (!IsA(tle->expr, Const) ||
				!((Const *) tle->expr)->constisnull)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Query provides a value for a dropped column at ordinal position %d.",
								   attno)));
		}
	}
	if (attno != resultDesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("table row type and query-specified row type do not match"),
				 errdetail("Query has too few columns.")));
}

/*
 * ExecProcessReturning --- evaluate a RETURNING list
 *
 * resultRelInfo: current result rel
 * tupleSlot: slot holding tuple actually inserted/updated/deleted
 * planSlot: slot holding tuple returned by top subplan node
 *
 * Note: If tupleSlot is NULL, the FDW should have already provided econtext's
 * scan tuple.
 *
 * Returns a slot holding the result tuple
 */
static TupleTableSlot *
ExecProcessReturning(ResultRelInfo *resultRelInfo,
					 TupleTableSlot *tupleSlot,
					 TupleTableSlot *planSlot)
{
	ProjectionInfo *projectReturning = resultRelInfo->ri_projectReturning;
	ExprContext *econtext = projectReturning->pi_exprContext;

	/* Make tuple and any needed join variables available to ExecProject */
	if (tupleSlot)
		econtext->ecxt_scantuple = tupleSlot;
	econtext->ecxt_outertuple = planSlot;

	/*
	 * RETURNING expressions might reference the tableoid column, so
	 * reinitialize tts_tableOid before evaluating them.
	 */
	econtext->ecxt_scantuple->tts_tableOid =
		RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/* Compute the RETURNING expressions */
	return ExecProject(projectReturning);
}

/*
 * ExecCheckTupleVisible -- verify tuple is visible
 *
 * It would not be consistent with guarantees of the higher isolation levels to
 * proceed with avoiding insertion (taking speculative insertion's alternative
 * path) on the basis of another tuple that is not visible to MVCC snapshot.
 * Check for the need to raise a serialization failure, and do so as necessary.
 */
static void
ExecCheckTupleVisible(EState *estate,
					  Relation rel,
					  TupleTableSlot *slot)
{
	if (!IsolationUsesXactSnapshot())
		return;

	if (!table_tuple_satisfies_snapshot(rel, slot, estate->es_snapshot))
	{
		Datum		xminDatum;
		TransactionId xmin;
		bool		isnull;

		xminDatum = slot_getsysattr(slot, MinTransactionIdAttributeNumber, &isnull);
		Assert(!isnull);
		xmin = DatumGetTransactionId(xminDatum);

		/*
		 * We should not raise a serialization failure if the conflict is
		 * against a tuple inserted by our own transaction, even if it's not
		 * visible to our snapshot.  (This would happen, for example, if
		 * conflicting keys are proposed for insertion in a single command.)
		 */
		if (!TransactionIdIsCurrentTransactionId(xmin))
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));
	}
}

/*
 * ExecCheckTIDVisible -- convenience variant of ExecCheckTupleVisible()
 */
static void
ExecCheckTIDVisible(EState *estate,
					ResultRelInfo *relinfo,
					ItemPointer tid,
					TupleTableSlot *tempSlot)
{
	Relation	rel = relinfo->ri_RelationDesc;

	/* Redundantly check isolation level */
	if (!IsolationUsesXactSnapshot())
		return;

	if (!table_tuple_fetch_row_version(rel, tid, SnapshotAny, tempSlot))
		elog(ERROR, "failed to fetch conflicting tuple for ON CONFLICT");
	ExecCheckTupleVisible(estate, rel, tempSlot);
	ExecClearTuple(tempSlot);
}

/*
 * Compute stored generated columns for a tuple
 */
void
ExecComputeStoredGenerated(ResultRelInfo *resultRelInfo,
						   EState *estate, TupleTableSlot *slot,
						   CmdType cmdtype)
{
	Relation	rel = resultRelInfo->ri_RelationDesc;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			natts = tupdesc->natts;
	MemoryContext oldContext;
	Datum	   *values;
	bool	   *nulls;

	Assert(tupdesc->constr && tupdesc->constr->has_generated_stored);

	/*
	 * If first time through for this result relation, build expression
	 * nodetrees for rel's stored generation expressions.  Keep them in the
	 * per-query memory context so they'll survive throughout the query.
	 */
	if (resultRelInfo->ri_GeneratedExprs == NULL)
	{
		oldContext = MemoryContextSwitchTo(estate->es_query_cxt);

		resultRelInfo->ri_GeneratedExprs =
			(ExprState **) palloc(natts * sizeof(ExprState *));
		resultRelInfo->ri_NumGeneratedNeeded = 0;

		for (int i = 0; i < natts; i++)
		{
			if (TupleDescAttr(tupdesc, i)->attgenerated == ATTRIBUTE_GENERATED_STORED)
			{
				Expr	   *expr;

				/*
				 * If it's an update and the current column was not marked as
				 * being updated, then we can skip the computation.  But if
				 * there is a BEFORE ROW UPDATE trigger, we cannot skip
				 * because the trigger might affect additional columns.
				 */
				if (cmdtype == CMD_UPDATE &&
					!(rel->trigdesc && rel->trigdesc->trig_update_before_row) &&
					!bms_is_member(i + 1 - FirstLowInvalidHeapAttributeNumber,
								   ExecGetExtraUpdatedCols(resultRelInfo, estate)))
				{
					resultRelInfo->ri_GeneratedExprs[i] = NULL;
					continue;
				}

				expr = (Expr *) build_column_default(rel, i + 1);
				if (expr == NULL)
					elog(ERROR, "no generation expression found for column number %d of table \"%s\"",
						 i + 1, RelationGetRelationName(rel));

				resultRelInfo->ri_GeneratedExprs[i] = ExecPrepareExpr(expr, estate);
				resultRelInfo->ri_NumGeneratedNeeded++;
			}
		}

		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * If no generated columns have been affected by this change, then skip
	 * the rest.
	 */
	if (resultRelInfo->ri_NumGeneratedNeeded == 0)
		return;

	oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	values = palloc(sizeof(*values) * natts);
	nulls = palloc(sizeof(*nulls) * natts);

	slot_getallattrs(slot);
	memcpy(nulls, slot->tts_isnull, sizeof(*nulls) * natts);

	for (int i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attgenerated == ATTRIBUTE_GENERATED_STORED &&
			resultRelInfo->ri_GeneratedExprs[i])
		{
			ExprContext *econtext;
			Datum		val;
			bool		isnull;

			econtext = GetPerTupleExprContext(estate);
			econtext->ecxt_scantuple = slot;

			val = ExecEvalExpr(resultRelInfo->ri_GeneratedExprs[i], econtext, &isnull);

			/*
			 * We must make a copy of val as we have no guarantees about where
			 * memory for a pass-by-reference Datum is located.
			 */
			if (!isnull)
				val = datumCopy(val, attr->attbyval, attr->attlen);

			values[i] = val;
			nulls[i] = isnull;
		}
		else
		{
			if (!nulls[i])
				values[i] = datumCopy(slot->tts_values[i], attr->attbyval, attr->attlen);
		}
	}

	ExecClearTuple(slot);
	memcpy(slot->tts_values, values, sizeof(*values) * natts);
	memcpy(slot->tts_isnull, nulls, sizeof(*nulls) * natts);
	ExecStoreVirtualTuple(slot);
	ExecMaterializeSlot(slot);

	MemoryContextSwitchTo(oldContext);
}

/*
 * ExecInitInsertProjection
 *		Do one-time initialization of projection data for INSERT tuples.
 *
 * INSERT queries may need a projection to filter out junk attrs in the tlist.
 *
 * This is also a convenient place to verify that the
 * output of an INSERT matches the target table.
 */
static void
ExecInitInsertProjection(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	Plan	   *subplan = outerPlan(node);
	EState	   *estate = mtstate->ps.state;
	List	   *insertTargetList = NIL;
	bool		need_projection = false;
	ListCell   *l;

	/* Extract non-junk columns of the subplan's result tlist. */
	foreach(l, subplan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (!tle->resjunk)
			insertTargetList = lappend(insertTargetList, tle);
		else
			need_projection = true;
	}

	/*
	 * The junk-free list must produce a tuple suitable for the result
	 * relation.
	 */
	ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, insertTargetList);

	/* We'll need a slot matching the table's format. */
	resultRelInfo->ri_newTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);

	/* Build ProjectionInfo if needed (it probably isn't). */
	if (need_projection)
	{
		TupleDesc	relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

		/* need an expression context to do the projection */
		if (mtstate->ps.ps_ExprContext == NULL)
			ExecAssignExprContext(estate, &mtstate->ps);

		resultRelInfo->ri_projectNew =
			ExecBuildProjectionInfo(insertTargetList,
									mtstate->ps.ps_ExprContext,
									resultRelInfo->ri_newTupleSlot,
									&mtstate->ps,
									relDesc);
	}

	resultRelInfo->ri_projectNewInfoValid = true;
}

/*
 * ExecInitUpdateProjection
 *		Do one-time initialization of projection data for UPDATE tuples.
 *
 * UPDATE always needs a projection, because (1) there's always some junk
 * attrs, and (2) we may need to merge values of not-updated columns from
 * the old tuple into the final tuple.  In UPDATE, the tuple arriving from
 * the subplan contains only new values for the changed columns, plus row
 * identity info in the junk attrs.
 *
 * This is "one-time" for any given result rel, but we might touch more than
 * one result rel in the course of an inherited UPDATE, and each one needs
 * its own projection due to possible column order variation.
 *
 * This is also a convenient place to verify that the output of an UPDATE
 * matches the target table (ExecBuildUpdateProjection does that).
 */
static void
ExecInitUpdateProjection(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	Plan	   *subplan = outerPlan(node);
	EState	   *estate = mtstate->ps.state;
	TupleDesc	relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
	int			whichrel;
	List	   *updateColnos;

	/*
	 * Usually, mt_lastResultIndex matches the target rel.  If it happens not
	 * to, we can get the index the hard way with an integer division.
	 */
	whichrel = mtstate->mt_lastResultIndex;
	if (resultRelInfo != mtstate->resultRelInfo + whichrel)
	{
		whichrel = resultRelInfo - mtstate->resultRelInfo;
		Assert(whichrel >= 0 && whichrel < mtstate->mt_nrels);
	}

	updateColnos = (List *) list_nth(node->updateColnosLists, whichrel);

	/*
	 * For UPDATE, we use the old tuple to fill up missing values in the tuple
	 * produced by the subplan to get the new tuple.  We need two slots, both
	 * matching the table's desired format.
	 */
	resultRelInfo->ri_oldTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);
	resultRelInfo->ri_newTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);

	/* need an expression context to do the projection */
	if (mtstate->ps.ps_ExprContext == NULL)
		ExecAssignExprContext(estate, &mtstate->ps);

	resultRelInfo->ri_projectNew =
		ExecBuildUpdateProjection(subplan->targetlist,
								  false,	/* subplan did the evaluation */
								  updateColnos,
								  relDesc,
								  mtstate->ps.ps_ExprContext,
								  resultRelInfo->ri_newTupleSlot,
								  &mtstate->ps);

	resultRelInfo->ri_projectNewInfoValid = true;
}

/*
 * ExecGetInsertNewTuple
 *		This prepares a "new" tuple ready to be inserted into given result
 *		relation, by removing any junk columns of the plan's output tuple
 *		and (if necessary) coercing the tuple to the right tuple format.
 */
static TupleTableSlot *
ExecGetInsertNewTuple(ResultRelInfo *relinfo,
					  TupleTableSlot *planSlot)
{
	ProjectionInfo *newProj = relinfo->ri_projectNew;
	ExprContext *econtext;

	/*
	 * If there's no projection to be done, just make sure the slot is of the
	 * right type for the target rel.  If the planSlot is the right type we
	 * can use it as-is, else copy the data into ri_newTupleSlot.
	 */
	if (newProj == NULL)
	{
		if (relinfo->ri_newTupleSlot->tts_ops != planSlot->tts_ops)
		{
			ExecCopySlot(relinfo->ri_newTupleSlot, planSlot);
			return relinfo->ri_newTupleSlot;
		}
		else
			return planSlot;
	}

	/*
	 * Else project; since the projection output slot is ri_newTupleSlot, this
	 * will also fix any slot-type problem.
	 *
	 * Note: currently, this is dead code, because INSERT cases don't receive
	 * any junk columns so there's never a projection to be done.
	 */
	econtext = newProj->pi_exprContext;
	econtext->ecxt_outertuple = planSlot;
	return ExecProject(newProj);
}

/*
 * ExecGetUpdateNewTuple
 *		This prepares a "new" tuple by combining an UPDATE subplan's output
 *		tuple (which contains values of changed columns) with unchanged
 *		columns taken from the old tuple.
 *
 * The subplan tuple might also contain junk columns, which are ignored.
 * Note that the projection also ensures we have a slot of the right type.
 */
TupleTableSlot *
ExecGetUpdateNewTuple(ResultRelInfo *relinfo,
					  TupleTableSlot *planSlot,
					  TupleTableSlot *oldSlot)
{
	ProjectionInfo *newProj = relinfo->ri_projectNew;
	ExprContext *econtext;

	/* Use a few extra Asserts to protect against outside callers */
	Assert(relinfo->ri_projectNewInfoValid);
	Assert(planSlot != NULL && !TTS_EMPTY(planSlot));
	Assert(oldSlot != NULL && !TTS_EMPTY(oldSlot));

	econtext = newProj->pi_exprContext;
	econtext->ecxt_outertuple = planSlot;
	econtext->ecxt_scantuple = oldSlot;
	return ExecProject(newProj);
}


/* ----------------------------------------------------------------
 *		ExecInsert
 *
 *		For INSERT, we have to insert the tuple into the target relation
 *		(or partition thereof) and insert appropriate tuples into the index
 *		relations.
 *
 *		slot contains the new tuple value to be stored.
 *		planSlot is the output of the ModifyTable's subplan; we use it
 *		to access "junk" columns that are not going to be stored.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 *
 *		This may change the currently active tuple conversion map in
 *		mtstate->mt_transition_capture, so the callers must take care to
 *		save the previous value to avoid losing track of it.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecInsert(ModifyTableState *mtstate,
		   ResultRelInfo *resultRelInfo,
		   TupleTableSlot *slot,
		   TupleTableSlot *planSlot,
		   EState *estate,
		   bool canSetTag)
{
	Relation	resultRelationDesc;
	List	   *recheckIndexes = NIL;
	TupleTableSlot *result = NULL;
	TransitionCaptureState *ar_insert_trig_tcs;
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	OnConflictAction onconflict = node->onConflictAction;
	PartitionTupleRouting *proute = mtstate->mt_partition_tuple_routing;
	MemoryContext oldContext;

	/*
	 * If the input result relation is a partitioned table, find the leaf
	 * partition to insert the tuple into.
	 */
	if (proute)
	{
		ResultRelInfo *partRelInfo;

		slot = ExecPrepareTupleRouting(mtstate, estate, proute,
									   resultRelInfo, slot,
									   &partRelInfo);
		resultRelInfo = partRelInfo;
	}

	ExecMaterializeSlot(slot);

	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/*
	 * Open the table's indexes, if we have not done so already, so that we
	 * can add new index entries for the inserted tuple.
	 */
	if (resultRelationDesc->rd_rel->relhasindex &&
		resultRelInfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(resultRelInfo, onconflict != ONCONFLICT_NONE);

	/*
	 * BEFORE ROW INSERT Triggers.
	 *
	 * Note: We fire BEFORE ROW TRIGGERS for every attempted insertion in an
	 * INSERT ... ON CONFLICT statement.  We cannot check for constraint
	 * violations before firing these triggers, because they can change the
	 * values to insert.  Also, they can run arbitrary user-defined code with
	 * side-effects that we can't cancel by just not inserting the tuple.
	 */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_insert_before_row)
	{
		if (!ExecBRInsertTriggers(estate, resultRelInfo, slot))
			return NULL;		/* "do nothing" */
	}

	/* INSTEAD OF ROW INSERT Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_insert_instead_row)
	{
		if (!ExecIRInsertTriggers(estate, resultRelInfo, slot))
			return NULL;		/* "do nothing" */
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		/*
		 * GENERATED expressions might reference the tableoid column, so
		 * (re-)initialize tts_tableOid before evaluating them.
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

		/*
		 * Compute stored generated columns
		 */
		if (resultRelationDesc->rd_att->constr &&
			resultRelationDesc->rd_att->constr->has_generated_stored)
			ExecComputeStoredGenerated(resultRelInfo, estate, slot,
									   CMD_INSERT);

		/*
		 * If the FDW supports batching, and batching is requested, accumulate
		 * rows and insert them in batches. Otherwise use the per-row inserts.
		 */
		if (resultRelInfo->ri_BatchSize > 1)
		{
			/*
			 * If a certain number of tuples have already been accumulated, or
			 * a tuple has come for a different relation than that for the
			 * accumulated tuples, perform the batch insert
			 */
			if (resultRelInfo->ri_NumSlots == resultRelInfo->ri_BatchSize)
			{
				ExecBatchInsert(mtstate, resultRelInfo,
								resultRelInfo->ri_Slots,
								resultRelInfo->ri_PlanSlots,
								resultRelInfo->ri_NumSlots,
								estate, canSetTag);
				resultRelInfo->ri_NumSlots = 0;
			}

			oldContext = MemoryContextSwitchTo(estate->es_query_cxt);

			if (resultRelInfo->ri_Slots == NULL)
			{
				resultRelInfo->ri_Slots = palloc(sizeof(TupleTableSlot *) *
												 resultRelInfo->ri_BatchSize);
				resultRelInfo->ri_PlanSlots = palloc(sizeof(TupleTableSlot *) *
													 resultRelInfo->ri_BatchSize);
			}

			/*
			 * Initialize the batch slots. We don't know how many slots will
			 * be needed, so we initialize them as the batch grows, and we
			 * keep them across batches. To mitigate an inefficiency in how
			 * resource owner handles objects with many references (as with
			 * many slots all referencing the same tuple descriptor) we copy
			 * the appropriate tuple descriptor for each slot.
			 */
			if (resultRelInfo->ri_NumSlots >= resultRelInfo->ri_NumSlotsInitialized)
			{
				TupleDesc	tdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
				TupleDesc	plan_tdesc =
					CreateTupleDescCopy(planSlot->tts_tupleDescriptor);

				resultRelInfo->ri_Slots[resultRelInfo->ri_NumSlots] =
					MakeSingleTupleTableSlot(tdesc, slot->tts_ops);

				resultRelInfo->ri_PlanSlots[resultRelInfo->ri_NumSlots] =
					MakeSingleTupleTableSlot(plan_tdesc, planSlot->tts_ops);

				/* remember how many batch slots we initialized */
				resultRelInfo->ri_NumSlotsInitialized++;
			}

			ExecCopySlot(resultRelInfo->ri_Slots[resultRelInfo->ri_NumSlots],
						 slot);

			ExecCopySlot(resultRelInfo->ri_PlanSlots[resultRelInfo->ri_NumSlots],
						 planSlot);

			resultRelInfo->ri_NumSlots++;

			MemoryContextSwitchTo(oldContext);

			return NULL;
		}

		/*
		 * insert into foreign table: let the FDW do it
		 */
		slot = resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate,
															   resultRelInfo,
															   slot,
															   planSlot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/*
		 * AFTER ROW Triggers or RETURNING expressions might reference the
		 * tableoid column, so (re-)initialize tts_tableOid before evaluating
		 * them.  (This covers the case where the FDW replaced the slot.)
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
	}
	else
	{
		WCOKind		wco_kind;

		/*
		 * Constraints and GENERATED expressions might reference the tableoid
		 * column, so (re-)initialize tts_tableOid before evaluating them.
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelationDesc);

		/*
		 * Compute stored generated columns
		 */
		if (resultRelationDesc->rd_att->constr &&
			resultRelationDesc->rd_att->constr->has_generated_stored)
			ExecComputeStoredGenerated(resultRelInfo, estate, slot,
									   CMD_INSERT);

		/*
		 * Check any RLS WITH CHECK policies.
		 *
		 * Normally we should check INSERT policies. But if the insert is the
		 * result of a partition key update that moved the tuple to a new
		 * partition, we should instead check UPDATE policies, because we are
		 * executing policies defined on the target table, and not those
		 * defined on the child partitions.
		 */
		wco_kind = (mtstate->operation == CMD_UPDATE) ?
			WCO_RLS_UPDATE_CHECK : WCO_RLS_INSERT_CHECK;

		/*
		 * ExecWithCheckOptions() will skip any WCOs which are not of the kind
		 * we are looking for at this point.
		 */
		if (resultRelInfo->ri_WithCheckOptions != NIL)
			ExecWithCheckOptions(wco_kind, resultRelInfo, slot, estate);

		/*
		 * Check the constraints of the tuple.
		 */
		if (resultRelationDesc->rd_att->constr)
			ExecConstraints(resultRelInfo, slot, estate);

		/*
		 * Also check the tuple against the partition constraint, if there is
		 * one; except that if we got here via tuple-routing, we don't need to
		 * if there's no BR trigger defined on the partition.
		 */
		if (resultRelationDesc->rd_rel->relispartition &&
			(resultRelInfo->ri_RootResultRelInfo == NULL ||
			 (resultRelInfo->ri_TrigDesc &&
			  resultRelInfo->ri_TrigDesc->trig_insert_before_row)))
			ExecPartitionCheck(resultRelInfo, slot, estate, true);

		if (onconflict != ONCONFLICT_NONE && resultRelInfo->ri_NumIndices > 0)
		{
			/* Perform a speculative insertion. */
			uint32		specToken;
			ItemPointerData conflictTid;
			bool		specConflict;
			List	   *arbiterIndexes;

			arbiterIndexes = resultRelInfo->ri_onConflictArbiterIndexes;

			/*
			 * Do a non-conclusive check for conflicts first.
			 *
			 * We're not holding any locks yet, so this doesn't guarantee that
			 * the later insert won't conflict.  But it avoids leaving behind
			 * a lot of canceled speculative insertions, if you run a lot of
			 * INSERT ON CONFLICT statements that do conflict.
			 *
			 * We loop back here if we find a conflict below, either during
			 * the pre-check, or when we re-check after inserting the tuple
			 * speculatively.  Better allow interrupts in case some bug makes
			 * this an infinite loop.
			 */
	vlock:
			CHECK_FOR_INTERRUPTS();
			specConflict = false;
			if (!ExecCheckIndexConstraints(resultRelInfo, slot, estate,
										   &conflictTid, arbiterIndexes))
			{
				/* committed conflict tuple found */
				if (onconflict == ONCONFLICT_UPDATE)
				{
					/*
					 * In case of ON CONFLICT DO UPDATE, execute the UPDATE
					 * part.  Be prepared to retry if the UPDATE fails because
					 * of another concurrent UPDATE/DELETE to the conflict
					 * tuple.
					 */
					TupleTableSlot *returning = NULL;

					if (ExecOnConflictUpdate(mtstate, resultRelInfo,
											 &conflictTid, planSlot, slot,
											 estate, canSetTag, &returning))
					{
						InstrCountTuples2(&mtstate->ps, 1);
						return returning;
					}
					else
						goto vlock;
				}
				else
				{
					/*
					 * In case of ON CONFLICT DO NOTHING, do nothing. However,
					 * verify that the tuple is visible to the executor's MVCC
					 * snapshot at higher isolation levels.
					 *
					 * Using ExecGetReturningSlot() to store the tuple for the
					 * recheck isn't that pretty, but we can't trivially use
					 * the input slot, because it might not be of a compatible
					 * type. As there's no conflicting usage of
					 * ExecGetReturningSlot() in the DO NOTHING case...
					 */
					Assert(onconflict == ONCONFLICT_NOTHING);
					ExecCheckTIDVisible(estate, resultRelInfo, &conflictTid,
										ExecGetReturningSlot(estate, resultRelInfo));
					InstrCountTuples2(&mtstate->ps, 1);
					return NULL;
				}
			}

			/*
			 * Before we start insertion proper, acquire our "speculative
			 * insertion lock".  Others can use that to wait for us to decide
			 * if we're going to go ahead with the insertion, instead of
			 * waiting for the whole transaction to complete.
			 */
			specToken = SpeculativeInsertionLockAcquire(GetCurrentTransactionId());

			/* insert the tuple, with the speculative token */
			table_tuple_insert_speculative(resultRelationDesc, slot,
										   estate->es_output_cid,
										   0,
										   NULL,
										   specToken);

			/* insert index entries for tuple */
			recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
												   slot, estate, false, true,
												   &specConflict,
												   arbiterIndexes);

			/* adjust the tuple's state accordingly */
			table_tuple_complete_speculative(resultRelationDesc, slot,
											 specToken, !specConflict);

			/*
			 * Wake up anyone waiting for our decision.  They will re-check
			 * the tuple, see that it's no longer speculative, and wait on our
			 * XID as if this was a regularly inserted tuple all along.  Or if
			 * we killed the tuple, they will see it's dead, and proceed as if
			 * the tuple never existed.
			 */
			SpeculativeInsertionLockRelease(GetCurrentTransactionId());

			/*
			 * If there was a conflict, start from the beginning.  We'll do
			 * the pre-check again, which will now find the conflicting tuple
			 * (unless it aborts before we get there).
			 */
			if (specConflict)
			{
				list_free(recheckIndexes);
				goto vlock;
			}

			/* Since there was no insertion conflict, we're done */
		}
		else
		{
			/* insert the tuple normally */
			table_tuple_insert(resultRelationDesc, slot,
							   estate->es_output_cid,
							   0, NULL);

			/* insert index entries for tuple */
			if (resultRelInfo->ri_NumIndices > 0)
				recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
													   slot, estate, false,
													   false, NULL, NIL);
		}
	}

	if (canSetTag)
		(estate->es_processed)++;

	/*
	 * If this insert is the result of a partition key update that moved the
	 * tuple to a new partition, put this row into the transition NEW TABLE,
	 * if there is one. We need to do this separately for DELETE and INSERT
	 * because they happen on different tables.
	 */
	ar_insert_trig_tcs = mtstate->mt_transition_capture;
	if (mtstate->operation == CMD_UPDATE && mtstate->mt_transition_capture
		&& mtstate->mt_transition_capture->tcs_update_new_table)
	{
		ExecARUpdateTriggers(estate, resultRelInfo, NULL,
							 NULL,
							 slot,
							 NULL,
							 mtstate->mt_transition_capture);

		/*
		 * We've already captured the NEW TABLE row, so make sure any AR
		 * INSERT trigger fired below doesn't capture it again.
		 */
		ar_insert_trig_tcs = NULL;
	}

	/* AFTER ROW INSERT Triggers */
	ExecARInsertTriggers(estate, resultRelInfo, slot, recheckIndexes,
						 ar_insert_trig_tcs);

	list_free(recheckIndexes);

	/*
	 * Check any WITH CHECK OPTION constraints from parent views.  We are
	 * required to do this after testing all constraints and uniqueness
	 * violations per the SQL spec, so we do it after actually inserting the
	 * record into the heap and all indexes.
	 *
	 * ExecWithCheckOptions will elog(ERROR) if a violation is found, so the
	 * tuple will never be seen, if it violates the WITH CHECK OPTION.
	 *
	 * ExecWithCheckOptions() will skip any WCOs which are not of the kind we
	 * are looking for at this point.
	 */
	if (resultRelInfo->ri_WithCheckOptions != NIL)
		ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);

	/* Process RETURNING if present */
	if (resultRelInfo->ri_projectReturning)
		result = ExecProcessReturning(resultRelInfo, slot, planSlot);

	return result;
}

/* ----------------------------------------------------------------
 *		ExecBatchInsert
 *
 *		Insert multiple tuples in an efficient way.
 *		Currently, this handles inserting into a foreign table without
 *		RETURNING clause.
 * ----------------------------------------------------------------
 */
static void
ExecBatchInsert(ModifyTableState *mtstate,
				ResultRelInfo *resultRelInfo,
				TupleTableSlot **slots,
				TupleTableSlot **planSlots,
				int numSlots,
				EState *estate,
				bool canSetTag)
{
	int			i;
	int			numInserted = numSlots;
	TupleTableSlot *slot = NULL;
	TupleTableSlot **rslots;

	/*
	 * insert into foreign table: let the FDW do it
	 */
	rslots = resultRelInfo->ri_FdwRoutine->ExecForeignBatchInsert(estate,
																  resultRelInfo,
																  slots,
																  planSlots,
																  &numInserted);

	for (i = 0; i < numInserted; i++)
	{
		slot = rslots[i];

		/*
		 * AFTER ROW Triggers or RETURNING expressions might reference the
		 * tableoid column, so (re-)initialize tts_tableOid before evaluating
		 * them.
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

		/* AFTER ROW INSERT Triggers */
		ExecARInsertTriggers(estate, resultRelInfo, slot, NIL,
							 mtstate->mt_transition_capture);

		/*
		 * Check any WITH CHECK OPTION constraints from parent views.  See the
		 * comment in ExecInsert.
		 */
		if (resultRelInfo->ri_WithCheckOptions != NIL)
			ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);
	}

	if (canSetTag && numInserted > 0)
		estate->es_processed += numInserted;
}

/* ----------------------------------------------------------------
 *		ExecDelete
 *
 *		DELETE is like UPDATE, except that we delete the tuple and no
 *		index modifications are needed.
 *
 *		When deleting from a table, tupleid identifies the tuple to
 *		delete and oldtuple is NULL.  When deleting from a view,
 *		oldtuple is passed to the INSTEAD OF triggers and identifies
 *		what to delete, and tupleid is invalid.  When deleting from a
 *		foreign table, tupleid is invalid; the FDW has to figure out
 *		which row to delete using data from the planSlot.  oldtuple is
 *		passed to foreign table triggers; it is NULL when the foreign
 *		table has no relevant triggers.  We use tupleDeleted to indicate
 *		whether the tuple is actually deleted, callers can use it to
 *		decide whether to continue the operation.  When this DELETE is a
 *		part of an UPDATE of partition-key, then the slot returned by
 *		EvalPlanQual() is passed back using output parameter epqslot.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecDelete(ModifyTableState *mtstate,
		   ResultRelInfo *resultRelInfo,
		   ItemPointer tupleid,
		   HeapTuple oldtuple,
		   TupleTableSlot *planSlot,
		   EPQState *epqstate,
		   EState *estate,
		   bool processReturning,
		   bool canSetTag,
		   bool changingPart,
		   bool *tupleDeleted,
		   TupleTableSlot **epqreturnslot)
{
	Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;
	TM_Result	result;
	TM_FailureData tmfd;
	TupleTableSlot *slot = NULL;
	TransitionCaptureState *ar_delete_trig_tcs;

	if (tupleDeleted)
		*tupleDeleted = false;

	/* BEFORE ROW DELETE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_delete_before_row)
	{
		bool		dodelete;

		dodelete = ExecBRDeleteTriggers(estate, epqstate, resultRelInfo,
										tupleid, oldtuple, epqreturnslot);

		if (!dodelete)			/* "do nothing" */
			return NULL;
	}

	/* INSTEAD OF ROW DELETE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_delete_instead_row)
	{
		bool		dodelete;

		Assert(oldtuple != NULL);
		dodelete = ExecIRDeleteTriggers(estate, resultRelInfo, oldtuple);

		if (!dodelete)			/* "do nothing" */
			return NULL;
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		/*
		 * delete from foreign table: let the FDW do it
		 *
		 * We offer the returning slot as a place to store RETURNING data,
		 * although the FDW can return some other slot if it wants.
		 */
		slot = ExecGetReturningSlot(estate, resultRelInfo);
		slot = resultRelInfo->ri_FdwRoutine->ExecForeignDelete(estate,
															   resultRelInfo,
															   slot,
															   planSlot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/*
		 * RETURNING expressions might reference the tableoid column, so
		 * (re)initialize tts_tableOid before evaluating them.
		 */
		if (TTS_EMPTY(slot))
			ExecStoreAllNullTuple(slot);

		slot->tts_tableOid = RelationGetRelid(resultRelationDesc);
	}
	else
	{
		/*
		 * delete the tuple
		 *
		 * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check
		 * that the row to be deleted is visible to that snapshot, and throw a
		 * can't-serialize error if not. This is a special-case behavior
		 * needed for referential integrity updates in transaction-snapshot
		 * mode transactions.
		 */
ldelete:;
		result = table_tuple_delete(resultRelationDesc, tupleid,
									estate->es_output_cid,
									estate->es_snapshot,
									estate->es_crosscheck_snapshot,
									true /* wait for commit */ ,
									&tmfd,
									changingPart);

		switch (result)
		{
			case TM_SelfModified:

				/*
				 * The target tuple was already updated or deleted by the
				 * current command, or by a later command in the current
				 * transaction.  The former case is possible in a join DELETE
				 * where multiple tuples join to the same target tuple. This
				 * is somewhat questionable, but Postgres has always allowed
				 * it: we just ignore additional deletion attempts.
				 *
				 * The latter case arises if the tuple is modified by a
				 * command in a BEFORE trigger, or perhaps by a command in a
				 * volatile function used in the query.  In such situations we
				 * should not ignore the deletion, but it is equally unsafe to
				 * proceed.  We don't want to discard the original DELETE
				 * while keeping the triggered actions based on its deletion;
				 * and it would be no better to allow the original DELETE
				 * while discarding updates that it triggered.  The row update
				 * carries some information that might be important according
				 * to business rules; so throwing an error is the only safe
				 * course.
				 *
				 * If a trigger actually intends this type of interaction, it
				 * can re-execute the DELETE and then return NULL to cancel
				 * the outer delete.
				 */
				if (tmfd.cmax != estate->es_output_cid)
					ereport(ERROR,
							(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
							 errmsg("tuple to be deleted was already modified by an operation triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

				/* Else, already deleted by self; nothing to do */
				return NULL;

			case TM_Ok:
				break;

			case TM_Updated:
				{
					TupleTableSlot *inputslot;
					TupleTableSlot *epqslot;

					if (IsolationUsesXactSnapshot())
						ereport(ERROR,
								(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
								 errmsg("could not serialize access due to concurrent update")));

					/*
					 * Already know that we're going to need to do EPQ, so
					 * fetch tuple directly into the right slot.
					 */
					EvalPlanQualBegin(epqstate);
					inputslot = EvalPlanQualSlot(epqstate, resultRelationDesc,
												 resultRelInfo->ri_RangeTableIndex);

					result = table_tuple_lock(resultRelationDesc, tupleid,
											  estate->es_snapshot,
											  inputslot, estate->es_output_cid,
											  LockTupleExclusive, LockWaitBlock,
											  TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
											  &tmfd);

					switch (result)
					{
						case TM_Ok:
							Assert(tmfd.traversed);
							epqslot = EvalPlanQual(epqstate,
												   resultRelationDesc,
												   resultRelInfo->ri_RangeTableIndex,
												   inputslot);
							if (TupIsNull(epqslot))
								/* Tuple not passing quals anymore, exiting... */
								return NULL;

							/*
							 * If requested, skip delete and pass back the
							 * updated row.
							 */
							if (epqreturnslot)
							{
								*epqreturnslot = epqslot;
								return NULL;
							}
							else
								goto ldelete;

						case TM_SelfModified:

							/*
							 * This can be reached when following an update
							 * chain from a tuple updated by another session,
							 * reaching a tuple that was already updated in
							 * this transaction. If previously updated by this
							 * command, ignore the delete, otherwise error
							 * out.
							 *
							 * See also TM_SelfModified response to
							 * table_tuple_delete() above.
							 */
							if (tmfd.cmax != estate->es_output_cid)
								ereport(ERROR,
										(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
										 errmsg("tuple to be deleted was already modified by an operation triggered by the current command"),
										 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
							return NULL;

						case TM_Deleted:
							/* tuple already deleted; nothing to do */
							return NULL;

						default:

							/*
							 * TM_Invisible should be impossible because we're
							 * waiting for updated row versions, and would
							 * already have errored out if the first version
							 * is invisible.
							 *
							 * TM_Updated should be impossible, because we're
							 * locking the latest version via
							 * TUPLE_LOCK_FLAG_FIND_LAST_VERSION.
							 */
							elog(ERROR, "unexpected table_tuple_lock status: %u",
								 result);
							return NULL;
					}

					Assert(false);
					break;
				}

			case TM_Deleted:
				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent delete")));
				/* tuple already deleted; nothing to do */
				return NULL;

			default:
				elog(ERROR, "unrecognized table_tuple_delete status: %u",
					 result);
				return NULL;
		}

		/*
		 * Note: Normally one would think that we have to delete index tuples
		 * associated with the heap tuple now...
		 *
		 * ... but in POSTGRES, we have no need to do this because VACUUM will
		 * take care of it later.  We can't delete index tuples immediately
		 * anyway, since the tuple is still visible to other transactions.
		 */
	}

	if (canSetTag)
		(estate->es_processed)++;

	/* Tell caller that the delete actually happened. */
	if (tupleDeleted)
		*tupleDeleted = true;

	/*
	 * If this delete is the result of a partition key update that moved the
	 * tuple to a new partition, put this row into the transition OLD TABLE,
	 * if there is one. We need to do this separately for DELETE and INSERT
	 * because they happen on different tables.
	 */
	ar_delete_trig_tcs = mtstate->mt_transition_capture;
	if (mtstate->operation == CMD_UPDATE && mtstate->mt_transition_capture
		&& mtstate->mt_transition_capture->tcs_update_old_table)
	{
		ExecARUpdateTriggers(estate, resultRelInfo,
							 tupleid,
							 oldtuple,
							 NULL,
							 NULL,
							 mtstate->mt_transition_capture);

		/*
		 * We've already captured the NEW TABLE row, so make sure any AR
		 * DELETE trigger fired below doesn't capture it again.
		 */
		ar_delete_trig_tcs = NULL;
	}

	/* AFTER ROW DELETE Triggers */
	ExecARDeleteTriggers(estate, resultRelInfo, tupleid, oldtuple,
						 ar_delete_trig_tcs);

	/* Process RETURNING if present and if requested */
	if (processReturning && resultRelInfo->ri_projectReturning)
	{
		/*
		 * We have to put the target tuple into a slot, which means first we
		 * gotta fetch it.  We can use the trigger tuple slot.
		 */
		TupleTableSlot *rslot;

		if (resultRelInfo->ri_FdwRoutine)
		{
			/* FDW must have provided a slot containing the deleted row */
			Assert(!TupIsNull(slot));
		}
		else
		{
			slot = ExecGetReturningSlot(estate, resultRelInfo);
			if (oldtuple != NULL)
			{
				ExecForceStoreHeapTuple(oldtuple, slot, false);
			}
			else
			{
				if (!table_tuple_fetch_row_version(resultRelationDesc, tupleid,
												   SnapshotAny, slot))
					elog(ERROR, "failed to fetch deleted tuple for DELETE RETURNING");
			}
		}

		rslot = ExecProcessReturning(resultRelInfo, slot, planSlot);

		/*
		 * Before releasing the target tuple again, make sure rslot has a
		 * local copy of any pass-by-reference values.
		 */
		ExecMaterializeSlot(rslot);

		ExecClearTuple(slot);

		return rslot;
	}

	return NULL;
}

/*
 * ExecCrossPartitionUpdate --- Move an updated tuple to another partition.
 *
 * This works by first deleting the old tuple from the current partition,
 * followed by inserting the new tuple into the root parent table, that is,
 * mtstate->rootResultRelInfo.  It will be re-routed from there to the
 * correct partition.
 *
 * Returns true if the tuple has been successfully moved, or if it's found
 * that the tuple was concurrently deleted so there's nothing more to do
 * for the caller.
 *
 * False is returned if the tuple we're trying to move is found to have been
 * concurrently updated.  In that case, the caller must to check if the
 * updated tuple that's returned in *retry_slot still needs to be re-routed,
 * and call this function again or perform a regular update accordingly.
 */
static bool
ExecCrossPartitionUpdate(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo,
						 ItemPointer tupleid, HeapTuple oldtuple,
						 TupleTableSlot *slot, TupleTableSlot *planSlot,
						 EPQState *epqstate, bool canSetTag,
						 TupleTableSlot **retry_slot,
						 TupleTableSlot **inserted_tuple)
{
	EState	   *estate = mtstate->ps.state;
	TupleConversionMap *tupconv_map;
	bool		tuple_deleted;
	TupleTableSlot *epqslot = NULL;

	*inserted_tuple = NULL;
	*retry_slot = NULL;

	/*
	 * Disallow an INSERT ON CONFLICT DO UPDATE that causes the original row
	 * to migrate to a different partition.  Maybe this can be implemented
	 * some day, but it seems a fringe feature with little redeeming value.
	 */
	if (((ModifyTable *) mtstate->ps.plan)->onConflictAction == ONCONFLICT_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid ON UPDATE specification"),
				 errdetail("The result tuple would appear in a different partition than the original tuple.")));

	/*
	 * When an UPDATE is run directly on a leaf partition, simply fail with a
	 * partition constraint violation error.
	 */
	if (resultRelInfo == mtstate->rootResultRelInfo)
		ExecPartitionCheckEmitError(resultRelInfo, slot, estate);

	/* Initialize tuple routing info if not already done. */
	if (mtstate->mt_partition_tuple_routing == NULL)
	{
		Relation	rootRel = mtstate->rootResultRelInfo->ri_RelationDesc;
		MemoryContext oldcxt;

		/* Things built here have to last for the query duration. */
		oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

		mtstate->mt_partition_tuple_routing =
			ExecSetupPartitionTupleRouting(estate, rootRel);

		/*
		 * Before a partition's tuple can be re-routed, it must first be
		 * converted to the root's format, so we'll need a slot for storing
		 * such tuples.
		 */
		Assert(mtstate->mt_root_tuple_slot == NULL);
		mtstate->mt_root_tuple_slot = table_slot_create(rootRel, NULL);

		MemoryContextSwitchTo(oldcxt);
	}

	/*
	 * Row movement, part 1.  Delete the tuple, but skip RETURNING processing.
	 * We want to return rows from INSERT.
	 */
	ExecDelete(mtstate, resultRelInfo, tupleid, oldtuple, planSlot,
			   epqstate, estate,
			   false,			/* processReturning */
			   false,			/* canSetTag */
			   true,			/* changingPart */
			   &tuple_deleted, &epqslot);

	/*
	 * For some reason if DELETE didn't happen (e.g. trigger prevented it, or
	 * it was already deleted by self, or it was concurrently deleted by
	 * another transaction), then we should skip the insert as well;
	 * otherwise, an UPDATE could cause an increase in the total number of
	 * rows across all partitions, which is clearly wrong.
	 *
	 * For a normal UPDATE, the case where the tuple has been the subject of a
	 * concurrent UPDATE or DELETE would be handled by the EvalPlanQual
	 * machinery, but for an UPDATE that we've translated into a DELETE from
	 * this partition and an INSERT into some other partition, that's not
	 * available, because CTID chains can't span relation boundaries.  We
	 * mimic the semantics to a limited extent by skipping the INSERT if the
	 * DELETE fails to find a tuple.  This ensures that two concurrent
	 * attempts to UPDATE the same tuple at the same time can't turn one tuple
	 * into two, and that an UPDATE of a just-deleted tuple can't resurrect
	 * it.
	 */
	if (!tuple_deleted)
	{
		/*
		 * epqslot will be typically NULL.  But when ExecDelete() finds that
		 * another transaction has concurrently updated the same row, it
		 * re-fetches the row, skips the delete, and epqslot is set to the
		 * re-fetched tuple slot.  In that case, we need to do all the checks
		 * again.
		 */
		if (TupIsNull(epqslot))
			return true;
		else
		{
			/* Fetch the most recent version of old tuple. */
			TupleTableSlot *oldSlot;

			/* ... but first, make sure ri_oldTupleSlot is initialized. */
			if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
				ExecInitUpdateProjection(mtstate, resultRelInfo);
			oldSlot = resultRelInfo->ri_oldTupleSlot;
			if (!table_tuple_fetch_row_version(resultRelInfo->ri_RelationDesc,
											   tupleid,
											   SnapshotAny,
											   oldSlot))
				elog(ERROR, "failed to fetch tuple being updated");
			*retry_slot = ExecGetUpdateNewTuple(resultRelInfo, epqslot,
												oldSlot);
			return false;
		}
	}

	/*
	 * resultRelInfo is one of the per-relation resultRelInfos.  So we should
	 * convert the tuple into root's tuple descriptor if needed, since
	 * ExecInsert() starts the search from root.
	 */
	tupconv_map = ExecGetChildToRootMap(resultRelInfo);
	if (tupconv_map != NULL)
		slot = execute_attr_map_slot(tupconv_map->attrMap,
									 slot,
									 mtstate->mt_root_tuple_slot);

	/* Tuple routing starts from the root table. */
	*inserted_tuple = ExecInsert(mtstate, mtstate->rootResultRelInfo, slot,
								 planSlot, estate, canSetTag);

	/*
	 * Reset the transition state that may possibly have been written by
	 * INSERT.
	 */
	if (mtstate->mt_transition_capture)
		mtstate->mt_transition_capture->tcs_original_insert_tuple = NULL;

	/* We're done moving. */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecUpdate
 *
 *		note: we can't run UPDATE queries with transactions
 *		off because UPDATEs are actually INSERTs and our
 *		scan will mistakenly loop forever, updating the tuple
 *		it just inserted..  This should be fixed but until it
 *		is, we don't want to get stuck in an infinite loop
 *		which corrupts your database..
 *
 *		When updating a table, tupleid identifies the tuple to
 *		update and oldtuple is NULL.  When updating a view, oldtuple
 *		is passed to the INSTEAD OF triggers and identifies what to
 *		update, and tupleid is invalid.  When updating a foreign table,
 *		tupleid is invalid; the FDW has to figure out which row to
 *		update using data from the planSlot.  oldtuple is passed to
 *		foreign table triggers; it is NULL when the foreign table has
 *		no relevant triggers.
 *
 *		slot contains the new tuple value to be stored.
 *		planSlot is the output of the ModifyTable's subplan; we use it
 *		to access values from other input tables (for RETURNING),
 *		row-ID junk columns, etc.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecUpdate(ModifyTableState *mtstate,
		   ResultRelInfo *resultRelInfo,
		   ItemPointer tupleid,
		   HeapTuple oldtuple,
		   TupleTableSlot *slot,
		   TupleTableSlot *planSlot,
		   EPQState *epqstate,
		   EState *estate,
		   bool canSetTag)
{
	Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;
	TM_Result	result;
	TM_FailureData tmfd;
	List	   *recheckIndexes = NIL;

	/*
	 * abort the operation if not running transactions
	 */
	if (IsBootstrapProcessingMode())
		elog(ERROR, "cannot UPDATE during bootstrap");

	ExecMaterializeSlot(slot);

	/*
	 * Open the table's indexes, if we have not done so already, so that we
	 * can add new index entries for the updated tuple.
	 */
	if (resultRelationDesc->rd_rel->relhasindex &&
		resultRelInfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(resultRelInfo, false);

	/* BEFORE ROW UPDATE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_before_row)
	{
		if (!ExecBRUpdateTriggers(estate, epqstate, resultRelInfo,
								  tupleid, oldtuple, slot))
			return NULL;		/* "do nothing" */
	}

	/* INSTEAD OF ROW UPDATE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_instead_row)
	{
		if (!ExecIRUpdateTriggers(estate, resultRelInfo,
								  oldtuple, slot))
			return NULL;		/* "do nothing" */
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		/*
		 * GENERATED expressions might reference the tableoid column, so
		 * (re-)initialize tts_tableOid before evaluating them.
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

		/*
		 * Compute stored generated columns
		 */
		if (resultRelationDesc->rd_att->constr &&
			resultRelationDesc->rd_att->constr->has_generated_stored)
			ExecComputeStoredGenerated(resultRelInfo, estate, slot,
									   CMD_UPDATE);

		/*
		 * update in foreign table: let the FDW do it
		 */
		slot = resultRelInfo->ri_FdwRoutine->ExecForeignUpdate(estate,
															   resultRelInfo,
															   slot,
															   planSlot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/*
		 * AFTER ROW Triggers or RETURNING expressions might reference the
		 * tableoid column, so (re-)initialize tts_tableOid before evaluating
		 * them.  (This covers the case where the FDW replaced the slot.)
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelationDesc);
	}
	else
	{
		LockTupleMode lockmode;
		bool		partition_constraint_failed;
		bool		update_indexes;

		/*
		 * Constraints and GENERATED expressions might reference the tableoid
		 * column, so (re-)initialize tts_tableOid before evaluating them.
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelationDesc);

		/*
		 * Compute stored generated columns
		 */
		if (resultRelationDesc->rd_att->constr &&
			resultRelationDesc->rd_att->constr->has_generated_stored)
			ExecComputeStoredGenerated(resultRelInfo, estate, slot,
									   CMD_UPDATE);

		/*
		 * Check any RLS UPDATE WITH CHECK policies
		 *
		 * If we generate a new candidate tuple after EvalPlanQual testing, we
		 * must loop back here and recheck any RLS policies and constraints.
		 * (We don't need to redo triggers, however.  If there are any BEFORE
		 * triggers then trigger.c will have done table_tuple_lock to lock the
		 * correct tuple, so there's no need to do them again.)
		 */
lreplace:;

		/* ensure slot is independent, consider e.g. EPQ */
		ExecMaterializeSlot(slot);

		/*
		 * If partition constraint fails, this row might get moved to another
		 * partition, in which case we should check the RLS CHECK policy just
		 * before inserting into the new partition, rather than doing it here.
		 * This is because a trigger on that partition might again change the
		 * row.  So skip the WCO checks if the partition constraint fails.
		 */
		partition_constraint_failed =
			resultRelationDesc->rd_rel->relispartition &&
			!ExecPartitionCheck(resultRelInfo, slot, estate, false);

		if (!partition_constraint_failed &&
			resultRelInfo->ri_WithCheckOptions != NIL)
		{
			/*
			 * ExecWithCheckOptions() will skip any WCOs which are not of the
			 * kind we are looking for at this point.
			 */
			ExecWithCheckOptions(WCO_RLS_UPDATE_CHECK,
								 resultRelInfo, slot, estate);
		}

		/*
		 * If a partition check failed, try to move the row into the right
		 * partition.
		 */
		if (partition_constraint_failed)
		{
			TupleTableSlot *inserted_tuple,
					   *retry_slot;
			bool		retry;

			/*
			 * ExecCrossPartitionUpdate will first DELETE the row from the
			 * partition it's currently in and then insert it back into the
			 * root table, which will re-route it to the correct partition.
			 * The first part may have to be repeated if it is detected that
			 * the tuple we're trying to move has been concurrently updated.
			 */
			retry = !ExecCrossPartitionUpdate(mtstate, resultRelInfo, tupleid,
											  oldtuple, slot, planSlot,
											  epqstate, canSetTag,
											  &retry_slot, &inserted_tuple);
			if (retry)
			{
				slot = retry_slot;
				goto lreplace;
			}

			return inserted_tuple;
		}

		/*
		 * Check the constraints of the tuple.  We've already checked the
		 * partition constraint above; however, we must still ensure the tuple
		 * passes all other constraints, so we will call ExecConstraints() and
		 * have it validate all remaining checks.
		 */
		if (resultRelationDesc->rd_att->constr)
			ExecConstraints(resultRelInfo, slot, estate);

		/*
		 * replace the heap tuple
		 *
		 * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check
		 * that the row to be updated is visible to that snapshot, and throw a
		 * can't-serialize error if not. This is a special-case behavior
		 * needed for referential integrity updates in transaction-snapshot
		 * mode transactions.
		 */
		result = table_tuple_update(resultRelationDesc, tupleid, slot,
									estate->es_output_cid,
									estate->es_snapshot,
									estate->es_crosscheck_snapshot,
									true /* wait for commit */ ,
									&tmfd, &lockmode, &update_indexes);

		switch (result)
		{
			case TM_SelfModified:

				/*
				 * The target tuple was already updated or deleted by the
				 * current command, or by a later command in the current
				 * transaction.  The former case is possible in a join UPDATE
				 * where multiple tuples join to the same target tuple. This
				 * is pretty questionable, but Postgres has always allowed it:
				 * we just execute the first update action and ignore
				 * additional update attempts.
				 *
				 * The latter case arises if the tuple is modified by a
				 * command in a BEFORE trigger, or perhaps by a command in a
				 * volatile function used in the query.  In such situations we
				 * should not ignore the update, but it is equally unsafe to
				 * proceed.  We don't want to discard the original UPDATE
				 * while keeping the triggered actions based on it; and we
				 * have no principled way to merge this update with the
				 * previous ones.  So throwing an error is the only safe
				 * course.
				 *
				 * If a trigger actually intends this type of interaction, it
				 * can re-execute the UPDATE (assuming it can figure out how)
				 * and then return NULL to cancel the outer update.
				 */
				if (tmfd.cmax != estate->es_output_cid)
					ereport(ERROR,
							(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
							 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

				/* Else, already updated by self; nothing to do */
				return NULL;

			case TM_Ok:
				break;

			case TM_Updated:
				{
					TupleTableSlot *inputslot;
					TupleTableSlot *epqslot;
					TupleTableSlot *oldSlot;

					if (IsolationUsesXactSnapshot())
						ereport(ERROR,
								(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
								 errmsg("could not serialize access due to concurrent update")));

					/*
					 * Already know that we're going to need to do EPQ, so
					 * fetch tuple directly into the right slot.
					 */
					inputslot = EvalPlanQualSlot(epqstate, resultRelationDesc,
												 resultRelInfo->ri_RangeTableIndex);

					result = table_tuple_lock(resultRelationDesc, tupleid,
											  estate->es_snapshot,
											  inputslot, estate->es_output_cid,
											  lockmode, LockWaitBlock,
											  TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
											  &tmfd);

					switch (result)
					{
						case TM_Ok:
							Assert(tmfd.traversed);

							epqslot = EvalPlanQual(epqstate,
												   resultRelationDesc,
												   resultRelInfo->ri_RangeTableIndex,
												   inputslot);
							if (TupIsNull(epqslot))
								/* Tuple not passing quals anymore, exiting... */
								return NULL;

							/* Make sure ri_oldTupleSlot is initialized. */
							if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
								ExecInitUpdateProjection(mtstate, resultRelInfo);

							/* Fetch the most recent version of old tuple. */
							oldSlot = resultRelInfo->ri_oldTupleSlot;
							if (!table_tuple_fetch_row_version(resultRelationDesc,
															   tupleid,
															   SnapshotAny,
															   oldSlot))
								elog(ERROR, "failed to fetch tuple being updated");
							slot = ExecGetUpdateNewTuple(resultRelInfo,
														 epqslot, oldSlot);
							goto lreplace;

						case TM_Deleted:
							/* tuple already deleted; nothing to do */
							return NULL;

						case TM_SelfModified:

							/*
							 * This can be reached when following an update
							 * chain from a tuple updated by another session,
							 * reaching a tuple that was already updated in
							 * this transaction. If previously modified by
							 * this command, ignore the redundant update,
							 * otherwise error out.
							 *
							 * See also TM_SelfModified response to
							 * table_tuple_update() above.
							 */
							if (tmfd.cmax != estate->es_output_cid)
								ereport(ERROR,
										(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
										 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
										 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
							return NULL;

						default:
							/* see table_tuple_lock call in ExecDelete() */
							elog(ERROR, "unexpected table_tuple_lock status: %u",
								 result);
							return NULL;
					}
				}

				break;

			case TM_Deleted:
				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent delete")));
				/* tuple already deleted; nothing to do */
				return NULL;

			default:
				elog(ERROR, "unrecognized table_tuple_update status: %u",
					 result);
				return NULL;
		}

		/* insert index entries for tuple if necessary */
		if (resultRelInfo->ri_NumIndices > 0 && update_indexes)
			recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
												   slot, estate, true, false,
												   NULL, NIL);
	}

	if (canSetTag)
		(estate->es_processed)++;

	/* AFTER ROW UPDATE Triggers */
	ExecARUpdateTriggers(estate, resultRelInfo, tupleid, oldtuple, slot,
						 recheckIndexes,
						 mtstate->operation == CMD_INSERT ?
						 mtstate->mt_oc_transition_capture :
						 mtstate->mt_transition_capture);

	list_free(recheckIndexes);

	/*
	 * Check any WITH CHECK OPTION constraints from parent views.  We are
	 * required to do this after testing all constraints and uniqueness
	 * violations per the SQL spec, so we do it after actually updating the
	 * record in the heap and all indexes.
	 *
	 * ExecWithCheckOptions() will skip any WCOs which are not of the kind we
	 * are looking for at this point.
	 */
	if (resultRelInfo->ri_WithCheckOptions != NIL)
		ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);

	/* Process RETURNING if present */
	if (resultRelInfo->ri_projectReturning)
		return ExecProcessReturning(resultRelInfo, slot, planSlot);

	return NULL;
}

/*
 * ExecOnConflictUpdate --- execute UPDATE of INSERT ON CONFLICT DO UPDATE
 *
 * Try to lock tuple for update as part of speculative insertion.  If
 * a qual originating from ON CONFLICT DO UPDATE is satisfied, update
 * (but still lock row, even though it may not satisfy estate's
 * snapshot).
 *
 * Returns true if we're done (with or without an update), or false if
 * the caller must retry the INSERT from scratch.
 */
static bool
ExecOnConflictUpdate(ModifyTableState *mtstate,
					 ResultRelInfo *resultRelInfo,
					 ItemPointer conflictTid,
					 TupleTableSlot *planSlot,
					 TupleTableSlot *excludedSlot,
					 EState *estate,
					 bool canSetTag,
					 TupleTableSlot **returning)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	Relation	relation = resultRelInfo->ri_RelationDesc;
	ExprState  *onConflictSetWhere = resultRelInfo->ri_onConflict->oc_WhereClause;
	TupleTableSlot *existing = resultRelInfo->ri_onConflict->oc_Existing;
	TM_FailureData tmfd;
	LockTupleMode lockmode;
	TM_Result	test;
	Datum		xminDatum;
	TransactionId xmin;
	bool		isnull;

	/* Determine lock mode to use */
	lockmode = ExecUpdateLockMode(estate, resultRelInfo);

	/*
	 * Lock tuple for update.  Don't follow updates when tuple cannot be
	 * locked without doing so.  A row locking conflict here means our
	 * previous conclusion that the tuple is conclusively committed is not
	 * true anymore.
	 */
	test = table_tuple_lock(relation, conflictTid,
							estate->es_snapshot,
							existing, estate->es_output_cid,
							lockmode, LockWaitBlock, 0,
							&tmfd);
	switch (test)
	{
		case TM_Ok:
			/* success! */
			break;

		case TM_Invisible:

			/*
			 * This can occur when a just inserted tuple is updated again in
			 * the same command. E.g. because multiple rows with the same
			 * conflicting key values are inserted.
			 *
			 * This is somewhat similar to the ExecUpdate() TM_SelfModified
			 * case.  We do not want to proceed because it would lead to the
			 * same row being updated a second time in some unspecified order,
			 * and in contrast to plain UPDATEs there's no historical behavior
			 * to break.
			 *
			 * It is the user's responsibility to prevent this situation from
			 * occurring.  These problems are why SQL-2003 similarly specifies
			 * that for SQL MERGE, an exception must be raised in the event of
			 * an attempt to update the same row twice.
			 */
			xminDatum = slot_getsysattr(existing,
										MinTransactionIdAttributeNumber,
										&isnull);
			Assert(!isnull);
			xmin = DatumGetTransactionId(xminDatum);

			if (TransactionIdIsCurrentTransactionId(xmin))
				ereport(ERROR,
						(errcode(ERRCODE_CARDINALITY_VIOLATION),
						 errmsg("ON CONFLICT DO UPDATE command cannot affect row a second time"),
						 errhint("Ensure that no rows proposed for insertion within the same command have duplicate constrained values.")));

			/* This shouldn't happen */
			elog(ERROR, "attempted to lock invisible tuple");
			break;

		case TM_SelfModified:

			/*
			 * This state should never be reached. As a dirty snapshot is used
			 * to find conflicting tuples, speculative insertion wouldn't have
			 * seen this row to conflict with.
			 */
			elog(ERROR, "unexpected self-updated tuple");
			break;

		case TM_Updated:
			if (IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));

			/*
			 * As long as we don't support an UPDATE of INSERT ON CONFLICT for
			 * a partitioned table we shouldn't reach to a case where tuple to
			 * be lock is moved to another partition due to concurrent update
			 * of the partition key.
			 */
			Assert(!ItemPointerIndicatesMovedPartitions(&tmfd.ctid));

			/*
			 * Tell caller to try again from the very start.
			 *
			 * It does not make sense to use the usual EvalPlanQual() style
			 * loop here, as the new version of the row might not conflict
			 * anymore, or the conflicting tuple has actually been deleted.
			 */
			ExecClearTuple(existing);
			return false;

		case TM_Deleted:
			if (IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent delete")));

			/* see TM_Updated case */
			Assert(!ItemPointerIndicatesMovedPartitions(&tmfd.ctid));
			ExecClearTuple(existing);
			return false;

		default:
			elog(ERROR, "unrecognized table_tuple_lock status: %u", test);
	}

	/* Success, the tuple is locked. */

	/*
	 * Verify that the tuple is visible to our MVCC snapshot if the current
	 * isolation level mandates that.
	 *
	 * It's not sufficient to rely on the check within ExecUpdate() as e.g.
	 * CONFLICT ... WHERE clause may prevent us from reaching that.
	 *
	 * This means we only ever continue when a new command in the current
	 * transaction could see the row, even though in READ COMMITTED mode the
	 * tuple will not be visible according to the current statement's
	 * snapshot.  This is in line with the way UPDATE deals with newer tuple
	 * versions.
	 */
	ExecCheckTupleVisible(estate, relation, existing);

	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject.  The EXCLUDED tuple is installed in ecxt_innertuple, while
	 * the target's existing tuple is installed in the scantuple.  EXCLUDED
	 * has been made to reference INNER_VAR in setrefs.c, but there is no
	 * other redirection.
	 */
	econtext->ecxt_scantuple = existing;
	econtext->ecxt_innertuple = excludedSlot;
	econtext->ecxt_outertuple = NULL;

	if (!ExecQual(onConflictSetWhere, econtext))
	{
		ExecClearTuple(existing);	/* see return below */
		InstrCountFiltered1(&mtstate->ps, 1);
		return true;			/* done with the tuple */
	}

	if (resultRelInfo->ri_WithCheckOptions != NIL)
	{
		/*
		 * Check target's existing tuple against UPDATE-applicable USING
		 * security barrier quals (if any), enforced here as RLS checks/WCOs.
		 *
		 * The rewriter creates UPDATE RLS checks/WCOs for UPDATE security
		 * quals, and stores them as WCOs of "kind" WCO_RLS_CONFLICT_CHECK,
		 * but that's almost the extent of its special handling for ON
		 * CONFLICT DO UPDATE.
		 *
		 * The rewriter will also have associated UPDATE applicable straight
		 * RLS checks/WCOs for the benefit of the ExecUpdate() call that
		 * follows.  INSERTs and UPDATEs naturally have mutually exclusive WCO
		 * kinds, so there is no danger of spurious over-enforcement in the
		 * INSERT or UPDATE path.
		 */
		ExecWithCheckOptions(WCO_RLS_CONFLICT_CHECK, resultRelInfo,
							 existing,
							 mtstate->ps.state);
	}

	/* Project the new tuple version */
	ExecProject(resultRelInfo->ri_onConflict->oc_ProjInfo);

	/*
	 * Note that it is possible that the target tuple has been modified in
	 * this session, after the above table_tuple_lock. We choose to not error
	 * out in that case, in line with ExecUpdate's treatment of similar cases.
	 * This can happen if an UPDATE is triggered from within ExecQual(),
	 * ExecWithCheckOptions() or ExecProject() above, e.g. by selecting from a
	 * wCTE in the ON CONFLICT's SET.
	 */

	/* Execute UPDATE with projection */
	*returning = ExecUpdate(mtstate, resultRelInfo, conflictTid, NULL,
							resultRelInfo->ri_onConflict->oc_ProjSlot,
							planSlot,
							&mtstate->mt_epqstate, mtstate->ps.state,
							canSetTag);

	/*
	 * Clear out existing tuple, as there might not be another conflict among
	 * the next input rows. Don't want to hold resources till the end of the
	 * query.
	 */
	ExecClearTuple(existing);
	return true;
}


/*
 * Process BEFORE EACH STATEMENT triggers
 */
static void
fireBSTriggers(ModifyTableState *node)
{
	ModifyTable *plan = (ModifyTable *) node->ps.plan;
	ResultRelInfo *resultRelInfo = node->rootResultRelInfo;

	switch (node->operation)
	{
		case CMD_INSERT:
			ExecBSInsertTriggers(node->ps.state, resultRelInfo);
			if (plan->onConflictAction == ONCONFLICT_UPDATE)
				ExecBSUpdateTriggers(node->ps.state,
									 resultRelInfo);
			break;
		case CMD_UPDATE:
			ExecBSUpdateTriggers(node->ps.state, resultRelInfo);
			break;
		case CMD_DELETE:
			ExecBSDeleteTriggers(node->ps.state, resultRelInfo);
			break;
		default:
			elog(ERROR, "unknown operation");
			break;
	}
}

/*
 * Process AFTER EACH STATEMENT triggers
 */
static void
fireASTriggers(ModifyTableState *node)
{
	ModifyTable *plan = (ModifyTable *) node->ps.plan;
	ResultRelInfo *resultRelInfo = node->rootResultRelInfo;

	switch (node->operation)
	{
		case CMD_INSERT:
			if (plan->onConflictAction == ONCONFLICT_UPDATE)
				ExecASUpdateTriggers(node->ps.state,
									 resultRelInfo,
									 node->mt_oc_transition_capture);
			ExecASInsertTriggers(node->ps.state, resultRelInfo,
								 node->mt_transition_capture);
			break;
		case CMD_UPDATE:
			ExecASUpdateTriggers(node->ps.state, resultRelInfo,
								 node->mt_transition_capture);
			break;
		case CMD_DELETE:
			ExecASDeleteTriggers(node->ps.state, resultRelInfo,
								 node->mt_transition_capture);
			break;
		default:
			elog(ERROR, "unknown operation");
			break;
	}
}

/*
 * Set up the state needed for collecting transition tuples for AFTER
 * triggers.
 */
static void
ExecSetupTransitionCaptureState(ModifyTableState *mtstate, EState *estate)
{
	ModifyTable *plan = (ModifyTable *) mtstate->ps.plan;
	ResultRelInfo *targetRelInfo = mtstate->rootResultRelInfo;

	/* Check for transition tables on the directly targeted relation. */
	mtstate->mt_transition_capture =
		MakeTransitionCaptureState(targetRelInfo->ri_TrigDesc,
								   RelationGetRelid(targetRelInfo->ri_RelationDesc),
								   mtstate->operation);
	if (plan->operation == CMD_INSERT &&
		plan->onConflictAction == ONCONFLICT_UPDATE)
		mtstate->mt_oc_transition_capture =
			MakeTransitionCaptureState(targetRelInfo->ri_TrigDesc,
									   RelationGetRelid(targetRelInfo->ri_RelationDesc),
									   CMD_UPDATE);
}

/*
 * ExecPrepareTupleRouting --- prepare for routing one tuple
 *
 * Determine the partition in which the tuple in slot is to be inserted,
 * and return its ResultRelInfo in *partRelInfo.  The return value is
 * a slot holding the tuple of the partition rowtype.
 *
 * This also sets the transition table information in mtstate based on the
 * selected partition.
 */
static TupleTableSlot *
ExecPrepareTupleRouting(ModifyTableState *mtstate,
						EState *estate,
						PartitionTupleRouting *proute,
						ResultRelInfo *targetRelInfo,
						TupleTableSlot *slot,
						ResultRelInfo **partRelInfo)
{
	ResultRelInfo *partrel;
	TupleConversionMap *map;

	/*
	 * Lookup the target partition's ResultRelInfo.  If ExecFindPartition does
	 * not find a valid partition for the tuple in 'slot' then an error is
	 * raised.  An error may also be raised if the found partition is not a
	 * valid target for INSERTs.  This is required since a partitioned table
	 * UPDATE to another partition becomes a DELETE+INSERT.
	 */
	partrel = ExecFindPartition(mtstate, targetRelInfo, proute, slot, estate);

	/*
	 * If we're capturing transition tuples, we might need to convert from the
	 * partition rowtype to root partitioned table's rowtype.  But if there
	 * are no BEFORE triggers on the partition that could change the tuple, we
	 * can just remember the original unconverted tuple to avoid a needless
	 * round trip conversion.
	 */
	if (mtstate->mt_transition_capture != NULL)
	{
		bool		has_before_insert_row_trig;

		has_before_insert_row_trig = (partrel->ri_TrigDesc &&
									  partrel->ri_TrigDesc->trig_insert_before_row);

		mtstate->mt_transition_capture->tcs_original_insert_tuple =
			!has_before_insert_row_trig ? slot : NULL;
	}

	/*
	 * Convert the tuple, if necessary.
	 */
	map = partrel->ri_RootToPartitionMap;
	if (map != NULL)
	{
		TupleTableSlot *new_slot = partrel->ri_PartitionTupleSlot;

		slot = execute_attr_map_slot(map->attrMap, slot, new_slot);
	}

	*partRelInfo = partrel;
	return slot;
}

/* ----------------------------------------------------------------
 *	   ExecModifyTable
 *
 *		Perform table modifications as required, and return RETURNING results
 *		if needed.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecModifyTable(PlanState *pstate)
{
	ModifyTableState *node = castNode(ModifyTableState, pstate);
	EState	   *estate = node->ps.state;
	CmdType		operation = node->operation;
	ResultRelInfo *resultRelInfo;
	PlanState  *subplanstate;
	TupleTableSlot *slot;
	TupleTableSlot *planSlot;
	TupleTableSlot *oldSlot;
	ItemPointer tupleid;
	ItemPointerData tuple_ctid;
	HeapTupleData oldtupdata;
	HeapTuple	oldtuple;
	PartitionTupleRouting *proute = node->mt_partition_tuple_routing;
	List	   *relinfos = NIL;
	ListCell   *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * This should NOT get called during EvalPlanQual; we should have passed a
	 * subplan tree to EvalPlanQual, instead.  Use a runtime test not just
	 * Assert because this condition is easy to miss in testing.  (Note:
	 * although ModifyTable should not get executed within an EvalPlanQual
	 * operation, we do have to allow it to be initialized and shut down in
	 * case it is within a CTE subplan.  Hence this test must be here, not in
	 * ExecInitModifyTable.)
	 */
	if (estate->es_epq_active != NULL)
		elog(ERROR, "ModifyTable should not be called during EvalPlanQual");

	/*
	 * If we've already completed processing, don't try to do more.  We need
	 * this test because ExecPostprocessPlan might call us an extra time, and
	 * our subplan's nodes aren't necessarily robust against being called
	 * extra times.
	 */
	if (node->mt_done)
		return NULL;

	/*
	 * On first call, fire BEFORE STATEMENT triggers before proceeding.
	 */
	if (node->fireBSTriggers)
	{
		fireBSTriggers(node);
		node->fireBSTriggers = false;
	}

	/* Preload local variables */
	resultRelInfo = node->resultRelInfo + node->mt_lastResultIndex;
	subplanstate = outerPlanState(node);

	/*
	 * Fetch rows from subplan, and execute the required table modification
	 * for each row.
	 */
	for (;;)
	{
		/*
		 * Reset the per-output-tuple exprcontext.  This is needed because
		 * triggers expect to use that context as workspace.  It's a bit ugly
		 * to do this below the top level of the plan, however.  We might need
		 * to rethink this later.
		 */
		ResetPerTupleExprContext(estate);

		/*
		 * Reset per-tuple memory context used for processing on conflict and
		 * returning clauses, to free any expression evaluation storage
		 * allocated in the previous cycle.
		 */
		if (pstate->ps_ExprContext)
			ResetExprContext(pstate->ps_ExprContext);

		planSlot = ExecProcNode(subplanstate);

		/* No more tuples to process? */
		if (TupIsNull(planSlot))
			break;

		/*
		 * When there are multiple result relations, each tuple contains a
		 * junk column that gives the OID of the rel from which it came.
		 * Extract it and select the correct result relation.
		 */
		if (AttributeNumberIsValid(node->mt_resultOidAttno))
		{
			Datum		datum;
			bool		isNull;
			Oid			resultoid;

			datum = ExecGetJunkAttribute(planSlot, node->mt_resultOidAttno,
										 &isNull);
			if (isNull)
				elog(ERROR, "tableoid is NULL");
			resultoid = DatumGetObjectId(datum);

			/* If it's not the same as last time, we need to locate the rel */
			if (resultoid != node->mt_lastResultOid)
				resultRelInfo = ExecLookupResultRelByOid(node, resultoid,
														 false, true);
		}

		/*
		 * If resultRelInfo->ri_usesFdwDirectModify is true, all we need to do
		 * here is compute the RETURNING expressions.
		 */
		if (resultRelInfo->ri_usesFdwDirectModify)
		{
			Assert(resultRelInfo->ri_projectReturning);

			/*
			 * A scan slot containing the data that was actually inserted,
			 * updated or deleted has already been made available to
			 * ExecProcessReturning by IterateDirectModify, so no need to
			 * provide it here.
			 */
			slot = ExecProcessReturning(resultRelInfo, NULL, planSlot);

			return slot;
		}

		EvalPlanQualSetSlot(&node->mt_epqstate, planSlot);
		slot = planSlot;

		tupleid = NULL;
		oldtuple = NULL;

		/*
		 * For UPDATE/DELETE, fetch the row identity info for the tuple to be
		 * updated/deleted.  For a heap relation, that's a TID; otherwise we
		 * may have a wholerow junk attr that carries the old tuple in toto.
		 * Keep this in step with the part of ExecInitModifyTable that sets up
		 * ri_RowIdAttNo.
		 */
		if (operation == CMD_UPDATE || operation == CMD_DELETE)
		{
			char		relkind;
			Datum		datum;
			bool		isNull;

			relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
			if (relkind == RELKIND_RELATION ||
				relkind == RELKIND_MATVIEW ||
				relkind == RELKIND_PARTITIONED_TABLE)
			{
				/* ri_RowIdAttNo refers to a ctid attribute */
				Assert(AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo));
				datum = ExecGetJunkAttribute(slot,
											 resultRelInfo->ri_RowIdAttNo,
											 &isNull);
				/* shouldn't ever get a null result... */
				if (isNull)
					elog(ERROR, "ctid is NULL");

				tupleid = (ItemPointer) DatumGetPointer(datum);
				tuple_ctid = *tupleid;	/* be sure we don't free ctid!! */
				tupleid = &tuple_ctid;
			}

			/*
			 * Use the wholerow attribute, when available, to reconstruct the
			 * old relation tuple.  The old tuple serves one or both of two
			 * purposes: 1) it serves as the OLD tuple for row triggers, 2) it
			 * provides values for any unchanged columns for the NEW tuple of
			 * an UPDATE, because the subplan does not produce all the columns
			 * of the target table.
			 *
			 * Note that the wholerow attribute does not carry system columns,
			 * so foreign table triggers miss seeing those, except that we
			 * know enough here to set t_tableOid.  Quite separately from
			 * this, the FDW may fetch its own junk attrs to identify the row.
			 *
			 * Other relevant relkinds, currently limited to views, always
			 * have a wholerow attribute.
			 */
			else if (AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
			{
				datum = ExecGetJunkAttribute(slot,
											 resultRelInfo->ri_RowIdAttNo,
											 &isNull);
				/* shouldn't ever get a null result... */
				if (isNull)
					elog(ERROR, "wholerow is NULL");

				oldtupdata.t_data = DatumGetHeapTupleHeader(datum);
				oldtupdata.t_len =
					HeapTupleHeaderGetDatumLength(oldtupdata.t_data);
				ItemPointerSetInvalid(&(oldtupdata.t_self));
				/* Historically, view triggers see invalid t_tableOid. */
				oldtupdata.t_tableOid =
					(relkind == RELKIND_VIEW) ? InvalidOid :
					RelationGetRelid(resultRelInfo->ri_RelationDesc);

				oldtuple = &oldtupdata;
			}
			else
			{
				/* Only foreign tables are allowed to omit a row-ID attr */
				Assert(relkind == RELKIND_FOREIGN_TABLE);
			}
		}

		switch (operation)
		{
			case CMD_INSERT:
				/* Initialize projection info if first time for this table */
				if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
					ExecInitInsertProjection(node, resultRelInfo);
				slot = ExecGetInsertNewTuple(resultRelInfo, planSlot);
				slot = ExecInsert(node, resultRelInfo, slot, planSlot,
								  estate, node->canSetTag);
				break;
			case CMD_UPDATE:
				/* Initialize projection info if first time for this table */
				if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
					ExecInitUpdateProjection(node, resultRelInfo);

				/*
				 * Make the new tuple by combining plan's output tuple with
				 * the old tuple being updated.
				 */
				oldSlot = resultRelInfo->ri_oldTupleSlot;
				if (oldtuple != NULL)
				{
					/* Use the wholerow junk attr as the old tuple. */
					ExecForceStoreHeapTuple(oldtuple, oldSlot, false);
				}
				else
				{
					/* Fetch the most recent version of old tuple. */
					Relation	relation = resultRelInfo->ri_RelationDesc;

					Assert(tupleid != NULL);
					if (!table_tuple_fetch_row_version(relation, tupleid,
													   SnapshotAny,
													   oldSlot))
						elog(ERROR, "failed to fetch tuple being updated");
				}
				slot = ExecGetUpdateNewTuple(resultRelInfo, planSlot,
											 oldSlot);

				/* Now apply the update. */
				slot = ExecUpdate(node, resultRelInfo, tupleid, oldtuple, slot,
								  planSlot, &node->mt_epqstate, estate,
								  node->canSetTag);
				break;
			case CMD_DELETE:
				slot = ExecDelete(node, resultRelInfo, tupleid, oldtuple,
								  planSlot, &node->mt_epqstate, estate,
								  true, /* processReturning */
								  node->canSetTag,
								  false,	/* changingPart */
								  NULL, NULL);
				break;
			default:
				elog(ERROR, "unknown operation");
				break;
		}

		/*
		 * If we got a RETURNING result, return it to caller.  We'll continue
		 * the work on next call.
		 */
		if (slot)
			return slot;
	}

	/*
	 * Insert remaining tuples for batch insert.
	 */
	if (proute)
		relinfos = estate->es_tuple_routing_result_relations;
	else
		relinfos = estate->es_opened_result_relations;

	foreach(lc, relinfos)
	{
		resultRelInfo = lfirst(lc);
		if (resultRelInfo->ri_NumSlots > 0)
			ExecBatchInsert(node, resultRelInfo,
							resultRelInfo->ri_Slots,
							resultRelInfo->ri_PlanSlots,
							resultRelInfo->ri_NumSlots,
							estate, node->canSetTag);
	}

	/*
	 * We're done, but fire AFTER STATEMENT triggers before exiting.
	 */
	fireASTriggers(node);

	node->mt_done = true;

	return NULL;
}

/*
 * ExecLookupResultRelByOid
 * 		If the table with given OID is among the result relations to be
 * 		updated by the given ModifyTable node, return its ResultRelInfo.
 *
 * If not found, return NULL if missing_ok, else raise error.
 *
 * If update_cache is true, then upon successful lookup, update the node's
 * one-element cache.  ONLY ExecModifyTable may pass true for this.
 */
ResultRelInfo *
ExecLookupResultRelByOid(ModifyTableState *node, Oid resultoid,
						 bool missing_ok, bool update_cache)
{
	if (node->mt_resultOidHash)
	{
		/* Use the pre-built hash table to locate the rel */
		MTTargetRelLookup *mtlookup;

		mtlookup = (MTTargetRelLookup *)
			hash_search(node->mt_resultOidHash, &resultoid, HASH_FIND, NULL);
		if (mtlookup)
		{
			if (update_cache)
			{
				node->mt_lastResultOid = resultoid;
				node->mt_lastResultIndex = mtlookup->relationIndex;
			}
			return node->resultRelInfo + mtlookup->relationIndex;
		}
	}
	else
	{
		/* With few target rels, just search the ResultRelInfo array */
		for (int ndx = 0; ndx < node->mt_nrels; ndx++)
		{
			ResultRelInfo *rInfo = node->resultRelInfo + ndx;

			if (RelationGetRelid(rInfo->ri_RelationDesc) == resultoid)
			{
				if (update_cache)
				{
					node->mt_lastResultOid = resultoid;
					node->mt_lastResultIndex = ndx;
				}
				return rInfo;
			}
		}
	}

	if (!missing_ok)
		elog(ERROR, "incorrect result relation OID %u", resultoid);
	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitModifyTable
 * ----------------------------------------------------------------
 */
ModifyTableState *
ExecInitModifyTable(ModifyTable *node, EState *estate, int eflags)
{
	ModifyTableState *mtstate;
	Plan	   *subplan = outerPlan(node);
	CmdType		operation = node->operation;
	int			nrels = list_length(node->resultRelations);
	ResultRelInfo *resultRelInfo;
	List	   *arowmarks;
	ListCell   *l;
	int			i;
	Relation	rel;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	mtstate = makeNode(ModifyTableState);
	mtstate->ps.plan = (Plan *) node;
	mtstate->ps.state = estate;
	mtstate->ps.ExecProcNode = ExecModifyTable;

	mtstate->operation = operation;
	mtstate->canSetTag = node->canSetTag;
	mtstate->mt_done = false;

	mtstate->mt_nrels = nrels;
	mtstate->resultRelInfo = (ResultRelInfo *)
		palloc(nrels * sizeof(ResultRelInfo));

	/*----------
	 * Resolve the target relation. This is the same as:
	 *
	 * - the relation for which we will fire FOR STATEMENT triggers,
	 * - the relation into whose tuple format all captured transition tuples
	 *   must be converted, and
	 * - the root partitioned table used for tuple routing.
	 *
	 * If it's a partitioned table, the root partition doesn't appear
	 * elsewhere in the plan and its RT index is given explicitly in
	 * node->rootRelation.  Otherwise (i.e. table inheritance) the target
	 * relation is the first relation in the node->resultRelations list.
	 *----------
	 */
	if (node->rootRelation > 0)
	{
		mtstate->rootResultRelInfo = makeNode(ResultRelInfo);
		ExecInitResultRelation(estate, mtstate->rootResultRelInfo,
							   node->rootRelation);
	}
	else
	{
		mtstate->rootResultRelInfo = mtstate->resultRelInfo;
		ExecInitResultRelation(estate, mtstate->resultRelInfo,
							   linitial_int(node->resultRelations));
	}

	/* set up epqstate with dummy subplan data for the moment */
	EvalPlanQualInit(&mtstate->mt_epqstate, estate, NULL, NIL, node->epqParam);
	mtstate->fireBSTriggers = true;

	/*
	 * Build state for collecting transition tuples.  This requires having a
	 * valid trigger query context, so skip it in explain-only mode.
	 */
	if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		ExecSetupTransitionCaptureState(mtstate, estate);

	/*
	 * Open all the result relations and initialize the ResultRelInfo structs.
	 * (But root relation was initialized above, if it's part of the array.)
	 * We must do this before initializing the subplan, because direct-modify
	 * FDWs expect their ResultRelInfos to be available.
	 */
	resultRelInfo = mtstate->resultRelInfo;
	i = 0;
	foreach(l, node->resultRelations)
	{
		Index		resultRelation = lfirst_int(l);

		if (resultRelInfo != mtstate->rootResultRelInfo)
		{
			ExecInitResultRelation(estate, resultRelInfo, resultRelation);

			/*
			 * For child result relations, store the root result relation
			 * pointer.  We do so for the convenience of places that want to
			 * look at the query's original target relation but don't have the
			 * mtstate handy.
			 */
			resultRelInfo->ri_RootResultRelInfo = mtstate->rootResultRelInfo;
		}

		/* Initialize the usesFdwDirectModify flag */
		resultRelInfo->ri_usesFdwDirectModify = bms_is_member(i,
															  node->fdwDirectModifyPlans);

		/*
		 * Verify result relation is a valid target for the current operation
		 */
		CheckValidResultRel(resultRelInfo, operation);

		resultRelInfo++;
		i++;
	}

	/*
	 * Now we may initialize the subplan.
	 */
	outerPlanState(mtstate) = ExecInitNode(subplan, estate, eflags);

	/*
	 * Do additional per-result-relation initialization.
	 */
	for (i = 0; i < nrels; i++)
	{
		resultRelInfo = &mtstate->resultRelInfo[i];

		/* Let FDWs init themselves for foreign-table result rels */
		if (!resultRelInfo->ri_usesFdwDirectModify &&
			resultRelInfo->ri_FdwRoutine != NULL &&
			resultRelInfo->ri_FdwRoutine->BeginForeignModify != NULL)
		{
			List	   *fdw_private = (List *) list_nth(node->fdwPrivLists, i);

			resultRelInfo->ri_FdwRoutine->BeginForeignModify(mtstate,
															 resultRelInfo,
															 fdw_private,
															 i,
															 eflags);
		}

		/*
		 * For UPDATE/DELETE, find the appropriate junk attr now, either a
		 * 'ctid' or 'wholerow' attribute depending on relkind.  For foreign
		 * tables, the FDW might have created additional junk attr(s), but
		 * those are no concern of ours.
		 */
		if (operation == CMD_UPDATE || operation == CMD_DELETE)
		{
			char		relkind;

			relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
			if (relkind == RELKIND_RELATION ||
				relkind == RELKIND_MATVIEW ||
				relkind == RELKIND_PARTITIONED_TABLE)
			{
				resultRelInfo->ri_RowIdAttNo =
					ExecFindJunkAttributeInTlist(subplan->targetlist, "ctid");
				if (!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
					elog(ERROR, "could not find junk ctid column");
			}
			else if (relkind == RELKIND_FOREIGN_TABLE)
			{
				/*
				 * When there is a row-level trigger, there should be a
				 * wholerow attribute.  We also require it to be present in
				 * UPDATE, so we can get the values of unchanged columns.
				 */
				resultRelInfo->ri_RowIdAttNo =
					ExecFindJunkAttributeInTlist(subplan->targetlist,
												 "wholerow");
				if (mtstate->operation == CMD_UPDATE &&
					!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
					elog(ERROR, "could not find junk wholerow column");
			}
			else
			{
				/* Other valid target relkinds must provide wholerow */
				resultRelInfo->ri_RowIdAttNo =
					ExecFindJunkAttributeInTlist(subplan->targetlist,
												 "wholerow");
				if (!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
					elog(ERROR, "could not find junk wholerow column");
			}
		}
	}

	/*
	 * If this is an inherited update/delete, there will be a junk attribute
	 * named "tableoid" present in the subplan's targetlist.  It will be used
	 * to identify the result relation for a given tuple to be
	 * updated/deleted.
	 */
	mtstate->mt_resultOidAttno =
		ExecFindJunkAttributeInTlist(subplan->targetlist, "tableoid");
	Assert(AttributeNumberIsValid(mtstate->mt_resultOidAttno) || nrels == 1);
	mtstate->mt_lastResultOid = InvalidOid; /* force lookup at first tuple */
	mtstate->mt_lastResultIndex = 0;	/* must be zero if no such attr */

	/* Get the root target relation */
	rel = mtstate->rootResultRelInfo->ri_RelationDesc;

	/*
	 * Build state for tuple routing if it's a partitioned INSERT.  An UPDATE
	 * might need this too, but only if it actually moves tuples between
	 * partitions; in that case setup is done by ExecCrossPartitionUpdate.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
		operation == CMD_INSERT)
		mtstate->mt_partition_tuple_routing =
			ExecSetupPartitionTupleRouting(estate, rel);

	/*
	 * Initialize any WITH CHECK OPTION constraints if needed.
	 */
	resultRelInfo = mtstate->resultRelInfo;
	foreach(l, node->withCheckOptionLists)
	{
		List	   *wcoList = (List *) lfirst(l);
		List	   *wcoExprs = NIL;
		ListCell   *ll;

		foreach(ll, wcoList)
		{
			WithCheckOption *wco = (WithCheckOption *) lfirst(ll);
			ExprState  *wcoExpr = ExecInitQual((List *) wco->qual,
											   &mtstate->ps);

			wcoExprs = lappend(wcoExprs, wcoExpr);
		}

		resultRelInfo->ri_WithCheckOptions = wcoList;
		resultRelInfo->ri_WithCheckOptionExprs = wcoExprs;
		resultRelInfo++;
	}

	/*
	 * Initialize RETURNING projections if needed.
	 */
	if (node->returningLists)
	{
		TupleTableSlot *slot;
		ExprContext *econtext;

		/*
		 * Initialize result tuple slot and assign its rowtype using the first
		 * RETURNING list.  We assume the rest will look the same.
		 */
		mtstate->ps.plan->targetlist = (List *) linitial(node->returningLists);

		/* Set up a slot for the output of the RETURNING projection(s) */
		ExecInitResultTupleSlotTL(&mtstate->ps, &TTSOpsVirtual);
		slot = mtstate->ps.ps_ResultTupleSlot;

		/* Need an econtext too */
		if (mtstate->ps.ps_ExprContext == NULL)
			ExecAssignExprContext(estate, &mtstate->ps);
		econtext = mtstate->ps.ps_ExprContext;

		/*
		 * Build a projection for each result rel.
		 */
		resultRelInfo = mtstate->resultRelInfo;
		foreach(l, node->returningLists)
		{
			List	   *rlist = (List *) lfirst(l);

			resultRelInfo->ri_returningList = rlist;
			resultRelInfo->ri_projectReturning =
				ExecBuildProjectionInfo(rlist, econtext, slot, &mtstate->ps,
										resultRelInfo->ri_RelationDesc->rd_att);
			resultRelInfo++;
		}
	}
	else
	{
		/*
		 * We still must construct a dummy result tuple type, because InitPlan
		 * expects one (maybe should change that?).
		 */
		mtstate->ps.plan->targetlist = NIL;
		ExecInitResultTypeTL(&mtstate->ps);

		mtstate->ps.ps_ExprContext = NULL;
	}

	/* Set the list of arbiter indexes if needed for ON CONFLICT */
	resultRelInfo = mtstate->resultRelInfo;
	if (node->onConflictAction != ONCONFLICT_NONE)
	{
		/* insert may only have one relation, inheritance is not expanded */
		Assert(nrels == 1);
		resultRelInfo->ri_onConflictArbiterIndexes = node->arbiterIndexes;
	}

	/*
	 * If needed, Initialize target list, projection and qual for ON CONFLICT
	 * DO UPDATE.
	 */
	if (node->onConflictAction == ONCONFLICT_UPDATE)
	{
		OnConflictSetState *onconfl = makeNode(OnConflictSetState);
		ExprContext *econtext;
		TupleDesc	relationDesc;

		/* already exists if created by RETURNING processing above */
		if (mtstate->ps.ps_ExprContext == NULL)
			ExecAssignExprContext(estate, &mtstate->ps);

		econtext = mtstate->ps.ps_ExprContext;
		relationDesc = resultRelInfo->ri_RelationDesc->rd_att;

		/* create state for DO UPDATE SET operation */
		resultRelInfo->ri_onConflict = onconfl;

		/* initialize slot for the existing tuple */
		onconfl->oc_Existing =
			table_slot_create(resultRelInfo->ri_RelationDesc,
							  &mtstate->ps.state->es_tupleTable);

		/*
		 * Create the tuple slot for the UPDATE SET projection. We want a slot
		 * of the table's type here, because the slot will be used to insert
		 * into the table, and for RETURNING processing - which may access
		 * system attributes.
		 */
		onconfl->oc_ProjSlot =
			table_slot_create(resultRelInfo->ri_RelationDesc,
							  &mtstate->ps.state->es_tupleTable);

		/* build UPDATE SET projection state */
		onconfl->oc_ProjInfo =
			ExecBuildUpdateProjection(node->onConflictSet,
									  true,
									  node->onConflictCols,
									  relationDesc,
									  econtext,
									  onconfl->oc_ProjSlot,
									  &mtstate->ps);

		/* initialize state to evaluate the WHERE clause, if any */
		if (node->onConflictWhere)
		{
			ExprState  *qualexpr;

			qualexpr = ExecInitQual((List *) node->onConflictWhere,
									&mtstate->ps);
			onconfl->oc_WhereClause = qualexpr;
		}
	}

	/*
	 * If we have any secondary relations in an UPDATE or DELETE, they need to
	 * be treated like non-locked relations in SELECT FOR UPDATE, ie, the
	 * EvalPlanQual mechanism needs to be told about them.  Locate the
	 * relevant ExecRowMarks.
	 */
	arowmarks = NIL;
	foreach(l, node->rowMarks)
	{
		PlanRowMark *rc = lfirst_node(PlanRowMark, l);
		ExecRowMark *erm;
		ExecAuxRowMark *aerm;

		/* ignore "parent" rowmarks; they are irrelevant at runtime */
		if (rc->isParent)
			continue;

		/* Find ExecRowMark and build ExecAuxRowMark */
		erm = ExecFindRowMark(estate, rc->rti, false);
		aerm = ExecBuildAuxRowMark(erm, subplan->targetlist);
		arowmarks = lappend(arowmarks, aerm);
	}

	EvalPlanQualSetPlan(&mtstate->mt_epqstate, subplan, arowmarks);

	/*
	 * If there are a lot of result relations, use a hash table to speed the
	 * lookups.  If there are not a lot, a simple linear search is faster.
	 *
	 * It's not clear where the threshold is, but try 64 for starters.  In a
	 * debugging build, use a small threshold so that we get some test
	 * coverage of both code paths.
	 */
#ifdef USE_ASSERT_CHECKING
#define MT_NRELS_HASH 4
#else
#define MT_NRELS_HASH 64
#endif
	if (nrels >= MT_NRELS_HASH)
	{
		HASHCTL		hash_ctl;

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(MTTargetRelLookup);
		hash_ctl.hcxt = CurrentMemoryContext;
		mtstate->mt_resultOidHash =
			hash_create("ModifyTable target hash",
						nrels, &hash_ctl,
						HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		for (i = 0; i < nrels; i++)
		{
			Oid			hashkey;
			MTTargetRelLookup *mtlookup;
			bool		found;

			resultRelInfo = &mtstate->resultRelInfo[i];
			hashkey = RelationGetRelid(resultRelInfo->ri_RelationDesc);
			mtlookup = (MTTargetRelLookup *)
				hash_search(mtstate->mt_resultOidHash, &hashkey,
							HASH_ENTER, &found);
			Assert(!found);
			mtlookup->relationIndex = i;
		}
	}
	else
		mtstate->mt_resultOidHash = NULL;

	/*
	 * Determine if the FDW supports batch insert and determine the batch size
	 * (a FDW may support batching, but it may be disabled for the
	 * server/table).
	 *
	 * We only do this for INSERT, so that for UPDATE/DELETE the batch size
	 * remains set to 0.
	 */
	if (operation == CMD_INSERT)
	{
		/* insert may only have one relation, inheritance is not expanded */
		Assert(nrels == 1);
		resultRelInfo = mtstate->resultRelInfo;
		if (!resultRelInfo->ri_usesFdwDirectModify &&
			resultRelInfo->ri_FdwRoutine != NULL &&
			resultRelInfo->ri_FdwRoutine->GetForeignModifyBatchSize &&
			resultRelInfo->ri_FdwRoutine->ExecForeignBatchInsert)
		{
			resultRelInfo->ri_BatchSize =
				resultRelInfo->ri_FdwRoutine->GetForeignModifyBatchSize(resultRelInfo);
			Assert(resultRelInfo->ri_BatchSize >= 1);
		}
		else
			resultRelInfo->ri_BatchSize = 1;
	}

	/*
	 * Lastly, if this is not the primary (canSetTag) ModifyTable node, add it
	 * to estate->es_auxmodifytables so that it will be run to completion by
	 * ExecPostprocessPlan.  (It'd actually work fine to add the primary
	 * ModifyTable node too, but there's no need.)  Note the use of lcons not
	 * lappend: we need later-initialized ModifyTable nodes to be shut down
	 * before earlier ones.  This ensures that we don't throw away RETURNING
	 * rows that need to be seen by a later CTE subplan.
	 */
	if (!mtstate->canSetTag)
		estate->es_auxmodifytables = lcons(mtstate,
										   estate->es_auxmodifytables);

	return mtstate;
}

/* ----------------------------------------------------------------
 *		ExecEndModifyTable
 *
 *		Shuts down the plan.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndModifyTable(ModifyTableState *node)
{
	int			i;

	/*
	 * Allow any FDWs to shut down
	 */
	for (i = 0; i < node->mt_nrels; i++)
	{
		int			j;
		ResultRelInfo *resultRelInfo = node->resultRelInfo + i;

		if (!resultRelInfo->ri_usesFdwDirectModify &&
			resultRelInfo->ri_FdwRoutine != NULL &&
			resultRelInfo->ri_FdwRoutine->EndForeignModify != NULL)
			resultRelInfo->ri_FdwRoutine->EndForeignModify(node->ps.state,
														   resultRelInfo);

		/*
		 * Cleanup the initialized batch slots. This only matters for FDWs
		 * with batching, but the other cases will have ri_NumSlotsInitialized
		 * == 0.
		 */
		for (j = 0; j < resultRelInfo->ri_NumSlotsInitialized; j++)
		{
			ExecDropSingleTupleTableSlot(resultRelInfo->ri_Slots[j]);
			ExecDropSingleTupleTableSlot(resultRelInfo->ri_PlanSlots[j]);
		}
	}

	/*
	 * Close all the partitioned tables, leaf partitions, and their indices
	 * and release the slot used for tuple routing, if set.
	 */
	if (node->mt_partition_tuple_routing)
	{
		ExecCleanupTupleRouting(node, node->mt_partition_tuple_routing);

		if (node->mt_root_tuple_slot)
			ExecDropSingleTupleTableSlot(node->mt_root_tuple_slot);
	}

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ps.ps_ResultTupleSlot);

	/*
	 * Terminate EPQ execution if active
	 */
	EvalPlanQualEnd(&node->mt_epqstate);

	/*
	 * shut down subplan
	 */
	ExecEndNode(outerPlanState(node));
}

void
ExecReScanModifyTable(ModifyTableState *node)
{
	/*
	 * Currently, we don't need to support rescan on ModifyTable nodes. The
	 * semantics of that would be a bit debatable anyway.
	 */
	elog(ERROR, "ExecReScanModifyTable is not implemented");
}
