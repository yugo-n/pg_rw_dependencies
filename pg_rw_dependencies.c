#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/predicate.h"
#include "storage/predicate_internals.h"

static void get_rw_conflict_values(RWConflict conflict, Datum *values, bool *nulls);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_rw_dependencies);
Datum
pg_rw_dependencies(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	SERIALIZABLEXACT *sxact = (SERIALIZABLEXACT *) ShareSerializableXact();
	RWConflict	conflict;


	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* need to build tuplestore in query context */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* Check if we are in SERIALIZABLE transaction */
	if (sxact == InvalidSerializableXact)
		goto end;

	/* A conflict is possible; walk the list to find out. */
	conflict = (RWConflict)
		SHMQueueNext(&sxact->outConflicts,
					 &sxact->outConflicts,
					 offsetof(RWConflictData, outLink));
	while (conflict)
	{
		Datum		values[4];
		bool		nulls[4];

		memset(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		get_rw_conflict_values(conflict, values, nulls);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		
		conflict = (RWConflict)
			SHMQueueNext(&sxact->outConflicts,
						 &conflict->outLink,
						 offsetof(RWConflictData, outLink));
	}

	conflict = (RWConflict)
		SHMQueueNext(&sxact->inConflicts,
					 &sxact->inConflicts,
					 offsetof(RWConflictData, inLink));
	while (conflict)
	{
		Datum		values[4];
		bool		nulls[4];

		memset(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		get_rw_conflict_values(conflict, values, nulls);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		conflict = (RWConflict)
			SHMQueueNext(&sxact->inConflicts,
						 &conflict->inLink,
						 offsetof(RWConflictData, inLink));
	}

end:
	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}

static void
get_rw_conflict_values(RWConflict conflict, Datum *values, bool *nulls)
{
	SERIALIZABLEXACT *in = conflict->sxactIn;
	SERIALIZABLEXACT *out = conflict->sxactOut;

	if (out->topXid != InvalidTransactionId)
		values[0] = TransactionIdGetDatum(out->topXid);
	else
		nulls[0] = true;

	values[1] = Int32GetDatum(out->pid);

	if (in->topXid != InvalidTransactionId)
		values[2] = TransactionIdGetDatum(in->topXid);
	else
		nulls[2] = true;

	values[3] = Int32GetDatum(in->pid);
}
