#include "protobuf/pg_query.pb-c.h"

#include "pg_query_outfuncs.h"

#include "postgres.h"
#include <ctype.h>
#include "access/relation.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/value.h"
#include "utils/datum.h"

typedef void (*out_func)(void*, void*);
int register_specific_node(void* out, void* node, out_func func);

#define OUT_TYPE(typename, typename_c) PgQuery__##typename_c*

#define OUT_NODE(typename, typename_c, typename_underscore, typename_underscore_upcase, typename_cast, fldname) \
  { \
    PgQuery__##typename_c *__node = palloc(sizeof(PgQuery__##typename_c)); \
	pg_query__##typename_underscore##__init(__node); \
    _out##typename_c(__node, (const typename_cast *) obj); \
	out->fldname = __node; \
	out->node_case = PG_QUERY__NODE__NODE_##typename_underscore_upcase; \
  }

#define WRITE_INT_FIELD(outname, outname_json, fldname) out->outname = node->fldname;
#define WRITE_UINT_FIELD(outname, outname_json, fldname) out->outname = node->fldname;
#define WRITE_LONG_FIELD(outname, outname_json, fldname) out->outname = node->fldname;
#define WRITE_FLOAT_FIELD(outname, outname_json, fldname) out->outname = node->fldname;
#define WRITE_BOOL_FIELD(outname, outname_json, fldname) out->outname = node->fldname;

#define WRITE_CHAR_FIELD(outname, outname_json, fldname) \
	if (node->fldname != 0) { \
		out->outname = palloc(sizeof(char) * 2); \
		out->outname[0] = node->fldname; \
		out->outname[1] = '\0'; \
	}
#define WRITE_STRING_FIELD(outname, outname_json, fldname) \
	if (node->fldname != NULL) { \
		out->outname = pstrdup(node->fldname); \
	}

#define WRITE_ENUM_FIELD(typename, outname, outname_json, fldname) \
	out->outname = _enumToInt##typename(node->fldname);

#define WRITE_LIST_FIELD(outname, outname_json, fldname) \
	if (node->fldname != NULL) { \
	  out->n_##outname = list_length(node->fldname); \
	  out->outname = palloc(sizeof(PgQuery__Node*) * out->n_##outname); \
	  for (int i = 0; i < out->n_##outname; i++) \
      { \
	    PgQuery__Node *__node = palloc(sizeof(PgQuery__Node)); \
	    pg_query__node__init(__node); \
	    out->outname[i] = __node; \
	    _outNode(out->outname[i], list_nth(node->fldname, i)); \
      } \
    }

#define WRITE_BITMAPSET_FIELD(outname, outname_json, fldname) \
	if (!bms_is_empty(node->fldname)) \
	{ \
		int x = 0; \
		int i = 0; \
		out->n_##outname = bms_num_members(node->fldname); \
		out->outname = palloc(sizeof(PgQuery__Node*) * out->n_##outname); \
		while ((x = bms_first_member(node->fldname)) >= 0) \
			out->outname[i++] = x; \
    }

#define WRITE_NODE_FIELD(outname, outname_json, fldname) \
	{ \
		PgQuery__Node *__node = palloc(sizeof(PgQuery__Node)); \
		pg_query__node__init(__node); \
		out->outname = __node; \
		_outNode(out->outname, &node->fldname); \
	}

#define WRITE_NODE_PTR_FIELD(outname, outname_json, fldname) \
	if (node->fldname != NULL) { \
		PgQuery__Node *__node = palloc(sizeof(PgQuery__Node)); \
		pg_query__node__init(__node); \
		out->outname = __node; \
		_outNode(out->outname, node->fldname); \
	}

#define WRITE_SPECIFIC_NODE_FIELD(typename, typename_underscore, outname, outname_json, fldname) \
	{ \
		PgQuery__##typename *__node = palloc(sizeof(PgQuery__##typename)); \
		pg_query__##typename_underscore##__init(__node); \
		out->outname = __node; \
        if (!register_specific_node(__node, &node->fldname, (out_func)&_out##typename)) \
		_out##typename(__node, &node->fldname); \
	}

#define WRITE_SPECIFIC_NODE_PTR_FIELD(typename, typename_underscore, outname, outname_json, fldname) \
	if (node->fldname != NULL) { \
		PgQuery__##typename *__node = palloc(sizeof(PgQuery__##typename)); \
		pg_query__##typename_underscore##__init(__node); \
		out->outname = __node; \
        if (!register_specific_node(__node, node->fldname, (out_func)&_out##typename)) \
		_out##typename(__node, node->fldname); \
	}

static void _outNode(PgQuery__Node* out, const void *obj);

void _outNodeImpl(PgQuery__Node* out, const void *obj);

static void
_outList(PgQuery__List* out, const List *node)
{
	const ListCell *lc;
	int i = 0;
	out->n_items = list_length(node);
	out->items = palloc(sizeof(PgQuery__Node*) * out->n_items);
    foreach(lc, node)
    {
		out->items[i] = palloc(sizeof(PgQuery__Node));
		pg_query__node__init(out->items[i]);
	    _outNode(out->items[i], lfirst(lc));
		i++;
    }
}

static void
_outIntList(PgQuery__IntList* out, const List *node)
{
	const ListCell *lc;
	int i = 0;
	out->n_items = list_length(node);
	out->items = palloc(sizeof(PgQuery__Node*) * out->n_items);
    foreach(lc, node)
    {
		out->items[i] = palloc(sizeof(PgQuery__Node));
		pg_query__node__init(out->items[i]);
	    _outNode(out->items[i], lfirst(lc));
		i++;
    }
}

static void
_outOidList(PgQuery__OidList* out, const List *node)
{
	const ListCell *lc;
	int i = 0;
	out->n_items = list_length(node);
	out->items = palloc(sizeof(PgQuery__Node*) * out->n_items);
    foreach(lc, node)
    {
		out->items[i] = palloc(sizeof(PgQuery__Node));
		pg_query__node__init(out->items[i]);
	    _outNode(out->items[i], lfirst(lc));
		i++;
    }
}

// TODO: Add Bitmapset

static void
_outInteger(PgQuery__Integer* out, const Value *node)
{
  out->ival = node->val.ival;
}

static void
_outFloat(PgQuery__Float* out, const Value *node)
{
  out->str = node->val.str;
}

static void
_outString(PgQuery__String* out, const Value *node)
{
  out->str = node->val.str;
}

static void
_outBitString(PgQuery__BitString* out, const Value *node)
{
  out->str = node->val.str;
}

static void
_outNull(PgQuery__Null* out, const Value *node)
{
  // Null has no fields
}

#include "pg_query_enum_defs.c"
#include "pg_query_outfuncs_defs.c"

int register_out_node(PgQuery__Node* out, const void *obj);

static void
_outNode(PgQuery__Node* out, const void *obj) {
        if (register_out_node(out, obj))
               return;

        _outNodeImpl(out, obj);
}

void
_outNodeImpl(PgQuery__Node* out, const void *obj)
{
	if (obj == NULL)
		return; // Keep out as NULL

	switch (nodeTag(obj))
	{
		#include "pg_query_outfuncs_conds.c"

		default:
			printf("could not dump unrecognized node type: %d", (int) nodeTag(obj));
			elog(WARNING, "could not dump unrecognized node type: %d",
					(int) nodeTag(obj));

			return;
	}
}

PgQueryProtobuf
pg_query_nodes_to_protobuf(const void *obj)
{
	PgQueryProtobuf protobuf;
	const ListCell *lc;
	int i = 0;
	PgQuery__ParseResult parse_result = PG_QUERY__PARSE_RESULT__INIT;

	parse_result.version = PG_VERSION_NUM;

	if (obj == NULL) {
		parse_result.n_stmts = 0;
		parse_result.stmts = NULL;
	}
	else
	{
		parse_result.n_stmts = list_length(obj);
		parse_result.stmts = palloc(sizeof(PgQuery__RawStmt*) * parse_result.n_stmts);
		foreach(lc, obj)
		{
			parse_result.stmts[i] = palloc(sizeof(PgQuery__RawStmt));
			pg_query__raw_stmt__init(parse_result.stmts[i]);
			_outRawStmt(parse_result.stmts[i], lfirst(lc));
			i++;
		}
	}

	protobuf.len = pg_query__parse_result__get_packed_size(&parse_result);
	// Note: This is intentionally malloc so exiting the memory context doesn't free this
	protobuf.data = malloc(sizeof(char) * protobuf.len);
	pg_query__parse_result__pack(&parse_result, (void*) protobuf.data); 

	return protobuf;
}
