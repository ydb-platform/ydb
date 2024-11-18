#include <util/system/compiler.h>

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T

extern "C" {
#include "postgres.h"
#include "optimizer/paths.h"
#include "nodes/print.h"
#include "utils/selfuncs.h"
#include "utils/palloc.h"
}

#undef Min
#undef Max
#undef TypeName
#undef SortBy


extern "C" void
add_function_cost(PlannerInfo *root, Oid funcid, Node *node,
				  QualCost *cost)
{
    Y_UNUSED(root);
    Y_UNUSED(funcid);
    Y_UNUSED(node);
	cost->per_tuple += 100000;
}

extern "C" bool
op_mergejoinable(Oid opno, Oid inputtype) {
    Y_UNUSED(opno);
    Y_UNUSED(inputtype);
    return false;
}

extern "C" bool
op_hashjoinable(Oid opno, Oid inputtype) {
    Y_UNUSED(opno);
    Y_UNUSED(inputtype);
    return true;
}

extern "C" RegProcedure
get_oprjoin(Oid opno)
{
    Y_UNUSED(opno);
	return 105;
}

extern "C" char
func_volatile(Oid funcid)
{
    Y_UNUSED(funcid);
    return 'i';
}
