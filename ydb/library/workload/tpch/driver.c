#define DECLARER

#include <ydb/library/benchmarks/gen/tpch-dbgen/config.h>
#include <ydb/library/benchmarks/gen/tpch-dbgen/release.h>
#include <ydb/library/benchmarks/gen/tpch-dbgen/dss.h>
#include <ydb/library/benchmarks/gen/tpch-dbgen/dsstypes.h>

#define NO_FUNC (int (*) ()) NULL    /* to clean up tdefs */
#define NO_LFUNC (long (*) ()) NULL        /* to clean up tdefs */

long sd_cust (int child, DSS_HUGE skip_count);
long sd_line (int child, DSS_HUGE skip_count);
long sd_order (int child, DSS_HUGE skip_count);
long sd_part (int child, DSS_HUGE skip_count);
long sd_psupp (int child, DSS_HUGE skip_count);
long sd_supp (int child, DSS_HUGE skip_count);
long sd_order_line (int child, DSS_HUGE skip_count);
long sd_part_psupp (int child, DSS_HUGE skip_count);
void ReadDistFromResource(const char* name, distribution* target);

tdef tdefs[] =
{
    {"part", "part table", 200000, NO_FUNC, sd_part, PSUPP, 0},                                         //PART          0
    {"partsupp", "partsupplier table", 200000, NO_FUNC, sd_psupp, NONE, 0},                             //PSUPP         1
    {"supplier", "suppliers table", 10000, NO_FUNC, sd_supp, NONE, 0},                                  //SUPP          2
    {"customer", "customers table", 150000, NO_FUNC, sd_cust, NONE, 0},                                 //CUST          3
    {"orders", "order table", 150000 * ORDERS_PER_CUST, NO_FUNC, sd_order, LINE, 0},                    //ORDER         4
    {"lineitem", "lineitem table", 150000 * ORDERS_PER_CUST, NO_FUNC, sd_line, NONE, 0},                //LINE          5
    {"orders", "orders/lineitem tables", 150000 * ORDERS_PER_CUST, NO_FUNC, sd_order, LINE, 0},         //ORDER_LINE    6
    {"part", "part/partsupplier tables", 200000, NO_FUNC, sd_part, PSUPP, 0},                           //PART_PSUPP    7
    {"nation", "nation table", NATIONS_MAX, NO_FUNC, NO_LFUNC, NONE, 0},                                //NATION        8
    {"region", "region table", NATIONS_MAX, NO_FUNC, NO_LFUNC, NONE, 0},                                //REGION        9
};


void LoadDists() {
    ReadDistFromResource ("p_cntr", &p_cntr_set);
    ReadDistFromResource ("colors", &colors);
    ReadDistFromResource ("p_types", &p_types_set);
    ReadDistFromResource ("nations", &nations);
    ReadDistFromResource ("regions", &regions);
    ReadDistFromResource ("o_oprio",
        &o_priority_set);
    ReadDistFromResource ("instruct",
        &l_instruct_set);
    ReadDistFromResource ("smode", &l_smode_set);
    ReadDistFromResource ("category",
        &l_category_set);
    ReadDistFromResource ("rflag", &l_rflag_set);
    ReadDistFromResource ("msegmnt", &c_mseg_set);

    /* load the distributions that contain text generation */
    ReadDistFromResource ("nouns", &nouns);
    ReadDistFromResource ("verbs", &verbs);
    ReadDistFromResource ("adjectives", &adjectives);
    ReadDistFromResource ("adverbs", &adverbs);
    ReadDistFromResource ("auxillaries", &auxillaries);
    ReadDistFromResource ("terminators", &terminators);
    ReadDistFromResource ("articles", &articles);
    ReadDistFromResource ("prepositions", &prepositions);
    ReadDistFromResource ("grammar", &grammar);
    ReadDistFromResource ("np", &np);
    ReadDistFromResource ("vp", &vp);
}

extern seed_t Seed[];

void InitTpchGen(DSS_HUGE _scale) {
    scale = _scale;
    LoadDists();
    tdefs[NATION].base = nations.count;
    tdefs[REGION].base = regions.count;
    for (int i = 0; i <= MAX_STREAM; i++)
        Seed[i].nCalls = 0;
}

void GenSeed(int tableNum, DSS_HUGE rowsCount) {
    typedef long (*gen_seed)(int, DSS_HUGE);
    if (tdefs[tableNum].gen_seed != NULL) {
        ((gen_seed)tdefs[tableNum].gen_seed)(tableNum == LINE, rowsCount);
    }
    const int childNum = tdefs[tableNum].child;
    if (childNum != NONE && tdefs[childNum].gen_seed != NULL) {
        ((gen_seed)tdefs[childNum].gen_seed)(0, rowsCount);
    }
}
