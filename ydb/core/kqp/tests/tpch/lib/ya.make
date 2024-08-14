LIBRARY()

SUBSCRIBER(
    imakunin
    g:kikimr
)

SRCS(
    tpch_runner.cpp
    tpch_tables.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/core/protos
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
)

RESOURCE(
    ydb/core/kqp/tests/tpch/lib/data/ps_0.001/customer.tbl customer.tbl
    ydb/core/kqp/tests/tpch/lib/data/ps_0.001/lineitem.tbl lineitem.tbl
    ydb/core/kqp/tests/tpch/lib/data/ps_0.001/nation.tbl   nation.tbl
    ydb/core/kqp/tests/tpch/lib/data/ps_0.001/orders.tbl   orders.tbl
    ydb/core/kqp/tests/tpch/lib/data/ps_0.001/partsupp.tbl partsupp.tbl
    ydb/core/kqp/tests/tpch/lib/data/ps_0.001/part.tbl     part.tbl
    ydb/core/kqp/tests/tpch/lib/data/ps_0.001/region.tbl   region.tbl
    ydb/core/kqp/tests/tpch/lib/data/ps_0.001/supplier.tbl supplier.tbl

    ydb/core/kqp/tests/tpch/lib/data/queries/01_Pricing_Summary_Report_Query.sql            01.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/02_Minimum_Cost_Supplier_Query.sql             02.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/03_Shipping_Priority_Query.sql                 03.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/04_Order_Priority_Checking_Query.sql           04.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/05_Local_Supplier_Volume_Query.sql             05.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/06_Forecasting_Revenue_Change_Query.sql        06.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/07_Volume_Shipping_Query.sql                   07.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/08_National_Market_Share_Query.sql             08.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/09_Product_Type_Profit_Measure_Query.sql       09.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/10_Returned_Item_Reporting_Query.sql           10.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/11_Important_Stock_Identification_Query.sql    11.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/12_Shipping_Modes_and_Order_Priority_Query.sql 12.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/13_Customer_Distribution_Query.sql             13.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/14_Promotion_Effect_Query.sql                  14.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/15_Top_Supplier_Query.sql                      15.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/16_Parts_Supplier_Relationship_Query.sql       16.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/17_Small_Quantity_Order_Revenue_Query.sql      17.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/18_Large_Volume_Customer_Query.sql             18.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/19_Discounted_Revenue_Query.sql                19.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/20_Potential_Part_Promotion_Query.sql          20.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/21_Suppliers_Who_Kept_Orders_Waiting_Query.sql 21.sql
    ydb/core/kqp/tests/tpch/lib/data/queries/22_Global_Sales_Opportunity_Query.sql          22.sql
)

END()
