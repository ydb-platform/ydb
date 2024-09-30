CREATE TABLE`/Root/test/tpcc/warehouse` (
                W_ID       Int32          NOT NULL,
                W_YTD      Double,
                W_TAX      Double,
                W_NAME     Utf8,
                W_STREET_1 Utf8,
                W_STREET_2 Utf8,
                W_CITY     Utf8,
                W_STATE    Utf8,
                W_ZIP      Utf8,
                PRIMARY KEY (W_ID)
            );

CREATE TABLE `/Root/test/tpcc/item` (
                I_ID    Int32           NOT NULL,
                I_NAME  Utf8,
                I_PRICE Double,
                I_DATA  Utf8,
                I_IM_ID Int32,
                PRIMARY KEY (I_ID)
            );

CREATE TABLE `/Root/test/tpcc/stock` (
                S_W_ID       Int32           NOT NULL,
                S_I_ID       Int32           NOT NULL,
                S_QUANTITY   Int32,
                S_YTD        Double,
                S_ORDER_CNT  Int32,
                S_REMOTE_CNT Int32,
                S_DATA       Utf8,
                S_DIST_01    Utf8,
                S_DIST_02    Utf8,
                S_DIST_03    Utf8,
                S_DIST_04    Utf8,
                S_DIST_05    Utf8,
                S_DIST_06    Utf8,
                S_DIST_07    Utf8,
                S_DIST_08    Utf8,
                S_DIST_09    Utf8,
                S_DIST_10    Utf8,
                PRIMARY KEY (S_W_ID, S_I_ID)
            );

CREATE TABLE `/Root/test/tpcc/district` (
                D_W_ID      Int32            NOT NULL,
                D_ID        Int32            NOT NULL,
                D_YTD       Double,
                D_TAX       Double,
                D_NEXT_O_ID Int32,
                D_NAME      Utf8,
                D_STREET_1  Utf8,
                D_STREET_2  Utf8,
                D_CITY      Utf8,
                D_STATE     Utf8,
                D_ZIP       Utf8,
                PRIMARY KEY (D_W_ID, D_ID)
            );

CREATE TABLE `/Root/test/tpcc/customer` (
                C_W_ID         Int32            NOT NULL,
                C_D_ID         Int32            NOT NULL,
                C_ID           Int32            NOT NULL,
                C_DISCOUNT     Double,
                C_CREDIT       Utf8,
                C_LAST         Utf8,
                C_FIRST        Utf8,
                C_CREDIT_LIM   Double,
                C_BALANCE      Double,
                C_YTD_PAYMENT  Double,
                C_PAYMENT_CNT  Int32,
                C_DELIVERY_CNT Int32,
                C_STREET_1     Utf8,
                C_STREET_2     Utf8,
                C_CITY         Utf8,
                C_STATE        Utf8,
                C_ZIP          Utf8,
                C_PHONE        Utf8,
                C_SINCE        Timestamp,
                C_MIDDLE       Utf8,
                C_DATA         Utf8,

                PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
            );

CREATE TABLE `/Root/test/tpcc/history` (
                H_C_W_ID    Int32       NOT NULL,
                H_C_ID      Int32,
                H_C_D_ID    Int32,
                H_D_ID      Int32,
                H_W_ID      Int32,
                H_DATE      Timestamp,
                H_AMOUNT    Double,
                H_DATA      Utf8,
                H_C_NANO_TS Int64        NOT NULL,

                PRIMARY KEY (H_C_W_ID, H_C_NANO_TS)
            );

CREATE TABLE `/Root/test/tpcc/oorder` (
                O_W_ID       Int32       NOT NULL,
                O_D_ID       Int32       NOT NULL,
                O_ID         Int32       NOT NULL,
                O_C_ID       Int32,
                O_CARRIER_ID Int32,
                O_OL_CNT     Int32,
                O_ALL_LOCAL  Int32,
                O_ENTRY_D    Timestamp,

                PRIMARY KEY (O_W_ID, O_D_ID, O_ID)
            );

 CREATE TABLE `/Root/test/tpcc/new_order` (
                NO_W_ID Int32 NOT NULL,
                NO_D_ID Int32 NOT NULL,
                NO_O_ID Int32 NOT NULL,

                PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID)
            );

CREATE TABLE `/Root/test/tpcc/order_line` (
                OL_W_ID        Int32           NOT NULL,
                OL_D_ID        Int32           NOT NULL,
                OL_O_ID        Int32           NOT NULL,
                OL_NUMBER      Int32           NOT NULL,
                OL_I_ID        Int32,
                OL_DELIVERY_D  Timestamp,
                OL_AMOUNT      Double,
                OL_SUPPLY_W_ID Int32,
                OL_QUANTITY    Double,
                OL_DIST_INFO   Utf8,

                PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
            );
