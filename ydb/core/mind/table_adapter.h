#pragma once

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/base/id_wrapper.h>
namespace NKikimr {

    // inline table specifier
    template<typename TContainer, typename TTable>
    struct TInlineTable
    {};

    namespace NTableAdapter {

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // KEY COLUMN MANAGER
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        template<size_t Index, typename... TableKeyTypes>
        struct TGetTableKeyColumnImpl;

        template<typename First, typename... TableKeyTypes>
        struct TGetTableKeyColumnImpl<0, First, TableKeyTypes...> {
            using Type = First;
        };

        template<size_t Index, typename First, typename... TableKeyTypes>
        struct TGetTableKeyColumnImpl<Index, First, TableKeyTypes...> : TGetTableKeyColumnImpl<Index - 1, TableKeyTypes...> {};

        template<size_t Index, typename T>
        struct TGetTableKeyColumn;

        template<size_t Index, typename... TableKeyTypes>
        struct TGetTableKeyColumn<Index, std::tuple<TableKeyTypes...>> : TGetTableKeyColumnImpl<Index, TableKeyTypes...> {};

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // KEY MAPPER
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        template<typename Table, size_t Index, size_t Count, typename... TKeyItems>
        struct TTupleKeyMapper {
            using TColumn = typename TGetTableKeyColumn<Index, typename Table::TKey::KeyColumnsType>::Type;

            template<typename... TArgs>
            static auto PrepareKeyTuple(const std::tuple<TKeyItems...> *items, std::tuple<TArgs...> &&tuple) {
                TMaybe<typename NIceDb::NSchemeTypeMapper<TColumn::ColumnType>::Type> value(std::get<Index>(*items));
                return TTupleKeyMapper<Table, Index + 1, Count, TKeyItems...>::PrepareKeyTuple(items,
                    std::tuple_cat(std::forward<std::tuple<TArgs...>>(tuple), std::make_tuple(std::move(value))));
            }

            template<typename TTuple, typename TKey>
            static void MapKey(TTuple *tuple, TKey &key) {
                Y_ABORT_UNLESS(key.size() == Index);

                auto &maybe = std::get<Index>(*tuple);
                key.push_back(NIceDb::TConvertTypeValue<TColumn::ColumnType>(
                        maybe ? NIceDb::TTypeValue(*maybe) : NIceDb::TTypeValue()
                    ));

                TTupleKeyMapper<Table, Index + 1, Count, TKeyItems...>::MapKey(tuple, key);
            }
        };

        template<typename Table, size_t Count, typename... TKeyItems>
        struct TTupleKeyMapper<Table, Count, Count, TKeyItems...> {
            template<typename... TArgs>
            static std::tuple<TArgs...> PrepareKeyTuple(const std::tuple<TKeyItems...>* /*items*/,
                    std::tuple<TArgs...> &&tuple) {
                return std::move(tuple);
            }

            template<typename TTuple, typename TKey>
            static void MapKey(TTuple* /*tuple*/, TKey& /*key*/)
            {}
        };

        template<typename Table, typename... TKeyItems>
        auto PrepareKeyTuple(const std::tuple<TKeyItems...> *key) {
            return TTupleKeyMapper<Table, 0, sizeof...(TKeyItems), TKeyItems...>::PrepareKeyTuple(key, std::make_tuple());
        }

        template<typename Table, typename... TTupleItems, typename TKey>
        inline void MapKey(std::tuple<TTupleItems...> *tuple, TKey &key) {
            TTupleKeyMapper<Table, 0, sizeof...(TTupleItems), TTupleItems...>::MapKey(tuple, key);
        }

        template<typename T, typename TTuple = decltype(std::declval<T>().GetKey())>
        TTuple WrapTuple(const T &object) {
            return object.GetKey();
        }

        template<typename... TItems>
        auto WrapTuple(const std::tuple<TItems...> &tuple) {
            return tuple;
        }

        template<typename T>
        auto WrapTuple(const T &item, std::enable_if_t<std::is_integral<T>::value>* = nullptr) {
            return std::make_tuple(item);
        }

        template<typename T>
        auto WrapTuple(const T &item, std::enable_if_t<std::is_same_v<T, TString>>* = nullptr) {
            return std::make_tuple(item);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // CELL
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        template<typename To, typename From>
        std::enable_if_t<std::is_convertible_v<From, To>> Cast(const From& from, TMaybe<To>& to) {
            to.ConstructInPlace(from);
        }

        inline void Cast(const TInstant& from, TMaybe<ui64>& to) {
            to.ConstructInPlace(from.GetValue());
        }

        inline void Cast(const TDuration& from, TMaybe<ui64>& to) {
            to.ConstructInPlace(from.GetValue());
        }

        template<typename T, typename Tag>
        inline void Cast(const TIdWrapper<T, Tag>& from, TMaybe<T>& to){
            to.ConstructInPlace(from.GetRawId());
        }

        template<typename TRow, typename TColumn>
        struct TCell {
            typename TColumn::Type TRow::*CellPtr = nullptr;
            TMaybe<typename TColumn::Type> TRow::*MaybeCellPtr = nullptr;

            constexpr TCell(typename TColumn::Type TRow::*cell)
                : CellPtr(cell)
            {}

            constexpr TCell(TMaybe<typename TColumn::Type> TRow::*maybe)
                : MaybeCellPtr(maybe)
            {}

            const typename TColumn::Type& GetValue(const TRow &row) const {
                if (CellPtr) {
                    return row.*CellPtr;
                } else if (MaybeCellPtr) {
                    return *(row.*MaybeCellPtr);
                } else {
                    Y_ABORT();
                }
            }

            bool Equals(const TRow &x, const TRow &y) const {
                if (CellPtr) {
                    return x.*CellPtr == y.*CellPtr;
                } else if (MaybeCellPtr) {
                    return x.*MaybeCellPtr == y.*MaybeCellPtr;
                } else {
                    Y_ABORT();
                }
            }

            template<typename TRowset>
            void ConstructFromRowset(const TRowset &rowset, TRow &row) const {
                if (CellPtr) {
                    row.*CellPtr = rowset.template GetValue<TColumn>();
                } else if (MaybeCellPtr) {
                    if (rowset.template HaveValue<TColumn>()) {
                        row.*MaybeCellPtr = rowset.template GetValue<TColumn>();
                    }
                } else {
                    Y_ABORT();
                }
            }

            // extend tuple with one more item
            template<typename... TArgs>
            auto PrepareUpdateRow(const TRow &row, std::tuple<TArgs...> &&tuple) const {
                using T = typename NIceDb::NSchemeTypeMapper<TColumn::ColumnType>::Type;
                TMaybe<T> value;

                if (CellPtr) {
                    Cast(row.*CellPtr, value);
                } else if (MaybeCellPtr) {
                    if (const auto& m = row.*MaybeCellPtr) {
                        Cast(*m, value);
                    } else {
                        value = Nothing();
                    }
                } else {
                    Y_ABORT();
                }

                return std::tuple_cat(tuple, std::make_tuple(std::move(value)));
            }

            template<size_t Index, typename TUpdates, typename... TArgs>
            static void PopulateUpdateOp(TUpdates &updates, std::tuple<TArgs...> *tuple) {
                NTable::TUpdateOp op;
                op.Tag = TColumn::ColumnId;
                op.Op = NTable::ECellOp::Set;

                std::tuple_element_t<Index, std::tuple<TArgs...>> &maybe = std::get<Index>(*tuple);
                op.Value = NIceDb::TConvertTypeValue<TColumn::ColumnType>(
                        maybe ? NIceDb::TTypeValue(*maybe) : NIceDb::TTypeValue()
                    );

                updates.push_back(std::move(op));
            }

            template<typename TCallback>
            void ForEachInlineTable(TCallback&& /*callback*/) const
            {}
        };

        template<typename TRow, typename TContainer, typename Table>
        struct TInlineTableCell {
            TContainer TRow::*ContainerPtr;

            constexpr TInlineTableCell(TContainer TRow::*m)
                : ContainerPtr(m)
            {}

            bool Equals(const TRow& /*x*/, const TRow& /*y*/) const {
                return true; // ignored in inlined tables
            }

            template<typename TRowset>
            void ConstructFromRowset(const TRowset& /*rowset*/, TRow& /*row*/) const
            {}

            template<typename... TArgs>
            std::tuple<TArgs...> PrepareUpdateRow(const TRow& /*row*/, std::tuple<TArgs...> &&tuple) const {
                return std::move(tuple);
            }

            template<size_t Index, typename TUpdates, typename... TArgs>
            static void PopulateUpdateOp(TUpdates& /*updates*/, std::tuple<TArgs...>* /*tuple*/)
            {}

            template<typename TCallback>
            void ForEachInlineTable(TCallback &&callback) const {
                callback(ContainerPtr, static_cast<Table*>(nullptr));
            }
        };

        template<typename TRow, typename TColumn>
        struct TMapColumnToCell {
            using Type = TCell<TRow, TColumn>;
        };

        template<typename TRow, typename TContainer, typename Table>
        struct TMapColumnToCell<TRow, TInlineTable<TContainer, Table>> {
            using Type = TInlineTableCell<TRow, TContainer, Table>;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // CELL TUPLE
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Cell tuple is a tuple container for TCell instances describing columns of a specific table

        template<typename... T>
        struct TCellTuple;

        template<typename TCell>
        struct TCellTuple<TCell> {
            const TCell Value;

            template<typename... TArgs>
            constexpr TCellTuple(TCell&& value)
                : Value(std::forward<TCell>(value))
            {}

            template<typename TRow>
            bool Equals(const TRow &x, const TRow &y) const {
                return Value.Equals(x, y);
            }

            template<typename TRowset, typename TRow>
            void ConstructFromRowset(const TRowset &rowset, TRow &row) const {
                Value.ConstructFromRowset(rowset, row);
            }

            template<typename TRow, typename... TArgs>
            auto PrepareUpdateRow(const TRow &row, std::tuple<TArgs...> &&tuple) const {
                return Value.PrepareUpdateRow(row, std::forward<std::tuple<TArgs...>>(tuple));
            }

            template<size_t Index, typename TUpdates, typename... TArgs>
            static void PopulateUpdateOp(TUpdates &updates, std::tuple<TArgs...> *tuple) {
                TCell::template PopulateUpdateOp<Index>(updates, tuple);
            }

            template<typename TCallback>
            void ForEachInlineTable(TCallback &&callback) const {
                Value.ForEachInlineTable(std::forward<TCallback>(callback));
            }
        };

        template<typename TCell, typename... TRest>
        struct TCellTuple<TCell, TRest...> {
            const TCellTuple<TCell> Value;
            const TCellTuple<TRest...> Rest;

            template<typename TCellV, typename... TRestV>
            constexpr TCellTuple(TCellV&& value, TRestV&&... rest)
                : Value(std::forward<TCellV>(value))
                , Rest(std::forward<TRestV>(rest)...)
            {}

            template<typename TRow>
            bool Equals(const TRow &x, const TRow &y) const {
                return Value.Equals(x, y) && Rest.Equals(x, y);
            }

            template<typename TRowset, typename TRow>
            void ConstructFromRowset(const TRowset &rowset, TRow &row) const {
                Value.ConstructFromRowset(rowset, row);
                Rest.ConstructFromRowset(rowset, row);
            }

            template<typename TRow, typename... TArgs>
            auto PrepareUpdateRow(const TRow &row, std::tuple<TArgs...> &&tuple) const {
                return Rest.PrepareUpdateRow(row, Value.PrepareUpdateRow(row, std::forward<std::tuple<TArgs...>>(tuple)));
            }

            template<size_t Index, typename TUpdates, typename... TArgs>
            static auto PopulateUpdateOp(TUpdates &updates, std::tuple<TArgs...> *tuple) {
                TCellTuple<TCell>::template PopulateUpdateOp<Index>(updates, tuple);
                TCellTuple<TRest...>::template PopulateUpdateOp<Index + 1>(updates, tuple);
            }

            template<typename TCallback>
            void ForEachInlineTable(TCallback &&callback) const {
                Value.ForEachInlineTable(callback);
                Rest.ForEachInlineTable(callback);
            }
        };




        template<typename T1, typename T2>
        struct TConcatTuple;

        template<typename T1, typename... T2>
        struct TConcatTuple<TCellTuple<T1>, TCellTuple<T2...>> {
            using Type = TCellTuple<T1, T2...>;
        };


        template<typename TRow, typename... TColumns>
        struct TCreateCellTuple;

        template<typename TRow, typename TFirstCol>
        struct TCreateCellTuple<TRow, TFirstCol> {
            using Type = TCellTuple<typename TMapColumnToCell<TRow, TFirstCol>::Type>;
        };

        template<typename TRow, typename TFirstCol, typename... TRest>
        struct TCreateCellTuple<TRow, TFirstCol, TRest...>
        {
            using Type = typename TConcatTuple<
                    typename TCreateCellTuple<TRow, TFirstCol>::Type,
                    typename TCreateCellTuple<TRow, TRest...>::Type
                >::Type;
        };

        template<typename TRow, typename... T>
        struct TColumnList {
            using TCellTuple = typename TCreateCellTuple<TRow, T...>::Type;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // TABLE FETCHER
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        template<typename TKey, typename TRowset>
        auto CreateFromRowset(const TRowset &rowset) -> decltype(TKey(std::declval<TRowset>().GetKey())) {
            return TKey(rowset.GetKey());
        }

        template<typename TKey, typename TRowset>
        auto CreateFromRowset(const TRowset &rowset) -> decltype(TKey::CreateFromRowset(std::declval<const TRowset&>())) {
            return TKey::CreateFromRowset(rowset);
        }

        template<typename TTableSelector>
        auto CreateRange(TTableSelector &&table) {
            return table.Range();
        }

        template<size_t Index, size_t Count, typename... TArgs>
        struct TUnrollRange {
            template<typename TTableSelector, typename... TPrefix>
            static auto CreateRange(TTableSelector &&table, const std::tuple<TArgs...> &key, TPrefix&&... prefix) {
                return TUnrollRange<Index + 1, Count, TArgs...>::CreateRange(std::forward<TTableSelector>(table),
                    key, std::forward<TPrefix>(prefix)..., std::get<Index>(key));
            }
        };

        template<size_t Count, typename... TArgs>
        struct TUnrollRange<Count, Count, TArgs...> {
            template<typename TTableSelector, typename... TPrefix>
            static auto CreateRange(TTableSelector &&table, const std::tuple<TArgs...>& /*key*/, TPrefix&&... prefix) {
                return table.Range(std::forward<TPrefix>(prefix)...);
            }
        };

        template<typename TTableSelector, typename... TArgs>
        auto CreateRange(TTableSelector &&table, const std::tuple<TArgs...> &prefixKey) {
            return TUnrollRange<0, sizeof...(TArgs), TArgs...>::CreateRange(std::forward<TTableSelector>(table),
                prefixKey);
        }

        template<typename T>        struct TIsTuple                       : std::false_type {};
        template<typename... TArgs> struct TIsTuple<std::tuple<TArgs...>> : std::true_type  {};

        template<typename TTableSelector, typename T>
        auto CreateRange(TTableSelector &&table, const T &prefixKey) {
            return table.Range(prefixKey);
        }

        template<typename Table, typename TParam, typename TKey, typename... TPrefixKey>
        bool FetchTable(NIceDb::TNiceDb &db, TParam /*param*/, TSet<TKey> &data, TPrefixKey&&... prefixKey) {
            auto rowset = CreateRange(db.Table<Table>(), std::forward<TPrefixKey>(prefixKey)...).Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (rowset.IsValid()) {
                data.emplace(CreateFromRowset<TKey>(rowset));
                if (!rowset.Next()) {
                    return false;
                }
            }

            return true;
        }

        template<typename Table>
        struct TConstructFromRowset {
            // use SFINAE here to prevent constructing Table from adapter of different Table; in this case generate runtime error
            template<typename TAdapter>
            using TCheckTable = typename std::enable_if<Table::TableId == TAdapter::Table::TableId>::type;

            template<typename TRowset, typename TRow, typename TAdapter>
            TConstructFromRowset(TRowset *rowset, TRow *item, TAdapter *adapter, TCheckTable<TAdapter>* = nullptr) {
                adapter->ConstructFromRowset(*rowset, *item);
            }

            TConstructFromRowset(...) {
                Y_ABORT("table mismatch");
            }
        };

        template<typename TRow>
        struct TRowTraits {
            using TBase = TRow;

            template<typename TMap, typename TKey, typename TValue>
            static void Insert(TMap& map, TKey&& key, TValue&& value) {
                map.emplace(std::forward<TKey>(key), std::forward<TValue>(value));
            }
        };

        template<typename TRow>
        struct TRowTraits<THolder<TRow>> {
            using TBase = TRow;

            template<typename TMap, typename TKey, typename TValue>
            static void Insert(TMap& map, TKey&& key, TValue&& value) {
                map.emplace(std::forward<TKey>(key), MakeHolder<TValue>(std::forward<TValue>(value)));
            }
        };

        template<typename Table, typename TParam, typename TKey, typename TRow, typename... TPrefixKey>
        bool FetchTable(NIceDb::TNiceDb &db, TParam param, TMap<TKey, TRow> &data, TPrefixKey&&... prefixKey) {
            using TTraits = TRowTraits<TRow>;
            using TBase = typename TTraits::TBase;

            auto rowset = db.Table<Table>().Range(std::forward<TPrefixKey>(prefixKey)...).Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (rowset.IsValid()) {
                TKey key = CreateFromRowset<TKey>(rowset);

                TBase item;
                TBase::Apply(param, [&](auto *adapter) { TConstructFromRowset<Table>(&rowset, &item, adapter); });

                struct TNotReady : yexception {};

                auto processInlineTable = [&](auto (TRow::*member), const auto *inlineTable) {
                    using TInlineTable = std::remove_pointer_t<decltype(inlineTable)>;
                    if (!FetchTable<TInlineTable>(db, param, item.*member, key)) {
                        ythrow TNotReady();
                    }
                };
                try {
                    TBase::Apply(param, [&](auto *adapter) { adapter->ForEachInlineTable(processInlineTable); });
                } catch (const TNotReady&) {
                    return false;
                }

                TTraits::Insert(data, std::move(key), std::move(item));

                if (!rowset.Next()) {
                    return false;
                }
            }

            return true;
        }

    } // NTableAdapter

    template<typename TTable, typename TRow, typename... TColumns>
    class TTableAdapter {
        using TColumnList = NTableAdapter::TColumnList<TRow, TColumns...>;
        using TCells = typename TColumnList::TCellTuple;

    private:
        const TCells Cells;

    public:
        using Table = TTable;

        template<typename... TArgs>
        constexpr TTableAdapter(TArgs&&... args)
            : Cells(std::forward<TArgs>(args)...)
        {}

        bool Equals(const TRow &x, const TRow &y) const {
            return Cells.Equals(x, y);
        }

        template<typename TRowset>
        void ConstructFromRowset(const TRowset &rowset, TRow &row) const {
            return Cells.ConstructFromRowset(rowset, row);
        }

        template<typename TKey>
        void IssueUpdateRow(NTabletFlatExecutor::TTransactionContext &txc, const TKey &key, const TRow &row) const {
            auto x = NTableAdapter::WrapTuple(key);
            auto keyTuple = NTableAdapter::PrepareKeyTuple<TTable>(&x);
            TStackVec<TRawTypeValue, std::tuple_size<decltype(keyTuple)>::value> keyForTable;
            NTableAdapter::MapKey<TTable>(&keyTuple, keyForTable);

            // prepare data row in single tuple to obtain correct references in TRawTypeValue
            auto data = Cells.PrepareUpdateRow(row, std::make_tuple());

            // create vector of references to row cell values
            TStackVec<NTable::TUpdateOp, std::tuple_size<decltype(data)>::value> updates;
            TCells::template PopulateUpdateOp<0>(updates, &data);

            txc.DB.Update(TTable::TableId, NTable::ERowOp::Upsert, keyForTable, updates);
        }

        template<typename TKey>
        void IssueEraseRow(NTabletFlatExecutor::TTransactionContext &txc, const TKey &key) const {
            auto x = NTableAdapter::WrapTuple(key);
            auto keyTuple = NTableAdapter::PrepareKeyTuple<TTable>(&x);
            TStackVec<TRawTypeValue, std::tuple_size<decltype(keyTuple)>::value> keyForTable;
            NTableAdapter::MapKey<TTable>(&keyTuple, keyForTable);

            txc.DB.Update(TTable::TableId, NTable::ERowOp::Erase, keyForTable, { });
        }

        template<typename TCallback>
        void ForEachInlineTable(TCallback &&callback) const {
            Cells.ForEachInlineTable(std::forward<TCallback>(callback));
        }
    };

} // NKikimr
