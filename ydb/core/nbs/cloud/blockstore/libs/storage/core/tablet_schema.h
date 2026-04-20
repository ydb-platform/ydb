#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <
    ui32 channel = 1,
    NKikimr::NTable::NPage::ECache cache = NKikimr::NTable::NPage::ECache::None,
    NKikimr::NTable::NPage::ECodec codec =
        NKikimr::NTable::NPage::ECodec::Plain>
struct TStoragePolicy
{
    static constexpr ui32 Room = 0;
    static constexpr ui32 Channel = channel;

    static constexpr ui32 Family = 0;
    static constexpr NKikimr::NTable::NPage::ECache Cache = cache;
    static constexpr NKikimr::NTable::NPage::ECodec Codec = codec;
};

////////////////////////////////////////////////////////////////////////////////

enum class ECompactionPolicy
{
    None,
    Default,
    IndexTable,
};

template <ECompactionPolicy compactionPolicy = ECompactionPolicy::None>
struct TCompactionPolicy
{
    static constexpr ECompactionPolicy CompactionPolicy = compactionPolicy;
};

void InitCompactionPolicy(
    NKikimr::NTable::TAlter& alter,
    ui32 tableId,
    ECompactionPolicy сompactionPolicy);

////////////////////////////////////////////////////////////////////////////////

template <NKikimr::NIceDb::TTableId tableId>
struct TTableSchema: public NKikimr::NIceDb::Schema::Table<tableId>
{
    using StoragePolicy = TStoragePolicy<>;
    using CompactionPolicy = TCompactionPolicy<>;
};

////////////////////////////////////////////////////////////////////////////////

template <typename Type>
struct TSchemaInitializer;

template <typename Type>
struct TSchemaInitializer<NKikimr::NIceDb::Schema::SchemaTables<Type>>
{
    static void InitStorage(NKikimr::NTable::TAlter& alter)
    {
        alter.SetRoom(
            Type::TableId,
            Type::StoragePolicy::Room,
            Type::StoragePolicy::Channel,
            {Type::StoragePolicy::Channel},
            Type::StoragePolicy::Channel);

        alter.SetFamily(
            Type::TableId,
            Type::StoragePolicy::Family,
            Type::StoragePolicy::Cache,
            Type::StoragePolicy::Codec);

        InitCompactionPolicy(
            alter,
            Type::TableId,
            Type::CompactionPolicy::CompactionPolicy);
    }
};

template <typename Type, typename... Types>
struct TSchemaInitializer<NKikimr::NIceDb::Schema::SchemaTables<Type, Types...>>
{
    static void InitStorage(NKikimr::NTable::TAlter& alter)
    {
        TSchemaInitializer<
            NKikimr::NIceDb::Schema::SchemaTables<Type>>::InitStorage(alter);
        TSchemaInitializer<NKikimr::NIceDb::Schema::SchemaTables<Types...>>::
            InitStorage(alter);
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage
