#pragma once

#include <ydb/core/kqp/gateway/kqp_gateway.h>

#include <library/cpp/json/json_value.h>

#include <memory>

struct TMetadataInfoHolder {
    const THashMap<TString, NYql::TKikimrTableMetadataPtr> TableMetadata;
    THashMap<TString, NYql::TKikimrTableMetadataPtr> Indexes;

    explicit TMetadataInfoHolder(THashMap<TString, NYql::TKikimrTableMetadataPtr>&& tableMetadata);

    THashMap<TString, NYql::TKikimrTableMetadataPtr>::const_iterator find(const TString& key) const {
        return TableMetadata.find(key);
    }

    THashMap<TString, NYql::TKikimrTableMetadataPtr>::const_iterator end() const {
        return TableMetadata.end();
    }

    const NYql::TKikimrTableMetadataPtr* FindPtr(const TString& key) const;
};

THashMap<TString, NYql::TKikimrTableMetadataPtr> ExtractStaticMetadata(const NJson::TJsonValue& data);
