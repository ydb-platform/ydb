#pragma once

#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <yt/cpp/mapreduce/interface/io.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/flags.h>

namespace NYql {

class TMkqlIOSpecs {
public:
    enum class ESystemField: ui32 {
        KeySwitch   = 1 << 0,
        RowIndex    = 1 << 1,
        RangeIndex  = 1 << 2,
    };

    Y_DECLARE_FLAGS(TSystemFields, ESystemField);

    struct TSpecInfo {
        NKikimr::NMiniKQL::TType* Type = nullptr;
        bool StrictSchema = true;
        THashMap<TString, TString> DefaultValues;
        THashMap<TString, NKikimr::NMiniKQL::TType*> AuxColumns;
        ui64 NativeYtTypeFlags = 0;
        bool Dynamic = false;
        THashSet<TString> SysColumns;
        THashSet<TString> ExplicitYson;
    };

    struct TDecoderSpec {
        struct TDecodeField {
            TString Name;
            ui32 StructIndex = 0;
            NKikimr::NMiniKQL::TType* Type = nullptr;
            bool Virtual = false;
            bool ExplicitYson = false;
        };

        TMaybe<ui32> OthersStructIndex; // filled if scheme is not strict
        NKikimr::NMiniKQL::TKeyTypes OthersKeyTypes;
        THashMap<TString, TDecodeField> Fields;
        TVector<TDecodeField> FieldsVec;
        ui32 StructSize = 0; // Number of visible columns after decoding (excludes all aux columns)
        ui32 SkiffSize = 0; // Number of columns expected by skiff (includes all visible columns and explicitly requested aux columns)
        TVector<NKikimr::NUdf::TUnboxedValue> DefaultValues;
        ui64 NativeYtTypeFlags = 0;
        bool Dynamic = false;
        TMaybe<ui32> FillSysColumnPath;
        TMaybe<ui32> FillSysColumnRecord;
        TMaybe<ui32> FillSysColumnIndex;
        TMaybe<ui32> FillSysColumnNum;
        TMaybe<ui32> FillSysColumnKeySwitch;
    };

    struct TEncoderSpec {
        NKikimr::NMiniKQL::TStructType* RowType = nullptr;
        ui64 NativeYtTypeFlags = 0;
    };

public:
    // Job specific initialization
    void Init(NCommon::TCodecContext& codecCtx,
        const TString& inputSpecs,
        const TVector<ui32>& inputGroups,
        const TVector<TString>& tableNames,
        NKikimr::NMiniKQL::TType* itemType,
        const THashSet<TString>& auxColumns,
        const TString& outSpecs,
        NKikimr::NMiniKQL::IStatsRegistry* jobStats = nullptr
    );

    // Job specific initialization
    void Init(NCommon::TCodecContext& codecCtx,
        const NYT::TNode& inAttrs,
        const TVector<ui32>& inputGroups,
        const TVector<TString>& tableNames,
        NKikimr::NMiniKQL::TType* itemType,
        const THashSet<TString>& auxColumns,
        const NYT::TNode& outAttrs,
        NKikimr::NMiniKQL::IStatsRegistry* jobStats = nullptr
    );

    // Pull specific initialization
    void Init(NCommon::TCodecContext& codecCtx,
        const TString& inputSpecs,
        const TVector<TString>& tableNames,
        const TMaybe<TVector<TString>>& columns // Use Nothing to select all columns in original order
    );

    // Pull specific initialization
    void Init(NCommon::TCodecContext& codecCtx,
        const NYT::TNode& inAttrs,
        const TVector<TString>& tableNames,
        const TMaybe<TVector<TString>>& columns // Use Nothing to select all columns in original order
    );

    // Fill specific initialization
    void Init(NCommon::TCodecContext& codecCtx,
        const TString& outSpecs
    );

    // Fill specific initialization
    void Init(NCommon::TCodecContext& codecCtx,
        const NYT::TNode& outAttrs
    );

    void SetUseSkiff(const TString& optLLVM, TSystemFields sysFields = {}) {
        UseSkiff_ = true;
        OptLLVM_ = optLLVM;
        SystemFields_ = sysFields;
    }

    void SetTableOffsets(const TVector<ui64>& offsets);

    void Clear();

    static void LoadSpecInfo(bool inputSpec, const NYT::TNode& attrs, NCommon::TCodecContext& codecCtx, TSpecInfo& info);

    NYT::TFormat MakeInputFormat(const THashSet<TString>& auxColumns) const; // uses Inputs
    NYT::TFormat MakeInputFormat(size_t tableIndex) const; // uses Inputs
    NYT::TFormat MakeOutputFormat() const; // uses Outputs
    NYT::TFormat MakeOutputFormat(size_t tableIndex) const; // uses Outputs

public:
    bool UseSkiff_ = false;
    TString OptLLVM_;
    TSystemFields SystemFields_;

    NKikimr::NMiniKQL::IStatsRegistry* JobStats_ = nullptr;
    THashMap<TString, TDecoderSpec> Decoders;
    TVector<const TDecoderSpec*> Inputs;
    TVector<ui32> InputGroups; // translation of tableindex->index of Inputs
    TVector<TEncoderSpec> Outputs;

    NYT::TNode InputSpec;
    NYT::TNode OutputSpec;
    TVector<NKikimr::NUdf::TUnboxedValue> TableNames;
    TVector<ui64> TableOffsets;

protected:
    void PrepareInput(const TVector<ui32>& inputGroups);

    void InitInput(NCommon::TCodecContext& codecCtx,
        const NYT::TNode& inAttrs,
        const TVector<ui32>& inputGroups,
        const TVector<TString>& tableNames,
        NKikimr::NMiniKQL::TType* itemType,
        const TMaybe<TVector<TString>>& columns,
        const THashSet<TString>& auxColumns
    );

    void InitDecoder(NCommon::TCodecContext& codecCtx,
        const TSpecInfo& specInfo,
        const THashMap<TString, ui32>& structColumns,
        const THashSet<TString>& auxColumns,
        TDecoderSpec& decoder
    );

    void InitOutput(NCommon::TCodecContext& codecCtx,
        const NYT::TNode& outAttrs
    );
};

Y_DECLARE_OPERATORS_FOR_FLAGS(TMkqlIOSpecs::TSystemFields);

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TMkqlIOCache {
public:
    TMkqlIOCache(const TMkqlIOSpecs& specs, const NKikimr::NMiniKQL::THolderFactory& holderFactory);

    NKikimr::NUdf::TUnboxedValue NewRow(size_t tableIndex, NKikimr::NUdf::TUnboxedValue*& items) {
        const auto group = Specs_.InputGroups.empty() ? 0 : Specs_.InputGroups[tableIndex];
        return RowCache_[group]->NewArray(HolderFactory, Specs_.Inputs[tableIndex]->StructSize, items);
    }

    const TMkqlIOSpecs& GetSpecs() const {
        return Specs_;
    }
    const NKikimr::NMiniKQL::THolderFactory& GetHolderFactory() {
        return HolderFactory;
    }

    ui32 GetMaxOthersFields(size_t tableIndex) const {
        return DecoderCache_.at(tableIndex).MaxOthersFields_;
    }

    void UpdateMaxOthersFields(size_t tableIndex, ui32 maxOthersFields) {
        DecoderCache_[tableIndex].MaxOthersFields_ = Max<ui32>(DecoderCache_.at(tableIndex).MaxOthersFields_, maxOthersFields);
    }

    TVector<const TMkqlIOSpecs::TDecoderSpec::TDecodeField*>& GetLastFields(size_t tableIndex) {
        return DecoderCache_.at(tableIndex).LastFields_;
    }


private:
    const TMkqlIOSpecs& Specs_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    TVector<THolder<NKikimr::NMiniKQL::TPlainContainerCache>> RowCache_;

    struct TDecoderCache {
        ui32 MaxOthersFields_ = 0;
        TVector<const TMkqlIOSpecs::TDecoderSpec::TDecodeField*> LastFields_;
    };
    TVector<TDecoderCache> DecoderCache_;
};


//////////////////////////////////////////////////////////////////////////////////////////////////////////

class IMkqlReaderImpl : public NYT::IReaderImplBase {
public:
    virtual ~IMkqlReaderImpl() = default;
    virtual NKikimr::NUdf::TUnboxedValue GetRow() const = 0;
};

using IMkqlReaderImplPtr = TIntrusivePtr<IMkqlReaderImpl>;

class IMkqlWriterImpl : public TThrRefBase {
public:
    virtual ~IMkqlWriterImpl() = default;
    virtual void AddRow(const NUdf::TUnboxedValuePod row) = 0;
    virtual void AddFlatRow(const NUdf::TUnboxedValuePod* row) = 0;
    virtual void Finish() = 0;
    virtual void Abort() = 0;
};

using IMkqlWriterImplPtr = TIntrusivePtr<IMkqlWriterImpl>;

} // NYql
