#pragma once

#include "yql_yt_op_settings.h"

#include <ydb/library/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <yt/cpp/mapreduce/interface/node.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/variant.h>
#include <util/generic/maybe.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>

#include <utility>

namespace NYql {

class TYtKey;

struct TYtTableStatInfo: public TThrRefBase {
    using TPtr = TIntrusivePtr<TYtTableStatInfo>;

    TYtTableStatInfo() {
    }

    TYtTableStatInfo(NNodes::TExprBase node) {
        Parse(node);
    }
    TYtTableStatInfo(TExprNode::TPtr node)
        : TYtTableStatInfo(NNodes::TExprBase(node))
    {
    }

    static bool Validate(const TExprNode& node, TExprContext& ctx);
    void Parse(NNodes::TExprBase node);
    NNodes::TExprBase ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const;

    bool IsEmpty() const {
        return 0 == RecordsCount;
    }

    NNodes::TMaybeNode<NNodes::TExprBase> FromNode;
    TString Id;
    ui64 RecordsCount = 0;
    ui64 DataSize = 0;
    ui64 ChunkCount = 0;
    ui64 ModifyTime = 0;
    ui64 Revision = 0;
    ui64 TableRevision = 0; // Not serializable
};

struct TYtTableMetaInfo: public TThrRefBase {
    using TPtr = TIntrusivePtr<TYtTableMetaInfo>;

    TYtTableMetaInfo() {
    }
    TYtTableMetaInfo(NNodes::TExprBase node) {
        Parse(node);
    }
    TYtTableMetaInfo(TExprNode::TPtr node)
        : TYtTableMetaInfo(NNodes::TExprBase(node))
    {
    }

    static bool Validate(const TExprNode& node, TExprContext& ctx);
    void Parse(NNodes::TExprBase node);
    NNodes::TExprBase ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const;

    NNodes::TMaybeNode<NNodes::TExprBase> FromNode;
    bool CanWrite = true;
    bool DoesExist = false;
    bool YqlCompatibleScheme = false;
    bool InferredScheme = false;
    bool IsDynamic = false;
    TString SqlView;
    ui16 SqlViewSyntaxVersion = 1;

    THashMap<TString, TString> Attrs;
};

struct TEpochInfo {
    static bool Validate(const TExprNode& node, TExprContext& ctx);
    static TMaybe<ui32> Parse(const TExprNode& node);
    static NNodes::TExprBase ToExprNode(const TMaybe<ui32>& epoch, TExprContext& ctx, const TPositionHandle& pos);
};

struct TYtTableBaseInfo: public TThrRefBase {
    using TPtr = TIntrusivePtr<TYtTableBaseInfo>;

    TYtTableBaseInfo() = default;
    TYtTableBaseInfo(const TYtKey& key, TStringBuf cluster);

    virtual NNodes::TExprBase ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const = 0;

    NYT::TNode GetCodecSpecNode(const NCommon::TStructMemberMapper& mapper = {}) const;
    NYT::TNode GetAttrSpecNode(ui64 nativeTypeCompatibility, bool rowSpecCompactForm) const;

    static TPtr Parse(NNodes::TExprBase node);
    static TYtTableMetaInfo::TPtr GetMeta(NNodes::TExprBase node);
    static TYqlRowSpecInfo::TPtr GetRowSpec(NNodes::TExprBase node);
    static TYtTableStatInfo::TPtr GetStat(NNodes::TExprBase node);
    static TStringBuf GetTableName(NNodes::TExprBase node);

    bool RequiresRemap() const;
    bool HasSameScheme(const TTypeAnnotationNode& scheme) const;
    bool HasSamePhysicalScheme(const TYtTableBaseInfo& info) const;

    NNodes::TMaybeNode<NNodes::TExprBase> FromNode;
    TString Name;
    TString Cluster;
    TYqlRowSpecInfo::TPtr RowSpec;
    TYtTableMetaInfo::TPtr Meta;
    TYtTableStatInfo::TPtr Stat;
    NNodes::TMaybeNode<NNodes::TExprBase> Settings;
    bool IsTemp = false;
    bool IsAnonymous = false;
    bool IsUnordered = false;
    TMaybe<ui32> Epoch; // Epoch, from which the table is read
    TMaybe<ui32> CommitEpoch; // Epoch, in which the table modifications became accessible
};

struct TYtTableInfo: public TYtTableBaseInfo {
    using TPtr = TIntrusivePtr<TYtTableInfo>;

    TYtTableInfo() = default;
    TYtTableInfo(const TYtKey& key, TStringBuf cluster);
    TYtTableInfo(NNodes::TExprBase node, bool useTypes = true) {
        Parse(node, useTypes);
    }
    TYtTableInfo(TExprNode::TPtr node)
        : TYtTableInfo(NNodes::TExprBase(node))
    {
    }

    static TStringBuf GetTableLabel(NNodes::TExprBase node);
    static bool HasSubstAnonymousLabel(NNodes::TExprBase node);

    static bool Validate(const TExprNode& node, EYtSettingTypes accepted, TExprContext& ctx);
    void Parse(NNodes::TExprBase node, bool useTypes = true);
    NNodes::TExprBase ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const override;
};

struct TYtOutTableInfo: public TYtTableBaseInfo {
    using TPtr = TIntrusivePtr<TYtOutTableInfo>;

    TYtOutTableInfo() {
        IsTemp = true;
    }
    TYtOutTableInfo(const TStructExprType* type, ui64 nativeYtTypeFlags);
    TYtOutTableInfo(NNodes::TExprBase node) {
        Parse(node);
        IsTemp = true;
    }
    TYtOutTableInfo(TExprNode::TPtr node)
        : TYtOutTableInfo(NNodes::TExprBase(node))
    {
    }

    static bool Validate(const TExprNode& node, TExprContext& ctx);
    void Parse(NNodes::TExprBase node);
    NNodes::TExprBase ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const override;

    TYtOutTableInfo& SetUnique(const TDistinctConstraintNode* distinct, const TPositionHandle& pos, TExprContext& ctx);
    NYT::TNode GetColumnGroups() const;
};

struct TYtRangesInfo: public TThrRefBase {
    using TPtr = TIntrusivePtr<TYtRangesInfo>;

    struct TRowSingle {
        ui64 Offset = Max<ui64>();
    };
    struct TRowRange {
        TMaybe<ui64> Lower;
        TMaybe<ui64> Upper;
    };
    struct TKeySingle {
        TVector<NNodes::TExprBase> Key;
    };
    struct TKeyRange {
        TVector<NNodes::TExprBase> Lower;
        bool LowerInclude = true;
        TVector<NNodes::TExprBase> Upper;
        bool UpperInclude = false;
        bool UseKeyBoundApi = DEFAULT_USE_KEY_BOUND_API;
    };
    using TRange = std::variant<TRowSingle, TRowRange, TKeySingle, TKeyRange>;

    TYtRangesInfo() {
    }
    TYtRangesInfo(NNodes::TExprBase node) {
        Parse(node);
    }
    TYtRangesInfo(TExprNode::TPtr node)
        : TYtRangesInfo(NNodes::TExprBase(node))
    {
    }

    static bool Validate(const TExprNode& node, TExprContext& ctx, bool exists, const TYqlRowSpecInfo::TPtr& rowSpec = {});
    void Parse(NNodes::TExprBase node);
    void Parse(const TVector<NYT::TReadRange>& ranges, TExprContext& ctx, const TPositionHandle& pos = {});
    void AddRowRange(const TRowRange& range);
    void SetUseKeyBoundApi(bool useKeyBoundApi);
    NNodes::TExprBase ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const;
    void FillRichYPath(NYT::TRichYPath& path, size_t keyColumnsCount) const;
    TMaybe<ui64> GetUsedRows(ui64 tableRowCount) const;
    size_t GetRangesCount() const;
    size_t GetUsedKeyPrefixLength() const;
    bool IsEmpty() const;
    TVector<TRange> GetRanges() const;
    static TYtRangesInfo::TPtr ApplyLegacyKeyFilters(const TVector<NNodes::TExprBase>& keyFilters,
        const TYqlRowSpecInfo::TPtr& rowSpec, TExprContext& ctx);
    static TYtRangesInfo::TPtr ApplyKeyFilter(const TExprNode& keyFilter);
    static TYtRangesInfo::TPtr MakeEmptyRange();

private:
    TVector<TRange> Ranges;
};

struct TYtColumnsInfo: public TThrRefBase {
    using TPtr = TIntrusivePtr<TYtColumnsInfo>;

    struct TColumn {
        TString Name;
        TString Type;
    };

    TYtColumnsInfo() = default;

    TYtColumnsInfo(NNodes::TExprBase node)
    {
        Parse(node);
    }

    TYtColumnsInfo(TExprNode::TPtr node)
        : TYtColumnsInfo(NNodes::TExprBase(node))
    {
    }

    static bool Validate(TExprNode& node, TExprContext& ctx);
    void Parse(NNodes::TExprBase node);

    NNodes::TExprBase ToExprNode(TExprContext& ctx, const TPositionHandle& pos, const THashSet<TString>* filter = nullptr) const;

    bool HasOthers() const {
        return Others;
    }

    void FillRichYPath(NYT::TRichYPath& path, bool withColumns) const;

    bool HasColumns() const {
        return Columns.Defined();
    }

    template <class TContainer>
    void SetColumns(const TContainer& columns) {
        Columns.ConstructInPlace();
        Others = false;
        for (auto& col: columns) {
            Columns->emplace_back(TColumn{::ToString(col), TString()});
            UpdateOthers(col);
        }
    }

    void SetRenames(const THashMap<TString, TString>& renames) {
        YQL_ENSURE(!Renames.Defined());
        if (HasColumns()) {
            for (auto& col : *Columns) {
                auto r = renames.find(col.Name);
                if (r != renames.end()) {
                    col.Name = r->second;
                }
            }
        }
        Renames.ConstructInPlace(renames);
    }

    const TMaybe<TVector<TColumn>>& GetColumns() const {
        return Columns;
    };

    const TMaybe<THashMap<TString, TString>>& GetRenames() const {
        return Renames;
    };

private:
    void UpdateOthers(TStringBuf col);

    TMaybe<THashMap<TString, TString>> Renames;
    TMaybe<TVector<TColumn>> Columns;
    bool Others = false;
};

// TODO: 1) additional fields, which missing in original table 2) field type converting
struct TYtPathInfo: public TThrRefBase {
    using TPtr = TIntrusivePtr<TYtPathInfo>;

    TYtPathInfo() {
    }
    TYtPathInfo(NNodes::TExprBase node) {
        Parse(node);
    }
    TYtPathInfo(TExprNode::TPtr node)
        : TYtPathInfo(NNodes::TExprBase(node))
    {
    }

    static bool Validate(const TExprNode& node, TExprContext& ctx);
    void Parse(NNodes::TExprBase node);
    NNodes::TExprBase ToExprNode(TExprContext& ctx, const TPositionHandle& pos, NNodes::TExprBase table) const;
    void FillRichYPath(NYT::TRichYPath& path) const;
    IGraphTransformer::TStatus GetType(const TTypeAnnotationNode*& filtered, TExprNode::TPtr& newFields,
        TExprContext& ctx, const TPositionHandle& pos) const;

    bool HasColumns() const {
        return Columns && Columns->HasColumns();
    }

    template <class TContainer>
    void SetColumns(const TContainer& columns) {
        if (!Columns) {
            Columns = MakeIntrusive<TYtColumnsInfo>();
        }
        Columns->SetColumns(columns);
    }

    NYT::TNode GetCodecSpecNode();
    TString GetCodecSpecStr();
    bool RequiresRemap() const;
    ui64 GetNativeYtTypeFlags();
    TMaybe<NYT::TNode> GetNativeYtType();

    NNodes::TMaybeNode<NNodes::TExprBase> FromNode;
    TYtTableBaseInfo::TPtr Table;
    TYtColumnsInfo::TPtr Columns;
    TYtRangesInfo::TPtr Ranges;
    TYtTableStatInfo::TPtr Stat;
    TMaybe<TString> AdditionalAttributes;
private:
    const NCommon::TStructMemberMapper& GetColumnMapper();

    TMaybe<NCommon::TStructMemberMapper> Mapper_;
};

}
