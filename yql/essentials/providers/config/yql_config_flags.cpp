#include "yql_config_flags.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>

namespace NYql {

TConfigFlags::TConfigFlags(TTypeAnnotationContext& types, TAllowSettingPolicy policy, bool forPartialTypeCheck)
    : Types_(types)
    , ForPartialTypeCheck_(forPartialTypeCheck)
    , Policy_(std::move(policy))
{
    InitFlags();
}

TConfigFlags::TFlagHandler TConfigFlags::ToggleHandler(bool* field, TStringBuf enableName) {
    return [field, enableName](TStringBuf name, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        *field = (name == enableName);
        return true;
    };
};

TConfigFlags::TFlagHandler TConfigFlags::ToggleMaybeHandler(TMaybe<bool>* field, TStringBuf enableName) {
    return [field, enableName](TStringBuf name, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        *field = (name == enableName);
        return true;
    };
};

TConfigFlags::TFlagHandler TConfigFlags::OptionalBoolHandler(bool* field) {
    return [field](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() > 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at most 1 argument, but got " << args.size()));
            return false;
        }
        bool res = true;
        if (!args.empty()) {
            if (!TryFromString(args[0], res)) {
                ctx.AddError(TIssue(pos, TStringBuilder() << "Expected bool, but got: " << args[0]));
                return false;
            }
        }
        *field = res;
        return true;
    };
};

TConfigFlags::TFlagHandler TConfigFlags::BoolHandler(bool* field, bool value) {
    return [field, value](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        *field = value;
        return true;
    };
};

TConfigFlags::TFlagHandler TConfigFlags::IntegerCtxFlagHandler(ui64 TExprContext::*field) {
    return [field](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        if (!TryFromString(args[0], ctx.*field)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected integer, but got: " << args[0]));
            return false;
        }
        return true;
    };
};

TConfigFlags::TFlagHandler TConfigFlags::Ui32Handler(ui32* field) {
    return [field](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        if (!TryFromString(args[0], *field)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected non-negative integer, but got: " << args[0]));
            return false;
        }
        return true;
    };
};

TConfigFlags::TFlagHandler TConfigFlags::Ui64Handler(ui64* field) {
    return [field](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        if (!TryFromString(args[0], *field)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected integer, but got: " << args[0]));
            return false;
        }
        return true;
    };
};

TConfigFlags::TFlagHandler TConfigFlags::NoopHandler() {
    return [](TStringBuf, const TPosition&, const TVector<TStringBuf>&, TExprContext&) {
        return true;
    };
};

bool TConfigFlags::IsSettingAllowed(const TPosition& pos, TStringBuf name, TExprContext& ctx) {
    if (Policy_ && !Policy_(name)) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Changing setting " << name << " is not allowed"));
        return false;
    }
    return true;
}

bool TConfigFlags::ApplyFlag(const TPosition& pos, const TStringBuf name, const TVector<TStringBuf>& args, TExprContext& ctx, bool fromInitialize) {
    if (!IsSettingAllowed(pos, name, ctx)) {
        return false;
    }

    auto it = Flags_.find(name);
    if (it == Flags_.end()) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Unsupported command: " << name));
        return false;
    }

    if (it->second.System && !fromInitialize) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Changing system setting " << name << " is not allowed"));
        return false;
    }

    return it->second.Handler(name, pos, args, ctx);
}

void TConfigFlags::AddFlag(TStringBuf name, TStringBuf disableName, bool system, TFlagHandler handler) {
    TFlag flag{.Name = name, .System = system, .Handler = std::move(handler)};
    Flags_[name] = flag;
    if (!disableName.empty()) {
        flag.System = false; // Always allow disable pragma
        Flags_[disableName] = std::move(flag);
    }
}

void TConfigFlags::InitFlags() {
    AddFlag("UnsecureCredential", {}, /*system=*/false, DelegateHandler(&TConfigFlags::AddCredential));
    AddFlag("ImportUdfs", {}, /*system=*/false, DelegateHandler(&TConfigFlags::ImportUdfs));
    AddFlag("AddFileByUrl", {}, /*system=*/false, DelegateHandler(&TConfigFlags::AddFileByUrl));
    AddFlag("SetFileOption", {}, /*system=*/false, DelegateHandler(&TConfigFlags::SetFileOption));
    AddFlag("AddFolderByUrl", {}, /*system=*/false, DelegateHandler(&TConfigFlags::AddFolderByUrl));
    AddFlag("SetPackageVersion", {}, /*system=*/false, DelegateHandler(&TConfigFlags::SetPackageVersion));
    AddFlag(TStringBuf("Warning"), {}, /*system=*/false, DelegateHandler(&TConfigFlags::SetWarningRule));

    AddFlag("ValidateUdf", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        try {
            Types_.ValidateMode = NKikimr::NUdf::ValidateModeByStr(TString(args[0]));
        } catch (const yexception& err) {
            ctx.AddError(TIssue(pos, TStringBuilder() << err.AsStrBuf() << ", available modes: " << NKikimr::NUdf::ValidateModeAvailables()));
            return false;
        }
        return true;
    });

    AddFlag("UdfBridge", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        if (ForPartialTypeCheck_) {
            return true;
        }
        if (Types_.UdfBridgeBinaryPath.empty()) {
            ctx.AddError(TIssue(pos, "udf_bridge is not available"));
            return false;
        }
        Types_.BridgeMode = NKikimr::NUdf::EBridgeMode::OutProcess;
        return true;
    });

    AddFlag("LLVM_OFF", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        Types_.OptLLVM = "OFF";
        return true;
    });

    AddFlag("LLVM", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() > 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at most 1 argument, but got " << args.size()));
            return false;
        }
        Types_.OptLLVM = args.empty() ? TString() : TString(args[0]);
        return true;
    });

    AddFlag("RuntimeLogLevel", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        auto value = NUdf::TryLevelFromString(args[0]);
        if (!value) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Invalid log level value: " << args[0]));
            return false;
        }
        Types_.RuntimeLogLevel = *value;
        return true;
    });

    AddFlag("NodesAllocationLimit", {}, /*system=*/false, IntegerCtxFlagHandler(&TExprContext::NodesAllocationLimit));
    AddFlag("StringsAllocationLimit", {}, /*system=*/false, IntegerCtxFlagHandler(&TExprContext::StringsAllocationLimit));
    AddFlag("RepeatTransformLimit", {}, /*system=*/false, IntegerCtxFlagHandler(&TExprContext::RepeatTransformLimit));
    AddFlag("TypeAnnNodeRepeatLimit", {}, /*system=*/false, IntegerCtxFlagHandler(&TExprContext::TypeAnnNodeRepeatLimit));

    AddFlag("TransformCycleDetector", {}, /*system=*/false, [](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        ui64 cnt;
        if (!TryFromString(args[0], cnt)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected integer, but got: " << args[0]));
            return false;
        }
        if (!ctx.CycleDetector) {
            ctx.CycleDetector.ConstructInPlace(cnt);
        }
        return true;
    });

    AddFlag("PureDataSource", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        if (ForPartialTypeCheck_) {
            return true;
        }
        auto dataSource = args[0];
        if (Find(Types_.AvailablePureResultDataSources, dataSource) == Types_.AvailablePureResultDataSources.end()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Unsupported datasource for result provider: " << dataSource));
            return false;
        }
        if (auto p = Types_.DataSourceMap.FindPtr(dataSource)) {
            if ((*p)->GetName() != dataSource) {
                dataSource = (*p)->GetName();
            }
        } else {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Unknown datasource for result provider: " << dataSource));
            return false;
        }
        Types_.PureResultDataSource = dataSource;
        return true;
    });

    AddFlag("FullResultDataSink", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        auto dataSink = args[0];
        if (auto p = Types_.DataSinkMap.FindPtr(dataSink)) {
            if ((*p)->GetName() != dataSink) {
                dataSink = (*p)->GetName();
            }
        } else {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Unknown datasink for full result provider: " << dataSink));
            return false;
        }
        Types_.FullResultDataSink = dataSink;
        return true;
    });

    AddFlag("Diagnostics", {}, /*system=*/false, BoolHandler(&Types_.Diagnostics, /*value=*/true));

    AddFlag("UdfSupportsYield", {}, /*system=*/false, OptionalBoolHandler(&Types_.UdfSupportsYield));

    AddFlag("EvaluateForLimit", {}, /*system=*/false, Ui32Handler(&Types_.EvaluateForLimit));

    AddFlag("EvaluateParallelForLimit", {}, /*system=*/false, Ui32Handler(&Types_.EvaluateParallelForLimit));

    AddFlag("PullUpFlatMapOverJoin", "DisablePullUpFlatMapOverJoin", /*system=*/false,
            ToggleHandler(&Types_.PullUpFlatMapOverJoin, "PullUpFlatMapOverJoin"));

    AddFlag("FilterPushdownOverJoinOptionalSide", "DisableFilterPushdownOverJoinOptionalSide", /*system=*/false,
            ToggleHandler(&Types_.FilterPushdownOverJoinOptionalSide, "FilterPushdownOverJoinOptionalSide"));

    AddFlag("RotateJoinTree", {}, /*system=*/false, OptionalBoolHandler(&Types_.RotateJoinTree));

    AddFlag("SQL", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() > 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at most 1 argument, but got " << args.size()));
            return false;
        }
        Types_.DeprecatedSQL = args.empty() ? false : (args[0] == "0");
        return true;
    });

    AddFlag("DisableConstraintCheck", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at least 1 argument, but got " << args.size()));
            return false;
        }
        for (auto arg : args) {
            Types_.DisableConstraintCheck.emplace(arg);
        }
        return true;
    });

    AddFlag("EnableConstraintCheck", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at least 1 argument, but got " << args.size()));
            return false;
        }
        for (auto arg : args) {
            Types_.DisableConstraintCheck.erase(TString{arg});
        }
        return true;
    });

    AddFlag("DisableConstraints", {}, /*system=*/false, [](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at least 1 argument, but got " << args.size()));
            return false;
        }
        for (auto arg : args) {
            ctx.DisabledConstraints.emplace(arg);
        }
        return true;
    });

    AddFlag("EnableConstraints", {}, /*system=*/false, [](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at least 1 argument, but got " << args.size()));
            return false;
        }
        for (auto arg : args) {
            ctx.DisabledConstraints.erase(arg);
        }
        return true;
    });

    AddFlag("UseTableMetaFromGraph", {}, /*system=*/true, OptionalBoolHandler(&Types_.UseTableMetaFromGraph));

    AddFlag("DiscoveryMode", {}, /*system=*/false, BoolHandler(&Types_.DiscoveryMode, /*value=*/true));

    AddFlag("WindowNewPipeline", "DisableWindowNewPipeline", /*system=*/false,
            ToggleHandler(&Types_.WindowNewPipeline, "WindowNewPipeline"));

    AddFlag("EnableSystemColumns", {}, /*system=*/false, [](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        return true;
    });

    AddFlag("UdfIgnoreCase", "UdfStrictCase", /*system=*/false, [this](TStringBuf name, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        if (ForPartialTypeCheck_) {
            return true;
        }
        if (!Types_.UdfIndex) {
            ctx.AddError(TIssue(pos, "UdfIndex is not available"));
            return false;
        }
        Types_.UdfIndex->SetCaseSentiveSearch(name == "UdfStrictCase");
        return true;
    });

    AddFlag("NamedArgsIgnoreCase", "NamedArgsStrictCase", /*system=*/false,
            ToggleHandler(&Types_.CaseInsensitiveNamedArgs, "NamedArgsIgnoreCase"));

    AddFlag("DqEngine", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        auto arg = TString{args[0]};
        if (Types_.EngineType == EEngineType::Ytflow) {
            if (arg == "force") {
                ctx.AddError(TIssue(pos, TStringBuilder()
                                             << "Expected `disable|auto` argument for DqEngine pragma "
                                             << "with Engine pragma argument `ytflow`"));
                return false;
            }
            arg = "disable";
        } else if (Types_.EngineType == EEngineType::Dq) {
            arg = "force";
        }
        if (Find(Types_.AvailablePureResultDataSources, DqProviderName) == Types_.AvailablePureResultDataSources.end() || arg == "disable") {
            ; // reserved
        } else if (arg == "auto") {
            Types_.PureResultDataSource = DqProviderName;
            Types_.ForceDq = false;
        } else if (arg == "force") {
            Types_.PureResultDataSource = DqProviderName;
            Types_.ForceDq = true;
        } else {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected `disable|auto|force', but got: " << args[0]));
            return false;
        }
        return true;
    });

    AddFlag("IssueCountLimit", {}, /*system=*/false, [](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        size_t limit = 0;
        if (!TryFromString(args[0], limit)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected unsigned integer, but got: " << args[0]));
            return false;
        }
        ctx.IssueManager.SetIssueCountLimit(limit);
        return true;
    });

    AddFlag("StrictTableProps", "DisableStrictTableProps", /*system=*/false,
            ToggleHandler(&Types_.StrictTableProps, "StrictTableProps"));

    AddFlag("GeobaseDownloadUrl", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
            return false;
        }
        auto& userDataBlock = (Types_.UserDataStorageCrutches[TUserDataKey::File(TStringBuf("/home/geodata6.bin"))] = TUserDataBlock{.Type = EUserDataType::URL, .UrlToken = {}, .Data = TString(args[0]), .Usage = {}, .FrozenFile = {}});
        userDataBlock.Usage.Set(EUserDataBlockUsage::Path);
        return true;
    });

    AddFlag("JsonQueryReturnsJsonDocument", "DisableJsonQueryReturnsJsonDocument", /*system=*/false,
            ToggleHandler(&Types_.JsonQueryReturnsJsonDocument, "JsonQueryReturnsJsonDocument"));

    AddFlag("OrderedColumns", "DisableOrderedColumns", /*system=*/false, [this](TStringBuf name, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        Types_.DeriveColumnOrder = (name == "OrderedColumns");
        Types_.OrderedColumns = (name == "OrderedColumns");
        return true;
    });

    AddFlag("ShowLinksInPlan", "DisableShowLinksInPlan", /*system=*/false,
            ToggleHandler(&Types_.ShowLinksInPlan, "ShowLinksInPlan"));

    AddFlag("DeriveColumnOrder", "DisableDeriveColumnOrder", /*system=*/false,
            ToggleHandler(&Types_.DeriveColumnOrder, "DeriveColumnOrder"));

    AddFlag("FolderSubDirsLimit", {}, /*system=*/false, Ui32Handler(&Types_.FolderSubDirsLimit));

    AddFlag("YsonCastToString", "DisableYsonCastToString", /*system=*/false,
            ToggleHandler(&Types_.YsonCastToString, "YsonCastToString"));

    AddFlag("UseBlocks", "DisableUseBlocks", /*system=*/false,
            ToggleHandler(&Types_.UseBlocks, "UseBlocks"));

    AddFlag("DebugPositions", "DisableDebugPositions", /*system=*/false,
            ToggleHandler(&Types_.DebugPositions, "DebugPositions"));

    AddFlag("UseCanonicalLibrarySuffix", "DisableUseCanonicalLibrarySuffix", /*system=*/false, [this](TStringBuf name, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        if (auto modules = dynamic_cast<TModuleResolver*>(Types_.Modules.get())) {
            modules->SetUseCanonicalLibrarySuffix(name == "UseCanonicalLibrarySuffix");
        }
        return true;
    });

    AddFlag("PgEmitAggApply", "DisablePgEmitAggApply", /*system=*/false,
            ToggleMaybeHandler(&Types_.PgEmitAggApply, "PgEmitAggApply"));

    AddFlag("CostBasedOptimizer", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at most 1 argument, but got " << args.size()));
            return false;
        }
        if (!TryFromString(args[0], Types_.CostBasedOptimizer)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected `disable|pg|native', but got: " << args[0]));
            return false;
        }
        return true;
    });

    AddFlag("CostBasedOptimizerVersion", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at most 1 argument, but got " << args.size()));
            return false;
        }
        ui32 version;
        if (!TryFromString(args[0], version)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected integer, but got: " << args[0]));
            return false;
        }
        const ui32 maxCBOVersion = 1;
        if (version > maxCBOVersion) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected value <= " << maxCBOVersion << ", but got: " << args[0]));
            return false;
        }
        Types_.CostBasedOptimizerVersion = version;
        return true;
    });

    AddFlag("_EnableMatchRecognize", "DisableMatchRecognize", /*system=*/true,
            ToggleHandler(&Types_.MatchRecognize, "_EnableMatchRecognize"));

    AddFlag("TimeOrderRecoverDelay", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected one argument, but got " << args.size()));
            return false;
        }
        i64 value;
        if (!TryFromString(args[0], value)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected integer, but got: " << args[0]));
            return false;
        }
        if (value >= 0) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected negative value, but got: " << args[0]));
            return false;
        }
        Types_.TimeOrderRecoverDelay = value;
        return true;
    });

    AddFlag("TimeOrderRecoverAhead", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected one argument, but got " << args.size()));
            return false;
        }
        if (!TryFromString(args[0], Types_.TimeOrderRecoverAhead)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected integer, but got: " << args[0]));
            return false;
        }
        if (Types_.TimeOrderRecoverAhead <= 0) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected positive value, but got: " << args[0]));
            return false;
        }
        return true;
    });

    AddFlag("TimeOrderRecoverRowLimit", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected one argument, but got " << args.size()));
            return false;
        }
        if (!TryFromString(args[0], Types_.TimeOrderRecoverRowLimit)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected integer, but got: " << args[0]));
            return false;
        }
        if (Types_.TimeOrderRecoverRowLimit == 0) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected positive value, but got: " << args[0]));
            return false;
        }
        return true;
    });

    AddFlag("MatchRecognizeStream", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at most 1 argument, but got " << args.size()));
            return false;
        }
        const auto& arg = args[0];
        if (arg == "disable") {
            Types_.MatchRecognizeStreaming = EMatchRecognizeStreamingMode::Disable;
        } else if (arg == "auto") {
            Types_.MatchRecognizeStreaming = EMatchRecognizeStreamingMode::Auto;
        } else if (arg == "force") {
            Types_.MatchRecognizeStreaming = EMatchRecognizeStreamingMode::Force;
        } else {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected `disable|auto|force', but got: " << args[0]));
            return false;
        }
        return true;
    });

    AddFlag("BlockEngine", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at most 1 argument, but got " << args.size()));
            return false;
        }
        auto arg = TString{args[0]};
        if (!TryFromString(arg, Types_.BlockEngineMode)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected `disable|auto|force', but got: " << args[0]));
            return false;
        }
        return true;
    });

    AddFlag("DecimalCommonTypeConversionMode", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at most 1 argument, but got " << args.size()));
            return false;
        }
        auto arg = TString{args[0]};
        EDecimalConversionMode decimalConversionMode;
        if (!TryFromString(arg, decimalConversionMode)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected `without_common_type_fixup|with_common_type_fixup', but got: " << args[0]));
            return false;
        }
        Types_.UpdateDecimalConversionMode(decimalConversionMode);
        return true;
    });

    AddFlag("OptimizerFlags", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        for (auto& arg : args) {
            if (arg.empty()) {
                ctx.AddError(TIssue(pos, "Empty flags are not supported"));
                return false;
            }
            Types_.OptimizerFlags.insert(to_lower(ToString(arg)));
        }
        return true;
    });

    AddFlag("PeepholeFlags", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        for (auto& arg : args) {
            if (arg.empty()) {
                ctx.AddError(TIssue(pos, "Empty flags are not supported"));
                return false;
            }
            Types_.PeepholeFlags.insert(to_lower(ToString(arg)));
        }
        return true;
    });

    AddFlag("_EnableStreamLookupJoin", "DisableStreamLookupJoin", /*system=*/true,
            ToggleHandler(&Types_.StreamLookupJoin, "_EnableStreamLookupJoin"));

    AddFlag("MaxAggPushdownPredicates", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected single numeric argument, but got " << args.size()));
            return false;
        }
        ui32 value;
        if (!TryFromString(args[0], value)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected non-negative integer, but got: " << args[0]));
            return false;
        }
        const ui32 hardLimit = 10;
        if (value > hardLimit) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Hard limit for setting MaxAggPushdownPredicates is " << hardLimit << ", but got: " << args[0]));
            return false;
        }
        Types_.MaxAggPushdownPredicates = value;
        return true;
    });

    AddFlag("AndOverOrExpansionLimit", {}, /*system=*/false, Ui32Handler(&Types_.AndOverOrExpansionLimit));

    AddFlag("Engine", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected at most 1 argument, but got " << args.size()));
            return false;
        }
        auto arg = TString{args[0]};
        if (arg == "ytflow") {
            if (Types_.ForceDq) {
                ctx.AddError(TIssue(pos, TStringBuilder()
                                             << "Expected `disable|auto` argument for DqEngine pragma "
                                             << "with Engine pragma argument `ytflow`"));
                return false;
            }
            if (Types_.PureResultDataSource == DqProviderName) {
                Types_.PureResultDataSource.clear();
            }
            Types_.EngineType = EEngineType::Ytflow;
        } else if (arg == "dq") {
            if (Find(Types_.AvailablePureResultDataSources, DqProviderName) == Types_.AvailablePureResultDataSources.end()) {
                ; // reserved
            } else {
                Types_.PureResultDataSource = DqProviderName;
                Types_.ForceDq = true;
            }
            Types_.EngineType = EEngineType::Dq;
        } else if (arg == "default") {
            Types_.EngineType = EEngineType::Default;
        } else {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected `default|dq|ytflow', but got: " << arg));
            return false;
        }
        return true;
    });

    AddFlag("NormalizeDependsOn", {}, /*system=*/false, OptionalBoolHandler(&Types_.NormalizeDependsOn));

    AddFlag("UseUrlListerForFolder", "DisableUseUrlListerForFolder", /*system=*/false, [](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        // TODO: remove
        if (!args.empty()) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected no arguments, but got " << args.size()));
            return false;
        }
        return true;
    });

    AddFlag("EarlyExpandSeq", "DisableEarlyExpandSeq", /*system=*/false,
            ToggleHandler(&Types_.EarlyExpandSeq, "EarlyExpandSeq"));

    AddFlag("DirectRowDependsOn", "DisableDirectRowDependsOn", /*system=*/false,
            ToggleHandler(&Types_.DirectRowDependsOn, "DirectRowDependsOn"));

    AddFlag("EnableLineage", "DisableLineage", /*system=*/false,
            ToggleHandler(&Types_.LineageSettings.EnableLineage, "EnableLineage"));

    AddFlag("EnableStandaloneLineage", "DisableStandaloneLineage", /*system=*/false,
            ToggleHandler(&Types_.LineageSettings.EnableStandaloneLineage, "EnableStandaloneLineage"));

    AddFlag("EnableEvaluateExprCache", {}, /*system=*/false, BoolHandler(&Types_.EnableEvaluateExprCache, /*value=*/true));

    AddFlag("LineageOutputLimit", {}, /*system=*/false, Ui64Handler(&Types_.LineageSettings.LineageOutputLimit));

    AddFlag("LineageMemoryLimit", {}, /*system=*/false, Ui64Handler(&Types_.LineageSettings.LineageMemoryLimit));

    AddFlag("LineageVersion", {}, /*system=*/false, Ui32Handler(&Types_.LineageSettings.LineageVersion));

    AddFlag("LineageStandaloneVersion", {}, /*system=*/false, Ui32Handler(&Types_.LineageSettings.LineageStandaloneVersion));

    AddFlag("Layer", {}, /*system=*/false, [this](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
        if (args.size() != 1) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Expected exactly 1 argument, but got " << args.size()));
            return false;
        }
        if (!Types_.LayersRegistry->AddLayerFromJson(args[0], ctx)) {
            return false;
        }
        return true;
    });

    AddFlag("ProcessedLayer", {}, /*system=*/false, [](TStringBuf, const TPosition&, const TVector<TStringBuf>&, TExprContext&) {
        return true;
    });
}

bool TConfigFlags::ImportUdfs(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
    if (args.size() != 1 && args.size() != 2) {
        ctx.AddError(TIssue(pos, TStringBuilder()
                                     << "Expected 1 or 2 arguments, but got " << args.size()));
        return false;
    }

    if (Types_.DisableNativeUdfSupport) {
        ctx.AddError(TIssue(pos, "Native UDF support is disabled"));
        return false;
    }

    if (ForPartialTypeCheck_) {
        return true;
    }

    // file alias
    const auto& fileAlias = args[0];
    TString customUdfPrefix = args.size() > 1 ? TString(args[1]) : "";
    const auto key = TUserDataStorage::ComposeUserDataKey(fileAlias);
    TString errorMessage;
    TUserDataBlock* udfSource = nullptr;
    if (!Types_.QContext.CanRead()) {
        udfSource = Types_.UserDataStorage->FreezeUdfNoThrow(key, errorMessage, customUdfPrefix, Types_.RuntimeLogLevel, fileAlias);
        if (!udfSource) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Unknown file: " << fileAlias << ", details: " << errorMessage));
            return false;
        }
    } else {
        udfSource = &Types_.UserDataStorage->GetUserDataBlock(key);
        udfSource->CustomUdfPrefix = customUdfPrefix;
    }

    IUdfResolver::TImport import;
    import.Pos = pos;
    import.FileAlias = fileAlias;
    import.Block = udfSource;
    Types_.UdfImports.insert({TString(fileAlias), import});
    return true;
}

bool TConfigFlags::AddCredential(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
    if (args.size() != 4) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 4 arguments, but got " << args.size()));
        return false;
    }

    if (Types_.Credentials->FindCredential(args[0])) {
        return true;
    }

    Types_.Credentials->AddCredential(TString(args[0]), TCredential(TString(args[1]), TString(args[2]), TString(args[3])));
    return true;
}

bool TConfigFlags::AddFileByUrlImpl(const TStringBuf alias, const TStringBuf url, const TStringBuf token, const TPosition pos, TExprContext& ctx) {
    if (url.empty()) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Empty URL for file '" << alias << "'."));
        return false;
    }

    auto key = TUserDataStorage::ComposeUserDataKey(alias);
    if (Types_.UserDataStorage->ContainsUserDataBlock(key)) {
        // Don't overwrite.
        return true;
    }

    TUserDataBlock block;
    if (Types_.QContext.CanRead()) {
        block.Type = EUserDataType::RAW_INLINE_DATA;
    } else {
        block.Type = EUserDataType::URL;
        block.Data = url;
        if (token) {
            block.UrlToken = token;
        }
    }

    Types_.UserDataStorage->AddUserDataBlock(key, block);
    return true;
}

bool TConfigFlags::AddFileByUrl(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
    if (ForPartialTypeCheck_) {
        return true;
    }

    if (args.size() < 2 || args.size() > 3) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 2 or 3 arguments, but got " << args.size()));
        return false;
    }

    TStringBuf token = args.size() == 3 ? args[2] : TStringBuf();
    if (token) {
        if (auto cred = Types_.Credentials->FindCredential(token)) {
            token = cred->Content;
        } else {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Unknown token name '" << token << "'."));
            return false;
        }
    }

    return AddFileByUrlImpl(args[0], args[1], token, pos, ctx);
}

bool TConfigFlags::SetFileOptionImpl(const TStringBuf alias, const TString& key, const TString& value, const TPosition pos, TExprContext& ctx) {
    const auto dataKey = TUserDataStorage::ComposeUserDataKey(alias);
    const auto dataBlock = Types_.UserDataStorage->FindUserDataBlock(dataKey);
    if (!dataBlock) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "No such file '" << alias << "'"));
        return false;
    }
    dataBlock->Options[key] = value;
    return true;
}

bool TConfigFlags::SetFileOption(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
    if (ForPartialTypeCheck_) {
        return true;
    }

    if (args.size() != 3) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 3 arguments, but got " << args.size()));
        return false;
    }
    return SetFileOptionImpl(args[0], ToString(args[1]), ToString(args[2]), pos, ctx);
}

bool TConfigFlags::SetPackageVersion(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
    if (ForPartialTypeCheck_) {
        return true;
    }

    if (args.size() != 2) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 2 arguments, but got " << args.size()));
        return false;
    }

    ui32 version = 0;
    if (!TryFromString(args[1], version)) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Unable to parse package version from " << args[1]));
        return false;
    }

    if (!Types_.UdfIndexPackageSet || !Types_.UdfIndex) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "UdfIndex is not initialized, unable to set version for package " << args[0]));
        return false;
    }

    if (!Types_.UdfIndexPackageSet->AddResourceTo(TString(args[0]), version, Types_.UdfIndex)) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Unable set default version to " << version << " for package " << args[0]));
        return false;
    }

    return true;
}

bool TConfigFlags::AddFolderByUrl(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
    if (ForPartialTypeCheck_) {
        return true;
    }

    if (args.size() < 2 || args.size() > 3) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 2 or 3 arguments, but got " << args.size()));
        return false;
    }

    TStringBuf prefix = args[0];
    TStringBuf url = args[1];
    TStringBuf tokenName = args.size() == 3 ? args[2] : TStringBuf();

    TStringBuf token;
    if (tokenName) {
        if (auto cred = Types_.Credentials->FindCredential(tokenName)) {
            token = cred->Content;
        } else {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Unknown token name '" << tokenName << "' for folder."));
            return false;
        }
    }

    if (!Types_.UrlListerManager) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "UrlListerManager is not initialized, unable to add folder by url"));
        return false;
    }

    TString separator = "/";
    TVector<TUrlListEntry> entries;
    try {
        entries = Types_.UrlListerManager->ListUrlRecursive(TString(url), TString(tokenName), separator, Types_.FolderSubDirsLimit);
    } catch (const std::exception& e) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "failed to list URL '" << url << "', details: " << e.what()));
        return false;
    }

    for (const auto& entry : entries) {
        if (!AddFileByUrlImpl(TStringBuilder() << prefix << entry.Name, entry.Url, token, pos, ctx)) {
            return false;
        }
    }

    return true;
}

bool TConfigFlags::SetWarningRule(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
    if (args.size() != 2) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 2 arguments, but got " << args.size()));
        return false;
    }

    TString codePattern = TString{args[0]};
    TString action = TString{args[1]};

    TWarningRule rule;
    TString parseError;
    auto parseResult = TWarningRule::ParseFrom(codePattern, action, rule, parseError);
    switch (parseResult) {
        case TWarningRule::EParseResult::PARSE_OK:
            ctx.IssueManager.AddWarningRule(rule);
            break;
        case TWarningRule::EParseResult::PARSE_PATTERN_FAIL:
        case TWarningRule::EParseResult::PARSE_ACTION_FAIL:
            ctx.AddError(TIssue(pos, parseError));
            break;
        default:
            YQL_ENSURE(false, "Unknown parse result");
    }

    return parseResult == TWarningRule::EParseResult::PARSE_OK;
}

} // namespace NYql
