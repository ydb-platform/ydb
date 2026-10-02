#pragma once

#include <util/generic/hash.h>

#include <functional>

namespace NYql {

struct TTypeAnnotationContext;
struct TPosition;
struct TExprContext;

using TAllowSettingPolicy = std::function<bool(TStringBuf settingName)>;

class TConfigFlags {
public:
    TConfigFlags(TTypeAnnotationContext& types, TAllowSettingPolicy policy, bool forPartialTypeCheck);

    using TFlagHandler = std::function<bool(TStringBuf name, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx)>;

    struct TFlag {
        TStringBuf Name;
        bool System = false;
        TFlagHandler Handler;
    };

    template <class TDerived>
    TFlagHandler DelegateHandler(bool (TDerived::*method)(const TPosition&, const TVector<TStringBuf>&, TExprContext&)) {
        return [this, method](TStringBuf, const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx) {
            return (this->*method)(pos, args, ctx);
        };
    };

    TFlagHandler ToggleHandler(bool* field, TStringBuf enableName);
    TFlagHandler ToggleMaybeHandler(TMaybe<bool>* field, TStringBuf enableName);
    TFlagHandler OptionalBoolHandler(bool* field);
    TFlagHandler BoolHandler(bool* field, bool value);
    TFlagHandler IntegerCtxFlagHandler(ui64 TExprContext::*field);
    TFlagHandler Ui32Handler(ui32* field);
    TFlagHandler Ui64Handler(ui64* field);
    TFlagHandler NoopHandler();

protected:
    bool IsSettingAllowed(const TPosition& pos, TStringBuf name, TExprContext& ctx);
    bool ApplyFlag(const TPosition& pos, TStringBuf name, const TVector<TStringBuf>& args, TExprContext& ctx, bool fromInitialize = false);
    void AddFlag(TStringBuf name, TStringBuf disableName, bool system, TFlagHandler handler);
    void InitFlags();

    bool ImportUdfs(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx);
    bool AddCredential(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx);
    bool AddFileByUrlImpl(TStringBuf alias, TStringBuf url, TStringBuf token, TPosition pos, TExprContext& ctx);
    bool AddFileByUrl(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx);
    bool SetFileOptionImpl(TStringBuf alias, const TString& key, const TString& value, TPosition pos, TExprContext& ctx);
    bool SetFileOption(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx);
    bool SetPackageVersion(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx);
    bool AddFolderByUrl(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx);
    bool SetWarningRule(const TPosition& pos, const TVector<TStringBuf>& args, TExprContext& ctx);

    TTypeAnnotationContext& Types_;
    const bool ForPartialTypeCheck_;
    const TAllowSettingPolicy Policy_;
    THashMap<TStringBuf, TFlag> Flags_;
};

} // namespace NYql
