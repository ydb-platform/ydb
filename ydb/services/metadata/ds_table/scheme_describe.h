#pragma once

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/services/metadata/common/ss_dialog.h>

namespace NKikimr::NMetadata::NProvider {

class ISchemeDescribeController {
public:
    using TPtr = std::shared_ptr<ISchemeDescribeController>;
    virtual ~ISchemeDescribeController() = default;
    virtual void OnDescriptionFailed(const TString& errorMessage, const TString& requestId) const = 0;
    virtual void OnDescriptionSuccess(Ydb::Table::DescribeTableResult&& result, const TString& requestId) const = 0;
};

class TSchemeDescriptionActor: public NInternal::TSSDialogActor {
private:
    using TBase = NInternal::TSSDialogActor;
    ISchemeDescribeController::TPtr Controller;
    TString Path;
    TString RequestId;
    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev);
protected:
    virtual void OnBootstrap() override {
        Become(&TSchemeDescriptionActor::StateMain);
        TBase::OnBootstrap();
    }
    virtual void OnFail(const TString& errorMessage) override;
    virtual void Execute() override;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType();
    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default:
                TBase::StateMain(ev, ctx);
        }
    }
    TSchemeDescriptionActor(ISchemeDescribeController::TPtr controller, const TString& reqId, const TString& path, const TDuration livetime)
        : TBase(livetime)
        , Controller(controller)
        , Path(path)
        , RequestId(reqId)
    {

    }
};

}
