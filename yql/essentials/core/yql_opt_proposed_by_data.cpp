#include "yql_opt_proposed_by_data.h"

namespace NYql {

namespace {

enum class ESource {
    DataSource,
    DataSink,
    All
};

template <ESource Source, typename TGetTransformer, typename TFinish>
class TDataProposalsInspector : public TGraphTransformerBase {
public:
    TDataProposalsInspector(const TTypeAnnotationContext& types, TGetTransformer getTransformer,
        TFinish finish)
        : Types_(types)
        , GetTransformer_(getTransformer)
        , Finish_(finish)
    {}

private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        HasRepeats_ = false;
        if (Source == ESource::DataSource || Source == ESource::All) {
            for (auto& x : Types_.DataSources) {
                auto s = HandleProvider(x.Get(), input, output, ctx);
                if (s.Level == TStatus::Error) return s;
                HasRepeats_ = HasRepeats_ || s == TStatus::Repeat;

                if (!NewRoots_.empty()) {
                    break;
                }
            }
        }

        if (Source == ESource::DataSink || Source == ESource::All) {
            for (auto& x : Types_.DataSinks) {
                auto s = HandleProvider(x.Get(), input, output, ctx);
                if (s.Level == TStatus::Error) return s;
                HasRepeats_ = HasRepeats_ || s == TStatus::Repeat;

                if (!NewRoots_.empty()) {
                    break;
                }
            }
        }

        if (!PendingProviders_.empty()) {
            return TStatus::Async;
        }

        if (NewRoots_.empty()) {
            if (HasRepeats_) {
                return TStatus::Repeat;
            }

            Finish_(ctx);
            return TStatus::Ok;
        }

        ChooseRoot(std::move(input), output);
        return TStatus(TStatus::Repeat, true);
    }

    void Rewind() final {
        if (Source == ESource::DataSource || Source == ESource::All) {
            for (auto& x : Types_.DataSources) {
                GetTransformer_(x.Get()).Rewind();
            }
        }

        if (Source == ESource::DataSink || Source == ESource::All) {
            for (auto& x : Types_.DataSinks) {
                GetTransformer_(x.Get()).Rewind();
            }
        }
        PendingProviders_.clear();
        HasRepeats_ = false;
        NewRoots_.clear();
    }

    TStatus HandleProvider(IDataProvider* provider, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TExprNode::TPtr newRoot;
        TStatus status = GetTransformer_(provider).Transform(input, newRoot, ctx);


        if (status.Level == TStatus::Ok || status.Level == TStatus::Repeat) {
            if (newRoot && newRoot != input) {
                NewRoots_.push_back(newRoot);
            }
        } else if (status.Level == TStatus::Async) {
            PendingProviders_.push_back(provider);
            if (newRoot && newRoot != input) {
                output = newRoot;
            }
        }

        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        TVector<NThreading::TFuture<void>> futures;
        for (auto& x : PendingProviders_) {
            futures.push_back(GetTransformer_(x).GetAsyncFuture(input));
        }

        return WaitExceptionOrAll(futures);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        for (auto& x : PendingProviders_) {
            TExprNode::TPtr newRoot;
            TStatus status = GetTransformer_(x).ApplyAsyncChanges(input, newRoot, ctx);
            if (status.Level == TStatus::Ok || status.Level == TStatus::Repeat) {
                if (newRoot && newRoot != input) {
                    NewRoots_.push_back(newRoot);
                }
                HasRepeats_ = HasRepeats_ || status == TStatus::Repeat;
            } else {
                return status;
            }
        }

        PendingProviders_.clear();
        bool hasRepeats = HasRepeats_;
        HasRepeats_ = false;
        if (NewRoots_.empty()) {
            if (hasRepeats) {
                return TStatus::Repeat;
            }

            Finish_(ctx);
            return TStatus::Ok;
        }

        ChooseRoot(std::move(input), output);
        return TStatus(TStatus::Repeat, true);
    }

    void ChooseRoot(TExprNode::TPtr&& input, TExprNode::TPtr& output) {
        // just get first
        output = std::move(NewRoots_.empty() ? input : NewRoots_.front());
        NewRoots_.clear();
    }

    const TTypeAnnotationContext& Types_;
    TGetTransformer GetTransformer_;
    TFinish Finish_;
    TVector<IDataProvider*> PendingProviders_;
    bool HasRepeats_ = false;
    TExprNode::TListType NewRoots_;
};

template <ESource Source, typename TGetTransformer, typename TFinish>
class TSpecificDataProposalsInspector : public TGraphTransformerBase {
public:
    TSpecificDataProposalsInspector(const TTypeAnnotationContext& types, const TString& provider, TGetTransformer getTransformer,
        TFinish finish)
        : Types_(types)
        , Provider_(provider)
        , GetTransformer_(getTransformer)
        , Finish_(finish)
    {}

private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        bool hasRepeats = false;
        if (Source == ESource::DataSource || Source == ESource::All) {
            if (auto p = Types_.DataSourceMap.FindPtr(Provider_)) {
                auto s = HandleProvider(p->Get(), input, output, ctx);
                if (s.Level == TStatus::Error) return s;
                hasRepeats = hasRepeats || s == TStatus::Repeat;
            }
        }

        if (Source == ESource::DataSink || Source == ESource::All) {
            if (auto p = Types_.DataSinkMap.FindPtr(Provider_)) {
                auto s = HandleProvider(p->Get(), input, output, ctx);
                if (s.Level == TStatus::Error) return s;
                hasRepeats = hasRepeats || s == TStatus::Repeat;
            }
        }

        if (!PendingProviders_.empty()) {
            return TStatus::Async;
        }

        if (NewRoots_.empty()) {
            if (hasRepeats) {
                return TStatus::Repeat;
            }

            Finish_(ctx);
            return TStatus::Ok;
        }

        ChooseRoot(std::move(input), output);
        return TStatus(TStatus::Repeat, true);
    }

    void Rewind() final {
        if (Source == ESource::DataSource || Source == ESource::All) {
            if (auto p = Types_.DataSourceMap.FindPtr(Provider_)) {
                GetTransformer_(p->Get()).Rewind();
            }
        }

        if (Source == ESource::DataSink || Source == ESource::All) {
            if (auto p = Types_.DataSinkMap.FindPtr(Provider_)) {
                GetTransformer_(p->Get()).Rewind();
            }
        }
        PendingProviders_.clear();
        NewRoots_.clear();
    }

    TStatus HandleProvider(IDataProvider* provider, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TExprNode::TPtr newRoot;
        TStatus status = GetTransformer_(provider).Transform(input, newRoot, ctx);


        if (status.Level == TStatus::Ok || status.Level == TStatus::Repeat) {
            if (newRoot && newRoot != input) {
                NewRoots_.push_back(newRoot);
            }
        } else if (status.Level == TStatus::Async) {
            PendingProviders_.push_back(provider);
            if (newRoot && newRoot != input) {
                output = newRoot;
            }
        }

        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        TVector<NThreading::TFuture<void>> futures;
        for (auto& x : PendingProviders_) {
            futures.push_back(GetTransformer_(x).GetAsyncFuture(input));
        }

        return WaitExceptionOrAll(futures);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        bool hasRepeats = false;
        for (auto& x : PendingProviders_) {
            TExprNode::TPtr newRoot;
            TStatus status = GetTransformer_(x).ApplyAsyncChanges(input, newRoot, ctx);
            if (status.Level == TStatus::Ok || status.Level == TStatus::Repeat) {
                if (newRoot && newRoot != input) {
                    NewRoots_.push_back(newRoot);
                }
                hasRepeats = hasRepeats || status == TStatus::Repeat;
            } else {
                return status;
            }
        }

        PendingProviders_.clear();
        if (NewRoots_.empty()) {
            return hasRepeats
                ? TStatus::Repeat
                : TStatus::Ok;
        }

        ChooseRoot(std::move(input), output);
        return TStatus(TStatus::Repeat, true);
    }

    void ChooseRoot(TExprNode::TPtr&& input, TExprNode::TPtr& output) {
        // just get first
        output = std::move(NewRoots_.empty() ? input : NewRoots_.front());
        NewRoots_.clear();
    }

    const TTypeAnnotationContext& Types_;
    TString Provider_;
    TGetTransformer GetTransformer_;
    TFinish Finish_;
    TVector<IDataProvider*> PendingProviders_;
    TExprNode::TListType NewRoots_;
};

auto DefaultFinish = [](TExprContext&){};

template <ESource Source, typename TGetTransformer, typename TFinish = decltype(DefaultFinish)>
TAutoPtr<IGraphTransformer> CreateDataProposalsInspector(const TTypeAnnotationContext& types,
    TGetTransformer getTransformer, TFinish finish = DefaultFinish) {
    return new TDataProposalsInspector<Source, TGetTransformer, TFinish>(types, getTransformer, finish);
}

template <ESource Source, typename TGetTransformer, typename TFinish = decltype(DefaultFinish)>
TAutoPtr<IGraphTransformer> CreateSpecificDataProposalsInspector(const TTypeAnnotationContext& types, const TString& provider,
    TGetTransformer getTransformer, TFinish finish = DefaultFinish) {
    return new TSpecificDataProposalsInspector<Source, TGetTransformer, TFinish>(types, provider, getTransformer, finish);
}

} // namespace

TAutoPtr<IGraphTransformer> CreateConfigureTransformer(const TTypeAnnotationContext& types) {
    return CreateDataProposalsInspector<ESource::DataSource>(
        types,
        [](IDataProvider* provider) -> IGraphTransformer& {
        return provider->GetConfigurationTransformer();
    },
        [](TExprContext& ctx) {
        ctx.Step.Done(TExprStep::ELevel::Configure);
    }
    );
}

TAutoPtr<IGraphTransformer> CreateIODiscoveryTransformer(const TTypeAnnotationContext& types) {
    return CreateDataProposalsInspector<ESource::DataSource>(
        types,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetIODiscoveryTransformer();
        },
        [](TExprContext& ctx) {
            ctx.Step.Done(TExprStep::ELevel::DiscoveryIO);
        }
    );
}

TAutoPtr<IGraphTransformer> CreateEpochsTransformer(const TTypeAnnotationContext& types) {
    return CreateDataProposalsInspector<ESource::DataSource>(
        types,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetEpochsTransformer();
        },
        [](TExprContext& ctx) {
            ctx.Step.Done(TExprStep::ELevel::Epochs);
        }
    );
}

TAutoPtr<IGraphTransformer> CreateLogicalDataProposalsInspector(const TTypeAnnotationContext& types) {
    return CreateDataProposalsInspector<ESource::DataSink>(
        types,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetLogicalOptProposalTransformer();
        }
    );
}

TAutoPtr<IGraphTransformer> CreatePhysicalDataProposalsInspector(const TTypeAnnotationContext& types) {
    return CreateDataProposalsInspector<ESource::DataSink>(
        types,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetPhysicalOptProposalTransformer();
        }
    );
}

TAutoPtr<IGraphTransformer> CreatePhysicalFinalizers(const TTypeAnnotationContext& types) {
    return CreateDataProposalsInspector<ESource::DataSink>(
        types,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetPhysicalFinalizingTransformer();
        }
    );
}

TAutoPtr<IGraphTransformer> CreateTableMetadataLoader(const TTypeAnnotationContext& types) {
    return CreateDataProposalsInspector<ESource::DataSource>(
        types,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetLoadTableMetadataTransformer();
        },
        [](TExprContext& ctx) {
            ctx.Step.Done(TExprStep::LoadTablesMetadata);
        }
    );
}

TAutoPtr<IGraphTransformer> CreateCompositeFinalizingTransformer(const TTypeAnnotationContext& types) {
    return CreateDataProposalsInspector<ESource::DataSink>(
        types,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetFinalizingTransformer();
        }
    );
}

TAutoPtr<IGraphTransformer> CreatePlanInfoTransformer(const TTypeAnnotationContext& types) {
    return CreateDataProposalsInspector<ESource::DataSink>(
        types,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetPlanInfoTransformer();
        }
    );
}

TAutoPtr<IGraphTransformer> CreateRecaptureDataProposalsInspector(const TTypeAnnotationContext& types, const TString& provider) {
    return CreateSpecificDataProposalsInspector<ESource::DataSink>(
        types,
        provider,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetRecaptureOptProposalTransformer();
        },
        [](TExprContext& ctx) {
            ctx.Step.Done(TExprStep::Recapture);
        }
    );
}

TAutoPtr<IGraphTransformer> CreateStatisticsProposalsInspector(const TTypeAnnotationContext& types, const TString& provider) {
    return CreateSpecificDataProposalsInspector<ESource::DataSource>(
        types,
        provider,
        [](IDataProvider* provider) -> IGraphTransformer& {
            return provider->GetStatisticsProposalTransformer();
        }
    );
}

} // namespace NYql
