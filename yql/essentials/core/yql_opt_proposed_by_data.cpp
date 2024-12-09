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
        : Types(types)
        , GetTransformer(getTransformer)
        , Finish(finish)
    {}

private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        HasRepeats = false;
        if (Source == ESource::DataSource || Source == ESource::All) {
            for (auto& x : Types.DataSources) {
                auto s = HandleProvider(x.Get(), input, output, ctx);
                if (s.Level == TStatus::Error) return s;
                HasRepeats = HasRepeats || s == TStatus::Repeat;

                if (!NewRoots.empty()) {
                    break;
                }
            }
        }

        if (Source == ESource::DataSink || Source == ESource::All) {
            for (auto& x : Types.DataSinks) {
                auto s = HandleProvider(x.Get(), input, output, ctx);
                if (s.Level == TStatus::Error) return s;
                HasRepeats = HasRepeats || s == TStatus::Repeat;

                if (!NewRoots.empty()) {
                    break;
                }
            }
        }

        if (!PendingProviders.empty()) {
            return TStatus::Async;
        }

        if (NewRoots.empty()) {
            if (HasRepeats) {
                return TStatus::Repeat;
            }

            Finish(ctx);
            return TStatus::Ok;
        }

        ChooseRoot(std::move(input), output);
        return TStatus(TStatus::Repeat, true);
    }

    void Rewind() final {
        if (Source == ESource::DataSource || Source == ESource::All) {
            for (auto& x : Types.DataSources) {
                GetTransformer(x.Get()).Rewind();
            }
        }

        if (Source == ESource::DataSink || Source == ESource::All) {
            for (auto& x : Types.DataSinks) {
                GetTransformer(x.Get()).Rewind();
            }
        }
        PendingProviders.clear();
        HasRepeats = false;
        NewRoots.clear();
    }

    TStatus HandleProvider(IDataProvider* provider, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TExprNode::TPtr newRoot;
        TStatus status = GetTransformer(provider).Transform(input, newRoot, ctx);


        if (status.Level == TStatus::Ok || status.Level == TStatus::Repeat) {
            if (newRoot && newRoot != input) {
                NewRoots.push_back(newRoot);
            }
        } else if (status.Level == TStatus::Async) {
            PendingProviders.push_back(provider);
            if (newRoot && newRoot != input) {
                output = newRoot;
            }
        }

        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        TVector<NThreading::TFuture<void>> futures;
        for (auto& x : PendingProviders) {
            futures.push_back(GetTransformer(x).GetAsyncFuture(input));
        }

        return WaitExceptionOrAll(futures);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        for (auto& x : PendingProviders) {
            TExprNode::TPtr newRoot;
            TStatus status = GetTransformer(x).ApplyAsyncChanges(input, newRoot, ctx);
            if (status.Level == TStatus::Ok || status.Level == TStatus::Repeat) {
                if (newRoot && newRoot != input) {
                    NewRoots.push_back(newRoot);
                }
                HasRepeats = HasRepeats || status == TStatus::Repeat;
            } else {
                return status;
            }
        }

        PendingProviders.clear();
        bool hasRepeats = HasRepeats;
        HasRepeats = false;
        if (NewRoots.empty()) {
            if (hasRepeats) {
                return TStatus::Repeat;
            }

            Finish(ctx);
            return TStatus::Ok;
        }

        ChooseRoot(std::move(input), output);
        return TStatus(TStatus::Repeat, true);
    }

    void ChooseRoot(TExprNode::TPtr&& input, TExprNode::TPtr& output) {
        // just get first
        output = std::move(NewRoots.empty() ? input : NewRoots.front());
        NewRoots.clear();
    }

    const TTypeAnnotationContext& Types;
    TGetTransformer GetTransformer;
    TFinish Finish;
    TVector<IDataProvider*> PendingProviders;
    bool HasRepeats = false;
    TExprNode::TListType NewRoots;
};

template <ESource Source, typename TGetTransformer, typename TFinish>
class TSpecificDataProposalsInspector : public TGraphTransformerBase {
public:
    TSpecificDataProposalsInspector(const TTypeAnnotationContext& types, const TString& provider, TGetTransformer getTransformer,
        TFinish finish)
        : Types(types)
        , Provider(provider)
        , GetTransformer(getTransformer)
        , Finish(finish)
    {}

private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        bool hasRepeats = false;
        if (Source == ESource::DataSource || Source == ESource::All) {
            if (auto p = Types.DataSourceMap.FindPtr(Provider)) {
                auto s = HandleProvider(p->Get(), input, output, ctx);
                if (s.Level == TStatus::Error) return s;
                hasRepeats = hasRepeats || s == TStatus::Repeat;
            }
        }

        if (Source == ESource::DataSink || Source == ESource::All) {
            if (auto p = Types.DataSinkMap.FindPtr(Provider)) {
                auto s = HandleProvider(p->Get(), input, output, ctx);
                if (s.Level == TStatus::Error) return s;
                hasRepeats = hasRepeats || s == TStatus::Repeat;
            }
        }

        if (!PendingProviders.empty()) {
            return TStatus::Async;
        }

        if (NewRoots.empty()) {
            if (hasRepeats) {
                return TStatus::Repeat;
            }

            Finish(ctx);
            return TStatus::Ok;
        }

        ChooseRoot(std::move(input), output);
        return TStatus(TStatus::Repeat, true);
    }

    void Rewind() final {
        if (Source == ESource::DataSource || Source == ESource::All) {
            if (auto p = Types.DataSourceMap.FindPtr(Provider)) {
                GetTransformer(p->Get()).Rewind();
            }
        }

        if (Source == ESource::DataSink || Source == ESource::All) {
            if (auto p = Types.DataSinkMap.FindPtr(Provider)) {
                GetTransformer(p->Get()).Rewind();
            }
        }
        PendingProviders.clear();
        NewRoots.clear();
    }

    TStatus HandleProvider(IDataProvider* provider, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TExprNode::TPtr newRoot;
        TStatus status = GetTransformer(provider).Transform(input, newRoot, ctx);


        if (status.Level == TStatus::Ok || status.Level == TStatus::Repeat) {
            if (newRoot && newRoot != input) {
                NewRoots.push_back(newRoot);
            }
        } else if (status.Level == TStatus::Async) {
            PendingProviders.push_back(provider);
            if (newRoot && newRoot != input) {
                output = newRoot;
            }
        }

        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        TVector<NThreading::TFuture<void>> futures;
        for (auto& x : PendingProviders) {
            futures.push_back(GetTransformer(x).GetAsyncFuture(input));
        }

        return WaitExceptionOrAll(futures);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        bool hasRepeats = false;
        for (auto& x : PendingProviders) {
            TExprNode::TPtr newRoot;
            TStatus status = GetTransformer(x).ApplyAsyncChanges(input, newRoot, ctx);
            if (status.Level == TStatus::Ok || status.Level == TStatus::Repeat) {
                if (newRoot && newRoot != input) {
                    NewRoots.push_back(newRoot);
                }
                hasRepeats = hasRepeats || status == TStatus::Repeat;
            } else {
                return status;
            }
        }

        PendingProviders.clear();
        if (NewRoots.empty()) {
            return hasRepeats
                ? TStatus::Repeat
                : TStatus::Ok;
        }

        ChooseRoot(std::move(input), output);
        return TStatus(TStatus::Repeat, true);
    }

    void ChooseRoot(TExprNode::TPtr&& input, TExprNode::TPtr& output) {
        // just get first
        output = std::move(NewRoots.empty() ? input : NewRoots.front());
        NewRoots.clear();
    }

    const TTypeAnnotationContext& Types;
    TString Provider;
    TGetTransformer GetTransformer;
    TFinish Finish;
    TVector<IDataProvider*> PendingProviders;
    TExprNode::TListType NewRoots;
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
