#include "purecalc.h"
#include "scheme.h"

namespace NKikimr::NReplication::NTransfer {

TMessageOutputSpec::TMessageOutputSpec(const TScheme::TPtr& tableScheme, const NYT::TNode& schema)
    : TableScheme(tableScheme)
    , Schema(schema)
{}

const NYT::TNode& TMessageOutputSpec::GetSchema() const {
    return Schema;
}

const TVector<NKikimrKqp::TKqpColumnMetadataProto>& TMessageOutputSpec::GetTableColumns() const {
    return TableScheme->ColumnsMetadata;
}

const TVector<NKikimrKqp::TKqpColumnMetadataProto>& TMessageOutputSpec::GetStructColumns() const {
    return TableScheme->StructMetadata;
}

namespace {

using namespace NYql::NPureCalc;
using namespace NKikimr::NMiniKQL;

class TOutputListImpl final: public IStream<TOutputMessage*> {
protected:
    TWorkerHolder<IPullListWorker> WorkerHolder_;
    const TMessageOutputSpec& OutputSpec;

public:
    explicit TOutputListImpl(const TMessageOutputSpec& outputSpec, TWorkerHolder<IPullListWorker> worker)
        : WorkerHolder_(std::move(worker))
        , OutputSpec(outputSpec)
    {
        Row.resize(1);
    }

public:
    TOutputMessage* Fetch() override {
        TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

        with_lock(WorkerHolder_->GetScopedAlloc()) {
            Out.Data.clear();

            NYql::NUdf::TUnboxedValue value;

            if (!WorkerHolder_->GetOutputIterator().Next(value)) {
                return nullptr;
            }

            Out.Value = value.GetElement(0);

            const auto& columns = OutputSpec.GetStructColumns();
            for (size_t i = 0; i < columns.size(); ++i) {
                const auto& column = columns[i];
                const auto e = Out.Value.GetElement(i);
                if (column.name() == SystemColumns::TargetTable) {
                    if (e) {
                        auto opt = e.GetOptionalValue();
                        if (opt) {
                            Out.Table = opt.AsStringRef();
                        }
                    }
                    continue;
                }

                if (column.GetNotNull() && !e) {
                    throw yexception() << "The value of the '" << column.GetName() << "' column must be non-NULL";
                }
            }

            Out.Data.PushRow(&Out.Value, 1);

            return &Out;
        }
    }

private:
    std::vector<NUdf::TUnboxedValue> Row;
    TOutputMessage Out;
};

} // namespace

} // namespace NKikimr::NReplication::NTransfer

namespace NYql::NPureCalc {

THolder<IStream<NKikimr::NReplication::NTransfer::TOutputMessage*>> TOutputSpecTraits<NKikimr::NReplication::NTransfer::TMessageOutputSpec>::ConvertPullListWorkerToOutputType(
    const NKikimr::NReplication::NTransfer::TMessageOutputSpec& outputSpec,
    TWorkerHolder<IPullListWorker> worker
) {
    return MakeHolder<NKikimr::NReplication::NTransfer::TOutputListImpl>(outputSpec, std::move(worker));
}

}
