#pragma once

#include <util/generic/fwd.h>
#include <memory>

namespace NYql::NPureCalc {
    class TCompileError;

    template <typename>
    class IConsumer;

    template <typename>
    class IStream;

    class IProgramFactory;

    class IWorkerFactory;

    class IPullStreamWorkerFactory;

    class IPullListWorkerFactory;

    class IPushStreamWorkerFactory;

    class IWorker;

    class IPullStreamWorker;

    class IPullListWorker;

    class IPushStreamWorker;

    class TInputSpecBase;

    class TOutputSpecBase;

    class IProgram;

    template <typename, typename, typename>
    class TProgramCommon;

    template <typename, typename>
    class TPullStreamProgram;

    template <typename, typename>
    class TPullListProgram;

    template <typename, typename>
    class TPushStreamProgram;

    using IProgramFactoryPtr = TIntrusivePtr<IProgramFactory>;
    using IWorkerFactoryPtr = std::shared_ptr<IWorkerFactory>;
    using IPullStreamWorkerFactoryPtr = std::shared_ptr<IPullStreamWorkerFactory>;
    using IPullListWorkerFactoryPtr = std::shared_ptr<IPullListWorkerFactory>;
    using IPushStreamWorkerFactoryPtr = std::shared_ptr<IPushStreamWorkerFactory>;
}
