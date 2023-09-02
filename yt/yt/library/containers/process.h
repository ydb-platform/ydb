#pragma once

#include "public.h"

#include <yt/yt/library/process/process.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/porto/libporto.hpp>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

// NB(psushin): this class is deprecated and only used to run job proxy.
// ToDo(psushin): kill me.
class TPortoProcess
    : public TProcessBase
{
public:
    TPortoProcess(
        const TString& path,
        NContainers::IInstanceLauncherPtr containerLauncher,
        bool copyEnv = true);
    void Kill(int signal) override;
    NNet::IConnectionWriterPtr GetStdInWriter() override;
    NNet::IConnectionReaderPtr GetStdOutReader() override;
    NNet::IConnectionReaderPtr GetStdErrReader() override;

    NContainers::IInstancePtr GetInstance();

private:
    const NContainers::IInstanceLauncherPtr ContainerLauncher_;

    TAtomicIntrusivePtr<NContainers::IInstance> ContainerInstance_;
    std::vector<NPipes::TNamedPipePtr> NamedPipes_;

    void DoSpawn() override;
    THashMap<TString, TString> DecomposeEnv() const;
};

DEFINE_REFCOUNTED_TYPE(TPortoProcess)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
