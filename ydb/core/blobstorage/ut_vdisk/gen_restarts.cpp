#include "gen_restarts.h"


void WriteRestartRead(const TWriteRestartReadSettings &settings, TDuration testTimeout) {
    TConfiguration Conf;
    Conf.Prepare(settings.WriteRunSetup.get());
    std::shared_ptr<TSet<ui32>> badSteps(new TSet<ui32>());

    TManyPutsTest w(false, settings.MsgNum, settings.MsgSize, settings.Cls, badSteps);
    bool success1 = Conf.Run<TManyPutsTest>(&w, testTimeout);
    UNIT_ASSERT(success1);
    Conf.Shutdown();

    Conf.Prepare(settings.ReadRunSetup.get(), false);
    TManyGetsTest r(false, settings.MsgNum, settings.MsgSize, settings.Cls, badSteps);
    bool success2 = Conf.Run<TManyGetsTest>(&r, testTimeout);
    UNIT_ASSERT(success2);
    Conf.Shutdown();
}

void MultiPutWriteRestartRead(const TMultiPutWriteRestartReadSettings &settings, TDuration testTimeout) {
    TConfiguration Conf;
    Conf.Prepare(settings.WriteRunSetup.get());
    std::shared_ptr<TSet<ui32>> badSteps(new TSet<ui32>());

    TManyMultiPutsTest w(false, settings.MsgNum, settings.MsgSize, settings.BatchSize, settings.Cls, badSteps);
    bool success1 = Conf.Run<TManyMultiPutsTest>(&w, testTimeout);
    UNIT_ASSERT(success1);
    Conf.Shutdown();

    if (settings.MsgSize < 65536) { // huge blob
        Conf.Prepare(settings.ReadRunSetup.get(), false);
        TManyGetsTest r(false, settings.MsgNum, settings.MsgSize, settings.Cls, badSteps);
        bool success2 = Conf.Run<TManyGetsTest>(&r, testTimeout);
        UNIT_ASSERT(success2);
        Conf.Shutdown();
    }
}

void ChaoticWriteRestartWrite(const TChaoticWriteRestartWriteSettings &settings, TDuration testTimeout) {
    TConfiguration Conf;
    Conf.Prepare(settings.WriteRunSetup.get());

    auto cls1 = std::make_shared<TPutHandleClassGenerator>(settings.Cls);
    TChaoticManyPutsTest w(settings.Parallel, settings.MsgNum, settings.MsgSize, cls1, settings.WorkingTime,
        settings.RequestTimeout);
    bool success1 = Conf.Run<TChaoticManyPutsTest>(&w, testTimeout);
    LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "Chaotic write done");
    UNIT_ASSERT(success1);
    Conf.Shutdown();

    Conf.Prepare(settings.SecondWriteRunSetup.get(), false);
    auto cls2 = std::make_shared<TPutHandleClassGenerator>(settings.Cls);
    TChaoticManyPutsTest x(settings.Parallel, 1, settings.MsgSize, cls2, settings.WorkingTime,
        settings.RequestTimeout);
    bool success2 = Conf.Run<TChaoticManyPutsTest>(&x, testTimeout);
    LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "System has been restarted");
    UNIT_ASSERT(success2);
    Conf.Shutdown();
}

