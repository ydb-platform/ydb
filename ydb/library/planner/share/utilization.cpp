#include "utilization.h"

namespace NScheduling {

TUtilizationMeter::TUtilizationMeter()
{
    for (unsigned i = 0; i < Slots; i++) {
        DoneInSlot[i] = 0;
    }
    DoneInWindow = 0;
    SlotVolume = 0;
    CurrentSlot = 0;
}

void TUtilizationMeter::AddToSlot(TEnergy cost, bool wasted)
{
    SlotVolume += cost;
    Y_ASSERT(SlotVolume <= SlotCapacity && "slot overflow");
    if (!wasted) {
        DoneInSlot[CurrentSlot] += cost;
        DoneInWindow += cost;
    }
}

void TUtilizationMeter::NextSlot()
{
    Y_ASSERT(SlotVolume == SlotCapacity && "previous slot must be filled before moving to the next slot");
    CurrentSlot = (CurrentSlot + 1)%Slots;
    DoneInWindow -= DoneInSlot[CurrentSlot];
    DoneInSlot[CurrentSlot] = 0;
    SlotVolume = 0;
}

void TUtilizationMeter::Add(TEnergy cost, bool wasted)
{
    while (SlotVolume + cost > SlotCapacity) {
        TEnergy added = SlotCapacity - SlotVolume;
        AddToSlot(added, wasted);
        cost -= added;
        NextSlot();
    }
    AddToSlot(cost, wasted);
}

void TUtilizationMeter::Save(TUtilizationHistory* history) const
{
    for (unsigned i = 0; i < Slots; i++) {
        history->AddDoneInSlot(DoneInSlot[i]);
    }
    history->SetSlotVolume(SlotVolume);
    history->SetCurrentSlot(CurrentSlot);
}

void TUtilizationMeter::Load(const TUtilizationHistory& history)
{
    DoneInWindow = 0;
    for (unsigned i = 0; i < Slots; i++) {
        DoneInWindow += (DoneInSlot[i] = history.GetDoneInSlot(i));
    }
    SlotVolume = history.GetSlotVolume();
    CurrentSlot = history.GetCurrentSlot();
}

void TUtilizationMeter::GetSensors(TSharePlannerSensors& sensors) const
{
    sensors.SetResUsedInWindow(double(Used())*1000/gV);
    sensors.SetResWastedInWindow(double(Wasted())*1000/gV);
    sensors.SetUtilization(Utilization());
}

}
