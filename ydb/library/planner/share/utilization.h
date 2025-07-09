#pragma once

#include <ydb/library/planner/base/defs.h>
#include <ydb/library/planner/share/protos/shareplanner.pb.h>
#include <ydb/library/planner/share/protos/shareplanner_sensors.pb.h>

namespace NScheduling {

class TUtilizationMeter {
private:
    static const unsigned Slots = 2*gX; // Number of slots in averaging window
    static const TEnergy SlotCapacity = gV/Slots; // Size of a slot in res*sec
    TEnergy DoneInSlot[Slots];
    TEnergy DoneInWindow; // Sum of values DoneInSlot[i] for all i
    TEnergy SlotVolume; // Done in slot plus wasted in slot
    unsigned CurrentSlot;
public:
    TUtilizationMeter();
    void Add(TEnergy cost, bool wasted);
    void Save(TUtilizationHistory* history) const;
    void Load(const TUtilizationHistory& history);
public: // Accessors
    inline double Utilization() const { return double(DoneInWindow)/(gV-SlotCapacity+SlotVolume); }
    inline TEnergy Used() const { return DoneInWindow; }
    inline TEnergy Wasted() const { return (gV-SlotCapacity+SlotVolume) - DoneInWindow; }
public: // Monitoring and sensors
    void GetSensors(TSharePlannerSensors& sensors) const;
private:
    void AddToSlot(TEnergy cost, bool wasted);
    void NextSlot();
};

}
