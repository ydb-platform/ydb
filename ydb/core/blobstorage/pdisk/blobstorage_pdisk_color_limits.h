#pragma once 
#include "defs.h" 
#include "blobstorage_pdisk_defs.h" 
 
namespace NKikimr { 
namespace NPDisk { 
 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
// Color limits for the Quota Tracker 
// 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
 
struct TColorLimits { 
    i64 BlackMultiplier = 0; 
    i64 BlackDivisor = 1; 
    i64 BlackAddend = 0; 
 
    i64 RedMultiplier = 0; 
    i64 RedDivisor = 1; 
    i64 RedAddend = 0; 
 
    i64 OrangeMultiplier = 0; 
    i64 OrangeDivisor = 1; 
    i64 OrangeAddend = 0; 
 
    i64 LightOrangeMultiplier = 0;
    i64 LightOrangeDivisor = 1;
    i64 LightOrangeAddend = 0;

    i64 YellowMultiplier = 0; 
    i64 YellowDivisor = 1; 
    i64 YellowAddend = 0; 
 
    i64 LightYellowMultiplier = 0; 
    i64 LightYellowDivisor = 1; 
    i64 LightYellowAddend = 0; 
 
    i64 CyanMultiplier = 0; 
    i64 CyanDivisor = 1; 
    i64 CyanAddend = 0; 
 
    void Print(IOutputStream &str) { 
        str << "  Black = Total * " << BlackMultiplier << " / " << BlackDivisor << " + " << BlackAddend << "\n";
        str << "  Red = Total * " << RedMultiplier << " / " << RedDivisor << " + " << RedAddend << "\n";
        str << "  Orange = Total * " << OrangeMultiplier << " / " << OrangeDivisor << " + " << OrangeAddend << "\n";
        str << "  LightOrange = Total * " << LightOrangeMultiplier << " / " << LightOrangeDivisor << " + " << LightOrangeAddend << "\n";
        str << "  Yellow = Total * " << YellowMultiplier << " / " << YellowDivisor << " + " << YellowAddend << "\n";
        str << "  LightYellow = Total * " << LightYellowMultiplier << " / " << LightYellowDivisor << " + " << LightYellowAddend << "\n"; 
        str << "  Cyan = Total * " << CyanMultiplier << " / " << CyanDivisor << " + " << CyanAddend << "\n";
    } 
 
    static TColorLimits MakeChunkLimits() { 
        TColorLimits l; 
 
        l.BlackMultiplier = 1; // Leave bare minimum for disaster recovery 
        l.BlackDivisor = 1000; 
        l.BlackAddend = 2; 
 
        l.RedMultiplier = 10; 
        l.RedDivisor =  1000; 
        l.RedAddend = 3; 
 
        l.OrangeMultiplier = 30; 
        l.OrangeDivisor =  1000; 
        l.OrangeAddend = 4; 
 
        l.LightOrangeMultiplier = 65; 
        l.LightOrangeDivisor =  1000;
        l.LightOrangeAddend = 5; 

        l.YellowMultiplier = 80; // Stop serving user writes at 8% free space 
        l.YellowDivisor =  1000; 
        l.YellowAddend = 6; 
 
        l.LightYellowMultiplier = 100; // Ask tablets to move to another group at 10% free space 
        l.LightYellowDivisor =   1000; 
        l.LightYellowAddend = 7; 
 
        l.CyanMultiplier = 130; // 13% free space or less
        l.CyanDivisor =   1000; 
        l.CyanAddend = 8; 
 
        return l; 
    } 
 
    static TColorLimits MakeLogLimits() { 
        TColorLimits l; 
 
        l.BlackMultiplier = 250; // Stop early to leave some space for disaster recovery 
        l.BlackDivisor =   1000; 
 
        l.RedMultiplier = 350; 
        l.RedDivisor =   1000; 
 
        l.OrangeMultiplier = 500; 
        l.OrangeDivisor =   1000; 
 
        l.LightOrangeMultiplier = 700;
        l.LightOrangeDivisor =   1000;

        l.YellowMultiplier = 900; 
        l.YellowDivisor =   1000; 
 
        l.LightYellowMultiplier = 930; 
        l.LightYellowDivisor =   1000; 
 
        l.CyanMultiplier = 982; // Ask to cut log 
        l.CyanDivisor =   1000; 
 
        return l; 
    } 
}; 
 
} // NPDisk 
} // NKikimr 
 
