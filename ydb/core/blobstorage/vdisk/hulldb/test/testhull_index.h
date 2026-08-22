#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

namespace NKikimr {
namespace NTest {

    //////////////////////////////////////////////////////////////////////////////////////////////
    // TLogoBlobsCmd
    //////////////////////////////////////////////////////////////////////////////////////////////
    struct TLogoBlobsCmd {
    private:
        TKeyLogoBlob Key;
        TMemRecLogoBlob MemRec;

    public:
        TLogoBlobsCmd(const TKeyLogoBlob& key, const TMemRecLogoBlob& memRec)
            : Key(key)
            , MemRec(memRec)
        {}

        TKeyLogoBlob GetKey() const {
            return ReadUnaligned<TKeyLogoBlob>(&Key);
        }

        TMemRecLogoBlob GetMemRec() const {
            return ReadUnaligned<TMemRecLogoBlob>(&MemRec);
        }

        bool operator <(const TLogoBlobsCmd &cmd) const {
            return GetKey() < cmd.GetKey();
        }
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // TBarriersCmd
    //////////////////////////////////////////////////////////////////////////////////////////////
    struct TBarriersCmd {
    private:
        TKeyBarrier Key;
        TMemRecBarrier MemRec;

    public:
        TBarriersCmd(const TKeyBarrier& key, const TMemRecBarrier& memRec)
            : Key(key)
            , MemRec(memRec)
        {}

        TKeyBarrier GetKey() const {
            return ReadUnaligned<TKeyBarrier>(&Key);
        }

        TMemRecBarrier GetMemRec() const {
            return ReadUnaligned<TMemRecBarrier>(&MemRec);
        }

        bool operator <(const TBarriersCmd &cmd) const {
            return GetKey() < cmd.GetKey();
        }
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // TTabletCommand atomic update for a HullDs from a tablet
    //////////////////////////////////////////////////////////////////////////////////////////////
    class TTabletCommand {
    public:
        TMaybe<TLogoBlobsCmd> LogoBlobsCmd;
        TMaybe<TBarriersCmd> BarriersCmd;
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // ITabletMock -- this class encapsultes test load for blobstorage from a tablet
    //////////////////////////////////////////////////////////////////////////////////////////////
    class ITabletMock {
    public:
        virtual const TTabletCommand &Next() = 0;
        virtual ~ITabletMock() = default;
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // IHullDsIndexMock -- interface for test hull ds generator
    //////////////////////////////////////////////////////////////////////////////////////////////
    class IHullDsIndexMock {
    public:
        virtual TIntrusivePtr<THullDs> GenerateHullDs() = 0;
        virtual ~IHullDsIndexMock() = default;
    };


    //////////////////////////////////////////////////////////////////////////////////////////////
    // DS generators
    //////////////////////////////////////////////////////////////////////////////////////////////
    TIntrusivePtr<THullDs> GenerateDs_17Level_Logs();

} // NTest
} // NKikimr
