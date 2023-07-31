#pragma once

#include "range.h"

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NIPREG {

class TWriter {
public:
    static constexpr char const * const ADDR_SEP = "-";
    static constexpr char const * const DATA_SEP = "\t";

public:
    TWriter(const TString& filename = "");
    TWriter(IOutputStream& stream, EAddressFormat addressFormat = EAddressFormat::IPV6, const TString& addrSep = ADDR_SEP, const TString& dataSep = DATA_SEP, const bool splitMixed = false);
    TWriter(IOutputStream& stream, const TString& addrSep, EAddressFormat addressFormat)
        : TWriter(stream, addressFormat, addrSep, addrSep)
    {}
    virtual ~TWriter() {}

    void Write(const TGenericEntry& entry, bool printRange = true) {
        Write(entry.First, entry.Last, entry.Data, printRange);
    }
    virtual void Write(const TAddress& first, const TAddress& last, const TString& data, bool printRange = true);
    virtual void Finalize();

    operator IOutputStream&() {
        return Stream;
    }

private:
    void WriteImpl(const TAddress& first, const TAddress& last, const TString& data, bool printRange);

    TAutoPtr<IOutputStream> OwnedStreamPtr;
    IOutputStream& Stream;

    EAddressFormat AddressFormat = EAddressFormat::IPV6;
    const TString AddrSeparator = ADDR_SEP;
    const TString DataSeparator = DATA_SEP;
    const bool SplitMixed;
};

class TMergingWriter : public TWriter {
public:
    TMergingWriter(IOutputStream& stream, EAddressFormat addressFormat = EAddressFormat::IPV6, const TString& addrSep = ADDR_SEP, const TString& dataSep = DATA_SEP, const bool splitMixed = false);
    TMergingWriter(IOutputStream& stream, const TString& addrSep, EAddressFormat addressFormat)
        : TWriter(stream, addressFormat, addrSep, addrSep)
    {}
    void Write(const TAddress& first, const TAddress& last, const TString& data, bool printRange = true) final override;
    void Finalize() final;

private:
    TAddress StoredFirst;
    TAddress StoredLast;
    TString StoredData;
    bool Initialized = false;
};

} // NIPREG
