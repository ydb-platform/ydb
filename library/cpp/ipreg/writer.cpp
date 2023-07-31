#include "writer.h"

#include <util/stream/file.h>

namespace NIPREG {

TWriter::TWriter(const TString& fname)
    : OwnedStreamPtr(fname.empty() ? nullptr : new TFileOutput(fname))
    , Stream(OwnedStreamPtr ? *OwnedStreamPtr.Get() : Cout)
    , AddrSeparator(ADDR_SEP)
    , DataSeparator(DATA_SEP)
    , SplitMixed(false)
{
}

TWriter::TWriter(IOutputStream& stream, EAddressFormat addressFormat, const TString& addrSep, const TString& dataSep, const bool splitMixed)
    : Stream(stream)
    , AddressFormat(addressFormat)
    , AddrSeparator(addrSep)
    , DataSeparator(dataSep)
    , SplitMixed(splitMixed)
{
}

namespace {
    const TAddress IPv4Start = TAddress::ParseIPv4("0.0.0.0");
    const TAddress IPv4End = TAddress::ParseIPv4("255.255.255.255");

    const TAddress IPv6BeforeV4 = IPv4Start.Prev();
    const TAddress IPv6AfterV4 = IPv4End.Next();
}

void TWriter::Write(const TAddress& first, const TAddress& last, const TString& data, bool printRange) {
    if (SplitMixed) {
        if (first < IPv4Start && IPv4Start < last) {
            Write(first, IPv6BeforeV4, data, printRange);
            Write(IPv4Start, last, data, printRange);
            return;
        }

        if (first < IPv4End && IPv4End < last) {
            Write(first, IPv4End, data, printRange);
            Write(IPv6AfterV4, last, data, printRange);
            return;
        }
    }
    WriteImpl(first, last, data, printRange);
}

void TWriter::WriteImpl(const TAddress& first, const TAddress& last, const TString& data, bool printRange) {
    if (printRange) {
        Stream << first.Format(AddressFormat) << AddrSeparator << last.Format(AddressFormat);
    }
    if (!data.empty()) {
        if (printRange) {
            Stream << DataSeparator;
        }
        Stream << data;
    }
    if (!data.empty() || printRange) {
        Stream << "\n";
    }
}

void TWriter::Finalize() {
}

TMergingWriter::TMergingWriter(IOutputStream& stream, EAddressFormat addressFormat, const TString& addrSep, const TString& dataSep, const bool splitMixed)
    : TWriter(stream, addressFormat, addrSep, dataSep, splitMixed) {
}

void TMergingWriter::Write(const TAddress& first, const TAddress& last, const TString& data, bool) {
    if (Initialized && data == StoredData && first == StoredLast.Next()) {
        StoredLast = last;
    } else {
        if (Initialized)
            TWriter::Write(StoredFirst, StoredLast, StoredData);
        StoredFirst = first;
        StoredLast = last;
        StoredData = data;
        Initialized = true;
    }
}

void TMergingWriter::Finalize() {
    if (Initialized)
        TWriter::Write(StoredFirst, StoredLast, StoredData);
    Initialized = false;
}

} // NIPREG
