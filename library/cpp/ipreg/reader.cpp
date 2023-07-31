#include "reader.h"

#include <util/stream/file.h>

namespace NIPREG {

namespace {
    const TString DASH_FNAME = "-";
}

TReader::TReader(const TString& filename, bool isEmptyData, const TString& dataDelim)
    : OwnedStreamPtr((filename.empty() || filename == DASH_FNAME) ? nullptr : new TFileInput(filename))
    , Stream(OwnedStreamPtr ? *OwnedStreamPtr.Get() : Cin)
    , IsEmptyData(isEmptyData)
    , DataDelim(dataDelim)
{
}

TReader::TReader(IInputStream& stream, bool isEmptyData, const TString& dataDelim)
    : Stream(stream)
    , IsEmptyData(isEmptyData)
    , DataDelim(dataDelim)
{
}

bool TReader::Next() {
    TString line;
    if (!Stream.ReadLine(line))
        return false;

    CurrentEntry = TRange::BuildRange(line, IsEmptyData, DataDelim);
    if (CurrentEntry.Data.empty()) {
        if (!IsEmptyData) {
            throw yexception() << "empty data part detected for [" << line << "]";
        }
        CurrentEntry.Data = "";
    }
    return true;
}

TReverseByLastIpReader::TReverseByLastIpReader(const TString& filename, bool isEmptyData, const TString& dataDelim)
    : TParent(filename, isEmptyData, dataDelim)
{
    Valid = TParent::Next();
}

TReverseByLastIpReader::TReverseByLastIpReader(IInputStream& stream, bool isEmptyData, const TString& dataDelim)
    : TParent(stream, isEmptyData, dataDelim)
{
    Valid = TParent::Next();
}

bool TReverseByLastIpReader::Next() {
    if (!CurrentEntries.empty()) {
        CurrentEntries.pop_back();
    }

    if (CurrentEntries.empty()) {
        return PrepareNextEntries();
    } else {
        return true;
    }
}

const TGenericEntry& TReverseByLastIpReader::Get() const {
    return CurrentEntries.back();
}

bool TReverseByLastIpReader::PrepareNextEntries() {
    if (!Valid) {
        return false;
    }

    do {
        CurrentEntries.push_back(TParent::Get());
        Valid = TParent::Next();
    } while (Valid && TParent::Get().First == CurrentEntries.back().First);

    return true;
}

} // NIPREG
