#pragma once

#include "range.h"

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/stream/input.h>

namespace NIPREG {

class TReader {
public:
    TReader(const TString& filename = "", bool isEmptyData = false, const TString& dataDelim = "\t");
    TReader(IInputStream& stream, bool isEmptyData = false, const TString& dataDelim = "\t");

    virtual bool Next();

    virtual const TGenericEntry& Get() const {
        return CurrentEntry;
    }

    operator IInputStream&() {
        return Stream;
    }

    virtual ~TReader() = default;

private:
    TAutoPtr<IInputStream> OwnedStreamPtr;
    IInputStream& Stream;

    bool IsEmptyData = false;
    const TString DataDelim;

    TGenericEntry CurrentEntry;
};

class TReverseByLastIpReader : public TReader {
public:
    using TParent = TReader;

    explicit TReverseByLastIpReader(const TString& filename = "", bool isEmptyData = false, const TString& dataDelim = "\t");
    explicit TReverseByLastIpReader(IInputStream& stream, bool isEmptyData = false, const TString& dataDelim = "\t");

    bool Next() override;

    const TGenericEntry& Get() const override;

private:
    bool PrepareNextEntries();

private:
    bool Valid = false;
    TVector<TGenericEntry> CurrentEntries;
};

} // NIPREG
