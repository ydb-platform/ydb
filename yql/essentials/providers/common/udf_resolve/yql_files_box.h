#pragma once

#include <yql/essentials/utils/rand_guid.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/folder/path.h>

namespace NYql::NCommon {

/*
  Resembles sandbox for external UDFs
*/
class TFilesBox {
public:
    TFilesBox(TFsPath dir, TRandGuid randGuid);
    ~TFilesBox();

    TString MakeLinkFrom(const TString& source, const TString& filename = {});
    TString GetDir() const;
    const TVector<TString>& GetLinks() const;

    void Destroy();

private:
    TFsPath Dir_;
    TRandGuid RandGuid_;
    THashMap<TString, TString> Mapping_;
    TVector<TString> Links_;
};

THolder<TFilesBox> CreateFilesBox(const TFsPath& baseDir);

} // namespace NYql::NCommon
