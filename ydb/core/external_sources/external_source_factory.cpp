#include "external_source_factory.h"
#include "object_storage.h"

#include <util/generic/map.h>


namespace NKikimr::NExternalSource {

namespace {

struct TExternalSourceFactory : public IExternalSourceFactory {
    TExternalSourceFactory(const TMap<TString, IExternalSource::TPtr>& sources)
        : Sources(sources)
    {}

    IExternalSource::TPtr GetOrCreate(const TString& type) const override {
        auto it = Sources.find(type);
        if (it != Sources.end()) {
            return it->second;
        }
        ythrow TExternalSourceException() << "External source with type " << type << " was not found";
    }

private:
    TMap<TString, IExternalSource::TPtr> Sources;
};

}

IExternalSourceFactory::TPtr CreateExternalSourceFactory() {
    return MakeIntrusive<TExternalSourceFactory>(TMap<TString, IExternalSource::TPtr>{
        {"ObjectStorage", CreateObjectStorageExternalSource()}
    });
}

}
