#include "ini.h"

#include <util/string/strip.h>
#include <util/stream/input.h>
#include <util/stream/mem.h>
#include <util/string/split.h>

namespace NIniConfig {

    namespace {
        TStringBuf StripComment(TStringBuf line) {
            return line.Before('#').Before(';');
        }
    }

    TConfig ParseIni(IInputStream& in) {
        TConfig ret(ConstructValue(TDict()));
        TConfig* cur = &ret;
        TString line;

        while (in.ReadLine(line)) {
            TStringBuf tmp = StripComment(line);
            TStringBuf stmp = StripString(tmp);

            if (stmp.empty()) {
                continue;
            }

            if (stmp.StartsWith('[') && stmp.EndsWith(']')) {
                stmp = TStringBuf(stmp.data() + 1, stmp.Size() - 2);
                cur = &ret;

                while (!!stmp) {
                    TStringBuf sectionPart;
                    stmp.Split('.', sectionPart, stmp);
                    cur = &cur->GetNonConstant<TDict>()[sectionPart];
                    if (!cur->IsA<TDict>()) {
                        *cur = TConfig(ConstructValue(TDict()));
                    }
                }
            } else {
                TStringBuf key, value;
                tmp.Split('=', key, value);
                auto& dict = cur->GetNonConstant<TDict>();
                dict[StripString(key)] = TConfig(ConstructValue(TString(StripString(value))));
            }
        }

        return ret;
    }

    TConfig TConfig::ReadIni(TStringBuf content) {
        TMemoryInput memIn(content.data(), content.size());
        return ParseIni(memIn);
    }

}
