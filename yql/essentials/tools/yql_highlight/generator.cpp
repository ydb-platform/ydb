#include "generator.h"

#include <util/stream/file.h>

namespace NSQLHighlight {

class TOnlyFunctionGenerator: public IGenerator {
public:
    explicit TOnlyFunctionGenerator(TGeneratorFunction function)
        : Function_(std::move(function))
    {
    }

    void Write(IOutputStream& out, const THighlighting& highlighting, bool ansi) override {
        Function_(out, highlighting, ansi);
    }

    void Write(const TFsPath& path, const THighlighting& highlighting, bool ansi) override {
        TFileOutput out(path);
        Write(out, highlighting, ansi);
    }

private:
    TGeneratorFunction Function_;
};

IGenerator::TPtr MakeOnlyFileGenerator(TGeneratorFunction function) {
    return new TOnlyFunctionGenerator(std::move(function));
}

} // namespace NSQLHighlight
