#include "generator.h"

#include <util/stream/file.h>

namespace NSQLHighlight {

    class TOnlyFunctionGenerator: public IGenerator {
    public:
        explicit TOnlyFunctionGenerator(std::function<void(IOutputStream&, const THighlighting&)> function)
            : Function_(std::move(function))
        {
        }

        void Write(IOutputStream& out, const THighlighting& highlighting) override {
            Function_(out, highlighting);
        }

        void Write(const TFsPath& path, const THighlighting& highlighting) override {
            TFileOutput out(path);
            Write(out, highlighting);
        }

    private:
        std::function<void(IOutputStream&, const THighlighting&)> Function_;
    };

    IGenerator::TPtr MakeOnlyFileGenerator(std::function<void(IOutputStream&, const THighlighting&)> function) {
        return new TOnlyFunctionGenerator(std::move(function));
    }

} // namespace NSQLHighlight
