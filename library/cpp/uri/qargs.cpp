#include "qargs.h"
#include <string>
#include <vector>

namespace NUri {
    namespace NOnStackArgsList {
        struct TQArgNode {
            TStringBuf Name;
            TStringBuf Value;
            TStringBuf All;
        };

        const char* SkipDelimiter(const char* str, const char* end) {
            while (str != end)
                if (*str == '&')
                    ++str;
                else
                    break;
            return str;
        }

        /// return next pos or 0 if error
        const char* ExtractArgData(const char* pos, const char* end, TQArgNode& arg) {
            const char* nameStart = pos;
            const char* nextArg = strchr(pos, '&');
            const char* valueStart = strchr(pos, '=');
            if (valueStart && nextArg && valueStart < nextArg) // a=1& or a=&
            {
                arg.Name = TStringBuf(nameStart, valueStart - nameStart);
                arg.Value = TStringBuf(valueStart + 1, nextArg - valueStart - 1);
                arg.All = TStringBuf(nameStart, nextArg - nameStart);
                return nextArg;
            } else if (valueStart && nextArg && valueStart > nextArg) // a&b=2
            {
                arg.Name = TStringBuf(nameStart, nextArg - nameStart);
                arg.All = arg.Name;
                return nextArg;
            } else if (valueStart && !nextArg) // a=1 or a=
            {
                arg.Name = TStringBuf(nameStart, valueStart - nameStart);
                arg.Value = TStringBuf(valueStart + 1, end - valueStart - 1);
                arg.All = TStringBuf(nameStart, end - nameStart);
                return end;
            } else if (!valueStart && nextArg) // a&b
            {
                arg.Name = TStringBuf(nameStart, nextArg - nameStart);
                arg.All = arg.Name;
                return nextArg;
            } else { // a
                arg.Name = TStringBuf(nameStart, end - nameStart);
                arg.All = arg.Name;
                return end;
            }
        }
    }

    using namespace NOnStackArgsList;

    class TQueryArgProcessing::Pipeline {
    public:
        Pipeline(TQueryArgProcessing& parent, TUri& subject)
            : Parent(parent)
            , Subject(subject)
            , IsDirty(false)
        {
        }

        TQueryArg::EProcessed Process() {
            const TStringBuf& query = Subject.GetField(NUri::TField::FieldQuery);
            if (query.empty())
                return ProcessEmpty();

            const char* start = query.data();
            return Parse(start, start + query.length());
        }

        TQueryArg::EProcessed ProcessEmpty() {
            if (Parent.Flags & TQueryArg::FeatureRemoveEmptyQuery)
                Subject.FldClr(NUri::TField::FieldQuery);

            return TQueryArg::ProcessedOK;
        }

        TQueryArg::EProcessed Parse(const char* str, const char* end) {
            if (str != end)
                Nodes.reserve(8);

            while (str != end) {
                str = SkipDelimiter(str, end);

                TQArgNode current;
                str = ExtractArgData(str, end, current);

                if (!str)
                    return TQueryArg::ProcessedMalformed;

                if (Parent.Flags & TQueryArg::FeatureFilter) {
                    TQueryArg arg = {current.Name, current.Value};
                    if (!Parent.Filter(arg, Parent.FilterData)) {
                        IsDirty = true;
                        continue;
                    }
                }

                Nodes.push_back(current);
            }

            if (Parent.Flags & TQueryArg::FeatureSortByName) {
                std::stable_sort(Nodes.begin(), Nodes.end(), [](auto l, auto r) {
                    return l.Name < r.Name;
                });
                
                IsDirty = true;
            }

            return FinalizeParsing();
        }

        TQueryArg::EProcessed FinalizeParsing() {
            if (!IsDirty)
                return TQueryArg::ProcessedOK;

            bool dirty = Render();

            bool rewrite = Parent.Flags & TQueryArg::FeatureRewriteDirty;
            if (dirty && rewrite)
                Subject.Rewrite();
            return (!dirty || rewrite) ? TQueryArg::ProcessedOK : TQueryArg::ProcessedDirty;
        }

        bool Render() {
            std::string& result = Parent.Buffer;
            result.clear();
            result.reserve(Subject.GetField(NUri::TField::FieldQuery).length());

            bool first = true;
            for (const auto& node: Nodes)
            {
                if (!first)
                    result.append("&");
                result.append(node.All);
                first = false;
            }

            if (result.empty())
                return RenderEmpty();
            else
                return Subject.FldMemSet(NUri::TField::FieldQuery, result);
        }

        bool RenderEmpty() {
            if (Parent.Flags & TQueryArg::FeatureRemoveEmptyQuery)
                Subject.FldClr(NUri::TField::FieldQuery);
            return false;
        }

    private:
        TQueryArgProcessing& Parent;
        TUri& Subject;

        std::vector<TQArgNode> Nodes;
        bool IsDirty;
    };

    TQueryArgProcessing::TQueryArgProcessing(ui32 flags, TQueryArgFilter filter, void* filterData)
        : Flags(flags)
        , Filter(filter)
        , FilterData(filterData)
    {
    }

    TQueryArg::EProcessed TQueryArgProcessing::Process(TUri& uri) {
        Pipeline pipeline(*this, uri);
        return pipeline.Process();
    }
}
