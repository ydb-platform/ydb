#include "qargs.h"
#include <string>

namespace NUri {
    namespace NOnStackArgsList { 
        struct TQArgNode { 
            TQArgNode* Prev; 
            TQArgNode* Next; 

            TStringBuf Name; 
            TStringBuf Value; 
            TStringBuf All; 
        }; 

        TQArgNode MakeArg(TQArgNode* prev) { 
            return {prev, 0, {}, {}, {}}; 
        } 

        const char* SkipDelimiter(const char* str, const char* end) { 
            while (str != end) 
                if (*str == '&') 
                    ++str; 
                else 
                    break; 
            return str; 
        } 

        /// return next pos or 0 if error 
        const char* ExtractArgData(const char* pos, const char* end, TQArgNode* arg) { 
            const char* nameStart = pos; 
            const char* nextArg = strchr(pos, '&');
            const char* valueStart = strchr(pos, '=');
            if (valueStart && nextArg && valueStart < nextArg) // a=1& or a=&
            {
                arg->Name = TStringBuf(nameStart, valueStart - nameStart);
                arg->Value = TStringBuf(valueStart + 1, nextArg - valueStart - 1);
                arg->All = TStringBuf(nameStart, nextArg - nameStart);
                return nextArg;
            } else if (valueStart && nextArg && valueStart > nextArg) // a&b=2
            {
                arg->Name = TStringBuf(nameStart, nextArg - nameStart);
                arg->All = arg->Name;
                return nextArg;
            } else if (valueStart && !nextArg) // a=1 or a=
            {
                arg->Name = TStringBuf(nameStart, valueStart - nameStart);
                arg->Value = TStringBuf(valueStart + 1, end - valueStart - 1);
                arg->All = TStringBuf(nameStart, end - nameStart);
                return end;
            } else if (!valueStart && nextArg) // a&b
            {
                arg->Name = TStringBuf(nameStart, nextArg - nameStart);
                arg->All = arg->Name;
                return nextArg;
            } else { // a
                arg->Name = TStringBuf(nameStart, end - nameStart);
                arg->All = arg->Name;
                return end;
            }
        } 

        // arg can be null 
        TQArgNode* GetHead(TQArgNode* arg) { 
            TQArgNode* prev = arg; 
            while (prev) { 
                arg = prev; 
                prev = prev->Prev; 
            } 
            return arg; 
        } 

        // arg can be null 
        TQArgNode* GetLast(TQArgNode* arg) { 
            TQArgNode* next = arg; 
            while (next) { 
                arg = next; 
                next = arg->Next; 
            } 
            return arg; 
        } 

        int CompareName(const TQArgNode* l, const TQArgNode* r) { 
            return l->Name.compare(r->Name); 
        } 

        TQArgNode* Move(TQArgNode* before, TQArgNode* node) { 
            TQArgNode* tn = node->Next; 
            TQArgNode* tp = node->Prev; 

            node->Prev = before->Prev; 
            if (node->Prev) 
                node->Prev->Next = node; 

            node->Next = before; 
            before->Prev = node; 

            if (tn) 
                tn->Prev = tp; 
            if (tp) 
                tp->Next = tn; 

            return node; 
        } 

        // return new head 
        TQArgNode* QSortByName(TQArgNode* iter, TQArgNode* last) { 
            if (iter == last) 
                return iter; 
            if (iter->Next == last) { 
                int c = CompareName(iter, last); 
                return c <= 0 ? iter : Move(iter, last); 
            } else { 
                TQArgNode* pivot = iter; 
                iter = iter->Next; 
                TQArgNode* head = 0; 
                TQArgNode* tail = 0; 
                TQArgNode* tailPartitionStart = pivot; 
                while (true) { 
                    TQArgNode* next = iter->Next; 
                    int c = CompareName(iter, pivot); 
                    int sign = (0 < c) - (c < 0); 
                    switch (sign) { 
                        case -1: 
                            head = head ? Move(head, iter) : Move(pivot, iter); 
                            break; 

                        case 0: 
                            pivot = Move(pivot, iter); 
                            break; 

                        case 1: 
                            tail = iter; 
                            break; 
                    } 

                    if (iter == last) 
                        break; 
                    iter = next; 
                } 

                if (head) 
                    head = QSortByName(head, pivot->Prev); 
                if (tail) 
                    QSortByName(tailPartitionStart->Next, tail); 
                return head ? head : pivot; 
            } 
        }
    }

    using namespace NOnStackArgsList; 

    class TQueryArgProcessing::Pipeline { 
    public: 
        Pipeline(TQueryArgProcessing& parent, TUri& subject) 
            : Parent(parent) 
            , Subject(subject) 
            , ArgsCount(0) 
            , IsDirty(false) 
        { 
        } 

        TQueryArg::EProcessed Process() { 
            const TStringBuf& query = Subject.GetField(NUri::TField::FieldQuery); 
            if (query.empty()) 
                return ProcessEmpty(); 

            const char* start = query.data(); 
            return Parse(start, start + query.length(), 0); 
        } 

        TQueryArg::EProcessed ProcessEmpty() { 
            if (Parent.Flags & TQueryArg::FeatureRemoveEmptyQuery) 
                Subject.FldClr(NUri::TField::FieldQuery); 

            return TQueryArg::ProcessedOK; 
        } 

        TQueryArg::EProcessed Parse(const char* str, const char* end, TQArgNode* prev) { 
            str = SkipDelimiter(str, end); 

            if (str == end) { 
                TQArgNode* head = GetHead(prev); 
                TQArgNode* last = GetLast(prev); 
                return FinalizeParsing(head, last); 
            } else { 
                TQArgNode current = MakeArg(prev); 
                const char* next = ExtractArgData(str, end, &current); 
                if (!next) 
                    return TQueryArg::ProcessedMalformed; 

                TQArgNode* tail = ApplyFilter(prev, &current); 

                if (++ArgsCount > MaxCount) 
                    return TQueryArg::ProcessedTooMany; 

                return Parse(next, end, tail); 
            } 
        } 

        TQArgNode* ApplyFilter(TQArgNode* prev, TQArgNode* current) { 
            if (Parent.Flags & TQueryArg::FeatureFilter) { 
                TQueryArg arg = {current->Name, current->Value}; 
                if (!Parent.Filter(arg, Parent.FilterData)) { 
                    IsDirty = true; 
                    return prev; 
                } 
            } 
 
            if (prev) 
                prev->Next = current; 
            return current; 
        }

        TQueryArg::EProcessed FinalizeParsing(TQArgNode* head, TQArgNode* last) { 
            if (Parent.Flags & TQueryArg::FeatureSortByName) { 
                head = QSortByName(head, last); 
                IsDirty = true; 
            } 

            if (!IsDirty) 
                return TQueryArg::ProcessedOK; 

            bool dirty = Render(head); 

            bool rewrite = Parent.Flags & TQueryArg::FeatureRewriteDirty; 
            if (dirty && rewrite) 
                Subject.Rewrite(); 
            return (!dirty || rewrite) ? TQueryArg::ProcessedOK : TQueryArg::ProcessedDirty; 
        } 

        bool Render(TQArgNode* head) { 
            std::string& result = Parent.Buffer; 
            result.clear(); 
            result.reserve(Subject.GetField(NUri::TField::FieldQuery).length()); 
            bool first = true; 
            while (head) { 
                if (first) 
                    first = false; 
                else 
                    result.append("&"); 

                result.append(head->All); 
                head = head->Next; 
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

        unsigned ArgsCount; 
        bool IsDirty; 

        static const unsigned MaxCount = 100; 
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
