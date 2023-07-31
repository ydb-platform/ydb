#include "split.h"

#include <util/generic/list.h>
#include <util/generic/vector.h>

namespace NIPREG {

void SplitIPREG(TReader &reader, std::function<void(const TAddress& first, const TAddress& last, const TVector<TString>& data)>&& proc) {
    TList<TGenericEntry> prevEntries;

    bool end;
    do {
        end = !reader.Next();

        while (!prevEntries.empty() && (end || prevEntries.front().First < reader.Get().First)) {
            // find smallest common range to process
            TAddress first = prevEntries.front().First;
            TAddress last = end ? TAddress::Highest() : reader.Get().First.Prev();

            for (const auto& entry: prevEntries)
                last = Min(last, entry.Last);

            // extract data for the range
            TVector<TString> strings;
            auto item = prevEntries.begin();
            while (item != prevEntries.end()) {
                Y_ASSERT(item->First == first);
                strings.push_back(item->Data);

                if (item->Last == last) {
                    // item completely processed, remove
                    auto victim = item;
                    item++;
                    prevEntries.erase(victim);
                } else {
                    // item still have part of range left, update it
                    item->First = last.Next();
                    item++;
                }
            }

            proc(first, last, strings);
        }

        if (!end) {
            if (!prevEntries.empty()) {
                Y_ASSERT(prevEntries.front().First == reader.Get().First);
            }
            prevEntries.push_back(reader.Get());
        }
    } while (!end);
}

}
