#include "markov_model_builder.h"

#include <library/cpp/streams/factory/open_by_signature/factory.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/stream/zlib.h>
#include <util/generic/deque.h>
#include <util/string/builder.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <functional>

namespace NYdb::NConsoleClient {
    namespace {
        struct TTransitionCounts {
            THashMap<TString, ui64> Counts;
            ui64 Total = 0;
        };

        TString NormalizeToken(TStringBuf token) {
            size_t left = 0;
            while (left < token.size() && !std::isalnum((unsigned char)token[left])) {
                ++left;
            }
            size_t right = token.size();
            while (right > left && !std::isalnum((unsigned char)token[right - 1])) {
                --right;
            }
            if (left >= right) {
                return {};
            }
            TString result(token.SubStr(left, right - left));
            for (char& c : result) {
                c = std::tolower((unsigned char)c);
            }
            return result;
        }

        TString MakeKey(const TDeque<TString>& context) {
            TStringBuilder sb;
            bool first = true;
            for (const TString& w : context) {
                if (!first) {
                    sb << " ";
                }
                first = false;
                sb << w;
            }
            return sb;
        }

        void ProcessText(TStringBuf text, int order,
                         THashMap<TString, TTransitionCounts>& transitions) {
            static constexpr TStringBuf kStart = "__START__";
            static constexpr TStringBuf kEnd = "__END__";

            TVector<TString> tokens;
            for (TStringBuf word : StringSplitter(text).Split(' ').SkipEmpty()) {
                TString norm = NormalizeToken(word);
                if (!norm.empty()) {
                    tokens.push_back(std::move(norm));
                }
            }
            if (tokens.empty()) {
                return;
            }

            TDeque<TString> context;
            for (int i = 0; i < order; ++i) {
                context.push_back(TString(kStart));
            }

            for (const TString& token : tokens) {
                TString key = MakeKey(context);
                auto& trans = transitions[key];
                ++trans.Counts[token];
                ++trans.Total;

                context.pop_front();
                context.push_back(token);
            }

            TString key = MakeKey(context);
            auto& trans = transitions[key];
            ++trans.Counts[TString(kEnd)];
            ++trans.Total;
        }

        TVector<TString> SplitCsvLine(TStringBuf line, char delimiter) {
            TVector<TString> fields;
            TString field;
            bool inQuotes = false;
            for (size_t i = 0; i < line.size(); ++i) {
                const char c = line[i];
                if (inQuotes) {
                    if (c == '"') {
                        if (i + 1 < line.size() && line[i + 1] == '"') {
                            field += '"';
                            ++i;
                        } else {
                            inQuotes = false;
                        }
                    } else {
                        field += c;
                    }
                } else {
                    if (c == '"') {
                        inQuotes = true;
                    } else if (c == delimiter) {
                        fields.push_back(std::move(field));
                        field.clear();
                    } else {
                        field += c;
                    }
                }
            }
            fields.push_back(std::move(field));
            return fields;
        }

        void ReadCsvFile(const TString& path, char delimiter,
                         const std::function<void(TStringBuf)>& callback) {
            auto input = OpenOwnedMaybeCompressedInput(MakeHolder<TFileInput>(path));
            TString headerLine;
            if (!input->ReadLine(headerLine)) {
                return;
            }

            TVector<TString> headers = SplitCsvLine(headerLine, delimiter);
            int textIdx = -1;
            for (int i = 0; i < (int)headers.size(); ++i) {
                if (StripString(TStringBuf(headers[i])) == "text") {
                    textIdx = i;
                    break;
                }
            }
            if (textIdx < 0) {
                ythrow yexception() << "Column 'text' not found in " << path;
            }

            TString line;
            while (input->ReadLine(line)) {
                TVector<TString> fields = SplitCsvLine(line, delimiter);
                if (textIdx < (int)fields.size()) {
                    callback(fields[textIdx]);
                }
            }
        }

        void ProcessFile(const TString& path, int order,
                         THashMap<TString, TTransitionCounts>& transitions,
                         ui64& docCount) {
            const TString name = TFsPath(path).GetName();
            auto callback = [&](TStringBuf text) {
                ProcessText(text, order, transitions);
                ++docCount;
                if (docCount % 10000 == 0) {
                    Cerr << "\r  processed " << docCount << " documents..." << Flush;
                }
            };

            if (name.EndsWith(".tsv") || name.EndsWith(".tsv.gz")) {
                ReadCsvFile(path, '\t', callback);
            } else if (name.EndsWith(".csv") || name.EndsWith(".csv.gz")) {
                ReadCsvFile(path, ',', callback);
            } else {
                ythrow yexception() << "Unsupported file format: " << name
                                    << " (expected .csv[.gz] or .tsv[.gz])";
            }
        }

        int ScaleWeight(ui64 count, ui64 total, int maxWeight = 100) {
            if (total == 0) {
                return 1;
            }
            int w = static_cast<int>(std::round(static_cast<double>(count) / total * maxWeight));
            return std::max(w, 1);
        }

    } // namespace

    TMarkovModelBuilder::TMarkovModelBuilder()
        : TClientCommand("model", {}, "Build a Markov chain model from a text dataset")
    {
    }

    void TMarkovModelBuilder::Config(TConfig& config) {
        TClientCommand::Config(config);
        config.Opts->AddLongOption('i', "input",
                                   "Path to dataset file or directory. Supports .csv[.gz], .tsv[.gz]. "
                                   "The file must have a 'text' column.")
            .RequiredArgument("PATH")
            .Required()
            .StoreResult(&InputPath);
        config.Opts->AddLongOption('o', "output",
                                   "Output file path for the Markov chain dictionary")
            .DefaultValue(OutputPath)
            .StoreResult(&OutputPath);
        config.Opts->AddLongOption('n', "order",
                                   "Order of the Markov chain (n-gram size for context). "
                                   "Order 1 = unigram context (bigram model), order 2 = bigram context, etc.")
            .DefaultValue(Order)
            .StoreResult(&Order);
    }

    int TMarkovModelBuilder::Run(TConfig& /*config*/) {
        if (Order < 1 || Order > 5) {
            ythrow yexception() << "--order must be between 1 and 5";
        }

        THashMap<TString, TTransitionCounts> transitions;
        ui64 docCount = 0;

        const TFsPath inputPath(InputPath);
        if (inputPath.IsDirectory()) {
            TVector<TFsPath> children;
            inputPath.List(children);
            std::sort(children.begin(), children.end(), [](const TFsPath& a, const TFsPath& b) {
                return a.GetName() < b.GetName();
            });
            Cerr << "Found " << children.size() << " entries in directory" << Endl;
            for (const auto& child : children) {
                if (!child.IsFile()) {
                    continue;
                }
                Cerr << "Processing: " << child.GetName() << Endl;
                ProcessFile(child.GetPath(), Order, transitions, docCount);
                Cerr << Endl;
            }
        } else {
            ProcessFile(InputPath, Order, transitions, docCount);
            Cerr << Endl;
        }

        Cerr << "Total documents processed: " << docCount << Endl;
        Cerr << "Unique n-gram contexts: " << transitions.size() << Endl;
        Cerr << "Writing model to: " << OutputPath << Endl;

        TVector<TString> keys;
        keys.reserve(transitions.size());
        for (const auto& [key, _] : transitions) {
            keys.push_back(key);
        }
        std::sort(keys.begin(), keys.end());

        TFileOutput fileOut(OutputPath);
        TZLibCompress out(&fileOut, ZLib::GZip);

        out << "#order=" << Order << "\n";

        for (const TString& key : keys) {
            const auto& trans = transitions.at(key);

            TVector<std::pair<TString, ui64>> sorted(trans.Counts.begin(), trans.Counts.end());
            std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });
            constexpr size_t kMaxSuccessors = 200;
            if (sorted.size() > kMaxSuccessors) {
                sorted.resize(kMaxSuccessors);
            }

            ui64 truncTotal = 0;
            for (const auto& [_, cnt] : sorted) {
                truncTotal += cnt;
            }

            out << key;
            for (const auto& [word, count] : sorted) {
                int w = ScaleWeight(count, truncTotal);
                out << "\t" << word << ":" << w;
            }
            out << "\n";
        }

        out.Finish();
        Cerr << "Done." << Endl;
        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
