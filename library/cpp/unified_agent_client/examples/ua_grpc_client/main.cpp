#include <library/cpp/unified_agent_client/client.h>

#include <library/cpp/getopt/opt.h>

#include <util/string/split.h>

using namespace NUnifiedAgent;

class TOptions {
public:
    TString Uri;
    TString SharedSecretKey;
    TString SessionId;
    TString SessionMeta;

    TOptions(int argc, const char* argv[]) {
        NLastGetopt::TOpts opts;
        TString logPriorityStr;

        opts
            .AddLongOption("uri")
            .RequiredArgument()
            .Required()
            .StoreResult(&Uri);
        opts
            .AddLongOption("shared-secret-key")
            .RequiredArgument()
            .Optional()
            .StoreResult(&SharedSecretKey);
        opts
            .AddLongOption("session-id")
            .RequiredArgument()
            .Optional()
            .StoreResult(&SessionId);
        opts
            .AddLongOption("session-meta", "key-value pairs separated by comma, e.g. 'k1=v1,k2=v2'")
            .RequiredArgument()
            .Optional()
            .StoreResult(&SessionMeta);

        opts.AddHelpOption();
        opts.AddVersionOption();
        NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    }
};

bool TryParseMeta(const TString& s, THashMap<TString, TString>& meta) {
    for (auto& t: StringSplitter(s).Split(',')) {
        TString key;
        TString value;
        if (!StringSplitter(t.Token()).Split('=').TryCollectInto(&key, &value)) {
            Cout << "invalid meta, can't extract key-value pair from [" << t.Token() << "]" << Endl;
            return false;
        }
        meta[key] = value;
    }
    return true;
}

bool TryParseLine(const TString& line, TVector<TString>& lineItems) {
    lineItems = StringSplitter(line).Split('|').ToList<TString>();
    Y_ABORT_UNLESS(lineItems.size() >= 1);
    if (lineItems.size() > 2) {
        Cout << "invalid line format, expected 'k1=v1,k2=v2|payload' or just 'payload'" << Endl;
        return false;
    }
    return true;
}

int main(int argc, const char* argv[]) {
    TOptions options(argc, argv);

    TClientSessionPtr sessionPtr;
    {
        TLog emptyLog;
        auto clientParameters = TClientParameters(options.Uri).SetLog(emptyLog);
        if (!options.SharedSecretKey.Empty()) {
            clientParameters.SetSharedSecretKey(options.SharedSecretKey);
        }
        auto clientPtr = MakeClient(clientParameters);
        auto sessionParameters = TSessionParameters();
        if (!options.SessionId.Empty()) {
            sessionParameters.SetSessionId(options.SessionId);
        }
        if (!options.SessionMeta.empty()) {
            THashMap<TString, TString> sessionMeta;
            if (!TryParseMeta(options.SessionMeta, sessionMeta)) {
                return -1;
            }
            sessionParameters.SetMeta(sessionMeta);
        }
        sessionPtr = clientPtr->CreateSession(sessionParameters);
    }

    TString line;
    while (true) {
        Cin.ReadLine(line);
        if (line.Empty()) {
            break;
        }

        TVector<TString> lineItems;
        if (!TryParseLine(line, lineItems)) {
            continue;
        }

        TClientMessage clientMessage;
        clientMessage.Payload = lineItems.back();
        if (lineItems.size() == 2) {
            THashMap<TString, TString> messageMeta;
            if (!TryParseMeta(lineItems[0], messageMeta)) {
                continue;
            }
            clientMessage.Meta = std::move(messageMeta);
        }
        sessionPtr->Send(std::move(clientMessage));
    }

    sessionPtr->Close();

    return 0;
}
