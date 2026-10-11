#include "group_selector.h"

#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/mon.h>

#include <library/cpp/html/escape/escape.h>

#include <util/folder/path.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/tempfile.h>

#include <cerrno>

namespace NKikimr {
namespace {

class TGroupSelectorMonActor : public NActors::TActorBootstrapped<TGroupSelectorMonActor> {
public:
    TGroupSelectorMonActor(TBlobStorageGroupSelector* selector, NActors::TMon* mon)
        : Selector(selector)
        , Mon(mon)
    {}

    void Bootstrap() {
        auto* index = Mon->RegisterIndexPage("actors", "Actors");
        Mon->RegisterActorPage(index, "blobstorage_group_selector", "BlobStorage emergency recovery groups",
            false, NActors::TActivationContext::ActorSystem(), SelfId());
        Become(&TThis::StateWork);
    }

private:
    void Handle(NActors::NMon::TEvHttpInfo::TPtr& ev) {
        const auto& request = ev->Get()->Request;
        TString message;
        if (!Selector->ReloadRules(&message)) {
            message = TString("Could not read rules file; showing the last loaded list: ") + message;
        } else if (request.GetMethod() == HTTP_METHOD_POST) {
            const auto& post = request.GetPostParams();
            if (post.Get("action") == "add") {
                if (Selector->AddRules(post.Get("rules"), &message)) {
                    message = "Rules saved. They apply after VDisk restart.";
                } else {
                    message = TString("Could not save rules: ") + message;
                }
            } else {
                message = "Unknown action.";
            }
        }

        const TString rules = Selector->GetRules();
        TStringStream html;
        html << "<h2>BlobStorage emergency recovery groups</h2>"
             << "<p>Rules file: <code>" << NHtml::EscapeText(Selector->GetRulesFilePath()) << "</code></p>"
             << "<p>One rule per line: <code>*</code> selects all groups, <code>g123</code> selects group 123, "
                "<code>pmy-pool-</code> selects pool names starting with my-pool-.</p>"
             << "<p>This page reloads the file. Additions are saved to the file on this node. VDisks apply the cached rules on startup.</p>";
        if (!message.empty()) {
            html << "<p>" << NHtml::EscapeText(message) << "</p>";
        }
        html << "<h3>Current rules</h3><pre>" << NHtml::EscapeText(rules.empty() ? TString("(empty)") : rules)
             << "</pre><form method=\"POST\">"
                "<input type=\"hidden\" name=\"action\" value=\"add\">"
                "<label for=\"rules\">Rules to add</label><br>"
                "<textarea id=\"rules\" name=\"rules\" rows=\"5\" cols=\"60\"></textarea><br>"
                "<button type=\"submit\">Add rules</button></form>";
        Send(ev->Sender, new NActors::NMon::TEvHttpInfoRes(html.Str(), ev->Get()->SubRequestId), 0, ev->Cookie);
    }

    STRICT_STFUNC(StateWork,
        hFunc(NActors::NMon::TEvHttpInfo, Handle)
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway)
    )

    TBlobStorageGroupSelector* const Selector;
    NActors::TMon* const Mon;
};

} // namespace

TBlobStorageGroupSelector::TBlobStorageGroupSelector(TString rulesFilePath, NActors::TMon* mon, ui32 poolId)
    : RulesFilePath(std::move(rulesFilePath))
    , Mon(mon)
    , PoolId(poolId)
{
    Rules = ReadRulesFile();
}

TBlobStorageGroupSelector::TRules TBlobStorageGroupSelector::ReadRulesFile() const {
    if (!RulesFilePath.empty()) {
        try {
            return ParseRules(TFileInput(RulesFilePath).ReadAll());
        } catch (const TFileError& e) {
            if (e.Status() != ENOENT) {
                throw;
            }
        }
    }
    return {};
}

void TBlobStorageGroupSelector::SaveRulesFile(const TRules& rules) const {
    if (RulesFilePath.empty()) {
        return;
    }
    const TFsPath path(RulesFilePath);
    path.Parent().MkDirs();
    auto temporary = TTempFileHandle::InDir(path.Parent(), path.GetName());
    const TString data = rules.ToString();
    temporary.Write(data.data(), data.size());
    temporary.FlushData();
    temporary.Close();
    TFsPath(temporary.Name()).RenameTo(path);
}

TBlobStorageGroupSelector::TRules TBlobStorageGroupSelector::ParseRules(TStringBuf text) {
    TRules result;
    TStringBuf line;
    ui32 lineNumber = 0;
    while (text.ReadLine(line)) {
        ++lineNumber;
        line = StripString(line);
        if (line.empty() || line.StartsWith('#')) {
            continue;
        }
        if (line == "*") {
            result.AllGroups = true;
        } else if (line.StartsWith('g')) {
            const TStringBuf value = line.SubStr(1);
            ui32 groupId;
            bool digitsOnly = !value.empty();
            for (const char c : value) {
                digitsOnly &= c >= '0' && c <= '9';
            }
            Y_ENSURE(digitsOnly && TryFromString(value, groupId), "Invalid group rule at line " << lineNumber);
            result.Groups.insert(groupId);
        } else if (line.StartsWith('p') && line.size() > 1) {
            result.PoolPrefixes.emplace(line.SubStr(1));
        } else {
            ythrow yexception() << "Invalid selection rule at line " << lineNumber;
        }
    }
    return result;
}

TString TBlobStorageGroupSelector::TRules::ToString() const {
    TStringStream text;
    if (AllGroups) {
        text << "*\n";
    }
    const std::set<ui32> groups(Groups.begin(), Groups.end());
    for (const ui32 groupId : groups) {
        text << 'g' << groupId << '\n';
    }
    for (const auto& prefix : PoolPrefixes) {
        text << 'p' << prefix << '\n';
    }
    return text.Str();
}

bool TBlobStorageGroupSelector::Matches(ui32 groupId, TStringBuf storagePoolName) const {
    const TGuard<TMutex> guard(Mutex);
    if (Rules.AllGroups || Rules.Groups.contains(groupId)) {
        return true;
    }
    for (const auto& prefix : Rules.PoolPrefixes) {
        if (storagePoolName.StartsWith(prefix)) {
            return true;
        }
    }
    return false;
}

TString TBlobStorageGroupSelector::GetRules() const {
    const TGuard<TMutex> guard(Mutex);
    return Rules.ToString();
}

bool TBlobStorageGroupSelector::ReloadRules(TString* error) {
    try {
        const TGuard<TMutex> updateGuard(UpdateMutex);
        if (!RulesFilePath.empty()) {
            auto rules = ReadRulesFile();
            const TGuard<TMutex> guard(Mutex);
            Rules = std::move(rules);
        }
        error->clear();
        return true;
    } catch (const yexception& e) {
        *error = e.what();
        return false;
    }
}

bool TBlobStorageGroupSelector::AddRules(TStringBuf rules, TString* error) {
    try {
        const auto additions = ParseRules(rules);
        const TGuard<TMutex> updateGuard(UpdateMutex);
        TRules updated;
        if (RulesFilePath.empty()) {
            const TGuard<TMutex> guard(Mutex);
            updated = Rules;
        } else {
            updated = ReadRulesFile();
        }
        updated.AllGroups |= additions.AllGroups;
        updated.Groups.insert(additions.Groups.begin(), additions.Groups.end());
        updated.PoolPrefixes.insert(additions.PoolPrefixes.begin(), additions.PoolPrefixes.end());
        SaveRulesFile(updated);
        {
            const TGuard<TMutex> guard(Mutex);
            Rules = std::move(updated);
        }
        error->clear();
        return true;
    } catch (const yexception& e) {
        *error = e.what();
        return false;
    }
}

const TString& TBlobStorageGroupSelector::GetRulesFilePath() const {
    return RulesFilePath;
}

void TBlobStorageGroupSelector::OnAfterStart(NActors::TActorSystem& actorSystem) {
    if (Mon) {
        actorSystem.Register(new TGroupSelectorMonActor(this, Mon), NActors::TMailboxType::Simple, PoolId);
    }
}

} // namespace NKikimr
