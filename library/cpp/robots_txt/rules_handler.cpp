#include "robots_txt.h"
#include "constants.h"

#include <library/cpp/uri/http_url.h>
#include <library/cpp/charset/ci_string.h>
#include <library/cpp/string_utils/url/url.h>
#include <util/system/maxlen.h>
#include <util/generic/yexception.h>
#include <util/generic/algorithm.h>


namespace {

TBotIdSet ConvertBotIdSet(const TSet<ui32>& botIds) noexcept {
    TBotIdSet result;
    for (auto id : botIds) {
        result.insert(id);
    }
    return result;
}

} // namespace

TRobotsTxtRulesIterator::TRobotsTxtRulesIterator(const char* begin, const char* end)
    : Begin(begin)
    , End(end)
{
}

void TRobotsTxtRulesIterator::Next() {
    while (Begin < End && *Begin)
        ++Begin;
    while (Begin < End && !isalpha(*Begin))
        ++Begin;
}

bool TRobotsTxtRulesIterator::HasRule() const {
    return Begin < End;
}

const char* TRobotsTxtRulesIterator::GetRule() const {
    return Begin + 1;
}

TString TRobotsTxtRulesIterator::GetInitialRule() const {
    auto begin = Begin + 1;
    TStringBuf rule(begin, strlen(begin));

    switch (*Begin) {
    case 'a':
    case 'd':
        return rule.EndsWith('*') ? TString(rule.Chop(1)) : TString::Join(rule, '$');
    default:
        return TString(rule);
    }
}

EDirectiveType TRobotsTxtRulesIterator::GetRuleType() const {
    return CharToDirType(*Begin);
}

EDirectiveType TRobotsTxtRulesIterator::CharToDirType(char ch) {
    switch (toupper(ch)) {
        case 'A':
            return ALLOW;
        case 'C':
            return CRAWL_DELAY;
        case 'D':
            return DISALLOW;
        case 'H':
            return HOST;
        case 'P':
            return CLEAN_PARAM;
        case 'S':
            return SITEMAP;
    }
    return UNKNOWN;
}

TRobotsTxtRulesHandlerBase::TRobotsTxtRulesHandlerBase(
    TBotIdSet supportedBotIds,
    int robotsMaxSize,
    int maxRulesNumber,
    bool saveDataForAnyBot)
    : HandleErrors(false)
    , SiteMaps()
    , CleanParams()
    , HostDirective("")
    , Errors()
    , AcceptedLines()
    , CrossSectionAcceptedLines()
    , BotIdToInfo(robotstxtcfg::max_botid)
    , RobotsMaxSize(robotsMaxSize)
    , MaxRulesNumber(maxRulesNumber)
    , SaveDataForAnyBot(saveDataForAnyBot)
    , SupportedBotIds(supportedBotIds)
{
    Y_ENSURE(!supportedBotIds.empty());

    if (RobotsMaxSize <= 0)
        RobotsMaxSize = robots_max;
    if (MaxRulesNumber <= 0)
        MaxRulesNumber = max_rules_count;

    ResetOptimized();
}

TRobotsTxtRulesHandlerBase::TRobotsTxtRulesHandlerBase(
    const TSet<ui32>& supportedBotIds,
    int robotsMaxSize,
    int maxRulesNumber,
    bool saveDataForAnyBot)
    : TRobotsTxtRulesHandlerBase(ConvertBotIdSet(supportedBotIds), robotsMaxSize, maxRulesNumber, saveDataForAnyBot)
{}

TRobotsTxtRulesHandlerBase::~TRobotsTxtRulesHandlerBase() = default;

void TRobotsTxtRulesHandlerBase::CheckBotIdValidity(const ui32 botId) const {
    if (botId >= robotstxtcfg::max_botid || !IsBotIdSupported(botId))
        ythrow yexception() << "robots.txt parser requested for invalid or unsupported botId = " << botId << Endl;
    ;
}

int TRobotsTxtRulesHandlerBase::GetCrawlDelay(const ui32 botId, bool* realInfo) const {
    const auto id = GetMappedBotId(botId, false);
    if (realInfo)
        *realInfo = bool(id);
    return BotIdToInfo[id.GetOrElse(robotstxtcfg::id_anybot)].CrawlDelay;
}

int TRobotsTxtRulesHandlerBase::GetMinCrawlDelay(int defaultCrawlDelay) const {
    int res = INT_MAX;
    bool useDefault = false;
    for (ui32 botId = 0; botId < robotstxtcfg::max_botid; ++botId) {
        if (robotstxtcfg::IsYandexBotId(botId) && IsBotIdSupported(botId) && !IsDisallowAll(botId)) {
            bool realInfo;
            int curCrawlDelay = GetCrawlDelay(botId, &realInfo);
            if (realInfo) {
                if (curCrawlDelay == -1) {
                    useDefault = true;
                } else {
                    res = Min(res, curCrawlDelay);
                }
            }
        }
    }

    if (useDefault && defaultCrawlDelay < res) {
        return -1;
    }

    if (res == INT_MAX) {
        res = GetCrawlDelay(robotstxtcfg::id_anybot);
    }

    return res;
}

void TRobotsTxtRulesHandlerBase::SetCrawlDelay(const ui32 botId, int crawlDelay) {
    CheckBotIdValidity(botId);
    BotIdToInfo[botId].CrawlDelay = crawlDelay;
}

const TVector<TString> TRobotsTxtRulesHandlerBase::GetSiteMaps() const {
    return TVector<TString>(SiteMaps.begin(), SiteMaps.end());
}

void TRobotsTxtRulesHandlerBase::AddSiteMap(const char* sitemap) {
    SiteMaps.insert(sitemap);
}

const TVector<TString> TRobotsTxtRulesHandlerBase::GetCleanParams() const {
    return TVector<TString>(CleanParams.begin(), CleanParams.end());
}

void TRobotsTxtRulesHandlerBase::AddCleanParam(const char* cleanParam) {
    CleanParams.insert(cleanParam);
}

const TString& TRobotsTxtRulesHandlerBase::GetHostDirective() const {
    return HostDirective;
}

void TRobotsTxtRulesHandlerBase::SetHostDirective(const char* hostDirective) {
    HostDirective = hostDirective;
}

const TRobotsTxtRulesHandlerBase::TErrorVector& TRobotsTxtRulesHandlerBase::GetErrors() const {
    return Errors;
}

TVector<int> TRobotsTxtRulesHandlerBase::GetAcceptedLines(const ui32 botId) const {
    TVector<int> ret;
    for (size_t i = 0; i < CrossSectionAcceptedLines.size(); ++i)
        ret.push_back(CrossSectionAcceptedLines[i]);

    bool hasLinesForBotId = false;
    for (size_t i = 0; i < AcceptedLines.size(); ++i) {
        if (AcceptedLines[i].first == botId) {
            hasLinesForBotId = true;
            break;
        }
    }

    for (size_t i = 0; i < AcceptedLines.size(); ++i) {
        if (hasLinesForBotId && AcceptedLines[i].first == botId) {
            ret.push_back(AcceptedLines[i].second);
        } else if (!hasLinesForBotId && AcceptedLines[i].first == robotstxtcfg::id_anybot) {
            ret.push_back(AcceptedLines[i].second);
        }
    }

    Sort(ret.begin(), ret.end());

    return ret;
}

void TRobotsTxtRulesHandlerBase::AddAcceptedLine(ui32 line, const TBotIdSet& botIds, bool isCrossSection) {
    if (isCrossSection) {
        CrossSectionAcceptedLines.push_back(line);
        return;
    }

    for (auto botId : botIds) {
        AcceptedLines.push_back(TBotIdAcceptedLine(botId, line));
    }
}

void TRobotsTxtRulesHandlerBase::SetErrorsHandling(bool handleErrors) {
    HandleErrors = handleErrors;
}

bool TRobotsTxtRulesHandlerBase::IsHandlingErrors() const {
    return HandleErrors;
}

EDirectiveType TRobotsTxtRulesHandlerBase::NameToDirType(const char* d) {
    if (!strcmp("disallow", d))
        return DISALLOW;
    if (!strcmp("allow", d))
        return ALLOW;
    if (!strcmp("user-agent", d))
        return USER_AGENT;
    if (!strcmp("host", d))
        return HOST;
    if (!strcmp("sitemap", d))
        return SITEMAP;
    if (!strcmp("clean-param", d))
        return CLEAN_PARAM;
    if (!strcmp("crawl-delay", d))
        return CRAWL_DELAY;
    return UNKNOWN;
}

const char* TRobotsTxtRulesHandlerBase::DirTypeToName(EDirectiveType t) {
    static const char* name[] = {"Allow", "Crawl-Delay", "Disallow", "Host", "Clean-Param", "Sitemap", "User-Agent", "Unknown"};
    switch (t) {
        case ALLOW:
            return name[0];
        case CRAWL_DELAY:
            return name[1];
        case DISALLOW:
            return name[2];
        case HOST:
            return name[3];
        case CLEAN_PARAM:
            return name[4];
        case SITEMAP:
            return name[5];
        case USER_AGENT:
            return name[6];
        case UNKNOWN:
            return name[7];
    }
    return name[7];
}

bool TRobotsTxtRulesHandlerBase::CheckRobot(
    const char* userAgent,
    TBotIdSet& botIds,
    const TVector<ui32>* botIdToMaxAppropriateUserAgentNameLength) const
{
    TCaseInsensitiveStringBuf agent(userAgent);

    for (size_t botIndex = 0; botIndex < robotstxtcfg::max_botid; ++botIndex) {
        if (!IsBotIdSupported(botIndex))
            continue;

        bool hasRequiredAgentNamePrefix = agent.StartsWith(robotstxtcfg::GetReqPrefix(botIndex));
        bool isContainedInFullName = robotstxtcfg::GetFullName(botIndex).StartsWith(agent);
        bool wasMoreImportantAgent = false;
        if (botIdToMaxAppropriateUserAgentNameLength)
            wasMoreImportantAgent = agent.size() < (*botIdToMaxAppropriateUserAgentNameLength)[botIndex];

        if (hasRequiredAgentNamePrefix && isContainedInFullName && !wasMoreImportantAgent) {
            botIds.insert(botIndex);
        }
    }

    return !botIds.empty();
}

int TRobotsTxtRulesHandlerBase::CheckRule(const char* value, int line, TRobotsTxtRulesHandlerBase* rulesHandler) {
    if (!rulesHandler->IsHandlingErrors())
        return 0;

    if (auto len = strlen(value); len > max_rule_length) {
        rulesHandler->AddError(ERROR_RULE_HUGE, line);
    }

    bool upper = false, suspect = false;
    for (const char* r = value; *r; ++r) {
        if (!upper && isupper(*r))
            upper = true;
        if (!suspect && !isalnum(*r) && !strchr("/_?=.-*%&~[]:;@", *r) && (*(r + 1) || *r != '$'))
            suspect = true;
    }
    if (suspect)
        rulesHandler->AddError(WARNING_SUSPECT_SYMBOL, line);
    if (upper)
        rulesHandler->AddError(WARNING_UPPER_REGISTER, line);
    return suspect || upper;
}

void TRobotsTxtRulesHandlerBase::AddError(EFormatErrorType type, int line) {
    if (!HandleErrors)
        return;
    Errors.push_back(std::make_pair(type, line));
}

void TRobotsTxtRulesHandlerBase::ResetOptimized() noexcept {
    for (ui32 i = 0; i < OptimizedBotIdToStoredBotId.size(); ++i) {
        OptimizedBotIdToStoredBotId[i] = i; // by default, every bot maps to itself
    }
}

void TRobotsTxtRulesHandlerBase::Clear() {
    SiteMaps.clear();
    CleanParams.clear();
    HostDirective = "";
    if (HandleErrors) {
        AcceptedLines.clear();
        CrossSectionAcceptedLines.clear();
        Errors.clear();
    }

    for (size_t botId = 0; botId < BotIdToInfo.size(); ++botId) {
        BotIdToInfo[botId].CrawlDelay = -1;
    }

    LoadedBotIds.clear();
}

void TRobotsTxtRulesHandlerBase::ClearInternal(const ui32 botId) {
    CheckBotIdValidity(botId);
    BotIdToInfo[botId].CrawlDelay = -1;

    TVector<TBotIdAcceptedLine> newAcceptedLines;
    for (size_t i = 0; i < AcceptedLines.size(); ++i)
        if (AcceptedLines[i].first != botId)
            newAcceptedLines.push_back(AcceptedLines[i]);

    AcceptedLines.swap(newAcceptedLines);
}

int TRobotsTxtRulesHandlerBase::CheckHost(const char* host) {
    THttpURL parsed;
    TString copyHost = host;

    if (GetHttpPrefixSize(copyHost) == 0) {
        copyHost = TString("http://") + copyHost;
    }

    return parsed.Parse(copyHost.data(), THttpURL::FeaturesRobot) == THttpURL::ParsedOK && parsed.GetField(THttpURL::FieldHost) != TString("");
}

int TRobotsTxtRulesHandlerBase::CheckSitemapUrl(const char* url, const char* host, TString& modifiedUrl) {
    if (host != nullptr && strlen(url) > 0 && url[0] == '/') {
        modifiedUrl = TString(host) + url;
    } else {
        modifiedUrl = url;
    }

    url = modifiedUrl.data();

    if (strlen(url) >= URL_MAX - 8)
        return 0;
    THttpURL parsed;
    if (parsed.Parse(url, THttpURL::FeaturesRobot) || !parsed.IsValidAbs())
        return 0;
    if (parsed.GetScheme() != THttpURL::SchemeHTTP && parsed.GetScheme() != THttpURL::SchemeHTTPS)
        return 0;
    return CheckHost(parsed.PrintS(THttpURL::FlagHostPort).data());
}

// s - is space separated pair of clean-params (separated by &) and path prefix
int TRobotsTxtRulesHandlerBase::CheckAndNormCleanParam(TString& value) {
    if (value.find(' ') == TString::npos) {
        value.push_back(' ');
    }

    const char* s = value.data();
    if (!s || !*s || strlen(s) > URL_MAX / 2 - 9)
        return 0;
    const char* p = s;
    while (*p && !isspace(*p))
        ++p;
    for (; s != p; ++s) {
        // allowed only following not alpha-numerical symbols
        if (!isalnum(*s) && !strchr("+-=_&%[]{}():.", *s))
            return 0;
        // clean-params for prefix can be enumerated by & symbol, && not allowed syntax
        if (*s == '&' && *(s + 1) == '&')
            return 0;
    }
    const char* pathPrefix = p + 1;
    while (isspace(*p))
        ++p;
    char r[URL_MAX];
    char* pr = r;
    for (; *p; ++p) {
        if (!isalnum(*p) && !strchr(".-/*_,;:%", *p))
            return 0;
        if (*p == '*')
            *pr++ = '.';
        if (*p == '.')
            *pr++ = '\\';
        *pr++ = *p;
    }
    *pr++ = '.';
    *pr++ = '*';
    *pr = 0;
    TString params = value.substr(0, pathPrefix - value.data());
    value = params + r;
    return 1;
}

int TRobotsTxtRulesHandlerBase::ParseCrawlDelay(const char* value, int& crawlDelay) {
    static const int MAX_CRAWL_DELAY = 1 << 10;
    int val = 0;
    const char* p = value;
    for (; isdigit(*p); ++p) {
        val = val * 10 + *p - '0';
        if (val > MAX_CRAWL_DELAY)
            return 0;
    }
    if (*p) {
        if (*p++ != '.')
            return 0;
        if (strspn(p, "1234567890") != strlen(p))
            return 0;
    }
    for (const char* s = p; s - p < 3; ++s)
        val = val * 10 + (s < p + strlen(p) ? *s - '0' : 0);
    crawlDelay = val;
    return 1;
}

bool TRobotsTxtRulesHandlerBase::AddRuleWithErrorCheck(const ui32 botId, TStringBuf rule, char type, TRobotsTxtParser& parser) {
    if (!IsBotIdSupported(botId))
        return true;

    if (!AddRule(botId, rule, type)) {
        AddError(ERROR_ROBOTS_HUGE, parser.GetLineNumber());
        AfterParse(botId);
        return false;
    }
    return true;
}

int TRobotsTxtRulesHandlerBase::OnHost(const ui32 botId, TRobotsTxtParser& parser, const char* value, TRobotsTxtRulesHandlerBase*& rulesHandler) {
    // Temporary hack for correct repacking robots.txt from new format to old
    // Remove it, when robot-stable-2010-10-17 will be deployed in production
    if (!IsBotIdSupported(botId))
        return 0;
    // end of hack

    if (rulesHandler->HostDirective != "")
        rulesHandler->AddError(ERROR_HOST_MULTI, parser.GetLineNumber());
    else {
        if (!CheckHost(value))
            rulesHandler->AddError(ERROR_HOST_FORMAT, parser.GetLineNumber());
        else {
            rulesHandler->SetHostDirective(value);
            if (!rulesHandler->AddRuleWithErrorCheck(botId, value, 'H', parser))
                return 2;
        }
    }
    return 0;
}

bool TRobotsTxtRulesHandlerBase::IsBotIdLoaded(const ui32 botId) const {
    return LoadedBotIds.contains(botId);
}

bool TRobotsTxtRulesHandlerBase::IsBotIdSupported(const ui32 botId) const {
    return (SaveDataForAnyBot && botId == robotstxtcfg::id_anybot) || SupportedBotIds.contains(botId);
}

ui32 TRobotsTxtRulesHandlerBase::GetNotOptimizedBotId(const ui32 botId) const {
    return (botId < OptimizedBotIdToStoredBotId.size())
        ? OptimizedBotIdToStoredBotId[botId]
        : botId;
}

TMaybe<ui32> TRobotsTxtRulesHandlerBase::GetMappedBotId(ui32 botId, bool useAny) const {
    botId = GetNotOptimizedBotId(botId);
    CheckBotIdValidity(botId);
    if (IsBotIdLoaded(botId))
        return botId;
    if (useAny)
        return robotstxtcfg::id_anybot;
    return {};
}
