#pragma once

#include "constants.h"
#include "robots_txt_parser.h"
#include "prefix_tree.h"
#include "robotstxtcfg.h"

#include <util/generic/noncopyable.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>

#include <array>
#include <utility>


enum EDirectiveType {
    USER_AGENT = 1,
    DISALLOW = 2,
    ALLOW = 3,
    HOST = 4,
    SITEMAP = 5,
    CRAWL_DELAY = 6,
    CLEAN_PARAM = 7,
    UNKNOWN = 9,
};

enum EFormatErrorType {
    ERROR_RULE_NOT_SLASH = 1,
    ERROR_ASTERISK_MULTI = 2,
    ERROR_HOST_MULTI = 3,
    ERROR_ROBOTS_HUGE = 4,
    ERROR_RULE_BEFORE_USER_AGENT = 5,
    ERROR_RULE_HUGE = 6,
    ERROR_HOST_FORMAT = 7,
    ERROR_TRASH = 8,
    ERROR_SITEMAP_FORMAT = 9,
    ERROR_CRAWL_DELAY_FORMAT = 10,
    ERROR_CRAWL_DELAY_MULTI = 11,
    ERROR_CLEAN_PARAM_FORMAT = 12,

    WARNING_EMPTY_RULE = 30,
    WARNING_SUSPECT_SYMBOL = 31,
    WARNING_UNKNOWN_FIELD = 33,
    WARNING_UPPER_REGISTER = 34,
    WARNING_SITEMAP = 35,
};

class TRobotsTxtRulesIterator {
private:
    const char* Begin = nullptr;
    const char* End = nullptr;

public:
    TRobotsTxtRulesIterator() = default;
    TRobotsTxtRulesIterator(const char* begin, const char* end);
    void Next();
    bool HasRule() const;
    const char* GetRule() const;
    TString GetInitialRule() const; // unlike GetRule(), it neither omits trailing '$' nor adds redundant '*'
    EDirectiveType GetRuleType() const;

    static EDirectiveType CharToDirType(char ch);
};

class TRobotsTxtRulesHandlerBase {
public:
    typedef TVector<std::pair<EFormatErrorType, int>> TErrorVector;

    TRobotsTxtRulesHandlerBase(
        TBotIdSet supportedBotIds,
        int robotsMaxSize,
        int maxRulesNumber,
        bool saveDataForAnyBot);

    TRobotsTxtRulesHandlerBase(
        const TSet<ui32>& supportedBotIds,
        int robotsMaxSize,
        int maxRulesNumber,
        bool saveDataForAnyBot);

    virtual ~TRobotsTxtRulesHandlerBase();

    int GetCrawlDelay(ui32 botId, bool* realInfo = nullptr) const;
    int GetMinCrawlDelay(int defaultCrawlDelay = -1) const;
    bool IsHandlingErrors() const;
    const TString& GetHostDirective() const;
    const TVector<TString> GetSiteMaps() const;
    const TVector<TString> GetCleanParams() const;
    const TErrorVector& GetErrors() const;
    TVector<int> GetAcceptedLines(ui32 botId = robotstxtcfg::id_yandexbot) const;

    template <class THostHandler>
    static int ParseRules(TRobotsTxtParser& parser, TRobotsTxtRulesHandlerBase* rulesHandler, THostHandler* hostHandler, const char* host = nullptr);
    static inline void ClearAllExceptCrossSection(TRobotsTxtParser& parser, TRobotsTxtRulesHandlerBase* rulesHandler, ui32 botId);
    static int CheckHost(const char* host);
    static int CheckSitemapUrl(const char* url, const char* host, TString& modifiedUrl);
    static int CheckRule(const char* value, int line, TRobotsTxtRulesHandlerBase* rulesHandler);
    static int CheckAndNormCleanParam(TString& s);
    static int ParseCrawlDelay(const char* value, int& crawlDelay);
    static EDirectiveType NameToDirType(const char* d);
    static const char* DirTypeToName(EDirectiveType t);

    void SetErrorsHandling(bool handleErrors);
    void SetHostDirective(const char* hostDirective);
    void SetCrawlDelay(ui32 botId, int crawlDelay);
    void AddAcceptedLine(ui32 line, const TBotIdSet& botIds, bool isCrossSection);
    void AddSiteMap(const char* sitemap);
    void AddCleanParam(const char* cleanParam);
    bool AddRuleWithErrorCheck(ui32 botId, TStringBuf rule, char type, TRobotsTxtParser& parser);
    int OnHost(ui32 botId, TRobotsTxtParser& parser, const char* value, TRobotsTxtRulesHandlerBase*& rulesHandler);

    virtual void Clear();
    virtual bool IsAllowAll(ui32 botId) const = 0;
    virtual bool IsAllowAll() const = 0;
    virtual bool IsDisallowAll(ui32 botId, bool useAny = true) const = 0;
    virtual bool IsDisallowAll() const = 0;
    virtual const char* IsDisallow(ui32 botId, const char* s, bool useAny = true) const = 0;
    virtual const char* IsAllow(ui32 botId, const char* s) const = 0;
    virtual TRobotsTxtRulesIterator GetRulesIterator(ui32 botId) const = 0;
    virtual void Dump(ui32 botId, FILE* logFile) = 0;
    virtual void Dump(ui32 botId, IOutputStream& out) = 0;
    virtual bool Empty(ui32 botId) const = 0;
    virtual void LoadPacked(const char* botsData, const char* botsDataEnd = nullptr) = 0;
    virtual size_t GetPacked(const char*& data) const = 0;
    virtual void AfterParse(ui32 botId) = 0;
    virtual void DoAllowAll() = 0;
    virtual void DoDisallowAll() = 0;
    bool IsBotIdLoaded(ui32 botId) const;
    bool IsBotIdSupported(ui32 botId) const;
    ui32 GetNotOptimizedBotId(ui32 botId) const;
    TMaybe<ui32> GetMappedBotId(ui32 botId, bool useAny = true) const;

protected:
    void CheckBotIdValidity(ui32 botId) const;
    virtual bool OptimizeSize() = 0;

private:
    bool HandleErrors;

protected:
    struct TBotInfo {
        int CrawlDelay;

        TBotInfo()
            : CrawlDelay(-1)
        {
        }
    };

    TBotIdSet LoadedBotIds;
    TSet<TString> SiteMaps;
    TSet<TString> CleanParams;
    TString HostDirective;
    TErrorVector Errors;
    typedef std::pair<ui32, ui32> TBotIdAcceptedLine;
    TVector<TBotIdAcceptedLine> AcceptedLines;
    TVector<ui32> CrossSectionAcceptedLines;

    TVector<TBotInfo> BotIdToInfo;
    int CrawlDelay;
    size_t RobotsMaxSize;
    size_t MaxRulesNumber;
    bool SaveDataForAnyBot;

    TBotIdSet SupportedBotIds;
    std::array<ui8, robotstxtcfg::max_botid> OptimizedBotIdToStoredBotId;

    virtual bool IsFull(ui32 botId, size_t length) const = 0;
    virtual bool IsFullTotal() const = 0;
    virtual bool AddRule(ui32 botId, TStringBuf rule, char type) = 0;
    //parts of ParseRules
    inline static void CheckRobotsLines(TRobotsTxtRulesHandlerBase* rulesHandler, TVector<int>& nonRobotsLines);
    inline static void CheckAsterisk(TRobotsTxtRulesHandlerBase* rulesHandler, const char* value, ui32 lineNumber, bool& wasAsterisk);
    inline static bool CheckWasUserAgent(TRobotsTxtRulesHandlerBase* rulesHandler, bool wasUserAgent, bool& ruleBeforeUserAgent, bool& wasRule, ui32 lineNumber);
    inline static bool CheckRuleNotSlash(TRobotsTxtRulesHandlerBase* rulesHandler, const char* value, ui32 lineNumber);
    inline static bool CheckSupportedBots(const TBotIdSet& currentBotIds, TBotIdSet& wasRuleForBot, const TBotIdSet& isSupportedBot);
    inline static bool CheckEmptyRule(TRobotsTxtRulesHandlerBase* rulesHandler, const char* value, EDirectiveType& type, ui32 lineNumber);
    inline static bool ProcessSitemap(TRobotsTxtRulesHandlerBase* rulesHandler, TRobotsTxtParser& parser, const char* value, const char* host);
    inline static bool ProcessCleanParam(TRobotsTxtRulesHandlerBase* rulesHandler, TRobotsTxtParser& parser, TString& value);
    inline static bool AddRules(
        TRobotsTxtRulesHandlerBase* rulesHandler,
        TRobotsTxtParser& parser,
        const char* value,
        char type,
        const TBotIdSet& currentBotIds,
        const TBotIdSet& isSupportedBot);

    inline static bool ProcessCrawlDelay(
        TRobotsTxtRulesHandlerBase* rulesHandler,
        TRobotsTxtParser& parser,
        const TBotIdSet& currentBotIds,
        const TBotIdSet& isSupportedBot,
        const char* value);

    inline static void ProcessUserAgent(
        TRobotsTxtRulesHandlerBase* rulesHandler,
        TRobotsTxtParser& parser,
        const TBotIdSet& currentBotIds,
        TBotIdSet& wasRuleForBot,
        TBotIdSet& isSupportedBot,
        TVector<ui32>& botIdToMaxAppropriateUserAgentNameLength,
        const char* value);

    bool CheckRobot(
        const char* userAgent,
        TBotIdSet& botIds,
        const TVector<ui32>* botIdToMaxAppropriateUserAgentNameLength = nullptr) const;

    virtual void ClearInternal(ui32 botId);

    void AddError(EFormatErrorType type, int line);

    void ResetOptimized() noexcept;
};

class TPrefixTreeRobotsTxtRulesHandler: public TRobotsTxtRulesHandlerBase, TNonCopyable {
private:
    static const int INIT_BUFFER_SIZE = 1 << 6;

    struct TRuleInfo {
        size_t Len;
        bool Allow;
    };

    bool IsFull(ui32 botId, size_t length) const override;
    bool IsFullTotal() const override;
    bool AddRule(ui32 botId, TStringBuf rule, char type) override;
    const char* GetRule(ui32 botId, const char* s, char type) const;
    void ResizeBuffer(ui32 botId, int newSize);
    void SaveRulesFromBuffer(ui32 botId);
    int TraceBuffer(ui32 botId, int countRules, const TArrayHolder<TRuleInfo>* ruleInfos);
    bool CheckAllowDisallowAll(ui32 botId, bool checkDisallow);
    void SaveRulesToBuffer();
    int StrLenWithoutStars(const char* s);

protected:
    class TRulesSortFunc {
    private:
        const TArrayHolder<TRuleInfo>* RuleInfos;

    public:
        TRulesSortFunc(const TArrayHolder<TRuleInfo>* ruleInfos)
            : RuleInfos(ruleInfos)
        {
        }
        bool operator()(const size_t& lhs, const size_t& rhs) {
            const TRuleInfo& left = (*RuleInfos).Get()[lhs];
            const TRuleInfo& right = (*RuleInfos).Get()[rhs];
            return (left.Len == right.Len) ? left.Allow && !right.Allow : left.Len > right.Len;
        }
    };

    struct TPrefixTreeBotInfo {
        bool DisallowAll = false;
        bool AllowAll = false;
        bool HasDisallow = false;
        bool HasAllow = false;

        TArrayHolder<char> Buffer{new char[INIT_BUFFER_SIZE]};
        ui32 BufferPosition = sizeof(BufferPosition);
        int BufferSize = INIT_BUFFER_SIZE;

        TArrayHolder<char*> Rules = nullptr;
        int RulesPosition = 0;
        int RulesSize = 0;

        TArrayHolder<char**> ComplexRules = nullptr;
        int ComplexRulesPosition = 0;
        int ComplexRulesSize = 0;

        TPrefixTree PrefixRules {0};
    };

    std::array<THolder<TPrefixTreeBotInfo>, robotstxtcfg::max_botid> BotIdToPrefixTreeBotInfo;

    TPrefixTreeBotInfo& GetInfo(ui32 botId);
    static bool CheckRule(const char* s, const char* rule);
    void ClearInternal(ui32 botId) override;
    bool OptimizeSize() override;

private:
    void SortRules(TPrefixTreeBotInfo& prefixBotInfo, size_t count, const TArrayHolder<TRuleInfo>* ruleInfos);
    bool HasDisallowRulePrevAllowAll(const TPrefixTreeBotInfo& prefixBotInfo, int ruleAllAllow);
    int FindRuleAll(const TPrefixTreeBotInfo& prefixBotInfo, char neededType);

public:
    TPrefixTreeRobotsTxtRulesHandler(
        TBotIdSet supportedBotIds = robotstxtcfg::defaultSupportedBotIds,
        int robotsMaxSize = robots_max,
        int maxRulesCount = -1,
        bool saveDataForAnyBot = true);

    TPrefixTreeRobotsTxtRulesHandler(
        std::initializer_list<ui32> supportedBotIds,
        int robotsMaxSize = robots_max,
        int maxRulesCount = -1,
        bool saveDataForAnyBot = true);

    TPrefixTreeRobotsTxtRulesHandler(
        const TSet<ui32>& supportedBotIds,
        int robotsMaxSize = robots_max,
        int maxRulesCount = -1,
        bool saveDataForAnyBot = true);

    void Clear() override;
    void AfterParse(ui32 botId) override;
    bool IsAllowAll(ui32 botId) const override;
    bool IsAllowAll() const override;
    bool IsDisallowAll(ui32 botId, bool useAny = true) const override;
    bool IsDisallowAll() const override;
    const char* IsDisallow(ui32 botId, const char* s, bool useAny = true) const override;
    const char* IsAllow(ui32 botId, const char* s) const override;
    TRobotsTxtRulesIterator GetRulesIterator(ui32 botId) const override;
    void DoAllowAll() override;
    void DoDisallowAll() override;
    bool Empty(ui32 botId) const override;

    void LoadPacked(const char* botsData, const char* botsDataEnd = nullptr) override;
    size_t GetPacked(const char*& data) const override;
    void Dump(ui32 botId, FILE* logFile) override;
    void Dump(ui32 botId, IOutputStream& out) override;
    size_t GetMemorySize();
};

using TRobotsTxt = TPrefixTreeRobotsTxtRulesHandler;

void TRobotsTxtRulesHandlerBase::ClearAllExceptCrossSection(TRobotsTxtParser& parser, TRobotsTxtRulesHandlerBase* rulesHandler, ui32 botId) {
    rulesHandler->ClearInternal(botId);
    if (botId == robotstxtcfg::id_anybot) {
        // as sitemaps, clean-params and HostDirective from prefix tree was deleted
        for (const auto& sitemap : rulesHandler->SiteMaps) {
            rulesHandler->AddRuleWithErrorCheck(robotstxtcfg::id_anybot, sitemap, 'S', parser);
        }
        for (const auto& param : rulesHandler->CleanParams) {
            rulesHandler->AddRuleWithErrorCheck(robotstxtcfg::id_anybot, param, 'P', parser);
        }
        if (!rulesHandler->HostDirective.empty()) {
            rulesHandler->AddRuleWithErrorCheck(robotstxtcfg::id_anybot, rulesHandler->HostDirective, 'H', parser);
        }
    }
}

void TRobotsTxtRulesHandlerBase::CheckRobotsLines(TRobotsTxtRulesHandlerBase* rulesHandler, TVector<int>& nonRobotsLines) {
    if (rulesHandler->IsHandlingErrors()) {
        for (size_t i = 0; i < nonRobotsLines.size(); ++i)
            rulesHandler->AddError(ERROR_TRASH, nonRobotsLines[i]);
        nonRobotsLines.clear();
    }
}

void TRobotsTxtRulesHandlerBase::CheckAsterisk(TRobotsTxtRulesHandlerBase* rulesHandler, const char* value, ui32 lineNumber, bool& wasAsterisk) {
    if (strcmp(value, "*") == 0) {
        if (wasAsterisk)
            rulesHandler->AddError(ERROR_ASTERISK_MULTI, lineNumber);
        wasAsterisk = true;
    }
}

bool TRobotsTxtRulesHandlerBase::CheckWasUserAgent(TRobotsTxtRulesHandlerBase* rulesHandler, bool wasUserAgent, bool& ruleBeforeUserAgent, bool& wasRule, ui32 lineNumber) {
    if (wasUserAgent) {
        wasRule = true;
        return false;
    }
    if (!ruleBeforeUserAgent) {
        ruleBeforeUserAgent = true;
        rulesHandler->AddError(ERROR_RULE_BEFORE_USER_AGENT, lineNumber);
    }
    return true;
}

bool TRobotsTxtRulesHandlerBase::CheckRuleNotSlash(TRobotsTxtRulesHandlerBase* rulesHandler, const char* value, ui32 lineNumber) {
    if (*value && *value != '/' && *value != '*') {
        rulesHandler->AddError(ERROR_RULE_NOT_SLASH, lineNumber);
        return true;
    }
    return false;
}

bool TRobotsTxtRulesHandlerBase::CheckSupportedBots(
    const TBotIdSet& currentBotIds,
    TBotIdSet& wasRuleForBot,
    const TBotIdSet& isSupportedBot)
{
    bool hasAtLeastOneSupportedBot = false;
    for (ui32 currentBotId : currentBotIds) {
        wasRuleForBot.insert(currentBotId);
        hasAtLeastOneSupportedBot = hasAtLeastOneSupportedBot || isSupportedBot.contains(currentBotId);
    }
    return hasAtLeastOneSupportedBot;
}

bool TRobotsTxtRulesHandlerBase::CheckEmptyRule(TRobotsTxtRulesHandlerBase* rulesHandler, const char* value, EDirectiveType& type, ui32 lineNumber) {
    if (value && strlen(value) == 0) {
        rulesHandler->AddError(WARNING_EMPTY_RULE, lineNumber);
        type = type == ALLOW ? DISALLOW : ALLOW;
        return true;
    }
    return false;
}

bool TRobotsTxtRulesHandlerBase::AddRules(
    TRobotsTxtRulesHandlerBase* rulesHandler,
    TRobotsTxtParser& parser,
    const char* value,
    char type,
    const TBotIdSet& currentBotIds,
    const TBotIdSet& isSupportedBot)
{
    for (ui32 currentBotId : currentBotIds) {
        if (!isSupportedBot.contains(currentBotId))
            continue;
        if (!rulesHandler->AddRuleWithErrorCheck(currentBotId, value, type, parser))
            return true;
    }
    return false;
}

bool TRobotsTxtRulesHandlerBase::ProcessSitemap(TRobotsTxtRulesHandlerBase* rulesHandler, TRobotsTxtParser& parser, const char* value, const char* host) {
    TString modifiedUrl;
    if (!CheckSitemapUrl(value, host, modifiedUrl))
        rulesHandler->AddError(ERROR_SITEMAP_FORMAT, parser.GetLineNumber());
    else {
        rulesHandler->AddSiteMap(modifiedUrl.data());
        if (!rulesHandler->AddRuleWithErrorCheck(robotstxtcfg::id_anybot, modifiedUrl.data(), 'S', parser))
            return true;
    }
    return false;
}

bool TRobotsTxtRulesHandlerBase::ProcessCleanParam(TRobotsTxtRulesHandlerBase* rulesHandler, TRobotsTxtParser& parser, TString& value) {
    if (!CheckAndNormCleanParam(value))
        rulesHandler->AddError(ERROR_CLEAN_PARAM_FORMAT, parser.GetLineNumber());
    else {
        rulesHandler->AddCleanParam(value.data());
        if (!rulesHandler->AddRuleWithErrorCheck(robotstxtcfg::id_anybot, value.data(), 'P', parser))
            return true;
    }
    return false;
}

bool TRobotsTxtRulesHandlerBase::ProcessCrawlDelay(
    TRobotsTxtRulesHandlerBase* rulesHandler,
    TRobotsTxtParser& parser,
    const TBotIdSet& currentBotIds,
    const TBotIdSet& isSupportedBot,
    const char* value) {
    for (ui32 currentBotId : currentBotIds) {
        if (!isSupportedBot.contains(currentBotId))
            continue;
        if (rulesHandler->BotIdToInfo[currentBotId].CrawlDelay >= 0) {
            rulesHandler->AddError(ERROR_CRAWL_DELAY_MULTI, parser.GetLineNumber());
            break;
        }
        int crawlDelay = -1;
        if (!ParseCrawlDelay(value, crawlDelay))
            rulesHandler->AddError(ERROR_CRAWL_DELAY_FORMAT, parser.GetLineNumber());
        else {
            rulesHandler->SetCrawlDelay(currentBotId, crawlDelay);
            if (!rulesHandler->AddRuleWithErrorCheck(currentBotId, value, 'C', parser))
                return true;
        }
    }
    return false;
}

void TRobotsTxtRulesHandlerBase::ProcessUserAgent(
    TRobotsTxtRulesHandlerBase* rulesHandler,
    TRobotsTxtParser& parser,
    const TBotIdSet& currentBotIds,
    TBotIdSet& wasSupportedBot,
    TBotIdSet& isSupportedBot,
    TVector<ui32>& botIdToMaxAppropriateUserAgentNameLength,
    const char* value)
{
    ui32 userAgentNameLength = (ui32)strlen(value);

    for (ui32 currentBotId : currentBotIds) {
        bool userAgentNameLonger = userAgentNameLength > botIdToMaxAppropriateUserAgentNameLength[currentBotId];
        bool userAgentNameSame = userAgentNameLength == botIdToMaxAppropriateUserAgentNameLength[currentBotId];

        if (!wasSupportedBot.contains(currentBotId) || userAgentNameLonger)
            ClearAllExceptCrossSection(parser, rulesHandler, currentBotId);

        wasSupportedBot.insert(currentBotId);
        if (userAgentNameLonger || userAgentNameSame) {
            isSupportedBot.insert(currentBotId); // Allow multiple blocks for the same user agent
        }
        botIdToMaxAppropriateUserAgentNameLength[currentBotId] = Max(userAgentNameLength, botIdToMaxAppropriateUserAgentNameLength[currentBotId]);
    }
}

template <class THostHandler>
int TRobotsTxtRulesHandlerBase::ParseRules(TRobotsTxtParser& parser, TRobotsTxtRulesHandlerBase* rulesHandler, THostHandler* hostHandler, const char* host) {
    rulesHandler->Clear();

    TBotIdSet wasSupportedBot;
    TBotIdSet wasRuleForBot;
    bool wasAsterisk = false;
    TVector<int> nonRobotsLines;
    TVector<ui32> botIdToMaxAppropriateUserAgentNameLength(robotstxtcfg::max_botid, 0);
    static char all[] = "/";
    EDirectiveType prevType = USER_AGENT;
    while (parser.HasRecord()) {
        TRobotsTxtRulesRecord record = parser.NextRecord();
        bool wasUserAgent = false;
        bool isRobotsRecordUseful = false;
        TBotIdSet isSupportedBot;
        TBotIdSet currentBotIds;
        TString field;
        TString value;
        bool ruleBeforeUserAgent = false;
        int ret = 0;
        bool wasRule = false;
        bool wasBlank = false;
        while (record.NextPair(field, value, isRobotsRecordUseful && rulesHandler->IsHandlingErrors(), nonRobotsLines, &wasBlank)) {
            CheckRobotsLines(rulesHandler, nonRobotsLines);
            EDirectiveType type = NameToDirType(field.data());
            EDirectiveType typeBeforeChange = type;

            if ((prevType != type || wasBlank) && type == USER_AGENT) {
                currentBotIds.clear();
            }
            prevType = type;

            switch (type) {
                case USER_AGENT:
                    if (wasUserAgent && wasRule) {
                        wasRule = false;
                        currentBotIds.clear();
                        isSupportedBot.clear();
                    }
                    wasUserAgent = true;
                    value.to_lower();
                    CheckAsterisk(rulesHandler, value.data(), parser.GetLineNumber(), wasAsterisk);
                    isRobotsRecordUseful = rulesHandler->CheckRobot(value.data(), currentBotIds, &botIdToMaxAppropriateUserAgentNameLength);
                    if (isRobotsRecordUseful)
                        ProcessUserAgent(rulesHandler, parser, currentBotIds, wasSupportedBot, isSupportedBot, botIdToMaxAppropriateUserAgentNameLength, value.data());
                    break;

                case DISALLOW:
                case ALLOW:
                    if (CheckWasUserAgent(rulesHandler, wasUserAgent, ruleBeforeUserAgent, wasRule, parser.GetLineNumber()))
                        break;
                    if (CheckRuleNotSlash(rulesHandler, value.data(), parser.GetLineNumber()))
                        break;
                    CheckRule(value.data(), parser.GetLineNumber(), rulesHandler);
                    if (!CheckSupportedBots(currentBotIds, wasRuleForBot, isSupportedBot)) {
                        break;
                    }
                    if (CheckEmptyRule(rulesHandler, value.data(), type, parser.GetLineNumber())) {
                        value = all;
                        if (typeBeforeChange == ALLOW)
                            continue;
                    }

                    if (AddRules(rulesHandler, parser, value.data(), type == ALLOW ? 'A' : 'D', currentBotIds, isSupportedBot))
                        return 2;
                    break;

                case HOST:
                    value.to_lower();
                    ret = hostHandler->OnHost(robotstxtcfg::id_anybot, parser, value.data(), rulesHandler);
                    if (ret)
                        return ret;
                    break;

                case SITEMAP:
                    if (ProcessSitemap(rulesHandler, parser, value.data(), host))
                        return 2;
                    break;

                case CLEAN_PARAM:
                    if (ProcessCleanParam(rulesHandler, parser, value))
                        return 2;
                    break;

                case CRAWL_DELAY:
                    if (ProcessCrawlDelay(rulesHandler, parser, currentBotIds, isSupportedBot, value.data()))
                        return 2;
                    break;

                default:
                    rulesHandler->AddError(WARNING_UNKNOWN_FIELD, parser.GetLineNumber());
                    break;
            }
            bool isCrossSection = type == SITEMAP || type == HOST || type == CLEAN_PARAM;
            if (rulesHandler->IsHandlingErrors() && (isRobotsRecordUseful || isCrossSection))
                rulesHandler->AddAcceptedLine(parser.GetLineNumber(), currentBotIds, isCrossSection);
        }
    }

    for (auto botId : wasSupportedBot) {
        rulesHandler->LoadedBotIds.insert(botId);
        if (rulesHandler->IsBotIdSupported(botId))
            rulesHandler->AfterParse(botId);
    }

    if (!rulesHandler->OptimizeSize()) {
        return 2;
    }

    return 1;
}
