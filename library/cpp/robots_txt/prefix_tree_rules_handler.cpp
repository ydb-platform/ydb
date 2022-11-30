#include "robots_txt.h"

#include <util/digest/fnv.h>
#include <util/system/tls.h>
#include <util/generic/buffer.h>
#include <util/generic/yexception.h>

namespace {

TString NormalizeRule(TStringBuf rule) {
    TString result;
    result.reserve(rule.size() + 1);

    // remove consecutive '*'
    for (auto c : rule) {
        if (c != '*' || !result.EndsWith('*')) {
            result.append(c);
        }
    }

    if (rule == "*") {
        result = "/*";
        return result;
    }

    // unify suffix
    if (result.EndsWith('$')) {
        result.pop_back();
    } else if (!result.EndsWith('*')) {
        result.append('*');
    }

    return result;
}

// Prefix rules
bool IsPrefixRule(TStringBuf rule) {
    return rule.EndsWith('*') && !TStringBuf(rule.begin(), rule.end() - 1).Contains('*');
}

// Converts rule to  internal representation, i.e.
// For prefix rules: "/foo", 'D' -> 'D', "/foo"
// For generic rules: "/*foo", 'D' -> ("/*/*foo*", 'd') or ("/*foo$", 'A') -> ("/*foo", 'a')
// The distinction is in uppercase/lowercase rule type
std::pair<TString, char> ConvertRule(TStringBuf rule, char type) {
    switch (type) {
    case 'H':
    case 'S':
    case 'C':
    case 'P':
        return {TString(rule), type};
    case 'A':
    case 'D':
        break;
    default:
        return {{}, type};
    }

    auto result = NormalizeRule(rule);
    if (IsPrefixRule(result)) {
        result.pop_back(); // remove extra '*' from the end
    } else {
        type = tolower(type);
    }

    return {std::move(result), type};
}

} // namespace

TPrefixTreeRobotsTxtRulesHandler::TPrefixTreeRobotsTxtRulesHandler(
    TBotIdSet supportedBotIds,
    int robotsMaxSize,
    int maxRulesNumber,
    bool saveDataForAnyBot)
    : TRobotsTxtRulesHandlerBase(supportedBotIds, robotsMaxSize, maxRulesNumber, saveDataForAnyBot)
{}

TPrefixTreeRobotsTxtRulesHandler::TPrefixTreeRobotsTxtRulesHandler(
    std::initializer_list<ui32> supportedBotIds,
    int robotsMaxSize,
    int maxRulesNumber,
    bool saveDataForAnyBot)
    : TRobotsTxtRulesHandlerBase(TBotIdSet(supportedBotIds), robotsMaxSize, maxRulesNumber, saveDataForAnyBot)
{}

TPrefixTreeRobotsTxtRulesHandler::TPrefixTreeRobotsTxtRulesHandler(
    const TSet<ui32>& supportedBotIds,
    int robotsMaxSize,
    int maxRulesNumber,
    bool saveDataForAnyBot)
    : TRobotsTxtRulesHandlerBase(supportedBotIds, robotsMaxSize, maxRulesNumber, saveDataForAnyBot)
{}

bool TPrefixTreeRobotsTxtRulesHandler::Empty(const ui32 botId) const {
    const auto& botInfo = BotIdToPrefixTreeBotInfo[GetNotOptimizedBotId(botId)];
    return !botInfo || (botInfo->BufferPosition <= sizeof(botInfo->BufferPosition));
}

TRobotsTxtRulesIterator TPrefixTreeRobotsTxtRulesHandler::GetRulesIterator(const ui32 botId) const {
    const auto& botInfo = BotIdToPrefixTreeBotInfo[GetNotOptimizedBotId(botId)];
    if (!botInfo) {
        return {};
    }
    return TRobotsTxtRulesIterator(botInfo->Buffer.Get() + sizeof(botInfo->BufferPosition), botInfo->Buffer.Get() + botInfo->BufferPosition);
}

size_t TPrefixTreeRobotsTxtRulesHandler::GetMemorySize() {
    size_t allBotsSize = 0;
    for (const auto& botInfo : BotIdToPrefixTreeBotInfo) {
        if (!botInfo) {
            continue;
        }

        allBotsSize += botInfo->PrefixRules.GetMemorySize()
            + botInfo->BufferSize * sizeof(char)
            + botInfo->ComplexRulesSize * sizeof(char**)
            + botInfo->RulesSize * sizeof(char*) + (1 << 8);
    }
    return allBotsSize;
}

void TPrefixTreeRobotsTxtRulesHandler::ClearInternal(const ui32 botId) {
    if (botId >= BotIdToPrefixTreeBotInfo.size()) {
        return;
    }
    BotIdToPrefixTreeBotInfo[botId].Reset();
    TRobotsTxtRulesHandlerBase::ClearInternal(botId);
}

bool TPrefixTreeRobotsTxtRulesHandler::OptimizeSize() {
    ResetOptimized();

    TMap<ui64, ui32> hashToBotId;
    for (auto botId : LoadedBotIds) {
        auto& botInfo = BotIdToPrefixTreeBotInfo[botId];
        if (botInfo->BufferPosition <= sizeof(ui32)) {
            botInfo.Reset();
            LoadedBotIds.remove(botId);
            continue;
        }

        ui64 hash = FnvHash<ui64>(botInfo->Buffer.Get(), botInfo->BufferPosition);
        if (auto p = hashToBotId.FindPtr(hash)) {
            OptimizedBotIdToStoredBotId[botId] = *p;
            ClearInternal(botId);
            botInfo.Reset();
        } else {
            hashToBotId[hash] = botId;
        }
    }

    if (IsFullTotal()) {
        DoAllowAll();
        return false;
    }

    return true;
}

void TPrefixTreeRobotsTxtRulesHandler::Clear() {
    for (size_t botId = 0; botId < robotstxtcfg::max_botid; ++botId)
        if (IsBotIdSupported(botId))
            ClearInternal(botId);
    TRobotsTxtRulesHandlerBase::Clear();
}

void TPrefixTreeRobotsTxtRulesHandler::ResizeBuffer(const ui32 botId, int newSize) {
    auto& botInfo = GetInfo(botId);
    TArrayHolder<char> newBuffer(new char[newSize]);
    memcpy(newBuffer.Get(), botInfo.Buffer.Get(), std::min(botInfo.BufferSize, newSize));
    botInfo.Buffer.Swap(newBuffer);
    botInfo.BufferSize = newSize;
}

bool TPrefixTreeRobotsTxtRulesHandler::AddRule(const ui32 botId, TStringBuf rule, char type) {
    if (rule.empty() || rule.Contains('\0')) {
        return true;
    }

    auto& botInfo = GetInfo(botId);

    if (IsFull(botId, rule.size())) {
        DoAllowAll();
        return false;
    }

    auto [convertedRule, convertedType] = ConvertRule(rule, type);
    const auto len = convertedRule.size() + 2; // 1 byte for convertedType and another for '\0'

    if (auto newPos = botInfo.BufferPosition + len; newPos >= size_t(botInfo.BufferSize)) {
        size_t newSize = botInfo.BufferSize;
        while (newPos >= newSize)
            newSize *= 2;
        ResizeBuffer(botId, newSize);
    }

    auto out = botInfo.Buffer.Get() + botInfo.BufferPosition;
    *out++ = convertedType;
    strcpy(out, convertedRule.data());
    botInfo.BufferPosition += len;

    if (type == 'A' || type == 'D') {
        botInfo.RulesPosition++;
    }

    return true;
}

const char* TPrefixTreeRobotsTxtRulesHandler::GetRule(const ui32 botId, const char* s, char type) const {
    const auto& botInfo = BotIdToPrefixTreeBotInfo[GetNotOptimizedBotId(botId)];
    if (!botInfo) {
        return nullptr;
    }

    int m = botInfo->RulesPosition + 1;
    int k = botInfo->PrefixRules.MinPrefixIndex(s);
    if (k >= 0)
        m = k;
    char* rule;
    int j;
    for (int i = 0; i < botInfo->ComplexRulesPosition; ++i) {
        rule = *botInfo->ComplexRules.Get()[i];
        j = botInfo->ComplexRules.Get()[i] - botInfo->Rules.Get();
        if (j >= m)
            break;
        if (CheckRule(s, rule)) {
            m = j;
            break;
        }
    }
    if (m >= botInfo->RulesPosition)
        return nullptr;
    return toupper(*(botInfo->Rules.Get()[m] - 1)) == type ? botInfo->Rules.Get()[m] : nullptr;
}

inline bool TPrefixTreeRobotsTxtRulesHandler::IsAllowAll(const ui32 botId) const {
    const auto id = GetMappedBotId(botId, false);
    auto& botInfo = BotIdToPrefixTreeBotInfo[id ? *id : robotstxtcfg::id_anybot];
    return botInfo && botInfo->AllowAll;
}

inline bool TPrefixTreeRobotsTxtRulesHandler::IsAllowAll() const {
    for (ui32 botId = 0; botId < robotstxtcfg::max_botid; ++botId)
        if (robotstxtcfg::IsYandexBotId(botId) && IsBotIdSupported(botId) && !IsAllowAll(botId)) {
            return false;
        }

    return true;
}

inline bool TPrefixTreeRobotsTxtRulesHandler::IsDisallowAll(const ui32 botId, bool useAny) const {
    const auto id = GetMappedBotId(botId, false);
    if (id) {
        const auto& botInfo = BotIdToPrefixTreeBotInfo[*id];
        return botInfo && botInfo->DisallowAll;
    }

    auto& botInfo = BotIdToPrefixTreeBotInfo[robotstxtcfg::id_anybot];
    return useAny && botInfo && botInfo->DisallowAll;
}

inline bool TPrefixTreeRobotsTxtRulesHandler::IsDisallowAll() const {
    for (ui32 botId = 0; botId < robotstxtcfg::max_botid; ++botId)
        if (robotstxtcfg::IsYandexBotId(botId) && IsBotIdSupported(botId) && !IsDisallowAll(botId))
            return false;

    return true;
}

void TPrefixTreeRobotsTxtRulesHandler::DoAllowAll() {
    using robotstxtcfg::id_anybot;

    // Drop all bots to default
    SupportedBotIds.insert(id_anybot);
    for (ui32 botId = 0; botId < robotstxtcfg::max_botid; ++botId) {
        if (IsBotIdSupported(botId)) {
            ClearInternal(botId);
            OptimizedBotIdToStoredBotId[botId] = id_anybot;
            LoadedBotIds.insert(botId);
        }
    }

    // Initialize anybot with "allow all" rule
    AddRule(id_anybot, "/", 'A');
    GetInfo(id_anybot).AllowAll = true;
    SaveRulesToBuffer();
}

void TPrefixTreeRobotsTxtRulesHandler::DoDisallowAll() {
    for (ui32 botId = 0; botId < robotstxtcfg::max_botid; ++botId) {
        if (!IsBotIdSupported(botId))
            continue;
        ClearInternal(botId);
        if (botId == robotstxtcfg::id_anybot) {
            auto& botInfo = GetInfo(botId);
            AddRule(botId, "/", 'D');
            botInfo.DisallowAll = true;
            SaveRulesToBuffer();
        } else {
            OptimizedBotIdToStoredBotId[botId] = robotstxtcfg::id_anybot;
        }
        LoadedBotIds.insert(botId);
    }
}

const char* TPrefixTreeRobotsTxtRulesHandler::IsDisallow(const ui32 botId, const char* s, bool useAny) const {
    const auto id = GetMappedBotId(botId, useAny);
    if (!id)
        return nullptr;

    const auto& botInfo = BotIdToPrefixTreeBotInfo[*id];
    if (botInfo && IsDisallowAll(*id, useAny)) {
        int index = (const_cast<TPrefixTreeRobotsTxtRulesHandler*>(this))->FindRuleAll(*botInfo, 'D');
        if (index < 0) { //o_O
            return botInfo->Rules.Get()[0];
        } else {
            return botInfo->Rules.Get()[index];
        }
    }

    return GetRule(*id, s, 'D');
}

const char* TPrefixTreeRobotsTxtRulesHandler::IsAllow(const ui32 botId, const char* s) const {
    const auto id = GetMappedBotId(botId, true);
    if (auto p = GetRule(*id, s, 'A'))
        return p;
    return GetRule(*id, s, 'D') ? nullptr : "/";
}

int TPrefixTreeRobotsTxtRulesHandler::StrLenWithoutStars(const char* s) {
    int len = 0;

    for (size_t index = 0; s[index]; ++index) {
        if (s[index] != '*') {
            ++len;
        }
    }

    return len;
}

int TPrefixTreeRobotsTxtRulesHandler::TraceBuffer(const ui32 botId, int countRules, const TArrayHolder<TRuleInfo>* ruleInfos) {
    CheckBotIdValidity(botId);
    auto& prefixBotInfo = GetInfo(botId);
    TBotInfo& botInfo = BotIdToInfo[botId];

    bool store = countRules >= 0;
    if (store) {
        prefixBotInfo.Rules.Reset(new char*[prefixBotInfo.RulesSize = countRules]);
    }

    int beg = -1, n = 0;
    *((int*)prefixBotInfo.Buffer.Get()) = prefixBotInfo.BufferSize;
    for (size_t i = sizeof(prefixBotInfo.BufferPosition); i < prefixBotInfo.BufferPosition; ++i)
        if (prefixBotInfo.Buffer.Get()[i] == '\n' || prefixBotInfo.Buffer.Get()[i] == 0) {
            if (beg < 0 || beg + 1 == (int)i)
                continue;

            char* s = prefixBotInfo.Buffer.Get() + beg;
            if (store) {
                switch (*s) {
                    case 'H':
                        HostDirective = s + 1;
                        break;
                    case 'S':
                        SiteMaps.insert(s + 1);
                        break;
                    case 'C':
                        ParseCrawlDelay(s + 1, botInfo.CrawlDelay);
                        break;
                    case 'P':
                        CleanParams.insert(s + 1);
                        break;
                    default:
                        prefixBotInfo.Rules.Get()[n] = s + 1;
                        (*ruleInfos).Get()[n].Len = StrLenWithoutStars(s + 1);
                        (*ruleInfos).Get()[n].Allow = toupper(*s) == 'A';

                        prefixBotInfo.HasAllow |= toupper(*s) == 'A';
                        prefixBotInfo.HasDisallow |= toupper(*s) == 'D';
                        break;
                }
            }
            n += (*s != 'H' && *s != 'S' && *s != 'C' && *s != 'P');
            beg = -1;
        } else if (beg < 0)
            beg = i;

    return n;
}

int TPrefixTreeRobotsTxtRulesHandler::FindRuleAll(const TPrefixTreeBotInfo& prefixBotInfo, const char neededType) {
    static const char* all[] = {"*", "/", "*/", "/*", "*/*"};
    for (int ruleNumber = prefixBotInfo.RulesSize - 1; ruleNumber >= 0; --ruleNumber) {
        const char* curRule = prefixBotInfo.Rules.Get()[ruleNumber];
        char ruleType = *(curRule - 1);

        if (strlen(curRule) > 3)
            break;
        if (neededType != ruleType)
            continue;

        for (size_t i = 0; i < sizeof(all) / sizeof(char*); ++i)
            if (strcmp(all[i], curRule) == 0)
                return ruleNumber;
    }
    return -1;
}

bool TPrefixTreeRobotsTxtRulesHandler::HasDisallowRulePrevAllowAll(const TPrefixTreeBotInfo& prefixBotInfo, int ruleAllAllow) {
    for (int ruleNumber = ruleAllAllow - 1; ruleNumber >= 0; --ruleNumber) {
        const char* curRule = prefixBotInfo.Rules.Get()[ruleNumber];
        char ruleType = *(curRule - 1);
        if (tolower(ruleType) == 'd')
            return true;
    }
    return false;
}

bool TPrefixTreeRobotsTxtRulesHandler::CheckAllowDisallowAll(const ui32 botId, const bool checkDisallow) {
    CheckBotIdValidity(botId);

    auto& botInfo = GetInfo(botId);

    if (botInfo.RulesSize == 0)
        return !checkDisallow;
    if (botInfo.RulesPosition <= 0)
        return 0;

    if (checkDisallow)
        return !botInfo.HasAllow && FindRuleAll(botInfo, 'D') >= 0;
    int ruleAllAllow = FindRuleAll(botInfo, 'A');
    if (ruleAllAllow == -1)
        return !botInfo.HasDisallow;
    return !HasDisallowRulePrevAllowAll(botInfo, ruleAllAllow);
}

void TPrefixTreeRobotsTxtRulesHandler::SortRules(
    TPrefixTreeBotInfo& prefixBotInfo,
    size_t count,
    const TArrayHolder<TRuleInfo>* ruleInfos) {
    TVector<size_t> indexes(count);
    for (size_t index = 0; index < count; ++index)
        indexes[index] = index;

    TRulesSortFunc sortFunc(ruleInfos);
    std::sort(indexes.begin(), indexes.end(), sortFunc);

    TArrayHolder<char*> workingCopy;
    workingCopy.Reset(new char*[count]);

    for (size_t index = 0; index < count; ++index)
        workingCopy.Get()[index] = prefixBotInfo.Rules.Get()[index];
    for (size_t index = 0; index < count; ++index)
        prefixBotInfo.Rules.Get()[index] = workingCopy.Get()[indexes[index]];
}

void TPrefixTreeRobotsTxtRulesHandler::SaveRulesToBuffer() {
    // as sitemaps, clean-params and HostDirective from prefix tree was deleted
    for (const auto& sitemap:  SiteMaps)
        AddRule(robotstxtcfg::id_anybot, sitemap, 'S');
    for (const auto& param : CleanParams)
        AddRule(robotstxtcfg::id_anybot, param, 'P');
    if (!HostDirective.empty())
        AddRule(robotstxtcfg::id_anybot, HostDirective, 'H');
}

void TPrefixTreeRobotsTxtRulesHandler::SaveRulesFromBuffer(const ui32 botId) {
    CheckBotIdValidity(botId);

    auto& botInfo = GetInfo(botId);

    TArrayHolder<TRuleInfo> ruleInfos;

    int n = TraceBuffer(botId, -1, nullptr), countPrefix = 0;
    ruleInfos.Reset(new TRuleInfo[n]);
    botInfo.RulesPosition = TraceBuffer(botId, n, &ruleInfos);
    assert(botInfo.RulesPosition == n);

    SortRules(botInfo, n, &ruleInfos);

    botInfo.DisallowAll = CheckAllowDisallowAll(botId, true);
    botInfo.AllowAll = CheckAllowDisallowAll(botId, false);

    for (int i = 0; i < n; ++i)
        countPrefix += !!isupper(*(botInfo.Rules.Get()[i] - 1));

    botInfo.PrefixRules.Init(countPrefix);
    botInfo.ComplexRules.Reset(new char**[botInfo.ComplexRulesSize = n - countPrefix]);
    botInfo.ComplexRulesPosition = 0;

    for (int i = 0; i < n; ++i) {
        char* s = botInfo.Rules.Get()[i];
        if (isupper(*(s - 1)))
            botInfo.PrefixRules.Add(s, i);
        else
            botInfo.ComplexRules.Get()[botInfo.ComplexRulesPosition++] = &botInfo.Rules.Get()[i];
    }
    botInfo.PrefixRules.Compress();
}

void TPrefixTreeRobotsTxtRulesHandler::AfterParse(const ui32 botId) {
    CheckBotIdValidity(botId);

    auto& botInfo = GetInfo(botId);

    ResizeBuffer(botId, botInfo.BufferPosition);
    SaveRulesFromBuffer(botId);

    if (botInfo.RulesPosition == 0) {
        AddRule(botId, "/", 'A');
    }
}

TPrefixTreeRobotsTxtRulesHandler::TPrefixTreeBotInfo& TPrefixTreeRobotsTxtRulesHandler::GetInfo(ui32 botId) {
    Y_ENSURE(botId < robotstxtcfg::max_botid);
    auto& res = BotIdToPrefixTreeBotInfo[botId];
    if (!res) {
        res = MakeHolder<TPrefixTreeBotInfo>();
    }
    return *res;
}

bool TPrefixTreeRobotsTxtRulesHandler::CheckRule(const char* s, const char* rule) {
    const char* r = rule;
    const char* s_end = s + strlen(s);
    const char* r_end = r + strlen(r);
    //   assert( r && !strstr(r, "**") );
    for (; *s; ++s) {
        if ((s_end - s + 1) * 2 < (r_end - r))
            return 0;
        while (*r == '*')
            ++r;

        if (*s == *r) {
            ++r;
        } else {
            while (r != rule && *r != '*')
                --r;

            if (*r != '*')
                return 0;
            if (*r == '*')
                ++r;
            if (*r == *s)
                ++r;
        }
    }
    return !*r || (!*(r + 1) && *r == '*');
}

bool TPrefixTreeRobotsTxtRulesHandler::IsFull(ui32 botId, size_t length) const {
    Y_ENSURE(botId < robotstxtcfg::max_botid);
    const auto& botInfo = BotIdToPrefixTreeBotInfo[botId];
    if (!botInfo) {
        return false;
    }

    return (size_t(botInfo->RulesPosition) >= MaxRulesNumber) || (botInfo->BufferPosition + length + 300 > size_t(RobotsMaxSize));
}

bool TPrefixTreeRobotsTxtRulesHandler::IsFullTotal() const {
    size_t allBotsRulesCount = 0;
    size_t allBotsBufferSize = 0;

    for (const auto& botInfo : BotIdToPrefixTreeBotInfo) {
        if (botInfo) {
            allBotsRulesCount += botInfo->RulesPosition;
            allBotsBufferSize += botInfo->BufferPosition;
        }
    }

    return (allBotsRulesCount >= MaxRulesNumber) || (allBotsBufferSize + 300 > size_t(RobotsMaxSize));
}

size_t TPrefixTreeRobotsTxtRulesHandler::GetPacked(const char*& data) const {
    Y_STATIC_THREAD(TBuffer)
    packedRepresentation;

    // calculate size, needed for packed data
    size_t totalPackedSize = sizeof(ui32); // num of botids
    ui32 numOfSupportedBots = 0;

    for (size_t botId = 0; botId < robotstxtcfg::max_botid; ++botId) {
        if (!IsBotIdSupported(botId)) {
            continue;
        }

        const auto& botInfo = BotIdToPrefixTreeBotInfo[GetNotOptimizedBotId(botId)];
        // botId + packedDataSize + packedData
        totalPackedSize += sizeof(ui32) + (botInfo ? botInfo->BufferPosition : sizeof(ui32));
        ++numOfSupportedBots;
    }

    ((TBuffer&)packedRepresentation).Reserve(totalPackedSize);

    // fill packed data
    char* packedPtr = ((TBuffer&)packedRepresentation).Data();

    *((ui32*)packedPtr) = numOfSupportedBots;
    packedPtr += sizeof(ui32);

    for (size_t botId = 0; botId < robotstxtcfg::max_botid; ++botId) {
        if (!IsBotIdSupported(botId)) {
            continue;
        }

        const auto& botInfo = BotIdToPrefixTreeBotInfo[GetNotOptimizedBotId(botId)];
        memcpy(packedPtr, &botId, sizeof(ui32));
        packedPtr += sizeof(ui32);

        if (botInfo) {
            *((ui32*)botInfo->Buffer.Get()) = botInfo->BufferPosition;
            memcpy(packedPtr, botInfo->Buffer.Get(), botInfo->BufferPosition);
            packedPtr += botInfo->BufferPosition;
        } else {
            // In absense of bot info we serialize only size of its buffer, which is 4 because it takes 4 bytes
            ui32 emptyBufferPosition = sizeof(ui32);
            memcpy(packedPtr, &emptyBufferPosition, sizeof(ui32));
            packedPtr += sizeof(ui32);
        }
    }

    data = ((TBuffer&)packedRepresentation).Data();
    return totalPackedSize;
}

void TPrefixTreeRobotsTxtRulesHandler::LoadPacked(const char* botsData, const char* botsDataEnd) {
    Clear();

    if (Y_UNLIKELY(botsDataEnd != nullptr && botsData >= botsDataEnd)) {
        ythrow yexception() << "Buffer overflow";
    }

    ui32 numOfBots = *((ui32*)botsData);
    botsData += sizeof(ui32);

    for (ui32 botIndex = 0; botIndex < numOfBots; ++botIndex) {
        if (Y_UNLIKELY(botsDataEnd != nullptr && botsData >= botsDataEnd)) {
            ythrow yexception() << "Buffer overflow";
        }

        ui32 botId = 0;
        memcpy(&botId, botsData, sizeof(ui32));
        botsData += sizeof(ui32);

        // skip bot id's, that not supported for now
        if (botId >= robotstxtcfg::max_botid || !IsBotIdSupported(botId)) {
            if (Y_UNLIKELY(botsDataEnd != nullptr && botsData >= botsDataEnd)) {
                ythrow yexception() << "Buffer overflow";
            }

            ui32 oneBotPackedSize = 0;
            memcpy(&oneBotPackedSize, botsData, sizeof(ui32));
            botsData += oneBotPackedSize;

            continue;
        }

        //SupportedBotIds.insert(botId);

        auto& botInfo = GetInfo(botId);

        if (Y_UNLIKELY(botsDataEnd != nullptr && botsData >= botsDataEnd)) {
            ythrow yexception() << "Buffer overflow";
        }

        static_assert(sizeof(botInfo.BufferSize) == sizeof(ui32), "BufferSize must be 4 bytes");
        static_assert(sizeof(botInfo.BufferPosition) == sizeof(ui32), "BufferPosition must be 4 bytes");

        memcpy(&botInfo.BufferSize, botsData, sizeof(ui32));
        memcpy(&botInfo.BufferPosition, botsData, sizeof(ui32));

        if (Y_UNLIKELY(botsDataEnd != nullptr && (botsData + botInfo.BufferSize) > botsDataEnd)) {
            ythrow yexception() << "Buffer overflow";
        }

        botInfo.Buffer.Reset(new char[botInfo.BufferSize]);
        memcpy(botInfo.Buffer.Get(), botsData, botInfo.BufferSize);
        SaveRulesFromBuffer(botId);

        if (botInfo.BufferSize > (int)sizeof(ui32)) { // empty data for robots means, that we don't have section for this bot
            LoadedBotIds.insert(botId);
        }

        botsData += botInfo.BufferSize;
    }

    OptimizeSize();
}

void TPrefixTreeRobotsTxtRulesHandler::Dump(const ui32 botId, FILE* dumpFile) {
    if (!dumpFile)
        dumpFile = stderr;
    fprintf(dumpFile, "User-Agent: %s\n", robotstxtcfg::GetFullName(botId).data());
    for (TRobotsTxtRulesIterator it = GetRulesIterator(botId); it.HasRule(); it.Next())
        fprintf(dumpFile, "%s: %s\n", DirTypeToName(it.GetRuleType()), it.GetInitialRule().data());
}

void TPrefixTreeRobotsTxtRulesHandler::Dump(const ui32 botId, IOutputStream& out) {
    out << "User-Agent: " << robotstxtcfg::GetFullName(botId) << Endl;
    for (TRobotsTxtRulesIterator it = GetRulesIterator(botId); it.HasRule(); it.Next())
        out << DirTypeToName(it.GetRuleType()) << ": " << it.GetInitialRule() << Endl;
}
