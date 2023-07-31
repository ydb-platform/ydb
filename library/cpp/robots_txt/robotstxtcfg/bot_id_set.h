#pragma once

#include "user_agents.h"

#include <bitset>


/// Simple vector-based set for bot ids, meant to optimize memory and lookups
class TBotIdSet
{
public:
    using TData = std::bitset<robotstxtcfg::max_botid>;

    constexpr TBotIdSet() noexcept = default;
    constexpr TBotIdSet(const TBotIdSet&) noexcept = default;
    constexpr TBotIdSet(TBotIdSet&&) noexcept = default;
    constexpr TBotIdSet& operator = (const TBotIdSet&) noexcept = default;
    constexpr TBotIdSet& operator = (TBotIdSet&&) noexcept = default;

    TBotIdSet(std::initializer_list<ui32> botIds) {
        for (auto id : botIds) {
            insert(id);
        }
    }

    static TBotIdSet All() noexcept {
        TBotIdSet res;
        res.Bots.set();
        return res;
    }

    constexpr bool contains(ui32 botId) const noexcept {
        return (botId < Bots.size()) && Bots[botId];
    }

    bool insert(ui32 botId) noexcept {
        if (botId >= Bots.size() || Bots[botId]) {
            return false;
        }
        Bots[botId] = true;
        return true;
    }

    bool remove(ui32 botId) noexcept {
        if (botId >= Bots.size() || !Bots[botId]) {
            return false;
        }
        Bots[botId] = false;
        return true;
    }

    void clear() noexcept {
        Bots.reset();
    }

    size_t size() const noexcept {
        return Bots.count();
    }

    bool empty() const noexcept {
        return Bots.none();
    }

    bool operator==(const TBotIdSet& rhs) const noexcept = default;

    TBotIdSet operator&(TBotIdSet rhs) const noexcept {
        rhs.Bots &= Bots;
        return rhs;
    }

    TBotIdSet operator|(TBotIdSet rhs) const noexcept {
        rhs.Bots |= Bots;
        return rhs;
    }

    TBotIdSet operator~() const noexcept {
        TBotIdSet result;
        result.Bots = ~Bots;
        return result;
    }

    class iterator
    {
    public:
        auto operator * () const noexcept {
            return BotId;
        }

        iterator& operator ++ () noexcept {
            while (BotId < Bots.size()) {
                if (Bots[++BotId]) {
                    break;
                }
            }
            return *this;
        }

        bool operator == (const iterator& rhs) const noexcept {
            return (&Bots == &rhs.Bots) && (BotId == rhs.BotId);
        }

        bool operator != (const iterator& rhs) const noexcept {
            return !(*this == rhs);
        }

    private:
        friend class TBotIdSet;
        iterator(const TData& bots, ui32 botId)
            : Bots(bots)
            , BotId(botId)
        {
            while (BotId < Bots.size() && !Bots[BotId]) {
                ++BotId;
            }
        }

    private:
        const TData& Bots;
        ui32 BotId;
    };

    iterator begin() const noexcept {
        return {Bots, robotstxtcfg::id_anybot};
    }

    iterator end() const noexcept {
        return {Bots, robotstxtcfg::max_botid};
    }

private:
    TData Bots {};
};
