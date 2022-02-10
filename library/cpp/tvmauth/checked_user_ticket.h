#pragma once

#include "ticket_status.h"
#include "type.h"
#include "utils.h"

#include <util/generic/ptr.h>

namespace NTvmAuth::NInternal {
    class TCanningKnife;
}

namespace NTvmAuth {
    /*!
     * BlackboxEnv describes environment of Passport:
     * https://wiki.yandex-team.ru/passport/tvm2/user-ticket/#0-opredeljaemsjasokruzhenijami
     */
    enum class EBlackboxEnv: ui8 {
        Prod,
        Test,
        ProdYateam,
        TestYateam,
        Stress
    };

    /*!
     * UserTicket contains only valid users.
     * Details: https://wiki.yandex-team.ru/passport/tvm2/user-ticket/#chtoestvusertickete
     */
    class TCheckedUserTicket {
    public:
        class TImpl;

        TCheckedUserTicket(THolder<TImpl> impl);
        TCheckedUserTicket(TCheckedUserTicket&&);
        ~TCheckedUserTicket();

        TCheckedUserTicket& operator=(TCheckedUserTicket&&);

        /*!
         * @return True value if ticket parsed and checked successfully
         */
        explicit operator bool() const;

        /*!
         * Never empty
         * @return UIDs of users listed in ticket
         */
        const TUids& GetUids() const;

        /*!
         * Maybe 0
         * @return Default user in ticket
         */
        TUid GetDefaultUid() const;

        /*!
         * Scopes inherited from credential - never empty
         * @return Newly constructed vector of scopes
         */
        const TScopes& GetScopes() const;

        /*!
         * Check if scope presented in ticket
         */
        bool HasScope(TStringBuf scopeName) const;

        /*!
         * @return Ticket check status
         */
        ETicketStatus GetStatus() const;

        /*!
         * DebugInfo is human readable data for debug purposes
         * @return Serialized ticket
         */
        TString DebugInfo() const;

        /*!
         * Env of user
         */
        EBlackboxEnv GetEnv() const;

    public: // for python binding
        TCheckedUserTicket() = default;

    private:
        THolder<TImpl> Impl_;
        friend class NInternal::TCanningKnife;
    };
}
