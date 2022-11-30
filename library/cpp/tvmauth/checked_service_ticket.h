#pragma once

#include "ticket_status.h"
#include "type.h"
#include "utils.h"

#include <util/generic/ptr.h>

namespace NTvmAuth::NInternal {
    class TCanningKnife;
}

namespace NTvmAuth {
    class TCheckedServiceTicket {
    public:
        class TImpl;

        TCheckedServiceTicket(THolder<TImpl> impl);
        TCheckedServiceTicket(TCheckedServiceTicket&& o);
        ~TCheckedServiceTicket();

        TCheckedServiceTicket& operator=(TCheckedServiceTicket&&);

        /*!
         * @return True value if ticket parsed and checked successfully
         */
        explicit operator bool() const;

        /*!
         * @return TTvmId of request destination
         */
        TTvmId GetDst() const;

        /*!
         * You should check src with your ACL
         * @return TvmId of request source
         */
        TTvmId GetSrc() const;

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
         * IssuerUID is UID of developer who is debuging something,
         * so he(she) issued ServiceTicket with his(her) ssh-sign:
         * it is grant_type=sshkey in tvm-api.
         * https://wiki.yandex-team.ru/passport/tvm2/debug/#sxoditvapizakrytoeserviceticketami
         * @return uid
         */
        TMaybe<TUid> GetIssuerUid() const;

    public: // for python binding
        TCheckedServiceTicket() = default;

    private:
        THolder<TImpl> Impl_;
        friend class NInternal::TCanningKnife;
    };

    namespace NBlackboxTvmId {
        const TStringBuf Prod = "222";
        const TStringBuf Test = "224";
        const TStringBuf ProdYateam = "223";
        const TStringBuf TestYateam = "225";
        const TStringBuf Stress = "226";
        const TStringBuf Mimino = "239";
    }
}
