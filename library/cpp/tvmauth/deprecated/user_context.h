#pragma once

#include <library/cpp/tvmauth/checked_user_ticket.h>

#include <util/generic/ptr.h>

namespace NTvmAuth {
    class TUserContext: public TAtomicRefCount<TUserContext> {
    public:
        TUserContext(EBlackboxEnv env, TStringBuf tvmKeysResponse);
        TUserContext(TUserContext&&);
        ~TUserContext();

        TUserContext& operator=(TUserContext&&);

        /*!
         * Parse and validate user ticket body then create TCheckedUserTicket object.
         * @param ticketBody
         * @return TCheckedUserTicket object
         */
        TCheckedUserTicket Check(TStringBuf ticketBody) const;

        class TImpl;

    private:
        THolder<TImpl> Impl_;
    };

    using TUserContextPtr = TIntrusiveConstPtr<TUserContext>;
}
