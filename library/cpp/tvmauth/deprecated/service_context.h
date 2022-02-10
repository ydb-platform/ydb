#pragma once

#include <library/cpp/tvmauth/checked_service_ticket.h>

#include <util/generic/ptr.h>

namespace NTvmAuth {
    class TServiceContext: public TAtomicRefCount<TServiceContext> {
    public:
        /*!
         * Create service context. Serivce contexts are used to store TVM keys and parse service tickets.
         * @param selfTvmId
         * @param secretBase64
         * @param tvmKeysResponse
         */
        TServiceContext(TStringBuf secretBase64, TTvmId selfTvmId, TStringBuf tvmKeysResponse);
        TServiceContext(TServiceContext&&);
        ~TServiceContext();

        /*!
         * Create service context only for checking service tickets
         * \param[in] selfTvmId
         * \param[in] tvmKeysResponse
         * \return
         */
        static TServiceContext CheckingFactory(TTvmId selfTvmId, TStringBuf tvmKeysResponse);

        /*!
         * Create service context only for signing HTTP request to TVM-API
         * \param[in] secretBase64
         * \return
         */
        static TServiceContext SigningFactory(TStringBuf secretBase64);

        TServiceContext& operator=(TServiceContext&&);

        /*!
         * Parse and validate service ticket body then create TCheckedServiceTicket object.
         * @param ticketBody
         * @return TCheckedServiceTicket object
         */
        TCheckedServiceTicket Check(TStringBuf ticketBody) const;

        /*!
         * Sign params for TVM API
         * @param ts Param 'ts' of request to TVM
         * @param dst Param 'dst' of request to TVM
         * @param scopes Param 'scopes' of request to TVM
         * @return Signed string
         */
        TString SignCgiParamsForTvm(TStringBuf ts, TStringBuf dst, TStringBuf scopes = TStringBuf()) const;

        class TImpl;

    private:
        TServiceContext() = default;

    private:
        THolder<TImpl> Impl_;
    };

    using TServiceContextPtr = TIntrusiveConstPtr<TServiceContext>;
}
