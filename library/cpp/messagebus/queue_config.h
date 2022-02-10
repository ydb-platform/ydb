#pragma once

#include <library/cpp/getopt/last_getopt.h>

namespace NBus {
    //////////////////////////////////////////////////////////////////
    /// \brief Configuration for message queue
    struct TBusQueueConfig {
        TString Name;
        int NumWorkers; ///< number of threads calling OnMessage(), OnReply() handlers

        TBusQueueConfig(); ///< initializes with default settings

        void ConfigureLastGetopt(NLastGetopt::TOpts&, const TString& prefix = "mb-");

        TString PrintToString() const;
    };

}
