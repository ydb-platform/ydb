#include "httpload.h"
#include "http_digest.h"

socketAbstractHandler* socketHandlerFactory::chooseHandler(const THttpURL& url) {
    if (url.IsValidGlobal() && url.GetScheme() == THttpURL::SchemeHTTP)
        return new socketRegularHandler;

    return nullptr;
}

/************************************************************/
socketHandlerFactory socketHandlerFactory::sInstance;
/************************************************************/
