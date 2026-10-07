#include "yql_yt_message_stream_impl.h"
namespace NYql {
std::shared_ptr<IYtMessageStreamIntegration> CreateYtMessageStreamIntegration(IStructuredTokenCredentialsFactory::TPtr credentialsFactory) {
    auto state = std::make_shared<TYtMessageStreamState>();
    state->Credentials = std::move(credentialsFactory);
    return CreateYtMessageStreamIntegrationImpl(std::move(state));
}
}
