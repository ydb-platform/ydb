#include "application.h"
#include "options.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <optional>

std::optional<TApplication> App;

void StopHandler(int)
{
    std::cout << "Stopping session" << std::endl;
    if (App) {
        App->Stop();
    } else {
        exit(EXIT_FAILURE);
    }
}

int main(int argc, const char* argv[])
{
    signal(SIGINT, &StopHandler);
    signal(SIGTERM, &StopHandler);

    TOptions options(argc, argv);

    App.emplace(options);
    std::cout << "Application initialized" << std::endl;

    App->Run();
    std::cout << "Event loop completed" << std::endl;

    App->Finalize();
}
