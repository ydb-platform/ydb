#include "application.h"
#include "options.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/stream/output.h>

#include <optional>

std::optional<TApplication> App;

void StopHandler(int)
{
    Cout << "Stopping session" << Endl;
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
    Cout << "Application initialized" << Endl;

    App->Run();
    Cout << "Event loop completed" << Endl;

    App->Finalize();
}
