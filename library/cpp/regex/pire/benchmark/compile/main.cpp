#include <library/cpp/regex/pire/pire.h>
#include <util/stream/output.h>
#include <util/string/cast.h>

#include <chrono>
#include <optional>
#include <sys/resource.h>

int main(int argc, char** argv) {
    if (argc != 3 && argc != 4) {
        Cerr << "Usage: pire_compile_benchmark REGEXP ITERATIONS [BUDGET]\n";
        return 2;
    }
    const size_t iterations = FromString<size_t>(argv[2]);
    if (!iterations) {
        Cerr << "ITERATIONS must be positive\n";
        return 2;
    }
    const std::optional<size_t> limit = argc == 4 ? std::optional<size_t>(FromString<size_t>(argv[3])) : std::nullopt;
    size_t states = 0;
    const auto start = std::chrono::steady_clock::now();
    const char* status = "ok";
    try {
        for (size_t i = 0; i < iterations; ++i) {
            std::optional<Pire::ScopedOperationBudget> budget;
            if (limit)
                budget.emplace(*limit);
            auto scanner = Pire::Lexer(static_cast<const char*>(argv[1])).Parse().Compile<Pire::Scanner>();
            states += scanner.Size();
        }
    } catch (const Pire::BudgetExceeded& e) {
        status = "budget_exceeded";
        Cerr << e.what() << Endl;
    } catch (const Pire::Error& e) {
        status = "error";
        Cerr << e.what() << Endl;
    }
    const double elapsed = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - start).count();
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    Cout << status << '\t' << elapsed << '\t' << usage.ru_maxrss << '\t' << states << Endl;
}
