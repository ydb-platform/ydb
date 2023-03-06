#pragma once

#include <memory>
#include <string>
#include <vector>
#include "term_io.h"

class TInteractiveCli {
public:
    // parameterize interactive cli with this logic to handle events
    class ILogic {
    public:
        // called after keyboards Enter is pressed, returns true is command is ready to run
        virtual bool Ready(const std::string &text) = 0;
        // called to run a command
        virtual void Run(const std::string &text, const std::string &stat) = 0;
        virtual ~ILogic() {}
    };

    struct TConfig {
        std::string Prompt = "=> ";
        size_t HistorySize = 10;

        static TConfig Default();
        static TConfig LittleColor();
        static TConfig FunnyTest();
    };

    // init
    TInteractiveCli(std::shared_ptr<ILogic> logic, TConfig&& cfg = TConfig::Default());
    // cleanup
    ~TInteractiveCli();
    // run cli
    void Run();
    // handle input from a keyboard
    void HandleInput(TKeyboardAction action);

private:
    class TState;
    std::unique_ptr<TState> State;
};
