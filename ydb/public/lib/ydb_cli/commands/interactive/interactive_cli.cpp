#include <string>
#include <memory>
#include <deque>
#include <vector>
#include <sstream>
#include "interactive_cli.h"

using namespace std;

/////////////////////////////////////////////////////////////////////////////////
// TInteractiveCli::TConfig
/////////////////////////////////////////////////////////////////////////////////
TInteractiveCli::TConfig TInteractiveCli::TConfig::Default() {
    return TConfig();
}

TInteractiveCli::TConfig TInteractiveCli::TConfig::LittleColor() {
    TConfig cfg;
    cfg.Prompt = "\033[33m=> \033[0m";
    return cfg;
}

TInteractiveCli::TConfig TInteractiveCli::TConfig::FunnyTest() {
    TConfig cfg;
    cfg.Prompt = "\033[36my\033[35md\033[33mb> \033[0m";
    cfg.HistorySize = 5;
    return cfg;
}


/////////////////////////////////////////////////////////////////////////////////
// TLine
/////////////////////////////////////////////////////////////////////////////////
class TLine {
public:
    void Insert(char c) {
        if (Data.size() == CursorPos) {
            Data.append(1, c);
            ++CursorPos;
            TTerminalOutput::Print(c);
        } else {
            string m = Data.substr(CursorPos, string::npos);
            Data.insert(CursorPos, 1, c);
            TTerminalOutput::Print(c);
            ++CursorPos;
            TTerminalOutput::Print(m.c_str());
            for (size_t i = 0; i < m.size(); ++i)
                TTerminalOutput::MoveCursorLeft();
        }
    }

    bool Backspace() {
        if (CursorPos > 0) {
            string m = Data.substr(CursorPos, string::npos);
            Data.erase(CursorPos-1, 1);
            --CursorPos;
            TTerminalOutput::MoveCursorLeft();

            TTerminalOutput::Print(m.c_str());
            TTerminalOutput::Print(' ');

            for (size_t i = 0; i < m.size() + 1; ++i)
                TTerminalOutput::MoveCursorLeft();

            return true;
        } else {
            return false;
        }
    }

    bool MoveCursorRight() {
        if (CursorPos < Data.size()) {
            ++CursorPos;
            TTerminalOutput::MoveCursorRight();
            return true;
        } else {
            return false;
        }
    }

    bool MoveCursorLeft() {
        if (CursorPos > 0) {
            --CursorPos;
            TTerminalOutput::MoveCursorLeft();
            return true;
        } else {
            return false;
        }
    }

    string GetCommand() const {
        return Data;
    }

    void ClearScreenArea() {
        for (size_t i = 0; i < CursorPos; ++i)
            TTerminalOutput::MoveCursorLeft();

        for (size_t i = 0; i < Data.size(); ++i)
            TTerminalOutput::Print(' ');

        for (size_t i = 0; i < Data.size(); ++i)
            TTerminalOutput::MoveCursorLeft();

        CursorPos = 0;
    }

    void DrawScreenArea() {
        for (size_t i = 0; i < Data.size(); ++i)
            TTerminalOutput::Print(Data[i]);

        CursorPos = Data.size();
    }

    void GoToEnd() {
        for (size_t i = CursorPos; i < Data.size(); ++i)
            TTerminalOutput::MoveCursorRight();

        CursorPos = Data.size();
    }

    const string &GetData() const {
        return Data;
    }

    bool CursorAtTheEnd() const {
        return CursorPos == Data.size();
    }

    bool CursorAtTheBeginning() const {
        return CursorPos == 0;
    }

    void SetCursorPosToZero() {
        CursorPos = 0;
    }

private:
    string Data;
    size_t CursorPos = 0;
};

/////////////////////////////////////////////////////////////////////////////////
// TCommand
/////////////////////////////////////////////////////////////////////////////////
class TCommand {
public:
    TCommand(std::shared_ptr<TInteractiveCli::ILogic> logic, shared_ptr<TInteractiveCli::TConfig> config)
        : Logic(std::move(logic))
        , Config(std::move(config))
    {
        Lines.push_back(TLine());
    }

    void Insert(char c) {
        Lines[CurLine].Insert(c);
    }

    void MoveCursorLeft() {
        if (Lines[CurLine].CursorAtTheBeginning()) {
            if (CurLine != 0) {
                TTerminalOutput::MoveCursorUpAtBeginningOfTheLine();
                --CurLine;
                if (CurLine == 0) {
                    TTerminalOutput::Print(Config->Prompt.c_str());
                }

                Lines[CurLine].DrawScreenArea();
            }
        } else {
            Lines[CurLine].MoveCursorLeft();
        }
    }

    void MoveCursorRight() {
        if (Lines[CurLine].CursorAtTheEnd()) {
            if (CurLine + 1 < Lines.size()) {
                ++CurLine;
                TTerminalOutput::MoveCursorDownAtBeginningOfTheLine();
                Lines[CurLine].SetCursorPosToZero();
            }
        } else {
            Lines[CurLine].MoveCursorRight();
        }
    }

    bool EnterPressed() {
        if (QueryIsReady()) {
            for (; CurLine < Lines.size() - 1; ++CurLine) {
                TTerminalOutput::MoveCursorDown();
            }
            TTerminalOutput::Print('\n');
            return true;
        } else {
            for (size_t i = 1; i < (Lines.size() - CurLine); ++i)
                TTerminalOutput::MoveCursorDownAtBeginningOfTheLine();

            CurLine = Lines.size() - 1;
            Lines[CurLine].GoToEnd();

            Lines.push_back(TLine());
            ++CurLine;
            TTerminalOutput::Print('\n');
            return false;
        }
    }

    void Backspace() {
        bool res = Lines[CurLine].Backspace();
        if (!res) {
            if (CurLine != 0) {
                // what is left from the line being deleted
                //int deletedLine = CurLine;
                string m = Lines[CurLine].GetData();


                // erase all lines below
                TTerminalOutput::EraseEntireLine();
                for (size_t i = CurLine + 1; i < Lines.size(); ++i) {
                    TTerminalOutput::MoveCursorDownAtBeginningOfTheLine();
                    TTerminalOutput::EraseEntireLine();
                }

                // return cursor back
                for (size_t i = CurLine + 1; i < Lines.size(); ++i) {
                    TTerminalOutput::MoveCursorUpAtBeginningOfTheLine();
                }

                // draw lines below the erased line
                for (size_t i = CurLine + 1; i < Lines.size(); ++i) {
                    Lines[i].DrawScreenArea();
                    TTerminalOutput::Print('\n');
                }

                // return cursor back
                for (size_t i = CurLine + 1; i < Lines.size(); ++i) {
                    TTerminalOutput::MoveCursorUpAtBeginningOfTheLine();
                }

                // erase deleted line from the vector
                Lines.erase(Lines.begin() + CurLine);

                // go to the line up
                TTerminalOutput::MoveCursorUpAtBeginningOfTheLine();
                --CurLine;
                if (CurLine == 0) {
                    TTerminalOutput::Print(Config->Prompt.c_str());
                }

                Lines[CurLine].DrawScreenArea();

                // append data from removed line
                for (size_t i = 0; i < m.size(); ++i) {
                    Lines[CurLine].Insert(m[i]);
                }
                for (size_t i = 0; i < m.size(); ++i) {
                    Lines[CurLine].MoveCursorLeft();
                }
            }
        }
    }

    string GetCommand() {
        string res;
        for (size_t i = 0; i < Lines.size() - 1; ++i) {
            res += Lines[i].GetCommand();
            res += '\n';
        }
        res += Lines[Lines.size() - 1].GetCommand();

        return res;
    }

    void ClearScreenArea() {
        for (size_t i = CurLine; i < Lines.size() - 1; ++i) {
            TTerminalOutput::MoveCursorDown();
        }

        for (size_t i = 0; i < Lines.size() - 1; ++i) {
            TTerminalOutput::EraseEntireLine();
            TTerminalOutput::MoveCursorUp();
        }

        TTerminalOutput::EraseEntireLine();
        TTerminalOutput::MoveCursorToColumn0();
        TTerminalOutput::Print(Config->Prompt.c_str());

        CurLine = 0;
    }

    void DrawScreenArea() {
        for (size_t i = 0; i < Lines.size() - 1; ++i) {
            Lines[i].DrawScreenArea();
            TTerminalOutput::Print('\n');
        }
        CurLine = Lines.size() - 1;
        Lines[CurLine].DrawScreenArea();
    }

private:
    shared_ptr<TInteractiveCli::ILogic> Logic;
    shared_ptr<TInteractiveCli::TConfig> Config;
    vector<TLine> Lines;
    size_t CurLine = 0;

    bool QueryIsReady() {
        return Logic->Ready(GetCommand());
    }
};


/////////////////////////////////////////////////////////////////////////////////
// TInteractiveCli::TState
/////////////////////////////////////////////////////////////////////////////////
class TInteractiveCli::TState {
public:
    TState(std::shared_ptr<TInteractiveCli::ILogic> logic, TInteractiveCli::TConfig&& cfg)
        : Logic(std::move(logic))
        , Config(make_shared<TConfig>(std::move(cfg)))
        , CurCmd(Logic, Config)
    {
        StartNewCommand();
    }

    void Insert(char c) {
        CurCmd.Insert(c);
    }

    void MoveCursorLeft() {
        CurCmd.MoveCursorLeft();
    }

    void MoveCursorRight() {
        CurCmd.MoveCursorRight();
    }

    void Backspace() {
        CurCmd.Backspace();
    }

    void MoveCursorUp() {
        if (History.size() == 1)
            return;

        if (HistoryPosition > 1) {
            CurCmd.ClearScreenArea();

            if (HistoryPosition == History.size()) {
                History[HistoryPosition - 1] = CurCmd;
            }

            --HistoryPosition;
            History[HistoryPosition - 1].DrawScreenArea();
            CurCmd = History[HistoryPosition - 1];
        }
    }

    void MoveCursorDown() {
        if (HistoryPosition == History.size())
            return;

        CurCmd.ClearScreenArea();

        ++HistoryPosition;
        History[HistoryPosition - 1].DrawScreenArea();
        CurCmd = History[HistoryPosition - 1];
    }

    void Enter() {
        bool completed = CurCmd.EnterPressed();
        if (completed) {
            History[History.size() - 1] = CurCmd;

            const auto &text = History[History.size() - 1].GetCommand();
            stringstream stat;
            stat << "HistoryPosition=" << HistoryPosition << " History.size()=" << History.size();
            Logic->Run(text, stat.str());

            CurCmd = TCommand(Logic, Config);
            StartNewCommand();
            Prompt();
        }
    }

    void Welcome() {
        Prompt();
    }

private:
    std::shared_ptr<ILogic> Logic;
    std::shared_ptr<TConfig> Config;
    size_t HistoryPosition = 0;
    TCommand CurCmd;
    deque<TCommand> History;

    void Prompt() {
        TTerminalOutput::Print(Config->Prompt.c_str());
    }

    void StartNewCommand() {
        if (History.size() < Config->HistorySize) {
            History.push_back(TCommand(Logic, Config));
        } else {
            History.pop_front();
            History.push_back(TCommand(Logic, Config));
        }
        HistoryPosition = History.size();
    }
};


/////////////////////////////////////////////////////////////////////////////////
// TInteractiveCli
/////////////////////////////////////////////////////////////////////////////////
TInteractiveCli::TInteractiveCli(std::shared_ptr<TInteractiveCli::ILogic> logic, TInteractiveCli::TConfig&& cfg)
    : State(make_unique<TInteractiveCli::TState>(std::move(logic), std::move(cfg)))
{
}

TInteractiveCli::~TInteractiveCli() {
}

void TInteractiveCli::Run() {
    State->Welcome();
}

void TInteractiveCli::HandleInput(TKeyboardAction action) {
    switch(action.Control) {
        case TKeyboardAction::Symbol:
            State->Insert((char)(action.Char));
            break;
        case TKeyboardAction::KeyLeft:
            State->MoveCursorLeft();
            break;
        case TKeyboardAction::KeyRight:
            State->MoveCursorRight();
            break;
        case TKeyboardAction::KeyUp:
            State->MoveCursorUp();
            break;
        case TKeyboardAction::KeyDown:
            State->MoveCursorDown();
            break;
        case TKeyboardAction::KeyEnter:
            State->Enter();
            break;
        case TKeyboardAction::KeyBackspace:
            State->Backspace();
            break;
        default:
            break;
    }
}
