#pragma once

struct TKeyboardAction {
    enum EControl {
        Symbol = 0,         // A symbol, take a look into Char variable for value

        KeyUnknown,
        KeyUp,
        KeyDown,
        KeyLeft,
        KeyRight,
        KeyBackspace,
        KeyEnter
    };

    EControl Control = KeyUnknown;
    int Char = 0;

    TKeyboardAction(EControl control, int c)
        : Control(control)
        , Char(c)
    {}

    TKeyboardAction(EControl control)
        : Control(control)
        , Char(0)
    {}
};

/////////////////////////////////////////////////////////////////////////////////
// GetKeyboardAction -- listen for events from a keyboard
/////////////////////////////////////////////////////////////////////////////////
TKeyboardAction GetKeyboardAction();

/////////////////////////////////////////////////////////////////////////////////
// TTerminalOutput -- terminal output abstraction
/////////////////////////////////////////////////////////////////////////////////
class TTerminalOutput {
private:
    static void FlushOutput();

public:
    static void Print(char c);
    static void Print(const char* str);
    static void MoveCursorLeft(int n = 1);
    static void MoveCursorRight(int n = 1);
    static void MoveCursorUp();
    static void MoveCursorDown();
    static void MoveCursorToColumn0();
    static void MoveCursorUpAtBeginningOfTheLine();
    static void MoveCursorDownAtBeginningOfTheLine();
    static void EraseEntireLine();
};