#include <stdio.h>
#include "term_io.h"

#define KEY_UP 65
#define KEY_DOWN 66
#define KEY_LEFT 68
#define KEY_RIGHT 67
#define KEY_BACKSPACE 8

#if defined(_WIN32) || defined(_WIN64)

static char getch(void) {
    // Not implemented on Windows
    return 0;
}

#else

#include <termios.h>

static struct termios old, current;

// Initialize new terminal i/o settings
static void initTermios(int echo) {
    tcgetattr(0, &old); // grab old terminal i/o settings
    current = old; // make new settings same as old settings
    current.c_lflag &= ~ICANON; // disable buffered i/o
    if (echo) {
        current.c_lflag |= ECHO; // set echo mode
    } else {
        current.c_lflag &= ~ECHO; // set no echo mode
    }
    tcsetattr(0, TCSANOW, &current); // use these new terminal i/o settings now
}

// Restore old terminal i/o settings
static void resetTermios(void) {
    tcsetattr(0, TCSANOW, &old);
}

// Read 1 character - echo defines echo mode
static char getch_(int echo) {
    char ch;
    initTermios(echo);
    ch = getchar();
    resetTermios();
    return ch;
}

// Read 1 character without echo
static char getch(void) {
    return getch_(0);
}

#endif // _win_

#define DEBUG_OUTPUT(x)

TKeyboardAction GetKeyboardAction() {
    int c, ex1, ex2;
    DEBUG_OUTPUT(printf("GetKeyboardAction\n"));

    c = getch();

    if (c && c != 27) {
        if (c == 127) {
            DEBUG_OUTPUT(printf("Backspace\n"));
            return TKeyboardAction(TKeyboardAction::KeyBackspace);
        } else if (c == 10) {
            DEBUG_OUTPUT(printf("Enter\n"));
            return TKeyboardAction(TKeyboardAction::KeyEnter);
        } else {
            DEBUG_OUTPUT(printf("Symbol %c %d\n", (char) c, c));
            return TKeyboardAction(TKeyboardAction::Symbol, c);
        }
    } else {
        ex1 = getch();
        if (ex1 && ex1 != 91) {
            DEBUG_OUTPUT(printf("some unexpected escape symbol\n"));
            return TKeyboardAction(TKeyboardAction::KeyUnknown);
        } else {
            switch(ex2 = getch()) {
                case KEY_UP:
                    DEBUG_OUTPUT(printf("Up\n"));
                    return TKeyboardAction(TKeyboardAction::KeyUp);
                case KEY_DOWN:
                    DEBUG_OUTPUT(printf("Down\n"));
                    return TKeyboardAction(TKeyboardAction::KeyDown);
                case KEY_LEFT:
                    DEBUG_OUTPUT(printf("Left\n"));
                    return TKeyboardAction(TKeyboardAction::KeyLeft);
                case KEY_RIGHT:
                    DEBUG_OUTPUT(printf("Right\n"));
                    return TKeyboardAction(TKeyboardAction::KeyRight);
                default:
                    DEBUG_OUTPUT(printf("Excapes: c=%d ex1=%d ex2=%d\n", c, ex1, ex2));
                    return TKeyboardAction(TKeyboardAction::KeyUnknown);
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////
// TTerminalOutput -- terminal output abstraction
/////////////////////////////////////////////////////////////////////////////////
void TTerminalOutput::FlushOutput() {
    fflush(stdout);
}

void TTerminalOutput::Print(char c) {
    printf("%c", c);
    FlushOutput();
}

void TTerminalOutput::Print(const char* str) {
    printf("%s", str);
    FlushOutput();
}

void TTerminalOutput::MoveCursorLeft(int n) {
    printf("\033[%dD", n);
    FlushOutput();
}

void TTerminalOutput::MoveCursorRight(int n) {
    for (int i = 0; i < n; ++i) {
        printf("\033[1C");
    }
    FlushOutput();
}

void TTerminalOutput::MoveCursorUp() {
    printf("\033[1A");
    FlushOutput();
}

void TTerminalOutput::MoveCursorDown() {
    printf("\033[1B");
    FlushOutput();
}

void TTerminalOutput::MoveCursorToColumn0() {
    printf("\033[0G");
    FlushOutput();    
}

void TTerminalOutput::MoveCursorUpAtBeginningOfTheLine() {
    printf("\033[1F");
    FlushOutput();
}

void TTerminalOutput::MoveCursorDownAtBeginningOfTheLine() {
    printf("\033[1E");
    FlushOutput();
}

void TTerminalOutput::EraseEntireLine() {
    printf("\033[2K");
    FlushOutput();

}

