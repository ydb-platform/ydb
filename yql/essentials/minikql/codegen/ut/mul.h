#pragma once

class TMul {
public:
    TMul(int x, int y)
        : X_(x)
        , Y_(y)
    {
    }

    int GetValue() const {
        return X_ * Y_;
    }

private:
    const int X_;
    const int Y_;
};
