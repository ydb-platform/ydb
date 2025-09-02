#pragma once

class TMul {
public:
   TMul(int x, int y)
      : X(x)
      , Y(y)
   {}

   int GetValue() const {
      return X * Y;
   }

private:
   const int X;
   const int Y;
};
