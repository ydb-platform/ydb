#include "exc.h"
Base::~Base() {}
const char *Base::name() const { return "Base"; }
const char *Derived::name() const { return "Derived"; }
