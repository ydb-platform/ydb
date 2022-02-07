#pragma once

#include <util/system/types.h>


using poly1305_state = ui8[512];

class Poly1305Vec
{
public:
    static constexpr size_t KEY_SIZE = 32;
    static constexpr size_t MAC_SIZE = 16;

public:
    void SetKey(const ui8* key, size_t size);
    void Update(const ui8* msg, size_t size);
    void Finish(ui8 mac[MAC_SIZE]);

private:
    poly1305_state state;
};
