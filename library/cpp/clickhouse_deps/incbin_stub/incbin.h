#pragma once

#define INCBIN(name, file) \
const unsigned char * g ## name ## Data = nullptr; \
unsigned int g ## name ## Size = 0;
