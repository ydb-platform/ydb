/*
Copyright (c) 2012-2021 Ben Croston

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <string.h>
#include "c_gpio.h"

#define BCM2708_PERI_BASE_DEFAULT   0x20000000
#define BCM2709_PERI_BASE_DEFAULT   0x3f000000
#define BCM2710_PERI_BASE_DEFAULT   0x3f000000
#define BCM2711_PERI_BASE_DEFAULT   0xfe000000
#define GPIO_BASE_OFFSET            0x200000
#define FSEL_OFFSET                 0   // 0x0000
#define SET_OFFSET                  7   // 0x001c / 4
#define CLR_OFFSET                  10  // 0x0028 / 4
#define PINLEVEL_OFFSET             13  // 0x0034 / 4
#define EVENT_DETECT_OFFSET         16  // 0x0040 / 4
#define RISING_ED_OFFSET            19  // 0x004c / 4
#define FALLING_ED_OFFSET           22  // 0x0058 / 4
#define HIGH_DETECT_OFFSET          25  // 0x0064 / 4
#define LOW_DETECT_OFFSET           28  // 0x0070 / 4
#define PULLUPDN_OFFSET             37  // 0x0094 / 4
#define PULLUPDNCLK_OFFSET          38  // 0x0098 / 4

#define PULLUPDN_OFFSET_2711_0      57
#define PULLUPDN_OFFSET_2711_1      58
#define PULLUPDN_OFFSET_2711_2      59
#define PULLUPDN_OFFSET_2711_3      60

#define PAGE_SIZE  (4*1024)
#define BLOCK_SIZE (4*1024)

static volatile uint32_t *gpio_map;

void short_wait(void)
{
    int i;

    for (i=0; i<150; i++) {    // wait 150 cycles
        asm volatile("nop");
    }
}

int setup(void)
{
    int mem_fd;
    uint8_t *gpio_mem;
    uint32_t peri_base = 0;
    uint32_t gpio_base;
    uint8_t ranges[12] = { 0 };
    uint8_t rev[4] = { 0 };
    uint32_t cpu = 0;
    FILE *fp;
    char buffer[1024];
    char hardware[1024];
    int found = 0;

    // try /dev/gpiomem first - this does not require root privs
    if ((mem_fd = open("/dev/gpiomem", O_RDWR|O_SYNC)) > 0)
    {
        if ((gpio_map = (uint32_t *)mmap(NULL, BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, mem_fd, 0)) == MAP_FAILED) {
            return SETUP_MMAP_FAIL;
        } else {
            return SETUP_OK;
        }
    }

    // revert to /dev/mem method - requires root privileges

    if ((fp = fopen("/proc/device-tree/soc/ranges", "rb")) != NULL)
    {
        // get peri base from device tree
        if (fread(ranges, 1, sizeof(ranges), fp) >= 8) {
            peri_base = ranges[4] << 24 | ranges[5] << 16 | ranges[6] << 8 | ranges[7] << 0;
            if (!peri_base) {
                peri_base = ranges[8] << 24 | ranges[9] << 16 | ranges[10] << 8 | ranges[11] << 0;
            }
        }
        if ((ranges[0] != 0x7e) ||
            (ranges[1] != 0x00) ||
            (ranges[2] != 0x00) ||
            (ranges[3] != 0x00) ||
            ((peri_base != BCM2708_PERI_BASE_DEFAULT) && 
             (peri_base != BCM2709_PERI_BASE_DEFAULT) && 
             (peri_base != BCM2711_PERI_BASE_DEFAULT))) {
                 // invalid ranges file
                 peri_base = 0;
        }
        fclose(fp);
    }

    // guess peri_base based on /proc/device-tree/system/linux,revision
    if (!peri_base) {
        if ((fp = fopen("/proc/device-tree/system/linux,revision", "rb")) != NULL) {
            if (fread(rev, 1, sizeof(rev), fp) == 4) {
                cpu = (rev[2] >> 4) & 0xf;
                switch (cpu) {
                    case 0 : peri_base = BCM2708_PERI_BASE_DEFAULT;
                             break;
                    case 1 : 
                    case 2 : peri_base = BCM2709_PERI_BASE_DEFAULT;
                             break;
                    case 3 : peri_base = BCM2711_PERI_BASE_DEFAULT;
                             break;
                }
            }
            fclose(fp);
        }
    }

    // guess peri_base based on /proc/cpuinfo hardware field
    if (!peri_base) {
        if ((fp = fopen("/proc/cpuinfo", "r")) == NULL)
            return SETUP_CPUINFO_FAIL;

        while(!feof(fp) && !found && fgets(buffer, sizeof(buffer), fp)) {
            sscanf(buffer, "Hardware	: %s", hardware);
            if (strcmp(hardware, "BCM2708") == 0 || strcmp(hardware, "BCM2835") == 0) {
                // pi 1 hardware
                peri_base = BCM2708_PERI_BASE_DEFAULT;
            } else if (strcmp(hardware, "BCM2709") == 0 || strcmp(hardware, "BCM2836") == 0) {
                // pi 2 hardware
                peri_base = BCM2709_PERI_BASE_DEFAULT;
            } else if (strcmp(hardware, "BCM2710") == 0 || strcmp(hardware, "BCM2837") == 0) {
                // pi 3 hardware
                peri_base = BCM2710_PERI_BASE_DEFAULT;
            } else if (strcmp(hardware, "BCM2711") == 0) {
                // pi 4 hardware
                peri_base = BCM2711_PERI_BASE_DEFAULT;
            }
        }
        fclose(fp);
    }

    if (!peri_base)
        return SETUP_NO_PERI_ADDR;

    gpio_base = peri_base + GPIO_BASE_OFFSET;

    // mmap the GPIO memory registers
    if ((mem_fd = open("/dev/mem", O_RDWR|O_SYNC) ) < 0)
        return SETUP_DEVMEM_FAIL;

    if ((gpio_mem = malloc(BLOCK_SIZE + (PAGE_SIZE-1))) == NULL)
        return SETUP_MALLOC_FAIL;

    if ((uint32_t)gpio_mem % PAGE_SIZE)
        gpio_mem += PAGE_SIZE - ((uint32_t)gpio_mem % PAGE_SIZE);

    if ((gpio_map = (uint32_t *)mmap( (void *)gpio_mem, BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, mem_fd, gpio_base)) == MAP_FAILED)
        return SETUP_MMAP_FAIL;

    return SETUP_OK;
}

void clear_event_detect(int gpio)
{
    int offset = EVENT_DETECT_OFFSET + (gpio/32);
    int shift = (gpio%32);

    *(gpio_map+offset) |= (1 << shift);
    short_wait();
    *(gpio_map+offset) = 0;
}

int eventdetected(int gpio)
{
    int offset, value, bit;

    offset = EVENT_DETECT_OFFSET + (gpio/32);
    bit = (1 << (gpio%32));
    value = *(gpio_map+offset) & bit;
    if (value)
        clear_event_detect(gpio);
    return value;
}

void set_rising_event(int gpio, int enable)
{
    int offset = RISING_ED_OFFSET + (gpio/32);
    int shift = (gpio%32);

    if (enable)
        *(gpio_map+offset) |= 1 << shift;
    else
        *(gpio_map+offset) &= ~(1 << shift);
    clear_event_detect(gpio);
}

void set_falling_event(int gpio, int enable)
{
    int offset = FALLING_ED_OFFSET + (gpio/32);
    int shift = (gpio%32);

    if (enable) {
        *(gpio_map+offset) |= (1 << shift);
        *(gpio_map+offset) = (1 << shift);
    } else {
        *(gpio_map+offset) &= ~(1 << shift);
    }
    clear_event_detect(gpio);
}

void set_high_event(int gpio, int enable)
{
    int offset = HIGH_DETECT_OFFSET + (gpio/32);
    int shift = (gpio%32);

    if (enable)
        *(gpio_map+offset) |= (1 << shift);
    else
        *(gpio_map+offset) &= ~(1 << shift);
    clear_event_detect(gpio);
}

void set_low_event(int gpio, int enable)
{
    int offset = LOW_DETECT_OFFSET + (gpio/32);
    int shift = (gpio%32);

    if (enable)
        *(gpio_map+offset) |= 1 << shift;
    else
        *(gpio_map+offset) &= ~(1 << shift);
    clear_event_detect(gpio);
}

void set_pullupdn(int gpio, int pud)
{
    // Check GPIO register
    int is2711 = *(gpio_map+PULLUPDN_OFFSET_2711_3) != 0x6770696f;
    if (is2711) {
        // Pi 4 Pull-up/down method
        int pullreg = PULLUPDN_OFFSET_2711_0 + (gpio >> 4);
        int pullshift = (gpio & 0xf) << 1;
        unsigned int pullbits;
        unsigned int pull = 0;
        switch (pud) {
            case PUD_OFF:  pull = 0; break;
            case PUD_UP:   pull = 1; break;
            case PUD_DOWN: pull = 2; break;
            default:       pull = 0; // switch PUD to OFF for other values
        }
        pullbits = *(gpio_map + pullreg);
        pullbits &= ~(3 << pullshift);
        pullbits |= (pull << pullshift);
        *(gpio_map + pullreg) = pullbits;
    } else {
        // Legacy Pull-up/down method
        int clk_offset = PULLUPDNCLK_OFFSET + (gpio/32);
        int shift = (gpio%32);

        if (pud == PUD_DOWN) {
            *(gpio_map+PULLUPDN_OFFSET) = (*(gpio_map+PULLUPDN_OFFSET) & ~3) | PUD_DOWN;
        } else if (pud == PUD_UP) {
            *(gpio_map+PULLUPDN_OFFSET) = (*(gpio_map+PULLUPDN_OFFSET) & ~3) | PUD_UP;
        } else  { // pud == PUD_OFF
            *(gpio_map+PULLUPDN_OFFSET) &= ~3;
        }
        short_wait();
        *(gpio_map+clk_offset) = 1 << shift;
        short_wait();
        *(gpio_map+PULLUPDN_OFFSET) &= ~3;
        *(gpio_map+clk_offset) = 0;
    }
}

void setup_gpio(int gpio, int direction, int pud)
{
    int offset = FSEL_OFFSET + (gpio/10);
    int shift = (gpio%10)*3;

    set_pullupdn(gpio, pud);
    if (direction == OUTPUT)
        *(gpio_map+offset) = (*(gpio_map+offset) & ~(7<<shift)) | (1<<shift);
    else  // direction == INPUT
        *(gpio_map+offset) = (*(gpio_map+offset) & ~(7<<shift));
}

// Contribution by Eric Ptak <trouch@trouch.com>
int gpio_function(int gpio)
{
    int offset = FSEL_OFFSET + (gpio/10);
    int shift = (gpio%10)*3;
    int value = *(gpio_map+offset);
    value >>= shift;
    value &= 7;
    return value; // 0=input, 1=output, 4=alt0
}

void output_gpio(int gpio, int value)
{
    int offset, shift;

    if (value) // value == HIGH
        offset = SET_OFFSET + (gpio/32);
    else       // value == LOW
       offset = CLR_OFFSET + (gpio/32);

    shift = (gpio%32);

    *(gpio_map+offset) = 1 << shift;
}

int input_gpio(int gpio)
{
   int offset, value, mask;

   offset = PINLEVEL_OFFSET + (gpio/32);
   mask = (1 << gpio%32);
   value = *(gpio_map+offset) & mask;
   return value;
}

void cleanup(void)
{
    munmap((void *)gpio_map, BLOCK_SIZE);
}
