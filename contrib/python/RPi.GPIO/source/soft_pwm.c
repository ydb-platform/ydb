/*
Copyright (c) 2013-2021 Ben Croston

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

#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include "c_gpio.h"
#include "soft_pwm.h"

struct pwm
{
    unsigned int gpio;
    float freq;
    float dutycycle;
    float basetime;
    float slicetime;
    struct timespec req_on, req_off;
    int running;
    struct pwm *next;
};
struct pwm *pwm_list = NULL;

void remove_pwm(unsigned int gpio)
{
    struct pwm *p = pwm_list;
    struct pwm *prev = NULL;
    struct pwm *temp;

    while (p != NULL)
    {
        if (p->gpio == gpio)
        {
            if (prev == NULL) {
                pwm_list = p->next;
            } else {
                prev->next = p->next;
            }
            temp = p;
            p = p->next;
            temp->running = 0; // signal the thread to stop. The thread will free() the pwm struct when it's done with it.
        } else {
            prev = p;
            p = p->next;
        }
    }
}

void calculate_times(struct pwm *p)
{
    long long usec;

    usec = (long long)(p->dutycycle * p->slicetime * 1000.0);
    p->req_on.tv_sec = (int)(usec / 1000000LL);
    usec -= (long long)p->req_on.tv_sec * 1000000LL;
    p->req_on.tv_nsec = (long)usec * 1000L;

    usec = (long long)((100.0-p->dutycycle) * p->slicetime * 1000.0);
    p->req_off.tv_sec = (int)(usec / 1000000LL);
    usec -= (long long)p->req_off.tv_sec * 1000000LL;
    p->req_off.tv_nsec = (long)usec * 1000L;
}

void full_sleep(struct timespec *req)
{
    struct timespec rem = {0};

    if (nanosleep(req,&rem) == -1)
        full_sleep(&rem);
}

void *pwm_thread(void *threadarg)
{
    struct pwm *p = (struct pwm *)threadarg;

    while (p->running)
    {

        if (p->dutycycle > 0.0)
        {
            output_gpio(p->gpio, 1);
            full_sleep(&p->req_on);
        }

        if (p->dutycycle < 100.0)
        {
            output_gpio(p->gpio, 0);
            full_sleep(&p->req_off);
        }
    }

    // clean up
    output_gpio(p->gpio, 0);
    free(p);
    pthread_exit(NULL);
}

struct pwm *add_new_pwm(unsigned int gpio)
{
    struct pwm *new_pwm;

    new_pwm = malloc(sizeof(struct pwm));
    new_pwm->gpio = gpio;
    new_pwm->running = 0;
    new_pwm->next = NULL;
    // default to 1 kHz frequency, dutycycle 0.0
    new_pwm->freq = 1000.0;
    new_pwm->dutycycle = 0.0;
    new_pwm->basetime = 1.0;    // 1 ms
    new_pwm->slicetime = 0.01;  // 0.01 ms
    calculate_times(new_pwm);
    return new_pwm;
}

struct pwm *find_pwm(unsigned int gpio)
/* Return the pwm record for gpio, creating it if it does not exist */
{
    struct pwm *p = pwm_list;

    if (pwm_list == NULL)
    {
        pwm_list = add_new_pwm(gpio);
        return pwm_list;
    }

    while (p != NULL)
    {
        if (p->gpio == gpio)
            return p;
        if (p->next == NULL)
        {
            p->next = add_new_pwm(gpio);
            return p->next;
        }
        p = p->next;
    }
    return NULL;
}

void pwm_set_duty_cycle(unsigned int gpio, float dutycycle)
{
    struct pwm *p;

    if (dutycycle < 0.0 || dutycycle > 100.0)
    {
        // btc fixme - error
        return;
    }

    if ((p = find_pwm(gpio)) != NULL)
    {
        p->dutycycle = dutycycle;
        calculate_times(p);
    }
}

void pwm_set_frequency(unsigned int gpio, float freq)
{
    struct pwm *p;

    if (freq <= 0.0) // to avoid divide by zero
    {
        // btc fixme - error
        return;
    }

    if ((p = find_pwm(gpio)) != NULL)
    {
        p->basetime = 1000.0 / freq;    // calculated in ms
        p->slicetime = p->basetime / 100.0;
        calculate_times(p);
    }
}

void pwm_start(unsigned int gpio)
{
    pthread_t threads;
    struct pwm *p;

    if (((p = find_pwm(gpio)) == NULL) || p->running)
        return;

    p->running = 1;
    if (pthread_create(&threads, NULL, pwm_thread, (void *)p) != 0)
    {
        // btc fixme - error
        p->running = 0;
        return;
    }
    pthread_detach(threads);
}

void pwm_stop(unsigned int gpio)
{
    remove_pwm(gpio);
}

// returns 1 if there is a PWM for this gpio, 0 otherwise
int pwm_exists(unsigned int gpio)
{
    struct pwm *p = pwm_list;

    while (p != NULL)
    {
        if (p->gpio == gpio)
        {
            return 1;
        } else {
            p = p->next;
        }
    }
    return 0;
}
