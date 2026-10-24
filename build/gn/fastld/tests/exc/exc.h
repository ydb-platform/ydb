#pragma once
#include <exception>
#include <typeinfo>
struct Base {  // key function in ex_base.cpp: typeinfo is strong, in ex_base only
    virtual ~Base();
    virtual const char *name() const;
};
struct Derived : Base {
    const char *name() const override;
};
struct Vague {  // no key function: typeinfo emitted (weak) in every user
    int v;
};
struct VBase {  // all inline: vtable + typeinfo have vague linkage everywhere
    virtual ~VBase() = default;
    virtual int id() const { return 1; }
};
struct VDer : VBase {
    int id() const override { return 2; }
};
void throw_derived();
void throw_vague(int v);
void throw_runtime(const char *msg);
VBase *make_vder();
const std::type_info *typeid_vague();
const std::type_info *typeid_vder();
void call_cb(void (*cb)());
const char *catcher_report();
std::exception_ptr capture_runtime();
int static_init_caught();
