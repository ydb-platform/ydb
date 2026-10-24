#pragma once
extern thread_local int a_tls;      // defined in tls_a, initializer 101
extern thread_local char a_str[16]; // "a-init"
extern thread_local int a_bss;
int a_local_counter();              // local-dynamic: static thread_local inside a function
int a_obj_value();                  // thread_local object with ctor/dtor
void a_set(int v);
int b_read_a_tls();                 // tls_b reads tls_a's variable (GD, cross-object)
void b_write_a_tls(int v);
long b_tls_value();                 // tls_b's own, initializer 202
void b_inc();
int ie_get();                       // tls_ie: initial-exec TLS in a .so, initializer 909
void ie_inc();
