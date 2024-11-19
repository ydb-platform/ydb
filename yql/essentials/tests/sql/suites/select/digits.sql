/* postgres can not */
select 0xffut, 0x0ffut, 0o77ut, 0xdead, 0xdeadl, 0xdeadbeef, 0x7fffffff, 0x80000000, 0x80000000u, 0xffffffffu, 0x80000000ul;
select 0xfFUt, 0X0FfuT, 0O77ut, 0xDeaD, 0xdeaDL, 0xdeaDBeef, 0X7fFfFfff, 0x80000000, 0x80000000U, 0xffffffffu, 0x80000000uL;

select 0.f, 0.0f, 1, 1l, 1u, 1ut, 1., 2.f, Math::Round(3.14f, -3), 1.e2f, 1.0e2f;
select 0.F, 0.0f, 1, 1L, 1u, 1Ut, 1., 2.f, Math::Round(3.14F, -3), 1.e2f, 1.0E2F;

select 1t,2ut,3s,4us,5,6u,7l,8ul,9.0,10.0f;
