#pragma once

namespace WAVM { namespace Runtime {
	struct Compartment;
	struct Instance;
}}

namespace WAVM { namespace ThreadTest {
	WAVM_API Runtime::Instance* instantiate(Runtime::Compartment* compartment);
}}
