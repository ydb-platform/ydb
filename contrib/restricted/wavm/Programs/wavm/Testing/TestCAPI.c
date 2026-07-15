#include <inttypes.h>
#include <string.h>
#include "WAVM/wavm-c/wavm-c.h"
#include "wavm-test.h"

#define own

static uintptr_t numCallbacks = 0;

// A function to be called from Wasm code.
own wasm_trap_t* hello_callback(const wasm_val_t args[], wasm_val_t results[])
{
	++numCallbacks;
	return NULL;
}

int execCAPITest(int argc, char** argv)
{
	// Initialize.
	wasm_engine_t* engine = wasm_engine_new();
	wasm_compartment_t* compartment = wasm_compartment_new(engine, "compartment");
	wasm_store_t* store = wasm_store_new(compartment, "store");

	// Load binary.
	char hello_wasm[]
		= {0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x84, 0x80, 0x80, 0x80, 0x00, 0x01,
		   0x60, 0x00, 0x00, 0x02, 0x8a, 0x80, 0x80, 0x80, 0x00, 0x01, 0x00, 0x05, 0x68, 0x65, 0x6c,
		   0x6c, 0x6f, 0x00, 0x00, 0x03, 0x82, 0x80, 0x80, 0x80, 0x00, 0x01, 0x00, 0x07, 0x87, 0x80,
		   0x80, 0x80, 0x00, 0x01, 0x03, 0x72, 0x75, 0x6e, 0x00, 0x01, 0x0a, 0x8a, 0x80, 0x80, 0x80,
		   0x00, 0x01, 0x84, 0x80, 0x80, 0x80, 0x00, 0x00, 0x10, 0x00, 0x0b};

	// Compile.
	own wasm_module_t* module = wasm_module_new(engine, hello_wasm, sizeof(hello_wasm));
	if(!module) { return 1; }

	// Create external print functions.
	own wasm_functype_t* hello_type = wasm_functype_new_0_0();
	own wasm_func_t* hello_func
		= wasm_func_new(compartment, hello_type, hello_callback, "hello_callback");

	wasm_functype_delete(hello_type);

	// Instantiate.
	const wasm_extern_t* imports[1];
	imports[0] = wasm_func_as_extern(hello_func);
	own wasm_instance_t* instance = wasm_instance_new(store, module, imports, NULL, "instance");
	if(!instance) { return 1; }

	wasm_func_delete(hello_func);

	// Extract export.
	wasm_extern_t* run_extern = wasm_instance_export(instance, 0);
	if(run_extern == NULL) { return 1; }
	const wasm_func_t* run_func = wasm_extern_as_func(run_extern);
	if(run_func == NULL) { return 1; }

	wasm_module_delete(module);
	wasm_instance_delete(instance);

	// Call.
	if(wasm_func_call(store, run_func, NULL, NULL)) { return 1; }

	// Shut down.
	wasm_store_delete(store);
	wasm_compartment_delete(compartment);
	wasm_engine_delete(engine);

	// Assert that the callback was called exactly once.
	if(numCallbacks != 1) { return 1; }

	return 0;
}
