#ifndef STAN_MATH_OPENCL_KERNEL_CL_HPP
#define STAN_MATH_OPENCL_KERNEL_CL_HPP
#ifdef STAN_OPENCL
#include <stan/math/opencl/opencl_context.hpp>
#include <stan/math/opencl/kernels/helpers.hpp>
#error #include <CL/cl.hpp>
#include <string>
#include <algorithm>
#include <map>
#include <vector>

// Used for importing the OpenCL kernels at compile time.
// There has been much discussion about the best ways to do this:
// https://github.com/bstatcomp/math/pull/7
// and https://github.com/stan-dev/math/pull/966
#ifndef STRINGIFY
#define STRINGIFY(src) #src
#endif

namespace stan {
namespace math {
namespace opencl_kernels {

/**
 * Compile an OpenCL kernel.
 *
 * @param name The name for the kernel
 * @param sources A std::vector of strings containing the code for the kernel.
 * @param options The values of macros to be passed at compile time.
 */
inline auto compile_kernel(const char* name,
                           const std::vector<const char*>& sources,
                           std::map<const char*, int>& options) {
  std::string kernel_opts = "";
  for (auto&& comp_opts : options) {
    kernel_opts += std::string(" -D") + comp_opts.first + "="
                   + std::to_string(comp_opts.second);
  }
  std::string kernel_source;
  for (const char* source : sources) {
    kernel_source.append(source);
  }
  cl::Program program;
  try {
    cl::Program::Sources src(1, std::make_pair(kernel_source.c_str(),
                                               strlen(kernel_source.c_str())));
    program = cl::Program(opencl_context.context(), src);
    program.build({opencl_context.device()}, kernel_opts.c_str());

    return cl::Kernel(program, name);
  } catch (const cl::Error& e) {
    // in case of CL_BUILD_PROGRAM_FAILURE, print the build error
    if (e.err() == -11) {
      std::string buildlog = program.getBuildInfo<CL_PROGRAM_BUILD_LOG>(
          opencl_context.device()[0]);
      system_error("compile_kernel", name, e.err(), buildlog.c_str());
    } else {
      check_opencl_error(name, e);
    }
  }
  return cl::Kernel();  // never reached because check_opencl_error throws
}

/**
 * Functor used for compiling kernels.
 *
 * @tparam Args Parameter pack of all kernel argument types.
 */
template <typename... Args>
class kernel_functor {
 private:
  cl::Kernel kernel_;
  std::map<const char*, int> opts_;

 public:
  /**
   * functor to access the kernel compiler.
   * @param name The name for the kernel.
   * @param sources A std::vector of strings containing the code for the kernel.
   * @param options The values of macros to be passed at compile time.
   */
  kernel_functor(const char* name, const std::vector<const char*>& sources,
                 const std::map<const char*, int>& options) {
    auto base_opts = opencl_context.base_opts();
    for (auto& it : options) {
      if (base_opts[it.first] > it.second) {
        base_opts[it.first] = it.second;
      }
    }
    kernel_ = compile_kernel(name, sources, base_opts);
    opts_ = base_opts;
  }

  auto operator()() const { return cl::make_kernel<Args...>(kernel_); }

  /**
   * @return The options that the kernel was compiled with.
   */
  const std::map<const char*, int>& get_opts() const { return opts_; }
};

/**
 * Creates functor for kernels that only need access to defining
 *  the global work size.
 *
 * @tparam Args Parameter pack of all kernel argument types.
 */
template <typename... Args>
struct global_range_kernel {
  const kernel_functor<Args...> make_functor;
  /**
   * Creates functor for kernels that only need access to defining
   *  the global work size.
   * @param name The name for the kernel
   * @param source A string literal containing the code for the kernel.
   * @param options The values of macros to be passed at compile time.
   */
  global_range_kernel(const char* name, const char* source,
                      const std::map<const char*, int>& options = {})
      : make_functor(name, {source}, options) {}
  /**
   * Creates functor for kernels that only need access to defining
   *  the global work size.
   * @param name The name for the kernel
   * @param sources A std::vector of strings containing the code for the kernel.
   * @param options The values of macros to be passed at compile time.
   */
  global_range_kernel(const char* name, const std::vector<const char*>& sources,
                      const std::map<const char*, int>& options = {})
      : make_functor(name, sources, options) {}
  /**
   * Executes a kernel
   * @param global_thread_size The global work size.
   * @param args The arguments to pass to the kernel.
   * @tparam Args Parameter pack of all kernel argument types.
   */
  auto operator()(cl::NDRange global_thread_size, Args... args) const {
    auto f = make_functor();
    cl::EnqueueArgs eargs(opencl_context.queue(), global_thread_size);
    f(eargs, args...).wait();
  }
};
/**
 * Creates functor for kernels that need to define both
 *  local and global work size.
 * @tparam Args Parameter pack of all kernel argument types.
 */
template <typename... Args>
struct local_range_kernel {
  const kernel_functor<Args...> make_functor;
  /**
   * Creates kernels that need access to defining the global thread
   * size and the thread block size.
   * @param name The name for the kernel
   * @param source A string literal containing the code for the kernel.
   * @param options The values of macros to be passed at compile time.
   */
  local_range_kernel(const char* name, const char* source,
                     const std::map<const char*, int>& options = {})
      : make_functor(name, {source}, options) {}
  /**
   * Creates kernels that need access to defining the global thread
   * size and the thread block size.
   * @param name The name for the kernel
   * @param sources A std::vector of strings containing the code for the kernel.
   * @param options The values of macros to be passed at compile time.
   */
  local_range_kernel(const char* name, const std::vector<const char*>& sources,
                     const std::map<const char*, int>& options = {})
      : make_functor(name, sources, options) {}
  /**
   * Executes a kernel
   * @param global_thread_size The global work size.
   * @param thread_block_size The thread block size.
   * @param args The arguments to pass to the kernel.
   * @tparam Args Parameter pack of all kernel argument types.
   */
  auto operator()(cl::NDRange global_thread_size, cl::NDRange thread_block_size,
                  Args... args) const {
    auto f = make_functor();
    cl::EnqueueArgs eargs(opencl_context.queue(), global_thread_size,
                          thread_block_size);
    f(eargs, args...).wait();
  }
};

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan

#endif
#endif
