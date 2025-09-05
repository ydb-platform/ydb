# YDB - Distributed SQL Database

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

YDB is an open source distributed SQL database that combines high availability and scalability with strict consistency and ACID transactions. The codebase is primarily C++ with sophisticated build and test infrastructure.

**Related Documentation**: See `AGENTS.md` for basic project guidance and `BUILD.md` for detailed build instructions.

## Working Effectively

### Quick Start - Essential Commands
**CRITICAL: All build and test commands take significant time. NEVER CANCEL these operations.**

#### Bootstrap and Build (Ya Make - Primary Method)
- Install dependencies: `sudo apt-get update && sudo apt-get -y install git python3-pip`
- **Ya Make requires internet access** to download Yandex toolchains from `devtools-registry.s3.yandex.net`. If blocked, use CMake method below.
- Navigate to repository root: `cd /path/to/ydb`
- Check ya version: `./ya --version` -- downloads toolchains on first run, takes 2-5 minutes
- Build YDB server: `./ya make ydb/apps/ydbd --build relwithdebinfo` -- **NEVER CANCEL: 45-90 minutes. Set timeout 120+ minutes.**
- Build YDB CLI: `./ya make ydb/apps/ydb --build relwithdebinfo` -- **NEVER CANCEL: 30-60 minutes. Set timeout 90+ minutes.**

#### Bootstrap and Build (CMake - Secondary Method) 
If Ya Make fails due to network restrictions:
- **Note**: CMake files are pre-generated in the `cmakebuild` branch, but this branch is on `main`
- Generate CMake files: `./generate_cmake` -- requires working ya tool initially, may fail without network
- Create build directory: `mkdir build && cd build`
- Configure: `cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ../`
- Build YDB server: `ninja ydb/apps/ydbd/all` -- **NEVER CANCEL: 60-120 minutes. Set timeout 150+ minutes.**
- Build YDB CLI: `ninja ydb/apps/ydb/all` -- **NEVER CANCEL: 45-90 minutes. Set timeout 120+ minutes.**

#### Testing
- Run small/medium tests: `./ya test ydb/core --test-size small,medium` -- **NEVER CANCEL: 15-45 minutes. Set timeout 60+ minutes.**
- Run specific test: `./ya test ydb/core/engine/ut` -- **NEVER CANCEL: 5-15 minutes. Set timeout 30+ minutes.**
- Add large tests: Include `--test-size small,medium,large` for comprehensive testing

### Build Presets and Configurations
- `debug` - Debug build with full symbols
- `relwithdebinfo` - **RECOMMENDED** - Release with debug info, fastest builds with caching
- `release` - Full release build
- `release-asan` - Release with AddressSanitizer (slower, 1 retry default)
- `release-tsan` - Release with ThreadSanitizer (slower, 1 retry default)  
- `release-msan` - Release with MemorySanitizer (slower, 1 retry default)

### Test Categories
- `small` - Fast unit tests, run frequently 
- `medium` - Integration tests, moderate time
- `large` - System tests, longest running

## Repository Structure and Navigation

### Key Directories
- `/ydb/core` - Core database engine modules 
- `/ydb/library` - YDB-specific libraries
- `/ydb/apps` - Applications:
  - `ydb/apps/ydb` - YDB CLI tool
  - `ydb/apps/ydbd` - YDB database server
  - `ydb/apps/dstool` - Distributed storage tool
- `/ydb/docs` - Documentation
- `/library` - **Common libraries; NEVER change these**
- `/util` - **Common libraries; NEVER change these**
- `/contrib` - Third-party contributions
- `/yql` - YQL query language implementation

### Build System Files  
- `ya.make` - Ya Make build file (one per directory)
- `CMakeLists.txt` - Generated CMake files (in cmakebuild branch)
- `ya.conf` - Ya Make configuration
- `.github/workflows/` - CI/CD workflows
- `build/` - Build system scripts and tools

## Validation and Quality Assurance

### Always Run Before Committing
- Build your changes: `./ya make path/to/modified/code --build relwithdebinfo`
- Run related tests: `./ya test path/to/modified/code --test-size small,medium`
- **No specific linting required** - the build system handles code quality checks

### Manual Validation After Changes
- **Build validation**: Verify executables are created in expected locations
- **Functionality testing**: If modifying core components, run a basic YDB scenario:
  1. Start server: `./ydb/apps/ydbd/ydbd --help` (verify it runs)
  2. Check CLI: `./ydb/apps/ydb/ydb --help` (verify it runs)
- **For UI changes**: Take screenshots to document visual impact
- **For API changes**: Test with actual client code if possible

### CI Expectations
- **Build checks**: PR must pass `build_relwithdebinfo` and `build_release-asan`
- **Test checks**: PR must pass `test_relwithdebinfo` for small,medium tests
- **Integration status**: All checks must show "success" for merge approval
- **Timeout failures**: If CI times out, it's usually infrastructure - rerun the checks

## Development Workflow Patterns

### Common Tasks
1. **Finding code**: Use `find ydb/core -name "*.cpp" | xargs grep "pattern"` for C++ code searches
2. **Understanding dependencies**: Check `ya.make` files for `PEERDIR()` entries
3. **Adding new features**: 
   - Create/modify `ya.make` in target directory
   - Add source files 
   - Add `PEERDIR()` dependencies
   - Add unit tests in `ut/` subdirectory
4. **Testing specific areas**:
   - Core engine: `./ya test ydb/core/engine/ut`  
   - Storage: `./ya test ydb/core/blobstorage/ut`
   - Query processing: `./ya test ydb/core/kqp/ut`

### Troubleshooting Build Issues
- **Ya tool network errors**: Check internet connectivity to `devtools-registry.s3.yandex.net`. Error pattern: `<urlopen error [Errno -5] No address associated with hostname>`
- **CMake generation fails**: Ensure ya tool works first - `./ya --version`
- **Build failures**: Check specific error in build output, often missing dependencies in `ya.make`
- **Test failures**: Check if tests are muted in `.github/config/muted_ya.txt`
- **Disk space**: Builds require 80+ GB free space (repository is ~2.4GB, builds can expand 20-30x), preferably on SSD

### Performance Notes
- **Remote caching**: Ya Make uses distributed build cache for faster builds
- **Parallel builds**: Default thread counts are optimized (52 for build, 28 for tests)
- **Incremental builds**: Subsequent builds after initial are much faster
- **ccache**: CMake builds can leverage ccache for significant speedup

## Project-Specific Conventions

### Code Standards
- **C++ Standard**: Modern C++, no later than C++20
- **Memory management**: Uses custom allocators (tcmalloc on Linux)
- **Error handling**: Uses YDB-specific status codes and error types
- **Logging**: Uses YDB logging framework, not std::cout

### Testing Patterns
- **Unit tests**: In `ut/` subdirectories, file pattern `*_ut.cpp`
- **Test types**: `UNITTEST_FOR(target)` for unit tests
- **Test sizes**: Set with `SIZE(SMALL|MEDIUM|LARGE)` 
- **Sanitizer tests**: Automatically run with retry logic in CI

### Pull Request Guidelines
- **Changelog entry required**: Brief summary of changes
- **PR categories**: Choose one: New Feature, Improvement, Bugfix, etc.
- **Documentation**: Update if API or behavior changes
- **Review process**: Core team reviews, automated checks must pass

## Timeouts and Performance Expectations

**CRITICAL TIMEOUT VALUES - NEVER use default timeouts:**

### Build Commands (Set timeout 120+ minutes)
- Full repository build: 90-150 minutes
- Single application build: 30-90 minutes  
- Incremental builds: 5-30 minutes

### Test Commands (Set timeout 60+ minutes)
- Small tests: 5-15 minutes
- Medium tests: 15-30 minutes  
- Large tests: 30-90 minutes
- Full test suite: 60+ minutes

### One-time Setup (Set timeout 10+ minutes)
- Ya tool download: 2-5 minutes
- Dependency installation: 3-10 minutes
- CMake generation: 5-15 minutes

**WARNING: Build and test times vary significantly based on system resources and network. Always use generous timeouts and NEVER CANCEL long-running operations.**