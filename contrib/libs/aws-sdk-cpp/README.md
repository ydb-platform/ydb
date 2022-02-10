# AWS SDK for C++
The AWS SDK for C++ provides a modern C++ (version C++ 11 or later) interface for Amazon Web Services (AWS). It is meant to be performant and fully functioning with low- and high-level SDKs, while minimizing dependencies and providing platform portability (Windows, OSX, Linux, and mobile). 

AWS SDK for C++ is in now in General Availability and recommended for production use. We invite our customers to join
the development efforts by submitting pull requests and sending us feedback and ideas via GitHub Issues.

## Version 1.8 is now Available! 

Version 1.8 introduces much asked for new features and changes to the SDK but, because this might also cause compatibility issues with previous versions we've decided to keep it as a seperate branch to make the transition less jarring. 

For more information see the [What’s New in AWS SDK for CPP Version 1.8](https://github.com/aws/aws-sdk-cpp/wiki/What%E2%80%99s-New-in-AWS-SDK-for-CPP-Version-1.8) entry of the wiki, and also please provide any feedback you may have of these changes on our pinned [issue](https://github.com/aws/aws-sdk-cpp/issues/1373). 


__Jump To:__ 
* [Getting Started](#Getting-Started) 
* [Issues and Contributions](#issues-and-contributions) 
* [Getting Help](#Getting-Help) 
* [Using the SDK and Other Topics](#Using-the-SDK-and-Other-Topics) 

# Getting Started 
 
## Building the SDK: 
 
### Minimum Requirements: 
* Visual Studio 2015 or later
* OR GNU Compiler Collection (GCC) 4.9 or later
* OR Clang 3.3 or later
* 4GB of RAM
  * 4GB of RAM is required to build some of the larger clients. The SDK build may fail on EC2 instance types t2.micro, t2.small and other small instance types due to insufficient memory.

### Building From Source: 
 
#### To create an **out-of-source build**: 
1. Install CMake and the relevant build tools for your platform. Ensure these are available in your executable path.
2. Create your build directory. Replace <BUILD_DIR> with your build directory name: 

3. Build the project: 

   * For Auto Make build systems: 
   ```sh 
   cd <BUILD_DIR> 
   cmake <path-to-root-of-this-source-code> -DCMAKE_BUILD_TYPE=Debug 
   make 
   sudo make install 
   ``` 

   * For Visual Studio: 
   ```sh 
   cd <BUILD_DIR> 
   cmake <path-to-root-of-this-source-code> -G "Visual Studio 15 Win64" -DCMAKE_BUILD_TYPE=Debug 
   msbuild ALL_BUILD.vcxproj /p:Configuration=Debug 
   ``` 

   * For macOS - Xcode: 
   ```sh 
   cmake <path-to-root-of-this-source-code> -G Xcode -DTARGET_ARCH="APPLE" -DCMAKE_BUILD_TYPE=Debug 
   xcodebuild -target ALL_BUILD 
   ``` 

### Third party dependencies: 
Starting from version 1.7.0, we added several third party dependencies, including [`aws-c-common`](https://github.com/awslabs/aws-c-common), [`aws-checksums`](https://github.com/awslabs/aws-checksums) and [`aws-c-event-stream`](https://github.com/awslabs/aws-c-event-stream). By default, they will be built and installed in `<BUILD_DIR>/.deps/install`, and copied to default system directory during SDK installation. You can change the location by specifying `CMAKE_INSTALL_PREFIX`.

However, if you want to build and install these libraries in custom locations: 
1. Download, build and install `aws-c-common`: 
   ```sh 
   git clone https://github.com/awslabs/aws-c-common 
   cd aws-c-common 
   # checkout to a specific commit id if you want. 
   git checkout <commit-id> 
   mkdir build && cd build 
   # without CMAKE_INSTALL_PREFIX, it will be installed to default system directory. 
   cmake .. -DCMAKE_INSTALL_PREFIX=<deps-install-dir> <extra-cmake-parameters-here> 
   make # or MSBuild ALL_BUILD.vcxproj on Windows 
   make install # or MSBuild INSTALL.vcxproj on Windows 
   ``` 
2. Download, build and install `aws-checksums`:
   ```sh 
   git clone https://github.com/awslabs/aws-checksums 
   cd aws-checksums 
   # checkout to a specific commit id if you want 
   git checkout <commit-id> 
   mkdir build && cd build 
   # without CMAKE_INSTALL_PREFIX, it will be installed to default system directory. 
   cmake .. -DCMAKE_INSTALL_PREFIX=<deps-install-dir> <extra-cmake-parameters-here> 
   make # or MSBuild ALL_BUILD.vcxproj on Windows 
   make install # or MSBuild INSTALL.vcxproj on Windows 
   ``` 
3. Download, build and install `aws-c-event-stream`:
   ```sh 
   git clone https://github.com/awslabs/aws-c-event-stream 
   cd aws-c-event-stream 
   # checkout to a specific commit id if you want 
   git checkout <commit-id> 
   mkdir build && cd build 
   # aws-c-common and aws-checksums are dependencies of aws-c-event-stream 
   # without CMAKE_INSTALL_PREFIX, it will be installed to default system directory. 
   cmake .. -DCMAKE_INSTALL_PREFIX=<deps-install-dir> -DCMAKE_PREFIX_PATH=<deps-install-dir> <extra-cmake-parameters-here> 
   make # or MSBuild ALL_BUILD.vcxproj on Windows 
   make install # or MSBuild INSTALL.vcxproj on Windows 
   ``` 
4. Turn off `BUILD_DEPS` when building C++ SDK:
   ```sh 
   cd BUILD_DIR 
   cmake <path-to-root-of-this-source-code> -DBUILD_DEPS=OFF -DCMAKE_PREFIX_PATH=<deps-install-dir> 
   ``` 
You may also find the following link helpful for including the build in your project:
https://aws.amazon.com/blogs/developer/using-cmake-exports-with-the-aws-sdk-for-c/

#### Other Dependencies: 
To compile in Linux, you must have the header files for libcurl, libopenssl. The packages are typically available in your package manager.

Debian example:
   `sudo apt-get install libcurl-dev`

### Building for Android 
To build for Android, add `-DTARGET_ARCH=ANDROID` to your cmake command line. Currently we support Android APIs from 19 to 28 with Android NDK 19c and we are using build-in cmake toolchain file supplied by Android NDK, assuming you have the appropriate environment variables (ANDROID_NDK) set. 

##### Android on Windows 
Building for Android on Windows requires some additional setup.  In particular, you will need to run cmake from a Visual Studio developer command prompt (2015 or higher). Additionally, you will need 'git' and 'patch' in your path.  If you have git installed on a Windows system, then patch is likely found in a sibling directory (.../Git/usr/bin/). Once you've verified these requirements, your cmake command line will change slightly to use nmake: 

   ```sh 
   cmake -G "NMake Makefiles" `-DTARGET_ARCH=ANDROID` <other options> .. 
   ``` 

Nmake builds targets in a serial fashion.  To make things quicker, we recommend installing JOM as an alternative to nmake and then changing the cmake invocation to: 

   ```sh 
   cmake -G "NMake Makefiles JOM" `-DTARGET_ARCH=ANDROID` <other options> .. 
   ``` 

### Building for Docker 

To build for Docker, ensure your container meets the [minimum requirements](#minimum-requirements). By default, Docker Desktop is set to use 2 GB runtime memory. We have provided [Dockerfiles](https://github.com/aws/aws-sdk-cpp/tree/master/CI/docker-file) as templates for building the SDK in a container. 


### Building and running an app on EC2 
Checkout this walkthrough on how to set up an enviroment and build the [AWS SDK for C++ on an EC2 instance](https://github.com/aws/aws-sdk-cpp/wiki/Building-the-SDK-from-source-on-EC2). 

# Issues and Contributions 
We welcome all kinds of contributions, check [this guideline](./CONTRIBUTING.md) to learn how you can contribute or report issues. 

# Maintenance and support for SDK major versions 

For information about maintenance and support for SDK major versions and our underlying dependencies, see the following in the AWS SDKs and Tools Shared Configuration and Credentials Reference Guide 

* [AWS SDKs and Tools Maintenance Policy](https://docs.aws.amazon.com/credref/latest/refdocs/maint-policy.html) 
* [AWS SDKs and Tools Version Support Matrix](https://docs.aws.amazon.com/credref/latest/refdocs/version-support-matrix.html) 



# Getting Help 

The best way to interact with our team is through GitHub. You can [open an issue](https://github.com/aws/aws-sdk-cpp/issues/new/choose) and choose from one of our templates for guidance, bug reports, or feature requests. You may also find help on community resources such as [StackOverFlow](https://stackoverflow.com/questions/tagged/aws-sdk-cpp) with the tag #aws-sdk-cpp or If you have a support plan with [AWS Support](https://aws.amazon.com/premiumsupport/), you can also create a new support case. 

Please make sure to check out our resources too before opening an issue: 
* Our [Developer Guide](https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/welcome.html) and [API reference](http://sdk.amazonaws.com/cpp/api/LATEST/index.html) 
* Our [Changelog](./CHANGELOG.md) for recent breaking changes. 
* Our [Contribute](./CONTRIBUTING.md) guide. 
* Our [samples repo](https://github.com/awsdocs/aws-doc-sdk-examples/tree/master/cpp). 

# Using the SDK and Other Topics 
* [Using the SDK](./Docs/SDK_usage_guide.md) 
* [CMake Parameters](./Docs/CMake_Parameters.md) 
* [Credentials Providers](./Docs/Credentials_Providers.md) 
* [Client Configuration Parameters](./Docs/ClientConfiguration_Parameters.md) 
* [Service Client](./Docs/Service_Client.md) 
* [Memory Management](./Docs/Memory_Management.md) 
* [Advanced Topics](./Docs/Advanced_topics.md) 
* [Add as CMake external project](./Docs/CMake_External_Project.md) 
* [Coding Standards](./Docs/CODING_STANDARDS.md) 
* [License](./LICENSE) 
* [Code of Conduct](./CODE_OF_CONDUCT.md) 
