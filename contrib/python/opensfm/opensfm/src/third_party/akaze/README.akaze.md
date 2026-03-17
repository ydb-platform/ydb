## README - A-KAZE Features

Version: 1.4.0
Date: 07-10-2014

You can get the latest version of the code from github:
`https://github.com/pablofdezalc/akaze`

## CHANGELOG
Version: 1.4.0
Changes:
- Maximum number of OpenMP threads can be set with OMP_MAX_THREADS definition in AKAZEConfig.h. By default is set to 16. This avoid problems with some systems that have many cores. Thanks to Thomas Fraenz
- Namespace libAKAZE created to avoid conflict with AKAZE class in OpenCV 3.0
- Speed-up in detection in description thanks to the improvements done in the Google Summer of Code 2014 program. Most of this improvements are thanks to Fedor Mozorov, Vadim Pisarevsky and Gary Bradsky
- akaze_match and akaze_compare now automatically perform the image matching estimating a planar homography using RANSAC if the homography txt file is not provided as input argument

Version: 1.3.0
Changes:
- More efficient memory usage
- Now the smoothing is performed after the FED process

Version: 1.2.0
Changes:
- Header file config.h replaced by AKAZEConfig.h
- Header files akaze_compare.h, akaze_features.h and akaze_match.h have been removed
- Matlab interface added by Zohar Bar-Yehuda

Version: 1.1.0
Changes:
- Code style has been changed substantially to match portability with other libraries
- Small bug has been corrected in generateDescriptorSubsample for the random bit selection
- Several modifications proposed from Jesus Nuevo, Pierre Moulon and David Ok have been integrated
- Descriptor modes have changed. No longer use of use_upright flag
- Python interface added by David Ok

## What is this file?

This file explains how to make use of source code for computing A-KAZE features and
two practical image matching applications

## Library Dependencies

The code is mainly based on the OpenCV library using the C++ interface

In order to compile the code, the following libraries to be installed on your system:
- OpenCV version 2.4.0 or higher
- Cmake version 2.6 or higher

If you want to use OpenMP parallelization you will need to install OpenMP in your system
In Linux you can do this by installing the gomp library

You will also need **doxygen** in case you need to generate the documentation

Tested compilers:
- GCC 4.2-4.7
- MSVC 11 x64

Tested systems:
- Ubuntu 11.10, 12.04, 12.10
- Kubuntu 10.04
- Windows 8

## Getting Started

Compiling:

1. `$ mkdir build`
2. `$ cd build>`
3. `$ cmake ..`
4. `$ make`

Additionally you can also install the library in `/usr/local/akaze/lib` by typing:
`$ sudo make install`

If the compilation is successful you should see three executables in the folder bin:
- `akaze_features`
- `akaze_match`
- `akaze_compare`

Additionally, the library `libAKAZE[.a, .lib]` will be created in the folder `lib`.

If there is any error in the compilation, perhaps some libraries are missing.
Please check the Library dependencies section.

Examples:
To see how the code works, examine the three examples provided.

## MATLAB interface

A mex interface for computing AKAZE features is supplied, in the file `mex/akaze.cpp`.

To be able to use it, first compile the library as explained above. Then, you will need to compile the mex from Matlab.

The following is an example for compiling the mex on Windows 64 bit, Visual Studio 10 and OpenCV 2.4.8. from the `mex` folder, type in MATLAB:

`mex akaze.cpp -I'..\src\lib\' -L'..\build\lib\Release\' -I'<path_to_opencv>\build\include' -L'<path_to_opencv>\build\x64\vc10\lib' -lopencv_calib3d248 -lopencv_contrib248 -lopencv_core248 -lopencv_highgui248 -lopencv_imgproc248 -lAKAZE`

The following is an example for compiling the mex on Linux, Ubuntu 12.10, 64 bit, gcc 4.6.4 and OpenCV 2.4.8. from the `mex` folder, type in MATLAB:

For other platforms / compilers / OpenCV versions, change the above line accordingly.
`mex akaze.cpp -I'../src/lib/' -L'../build/lib/' -lAKAZE -L'/usr/local/lib/' -lopencv_imgproc -lopencv_core -lopencv_calib3d -lopencv_highgui -lgomp`

On Windows, you'll need to make sure that the corresponding OpenCV bin folder is added to your system path before staring MATLAB. e.g.:

`PATH=<path_to_opencv>\build\x64\vc10\bin`

Once the mex file is compiled successfully, type:

`akaze`

to display function help.

## Documentation
In the working folder, type: `doxygen`

The documentation will be generated in the ./doc folder

## Computing A-KAZE Features

For running the program you need to type in the command line the following arguments:
`./akaze_features img.jpg [options]`

The `[options]` are not mandatory. In case you do not specify additional options, default arguments will be
used. Here is a description of the additional options:

- `--verbose`: if verbosity is required
- `--help`: for showing the command line options
- `--soffset`: the base scale offset (sigma units)
- `--omax`: the coarsest nonlinear scale space level (sigma units)
- `--nsublevels`: number of sublevels per octave
- `--diffusivity`: diffusivity function `0` -> Perona-Malik 1, `1` -> Perona-Malik 2, `2` -> Weickert
- `--dthreshold`: Feature detector threshold response for accepting points
- `--descriptor`: Descriptor Type, 0-> SURF_UPRIGHT, 1->SURF
                                   2-> M-SURF_UPRIGHT, 3->M-SURF
                                   4-> M-LDB_UPRIGHT, 5->M-LDB
- `--descriptor_channels`: Descriptor Channels for M-LDB. Valid values: 1, 2 (intensity+gradient magnitude), 3(intensity + X and Y gradients)
- `--descriptor_size`: Descriptor size for M-LDB in bits. 0 means the full length descriptor (486). Any other value will use a random bit selection
- `--show_results`: `1` in case we want to show detection results. `0` otherwise

## Important Things:

* Check `config.h` in case you would like to change the value of some default settings
* The **k** constrast factor is computed as the 70% percentile of the gradient histogram of a
smoothed version of the original image. Normally, this empirical value gives good results, but
depending on the input image the diffusion will not be good enough. Therefore I highly
recommend you to visualize the output images from save_scale_space and test with other k
factors if the results are not satisfactory

## Image Matching Example with A-KAZE Features

The code contains one program to perform image matching between two images.
If the ground truth transformation is not provided, the program estimates a fundamental matrix or a planar homography using
RANSAC between the set of correspondences between the two images.

For running the program you need to type in the command line the following arguments:
`./akaze_match img1.jpg img2.pgm homography.txt [options]`

The `[options]` are not mandatory. In case you do not specify additional options, default arguments will be
used. 

The datasets folder contains the **Iguazu** dataset described in the paper and additional datasets from Mikolajczyk et al. evaluation.
The **Iguazu** dataset was generated by adding Gaussian noise of increasing standard deviation.

For example, with the default configuration parameters used in the current code version you should get
the following results:

```
./akaze_match ../../datasets/iguazu/img1.pgm 
              ../../datasets/iguazu/img4.pgm 
              ../../datasets/iguazu/H1to4p
              --descriptor 4
```

```
Number of Keypoints Image 1: 1823
Number of Keypoints Image 2: 2373
A-KAZE Features Extraction Time (ms): 304.796
Matching Descriptors Time (ms): 54.1619
Number of Matches: 1283
Number of Inliers: 1047
Number of Outliers: 236
Inliers Ratio: 81.6056
```

## Image Matching Comparison between A-KAZE, ORB and BRISK (OpenCV)

The code contains one program to perform image matching between two images, showing a comparison between A-KAZE features, ORB
and BRISK. All these implementations are based on the OpenCV library. 

The program assumes that the ground truth transformation is provided

For running the program you need to type in the command line the following arguments:
```
./akaze_compare img1.jpg img2.pgm homography.txt [options]
```

For example, running kaze_compare with the first and third images from the boat dataset you should get the following results:

```
./akaze_compare ../../datasets/boat/img1.pgm 
                ../../datasets/boat/img3.pgm
                ../../datasets/boat/H1to3p
                --dthreshold 0.004
```

```
ORB Results
**************************************
Number of Keypoints Image 1: 1510
Number of Keypoints Image 2: 1516
Number of Matches: 304
Number of Inliers: 277
Number of Outliers: 27
Inliers Ratio: 91.1184
ORB Features Extraction Time (ms): 62.7292

BRISK Results
**************************************
Number of Keypoints Image 1: 3457
Number of Keypoints Image 2: 3031
Number of Matches: 159
Number of Inliers: 116
Number of Outliers: 43
Inliers Ratio: 72.956
BRISK Features Extraction Time (ms): 311.954

A-KAZE Results
**************************************
Number of Keypoints Image 1: 2129
Number of Keypoints Image 2: 1668
Number of Matches: 766
Number of Inliers: 688
Number of Outliers: 78
Inliers Ratio: 89.8172
A-KAZE Features Extraction Time (ms): 519.336
```

**A-KAZE features** is **open source** and you can use that **freely even in commercial applications**. The code is released under BSD license.
While A-KAZE is a bit slower compared to **ORB** and **BRISK**, it provides much better performance. In addition, for images with small resolution such as 640x480 the algorithm can
run in real-time. In the next future we plan to release a GPGPU implementation.

## Citation

If you use this code as part of your work, please cite the following papers:

1. **Fast Explicit Diffusion for Accelerated Features in Nonlinear Scale Spaces**. Pablo F. Alcantarilla, J. Nuevo and Adrien Bartoli. _In British Machine Vision Conference (BMVC), Bristol, UK, September 2013_

2. **KAZE Features**. Pablo F. Alcantarilla, Adrien Bartoli and Andrew J. Davison. _In European Conference on Computer Vision (ECCV), Fiorenze, Italy, October 2012_

## Contact Info

**Important**: If you work in a research institution, university, company or you are a freelance and you are using KAZE or A-KAZE in your work, please send me an email!!
I would like to know the people that are using KAZE and A-KAZE around the world!!"

In case you have any question, find any bug in the code or want to share some improvements,
please contact me:

Pablo F. Alcantarilla
email: pablofdezalc@gmail.com
