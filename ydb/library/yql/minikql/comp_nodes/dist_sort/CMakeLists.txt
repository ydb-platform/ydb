cmake_minimum_required(VERSION 3.27)
project(distributed_sort)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_FLAGS "-O3")

add_executable(distributed_sort main.cpp)
