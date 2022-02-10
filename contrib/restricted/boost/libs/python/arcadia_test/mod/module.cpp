#include <boost/python.hpp> 
 
using namespace boost::python; 
 
static const char* hello() { 
    return "hello world!"; 
} 
 
BOOST_PYTHON_MODULE(arcadia_boost_python_test) { 
    def("hello", &hello); 
} 
