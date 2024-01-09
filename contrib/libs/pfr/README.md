# [PFR](https://apolukhin.github.io/pfr_non_boost/)

This is a C++14 library for very basic reflection that gives you access to structure elements by index and provides other `std::tuple` like methods for user defined types without any macro or boilerplate code.

[Boost.PFR](https://boost.org/libs/pfr) is a part of the [Boost C++ Libraries](https://github.com/boostorg). However, Boost.PFR is a header only library that does not depend on Boost. You can just copy the content of the "include" folder from the github into your project, and the library will work fine.

For a version of the library without `boost::` namespace see [PFR](https://github.com/apolukhin/pfr_non_boost).

### Test results

Branches        | Build         | Tests coverage | More info
----------------|-------------- | -------------- |-----------
Develop:        | [![CI](https://github.com/boostorg/pfr/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/boostorg/pfr/actions/workflows/ci.yml) [![Build status](https://ci.appveyor.com/api/projects/status/0mavmnkdmltcdmqa/branch/develop?svg=true)](https://ci.appveyor.com/project/apolukhin/pfr/branch/develop) | [![Coverage Status](https://coveralls.io/repos/github/apolukhin/magic_get/badge.png?branch=develop)](https://coveralls.io/github/apolukhin/magic_get?branch=develop) | [details...](https://www.boost.org/development/tests/develop/developer/pfr.html)
Master:         | [![CI](https://github.com/boostorg/pfr/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/boostorg/pfr/actions/workflows/ci.yml) [![Build status](https://ci.appveyor.com/api/projects/status/0mavmnkdmltcdmqa/branch/master?svg=true)](https://ci.appveyor.com/project/apolukhin/pfr/branch/master) | [![Coverage Status](https://coveralls.io/repos/github/apolukhin/magic_get/badge.png?branch=master)](https://coveralls.io/github/apolukhin/magic_get?branch=master) | [details...](https://www.boost.org/development/tests/master/developer/pfr.html)

[Latest developer documentation](https://www.boost.org/doc/libs/develop/doc/html/boost_pfr.html)

### Motivating Example #0
```c++
#include <iostream>
#include <fstream>
#include <string>

#include "pfr.hpp"

struct some_person {
  std::string name;
  unsigned birth_year;
};

int main(int argc, const char* argv[]) {
  some_person val{"Edgar Allan Poe", 1809};

  std::cout << pfr::get<0>(val)                // No macro!
      << " was born in " << pfr::get<1>(val);  // Works with any aggregate initializables!

  if (argc > 1) {
    std::ofstream ofs(argv[1]);
    ofs << pfr::io(val);                       // File now contains: {"Edgar Allan Poe", 1809}
  }
}
```
Outputs:
```
Edgar Allan Poe was born in 1809
```

[Run the above sample](https://godbolt.org/z/PfYsWKb7v)


### Motivating Example #1
```c++
#include <iostream>
#include "pfr.hpp"

struct my_struct { // no ostream operator defined!
    int i;
    char c;
    double d;
};

int main() {
    my_struct s{100, 'H', 3.141593};
    std::cout << "my_struct has " << pfr::tuple_size<my_struct>::value
        << " fields: " << pfr::io(s) << "\n";
}

```

Outputs:
```
my_struct has 3 fields: {100, H, 3.14159}
```

### Motivating Example #2

```c++
#include <iostream>
#include "pfr.hpp"

struct my_struct { // no ostream operator defined!
    std::string s;
    int i;
};

int main() {
    my_struct s{{"Das ist fantastisch!"}, 100};
    std::cout << "my_struct has " << pfr::tuple_size<my_struct>::value
        << " fields: " << pfr::io(s) << "\n";
}

```

Outputs:
```
my_struct has 2 fields: {"Das ist fantastisch!", 100}
```

### Motivating Example #3

```c++
#include <iostream>
#include <string>

#include <boost/config/warning_disable.hpp>
#include <boost/spirit/home/x3.hpp>
#include <boost/fusion/include/adapt_boost_pfr.hpp>

#include "pfr/io.hpp"

namespace x3 = boost::spirit::x3;

struct ast_employee { // No BOOST_FUSION_ADAPT_STRUCT defined
    int age;
    std::string forename;
    std::string surname;
    double salary;
};

auto const quoted_string = x3::lexeme['"' >> +(x3::ascii::char_ - '"') >> '"'];

x3::rule<class employee, ast_employee> const employee = "employee";
auto const employee_def =
    x3::lit("employee")
    >> '{'
    >>  x3::int_ >> ','
    >>  quoted_string >> ','
    >>  quoted_string >> ','
    >>  x3::double_
    >>  '}'
    ;
BOOST_SPIRIT_DEFINE(employee);

int main() {
    std::string str = R"(employee{34, "Chip", "Douglas", 2500.00})";
    ast_employee emp;
    x3::phrase_parse(str.begin(),
                     str.end(),
                     employee,
                     x3::ascii::space,
                     emp);
    std::cout << pfr::io(emp) << std::endl;
}

```
Outputs:
```
(34 Chip Douglas 2500)
```


### Requirements and Limitations

[See docs](https://www.boost.org/doc/libs/develop/doc/html/boost_pfr.html).

### License

Distributed under the [Boost Software License, Version 1.0](https://boost.org/LICENSE_1_0.txt).
