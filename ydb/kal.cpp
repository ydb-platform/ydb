#include <iostream>
#include <string>


template<typename T>
void PrintSizeIfString(const T& value)
{
    if constexpr (std::is_same_v<T, std::string>)
    {
        std::cout << value.size();
    }
    else
    {
        std::cout << "not string";
    }
}


int main() {
    std::string string{"10"};
    PrintSizeIfString(string);
    PrintSizeIfString(10);
}