#pragma once

#include <string>

class IWorkloadQueryGenerator {
public:
    virtual ~IWorkloadQueryGenerator() {}

    virtual std::string GetDDLQueries() = 0;
};

struct TWorkloadParams {
    std::string DbPath;
};