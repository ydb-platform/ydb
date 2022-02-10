#pragma once

#include <ydb/core/formats/arrow/compute/registry.h>
#include <ydb/core/formats/arrow/result.h>
#include <ydb/core/formats/arrow/status.h>

#include <algorithm>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include <ydb/core/formats/arrow/compute/function.h>

namespace NKikimr::NArrow { 

template <bool mt = false>
class TFunctionFactory {
public:

    TFunctionFactory() = default;

    arrow::Status AddFunction(std::shared_ptr<arrow::compute::Function> function, bool allowOverwrite) {
        auto it = nameToFunc.find(function->name());
        if (it != nameToFunc.end() && !allowOverwrite) {
            return arrow::Status::KeyError("Already have a function registered with name: ", function->name());
        }
        nameToFunc[function->name()] = std::move(function);
        return arrow::Status::OK();
    }


    arrow::Result<std::shared_ptr<arrow::compute::Function>> GetFunction(const std::string& name) const {
        auto it = nameToFunc.find(name);
        if (it == nameToFunc.end()) {
            return arrow::compute::GetFunctionRegistry()->GetFunction(name);
        }
        return it->second;
    }
private:
    std::unordered_map<std::string, std::shared_ptr<arrow::compute::Function>> nameToFunc;
};

template <>
class TFunctionFactory<true> {
public:

    TFunctionFactory() = default;

    arrow::Status AddFunction(std::shared_ptr<arrow::compute::Function> function, bool allowOverwrite) {
        std::unique_lock guard(lock);
        auto it = nameToFunc.find(function->name());
        if (it != nameToFunc.end() && !allowOverwrite) {
            return arrow::Status::KeyError("Already have a function registered with name: ", function->name());
        }
        nameToFunc[function->name()] = std::move(function);
        return arrow::Status::OK();
    }


    arrow::Result<std::shared_ptr<arrow::compute::Function>> GetFunction(const std::string& name) const {
        std::shared_lock guard(lock);
        auto it = nameToFunc.find(name);
        if (it == nameToFunc.end()) {
            return arrow::compute::GetFunctionRegistry()->GetFunction(name);
        }
        return it->second;
    }
private:
    mutable std::shared_mutex lock;
    std::unordered_map<std::string, std::shared_ptr<arrow::compute::Function>> nameToFunc;
};

} 
