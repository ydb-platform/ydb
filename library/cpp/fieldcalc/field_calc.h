#pragma once

#include <cstdio>

#include <library/cpp/deprecated/autoarray/autoarray.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/memory/segmented_string_pool.h>

struct dump_item;
struct calc_op;
struct named_dump_item;
struct calc_elem;
class IOutputStream;

template <class T>
std::pair<const named_dump_item*, size_t> get_named_dump_items();

class TFieldCalculatorBase {
private:
    segmented_string_pool pool;
    void emit_op(TVector<calc_op>& ops, calc_elem& left, calc_elem& right);
    void MarkLocalVarsAsUncalculated();

protected:
    autoarray<dump_item> printouts, conditions;
    int out_el, out_cond;
    TVector<calc_op> calc_ops; // operands for calculator, indexed by arr_ind for DIT_math_result

    TVector<std::pair<const named_dump_item*, size_t>> named_dump_items;
    TMap<const char*, dump_item> local_vars;

    char* get_field(dump_item& dst, char* s);
    bool get_local_var(dump_item& dst, char* s);
    virtual bool item_by_name(dump_item& it, const char* name) const;

    TFieldCalculatorBase();
    virtual ~TFieldCalculatorBase();

    bool Cond(const char** d);
    bool CondById(const char** d, int condNumber);
    void Print(FILE* p, const char** d, const char* Name);
    void Compile(char** field_names, int field_count);
    void SelfTest();
    void PrintDiff(const char* d1, const char* d2);
    void CalcAll(const char** d, TVector<float>& result) const;
    void DumpAll(IOutputStream& s, const char** d, const TStringBuf& delim);
};

template <class T>
class TFieldCalculator: protected  TFieldCalculatorBase {
public:
    TFieldCalculator() {
        named_dump_items.push_back(get_named_dump_items<T>());
    }

    ~TFieldCalculator() override = default;

    bool Cond(const T& d) {
        const char* dd = reinterpret_cast<const char*>(&d);
        return TFieldCalculatorBase::Cond(&dd);
    }

    bool CondById(const T& d, int condNumber) {
        const char* dd = reinterpret_cast<const char*>(&d);
        return TFieldCalculatorBase::CondById(&dd, condNumber);
    }

    void Print(const T& d, const char* Name) {
        const char* dd = reinterpret_cast<const char*>(&d);
        return TFieldCalculatorBase::Print(stdout, &dd, Name);
    }

    void Print(FILE* p, const T& d, const char* Name) {
        const char* dd = reinterpret_cast<const char*>(&d);
        return TFieldCalculatorBase::Print(p, &dd, Name);
    }

    size_t Compile(char** field_names, int field_count) {
        TFieldCalculatorBase::Compile(field_names, field_count);
        return out_el; // number of fields printed
    }

    void SelfTest() {
        return TFieldCalculatorBase::SelfTest();
    }

    void PrintDiff(const T& d1, const T& d2) {
        return TFieldCalculatorBase::PrintDiff((const char*)&d1, (const char*)&d2);
    }

    void CalcAll(const T& d, TVector<float>& result) const {
        const char* dd = reinterpret_cast<const char*>(&d);
        return TFieldCalculatorBase::CalcAll(&dd, result);
    }

    // it appends to `result', clear it yourself
    void DumpAll(IOutputStream& s, const T& d, const TStringBuf& delim) {
        const char* dd = reinterpret_cast<const char*>(&d);
        return TFieldCalculatorBase::DumpAll(s, &dd, delim);
    }
};

template <class T, class T2>
class TFieldCalculator2: protected  TFieldCalculator<T> {
public:
    TFieldCalculator2() {
        TFieldCalculator<T>::named_dump_items.push_back(get_named_dump_items<T2>());
    }

    ~TFieldCalculator2() override = default;

    bool Cond(const T& d, const T2& d2) {
        const char* dd[2] = {reinterpret_cast<const char*>(&d), reinterpret_cast<const char*>(&d2)};
        return TFieldCalculatorBase::Cond(dd);
    }

    bool CondById(const T& d, const T2& d2, int condNumber) {
        const char* dd[2] = {reinterpret_cast<const char*>(&d), reinterpret_cast<const char*>(&d2)};
        return TFieldCalculatorBase::CondById(dd, condNumber);
    }

    void Print(const T& d, const T2& d2, const char* Name) {
        const char* dd[2] = {reinterpret_cast<const char*>(&d), reinterpret_cast<const char*>(&d2)};
        return TFieldCalculatorBase::Print(stdout, dd, Name);
    }

    void Print(FILE* p, const T& d, const T2& d2, const char* Name) {
        const char* dd[2] = {reinterpret_cast<const char*>(&d), reinterpret_cast<const char*>(&d2)};
        return TFieldCalculatorBase::Print(p, dd, Name);
    }

    size_t Compile(char** field_names, int field_count) {
        return TFieldCalculator<T>::Compile(field_names, field_count);
    }
};
