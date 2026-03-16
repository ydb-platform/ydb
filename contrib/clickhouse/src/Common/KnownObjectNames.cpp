#include <Common/KnownObjectNames.h>
#include <DBPoco/String.h>


namespace DB
{

bool KnownObjectNames::exists(const String & name) const
{
    std::lock_guard lock{mutex};
    if (names.contains(name))
        return true;

    if (!case_insensitive_names.empty())
    {
        String lower_name = DBPoco::toLower(name);
        if (case_insensitive_names.contains(lower_name))
            return true;
    }

    return false;
}


void KnownObjectNames::add(const String & name, bool case_insensitive)
{
    std::lock_guard lock{mutex};
    if (case_insensitive)
        case_insensitive_names.emplace(DBPoco::toLower(name));
    else
        names.emplace(name);
}


KnownTableFunctionNames & KnownTableFunctionNames::instance()
{
    static KnownTableFunctionNames the_instance;
    return the_instance;
}


KnownFormatNames & KnownFormatNames::instance()
{
    static KnownFormatNames the_instance;
    return the_instance;
}

}
