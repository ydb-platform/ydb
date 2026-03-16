/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/profiler.h>
#include <iostream>
#include <map>
#include <string>
#include <sstream>
#include <utility>

using namespace std;

namespace geos {
namespace util { // geos.util

Profile::Profile(string newname) :
    name(newname),
    totaltime(timeunit::zero())
{}

double
Profile::getMax() const
{
    return static_cast<double>(max.count());
}

double
Profile::getMin() const
{
    return static_cast<double>(min.count());
}

double
Profile::getAvg() const
{
    return avg;
}

double
Profile::getTot() const
{
    return static_cast<double>(totaltime.count());
}

std::string
Profile::getTotFormatted() const
{
    std::stringstream usec;
    usec << totaltime.count();

    std::string fmt = usec.str();
    int insertPosition = static_cast<int>(fmt.length()) - 3;
    while (insertPosition > 0) {
        fmt.insert(insertPosition, ",");
        insertPosition-=3;
    }
    return fmt + " usec";
}

size_t
Profile::getNumTimings() const
{
    return timings.size();
}

void
Profiler::start(string name)
{
    auto prof = get(name);
    prof->start();
}

void
Profiler::stop(string name)
{
    auto iter = profs.find(name);
    if(iter == profs.end()) {
        cerr << name << ": no such Profile started";
        return;
    }
    iter->second->stop();
}

Profile*
Profiler::get(string name)
{
    auto& prof = profs[name];
    if (prof == nullptr) {
        prof.reset(new Profile(name));
    }

    return prof.get();
}

Profiler*
Profiler::instance()
{
    static Profiler internal_profiler;
    return &internal_profiler;
}


ostream&
operator<< (ostream& os, const Profile& prof)
{
    os << " num:" << prof.getNumTimings() << " min:" <<
       prof.getMin() << " max:" << prof.getMax() <<
       " avg:" << prof.getAvg() << " tot:" << prof.getTot() <<
       " [" << prof.name << "]";
    return os;
}

ostream&
operator<< (ostream& os, const Profiler& prof)
{
    for(const auto& entry : prof.profs) {
        os << *(entry.second) << endl;
    }
    return os;
}


} // namespace geos.util
} // namespace geos
