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

#ifndef GEOS_PROFILER_H
#define GEOS_PROFILER_H

#include <geos/export.h>
#include <chrono>

#include <map>
#include <memory>
#include <iostream>
#include <string>
#include <vector>

#ifndef PROFILE
#define PROFILE 0
#endif

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace util {


/*
 * \class Profile utils.h geos.h
 *
 * \brief Profile statistics
 */
class GEOS_DLL Profile {
public:
    using timeunit = std::chrono::microseconds;

    /** \brief Create a named profile */
    Profile(std::string name);

    /** \brief Destructor */
    ~Profile() = default;

    /** \brief start a new timer */
    void
    start()
    {
        starttime = std::chrono::high_resolution_clock::now();
    }

    /** \brief stop current timer */
    void
    stop()
    {
        stoptime = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<timeunit>(stoptime - starttime);

        timings.push_back(elapsed);

        totaltime += elapsed;
        if(timings.size() == 1) {
            max = min = elapsed;
        }
        else {
            if(elapsed > max) {
                max = elapsed;
            }
            if(elapsed < min) {
                min = elapsed;
            }
        }

        avg = static_cast<double>(totaltime.count()) / static_cast<double>(timings.size());
    }

    /** \brief Return Max stored timing */
    double getMax() const;

    /** \brief Return Min stored timing */
    double getMin() const;

    /** \brief Return total timing */
    double getTot() const;

    /** \brief Return total timing */
    std::string getTotFormatted() const;

    /** \brief Return average timing */
    double getAvg() const;

    /** \brief Return number of timings */
    size_t getNumTimings() const;

    /** \brief Profile name */
    std::string name;



private:
    /* \brief current start and stop times */
    std::chrono::high_resolution_clock::time_point starttime, stoptime;

    /* \brief actual times */
    std::vector<timeunit> timings;

    /* \brief total time */
    timeunit totaltime;

    /* \brief max time */
    timeunit max;

    /* \brief max time */
    timeunit min;

    /* \brief avg time */
    double avg;
};

/*
 * \class Profiler utils.h geos.h
 *
 * \brief Profiling class
 *
 */
class GEOS_DLL Profiler {

public:

    Profiler() = default;
    ~Profiler() = default;

    Profiler(const Profiler&) = delete;
    Profiler& operator=(const Profiler&) = delete;

    /**
     * \brief
     * Return the singleton instance of the
     * profiler.
     */
    static Profiler* instance(void);

    /**
     * \brief
     * Start timer for named task. The task is
     * created if does not exist.
     */
    void start(std::string name);

    /**
     * \brief
     * Stop timer for named task.
     * Elapsed time is registered in the given task.
     */
    void stop(std::string name);

    /** \brief get Profile of named task */
    Profile* get(std::string name);

    std::map<std::string, std::unique_ptr<Profile>> profs;
};


/** \brief Return a string representing the Profile */
GEOS_DLL std::ostream& operator<< (std::ostream& os, const Profile&);

/** \brief Return a string representing the Profiler */
GEOS_DLL std::ostream& operator<< (std::ostream& os, const Profiler&);

} // namespace geos::util
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_PROFILER_H
