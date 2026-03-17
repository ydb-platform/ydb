//
// TimerTask.h
//
// Library: Util
// Package: Timer
// Module:  TimerTask
//
// Definition of the TimerTask class.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Util_TimerTask_INCLUDED
#define DB_Util_TimerTask_INCLUDED


#include "DBPoco/AutoPtr.h"
#include "DBPoco/RefCountedObject.h"
#include "DBPoco/Runnable.h"
#include "DBPoco/Timestamp.h"
#include "DBPoco/Util/Util.h"


namespace DBPoco
{
namespace Util
{


    class Util_API TimerTask : public DBPoco::RefCountedObject, public DBPoco::Runnable
    /// A task that can be scheduled for one-time or
    /// repeated execution by a Timer.
    ///
    /// This is an abstract class. Subclasses must override the run() member
    /// function to implement the actual task logic.
    {
    public:
        typedef DBPoco::AutoPtr<TimerTask> Ptr;

        TimerTask();
        /// Creates the TimerTask.

        void cancel();
        /// Cancels the execution of the timer.
        /// If the task has been scheduled for one-time execution and has
        /// not yet run, or has not yet been scheduled, it will never run.
        /// If the task has been scheduled for repeated execution, it will never
        /// run again. If the task is running when this call occurs, the task
        /// will run to completion, but will never run again.
        ///
        /// Warning: A TimerTask that has been cancelled must not be scheduled again.
        /// An attempt to do so results in a DBPoco::Util::IllegalStateException being thrown.

        bool isCancelled() const;
        /// Returns true iff the TimerTask has been cancelled by a call
        /// to cancel().

        DBPoco::Timestamp lastExecution() const;
        /// Returns the time of the last execution of the timer task.
        ///
        /// Returns 0 if the timer has never been executed.

    protected:
        ~TimerTask();
        /// Destroys the TimerTask.

    private:
        TimerTask(const TimerTask &);
        TimerTask & operator=(const TimerTask &);

        DBPoco::Timestamp _lastExecution;
        bool _isCancelled;

        friend class TaskNotification;
    };


    //
    // inlines
    //
    inline bool TimerTask::isCancelled() const
    {
        return _isCancelled;
    }


    inline DBPoco::Timestamp TimerTask::lastExecution() const
    {
        return _lastExecution;
    }


}
} // namespace DBPoco::Util


#endif // DB_Util_TimerTask_INCLUDED
