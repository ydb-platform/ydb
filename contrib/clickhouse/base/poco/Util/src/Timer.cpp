//
// Timer.cpp
//
// Library: Util
// Package: Timer
// Module:  Timer
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/Timer.h"
#include "DBPoco/Notification.h"
#include "DBPoco/ErrorHandler.h"
#include "DBPoco/Event.h"


using DBPoco::ErrorHandler;


namespace DBPoco {
namespace Util {


class TimerNotification: public DBPoco::Notification
{
public:
	TimerNotification(DBPoco::TimedNotificationQueue& queue):
		_queue(queue)
	{
	}

	~TimerNotification()
	{
	}

	virtual bool execute() = 0;

	DBPoco::TimedNotificationQueue& queue()
	{
		return _queue;
	}

private:
	DBPoco::TimedNotificationQueue& _queue;
};


class StopNotification: public TimerNotification
{
public:
	StopNotification(DBPoco::TimedNotificationQueue& queue):
		TimerNotification(queue)
	{
	}

	~StopNotification()
	{
	}

	bool execute()
	{
		queue().clear();
		return false;
	}
};


class CancelNotification: public TimerNotification
{
public:
	CancelNotification(DBPoco::TimedNotificationQueue& queue):
		TimerNotification(queue)
	{
	}

	~CancelNotification()
	{
	}

	bool execute()
	{
		// Check if there's a StopNotification pending.
		DBPoco::AutoPtr<TimerNotification> pNf = static_cast<TimerNotification*>(queue().dequeueNotification());
		while (pNf)
		{
			if (pNf.cast<StopNotification>())
			{
				queue().clear();
				_finished.set();
				return false;
			}
			pNf = static_cast<TimerNotification*>(queue().dequeueNotification());
		}

		queue().clear();
		_finished.set();
		return true;
	}

	void wait()
	{
		_finished.wait();
	}

private:
	DBPoco::Event _finished;
};


class TaskNotification: public TimerNotification
{
public:
	TaskNotification(DBPoco::TimedNotificationQueue& queue, TimerTask::Ptr pTask):
		TimerNotification(queue),
		_pTask(pTask)
	{
	}

	~TaskNotification()
	{
	}

	TimerTask::Ptr task()
	{
		return _pTask;
	}

	bool execute()
	{
		if (!_pTask->isCancelled())
		{
			try
			{
				_pTask->_lastExecution.update();
				_pTask->run();
			}
			catch (Exception& exc)
			{
				ErrorHandler::handle(exc);
			}
			catch (std::exception& exc)
			{
				ErrorHandler::handle(exc);
			}
			catch (...)
			{
				ErrorHandler::handle();
			}
		}
		return true;
	}

private:
	TimerTask::Ptr _pTask;
};


class PeriodicTaskNotification: public TaskNotification
{
public:
	PeriodicTaskNotification(DBPoco::TimedNotificationQueue& queue, TimerTask::Ptr pTask, long interval):
		TaskNotification(queue, pTask),
		_interval(interval)
	{
	}

	~PeriodicTaskNotification()
	{
	}

	bool execute()
	{
		TaskNotification::execute();

		if (!task()->isCancelled())
		{
			DBPoco::Clock now;
			DBPoco::Clock nextExecution;
			nextExecution += static_cast<DBPoco::Clock::ClockDiff>(_interval)*1000;
			if (nextExecution < now) nextExecution = now;
			queue().enqueueNotification(this, nextExecution);
			duplicate();
		}
		return true;
	}

private:
	long _interval;
};


class FixedRateTaskNotification: public TaskNotification
{
public:
	FixedRateTaskNotification(DBPoco::TimedNotificationQueue& queue, TimerTask::Ptr pTask, long interval, DBPoco::Clock clock):
		TaskNotification(queue, pTask),
		_interval(interval),
		_nextExecution(clock)
	{
	}

	~FixedRateTaskNotification()
	{
	}

	bool execute()
	{
		TaskNotification::execute();

		if (!task()->isCancelled())
		{
			DBPoco::Clock now;
			_nextExecution += static_cast<DBPoco::Clock::ClockDiff>(_interval)*1000;
			if (_nextExecution < now) _nextExecution = now;
			queue().enqueueNotification(this, _nextExecution);
			duplicate();
		}
		return true;
	}

private:
	long _interval;
	DBPoco::Clock _nextExecution;
};


Timer::Timer()
{
	_thread.start(*this);
}


Timer::Timer(DBPoco::Thread::Priority priority)
{
	_thread.setPriority(priority);
	_thread.start(*this);
}


Timer::~Timer()
{
	try
	{
		_queue.enqueueNotification(new StopNotification(_queue), DBPoco::Clock(0));
		_thread.join();
	}
	catch (...)
	{
		DB_poco_unexpected();
	}
}


void Timer::cancel(bool wait)
{
	DBPoco::AutoPtr<CancelNotification> pNf = new CancelNotification(_queue);
	_queue.enqueueNotification(pNf, DBPoco::Clock(0));
	if (wait)
	{
		pNf->wait();
	}
}


void Timer::schedule(TimerTask::Ptr pTask, DBPoco::Timestamp time)
{
	validateTask(pTask);
	_queue.enqueueNotification(new TaskNotification(_queue, pTask), time);
}


void Timer::schedule(TimerTask::Ptr pTask, DBPoco::Clock clock)
{
	validateTask(pTask);
	_queue.enqueueNotification(new TaskNotification(_queue, pTask), clock);
}


void Timer::schedule(TimerTask::Ptr pTask, long delay, long interval)
{
	DBPoco::Clock clock;
	clock += static_cast<DBPoco::Clock::ClockDiff>(delay)*1000;
	schedule(pTask, clock, interval);
}


void Timer::schedule(TimerTask::Ptr pTask, DBPoco::Timestamp time, long interval)
{
	validateTask(pTask);
	_queue.enqueueNotification(new PeriodicTaskNotification(_queue, pTask, interval), time);
}


void Timer::schedule(TimerTask::Ptr pTask, DBPoco::Clock clock, long interval)
{
	validateTask(pTask);
	_queue.enqueueNotification(new PeriodicTaskNotification(_queue, pTask, interval), clock);
}


void Timer::scheduleAtFixedRate(TimerTask::Ptr pTask, long delay, long interval)
{
	DBPoco::Clock clock;
	clock += static_cast<DBPoco::Clock::ClockDiff>(delay)*1000;
	scheduleAtFixedRate(pTask, clock, interval);
}


void Timer::scheduleAtFixedRate(TimerTask::Ptr pTask, DBPoco::Timestamp time, long interval)
{
	validateTask(pTask);
	DBPoco::Timestamp tsNow;
	DBPoco::Clock clock;
	DBPoco::Timestamp::TimeDiff diff = time - tsNow;
	clock += diff;
	_queue.enqueueNotification(new FixedRateTaskNotification(_queue, pTask, interval, clock), clock);
}


void Timer::scheduleAtFixedRate(TimerTask::Ptr pTask, DBPoco::Clock clock, long interval)
{
	validateTask(pTask);
	_queue.enqueueNotification(new FixedRateTaskNotification(_queue, pTask, interval, clock), clock);
}


void Timer::run()
{
	bool cont = true;
	while (cont)
	{
		DBPoco::AutoPtr<TimerNotification> pNf = static_cast<TimerNotification*>(_queue.waitDequeueNotification());
		cont = pNf->execute();
	}
}


void Timer::validateTask(const TimerTask::Ptr& pTask)
{
	if (pTask->isCancelled())
	{
		throw DBPoco::IllegalStateException("A cancelled task must not be rescheduled");
	}
}


} } // namespace DBPoco::Util
