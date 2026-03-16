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


#include "CHDBPoco/Util/Timer.h"
#include "CHDBPoco/Notification.h"
#include "CHDBPoco/ErrorHandler.h"
#include "CHDBPoco/Event.h"


using CHDBPoco::ErrorHandler;


namespace CHDBPoco {
namespace Util {


class TimerNotification: public CHDBPoco::Notification
{
public:
	TimerNotification(CHDBPoco::TimedNotificationQueue& queue):
		_queue(queue)
	{
	}

	~TimerNotification()
	{
	}

	virtual bool execute() = 0;

	CHDBPoco::TimedNotificationQueue& queue()
	{
		return _queue;
	}

private:
	CHDBPoco::TimedNotificationQueue& _queue;
};


class StopNotification: public TimerNotification
{
public:
	StopNotification(CHDBPoco::TimedNotificationQueue& queue):
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
	CancelNotification(CHDBPoco::TimedNotificationQueue& queue):
		TimerNotification(queue)
	{
	}

	~CancelNotification()
	{
	}

	bool execute()
	{
		// Check if there's a StopNotification pending.
		CHDBPoco::AutoPtr<TimerNotification> pNf = static_cast<TimerNotification*>(queue().dequeueNotification());
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
	CHDBPoco::Event _finished;
};


class TaskNotification: public TimerNotification
{
public:
	TaskNotification(CHDBPoco::TimedNotificationQueue& queue, TimerTask::Ptr pTask):
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
	PeriodicTaskNotification(CHDBPoco::TimedNotificationQueue& queue, TimerTask::Ptr pTask, long interval):
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
			CHDBPoco::Clock now;
			CHDBPoco::Clock nextExecution;
			nextExecution += static_cast<CHDBPoco::Clock::ClockDiff>(_interval)*1000;
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
	FixedRateTaskNotification(CHDBPoco::TimedNotificationQueue& queue, TimerTask::Ptr pTask, long interval, CHDBPoco::Clock clock):
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
			CHDBPoco::Clock now;
			_nextExecution += static_cast<CHDBPoco::Clock::ClockDiff>(_interval)*1000;
			if (_nextExecution < now) _nextExecution = now;
			queue().enqueueNotification(this, _nextExecution);
			duplicate();
		}
		return true;
	}

private:
	long _interval;
	CHDBPoco::Clock _nextExecution;
};


Timer::Timer()
{
	_thread.start(*this);
}


Timer::Timer(CHDBPoco::Thread::Priority priority)
{
	_thread.setPriority(priority);
	_thread.start(*this);
}


Timer::~Timer()
{
	try
	{
		_queue.enqueueNotification(new StopNotification(_queue), CHDBPoco::Clock(0));
		_thread.join();
	}
	catch (...)
	{
		CHDB_poco_unexpected();
	}
}


void Timer::cancel(bool wait)
{
	CHDBPoco::AutoPtr<CancelNotification> pNf = new CancelNotification(_queue);
	_queue.enqueueNotification(pNf, CHDBPoco::Clock(0));
	if (wait)
	{
		pNf->wait();
	}
}


void Timer::schedule(TimerTask::Ptr pTask, CHDBPoco::Timestamp time)
{
	validateTask(pTask);
	_queue.enqueueNotification(new TaskNotification(_queue, pTask), time);
}


void Timer::schedule(TimerTask::Ptr pTask, CHDBPoco::Clock clock)
{
	validateTask(pTask);
	_queue.enqueueNotification(new TaskNotification(_queue, pTask), clock);
}


void Timer::schedule(TimerTask::Ptr pTask, long delay, long interval)
{
	CHDBPoco::Clock clock;
	clock += static_cast<CHDBPoco::Clock::ClockDiff>(delay)*1000;
	schedule(pTask, clock, interval);
}


void Timer::schedule(TimerTask::Ptr pTask, CHDBPoco::Timestamp time, long interval)
{
	validateTask(pTask);
	_queue.enqueueNotification(new PeriodicTaskNotification(_queue, pTask, interval), time);
}


void Timer::schedule(TimerTask::Ptr pTask, CHDBPoco::Clock clock, long interval)
{
	validateTask(pTask);
	_queue.enqueueNotification(new PeriodicTaskNotification(_queue, pTask, interval), clock);
}


void Timer::scheduleAtFixedRate(TimerTask::Ptr pTask, long delay, long interval)
{
	CHDBPoco::Clock clock;
	clock += static_cast<CHDBPoco::Clock::ClockDiff>(delay)*1000;
	scheduleAtFixedRate(pTask, clock, interval);
}


void Timer::scheduleAtFixedRate(TimerTask::Ptr pTask, CHDBPoco::Timestamp time, long interval)
{
	validateTask(pTask);
	CHDBPoco::Timestamp tsNow;
	CHDBPoco::Clock clock;
	CHDBPoco::Timestamp::TimeDiff diff = time - tsNow;
	clock += diff;
	_queue.enqueueNotification(new FixedRateTaskNotification(_queue, pTask, interval, clock), clock);
}


void Timer::scheduleAtFixedRate(TimerTask::Ptr pTask, CHDBPoco::Clock clock, long interval)
{
	validateTask(pTask);
	_queue.enqueueNotification(new FixedRateTaskNotification(_queue, pTask, interval, clock), clock);
}


void Timer::run()
{
	bool cont = true;
	while (cont)
	{
		CHDBPoco::AutoPtr<TimerNotification> pNf = static_cast<TimerNotification*>(_queue.waitDequeueNotification());
		cont = pNf->execute();
	}
}


void Timer::validateTask(const TimerTask::Ptr& pTask)
{
	if (pTask->isCancelled())
	{
		throw CHDBPoco::IllegalStateException("A cancelled task must not be rescheduled");
	}
}


} } // namespace CHDBPoco::Util
