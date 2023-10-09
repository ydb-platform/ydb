#pragma once

#include <library/cpp/lwtrace/all.h>
#include <util/stream/file.h>

// Example of custom state for custom executors
// Holds file for output from TMyActionExecutor
class TMyFile: public NLWTrace::IResource {
private:
    TMutex Mutex;
    THolder<TUnbufferedFileOutput> File;

public:
    // Note that this class must have default ctor (it's declared here just for clearness)
    TMyFile() {
    }

    // Note that dtor will be called by TManager::Delete() after detachment and destruction of all executors
    ~TMyFile() {
    }

    // Some kind of state initialization
    // Can be called only from executor constructor, and should not be thread-safe
    void Open(const TString& path) {
        if (File) {
            // We must avoid double open because each executor will call Open() on the same object
            // if the same file was specified in Opts
            return;
        }
        File.Reset(new TUnbufferedFileOutput(path));
    }

    // Outputs a line to opened file
    // Can be called from DoExecute() and must be thread-safe
    void Output(const TString& line) {
        Y_ABORT_UNLESS(File);
        TGuard<TMutex> g(Mutex); // Because DoExecute() call can come from any thread
        *File << line << Endl;
    }
};

// Action that prints events to specified file
class TMyActionExecutor: public NLWTrace::TCustomActionExecutor {
private:
    TMyFile& File;

public:
    TMyActionExecutor(NLWTrace::TProbe* probe, const NLWTrace::TCustomAction& action, NLWTrace::TSession* session)
        : NLWTrace::TCustomActionExecutor(probe, false /* not destructive */)
        , File(session->Resources().Get<TMyFile>("FileHolder/" + action.GetOpts(0))) // unique state id must include your d
    {
        if (action.GetOpts().size() != 1) {
            yexception() << "wrong number of Opts in MyAction";
        }
        File.Open(action.GetOpts(0));
    }

    static const char* GetActionName() {
        static const char name[] = "MyAction";
        return name;
    }

private:
    virtual bool DoExecute(NLWTrace::TOrbit&, const NLWTrace::TParams& params) {
        // Serialize param values to strings
        TString paramValues[LWTRACE_MAX_PARAMS];
        Probe->Event.Signature.SerializeParams(params, paramValues);

        // Generate output line
        TStringStream ss;
        ss << "TMyActionExecutor>>> ";
        ss << Probe->Event.Name;
        for (ui32 i = 0; i < Probe->Event.Signature.ParamCount; ++i) {
            ss << " " << Probe->Event.Signature.ParamNames[i] << "=" << paramValues[i];
        }

        // Write line to file
        File.Output(ss.Str());

        // Executors can chain if you specify multiple actions in one block (in trace query).
        // So return true to continue execution of actions for given trace query block (on current triggered event)
        // or return false if this action is the last for this block
        return true;
    }
};
