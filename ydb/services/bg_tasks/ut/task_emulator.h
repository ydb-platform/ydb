#include <ydb/services/bg_tasks/abstract/activity.h>
#include <ydb/services/bg_tasks/abstract/task.h>
#include <ydb/services/bg_tasks/service.h>

#include <util/system/hostname.h>

namespace NKikimr {

    class TTestInsertTaskState: public NBackgroundTasks::IJsonStringSerializable<NBackgroundTasks::ITaskState> {
    private:
        static TFactory::TRegistrator<TTestInsertTaskState> Registrator;

        YDB_ACCESSOR(ui32, Counter, 0);
    protected:
        virtual NJson::TJsonValue DoSerializeToJson() const override {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("counter", Counter);
            return result;
        }
        virtual bool DoDeserializeFromJson(const NJson::TJsonValue& jsonData) override {
            Counter = jsonData["counter"].GetUIntegerSafe();
            return true;
        }
    public:
        TTestInsertTaskState() = default;
        TTestInsertTaskState(const ui32 counter)
            : Counter(counter)
        {

        }
        static TString GetClassNameStatic() {
            return "test_insert";
        }
        virtual TString GetClassName() const override {
            return GetClassNameStatic();
        }
    };

    class TTestInsertTaskScheduler: public NBackgroundTasks::IJsonStringSerializable<NBackgroundTasks::ITaskScheduler> {
    private:
        static TFactory::TRegistrator<TTestInsertTaskScheduler> Registrator;

        YDB_ACCESSOR(ui32, Limit, 3);
    protected:
        virtual TInstant DoGetStartInstant() const override {
            return TInstant::Zero();
        }

        virtual std::optional<TInstant> DoGetNextStartInstant(
            const TInstant /*currentStartInstant*/, const NBackgroundTasks::TTaskStateContainer& state) const override {
            if (state.GetAsSafe<TTestInsertTaskState>().GetCounter() <= Limit) {
                return TInstant::Zero();
            } else {
                return {};
            }
        }

        virtual NJson::TJsonValue DoSerializeToJson() const override {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("limit", Limit);
            return result;
        }
        virtual bool DoDeserializeFromJson(const NJson::TJsonValue& jsonData) override {
            Limit = jsonData["limit"].GetUIntegerSafe();
            return true;
        }
    public:
        TTestInsertTaskScheduler() = default;
        virtual TString GetClassName() const override {
            return TTestInsertTaskState::GetClassNameStatic();
        }
    };

    class TTestInsertTaskActivity: public NBackgroundTasks::IJsonStringSerializable<NBackgroundTasks::ITaskActivity> {
    private:
        static TFactory::TRegistrator<TTestInsertTaskActivity> Registrator;
        YDB_READONLY(TString, ActivityTaskId, TGUID::CreateTimebased().AsUuidString());

    protected:
        virtual NJson::TJsonValue DoSerializeToJson() const override {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("id", ActivityTaskId);
            return result;
        }
        virtual bool DoDeserializeFromJson(const NJson::TJsonValue& jsonData) override {
            ActivityTaskId = jsonData["id"].GetStringRobust();
            return true;
        }
        virtual void DoFinished(const NBackgroundTasks::TTaskStateContainer& /*state*/) override;

        virtual void DoExecute(NBackgroundTasks::ITaskExecutorController::TPtr controller,
            const NBackgroundTasks::TTaskStateContainer& currentState) override;
    public:
        static bool IsFinished(const TString& id);
        static ui32 GetCounterSum(const TString& id);
        virtual TString GetClassName() const override {
            return TTestInsertTaskState::GetClassNameStatic();
        }
    };
}

